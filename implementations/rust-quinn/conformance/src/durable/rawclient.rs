//! Raw v2 wire-abuse peer for the G6 rows. The conformance driver itself
//! speaks QUIC+mTLS and hand-frames the small v2 control set with minimal
//! deterministic-CBOR writers (no production codec is imported; the crate's
//! existing quinn/rustls deps only). Malformed frames are replayed verbatim
//! from the frozen `test-vectors/v2/wire.tsv` corpus: the driver checks each
//! row's sha256 and never regenerates the bytes.

use crate::durable::mtls::Material;
use crate::{decode_hex, hex};
use anyhow::{Context, Result, bail, ensure};
use quinn::crypto::rustls::QuicClientConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Duration};

pub const ALPN_V2: &[u8] = b"pipestream/2";
/// QUIC application close codes: 0x200 + the Section 12.2 refusal code.
pub const QUIC_FRAME_ERROR: u64 = 0x201;
pub const QUIC_EXTENSION_UNSUPPORTED: u64 = 0x202;
pub const QUIC_CONTROL_RESET: u64 = 0x20e;
/// Named application refusal codes (Section 12.2).
pub const CODE_FRAME_ERROR: u64 = 1;
pub const CODE_LIMIT_EXCEEDED: u64 = 4;
pub const CODE_EXTENSION_UNSUPPORTED: u64 = 2;
pub const CODE_INTEGRITY_ERROR: u64 = 8;
pub const CODE_NOT_READY: u64 = 9;
/// Control frame types.
pub const FRAME_CAPABILITIES: u8 = 1;
pub const FRAME_SESSION: u8 = 2;
pub const FRAME_SCOPE: u8 = 3;
pub const FRAME_WORK: u8 = 4;
pub const FRAME_RESULT: u8 = 5;
pub const FRAME_DRAIN: u8 = 6;
pub const FRAME_REFUSAL: u8 = 7;

const PROBE_TIMEOUT: Duration = Duration::from_secs(15);
/// How long an open may wait for peer stream budget before the row records
/// transport-level ceiling enforcement (normative-clarifications item 4).
pub const OPEN_BUDGET_TIMEOUT: Duration = Duration::from_secs(4);

const WIRE_TSV: &str = include_str!("../../../../../test-vectors/v2/wire.tsv");

// ---------------------------------------------------------------------------
// Minimal deterministic-CBOR writer (definite lengths, minimal integers)
// ---------------------------------------------------------------------------

pub fn cbor_uint(out: &mut Vec<u8>, value: u64) {
    if value < 24 {
        out.push(value as u8);
    } else if value <= u8::MAX as u64 {
        out.extend_from_slice(&[0x18, value as u8]);
    } else if value <= u16::MAX as u64 {
        out.push(0x19);
        out.extend_from_slice(&(value as u16).to_be_bytes());
    } else if value <= u32::MAX as u64 {
        out.push(0x1a);
        out.extend_from_slice(&(value as u32).to_be_bytes());
    } else {
        out.push(0x1b);
        out.extend_from_slice(&value.to_be_bytes());
    }
}

pub fn cbor_array(out: &mut Vec<u8>, len: usize) {
    if len < 24 {
        out.push(0x80 | len as u8);
    } else if len <= u8::MAX as usize {
        out.extend_from_slice(&[0x98, len as u8]);
    } else {
        out.push(0x99);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    }
}

pub fn cbor_bytes(out: &mut Vec<u8>, data: &[u8]) {
    if data.len() < 24 {
        out.push(0x40 | data.len() as u8);
    } else if data.len() <= u8::MAX as usize {
        out.extend_from_slice(&[0x58, data.len() as u8]);
    } else {
        out.push(0x59);
        out.extend_from_slice(&(data.len() as u16).to_be_bytes());
    }
    out.extend_from_slice(data);
}

pub fn cbor_text(out: &mut Vec<u8>, text: &str) {
    let len = text.len();
    if len < 24 {
        out.push(0x60 | len as u8);
    } else if len <= u8::MAX as usize {
        out.extend_from_slice(&[0x78, len as u8]);
    } else {
        out.push(0x79);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    }
    out.extend_from_slice(text.as_bytes());
}

pub fn cbor_bool(out: &mut Vec<u8>, value: bool) {
    out.push(if value { 0xf5 } else { 0xf4 });
}

pub fn control_frame(frame_type: u8, body: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(body.len() + 5);
    frame.push(frame_type);
    frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
    frame.extend_from_slice(body);
    frame
}

// ---------------------------------------------------------------------------
// Minimal CBOR reader (definite items only; skip() for unneeded subtrees)
// ---------------------------------------------------------------------------

pub struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    pub fn done(&self) -> Result<()> {
        ensure!(
            self.pos == self.bytes.len(),
            "trailing bytes after CBOR item: {} unread",
            self.bytes.len() - self.pos
        );
        Ok(())
    }

    fn head(&mut self, major: u8) -> Result<u64> {
        let byte = *self.bytes.get(self.pos).context("truncated CBOR head")?;
        self.pos += 1;
        ensure!(
            byte >> 5 == major,
            "unexpected CBOR major type {} (wanted {major})",
            byte >> 5
        );
        let info = byte & 0x1f;
        match info {
            n @ 0..=23 => Ok(n as u64),
            24 => {
                let v = *self.bytes.get(self.pos).context("truncated CBOR u8")?;
                self.pos += 1;
                Ok(v as u64)
            }
            25 => {
                let v = u16::from_be_bytes(self.take::<2>()?);
                Ok(v as u64)
            }
            26 => {
                let v = u32::from_be_bytes(self.take::<4>()?);
                Ok(v as u64)
            }
            27 => Ok(u64::from_be_bytes(self.take::<8>()?)),
            other => bail!("indefinite or reserved CBOR info {other}"),
        }
    }

    fn take<const N: usize>(&mut self) -> Result<[u8; N]> {
        ensure!(self.pos + N <= self.bytes.len(), "truncated CBOR payload");
        let value = self.bytes[self.pos..self.pos + N].try_into()?;
        self.pos += N;
        Ok(value)
    }

    pub fn array_len(&mut self) -> Result<u64> {
        self.head(4)
    }

    pub fn uint(&mut self) -> Result<u64> {
        self.head(0)
    }

    pub fn boolean(&mut self) -> Result<bool> {
        let byte = *self.bytes.get(self.pos).context("truncated CBOR bool")?;
        self.pos += 1;
        match byte {
            0xf4 => Ok(false),
            0xf5 => Ok(true),
            other => bail!("not a CBOR bool: {other:#x}"),
        }
    }

    pub fn option_null(&mut self) -> Result<bool> {
        match self.bytes.get(self.pos) {
            Some(0xf6) => {
                self.pos += 1;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    pub fn text(&mut self) -> Result<String> {
        let len = self.head(3)? as usize;
        ensure!(self.pos + len <= self.bytes.len(), "truncated CBOR text");
        let value = std::str::from_utf8(&self.bytes[self.pos..self.pos + len])
            .context("CBOR text is not UTF-8")?
            .to_owned();
        self.pos += len;
        Ok(value)
    }

    pub fn skip(&mut self) -> Result<()> {
        let byte = *self.bytes.get(self.pos).context("truncated CBOR item")?;
        self.pos += 1;
        let (major, info) = (byte >> 5, byte & 0x1f);
        let arg: Option<u64> = match info {
            n @ 0..=23 => Some(n as u64),
            24 => {
                let v = *self.bytes.get(self.pos).context("truncated")? as u64;
                self.pos += 1;
                Some(v)
            }
            25 => Some(u16::from_be_bytes(self.take::<2>()?) as u64),
            26 => Some(u32::from_be_bytes(self.take::<4>()?) as u64),
            27 => Some(u64::from_be_bytes(self.take::<8>()?)),
            28..=30 => bail!("reserved CBOR info"),
            31 => None,
            _ => unreachable!(),
        };
        match major {
            0 | 1 => {}
            2 | 3 => {
                let n = arg.context("indefinite-length string")? as usize;
                self.pos = self
                    .pos
                    .checked_add(n)
                    .filter(|end| *end <= self.bytes.len())
                    .context("truncated CBOR string")?;
            }
            4 => {
                for _ in 0..arg.context("indefinite array")? {
                    self.skip()?;
                }
            }
            5 => {
                for _ in 0..arg.context("indefinite map")?.saturating_mul(2) {
                    self.skip()?;
                }
            }
            6 => self.skip()?, // tagged content
            7 => match info {
                25 => self.pos += 2,
                26 => self.pos += 4,
                27 => self.pos += 8,
                31 => bail!("indefinite break outside item"),
                _ => {}
            },
            _ => unreachable!(),
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Frozen wire corpus access (checked, never regenerated)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct FrozenRow {
    pub root: String,
    pub expectation: String,
    /// The named refusal the vector assigns (read by the refuse-subset test).
    #[cfg_attr(not(test), allow(dead_code))]
    pub error: String,
    /// The full frozen frame bytes including its length prefix.
    pub frame: Vec<u8>,
}

pub fn frozen(name: &str) -> Result<FrozenRow> {
    let mut lines = WIRE_TSV.lines();
    ensure!(
        lines.next() == Some("name\troot\tframing\tcddl\texpectation\terror\tsha256\thex"),
        "invalid version-2 corpus header"
    );
    for line in lines {
        let f = line.split('\t').collect::<Vec<_>>();
        if f.len() == 8 && f[0] == name {
            let frame = decode_hex(f[7])?;
            ensure!(
                hex(&Sha256::digest(&frame)) == f[6],
                "frozen bytes changed for vector {name}"
            );
            return Ok(FrozenRow {
                root: f[1].to_owned(),
                expectation: f[4].to_owned(),
                error: f[5].to_owned(),
                frame,
            });
        }
    }
    bail!("missing frozen wire vector {name}")
}

// ---------------------------------------------------------------------------
// Hand-framed control bodies (known-answer tested against the frozen corpus)
// ---------------------------------------------------------------------------

pub const POLICY: [u64; 3] = [60_000, 120_000, 300_000];

fn cbor_policy(out: &mut Vec<u8>) {
    cbor_array(out, 3);
    for ms in POLICY {
        cbor_uint(out, ms);
    }
}

pub fn session_create(request: u64, creation_sequence: u64) -> Vec<u8> {
    let mut body = Vec::new();
    cbor_array(&mut body, 4);
    cbor_uint(&mut body, 0); // Session::Create
    cbor_uint(&mut body, request);
    cbor_uint(&mut body, creation_sequence);
    cbor_policy(&mut body);
    body
}

pub fn session_attach(request: u64, authority: &str, owner: &str, generation: u64) -> Vec<u8> {
    let mut body = Vec::new();
    cbor_array(&mut body, 5);
    cbor_uint(&mut body, 2); // Session::Attach
    cbor_uint(&mut body, request);
    cbor_text(&mut body, authority);
    cbor_text(&mut body, owner);
    cbor_uint(&mut body, generation);
    body
}

pub fn session_binding_response(
    request: u64,
    authority: &str,
    owner: &str,
    generation: u64,
    creation_sequence: u64,
) -> Vec<u8> {
    // A server-direction Session::Binding (kind 1), used by the
    // direction-violation probe.
    let mut body = Vec::new();
    cbor_array(&mut body, 8);
    cbor_uint(&mut body, 1); // Session::Binding
    cbor_uint(&mut body, request);
    cbor_text(&mut body, authority);
    cbor_text(&mut body, owner);
    cbor_uint(&mut body, generation);
    cbor_uint(&mut body, creation_sequence);
    cbor_policy(&mut body);
    cbor_array(&mut body, 6); // negotiated limits echo (values unvalidated)
    for value in [32u64, 1024, 4096, 1_073_741_824, 1_073_741_824, 16] {
        cbor_uint(&mut body, value);
    }
    body
}

pub fn next_sequence(request: u64) -> Vec<u8> {
    let mut body = Vec::new();
    cbor_array(&mut body, 2);
    cbor_uint(&mut body, 3); // Session::NextSequence
    cbor_uint(&mut body, request);
    body
}

pub fn scope_declare(
    request: u64,
    operation: &[u8; 16],
    scope: u64,
    entities: &[u64],
    seal: bool,
) -> Vec<u8> {
    let mut body = Vec::new();
    cbor_array(&mut body, 6);
    cbor_uint(&mut body, 0); // Scope::Declare
    cbor_uint(&mut body, request);
    cbor_bytes(&mut body, operation);
    cbor_uint(&mut body, scope);
    cbor_array(&mut body, entities.len());
    for entity in entities {
        cbor_uint(&mut body, *entity);
    }
    cbor_bool(&mut body, seal);
    body
}

pub fn scope_page(request: u64, scope: u64) -> Vec<u8> {
    let mut body = Vec::new();
    cbor_array(&mut body, 5);
    cbor_uint(&mut body, 2); // Scope::Page
    cbor_uint(&mut body, request);
    cbor_uint(&mut body, scope);
    cbor_uint(&mut body, 0); // after_entity
    cbor_uint(&mut body, 256); // page limit
    body
}

pub fn work_watch(
    request: u64,
    work: (u64, u64, u64),
    after_revision: u64,
    wait_ms: u64,
) -> Vec<u8> {
    let mut body = Vec::new();
    cbor_array(&mut body, 5);
    cbor_uint(&mut body, 4); // Work::Watch
    cbor_uint(&mut body, request);
    cbor_array(&mut body, 3);
    cbor_uint(&mut body, work.0);
    cbor_uint(&mut body, work.1);
    cbor_uint(&mut body, work.2);
    cbor_uint(&mut body, after_revision);
    cbor_uint(&mut body, wait_ms);
    body
}

pub fn drain_detach(request: u64) -> Vec<u8> {
    let mut body = Vec::new();
    cbor_array(&mut body, 2);
    cbor_uint(&mut body, 2); // Drain::Detach
    cbor_uint(&mut body, request);
    body
}

/// Object-header-framed input header (4-byte length prefix, no type byte).
#[allow(clippy::too_many_arguments)]
pub fn input_header_framed(
    generation: u64,
    operation: &[u8; 16],
    work: (u64, u64, u64),
    length: u64,
    sha256: &[u8; 32],
    content_type: &str,
    application: &str,
    mode: u64,
    execution_ms: u64,
    output_count: u64,
    output_bytes: u64,
) -> Vec<u8> {
    let mut body = Vec::new();
    cbor_array(&mut body, 4);
    cbor_uint(&mut body, 0); // InputHeader kind
    cbor_uint(&mut body, generation);
    cbor_bytes(&mut body, operation);
    cbor_array(&mut body, 6); // AdmitParameters
    cbor_array(&mut body, 3);
    cbor_uint(&mut body, work.0);
    cbor_uint(&mut body, work.1);
    cbor_uint(&mut body, work.2);
    cbor_array(&mut body, 3); // Input{length, sha256, content_type}
    cbor_uint(&mut body, length);
    cbor_bytes(&mut body, sha256);
    cbor_text(&mut body, content_type);
    cbor_text(&mut body, application);
    cbor_uint(&mut body, mode);
    cbor_uint(&mut body, execution_ms);
    cbor_array(&mut body, 2); // OutputBudget{count, total_bytes}
    cbor_uint(&mut body, output_count);
    cbor_uint(&mut body, output_bytes);
    let mut framed = Vec::with_capacity(body.len() + 4);
    framed.extend_from_slice(&(body.len() as u32).to_be_bytes());
    framed.extend_from_slice(&body);
    framed
}

// ---------------------------------------------------------------------------
// Parsed control bodies
// ---------------------------------------------------------------------------

/// Refusal request-tag kind naming an input object stream (0 = control
/// request tag).
pub const TAG_INPUT_STREAM: u64 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    /// 0 = control request tag, 1 = input stream tag.
    pub tag_kind: u64,
    pub tag_id: u64,
    pub code: u64,
    pub detail: String,
}

pub fn parse_refusal(body: &[u8]) -> Result<Refusal> {
    let mut r = Reader::new(body);
    ensure!(r.array_len()? == 3, "refusal is not a 3-array");
    ensure!(r.array_len()? == 2, "refusal request tag is not a 2-array");
    let tag_kind = r.uint()?;
    let tag_id = r.uint()?;
    let code = r.uint()?;
    let detail = r.text()?;
    r.done()?;
    Ok(Refusal {
        tag_kind,
        tag_id,
        code,
        detail,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binding {
    pub request: u64,
    pub authority: String,
    pub owner: String,
    pub generation: u64,
    pub creation_sequence: u64,
    /// The session's retained-record ceilings as the SUBJECT declares them in
    /// the binding receipt: scopes, entities, operations, input bytes, output
    /// bytes, active jobs. These are the numbers an R row quotes as a
    /// subject's declared limits, rather than any documented default.
    pub limits: SessionLimits,
}

/// Session limits echoed in a `Session::Binding` receipt (a 6-array).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionLimits {
    pub scopes: u64,
    pub entities: u64,
    pub operations: u64,
    pub input_bytes: u64,
    pub output_bytes: u64,
    pub active_jobs: u64,
}

impl SessionLimits {
    /// Evidence text for an artifact line.
    pub fn text(&self) -> String {
        format!(
            "scopes={} entities={} operations={} input_bytes={} output_bytes={} active_jobs={}",
            self.scopes,
            self.entities,
            self.operations,
            self.input_bytes,
            self.output_bytes,
            self.active_jobs
        )
    }
}

pub fn parse_binding(body: &[u8]) -> Result<Binding> {
    let mut r = Reader::new(body);
    ensure!(r.array_len()? == 8, "session receipt is not an 8-array");
    ensure!(r.uint()? == 1, "not a Session::Binding receipt");
    let request = r.uint()?;
    let authority = r.text()?;
    let owner = r.text()?;
    let generation = r.uint()?;
    let creation_sequence = r.uint()?;
    r.skip().context("session receipt policy")?;
    ensure!(
        r.array_len()? == 6,
        "session receipt limits is not a 6-array"
    );
    let limits = SessionLimits {
        scopes: r.uint()?,
        entities: r.uint()?,
        operations: r.uint()?,
        input_bytes: r.uint()?,
        output_bytes: r.uint()?,
        active_jobs: r.uint()?,
    };
    Ok(Binding {
        request,
        authority,
        owner,
        generation,
        creation_sequence,
        limits,
    })
}

pub fn parse_declared(body: &[u8]) -> Result<u64> {
    let mut r = Reader::new(body);
    ensure!(r.array_len()? == 3, "declaration receipt is not a 3-array");
    ensure!(r.uint()? == 1, "not a Scope::Declared receipt");
    r.uint()
}

pub fn parse_detached(body: &[u8]) -> Result<u64> {
    let mut r = Reader::new(body);
    ensure!(r.array_len()? == 2, "detach response is not a 2-array");
    ensure!(r.uint()? == 3, "not a Drain::Detached response");
    r.uint()
}

pub fn parse_next_sequence(body: &[u8]) -> Result<u64> {
    let mut r = Reader::new(body);
    ensure!(r.array_len()? == 3, "sequence response is not a 3-array");
    ensure!(r.uint()? == 4, "not a Session::Sequence response");
    r.uint()?; // request
    r.uint()
}

/// ResultMessage::Read: request one output object of a terminal work; the
/// server answers with a result object stream (which the caller may hold
/// unread). FRAME_RESULT (5) carries this body.
pub fn result_read(
    request: u64,
    work: (u64, u64, u64),
    attempt: u64,
    index: u64,
    expected_sha256: &[u8; 32],
) -> Vec<u8> {
    let mut body = Vec::new();
    cbor_array(&mut body, 6);
    cbor_uint(&mut body, 0); // ResultMessage::Read
    cbor_uint(&mut body, request);
    cbor_array(&mut body, 3);
    for part in [work.0, work.1, work.2] {
        cbor_uint(&mut body, part);
    }
    cbor_uint(&mut body, attempt);
    cbor_uint(&mut body, index);
    cbor_bytes(&mut body, expected_sha256);
    body
}

/// Work::Admitted: returns the admitted input stream id from the request tag.
pub fn parse_admitted_stream(body: &[u8]) -> Result<u64> {
    let mut r = Reader::new(body);
    ensure!(r.array_len()? == 3, "admission receipt is not a 3-array");
    ensure!(r.uint()? == 1, "not a Work::Admitted receipt");
    ensure!(r.array_len()? == 2, "admission tag is not a 2-array");
    ensure!(r.uint()? == 1, "admission tag is not an input tag");
    r.uint()
}

/// Work::View response: returns (revision, work state) (records.rs State:
/// 5..=8 are terminal, 5 = SUCCEEDED). Only the leading fields are parsed;
/// the rest of the view is skipped.
pub fn parse_view_state(body: &[u8]) -> Result<(u64, u64)> {
    let mut r = Reader::new(body);
    ensure!(
        r.array_len()? == 4,
        "work view is not a [kind, request, revision, view] array"
    );
    ensure!(r.uint()? == 5, "not a Work::View response");
    r.uint()?; // request
    let revision = r.uint()?;
    let fields = r.array_len()?;
    ensure!(fields >= 2, "work view record lacks work/state");
    ensure!(r.array_len()? == 3, "work key is not a 3-array");
    r.uint()?;
    r.uint()?;
    r.uint()?;
    Ok((revision, r.uint()?))
}

/// Scope::PageResponse: returns (declared, (entity, state) pairs in page order).
pub fn parse_page(body: &[u8]) -> Result<(u64, Vec<(u64, u64)>)> {
    let mut r = Reader::new(body);
    ensure!(r.array_len()? == 10, "scope page is not a 10-array");
    ensure!(r.uint()? == 3, "not a Scope::PageResponse");
    r.uint()?; // request
    r.uint()?; // scope
    r.uint()?; // producer
    if !r.option_null()? {
        r.skip()?; // parent WorkKey
    }
    r.boolean()?; // sealed
    if !r.option_null()? {
        r.skip()?; // seal digest
    }
    let declared = r.uint()?;
    let entries = r.array_len()?;
    let mut entities = Vec::new();
    for _ in 0..entries {
        let fields = r.array_len()?;
        ensure!(fields >= 2, "scope page entry lacks entity/state");
        let entity = r.uint()?;
        let state = r.uint()?;
        entities.push((entity, state));
        for _ in 2..fields {
            r.skip()?;
        }
    }
    Ok((declared, entities))
}

// ---------------------------------------------------------------------------
// QUIC connection wrapper
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Close {
    /// QUIC APPLICATION_CLOSE from the peer: (error code, reason bytes).
    Application(u64, Vec<u8>),
    /// Transport loss or stateless reset; no close frame was received.
    Transport(String),
}

#[derive(Debug, PartialEq, Eq)]
pub enum Frame {
    /// A control frame: (frame type, body bytes).
    Control(u8, Vec<u8>),
    /// Ordered, clean end of the control receive direction.
    Fin,
}

/// Fixture-side QUIC idle timeout used when a probe arms keep-alive. Stated
/// explicitly rather than inherited from the quinn default so a row can say
/// what its own transport would have done.
pub const KEEP_ALIVE_MAX_IDLE: Duration = Duration::from_secs(60);

pub struct Peer {
    runtime: Arc<tokio::runtime::Runtime>,
    /// QUIC PING cadence for connections this peer opens. `None` = quinn's
    /// default (no keep-alive), which is what every row wants unless it
    /// deliberately holds a connection idle for longer than the transport
    /// idle timeout.
    keep_alive: Option<Duration>,
}

impl Peer {
    pub fn new() -> Result<Self> {
        Self::build(None)
    }

    /// A peer whose connections send QUIC PINGs every `interval`, with an
    /// explicit [`KEEP_ALIVE_MAX_IDLE`] idle timeout.
    ///
    /// Rows that observe a subject enforcing a deadline on an otherwise
    /// silent connection MUST use this: without it the fixture's own
    /// transport idle timeout can tear the connection down first, and a
    /// stream that dies with the connection is indistinguishable from a
    /// stream the subject enforced. Keep-alive is transport-level PING
    /// traffic and carries no application data, so it is not activity on
    /// any object stream and must not renew an application deadline —
    /// a subject that treats it as activity is itself a finding.
    pub fn with_keep_alive(interval: Duration) -> Result<Self> {
        Self::build(Some(interval))
    }

    /// The peer's runtime is MULTI-THREADED on purpose, and this is a
    /// measurement property, not a performance choice.
    ///
    /// quinn spawns its endpoint driver — the task that reads inbound
    /// packets and applies the frames in them — onto whatever runtime the
    /// endpoint is created in. On a current-thread runtime that task only
    /// progresses while some `block_on` is pending on this thread, so a row
    /// that sleeps between probes, or whose probe returns immediately out of
    /// local send credit, leaves inbound packets unprocessed for as long as
    /// it is not inside `block_on`. The subject's STOP_SENDING or refusal is
    /// then observed at the NEXT call that happens to wait, and the row
    /// reports the time of its own probe rather than the time of the
    /// subject's action.
    ///
    /// That is exactly how `r-stalled-principal-progress` came to bracket
    /// the Java input receive deadline between 40 s and 130 s when the
    /// subject was in fact refusing at 30.1 s: the STOP_SENDING frames had
    /// been arriving and being retransmitted for a minute and were all
    /// applied within one millisecond of each other the moment the client
    /// next blocked. Worker threads keep the driver running while the
    /// scenario thread sleeps, so an observation timestamp is the subject's
    /// timing and not the fixture's scheduling.
    fn build(keep_alive: Option<Duration>) -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        Ok(Self {
            runtime: Arc::new(runtime),
            keep_alive,
        })
    }

    pub fn connect(&self, material: &Material, principal: &str, address: &str) -> Result<RawConn> {
        let endpoint =
            self.runtime
                .block_on(client_endpoint(material, principal, self.keep_alive))?;
        let address = address
            .parse::<std::net::SocketAddr>()
            .context("invalid server address")?;
        let connection = self.runtime.block_on(async {
            let connecting = endpoint
                .connect(address, "localhost")
                .context("start v2 connection")?;
            tokio::time::timeout(PROBE_TIMEOUT, connecting)
                .await
                .context("v2 connect timeout")?
                .context("v2 connect failed")
        })?;
        let (control_send, control_recv) = self.runtime.block_on(async {
            tokio::time::timeout(PROBE_TIMEOUT, connection.open_bi())
                .await
                .context("control stream open timeout")?
                .context("open control stream")
        })?;
        Ok(RawConn {
            runtime: self.runtime.clone(),
            endpoint,
            connection,
            control_send: Some(control_send),
            control_recv,
            caps: None,
        })
    }
}

pub struct RawConn {
    runtime: Arc<tokio::runtime::Runtime>,
    /// Held for the connection's lifetime; dropping it would close the socket.
    #[allow(dead_code)]
    endpoint: quinn::Endpoint,
    connection: quinn::Connection,
    control_send: Option<quinn::SendStream>,
    control_recv: quinn::RecvStream,
    /// Negotiated capability selection, set by the negotiation handshake.
    caps: Option<SelectionCaps>,
}

/// Negotiated capability selection fields the R rows assert against.
#[derive(Debug, Clone, Copy)]
pub struct SelectionCaps {
    /// Maximum control body bytes the subject will accept.
    pub control_limit: u64,
    /// Concurrent object streams per direction per connection.
    pub stream_limit: u64,
    /// Maximum pending control responses on one connection.
    pub pending_limit: u64,
    /// Maximum input/result object payload bytes.
    pub object_limit: u64,
    pub idle_ms: u64,
    pub lifetime_ms: u64,
}

/// Capabilities selection body: array(9) [response, supported, required,
/// control_limit, stream_limit, pending_limit, object_limit, stream_idle_ms,
/// stream_lifetime_ms]. The profile arrays are validated as uint arrays and
/// skipped.
pub fn parse_capabilities(body: &[u8]) -> Result<SelectionCaps> {
    let mut r = Reader::new(body);
    ensure!(
        r.array_len()? == 9,
        "capabilities selection is not a 9-array"
    );
    ensure!(r.uint()? == 1, "capability selection is not a response");
    for _ in 0..r.array_len()? {
        r.uint().context("supported profile id")?;
    }
    for _ in 0..r.array_len()? {
        r.uint().context("required profile id")?;
    }
    let control_limit = r.uint()?;
    let stream_limit = r.uint()?;
    let pending_limit = r.uint()?;
    let object_limit = r.uint()?;
    Ok(SelectionCaps {
        control_limit,
        stream_limit,
        pending_limit,
        object_limit,
        idle_ms: r.uint()?,
        lifetime_ms: r.uint()?,
    })
}

fn block_on<T>(
    runtime: &tokio::runtime::Runtime,
    future: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    runtime
        .block_on(async { tokio::time::timeout(PROBE_TIMEOUT, future).await })
        .context("raw probe timed out")?
}

async fn recv_frame(control_recv: &mut quinn::RecvStream) -> Result<Frame> {
    let mut prefix = [0u8; 5];
    let mut read = 0;
    while read < prefix.len() {
        match control_recv
            .read(&mut prefix[read..])
            .await
            .context("read control frame prefix")?
        {
            Some(0) => continue,
            Some(n) => read += n,
            None => {
                ensure!(read == 0, "truncated control frame prefix at FIN");
                return Ok(Frame::Fin);
            }
        }
    }
    let frame_type = prefix[0];
    let length = u32::from_be_bytes(prefix[1..].try_into()?) as usize;
    ensure!(length <= 1 << 20, "control frame body unbounded");
    let mut body = vec![0u8; length];
    control_recv
        .read_exact(&mut body)
        .await
        .context("read control frame body")?;
    Ok(Frame::Control(frame_type, body))
}

impl RawConn {
    /// Negotiated capability selection recorded during the handshake.
    pub fn caps(&self) -> Option<&SelectionCaps> {
        self.caps.as_ref()
    }

    pub(crate) fn set_caps(&mut self, caps: SelectionCaps) {
        self.caps = Some(caps);
    }

    pub fn send_control(&mut self, frame_type: u8, body: &[u8]) -> Result<()> {
        let runtime = self.runtime.clone();
        let frame = control_frame(frame_type, body);
        let send = self
            .control_send
            .as_mut()
            .context("control send direction already finished")?;
        block_on(&runtime, async {
            send.write_all(&frame)
                .await
                .context("write control frame")?;
            Ok(())
        })
    }

    /// Write already-framed bytes (frozen corpus rows carry their own header).
    pub fn send_frozen(&mut self, frame: &[u8]) -> Result<()> {
        let runtime = self.runtime.clone();
        let send = self
            .control_send
            .as_mut()
            .context("control send direction already finished")?;
        block_on(&runtime, async {
            send.write_all(frame).await.context("write frozen frame")?;
            Ok(())
        })
    }

    /// One control frame or an ordered FIN.
    pub fn read_control(&mut self) -> Result<Frame> {
        let runtime = self.runtime.clone();
        block_on(&runtime, recv_frame(&mut self.control_recv))
    }

    /// Bounded variant of [`Self::read_control`]: `Ok(None)` on timeout. A
    /// peer reset of the control stream surfaces as an error.
    pub fn read_control_bounded(&mut self, timeout: Duration) -> Result<Option<Frame>> {
        match self.runtime.block_on(async {
            tokio::time::timeout(timeout, recv_frame(&mut self.control_recv)).await
        }) {
            Ok(frame) => Ok(Some(frame?)),
            Err(_timeout) => Ok(None),
        }
    }

    /// Expect a specific frame type, returning its parsed-position body.
    pub fn expect_control(&mut self, frame_type: u8) -> Result<Vec<u8>> {
        match self.read_control()? {
            Frame::Control(found, body) => {
                ensure!(
                    found == frame_type,
                    "expected control frame {frame_type}, got {found} (body {})",
                    hex(&body)
                );
                Ok(body)
            }
            Frame::Fin => bail!("expected control frame {frame_type}, got FIN"),
        }
    }

    pub fn finish_control(&mut self) -> Result<()> {
        let runtime = self.runtime.clone();
        let mut send = self
            .control_send
            .take()
            .context("control already finished")?;
        block_on(&runtime, async {
            send.finish().context("finish control send direction")?;
            Ok(())
        })
    }

    pub fn reset_control(&mut self, code: u64) -> Result<()> {
        let runtime = self.runtime.clone();
        let mut send = self.control_send.take().context("control already closed")?;
        block_on(&runtime, async {
            send.reset(code.try_into().context("reset code out of range")?)
                .context("reset control stream")?;
            Ok(())
        })
    }

    /// STOP_SENDING on the control response direction.
    pub fn stop_control_responses(&mut self, code: u64) -> Result<()> {
        let runtime = self.runtime.clone();
        block_on(&runtime, async {
            self.control_recv
                .stop(code.try_into().context("stop code out of range")?)
                .context("stop control response direction")?;
            Ok(())
        })
    }

    pub fn open_uni(&self) -> Result<quinn::SendStream> {
        let runtime = self.runtime.clone();
        block_on(&runtime, async {
            self.connection
                .open_uni()
                .await
                .context("open unidirectional stream")
        })
    }

    /// Open with a bounded wait: `Ok(None)` means the peer's transport stream
    /// budget blocked the open past the ceiling deadline (transport-level
    /// enforcement; normative-clarifications item 4).
    pub fn open_uni_ceiling(&self) -> Result<Option<quinn::SendStream>> {
        match self.runtime.block_on(async {
            tokio::time::timeout(OPEN_BUDGET_TIMEOUT, self.connection.open_uni()).await
        }) {
            Ok(Ok(stream)) => Ok(Some(stream)),
            Ok(Err(error)) => bail!("open uni failed: {error}"),
            Err(_timeout) => Ok(None),
        }
    }

    pub fn open_bi_ceiling(&self) -> Result<Option<(quinn::SendStream, quinn::RecvStream)>> {
        match self.runtime.block_on(async {
            tokio::time::timeout(OPEN_BUDGET_TIMEOUT, self.connection.open_bi()).await
        }) {
            Ok(Ok(streams)) => Ok(Some(streams)),
            Ok(Err(error)) => bail!("open bi failed: {error}"),
            Err(_timeout) => Ok(None),
        }
    }

    pub fn write_stream(&self, stream: &mut quinn::SendStream, bytes: &[u8]) -> Result<()> {
        let runtime = self.runtime.clone();
        block_on(&runtime, async {
            stream.write_all(bytes).await.context("write stream")?;
            Ok(())
        })
    }

    pub fn finish_stream(&self, stream: &mut quinn::SendStream) -> Result<()> {
        let runtime = self.runtime.clone();
        block_on(&runtime, async {
            stream.finish().context("finish stream")?;
            Ok(())
        })
    }

    pub fn reset_stream(&self, stream: &mut quinn::SendStream, code: u64) -> Result<()> {
        let runtime = self.runtime.clone();
        block_on(&runtime, async {
            stream
                .reset(code.try_into().context("reset code out of range")?)
                .context("reset stream")?;
            Ok(())
        })
    }

    /// Wait for the peer to close the connection, classifying the close.
    pub fn wait_closed(&self, timeout: Duration) -> Result<Close> {
        match self.try_wait_closed(timeout)? {
            Some(close) => Ok(close),
            None => bail!("connection did not close within {timeout:?}"),
        }
    }

    /// Like [`Self::wait_closed`] but `Ok(None)` marks the timeout: the peer
    /// kept the connection open (the server MAY leave connection close to the
    /// client after a control FIN, Section 12.8).
    pub fn try_wait_closed(&self, timeout: Duration) -> Result<Option<Close>> {
        match self
            .runtime
            .block_on(async { tokio::time::timeout(timeout, self.connection.closed()).await })
        {
            Ok(quinn::ConnectionError::ApplicationClosed(close)) => Ok(Some(Close::Application(
                close.error_code.into_inner(),
                close.reason.to_vec(),
            ))),
            Ok(other) => Ok(Some(Close::Transport(format!("{other}")))),
            Err(_timeout) => Ok(None),
        }
    }

    /// Client-initiated graceful close (application code 0) for directions the
    /// server left open.
    pub fn close_application(&self, reason: &[u8]) -> Result<()> {
        self.connection.close(0u32.into(), reason);
        // Drive the runtime so the close frame actually leaves the socket: a
        // current-thread runtime only runs while block_on is active, and an
        // undriven close strands the frame, leaving the peer connection open.
        self.runtime
            .block_on(async { tokio::time::sleep(Duration::from_millis(200)).await });
        Ok(())
    }

    /// Graceful close that also waits (bounded) for the local endpoint to
    /// have no connection left in a closing/draining state.
    ///
    /// A QUIC connection lingers for several PTOs after CONNECTION_CLOSE. A
    /// row that closes its peer and immediately signals the subject to shut
    /// down can catch the subject with that draining connection still on its
    /// books, which is a fixture race, not a subject defect. Rows that hold
    /// a connection open to the end of their window use this instead of
    /// `close_application` + drop.
    pub fn close_and_wait_idle(self, reason: &[u8], timeout: Duration) -> Result<()> {
        self.connection.close(0u32.into(), reason);
        let runtime = self.runtime.clone();
        let endpoint = self.endpoint.clone();
        runtime.block_on(async {
            let _ = tokio::time::timeout(timeout, endpoint.wait_idle()).await;
        });
        Ok(())
    }

    /// Transport statistics straight from the SOURCE-PINNED QUIC transport
    /// (quinn 0.11.11 / quinn-proto 0.11.17, both pinned in this workspace's
    /// Cargo.lock) for THIS connection: UDP datagram byte totals in each
    /// direction, per-frame-type receive and transmit counts, and path
    /// loss/retransmission counters.
    ///
    /// This is the transport's own accounting of the frames it decoded out
    /// of received packets and encoded into sent ones, not a fixture wrapper
    /// counting application calls. `udp_tx`/`udp_rx` are wire bytes for this
    /// connection alone, which is what makes them FIXTURE-SCOPED where an
    /// interface counter is host-scoped. What they are NOT: a byte-for-byte
    /// packet capture (no capability for one on this host), and not the
    /// SUBJECT's accounting — they are one endpoint's view.
    pub fn stats(&self) -> quinn::ConnectionStats {
        self.connection.stats()
    }

    /// NON-WRITING probe of one send stream's state.
    ///
    /// A probe that writes bytes to test whether a stream is still alive is
    /// not a passive observation: on an input stream those bytes are payload
    /// progress, and a subject whose receive deadline is measured from the
    /// last payload byte has its deadline legitimately renewed by the probe
    /// itself. This polls quinn's own `stopped()` future with a bounded
    /// wait instead, so the stream carries nothing the subject can read as
    /// activity.
    pub fn poll_stream_stopped(
        &self,
        stream: &quinn::SendStream,
        timeout: Duration,
    ) -> Result<StreamState> {
        let stopped = stream.stopped();
        self.runtime.block_on(async move {
            match tokio::time::timeout(timeout, stopped).await {
                Ok(Ok(Some(code))) => Ok(StreamState::Stopped(code.into_inner())),
                Ok(Ok(None)) => Ok(StreamState::Acknowledged),
                Ok(Err(error)) => Ok(StreamState::Lost(format!("{error}"))),
                Err(_elapsed) => Ok(StreamState::Open),
            }
        })
    }

    /// Abrupt loss: drop every handle without a close frame.
    pub fn vanish(self) {}
}

/// What a non-writing probe saw on one send stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamState {
    /// Nothing observed within the bounded wait: still open as far as this
    /// endpoint can tell.
    Open,
    /// The peer sent STOP_SENDING with this application error code.
    Stopped(u64),
    /// The peer acknowledged every byte of a finished stream. A stalled
    /// input is never finished, so this is recorded rather than expected.
    Acknowledged,
    /// The stream or its connection is gone; the reason is recorded, and a
    /// transport loss is never counted as subject enforcement.
    Lost(String),
}

async fn client_endpoint(
    material: &Material,
    principal: &str,
    keep_alive: Option<Duration>,
) -> Result<quinn::Endpoint> {
    let identity = material.principal(principal)?;
    let mut roots = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(&material.ca_cert)? {
        roots.add(cert?)?;
    }
    let chain: Vec<CertificateDer> =
        CertificateDer::pem_file_iter(&identity.cert)?.collect::<Result<_, _>>()?;
    let key = PrivateKeyDer::from_pem_file(&identity.key)?;
    let mut tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(chain, key)
        .context("build client TLS identity")?;
    tls.alpn_protocols = vec![ALPN_V2.to_vec()];
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse()?)?;
    let mut config = quinn::ClientConfig::new(Arc::new(
        QuicClientConfig::try_from(tls).context("QUIC client config")?,
    ));
    if let Some(interval) = keep_alive {
        let mut transport = quinn::TransportConfig::default();
        transport.keep_alive_interval(Some(interval));
        transport.max_idle_timeout(Some(
            quinn::IdleTimeout::try_from(KEEP_ALIVE_MAX_IDLE)
                .context("fixture idle timeout out of range")?,
        ));
        config.transport_config(Arc::new(transport));
    }
    endpoint.set_default_client_config(config);
    Ok(endpoint)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cbor_uint_is_minimal() {
        let cases: &[(u64, &[u8])] = &[
            (0, &[0x00]),
            (23, &[0x17]),
            (24, &[0x18, 0x18]),
            (255, &[0x18, 0xff]),
            (256, &[0x19, 0x01, 0x00]),
            (65_535, &[0x19, 0xff, 0xff]),
            (65_536, &[0x1a, 0x00, 0x01, 0x00, 0x00]),
            (u32::MAX as u64, &[0x1a, 0xff, 0xff, 0xff, 0xff]),
            (
                u32::MAX as u64 + 1,
                &[0x1b, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00],
            ),
        ];
        for (value, expected) in cases {
            let mut out = Vec::new();
            cbor_uint(&mut out, *value);
            assert_eq!(&out, expected, "uint {value}");
            let mut r = Reader::new(&out);
            assert_eq!(r.uint().unwrap(), *value);
            r.done().unwrap();
        }
    }

    #[test]
    fn hand_frames_match_frozen_valid_vectors() {
        // Session::Create(1, 1) is the frozen session-open vector's body.
        let frozen_open = frozen("session-open").unwrap();
        assert_eq!(
            frozen_open.frame[5..],
            session_create(1, 1)[..],
            "session create body differs from frozen session-open"
        );
        // Scope::Page(3, scope 0, after 0, limit 256).
        let frozen_page = frozen("scope-page-request").unwrap();
        assert_eq!(frozen_page.frame[5..], scope_page(3, 0)[..]);
        // Drain::Detach(request 14).
        let frozen_detach = frozen("detach-request").unwrap();
        assert_eq!(frozen_detach.frame[5..], drain_detach(14)[..]);
        // Work::Watch(7, 0:0:1, after revision 1, wait 30000).
        let frozen_watch = frozen("work-watch").unwrap();
        assert_eq!(
            frozen_watch.frame[5..],
            work_watch(7, (0, 0, 1), 1, 30_000)[..]
        );
    }

    #[test]
    fn input_header_matches_frozen_vector() {
        let frozen_header = frozen("input-header").unwrap();
        // The frozen header admits work 0:0:1 with operation 0x01,
        // input "abc" (sha256 known), application ascii-uppercase-v1.
        let mut operation = [0u8; 16];
        operation[15] = 1;
        let mut sha = [0u8; 32];
        sha[..].copy_from_slice(&Sha256::digest(b"abc"));
        let built = input_header_framed(
            1,
            &operation,
            (0, 0, 1),
            3,
            &sha,
            "text/plain",
            "ascii-uppercase-v1",
            0,
            60_000,
            1,
            3,
        );
        assert_eq!(
            frozen_header.frame, built,
            "hand-framed input header differs from the frozen vector"
        );
    }

    #[test]
    fn frozen_refuse_subset_loads_with_named_errors() {
        for name in [
            "caps-extra-position",
            "caps-missing-lifetime",
            "caps-control-too-small",
            "caps-duplicate-profile",
            "caps-required-not-supported",
            "result-profile-without-durable",
            "caps-idle-exceeds-lifetime",
            "zero-request",
            "generation-overflow",
            "session-policy-zero",
            "session-nonascii-owner",
            "zero-operation-id",
            "unsorted-declaration",
            "empty-unsealed-batch",
            "invalid-input-mode",
            "short-input-digest",
            "extra-work-key-position",
            "unknown-work-state",
            "inconsistent-success-without-input",
            "result-noncontiguous-index",
            "result-locator-userinfo",
            "summary-count-mismatch",
            "noncanonical-session-request",
            "trailing-cbor-item",
        ] {
            let row = frozen(name).unwrap_or_else(|e| panic!("{name}: {e:#}"));
            assert_eq!(row.expectation, "refuse", "{name} must be a refuse row");
            assert!(
                matches!(row.error.as_str(), "FRAME_ERROR" | "EXTENSION_UNSUPPORTED"),
                "{name} has no named refusal"
            );
        }
    }

    #[test]
    fn reader_skips_nested_items() {
        let mut out = Vec::new();
        cbor_array(&mut out, 3);
        cbor_uint(&mut out, 7);
        cbor_array(&mut out, 2);
        cbor_text(&mut out, "inner");
        cbor_bytes(&mut out, &[1, 2, 3]);
        cbor_bool(&mut out, true);
        let mut r = Reader::new(&out);
        assert_eq!(r.array_len().unwrap(), 3);
        assert_eq!(r.uint().unwrap(), 7);
        r.skip().unwrap();
        assert!(r.boolean().unwrap());
        r.done().unwrap();
    }

    #[test]
    fn cbor_text_round_trips() {
        for text in ["", "a", "copy/v2", "München", &"x".repeat(300)] {
            let mut out = Vec::new();
            cbor_text(&mut out, text);
            let mut r = Reader::new(&out);
            assert_eq!(r.text().unwrap(), text);
            r.done().unwrap();
        }
    }
}
