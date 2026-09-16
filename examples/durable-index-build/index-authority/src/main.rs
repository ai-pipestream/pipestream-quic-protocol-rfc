//! External durable-index worker authority.
//!
//! A real PipeStream authority executable built only on public library APIs.
//! It registers three application contracts for the index-build example:
//! `index-file/v1` (mode 2: authority-side expansion of one file into
//! `tf/v1` chunk children, then publish of the child reference list),
//! `tf/v1` (mode 0: deterministic term-frequency record over one chunk),
//! `index-merge/v1` (mode 0: merge TF records fetched by authenticated
//! result reference into one inverted index).
//!
//! The merge contract acts as a Section-12 consumer: it opens an
//! owner-authenticated client session to the peer authority using
//! separately configured reader credentials (flags below, never a URI or
//! unit-input bytes) and performs RESULT select+read per reference,
//! verifying digests. Reader use is recorded in SPEC-FRICTION.md (F1).
use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use index_core::{
    CHUNKS_PER_FILE, FILE_LABEL, MERGE_LABEL, TF_LABEL, format_ref, merge_index, parse_refs,
    tf_record,
};
use pipestream_quic::{
    persistence::PhysicalLimits,
    v2::*,
    v2_authority::{
        Authority,
        server::{Options, Server},
    },
    v2_client::{
        journal::{self, Journal},
        session::{self, Client},
        transport,
    },
    v2_tls::{ClientAuthentication, ServerSecurity},
};
use pipestream_quic::v2::authority::{
    AuthorityStore, Authorization, Clock, ClockReading, Permission, StoreError, StorePolicy,
    execution::{Application, ApplicationOutcome, Expansion, ExpansionContext, ExpansionOutcome, WorkContext},
    ingress::{Applications, InputPreparation, InputReception, RestartSafety},
    payload::{PayloadPolicy, PayloadStore},
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use sha2::{Digest as _, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::OpenOptions,
    io::Write,
    net::SocketAddr,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

const OBJECT_LIMIT: u64 = 16 * 1024 * 1024;
const READER_EXECUTION_MS: u64 = 60_000;

fn hex32(text: &str) -> Result<[u8; 32]> {
    if text.len() != 64 {
        bail!("digest must be 64 hex digits");
    }
    let mut out = [0u8; 32];
    for (i, chunk) in text.as_bytes().chunks(2).enumerate() {
        out[i] = u8::from_str_radix(std::str::from_utf8(chunk).context("hex")?, 16)
            .context("hex digits")?;
    }
    Ok(out)
}

/// Split merge input into its header line and reference body. Parents
/// publish bare ref lines; the coordinator prepends the merge header.
fn split_merge_input(input: &[u8]) -> Result<(String, String)> {
    let text = std::str::from_utf8(input).context("merge input is UTF-8")?;
    let mut lines = text.lines();
    let header = lines.next().context("merge input needs a header")?;
    Ok((header.to_string(), lines.collect::<Vec<_>>().join("\n")))
}

/// `index-file/v2` expansion: split the admitted file into exactly
/// CHUNKS_PER_FILE chunks, declare the sealed child scope, admit one
/// `tf/v1` leaf child per chunk with streamed input bytes.
struct FileExpander;

impl Expansion for FileExpander {
    fn expand(
        &self,
        context: &mut ExpansionContext<'_>,
    ) -> std::result::Result<ExpansionOutcome, StoreError> {
        let total = context.input_descriptor().length.0 as usize;
        let per = total / CHUNKS_PER_FILE;
        context.declare(
            context.operation(Id(1))?,
            &(1..=(CHUNKS_PER_FILE as u64)).map(Id).collect::<Vec<_>>(),
            true,
        )?;
        let mut staging = vec![0u8; per.max(1)];
        let mut consumed = 0usize;
        for entity in 1..=(CHUNKS_PER_FILE as u64) {
            context.renew()?;
            let end = if entity == CHUNKS_PER_FILE as u64 {
                total
            } else {
                (entity as usize) * per
            };
            let expected = end - consumed;
            let mut chunk = vec![0u8; expected];
            let mut filled = 0;
            while filled < expected {
                let limit = context.buffer_limit().min(staging.len());
                let n = context.read_input(&mut staging[..limit.min(expected - filled)])?;
                if n == 0 {
                    return Ok(ExpansionOutcome::Failed(diag(
                        ErrorCode::IntegrityError,
                        "file input ended before its commitment",
                    )));
                }
                chunk[filled..filled + n].copy_from_slice(&staging[..n]);
                filled += n;
            }
            consumed = end;
            let parameters = AdmitParameters {
                work: WorkKey {
                    scope: Number(context.child_scope().0),
                    producer: Producer(1),
                    entity: Id(entity),
                },
                input: Input {
                    length: Number(expected as u64),
                    sha256: Digest(Sha256::digest(&chunk).into()),
                    content_type: context.input_descriptor().content_type.clone(),
                },
                application: ApplicationLabel(TF_LABEL.into()),
                mode: Mode(0),
                execution_ms: context.execution_duration(),
                outputs: OutputBudget {
                    count: BatchCount(1),
                    total_bytes: Number(expected as u64 * 2 + 64),
                },
            };
            match context.receive_input(
                context.operation(Id(entity + 1))?,
                parameters,
                Instant::now(),
            )? {
                InputReception::Replay(_) => {}
                InputReception::Receiving(mut input) => {
                    for piece in chunk.chunks(context.buffer_limit()) {
                        input.receive(piece, Instant::now())?;
                    }
                    match context.prepare_input(input.finish(Instant::now())?)? {
                        InputPreparation::Replay(_) => {}
                        InputPreparation::Ready(input) => {
                            context.admit_input(*input)?;
                        }
                    }
                }
            }
        }
        if context.read_input(&mut staging[..1])? != 0 {
            return Ok(ExpansionOutcome::Failed(diag(
                ErrorCode::IntegrityError,
                "file input exceeds its commitment",
            )));
        }
        Ok(ExpansionOutcome::Complete)
    }
}

fn diag(code: ErrorCode, detail: impl Into<String>) -> Diagnostic {
    let short: String = detail.into().chars().take(400).collect();
    Diagnostic {
        code: DiagnosticCode(code as u64),
        detail: Detail(short),
    }
}

/// `index-file/v1` execute: read every child TF output (verified EOF) and
/// publish the reference list. The TF bytes themselves stay on this
/// authority; only digests travel.
struct FileContract {
    expander: FileExpander,
}

impl Application for FileContract {
    fn expansion(&self) -> Option<&dyn Expansion> {
        Some(&self.expander)
    }

    fn execute(
        &self,
        context: &mut WorkContext,
    ) -> std::result::Result<ApplicationOutcome, StoreError> {
        // Coordinator admits file f as entity f+1: the doc id rides along.
        let doc = context.work().entity.0 - 1;
        let mut refs = Vec::new();
        let mut after = Number(0);
        loop {
            let page = context.children(after, PageLimit(256))?;
            if page.members.is_empty() {
                break;
            }
            for member in &page.members {
                let output = context.begin_child_output(member.entity, OutputIndex(0))?;
                let mut record = Vec::new();
                let mut buf = [0u8; 8192];
                loop {
                    let n = context.read_child_output(&mut buf)?;
                    if n == 0 {
                        break;
                    }
                    record.extend_from_slice(&buf[..n]);
                    context.renew()?;
                }
                context.finish_child_output()?;
                let digest: [u8; 32] = Sha256::digest(&record).into();
                if digest != output.sha256.0 {
                    return Ok(ApplicationOutcome::Failed(diag(
                        ErrorCode::IntegrityError,
                        "child TF record digest differs from manifest",
                    )));
                }
                refs.push(format_ref(
                    doc,
                    member.scope.0,
                    member.producer.0,
                    member.entity.0,
                    &digest,
                ));
                after = Number(member.entity.0);
                context.renew()?;
            }
            if !page.more {
                break;
            }
        }
        let mut list = refs.join("\n");
        list.push('\n');
        context.begin_output(Number(list.len() as u64 + 256), ApplicationLabel("text/refs".into()))?;
        context.write_output(list.as_bytes())?;
        context.finish_output()?;
        Ok(ApplicationOutcome::Succeeded)
    }
}

/// Reader configuration for the merge contract: separately configured
/// owner credentials + peer endpoint (flags, never URIs or input bytes).
#[derive(Clone)]
struct Reader {
    endpoint: SocketAddr,
    server_name: String,
    ca: PathBuf,
    cert: PathBuf,
    key: PathBuf,
    owner: String,
    journal_dir: PathBuf,
}

fn reader_tls(reader: &Reader) -> Result<session::Endpoint> {
    let roots = {
        let bytes = std::fs::read(&reader.ca)?;
        let certs: Vec<_> = rustls::pki_types::CertificateDer::pem_slice_iter(&bytes)
            .collect::<std::result::Result<_, _>>()?;
        let mut roots = rustls::RootCertStore::empty();
        for c in certs {
            roots.add(c)?;
        }
        roots
    };
    let certs: Vec<_> =
        rustls::pki_types::CertificateDer::pem_slice_iter(&std::fs::read(&reader.cert)?)
            .collect::<std::result::Result<_, _>>()?;
    let key = rustls::pki_types::PrivateKeyDer::from_pem_slice(&std::fs::read(&reader.key)?)?;
    let mut options = transport::Options::default();
    options.offer.object_limit = Number(OBJECT_LIMIT);
    Ok(session::Endpoint {
        local: if reader.endpoint.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        }
        .parse()?,
        remote: reader.endpoint,
        server_name: reader.server_name.clone(),
        security: transport::Security::new(roots, Some((certs, key)))?,
        transport: options,
    })
}

/// Read one TF record through an owner session on the peer authority:
/// attach (same creation identity as the producing session), watch to
/// terminal, select output 0, transfer, verify the digest. Read-only: the
/// fresh journal never declares or admits.
/// One reader session for a whole merge: connect once, read every TF
/// reference over the same client, detach once. A connect per reference
/// churns server connections (default ceiling is 16) for no benefit.
async fn connect_reader(
    reader: &Reader,
    authority: &str,
    creation_sequence: u64,
    scratch: &Path,
) -> Result<Client> {
    if scratch.exists() {
        std::fs::remove_dir_all(scratch)?;
    }
    std::fs::create_dir_all(scratch)?;
    let creation = journal::Creation {
        authority: IdentityLabel(authority.into()),
        owner: IdentityLabel(reader.owner.clone()),
        creation_sequence: Id(creation_sequence),
        policy: Policy {
            execution_limit_ms: Duration(READER_EXECUTION_MS),
            output_retention_ms: Duration(3_600_000),
            receipt_retention_ms: Duration(86_400_000),
        },
        results: true,
    };
    creation.request(Id(1))?;
    let journal = Journal::initialize(
        scratch.join("reader.sqlite"),
        creation,
        journal::JournalLimits::default(),
        PhysicalLimits::default(),
        journal::Options::default(),
    )
    .await?;
    Client::connect(reader_tls(reader)?, journal, session::Options::default())
        .await
        .context("reader connect")
}

async fn read_reference(
    client: &Client,
    work: WorkKey,
    expect_digest: [u8; 32],
    scratch: &Path,
) -> Result<Vec<u8>> {
    if scratch.exists() {
        std::fs::remove_dir_all(scratch)?;
    }
    std::fs::create_dir_all(scratch)?;
    let tag = format!(
        "{}:{}:{} attempt?",
        work.scope.0, work.producer.0, work.entity.0
    );
    let mut after = Number(0);
    let attempt = loop {
        let observed = client
            .watch(work.clone(), after, WaitMs(10_000))
            .await
            .with_context(|| format!("reader watch for {tag}"))?;
        after = Number(observed.revision.0);
        let state = observed.view.state.0;
        if (5..=8).contains(&state) {
            if state != 5 {
                bail!("TF child terminal state {state}, need SUCCEEDED(5)");
            }
            break Id(observed.view.attempt.0);
        }
    };
    let tag = format!(
        "{}:{}:{} attempt {}",
        work.scope.0,
        work.producer.0,
        work.entity.0,
        attempt.0
    );
    client
        .select_output(work.clone(), attempt, OutputIndex(0))
        .await
        .with_context(|| format!("reader select for {tag}"))?;
    let path = scratch.join("tf.bin");
    client
        .read_output(work, attempt, OutputIndex(0))
        .await
        .map_err(anyhow::Error::from)
        .with_context(|| format!("reader read for {tag}"))?
        .save_to(path.clone(), OBJECT_LIMIT)
        .await
        .with_context(|| format!("reader save for {tag}"))?;
    let bytes = std::fs::read(&path)?;
    let digest: [u8; 32] = Sha256::digest(&bytes).into();
    if digest != expect_digest {
        bail!("TF record digest differs from the parent-published reference");
    }
    Ok(bytes)
}

/// `index-merge/v1` execute: parse the reference list, fetch every TF
/// record as a Section-12 consumer, merge, publish the index.
struct MergeContract {
    reader: Option<Reader>,
}

impl Application for MergeContract {
    fn execute(
        &self,
        context: &mut WorkContext,
    ) -> std::result::Result<ApplicationOutcome, StoreError> {
        let Some(reader) = &self.reader else {
            return Ok(ApplicationOutcome::Failed(diag(
                ErrorCode::InternalError,
                "merge reader not configured on this authority".to_string(),
            )));
        };
        let mut input = Vec::new();
        let mut buf = [0u8; 8192];
        let limit = buf.len().min(context.buffer_limit());
        loop {
            let n = context.read_input(&mut buf[..limit])?;
            if n == 0 {
                break;
            }
            input.extend_from_slice(&buf[..n]);
            context.renew()?;
        }
        let (header, body) = match split_merge_input(&input) {
            Ok(v) => v,
            Err(e) => {
                return Ok(ApplicationOutcome::Failed(diag(
                    ErrorCode::InternalError,
                    format!("merge input unreadable: {e:#}"),
                )))
            }
        };
        let refs = match parse_refs(&body) {
            Ok(v) => v,
            Err(e) => {
                return Ok(ApplicationOutcome::Failed(diag(
                    ErrorCode::InternalError,
                    format!("merge refs unreadable: {e}"),
                )))
            }
        };
        let (authority, creation_sequence) = match parse_header(&header) {
            Ok(v) => v,
            Err(e) => {
                return Ok(ApplicationOutcome::Failed(diag(
                    ErrorCode::InternalError,
                    format!("merge header unreadable: {e:#}"),
                )))
            }
        };
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                return Ok(ApplicationOutcome::Failed(diag(
                    ErrorCode::InternalError,
                    format!("reader runtime failed: {e}"),
                )))
            }
        };
        let scratch = reader.journal_dir.join("reader-scratch");
        let merged = match runtime.block_on(async {
            let client =
                connect_reader(reader, &authority, creation_sequence, &scratch.join("session"))
                    .await?;
            let mut docs: BTreeMap<u64, Vec<Vec<u8>>> = BTreeMap::new();
            let mut read_error: Option<anyhow::Error> = None;
            for (doc, scope, producer, entity, digest) in refs.iter() {
                let work = WorkKey {
                    scope: Number(*scope),
                    producer: Producer(*producer),
                    entity: Id(*entity),
                };
                match read_reference(
                    &client,
                    work,
                    *digest,
                    &scratch.join(format!("ref-{scope}-{producer}-{entity}")),
                )
                .await
                {
                    Ok(bytes) => {
                        docs.entry(*doc).or_default().push(bytes);
                    }
                    Err(e) => {
                        read_error = Some(e);
                        break;
                    }
                }
                context
                    .renew()
                    .map_err(|e| anyhow::anyhow!("lease renew failed: {e:?}"))?;
            }
            // Release the single reader session on success and on failure
            // alike: a failed merge must not leave the attachment behind.
            // A detach error after a read error is secondary; the read
            // error is the one reported.
            match client.detach().await {
                Ok(()) => {}
                Err(e) if read_error.is_some() => {
                    eprintln!("index-merge reader detach after failure failed: {e:?}");
                }
                Err(e) => return Err(anyhow::Error::from(e)),
            }
            if let Some(e) = read_error {
                return Err(e);
            }
            Ok::<_, anyhow::Error>(merge_index(&docs.into_iter().collect::<Vec<_>>()))
        }) {
            Ok(merged) => merged,
            Err(e) => {
                // Full chain goes to the authority log; the diagnostic
                // detail field caps at 512 bytes (see diag()).
                eprintln!("index-merge consumer read failed: {e:?}");
                return Ok(ApplicationOutcome::Failed(diag(
                    ErrorCode::InternalError,
                    format!("consumer read failed: {e:#}"),
                )))
            }
        };
        let _ = std::fs::remove_dir_all(&scratch);
        context.begin_output(Number(merged.len() as u64 + 16), ApplicationLabel("text/index".into()))?;
        context.write_output(&merged)?;
        context.finish_output()?;
        Ok(ApplicationOutcome::Succeeded)
    }
}

/// Merge input header: `v1 authority=<a> owner=<o> creation=<c>`; each ref
/// line is prefixed with its doc id: `doc scope producer entity digest`.
fn parse_header(header: &str) -> Result<(String, u64)> {
    let mut authority = None;
    let mut creation = None;
    for part in header.split(' ') {
        if let Some(v) = part.strip_prefix("authority=") {
            authority = Some(v.to_string());
        }
        if let Some(v) = part.strip_prefix("creation=") {
            creation = Some(v.parse()?);
        }
    }
    Ok((authority.context("header needs authority")?, creation.context("header needs creation")?))
}

/// `tf/v1` execute: deterministic term-frequency record over one chunk.
struct TfContract;

fn registry(reader: Option<Reader>) -> Result<Arc<Applications>> {
    let mut apps = Applications::default();
    apps.register(
        ApplicationLabel(FILE_LABEL.into()),
        vec![Mode(2)],
        RestartSafety::Pure,
        Arc::new(FileContract {
            expander: FileExpander,
        }),
    )?;
    apps.register(
        ApplicationLabel(TF_LABEL.into()),
        vec![Mode(0)],
        RestartSafety::Pure,
        Arc::new(TfContract),
    )?;
    apps.register(
        ApplicationLabel(MERGE_LABEL.into()),
        vec![Mode(0)],
        RestartSafety::Pure,
        Arc::new(MergeContract { reader }),
    )?;
    Ok(Arc::new(apps))
}

fn certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let bytes = std::fs::read(path)?;
    let certificates: Vec<_> =
        CertificateDer::pem_slice_iter(&bytes).collect::<std::result::Result<_, _>>()?;
    if certificates.is_empty()
        || certificates.len() > 16
        || certificates.iter().map(|c| c.len()).sum::<usize>() > 65535
    {
        bail!("certificate chain exceeds count/byte bounds or is empty");
    }
    Ok(certificates)
}

fn roots(path: &Path) -> Result<rustls::RootCertStore> {
    let mut roots = rustls::RootCertStore::empty();
    for certificate in certificates(path)? {
        roots.add(certificate)?;
    }
    Ok(roots)
}

fn key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    Ok(PrivateKeyDer::from_pem_slice(&std::fs::read(path)?)?)
}

fn mappings(path: &Path) -> Result<BTreeMap<[u8; 32], IdentityLabel>> {
    let bytes = std::fs::read(path)?;
    let text = std::str::from_utf8(&bytes)?;
    let mut lines = text.lines();
    if lines.next() != Some("sha256\tprincipal") {
        bail!("principal map needs sha256<TAB>principal header");
    }
    let mut mappings = BTreeMap::new();
    for line in lines {
        let (fingerprint, owner) = line
            .split_once('\t')
            .context("principal row needs two columns")?;
        let fingerprint = hex32(fingerprint)?;
        let owner = IdentityLabel(owner.into());
        owner.validate()?;
        if mappings.len() == 4096 || mappings.insert(fingerprint, owner).is_some() {
            bail!("duplicate fingerprint or too many principal mappings");
        }
    }
    if mappings.is_empty() {
        bail!("at least one mapped principal is required");
    }
    Ok(mappings)
}

struct TrustedSystemClock;
impl Clock for TrustedSystemClock {
    fn read(&self) -> ClockReading {
        match SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|t| u64::try_from(t.as_millis()).ok())
            .filter(|t| *t <= MAX_NUMBER)
        {
            Some(utc) => ClockReading {
                utc_ms: Number(utc),
                trusted: true,
            },
            None => ClockReading {
                utc_ms: Number(0),
                trusted: false,
            },
        }
    }
}

struct Access {
    owners: BTreeSet<String>,
}
impl Authorization for Access {
    fn permits(&self, owner: &IdentityLabel, permission: Permission) -> bool {
        self.owners.contains(&owner.0) && permission != Permission::Skip
    }
}

#[derive(Debug, Args)]
struct Storage {
    #[arg(long)]
    state_db: PathBuf,
    #[arg(long)]
    object_dir: PathBuf,
    #[arg(long)]
    authority: String,
    #[arg(long)]
    principal_map: PathBuf,
    #[arg(long)]
    trust_system_clock: bool,
    #[arg(long, default_value_t = 256)]
    db_mib: u64,
    #[arg(long, default_value_t = 64)]
    wal_mib: u64,
}

fn physical_limits(db_mib: u64, wal_mib: u64) -> Result<PhysicalLimits> {
    let limits = PhysicalLimits {
        database_bytes: db_mib
            .checked_mul(1 << 20)
            .filter(|n| *n >= 65536 && *n <= (16 << 30) && *n % 65536 == 0)
            .context("db-mib out of funded range")?,
        wal_bytes: wal_mib
            .checked_mul(1 << 20)
            .filter(|n| *n >= 65536 && *n <= (16 << 30) && *n % 65536 == 0)
            .context("wal-mib out of funded range")?,
        ..PhysicalLimits::default()
    };
    Ok(limits)
}

impl Storage {
    fn open(&self, fresh: bool) -> Result<(AuthorityStore, PayloadStore, BTreeMap<[u8; 32], IdentityLabel>)> {
        if !self.trust_system_clock {
            bail!("--trust-system-clock is required");
        }
        let mappings = mappings(&self.principal_map)?;
        let access = Arc::new(Access {
            owners: mappings.values().map(|v| v.0.clone()).collect(),
        });
        let policy = StorePolicy {
            owners: Id(4096),
            sessions: Id(1024),
            sessions_per_owner: Id(64),
            active_jobs: Id(64),
            active_jobs_per_owner: Id(16),
            session_limits: Limits {
                scopes: Id(4096),
                entities: Id(1000000),
                operations: Id(1000000),
                active_jobs: Id(16),
                retained_input_bytes: Number(1 << 30),
                retained_output_bytes: Number(1 << 30),
            },
        };
        let open = if fresh {
            AuthorityStore::initialize
        } else {
            AuthorityStore::open
        };
        let store = open(
            &self.state_db,
            IdentityLabel(self.authority.clone()),
            policy,
            physical_limits(self.db_mib, self.wal_mib)?,
            Arc::new(TrustedSystemClock),
            access,
        )?;
        let policy = PayloadPolicy {
            objects: Id(10000),
            bytes: Number(8 * 1024 * 1024 * 1024),
            owner_objects: Id(10000),
            owner_bytes: Number(8 * 1024 * 1024 * 1024),
            chunk_bytes: Id(8192),
            handles: Id(256),
            owner_handles: Id(64),
        };
        let open = if fresh {
            PayloadStore::initialize
        } else {
            PayloadStore::open
        };
        let payloads = open(&self.object_dir, store.payload_identity()?, policy)?;
        store.bind_payloads(&payloads)?;
        Ok((store, payloads, mappings))
    }
}

#[derive(Debug, Args)]
struct Network {
    #[arg(long)]
    bind: SocketAddr,
    #[arg(long)]
    cert: PathBuf,
    #[arg(long)]
    key: PathBuf,
    #[arg(long)]
    client_ca: PathBuf,
    #[arg(long)]
    result_authority: String,
    #[arg(long)]
    ready_file: Option<PathBuf>,
    #[arg(long, default_value_t = 16*1024*1024)]
    object_limit: u64,
    /// Merge-reader peer endpoint (authority A) for index-merge/v1.
    #[arg(long)]
    reader_endpoint: Option<SocketAddr>,
    #[arg(long, default_value = "localhost")]
    reader_server_name: String,
    #[arg(long)]
    reader_ca: Option<PathBuf>,
    #[arg(long)]
    reader_cert: Option<PathBuf>,
    #[arg(long)]
    reader_key: Option<PathBuf>,
    #[arg(long, default_value = "indexer")]
    reader_owner: String,
}

impl Network {
    fn reader(&self, journal_dir: &Path) -> Option<Reader> {
        Some(Reader {
            endpoint: self.reader_endpoint?,
            server_name: self.reader_server_name.clone(),
            ca: self.reader_ca.clone()?,
            cert: self.reader_cert.clone()?,
            key: self.reader_key.clone()?,
            owner: self.reader_owner.clone(),
            journal_dir: journal_dir.to_path_buf(),
        })
    }
}

async fn serve(storage: Storage, network: Network) -> Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let journal_dir = storage
        .state_db
        .parent()
        .context("state-db needs a parent")?
        .to_path_buf();
    let (store, payloads, principals) = storage.open(false)?;
    let authentication = ClientAuthentication::new(
        IdentityLabel(storage.authority),
        roots(&network.client_ca)?,
        principals,
        Arc::new(rustls::time_provider::DefaultTimeProvider),
    )?;
    let security = ServerSecurity::new(
        certificates(&network.cert)?,
        key(&network.key)?,
        Some(Arc::new(authentication)),
    )?;
    let authority = Authority::new(store, payloads, 4)?;
    let mut options = Options::default();
    options.offer.object_limit = Number(network.object_limit);
    let server = Server::bind(
        network.bind,
        security,
        authority,
        registry(network.reader(&journal_dir))?,
        authority::execution::ResultEndpoint::new(network.result_authority)?,
        options,
    )?;
    let address = server.local_addr()?;
    if let Some(path) = network.ready_file {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)?;
        writeln!(file, "{address}")?;
        file.sync_all()?;
    }
    println!("LISTENING {address}");
    let report = server
        .run(async {
            tokio::select! { _ = terminate.recv() => {}, _ = interrupt.recv() => {} }
        })
        .await?;
    if !report.drained() || report.fault.is_some() {
        bail!("server shutdown did not fully drain: {report:?}");
    }
    println!("DRAINED");
    Ok(())
}

#[derive(Debug, Parser)]
#[command(about = "Durable-index worker authority (index-file/tf/index-merge v1)")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    InitAuthority(Storage),
    Serve {
        #[command(flatten)]
        storage: Storage,
        #[command(flatten)]
        network: Network,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::InitAuthority(storage) => {
            storage.open(true)?;
            println!("INITIALIZED");
            Ok(())
        }
        Command::Serve { storage, network } => serve(storage, network).await,
    }
}

impl Application for TfContract {
    fn execute(
        &self,
        context: &mut WorkContext,
    ) -> std::result::Result<ApplicationOutcome, StoreError> {
        let mut input = Vec::new();
        let mut buf = [0u8; 8192];
        let limit = buf.len().min(context.buffer_limit());
        loop {
            let n = context.read_input(&mut buf[..limit])?;
            if n == 0 {
                break;
            }
            input.extend_from_slice(&buf[..n]);
            context.renew()?;
        }
        let record = tf_record(&input);
        context.begin_output(Number(record.len() as u64 + 16), ApplicationLabel("text/tf".into()))?;
        context.write_output(&record)?;
        context.finish_output()?;
        Ok(ApplicationOutcome::Succeeded)
    }
}
