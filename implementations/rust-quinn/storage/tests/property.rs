//! Property and model tests for the storage SPI crate.
//!
//! Complements `layout_vectors.rs` (pinned literals) with randomized
//! properties, all driven by a small deterministic xorshift PRNG (no `rand`
//! dependency, fixed seeds, no clocks, no I/O beyond a local tempdir and an
//! optional python3 subprocess):
//!
//! - Layout properties: fixed-width lowercase hex rendering, parse
//!   round-trips, canonical ordering, and `scope_digest` canonicalization.
//! - An independent oracle: `python3` hashlib recomputes fanout buckets and
//!   canonical digests from scratch (skipped when python3 is absent).
//! - A model test: random operation sequences run against `MemoryStore`
//!   (the oracle) and `FsStore` side by side.
//!
//! Cross-backend identity property: every backend must behave identically
//! on EVERY observable — success values, error kinds, entity listings,
//! scope listings, seal records, read bytes, replay output, and cursor
//! assignment. The model test asserts that identity after every single
//! operation; any divergence is a bug in one of the backends, never a
//! reason to weaken an assertion.
//!
//! Every failure message carries the PRNG seed and step number, so a
//! failure reproduces deterministically by re-running the same test.

#![forbid(unsafe_code)]

use pipestream_storage::layout::{self, Digest32, EntityId, MAX_SCOPE_DEPTH};
use pipestream_storage::{
    Cursor, FsStore, Intent, Journal, MemoryStore, ObjectStore, PutWriter, ReplayEntry, ScopePath,
    SealRecord, StorageError, sha256,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::io::{Read as _, Write as _};
use std::process::{Command, Stdio};

// ---------------------------------------------------------------------------
// Deterministic PRNG and small helpers
// ---------------------------------------------------------------------------

/// xorshift64* PRNG: deterministic, seedable, and more than uniform enough
/// for test data. Seeded through the splitmix64 finalizer so nearby seeds
/// produce unrelated streams.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        // xorshift needs a nonzero state.
        Rng(if z == 0 { 0x853C_49E6_748F_EA9B } else { z })
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn next_u32(&mut self) -> u32 {
        (self.next_u64() >> 32) as u32
    }

    /// A value in `0..bound` (modulo bias is irrelevant for these tests).
    fn below(&mut self, bound: u64) -> u64 {
        assert!(bound > 0);
        self.next_u64() % bound
    }

    fn bytes(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| self.next_u64() as u8).collect()
    }

    fn digest(&mut self) -> Digest32 {
        let mut digest = [0u8; 32];
        for chunk in digest.chunks_mut(8) {
            chunk.copy_from_slice(&self.next_u64().to_be_bytes());
        }
        digest
    }
}

/// In-place Fisher–Yates shuffle.
fn shuffle<T>(items: &mut [T], rng: &mut Rng) {
    for i in (1..items.len()).rev() {
        let j = rng.below(i as u64 + 1) as usize;
        items.swap(i, j);
    }
}

/// Splits `payload` into 1..=4 chunks (empty chunks allowed).
fn chunk(payload: &[u8], rng: &mut Rng) -> Vec<Vec<u8>> {
    let parts = 1 + rng.below(4) as usize;
    let mut cuts: Vec<usize> = (0..parts - 1)
        .map(|_| rng.below(payload.len() as u64 + 1) as usize)
        .collect();
    cuts.sort_unstable();
    let mut chunks = Vec::with_capacity(parts);
    let mut previous = 0;
    for cut in cuts {
        chunks.push(payload[previous..cut].to_vec());
        previous = cut;
    }
    chunks.push(payload[previous..].to_vec());
    chunks
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    out
}

fn is_lower_hex(text: &str) -> bool {
    text.bytes()
        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

// ---------------------------------------------------------------------------
// Error-kind comparison (StorageError intentionally has no PartialEq)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    NotFound,
    AlreadySealed,
    Corrupt,
    Transient,
    Failed,
}

fn error_kind(error: &StorageError) -> Kind {
    match error {
        StorageError::NotFound(_) => Kind::NotFound,
        StorageError::AlreadySealed(_) => Kind::AlreadySealed,
        StorageError::Corrupt(_) => Kind::Corrupt,
        StorageError::Transient(_) => Kind::Transient,
        StorageError::Failed(_) => Kind::Failed,
    }
}

/// Drops the error detail, keeping the value or the error kind: the pinned
/// contract is on kinds, and kinds must match across backends exactly.
fn strip<T>(result: Result<T, StorageError>) -> Result<T, Kind> {
    result.map_err(|error| error_kind(&error))
}

// ---------------------------------------------------------------------------
// Backend-generic operation helpers
// ---------------------------------------------------------------------------

fn read_all<S: ObjectStore>(
    store: &S,
    scope: &ScopePath,
    entity: EntityId,
) -> Result<Vec<u8>, StorageError> {
    let mut reader = store.open_read(scope, entity)?;
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn stream_put<S: ObjectStore>(
    store: &mut S,
    scope: &ScopePath,
    entity: EntityId,
    chunks: &[Vec<u8>],
    digest: &Digest32,
) -> Result<(), StorageError> {
    let mut writer = store.begin_put(scope, entity, digest)?;
    for part in chunks {
        writer.write_all(part)?;
    }
    writer.finish()
}

/// Begins a streaming put, writes the chunks, and aborts by dropping the
/// writer: nothing may become visible.
fn stream_abort<S: ObjectStore>(
    store: &mut S,
    scope: &ScopePath,
    entity: EntityId,
    chunks: &[Vec<u8>],
    digest: &Digest32,
) -> Result<(), StorageError> {
    let mut writer = store.begin_put(scope, entity, digest)?;
    for part in chunks {
        writer.write_all(part)?;
    }
    drop(writer);
    Ok(())
}

// ---------------------------------------------------------------------------
// 1. Layout properties
// ---------------------------------------------------------------------------

const LAYOUT_ITERATIONS: usize = 5000;

/// A random u32 biased toward boundary shapes (zero, small, near-max).
fn random_component(rng: &mut Rng) -> u32 {
    match rng.below(4) {
        0 => rng.below(256) as u32,
        1 => u32::MAX - rng.below(256) as u32,
        2 => 0,
        _ => rng.next_u32(),
    }
}

#[test]
fn layout_render_parse_roundtrip_properties() {
    let mut rng = Rng::new(0x1A70_0700_0000_0001);
    for iteration in 0..LAYOUT_ITERATIONS {
        let value = random_component(&mut rng);

        // Fanout bucket: exactly 2 lowercase hex chars decoding to the
        // bucket byte.
        let bucket = layout::bucket_hex(value);
        assert_eq!(bucket.len(), 2, "iteration {iteration}");
        assert!(is_lower_hex(&bucket), "iteration {iteration}");
        assert_eq!(
            u8::from_str_radix(&bucket, 16),
            Ok(layout::bucket(value)),
            "iteration {iteration}"
        );

        // Entity and scope components: 8 lowercase hex, zero-padded,
        // parse back to the value.
        let entity = layout::entity_component(value);
        let component = layout::scope_component(value);
        assert_eq!(entity, component, "iteration {iteration}");
        assert_eq!(entity.len(), 8, "iteration {iteration}");
        assert!(is_lower_hex(&entity), "iteration {iteration}");
        assert_eq!(
            u32::from_str_radix(&entity, 16),
            Ok(value),
            "iteration {iteration}"
        );
        if value < 0x10 {
            assert!(entity.starts_with("0000000"), "zero padding, iteration {iteration}");
        }
        // Fixed-width lowercase hex sorts exactly like the numeric value —
        // this is what makes "ascending rendered path" a canonical order.
        let other = random_component(&mut rng);
        assert_eq!(
            value.cmp(&other),
            layout::entity_component(value).cmp(&layout::entity_component(other)),
            "iteration {iteration}"
        );

        // Scope paths at protocol depths 0..=MAX_SCOPE_DEPTH: render,
        // split, parse, and re-render all agree.
        let depth = rng.below(MAX_SCOPE_DEPTH as u64 + 1) as usize;
        let mut components = Vec::with_capacity(depth + 1);
        let mut scope = ScopePath::root(random_component(&mut rng));
        components.push(scope.scope_id());
        for _ in 0..depth {
            let child = random_component(&mut rng);
            scope = scope.child(child).expect("depth within the protocol bound");
            components.push(child);
        }
        assert_eq!(scope.depth(), depth, "iteration {iteration}");
        assert_eq!(scope.components(), components.as_slice(), "iteration {iteration}");
        let rendered = scope.to_path_string();
        let parts: Vec<&str> = rendered.split('/').collect();
        assert_eq!(parts.len(), depth + 1, "iteration {iteration}");
        for (part, expected) in parts.iter().zip(&components) {
            assert_eq!(part.len(), 8, "iteration {iteration}");
            assert!(is_lower_hex(part), "iteration {iteration}");
            assert_eq!(
                u32::from_str_radix(part, 16),
                Ok(*expected),
                "iteration {iteration}"
            );
        }
        let parsed = ScopePath::parse(&rendered).expect("the rendered form parses");
        assert_eq!(parsed, scope, "iteration {iteration}");
        assert_eq!(
            parsed.to_path_string(),
            rendered,
            "the rendered form is canonical, iteration {iteration}"
        );
        assert_eq!(format!("{scope}"), format!("/{rendered}"), "iteration {iteration}");

        // ScopePath ordering (memory backend's BTreeMap) must equal
        // rendered-path ordering (fs backend's walk + string sort), or
        // list_scopes would diverge across backends.
        let other_depth = rng.below(MAX_SCOPE_DEPTH as u64 + 1) as usize;
        let mut other = ScopePath::root(random_component(&mut rng));
        for _ in 0..other_depth {
            other = other
                .child(random_component(&mut rng))
                .expect("depth within the protocol bound");
        }
        assert_eq!(
            scope.cmp(&other),
            rendered.cmp(&other.to_path_string()),
            "iteration {iteration}"
        );
    }
}

const SCOPE_DIGEST_ITERATIONS: usize = 5000;

#[test]
fn scope_digest_canonicalization_properties() {
    let mut rng = Rng::new(0x5C09_ED1E_5700_0001);
    // The empty scope's digest is SHA-256 of the empty input.
    assert_eq!(layout::scope_digest(&[]).expect("empty"), sha256(b""));
    for iteration in 0..SCOPE_DIGEST_ITERATIONS {
        let context = format!("scope_digest iteration {iteration}");
        let count = rng.below(13) as usize;
        let mut ids = BTreeSet::new();
        while ids.len() < count {
            ids.insert(rng.next_u32());
        }
        let mut entities: Vec<(EntityId, Digest32)> =
            ids.iter().map(|id| (*id, rng.digest())).collect();
        shuffle(&mut entities, &mut rng);
        let digest = layout::scope_digest(&entities).expect("ids are unique");

        // Permutation invariance: the input is canonicalized by sorting.
        let mut permuted = entities.clone();
        shuffle(&mut permuted, &mut rng);
        assert_eq!(
            layout::scope_digest(&permuted).expect("ids are unique"),
            digest,
            "permutation invariance — {context}"
        );

        // Independent recomputation using only the crate's sha256
        // primitive: ascending 0x00 || id-be32 || digest32.
        let mut ascending = entities.clone();
        ascending.sort_by_key(|(id, _)| *id);
        let mut concatenation = Vec::with_capacity(ascending.len() * 37);
        for (id, entity_digest) in &ascending {
            concatenation.push(0u8);
            concatenation.extend_from_slice(&id.to_be_bytes());
            concatenation.extend_from_slice(entity_digest);
        }
        assert_eq!(
            sha256(&concatenation),
            digest,
            "manual recomputation — {context}"
        );

        if entities.is_empty() {
            continue;
        }

        // Multiset sensitivity: any change to the set changes the digest.
        let mut mutated = entities.clone();
        match rng.below(3) {
            0 => mutated[0].1[0] ^= 1,
            1 => {
                mutated.remove(0);
            }
            _ => {
                let fresh_id = loop {
                    let candidate = rng.next_u32();
                    if mutated.iter().all(|(id, _)| *id != candidate) {
                        break candidate;
                    }
                };
                mutated.push((fresh_id, rng.digest()));
            }
        }
        assert_ne!(
            layout::scope_digest(&mutated).expect("ids are unique"),
            digest,
            "a different multiset must digest differently — {context}"
        );

        // Duplicate ids are rejected, whatever the digests.
        let (duplicate_id, duplicate_digest) = entities[0];
        let mut with_duplicate = entities.clone();
        with_duplicate.push((duplicate_id, duplicate_digest));
        assert!(
            matches!(
                layout::scope_digest(&with_duplicate),
                Err(StorageError::Failed(_))
            ),
            "duplicate id — {context}"
        );
        let mut with_conflicting_duplicate = entities.clone();
        with_conflicting_duplicate.push((duplicate_id, rng.digest()));
        assert!(
            matches!(
                layout::scope_digest(&with_conflicting_duplicate),
                Err(StorageError::Failed(_))
            ),
            "conflicting duplicate id — {context}"
        );
    }
}

// ---------------------------------------------------------------------------
// 1b. Independent python3 oracle
// ---------------------------------------------------------------------------

const PYTHON_ORACLE_ENTITIES: usize = 50;
const PYTHON_ORACLE_DIGEST_SETS: usize = 16;

fn python3_available() -> bool {
    Command::new("python3")
        .arg("--version")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

/// Reads per set: a count line, then `id digest-hex` lines; prints one
/// canonical digest hex per set. This is the layout doc's pinned
/// derivation re-implemented with hashlib only.
const PYTHON_DIGEST_SCRIPT: &str = r#"
import hashlib, sys
tokens = sys.stdin.read().split()
i = 0
out = []
while i < len(tokens):
    n = int(tokens[i]); i += 1
    h = hashlib.sha256()
    for _ in range(n):
        e = int(tokens[i]); d = bytes.fromhex(tokens[i + 1]); i += 2
        h.update(b"\x00"); h.update(e.to_bytes(4, "big")); h.update(d)
    out.append(h.hexdigest())
print("\n".join(out))
"#;

#[test]
fn layout_matches_independent_python3_hashlib() {
    if !python3_available() {
        eprintln!("python3 not found; skipping the independent-oracle check");
        return;
    }
    let mut rng = Rng::new(0x1A70_0700_0000_0002);

    // 50 random entities: python recomputes SHA-256(id as 4-octet
    // big-endian); the first byte as 2 lowercase hex chars is the bucket.
    let entities: Vec<u32> = (0..PYTHON_ORACLE_ENTITIES)
        .map(|_| rng.next_u32())
        .collect();
    let output = Command::new("python3")
        .arg("-c")
        .arg(
            "import hashlib,sys\n\
             for a in sys.argv[1:]:\n \
             print(hashlib.sha256(int(a).to_bytes(4,'big')).hexdigest())",
        )
        .args(entities.iter().map(u32::to_string))
        .output()
        .expect("python3 runs");
    assert!(
        output.status.success(),
        "python3 failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("python output is UTF-8");
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), entities.len());
    for (entity, line) in entities.iter().zip(&lines) {
        assert_eq!(line.len(), 64, "entity {entity:#010x}");
        // Stronger than the bucket check alone: the crate's sha256 itself
        // must agree with hashlib byte for byte.
        assert_eq!(
            hex(&sha256(&entity.to_be_bytes())),
            *line,
            "crate sha256 disagrees with hashlib for entity {entity:#010x}"
        );
        assert_eq!(
            layout::bucket_hex(*entity),
            &line[..2],
            "bucket disagrees with hashlib for entity {entity:#010x}"
        );
    }

    // Random digest sets: python recomputes the canonical scope digest.
    let mut sets: Vec<Vec<(EntityId, Digest32)>> = Vec::new();
    let mut input = String::new();
    for _ in 0..PYTHON_ORACLE_DIGEST_SETS {
        let count = rng.below(9) as usize;
        let mut ids = BTreeSet::new();
        while ids.len() < count {
            ids.insert(rng.below(100_000) as u32);
        }
        // BTreeSet iteration is ascending — the canonical order.
        let set: Vec<(EntityId, Digest32)> = ids.iter().map(|id| (*id, rng.digest())).collect();
        writeln!(input, "{}", set.len()).expect("writing to a String cannot fail");
        for (id, digest) in &set {
            writeln!(input, "{id} {}", hex(digest)).expect("writing to a String cannot fail");
        }
        sets.push(set);
    }
    let mut child = Command::new("python3")
        .arg("-c")
        .arg(PYTHON_DIGEST_SCRIPT)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("python3 spawns");
    let mut stdin = child.stdin.take().expect("stdin is piped");
    stdin
        .write_all(input.as_bytes())
        .expect("writing to python stdin");
    drop(stdin);
    let output = child.wait_with_output().expect("python3 completes");
    assert!(
        output.status.success(),
        "python3 failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("python output is UTF-8");
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), sets.len());
    for (set, line) in sets.iter().zip(&lines) {
        let expected = layout::scope_digest(set).expect("ids are unique");
        assert_eq!(
            hex(&expected),
            *line,
            "scope_digest disagrees with hashlib for set {set:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// 2. Model test: MemoryStore (oracle) vs FsStore, random op sequences
// ---------------------------------------------------------------------------

const MODEL_OPS_PER_ITERATION: usize = 400;
const MODEL_ITERATIONS_PER_SHARD: u64 = 5;
const MODEL_ENTITY_SPACE: u64 = 300;
const MODEL_SCOPE_COMPONENT_SPACE: u64 = 4;
const MODEL_MAX_DEPTH: u64 = 3;
const INTENT_ID_SPACE: u64 = 48;

#[derive(Debug)]
enum Op {
    Put {
        scope: ScopePath,
        entity: EntityId,
        bytes: Vec<u8>,
        digest: Digest32,
    },
    StreamPut {
        scope: ScopePath,
        entity: EntityId,
        chunks: Vec<Vec<u8>>,
        digest: Digest32,
    },
    StreamAbort {
        scope: ScopePath,
        entity: EntityId,
        chunks: Vec<Vec<u8>>,
        digest: Digest32,
    },
    OpenRead {
        scope: ScopePath,
        entity: EntityId,
    },
    Seal {
        scope: ScopePath,
        digest: Digest32,
        count: u64,
    },
    Verify {
        scope: ScopePath,
    },
    List {
        scope: ScopePath,
    },
    Retire {
        scope: ScopePath,
        cursor: u64,
    },
    Append {
        id: String,
        record: Vec<u8>,
    },
    Commit {
        id: String,
        record: Vec<u8>,
    },
}

impl Op {
    fn scope(&self) -> Option<&ScopePath> {
        match self {
            Op::Put { scope, .. }
            | Op::StreamPut { scope, .. }
            | Op::StreamAbort { scope, .. }
            | Op::OpenRead { scope, .. }
            | Op::Seal { scope, .. }
            | Op::Verify { scope, .. }
            | Op::List { scope, .. }
            | Op::Retire { scope, .. } => Some(scope),
            Op::Append { .. } | Op::Commit { .. } => None,
        }
    }
}

#[derive(Debug, PartialEq)]
enum OpOutcome {
    Unit(Result<(), Kind>),
    Bytes(Result<Vec<u8>, Kind>),
    Count(Result<u64, Kind>),
    SealRecord(Result<SealRecord, Kind>),
    Entities(Result<Vec<EntityId>, Kind>),
    Cursor(Result<Cursor, Kind>),
}

fn gen_scope(rng: &mut Rng, max_depth: u64) -> ScopePath {
    let depth = rng.below(max_depth + 1);
    let mut scope = ScopePath::root(rng.below(MODEL_SCOPE_COMPONENT_SPACE) as u32);
    for _ in 0..depth {
        scope = scope
            .child(rng.below(MODEL_SCOPE_COMPONENT_SPACE) as u32)
            .expect("depth stays below the protocol bound");
    }
    scope
}

fn make_put(
    rng: &mut Rng,
    scope: ScopePath,
    entity: EntityId,
    bytes: Vec<u8>,
    allow_bad_digest: bool,
    streamed: bool,
) -> Op {
    let mut digest = sha256(&bytes);
    if allow_bad_digest && rng.below(10) == 0 {
        digest[0] ^= 1; // a digest that cannot match the payload
    }
    if streamed {
        Op::StreamPut {
            scope,
            entity,
            chunks: chunk(&bytes, rng),
            digest,
        }
    } else {
        Op::Put {
            scope,
            entity,
            bytes,
            digest,
        }
    }
}

fn gen_put(rng: &mut Rng, memory: &MemoryStore, streamed: bool) -> Op {
    let scope = gen_scope(rng, MODEL_MAX_DEPTH);
    // A quarter of puts target an entity the oracle already holds: half of
    // those are idempotent re-puts, half conflicting re-puts.
    let live = memory.list(&scope).expect("oracle list");
    if !live.is_empty() && rng.below(4) == 0 {
        let entity = live[rng.below(live.len() as u64) as usize];
        let current = read_all(memory, &scope, entity).expect("oracle read");
        let bytes = if rng.below(2) == 0 {
            current
        } else {
            {
        let n = rng.below(49) as usize;
        rng.bytes(n)
    }
        };
        return make_put(rng, scope, entity, bytes, false, streamed);
    }
    let entity = rng.below(MODEL_ENTITY_SPACE) as u32;
    let bytes = {
        let n = rng.below(49) as usize;
        rng.bytes(n)
    };
    make_put(rng, scope, entity, bytes, true, streamed)
}

fn gen_stream_abort(rng: &mut Rng) -> Op {
    let scope = gen_scope(rng, MODEL_MAX_DEPTH);
    let entity = rng.below(MODEL_ENTITY_SPACE) as u32;
    let bytes = {
        let n = rng.below(49) as usize;
        rng.bytes(n)
    };
    Op::StreamAbort {
        scope,
        entity,
        chunks: chunk(&bytes, rng),
        digest: sha256(&bytes),
    }
}

fn gen_open_read(rng: &mut Rng, memory: &MemoryStore) -> Op {
    let scope = gen_scope(rng, MODEL_MAX_DEPTH);
    let live = memory.list(&scope).expect("oracle list");
    let entity = if !live.is_empty() && rng.below(10) < 7 {
        live[rng.below(live.len() as u64) as usize]
    } else {
        rng.below(MODEL_ENTITY_SPACE) as u32
    };
    Op::OpenRead { scope, entity }
}

fn gen_seal(rng: &mut Rng, memory: &MemoryStore) -> Op {
    let scope = gen_scope(rng, MODEL_MAX_DEPTH);
    // The canonical digest of the oracle's live set.
    let live = memory.list(&scope).expect("oracle list");
    let mut entries = Vec::with_capacity(live.len());
    for entity in &live {
        let bytes = read_all(memory, &scope, *entity).expect("oracle read");
        entries.push((*entity, sha256(&bytes)));
    }
    let canonical = layout::scope_digest(&entries).expect("oracle ids are unique");
    let count = live.len() as u64;
    let (digest, count) = match rng.below(10) {
        0 => {
            let mut wrong = canonical;
            wrong[0] ^= 1;
            (wrong, count)
        }
        1 => (canonical, count + 1),
        _ => (canonical, count),
    };
    Op::Seal {
        scope,
        digest,
        count,
    }
}

fn gen_append(rng: &mut Rng, appended: &BTreeMap<String, (Vec<u8>, Cursor)>) -> Op {
    if !appended.is_empty() && rng.below(10) < 4 {
        let ids: Vec<&String> = appended.keys().collect();
        let id = ids[rng.below(ids.len() as u64) as usize].clone();
        let record = if rng.below(2) == 0 {
            appended[&id].0.clone() // idempotent re-append
        } else {
            {
        let n = 1 + rng.below(24) as usize;
        rng.bytes(n)
    } // (usually) conflicting
        };
        Op::Append { id, record }
    } else {
        Op::Append {
            id: format!("intent-{}", rng.below(INTENT_ID_SPACE)),
            record: {
        let n = 1 + rng.below(24) as usize;
        rng.bytes(n)
    },
        }
    }
}

fn gen_commit(
    rng: &mut Rng,
    appended: &BTreeMap<String, (Vec<u8>, Cursor)>,
    committed: &BTreeMap<String, Vec<u8>>,
) -> Op {
    if !appended.is_empty() && rng.below(10) < 7 {
        let ids: Vec<&String> = appended.keys().collect();
        let id = ids[rng.below(ids.len() as u64) as usize].clone();
        let record = match committed.get(&id) {
            Some(existing) if rng.below(2) == 0 => existing.clone(), // idempotent
            _ => {
        let n = 1 + rng.below(24) as usize;
        rng.bytes(n)
    },
        };
        Op::Commit { id, record }
    } else {
        Op::Commit {
            id: format!("ghost-{}", rng.below(16)), // never appended
            record: {
        let n = 1 + rng.below(8) as usize;
        rng.bytes(n)
    },
        }
    }
}

fn gen_op(
    rng: &mut Rng,
    memory: &MemoryStore,
    appended: &BTreeMap<String, (Vec<u8>, Cursor)>,
    committed: &BTreeMap<String, Vec<u8>>,
) -> Op {
    match rng.below(100) {
        0..=24 => gen_put(rng, memory, false),
        25..=34 => gen_put(rng, memory, true),
        35..=37 => gen_stream_abort(rng),
        38..=47 => gen_open_read(rng, memory),
        48..=55 => gen_seal(rng, memory),
        56..=61 => Op::Verify {
            scope: gen_scope(rng, MODEL_MAX_DEPTH),
        },
        62..=65 => Op::List {
            scope: gen_scope(rng, MODEL_MAX_DEPTH),
        },
        66..=72 => Op::Retire {
            scope: gen_scope(rng, MODEL_MAX_DEPTH),
            cursor: match rng.below(10) {
                0 => 0,
                1 => u64::MAX,
                _ => rng.below(MODEL_ENTITY_SPACE + 20),
            },
        },
        73..=83 => gen_append(rng, appended),
        84..=95 => gen_commit(rng, appended, committed),
        _ => Op::OpenRead {
            scope: gen_scope(rng, MODEL_MAX_DEPTH),
            entity: rng.below(MODEL_ENTITY_SPACE) as u32,
        },
    }
}

fn apply_op<S: ObjectStore + Journal>(store: &mut S, op: &Op) -> OpOutcome {
    match op {
        Op::Put {
            scope,
            entity,
            bytes,
            digest,
        } => OpOutcome::Unit(strip(store.put(scope, *entity, bytes, digest))),
        Op::StreamPut {
            scope,
            entity,
            chunks,
            digest,
        } => OpOutcome::Unit(strip(stream_put(store, scope, *entity, chunks, digest))),
        Op::StreamAbort {
            scope,
            entity,
            chunks,
            digest,
        } => OpOutcome::Unit(strip(stream_abort(store, scope, *entity, chunks, digest))),
        Op::OpenRead { scope, entity } => {
            OpOutcome::Bytes(strip(read_all(store, scope, *entity)))
        }
        Op::Seal {
            scope,
            digest,
            count,
        } => OpOutcome::Unit(strip(store.seal_scope(scope, digest, *count))),
        Op::Verify { scope } => OpOutcome::SealRecord(strip(store.verify_scope(scope))),
        Op::List { scope } => OpOutcome::Entities(strip(store.list(scope))),
        Op::Retire { scope, cursor } => OpOutcome::Count(strip(store.retire(scope, *cursor))),
        Op::Append { id, record } => {
            let intent =
                Intent::new(id.clone(), record.clone()).expect("generated intents are valid");
            OpOutcome::Cursor(strip(store.append_intent(&intent)))
        }
        Op::Commit { id, record } => OpOutcome::Unit(strip(store.commit(id, record))),
    }
}

/// Compares everything the stores expose for every scope holding state:
/// entity listings, object bytes, and seal verification.
fn full_sweep<M: ObjectStore, F: ObjectStore>(memory: &M, fs: &F, context: &str) {
    let scopes = memory.list_scopes().expect("oracle list_scopes");
    for scope in &scopes {
        let memory_ids = strip(memory.list(scope));
        let fs_ids = strip(fs.list(scope));
        assert_eq!(memory_ids, fs_ids, "list divergence in sweep — {context}");
        for entity in memory_ids.expect("compared above") {
            let memory_bytes = strip(read_all(memory, scope, entity));
            let fs_bytes = strip(read_all(fs, scope, entity));
            assert_eq!(
                memory_bytes, fs_bytes,
                "open_read divergence for entity {entity} — {context}"
            );
        }
        let memory_verify = strip(memory.verify_scope(scope));
        let fs_verify = strip(fs.verify_scope(scope));
        assert_eq!(
            memory_verify, fs_verify,
            "verify_scope divergence in sweep — {context}"
        );
    }
}

/// One model iteration: `MODEL_OPS_PER_ITERATION` random operations applied
/// to both backends. This is the cross-backend identity property, asserted
/// after EVERY operation on EVERY observable: the operation outcome (value
/// or error kind), the touched scope's listing, the global scope listing,
/// and the full journal replay. MemoryStore is the oracle — when the two
/// disagree, one of them is wrong, and the seed in the panic message
/// reproduces the divergence deterministically.
fn run_model_iteration(seed: u64) {
    let mut rng = Rng::new(seed);
    let mut memory = MemoryStore::new();
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fs = FsStore::open(dir.path()).expect("fs store");
    // Driver-side journal bookkeeping (BTreeMap: deterministic iteration).
    let mut appended: BTreeMap<String, (Vec<u8>, Cursor)> = BTreeMap::new();
    let mut committed: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    let mut next_cursor = 1u64;

    for step in 0..MODEL_OPS_PER_ITERATION {
        let op = gen_op(&mut rng, &memory, &appended, &committed);
        let context = format!("seed {seed:#018x} step {step}: {op:?}");
        let memory_outcome = apply_op(&mut memory, &op);
        let fs_outcome = apply_op(&mut fs, &op);
        assert_eq!(
            memory_outcome, fs_outcome,
            "operation outcome — {context}"
        );

        // Fold the oracle's outcome into the driver model; this also pins
        // absolute cursor assignment (fresh appends and first-time commits
        // consume the next cursor; idempotent retries consume none).
        match (&op, &memory_outcome) {
            (Op::Append { id, record }, OpOutcome::Cursor(Ok(cursor))) => {
                match appended.get(id) {
                    Some((existing_record, original)) => {
                        assert_eq!(
                            existing_record, record,
                            "oracle accepted a conflicting re-append — {context}"
                        );
                        assert_eq!(
                            original, cursor,
                            "idempotent re-append must return the original cursor — {context}"
                        );
                    }
                    None => {
                        assert_eq!(
                            *cursor,
                            Cursor(next_cursor),
                            "fresh appends consume cursors densely — {context}"
                        );
                        next_cursor += 1;
                        appended.insert(id.clone(), (record.clone(), *cursor));
                    }
                }
            }
            (Op::Commit { id, record }, OpOutcome::Unit(Ok(()))) => {
                match committed.get(id) {
                    Some(existing) => assert_eq!(
                        existing, record,
                        "oracle accepted a conflicting re-commit — {context}"
                    ),
                    None => {
                        committed.insert(id.clone(), record.clone());
                        next_cursor += 1; // a first-time commit consumes a cursor
                    }
                }
            }
            _ => {}
        }

        // The full observable state must agree after every operation.
        assert_eq!(
            strip(memory.list_scopes()),
            strip(fs.list_scopes()),
            "list_scopes — {context}"
        );
        assert_eq!(
            strip(memory.replay(Cursor(0))),
            strip(fs.replay(Cursor(0))),
            "replay from Cursor(0) — {context}"
        );
        if let Some(scope) = op.scope() {
            assert_eq!(
                strip(memory.list(scope)),
                strip(fs.list(scope)),
                "list — {context}"
            );
        }

        if step % 25 == 24 {
            full_sweep(&memory, &fs, &context);
            if next_cursor > 1 {
                let since = Cursor(rng.below(next_cursor));
                assert_eq!(
                    strip(memory.replay(since)),
                    strip(fs.replay(since)),
                    "replay from {since:?} — {context}"
                );
            }
        }
    }

    full_sweep(&memory, &fs, "final sweep");

    // Everything observable must survive a close/reopen of the fs backend.
    drop(fs);
    let fs = FsStore::open(dir.path()).expect("fs store reopens");
    assert_eq!(
        strip(memory.list_scopes()),
        strip(fs.list_scopes()),
        "list_scopes after reopen (seed {seed:#018x})"
    );
    assert_eq!(
        strip(memory.replay(Cursor(0))),
        strip(fs.replay(Cursor(0))),
        "replay after reopen (seed {seed:#018x})"
    );
    full_sweep(&memory, &fs, "after reopen");
}

fn run_model_shard(shard: u64) {
    for iteration in 0..MODEL_ITERATIONS_PER_SHARD {
        let seed = 0x5EED_0000_0000_0000 | (shard << 16) | iteration;
        run_model_iteration(seed);
    }
}

// Six shards keep the fsync-heavy model test parallel under `cargo test`
// while covering the assigned 30 iterations x 400 operations in total.
#[test]
fn model_memory_vs_fs_shard_0() {
    run_model_shard(0);
}

#[test]
fn model_memory_vs_fs_shard_1() {
    run_model_shard(1);
}

#[test]
fn model_memory_vs_fs_shard_2() {
    run_model_shard(2);
}

#[test]
fn model_memory_vs_fs_shard_3() {
    run_model_shard(3);
}

#[test]
fn model_memory_vs_fs_shard_4() {
    run_model_shard(4);
}

#[test]
fn model_memory_vs_fs_shard_5() {
    run_model_shard(5);
}

// ---------------------------------------------------------------------------
// 3. Journal cursor properties
// ---------------------------------------------------------------------------

const JOURNAL_OPS_PER_ROUND: usize = 1200;
const JOURNAL_ROUNDS: u64 = 4;

struct ModelEntry {
    intent: Intent,
    cursor: Cursor,
    commit: Option<(Vec<u8>, Cursor)>,
}

fn expected_replay(model: &[ModelEntry]) -> Vec<ReplayEntry> {
    model
        .iter()
        .map(|entry| ReplayEntry {
            cursor: entry.cursor,
            intent: entry.intent.clone(),
            commit: entry.commit.as_ref().map(|(bytes, _)| bytes.clone()),
            commit_cursor: entry.commit.as_ref().map(|(_, cursor)| *cursor),
        })
        .collect()
}

fn assert_replay_matches_model<J: Journal>(
    store: &J,
    model: &[ModelEntry],
    next_cursor: u64,
    context: &str,
) {
    let replayed = store.replay(Cursor(0)).expect("replay");
    assert_eq!(
        replayed,
        expected_replay(model),
        "replay must equal the folded intent/commit model — {context}"
    );
    // Replay cursors strictly increase along the replay.
    for pair in replayed.windows(2) {
        assert!(
            pair[0].cursor < pair[1].cursor,
            "replay cursors strictly increase — {context}"
        );
    }
    // Every commit consumes a cursor later than its intent's, and intents
    // plus commits together consume exactly 1..next_cursor — i.e. commit
    // cursors land between intent cursors in the global append order, with
    // no gaps and no duplicates.
    let mut consumed = Vec::new();
    for entry in &replayed {
        consumed.push(entry.cursor.0);
        if let Some(commit_cursor) = entry.commit_cursor {
            assert!(
                entry.cursor < commit_cursor,
                "a commit lands after its intent — {context}"
            );
            consumed.push(commit_cursor.0);
        }
    }
    consumed.sort_unstable();
    let gapless: Vec<u64> = (1..next_cursor).collect();
    assert_eq!(
        consumed, gapless,
        "cursors are consumed densely in the global order — {context}"
    );
    // Replaying from the last intent cursor yields nothing.
    if let Some(last) = replayed.last() {
        assert!(
            store
                .replay(last.cursor)
                .expect("replay at the tip")
                .is_empty(),
            "replay from the last intent cursor is empty — {context}"
        );
    }
}

/// replay(since) returns exactly the intents with cursor > since.
fn check_suffix_replays<J: Journal>(
    store: &J,
    full: &[ReplayEntry],
    rng: &mut Rng,
    next_cursor: u64,
    context: &str,
) {
    for _ in 0..8 {
        let since = Cursor(rng.below(next_cursor));
        let replayed = store.replay(since).expect("replay");
        let expected: Vec<ReplayEntry> = full
            .iter()
            .filter(|entry| entry.cursor > since)
            .cloned()
            .collect();
        assert_eq!(replayed, expected, "suffix replay — {context}");
    }
}

fn run_journal_round(seed: u64) {
    let mut rng = Rng::new(seed);
    let mut memory = MemoryStore::new();
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fs = FsStore::open(dir.path()).expect("fs store");
    let mut model: Vec<ModelEntry> = Vec::new();
    let mut next_cursor = 1u64;
    let mut fresh = 0u64;

    for step in 0..JOURNAL_OPS_PER_ROUND {
        let context = format!("journal seed {seed:#018x} step {step}");
        let roll = rng.below(100);
        if roll < 50 {
            // Fresh intent: unique id, so the append must succeed and take
            // the next cursor.
            fresh += 1;
            let intent = Intent::new(
                format!("j-{seed}-{fresh}"),
                {
        let n = 1 + rng.below(32) as usize;
        rng.bytes(n)
    },
            )
            .expect("generated intents are valid");
            let memory_cursor = strip(memory.append_intent(&intent));
            let fs_cursor = strip(fs.append_intent(&intent));
            assert_eq!(memory_cursor, fs_cursor, "append — {context}");
            let cursor = memory_cursor.expect("a fresh intent always appends");
            assert_eq!(
                cursor,
                Cursor(next_cursor),
                "a fresh intent takes the next cursor — {context}"
            );
            next_cursor += 1;
            model.push(ModelEntry {
                intent,
                cursor,
                commit: None,
            });
        } else if roll < 65 {
            if model.is_empty() {
                continue;
            }
            // Re-append an existing id: identical record (idempotent,
            // original cursor, none consumed) or a conflicting one
            // (Corrupt).
            let index = rng.below(model.len() as u64) as usize;
            let identical = rng.below(2) == 0;
            let mut record = model[index].intent.record().to_vec();
            if !identical {
                record[0] ^= 0xff;
            }
            let intent = Intent::new(model[index].intent.id().to_string(), record)
                .expect("generated intents are valid");
            let memory_cursor = strip(memory.append_intent(&intent));
            let fs_cursor = strip(fs.append_intent(&intent));
            assert_eq!(memory_cursor, fs_cursor, "re-append — {context}");
            if identical {
                assert_eq!(
                    memory_cursor,
                    Ok(model[index].cursor),
                    "idempotent re-append returns the original cursor — {context}"
                );
            } else {
                assert_eq!(
                    memory_cursor,
                    Err(Kind::Corrupt),
                    "conflicting re-append — {context}"
                );
            }
        } else if roll < 95 {
            if model.is_empty() {
                continue;
            }
            // Commit an existing intent: first-time commit (Ok, consumes a
            // cursor), idempotent re-commit (Ok, consumes none), or a
            // conflicting re-commit (Corrupt).
            let index = rng.below(model.len() as u64) as usize;
            let id = model[index].intent.id().to_string();
            let record = match &model[index].commit {
                Some((existing, _)) if rng.below(2) == 0 => existing.clone(),
                Some((existing, _)) => {
                    let mut flipped = existing.clone();
                    flipped[0] ^= 0xff;
                    flipped
                }
                None => {
        let n = 1 + rng.below(32) as usize;
        rng.bytes(n)
    },
            };
            let memory_commit = strip(memory.commit(&id, &record));
            let fs_commit = strip(fs.commit(&id, &record));
            assert_eq!(memory_commit, fs_commit, "commit — {context}");
            match &model[index].commit {
                Some((existing, _)) => {
                    let expected = if *existing == record {
                        Ok(())
                    } else {
                        Err(Kind::Corrupt)
                    };
                    assert_eq!(memory_commit, expected, "re-commit — {context}");
                }
                None => {
                    assert_eq!(memory_commit, Ok(()), "first commit — {context}");
                    model[index].commit = Some((record, Cursor(next_cursor)));
                    next_cursor += 1;
                }
            }
        } else {
            // Committing an unknown id is NotFound.
            let id = format!("ghost-{}", rng.below(16));
            let record = {
        let n = 1 + rng.below(8) as usize;
        rng.bytes(n)
    };
            let memory_commit = strip(memory.commit(&id, &record));
            let fs_commit = strip(fs.commit(&id, &record));
            assert_eq!(memory_commit, fs_commit, "unknown commit — {context}");
            assert_eq!(memory_commit, Err(Kind::NotFound), "unknown commit — {context}");
        }

        if step % 200 == 199 {
            let memory_replay = memory.replay(Cursor(0)).expect("replay");
            let fs_replay = fs.replay(Cursor(0)).expect("replay");
            assert_eq!(memory_replay, fs_replay, "mid-round replay — {context}");
            assert_replay_matches_model(&memory, &model, next_cursor, &context);
        }
    }

    let context = format!("journal seed {seed:#018x} final");
    assert_replay_matches_model(&memory, &model, next_cursor, &context);
    assert_replay_matches_model(&fs, &model, next_cursor, &context);
    let memory_replay = memory.replay(Cursor(0)).expect("replay");
    let fs_replay = fs.replay(Cursor(0)).expect("replay");
    assert_eq!(memory_replay, fs_replay, "final replay — {context}");
    check_suffix_replays(&memory, &memory_replay, &mut rng, next_cursor, &context);
    check_suffix_replays(&fs, &fs_replay, &mut rng, next_cursor, &context);

    // The journal (and its cursor assignment) must survive a reopen.
    drop(fs);
    let fs = FsStore::open(dir.path()).expect("fs store reopens");
    assert_replay_matches_model(&fs, &model, next_cursor, "journal after reopen");
}

#[test]
fn journal_cursors_are_dense_ordered_and_cross_backend_identical() {
    for round in 0..JOURNAL_ROUNDS {
        run_journal_round(0x10A2_0000_0000_0000 | round);
    }
}

// ---------------------------------------------------------------------------
// 4. Digest property: stored bytes always rehash to the recorded digest
// ---------------------------------------------------------------------------

const PAYLOAD_OPS_PER_ROUND: usize = 300;
const PAYLOAD_ROUNDS: u64 = 3;

fn run_payload_round(seed: u64) {
    let mut rng = Rng::new(seed);
    let mut memory = MemoryStore::new();
    let dir = tempfile::tempdir().expect("tempdir");
    let mut fs = FsStore::open(dir.path()).expect("fs store");
    for step in 0..PAYLOAD_OPS_PER_ROUND {
        let context = format!("payload seed {seed:#018x} step {step}");
        let scope = gen_scope(&mut rng, 2);
        let entity = rng.below(64) as u32;
        let payload = {
        let n = rng.below(257) as usize;
        rng.bytes(n)
    };
        let digest = sha256(&payload);
        let streamed = rng.below(2) == 0;
        let chunks = if streamed {
            chunk(&payload, &mut rng)
        } else {
            Vec::new()
        };
        let memory_put = if streamed {
            strip(stream_put(&mut memory, &scope, entity, &chunks, &digest))
        } else {
            strip(memory.put(&scope, entity, &payload, &digest))
        };
        let fs_put = if streamed {
            strip(stream_put(&mut fs, &scope, entity, &chunks, &digest))
        } else {
            strip(fs.put(&scope, entity, &payload, &digest))
        };
        assert_eq!(memory_put, fs_put, "put — {context}");
        if memory_put != Ok(()) {
            // A conflicting re-put of an existing entity; its bytes were
            // verified when they were first stored.
            continue;
        }
        // Corrupt-free by construction: the bytes read back are exactly the
        // payload, so their rehash is exactly the recorded digest.
        let memory_bytes = read_all(&memory, &scope, entity).expect("read back");
        let fs_bytes = read_all(&fs, &scope, entity).expect("read back");
        assert_eq!(memory_bytes, payload, "memory read-back — {context}");
        assert_eq!(fs_bytes, payload, "fs read-back — {context}");
        assert_eq!(
            sha256(&memory_bytes),
            digest,
            "stored bytes rehash to the recorded digest — {context}"
        );
        assert_eq!(
            sha256(&fs_bytes),
            digest,
            "stored bytes rehash to the recorded digest — {context}"
        );

        if step % 40 == 39 {
            // A reader opened before retirement stays valid and yields the
            // original bytes, on both backends.
            let live = memory.list(&scope).expect("oracle list");
            if let Some(&victim) = live.first() {
                let expected = read_all(&memory, &scope, victim).expect("oracle read");
                let mut memory_reader = memory.open_read(&scope, victim).expect("open read");
                let mut fs_reader = fs.open_read(&scope, victim).expect("open read");
                let memory_removed = strip(memory.retire(&scope, u64::MAX));
                let fs_removed = strip(fs.retire(&scope, u64::MAX));
                assert_eq!(memory_removed, fs_removed, "retire — {context}");
                let mut survived_memory = Vec::new();
                memory_reader
                    .read_to_end(&mut survived_memory)
                    .expect("read survives retire");
                let mut survived_fs = Vec::new();
                fs_reader
                    .read_to_end(&mut survived_fs)
                    .expect("read survives retire");
                assert_eq!(survived_memory, expected, "memory read survives — {context}");
                assert_eq!(survived_fs, expected, "fs read survives — {context}");
            }
        }
    }
}

#[test]
fn stored_payloads_rehash_to_their_recorded_digest() {
    for round in 0..PAYLOAD_ROUNDS {
        run_payload_round(0xD1CE_5700_0000_0000 | round);
    }
}
