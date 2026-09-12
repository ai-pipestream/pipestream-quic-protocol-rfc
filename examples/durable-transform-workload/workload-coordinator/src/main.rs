//! External durable-transform coordinator.
//!
//! Drives three worker-authority sessions over the public PipeStream client
//! API: deterministic operation identities, journaled intent replay, bounded
//! watch loops, authenticated result reads, byte-exact ordered reassembly,
//! and idempotent resume after coordinator death. No server-directory reads.
use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use pipestream_quic::{
    persistence::PhysicalLimits,
    v2::*,
    v2_client::{
        journal::{self, Journal},
        session::{self, Client, files::FileInput},
        transport,
    },
};
use rustls::pki_types::pem::PemObject;
use sha2::{Digest as _, Sha256};
use std::{
    fs::OpenOptions,
    io::{Read, Write},
    net::SocketAddr,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use workload_core::{
    chunk_count, chunk_len, expected_chunk, expected_final_digest, generate_chunk,
};

const TRANSFORM_LABEL: &str = "transform/v2";
const CONTENT_TYPE: &str = "application/octet-stream";
const OBJECT_LIMIT: u64 = 16 * 1024 * 1024;
const WORKERS: usize = 3;
/// Max entity IDs in one Declare mutation. The V2 wire bounds every list at
/// 256 entries, and the authority additionally funds a single transaction
/// below that: a 254-entity declare fails with physical-database
/// LIMIT_EXCEEDED while 115 succeeds (conformance durable scenarios), so
/// batches stay at 100 to keep single-transaction fit with margin.
const DECLARE_BATCH: usize = 100;

fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|t| t.as_millis() as u64)
        .unwrap_or(0)
}

/// Deterministic nonzero 16-octet operation ID. Recovery recomputes the same
/// identities instead of inventing new work.
fn operation_id(seed: u64, worker: u64, kind: &str, ordinal: u64) -> OperationId {
    let mut hash = Sha256::new();
    hash.update(b"workload-op-v1");
    hash.update(seed.to_le_bytes());
    hash.update(worker.to_le_bytes());
    hash.update(kind.as_bytes());
    hash.update(ordinal.to_le_bytes());
    let digest: [u8; 32] = hash.finalize().into();
    let mut id: [u8; 16] = digest[..16].try_into().unwrap();
    if id == [0; 16] {
        id[15] = 1;
    }
    OperationId(id)
}

/// Cursor starts for observing a shard's scope membership page by page.
/// Pages hold at most 256 members in ascending entity order, so each
/// cursor after the first is the previous page's last entity (0 starts
/// the walk). An empty shard still observes once, preserving the
/// historical single-page behavior.
fn page_cursors(entity_ids: &[u64]) -> Vec<Number> {
    let mut cursors = Vec::new();
    let mut after = Number(0);
    for batch in entity_ids.chunks(256) {
        cursors.push(after);
        if let Some(last) = batch.last() {
            after = Number(*last);
        }
    }
    if cursors.is_empty() {
        cursors.push(after);
    }
    cursors
}

/// Split a worker shard's ordinals into Declare batches of at most
/// DECLARE_BATCH entity IDs with strictly increasing IDs, sealing only the
/// last batch (sealing earlier would conflict the following batches on the
/// authority). Batch 0 keeps the historical ("declare", 0) identity, so
/// shards that fit in one batch send frame-identical declares. A zero-chunk
/// shard keeps the historical single empty sealed declare.
fn declare_plan(seed: u64, worker: u64, ordinals: &[u64]) -> Vec<(OperationId, Vec<Id>, bool)> {
    let mut batches: Vec<Vec<Id>> = ordinals
        .chunks(DECLARE_BATCH)
        .map(|chunk| chunk.iter().map(|o| Id(o + 1)).collect())
        .collect();
    if batches.is_empty() {
        batches.push(Vec::new());
    }
    let last = batches.len() - 1;
    batches
        .into_iter()
        .enumerate()
        .map(|(b, ids)| {
            (
                operation_id(seed, worker, "declare", b as u64),
                ids,
                b == last,
            )
        })
        .collect()
}

#[cfg(test)]
mod declare_tests {
    use super::*;

    fn cursors(entity_ids: &[u64]) -> Vec<u64> {
        page_cursors(entity_ids).iter().map(|n| n.0).collect()
    }

    #[test]
    fn empty_shard_observes_once_from_zero() {
        assert_eq!(cursors(&[]), vec![0]);
    }

    #[test]
    fn small_shard_observes_once_from_zero() {
        let ids: Vec<u64> = (1..=43).collect();
        assert_eq!(cursors(&ids), vec![0]);
    }

    #[test]
    fn exact_page_bound_observes_once() {
        let ids: Vec<u64> = (1..=256).collect();
        assert_eq!(cursors(&ids), vec![0]);
    }

    #[test]
    fn swap_inputs_exchanges_files() {
        let dir = std::env::temp_dir().join("swap-test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("chunk-000000.bin"), b"aaaa").unwrap();
        std::fs::write(dir.join("chunk-000001.bin"), b"bbbb").unwrap();
        assert_eq!(swap_inputs(&dir, "0:1").unwrap(), (0, 1));
        assert_eq!(std::fs::read(dir.join("chunk-000000.bin")).unwrap(), b"bbbb");
        assert_eq!(std::fs::read(dir.join("chunk-000001.bin")).unwrap(), b"aaaa");
        assert!(swap_inputs(&dir, "0:0").is_err());
        assert!(swap_inputs(&dir, "0:9").is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn oversize_shard_walks_pages_in_order() {
        let ids: Vec<u64> = (1..=300).collect();
        assert_eq!(cursors(&ids), vec![0, 256]);
        // Spaced worker-shard IDs walk the same way.
        let spaced: Vec<u64> = (0..300).filter(|o| o % 3 == 0).map(|o| o + 1).collect();
        assert_eq!(spaced.len(), 100);
        assert_eq!(cursors(&spaced), vec![0]);
        let big: Vec<u64> = (0..1024).filter(|o| o % 3 == 0).map(|o| o + 1).collect();
        assert_eq!(big.len(), 342);
        assert_eq!(cursors(&big), vec![0, big[255]]);
    }

    #[test]
    fn empty_shard_keeps_single_empty_sealed_declare() {
        let plan = declare_plan(6, 0, &[]);
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].0, operation_id(6, 0, "declare", 0));
        assert!(plan[0].1.is_empty());
        assert!(plan[0].2);
    }

    #[test]
    fn single_batch_matches_historical_identity() {
        let ordinals: Vec<u64> = (0..43).collect();
        let plan = declare_plan(6, 1, &ordinals);
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].0, operation_id(6, 1, "declare", 0));
        assert_eq!(plan[0].1.len(), 43);
        assert!(plan[0].2);
    }

    #[test]
    fn exact_bound_stays_single_batch() {
        let ordinals: Vec<u64> = (0..DECLARE_BATCH as u64).collect();
        let plan = declare_plan(6, 0, &ordinals);
        assert_eq!(plan.len(), 1);
        assert!(plan[0].2);
    }

    #[test]
    fn oversize_shard_splits_with_last_only_seal() {
        let ordinals: Vec<u64> = (0..300).collect();
        let plan = declare_plan(6, 2, &ordinals);
        assert_eq!(plan.len(), 3);
        assert_eq!(plan[0].1.len(), DECLARE_BATCH);
        assert!(!plan[0].2);
        assert_eq!(plan[1].1.len(), DECLARE_BATCH);
        assert!(!plan[1].2);
        assert_eq!(plan[2].1.len(), 300 - 2 * DECLARE_BATCH);
        assert!(plan[2].2);
        assert_eq!(plan[0].0, operation_id(6, 2, "declare", 0));
        assert_eq!(plan[2].0, operation_id(6, 2, "declare", 2));
        // IDs strictly increase across batch boundaries.
        assert!(*plan[0].1.last().unwrap() < plan[1].1[0]);
        assert!(*plan[1].1.last().unwrap() < plan[2].1[0]);
        for batch in plan.iter().map(|(_, ids, _)| ids) {
            // Wire bound (256) always holds; storage fit is stricter.
            assert!(batch.len() <= DECLARE_BATCH && batch.len() <= 256);
        }
    }
}

#[derive(Debug, Args)]
struct Tls {
    #[arg(long)]
    ca: PathBuf,
    #[arg(long)]
    cert: PathBuf,
    #[arg(long)]
    key: PathBuf,
    #[arg(long, default_value = "localhost")]
    server_name: String,
}

#[derive(Debug, Args)]
struct Run {
    #[command(flatten)]
    tls: Tls,
    #[arg(long)]
    owner: String,
    #[arg(long)]
    journal_a: PathBuf,
    #[arg(long)]
    journal_b: PathBuf,
    #[arg(long)]
    journal_c: PathBuf,
    #[arg(long)]
    connect_a: SocketAddr,
    #[arg(long)]
    connect_b: SocketAddr,
    #[arg(long)]
    connect_c: SocketAddr,
    #[arg(long, default_value = "workload-a")]
    authority_a: String,
    #[arg(long, default_value = "workload-b")]
    authority_b: String,
    #[arg(long, default_value = "workload-c")]
    authority_c: String,
    #[arg(long, default_value_t = 1)]
    creation_a: u64,
    #[arg(long, default_value_t = 1)]
    creation_b: u64,
    #[arg(long, default_value_t = 1)]
    creation_c: u64,
    #[arg(long)]
    seed: u64,
    #[arg(long)]
    size: u64,
    #[arg(long)]
    staging: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    events: PathBuf,
    #[arg(long, default_value_t = 60000)]
    execution_ms: u64,
    /// Resume an interrupted run instead of starting fresh.
    #[arg(long, default_value_t = false)]
    resume: bool,
    /// TEST-ONLY: swap two materialized input chunk files ("A:B") so the
    /// run uploads them in swapped order (negative-control runs).
    #[arg(long)]
    test_swap_inputs: Option<String>,
    /// TEST-ONLY: remove materialized input chunk N after generation so
    /// the upload fails on a missing chunk (negative-control runs).
    #[arg(long)]
    test_drop_input: Option<u64>,
    /// TEST-ONLY: abort the process right after the first verified result
    /// (RESULT_VERIFIED boundary, fault F4). One-shot via a sentinel file
    /// in staging, so the --resume restart proceeds past it.
    #[arg(long, default_value_t = false)]
    test_kill_after_first_verified: bool,
    /// TEST-ONLY: admit all chunks then stop without fetching (stopped
    /// consumer arm). Arrival rows exist, completion rows must not.
    #[arg(long, default_value_t = false)]
    test_no_fetch: bool,
    /// TEST-ONLY: after admitting everything, hold the consumer still for
    /// this many ms (stopped-consumer arm) before fetching. The stall
    /// interval is logged; authorities hold the results meanwhile.
    #[arg(long, default_value_t = 0)]
    test_fetch_delay_ms: u64,
    /// TEST-ONLY: after selecting an output, stall this many ms before the
    /// first read (stalled-read probe). Whatever the peer does to the
    /// stalled stream (refusal detail or quiet patience) is logged
    /// verbatim in the event stream.
    #[arg(long, default_value_t = 0)]
    test_stall_read_ms: u64,
    /// Admit and verify serially (one chunk at a time), reproducing the
    /// pre-pipelining behavior and numbers. Default (off) pipelines both
    /// phases up to --pending-limit.
    #[arg(long, default_value_t = false)]
    serial: bool,
    /// Coordinator-side cap on in-flight admissions/verifications per
    /// worker session. This is a local concurrency cap, not a negotiated
    /// protocol limit; durability rules are unchanged (journal before
    /// send, validate before journal, one receipt per operation).
    #[arg(long, default_value_t = 16)]
    pending_limit: usize,
}

#[derive(Clone)]
struct Session {
    worker: u64,
    client: Client,
    events: PathBuf,
    started: Instant,
}

impl Session {
    fn log(&self, event: &str, ordinal: i64, detail: &str) {
        let line = format!(
            "{}\t{}\t{}\t{}\t{}\t{}\n",
            self.started.elapsed().as_millis(),
            wall_ms(),
            self.worker,
            ordinal,
            event,
            detail
        );
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.events)
            .expect("events file");
        file.write_all(line.as_bytes()).expect("events write");
    }
}

fn endpoint(tls: &Tls, connect: SocketAddr) -> Result<session::Endpoint> {
    let mut options = transport::Options::default();
    options.offer.object_limit = Number(OBJECT_LIMIT);
    let roots = {
        let bytes = std::fs::read(&tls.ca)?;
        let certs: Vec<_> =
            rustls::pki_types::CertificateDer::pem_slice_iter(&bytes)
                .collect::<std::result::Result<_, _>>()?;
        let mut roots = rustls::RootCertStore::empty();
        for c in certs {
            roots.add(c)?;
        }
        roots
    };
    let certs: Vec<_> = rustls::pki_types::CertificateDer::pem_slice_iter(&std::fs::read(&tls.cert)?)
        .collect::<std::result::Result<_, _>>()?;
    let key = rustls::pki_types::PrivateKeyDer::from_pem_slice(&std::fs::read(&tls.key)?)?;
    Ok(session::Endpoint {
        local: if connect.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        }
        .parse()?,
        remote: connect,
        server_name: tls.server_name.clone(),
        security: transport::Security::new(roots, Some((certs, key)))?,
        transport: options,
    })
}

async fn open_session(
    tls: &Tls,
    owner: &str,
    execution_ms: u64,
    worker: u64,
    authority: &str,
    connect: SocketAddr,
    journal_path: &Path,
    creation_sequence: u64,
    fresh: bool,
    events: &Path,
    started: Instant,
) -> Result<Session> {
    let creation = journal::Creation {
        authority: IdentityLabel(authority.into()),
        owner: IdentityLabel(owner.into()),
        creation_sequence: Id(creation_sequence),
        policy: Policy {
            execution_limit_ms: Duration(execution_ms),
            output_retention_ms: Duration(3_600_000),
            receipt_retention_ms: Duration(86_400_000),
        },
        results: true,
    };
    creation.request(Id(1))?;
    if fresh && journal_path.exists() {
        bail!("journal {} exists; rerun with --resume or a fresh directory", journal_path.display());
    }
    if !fresh && !journal_path.exists() {
        bail!("journal {} missing; nothing to resume", journal_path.display());
    }
    let journal = if fresh {
        Journal::initialize(
            journal_path.to_path_buf(),
            creation,
            journal::JournalLimits::default(),
            PhysicalLimits::default(),
            journal::Options::default(),
        )
        .await?
    } else {
        Journal::open(
            journal_path.to_path_buf(),
            creation,
            journal::JournalLimits::default(),
            PhysicalLimits::default(),
            journal::Options::default(),
        )
        .await?
    };
    // Startup race: the ready-file precedes accept(). Retry briefly rather
    // than failing a healthy run on a transient refusal.
    let mut client = None;
    for _ in 0..60 {
        match Client::connect(endpoint(tls, connect)?, journal.clone(), session::Options::default()).await {
            Ok(c) => {
                client = Some(c);
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(500)).await,
        }
    }
    let client = client.context("coordinator connect timed out")?;
    Ok(Session {
        worker,
        client,
        events: events.into(),
        started,
    })
}

/// Replay journaled-but-unconfirmed intents with their ORIGINAL identities.
/// Admission replay resends the same chunk file under the recomputed
/// declaration; nothing invents a new operation or work identity. Returns
/// the set of replayed operation IDs so the admit loop never resubmits
/// them (and never probes receipts for operations the journal never saw:
// `receipt()` reports unknown operations as an error, not `None`).
async fn replay_unresolved(
    session: &Session,
    staging: &Path,
    ordinals: &[u64],
    declare_ops: &[OperationId],
) -> Result<std::collections::HashSet<[u8; 16]>> {
    let mut replayed = std::collections::HashSet::new();
    let pending = session.client.unresolved(Number(0), PageLimit(256)).await?;
    for (_, intent) in pending {
        replayed.insert(intent.operation.0);
        match &intent.mutation {
            Mutation::Admit(params) => {
                let ordinal = (params.work.entity.0 - 1) as u64;
                let path = chunk_path(staging, ordinal);
                let replay_intent = journal::Intent {
                    operation: intent.operation,
                    mutation: Mutation::Admit(params.clone()),
                };
                let pos = ordinals
                    .iter()
                    .position(|&o| o == ordinal)
                    .context("replayed ordinal not in shard")?;
                send_admission(
                    session,
                    &path,
                    &replay_intent,
                    declare_ops[pos / DECLARE_BATCH],
                    ordinal,
                )
                .await?;
                session.log("replayed-admit", ordinal as i64, "");
            }
            _ => {
                session.client.mutate(intent).await?;
                session.log("replayed-mutation", -1, "");
            }
        }
    }
    Ok(replayed)
}

fn chunk_path(staging: &Path, ordinal: u64) -> PathBuf {
    staging.join(format!("chunk-{ordinal:06}.bin"))
}

fn out_path(staging: &Path, ordinal: u64) -> PathBuf {
    staging.join(format!("out-{ordinal:06}.bin"))
}

fn work_key(ordinal: u64) -> WorkKey {
    WorkKey {
        scope: Number(0),
        producer: Producer(0),
        entity: Id(ordinal + 1),
    }
}

async fn watch_terminal(session: &Session, ordinal: u64) -> Result<(Id, u64)> {
    let key = work_key(ordinal);
    let mut after = Number(0);
    loop {
        let observed = session.client.watch(key.clone(), after, WaitMs(10_000)).await?;
        after = Number(observed.revision.0);
        let state = observed.view.state.0;
        if (5..=8).contains(&state) {
            if state != 5 {
                bail!(
                    "chunk {ordinal} terminal state {state} (attempt {})",
                    observed.view.attempt.0
                );
            }
            return Ok((Id(observed.view.attempt.0), observed.revision.0));
        }
    }
}

/// Select, read and stage one chunk's output (transfer only: no oracle
/// check, no completion rows). Retried as backpressure by fetch_one; the
/// oracle comparison afterwards always fails fast.
async fn fetch_transfer(
    session: &Session,
    seed: u64,
    total: u64,
    ordinal: u64,
    attempt: Id,
    staging: &Path,
    stall_read_ms: u64,
) -> Result<bool> {
    let key = work_key(ordinal);
    let expected = expected_chunk(seed, total, ordinal);
    let path = out_path(staging, ordinal);
    if path.exists() {
        let bytes = std::fs::read(&path)?;
        if bytes == expected {
            return Ok(true);
        }
        std::fs::remove_file(&path)?;
    }
    session.client.select_output(key.clone(), attempt, OutputIndex(0)).await?;
    if stall_read_ms > 0 {
        session.log(
            "stalled-read",
            ordinal as i64,
            &format!("selected, holding first read {stall_read_ms} ms"),
        );
        eprintln!("TEST-ONLY test-stall-read-ms: holding first read {stall_read_ms} ms");
        tokio::time::sleep(std::time::Duration::from_millis(stall_read_ms)).await;
    }
    let saved = session
        .client
        .read_output(key, attempt, OutputIndex(0))
        .await
        .map_err(anyhow::Error::from)
        .with_context(|| format!("stalled read ordinal {ordinal}"))?
        .save_to(path.clone(), OBJECT_LIMIT)
        .await?;
    let _ = saved;
    let _ = saved;
    Ok(false)
}

/// Bounded backpressure retry for one fallible fetch step: transport and
/// authority backpressure (same classifier as admission) retry under a
/// 240-attempt budget; anything else, including a dead worker's cancelled
/// watch (the F3 signal), fails fast.
async fn fetch_retry<F, Fut, T>(session: &Session, ordinal: u64, mut step: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut wait_ms = 100u64;
    for attempt in 0..240u32 {
        match step().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                let Some(label) = e
                    .downcast_ref::<session::Failure>()
                    .and_then(backpressure_label)
                else {
                    return Err(e);
                };
                if attempt + 1 >= 240 {
                    session.log("fetch-budget-out", ordinal as i64, &e.to_string());
                    return Err(e);
                }
                session.log(label, ordinal as i64, &e.to_string());
                tokio::time::sleep(std::time::Duration::from_millis(wait_ms)).await;
                wait_ms = (wait_ms * 2).min(5_000);
            }
        }
    }
    unreachable!("fetch retry loop always returns");
}

/// Admit one chunk, treating capacity refusals as backpressure.
///
/// A LIMIT_EXCEEDED refusal means the authority is healthy but full; a
/// NOT_READY refusal means it is not ready yet; our own client's
/// unresolved-admission ceiling means we over-admitted against the peer's
/// stream budget. In all three cases the journaled intent is unchanged, so
/// resending the SAME operation identity is safe and required for durable
/// progress (admission is idempotent on prior receipt). Every backpressure
/// event is recorded in the event stream with its named code; any other
/// error fails fast. Bounded: 240 attempts, 100 ms doubling to 5 s (about
/// 10 minutes worst case).
async fn send_admission(
    session: &Session,
    chunk: &Path,
    intent: &journal::Intent,
    declaration: OperationId,
    ordinal: u64,
) -> Result<()> {
    const MAX_ATTEMPTS: u32 = 240;
    let mut wait_ms = 100u64;
    for attempt in 0..MAX_ATTEMPTS {
        let outcome = FileInput::open(chunk.to_path_buf(), OBJECT_LIMIT)
            .await?
            .send(session.client.clone(), intent.clone(), declaration)
            .await;
        match outcome {
            Ok(_) => {
                if attempt > 0 {
                    session.log(
                        "admit-retried",
                        ordinal as i64,
                        &format!("succeeded after {attempt} refusals"),
                    );
                }
                return Ok(());
            }
            Err(e) => {
                let Some(label) = backpressure_label(&e) else {
                    session.log("admit-fatal-send", ordinal as i64, &format!("{e:?}"));
                    return Err(e.into());
                };
                session.log(label, ordinal as i64, &e.to_string());
                if attempt + 1 >= MAX_ATTEMPTS {
                    session.log("admit-budget-out-send", ordinal as i64, &e.to_string());
                    return Err(e.into());
                }
                tokio::time::sleep(std::time::Duration::from_millis(wait_ms)).await;
                wait_ms = (wait_ms * 2).min(5_000);
            }
        }
    }
    unreachable!("admit retry loop always returns");
}

/// Classify an admission-path error: Some(label) if it is retryable
/// backpressure under the same journaled identity, None if fatal.
/// Authority full (LIMIT_EXCEEDED), authority not ready yet (NOT_READY),
/// and our own client's local ceilings (unresolved admissions, outgoing
/// streams: LimitExceeded at the transport means too much in flight) all
/// retry; anything else fails fast. The verbatim detail stays on the row,
/// so each trigger remains distinguishable in the event stream.
fn backpressure_label(e: &session::Failure) -> Option<&'static str> {
    match e {
        session::Failure::Refused(r) if r.code == ErrorCode::LimitExceeded => Some("admit-refused"),
        session::Failure::Refused(r) if r.code == ErrorCode::NotReady => Some("admit-notready"),
        session::Failure::Protocol(p) if p.code == ErrorCode::LimitExceeded => Some("admit-ceiling"),
        _ => None,
    }
}

/// Admit one chunk under its frozen identity (terminal work and
/// just-replayed operations are skipped silently, as in the serial loop).
/// Shared by the serial and pipelined paths so both execute the same
/// durable sequence; the admit latency is recorded on the event row.
async fn admit_one(
    session: &Session,
    seed: u64,
    total: u64,
    worker: u64,
    execution_ms: u64,
    staging: &Path,
    declaration: OperationId,
    replayed: &std::collections::HashSet<[u8; 16]>,
    ordinal: u64,
) -> Result<()> {
    // The terminal-state probe touches authority metadata too, so under
    // pipelined concurrency it meets the same backpressure as the send;
    // retry it under the same identity and budget (240 attempts).
    let admitted = {
        let mut wait_ms = 100u64;
        let mut attempt = 0u32;
        loop {
            match session.client.observed_work(work_key(ordinal)).await {
                Ok(o) => break o,
                Err(e) => {
                    let Some(label) = backpressure_label(&e) else {
                        session.log("admit-fatal-probe", ordinal as i64, &format!("{e:?}"));
                        return Err(e.into());
                    };
                    attempt += 1;
                    if attempt >= 240 {
                        session.log("admit-budget-out-probe", ordinal as i64, &e.to_string());
                        return Err(e.into());
                    }
                    session.log(label, ordinal as i64, &e.to_string());
                    tokio::time::sleep(std::time::Duration::from_millis(wait_ms)).await;
                    wait_ms = (wait_ms * 2).min(5_000);
                }
            }
        }
    };
    let terminal = admitted.map(|o| (5..=8).contains(&o.view.state.0)).unwrap_or(false);
    if terminal {
        return Ok(());
    }
    let operation = operation_id(seed, worker, "admit", ordinal);
    if replayed.contains(&operation.0) {
        return Ok(());
    }
    let len = chunk_len(total, ordinal);
    let intent = journal::Intent {
        operation,
        mutation: Mutation::Admit(AdmitParameters {
            work: work_key(ordinal),
            input: Input {
                length: Number(len as u64),
                sha256: Digest(blake_of_chunk(seed, total, ordinal)),
                content_type: ApplicationLabel(CONTENT_TYPE.into()),
            },
            application: ApplicationLabel(TRANSFORM_LABEL.into()),
            mode: Mode(0),
            execution_ms: Duration(execution_ms),
            outputs: OutputBudget {
                count: BatchCount(1),
                total_bytes: Number(len as u64),
            },
        }),
    };
    let t = Instant::now();
    send_admission(session, &chunk_path(staging, ordinal), &intent, declaration, ordinal).await?;
    session.log("admitted", ordinal as i64, &format!("admit={}ms", t.elapsed().as_millis()));
    Ok(())
}

/// Watch, fetch and verify one chunk, then apply the F4 one-shot hook when
/// armed. Shared by the serial and pipelined paths. first_usable fires once
/// per session (atomically: in pipelined mode the first completer wins).
async fn fetch_one(
    session: &Session,
    seed: u64,
    total: u64,
    ordinal: u64,
    staging: &Path,
    stall_read_ms: u64,
    kill_after_verified: bool,
    first_usable: &std::sync::atomic::AtomicBool,
) -> Result<()> {
    use std::sync::atomic::Ordering;
    // Watch for terminal state. Backpressure retries; a dead worker's
    // cancelled watch (the F3 signal) fails fast.
    let t = Instant::now();
    let (attempt, _) = fetch_retry(session, ordinal, || async {
        watch_terminal(session, ordinal).await
    })
    .await?;
    session.log("succeeded", ordinal as i64, &format!("attempt {} watch={}ms", attempt.0, t.elapsed().as_millis()));
    // Transfer the output. Backpressure retries; anything else fails fast.
    let t = Instant::now();
    let shortcut = fetch_retry(session, ordinal, || async {
        fetch_transfer(session, seed, total, ordinal, attempt, staging, stall_read_ms).await
    })
    .await?;
    if shortcut {
        // Resume shortcut (byte-verified staged output): no completion
        // rows, no hook, exactly as before.
        return Ok(());
    }
    // Oracle comparison: always fail-fast, never retried. A mismatch here
    // is corruption, not backpressure.
    let expected = expected_chunk(seed, total, ordinal);
    let bytes = std::fs::read(out_path(staging, ordinal))?;
    if bytes != expected {
        bail!("chunk {ordinal} failed byte verification after verified transfer");
    }
    if !first_usable.swap(true, Ordering::SeqCst) {
        session.log("first-usable-output", ordinal as i64, "");
    }
    session.log("chunk-verified", ordinal as i64, &format!("fetch={}ms", t.elapsed().as_millis()));
    // TEST-ONLY F4 hook: abort at the first RESULT_VERIFIED boundary.
    // The chunk-verified row above proves the boundary was reached.
    // One-shot via sentinel so the --resume restart proceeds past it.
    if kill_after_verified {
        let sentinel = staging.join("kill-f4-fired");
        if !sentinel.exists() {
            std::fs::write(&sentinel, b"fired")?;
            eprintln!("TEST-ONLY test-kill-after-first-verified: aborting");
            std::process::abort();
        }
    }
    Ok(())
}

fn sync_dir(path: &Path) -> Result<()> {
    let dir = OpenOptions::new().read(true).open(path)?;
    dir.sync_all()?;
    Ok(())
}

async fn run_session(
    run: &Run,
    worker: u64,
    authority: &str,
    connect: SocketAddr,
    journal_path: &Path,
    creation_sequence: u64,
    seed: u64,
    total: u64,
    staging: &Path,
    events: &Path,
    started: Instant,
) -> Result<()> {
    let ordinals: Vec<u64> = (0..chunk_count(total))
        .filter(|o| o % WORKERS as u64 == worker)
        .collect();
    let fresh = !run.resume;
    let session = open_session(
        &run.tls, &run.owner, run.execution_ms, worker, authority, connect,
        journal_path, creation_sequence, fresh, events, started,
    )
    .await?;
    session.log("session-open", -1, authority);
    // Shard membership is declared in increasing batches of at most
    // DECLARE_BATCH entities: a larger entity_ids list violates the V2
    // 256-entry bound and is rejected before it reaches the wire. Batch 0
    // keeps the historical ("declare", 0) identity, so shards that fit in
    // one batch send frame-identical declares. Only the last batch seals
    // the scope: sealing earlier would conflict the following batches.
    // Declare (fresh runs only) then replay anything the journal still
    // holds as uncertain.
    let plan = declare_plan(seed, worker, &ordinals);
    if fresh {
        for (b, (op, ids, seal)) in plan.iter().enumerate() {
            session
                .client
                .mutate(journal::Intent {
                    operation: *op,
                    mutation: Mutation::Declare {
                        scope: Number(0),
                        entity_ids: ids.clone(),
                        seal: *seal,
                    },
                })
                .await?;
            session.log(
                "declared",
                -1,
                &format!("batch {b}: {} entities", ids.len()),
            );
        }
    }
    let declare_ops: Vec<OperationId> = plan.iter().map(|(op, _, _)| *op).collect();
    let replayed = replay_unresolved(&session, staging, &ordinals, &declare_ops).await?;
    // Admit every chunk of this shard under its frozen identity, except
    // terminal work and just-replayed operations. Pipelined by default
    // (up to pending-limit in flight); --serial keeps the old order.
    // Every admission journals its intent before sending either way.
    if run.serial {
        for (pos, &ordinal) in ordinals.iter().enumerate() {
            admit_one(&session, seed, total, worker, run.execution_ms, staging, declare_ops[pos / DECLARE_BATCH], &replayed, ordinal).await?;
        }
    } else {
        let execution_ms = run.execution_ms;
        let limit = run.pending_limit.max(1);
        let mut set = tokio::task::JoinSet::new();
        for (pos, &ordinal) in ordinals.iter().enumerate() {
            while set.len() >= limit {
                if let Some(r) = set.join_next().await {
                    r??;
                }
            }
            let s = session.clone();
            let st = staging.to_path_buf();
            let rp = replayed.clone();
            let declaration = declare_ops[pos / DECLARE_BATCH];
            set.spawn(async move {
                admit_one(&s, seed, total, worker, execution_ms, &st, declaration, &rp, ordinal).await
            });
        }
        while let Some(r) = set.join_next().await {
            r??;
        }
    }
    // Watch, fetch, and verify each chunk (skipped for stopped-consumer).
    if run.test_no_fetch {
        session.log("consumer-stopped", -1, "admitted, not fetching");
        eprintln!("TEST-ONLY test-no-fetch: admitted, not fetching");
        return Ok(());
    }
    // TEST-ONLY stopped consumer: everything is admitted and the
    // authorities hold the results; hold still for the stated interval
    // (logged) before the first fetch, then complete normally.
    if run.test_fetch_delay_ms > 0 {
        session.log(
            "consumer-stopped",
            -1,
            &format!("admitted, holding fetch {} ms", run.test_fetch_delay_ms),
        );
        eprintln!(
            "TEST-ONLY test-fetch-delay-ms: admitted, holding fetch {} ms",
            run.test_fetch_delay_ms
        );
        tokio::time::sleep(std::time::Duration::from_millis(run.test_fetch_delay_ms)).await;
        session.log("consumer-resumed", -1, "fetching after hold");
    }
    let first_usable = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    if run.serial {
        for &ordinal in &ordinals {
            fetch_one(&session, seed, total, ordinal, staging, run.test_stall_read_ms, run.test_kill_after_first_verified, first_usable.as_ref()).await?;
        }
    } else {
        let mut set = tokio::task::JoinSet::new();
        for &ordinal in &ordinals {
            while set.len() >= run.pending_limit.max(1) {
                if let Some(r) = set.join_next().await {
                    r??;
                }
            }
            let s = session.clone();
            let st = staging.to_path_buf();
            let stall = run.test_stall_read_ms;
            let kill = run.test_kill_after_first_verified;
            let fu = first_usable.clone();
            set.spawn(async move {
                fetch_one(&s, seed, total, ordinal, &st, stall, kill, fu.as_ref()).await
            });
        }
        while let Some(r) = set.join_next().await {
            r??;
        }
    }
    // Checkpoint the sealed root scope, then assert durable completion.
    // Membership is observed page by page: one 256-member page cannot
    // cover a larger shard, and closure requires every member observed
    // before the seal verifies.
    let entity_ids: Vec<u64> = ordinals.iter().map(|o| o + 1).collect();
    let mut seal = None;
    let mut pages = 0u32;
    for after in page_cursors(&entity_ids) {
        let page = session
            .client
            .scope_page(Number(0), after, PageLimit(256))
            .await?;
        pages += 1;
        if seal.is_none() {
            seal = page.seal;
        }
    }
    session.log("scope-pages", -1, &format!("{} pages", pages));
    let seal = seal.context("root scope has no committed seal")?;
    let summary = session.client.checkpoint(Number(0), seal, WaitMs(30_000)).await?;
    session.log("checkpoint", -1, &format!("declared {}", summary.declared.0));
    let completed = session.client.complete().await?;
    session.log("complete", -1, &format!("declared {}", completed.declared.0));
    // Durable commits behind this worker: one admission commit plus one
    // terminal-outcome commit per chunk; the chunk count bounds the total.
    session.log("commits", -1, &format!("{} chunks", ordinals.len()));
    session.client.shutdown().await?;
    session.log("session-close", -1, "");
    Ok(())
}

fn blake_of_chunk(seed: u64, total: u64, ordinal: u64) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(generate_chunk(seed, ordinal, chunk_len(total, ordinal)));
    hash.finalize().into()
}

/// CR13 idle/lifetime probe: script partial object uploads and assert the
/// client-side stream deadlines fire. `idle` stalls all payload progress
/// while issuing varied unrelated wire requests (with a 3 s boundary
/// control proving the stream was alive first); `lifetime` makes slow
/// continuous progress past the absolute stream lifetime; `complete`
/// finishes valid transfers inside both caps as the non-vacuity control.
/// Deadline arms PASS only when the post-stall payload step fails inside
/// the expected time window with a deadline-adjacent code, which
/// distinguishes the deadline from instant integrity errors (wrong window)
/// and unrelated cancellation (wrong code or window). Scoped claim: this
/// exercises the Rust client's upload path against the connected worker;
/// server-side enforcement and the download direction are NOT covered.
#[derive(Debug, Args)]
struct Probe {
    #[command(flatten)]
    tls: Tls,
    #[arg(long)]
    owner: String,
    #[arg(long)]
    authority: String,
    #[arg(long)]
    connect: SocketAddr,
    #[arg(long)]
    journal: PathBuf,
    #[arg(long, default_value_t = 1)]
    creation_sequence: u64,
    #[arg(long)]
    seed: u64,
    /// "idle", "lifetime", or "complete".
    #[arg(long)]
    arm: String,
    #[arg(long)]
    events: PathBuf,
    #[arg(long, default_value_t = 60000)]
    execution_ms: u64,
}

#[derive(Debug, Subcommand)]
enum Command {
    Run(Run),
    Probe(Probe),
}

#[derive(Debug, Parser)]
#[command(about = "Durable-transform coordinator over public PipeStream client APIs")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// Materialize deterministic input chunk files. Idempotent: an existing file
/// with matching length and hash is kept, anything else is replaced.
fn materialize_chunks(seed: u64, total: u64, staging: &Path) -> Result<()> {
    std::fs::create_dir_all(staging)?;
    for ordinal in 0..chunk_count(total) {
        let path = chunk_path(staging, ordinal);
        let expected = generate_chunk(seed, ordinal, chunk_len(total, ordinal));
        let reuse = std::fs::read(&path).map(|b| b == expected).unwrap_or(false);
        if !reuse {
            std::fs::write(&path, &expected)?;
        }
    }
    sync_dir(staging)?;
    Ok(())
}

/// TEST-ONLY: swap two materialized input files ("A:B") so the run uploads
/// them in swapped order. Both files must exist and differ.
fn swap_inputs(staging: &Path, spec: &str) -> Result<(u64, u64)> {
    let (a, b) = spec.split_once(':').context("test-swap-inputs needs A:B")?;
    let (a, b): (u64, u64) = (a.parse().context("bad swap A")?, b.parse().context("bad swap B")?);
    if a == b {
        bail!("test-swap-inputs needs distinct ordinals");
    }
    let pa = chunk_path(staging, a);
    let pb = chunk_path(staging, b);
    let ba = std::fs::read(&pa).with_context(|| format!("swap input {a} missing"))?;
    let bb = std::fs::read(&pb).with_context(|| format!("swap input {b} missing"))?;
    if ba == bb {
        bail!("test-swap-inputs needs inputs that differ");
    }
    std::fs::write(&pa, &bb)?;
    std::fs::write(&pb, &ba)?;
    sync_dir(staging)?;
    Ok((a, b))
}

async fn assemble_final(seed: u64, total: u64, staging: &Path, output: &Path, events: &Path) -> Result<()> {
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = output.with_extension("partial");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&tmp)?;
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 65536];
    for ordinal in 0..chunk_count(total) {
        let mut chunk = std::fs::File::open(out_path(staging, ordinal))?;
        loop {
            let n = chunk.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            file.write_all(&buffer[..n])?;
            hash.update(&buffer[..n]);
        }
    }
    file.sync_all()?;
    drop(file);
    let digest: [u8; 32] = hash.finalize().into();
    if digest != expected_final_digest(seed, total) {
        bail!("final object digest mismatch against independent oracle");
    }
    std::fs::rename(&tmp, output)?;
    sync_dir(output.parent().unwrap_or(Path::new(".")))?;
    let line = format!(
        "{}\t{}\t-\t-\tfinal-verified\t{} bytes\n",
        0,
        wall_ms(),
        total
    );
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(events)?
        .write_all(line.as_bytes())?;
    Ok(())
}

/// Assert a stalled upload stream died AT the deadline. The client enforces
/// the idle and absolute stream deadlines inside its upload task (`bounded`
/// in `v2_client::transport::objects`); by the time a post-stall payload
/// step runs, the stream is already reset, so the step surfaces whatever
/// post-mortem error the writer reports rather than the deadline itself.
/// The fingerprint is therefore code family AND time window AND session
/// survival: the failure must carry a deadline-adjacent code (LimitExceeded
/// or Cancelled — the two this stack surfaces around stream death;
/// integrity, conflict, authorization and not-found codes are explicitly
/// rejected), must land inside [floor, ceiling] measured from the given
/// anchor instant, and the session itself must still serve reads
/// afterwards. That last check is what rejects unrelated cancellation: a
/// session-wide shutdown or connection loss fails the survival read, so it
/// can never earn PASS no matter how well-timed. An instant integrity
/// error fails the floor. Timestamps are first-observation times, not
/// termination instants: the stream died somewhere in
/// (last_alive_ms, dead_ms], with the cap strictly inside. Follow-up for
/// the transport owners: post-deadline writes report Cancelled instead of
/// the deadline that killed the stream, and exact death instants are not
/// externally observable.
async fn assert_deadline_fired(
    session: &Session,
    result: &std::result::Result<(), session::Failure>,
    anchor: Instant,
    alive_ms: u64,
    floor_ms: u64,
    ceiling_ms: u64,
    what: &str,
) -> Result<()> {
    let elapsed_ms = anchor.elapsed().as_millis() as u64;
    match result {
        Err(session::Failure::Protocol(e))
            if e.code == ErrorCode::LimitExceeded || e.code == ErrorCode::Cancelled =>
        {
            session.log(
                "probe-stream-dead",
                -1,
                &format!("{what}: {e:?} alive_at={alive_ms}ms dead_at={elapsed_ms}ms"),
            );
            if elapsed_ms < floor_ms || elapsed_ms > ceiling_ms {
                bail!(
                    "{what}: failure at {elapsed_ms}ms outside deadline window [{floor_ms},{ceiling_ms}]ms"
                );
            }
            // Stream-scoped death only: the session must still serve reads.
            session
                .client
                .scope_page(Number(0), Number(0), PageLimit(16))
                .await
                .map(|_| ())
                .map_err(|e| {
                    anyhow::anyhow!(
                        "{what}: session did not survive the stream death ({e:?}); refusing to attribute it to the deadline"
                    )
                })?;
            session.log("probe-session-survived", -1, what);
            Ok(())
        }
        other => bail!("{what}: expected deadline-adjacent failure, got {other:?}"),
    }
}

fn probe_params(
    seed: u64,
    worker: u64,
    arm: &str,
    entity: u64,
    bytes: &[u8],
) -> journal::Intent {
    let mut hash = Sha256::new();
    hash.update(bytes);
    let digest: [u8; 32] = hash.finalize().into();
    journal::Intent {
        operation: operation_id(seed, worker, arm, entity),
        mutation: Mutation::Admit(AdmitParameters {
            work: WorkKey {
                scope: Number(0),
                producer: Producer(0),
                entity: Id(entity),
            },
            input: Input {
                length: Number(bytes.len() as u64),
                sha256: Digest(digest),
                content_type: ApplicationLabel(CONTENT_TYPE.into()),
            },
            application: ApplicationLabel(TRANSFORM_LABEL.into()),
            mode: Mode(0),
            execution_ms: Duration(60_000),
            outputs: OutputBudget {
                count: BatchCount(1),
                total_bytes: Number(bytes.len() as u64),
            },
        }),
    }
}

/// Declare a per-arm entity pair, fully admit the control entity as the
/// stall-traffic watch target (proving the watch path against really
/// admitted work), and open a partial upload on the probe entity. Entities
/// are partitioned per arm (`base`, `base`+1) because the worker remembers
/// admitted-but-incomplete work across arms. Returns the probe handle, the
/// full deterministic content (only a prefix is ever written: the stream
/// must die by deadline before FIN, so no digest is ever verified on it),
/// and the control entity id for stall-traffic watches.
async fn open_probe_upload(
    session: &Session,
    seed: u64,
    worker: u64,
    arm: &str,
    base: u64,
) -> Result<(session::Admission, Vec<u8>, u64)> {
    let declaration = operation_id(seed, worker, "probe-declare", base);
    session
        .client
        .mutate(journal::Intent {
            operation: declaration,
            mutation: Mutation::Declare {
                scope: Number(0),
                entity_ids: vec![Id(base), Id(base + 1)],
                seal: true,
            },
        })
        .await?;
    session.log("probe-declared", -1, arm);
    let content = generate_chunk(seed, 424242, 1024);
    // Control entity goes through the full commit path first, so stall
    // traffic watches really admitted work.
    let mut control = session
        .client
        .input(
            probe_params(seed, worker, arm, base + 1, &content[..8]),
            declaration,
        )
        .await?;
    control.write(&content[..8]).await?;
    control.finish().await?;
    control.receipt().await?;
    session.log("probe-control-committed", -1, arm);
    let intent = probe_params(seed, worker, arm, base, &content);
    let admission = session.client.input(intent, declaration).await?;
    Ok((admission, content, base + 1))
}

/// Varied unrelated WIRE traffic: scope pages and watch polls on the
/// declared entity, alternating. All read-only, all real round trips, none
/// of them payload on any object stream, so none may renew object idle.
/// Every response is validated (not merely awaited): traffic that fails
/// voids the "despite traffic" claim, so any error fails the arm. Note:
/// `observed_work` is deliberately NOT used here — it serves from the
/// local client journal, not the wire, and claiming it as wire traffic
/// would be false.
async fn unrelated_traffic(
    session: &Session,
    ordinal: u64,
    watch_entity: u64,
) -> Result<()> {
    if ordinal % 2 == 0 {
        let page = session
            .client
            .scope_page(Number(0), Number(0), PageLimit(16))
            .await?;
        session.log(
            "probe-traffic",
            ordinal as i64,
            &format!("scope-page declared={}", page.declared.0),
        );
    } else {
        let observed = session
            .client
            .watch(
                WorkKey {
                    scope: Number(0),
                    producer: Producer(0),
                    entity: Id(watch_entity),
                },
                Number(0),
                WaitMs(100),
            )
            .await?;
        session.log(
            "probe-traffic",
            ordinal as i64,
            &format!("watch state={}", observed.view.state.0),
        );
    }
    Ok(())
}

async fn probe_idle(session: &Session, seed: u64, worker: u64) -> Result<()> {
    let (mut admission, content, control) = open_probe_upload(session, seed, worker, "probe-idle", 1).await?;
    admission.write(&content[..16]).await?;
    // Boundary control: a 3 s stall must NOT kill the stream (idle cap is
    // 5 s). This write must succeed, proving liveness before the real stall.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    admission.write(&content[16..32]).await?;
    let alive_ms = session.started.elapsed().as_millis() as u64;
    session.log("probe-alive", -1, "idle arm: stream alive after 3 s stall");
    // Real stall: 7 s of varied unrelated wire requests that must NOT
    // renew object idle.
    session.log("probe-stalled", -1, "idle arm: payload stopped, mixed control traffic continues");
    let anchor = Instant::now();
    for i in 0..14 {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        unrelated_traffic(session, i, control).await?;
    }
    // Idle cap (5 s from the last payload progress) has passed: the next
    // payload step must fail inside the deadline window.
    let outcome = match admission.write(&content[32..48]).await {
        Err(e) => Err(e),
        Ok(()) => admission.finish().await.map(|_| ()),
    };
    let _ = admission.abort().await;
    session.log("probe-deadline-observed", -1, "idle");
    assert_deadline_fired(session, &outcome, anchor, alive_ms, 4_000, 40_000, "idle").await
}

async fn probe_lifetime(session: &Session, seed: u64, worker: u64) -> Result<()> {
    let (mut admission, content, _) = open_probe_upload(session, seed, worker, "probe-lifetime", 3).await?;
    // Slow continuous progress (8 bytes / 2 s) past the 30 s absolute
    // lifetime: progress must NOT extend it.
    let anchor = Instant::now();
    let alive_ms = session.started.elapsed().as_millis() as u64;
    let mut outcome: std::result::Result<(), session::Failure> = Ok(());
    for i in 0..17 {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let off = (i * 8) % (content.len() - 8);
        match admission.write(&content[off..off + 8]).await {
            Ok(()) => session.log("probe-progress", i as i64, "lifetime arm"),
            Err(e) => {
                outcome = Err(e);
                break;
            }
        }
    }
    if outcome.is_ok() {
        outcome = admission.finish().await.map(|_| ());
    }
    let _ = admission.abort().await;
    session.log("probe-deadline-observed", -1, "lifetime");
    assert_deadline_fired(session, &outcome, anchor, alive_ms, 25_000, 120_000, "lifetime").await
}

/// Negative control: inject an unrelated session-wide cancellation
/// mid-stall and require the predicate to REJECT the resulting death.
/// Shutdown at ~3 s, probe at ~8 s: code (Cancelled) and window both look
/// deadline-like, so the old code+window predicate would PASS — only the
/// session-survival check rejects it. If this arm's death ever earns PASS,
/// the predicate has a hole. PASS here means correct rejection.
async fn probe_cancel_neg(session: &Session, seed: u64, worker: u64) -> Result<()> {
    let (mut admission, content, _) = open_probe_upload(session, seed, worker, "probe-cancel-neg", 7).await?;
    admission.write(&content[..16]).await?;
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    admission.write(&content[16..32]).await?;
    let alive_ms = session.started.elapsed().as_millis() as u64;
    session.log("probe-alive", -1, "cancel-neg arm: stream alive after 3 s stall");
    // Unrelated cancellation, not a deadline: shut the whole client down.
    session.client.shutdown().await?;
    session.log("probe-injected-shutdown", -1, "cancel-neg arm: session shut down mid-stall");
    let anchor = Instant::now();
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    let outcome = match admission.write(&content[32..48]).await {
        Err(e) => Err(e),
        Ok(()) => admission.finish().await.map(|_| ()),
    };
    let _ = admission.abort().await;
    match assert_deadline_fired(session, &outcome, anchor, alive_ms, 4_000, 40_000, "cancel-neg").await {
        Ok(()) => bail!("cancel-neg: injected unrelated cancellation earned PASS"),
        Err(_) => {
            session.log("probe-cancel-rejected", -1, "unrelated cancellation correctly rejected");
            println!("PROBE PASS: cancel-neg correctly rejected");
            Ok(())
        }
    }
}

/// Non-vacuity control: a transfer that stays inside both caps must
/// commit cleanly. Progress over ~6 s also positively shows payload
/// activity renewing idle (the complement of the idle arm).
async fn probe_complete(session: &Session, seed: u64, worker: u64) -> Result<()> {
    let (mut admission, content, _) = open_probe_upload(session, seed, worker, "probe-complete", 5).await?;
    for i in 0..4 {
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        let off = i * 256;
        admission.write(&content[off..off + 256]).await?;
        session.log("probe-progress", i as i64, "complete arm");
    }
    admission.finish().await?;
    let receipt = admission.receipt().await?;
    session.log(
        "probe-complete",
        -1,
        &format!("committed operation {:02x?}", receipt.operation.0),
    );
    Ok(())
}

async fn run_probe(p: &Probe) -> Result<()> {
    let started = Instant::now();
    let session = open_session(
        &p.tls, &p.owner, p.execution_ms, 0, &p.authority, p.connect,
        &p.journal, p.creation_sequence, true, &p.events, started,
    )
    .await?;
    session.log("probe-arm-start", -1, &p.arm);
    match p.arm.as_str() {
        "idle" => probe_idle(&session, p.seed, 0).await?,
        "lifetime" => probe_lifetime(&session, p.seed, 0).await?,
        "complete" => probe_complete(&session, p.seed, 0).await?,
        "cancel-neg" => probe_cancel_neg(&session, p.seed, 0).await?,
        other => bail!("unknown probe arm: {other}"),
    }
    session.client.shutdown().await?;
    session.log("probe-pass", -1, &p.arm);
    println!("PROBE PASS: {} deadline observed", p.arm);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let command = Cli::parse().command;
    if let Command::Probe(p) = command {
        return run_probe(&p).await;
    }
    let Command::Run(run) = command else {
        bail!("unreachable: command dispatch exhausted")
    };
    let started = Instant::now();
    let staging = run.staging.clone();
    materialize_chunks(run.seed, run.size, &staging)?;
    if let Some(spec) = &run.test_swap_inputs {
        let (a, b) = swap_inputs(&staging, spec)?;
        eprintln!("TEST-ONLY test-swap-inputs enabled: chunk {a} and {b} exchanged");
    }
    if let Some(n) = run.test_drop_input {
        let path = chunk_path(&staging, n);
        std::fs::remove_file(&path)
            .with_context(|| format!("test-drop-input chunk {n} missing already"))?;
        sync_dir(&staging)?;
        eprintln!("TEST-ONLY test-drop-input enabled: chunk {n} removed");
    }
    let workers = [
        (0u64, run.authority_a.clone(), run.connect_a, run.journal_a.clone(), run.creation_a),
        (1u64, run.authority_b.clone(), run.connect_b, run.journal_b.clone(), run.creation_b),
        (2u64, run.authority_c.clone(), run.connect_c, run.journal_c.clone(), run.creation_c),
    ];
    // Sessions are independent authorities; drive them concurrently.
    let mut handles = Vec::new();
    for (worker, authority, connect, journal, sequence) in workers {
        let run = Run {
            tls: Tls {
                ca: run.tls.ca.clone(),
                cert: run.tls.cert.clone(),
                key: run.tls.key.clone(),
                server_name: run.tls.server_name.clone(),
            },
            owner: run.owner.clone(),
            journal_a: PathBuf::new(),
            journal_b: PathBuf::new(),
            journal_c: PathBuf::new(),
            connect_a: connect,
            connect_b: connect,
            connect_c: connect,
            authority_a: String::new(),
            authority_b: String::new(),
            authority_c: String::new(),
            creation_a: 0,
            creation_b: 0,
            creation_c: 0,
            seed: run.seed,
            size: run.size,
            staging: staging.clone(),
            output: run.output.clone(),
            events: run.events.clone(),
            execution_ms: run.execution_ms,
            resume: run.resume,
            test_swap_inputs: None,
            test_drop_input: None,
            test_kill_after_first_verified: run.test_kill_after_first_verified,
            test_no_fetch: run.test_no_fetch,
            test_fetch_delay_ms: run.test_fetch_delay_ms,
            test_stall_read_ms: run.test_stall_read_ms,
            serial: run.serial,
            pending_limit: run.pending_limit,
        };
        handles.push(tokio::spawn(async move {
            let staging = run.staging.clone();
            run_session(
                &run, worker, &authority, connect, &journal, sequence,
                run.seed, run.size, &staging, &run.events, started,
            )
            .await
        }));
    }
    for handle in handles {
        handle.await??;
    }
    // TEST-ONLY stopped consumer: arrivals exist but nothing was fetched;
    // refuse final assembly with a named reason instead of crashing on
    // absent staged outputs.
    if run.test_no_fetch {
        bail!("TEST-ONLY test-no-fetch: admitted without fetching; refusing final assembly");
    }
    assemble_final(run.seed, run.size, &staging, &run.output, &run.events).await?;
    Ok(())
}
