//! gRPC baseline coordinator: same sharding, journaling, and verified
//! reassembly discipline as the PipeStream coordinator, over the baseline
//! protocol. Same events TSV schema, same oracle gates.
use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use grpc_baseline::{
    COMMITTED, hex_id, open_durable, operation_id, params_digest, proto,
    sync_file, wall_ms,
};
use rusqlite::{Connection, OptionalExtension};
use sha2::{Digest as _, Sha256};
use std::{
    fs::OpenOptions,
    io::{Read, Write},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::time::sleep;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use workload_core::{
    chunk_count, chunk_len, expected_chunk, expected_final_digest, generate_chunk,
};

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
    db: PathBuf,
    #[arg(long)]
    endpoint_a: String,
    #[arg(long)]
    endpoint_b: String,
    #[arg(long)]
    endpoint_c: String,
    #[arg(long, default_value = "workload-a")]
    authority_a: String,
    #[arg(long, default_value = "workload-b")]
    authority_b: String,
    #[arg(long, default_value = "workload-c")]
    authority_c: String,
    #[arg(long, default_value_t = 1)]
    generation: u64,
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
    /// TEST-ONLY: admit all chunks then stop without fetching (stopped
    /// consumer arm). Arrival rows exist, completion rows must not.
    #[arg(long, default_value_t = false)]
    test_no_fetch: bool,
    /// TEST-ONLY: after admitting everything, hold the consumer still for
    /// this many ms (stopped-consumer arm) before fetching. The stall
    /// interval is logged; workers hold the results meanwhile.
    #[arg(long, default_value_t = 0)]
    test_fetch_delay_ms: u64,
    /// TEST-ONLY: after the first stream message arrives, stall this many
    /// ms mid-drain before reading on (stalled-read probe). Whatever the
    /// peer does to the stalled stream is logged verbatim.
    #[arg(long, default_value_t = 0)]
    test_stall_read_ms: u64,
    /// Admit and verify serially (one chunk at a time), reproducing the
    /// pre-pipelining behavior and numbers. Default (off) pipelines both
    /// phases up to --pending-limit. Same concurrency as the PipeStream
    /// coordinator (fairness).
    #[arg(long, default_value_t = false)]
    serial: bool,
    /// Coordinator-side cap on in-flight admissions/verifications per
    /// worker task. Local concurrency cap, not a negotiated protocol
    /// limit.
    #[arg(long, default_value_t = 16)]
    pending_limit: usize,
    /// TEST-ONLY: abort at the firing of a client boundary for one chunk
    /// ordinal: "BOUNDARY:N" (N indexes chunk ordinals; "first" for the
    /// first firing on any ordinal). Runnable on this arm: INTENT_JOURNALED
    /// (submit intent row committed), RECEIPT_JOURNALED (commit response
    /// received and journaled), RESULT_VERIFIED, RESULT_INSTALLED. The rest
    /// are rejected at startup with the exact reason (see
    /// GrpcBoundary::unavailable_on_grpc). The boundary row is the last
    /// event row; --resume on the same db must complete byte-exact. Never
    /// set in a measured cell.
    #[arg(long)]
    test_kill_at: Option<String>,
}

/// TEST-ONLY: parse an "A:B" ordinal pair with distinct ordinals.
fn parse_swap_pair(spec: &str) -> Result<(u64, u64)> {
    let (a, b) = spec.split_once(':').context("test-swap-inputs needs A:B")?;
    let (a, b): (u64, u64) = (a.parse().context("bad swap A")?, b.parse().context("bad swap B")?);
    if a == b {
        bail!("test-swap-inputs needs distinct ordinals");
    }
    Ok((a, b))
}

type Client = proto::transform_worker_client::TransformWorkerClient<Channel>;

async fn connect(tls: &Tls, endpoint: &str) -> Result<Client> {
    let tls_config = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(std::fs::read(&tls.ca)?))
        .domain_name(tls.server_name.clone())
        .identity(Identity::from_pem(
            std::fs::read(&tls.cert)?,
            std::fs::read(&tls.key)?,
        ));
    // Startup race: the ready-file precedes accept(). Retry briefly rather
    // than failing a healthy run on a transient refusal.
    let mut last = String::from("no attempts");
    for _ in 0..60 {
        match Endpoint::from_shared(endpoint.to_string())?
            .tls_config(tls_config.clone())?
            .connect()
            .await
        {
            Ok(channel) => return Ok(Client::new(channel)),
            Err(e) => {
                last = e.to_string();
                sleep(Duration::from_millis(500)).await;
            }
        }
    }
    bail!("coordinator connect timed out: {last}")
}

fn chunk_path(staging: &Path, ordinal: u64) -> PathBuf {
    staging.join(format!("chunk-{ordinal:06}.bin"))
}

fn out_path(staging: &Path, ordinal: u64) -> PathBuf {
    staging.join(format!("out-{ordinal:06}.bin"))
}

fn log(events: &Path, started: Instant, worker: i64, ordinal: i64, event: &str, detail: &str) {
    let line = format!(
        "{}\t{}\t{}\t{}\t{}\t{}\n",
        started.elapsed().as_millis(),
        wall_ms(),
        worker,
        ordinal,
        event,
        detail
    );
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(events)
        .expect("events")
        .write_all(line.as_bytes())
        .expect("events write");
}

fn identity(authority: &str, owner: &str, generation: u64, ordinal: u64, op: &[u8; 16]) -> proto::ChunkIdentity {
    proto::ChunkIdentity {
        authority: authority.to_string(),
        owner: owner.to_string(),
        generation,
        ordinal,
        operation_id: hex_id(op),
    }
}

async fn submit_chunk(
    client: &mut Client,
    authority: &str,
    owner: &str,
    generation: u64,
    ordinal: u64,
    op: [u8; 16],
    input: &[u8],
    execution_ms: u64,
) -> Result<proto::SubmitResponse> {
    let mut input_sha = [0u8; 32];
    input_sha.copy_from_slice(&Sha256::digest(input));
    let digest = params_digest(
        authority, owner, generation, ordinal, &op,
        input.len() as u64, &input_sha, execution_ms,
    );
    let mut messages = vec![proto::SubmitRequest {
        payload: Some(proto::submit_request::Payload::Header(proto::SubmitHeader {
            id: Some(identity(authority, owner, generation, ordinal, &op)),
            total_length: input.len() as u64,
            input_sha256: input_sha.to_vec(),
            execution_ceiling_ms: execution_ms,
            params_digest: digest.to_vec(),
        })),
    }];
    for piece in input.chunks(32 * 1024) {
        messages.push(proto::SubmitRequest {
            payload: Some(proto::submit_request::Payload::Content(piece.to_vec())),
        });
    }
    let response = client
        .submit(tonic::Request::new(tokio_stream::iter(messages)))
        .await
        .map_err(|e| anyhow::anyhow!("submit ordinal {ordinal}: {e}"))?;
    Ok(response.into_inner())
}

async fn wait_manifest(
    client: &mut Client,
    authority: &str,
    owner: &str,
    generation: u64,
    ordinal: u64,
    op: [u8; 16],
    deadline_ms: u64,
) -> Result<proto::OutputManifest> {
    loop {
        match client
            .get_manifest(tonic::Request::new(proto::ManifestRequest {
                id: Some(identity(authority, owner, generation, ordinal, &op)),
                attempt: 1,
            }))
            .await
        {
            Ok(r) => return Ok(r.into_inner().manifest.context("missing manifest")?),
            Err(e) if e.code() == tonic::Code::NotFound && wall_ms() < deadline_ms => {
                sleep(Duration::from_millis(200)).await;
            }
            Err(e) => bail!("manifest ordinal {ordinal}: {e}"),
        }
    }
}

async fn fetch_output(
    client: &mut Client,
    authority: &str,
    owner: &str,
    generation: u64,
    ordinal: u64,
    op: [u8; 16],
    manifest: &proto::OutputManifest,
    staging: &Path,
    stall_read_ms: u64,
    log_ctx: (&Path, Instant, i64),
    arm: Option<&KillArm>,
) -> Result<()> {
    let path = out_path(staging, ordinal);
    let mut stream = client
        .read_output(tonic::Request::new(proto::ReadRequest {
            id: Some(identity(authority, owner, generation, ordinal, &op)),
            attempt: manifest.attempt,
            expected_output_sha256: manifest.output_sha256.clone(),
        }))
        .await
        .map_err(|e| anyhow::anyhow!("read ordinal {ordinal}: {e}"))?
        .into_inner();
    let tmp = path.with_extension("partial");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&tmp)?;
    let mut hasher = Sha256::new();
    let mut len: u64 = 0;
    let mut fin = false;
    let mut first = true;
    while let Some(chunk) = stream
        .message()
        .await
        .map_err(|e| anyhow::anyhow!("stream ordinal {ordinal}: {e}"))?
    {
        // TEST-ONLY stalled-read probe: the stream is open and the first
        // message has arrived; hold the drain still, then read on.
        // Whatever the peer does is logged verbatim (here and in any
        // error the drain returns below).
        if first {
            first = false;
            if stall_read_ms > 0 {
                let (events, started, worker) = log_ctx;
                log(
                    events,
                    started,
                    worker,
                    ordinal as i64,
                    "stalled-read",
                    &format!("stream open, holding drain {stall_read_ms} ms"),
                );
                eprintln!(
                    "TEST-ONLY test-stall-read-ms: holding drain {stall_read_ms} ms"
                );
                tokio::time::sleep(std::time::Duration::from_millis(stall_read_ms)).await;
            }
        }
        file.write_all(&chunk.content)?;
        hasher.update(&chunk.content);
        len += chunk.content.len() as u64;
        if chunk.fin {
            fin = true;
            break;
        }
    }
    if !fin {
        bail!("result stream ended without FIN");
    }
    file.sync_all()?;
    drop(file);
    if len != manifest.output_length {
        bail!("output length mismatch");
    }
    let digest: [u8; 32] = hasher.finalize().into();
    if digest.as_slice() != manifest.output_sha256 {
        bail!("output hash mismatch");
    }
    std::fs::rename(&tmp, &path)?;
    sync_file(&path)?;
    // The staged output file is installed; killing here dies after install
    // with verification still ahead.
    if let Some(arm) = arm {
        let (events, started, worker) = log_ctx;
        arm.fire(events, started, worker, GrpcBoundary::ResultInstalled, ordinal);
    }
    Ok(())
}

/// TEST-ONLY kill boundary names shared with the PipeStream arm's
/// --test-kill-at (same flag, same ordinal rule). Only the boundaries with
/// a durable commit on this arm are runnable; the rest are rejected in
/// build_kill_arm with the exact reason.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum GrpcBoundary {
    IntentJournaled,
    RequestSent,
    ReceiptValidated,
    ReceiptJournaled,
    ObservationJournaled,
    ResultVerified,
    ResultInstalled,
    RefusalReceived,
}

impl GrpcBoundary {
    fn name(&self) -> &'static str {
        match self {
            GrpcBoundary::IntentJournaled => "INTENT_JOURNALED",
            GrpcBoundary::RequestSent => "REQUEST_SENT",
            GrpcBoundary::ReceiptValidated => "RECEIPT_VALIDATED",
            GrpcBoundary::ReceiptJournaled => "RECEIPT_JOURNALED",
            GrpcBoundary::ObservationJournaled => "OBSERVATION_JOURNALED",
            GrpcBoundary::ResultVerified => "RESULT_VERIFIED",
            GrpcBoundary::ResultInstalled => "RESULT_INSTALLED",
            GrpcBoundary::RefusalReceived => "REFUSAL_RECEIVED",
        }
    }

    fn parse(name: &str) -> Result<GrpcBoundary> {
        match name {
            "INTENT_JOURNALED" => Ok(GrpcBoundary::IntentJournaled),
            "REQUEST_SENT" => Ok(GrpcBoundary::RequestSent),
            "RECEIPT_VALIDATED" => Ok(GrpcBoundary::ReceiptValidated),
            "RECEIPT_JOURNALED" => Ok(GrpcBoundary::ReceiptJournaled),
            "OBSERVATION_JOURNALED" => Ok(GrpcBoundary::ObservationJournaled),
            "RESULT_VERIFIED" => Ok(GrpcBoundary::ResultVerified),
            "RESULT_INSTALLED" => Ok(GrpcBoundary::ResultInstalled),
            "REFUSAL_RECEIVED" => Ok(GrpcBoundary::RefusalReceived),
            other => bail!("test-kill-at: unknown client boundary {other:?}"),
        }
    }

    /// Why this boundary cannot be armed on the gRPC arm (None = runnable).
    fn unavailable_on_grpc(&self) -> Option<&'static str> {
        match self {
            GrpcBoundary::IntentJournaled
            | GrpcBoundary::ReceiptJournaled
            | GrpcBoundary::ResultVerified
            | GrpcBoundary::ResultInstalled => None,
            GrpcBoundary::RequestSent => Some(
                "tonic submit is one atomic request/response call from the caller, \
                 so there is no inter-commit seam between request-sent and \
                 response-received (the durable boundary is RECEIPT_JOURNALED)",
            ),
            GrpcBoundary::ReceiptValidated => Some(
                "receipt validation is a pure in-memory check with no journal \
                 commit (the commit is RECEIPT_JOURNALED)",
            ),
            GrpcBoundary::ObservationJournaled => Some(
                "manifest polling never journals (wait_manifest only reads), \
                 so there is no observation commit to kill at",
            ),
            GrpcBoundary::RefusalReceived => Some(
                "this arm has no backpressure-refusal vocabulary: submit \
                 errors fail fast with no retry rows, so no refusal row exists",
            ),
        }
    }
}

/// Parse "BOUNDARY:N" (N = chunk ordinal) or "BOUNDARY:first".
fn parse_kill_at(spec: &str) -> Result<(GrpcBoundary, Option<u64>)> {
    let (name, n) = spec
        .split_once(':')
        .context("test-kill-at needs BOUNDARY:N")?;
    let boundary = GrpcBoundary::parse(name)?;
    if n == "first" {
        return Ok((boundary, None));
    }
    let ordinal: u64 = n.parse().context("test-kill-at N must be a chunk ordinal")?;
    Ok((boundary, Some(ordinal)))
}

/// TEST-ONLY armed crash: at most one firing per process (abort is
/// immediate) and one per staging dir (sentinel file, so --resume proceeds
/// past it). fire() logs the boundary row itself so the last event row
/// always names the reached boundary.
struct KillArm {
    boundary: GrpcBoundary,
    /// None = first firing on any ordinal.
    ordinal: Option<u64>,
    sentinel: PathBuf,
    fired: AtomicBool,
}

impl KillArm {
    fn should_fire(&self, boundary: GrpcBoundary, ordinal: u64) -> bool {
        if self.boundary != boundary {
            return false;
        }
        if let Some(n) = self.ordinal {
            if n != ordinal {
                return false;
            }
        }
        !self.sentinel.exists()
    }

    fn fire(
        &self,
        events: &Path,
        started: Instant,
        worker: i64,
        boundary: GrpcBoundary,
        ordinal: u64,
    ) {
        if !self.should_fire(boundary, ordinal) {
            return;
        }
        if self.ordinal.is_none() && self.fired.swap(true, Ordering::SeqCst) {
            return;
        }
        log(
            events,
            started,
            worker,
            ordinal as i64,
            "boundary",
            &format!("{} reached, killing", boundary.name()),
        );
        let _ = std::fs::write(&self.sentinel, format!("{}:{ordinal}", boundary.name()));
        eprintln!(
            "TEST-ONLY test-kill-at: {} firing for ordinal {ordinal}, aborting",
            boundary.name()
        );
        std::process::abort();
    }
}

/// Build the TEST-ONLY crash arm for one worker from the run flags.
fn build_kill_arm(run: &Run, staging: &Path) -> Result<Option<Arc<KillArm>>> {
    if let Some(spec) = &run.test_kill_at {
        let (boundary, ordinal) = parse_kill_at(spec)?;
        if let Some(reason) = boundary.unavailable_on_grpc() {
            bail!(
                "TEST-ONLY test-kill-at {} unavailable on the gRPC arm: {}",
                boundary.name(),
                reason
            );
        }
        return Ok(Some(Arc::new(KillArm {
            boundary,
            ordinal,
            sentinel: staging.join("kill-armed"),
            fired: AtomicBool::new(false),
        })));
    }
    Ok(None)
}

/// Submit and admit one chunk (frozen submit identity). Shared by the
/// serial and pipelined paths; the admit latency is recorded on the row.
#[allow(clippy::too_many_arguments)]
async fn grpc_admit_one(
    client: &mut Client,
    db: &std::sync::Arc<std::sync::Mutex<Connection>>,
    authority: &str,
    owner: &str,
    generation: u64,
    seed: u64,
    worker: u64,
    execution_ms: u64,
    total: u64,
    staging: &Path,
    events: &Path,
    started: Instant,
    swap: Option<(u64, u64)>,
    drop_input: Option<u64>,
    resume: bool,
    ordinal: u64,
    arm: Option<&KillArm>,
) -> Result<()> {
    let t = Instant::now();
    let op = operation_id(seed, worker, "submit", ordinal);
    let expected = expected_chunk(seed, total, ordinal);
    if drop_input == Some(ordinal) {
        bail!("test-drop-input: chunk {ordinal} missing, not submitted");
    }
    let upload_ordinal = match swap {
        Some((a, b)) if ordinal == a => b,
        Some((a, b)) if ordinal == b => a,
        _ => ordinal,
    };
    if resume {
        if let Ok(bytes) = std::fs::read(out_path(staging, ordinal)) {
            if bytes == expected {
                return Ok(());
            }
        }
    }
    let known: Option<String> = db
        .lock()
        .unwrap()
        .query_row(
            "SELECT state FROM operations WHERE op_id = ?1",
            [op.as_slice()],
            |r| r.get(0),
        )
        .optional()
        .map_err(anyhow::Error::from)?;
    let input = generate_chunk(seed, upload_ordinal, chunk_len(total, ordinal));
    if known.as_deref() != Some(COMMITTED) {
        // Durable submit intent (always on, WAL+FULL commit): a death
        // between here and the commit row leaves this same identity for
        // the resume to resubmit. Resume treats any non-COMMITTED state
        // identically, so unarmed behavior is unchanged.
        let mut input_sha = [0u8; 32];
        input_sha.copy_from_slice(&Sha256::digest(&input));
        let digest = params_digest(
            authority, owner, generation, ordinal, &op,
            input.len() as u64, &input_sha, execution_ms,
        );
        db.lock().unwrap().execute(
            "INSERT OR IGNORE INTO operations(op_id, params_digest, kind, state, attempt, committed_at_ms, detail)
             VALUES(?1, ?2, 'submit', 'submitted', 0, ?3, '')",
            rusqlite::params![op.as_slice(), digest, wall_ms() as i64],
        )?;
        if let Some(arm) = arm {
            arm.fire(events, started, worker as i64, GrpcBoundary::IntentJournaled, ordinal);
        }
        let reply = submit_chunk(
            client, authority, owner, generation,
            ordinal, op, &input, execution_ms,
        )
        .await?;
        if reply.outcome != COMMITTED {
            let looked = client
                .lookup(tonic::Request::new(proto::LookupRequest {
                    operation_id: hex_id(&op),
                }))
                .await
                .map_err(|e| anyhow::anyhow!("lookup: {e}"))?
                .into_inner();
            if looked.outcome != COMMITTED {
                bail!("chunk {ordinal} not committed: {}", looked.outcome);
            }
        }
        db.lock().unwrap().execute(
            "INSERT OR REPLACE INTO operations(op_id, params_digest, kind, state, attempt, committed_at_ms, detail)
             VALUES(?1, ?2, 'submit', ?3, 1, ?4, '')",
            rusqlite::params![op.as_slice(), reply.params_digest, COMMITTED, wall_ms() as i64],
        )?;
        // The commit response is received and journaled; killing here dies
        // with the receipt durable and nothing fetched yet.
        if let Some(arm) = arm {
            arm.fire(events, started, worker as i64, GrpcBoundary::ReceiptJournaled, ordinal);
        }
        log(events, started, worker as i64, ordinal as i64, "admitted", &format!("admit={}ms", t.elapsed().as_millis()));
    }
    Ok(())
}

/// Wait for one chunk's manifest, fetch, and oracle-verify it. Shared by
/// the serial and pipelined paths; first_usable fires once per worker.
#[allow(clippy::too_many_arguments)]
async fn grpc_fetch_one(
    client: &mut Client,
    authority: &str,
    owner: &str,
    generation: u64,
    seed: u64,
    worker: u64,
    execution_ms: u64,
    total: u64,
    staging: &Path,
    events: &Path,
    started: Instant,
    stall_read_ms: u64,
    first_usable: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    ordinal: u64,
    arm: Option<&KillArm>,
) -> Result<()> {
    let t = Instant::now();
    let op = operation_id(seed, worker, "submit", ordinal);
    let expected = expected_chunk(seed, total, ordinal);
    let manifest = wait_manifest(
        client, authority, owner, generation, ordinal, op,
        wall_ms() + execution_ms + 30_000,
    )
    .await?;
    if manifest.output_sha256.is_empty() {
        bail!("empty manifest commitment");
    }
    fetch_output(client, authority, owner, generation, ordinal, op, &manifest, staging, stall_read_ms, (events, started, worker as i64), arm).await?;
    let bytes = std::fs::read(out_path(staging, ordinal))?;
    if bytes != expected {
        bail!("chunk {ordinal} failed oracle byte verification");
    }
    if !first_usable.swap(true, std::sync::atomic::Ordering::SeqCst) {
        log(events, started, worker as i64, ordinal as i64, "first-usable-output", "");
    }
    log(events, started, worker as i64, ordinal as i64, "chunk-verified", &format!("fetch={}ms", t.elapsed().as_millis()));
    // TEST-ONLY kill hook at RESULT_VERIFIED: the chunk-verified row above
    // proves the boundary was reached.
    if let Some(arm) = arm {
        arm.fire(events, started, worker as i64, GrpcBoundary::ResultVerified, ordinal);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_worker(
    run: &Run,
    db: Arc<Mutex<Connection>>,
    worker: u64,
    authority: &str,
    endpoint: &str,
    seed: u64,
    total: u64,
    staging: &Path,
    started: Instant,
) -> Result<()> {
    let mut client = connect(&run.tls, endpoint).await?;
    log(&run.events, started, worker as i64, -1, "session-open", authority);
    let ordinals: Vec<u64> = (0..chunk_count(total))
        .filter(|o| o % 3 == worker)
        .collect();
    let swap = run
        .test_swap_inputs
        .as_deref()
        .map(parse_swap_pair)
        .transpose()?;
    let first_usable = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    // TEST-ONLY armed crash shared by this worker's tasks (Arc: the
    // pipelined loops spawn 'static tasks).
    let arm = build_kill_arm(run, staging)?;
    let arm_ref = arm.as_deref();
    // Admit phase: pipelined by default (up to pending-limit in flight);
    // --serial keeps the old one-at-a-time order.
    if run.serial {
        for &ordinal in &ordinals {
            grpc_admit_one(&mut client, &db, authority, &run.owner, run.generation, seed, worker, run.execution_ms, total, staging, &run.events, started, swap, run.test_drop_input, run.resume, ordinal, arm_ref).await?;
        }
    } else {
        let limit = run.pending_limit.max(1);
        let mut set = tokio::task::JoinSet::new();
        for &ordinal in &ordinals {
            while set.len() >= limit {
                if let Some(r) = set.join_next().await {
                    r??;
                }
            }
            let mut c = client.clone();
            let d = db.clone();
            let auth = authority.to_string();
            let own = run.owner.clone();
            let ev = run.events.clone();
            let st = staging.to_path_buf();
            let (generation, ems, drop_in, res) = (run.generation, run.execution_ms, run.test_drop_input, run.resume);
            let arm_task = arm.clone();
            set.spawn(async move {
                grpc_admit_one(&mut c, &d, &auth, &own, generation, seed, worker, ems, total, &st, &ev, started, swap, drop_in, res, ordinal, arm_task.as_deref()).await
            });
        }
        while let Some(r) = set.join_next().await {
            r??;
        }
    }
        // Fetch phase over the same ordinals (admit phase above is
        // complete for this worker). Skipped for stopped-consumer.
        if run.test_no_fetch {
            log(&run.events, started, worker as i64, -1, "consumer-stopped", "admitted, not fetching");
        } else {
        // TEST-ONLY stopped consumer: everything is admitted and the
        // workers hold the results; hold still the stated interval
        // (logged) before the first fetch, then complete normally.
        if run.test_fetch_delay_ms > 0 {
            log(&run.events, started, worker as i64, -1, "consumer-stopped", &format!("admitted, holding fetch {} ms", run.test_fetch_delay_ms));
            eprintln!("TEST-ONLY test-fetch-delay-ms: admitted, holding fetch {} ms", run.test_fetch_delay_ms);
            tokio::time::sleep(std::time::Duration::from_millis(run.test_fetch_delay_ms)).await;
            log(&run.events, started, worker as i64, -1, "consumer-resumed", "fetching after hold");
        }
        if run.serial {
            for &ordinal in &ordinals {
                grpc_fetch_one(&mut client, authority, &run.owner, run.generation, seed, worker, run.execution_ms, total, staging, &run.events, started, run.test_stall_read_ms, &first_usable, ordinal, arm_ref).await?;
            }
        } else {
            let limit = run.pending_limit.max(1);
            let mut set = tokio::task::JoinSet::new();
            for &ordinal in &ordinals {
                while set.len() >= limit {
                    if let Some(r) = set.join_next().await {
                        r??;
                    }
                }
                let mut c = client.clone();
                let auth = authority.to_string();
                let own = run.owner.clone();
                let ev = run.events.clone();
                let st = staging.to_path_buf();
                let fu = first_usable.clone();
                let (generation, ems, stall) = (run.generation, run.execution_ms, run.test_stall_read_ms);
                let arm_task = arm.clone();
                set.spawn(async move {
                    grpc_fetch_one(&mut c, &auth, &own, generation, seed, worker, ems, total, &st, &ev, started, stall, &fu, ordinal, arm_task.as_deref()).await
                });
            }
            while let Some(r) = set.join_next().await {
                r??;
            }
        }
        } // else (not stopped)
    // One SQLite txn commit per submitted chunk plus one output fsync each;
    // the count below is measured evidence for the fsync accounting.
    log(&run.events, started, worker as i64, -1, "commits", &format!("{} chunks", ordinals.len()));
    log(&run.events, started, worker as i64, -1, "session-close", "");
    Ok(())
}

#[derive(Debug, Subcommand)]
enum Command {
    Run(Run),
}

#[derive(Debug, Parser)]
#[command(about = "Durable streaming-gRPC baseline coordinator")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[tokio::main]
async fn main() -> Result<()> {
    let Command::Run(run) = Cli::parse().command;
    let started = Instant::now();
    std::fs::create_dir_all(&run.staging)?;
    for ordinal in 0..chunk_count(run.size) {
        let path = chunk_path(&run.staging, ordinal);
        let expected = generate_chunk(run.seed, ordinal, chunk_len(run.size, ordinal));
        if std::fs::read(&path).map(|b| b == expected).unwrap_or(false) {
            continue;
        }
        std::fs::write(&path, &expected)?;
    }
    if let Some(spec) = &run.test_swap_inputs {
        // Validated here; applied per ordinal in run_worker, because this
        // coordinator regenerates inputs from the oracle instead of
        // uploading staging files (swapping files would be a no-op).
        let (a, b) = parse_swap_pair(spec)?;
        eprintln!("TEST-ONLY test-swap-inputs enabled: chunk {a} and {b} exchanged");
    }
    if let Some(n) = run.test_drop_input {
        // Enforced per ordinal in run_worker (this coordinator uploads
        // regenerated inputs, so removing a staging file would be a no-op).
        eprintln!("TEST-ONLY test-drop-input enabled: chunk {n} will not be submitted");
    }
    let workers = [
        (0u64, run.authority_a.clone(), run.endpoint_a.clone()),
        (1u64, run.authority_b.clone(), run.endpoint_b.clone()),
        (2u64, run.authority_c.clone(), run.endpoint_c.clone()),
    ];
    // One shared connection: concurrent WAL-mode initialization ignores the
    // busy handler on the journal_mode pragma, so initialization happens
    // exactly once and all tasks share the handle behind a mutex.
    let db = Arc::new(Mutex::new(open_durable(&run.db)?));
    let mut handles = Vec::new();
    for (worker, authority, endpoint) in workers {
        let staging = run.staging.clone();
        let events = run.events.clone();
        let owner = run.owner.clone();
        let db = db.clone();
        let tls = Tls {
            ca: run.tls.ca.clone(),
            cert: run.tls.cert.clone(),
            key: run.tls.key.clone(),
            server_name: run.tls.server_name.clone(),
        };
        let (seed, size, generation, execution_ms, resume, test_no_fetch, test_fetch_delay_ms, test_stall_read_ms) =
            (run.seed, run.size, run.generation, run.execution_ms, run.resume, run.test_no_fetch, run.test_fetch_delay_ms, run.test_stall_read_ms);
        let swap_inputs = run.test_swap_inputs.clone();
        let kill_at = run.test_kill_at.clone();
        handles.push(tokio::spawn(async move {
            let run = Run {
                tls,
                owner,
                db: PathBuf::new(),
                endpoint_a: String::new(),
                endpoint_b: String::new(),
                endpoint_c: String::new(),
                authority_a: String::new(),
                authority_b: String::new(),
                authority_c: String::new(),
                generation,
                seed,
                size,
                staging: staging.clone(),
                output: PathBuf::new(),
                events,
                execution_ms,
                resume,
                test_swap_inputs: swap_inputs.clone(),
                test_drop_input: run.test_drop_input,
                test_kill_at: kill_at.clone(),
                test_no_fetch,
                test_fetch_delay_ms,
                test_stall_read_ms,
                serial: run.serial,
                pending_limit: run.pending_limit,
            };
            run_worker(&run, db, worker, &authority, &endpoint, seed, size, &staging, started).await
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
    // Ordered, fsynced final assembly behind the oracle digest gate.
    let tmp = run.output.with_extension("partial");
    if let Some(parent) = run.output.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&tmp)?;
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 65536];
    for ordinal in 0..chunk_count(run.size) {
        let mut chunk = std::fs::File::open(out_path(&run.staging, ordinal))?;
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
    if digest != expected_final_digest(run.seed, run.size) {
        bail!("final object digest mismatch against independent oracle");
    }
    std::fs::rename(&tmp, &run.output)?;
    sync_file(&run.output)?;
    let line = format!(
        "0\t{}\t-\t-\tfinal-verified\t{} bytes\n",
        wall_ms(),
        run.size
    );
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&run.events)?
        .write_all(line.as_bytes())?;
    Ok(())
}

#[cfg(test)]
mod swap_tests {
    use super::*;

    #[test]
    fn parse_swap_pair_accepts_distinct_ordinals() {
        assert_eq!(parse_swap_pair("0:1").unwrap(), (0, 1));
        assert_eq!(parse_swap_pair("3:11").unwrap(), (3, 11));
        assert!(parse_swap_pair("0:0").is_err());
        assert!(parse_swap_pair("0").is_err());
        assert!(parse_swap_pair("a:b").is_err());
    }

    #[test]
    fn kill_at_parses_shared_boundary_vocabulary() {
        for (name, boundary) in [
            ("INTENT_JOURNALED", GrpcBoundary::IntentJournaled),
            ("REQUEST_SENT", GrpcBoundary::RequestSent),
            ("RECEIPT_VALIDATED", GrpcBoundary::ReceiptValidated),
            ("RECEIPT_JOURNALED", GrpcBoundary::ReceiptJournaled),
            ("OBSERVATION_JOURNALED", GrpcBoundary::ObservationJournaled),
            ("RESULT_VERIFIED", GrpcBoundary::ResultVerified),
            ("RESULT_INSTALLED", GrpcBoundary::ResultInstalled),
            ("REFUSAL_RECEIVED", GrpcBoundary::RefusalReceived),
        ] {
            assert_eq!(parse_kill_at(&format!("{name}:2")).unwrap(), (boundary, Some(2)));
            assert_eq!(parse_kill_at(&format!("{name}:first")).unwrap(), (boundary, None));
            assert_eq!(boundary.name(), name);
        }
        assert!(parse_kill_at("COMMITTED:0").is_err());
        assert!(parse_kill_at("RESULT_VERIFIED").is_err());
        assert!(parse_kill_at("RESULT_VERIFIED:x").is_err());
    }

    #[test]
    fn grpc_runnable_boundaries_are_exactly_four() {
        let runnable: Vec<&str> = [
            GrpcBoundary::IntentJournaled,
            GrpcBoundary::RequestSent,
            GrpcBoundary::ReceiptValidated,
            GrpcBoundary::ReceiptJournaled,
            GrpcBoundary::ObservationJournaled,
            GrpcBoundary::ResultVerified,
            GrpcBoundary::ResultInstalled,
            GrpcBoundary::RefusalReceived,
        ]
        .into_iter()
        .filter(|b| b.unavailable_on_grpc().is_none())
        .map(|b| b.name())
        .collect();
        assert_eq!(
            runnable,
            vec!["INTENT_JOURNALED", "RECEIPT_JOURNALED", "RESULT_VERIFIED", "RESULT_INSTALLED"]
        );
    }

    #[test]
    fn kill_arm_fires_only_on_match_without_sentinel() {
        let dir = std::env::temp_dir().join("grpc-kill-arm-test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let arm = KillArm {
            boundary: GrpcBoundary::ReceiptJournaled,
            ordinal: Some(2),
            sentinel: dir.join("kill-armed"),
            fired: AtomicBool::new(false),
        };
        assert!(!arm.should_fire(GrpcBoundary::IntentJournaled, 2));
        assert!(!arm.should_fire(GrpcBoundary::ReceiptJournaled, 1));
        assert!(arm.should_fire(GrpcBoundary::ReceiptJournaled, 2));
        std::fs::write(dir.join("kill-armed"), b"x").unwrap();
        assert!(!arm.should_fire(GrpcBoundary::ReceiptJournaled, 2));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
