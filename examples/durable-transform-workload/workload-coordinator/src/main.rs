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
}

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
    run: &Run,
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
        owner: IdentityLabel(run.owner.clone()),
        creation_sequence: Id(creation_sequence),
        policy: Policy {
            execution_limit_ms: Duration(run.execution_ms),
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
        match Client::connect(endpoint(&run.tls, connect)?, journal.clone(), session::Options::default()).await {
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
    declaration: OperationId,
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
                send_admission(session, &path, &replay_intent, declaration, ordinal).await?;
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

async fn fetch_verified(
    session: &Session,
    seed: u64,
    total: u64,
    ordinal: u64,
    attempt: Id,
    staging: &Path,
    first_usable: &mut bool,
) -> Result<()> {
    let key = work_key(ordinal);
    let expected = expected_chunk(seed, total, ordinal);
    let path = out_path(staging, ordinal);
    if path.exists() {
        let bytes = std::fs::read(&path)?;
        if bytes == expected {
            return Ok(());
        }
        std::fs::remove_file(&path)?;
    }
    session.client.select_output(key.clone(), attempt, OutputIndex(0)).await?;
    let saved = session
        .client
        .read_output(key, attempt, OutputIndex(0))
        .await?
        .save_to(path.clone(), OBJECT_LIMIT)
        .await?;
    let _ = saved;
    let bytes = std::fs::read(&path)?;
    if bytes != expected {
        bail!("chunk {ordinal} failed byte verification after verified transfer");
    }
    if !*first_usable {
        *first_usable = true;
        session.log("first-usable-output", ordinal as i64, "");
    }
    session.log("chunk-verified", ordinal as i64, "");
    Ok(())
}

/// Admit one chunk, treating authority capacity refusals as backpressure.
///
/// A LIMIT_EXCEEDED refusal means the authority is healthy but full; the
/// journaled intent is unchanged, so resending the SAME operation identity is
/// safe and required for durable progress. Every refusal is recorded in the
/// event stream with its named code; any other error fails fast. Bounded:
/// 240 attempts, 100 ms doubling to 5 s (about 10 minutes worst case).
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
                let backpressure = matches!(
                    &e,
                    session::Failure::Refused(r) if r.code == ErrorCode::LimitExceeded
                );
                if !backpressure {
                    return Err(e.into());
                }
                session.log("admit-refused", ordinal as i64, "LIMIT_EXCEEDED");
                if attempt + 1 >= MAX_ATTEMPTS {
                    return Err(e.into());
                }
                tokio::time::sleep(std::time::Duration::from_millis(wait_ms)).await;
                wait_ms = (wait_ms * 2).min(5_000);
            }
        }
    }
    unreachable!("admit retry loop always returns");
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
        run, worker, authority, connect, journal_path, creation_sequence,
        fresh, events, started,
    )
    .await?;
    session.log("session-open", -1, authority);
    let declaration = operation_id(seed, worker, "declare", 0);
    // Declare (fresh runs only) then replay anything the journal still
    // holds as uncertain.
    if fresh {
        let ids: Vec<Id> = ordinals.iter().map(|o| Id(o + 1)).collect();
        session
            .client
            .mutate(journal::Intent {
                operation: declaration,
                mutation: Mutation::Declare {
                    scope: Number(0),
                    entity_ids: ids,
                    seal: true,
                },
            })
            .await?;
        session.log("declared", -1, &format!("{} entities", ordinals.len()));
    }
    let replayed = replay_unresolved(&session, staging, declaration).await?;
    // Admit every chunk of this shard under its frozen identity, except
    // terminal work and just-replayed operations.
    for &ordinal in &ordinals {
        let admitted = session.client.observed_work(work_key(ordinal)).await?;
        let terminal = admitted.map(|o| (5..=8).contains(&o.view.state.0)).unwrap_or(false);
        if terminal {
            continue;
        }
        let operation = operation_id(seed, worker, "admit", ordinal);
        if replayed.contains(&operation.0) {
            continue;
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
                execution_ms: Duration(run.execution_ms),
                outputs: OutputBudget {
                    count: BatchCount(1),
                    total_bytes: Number(len as u64),
                },
            }),
        };
        send_admission(
            &session,
            &chunk_path(staging, ordinal),
            &intent,
            declaration,
            ordinal,
        )
        .await?;
        session.log("admitted", ordinal as i64, "");
    }
    // Watch, fetch, and verify each chunk.
    let mut first_usable = false;
    for &ordinal in &ordinals {
        let (attempt, _) = watch_terminal(&session, ordinal).await?;
        session.log("succeeded", ordinal as i64, &format!("attempt {}", attempt.0));
        fetch_verified(&session, seed, total, ordinal, attempt, staging, &mut first_usable).await?;
    }
    // Checkpoint the sealed root scope, then assert durable completion.
    let page = session.client.scope_page(Number(0), Number(0), PageLimit(256)).await?;
    let seal = page.seal.context("root scope has no committed seal")?;
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

#[derive(Debug, Subcommand)]
enum Command {
    Run(Run),
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

#[tokio::main]
async fn main() -> Result<()> {
    let Command::Run(run) = Cli::parse().command;
    let started = Instant::now();
    let staging = run.staging.clone();
    materialize_chunks(run.seed, run.size, &staging)?;
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
    assemble_final(run.seed, run.size, &staging, &run.output, &run.events).await?;
    Ok(())
}
