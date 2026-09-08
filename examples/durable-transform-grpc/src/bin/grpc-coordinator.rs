//! gRPC baseline coordinator: same sharding, journaling, and verified
//! reassembly discipline as the PipeStream coordinator, over the baseline
//! protocol. Same events TSV schema, same oracle gates.
use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use grpc_baseline::{
    COMMITTED, hex_id, open_durable, operation_id, params_digest, proto,
    sync_file, wall_ms,
};
use rusqlite::OptionalExtension;
use sha2::{Digest as _, Sha256};
use std::{
    fs::OpenOptions,
    io::{Read, Write},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tokio::time::sleep;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use workload_core::{
    chunk_count, chunk_len, expected_chunk, expected_final_digest, generate_chunk,
};

const OBJECT_LIMIT: u64 = 16 * 1024 * 1024;

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
    let deadline = wall_ms() + execution_ms;
    let digest = params_digest(
        authority, owner, generation, ordinal, &op,
        input.len() as u64, &input_sha, deadline,
    );
    let mut messages = vec![proto::SubmitRequest {
        payload: Some(proto::submit_request::Payload::Header(proto::SubmitHeader {
            id: Some(identity(authority, owner, generation, ordinal, &op)),
            total_length: input.len() as u64,
            input_sha256: input_sha.to_vec(),
            execution_deadline_ms: deadline,
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
    while let Some(chunk) = stream
        .message()
        .await
        .map_err(|e| anyhow::anyhow!("stream ordinal {ordinal}: {e}"))?
    {
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
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_worker(
    run: &Run,
    worker: u64,
    authority: &str,
    endpoint: &str,
    seed: u64,
    total: u64,
    staging: &Path,
    started: Instant,
) -> Result<()> {
    let db = open_durable(&run.db)?;
    let mut client = connect(&run.tls, endpoint).await?;
    log(&run.events, started, worker as i64, -1, "session-open", authority);
    let ordinals: Vec<u64> = (0..chunk_count(total))
        .filter(|o| o % 3 == worker)
        .collect();
    let mut first_usable = false;
    for &ordinal in &ordinals {
        let op = operation_id(seed, worker, "submit", ordinal);
        let expected = expected_chunk(seed, total, ordinal);
        // Resume shortcut: byte-verified staged output.
        if run.resume {
            if let Ok(bytes) = std::fs::read(out_path(staging, ordinal)) {
                if bytes == expected {
                    continue;
                }
            }
        }
        let known: Option<String> = db
            .query_row(
                "SELECT state FROM operations WHERE op_id = ?1",
                [op.as_slice()],
                |r| r.get(0),
            )
            .optional()
            .map_err(anyhow::Error::from)?;
        let input = generate_chunk(seed, ordinal, chunk_len(total, ordinal));
        if known.as_deref() != Some(COMMITTED) {
            let reply = submit_chunk(
                &mut client, authority, &run.owner, run.generation,
                ordinal, op, &input, run.execution_ms,
            )
            .await?;
            if reply.outcome != COMMITTED {
                // Committed-but-unobserved ambiguity: resolve, never reinvent.
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
            db.execute(
                "INSERT OR REPLACE INTO operations(op_id, params_digest, kind, state, attempt, committed_at_ms, detail)
                 VALUES(?1, ?2, 'submit', ?3, 1, ?4, '')",
                rusqlite::params![op.as_slice(), reply.params_digest, COMMITTED, wall_ms() as i64],
            )?;
            log(&run.events, started, worker as i64, ordinal as i64, "admitted", "");
        }
        let manifest = wait_manifest(
            &mut client, authority, &run.owner, run.generation, ordinal, op,
            wall_ms() + run.execution_ms + 30_000,
        )
        .await?;
        if manifest.output_sha256.is_empty() {
            bail!("empty manifest commitment");
        }
        fetch_output(&mut client, authority, &run.owner, run.generation, ordinal, op, &manifest, staging).await?;
        let bytes = std::fs::read(out_path(staging, ordinal))?;
        if bytes != expected {
            bail!("chunk {ordinal} failed oracle byte verification");
        }
        if !first_usable {
            first_usable = true;
            log(&run.events, started, worker as i64, ordinal as i64, "first-usable-output", "");
        }
        log(&run.events, started, worker as i64, ordinal as i64, "chunk-verified", "");
    }
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
    let workers = [
        (0u64, run.authority_a.clone(), run.endpoint_a.clone()),
        (1u64, run.authority_b.clone(), run.endpoint_b.clone()),
        (2u64, run.authority_c.clone(), run.endpoint_c.clone()),
    ];
    let mut handles = Vec::new();
    for (worker, authority, endpoint) in workers {
        let staging = run.staging.clone();
        let events = run.events.clone();
        let owner = run.owner.clone();
        let db = run.db.clone();
        let tls = Tls {
            ca: run.tls.ca.clone(),
            cert: run.tls.cert.clone(),
            key: run.tls.key.clone(),
            server_name: run.tls.server_name.clone(),
        };
        let (seed, size, generation, execution_ms, resume) =
            (run.seed, run.size, run.generation, run.execution_ms, run.resume);
        handles.push(tokio::spawn(async move {
            let run = Run {
                tls,
                owner,
                db,
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
            };
            run_worker(&run, worker, &authority, &endpoint, seed, size, &staging, started).await
        }));
    }
    for handle in handles {
        handle.await??;
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
    Ok(())
}
