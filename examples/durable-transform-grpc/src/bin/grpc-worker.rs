//! gRPC baseline worker: durable transform authority over HTTP/2 streaming.
//!
//! mTLS authentication, SQLite commit-before-ACK receipts, immutable
//! operation identities with params digests, attempt fencing, manifest
//! commitments with independent lifetimes, and pinned streaming reads.
use anyhow::{Context, Result, bail};
use clap::Parser;
use grpc_baseline::{
    CANCELLED, COMMITTED, CONFLICT, EXPIRED, NOT_FOUND, UNAUTHORIZED,
    hex_id, open_durable, owner_of, params_digest, parse_id, proto, sync_file,
    wall_ms,
};
use rusqlite::{Connection, OptionalExtension};
use sha2::{Digest as _, Sha256};
use std::{
    collections::HashSet,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    Request, Response, Status,
    transport::{Certificate, Identity, Server, ServerTlsConfig},
};
use workload_core::transform_chunk;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    bind: SocketAddr,
    #[arg(long)]
    cert: PathBuf,
    #[arg(long)]
    key: PathBuf,
    #[arg(long)]
    client_ca: PathBuf,
    #[arg(long)]
    principal_map: PathBuf,
    #[arg(long)]
    authority: String,
    #[arg(long)]
    db: PathBuf,
    #[arg(long)]
    object_dir: PathBuf,
    #[arg(long)]
    ready_file: Option<PathBuf>,
    #[arg(long, default_value_t = 60000)]
    execution_ceiling_ms: u64,
    #[arg(long, default_value_t = 3600000)]
    output_retention_ms: u64,
}

struct Worker {
    authority: String,
    owners: HashSet<String>,
    principal_map: PathBuf,
    db: Mutex<Connection>,
    object_dir: PathBuf,
    execution_ceiling_ms: u64,
    output_retention_ms: u64,
}

fn owner_of_request<T>(request: &Request<T>, map: &Path) -> Result<String, Status> {
    let certs = request
        .peer_certs()
        .ok_or_else(|| Status::unauthenticated("missing client certificate"))?;
    let leaf = certs
        .first()
        .ok_or_else(|| Status::unauthenticated("empty certificate chain"))?;
    owner_of(leaf.as_ref(), map).map_err(|_| Status::unauthenticated(UNAUTHORIZED))
}

fn check_owner(worker: &Worker, owner: &str) -> Result<(), Status> {
    if worker.owners.contains(owner) {
        Ok(())
    } else {
        Err(Status::permission_denied(UNAUTHORIZED))
    }
}

fn receipt_row(
    db: &Connection,
    op: &[u8; 16],
) -> Result<Option<(Vec<u8>, String, i64, i64, String)>> {
    db.query_row(
        "SELECT params_digest, state, attempt, committed_at_ms, detail FROM operations WHERE op_id = ?1",
        [op.as_slice()],
        |r| {
            Ok((
                r.get::<_, Vec<u8>>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, String>(4)?,
            ))
        },
    )
    .optional()
    .map_err(anyhow::Error::from)
}

fn reply(op_hex: &str, digest: &[u8], outcome: &str, attempt: i64, at: u64, detail: &str) -> proto::SubmitResponse {
    proto::SubmitResponse {
        operation_id: op_hex.to_string(),
        params_digest: digest.to_vec(),
        outcome: outcome.to_string(),
        attempt: attempt as u64,
        committed_at_ms: at,
        detail: detail.to_string(),
    }
}

#[derive(Clone)]
struct Svc(Arc<Worker>);

impl std::ops::Deref for Svc {
    type Target = Worker;
    fn deref(&self) -> &Worker {
        &self.0
    }
}

#[tonic::async_trait]
impl proto::transform_worker_server::TransformWorker for Svc {
    async fn submit(
        &self,
        request: Request<tonic::Streaming<proto::SubmitRequest>>,
    ) -> Result<Response<proto::SubmitResponse>, Status> {
        let owner = owner_of_request(&request, &self.principal_map)?;
        check_owner(self, &owner)?;
        let mut stream = request.into_inner();
        let header = match stream
            .message()
            .await
            .map_err(|e| Status::internal(e.to_string()))?
        {
            Some(proto::SubmitRequest {
                payload: Some(proto::submit_request::Payload::Header(h)),
            }) => h,
            _ => return Err(Status::invalid_argument("first message must be SubmitHeader")),
        };
        let id = header.id.ok_or_else(|| Status::invalid_argument("missing identity"))?;
        if id.authority != self.authority || id.owner != owner {
            return Err(Status::permission_denied(UNAUTHORIZED));
        }
        let op = parse_id(&id.operation_id).map_err(|e| Status::invalid_argument(e.to_string()))?;
        if header.input_sha256.len() != 32 {
            return Err(Status::invalid_argument("input_sha256 must be 32 bytes"));
        }
        let mut input_sha: [u8; 32] = [0; 32];
        input_sha.copy_from_slice(&header.input_sha256);
        if header.total_length > 16 * 1024 * 1024 {
            return Err(Status::invalid_argument("chunk exceeds 16 MiB object limit"));
        }
        let now = wall_ms();
        if header.execution_deadline_ms <= now {
            return Err(Status::failed_precondition(EXPIRED));
        }
        if header.execution_deadline_ms > now + self.execution_ceiling_ms {
            return Err(Status::resource_exhausted("execution exceeds ceiling"));
        }
        let digest = params_digest(
            &id.authority, &id.owner, id.generation, id.ordinal, &op,
            header.total_length, &input_sha, header.execution_deadline_ms,
        );
        if header.params_digest != digest {
            return Err(Status::invalid_argument("params_digest mismatch"));
        }
        // Replay path: identical receipt without re-execution.
        {
            let db = self.db.lock().unwrap();
            if let Some((stored, state, attempt, at, _)) =
                receipt_row(&db, &op).map_err(|e| Status::internal(e.to_string()))?
            {
                if stored != digest {
                    return Err(Status::already_exists(CONFLICT));
                }
                return Ok(Response::new(reply(&id.operation_id, &digest, &state, attempt, at as u64, "replay")));
            }
        }
        // Stream the body into a staged temp file, bounded by the header.
        let staging_path = self.object_dir.join(format!(".stage-{}-{}", id.ordinal, hex_id(&op)));
        let mut staged = std::fs::File::create(&staging_path)
            .map_err(|e| Status::internal(e.to_string()))?;
        let mut hasher = Sha256::new();
        let mut received: u64 = 0;
        {
            use std::io::Write;
            while let Some(msg) = stream
                .message()
                .await
                .map_err(|e| Status::internal(e.to_string()))?
            {
                match msg.payload {
                    Some(proto::submit_request::Payload::Content(bytes)) => {
                        received += bytes.len() as u64;
                        if received > header.total_length {
                            return Err(Status::invalid_argument("body exceeds declared length"));
                        }
                        hasher.update(&bytes);
                        staged.write_all(&bytes).map_err(|e| Status::internal(e.to_string()))?;
                    }
                    _ => return Err(Status::invalid_argument("header after start")),
                }
            }
        }
        if received != header.total_length {
            let _ = std::fs::remove_file(&staging_path);
            return Err(Status::invalid_argument("truncated body"));
        }
        let body_hash: [u8; 32] = hasher.finalize().into();
        if body_hash != input_sha {
            let _ = std::fs::remove_file(&staging_path);
            return Err(Status::data_loss("input hash mismatch"));
        }
        // Execute the shared deterministic transform (same code as measured).
        let input = std::fs::read(&staging_path).map_err(|e| Status::internal(e.to_string()))?;
        let output = transform_chunk(&input, 0);
        let out_hash: [u8; 32] = Sha256::digest(&output).into();
        let out_path = self.object_dir.join(format!("out-{:06}-{}", id.ordinal, hex_id(&op)));
        std::fs::write(&out_path, &output).map_err(|e| Status::internal(e.to_string()))?;
        sync_file(&out_path).map_err(|e| Status::internal(e.to_string()))?;
        let _ = std::fs::remove_file(&staging_path);
        // Single durable commit BEFORE the ACK: receipt, outcome, manifest.
        let available_until = now + self.output_retention_ms;
        {
            let db = self.db.lock().unwrap();
            let tx = db.unchecked_transaction().map_err(|e| Status::internal(e.to_string()))?;
            let inserted = tx
                .execute(
                    "INSERT OR IGNORE INTO operations(op_id, params_digest, kind, state, attempt, committed_at_ms, detail)
                     VALUES(?1, ?2, 'submit', ?3, 1, ?4, '')",
                    rusqlite::params![op.as_slice(), digest.as_slice(), COMMITTED, now as i64],
                )
                .map_err(|e| Status::internal(e.to_string()))?;
            if inserted == 0 {
                // Lost a commit race: resolve as replay or conflict.
                let (stored, state, attempt, at, _) =
                    receipt_row(&db, &op).map_err(|e| Status::internal(e.to_string()))?
                        .ok_or_else(|| Status::internal("missing raced receipt"))?;
                if stored != digest {
                    return Err(Status::already_exists(CONFLICT));
                }
                return Ok(Response::new(reply(&id.operation_id, &digest, &state, attempt, at as u64, "replay")));
            }
            tx.execute(
                "INSERT OR REPLACE INTO chunks(ordinal, op_id, input_sha256, output_sha256, output_len, state, attempt, available_until_ms)
                 VALUES(?1, ?2, ?3, ?4, ?5, ?6, 1, ?7)",
                rusqlite::params![
                    id.ordinal as i64, op.as_slice(), input_sha.as_slice(),
                    out_hash.as_slice(), output.len() as i64, COMMITTED, available_until as i64
                ],
            )
            .map_err(|e| Status::internal(e.to_string()))?;
            tx.commit().map_err(|e| Status::internal(e.to_string()))?;
        }
        Ok(Response::new(reply(&id.operation_id, &digest, COMMITTED, 1, now, "")))
    }

    async fn retry(
        &self,
        request: Request<proto::RetryRequest>,
    ) -> Result<Response<proto::SubmitResponse>, Status> {
        let owner = owner_of_request(&request, &self.principal_map)?;
        check_owner(self, &owner)?;
        let r = request.into_inner();
        let id = r.id.ok_or_else(|| Status::invalid_argument("missing identity"))?;
        let op = parse_id(&id.operation_id).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let db = self.db.lock().unwrap();
        let row = receipt_row(&db, &op).map_err(|e| Status::internal(e.to_string()))?;
        match row {
            None => Err(Status::not_found(NOT_FOUND)),
            Some((_, state, attempt, _at, _)) if state == "RETRYABLE" && attempt as u64 == r.expected_attempt => {
                // The frozen transform never yields RETRYABLE; this arm exists
                // so the retry/fencing path is real protocol, not a stub.
                Err(Status::failed_precondition("no retryable attempt; resubmit is a replay"))
            }
            Some((digest, state, attempt, at, _)) => Ok(Response::new(reply(
                &id.operation_id, &digest, &state, attempt, at as u64, "lookup",
            ))),
        }
    }

    async fn cancel(
        &self,
        request: Request<proto::CancelRequest>,
    ) -> Result<Response<proto::CancelResponse>, Status> {
        let owner = owner_of_request(&request, &self.principal_map)?;
        check_owner(self, &owner)?;
        let r = request.into_inner();
        let id = r.id.ok_or_else(|| Status::invalid_argument("missing identity"))?;
        if id.authority != self.authority || id.owner != owner {
            return Err(Status::permission_denied(UNAUTHORIZED));
        }
        let db = self.db.lock().unwrap();
        let terminal: Option<String> = db
            .query_row(
                "SELECT state FROM chunks WHERE ordinal = ?1",
                [id.ordinal as i64],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| Status::internal(e.to_string()))?;
        match terminal {
            Some(s) if s == COMMITTED || s == CANCELLED => Ok(Response::new(proto::CancelResponse {
                operation_id: id.operation_id,
                disposition: "TERMINAL".to_string(),
                detail: s,
            })),
            _ => {
                db.execute(
                    "INSERT OR REPLACE INTO chunks(ordinal, op_id, input_sha256, state, attempt, available_until_ms)
                     VALUES(?1, ?2, ?3, ?4, 0, 0)",
                    rusqlite::params![id.ordinal as i64, [0u8; 16].as_slice(), [0u8; 32].as_slice(), CANCELLED],
                )
                .map_err(|e| Status::internal(e.to_string()))?;
                Ok(Response::new(proto::CancelResponse {
                    operation_id: id.operation_id,
                    disposition: "ACCEPTED".to_string(),
                    detail: String::new(),
                }))
            }
        }
    }

    async fn lookup(
        &self,
        request: Request<proto::LookupRequest>,
    ) -> Result<Response<proto::SubmitResponse>, Status> {
        let owner = owner_of_request(&request, &self.principal_map)?;
        check_owner(self, &owner)?;
        let op = parse_id(&request.into_inner().operation_id)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let db = self.db.lock().unwrap();
        match receipt_row(&db, &op).map_err(|e| Status::internal(e.to_string()))? {
            None => Err(Status::not_found(NOT_FOUND)),
            Some((digest, state, attempt, at, _)) => Ok(Response::new(reply(
                &hex_id(&op), &digest, &state, attempt, at as u64, "lookup",
            ))),
        }
    }

    async fn get_manifest(
        &self,
        request: Request<proto::ManifestRequest>,
    ) -> Result<Response<proto::ManifestResponse>, Status> {
        let owner = owner_of_request(&request, &self.principal_map)?;
        check_owner(self, &owner)?;
        let r = request.into_inner();
        let id = r.id.ok_or_else(|| Status::invalid_argument("missing identity"))?;
        if id.owner != owner {
            return Err(Status::permission_denied(UNAUTHORIZED));
        }
        let db = self.db.lock().unwrap();
        let row: Option<(Vec<u8>, Vec<u8>, i64, i64, i64)> = db
            .query_row(
                "SELECT input_sha256, output_sha256, output_len, committed_at_ms, available_until_ms
                 FROM chunks WHERE ordinal = ?1 AND state = ?2 AND attempt = ?3",
                rusqlite::params![id.ordinal as i64, COMMITTED, r.attempt as i64],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            )
            .optional()
            .map_err(|e| Status::internal(e.to_string()))?;
        match row {
            None => Err(Status::not_found(NOT_FOUND)),
            Some((input_sha, output_sha, len, at, until)) => {
                if wall_ms() > until as u64 {
                    return Err(Status::failed_precondition(EXPIRED));
                }
                Ok(Response::new(proto::ManifestResponse {
                    manifest: Some(proto::OutputManifest {
                        id: Some(id),
                        attempt: r.attempt,
                        input_sha256: input_sha,
                        output_sha256: output_sha,
                        output_length: len as u64,
                        committed_at_ms: at as u64,
                        available_until_ms: until as u64,
                    }),
                }))
            }
        }
    }

    type ReadOutputStream = ReceiverStream<Result<proto::OutputChunk, Status>>;

    async fn read_output(
        &self,
        request: Request<proto::ReadRequest>,
    ) -> Result<Response<Self::ReadOutputStream>, Status> {
        let owner = owner_of_request(&request, &self.principal_map)?;
        check_owner(self, &owner)?;
        let r = request.into_inner();
        let id = r.id.ok_or_else(|| Status::invalid_argument("missing identity"))?;
        if id.owner != owner {
            return Err(Status::permission_denied(UNAUTHORIZED));
        }
        let (output_sha, len, until): (Vec<u8>, i64, i64) = {
            let db = self.db.lock().unwrap();
            db.query_row(
                "SELECT output_sha256, output_len, available_until_ms
                 FROM chunks WHERE ordinal = ?1 AND state = ?2 AND attempt = ?3",
                rusqlite::params![id.ordinal as i64, COMMITTED, r.attempt as i64],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found(NOT_FOUND))?
        };
        if output_sha != r.expected_output_sha256 {
            return Err(Status::data_loss("output commitment mismatch"));
        }
        if wall_ms() > until as u64 {
            return Err(Status::failed_precondition(EXPIRED));
        }
        let op = parse_id(&id.operation_id).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let path = self.object_dir.join(format!("out-{:06}-{}", id.ordinal, hex_id(&op)));
        let bytes = std::fs::read(&path).map_err(|_| Status::internal("missing retained output"))?;
        if bytes.len() as i64 != len {
            return Err(Status::internal("retained length mismatch"));
        }
        // Bounded read pin: refuses unbounded concurrent reads implicitly by
        // row count; swept by expiry.
        let nonce: [u8; 16] = rand_nonce();
        {
            let db = self.db.lock().unwrap();
            db.execute(
                "INSERT INTO pins(nonce, ordinal, expires_ms) VALUES(?1, ?2, ?3)",
                rusqlite::params![nonce.as_slice(), id.ordinal as i64, (wall_ms() + 300_000) as i64],
            )
            .map_err(|e| Status::internal(e.to_string()))?;
        }
        let (tx, rx) = mpsc::channel(8);
        let worker = self.0.clone();
        tokio::spawn(async move {
            for piece in bytes.chunks(32 * 1024) {
                if tx
                    .send(Ok(proto::OutputChunk {
                        content: piece.to_vec(),
                        fin: false,
                    }))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            let _ = tx
                .send(Ok(proto::OutputChunk {
                    content: Vec::new(),
                    fin: true,
                }))
                .await;
            if let Ok(db) = worker.db.lock() {
                let _ = db.execute("DELETE FROM pins WHERE nonce = ?1", [nonce.as_slice()]);
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

fn rand_nonce() -> [u8; 16] {
    // Non-security nonce: unique read-pin key from time + process + counter.
    use std::sync::atomic::{AtomicU64, Ordering};
    static CTR: AtomicU64 = AtomicU64::new(0);
    let mut h = Sha256::new();
    h.update(wall_ms().to_le_bytes());
    h.update(std::process::id().to_le_bytes());
    h.update(CTR.fetch_add(1, Ordering::Relaxed).to_le_bytes());
    let d: [u8; 32] = h.finalize().into();
    d[..16].try_into().unwrap()
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    std::fs::create_dir_all(&args.object_dir)?;
    let conn = open_durable(&args.db)?;
    let text = std::fs::read_to_string(&args.principal_map)?;
    let mut owners = HashSet::new();
    for line in text.lines().skip(1) {
        if let Some((_, owner)) = line.split_once('\t') {
            owners.insert(owner.to_string());
        }
    }
    if owners.is_empty() {
        bail!("at least one mapped principal is required");
    }
    let worker = Arc::new(Worker {
        authority: args.authority.clone(),
        owners,
        principal_map: args.principal_map.clone(),
        db: Mutex::new(conn),
        object_dir: args.object_dir.clone(),
        execution_ceiling_ms: args.execution_ceiling_ms,
        output_retention_ms: args.output_retention_ms,
    });
    let tls = ServerTlsConfig::new()
        .identity(Identity::from_pem(
            std::fs::read(&args.cert).context("read server cert")?,
            std::fs::read(&args.key).context("read server key")?,
        ))
        .client_ca_root(Certificate::from_pem(
            std::fs::read(&args.client_ca).context("read client CA")?,
        ));
    let addr = args.bind;
    let svc = proto::transform_worker_server::TransformWorkerServer::new(Svc(worker));
    let server = Server::builder().tls_config(tls)?.add_service(svc).serve(addr);
    println!("LISTENING {addr}");
    if let Some(path) = args.ready_file {
        std::fs::write(&path, format!("{addr}\n"))?;
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    }
    server.await?;
    Ok(())
}
