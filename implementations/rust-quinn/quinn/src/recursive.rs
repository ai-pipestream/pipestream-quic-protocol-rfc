//! Durable Layer 1 and Layer 2 service built on the transport-independent core.

mod control;
#[cfg(test)]
mod execution_tests;
pub mod executor;
mod ingress;
pub mod spool;
mod storage;
use spool::{PAYLOAD_IO_CHUNK, Payload, SpoolLimits, SpoolStore};

use crate::authentication::{AuthenticationPolicy, ClientIdentity};
use executor::{ExecutionLimits, WorkerPool};
use pipestream_core::authorization::{
    EXTENSION_AUTHENTICATED_SESSIONS, PrincipalBinding, unauthorized,
};
use pipestream_core::execution::{
    ExecutionKey, ExecutionLease, ExecutionStage, MAX_EXECUTION_LEASE_MICROS,
};
use pipestream_core::jobs::{JobInput, JobOutput, JobState, ProcessOutcome};
use pipestream_core::recovery::{
    self, EXTENSION_AUTHENTICATED_RECOVERY, FRAME_RECOVERY, RecoveryFrame, RecoveryOutcome,
    RecoveryReceipt, RecoveryRequest,
};
use pipestream_core::work_set::{self, EXTENSION_SEALED_WORK_SETS, FRAME_WORK_SET, WorkSetFrame};

use anyhow::{Context, Result, bail};
use pipestream_core::{
    Barrier, CHECKPOINT_ACK, CONNECTION_LEVEL, Capabilities, Checkpoint, ClaimRedemption,
    ERROR_ENTITY_INVALID, ERROR_FRAME, ERROR_LAYER_UNSUPPORTED, ERROR_LIMIT_EXCEEDED,
    ERROR_NO_ERROR, EntityHeader, FRAME_BARRIER, FRAME_CAPABILITIES, FRAME_CHECKPOINT,
    FRAME_CLAIM_REDEMPTION, FRAME_GOAWAY, FRAME_SCOPE_DIGEST, FRAME_STATUS, LayerSupport,
    MAX_CONTROL_FRAME, MAX_ENTITY_HEADER, MAX_PAYLOAD, ProtocolError, ScopeDigest, Status,
    StatusExtension, StatusFrame, StoppingPointValidation, decode_barrier, decode_capabilities,
    decode_checkpoint_for, decode_claim_redemption, decode_goaway, decode_scope_digest,
    decode_status_frame, encode_barrier, encode_capabilities, encode_checkpoint_for,
    encode_claim_redemption, encode_entity_for, encode_goaway, encode_scope_digest, encode_status,
    encode_status_frame,
    persistence::{SessionStore, SqliteSessionStore, StoreError},
    session::{
        ClaimRecord, EntityKey, EntityState, NewEntity, Session, merkle_root, validate_session_id,
    },
};
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs::{self, OpenOptions},
    io::{Read, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub const SESSION_METADATA_KEY: &str = "pipestream.session-id";
pub const ACTION_METADATA_KEY: &str = "pipestream.action";
pub const MAX_CHUNKS_PER_ENTITY: u64 = 65_536;
pub const MAX_JOB_OBSERVERS: usize = 1_024;

/// Server settings for the durable Rust recursive profile.
#[derive(Debug, Clone)]
pub struct RecursiveServerOptions {
    pub bind: SocketAddr,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
    pub state_database: PathBuf,
    pub entity_directory: PathBuf,
    pub ready_file: Option<PathBuf>,
    pub once: bool,
    pub max_scope_depth: u8,
    pub max_entities_per_scope: u32,
    pub max_entity_bytes: usize,
    pub max_chunks_per_entity: u64,
    pub max_concurrent_connections: usize,
}

/// Operator-configurable limits bounded by the implementation's hard safety caps.
#[derive(Debug, Clone, Copy)]
pub struct RecursiveLimits {
    pub max_scope_depth: u8,
    pub max_entities_per_scope: u32,
    pub max_entity_bytes: usize,
    pub max_chunks_per_entity: u64,
}

impl Default for RecursiveLimits {
    fn default() -> Self {
        Self {
            max_scope_depth: 7,
            max_entities_per_scope: pipestream_core::MAX_ENTITY_ID,
            max_entity_bytes: MAX_PAYLOAD,
            max_chunks_per_entity: MAX_CHUNKS_PER_ENTITY,
        }
    }
}

impl RecursiveLimits {
    fn validate(self) -> Result<Self> {
        if self.max_scope_depth > 7 {
            bail!("PIPESTREAM_DEPTH_EXCEEDED: max scope depth exceeds 7");
        }
        if self.max_entities_per_scope == 0
            || self.max_entities_per_scope > pipestream_core::MAX_ENTITY_ID
        {
            bail!("PIPESTREAM_LIMIT_EXCEEDED: invalid entity limit");
        }
        if self.max_entity_bytes == 0 || self.max_entity_bytes > MAX_PAYLOAD {
            bail!(
                "PIPESTREAM_LIMIT_EXCEEDED: max entity bytes must be between 1 and {MAX_PAYLOAD}"
            );
        }
        if self.max_chunks_per_entity == 0 || self.max_chunks_per_entity > MAX_CHUNKS_PER_ENTITY {
            bail!(
                "PIPESTREAM_LIMIT_EXCEEDED: max chunks must be between 1 and {MAX_CHUNKS_PER_ENTITY}"
            );
        }
        Ok(self)
    }
}

/// Client settings shared by recursive and claim-redemption sessions.
#[derive(Debug, Clone)]
pub struct RecursiveClientOptions {
    pub remote: SocketAddr,
    pub ca_certificate: PathBuf,
    pub server_name: String,
    pub identity: Option<ClientIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecursiveScenarioResult {
    pub completion_order: Vec<EntityKey>,
    pub nested_digest: ScopeDigest,
    pub child_digest: ScopeDigest,
}

/// One independently framed piece of a chunked entity.
#[derive(Debug, Clone)]
pub struct EntityChunk {
    pub header: EntityHeader,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ProcessContext<'a> {
    pub execution: &'a ExecutionLease,
    pub session_id: &'a str,
    pub header: &'a EntityHeader,
    pub payload: &'a Payload,
    pub now_micros: u64,
}

#[derive(Debug, Clone)]
pub struct RehydrateContext<'a> {
    pub execution: &'a ExecutionLease,
    pub session: &'a Session,
    pub parent: EntityKey,
}

#[derive(Debug, Clone)]
pub struct ResumeContext<'a> {
    pub execution: &'a ExecutionLease,
    pub session: &'a Session,
    pub entity: EntityKey,
    pub continuation_token: &'a [u8],
}

/// A bounded application decision. Durable protocol transitions are performed by the service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessingDisposition {
    Complete {
        output_digest: [u8; 32],
    },
    Dehydrate,
    Yield {
        reason: u8,
        continuation_token: Vec<u8>,
        validation: StoppingPointValidation,
        expires_at_micros: u64,
    },
    Failed,
}

/// Application behavior embedded in the generic recursive server.
///
/// Calls run in bounded blocking workers outside database transactions. The execution lease
/// fences protocol publication, not external effects; applications must make those
/// idempotent or fence them transactionally. Callbacks must finish within the lease.
pub trait EntityProcessor: Send + Sync + 'static {
    fn process(&self, context: ProcessContext<'_>) -> Result<ProcessingDisposition, ProtocolError>;

    fn rehydrate(&self, context: RehydrateContext<'_>) -> [u8; 32];

    fn resume(&self, context: ResumeContext<'_>) -> [u8; 32];
}

/// Deterministic application profile used by the runnable exemplar and conformance scenarios.
#[derive(Debug, Clone)]
pub struct ExemplarProcessor {
    pub claim_lifetime: Duration,
}

impl Default for ExemplarProcessor {
    fn default() -> Self {
        Self {
            claim_lifetime: Duration::from_secs(300),
        }
    }
}

impl EntityProcessor for ExemplarProcessor {
    fn process(&self, context: ProcessContext<'_>) -> Result<ProcessingDisposition, ProtocolError> {
        Ok(
            match context
                .header
                .metadata
                .get(ACTION_METADATA_KEY)
                .map(String::as_str)
            {
                Some("dehydrate") => ProcessingDisposition::Dehydrate,
                Some("yield") => {
                    let state_checksum =
                        tagged_payload_digest(b"pipestream-stopping-point-v1", context.payload)?;
                    let continuation_token =
                        tagged_payload_digest(b"pipestream-continuation-v1", context.payload)?
                            .to_vec();
                    let lifetime =
                        u64::try_from(self.claim_lifetime.as_micros()).unwrap_or(u64::MAX);
                    ProcessingDisposition::Yield {
                        reason: 1,
                        continuation_token,
                        validation: StoppingPointValidation {
                            state_checksum: Some(state_checksum),
                            bytes_processed: Some(context.payload.len()),
                            children_complete: Some(0),
                            children_total: Some(0),
                            is_resumable: Some(true),
                            checkpoint_ref: Some("durable-yield".to_owned()),
                        },
                        expires_at_micros: context.now_micros.saturating_add(lifetime),
                    }
                }
                Some("fail") => ProcessingDisposition::Failed,
                _ => ProcessingDisposition::Complete {
                    output_digest: tagged_payload_digest(
                        b"pipestream-processed-v1",
                        context.payload,
                    )?,
                },
            },
        )
    }

    fn rehydrate(&self, context: RehydrateContext<'_>) -> [u8; 32] {
        let manifest = context
            .session
            .manifests
            .get(&context.parent)
            .expect("the service only rehydrates registered manifests");
        let mut children = manifest.children.clone();
        children.sort_unstable();
        let mut hasher = Sha256::new();
        hasher.update(b"pipestream-rehydrated-v1");
        hasher.update(context.parent.scope_id.to_be_bytes());
        hasher.update(context.parent.entity_id.to_be_bytes());
        for key in children {
            let record = &context.session.entities[&key];
            hasher.update(key.scope_id.to_be_bytes());
            hasher.update(key.entity_id.to_be_bytes());
            hasher.update(record.output_digest.unwrap_or(record.payload_digest));
        }
        hasher.finalize().into()
    }

    fn resume(&self, context: ResumeContext<'_>) -> [u8; 32] {
        tagged_digest(b"pipestream-resumed-v1", context.continuation_token)
    }
}

fn tagged_digest(tag: &[u8], value: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(tag);
    hasher.update(value);
    hasher.finalize().into()
}

fn tagged_payload_digest(tag: &[u8], payload: &Payload) -> Result<[u8; 32], ProtocolError> {
    let mut reader = payload.reader();
    let mut hasher = Sha256::new();
    hasher.update(tag);
    let mut bytes = [0; PAYLOAD_IO_CHUNK];
    loop {
        let count = reader.read(&mut bytes).map_err(storage_error)?;
        if count == 0 {
            break;
        }
        hasher.update(&bytes[..count]);
    }
    Ok(hasher.finalize().into())
}

pub trait EntityStore: Send + Sync + 'static {
    fn put(&self, session_id: &str, key: EntityKey, payload: &[u8]) -> std::io::Result<()>;

    fn put_payload(
        &self,
        session_id: &str,
        key: EntityKey,
        payload: &Payload,
    ) -> std::io::Result<()>;

    fn spool(&self) -> &Arc<SpoolStore>;

    fn load_payload(
        &self,
        session_id: &str,
        key: EntityKey,
        length: u64,
        digest: [u8; 32],
    ) -> std::io::Result<Payload>;

    fn put_lineage(&self, session_id: &str, digest: [u8; 32]) -> std::io::Result<()>;
}

/// Immutable, fsync-backed payload storage for the standalone server.
#[derive(Debug)]
pub struct FileEntityStore {
    root: PathBuf,
    nonce: AtomicU64,
    spool: Arc<SpoolStore>,
}

impl FileEntityStore {
    pub fn open(root: impl Into<PathBuf>) -> std::io::Result<Self> {
        Self::open_with_spool_limits(root, SpoolLimits::default())
    }

    pub fn open_with_spool_limits(
        root: impl Into<PathBuf>,
        limits: SpoolLimits,
    ) -> std::io::Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        sync_directory(&root)?;
        Ok(Self {
            spool: SpoolStore::new(root.join(".spool"), limits).map_err(std::io::Error::other)?,
            root,
            nonce: AtomicU64::new(1),
        })
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn sync_payload_directories(&self, directory: &Path) -> std::io::Result<()> {
        let mut current = directory;
        loop {
            sync_directory(current)?;
            if current == self.root {
                return Ok(());
            }
            current = current
                .parent()
                .ok_or_else(|| std::io::Error::other("payload directory escaped store root"))?;
        }
    }

    fn entity_path(&self, session_id: &str, key: EntityKey) -> PathBuf {
        self.root
            .join(session_id)
            .join(format!("scope-{}", key.scope_id))
            .join(format!("entity-{}.bin", key.entity_id))
    }

    fn write_immutable(&self, path: &Path, bytes: &[u8]) -> std::io::Result<()> {
        let parent = path.parent().expect("stored paths always have a parent");
        fs::create_dir_all(parent)?;
        if path.exists() {
            let existing = fs::read(path)?;
            if existing == bytes {
                return Ok(());
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "immutable PipeStream object exists with different bytes",
            ));
        }
        let nonce = self.nonce.fetch_add(1, Ordering::Relaxed);
        let temporary = parent.join(format!(".pipestream-{}-{nonce}.tmp", std::process::id()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        match fs::hard_link(&temporary, path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let existing = fs::read(path)?;
                if existing != bytes {
                    let _ = fs::remove_file(&temporary);
                    return Err(error);
                }
            }
            Err(error) => {
                let _ = fs::remove_file(&temporary);
                return Err(error);
            }
        }
        fs::remove_file(&temporary)?;
        sync_directory(parent)
    }
}

impl EntityStore for FileEntityStore {
    fn load_payload(
        &self,
        session_id: &str,
        key: EntityKey,
        length: u64,
        digest: [u8; 32],
    ) -> std::io::Result<Payload> {
        validate_storage_session_id(session_id)?;
        Payload::open_retained(self.entity_path(session_id, key), length, digest)
    }

    fn spool(&self) -> &Arc<SpoolStore> {
        &self.spool
    }

    fn put_payload(
        &self,
        session_id: &str,
        key: EntityKey,
        payload: &Payload,
    ) -> std::io::Result<()> {
        validate_storage_session_id(session_id)?;
        let destination = self.entity_path(session_id, key);
        let parent = destination.parent().expect("entity parent directory");
        fs::create_dir_all(parent)?;
        let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
        let mut reader = payload.reader();
        let mut bytes = [0; PAYLOAD_IO_CHUNK];
        let mut digest = Sha256::new();
        let mut length = 0u64;
        loop {
            let count = reader.read(&mut bytes)?;
            if count == 0 {
                break;
            }
            temporary.write_all(&bytes[..count])?;
            digest.update(&bytes[..count]);
            length += count as u64;
        }
        let actual: [u8; 32] = digest.finalize().into();
        if length != payload.len() || actual != payload.digest() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "spooled payload changed before persistence",
            ));
        }
        temporary.as_file().sync_all()?;
        match temporary.persist_noclobber(&destination) {
            Ok(_) => self.sync_payload_directories(parent),
            Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                let mut existing = fs::File::open(&destination)?;
                let mut digest = Sha256::new();
                let mut length = 0u64;
                loop {
                    let count = existing.read(&mut bytes)?;
                    if count == 0 {
                        break;
                    }
                    length += count as u64;
                    digest.update(&bytes[..count]);
                }
                let actual: [u8; 32] = digest.finalize().into();
                if length != payload.len() || actual != payload.digest() {
                    return Err(error.error);
                }
                self.sync_payload_directories(parent)
            }
            Err(error) => Err(error.error),
        }
    }
    fn put(&self, session_id: &str, key: EntityKey, payload: &[u8]) -> std::io::Result<()> {
        validate_storage_session_id(session_id)?;
        self.write_immutable(&self.entity_path(session_id, key), payload)
    }

    fn put_lineage(&self, session_id: &str, digest: [u8; 32]) -> std::io::Result<()> {
        validate_storage_session_id(session_id)?;
        self.write_immutable(&self.root.join(session_id).join("lineage.sha256"), &digest)
    }
}

fn validate_storage_session_id(session_id: &str) -> std::io::Result<()> {
    validate_session_id(session_id)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
}

pub struct RecursiveService<P, E = FileEntityStore> {
    store: Arc<SqliteSessionStore>,
    entities: Arc<E>,
    processor: Arc<P>,
    capabilities: Capabilities,
    limits: RecursiveLimits,
    authentication: Option<Arc<AuthenticationPolicy>>,
    caller: Option<PrincipalBinding>,
    execution_lease_micros: u64,
    execution_limits: ExecutionLimits,
    storage_pool: Arc<storage::StoragePool>,
}

impl<P, E> Clone for RecursiveService<P, E> {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            entities: Arc::clone(&self.entities),
            processor: Arc::clone(&self.processor),
            capabilities: self.capabilities.clone(),
            limits: self.limits,
            authentication: self.authentication.clone(),
            caller: self.caller.clone(),
            execution_lease_micros: self.execution_lease_micros,
            execution_limits: self.execution_limits,
            storage_pool: self.storage_pool.clone(),
        }
    }
}

impl<P: EntityProcessor, E: EntityStore> RecursiveService<P, E> {
    pub fn new(
        store: Arc<SqliteSessionStore>,
        entities: Arc<E>,
        processor: Arc<P>,
        max_scope_depth: u8,
        max_entities_per_scope: u32,
    ) -> Result<Self> {
        Self::with_limits(
            store,
            entities,
            processor,
            RecursiveLimits {
                max_scope_depth,
                max_entities_per_scope,
                ..RecursiveLimits::default()
            },
        )
    }

    pub fn with_limits(
        store: Arc<SqliteSessionStore>,
        entities: Arc<E>,
        processor: Arc<P>,
        limits: RecursiveLimits,
    ) -> Result<Self> {
        let limits = limits.validate()?;
        let mut capabilities =
            recursive_capabilities(limits.max_scope_depth, limits.max_entities_per_scope)?;
        capabilities
            .extensions
            .supported
            .push(EXTENSION_SEALED_WORK_SETS);
        let storage_pool = storage::StoragePool::open(store.path())?;
        Ok(Self {
            store,
            entities,
            processor,
            capabilities,
            limits,
            authentication: None,
            caller: None,
            execution_lease_micros: MAX_EXECUTION_LEASE_MICROS,
            execution_limits: ExecutionLimits::default(),
            storage_pool,
        })
    }

    #[must_use]
    pub fn store(&self) -> &Arc<SqliteSessionStore> {
        &self.store
    }

    /// Set the publication lease, not a callback cancellation or CPU-time limit.
    pub fn with_execution_lease(mut self, duration: Duration) -> Result<Self> {
        let micros = u64::try_from(duration.as_micros())?;
        if !(1..=MAX_EXECUTION_LEASE_MICROS).contains(&micros) {
            bail!(
                "PIPESTREAM_LIMIT_EXCEEDED: execution lease must be 1 microsecond to 300 seconds"
            );
        }
        self.execution_lease_micros = micros;
        Ok(self)
    }

    pub fn with_execution_limits(mut self, limits: ExecutionLimits) -> Result<Self> {
        limits.validate()?;
        self.execution_limits = limits;
        Ok(self)
    }

    /// Require mutual TLS and use this authority's explicit principal mapping.
    pub fn with_authentication(mut self, policy: AuthenticationPolicy) -> Self {
        self.authentication = Some(Arc::new(policy));
        if !self
            .capabilities
            .extensions
            .supported
            .contains(&EXTENSION_AUTHENTICATED_RECOVERY)
        {
            self.capabilities
                .extensions
                .supported
                .push(EXTENSION_AUTHENTICATED_RECOVERY);
            self.capabilities.extensions.supported.sort_unstable();
        }
        for ids in [
            &mut self.capabilities.extensions.supported,
            &mut self.capabilities.extensions.required,
        ] {
            if !ids.contains(&EXTENSION_AUTHENTICATED_SESSIONS) {
                ids.push(EXTENSION_AUTHENTICATED_SESSIONS);
                ids.sort_unstable();
            }
        }
        self
    }

    fn transact<T>(
        &self,
        session_id: &str,
        operation: impl FnOnce(&mut Session) -> Result<T, ProtocolError>,
    ) -> Result<(T, pipestream_core::persistence::VersionedSession), StoreError> {
        self.store.transact(session_id, |session| {
            session.authorize(self.caller.as_ref())?;
            operation(session)
        })
    }

    fn bind_new_session(&self, session: &mut Session) -> Result<(), ProtocolError> {
        if let Some(caller) = &self.caller {
            session.bind_owner(caller.clone())?;
        }
        Ok(())
    }

    /// Blocking operator entry point for queued resume jobs; servers use periodic dispatch.
    pub fn recover_interrupted_resumptions(&self) -> Result<usize> {
        let pool = WorkerPool::open(self.store.path(), self.execution_limits)?;
        let mut recovered = 0;
        for job in self
            .store
            .ready_jobs(now_micros()?, self.store.job_limits().total)?
        {
            if !matches!(job.key.stage, ExecutionStage::Resume { .. }) || !self.permits_job(&job) {
                continue;
            }
            let Some(_permit) = pool.acquire(job.principal.as_ref(), &job.session_id, job.key)?
            else {
                continue;
            };
            let mut executor = self.clone();
            executor.caller = job.principal.clone();
            let executed = executor.execute_job(&job)?;
            if let Some(current) = self.store.load(&job.session_id)? {
                if executed
                    && matches!(
                        current.session.jobs[&job.key].state,
                        JobState::Finished(JobOutput::Resumed)
                    )
                {
                    recovered += 1;
                }
                if let Ok(lineage) = current.session.final_lineage_digest() {
                    self.entities.put_lineage(&job.session_id, lineage)?;
                }
            }
        }
        Ok(recovered)
    }

    pub async fn handle_connection(
        &self,
        connection: &quinn::Connection,
    ) -> Result<(), ProtocolError> {
        let mut service = self.clone();
        service.caller = self
            .authentication
            .as_ref()
            .map(|policy| policy.authenticate(connection))
            .transpose()?;
        service.handle_authorized_connection(connection).await
    }

    async fn handle_authorized_connection(
        &self,
        connection: &quinn::Connection,
    ) -> Result<(), ProtocolError> {
        let (mut control_send, mut control_recv) =
            connection.accept_bi().await.map_err(frame_error)?;
        let (frame_type, payload) = read_control(&mut control_recv).await?;
        if frame_type != FRAME_CAPABILITIES {
            return Err(frame_error("first frame must be CAPABILITIES"));
        }
        let peer = decode_capabilities(&payload)?;
        let negotiated = self.capabilities.negotiate(&peer)?;
        let sealed = negotiated
            .extensions
            .supported
            .contains(&EXTENSION_SEALED_WORK_SETS);
        if sealed && (!negotiated.layer1_recursive || negotiated.layer2_resilience) {
            return Err(extension_error(
                "sealed work sets require Layer 1 without Layer 2",
            ));
        }
        let layers = layers(&negotiated);
        let authenticated_recovery = negotiated
            .extensions
            .supported
            .contains(&EXTENSION_AUTHENTICATED_RECOVERY);
        if authenticated_recovery
            && (!layers.layer2_resilience
                || sealed
                || self.caller.is_none()
                || !negotiated
                    .extensions
                    .required
                    .contains(&EXTENSION_AUTHENTICATED_SESSIONS)
                || !negotiated
                    .extensions
                    .required
                    .contains(&EXTENSION_AUTHENTICATED_RECOVERY))
        {
            return Err(extension_error(
                "authenticated recovery requires mutual TLS, required session binding and Layer 2 without sealed work",
            ));
        }
        write_control(&mut control_send, &encode_capabilities(&negotiated)?).await?;
        write_control(
            &mut control_send,
            &encode_status(Status {
                state: pipestream_core::STATUS_UNSPECIFIED,
                entity_id: CONNECTION_LEVEL,
                scope_id: 0,
                cursor: None,
                depth: 0,
            })?,
        )
        .await?;

        let spool = self
            .entities
            .spool()
            .connection(self.caller.as_ref(), self.limits.max_entity_bytes as u64)?;
        let deadlines = Arc::new(control::Deadlines::default());
        let (_readers, incoming, mut failures) = ingress::start(
            connection.clone(),
            control_recv,
            layers,
            self.limits,
            spool,
            deadlines.clone(),
        );
        tokio::select! {
            biased;
            Some(error) = failures.recv() => Err(error),
            error = deadlines.expired() => Err(error),
            result = self.dispatch_connection(connection, control_send, negotiated, incoming, &deadlines) => result,
        }
    }

    async fn dispatch_connection(
        &self,
        connection: &quinn::Connection,
        mut control_send: quinn::SendStream,
        negotiated: Capabilities,
        mut incoming: tokio::sync::mpsc::Receiver<Result<ingress::Event, ProtocolError>>,
        deadlines: &control::Deadlines,
    ) -> Result<(), ProtocolError> {
        let layers = layers(&negotiated);
        let sealed = negotiated
            .extensions
            .supported
            .contains(&EXTENSION_SEALED_WORK_SETS);
        let authenticated_recovery = negotiated
            .extensions
            .supported
            .contains(&EXTENSION_AUTHENTICATED_RECOVERY);
        let mut current_session: Option<String> = None;
        let mut establishing_session: Option<String> = None;
        let mut initial_entities = VecDeque::new();
        let mut unadmitted = BTreeSet::new();
        let mut announcements: BTreeMap<EntityKey, StatusFrame> = BTreeMap::new();
        let mut chunks = ingress::Chunks::default();
        let admission_pool = self
            .storage(|service| {
                WorkerPool::admission(service.store.path(), service.execution_limits)
            })
            .await?;
        let mut preparing: tokio::task::JoinSet<Result<(String, ExecutionKey), ProtocolError>> =
            tokio::task::JoinSet::new();
        let mut replies = BTreeMap::new();
        let mut checkpoints: BTreeMap<(u32, u64), (Checkpoint, usize)> = BTreeMap::new();
        let mut tick = tokio::time::interval(Duration::from_millis(10));
        loop {
            if let Some(id) = current_session.as_deref() {
                self.load_session(id).await?.ok_or_else(unauthorized)?;
            }
            self.flush_job_replies(&mut control_send, current_session.as_deref(), &mut replies)
                .await?;
            // A worker may commit between the reply snapshot and checkpoint snapshot.
            // Deliver observed outcomes before a checkpoint that covers their entities.
            let unsettled = announcements
                .keys()
                .chain(unadmitted.iter())
                .chain(replies.keys().map(|key| &key.entity))
                .copied()
                .collect();
            self.flush_checkpoints(
                &mut control_send,
                current_session.as_deref(),
                &mut checkpoints,
                &unsettled,
                layers,
                deadlines,
            )
            .await?;
            let event = tokio::select! {
                prepared = preparing.join_next(), if !preparing.is_empty() => {
                    let (session_id, key) = prepared.expect("preparation exists").map_err(storage_error)??;
                    if current_session.as_ref().is_some_and(|current| current != &session_id) {
                        return Err(entity_error("prepared entity changed connection session"));
                    }
                    current_session = Some(session_id);
                    establishing_session = None;
                    let state = self.load_session(current_session.as_deref().unwrap()).await?.ok_or_else(|| entity_error("admitted session is absent"))?;
                    unadmitted.remove(&key.entity);
                    write_status(&mut control_send, EntityState::Processing, key.entity, state.session.entities[&key.entity].depth, None).await?;
                    replies.insert(key, JobReply::Process);
                    continue;
                }
                deferred = async { initial_entities.pop_front().expect("initial entity exists") }, if current_session.is_some() && !initial_entities.is_empty() => Ok(ingress::Event::Entity(deferred)),
                _ = tick.tick() => continue,
                event = incoming.recv() => event.ok_or_else(|| frame_error("connection readers stopped"))?,
            };
            let event = match event {
                Ok(event) => event,
                Err(_error)
                    if self
                        .accepts_durable_disconnect(connection, current_session.as_deref())
                        .await? =>
                {
                    return Ok(());
                }
                Err(error) => return Err(error),
            };
            let (frame_type, payload) = match event {
                ingress::Event::Control(kind, bytes) => (kind, bytes),
                ingress::Event::Entity(received) => {
                    unadmitted.insert(EntityKey {
                        scope_id: received.header.scope_id.unwrap_or(0),
                        entity_id: received.header.entity_id,
                    });
                    if current_session.is_none() && !preparing.is_empty() {
                        if received.header.scope_id.unwrap_or(0) != 0
                            || received.header.parent_id.is_some()
                        {
                            return Err(entity_error("child precedes root admission"));
                        }
                        if received
                            .header
                            .metadata
                            .get(SESSION_METADATA_KEY)
                            .is_some_and(|id| Some(id) != establishing_session.as_ref())
                        {
                            return Err(entity_error("entity metadata changed session identity"));
                        }
                        if initial_entities.len() + preparing.len() >= MAX_JOB_OBSERVERS {
                            return Err(limit_error("connection job observation limit exhausted"));
                        }
                        initial_entities.push_back(received);
                        continue;
                    }
                    let Some(received) = chunks.insert(*received, self.limits)? else {
                        continue;
                    };
                    let header = &received.header;
                    let key = EntityKey {
                        scope_id: header.scope_id.unwrap_or(0),
                        entity_id: header.entity_id,
                    };
                    let depth = if key.scope_id == 0 {
                        0
                    } else {
                        let session_id = current_session
                            .as_deref()
                            .ok_or_else(|| entity_error("child precedes root admission"))?;
                        let current = self
                            .load_session(session_id)
                            .await?
                            .ok_or_else(|| entity_error("session is absent"))?;
                        let parent = EntityKey {
                            scope_id: header.parent_scope_id.unwrap_or(0),
                            entity_id: header.parent_id.unwrap_or(0),
                        };
                        current
                            .session
                            .entities
                            .get(&parent)
                            .ok_or_else(|| entity_error("parent is not admitted"))?
                            .depth
                            + 1
                    };
                    let pending = announcements.remove(&key).unwrap_or(StatusFrame {
                        status: Status {
                            state: pipestream_core::STATUS_PENDING,
                            entity_id: key.entity_id,
                            scope_id: key.scope_id,
                            depth,
                            cursor: None,
                        },
                        extension: None,
                    });
                    if replies.len() + preparing.len() + initial_entities.len() >= MAX_JOB_OBSERVERS
                    {
                        return Err(limit_error("connection job observation limit exhausted"));
                    }
                    let id = current_session
                        .as_deref()
                        .or_else(|| {
                            header
                                .metadata
                                .get(SESSION_METADATA_KEY)
                                .map(String::as_str)
                        })
                        .ok_or_else(|| entity_error("entity lacks session identity"))?;
                    validate_session_id(id)?;
                    let execution = ExecutionKey {
                        entity: key,
                        stage: ExecutionStage::Process,
                    };
                    let permit = admission_pool
                        .acquire(self.caller.as_ref(), id, execution)?
                        .ok_or_else(|| limit_error("payload installation capacity exhausted"))?;
                    if current_session.is_none() {
                        establishing_session = Some(id.to_owned());
                    }
                    let service = self.clone();
                    let current = current_session.clone();
                    let negotiated = negotiated.clone();
                    preparing.spawn_blocking(move || {
                        let _permit = permit;
                        let received = received.finish()?;
                        service.prepare_entity(
                            current.as_deref(),
                            &pending,
                            &received.header,
                            &received.body,
                            &negotiated,
                        )
                    });
                    continue;
                }
            };
            match frame_type {
                FRAME_WORK_SET => {
                    if !sealed {
                        return Err(extension_error("WORK_SET was not negotiated"));
                    }
                    let request = work_set::decode(&payload)?;
                    if current_session
                        .as_ref()
                        .is_some_and(|id| id != &request.session_id)
                    {
                        return Err(entity_error("WORK_SET changed the connection session"));
                    }
                    let requested = request.clone();
                    let attached = current_session.is_some();
                    let negotiated = negotiated.clone();
                    let ack = self
                        .storage(move |service| {
                            let request = requested;
                            let existing = service
                                .store
                                .load(&request.session_id)
                                .map_err(store_error)?;
                            if let Some(retained) = &existing {
                                retained.session.authorize(service.caller.as_ref())?;
                            }
                            if !attached && (request.scope_id != 0 || request.sequence != 0) {
                                return Err(entity_error(
                                    "attach with the root WORK_SET sequence zero",
                                ));
                            }
                            let ack = if existing.is_none() {
                                if attached || request.scope_id != 0 || request.sequence != 0 {
                                    return Err(entity_error(
                                        "first WORK_SET must declare root sequence zero",
                                    ));
                                }
                                let mut session = Session::new_sealed(
                                    &request.session_id,
                                    request.producer_id,
                                    negotiated.effective_max_scope_depth(),
                                    negotiated.effective_max_entities_per_scope(),
                                )?;
                                service.bind_new_session(&mut session)?;
                                let ack = session
                                    .declare_work(&request, now_micros().map_err(storage_error)?)?;
                                service.store.create(&session).map_err(store_error)?;
                                ack
                            } else {
                                service
                                    .transact(&request.session_id, |session| {
                                        if session.max_scope_depth
                                            > negotiated.effective_max_scope_depth()
                                            || session.max_entities_per_scope
                                                > negotiated.effective_max_entities_per_scope()
                                        {
                                            return Err(extension_error(
                                                "connection limits cannot resume this session",
                                            ));
                                        }
                                        session.declare_work(
                                            &request,
                                            now_micros().map_err(storage_error)?,
                                        )
                                    })
                                    .map_err(store_error)?
                                    .0
                            };
                            Ok(ack)
                        })
                        .await?;
                    current_session = Some(request.session_id);
                    write_control(&mut control_send, &work_set::encode(&ack)?).await?;
                }
                FRAME_STATUS => {
                    let pending = decode_status_frame(&payload, layers)?;
                    if pending.status.state == pipestream_core::STATUS_UNSPECIFIED
                        && pending.status.entity_id == CONNECTION_LEVEL
                        && pending.status.cursor.is_none()
                    {
                        continue;
                    }
                    if pending.status.state != pipestream_core::STATUS_PENDING
                        || pending.extension.is_some()
                        || (sealed && pending.status.cursor.is_some())
                    {
                        return Err(entity_error("entity announcement must be plain PENDING"));
                    }
                    let key = EntityKey {
                        scope_id: pending.status.scope_id,
                        entity_id: pending.status.entity_id,
                    };
                    if sealed {
                        let session_id = current_session
                            .as_deref()
                            .ok_or_else(|| entity_error("PENDING precedes work-set declaration"))?;
                        let current = self
                            .load_session(session_id)
                            .await?
                            .ok_or_else(|| entity_error("session is absent"))?;
                        let scope = current
                            .session
                            .scopes
                            .get(&key.scope_id)
                            .ok_or_else(|| entity_error("PENDING scope has no declaration"))?;
                        current.session.work_admission(key, scope.parent)?;
                        if pending.status.depth != scope.depth {
                            return Err(entity_error("PENDING depth differs from declared scope"));
                        }
                    }
                    if let Some(session_id) = current_session.as_deref()
                        && let Some(current) = self.load_session(session_id).await?
                        && let Some(entity) = current.session.entities.get(&key)
                    {
                        if entity.depth != pending.status.depth {
                            return Err(entity_error(
                                "late PENDING depth differs from admitted entity",
                            ));
                        }
                        continue;
                    }
                    if announcements.len() >= negotiated.max_window_size as usize {
                        return Err(limit_error("PENDING window exhausted"));
                    }
                    if let Some(previous) = announcements.insert(key, pending.clone())
                        && previous != pending
                    {
                        return Err(entity_error("conflicting PENDING announcement"));
                    }
                }
                FRAME_SCOPE_DIGEST => {
                    if !layers.layer1_recursive {
                        return Err(layer_error("SCOPE_DIGEST requires Layer 1"));
                    }
                    let session_id = current_session.as_deref().ok_or_else(|| {
                        entity_error("SCOPE_DIGEST precedes session establishment")
                    })?;
                    if replies.len() + preparing.len() + initial_entities.len() >= MAX_JOB_OBSERVERS
                    {
                        return Err(limit_error("connection job observation limit exhausted"));
                    }
                    let session_id = session_id.to_owned();
                    let digest = decode_scope_digest(&payload)?;
                    let key = self
                        .storage(move |service| service.enqueue_rehydration(&session_id, digest))
                        .await?;
                    replies.insert(key, JobReply::Rehydrate);
                }
                FRAME_BARRIER => {
                    if !layers.layer1_recursive {
                        return Err(layer_error("BARRIER requires Layer 1"));
                    }
                    let request = decode_barrier(&payload)?;
                    if request.released {
                        return Err(entity_error("BARRIER request has the released bit set"));
                    }
                    let session_id = current_session
                        .as_deref()
                        .ok_or_else(|| entity_error("BARRIER precedes session establishment"))?;
                    let versioned = self
                        .load_session(session_id)
                        .await?
                        .ok_or_else(|| entity_error("session is absent"))?;
                    let response = versioned.session.barrier(request.scope_id)?;
                    if response.parent_entity_id != request.parent_entity_id {
                        return Err(entity_error("BARRIER parent does not match the scope"));
                    }
                    write_control(&mut control_send, &encode_barrier(response)?).await?;
                }
                FRAME_CHECKPOINT => {
                    let request = decode_checkpoint_for(&payload, layers)?;
                    let session_id = current_session
                        .as_deref()
                        .ok_or_else(|| entity_error("CHECKPOINT precedes session establishment"))?;
                    let id = session_id.to_owned();
                    let requested = request.clone();
                    self.storage(move |service| {
                        service
                            .transact(&id, |session| session.request_checkpoint(&requested))
                            .map_err(store_error)
                            .map(|_| ())
                    })
                    .await?;
                    let key = (request.scope_id.unwrap_or(0), request.sequence_number);
                    if checkpoints.len() >= 1024 && !checkpoints.contains_key(&key) {
                        return Err(limit_error("pending checkpoint limit exhausted"));
                    }
                    checkpoints.entry(key).or_insert((request, 0)).1 += 1;
                }
                FRAME_RECOVERY => {
                    if !authenticated_recovery {
                        return Err(extension_error("authenticated recovery was not negotiated"));
                    }
                    let RecoveryFrame::Request(request) = recovery::decode(&payload)? else {
                        return Err(frame_error("recovery request carries receipt fields"));
                    };
                    if self
                        .caller
                        .as_ref()
                        .is_none_or(|caller| caller.authority != request.authority)
                    {
                        return Err(unauthorized());
                    }
                    if current_session
                        .as_ref()
                        .or(establishing_session.as_ref())
                        .is_some_and(|id| id != &request.session_id)
                    {
                        return Err(entity_error("connection changed session identity"));
                    }
                    if replies.len() + preparing.len() + initial_entities.len() >= MAX_JOB_OBSERVERS
                    {
                        return Err(limit_error("connection job observation limit exhausted"));
                    }
                    let requested = request.clone();
                    let receipt = self
                        .storage(move |service| {
                            let request = requested;
                            service
                                .transact(&request.session_id, |session| {
                                    session.accept_recovery(
                                        service.caller.as_ref(),
                                        &request,
                                        now_micros().map_err(storage_error)?,
                                    )
                                })
                                .map_err(|error| match error {
                                    StoreError::NotFound(_) => unauthorized(),
                                    other => store_error(other),
                                })
                                .map(|result| result.0)
                        })
                        .await?;
                    current_session = Some(request.session_id);
                    write_control(
                        &mut control_send,
                        &recovery::encode(&RecoveryFrame::Receipt(receipt.clone()))?,
                    )
                    .await?;
                    replies.insert(receipt.execution_key(), JobReply::Recovery(receipt));
                }
                FRAME_CLAIM_REDEMPTION => {
                    if authenticated_recovery {
                        return Err(extension_error(
                            "use retained recovery requests in this profile",
                        ));
                    }
                    if sealed {
                        return Err(extension_error(
                            "claim redemption is outside the sealed-work profile",
                        ));
                    }
                    if !layers.layer2_resilience {
                        return Err(layer_error("claim redemption requires Layer 2"));
                    }
                    let request = decode_claim_redemption(&payload)?;
                    if request.acknowledged {
                        return Err(frame_error("claim redemption request carries ACK"));
                    }
                    if current_session
                        .as_ref()
                        .or(establishing_session.as_ref())
                        .is_some_and(|value| value != &request.session_id)
                    {
                        return Err(entity_error("connection changed session identity"));
                    }
                    current_session = Some(request.session_id.clone());
                    if replies.len() + preparing.len() + initial_entities.len() >= MAX_JOB_OBSERVERS
                    {
                        return Err(limit_error("connection job observation limit exhausted"));
                    }
                    let requested = request.clone();
                    let key = self
                        .storage(move |service| service.enqueue_redemption(&requested))
                        .await?;
                    replies.insert(key, JobReply::Resume(request));
                }
                FRAME_GOAWAY => {
                    if !checkpoints.is_empty()
                        || !announcements.is_empty()
                        || !chunks.is_empty()
                        || !preparing.is_empty()
                        || !initial_entities.is_empty()
                        || !replies.is_empty()
                    {
                        return Err(entity_error("GOAWAY precedes outstanding work"));
                    }
                    let last = decode_goaway(&payload)?;
                    if sealed {
                        let id = current_session
                            .as_deref()
                            .ok_or_else(|| entity_error("GOAWAY lacks session"))?;
                        let state = self
                            .load_session(id)
                            .await?
                            .ok_or_else(|| entity_error("session is absent"))?
                            .session;
                        if !state.work_scope_ready(0)
                            || !state.checkpoints.values().any(|cp| {
                                cp.scope_id.unwrap_or(0) == 0
                                    && cp.acknowledged
                                    && cp.checkpoint_entity_id == last
                            })
                        {
                            return Err(entity_error(
                                "GOAWAY requires an acknowledged sealed root checkpoint",
                            ));
                        }
                    }
                    write_control(&mut control_send, &encode_goaway(last)?).await?;
                    control_send.finish().map_err(frame_error)?;
                    let _ = tokio::time::timeout(Duration::from_secs(5), connection.closed()).await;
                    return Ok(());
                }
                FRAME_CAPABILITIES => return Err(frame_error("duplicate CAPABILITIES")),
                _ => continue,
            }
        }
    }

    async fn flush_checkpoints(
        &self,
        control: &mut quinn::SendStream,
        session_id: Option<&str>,
        pending: &mut BTreeMap<(u32, u64), (Checkpoint, usize)>,
        unsettled: &BTreeSet<EntityKey>,
        layers: LayerSupport,
        deadlines: &control::Deadlines,
    ) -> Result<(), ProtocolError> {
        let Some(session_id) = session_id else {
            return Ok(());
        };
        let mut resolved = Vec::new();
        for (&(scope, sequence), (request, count)) in pending.iter() {
            deadlines.check((scope, sequence))?;
            let current = self
                .load_session(session_id)
                .await?
                .ok_or_else(|| entity_error("session is absent"))?;
            if unsettled.iter().any(|key| {
                let mut key = *key;
                loop {
                    if key.scope_id == scope {
                        return if current.session.work_sets.is_some() {
                            key.entity_id <= request.checkpoint_entity_id
                        } else {
                            pipestream_core::session::is_before(
                                key.entity_id,
                                request.checkpoint_entity_id,
                            )
                        };
                    }
                    let Some(parent) = current
                        .session
                        .scopes
                        .get(&key.scope_id)
                        .and_then(|scope| scope.parent)
                    else {
                        return false;
                    };
                    key = parent;
                }
            }) {
                continue;
            }
            if !current.session.checkpoint_satisfied(scope, sequence)? {
                continue;
            }
            let id = session_id.to_owned();
            let ack = self
                .storage(move |service| {
                    let (ack, versioned) = service
                        .transact(&id, |session| {
                            if session.checkpoint_satisfied(scope, sequence)? {
                                session.acknowledge_checkpoint(scope, sequence).map(Some)
                            } else {
                                Ok(None)
                            }
                        })
                        .map_err(store_error)?;
                    if ack.is_some()
                        && let Ok(lineage) = versioned.session.final_lineage_digest()
                    {
                        service
                            .entities
                            .put_lineage(&id, lineage)
                            .map_err(storage_error)?;
                    }
                    Ok(ack)
                })
                .await?;
            if let Some(ack) = ack {
                deadlines.check((scope, sequence))?;
                write_control(control, &encode_checkpoint_for(&ack, layers)?).await?;
                deadlines.acknowledged((scope, sequence), *count)?;
                resolved.push((scope, sequence));
            }
        }
        for key in resolved {
            pending.remove(&key);
        }
        Ok(())
    }

    async fn accepts_durable_disconnect(
        &self,
        connection: &quinn::Connection,
        session_id: Option<&str>,
    ) -> Result<bool, ProtocolError> {
        let clean_close = matches!(
            connection.close_reason(),
            Some(quinn::ConnectionError::ApplicationClosed(close))
                if close.error_code == ERROR_NO_ERROR.into()
        );
        if !clean_close {
            return Ok(false);
        }
        let Some(session_id) = session_id else {
            return Ok(false);
        };
        Ok(self
            .load_session(session_id)
            .await?
            .is_some_and(|versioned| {
                versioned
                    .session
                    .jobs
                    .values()
                    .any(|job| job.state.is_unfinished())
                    || versioned.session.claims.values().any(|claim| {
                        claim.redeemed_at_micros.is_none()
                            && versioned.session.entities[&claim.entity].state
                                == EntityState::Deferred
                    })
            }))
    }

    fn prepare_entity(
        &self,
        current_session: Option<&str>,
        pending: &StatusFrame,
        header: &EntityHeader,
        body: &Payload,
        negotiated: &Capabilities,
    ) -> Result<(String, ExecutionKey), ProtocolError> {
        let scope_id = header.scope_id.unwrap_or(0);
        let sealed = negotiated
            .extensions
            .supported
            .contains(&EXTENSION_SEALED_WORK_SETS);
        if sealed && current_session.is_none() {
            return Err(entity_error("entity precedes WORK_SET acknowledgment"));
        }
        if pending.status.entity_id != header.entity_id || pending.status.scope_id != scope_id {
            return Err(entity_error("PENDING and EntityHeader identity differ"));
        }
        let session_id = current_session
            .map(str::to_owned)
            .or_else(|| header.metadata.get(SESSION_METADATA_KEY).cloned())
            .ok_or_else(|| entity_error("root entity lacks pipestream.session-id metadata"))?;
        validate_session_id(&session_id)?;
        if header
            .metadata
            .get(SESSION_METADATA_KEY)
            .is_some_and(|value| value != &session_id)
        {
            return Err(entity_error("entity metadata changed session identity"));
        }
        let now = now_micros().map_err(storage_error)?;
        let key = ExecutionKey {
            entity: EntityKey {
                scope_id,
                entity_id: header.entity_id,
            },
            stage: ExecutionStage::Process,
        };
        let input = JobInput::Process {
            header: header.clone(),
            length: body.len(),
            digest: body.digest(),
            layers: layers(negotiated),
        };
        let existing = self.store.load(&session_id).map_err(store_error)?;
        if let Some(existing) = &existing {
            existing.session.authorize(self.caller.as_ref())?;
        }
        if current_session.is_none() && existing.is_some() {
            return Err(entity_error(
                "legacy session already exists; explicit attachment is required",
            ));
        }
        let mut snapshot = if let Some(existing) = &existing {
            existing.session.authorize(self.caller.as_ref())?;
            existing.session.clone()
        } else {
            if current_session.is_some() {
                return Err(entity_error("session is absent"));
            }
            if scope_id != 0 || header.parent_id.is_some() {
                return Err(entity_error("first entity must be a root entity"));
            }
            let mut session = Session::new(
                &session_id,
                negotiated.effective_max_scope_depth(),
                negotiated.effective_max_entities_per_scope(),
            )?;
            self.bind_new_session(&mut session)?;
            session
        };
        let admit = |session: &mut Session| {
            if scope_id == 0 {
                if header.parent_id.is_some() || pending.status.depth != 0 {
                    return Err(entity_error("root entity has a parent or nonzero depth"));
                }
                session.add_root(new_entity(header, body))?;
            } else {
                let parent = EntityKey {
                    scope_id: header
                        .parent_scope_id
                        .ok_or_else(|| entity_error("scoped entity lacks parent-scope-id"))?,
                    entity_id: header
                        .parent_id
                        .ok_or_else(|| entity_error("scoped entity lacks parent-id"))?,
                };
                let depth = session
                    .entities
                    .get(&parent)
                    .ok_or_else(|| entity_error("scoped entity parent is absent"))?
                    .depth
                    .checked_add(1)
                    .ok_or_else(|| entity_error("scope depth overflows"))?;
                if pending.status.depth != depth {
                    return Err(entity_error("PENDING depth differs from parent depth"));
                }
                match session.scopes.get(&scope_id) {
                    None => session.open_child_scope(parent, scope_id, now)?,
                    Some(scope) if scope.parent == Some(parent) => {}
                    Some(_) => return Err(entity_error("scope parent changed")),
                }
                session.add_child(scope_id, new_entity(header, body))?;
            }
            session.transition(key.entity, EntityState::Processing)?;
            session.enqueue_job(key, input.clone(), now)
        };
        admit(&mut snapshot)?;
        // Installation precedes atomic admission. Failure never creates a runnable job.
        // An interrupted installation can leave an orphan, not a successful receipt.
        self.entities
            .put_payload(&session_id, key.entity, body)
            .map_err(storage_error)?;
        if existing.is_some() {
            self.transact(&session_id, admit).map_err(store_error)?;
        } else {
            self.store.create(&snapshot).map_err(store_error)?;
        }
        Ok((session_id, key))
    }

    fn enqueue_rehydration(
        &self,
        session_id: &str,
        expected: ScopeDigest,
    ) -> Result<ExecutionKey, ProtocolError> {
        self.transact(session_id, |session| {
            let actual = session.close_scope_with_expected(&expected)?;
            let parent = session.scopes[&actual.scope_id]
                .parent
                .ok_or_else(|| entity_error("scope parent is absent"))?;
            let key = ExecutionKey {
                entity: parent,
                stage: ExecutionStage::Rehydrate,
            };
            if !session.jobs.contains_key(&key) {
                session.begin_rehydration(parent)?;
            }
            session.enqueue_job(
                key,
                JobInput::Rehydrate { digest: actual },
                now_micros().map_err(storage_error)?,
            )?;
            Ok(key)
        })
        .map_err(store_error)
        .map(|result| result.0)
    }

    fn enqueue_redemption(&self, request: &ClaimRedemption) -> Result<ExecutionKey, ProtocolError> {
        self.transact(&request.session_id, |session| {
            let entity = session
                .claims
                .get(&request.claim_id)
                .ok_or_else(|| claim_error("claim does not exist"))?
                .entity;
            let now = now_micros().map_err(storage_error)?;
            session.redeem_claim(request.claim_id, request.state_checksum, now)?;
            let key = ExecutionKey {
                entity,
                stage: ExecutionStage::Resume {
                    claim_id: request.claim_id,
                },
            };
            session.enqueue_job(
                key,
                JobInput::Resume {
                    claim_id: request.claim_id,
                },
                now,
            )?;
            Ok(key)
        })
        .map_err(store_error)
        .map(|result| result.0)
    }

    async fn flush_job_replies(
        &self,
        control: &mut quinn::SendStream,
        session_id: Option<&str>,
        replies: &mut BTreeMap<ExecutionKey, JobReply>,
    ) -> Result<(), ProtocolError> {
        if replies.is_empty() {
            return Ok(());
        }
        let session_id = session_id.ok_or_else(|| entity_error("job reply lacks session"))?;
        let retained = self
            .load_session(session_id)
            .await?
            .ok_or_else(|| entity_error("job session is absent"))?;
        let session = &retained.session;
        session.authorize(self.caller.as_ref())?;
        let now = now_micros().map_err(storage_error)?;
        let mut sent = Vec::new();
        for (key, reply) in replies.iter() {
            if let JobReply::Recovery(receipt) = reply
                && session.recovery_receipt(self.caller.as_ref(), &receipt.request, now)?
                    != Some(receipt)
            {
                return Err(entity_error("observed recovery receipt changed"));
            }
            let job = session
                .jobs
                .get(key)
                .ok_or_else(|| entity_error("observed durable job is absent"))?;
            if let JobReply::Recovery(receipt) = reply {
                let outcome = match &job.state {
                    JobState::Finished(JobOutput::Resumed) => Some(RecoveryOutcome::Complete),
                    JobState::Refused(failure) => Some(RecoveryOutcome::Refused(failure.clone())),
                    _ => None,
                };
                if let Some(outcome) = outcome {
                    write_control(
                        control,
                        &recovery::encode(&RecoveryFrame::Outcome {
                            receipt: receipt.clone(),
                            outcome,
                        })?,
                    )
                    .await?;
                    sent.push(*key);
                    continue;
                }
            }
            let output = match &job.state {
                JobState::Finished(output) => output,
                JobState::Refused(failure) => return Err(failure.protocol_error()),
                JobState::Running => {
                    if session.executions[key].expires_at_micros <= now {
                        return Err(entity_error("execution lease is stale or expired"));
                    }
                    continue;
                }
                JobState::Queued => continue,
            };
            let depth = session.entities[&key.entity].depth;
            match (reply, output) {
                (JobReply::Process, JobOutput::Processed(outcome)) => match outcome {
                    ProcessOutcome::Complete => {
                        write_status(control, EntityState::Complete, key.entity, depth, None)
                            .await?
                    }
                    ProcessOutcome::Dehydrate => {
                        write_status(control, EntityState::Dehydrating, key.entity, depth, None)
                            .await?
                    }
                    ProcessOutcome::Failed => {
                        write_status(control, EntityState::Failed, key.entity, depth, None).await?
                    }
                    ProcessOutcome::Deferred { reason, claim_id } => {
                        let claim = &session.claims[claim_id];
                        write_status(
                            control,
                            EntityState::Yielded,
                            key.entity,
                            depth,
                            Some(StatusExtension::Yield {
                                reason: *reason,
                                token: claim.token.clone(),
                            }),
                        )
                        .await?;
                        write_status(
                            control,
                            EntityState::Deferred,
                            key.entity,
                            depth,
                            Some(StatusExtension::ClaimCheck {
                                claim_id: *claim_id,
                                expiry_timestamp_micros: claim.expiry_timestamp_micros,
                            }),
                        )
                        .await?;
                    }
                },
                (JobReply::Rehydrate, JobOutput::Rehydrated(digest)) => {
                    write_control(control, &encode_scope_digest(digest)?).await?;
                    write_status(control, EntityState::Rehydrating, key.entity, depth, None)
                        .await?;
                    write_status(control, EntityState::Complete, key.entity, depth, None).await?;
                }
                (JobReply::Resume(request), JobOutput::Resumed) => {
                    if let Ok(lineage) = session.final_lineage_digest() {
                        let id = session_id.to_owned();
                        self.storage(move |service| {
                            service
                                .entities
                                .put_lineage(&id, lineage)
                                .map_err(storage_error)
                        })
                        .await?;
                    }
                    let mut acknowledgement = request.clone();
                    acknowledgement.acknowledged = true;
                    write_control(control, &encode_claim_redemption(&acknowledgement)?).await?;
                    write_status(control, EntityState::Processing, key.entity, depth, None).await?;
                    write_status(control, EntityState::Complete, key.entity, depth, None).await?;
                }
                _ => return Err(entity_error("job reply differs from retained outcome")),
            }
            sent.push(*key);
        }
        for key in sent {
            replies.remove(&key);
        }
        Ok(())
    }
}

#[derive(Debug)]
enum JobReply {
    Process,
    Rehydrate,
    Resume(ClaimRedemption),
    Recovery(RecoveryReceipt),
}

#[derive(Debug)]
enum AppliedDisposition {
    Complete,
    Dehydrate,
    Deferred(AppliedClaim),
    Failed,
}

#[derive(Debug)]
struct AppliedClaim {
    reason: u8,
    record: ClaimRecord,
}

fn apply_disposition(
    session: &mut Session,
    key: EntityKey,
    disposition: ProcessingDisposition,
    now_micros: u64,
) -> Result<AppliedDisposition, ProtocolError> {
    match disposition {
        ProcessingDisposition::Complete { output_digest } => {
            session.complete_entity(key, output_digest)?;
            Ok(AppliedDisposition::Complete)
        }
        ProcessingDisposition::Dehydrate => {
            session.begin_dehydrating(key)?;
            Ok(AppliedDisposition::Dehydrate)
        }
        ProcessingDisposition::Yield {
            reason,
            continuation_token,
            validation,
            expires_at_micros,
        } => {
            if !(1..=5).contains(&reason) {
                return Err(entity_error("yield reason is unassigned"));
            }
            let record = session.defer_with_random_claim(
                key,
                continuation_token,
                validation,
                expires_at_micros,
                now_micros,
            )?;
            Ok(AppliedDisposition::Deferred(AppliedClaim {
                reason,
                record,
            }))
        }
        ProcessingDisposition::Failed => {
            session.transition(key, EntityState::Failed)?;
            Ok(AppliedDisposition::Failed)
        }
    }
}

fn new_entity(header: &EntityHeader, body: &Payload) -> NewEntity {
    NewEntity {
        entity_id: header.entity_id,
        layer: header.layer,
        payload_digest: header.checksum.unwrap_or_else(|| body.digest()),
        policy: header.completion_policy.clone(),
    }
}

fn recursive_capabilities(
    max_scope_depth: u8,
    max_entities_per_scope: u32,
) -> Result<Capabilities> {
    let capabilities = Capabilities {
        layer0_core: true,
        layer1_recursive: true,
        layer2_resilience: true,
        max_scope_depth: Some(max_scope_depth),
        max_entities_per_scope: Some(max_entities_per_scope),
        max_window_size: 1024,
        serialization_format: 0,
        keepalive_timeout_ms: 30_000,
        extensions: Default::default(),
    };
    capabilities
        .negotiate(&capabilities)
        .map_err(anyhow::Error::from)?;
    Ok(capabilities)
}

fn layers(capabilities: &Capabilities) -> LayerSupport {
    LayerSupport {
        layer1_recursive: capabilities.layer1_recursive,
        layer2_resilience: capabilities.layer2_resilience,
    }
}

/// A bound server. The caller chooses whether to run one connection or a long-lived listener.
pub struct RecursiveServer<P, E = FileEntityStore> {
    endpoint: quinn::Endpoint,
    service: RecursiveService<P, E>,
    max_concurrent_connections: usize,
}

impl<P: EntityProcessor, E: EntityStore> RecursiveServer<P, E> {
    pub fn bind(options: &RecursiveServerOptions, service: RecursiveService<P, E>) -> Result<Self> {
        if options.max_concurrent_connections == 0 {
            bail!("PIPESTREAM_LIMIT_EXCEEDED: max concurrent connections is zero");
        }
        let endpoint = server_endpoint(options, service.authentication.as_deref())?;
        Ok(Self {
            endpoint,
            service,
            max_concurrent_connections: options.max_concurrent_connections,
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.endpoint.local_addr()?)
    }

    pub async fn run(self, once: bool) -> Result<()> {
        let mut executor = self.service.start_executor()?;
        if once {
            let incoming = tokio::select! {
                incoming = self.endpoint.accept() => incoming.context("accept QUIC connection")?,
                result = &mut executor.task => { result??; bail!("executor stopped unexpectedly"); }
            };
            let connection = tokio::select! {
                connection = incoming => connection.context("establish QUIC connection")?,
                result = &mut executor.task => { result??; bail!("executor stopped unexpectedly"); }
            };
            let handled = tokio::select! {
                result = self.service.handle_connection(&connection) => result,
                result = &mut executor.task => { result??; bail!("executor stopped unexpectedly"); }
            };
            if let Err(error) = handled {
                close_for_error(&connection, &error);
                self.endpoint.wait_idle().await;
                return Err(error.into());
            }
        } else {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    result = &mut executor.task => { result??; bail!("executor stopped unexpectedly"); }
                    result = connections.join_next(), if !connections.is_empty() => {
                        if let Some(Err(error)) = result {
                            return Err(error.into());
                        }
                    }
                    incoming = self.endpoint.accept(), if connections.len() < self.max_concurrent_connections => {
                        let Some(incoming) = incoming else { break; };
                        let service = self.service.clone();
                        connections.spawn(async move {
                            match incoming.await {
                                Ok(connection) => {
                                    if let Err(error) = service.handle_connection(&connection).await {
                                        close_for_error(&connection, &error);
                                        eprintln!("recursive connection refused: {error}");
                                    }
                                }
                                Err(error) => eprintln!("recursive handshake failed: {error}"),
                            }
                        });
                    }
                }
            }
        }
        let active = executor.shutdown(Duration::from_secs(5)).await?;
        if active != 0 {
            eprintln!("executor stopped dispatch; {active} store-wide callbacks still active");
        }
        self.endpoint.wait_idle().await;
        Ok(())
    }
}

/// Start the standalone durable recursive server.
pub async fn serve_recursive<P: EntityProcessor>(
    options: RecursiveServerOptions,
    processor: P,
) -> Result<()> {
    serve_recursive_authenticated(options, processor, None).await
}

/// Start a durable server with an optional, explicitly configured mutual-TLS policy.
pub async fn serve_recursive_authenticated<P: EntityProcessor>(
    options: RecursiveServerOptions,
    processor: P,
    authentication: Option<AuthenticationPolicy>,
) -> Result<()> {
    let store = Arc::new(SqliteSessionStore::open(&options.state_database)?);
    let entities = Arc::new(FileEntityStore::open(&options.entity_directory)?);
    let mut service = RecursiveService::with_limits(
        store,
        entities,
        Arc::new(processor),
        RecursiveLimits {
            max_scope_depth: options.max_scope_depth,
            max_entities_per_scope: options.max_entities_per_scope,
            max_entity_bytes: options.max_entity_bytes,
            max_chunks_per_entity: options.max_chunks_per_entity,
        },
    )?;
    if let Some(policy) = authentication {
        service = service.with_authentication(policy);
    }
    let server = RecursiveServer::bind(&options, service)?;
    let address = server.local_addr()?;
    if let Some(path) = &options.ready_file {
        fs::write(path, format!("{address}\n")).context("write ready file")?;
    }
    println!("READY {address}");
    server.run(options.once).await
}

/// Connected recursive client used by applications and conformance scenarios.
pub struct RecursiveClient {
    endpoint: quinn::Endpoint,
    connection: quinn::Connection,
    control_send: quinn::SendStream,
    control_recv: quinn::RecvStream,
    layers: LayerSupport,
    sealed: bool,
    authenticated_recovery: bool,
}

impl RecursiveClient {
    pub async fn connect(options: &RecursiveClientOptions) -> Result<Self> {
        Self::connect_profile(options, false, false).await
    }

    pub async fn connect_sealed(options: &RecursiveClientOptions) -> Result<Self> {
        Self::connect_profile(options, true, false).await
    }

    pub async fn connect_recovery(options: &RecursiveClientOptions) -> Result<Self> {
        if options.identity.is_none() {
            bail!("PIPESTREAM_UNAUTHORIZED: recovery requires a client identity");
        }
        Self::connect_profile(options, false, true).await
    }

    async fn connect_profile(
        options: &RecursiveClientOptions,
        sealed: bool,
        authenticated_recovery: bool,
    ) -> Result<Self> {
        let mut roots = rustls::RootCertStore::empty();
        for cert in CertificateDer::pem_file_iter(&options.ca_certificate)
            .context("read CA PEM")?
            .collect::<Result<Vec<_>, _>>()
            .context("parse CA PEM")?
        {
            roots.add(cert).context("add CA certificate")?;
        }
        let builder = rustls::ClientConfig::builder().with_root_certificates(roots);
        let mut tls = if let Some(identity) = &options.identity {
            let chain = CertificateDer::pem_file_iter(&identity.certificate)?
                .collect::<Result<Vec<_>, _>>()?;
            let key = PrivateKeyDer::from_pem_file(&identity.private_key)?;
            builder.with_client_auth_cert(chain, key)?
        } else {
            builder.with_no_client_auth()
        };
        if options.identity.is_some() {
            tls.resumption = rustls::client::Resumption::disabled();
        }
        tls.alpn_protocols = vec![pipestream_core::ALPN.to_vec()];
        let config = quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(tls)?));
        let mut endpoint = quinn::Endpoint::client("0.0.0.0:0".parse()?)?;
        endpoint.set_default_client_config(config);
        let connection = endpoint
            .connect(options.remote, &options.server_name)?
            .await
            .context("connect QUIC")?;
        let (mut control_send, mut control_recv) =
            connection.open_bi().await.context("open control stream")?;
        let mut offered = recursive_capabilities(7, pipestream_core::MAX_ENTITY_ID)?;
        if sealed {
            offered.layer2_resilience = false;
            offered
                .extensions
                .supported
                .push(EXTENSION_SEALED_WORK_SETS);
            offered.extensions.required.push(EXTENSION_SEALED_WORK_SETS);
        }
        if options.identity.is_some() {
            offered
                .extensions
                .supported
                .push(EXTENSION_AUTHENTICATED_SESSIONS);
            offered
                .extensions
                .required
                .push(EXTENSION_AUTHENTICATED_SESSIONS);
        }
        if authenticated_recovery {
            offered
                .extensions
                .supported
                .push(EXTENSION_AUTHENTICATED_RECOVERY);
            offered
                .extensions
                .required
                .push(EXTENSION_AUTHENTICATED_RECOVERY);
        }
        if let Err(error) = write_control(&mut control_send, &encode_capabilities(&offered)?).await
        {
            return Err(peer_failure(&connection, error).await);
        }
        let (frame_type, response) = match read_control(&mut control_recv).await {
            Ok(frame) => frame,
            Err(error) => return Err(peer_failure(&connection, error).await),
        };
        if frame_type != FRAME_CAPABILITIES {
            bail!("PIPESTREAM_FRAME_ERROR: server did not answer capabilities");
        }
        let negotiated = match decode_capabilities(&response).and_then(|peer| {
            offered.validate_response(&peer)?;
            if authenticated_recovery
                && (!peer.layer2_resilience
                    || !peer
                        .extensions
                        .required
                        .contains(&EXTENSION_AUTHENTICATED_SESSIONS))
            {
                return Err(frame_error("invalid authenticated recovery selection"));
            }
            Ok(peer)
        }) {
            Ok(peer) => peer,
            Err(error) => {
                connection.close(error.code.into(), error.to_string().as_bytes());
                endpoint.wait_idle().await;
                return Err(error.into());
            }
        };
        Ok(Self {
            endpoint,
            connection,
            control_send,
            control_recv,
            layers: layers(&negotiated),
            sealed,
            authenticated_recovery,
        })
    }

    /// Accept or replay one recovery request. The receipt is not application completion.
    /// Consume its outcome or reconnect before issuing another request on this client.
    pub async fn accept_recovery(&mut self, request: &RecoveryRequest) -> Result<RecoveryReceipt> {
        if !self.authenticated_recovery {
            bail!("PIPESTREAM_EXTENSION_UNSUPPORTED: recovery was not negotiated");
        }
        write_control(
            &mut self.control_send,
            &recovery::encode(&RecoveryFrame::Request(request.clone()))?,
        )
        .await?;
        let (kind, bytes) = self.read_response().await?;
        let receipt = if kind == FRAME_RECOVERY {
            recovery::decode(&bytes)
        } else {
            Err(frame_error("expected recovery receipt"))
        };
        match receipt {
            Ok(RecoveryFrame::Receipt(receipt)) if &receipt.request == request => Ok(receipt),
            Err(error) => {
                self.connection
                    .close(error.code.into(), error.to_string().as_bytes());
                Err(error.into())
            }
            _ => {
                self.connection
                    .close(ERROR_ENTITY_INVALID.into(), b"recovery receipt mismatch");
                bail!("PIPESTREAM_ENTITY_INVALID: recovery receipt mismatch");
            }
        }
    }

    pub async fn wait_recovery(&mut self, receipt: &RecoveryReceipt) -> Result<RecoveryOutcome> {
        receipt.validate()?;
        if !self.authenticated_recovery {
            bail!("PIPESTREAM_EXTENSION_UNSUPPORTED: recovery was not negotiated");
        }
        let (kind, bytes) = self.read_response().await?;
        let frame = if kind == FRAME_RECOVERY {
            recovery::decode(&bytes)
        } else {
            Err(frame_error("expected recovery outcome"))
        };
        match frame {
            Ok(RecoveryFrame::Outcome {
                receipt: observed,
                outcome,
            }) if &observed == receipt => Ok(outcome),
            Err(error) => {
                self.connection
                    .close(error.code.into(), error.to_string().as_bytes());
                Err(error.into())
            }
            _ => {
                self.connection
                    .close(ERROR_ENTITY_INVALID.into(), b"recovery outcome mismatch");
                bail!("PIPESTREAM_ENTITY_INVALID: recovery outcome mismatch");
            }
        }
    }

    pub async fn declare_work(&mut self, request: &WorkSetFrame) -> Result<()> {
        if !self.sealed || request.flags & work_set::ACK != 0 {
            bail!("PIPESTREAM_EXTENSION_UNSUPPORTED: client is not a sealed-work producer");
        }
        if let Err(error) = write_control(&mut self.control_send, &work_set::encode(request)?).await
        {
            return Err(peer_failure(&self.connection, error).await);
        }
        let (kind, body) = self.read_response().await?;
        let mut expected = request.clone();
        expected.flags |= work_set::ACK;
        if kind != FRAME_WORK_SET {
            self.connection
                .close(ERROR_ENTITY_INVALID.into(), b"WORK_SET ACK mismatch");
            bail!("PIPESTREAM_ENTITY_INVALID: WORK_SET ACK mismatch");
        }
        let acknowledgement = match work_set::decode(&body) {
            Ok(frame) => frame,
            Err(error) => {
                self.connection
                    .close(error.code.into(), error.to_string().as_bytes());
                return Err(error.into());
            }
        };
        if acknowledgement != expected {
            self.connection
                .close(ERROR_ENTITY_INVALID.into(), b"WORK_SET ACK mismatch");
            bail!("PIPESTREAM_ENTITY_INVALID: WORK_SET ACK mismatch");
        }
        Ok(())
    }

    pub async fn send_entity(
        &mut self,
        header: &EntityHeader,
        payload: &[u8],
        depth: u8,
    ) -> Result<Vec<StatusFrame>> {
        if header.chunk_info.is_some() {
            bail!(
                "PIPESTREAM_ENTITY_INVALID: use send_chunked_entity for an entity with chunk-info"
            );
        }
        self.announce_entity(header, depth).await?;
        self.write_entity_stream(header, payload).await?;
        self.read_entity_statuses(header).await
    }

    /// Send one lifecycle entity as independently framed chunks in caller-supplied order.
    pub async fn send_chunked_entity(
        &mut self,
        chunks: &[EntityChunk],
        depth: u8,
    ) -> Result<Vec<StatusFrame>> {
        let first = chunks
            .first()
            .context("PIPESTREAM_ENTITY_INVALID: chunk list is empty")?;
        if first.header.chunk_info.is_none() {
            bail!("PIPESTREAM_ENTITY_INVALID: first chunk lacks chunk-info");
        }
        self.announce_entity(&first.header, depth).await?;
        for chunk in chunks {
            if chunk.header.chunk_info.is_none() {
                bail!("PIPESTREAM_ENTITY_INVALID: chunk lacks chunk-info");
            }
            self.write_entity_stream(&chunk.header, &chunk.payload)
                .await?;
        }
        self.read_entity_statuses(&first.header).await
    }

    async fn announce_entity(&mut self, header: &EntityHeader, depth: u8) -> Result<()> {
        let key = EntityKey {
            scope_id: header.scope_id.unwrap_or(0),
            entity_id: header.entity_id,
        };
        let pending = StatusFrame {
            status: Status {
                state: pipestream_core::STATUS_PENDING,
                entity_id: key.entity_id,
                scope_id: key.scope_id,
                cursor: None,
                depth,
            },
            extension: None,
        };
        write_control(
            &mut self.control_send,
            &encode_status_frame(&pending, self.layers)?,
        )
        .await?;
        Ok(())
    }

    async fn write_entity_stream(&self, header: &EntityHeader, payload: &[u8]) -> Result<()> {
        let mut stream = self
            .connection
            .open_uni()
            .await
            .context("open entity stream")?;
        write_control(
            &mut stream,
            &encode_entity_for(header, payload, self.layers)?,
        )
        .await?;
        stream.finish().context("finish entity stream")?;
        Ok(())
    }

    async fn read_entity_statuses(&mut self, header: &EntityHeader) -> Result<Vec<StatusFrame>> {
        let key = EntityKey {
            scope_id: header.scope_id.unwrap_or(0),
            entity_id: header.entity_id,
        };
        let mut statuses = Vec::new();
        loop {
            let (frame_type, response) = self.read_response().await?;
            if frame_type != FRAME_STATUS {
                bail!("PIPESTREAM_FRAME_ERROR: expected STATUS after entity");
            }
            let status = decode_status_frame(&response, self.layers)?;
            if status.status.entity_id != key.entity_id || status.status.scope_id != key.scope_id {
                bail!("PIPESTREAM_ENTITY_INVALID: response status identity differs");
            }
            let state = status.status.state;
            statuses.push(status);
            if matches!(
                state,
                pipestream_core::STATUS_COMPLETE
                    | pipestream_core::STATUS_FAILED
                    | pipestream_core::STATUS_DEHYDRATING
                    | pipestream_core::STATUS_DEFERRED
            ) {
                break;
            }
        }
        Ok(statuses)
    }

    pub async fn close_scope(
        &mut self,
        digest: &ScopeDigest,
    ) -> Result<(ScopeDigest, Vec<StatusFrame>)> {
        write_control(&mut self.control_send, &encode_scope_digest(digest)?).await?;
        let (frame_type, response) = self.read_response().await?;
        if frame_type != FRAME_SCOPE_DIGEST {
            bail!("PIPESTREAM_FRAME_ERROR: expected SCOPE_DIGEST confirmation");
        }
        let observed = decode_scope_digest(&response)?;
        if &observed != digest {
            self.connection
                .close(ERROR_ENTITY_INVALID.into(), b"scope digest ACK mismatch");
            bail!("PIPESTREAM_ENTITY_INVALID: scope digest acknowledgement differs");
        }
        let mut statuses = Vec::with_capacity(2);
        for expected in [
            pipestream_core::STATUS_REHYDRATING,
            pipestream_core::STATUS_COMPLETE,
        ] {
            let (frame_type, response) = self.read_response().await?;
            if frame_type != FRAME_STATUS {
                bail!("PIPESTREAM_FRAME_ERROR: expected rehydration STATUS");
            }
            let status = decode_status_frame(&response, self.layers)?;
            if status.status.state != expected {
                bail!("PIPESTREAM_ENTITY_INVALID: unexpected rehydration status");
            }
            statuses.push(status);
        }
        Ok((observed, statuses))
    }

    pub async fn barrier(&mut self, scope_id: u32, parent_entity_id: u32) -> Result<Barrier> {
        write_control(
            &mut self.control_send,
            &encode_barrier(Barrier {
                released: false,
                scope_id,
                parent_entity_id,
            })?,
        )
        .await?;
        let (frame_type, response) = self.read_response().await?;
        if frame_type != FRAME_BARRIER {
            bail!("PIPESTREAM_FRAME_ERROR: expected BARRIER response");
        }
        let barrier = decode_barrier(&response)?;
        if barrier.scope_id != scope_id || barrier.parent_entity_id != parent_entity_id {
            self.connection
                .close(ERROR_ENTITY_INVALID.into(), b"barrier identity mismatch");
            bail!("PIPESTREAM_ENTITY_INVALID: barrier response identity differs");
        }
        Ok(barrier)
    }

    pub async fn checkpoint(&mut self, checkpoint: &Checkpoint) -> Result<Checkpoint> {
        write_control(
            &mut self.control_send,
            &encode_checkpoint_for(checkpoint, self.layers)?,
        )
        .await?;
        let (frame_type, response) = self.read_response().await?;
        if frame_type != FRAME_CHECKPOINT {
            bail!("PIPESTREAM_FRAME_ERROR: expected CHECKPOINT acknowledgement");
        }
        let acknowledgement = decode_checkpoint_for(&response, self.layers)?;
        let mut expected = checkpoint.clone();
        expected.flags = CHECKPOINT_ACK;
        if acknowledgement != expected {
            self.connection
                .close(ERROR_ENTITY_INVALID.into(), b"checkpoint ACK mismatch");
            bail!("PIPESTREAM_ENTITY_INVALID: checkpoint acknowledgement differs");
        }
        Ok(acknowledgement)
    }

    pub async fn redeem_claim(
        &mut self,
        redemption: &ClaimRedemption,
    ) -> Result<(ClaimRedemption, Vec<StatusFrame>)> {
        write_control(
            &mut self.control_send,
            &encode_claim_redemption(redemption)?,
        )
        .await?;
        let (frame_type, response) = self.read_response().await?;
        if frame_type != FRAME_CLAIM_REDEMPTION {
            bail!("PIPESTREAM_FRAME_ERROR: expected claim acknowledgement");
        }
        let acknowledgement = decode_claim_redemption(&response)?;
        let mut expected = redemption.clone();
        expected.acknowledged = true;
        if acknowledgement != expected {
            self.connection
                .close(ERROR_ENTITY_INVALID.into(), b"claim ACK mismatch");
            bail!("PIPESTREAM_ENTITY_INVALID: claim acknowledgement differs");
        }
        let mut statuses = Vec::with_capacity(2);
        for expected in [
            pipestream_core::STATUS_PROCESSING,
            pipestream_core::STATUS_COMPLETE,
        ] {
            let (frame_type, response) = self.read_response().await?;
            if frame_type != FRAME_STATUS {
                bail!("PIPESTREAM_FRAME_ERROR: expected claim STATUS");
            }
            let status = decode_status_frame(&response, self.layers)?;
            if status.status.state != expected {
                bail!("PIPESTREAM_ENTITY_INVALID: unexpected claim status");
            }
            statuses.push(status);
        }
        Ok((acknowledgement, statuses))
    }

    pub async fn goaway(mut self, last_entity_id: u32) -> Result<()> {
        write_control(&mut self.control_send, &encode_goaway(last_entity_id)?).await?;
        let (frame_type, response) = self.read_response().await?;
        if frame_type != FRAME_GOAWAY || decode_goaway(&response)? != last_entity_id {
            bail!("PIPESTREAM_FRAME_ERROR: invalid GOAWAY acknowledgement");
        }
        self.control_send
            .finish()
            .context("finish control stream")?;
        self.connection.close(ERROR_NO_ERROR.into(), b"complete");
        self.endpoint.wait_idle().await;
        Ok(())
    }

    /// Request disconnection without waiting for transport shutdown.
    ///
    /// Use [`Self::disconnect_gracefully`] before stopping the runtime. Neither
    /// operation acknowledges completion or redeems a retained claim.
    pub fn disconnect(self) {
        self.connection.close(ERROR_NO_ERROR.into(), b"disconnect");
    }

    /// Disconnect and drain the QUIC endpoint before its runtime can stop.
    ///
    /// This waits for transport shutdown, not application work or a completion
    /// barrier. If this future is cancelled, notification becomes best-effort.
    pub async fn disconnect_gracefully(self) {
        self.connection.close(ERROR_NO_ERROR.into(), b"disconnect");
        self.endpoint.wait_idle().await;
    }

    async fn read_response(&mut self) -> Result<(u8, Vec<u8>)> {
        loop {
            match read_control(&mut self.control_recv).await {
                Ok((FRAME_STATUS, payload)) => {
                    let status = decode_status_frame(&payload, self.layers)?;
                    if status.status.state == pipestream_core::STATUS_UNSPECIFIED
                        && status.status.entity_id == CONNECTION_LEVEL
                        && status.status.cursor.is_none()
                    {
                        continue;
                    }
                    return Ok((FRAME_STATUS, payload));
                }
                Ok((kind, _))
                    if !matches!(
                        kind,
                        FRAME_BARRIER
                            | FRAME_WORK_SET
                            | FRAME_RECOVERY
                            | FRAME_CAPABILITIES
                            | FRAME_CHECKPOINT
                            | FRAME_CLAIM_REDEMPTION
                            | FRAME_GOAWAY
                            | FRAME_SCOPE_DIGEST
                    ) =>
                {
                    continue;
                }
                Ok(frame) => return Ok(frame),
                Err(protocol_error) => {
                    return Err(peer_failure(&self.connection, protocol_error).await);
                }
            }
        }
    }
}

async fn peer_failure(connection: &quinn::Connection, fallback: ProtocolError) -> anyhow::Error {
    let close = match connection.close_reason() {
        Some(reason) => Some(reason),
        None => tokio::time::timeout(Duration::from_secs(1), connection.closed())
            .await
            .ok(),
    };
    if let Some(quinn::ConnectionError::ApplicationClosed(close)) = close {
        let reason = String::from_utf8_lossy(&close.reason);
        if !reason.is_empty() {
            return anyhow::anyhow!("{reason}");
        }
    }
    fallback.into()
}

/// Run the exemplar Layer 1 tree over the public client API.
pub async fn run_recursive_scenario(
    options: &RecursiveClientOptions,
    session_id: &str,
) -> Result<RecursiveScenarioResult> {
    let mut client = RecursiveClient::connect(options).await?;
    let mut completion_order = Vec::new();
    let root = exemplar_header(session_id, 1, None, None, "dehydrate", b"root");
    expect_states(
        &client.send_entity(&root, b"root", 0).await?,
        &[EntityState::Processing, EntityState::Dehydrating],
    )?;

    for (entity_id, payload) in [(3, b"child-c".as_slice()), (1, b"child-a".as_slice())] {
        let header = exemplar_header(session_id, entity_id, Some(1), Some(0), "complete", payload);
        expect_states(
            &client.send_entity(&header, payload, 1).await?,
            &[EntityState::Processing, EntityState::Complete],
        )?;
        completion_order.push(EntityKey {
            scope_id: 1,
            entity_id,
        });
    }
    let branch = exemplar_header(session_id, 2, Some(1), Some(0), "dehydrate", b"child-b");
    expect_states(
        &client.send_entity(&branch, b"child-b", 1).await?,
        &[EntityState::Processing, EntityState::Dehydrating],
    )?;

    for (entity_id, payload) in [
        (2, b"grandchild-b".as_slice()),
        (1, b"grandchild-a".as_slice()),
    ] {
        let header = exemplar_header(session_id, entity_id, Some(2), Some(1), "complete", payload);
        expect_states(
            &client.send_entity(&header, payload, 2).await?,
            &[EntityState::Processing, EntityState::Complete],
        )?;
        completion_order.push(EntityKey {
            scope_id: 2,
            entity_id,
        });
    }
    let nested_digest = complete_digest(2, &[1, 2])?;
    let (observed_nested, statuses) = client.close_scope(&nested_digest).await?;
    if observed_nested != nested_digest {
        bail!("PIPESTREAM_INTEGRITY_ERROR: nested scope confirmation differs");
    }
    expect_states(
        &statuses,
        &[EntityState::Rehydrating, EntityState::Complete],
    )?;
    completion_order.push(EntityKey {
        scope_id: 1,
        entity_id: 2,
    });
    if !client.barrier(2, 2).await?.released {
        bail!("PIPESTREAM_SCOPE_INVALID: nested barrier was not released");
    }
    client
        .checkpoint(&Checkpoint {
            checkpoint_id: "nested-complete".to_owned(),
            sequence_number: 1,
            checkpoint_entity_id: 3,
            scope_id: Some(2),
            flags: 0,
            timeout_ms: Some(30_000),
        })
        .await?;

    let child_digest = complete_digest(1, &[1, 2, 3])?;
    let (observed_child, statuses) = client.close_scope(&child_digest).await?;
    if observed_child != child_digest {
        bail!("PIPESTREAM_INTEGRITY_ERROR: child scope confirmation differs");
    }
    expect_states(
        &statuses,
        &[EntityState::Rehydrating, EntityState::Complete],
    )?;
    completion_order.push(EntityKey {
        scope_id: 0,
        entity_id: 1,
    });
    if !client.barrier(1, 1).await?.released {
        bail!("PIPESTREAM_SCOPE_INVALID: child barrier was not released");
    }
    client
        .checkpoint(&Checkpoint {
            checkpoint_id: "children-complete".to_owned(),
            sequence_number: 2,
            checkpoint_entity_id: 4,
            scope_id: Some(1),
            flags: 0,
            timeout_ms: Some(30_000),
        })
        .await?;
    client
        .checkpoint(&Checkpoint {
            checkpoint_id: "root-complete".to_owned(),
            sequence_number: 3,
            checkpoint_entity_id: 2,
            scope_id: None,
            flags: 0,
            timeout_ms: Some(30_000),
        })
        .await?;
    client.goaway(1).await?;
    Ok(RecursiveScenarioResult {
        completion_order,
        nested_digest,
        child_digest,
    })
}

/// Exercise sealed declarations, nested chunked work, replay, and named refusals
/// through the public producer API against an independent sealed server.
/// The server application must implement the exemplar processing actions.
pub async fn run_sealed_scenario(options: &RecursiveClientOptions, session_id: &str) -> Result<()> {
    let declaration = |scope_id, parent, ids: &[u32]| WorkSetFrame {
        session_id: session_id.to_owned(),
        producer_id: [7; 16],
        scope_id,
        parent,
        sequence: 0,
        entity_ids: ids.to_vec(),
        flags: work_set::SEAL,
        seal_digest: Some(work_set::seal_digest(
            session_id,
            [7; 16],
            scope_id,
            parent,
            &ids.iter().copied().collect(),
        )),
    };
    let root = declaration(0, None, &[1, 2]);
    let mut client = RecursiveClient::connect_sealed(options).await?;
    client.declare_work(&root).await?;
    client.disconnect_gracefully().await;
    let mut client = RecursiveClient::connect_sealed(options).await?;
    client.declare_work(&root).await?;
    for (id, action) in [(2, "complete"), (1, "dehydrate")] {
        let header = exemplar_header(session_id, id, None, None, action, b"root");
        expect_states(
            &client.send_entity(&header, b"root", 0).await?,
            &[
                EntityState::Processing,
                if id == 1 {
                    EntityState::Dehydrating
                } else {
                    EntityState::Complete
                },
            ],
        )?;
    }
    client
        .declare_work(&declaration(
            1,
            Some(EntityKey {
                scope_id: 0,
                entity_id: 1,
            }),
            &[1, 2, 3],
        ))
        .await?;
    for id in [3, 1, 2] {
        let action = if id == 2 { "dehydrate" } else { "complete" };
        let header = exemplar_header(session_id, id, Some(1), Some(0), action, b"child");
        expect_states(
            &client.send_entity(&header, b"child", 1).await?,
            &[
                EntityState::Processing,
                if id == 2 {
                    EntityState::Dehydrating
                } else {
                    EntityState::Complete
                },
            ],
        )?;
    }
    if client.barrier(1, 1).await?.released {
        bail!("unfinished child scope released its barrier");
    }
    client
        .declare_work(&declaration(
            2,
            Some(EntityKey {
                scope_id: 1,
                entity_id: 2,
            }),
            &[1, 2],
        ))
        .await?;
    let header = exemplar_header(session_id, 1, Some(2), Some(1), "complete", b"leaf");
    expect_states(
        &client.send_entity(&header, b"leaf", 2).await?,
        &[EntityState::Processing, EntityState::Complete],
    )?;
    let mut chunks = Vec::new();
    for (index, payload) in [(1, b"def"), (0, b"abc")] {
        let mut header = exemplar_header(session_id, 2, Some(2), Some(1), "complete", payload);
        header.chunk_info = Some(pipestream_core::ChunkInfo {
            total_chunks: 2,
            chunk_index: index,
            chunk_offset: index * 3,
        });
        chunks.push(EntityChunk {
            header,
            payload: payload.to_vec(),
        });
    }
    expect_states(
        &client.send_chunked_entity(&chunks, 2).await?,
        &[EntityState::Processing, EntityState::Complete],
    )?;
    for (scope, ids, parent) in [(2, vec![1, 2], 2), (1, vec![1, 2, 3], 1)] {
        let (_, statuses) = client.close_scope(&complete_digest(scope, &ids)?).await?;
        expect_states(
            &statuses,
            &[EntityState::Rehydrating, EntityState::Complete],
        )?;
        if !client.barrier(scope, parent).await?.released {
            bail!("closed child scope failed to release barrier");
        }
        client
            .checkpoint(&Checkpoint {
                checkpoint_id: format!("scope-{scope}"),
                sequence_number: u64::MAX,
                checkpoint_entity_id: *ids.last().unwrap(),
                scope_id: Some(scope),
                flags: 0,
                timeout_ms: Some(5000),
            })
            .await?;
    }
    let checkpoint = Checkpoint {
        checkpoint_id: "sealed-root".to_owned(),
        sequence_number: 1 << 63,
        checkpoint_entity_id: 2,
        scope_id: Some(0),
        flags: 0,
        timeout_ms: Some(5000),
    };
    client.checkpoint(&checkpoint).await?;
    client.goaway(2).await?;
    let mut replay = RecursiveClient::connect_sealed(options).await?;
    replay.declare_work(&root).await?;
    replay.checkpoint(&checkpoint).await?;
    replay.goaway(2).await?;
    let mut wrong_owner = RecursiveClient::connect_sealed(options).await?;
    let mut changed = root.clone();
    changed.producer_id = [8; 16];
    let error = wrong_owner
        .declare_work(&changed)
        .await
        .err()
        .context("changed owner was incorrectly accepted")?;
    if !error.to_string().contains("PIPESTREAM_ENTITY_INVALID") {
        return Err(error);
    }
    wrong_owner.disconnect_gracefully().await;
    let mut wrong_checkpoint = RecursiveClient::connect_sealed(options).await?;
    wrong_checkpoint.declare_work(&root).await?;
    let mut changed = checkpoint;
    changed.checkpoint_id = "changed-request".to_owned();
    let error = wrong_checkpoint
        .checkpoint(&changed)
        .await
        .err()
        .context("changed checkpoint identity was incorrectly accepted")?;
    if !error.to_string().contains("PIPESTREAM_ENTITY_INVALID") {
        return Err(error);
    }
    wrong_checkpoint.disconnect_gracefully().await;
    Ok(())
}

/// Persist a yielded root and return the cross-connection claim request.
pub async fn begin_durable_yield(
    options: &RecursiveClientOptions,
    session_id: &str,
) -> Result<ClaimRedemption> {
    let payload = b"durable-payload";
    let mut client = RecursiveClient::connect(options).await?;
    let header = exemplar_header(session_id, 1, None, None, "yield", payload);
    let statuses = client.send_entity(&header, payload, 0).await?;
    expect_states(
        &statuses,
        &[
            EntityState::Processing,
            EntityState::Yielded,
            EntityState::Deferred,
        ],
    )?;
    let claim_id = match statuses.last().and_then(|status| status.extension.as_ref()) {
        Some(StatusExtension::ClaimCheck { claim_id, .. }) => *claim_id,
        _ => bail!("PIPESTREAM_ENTITY_INVALID: DEFERRED lacks claim check"),
    };
    client.disconnect_gracefully().await;
    Ok(ClaimRedemption {
        session_id: session_id.to_owned(),
        claim_id,
        state_checksum: tagged_digest(b"pipestream-stopping-point-v1", payload),
        acknowledged: false,
    })
}

/// Redeem a persisted claim on any server sharing the durable session store.
pub async fn finish_durable_yield(
    options: &RecursiveClientOptions,
    redemption: &ClaimRedemption,
) -> Result<()> {
    let mut client = RecursiveClient::connect(options).await?;
    let (acknowledgement, statuses) = client.redeem_claim(redemption).await?;
    if acknowledgement.claim_id != redemption.claim_id
        || acknowledgement.session_id != redemption.session_id
        || acknowledgement.state_checksum != redemption.state_checksum
    {
        bail!("PIPESTREAM_ENTITY_INVALID: claim acknowledgement differs");
    }
    expect_states(&statuses, &[EntityState::Processing, EntityState::Complete])?;
    client.goaway(1).await
}

fn complete_digest(scope_id: u32, entity_ids: &[u32]) -> Result<ScopeDigest> {
    let statuses = entity_ids
        .iter()
        .map(|entity_id| (*entity_id, EntityState::Complete))
        .collect::<Vec<_>>();
    Ok(ScopeDigest {
        scope_id,
        entities_processed: entity_ids.len() as u64,
        entities_succeeded: entity_ids.len() as u64,
        entities_failed: 0,
        entities_deferred: 0,
        merkle_root: merkle_root(&statuses)?,
    })
}

fn exemplar_header(
    session_id: &str,
    entity_id: u32,
    parent_id: Option<u32>,
    parent_scope_id: Option<u32>,
    action: &str,
    payload: &[u8],
) -> EntityHeader {
    let mut metadata = BTreeMap::new();
    metadata.insert(SESSION_METADATA_KEY.to_owned(), session_id.to_owned());
    metadata.insert(ACTION_METADATA_KEY.to_owned(), action.to_owned());
    EntityHeader {
        entity_id,
        parent_id,
        scope_id: parent_id.map(|_| if parent_scope_id == Some(0) { 1 } else { 2 }),
        parent_scope_id,
        layer: 0,
        content_type: Some("application/octet-stream".to_owned()),
        payload_length: Some(payload.len() as u64),
        checksum: Some(Sha256::digest(payload).into()),
        metadata,
        chunk_info: None,
        completion_policy: None,
    }
}

fn expect_states(statuses: &[StatusFrame], expected: &[EntityState]) -> Result<()> {
    let observed = statuses
        .iter()
        .map(|status| status.status.state)
        .collect::<Vec<_>>();
    let expected = expected
        .iter()
        .map(|state| state.code())
        .collect::<Vec<_>>();
    if observed != expected {
        bail!("PIPESTREAM_ENTITY_INVALID: states {observed:?} differ from {expected:?}");
    }
    Ok(())
}

fn same_chunk_identity(first: &EntityHeader, current: &EntityHeader) -> bool {
    first.entity_id == current.entity_id
        && first.parent_id == current.parent_id
        && first.scope_id == current.scope_id
        && first.parent_scope_id == current.parent_scope_id
        && first.layer == current.layer
        && first.content_type == current.content_type
        && first.metadata == current.metadata
        && first.completion_policy == current.completion_policy
}

fn server_endpoint(
    options: &RecursiveServerOptions,
    authentication: Option<&AuthenticationPolicy>,
) -> Result<quinn::Endpoint> {
    let certs = CertificateDer::pem_file_iter(&options.certificate)
        .context("read certificate PEM")?
        .collect::<Result<Vec<_>, _>>()
        .context("parse certificate PEM")?;
    let key =
        PrivateKeyDer::from_pem_file(&options.private_key).context("parse private-key PEM")?;
    let builder = rustls::ServerConfig::builder();
    let builder = if let Some(policy) = authentication {
        builder.with_client_cert_verifier(policy.verifier()?)
    } else {
        builder.with_no_client_auth()
    };
    let mut tls = builder
        .with_single_cert(certs, key)
        .context("configure server certificate")?;
    if authentication.is_some() {
        tls.send_tls13_tickets = 0;
        tls.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
    }
    tls.alpn_protocols = vec![pipestream_core::ALPN.to_vec()];
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(tls)?));
    let transport = Arc::get_mut(&mut config.transport).expect("new transport config is unique");
    transport
        .receive_window((1u32 << 20).into())
        .stream_receive_window((64u32 << 10).into())
        .max_concurrent_bidi_streams(1u32.into())
        .max_concurrent_uni_streams(1024u32.into())
        .max_idle_timeout(Some(Duration::from_secs(30).try_into()?));
    Ok(quinn::Endpoint::server(config, options.bind)?)
}

async fn read_control(stream: &mut quinn::RecvStream) -> Result<(u8, Vec<u8>), ProtocolError> {
    let mut header = [0u8; 5];
    stream.read_exact(&mut header).await.map_err(frame_error)?;
    let length = u32::from_be_bytes(header[1..5].try_into().expect("slice length")) as usize;
    if length > MAX_CONTROL_FRAME {
        return Err(ProtocolError::new(
            ERROR_LIMIT_EXCEEDED,
            "PIPESTREAM_LIMIT_EXCEEDED",
            "control frame exceeds local limit",
        ));
    }
    let mut payload = Vec::new();
    while payload.len() < length {
        let chunk = stream
            .read_chunk((length - payload.len()).min(8192), true)
            .await
            .map_err(frame_error)?
            .ok_or_else(|| frame_error("truncated control body"))?;
        payload.extend_from_slice(&chunk.bytes);
    }
    Ok((header[0], payload))
}

async fn write_control(stream: &mut quinn::SendStream, bytes: &[u8]) -> Result<(), ProtocolError> {
    stream.write_all(bytes).await.map_err(frame_error)
}

async fn write_status(
    stream: &mut quinn::SendStream,
    state: EntityState,
    key: EntityKey,
    depth: u8,
    extension: Option<StatusExtension>,
) -> Result<(), ProtocolError> {
    write_control(
        stream,
        &encode_status_frame(
            &StatusFrame {
                status: Status {
                    state: state.code(),
                    entity_id: key.entity_id,
                    scope_id: key.scope_id,
                    cursor: None,
                    depth,
                },
                extension,
            },
            LayerSupport::LAYER2,
        )?,
    )
    .await
}

fn now_micros() -> std::io::Result<u64> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(std::io::Error::other)?;
    u64::try_from(elapsed.as_micros()).map_err(std::io::Error::other)
}

fn frame_error(detail: impl std::fmt::Display) -> ProtocolError {
    ProtocolError::new(ERROR_FRAME, "PIPESTREAM_FRAME_ERROR", detail.to_string())
}

fn entity_error(detail: impl std::fmt::Display) -> ProtocolError {
    ProtocolError::new(
        ERROR_ENTITY_INVALID,
        "PIPESTREAM_ENTITY_INVALID",
        detail.to_string(),
    )
}

fn claim_error(detail: impl std::fmt::Display) -> ProtocolError {
    ProtocolError::new(
        pipestream_core::ERROR_CLAIM_NOT_FOUND,
        "PIPESTREAM_CLAIM_NOT_FOUND",
        detail.to_string(),
    )
}

fn layer_error(detail: impl std::fmt::Display) -> ProtocolError {
    ProtocolError::new(
        ERROR_LAYER_UNSUPPORTED,
        "PIPESTREAM_LAYER_UNSUPPORTED",
        detail.to_string(),
    )
}

fn extension_error(detail: impl std::fmt::Display) -> ProtocolError {
    ProtocolError::new(
        pipestream_core::ERROR_EXTENSION_UNSUPPORTED,
        "PIPESTREAM_EXTENSION_UNSUPPORTED",
        detail.to_string(),
    )
}

fn storage_error(detail: impl std::fmt::Display) -> ProtocolError {
    ProtocolError::new(ERROR_FRAME, "PIPESTREAM_FRAME_ERROR", detail.to_string())
}

fn limit_error(detail: impl std::fmt::Display) -> ProtocolError {
    ProtocolError::new(
        ERROR_LIMIT_EXCEEDED,
        "PIPESTREAM_LIMIT_EXCEEDED",
        detail.to_string(),
    )
}

fn store_error(error: StoreError) -> ProtocolError {
    match error {
        StoreError::Protocol(error) => error,
        other => storage_error(other),
    }
}

fn close_for_error(connection: &quinn::Connection, error: &ProtocolError) {
    connection.close(error.code.into(), error.to_string().as_bytes());
}

fn sync_directory(path: &Path) -> std::io::Result<()> {
    fs::File::open(path)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pipestream_core::ChunkInfo;

    pub(super) fn test_options(directory: &Path, name: &str) -> RecursiveServerOptions {
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(["localhost".to_owned()]).unwrap();
        let certificate = directory.join(format!("{name}.crt"));
        let private_key = directory.join(format!("{name}.key"));
        fs::write(&certificate, cert.pem()).unwrap();
        fs::write(&private_key, signing_key.serialize_pem()).unwrap();
        RecursiveServerOptions {
            bind: "127.0.0.1:0".parse().unwrap(),
            certificate,
            private_key,
            state_database: directory.join("sessions.sqlite3"),
            entity_directory: directory.join("entities"),
            ready_file: None,
            once: true,
            max_scope_depth: 7,
            max_entities_per_scope: 1_000,
            max_entity_bytes: 1 << 20,
            max_chunks_per_entity: 1_024,
            max_concurrent_connections: 8,
        }
    }

    fn service(
        options: &RecursiveServerOptions,
    ) -> RecursiveService<ExemplarProcessor, FileEntityStore> {
        RecursiveService::with_limits(
            Arc::new(SqliteSessionStore::open(&options.state_database).unwrap()),
            Arc::new(FileEntityStore::open(&options.entity_directory).unwrap()),
            Arc::new(ExemplarProcessor::default()),
            RecursiveLimits {
                max_scope_depth: options.max_scope_depth,
                max_entities_per_scope: options.max_entities_per_scope,
                max_entity_bytes: options.max_entity_bytes,
                max_chunks_per_entity: options.max_chunks_per_entity,
            },
        )
        .unwrap()
    }

    async fn start_once(
        options: &RecursiveServerOptions,
    ) -> (SocketAddr, tokio::task::JoinHandle<Result<()>>) {
        let server = RecursiveServer::bind(options, service(options)).unwrap();
        let address = server.local_addr().unwrap();
        let task = tokio::spawn(server.run(true));
        (address, task)
    }

    pub(super) fn client_options(
        options: &RecursiveServerOptions,
        remote: SocketAddr,
    ) -> RecursiveClientOptions {
        RecursiveClientOptions {
            identity: None,
            remote,
            ca_certificate: options.certificate.clone(),
            server_name: "localhost".to_owned(),
        }
    }

    fn chunk(
        session_id: &str,
        total_chunks: u64,
        chunk_index: u64,
        chunk_offset: u64,
        payload: &[u8],
    ) -> EntityChunk {
        let mut header = exemplar_header(session_id, 1, None, None, "complete", payload);
        header.chunk_info = Some(ChunkInfo {
            total_chunks,
            chunk_index,
            chunk_offset,
        });
        EntityChunk {
            header,
            payload: payload.to_vec(),
        }
    }

    #[test]
    fn file_store_is_immutable_and_idempotent() {
        let directory = tempfile::tempdir().unwrap();
        let store = FileEntityStore::open(directory.path()).unwrap();
        let key = EntityKey {
            scope_id: 2,
            entity_id: 4,
        };
        store.put("session-1", key, b"first").unwrap();
        store.put("session-1", key, b"first").unwrap();
        assert_eq!(
            std::io::ErrorKind::AlreadyExists,
            store.put("session-1", key, b"second").unwrap_err().kind()
        );
        assert_eq!(
            std::io::ErrorKind::InvalidInput,
            store.put("../escape", key, b"first").unwrap_err().kind()
        );
        assert!(!directory.path().join("escape").exists());
    }

    #[test]
    fn exemplar_rehydration_is_independent_of_arrival_order() {
        let processor = ExemplarProcessor::default();
        let mut first = Session::new("order-1", 7, 10).unwrap();
        let root = first
            .add_root(NewEntity {
                entity_id: 1,
                layer: 0,
                payload_digest: [1; 32],
                policy: None,
            })
            .unwrap();
        first.transition(root, EntityState::Processing).unwrap();
        first.begin_dehydrating(root).unwrap();
        first.open_child_scope(root, 1, 1).unwrap();
        for id in [3, 1, 2] {
            let key = first
                .add_child(
                    1,
                    NewEntity {
                        entity_id: id,
                        layer: 0,
                        payload_digest: [id as u8; 32],
                        policy: None,
                    },
                )
                .unwrap();
            first.transition(key, EntityState::Processing).unwrap();
            first.complete_entity(key, [id as u8; 32]).unwrap();
        }
        first.close_scope(1).unwrap();
        first.begin_rehydration(root).unwrap();
        let execution = first
            .acquire_execution(
                None,
                ExecutionKey {
                    entity: root,
                    stage: ExecutionStage::Rehydrate,
                },
                10,
                100,
            )
            .unwrap()
            .unwrap();
        let digest = processor.rehydrate(RehydrateContext {
            execution: &execution,
            session: &first,
            parent: root,
        });
        first.manifests.get_mut(&root).unwrap().children.reverse();
        assert_eq!(
            digest,
            processor.rehydrate(RehydrateContext {
                execution: &execution,
                session: &first,
                parent: root,
            })
        );
    }

    #[tokio::test]
    async fn recursive_tree_runs_over_quic_and_persists_verified_lineage() {
        let directory = tempfile::tempdir().unwrap();
        let options = test_options(directory.path(), "recursive");
        let (remote, server) = start_once(&options).await;
        let result =
            run_recursive_scenario(&client_options(&options, remote), "recursive-network-1")
                .await
                .unwrap();
        server.await.unwrap().unwrap();
        assert_eq!(
            vec![
                EntityKey {
                    scope_id: 1,
                    entity_id: 3,
                },
                EntityKey {
                    scope_id: 1,
                    entity_id: 1,
                },
                EntityKey {
                    scope_id: 2,
                    entity_id: 2,
                },
                EntityKey {
                    scope_id: 2,
                    entity_id: 1,
                },
                EntityKey {
                    scope_id: 1,
                    entity_id: 2,
                },
                EntityKey {
                    scope_id: 0,
                    entity_id: 1,
                },
            ],
            result.completion_order
        );
        let store = SqliteSessionStore::open(&options.state_database).unwrap();
        let persisted = store.load("recursive-network-1").unwrap().unwrap();
        let lineage = persisted.session.final_lineage_digest().unwrap();
        assert_eq!(
            lineage.as_slice(),
            fs::read(
                options
                    .entity_directory
                    .join("recursive-network-1/lineage.sha256")
            )
            .unwrap()
        );
        store.integrity_check().unwrap();
    }

    #[tokio::test]
    async fn chunked_entity_reassembles_out_of_order_over_quic() {
        let directory = tempfile::tempdir().unwrap();
        let options = test_options(directory.path(), "chunks");
        let (remote, server) = start_once(&options).await;
        let mut client = RecursiveClient::connect(&client_options(&options, remote))
            .await
            .unwrap();
        let chunks = vec![
            chunk("chunk-network-1", 3, 2, 10, b"-gamma"),
            chunk("chunk-network-1", 3, 0, 0, b"alpha"),
            chunk("chunk-network-1", 3, 1, 5, b"-beta"),
        ];
        let statuses = client.send_chunked_entity(&chunks, 0).await.unwrap();
        expect_states(&statuses, &[EntityState::Processing, EntityState::Complete]).unwrap();
        client.goaway(1).await.unwrap();
        server.await.unwrap().unwrap();
        assert_eq!(
            b"alpha-beta-gamma",
            fs::read(
                options
                    .entity_directory
                    .join("chunk-network-1/scope-0/entity-1.bin")
            )
            .unwrap()
            .as_slice()
        );
    }

    #[tokio::test]
    async fn duplicate_chunk_index_is_a_named_refusal() {
        let directory = tempfile::tempdir().unwrap();
        let options = test_options(directory.path(), "duplicate-chunk");
        let (remote, server) = start_once(&options).await;
        let mut client = RecursiveClient::connect(&client_options(&options, remote))
            .await
            .unwrap();
        let chunks = vec![
            chunk("duplicate-chunk-1", 2, 0, 0, b"first"),
            chunk("duplicate-chunk-1", 2, 0, 5, b"second"),
        ];
        let error = client.send_chunked_entity(&chunks, 0).await.unwrap_err();
        assert!(error.to_string().contains("PIPESTREAM_ENTITY_INVALID"));
        assert!(error.to_string().contains("chunk-index is duplicated"));
        assert!(server.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn overlapping_chunk_ranges_are_a_named_refusal() {
        let directory = tempfile::tempdir().unwrap();
        let options = test_options(directory.path(), "overlapping-chunk");
        let (remote, server) = start_once(&options).await;
        let mut client = RecursiveClient::connect(&client_options(&options, remote))
            .await
            .unwrap();
        let chunks = vec![
            chunk("overlapping-chunk-1", 2, 0, 0, b"first"),
            chunk("overlapping-chunk-1", 2, 1, 4, b"second"),
        ];
        let error = client.send_chunked_entity(&chunks, 0).await.unwrap_err();
        assert!(error.to_string().contains("PIPESTREAM_ENTITY_INVALID"));
        assert!(error.to_string().contains("gap, overlap"));
        assert!(server.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn configured_chunk_limit_is_a_named_refusal() {
        let directory = tempfile::tempdir().unwrap();
        let mut options = test_options(directory.path(), "chunk-limit");
        options.max_chunks_per_entity = 1;
        let (remote, server) = start_once(&options).await;
        let mut client = RecursiveClient::connect(&client_options(&options, remote))
            .await
            .unwrap();
        let chunks = vec![
            chunk("chunk-limit-1", 2, 0, 0, b"first"),
            chunk("chunk-limit-1", 2, 1, 5, b"second"),
        ];
        let error = client.send_chunked_entity(&chunks, 0).await.unwrap_err();
        assert!(error.to_string().contains("PIPESTREAM_LIMIT_EXCEEDED"));
        assert!(error.to_string().contains("chunk count"));
        assert!(server.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn yielded_client_runtime_exit_notifies_server_without_claiming_completion() {
        let directory = tempfile::tempdir().unwrap();
        let options = test_options(directory.path(), "yield-runtime-exit");
        let (remote, mut server) = start_once(&options).await;
        let client_options = client_options(&options, remote);
        let claim = tokio::task::spawn_blocking(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(begin_durable_yield(&client_options, "yield-runtime-exit"))
            // The isolated client runtime stops immediately, as it does in the CLI.
        })
        .await
        .unwrap()
        .unwrap();
        let stopped = tokio::time::timeout(Duration::from_secs(3), &mut server).await;
        if stopped.is_err() {
            server.abort();
            let _ = server.await;
        }
        stopped
            .expect("client exit left the server waiting for the idle timeout")
            .unwrap()
            .unwrap();
        let store = SqliteSessionStore::open(&options.state_database).unwrap();
        let session = store.load("yield-runtime-exit").unwrap().unwrap().session;
        assert_eq!(
            session.entities[&EntityKey {
                scope_id: 0,
                entity_id: 1
            }]
                .state,
            EntityState::Deferred
        );
        assert!(session.claims[&claim.claim_id].redeemed_at_micros.is_none());
    }

    #[tokio::test]
    async fn durable_yield_moves_to_another_server_and_rejects_replay() {
        let directory = tempfile::tempdir().unwrap();
        let options = test_options(directory.path(), "recovery");

        let (first_remote, first_server) = start_once(&options).await;
        let claim = begin_durable_yield(
            &client_options(&options, first_remote),
            "recovery-network-1",
        )
        .await
        .unwrap();
        first_server.await.unwrap().unwrap();

        let (second_remote, second_server) = start_once(&options).await;
        finish_durable_yield(&client_options(&options, second_remote), &claim)
            .await
            .unwrap();
        second_server.await.unwrap().unwrap();

        let store = SqliteSessionStore::open(&options.state_database).unwrap();
        let persisted = store.load("recovery-network-1").unwrap().unwrap();
        let root = EntityKey {
            scope_id: 0,
            entity_id: 1,
        };
        assert_eq!(
            EntityState::Complete,
            persisted.session.entities[&root].state
        );
        assert!(
            persisted.session.claims[&claim.claim_id]
                .redeemed_at_micros
                .is_some()
        );
        assert_eq!(
            persisted.session.final_lineage_digest().unwrap().as_slice(),
            fs::read(
                options
                    .entity_directory
                    .join("recovery-network-1/lineage.sha256")
            )
            .unwrap()
        );

        let (third_remote, third_server) = start_once(&options).await;
        let error = finish_durable_yield(&client_options(&options, third_remote), &claim)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("PIPESTREAM_CLAIM_NOT_FOUND"));
        assert!(third_server.await.unwrap().is_err());
    }

    #[test]
    fn concurrent_recovery_fences_resume_across_store_handles() {
        let directory = tempfile::tempdir().unwrap();
        let options = test_options(directory.path(), "interrupted-resume");
        let store = Arc::new(SqliteSessionStore::open(&options.state_database).unwrap());
        let entities = Arc::new(FileEntityStore::open(&options.entity_directory).unwrap());
        #[derive(Default)]
        struct CountingResume(AtomicU64);
        impl EntityProcessor for CountingResume {
            fn process(
                &self,
                _: ProcessContext<'_>,
            ) -> Result<ProcessingDisposition, ProtocolError> {
                unreachable!()
            }
            fn rehydrate(&self, _: RehydrateContext<'_>) -> [u8; 32] {
                unreachable!()
            }
            fn resume(&self, context: ResumeContext<'_>) -> [u8; 32] {
                self.0.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(50));
                ExemplarProcessor::default().resume(context)
            }
        }
        let processor = Arc::new(CountingResume::default());
        let mut session = Session::new("interrupted-resume-1", 7, 1_000).unwrap();
        let entity = session
            .add_root(NewEntity {
                entity_id: 1,
                layer: 0,
                payload_digest: [3; 32],
                policy: None,
            })
            .unwrap();
        session.transition(entity, EntityState::Processing).unwrap();
        let checksum = [5; 32];
        let token = b"durable-continuation".to_vec();
        session
            .defer_with_claim_id(
                entity,
                token.clone(),
                StoppingPointValidation {
                    state_checksum: Some(checksum),
                    bytes_processed: Some(19),
                    children_complete: Some(0),
                    children_total: Some(0),
                    is_resumable: Some(true),
                    checkpoint_ref: Some("crash-boundary".to_owned()),
                },
                99,
                10_000,
                1_000,
            )
            .unwrap();
        store.create(&session).unwrap();

        store
            .transact("interrupted-resume-1", |session| {
                session.redeem_claim(99, checksum, 2_000)?;
                session.enqueue_job(
                    ExecutionKey {
                        entity,
                        stage: ExecutionStage::Resume { claim_id: 99 },
                    },
                    JobInput::Resume { claim_id: 99 },
                    2_000,
                )
            })
            .unwrap();
        assert_eq!(
            EntityState::Processing,
            store
                .load("interrupted-resume-1")
                .unwrap()
                .unwrap()
                .session
                .entities[&entity]
                .state
        );

        let service =
            RecursiveService::new(store.clone(), entities.clone(), processor.clone(), 7, 1_000)
                .unwrap();
        let second = RecursiveService::new(
            Arc::new(SqliteSessionStore::open(&options.state_database).unwrap()),
            entities,
            processor.clone(),
            7,
            1_000,
        )
        .unwrap();
        let start = std::sync::Barrier::new(2);
        let count = std::thread::scope(|threads| {
            let a = threads.spawn(|| {
                start.wait();
                service.recover_interrupted_resumptions().unwrap()
            });
            let b = threads.spawn(|| {
                start.wait();
                second.recover_interrupted_resumptions().unwrap()
            });
            a.join().unwrap() + b.join().unwrap()
        });
        assert_eq!(1, count);
        assert_eq!(1, processor.0.load(Ordering::SeqCst));
        assert_eq!(0, service.recover_interrupted_resumptions().unwrap());
        let recovered = store.load("interrupted-resume-1").unwrap().unwrap();
        assert_eq!(
            EntityState::Complete,
            recovered.session.entities[&entity].state
        );
        assert_eq!(
            Some(tagged_digest(b"pipestream-resumed-v1", &token)),
            recovered.session.entities[&entity].output_digest
        );
        assert_eq!(
            recovered.session.final_lineage_digest().unwrap().as_slice(),
            fs::read(
                options
                    .entity_directory
                    .join("interrupted-resume-1/lineage.sha256")
            )
            .unwrap()
        );
    }
}
