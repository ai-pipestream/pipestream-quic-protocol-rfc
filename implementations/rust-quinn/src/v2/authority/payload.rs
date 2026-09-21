//! Bounded immutable object storage. Private root ownership and live handles
//! protect installation from concurrent cleanup. Database admission is separate:
//! an installed object without a committing reference is still an orphan.

use super::*;
use crate::persistence::StoreIdentity;
use pipestream_storage::ObjectStore as _;
use rand::{TryRng, rngs::SysRng};
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::PathBuf,
    sync::{
        Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

const MAGIC: &[u8; 8] = b"PSOBJ004";
const ROOT_MAGIC: &[u8; 8] = b"PSROOT04";
const HEADER_LIMIT: usize = 512;
const OVERHEAD: u64 = 12 + HEADER_LIMIT as u64;
const CONFIG: &str = "binding";
const LOCK: &str = "root.lock";
/// Subdirectory holding the SPI plane's store when the backend is
/// SpiFilesystem. Never part of the local file grammar.
const SPI_DIR: &str = "spi";

/// Storage backend selection for a payload root, persisted in the store
/// binding so reopen refuses a changed backend (matching the policy
/// exactness check). Slice 2 of the SPI adoption plan wires only the seam;
/// `Filesystem` is the current behavior and the only backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PayloadBackendKind {
    /// The existing stage-file filesystem store.
    #[default]
    Filesystem,
    /// SPI plane on the local pipestream-storage filesystem store.
    SpiFilesystem,
    /// SPI plane on PostgreSQL.
    SpiPostgres,
    /// SPI plane on S3.
    SpiS3,
    /// SPI plane on MongoDB.
    SpiMongo,
}

impl PayloadBackendKind {
    const FILESYSTEM_TAG: u64 = 1;
    const SPI_FILESYSTEM_TAG: u64 = 2;
    const SPI_POSTGRES_TAG: u64 = 3;
    const SPI_S3_TAG: u64 = 4;
    const SPI_MONGO_TAG: u64 = 5;

    fn tag(self) -> Id {
        Id(match self {
            PayloadBackendKind::Filesystem => Self::FILESYSTEM_TAG,
            PayloadBackendKind::SpiFilesystem => Self::SPI_FILESYSTEM_TAG,
            PayloadBackendKind::SpiPostgres => Self::SPI_POSTGRES_TAG,
            PayloadBackendKind::SpiS3 => Self::SPI_S3_TAG,
            PayloadBackendKind::SpiMongo => Self::SPI_MONGO_TAG,
        })
    }

    fn from_tag(tag: u64) -> Result<Self> {
        match tag {
            Self::FILESYSTEM_TAG => Ok(Self::Filesystem),
            Self::SPI_FILESYSTEM_TAG => Ok(Self::SpiFilesystem),
            Self::SPI_POSTGRES_TAG => Ok(Self::SpiPostgres),
            Self::SPI_S3_TAG => Ok(Self::SpiS3),
            Self::SPI_MONGO_TAG => Ok(Self::SpiMongo),
            _ => Err(StoreError::Corrupt("unknown payload backend tag")),
        }
    }

    /// CLI/config spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            PayloadBackendKind::Filesystem => "fs",
            PayloadBackendKind::SpiFilesystem => "spi-fs",
            PayloadBackendKind::SpiPostgres => "spi-postgres",
            PayloadBackendKind::SpiS3 => "spi-s3",
            PayloadBackendKind::SpiMongo => "spi-mongo",
        }
    }
}

impl std::fmt::Display for PayloadBackendKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for PayloadBackendKind {
    type Err = StoreError;

    fn from_str(name: &str) -> Result<Self> {
        match name {
            "fs" => Ok(PayloadBackendKind::Filesystem),
            "spi-fs" => Ok(PayloadBackendKind::SpiFilesystem),
            "spi-postgres" => Ok(PayloadBackendKind::SpiPostgres),
            "spi-s3" => Ok(PayloadBackendKind::SpiS3),
            "spi-mongo" => Ok(PayloadBackendKind::SpiMongo),
            _ => Err(StoreError::Corrupt("unknown payload backend")),
        }
    }
}

/// A complete payload backend selection: the persisted kind plus the
/// connection parameters needed to reach a service backend. The binding
/// stores the kind and a fingerprint of the connection string, so reopening
/// against a different backend or a different endpoint is refused.
#[derive(Debug, Clone)]
pub enum PayloadBackendSpec {
    /// The default stage-file store (no connection parameters).
    Filesystem,
    /// SPI plane on the local pipestream-storage filesystem store.
    SpiFilesystem,
    /// SPI plane on PostgreSQL; `url` is a full `postgres://` URL.
    SpiPostgres { url: String },
    /// SPI plane on S3; credentials travel with the config.
    SpiS3 { config: Box<pipestream_storage::S3Config> },
    /// SPI plane on MongoDB.
    SpiMongo { uri: String, database: String },
}

impl PayloadBackendSpec {
    /// The persisted backend kind.
    pub fn kind(&self) -> PayloadBackendKind {
        match self {
            PayloadBackendSpec::Filesystem => PayloadBackendKind::Filesystem,
            PayloadBackendSpec::SpiFilesystem => PayloadBackendKind::SpiFilesystem,
            PayloadBackendSpec::SpiPostgres { .. } => PayloadBackendKind::SpiPostgres,
            PayloadBackendSpec::SpiS3 { .. } => PayloadBackendKind::SpiS3,
            PayloadBackendSpec::SpiMongo { .. } => PayloadBackendKind::SpiMongo,
        }
    }

    /// SHA-256 of the canonical connection parameters, persisted in the
    /// store binding. Empty for local backends. For S3 the fingerprint
    /// covers endpoint/bucket/region/prefix only, so rotating credentials
    /// does not look like a different store.
    fn connection_fingerprint(&self) -> Option<[u8; 32]> {
        let canonical = match self {
            PayloadBackendSpec::Filesystem | PayloadBackendSpec::SpiFilesystem => return None,
            PayloadBackendSpec::SpiPostgres { url } => url.clone(),
            PayloadBackendSpec::SpiS3 { config } => format!(
                "{}
{}
{}
{}",
                config.endpoint_url.as_deref().unwrap_or(""),
                config.bucket,
                config.region,
                config.prefix
            ),
            PayloadBackendSpec::SpiMongo { uri, database } => format!("{uri}
{database}"),
        };
        Some(Sha256::digest(canonical.as_bytes()).into())
    }
}

/// The checksummed binding body: CBOR array
/// [backend tag, connection fingerprint (empty for local backends), policy].
fn encode_root_config(
    spec: &PayloadBackendSpec,
    policy: &PayloadPolicy,
) -> Result<Vec<u8>> {
    let mut writer = codec::Writer::new();
    writer.array(3);
    spec.kind().tag().write(&mut writer);
    match spec.connection_fingerprint() {
        Some(hash) => writer.bytes(&hash),
        None => writer.bytes(&[]),
    };
    policy.write(&mut writer);
    Ok(writer.finish())
}

fn decode_root_config(bytes: &[u8]) -> Result<(PayloadBackendKind, Option<[u8; 32]>, PayloadPolicy)> {
    let mut decoder = minicbor::Decoder::new(bytes);
    codec::array(&mut decoder, 3)?;
    let tag = Id::read(&mut decoder)?;
    let fingerprint = decoder
        .bytes()
        .map_err(|_| StoreError::Corrupt("payload binding fingerprint is not bytes"))?;
    let fingerprint = match fingerprint.len() {
        0 => None,
        32 => Some(<[u8; 32]>::try_from(fingerprint).expect("length checked")),
        _ => return Err(StoreError::Corrupt("payload binding fingerprint length changed")),
    };
    let policy = PayloadPolicy::read(&mut decoder)?;
    if decoder.position() != bytes.len() {
        return Err(StoreError::Corrupt("payload binding has trailing bytes"));
    }
    Ok((PayloadBackendKind::from_tag(tag.0)?, fingerprint, policy))
}


mod audit;
mod local;
mod read_credit;
mod reservations;
pub(super) use read_credit::ReadCredit;
use reservations::{Funding, ReservationEntry};
pub use reservations::{OutputReservation, OutputStaging, ReservationUsage};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PayloadPolicy {
    pub objects: Id,
    /// Charged payload length plus a bounded per-object header allowance,
    /// including incomplete reception. This is not allocated filesystem blocks.
    pub bytes: Number,
    pub owner_objects: Id,
    pub owner_bytes: Number,
    pub chunk_bytes: Id,
    pub handles: Id,
    pub owner_handles: Id,
}
impl Wire for PayloadPolicy {
    fn read(d: &mut minicbor::Decoder<'_>) -> std::result::Result<Self, Error> {
        codec::array(d, 7)?;
        let result = Self {
            objects: Id::read(d)?,
            bytes: Number::read(d)?,
            owner_objects: Id::read(d)?,
            owner_bytes: Number::read(d)?,
            chunk_bytes: Id::read(d)?,
            handles: Id::read(d)?,
            owner_handles: Id::read(d)?,
        };
        result.check()?;
        Ok(result)
    }
    fn write(&self, w: &mut codec::Writer) {
        w.array(7);
        self.objects.write(w);
        self.bytes.write(w);
        self.owner_objects.write(w);
        self.owner_bytes.write(w);
        self.chunk_bytes.write(w);
        self.handles.write(w);
        self.owner_handles.write(w);
    }
    fn check(&self) -> std::result::Result<(), Error> {
        self.objects.check()?;
        self.bytes.check()?;
        self.owner_objects.check()?;
        self.owner_bytes.check()?;
        self.chunk_bytes.check()?;
        self.handles.check()?;
        self.owner_handles.check()?;
        require(
            self.objects.0 <= 1_000_000
                && self.owner_objects <= self.objects
                && self.owner_bytes <= self.bytes
                && self.chunk_bytes.0 <= 1_048_576
                && self.handles.0 <= 65536
                && self.owner_handles <= self.handles,
            "invalid payload policy ceilings",
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Header {
    owner: IdentityLabel,
    input: Input,
    funding: Option<Funding>,
}
impl Wire for Header {
    fn read(d: &mut minicbor::Decoder<'_>) -> std::result::Result<Self, Error> {
        codec::array(d, 3)?;
        Ok(Self {
            owner: IdentityLabel::read(d)?,
            input: Input::read(d)?,
            funding: Option::<Funding>::read(d)?,
        })
    }
    fn write(&self, w: &mut codec::Writer) {
        w.array(3);
        self.owner.write(w);
        self.input.write(w);
        self.funding.write(w);
    }
    fn check(&self) -> std::result::Result<(), Error> {
        self.owner.check()?;
        self.input.check()?;
        self.funding.check()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PayloadUsage {
    /// Charged slots: ordinary objects plus reservation files and their entire
    /// promised output count, including outputs not produced yet.
    pub objects: u64,
    pub charged_bytes: u64,
    pub incomplete_objects: u64,
}

#[derive(Debug)]
struct Entry {
    owner: IdentityLabel,
    input: Option<Input>,
    maximum: Number,
    funding: Option<Funding>,
    offset: u64,
    incomplete: bool,
    live: usize,
    // The staging/installed token (not additional readers) borrows a worker's
    // already charged reservation I/O slot. Never persisted across processes.
    prepaid: bool,
}
impl Entry {
    fn charge(&self) -> Result<u64> {
        if self.funding.is_some() {
            Ok(0)
        } else {
            charge(self.maximum)
        }
    }
    fn header(&self) -> Result<Header> {
        Ok(Header {
            owner: self.owner.clone(),
            input: self
                .input
                .clone()
                .ok_or(StoreError::Corrupt("output is not finished"))?,
            funding: self.funding.clone(),
        })
    }
}

/// One lock covers ordinary objects and durable output reservations. The map
/// dereference is only the object inventory, never the aggregate quota count.
#[derive(Default)]
struct Inventory {
    objects: BTreeMap<String, Entry>,
    reservations: BTreeMap<String, ReservationEntry>,
    readers: BTreeMap<u64, read_credit::CreditEntry>,
    next_reader: u64,
}
impl std::ops::Deref for Inventory {
    type Target = BTreeMap<String, Entry>;
    fn deref(&self) -> &Self::Target {
        &self.objects
    }
}
impl std::ops::DerefMut for Inventory {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.objects
    }
}
struct RootLock {
    file: File,
    process: u32,
}
impl Drop for RootLock {
    fn drop(&mut self) {
        if self.process == std::process::id() {
            let _ = rustix::fs::flock(&self.file, rustix::fs::FlockOperation::Unlock);
        }
    }
}
struct Root {
    path: PathBuf,
    binding: StoreIdentity,
    backend: PayloadBackendKind,
    /// The SPI object plane (payload bytes only) when the backend selects
    /// it. The mutex serializes begin_put, which takes &mut self.
    spi: Option<Mutex<PlaneStore>>,
    policy: PayloadPolicy,
    entries: Mutex<Inventory>,
    uncertain: AtomicBool,
    worker_pool: AtomicBool,
    recovered_stages: usize,
    _lock: RootLock,
}
#[derive(Clone)]
pub struct PayloadStore {
    root: Arc<Root>,
}

pub(super) struct WorkerPoolPin(Arc<Root>);
impl Drop for WorkerPoolPin {
    fn drop(&mut self) {
        self.0.worker_pool.store(false, Ordering::Release);
    }
}

fn charge(length: Number) -> Result<u64> {
    length
        .0
        .checked_add(OVERHEAD)
        .filter(|v| *v <= MAX_NUMBER)
        .ok_or_else(|| protocol(ErrorCode::LimitExceeded, "payload charge overflow"))
}
fn io(error: std::io::Error) -> StoreError {
    if error.raw_os_error().is_some_and(|code| {
        code == rustix::io::Errno::NOSPC.raw_os_error()
            || code == rustix::io::Errno::DQUOT.raw_os_error()
    }) {
        protocol(
            ErrorCode::LimitExceeded,
            "payload filesystem capacity exhausted",
        )
    } else {
        error.into()
    }
}
fn valid_key(key: &str) -> bool {
    key.len() == 32
        && key
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn checked_file(path: &Path) -> Result<fs::Metadata> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.nlink() != 1 {
        return Err(StoreError::Corrupt(
            "payload path is not a private regular file",
        ));
    }
    Ok(metadata)
}
fn new_file(path: &Path) -> Result<File> {
    OpenOptions::new()
        .write(true)
        .read(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(io)
}
fn new_staging_file(root: &Root, path: &Path) -> Result<File> {
    new_file(path).inspect_err(|_| {
        root.uncertain.store(true, Ordering::Release);
    })
}
fn install_file(root: &Root, source: &Path, destination: &Path) -> Result<()> {
    fs::rename(source, destination).map_err(|error| {
        // Do not guess whether a failed namespace operation reached disk. No
        // cleanup or new capacity is allowed until exclusive reopen audits it.
        root.uncertain.store(true, Ordering::Release);
        io(error)
    })
}
fn sync(root: &Root) -> Result<()> {
    fn attempt(root: &Root) -> Result<()> {
        #[cfg(test)]
        if crate::v2::client::results::tests::fail_sync() {
            return Err(StoreError::Io(std::io::Error::other(
                "injected directory sync failure",
            )));
        }
        crate::persistence::sync_directory(&root.path).map_err(|error| match error {
            crate::persistence::StoreError::Io(error) => io(error),
            other => StoreError::Physical(other),
        })
    }
    attempt(root).inspect_err(|_| {
        // A completed rename/unlink is not a durable namespace commitment if
        // directory synchronization failed. Reads and capacity decisions must
        // wait for exclusive reopen to audit and synchronize that namespace.
        root.uncertain.store(true, Ordering::Release);
    })
}

impl Root {
    fn owned(&self) -> Result<()> {
        if self._lock.process != std::process::id() {
            return Err(StoreError::Corrupt(
                "payload root handle inherited across fork",
            ));
        }
        if self.uncertain.load(Ordering::Acquire) {
            return Err(StoreError::Corrupt(
                "payload namespace outcome uncertain; exclusive reopen required",
            ));
        }
        Ok(())
    }
    fn entries(&self) -> Result<MutexGuard<'_, Inventory>> {
        self.owned()?;
        let entries = self
            .entries
            .lock()
            .map_err(|_| StoreError::Corrupt("payload inventory poisoned"))?;
        // A writer may quarantine the root while this caller waits for the
        // inventory lock. Recheck before permitting any new quota decision.
        self.owned()?;
        Ok(entries)
    }
    fn path(&self, key: &str, incomplete: bool) -> PathBuf {
        self.path.join(format!(
            "{}-{key}",
            if incomplete { "stage" } else { "object" }
        ))
    }
}

/// Maps an SPI failure into the store's taxonomy: transient SPI failures
/// are I/O; everything else here is corruption of retained state.
fn spi(error: pipestream_storage::StorageError) -> StoreError {
    match error {
        pipestream_storage::StorageError::Transient(detail) => {
            StoreError::Io(std::io::Error::other(detail))
        }
        other => StoreError::Corrupt(format!("payload SPI plane: {other}").leak()),
    }
}

/// Retires SPI-plane objects with no local object stub: the kill window
/// between the SPI put and the stub write leaves an entity nothing local
/// references.
fn sweep_spi_orphans(root: &mut Root) -> Result<()> {
    let Some(plane) = &root.spi else {
        return Ok(());
    };
    let mut plane = plane
        .lock()
        .map_err(|_| StoreError::Corrupt("payload SPI plane poisoned"))?;
    let stub_prefixes: std::collections::BTreeSet<String> = {
        let entries = root
            .entries
            .lock()
            .map_err(|_| StoreError::Corrupt("payload inventory poisoned"))?;
        entries.keys().map(|key| key[..8].to_owned()).collect()
    };
    let tag = plane_tag(root.binding);
    for scope in plane.list_scopes().map_err(spi)? {
        // Only this installation's plane scopes (depth 1: [tag, prefix]);
        // anything else (another installation's objects, another lane's
        // plane, anything foreign) is left alone — never an error.
        if scope.depth() != 1 || scope.components()[0] != tag {
            continue;
        }
        let prefix = format!("{:08x}", scope.scope_id());
        if !stub_prefixes.contains(&prefix) {
            // Absent locally: never referenced by an admission.
            match plane.retire(&scope, 0) {
                Ok(_) | Err(pipestream_storage::StorageError::NotFound(_)) => {}
                Err(error) => return Err(spi(error)),
            }
        }
    }
    Ok(())
}

/// The SPI plane scope of one payload object:
/// `root(<installation tag>)/<key prefix>`. The installation tag is the
/// first four bytes of the store binding, so installations sharing one
/// service database (concurrent certification rows, both lanes on one
/// `probe` database) never see each other's objects — crucially the orphan
/// sweep only retires its own installation's entities.
fn plane_scope(binding: StoreIdentity, key: &str) -> pipestream_storage::ScopePath {
    pipestream_storage::ScopePath::root(plane_tag(binding))
        .child(plane_entity(key))
        .expect("plane scope is depth 1")
}

/// The first four bytes of the store binding, as the plane root scope.
fn plane_tag(binding: StoreIdentity) -> u32 {
    u32::from_be_bytes(binding.as_bytes()[..4].try_into().expect("16-byte identity"))
}

/// The SPI plane entity of an object is always 0; the payload key's first 8
/// hex characters become the child-scope component.
fn plane_entity(key: &str) -> u32 {
    u32::from_str_radix(&key[..8], 16).expect("payload keys are 32 lowercase hex")
}

/// Boxed streaming writer over any SPI object backend.
trait PlaneWriter: std::io::Write + Send {
    fn finish(self: Box<Self>) -> std::result::Result<(), pipestream_storage::StorageError>;
}

impl<W: pipestream_storage::PutWriter + Send + 'static> PlaneWriter for W {
    fn finish(self: Box<Self>) -> std::result::Result<(), pipestream_storage::StorageError> {
        pipestream_storage::PutWriter::finish(*self)
    }
}

/// The SPI object plane over any pipestream-storage object backend. Only
/// the operations the payload store needs; readers and writers are boxed
/// because associated types differ per backend.
enum PlaneStore {
    Filesystem(pipestream_storage::FsStore),
    Postgres(pipestream_storage::PostgresStore),
    S3(pipestream_storage::S3Store),
    Mongo(pipestream_storage::MongoStore),
}

macro_rules! plane_dispatch {
    ($self:expr, $method:ident ( $( $arg:expr ),* )) => {
        match $self {
            PlaneStore::Filesystem(store) => store.$method($($arg),*),
            PlaneStore::Postgres(store) => store.$method($($arg),*),
            PlaneStore::S3(store) => store.$method($($arg),*),
            PlaneStore::Mongo(store) => store.$method($($arg),*),
        }
    };
}

impl PlaneStore {
    fn open(
        spec: &PayloadBackendSpec,
        root_path: &Path,
    ) -> Result<Option<Mutex<PlaneStore>>> {
        match spec {
            PayloadBackendSpec::Filesystem => Ok(None),
            PayloadBackendSpec::SpiFilesystem => Ok(Some(Mutex::new(PlaneStore::Filesystem(
                pipestream_storage::FsStore::open(root_path.join(SPI_DIR)).map_err(spi)?,
            )))),
            PayloadBackendSpec::SpiPostgres { url } => Ok(Some(Mutex::new(
                PlaneStore::Postgres(pipestream_storage::PostgresStore::open(url).map_err(spi)?),
            ))),
            PayloadBackendSpec::SpiS3 { config } => Ok(Some(Mutex::new(PlaneStore::S3(
                pipestream_storage::S3Store::open(config).map_err(spi)?,
            )))),
            PayloadBackendSpec::SpiMongo { uri, database } => Ok(Some(Mutex::new(
                PlaneStore::Mongo(
                    pipestream_storage::MongoStore::open(&pipestream_storage::MongoConfig {
                        uri: uri.clone(),
                        database: database.clone(),
                    })
                    .map_err(spi)?,
                ),
            ))),
        }
    }

    fn begin_put(
        &mut self,
        scope: &pipestream_storage::ScopePath,
        digest: &[u8; 32],
    ) -> std::result::Result<Box<dyn PlaneWriter>, pipestream_storage::StorageError> {
        Ok(match self {
            PlaneStore::Filesystem(store) => Box::new(store.begin_put(scope, 0, digest)?),
            PlaneStore::Postgres(store) => Box::new(store.begin_put(scope, 0, digest)?),
            PlaneStore::S3(store) => Box::new(store.begin_put(scope, 0, digest)?),
            PlaneStore::Mongo(store) => Box::new(store.begin_put(scope, 0, digest)?),
        })
    }

    fn open_read(
        &self,
        scope: &pipestream_storage::ScopePath,
    ) -> std::result::Result<Box<dyn std::io::Read + Send>, pipestream_storage::StorageError> {
        Ok(match self {
            PlaneStore::Filesystem(store) => Box::new(store.open_read(scope, 0)?),
            PlaneStore::Postgres(store) => Box::new(store.open_read(scope, 0)?),
            PlaneStore::S3(store) => Box::new(store.open_read(scope, 0)?),
            PlaneStore::Mongo(store) => Box::new(store.open_read(scope, 0)?),
        })
    }

    fn list(
        &self,
        scope: &pipestream_storage::ScopePath,
    ) -> std::result::Result<Vec<u32>, pipestream_storage::StorageError> {
        plane_dispatch!(self, list(scope))
    }

    fn list_scopes(
        &self,
    ) -> std::result::Result<Vec<pipestream_storage::ScopePath>, pipestream_storage::StorageError>
    {
        plane_dispatch!(self, list_scopes())
    }

    fn retire(
        &mut self,
        scope: &pipestream_storage::ScopePath,
        cursor: u64,
    ) -> std::result::Result<u64, pipestream_storage::StorageError> {
        plane_dispatch!(self, retire(scope, cursor))
    }
}

impl PayloadStore {
    pub(super) fn pin_worker_pool(&self) -> Result<WorkerPoolPin> {
        self.root.owned()?;
        self.root
            .worker_pool
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                protocol(
                    ErrorCode::Conflict,
                    "payload authority already has a worker pool",
                )
            })?;
        Ok(WorkerPoolPin(self.root.clone()))
    }
    /// Maximum chunk accepted by the configured storage root. Transports use
    /// this ceiling before allocating a reusable input/output buffer.
    pub fn chunk_limit(&self) -> usize {
        self.root.policy.chunk_bytes.0 as usize
    }
    /// Check permanent feasibility, not current occupancy: queued work may wait
    /// for other readers, but must be runnable under the immutable root policy.
    pub(super) fn check_execution_capacity(&self, parameters: &AdmitParameters) -> Result<()> {
        let ordinary = if parameters.outputs.count.0 == 0 {
            2
        } else {
            3
        };
        let required = if parameters.mode == Mode(2) {
            ordinary + 2
        } else if parameters.mode == Mode(1) {
            ordinary + 1
        } else {
            ordinary
        };
        if self.root.policy.handles.0 < required || self.root.policy.owner_handles.0 < required {
            return Err(protocol(
                ErrorCode::LimitExceeded,
                "payload policy cannot fund worker I/O",
            ));
        }
        Ok(())
    }
    /// Initialization only accepts a newly created directory. A damaged or lost
    /// existing root is never silently replaced, adopted, or cleared.
    ///
    /// Uses the default (filesystem) backend; see [`PayloadStore::initialize_with_backend`].
    pub fn initialize(path: &Path, binding: StoreIdentity, policy: PayloadPolicy) -> Result<Self> {
        Self::initialize_with_backend(path, binding, policy, &PayloadBackendSpec::Filesystem)
    }

    /// Like [`PayloadStore::initialize`], recording `backend` in the store
    /// binding so a later open with a different backend is refused.
    pub fn initialize_with_backend(
        path: &Path,
        binding: StoreIdentity,
        policy: PayloadPolicy,
        spec: &PayloadBackendSpec,
    ) -> Result<Self> {
        policy.check()?;
        fs::create_dir(path)?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
        let mut config = new_file(&path.join(CONFIG))?;
        let encoded = encode_root_config(spec, &policy)?;
        let mut bytes = Vec::with_capacity(24 + encoded.len() + 32);
        bytes.extend_from_slice(ROOT_MAGIC);
        bytes.extend_from_slice(binding.as_bytes());
        bytes.extend_from_slice(&encoded);
        let checksum = Sha256::digest(&bytes);
        bytes.extend_from_slice(&checksum);
        config.write_all(&bytes).map_err(io)?;
        config.sync_all()?;
        new_file(&path.join(LOCK))?.sync_all()?;
        crate::persistence::sync_directory(path)?;
        crate::persistence::sync_directory(
            path.parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new(".")),
        )?;
        Self::open_with_backend(path, binding, policy, spec)
    }

    /// Opens an existing root using the default (filesystem) backend; see
    /// [`PayloadStore::open_with_backend`].
    pub fn open(path: &Path, binding: StoreIdentity, policy: PayloadPolicy) -> Result<Self> {
        Self::open_with_backend(path, binding, policy, &PayloadBackendSpec::Filesystem)
    }

    /// Like [`PayloadStore::open`], refusing a root recorded under a
    /// different backend (or policy, or binding, as before).
    pub fn open_with_backend(
        path: &Path,
        binding: StoreIdentity,
        policy: PayloadPolicy,
        spec: &PayloadBackendSpec,
    ) -> Result<Self> {
        policy.check()?;
        let directory = fs::symlink_metadata(path)?;
        if !directory.is_dir() || directory.permissions().mode() & 0o077 != 0 {
            return Err(StoreError::Corrupt(
                "payload root must be a private directory",
            ));
        }
        let path = path.canonicalize()?;
        if checked_file(&path.join(LOCK))?.len() != 0 {
            return Err(StoreError::Corrupt("payload lock file is not empty"));
        }
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.join(LOCK))?;
        rustix::fs::flock(&lock, rustix::fs::FlockOperation::NonBlockingLockExclusive)
            .map_err(|_| protocol(ErrorCode::LimitExceeded, "payload root already owned"))?;
        let lock = RootLock {
            file: lock,
            process: std::process::id(),
        };
        let metadata = checked_file(&path.join(CONFIG))?;
        if !(56..=1024).contains(&metadata.len()) {
            return Err(StoreError::Corrupt("invalid payload binding length"));
        }
        let bytes = fs::read(path.join(CONFIG))?;
        if bytes.len() != metadata.len() as usize
            || &bytes[..8] != ROOT_MAGIC
            || &bytes[8..24] != binding.as_bytes()
            || Sha256::digest(&bytes[..bytes.len() - 32])[..] != bytes[bytes.len() - 32..]
        {
            return Err(StoreError::Corrupt(
                "payload authority binding or policy changed",
            ));
        }
        let (retained_backend, retained_fingerprint, retained_policy) =
            decode_root_config(&bytes[24..bytes.len() - 32])?;
        if retained_backend != spec.kind()
            || retained_fingerprint != spec.connection_fingerprint()
            || retained_policy != policy
        {
            return Err(StoreError::Corrupt(
                "payload backend, connection, authority binding, or policy changed",
            ));
        }
        let spi = PlaneStore::open(spec, &path)?;
        let mut root = Root {
            path,
            binding,
            backend: spec.kind(),
            spi,
            policy,
            entries: Mutex::new(Inventory::default()),
            uncertain: AtomicBool::new(false),
            worker_pool: AtomicBool::new(false),
            recovered_stages: 0,
            _lock: lock,
        };
        let mut entries = Inventory::default();
        let mut abandoned = BTreeMap::new();
        let mut scanned = 0u64;
        for entry in fs::read_dir(&root.path)? {
            let entry = entry?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| StoreError::Corrupt("non-UTF8 payload entry"))?;
            if name == CONFIG || name == LOCK || (name == SPI_DIR && root.spi.is_some()) {
                continue;
            }
            let (prefix, key) = name
                .split_once('-')
                .ok_or(StoreError::Corrupt("unexpected payload entry"))?;
            if !matches!(prefix, "stage" | "object" | "funding" | "reserve")
                || !valid_key(key)
                || entries.contains_key(key)
                || entries.reservations.contains_key(key)
                || abandoned.contains_key(key)
            {
                return Err(StoreError::Corrupt(
                    "unexpected or duplicate payload identity",
                ));
            }
            scanned += 1;
            if scanned > root.policy.objects.0 {
                return Err(StoreError::Corrupt(
                    "payload inventory exceeds object bound",
                ));
            }
            let incomplete = matches!(prefix, "stage" | "funding");
            if incomplete {
                // A stage is never referenceable by an admission. Exclusive
                // root ownership proves its former writer is gone. Its header
                // may be torn at any byte, so do not require a valid descriptor
                // to reclaim interrupted reception after restart.
                if checked_file(&entry.path())?.len() > root.policy.bytes.0 {
                    return Err(StoreError::Corrupt("abandoned stage exceeds root bound"));
                }
                abandoned.insert(key.to_owned(), entry.path());
                continue;
            }
            if prefix == "reserve" {
                entries
                    .reservations
                    .insert(key.to_owned(), reservations::read(&entry.path())?);
                continue;
            }
            let (header, offset) =
                read_header(&entry.path(), incomplete, root.spi.is_some() && !incomplete)?;
            let proposed = Entry {
                owner: header.owner,
                maximum: header.input.length,
                input: Some(header.input),
                funding: header.funding,
                offset,
                incomplete,
                live: 0,
                prepaid: false,
            };
            entries.insert(key.to_owned(), proposed);
        }
        reservations::audit(&root.policy, &entries)?;
        for path in abandoned.into_values() {
            fs::remove_file(path)?;
            sync(&root)?;
            root.recovered_stages += 1;
        }
        // Complete prior interrupted rename/unlink durability before issuing
        // new capacity based on the reconstructed directory inventory.
        sync(&root)?;
        *root
            .entries
            .lock()
            .map_err(|_| StoreError::Corrupt("payload inventory poisoned"))? = entries;
        sweep_spi_orphans(&mut root)?;
        Ok(Self {
            root: Arc::new(root),
        })
    }

    pub fn binding(&self) -> StoreIdentity {
        self.root.binding
    }
    /// The backend this root was recorded under.
    pub fn backend(&self) -> PayloadBackendKind {
        self.root.backend
    }
    pub(super) fn path(&self) -> &Path {
        &self.root.path
    }
    pub fn recovered_stages(&self) -> usize {
        self.root.recovered_stages
    }
    pub fn usage(&self, owner: Option<&IdentityLabel>) -> Result<PayloadUsage> {
        let entries = self.root.entries()?;
        let mut usage = PayloadUsage {
            objects: 0,
            charged_bytes: 0,
            incomplete_objects: 0,
        };
        for entry in entries
            .values()
            .filter(|e| owner.is_none_or(|o| e.owner == *o))
        {
            usage.objects += u64::from(entry.funding.is_none());
            usage.charged_bytes = usage
                .charged_bytes
                .checked_add(entry.charge()?)
                .ok_or(StoreError::Corrupt("payload inventory charge overflow"))?;
            usage.incomplete_objects += u64::from(entry.incomplete);
        }
        for entry in entries
            .reservations
            .values()
            .filter(|e| owner.is_none_or(|o| e.owner == *o))
        {
            usage.objects += entry.budget.count.0 + 1;
            usage.charged_bytes = usage
                .charged_bytes
                .checked_add(entry.charge()?)
                .ok_or(StoreError::Corrupt("reservation charge overflow"))?;
            usage.incomplete_objects += u64::from(entry.incomplete);
        }
        Ok(usage)
    }

    /// Call after authority header validation. This reservation bounds temporary
    /// reception, not the later job's output/metadata/publication promises.
    pub fn stage(
        &self,
        owner: &IdentityLabel,
        input: &Input,
        caps: &Capabilities,
        now: Instant,
    ) -> Result<StagedPayload> {
        owner.check()?;
        input.check()?;
        let receiver = PayloadReceiver::new(input.length, input.sha256, caps, now)?;
        let header = Header {
            owner: owner.clone(),
            input: input.clone(),
            funding: None,
        };
        let encoded = codec::encode(&header, HEADER_LIMIT)?;
        let proposed = Entry {
            owner: owner.clone(),
            input: Some(input.clone()),
            maximum: input.length,
            funding: None,
            offset: OVERHEAD,
            incomplete: true,
            live: 1,
            prepaid: false,
        };
        let mut entries = self.root.entries()?;
        check_capacity(&self.root.policy, &entries, &proposed)?;
        check_handles(&self.root.policy, &entries, owner)?;
        let key = self.fresh_key(&entries)?;
        let file = new_staging_file(&self.root, &self.root.path(&key, true))?;
        #[cfg(test)]
        tests::crash_point("stage-created");
        // Register before any fallible write so a failed cleanup cannot release
        // capacity for a file that still exists.
        entries.insert(key.clone(), proposed);
        drop(entries);
        let mut stage = StagedPayload {
            store: self.clone(),
            key,
            file: Some(file),
            receiver,
            failed: true,
        };
        let file = stage.file.as_mut().expect("new stage file");
        file.write_all(MAGIC).map_err(io)?;
        file.write_all(&(encoded.len() as u32).to_be_bytes())
            .map_err(io)?;
        file.write_all(&encoded).map_err(io)?;
        file.write_all(&[0; HEADER_LIMIT][..HEADER_LIMIT - encoded.len()])
            .map_err(io)?;
        #[cfg(test)]
        tests::crash_point("stage-header");
        stage.failed = false;
        Ok(stage)
    }

    /// Generates a random 32-hex key unused in the local inventory and (on
    /// the SPI plane) at the derived SPI entity. Key-prefix collisions on
    /// the plane are retried: they are the fanout working as designed.
    fn fresh_key(&self, entries: &Inventory) -> Result<String> {
        for _ in 0..8 {
            let mut random = [0u8; 16];
            SysRng
                .try_fill_bytes(&mut random)
                .map_err(|e| StoreError::Io(std::io::Error::other(e)))?;
            let key: String = random.iter().map(|b| format!("{b:02x}")).collect();
            if entries.contains_key(&key) || entries.reservations.contains_key(&key) {
                continue;
            }
            if let Some(plane) = &self.root.spi {
                let taken = !plane
                    .lock()
                    .map_err(|_| StoreError::Corrupt("payload SPI plane poisoned"))?
                    .list(&plane_scope(self.root.binding, &key))
                    .map_err(spi)?
                    .is_empty();
                if taken {
                    continue;
                }
            }
            return Ok(key);
        }
        Err(protocol(
            ErrorCode::LimitExceeded,
            "payload identity collision",
        ))
    }

    /// SPI-plane install: streams the stage file's content region to the SPI
    /// store (bounded memory via begin_put), then truncates the stage file
    /// to a header-only stub so the caller's rename leaves the local grammar
    /// intact. Kill windows: after the put but before the stub the SPI
    /// entity is swept as an orphan at open; after truncation but before the
    /// rename the stage file is reclaimed at open as usual.
    /// Returns false when the SPI plane is not selected.
    pub(super) fn spi_install(&self, key: &str, expected: &Digest) -> Result<bool> {
        let Some(plane) = &self.root.spi else {
            return Ok(false);
        };
        let stage_path = self.root.path(key, true);
        let mut stage = File::open(&stage_path)?;
        stage.seek(SeekFrom::Start(OVERHEAD))?;
        let mut writer = plane
            .lock()
            .map_err(|_| StoreError::Corrupt("payload SPI plane poisoned"))?
            .begin_put(&plane_scope(self.root.binding, key), &expected.0)
            .map_err(spi)?;
        let mut chunk = [0u8; 64 * 1024];
        loop {
            let n = stage.read(&mut chunk)?;
            if n == 0 {
                break;
            }
            std::io::Write::write_all(&mut writer, &chunk[..n])?;
        }
        PlaneWriter::finish(writer).map_err(spi)?;
        // Content now lives in the plane; the local file becomes a stub.
        let stage = OpenOptions::new().write(true).open(&stage_path)?;
        stage.set_len(OVERHEAD)?;
        stage.sync_all()?;
        Ok(true)
    }

    /// Removes the SPI-plane content of a collected object, if any. A
    /// NotFound means the kill window closed before the SPI put.
    pub(super) fn spi_remove(&self, key: &str) -> Result<()> {
        let Some(plane) = &self.root.spi else {
            return Ok(());
        };
        let scope = plane_scope(self.root.binding, key);
        match plane
            .lock()
            .map_err(|_| StoreError::Corrupt("payload SPI plane poisoned"))?
            .retire(&scope, 0)
        {
            Ok(_) | Err(pipestream_storage::StorageError::NotFound(_)) => Ok(()),
            Err(error) => Err(spi(error)),
        }
    }

    /// Pin and validate a retained immutable object. Caller authorization and
    /// result/read-lease admission belong to the authority, not this filesystem.
    pub fn open_object(
        &self,
        key: &str,
        owner: &IdentityLabel,
        input: &Input,
    ) -> Result<ObjectReader> {
        self.open_object_with(key, owner, input, None)
    }
    fn open_object_with(
        &self,
        key: &str,
        owner: &IdentityLabel,
        input: &Input,
        credit: Option<&Arc<ReadCredit>>,
    ) -> Result<ObjectReader> {
        let mut entries = self.root.entries()?;
        let entry = entries
            .get(key)
            .ok_or_else(|| protocol(ErrorCode::OutputUnavailable, "retained object absent"))?;
        if entry.incomplete || entry.owner != *owner || entry.input.as_ref() != Some(input) {
            return Err(protocol(
                ErrorCode::OutputUnavailable,
                "retained object identity mismatch",
            ));
        }
        if let Some(credit) = credit {
            credit.check(&entries, owner)?;
        } else {
            check_handles(&self.root.policy, &entries, owner)?;
        }
        let entry = entries.get_mut(key).expect("checked object");
        let path = self.root.path(key, false);
        let (header, offset) = read_header(&path, false, self.root.spi.is_some()).map_err(|_| {
            protocol(
                ErrorCode::OutputUnavailable,
                "retained object header corrupt",
            )
        })?;
        if header != entry.header()? || offset != entry.offset {
            return Err(protocol(
                ErrorCode::OutputUnavailable,
                "retained object descriptor changed",
            ));
        }
        let file: Box<dyn std::io::Read + Send> = if let Some(plane) = &self.root.spi {
            let reader = plane
                .lock()
                .map_err(|_| StoreError::Corrupt("payload SPI plane poisoned"))?
                .open_read(&plane_scope(self.root.binding, key))
                .map_err(|_| {
                    protocol(ErrorCode::OutputUnavailable, "retained object absent")
                })?;
            Box::new(reader)
        } else {
            let mut file = File::open(path)
                .map_err(|_| protocol(ErrorCode::OutputUnavailable, "retained object absent"))?;
            file.seek(SeekFrom::Start(offset))?;
            Box::new(file)
        };
        entry.live = entry
            .live
            .checked_add(1)
            .ok_or_else(|| protocol(ErrorCode::LimitExceeded, "object pin counter exhausted"))?;
        if let Some(credit) = credit {
            credit.set_busy(&mut entries, true);
        }
        Ok(ObjectReader {
            store: self.clone(),
            key: key.to_owned(),
            file: Some(file),
            remaining: input.length.0,
            expected: input.sha256,
            hash: Sha256::new(),
            verified: false,
            failed: false,
            credit: credit.cloned(),
        })
    }

    /// The authority must hold its SQLite writer transaction while testing
    /// references and deleting. All temporary/installed live handles are pinned.
    /// Deletion is idempotent across restart; unknown paths are never collected.
    #[cfg(test)]
    pub(crate) fn collect(
        &self,
        after: Option<&str>,
        limit: usize,
        referenced: impl FnMut(&str) -> Result<bool>,
    ) -> Result<CollectionProgress> {
        self.collect_until(after, None, limit, referenced)
    }

    fn collect_until(
        &self,
        after: Option<&str>,
        through: Option<&str>,
        limit: usize,
        mut referenced: impl FnMut(&str) -> Result<bool>,
    ) -> Result<CollectionProgress> {
        if limit == 0 || limit > 256 {
            return Err(protocol(ErrorCode::LimitExceeded, "invalid cleanup batch"));
        }
        let mut entries = self.root.entries()?;
        let mut removed = 0;
        if after.zip(through).is_some_and(|(a, b)| a >= b) {
            return Ok(CollectionProgress {
                inspected: 0,
                removed: 0,
                next: None,
            });
        }
        let range = (
            after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
            through.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Included),
        );
        let mut keys: Vec<String> = entries
            .range::<str, _>(range)
            .take(limit)
            .map(|(key, _)| key.clone())
            .collect();
        keys.extend(
            entries
                .reservations
                .range::<str, _>(range)
                .take(limit)
                .map(|(key, _)| key.clone()),
        );
        keys.sort_unstable();
        keys.truncate(limit);
        let next = keys.last().cloned();
        let inspected = keys.len();
        for key in keys {
            if let Some(reservation) = entries.reservations.get(&key) {
                if reservation.live != 0
                    || referenced(&key)?
                    || entries
                        .values()
                        .any(|e| e.funding.as_ref().is_some_and(|f| f.key == key))
                {
                    continue;
                }
                let path = reservations::path(&self.root, &key, reservation.incomplete);
                match checked_file(&path) {
                    Ok(_) => fs::remove_file(path)?,
                    Err(StoreError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e),
                }
                #[cfg(test)]
                reservations::tests::crash_point("reserve-unlinked");
                sync(&self.root)?;
                entries.reservations.remove(&key);
                removed += 1;
                continue;
            }
            let entry = entries.get(&key).expect("inventory entry");
            let budget_pinned = if let Some(funding) = &entry.funding {
                entries
                    .reservations
                    .get(&funding.key)
                    .is_some_and(|r| r.live != 0)
                    || referenced(&funding.key)?
            } else {
                false
            };
            if entry.live != 0 || budget_pinned || referenced(&key)? {
                continue;
            }
            let path = self.root.path(&key, entry.incomplete);
            match checked_file(&path) {
                Ok(_) => fs::remove_file(path)?,
                Err(StoreError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
            #[cfg(test)]
            tests::crash_point("cleanup-unlinked");
            self.spi_remove(&key)?;
            sync(&self.root)?;
            entries.remove(&key);
            removed += 1;
        }
        Ok(CollectionProgress {
            inspected,
            removed,
            next,
        })
    }

    pub(super) fn last_retained_key(&self) -> Result<Option<String>> {
        let entries = self.root.entries()?;
        Ok(entries
            .last_key_value()
            .map(|(key, _)| key)
            .max(entries.reservations.last_key_value().map(|(key, _)| key))
            .cloned())
    }

    /// Called under the authority writer after a committed release intent.
    /// Absence in this inventory follows a successful directory sync (or an
    /// exclusive reopen), not merely an unlink or an expired wall clock.
    pub(super) fn reclamation_status(
        &self,
        input: &str,
        reservation: &str,
    ) -> Result<(bool, bool)> {
        let entries = self.root.entries()?;
        Ok((
            !entries.contains_key(input),
            !entries.reservations.contains_key(reservation)
                && !entries
                    .values()
                    .any(|entry| entry.funding.as_ref().is_some_and(|f| f.key == reservation)),
        ))
    }
}

impl AuthorityStore {
    pub fn payload_identity(&self) -> Result<StoreIdentity> {
        let bytes: Vec<u8> = self.connect()?.query_row(
            "SELECT store_id FROM authority WHERE singleton=1",
            [],
            |r| r.get(0),
        )?;
        Ok(StoreIdentity::from_bytes(bytes.try_into().map_err(
            |_| StoreError::Corrupt("invalid authority store identity"),
        )?)?)
    }

    /// Claim one existing payload root for this authority store. This durable
    /// pairing prevents a missing root from being replaced with an empty one.
    pub fn bind_payloads(&self, payloads: &PayloadStore) -> Result<()> {
        payloads.root.owned()?;
        if payloads.backend() != self.payload_backend {
            return Err(StoreError::Corrupt(
                "authority payload backend selection changed",
            ));
        }
        let mut connection = self.connect()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (identity, retained): (Vec<u8>, Option<String>) = tx.query_row(
            "SELECT store_id,payload_path FROM authority WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if identity != payloads.binding().as_bytes() {
            return Err(StoreError::Corrupt(
                "payload root belongs to another database",
            ));
        }
        let path = payloads
            .root
            .path
            .to_str()
            .ok_or(StoreError::Corrupt("payload path must be UTF-8"))?;
        if retained.as_deref().is_some_and(|p| p != path) {
            return Err(StoreError::Corrupt("authority payload root path changed"));
        }
        payloads.audit_references(&tx)?;
        records::protect(&tx, 0, 0)?;
        tx.execute(
            "UPDATE authority SET payload_path=?1 WHERE singleton=1",
            [path],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn collect_payload_orphans(
        &self,
        payloads: &PayloadStore,
        after: Option<&str>,
        limit: usize,
    ) -> Result<CollectionProgress> {
        self.collect_payloads_until(payloads, after, None, limit)
    }

    pub(super) fn collect_payloads_until(
        &self,
        payloads: &PayloadStore,
        after: Option<&str>,
        through: Option<&str>,
        limit: usize,
    ) -> Result<CollectionProgress> {
        let mut connection = self.connect()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (identity, retained): (Vec<u8>, Option<String>) = tx.query_row(
            "SELECT store_id,payload_path FROM authority WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if identity != payloads.binding().as_bytes()
            || retained.as_deref() != payloads.root.path.to_str()
        {
            return Err(StoreError::Corrupt(
                "payload root is not bound to authority",
            ));
        }
        self.check_clock(&tx)?;
        // The writer lock excludes publication of new references. A live
        // installed token excludes collection before its admission commits.
        payloads.collect_until(after, through, limit, |key| {
            let reference: Option<(u64, Option<i64>)> = tx
                .query_row(
                    "SELECT p.purpose,j.work_row FROM payload_refs p JOIN work w ON w.generation=p.generation AND w.scope=p.scope AND w.entity=p.entity LEFT JOIN jobs j ON j.work_row=w.row_id WHERE p.object_key=?1",
                    [key],
                    |r| Ok((number(r, 0)?, r.get(1)?)),
                )
                .optional()?;
            let Some((purpose, job_row)) = reference else { return Ok(false); };
            let Some(row) = job_row else { return Ok(true); };
            let (_, job): (_, jobs::JobRecord) = records::read(&tx, records::Target {
                table: records::Table::Job, row,
            })?;
            if let Some(release) = &job.release {
                let (_, view): (_, WorkView) = records::read(&tx, records::Target { table: records::Table::Work, row })?;
                let (generation, owner): (u64, String) = tx.query_row(
                    "SELECT w.generation,s.owner FROM work w JOIN sessions s ON s.generation=w.generation WHERE w.row_id=?1",
                    [row], |r| Ok((number(r, 0)?, r.get(1)?)))?;
                let identity = SessionIdentity { authority: self.authority.clone(), owner: IdentityLabel(owner), generation: Id(generation) };
                if let Some(proof) = retirement::load(&tx, identity.generation)? {
                    retirement::verify_work(&proof, &view)?;
                    if job.executor_live || job.input_live || job.outputs_live {
                        return Err(StoreError::Corrupt("retiring job retains resources"));
                    }
                } else {
                    super::retention::verify_release(&tx, &identity, &view, release)?;
                }
            }
            Ok(if purpose == 0 {
                job.input_live && !job.release.as_ref().is_some_and(|r| r.input)
            } else {
                job.outputs_live && !job.release.as_ref().is_some_and(|r| r.outputs)
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionProgress {
    pub inspected: usize,
    pub removed: usize,
    pub next: Option<String>,
}

fn check_capacity(policy: &PayloadPolicy, entries: &Inventory, proposed: &Entry) -> Result<()> {
    reservations::check_capacity(
        policy,
        entries,
        &proposed.owner,
        u64::from(proposed.funding.is_none()),
        proposed.charge()?,
    )
}

fn check_handles(policy: &PayloadPolicy, entries: &Inventory, owner: &IdentityLabel) -> Result<()> {
    let total: usize = entries
        .values()
        .map(|e| e.live - usize::from(e.prepaid))
        .chain(
            entries
                .reservations
                .values()
                .map(|r| r.live + usize::from(r.io_reserved)),
        )
        .chain(entries.readers.values().map(|r| usize::from(!r.busy)))
        .sum();
    let owned: usize = entries
        .values()
        .filter(|e| e.owner == *owner)
        .map(|e| e.live - usize::from(e.prepaid))
        .chain(
            entries
                .reservations
                .values()
                .filter(|r| r.owner == *owner)
                .map(|r| r.live + usize::from(r.io_reserved)),
        )
        .chain(
            entries
                .readers
                .values()
                .filter(|r| r.owner == *owner)
                .map(|r| usize::from(!r.busy)),
        )
        .sum();
    if total as u64 >= policy.handles.0 || owned as u64 >= policy.owner_handles.0 {
        return Err(protocol(
            ErrorCode::LimitExceeded,
            "payload handle capacity exhausted",
        ));
    }
    Ok(())
}

/// Reads and validates an object header. `stub` marks the SPI-plane
/// grammar: the local file is header-only and the payload bytes live in the
/// SPI store, so the exact-length check expects the header region only.
fn read_header(path: &Path, incomplete: bool, stub: bool) -> Result<(Header, u64)> {
    let metadata = checked_file(path)?;
    let mut file = File::open(path)?;
    let mut prefix = [0u8; 12];
    file.read_exact(&mut prefix)?;
    let length = u32::from_be_bytes(prefix[8..].try_into().expect("four bytes")) as usize;
    if &prefix[..8] != MAGIC || length == 0 || length > HEADER_LIMIT {
        return Err(StoreError::Corrupt("invalid payload header framing"));
    }
    let mut bytes = vec![0u8; length];
    file.read_exact(&mut bytes)?;
    let header: Header = codec::decode(&bytes, HEADER_LIMIT)?;
    let mut padding = [0; HEADER_LIMIT];
    file.read_exact(&mut padding[..HEADER_LIMIT - length])?;
    if padding[..HEADER_LIMIT - length].iter().any(|b| *b != 0) {
        return Err(StoreError::Corrupt("payload header padding changed"));
    }
    let offset = OVERHEAD;
    let expected = if stub {
        offset
    } else {
        header
            .input
            .length
            .0
            .checked_add(offset)
            .ok_or(StoreError::Corrupt("payload file length overflow"))?
    };
    if metadata.len() > expected || (!incomplete && metadata.len() != expected) {
        return Err(StoreError::Corrupt("payload file length mismatch"));
    }
    Ok((header, offset))
}

pub struct StagedPayload {
    store: PayloadStore,
    key: String,
    file: Option<File>,
    receiver: PayloadReceiver,
    failed: bool,
}
impl StagedPayload {
    pub fn receive(&mut self, chunk: &[u8], now: Instant) -> Result<()> {
        self.store.root.owned()?;
        if self.failed {
            return Err(protocol(
                ErrorCode::IntegrityError,
                "payload stage already failed",
            ));
        }
        self.failed = true;
        if chunk.len() as u64 > self.store.root.policy.chunk_bytes.0 {
            return Err(protocol(
                ErrorCode::LimitExceeded,
                "payload write chunk exceeds bound",
            ));
        }
        self.receiver.receive(chunk, now)?;
        self.file
            .as_mut()
            .ok_or(StoreError::Corrupt("payload stage is closed"))?
            .write_all(chunk)
            .map_err(io)?;
        self.failed = false;
        Ok(())
    }
    pub fn check_deadline(&mut self, now: Instant) -> Result<()> {
        self.store.root.owned()?;
        if self.failed {
            return Err(protocol(
                ErrorCode::IntegrityError,
                "payload stage already failed",
            ));
        }
        let result = self.receiver.check_deadline(now);
        if result.is_err() {
            self.failed = true;
        }
        Ok(result?)
    }
    pub fn finish(mut self, now: Instant) -> Result<InstalledPayload> {
        self.store.root.owned()?;
        if self.failed {
            return Err(protocol(
                ErrorCode::IntegrityError,
                "payload stage already failed",
            ));
        }
        self.receiver.finish(now)?;
        self.file
            .as_mut()
            .ok_or(StoreError::Corrupt("payload stage is closed"))?
            .sync_all()
            .map_err(io)?;
        #[cfg(test)]
        tests::crash_point("object-synced");
        let mut entries = self.store.root.entries()?;
        let entry = entries
            .get_mut(&self.key)
            .ok_or(StoreError::Corrupt("payload reservation disappeared"))?;
        let expected = entry
            .input
            .as_ref()
            .ok_or(StoreError::Corrupt("installed input descriptor missing"))?
            .sha256;
        self.store.spi_install(&self.key, &expected)?;
        install_file(
            &self.store.root,
            &self.store.root.path(&self.key, true),
            &self.store.root.path(&self.key, false),
        )?;
        entry.incomplete = false;
        #[cfg(test)]
        tests::crash_point("object-renamed");
        sync(&self.store.root)?;
        #[cfg(test)]
        tests::crash_point("directory-synced");
        // Transfer this exact live pin to the opaque installed evidence.
        let installed = InstalledPayload {
            store: self.store.clone(),
            key: self.key.clone(),
            owner: entry.owner.clone(),
            input: entry
                .input
                .clone()
                .ok_or(StoreError::Corrupt("installed input descriptor missing"))?,
        };
        self.file.take();
        self.key.clear();
        Ok(installed)
    }
}
impl Drop for StagedPayload {
    fn drop(&mut self) {
        if self.key.is_empty() {
            return;
        }
        self.file.take();
        if let Ok(mut entries) = self.store.root.entries()
            && let Some(entry) = entries.get_mut(&self.key)
        {
            entry.live = entry.live.saturating_sub(1);
            // An installed object survives error/uncertain commit; only
            // uninstalled reception is eligible for eager cleanup here.
            if entry.incomplete
                && fs::remove_file(self.store.root.path(&self.key, true)).is_ok()
                && sync(&self.store.root).is_ok()
            {
                entries.remove(&self.key);
            }
        }
    }
}

/// Cannot be forged from a filename or a digest. The object remains pinned
/// through its caller's metadata transaction, including uncertain commit errors.
pub struct InstalledPayload {
    store: PayloadStore,
    key: String,
    owner: IdentityLabel,
    input: Input,
}
impl InstalledPayload {
    pub(super) fn check_owned(&self) -> Result<()> {
        self.store.root.owned()
    }
    pub(super) fn store(&self) -> &PayloadStore {
        &self.store
    }
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn owner(&self) -> &IdentityLabel {
        &self.owner
    }
    pub fn descriptor(&self) -> &Input {
        &self.input
    }
    pub fn binding(&self) -> StoreIdentity {
        self.store.binding()
    }
}
impl Drop for InstalledPayload {
    fn drop(&mut self) {
        if let Ok(mut entries) = self.store.root.entries()
            && let Some(entry) = entries.get_mut(&self.key)
        {
            entry.live = entry.live.saturating_sub(1);
            entry.prepaid = false;
        }
    }
}
fn unpin(store: &PayloadStore, key: &str) {
    if let Ok(mut entries) = store.root.entries()
        && let Some(entry) = entries.get_mut(key)
    {
        entry.live = entry.live.saturating_sub(1);
    }
}

pub struct ObjectReader {
    store: PayloadStore,
    key: String,
    file: Option<Box<dyn std::io::Read + Send>>,
    remaining: u64,
    expected: Digest,
    hash: Sha256,
    verified: bool,
    failed: bool,
    credit: Option<Arc<ReadCredit>>,
}
impl ObjectReader {
    /// Bounded read with end-to-end verification at EOF. Bytes before verified
    /// EOF are explicitly provisional, even when obtained from retained storage.
    pub fn read_chunk(&mut self, buffer: &mut [u8]) -> Result<usize> {
        self.store.root.owned()?;
        if self.failed {
            return Err(protocol(
                ErrorCode::OutputUnavailable,
                "retained object read already failed",
            ));
        }
        if self.verified {
            return Ok(0);
        }
        self.failed = true;
        if buffer.is_empty() || buffer.len() as u64 > self.store.root.policy.chunk_bytes.0 {
            return Err(protocol(
                ErrorCode::LimitExceeded,
                "invalid object read buffer",
            ));
        }
        let maximum = self.remaining.min(buffer.len() as u64) as usize;
        if maximum == 0 {
            let mut extra = [0u8; 1];
            if self
                .file
                .as_mut()
                .ok_or(StoreError::Corrupt("reader closed"))?
                .read(&mut extra)?
                != 0
                || Digest(self.hash.clone().finalize().into()) != self.expected
            {
                return Err(protocol(
                    ErrorCode::OutputUnavailable,
                    "retained object digest or length changed",
                ));
            }
            self.verified = true;
            self.failed = false;
            return Ok(0);
        }
        let count = self
            .file
            .as_mut()
            .ok_or(StoreError::Corrupt("reader closed"))?
            .read(&mut buffer[..maximum])?;
        if count == 0 {
            return Err(protocol(
                ErrorCode::OutputUnavailable,
                "retained object truncated",
            ));
        }
        self.remaining -= count as u64;
        self.hash.update(&buffer[..count]);
        self.failed = false;
        Ok(count)
    }
    pub fn verified(&self) -> bool {
        self.verified
    }
}
impl Drop for ObjectReader {
    fn drop(&mut self) {
        self.file.take();
        if let Some(credit) = &self.credit {
            if let Ok(mut entries) = self.store.root.entries() {
                if let Some(entry) = entries.get_mut(&self.key) {
                    entry.live = entry.live.saturating_sub(1);
                }
                credit.set_busy(&mut entries, false);
            }
        } else {
            unpin(&self.store, &self.key);
        }
    }
}

#[cfg(test)]
mod tests;
#[cfg(test)]
mod grammar_tests;
#[cfg(test)]
mod spi_backend_tests;
#[cfg(test)]
mod spi_plane_tests;
