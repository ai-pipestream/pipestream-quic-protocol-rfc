//! File-backed receive payloads. Quotas cover temporary files held by this store
//! handle, including incomplete streams, queued deliveries, and chunk assemblies.

use super::{PrincipalBinding, ProtocolError, limit_error, storage_error};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, Weak},
};

pub const PAYLOAD_IO_CHUNK: usize = 8192;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpoolLimits {
    pub max_bytes: u64,
    pub max_files: usize,
    pub principal_bytes: u64,
    pub principal_files: usize,
    pub connection_files: usize,
    pub max_principals: usize,
}

impl Default for SpoolLimits {
    fn default() -> Self {
        Self {
            max_bytes: 256 << 20,
            max_files: 4096,
            principal_bytes: 128 << 20,
            principal_files: 1024,
            connection_files: 512,
            max_principals: 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SpoolUsage {
    pub bytes: u64,
    pub files: usize,
    pub peak_bytes: u64,
    pub peak_files: usize,
}

#[derive(Debug)]
struct Budget {
    max_bytes: u64,
    max_files: usize,
    usage: Mutex<SpoolUsage>,
}

impl Budget {
    fn new(max_bytes: u64, max_files: usize) -> Arc<Self> {
        Arc::new(Self {
            max_bytes,
            max_files,
            usage: Mutex::new(SpoolUsage::default()),
        })
    }

    fn reserve(&self, bytes: u64, files: usize) -> Result<(), ProtocolError> {
        let mut usage = self.usage.lock().map_err(storage_error)?;
        if bytes > self.max_bytes - usage.bytes || files > self.max_files - usage.files {
            return Err(limit_error("temporary spool byte or file budget exhausted"));
        }
        usage.bytes += bytes;
        usage.files += files;
        usage.peak_bytes = usage.peak_bytes.max(usage.bytes);
        usage.peak_files = usage.peak_files.max(usage.files);
        Ok(())
    }

    fn release(&self, bytes: u64, files: usize) {
        if let Ok(mut usage) = self.usage.lock() {
            usage.bytes -= bytes;
            usage.files -= files;
        }
    }
}

#[derive(Debug)]
struct Charge {
    budgets: Vec<Arc<Budget>>,
    bytes: u64,
    release: bool,
}

impl Charge {
    fn new(budgets: &[Arc<Budget>]) -> Result<Self, ProtocolError> {
        let mut charged = Self {
            budgets: Vec::new(),
            bytes: 0,
            release: true,
        };
        for budget in budgets {
            budget.reserve(0, 1)?;
            charged.budgets.push(budget.clone());
        }
        Ok(charged)
    }

    fn grow(&mut self, bytes: u64) -> Result<(), ProtocolError> {
        for (index, budget) in self.budgets.iter().enumerate() {
            if let Err(error) = budget.reserve(bytes, 0) {
                for prior in &self.budgets[..index] {
                    prior.release(bytes, 0);
                }
                return Err(error);
            }
        }
        self.bytes += bytes;
        Ok(())
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        if self.release {
            for budget in &self.budgets {
                budget.release(self.bytes, 1);
            }
        }
    }
}

type PrincipalKey = Option<(String, String)>;

#[derive(Debug)]
pub struct SpoolStore {
    directory: PathBuf,
    limits: SpoolLimits,
    global: Arc<Budget>,
    principals: Mutex<BTreeMap<PrincipalKey, Weak<Budget>>>,
}

impl SpoolStore {
    pub fn new(directory: PathBuf, limits: SpoolLimits) -> Result<Arc<Self>, ProtocolError> {
        if limits.max_bytes == 0
            || limits.principal_bytes == 0
            || limits.max_files == 0
            || limits.principal_files == 0
            || limits.connection_files == 0
            || limits.max_principals == 0
        {
            return Err(limit_error("spool limits must be nonzero"));
        }
        static STORES: OnceLock<Mutex<BTreeMap<PathBuf, Weak<SpoolStore>>>> = OnceLock::new();
        let parent = directory
            .parent()
            .ok_or_else(|| storage_error("spool directory has no parent"))?;
        let name = directory
            .file_name()
            .ok_or_else(|| storage_error("spool directory has no name"))?;
        let directory = std::fs::canonicalize(parent)
            .map_err(storage_error)?
            .join(name);
        match std::fs::symlink_metadata(&directory) {
            Ok(metadata) if !metadata.is_dir() || metadata.file_type().is_symlink() => {
                return Err(storage_error(
                    "spool root must be a directory, not a symlink",
                ));
            }
            Err(error) if error.kind() != io::ErrorKind::NotFound => {
                return Err(storage_error(error));
            }
            _ => {}
        }
        let mut stores = STORES
            .get_or_init(|| Mutex::new(BTreeMap::new()))
            .lock()
            .map_err(storage_error)?;
        stores.retain(|_, store| store.strong_count() != 0);
        if let Some(store) = stores.get(&directory).and_then(Weak::upgrade) {
            if store.limits != limits {
                return Err(limit_error(
                    "spool directory already has different active limits",
                ));
            }
            return Ok(store);
        }
        let global = Budget::new(limits.max_bytes, limits.max_files);
        match std::fs::read_dir(&directory) {
            Ok(entries) => {
                for entry in entries {
                    let entry = entry.map_err(storage_error)?;
                    if !entry.file_type().map_err(storage_error)?.is_file() {
                        return Err(storage_error("unexpected non-file in spool directory"));
                    }
                    global.reserve(entry.metadata().map_err(storage_error)?.len(), 1)?;
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(storage_error(error)),
        }
        let store = Arc::new(Self {
            directory: directory.clone(),
            limits,
            global,
            principals: Mutex::new(BTreeMap::new()),
        });
        stores.insert(directory, Arc::downgrade(&store));
        Ok(store)
    }

    pub fn usage(&self) -> Result<SpoolUsage, ProtocolError> {
        Ok(*self.global.usage.lock().map_err(storage_error)?)
    }

    pub(super) fn connection(
        self: &Arc<Self>,
        principal: Option<&PrincipalBinding>,
        max_bytes: u64,
    ) -> Result<SpoolConnection, ProtocolError> {
        let key = principal.map(|p| (p.authority.clone(), p.principal.clone()));
        let mut principals = self.principals.lock().map_err(storage_error)?;
        principals.retain(|_, value| value.strong_count() != 0);
        let budget = if let Some(existing) = principals.get(&key).and_then(Weak::upgrade) {
            existing
        } else {
            if principals.len() >= self.limits.max_principals {
                return Err(limit_error("active spool principal budget exhausted"));
            }
            let budget = Budget::new(self.limits.principal_bytes, self.limits.principal_files);
            principals.insert(key, Arc::downgrade(&budget));
            budget
        };
        Ok(SpoolConnection {
            store: self.clone(),
            budgets: vec![
                self.global.clone(),
                budget,
                Budget::new(max_bytes, self.limits.connection_files),
            ],
        })
    }
}

#[derive(Clone)]
pub(super) struct SpoolConnection {
    store: Arc<SpoolStore>,
    budgets: Vec<Arc<Budget>>,
}

impl SpoolConnection {
    pub async fn create(&self) -> Result<SpoolWriter, ProtocolError> {
        let charge = Charge::new(&self.budgets)?;
        let directory = self.store.directory.clone();
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&directory).map_err(storage_error)?;
            let temporary = tempfile::Builder::new()
                .prefix("pipestream-")
                .tempfile_in(&directory)
                .map_err(storage_error)?;
            let (file, path) = temporary.into_parts();
            Ok(SpoolWriter {
                file,
                temporary: Temporary {
                    path: Some(path),
                    charge,
                    _store: store,
                },
                digest: Sha256::new(),
            })
        })
        .await
        .map_err(storage_error)?
    }
}

#[derive(Debug)]
struct Temporary {
    path: Option<tempfile::TempPath>,
    charge: Charge,
    _store: Arc<SpoolStore>,
}

impl Temporary {
    fn path(&self) -> &Path {
        self.path.as_ref().expect("live temporary path").as_ref()
    }
}

impl Drop for Temporary {
    fn drop(&mut self) {
        if let Some(path) = self.path.take()
            && let Err(error) = path.close()
        {
            // Do not release disk credit for a file whose removal failed.
            self.charge.release = false;
            eprintln!("spool cleanup failed; disk credit retained: {error}");
        }
    }
}

pub(super) struct SpoolWriter {
    file: File,
    temporary: Temporary,
    digest: Sha256,
}

impl SpoolWriter {
    pub fn len(&self) -> u64 {
        self.temporary.charge.bytes
    }

    pub async fn append(mut self, bytes: &[u8]) -> Result<Self, ProtocolError> {
        if bytes.len() > PAYLOAD_IO_CHUNK {
            return Err(limit_error("spool append exceeds I/O chunk"));
        }
        self.temporary.charge.grow(bytes.len() as u64)?;
        let bytes = bytes.to_vec();
        // The operation owns the writer and disk credit until I/O finishes, even
        // if the receiving task is cancelled while waiting for this join handle.
        tokio::task::spawn_blocking(move || {
            self.file.write_all(&bytes).map_err(storage_error)?;
            self.digest.update(&bytes);
            Ok(self)
        })
        .await
        .map_err(storage_error)?
    }

    pub async fn finish(self) -> Result<Payload, ProtocolError> {
        tokio::task::spawn_blocking(move || {
            self.file.sync_all().map_err(storage_error)?;
            let length = self.len();
            drop(self.file);
            Ok(Payload(Arc::new(PayloadData {
                segments: vec![Arc::new(Segment::Temporary(self.temporary))],
                length,
                digest: self.digest.finalize().into(),
            })))
        })
        .await
        .map_err(storage_error)?
    }
}

#[derive(Debug)]
struct PayloadData {
    segments: Vec<Arc<Segment>>,
    length: u64,
    digest: [u8; 32],
}

#[derive(Debug)]
enum Segment {
    Temporary(Temporary),
    Retained(PathBuf),
}

impl Segment {
    fn path(&self) -> &Path {
        match self {
            Self::Temporary(temporary) => temporary.path(),
            Self::Retained(path) => path,
        }
    }
}

/// Immutable, file-backed payload. Opening a reader does not allocate a whole entity.
#[derive(Debug, Clone)]
pub struct Payload(Arc<PayloadData>);

impl Payload {
    /// Reopen an immutable retained object, checking it against its durable descriptor.
    /// The caller must prevent replacement while readers use this path.
    pub fn open_retained(path: PathBuf, length: u64, expected: [u8; 32]) -> io::Result<Self> {
        if length > pipestream_core::MAX_PAYLOAD as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "retained payload exceeds entity limit",
            ));
        }
        let mut file = File::open(&path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.len() != length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "retained payload length differs from job input",
            ));
        }
        let mut bytes = [0; PAYLOAD_IO_CHUNK];
        let mut measured = 0u64;
        let mut digest = Sha256::new();
        loop {
            let count = file.read(&mut bytes)?;
            if count == 0 {
                break;
            }
            measured += count as u64;
            if measured > length {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "retained payload grew during validation",
                ));
            }
            digest.update(&bytes[..count]);
        }
        if measured != length || <[u8; 32]>::from(digest.finalize()) != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "retained payload differs from job digest",
            ));
        }
        Ok(Self(Arc::new(PayloadData {
            segments: vec![Arc::new(Segment::Retained(path))],
            length,
            digest: expected,
        })))
    }
    pub fn len(&self) -> u64 {
        self.0.length
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn digest(&self) -> [u8; 32] {
        self.0.digest
    }

    pub fn reader(&self) -> PayloadReader {
        PayloadReader {
            payload: self.clone(),
            index: 0,
            current: None,
        }
    }

    #[cfg(test)]
    pub(super) async fn concatenate(parts: Vec<Self>) -> Result<Self, ProtocolError> {
        tokio::task::spawn_blocking(move || Self::concatenate_blocking(parts))
            .await
            .map_err(storage_error)?
    }

    pub(super) fn concatenate_blocking(parts: Vec<Self>) -> Result<Self, ProtocolError> {
        let mut length = 0u64;
        let mut segments = Vec::new();
        let mut digest = Sha256::new();
        let mut bytes = [0; PAYLOAD_IO_CHUNK];
        for part in parts {
            let mut reader = part.reader();
            let mut part_digest = Sha256::new();
            let mut measured = 0u64;
            loop {
                let count = reader.read(&mut bytes).map_err(storage_error)?;
                if count == 0 {
                    break;
                }
                measured += count as u64;
                part_digest.update(&bytes[..count]);
                digest.update(&bytes[..count]);
            }
            let actual: [u8; 32] = part_digest.finalize().into();
            if measured != part.len() || actual != part.digest() {
                return Err(ProtocolError::new(
                    pipestream_core::ERROR_INTEGRITY,
                    "PIPESTREAM_INTEGRITY_ERROR",
                    "spooled chunk changed before assembly",
                ));
            }
            length = length
                .checked_add(part.len())
                .ok_or_else(|| limit_error("payload length overflow"))?;
            segments.extend(part.0.segments.iter().cloned());
        }
        Ok(Self(Arc::new(PayloadData {
            segments,
            length,
            digest: digest.finalize().into(),
        })))
    }
}

pub struct PayloadReader {
    payload: Payload,
    index: usize,
    current: Option<File>,
}

impl Read for PayloadReader {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        loop {
            if self.index == self.payload.0.segments.len() {
                return Ok(0);
            }
            if self.current.is_none() {
                self.current = Some(File::open(self.payload.0.segments[self.index].path())?);
            }
            let count = self.current.as_mut().unwrap().read(bytes)?;
            if count != 0 {
                return Ok(count);
            }
            self.current = None;
            self.index += 1;
        }
    }
}

#[cfg(test)]
mod tests;
