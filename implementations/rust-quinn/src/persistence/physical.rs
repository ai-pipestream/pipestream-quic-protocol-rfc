//! Fixed file-length budgets for the bundled SQLite Unix backend.
//!
//! The directory must be private to cooperating store users. These limits do
//! not measure allocated filesystem blocks, snapshots, payloads, or native RAM.

use super::{StoreError, sync_directory};
use crate::ProtocolError;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, Weak},
};

mod reservation;
#[cfg(test)]
mod reservation_tests;
pub(super) use reservation::protect;
mod vfs;

const MAGIC: &[u8; 8] = b"PSDBL002";
const POLICY_BYTES: u64 = 72;
const MAX_OPEN_STORES: usize = 64;
const SUFFIXES: [&str; 4] = ["", "-wal", "-journal", "-shm"];
static STORES: OnceLock<Mutex<BTreeMap<PathBuf, Weak<Guard>>>> = OnceLock::new();

/// Immutable per-file length caps. All writers must use the guarded store API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PhysicalLimits {
    pub database_bytes: u64,
    pub wal_bytes: u64,
    pub journal_bytes: u64,
    pub shared_memory_bytes: u64,
}

impl Default for PhysicalLimits {
    fn default() -> Self {
        Self {
            database_bytes: 256 << 20,
            wal_bytes: 64 << 20,
            journal_bytes: 64 << 20,
            shared_memory_bytes: 512 << 10,
        }
    }
}

impl PhysicalLimits {
    fn values(self) -> [u64; 4] {
        [
            self.database_bytes,
            self.wal_bytes,
            self.journal_bytes,
            self.shared_memory_bytes,
        ]
    }

    fn validate(self) -> Result<(), StoreError> {
        if self
            .values()
            .iter()
            .any(|n| !(65536..=16 << 30).contains(n) || n % 65536 != 0)
            || self.shared_memory_bytes > 16 << 20
        {
            return Err(StoreError::Protocol(ProtocolError::limit(
                "SQLite file limits must be bounded multiples of 64 KiB",
            )));
        }
        Ok(())
    }

    fn encode(self) -> [u8; POLICY_BYTES as usize] {
        let mut bytes = [0; POLICY_BYTES as usize];
        bytes[..8].copy_from_slice(MAGIC);
        for (slot, value) in bytes[8..40].chunks_exact_mut(8).zip(self.values()) {
            slot.copy_from_slice(&value.to_be_bytes());
        }
        let checksum = Sha256::digest(&bytes[..40]);
        bytes[40..].copy_from_slice(&checksum);
        bytes
    }

    fn read(path: &Path) -> Result<Self, StoreError> {
        if checked_length(path)? != Some(POLICY_BYTES) {
            return Err(corrupt("missing or malformed SQLite file policy"));
        }
        let mut bytes = [0; POLICY_BYTES as usize];
        let mut file = File::open(path)?;
        file.read_exact(&mut bytes)?;
        let mut extra = [0];
        if file.read(&mut extra)? != 0
            || &bytes[..8] != MAGIC
            || Sha256::digest(&bytes[..40])[..] != bytes[40..]
        {
            return Err(corrupt("invalid SQLite file policy checksum or version"));
        }
        let mut values = [0; 4];
        for (value, slot) in values.iter_mut().zip(bytes[8..40].chunks_exact(8)) {
            let mut octets = [0; 8];
            octets.copy_from_slice(slot);
            *value = u64::from_be_bytes(octets);
        }
        let limits = Self {
            database_bytes: values[0],
            wal_bytes: values[1],
            journal_bytes: values[2],
            shared_memory_bytes: values[3],
        };
        limits.validate()?;
        Ok(limits)
    }
}

/// Observed lengths, not a transactionally consistent usage snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PhysicalUsage {
    pub database_bytes: u64,
    pub wal_bytes: u64,
    pub journal_bytes: u64,
    pub shared_memory_bytes: u64,
    pub policy_bytes: u64,
}

#[derive(Debug)]
pub(super) struct Guard {
    pub path: PathBuf,
    pub limits: PhysicalLimits,
    paths: [PathBuf; 4],
    policy: PathBuf,
}

impl Guard {
    pub fn open(path: &Path, requested: Option<PhysicalLimits>) -> Result<Arc<Self>, StoreError> {
        vfs::register()?;
        if let Some(limits) = requested {
            limits.validate()?;
        }
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| corrupt("SQLite path must have a UTF-8 filename"))?;
        if SUFFIXES[1..]
            .iter()
            .chain([".pslimits"].iter())
            .any(|suffix| name.ends_with(suffix))
        {
            return Err(corrupt(
                "SQLite database name collides with a reserved sidecar suffix",
            ));
        }
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        fs::create_dir_all(parent)?;
        let parent = parent.canonicalize()?;
        let path = parent.join(name);
        if path.to_str().is_none() {
            return Err(corrupt("SQLite path must be UTF-8"));
        }
        let paths = SUFFIXES.map(|suffix| parent.join(format!("{name}{suffix}")));
        let policy = parent.join(format!("{name}.pslimits"));
        // Serialize same-process initialization. Other processes may refuse an
        // incomplete policy, but must never use it or overwrite its contents.
        let mut stores = STORES
            .get_or_init(Mutex::default)
            .lock()
            .map_err(|_| corrupt("SQLite guard registry poisoned"))?;
        stores.retain(|_, entry| entry.strong_count() != 0);
        if let Some(guard) = stores.get(&path).and_then(Weak::upgrade) {
            guard.verify()?;
            if requested.is_some_and(|limits| limits != guard.limits) {
                return Err(corrupt("SQLite file policy cannot change on reopen"));
            }
            return Ok(guard);
        }
        if stores.len() >= MAX_OPEN_STORES {
            return Err(StoreError::Protocol(ProtocolError::limit(
                "too many guarded databases",
            )));
        }
        if checked_length(&policy)?.is_none() {
            for file in &paths {
                if checked_length(file)?.is_some_and(|length| length != 0) {
                    return Err(corrupt(
                        "existing SQLite files have no physical policy; conversion refused",
                    ));
                }
            }
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&policy)
            {
                Ok(mut file) => {
                    file.write_all(&requested.unwrap_or_default().encode())?;
                    file.sync_all()?;
                    sync_directory(&parent)?;
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error.into()),
            }
        }
        let limits = PhysicalLimits::read(&policy)?;
        // Another process may have observed the complete policy before its
        // creator synced it. Every opener makes it durable before any DB write.
        File::open(&policy)?.sync_all()?;
        sync_directory(&parent)?;
        if requested.is_some_and(|requested| requested != limits) {
            return Err(corrupt("SQLite file policy cannot change on reopen"));
        }
        let guard = Arc::new(Self {
            path: path.clone(),
            limits,
            paths,
            policy,
        });
        guard.verify()?;
        stores.insert(path, Arc::downgrade(&guard));
        Ok(guard)
    }

    pub fn verify(&self) -> Result<(), StoreError> {
        if PhysicalLimits::read(&self.policy)? != self.limits {
            return Err(corrupt("SQLite file policy changed"));
        }
        self.usage()?;
        Ok(())
    }

    pub fn usage(&self) -> Result<PhysicalUsage, StoreError> {
        let mut sizes = [0; 4];
        for ((path, limit), size) in self.paths.iter().zip(self.limits.values()).zip(&mut sizes) {
            *size = checked_length(path)?.unwrap_or(0);
            if *size > limit {
                return Err(corrupt("SQLite file exceeds its retained policy"));
            }
        }
        Ok(PhysicalUsage {
            database_bytes: sizes[0],
            wal_bytes: sizes[1],
            journal_bytes: sizes[2],
            shared_memory_bytes: sizes[3],
            policy_bytes: POLICY_BYTES,
        })
    }
}

fn corrupt(detail: &str) -> StoreError {
    StoreError::Corrupt(detail.to_owned())
}

fn checked_length(path: &Path) -> Result<Option<u64>, StoreError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if !metadata.is_file() {
        return Err(corrupt("SQLite files must be regular, non-symlink files"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        // SQLite may unlink a WAL/journal between pathname lookup and stat
        // completion. A zero-link observation is not a hardlink alias; usage
        // sampling may conservatively count that just-unlinked file.
        if metadata.nlink() > 1 {
            return Err(corrupt("SQLite hardlink aliases are not supported"));
        }
    }
    Ok(Some(metadata.len()))
}

pub(super) const VFS_NAME: &str = "pipestream-bounded-unix-v1";

#[cfg(test)]
mod tests;
