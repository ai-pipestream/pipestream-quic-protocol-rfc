//! Filesystem backend implementing both SPI roles.
//!
//! Physical layout under the configured root (the shared logical layout from
//! [`crate::layout`], rendered as directories and files):
//!
//! ```text
//! <root>/
//!   objects/<scope-id-hex>/...        one directory per scope-path component
//!     objects/<bucket-hex>/<entity-hex>   entity file: 32-byte content digest, then payload
//!     seal                            40 bytes: 32-byte SCOPE_DIGEST + u64 BE entity count
//!   journal/log                     append-only journal (see below)
//!   scratch/                        staging area for atomic writes
//! ```
//!
//! Journal record framing: `u32 BE payload_len | u8 kind | payload`, where
//! kind 0 is an intent (payload = record) and kind 1 is a commit
//! (payload = `u64 BE intent cursor` followed by the record). The file is
//! append-only; a record's cursor is its 1-based position in the file.
//!
//! Durability configuration: every mutating operation writes through a
//! scratch file, fsyncs the file, atomically renames it into place, and
//! fsyncs the containing directory before returning success. Journal appends
//! write the record and fsync the log before returning. A success therefore
//! survives a process kill (assuming the filesystem honors fsync).
//!
//! Scratch files live under `<root>/scratch` — never an ambient temp dir —
//! and are removed by [`FsStore::close`] (and best-effort on drop).
//!
//! Open-read survival relies on POSIX semantics: an open file descriptor
//! stays valid after the file is unlinked, so retiring an unsealed object
//! does not invalidate a live reader.

use crate::layout;
use crate::{
    Cursor, Digest32, EntityId, Journal, JournalEntry, JournalKind, ObjectStore, ScopePath,
    SealRecord, StorageError,
};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const OBJECTS_DIR: &str = "objects";
const SEAL_FILE: &str = "seal";
const JOURNAL_DIR: &str = "journal";
const JOURNAL_LOG: &str = "log";
const SCRATCH_DIR: &str = "scratch";
const SEAL_RECORD_LEN: usize = 32 + 8;
const KIND_INTENT: u8 = 0;
const KIND_COMMIT: u8 = 1;
const READ_CHUNK: usize = 64 * 1024;

/// Filesystem backend for both the [`ObjectStore`] and [`Journal`] roles.
///
/// See the [module documentation](self) for the on-disk layout and the
/// durability configuration.
#[derive(Debug)]
pub struct FsStore {
    root: PathBuf,
    journal: Vec<JournalKind>,
    scratch_seq: AtomicU64,
}

impl FsStore {
    /// Opens (creating if necessary) the store rooted at `root`.
    ///
    /// Validates the journal's framing and loads its in-memory index. A
    /// truncated or malformed journal fails with [`StorageError::Corrupt`].
    /// Leftover scratch files from a previous unclean shutdown are removed.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, StorageError> {
        let root = root.into();
        fs::create_dir_all(root.join(OBJECTS_DIR))?;
        fs::create_dir_all(root.join(JOURNAL_DIR))?;
        let scratch = root.join(SCRATCH_DIR);
        if scratch.exists() {
            fs::remove_dir_all(&scratch)?;
        }
        fs::create_dir_all(&scratch)?;
        let journal = read_journal_index(&root.join(JOURNAL_DIR).join(JOURNAL_LOG))?;
        Ok(FsStore {
            root,
            journal,
            scratch_seq: AtomicU64::new(0),
        })
    }

    /// Removes the scratch directory. Called automatically on drop on a
    /// best-effort basis; explicit close surfaces errors.
    pub fn close(self) -> Result<(), StorageError> {
        fs::remove_dir_all(self.root.join(SCRATCH_DIR))?;
        Ok(())
    }

    fn scope_dir(&self, scope: &ScopePath) -> PathBuf {
        let mut dir = self.root.join(OBJECTS_DIR);
        for component in scope.components() {
            dir = dir.join(layout::scope_component(*component));
        }
        dir
    }

    fn entity_path(&self, scope: &ScopePath, entity: EntityId) -> PathBuf {
        self.scope_dir(scope)
            .join(OBJECTS_DIR)
            .join(layout::bucket_hex(entity))
            .join(layout::entity_component(entity))
    }

    fn scratch_path(&self) -> PathBuf {
        let seq = self.scratch_seq.fetch_add(1, Ordering::Relaxed);
        self.root
            .join(SCRATCH_DIR)
            .join(format!("scratch-{}-{seq}", std::process::id()))
    }

    /// Creates `dir` and any missing ancestors, fsyncing along the way so
    /// the directory structure itself is durable.
    fn create_dirs_durable(&self, dir: &Path) -> Result<(), StorageError> {
        let mut ancestors: Vec<&Path> = dir.ancestors().collect();
        ancestors.reverse();
        let mut created_any = false;
        for path in ancestors {
            if !path.exists() {
                fs::create_dir(path)?;
                created_any = true;
            }
            if created_any {
                sync_dir(path)?;
            }
        }
        Ok(())
    }

    /// Writes `contents` to a scratch file, fsyncs it, renames it onto
    /// `target`, and fsyncs the target's directory.
    fn write_file_durable(&self, target: &Path, contents: &[u8]) -> Result<(), StorageError> {
        let scratch = self.scratch_path();
        let write_result = (|| -> Result<(), StorageError> {
            let mut file = File::create(&scratch)?;
            file.write_all(contents)?;
            file.sync_all()?;
            Ok(())
        })();
        if let Err(error) = write_result {
            let _ = fs::remove_file(&scratch);
            return Err(error);
        }
        let rename_result = fs::rename(&scratch, target);
        if rename_result.is_err() {
            let _ = fs::remove_file(&scratch);
            rename_result?;
        }
        if let Some(parent) = target.parent() {
            sync_dir(parent)?;
        }
        Ok(())
    }

    fn append_journal_record(&mut self, kind: u8, payload: &[u8]) -> Result<Cursor, StorageError> {
        let log = self.root.join(JOURNAL_DIR).join(JOURNAL_LOG);
        let mut file = OpenOptions::new().create(true).append(true).open(&log)?;
        let len = u32::try_from(payload.len())
            .map_err(|_| StorageError::backend("journal record exceeds 4 GiB"))?;
        file.write_all(&len.to_be_bytes())?;
        file.write_all(&[kind])?;
        file.write_all(payload)?;
        file.sync_all()?;
        Ok(Cursor(self.journal.len() as u64 + 1))
    }
}

impl ObjectStore for FsStore {
    type Reader = File;

    fn put(
        &mut self,
        scope: &ScopePath,
        entity: EntityId,
        bytes: &[u8],
        expected_digest: &Digest32,
    ) -> Result<(), StorageError> {
        layout::check_entity_id(entity)?;
        let digest = layout::sha256(bytes);
        if &digest != expected_digest {
            return Err(StorageError::corrupt(format!(
                "digest mismatch for entity {entity} in {scope}"
            )));
        }
        let scope_dir = self.scope_dir(scope);
        if scope_dir.join(SEAL_FILE).exists() {
            return Err(StorageError::already_sealed(format!(
                "scope {scope} is sealed"
            )));
        }
        let target = self.entity_path(scope, entity);
        if target.exists() {
            let recorded = read_entity_header(&target)?;
            if recorded == digest {
                return Ok(());
            }
            return Err(StorageError::corrupt(format!(
                "conflicting bytes for entity {entity} in {scope}"
            )));
        }
        let bucket_dir = match target.parent() {
            Some(dir) => dir.to_path_buf(),
            None => return Err(StorageError::backend("entity path has no parent")),
        };
        self.create_dirs_durable(&bucket_dir)?;
        let mut contents = Vec::with_capacity(32 + bytes.len());
        contents.extend_from_slice(&digest);
        contents.extend_from_slice(bytes);
        self.write_file_durable(&target, &contents)
    }

    fn open_read(&self, scope: &ScopePath, entity: EntityId) -> Result<File, StorageError> {
        let path = self.entity_path(scope, entity);
        let mut file = File::open(&path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                StorageError::not_found(format!("entity {entity} in {scope}"))
            } else {
                StorageError::from(error)
            }
        })?;
        use std::io::Seek;
        file.seek(std::io::SeekFrom::Start(32))?;
        Ok(file)
    }

    fn seal_scope(
        &mut self,
        scope: &ScopePath,
        digest: &Digest32,
        entity_count: u64,
    ) -> Result<(), StorageError> {
        let scope_dir = self.scope_dir(scope);
        let seal_path = scope_dir.join(SEAL_FILE);
        if seal_path.exists() {
            let existing = read_seal_record(&seal_path)?;
            if existing.digest == *digest && existing.entity_count == entity_count {
                return Ok(());
            }
            return Err(StorageError::corrupt(format!(
                "scope {scope} already sealed with a different digest or entity count"
            )));
        }
        self.create_dirs_durable(&scope_dir)?;
        let mut contents = Vec::with_capacity(SEAL_RECORD_LEN);
        contents.extend_from_slice(digest);
        contents.extend_from_slice(&entity_count.to_be_bytes());
        self.write_file_durable(&seal_path, &contents)
    }

    fn verify_scope(&self, scope: &ScopePath) -> Result<SealRecord, StorageError> {
        let scope_dir = self.scope_dir(scope);
        let seal_path = scope_dir.join(SEAL_FILE);
        if !seal_path.exists() {
            return Err(StorageError::not_found(format!("seal record for {scope}")));
        }
        let seal = read_seal_record(&seal_path)?;
        let mut live_count = 0u64;
        let objects_dir = scope_dir.join(OBJECTS_DIR);
        if objects_dir.exists() {
            for bucket in read_dir_sorted(&objects_dir)? {
                for entity_file in read_dir_sorted(&bucket.path())? {
                    let entity = parse_entity_name(&entity_file.path())?;
                    verify_entity_file(&entity_file.path(), entity, scope)?;
                    live_count += 1;
                }
            }
        }
        if live_count != seal.entity_count {
            return Err(StorageError::corrupt(format!(
                "scope {scope} holds {live_count} entities but the seal record says {}",
                seal.entity_count
            )));
        }
        Ok(seal)
    }

    fn list(&self, scope: &ScopePath) -> Result<Vec<EntityId>, StorageError> {
        let objects_dir = self.scope_dir(scope).join(OBJECTS_DIR);
        let mut entities = Vec::new();
        if objects_dir.exists() {
            for bucket in read_dir_sorted(&objects_dir)? {
                for entity_file in read_dir_sorted(&bucket.path())? {
                    entities.push(parse_entity_name(&entity_file.path())?);
                }
            }
        }
        entities.sort_unstable();
        Ok(entities)
    }

    fn retire(&mut self, scope: &ScopePath, retention_cursor: u64) -> Result<u64, StorageError> {
        let scope_dir = self.scope_dir(scope);
        if !scope_dir.exists() {
            return Ok(0);
        }
        if scope_dir.join(SEAL_FILE).exists() {
            let removed = self.list(scope)?.len() as u64;
            fs::remove_dir_all(&scope_dir)?;
            self.prune_empty_ancestors(scope);
            return Ok(removed);
        }
        let objects_dir = scope_dir.join(OBJECTS_DIR);
        let mut removed = 0u64;
        if objects_dir.exists() {
            let mut touched_buckets = Vec::new();
            for bucket in read_dir_sorted(&objects_dir)? {
                for entity_file in read_dir_sorted(&bucket.path())? {
                    let entity = parse_entity_name(&entity_file.path())?;
                    if u64::from(entity) <= retention_cursor {
                        fs::remove_file(entity_file.path())?;
                        removed += 1;
                        touched_buckets.push(bucket.path());
                    }
                }
            }
            for bucket in touched_buckets {
                sync_dir(&bucket)?;
                let _ = fs::remove_dir(&bucket);
            }
            sync_dir(&objects_dir)?;
        }
        Ok(removed)
    }
}

impl FsStore {
    /// Removes ancestor scope directories left empty by retiring `scope`,
    /// stopping at the objects root.
    fn prune_empty_ancestors(&self, scope: &ScopePath) {
        let objects_root = self.root.join(OBJECTS_DIR);
        let mut dir = self.scope_dir(scope);
        while dir != objects_root {
            if fs::remove_dir(&dir).is_err() {
                break;
            }
            match dir.parent() {
                Some(parent) => dir = parent.to_path_buf(),
                None => break,
            }
        }
    }
}

impl Journal for FsStore {
    fn append_intent(&mut self, record: &[u8]) -> Result<Cursor, StorageError> {
        let cursor = self.append_journal_record(KIND_INTENT, record)?;
        self.journal.push(JournalKind::Intent(record.to_vec()));
        Ok(cursor)
    }

    fn commit(&mut self, intent: Cursor, record: &[u8]) -> Result<(), StorageError> {
        if intent.0 == 0 || intent.0 as usize > self.journal.len() {
            return Err(StorageError::not_found(format!("intent at {intent:?}")));
        }
        if !matches!(self.journal[intent.0 as usize - 1], JournalKind::Intent(_)) {
            return Err(StorageError::not_found(format!("intent at {intent:?}")));
        }
        let prior = self.journal.iter().find_map(|kind| match kind {
            JournalKind::Commit { intent: settled, record: existing } if *settled == intent => {
                Some(existing)
            }
            _ => None,
        });
        match prior {
            Some(existing) if existing == record => Ok(()),
            Some(_) => Err(StorageError::corrupt(format!(
                "intent at {intent:?} already committed with a different record"
            ))),
            None => {
                let mut payload = Vec::with_capacity(8 + record.len());
                payload.extend_from_slice(&intent.0.to_be_bytes());
                payload.extend_from_slice(record);
                let cursor = self.append_journal_record(KIND_COMMIT, &payload)?;
                debug_assert_eq!(cursor.0, self.journal.len() as u64 + 1);
                self.journal.push(JournalKind::Commit {
                    intent,
                    record: record.to_vec(),
                });
                Ok(())
            }
        }
    }

    fn replay(&self, since: Cursor) -> Result<Vec<JournalEntry>, StorageError> {
        Ok(self
            .journal
            .iter()
            .enumerate()
            .skip(since.0 as usize)
            .map(|(index, kind)| JournalEntry {
                cursor: Cursor(index as u64 + 1),
                kind: kind.clone(),
            })
            .collect())
    }
}

impl Drop for FsStore {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(self.root.join(SCRATCH_DIR));
    }
}

fn sync_dir(dir: &Path) -> Result<(), StorageError> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

fn read_dir_sorted(dir: &Path) -> Result<Vec<fs::DirEntry>, StorageError> {
    let mut entries: Vec<fs::DirEntry> = fs::read_dir(dir)?.collect::<Result<_, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    Ok(entries)
}

/// Reads the 32-byte content digest header of an entity file.
fn read_entity_header(path: &Path) -> Result<Digest32, StorageError> {
    let mut file = File::open(path)?;
    let mut digest: Digest32 = [0; 32];
    file.read_exact(&mut digest).map_err(|error| {
        StorageError::corrupt(format!("truncated entity file {}: {error}", path.display()))
    })?;
    Ok(digest)
}

fn read_seal_record(path: &Path) -> Result<SealRecord, StorageError> {
    let bytes = fs::read(path)?;
    if bytes.len() != SEAL_RECORD_LEN {
        return Err(StorageError::corrupt(format!(
            "seal record {} is {} bytes, expected {SEAL_RECORD_LEN}",
            path.display(),
            bytes.len()
        )));
    }
    let mut digest: Digest32 = [0; 32];
    digest.copy_from_slice(&bytes[..32]);
    let mut count = [0u8; 8];
    count.copy_from_slice(&bytes[32..]);
    Ok(SealRecord {
        digest,
        entity_count: u64::from_be_bytes(count),
    })
}

fn parse_entity_name(path: &Path) -> Result<EntityId, StorageError> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    u32::from_str_radix(name, 16).map_err(|_| {
        StorageError::corrupt(format!(
            "unparseable entity file name {}",
            path.display()
        ))
    })
}

/// Rehashes an entity file's payload and compares it against the recorded
/// digest header.
fn verify_entity_file(path: &Path, entity: EntityId, scope: &ScopePath) -> Result<(), StorageError> {
    let mut file = File::open(path)?;
    let mut recorded: Digest32 = [0; 32];
    file.read_exact(&mut recorded).map_err(|error| {
        StorageError::corrupt(format!("truncated entity file {}: {error}", path.display()))
    })?;
    let mut hasher = Sha256::new();
    let mut chunk = [0u8; READ_CHUNK];
    loop {
        let n = file.read(&mut chunk)?;
        if n == 0 {
            break;
        }
        hasher.update(&chunk[..n]);
    }
    let actual: Digest32 = hasher.finalize().into();
    if actual != recorded {
        return Err(StorageError::corrupt(format!(
            "entity {entity} in {scope} no longer matches its recorded digest"
        )));
    }
    Ok(())
}

/// Scans the journal log, returning the in-memory index. Missing log yields
/// an empty index; malformed framing fails with [`StorageError::Corrupt`].
fn read_journal_index(log: &Path) -> Result<Vec<JournalKind>, StorageError> {
    let bytes = match fs::read(log) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let corrupt = || StorageError::corrupt(format!("truncated journal log {}", log.display()));
    let mut entries = Vec::new();
    let mut position = 0usize;
    while position < bytes.len() {
        if bytes.len() - position < 5 {
            return Err(corrupt());
        }
        let mut len_buf = [0u8; 4];
        len_buf.copy_from_slice(&bytes[position..position + 4]);
        let len = u32::from_be_bytes(len_buf) as usize;
        let kind = bytes[position + 4];
        position += 5;
        if bytes.len() - position < len {
            return Err(corrupt());
        }
        let payload = &bytes[position..position + len];
        position += len;
        match kind {
            KIND_INTENT => entries.push(JournalKind::Intent(payload.to_vec())),
            KIND_COMMIT => {
                if payload.len() < 8 {
                    return Err(StorageError::corrupt(format!(
                        "commit record in {} shorter than 8 bytes",
                        log.display()
                    )));
                }
                let mut cursor = [0u8; 8];
                cursor.copy_from_slice(&payload[..8]);
                entries.push(JournalKind::Commit {
                    intent: Cursor(u64::from_be_bytes(cursor)),
                    record: payload[8..].to_vec(),
                });
            }
            other => {
                return Err(StorageError::corrupt(format!(
                    "unknown journal record kind {other} in {}",
                    log.display()
                )));
            }
        }
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sha256;

    fn store() -> (tempfile::TempDir, FsStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = FsStore::open(dir.path()).unwrap();
        (dir, store)
    }

    #[test]
    fn put_then_open_read_roundtrips_payload() {
        let (_dir, mut store) = store();
        let scope = ScopePath::root();
        let payload = b"hello pipestream";
        store.put(&scope, 7, payload, &sha256(payload)).unwrap();
        let mut reader = store.open_read(&scope, 7).unwrap();
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, payload);
    }

    #[test]
    fn journal_survives_reopen() {
        let (dir, mut store) = store();
        let intent = store.append_intent(b"work-1").unwrap();
        store.commit(intent, b"done-1").unwrap();
        drop(store);
        let store = FsStore::open(dir.path()).unwrap();
        let entries = store.replay(Cursor(0)).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].kind, JournalKind::Intent(b"work-1".to_vec()));
        assert_eq!(
            entries[1].kind,
            JournalKind::Commit {
                intent,
                record: b"done-1".to_vec()
            }
        );
    }

    #[test]
    fn seal_record_survives_reopen() {
        let (dir, mut store) = store();
        let scope = ScopePath::root().child(3).unwrap();
        let payload = b"sealed bytes";
        store.put(&scope, 1, payload, &sha256(payload)).unwrap();
        store.seal_scope(&scope, &sha256(b"scope"), 1).unwrap();
        drop(store);
        let store = FsStore::open(dir.path()).unwrap();
        let record = store.verify_scope(&scope).unwrap();
        assert_eq!(record.entity_count, 1);
        assert_eq!(record.digest, sha256(b"scope"));
    }

    #[test]
    fn scratch_dir_is_inside_root_and_cleaned_on_close() {
        let (dir, store) = store();
        assert!(dir.path().join(SCRATCH_DIR).exists());
        store.close().unwrap();
        assert!(!dir.path().join(SCRATCH_DIR).exists());
    }

    #[test]
    fn verify_scope_detects_tampered_entity_file() {
        let (dir, mut store) = store();
        let scope = ScopePath::root();
        let payload = b"integrity matters";
        store.put(&scope, 5, payload, &sha256(payload)).unwrap();
        store.seal_scope(&scope, &sha256(b"scope"), 1).unwrap();
        // Flip a payload byte behind the backend's back.
        let entity_path = store.entity_path(&scope, 5);
        let mut contents = std::fs::read(&entity_path).unwrap();
        let last = contents.len() - 1;
        contents[last] ^= 0xff;
        std::fs::write(&entity_path, contents).unwrap();
        match store.verify_scope(&scope) {
            Err(StorageError::Corrupt(_)) => {}
            other => panic!("expected Corrupt after tampering, got {other:?}"),
        }
        drop(dir);
    }
}

#[cfg(test)]
mod contract_tests {
    use super::*;
    use crate::contract;

    #[test]
    fn passes_the_spi_contract() {
        // Fresh stores each need a live root for the duration of run_all.
        let mut fresh_dirs: Vec<tempfile::TempDir> = Vec::new();
        let fresh = || {
            let dir = tempfile::tempdir().unwrap();
            let store = FsStore::open(dir.path()).unwrap();
            fresh_dirs.push(dir);
            store
        };
        // Durability-of-return crosses a real close/reopen of one root: the
        // contract drops its first handle before reopening.
        let reopen_dir = tempfile::tempdir().unwrap();
        let reopen_root = reopen_dir.path().to_path_buf();
        let reopen = || FsStore::open(&reopen_root).unwrap();
        contract::run_all(fresh, reopen);
    }
}
