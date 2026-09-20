//! Filesystem backend implementing both SPI roles: the reference default.
//!
//! The scope tree maps to directories, entities shard into 2-hex-character
//! fanout buckets within their leaf, and one seal record holding the scope
//! digest and entity count lives beside the objects. The physical formats
//! are pinned cross-language (design note, "Pinned decisions"):
//!
//! ```text
//! <root>/
//!   objects/<scope-hex>/.../scope.seal          [32B digest][8B BE entity count]
//!   objects/<scope-hex>/.../<bucket-2hex>/<entity-16hex>
//!                                               [8B BE payload length][32B sha256][payload]
//!   objects/writer.lock                         exclusive-writer lock (see below)
//!   journal/journal.log                         append-only journal (see below)
//!   journal/writer.lock
//! ```
//!
//! Journal framing: the file starts with the 7-byte magic `PSJSL01`, then
//! records of `[type:1][cursor:8 BE][idLen:4 BE][payloadLen:4 BE][id]
//! [payload][sha256(header || id || payload):32]`. A torn trailing record
//! left by a killed writer is truncated at open — that append never
//! returned success; corruption anywhere else fails the open with
//! [`StorageError::Corrupt`].
//!
//! Durability configuration: every mutation writes to a scratch file in the
//! target directory (`write-*.tmp`), fsyncs it, atomically renames it into
//! place, and fsyncs the containing directories before returning success;
//! journal appends fsync the log before returning. Success implies survival
//! of a process kill. Scratch is never placed in an ambient temp directory
//! and is swept on open and on close.
//!
//! One cooperating process owns a store root: opening takes an exclusive
//! advisory lock on both `writer.lock` files, and a second opener fails
//! with [`StorageError::Failed`]. On Linux this uses `flock(2)`; note that
//! the Java lane's `FileLock` uses `fcntl(2)` record locks, and the two
//! families do not interlock — the guard is same-language.
//!
//! Open-read survival relies on POSIX semantics: an open file descriptor
//! stays valid after the file is unlinked, so retiring an object does not
//! invalidate a live reader.

use crate::layout;
use crate::{
    Cursor, Digest32, EntityId, Intent, Journal, MAX_INTENT_ID_BYTES, MAX_RECORD_BYTES,
    ObjectStore, ReplayEntry, ScopePath, SealRecord, StorageError,
};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

const OBJECTS_DIR: &str = "objects";
const SEAL_FILE_NAME: &str = "scope.seal";
const JOURNAL_DIR: &str = "journal";
const JOURNAL_LOG_NAME: &str = "journal.log";
const WRITER_LOCK_NAME: &str = "writer.lock";
const SCRATCH_PREFIX: &str = "write-";
const SCRATCH_SUFFIX: &str = ".tmp";
const ENTITY_HEADER_BYTES: usize = 8 + 32;
const SEAL_RECORD_LEN: usize = 32 + 8;
const READ_CHUNK: usize = 64 * 1024;

const JOURNAL_MAGIC: &[u8; 7] = b"PSJSL01";
const TYPE_INTENT: u8 = 0;
const TYPE_COMMIT: u8 = 1;
/// `[type:1][cursor:8][idLen:4][payloadLen:4]`.
const RECORD_HEADER_BYTES: usize = 1 + 8 + 4 + 4;

/// The journal log handle. Production uses a real file; tests install a
/// failing sink to prove a failed append truncates back to the pre-append
/// offset.
pub(crate) trait LogIo: Send {
    /// Current logical length of the log.
    fn len(&mut self) -> std::io::Result<u64>;
    /// Appends bytes at the end of the log.
    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()>;
    /// Flushes the log to stable storage.
    fn sync(&mut self) -> std::io::Result<()>;
    /// Truncates the log to `len` bytes.
    fn truncate(&mut self, len: u64) -> std::io::Result<()>;
}

impl LogIo for File {
    fn len(&mut self) -> std::io::Result<u64> {
        Ok(self.metadata()?.len())
    }

    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        Write::write_all(self, bytes)
    }

    fn sync(&mut self) -> std::io::Result<()> {
        self.sync_all()
    }

    fn truncate(&mut self, len: u64) -> std::io::Result<()> {
        self.set_len(len)
    }
}

/// Filesystem backend for both the [`ObjectStore`] and [`Journal`] roles.
///
/// See the [module documentation](self) for the on-disk layout and the
/// durability configuration.
pub struct FsStore {
    root: PathBuf,
    journal: JournalIndex,
    log: Box<dyn LogIo>,
    scratch_seq: AtomicU64,
    _locks: WriterLocks,
}

/// In-process guard against a second writer on the same root (the OS locks
/// cover the cross-process case).
static OPEN_ROOTS: OnceLock<Mutex<std::collections::HashSet<PathBuf>>> = OnceLock::new();

fn open_roots() -> &'static Mutex<std::collections::HashSet<PathBuf>> {
    OPEN_ROOTS.get_or_init(|| Mutex::new(std::collections::HashSet::new()))
}

/// Held writer locks; dropping releases them.
#[derive(Debug)]
struct WriterLocks {
    _objects_lock: File,
    _journal_lock: File,
    objects_dir: PathBuf,
    journal_dir: PathBuf,
}

impl WriterLocks {
    fn acquire(root: &Path) -> Result<Self, StorageError> {
        let objects_dir = root.join(OBJECTS_DIR);
        let journal_dir = root.join(JOURNAL_DIR);
        let objects_lock = acquire_one(&objects_dir)?;
        let journal_lock = match acquire_one(&journal_dir) {
            Ok(lock) => lock,
            Err(error) => {
                drop(objects_lock);
                return Err(error);
            }
        };
        {
            let mut roots = open_roots()
                .lock()
                .map_err(|_| StorageError::failed("writer registry lock poisoned"))?;
            if !roots.insert(canonical_key(&objects_dir)) {
                return Err(StorageError::failed(format!(
                    "store at {} already has a writer in this process",
                    root.display()
                )));
            }
            if !roots.insert(canonical_key(&journal_dir)) {
                roots.remove(&canonical_key(&objects_dir));
                return Err(StorageError::failed(format!(
                    "store at {} already has a writer in this process",
                    root.display()
                )));
            }
        }
        Ok(WriterLocks {
            _objects_lock: objects_lock,
            _journal_lock: journal_lock,
            objects_dir,
            journal_dir,
        })
    }
}

impl Drop for WriterLocks {
    fn drop(&mut self) {
        if let Ok(mut roots) = open_roots().lock() {
            roots.remove(&canonical_key(&self.objects_dir));
            roots.remove(&canonical_key(&self.journal_dir));
        }
    }
}

fn canonical_key(dir: &Path) -> PathBuf {
    fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf())
}

/// Takes the exclusive advisory lock on `dir/writer.lock`.
#[cfg(unix)]
fn acquire_one(dir: &Path) -> Result<File, StorageError> {
    use rustix::fs::{FlockOperation, flock};
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(dir.join(WRITER_LOCK_NAME))?;
    match flock(&file, FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => Ok(file),
        Err(_) => Err(StorageError::failed(format!(
            "store directory {} already has a writer",
            dir.display()
        ))),
    }
}

/// Non-unix fallback: presence of the lock file rejects a second writer.
/// Stale locks after a kill are not recovered on these platforms.
#[cfg(not(unix))]
fn acquire_one(dir: &Path) -> Result<File, StorageError> {
    match OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(dir.join(WRITER_LOCK_NAME))
    {
        Ok(file) => Ok(file),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Err(
            StorageError::failed(format!(
                "store directory {} already has a writer",
                dir.display()
            )),
        ),
        Err(error) => Err(error.into()),
    }
}

impl FsStore {
    /// Opens (creating if necessary) the exclusively owned store rooted at
    /// `root`. Abandoned scratch files from a killed predecessor are swept
    /// before the store accepts operations; a torn trailing journal record
    /// is truncated; real journal corruption fails the open with
    /// [`StorageError::Corrupt`].
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, StorageError> {
        let root = root.into();
        if root.exists() && !root.is_dir() {
            return Err(StorageError::failed(format!(
                "store root {} is not a real directory",
                root.display()
            )));
        }
        create_dirs_durable(&root.join(OBJECTS_DIR))?;
        create_dirs_durable(&root.join(JOURNAL_DIR))?;
        let locks = WriterLocks::acquire(&root)?;
        let open_result = (|| -> Result<(JournalIndex, Box<dyn LogIo>), StorageError> {
            sweep_scratch(&root.join(OBJECTS_DIR))?;
            let log_path = root.join(JOURNAL_DIR).join(JOURNAL_LOG_NAME);
            // Recovery creates the log and its magic when absent, truncates
            // a torn tail, and rebuilds the index; the store's own handle
            // opens in append mode so writes always land at EOF.
            let journal = recover_journal(&log_path)?;
            let log: Box<dyn LogIo> = Box::new(OpenOptions::new().append(true).open(&log_path)?);
            Ok((journal, log))
        })();
        match open_result {
            Ok((journal, log)) => Ok(FsStore {
                root,
                journal,
                log,
                scratch_seq: AtomicU64::new(0),
                _locks: locks,
            }),
            Err(error) => Err(error),
        }
    }

    /// Sweeps any scratch files left under the root and releases the writer
    /// locks. Also done best-effort on drop; explicit close surfaces errors.
    pub fn close(mut self) -> Result<(), StorageError> {
        self.log.sync()?;
        sweep_scratch(&self.root.join(OBJECTS_DIR))?;
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
            .join(layout::bucket_hex(entity))
            .join(layout::entity_component(entity))
    }

    /// Fresh scratch path inside `dir` (the same directory as the final
    /// target, so rename is atomic on the same filesystem).
    fn scratch_path(&self, dir: &Path) -> PathBuf {
        let seq = self.scratch_seq.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        dir.join(format!(
            "{SCRATCH_PREFIX}{}-{seq}-{nanos}{SCRATCH_SUFFIX}",
            std::process::id()
        ))
    }

    /// Writes `parts` to a scratch file in the target's directory, fsyncs
    /// it, renames it onto `target`, and fsyncs the containing directory.
    fn write_durable(&self, target: &Path, parts: &[&[u8]]) -> Result<(), StorageError> {
        let dir = match target.parent() {
            Some(dir) => dir.to_path_buf(),
            None => return Err(StorageError::failed("target path has no parent")),
        };
        create_dirs_durable(&dir)?;
        let scratch = self.scratch_path(&dir);
        let result = (|| -> Result<(), StorageError> {
            {
                let mut file = OpenOptions::new().create_new(true).write(true).open(&scratch)?;
                for part in parts {
                    Write::write_all(&mut file, part)?;
                }
                file.sync_all()?;
            }
            fs::rename(&scratch, target)?;
            sync_dir(&dir)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&scratch);
        }
        result
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
        let scope_dir = self.scope_dir(scope);
        // Seal is checked before the digest: a sealed scope rejects every
        // put regardless of bytes.
        if scope_dir.join(SEAL_FILE_NAME).is_file() {
            return Err(StorageError::already_sealed(format!(
                "scope {scope} is sealed"
            )));
        }
        let digest = layout::sha256(bytes);
        if &digest != expected_digest {
            return Err(StorageError::corrupt(format!(
                "digest mismatch for entity {entity} in {scope}"
            )));
        }
        let target = self.entity_path(scope, entity);
        if target.exists() {
            let (recorded, _) = read_entity_header(&target)?;
            if recorded == digest {
                return Ok(());
            }
            return Err(StorageError::corrupt(format!(
                "conflicting bytes for entity {entity} in {scope}"
            )));
        }
        let mut header = Vec::with_capacity(ENTITY_HEADER_BYTES);
        header.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
        header.extend_from_slice(&digest);
        self.write_durable(&target, &[&header, bytes])
    }

    fn open_read(&self, scope: &ScopePath, entity: EntityId) -> Result<File, StorageError> {
        layout::check_entity_id(entity)?;
        let path = self.entity_path(scope, entity);
        if !path.is_file() {
            return Err(StorageError::not_found(format!(
                "entity {entity} is absent from scope {scope}"
            )));
        }
        let mut file = File::open(&path)?;
        if file.metadata()?.len() < ENTITY_HEADER_BYTES as u64 {
            return Err(StorageError::corrupt(format!(
                "entity file {} is truncated",
                path.display()
            )));
        }
        file.seek(SeekFrom::Start(ENTITY_HEADER_BYTES as u64))?;
        Ok(file)
    }

    fn seal_scope(
        &mut self,
        scope: &ScopePath,
        digest: &Digest32,
        entity_count: u64,
    ) -> Result<(), StorageError> {
        let scope_dir = self.scope_dir(scope);
        let seal_path = scope_dir.join(SEAL_FILE_NAME);
        if seal_path.exists() {
            let existing = read_seal_record(&seal_path, scope)?;
            if existing.digest == *digest && existing.entity_count == entity_count {
                return Ok(());
            }
            return Err(StorageError::corrupt(format!(
                "scope {scope} already carries a different seal"
            )));
        }
        let mut contents = Vec::with_capacity(SEAL_RECORD_LEN);
        contents.extend_from_slice(digest);
        contents.extend_from_slice(&entity_count.to_be_bytes());
        self.write_durable(&seal_path, &[&contents])
    }

    fn verify_scope(&self, scope: &ScopePath) -> Result<SealRecord, StorageError> {
        let scope_dir = self.scope_dir(scope);
        let seal_path = scope_dir.join(SEAL_FILE_NAME);
        if !seal_path.exists() {
            return Err(StorageError::not_found(format!(
                "scope {scope} has no seal record"
            )));
        }
        let seal = read_seal_record(&seal_path, scope)?;
        let ids = self.list(scope)?;
        let mut live_count = 0u64;
        for entity in ids {
            read_verified_digest(&self.entity_path(scope, entity), entity, scope)?;
            live_count += 1;
        }
        if live_count != seal.entity_count {
            return Err(StorageError::corrupt(format!(
                "scope {scope} entity count differs from its seal"
            )));
        }
        Ok(seal)
    }

    fn list(&self, scope: &ScopePath) -> Result<Vec<EntityId>, StorageError> {
        let scope_dir = self.scope_dir(scope);
        if !scope_dir.is_dir() {
            return Ok(Vec::new());
        }
        list_entity_ids(&scope_dir, scope)
    }

    fn retire(&mut self, scope: &ScopePath, retention_cursor: u64) -> Result<u64, StorageError> {
        let scope_dir = self.scope_dir(scope);
        if !scope_dir.is_dir() {
            return Err(StorageError::not_found(format!(
                "scope {scope} holds nothing to retire"
            )));
        }
        let seal_path = scope_dir.join(SEAL_FILE_NAME);
        let sealed = seal_path.exists();
        let ids = list_entity_ids(&scope_dir, scope)?;
        if ids.is_empty() && !sealed {
            return Err(StorageError::not_found(format!(
                "scope {scope} holds nothing to retire"
            )));
        }
        // A sealed scope retires whole — scope-locally: its own entities and
        // the seal record go away; child-scope containers are untouched.
        let cursor = if sealed { u64::MAX } else { retention_cursor };
        let mut removed = 0u64;
        let mut touched_buckets = std::collections::BTreeSet::new();
        for entity in ids {
            if u64::from(entity) > cursor {
                continue;
            }
            let target = self.entity_path(scope, entity);
            fs::remove_file(&target)?;
            removed += 1;
            if let Some(bucket) = target.parent() {
                touched_buckets.insert(bucket.to_path_buf());
            }
        }
        // Each bucket is fsynced once, removed when empty, and the removal
        // itself fsynced in the scope directory.
        for bucket in touched_buckets {
            sync_dir(&bucket)?;
            if fs::remove_dir(&bucket).is_ok() {
                sync_dir(&scope_dir)?;
            }
        }
        if sealed {
            fs::remove_file(&seal_path)?;
            sync_dir(&scope_dir)?;
        }
        Ok(removed)
    }
}

/// In-memory mirror of the journal: intents in append order plus the id
/// index. Commit records consume cursors but fold into their intent.
#[derive(Debug, Default)]
struct JournalIndex {
    intents: Vec<IntentState>,
    by_intent_id: HashMap<String, usize>,
    last_cursor: u64,
}

#[derive(Debug)]
struct IntentState {
    intent: Intent,
    cursor: Cursor,
    commit: Option<Vec<u8>>,
    commit_cursor: Option<Cursor>,
}

impl FsStore {
    /// Appends one framed record and fsyncs the log before returning.
    fn append_journal_record(
        &mut self,
        record_type: u8,
        id: &[u8],
        payload: &[u8],
    ) -> Result<Cursor, StorageError> {
        let cursor = Cursor(self.journal.last_cursor + 1);
        let mut header = Vec::with_capacity(RECORD_HEADER_BYTES);
        header.push(record_type);
        header.extend_from_slice(&cursor.0.to_be_bytes());
        header.extend_from_slice(&(id.len() as u32).to_be_bytes());
        header.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        let mut hasher = Sha256::new();
        hasher.update(&header);
        hasher.update(id);
        hasher.update(payload);
        let checksum: Digest32 = hasher.finalize().into();
        let offset = self.log.len()?;
        let append_result = (|| -> Result<(), StorageError> {
            self.log.write_all(&header)?;
            self.log.write_all(id)?;
            self.log.write_all(payload)?;
            self.log.write_all(&checksum)?;
            self.log.sync()?;
            Ok(())
        })();
        if let Err(error) = append_result {
            // The partial record never returned success; truncate it so a
            // later open is not wedged behind an unparseable tail.
            let _ = self.log.truncate(offset);
            return Err(error);
        }
        self.journal.last_cursor = cursor.0;
        Ok(cursor)
    }
}

impl Journal for FsStore {
    fn append_intent(&mut self, intent: &Intent) -> Result<Cursor, StorageError> {
        if let Some(index) = self.journal.by_intent_id.get(intent.id()) {
            let existing = &self.journal.intents[*index];
            if existing.intent.record() == intent.record() {
                return Ok(existing.cursor);
            }
            return Err(StorageError::corrupt(format!(
                "intent {} already exists with different bytes",
                intent.id()
            )));
        }
        let cursor = self.append_journal_record(TYPE_INTENT, intent.id().as_bytes(), intent.record())?;
        self.journal
            .by_intent_id
            .insert(intent.id().to_string(), self.journal.intents.len());
        self.journal.intents.push(IntentState {
            intent: intent.clone(),
            cursor,
            commit: None,
            commit_cursor: None,
        });
        Ok(cursor)
    }

    fn commit(&mut self, intent_id: &str, record: &[u8]) -> Result<(), StorageError> {
        if record.is_empty() || record.len() > MAX_RECORD_BYTES {
            return Err(StorageError::failed(format!(
                "commit record must be 1 to {MAX_RECORD_BYTES} octets"
            )));
        }
        let Some(index) = self.journal.by_intent_id.get(intent_id).copied() else {
            return Err(StorageError::not_found(format!(
                "intent {intent_id} has no durable append"
            )));
        };
        {
            let entry = &self.journal.intents[index];
            match &entry.commit {
                Some(existing) if existing == record => return Ok(()),
                Some(_) => {
                    return Err(StorageError::corrupt(format!(
                        "intent {intent_id} already carries a different commit"
                    )));
                }
                None => {}
            }
        }
        let cursor = self.append_journal_record(TYPE_COMMIT, intent_id.as_bytes(), record)?;
        self.journal.intents[index].commit = Some(record.to_vec());
        self.journal.intents[index].commit_cursor = Some(cursor);
        Ok(())
    }

    fn replay(&self, since: Cursor) -> Result<Vec<ReplayEntry>, StorageError> {
        Ok(self
            .journal
            .intents
            .iter()
            .filter(|entry| entry.cursor > since)
            .map(|entry| ReplayEntry {
                cursor: entry.cursor,
                intent: entry.intent.clone(),
                commit: entry.commit.clone(),
                commit_cursor: entry.commit_cursor,
            })
            .collect())
    }
}

impl Drop for FsStore {
    fn drop(&mut self) {
        let _ = self.log.sync();
        let _ = sweep_scratch(&self.root.join(OBJECTS_DIR));
    }
}

/// Creates `dir` and any missing ancestors, fsyncing each level so the
/// directory structure itself is durable.
fn create_dirs_durable(dir: &Path) -> Result<(), StorageError> {
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

fn sync_dir(dir: &Path) -> Result<(), StorageError> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

/// True for scratch file names (`write-*.tmp`).
fn is_scratch(name: &str) -> bool {
    name.starts_with(SCRATCH_PREFIX) && name.ends_with(SCRATCH_SUFFIX)
}

/// Recursively deletes scratch files under `dir`, fsyncing each affected
/// directory.
fn sweep_scratch(dir: &Path) -> Result<(), StorageError> {
    if !dir.is_dir() {
        return Ok(());
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            sweep_scratch(&path)?;
        } else if entry.file_name().to_str().is_some_and(is_scratch) {
            fs::remove_file(&path)?;
            sync_dir(dir)?;
        }
    }
    Ok(())
}

/// Lists the entity ids of a scope directory, validating the pinned layout:
/// 2-hex directories are this scope's buckets, 8-hex directories are child
/// scopes, `scope.seal` and scratch files are skipped, anything else is
/// corruption.
fn list_entity_ids(scope_dir: &Path, scope: &ScopePath) -> Result<Vec<EntityId>, StorageError> {
    let mut ids = Vec::new();
    for entry in fs::read_dir(scope_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(StorageError::corrupt(format!(
                "non-UTF-8 entry in scope {scope}"
            )));
        };
        let path = entry.path();
        if name == SEAL_FILE_NAME || is_scratch(name) {
            continue;
        }
        if name.len() == 8 && name.bytes().all(|b| b.is_ascii_hexdigit()) && path.is_dir() {
            continue; // child scope
        }
        if !(name.len() == 2 && name.bytes().all(|b| b.is_ascii_hexdigit()) && path.is_dir()) {
            return Err(StorageError::corrupt(format!(
                "unexpected entry {name} in scope {scope}"
            )));
        }
        for entity in fs::read_dir(&path)? {
            let entity = entity?;
            let entity_name = entity.file_name();
            let Some(entity_name) = entity_name.to_str() else {
                return Err(StorageError::corrupt(format!(
                    "non-UTF-8 entry in scope {scope}"
                )));
            };
            if is_scratch(entity_name) {
                continue;
            }
            if !(entity_name.len() == 8
                && entity_name.bytes().all(|b| b.is_ascii_hexdigit())
                && entity.path().is_file())
            {
                return Err(StorageError::corrupt(format!(
                    "unexpected entry {entity_name} in scope {scope}"
                )));
            }
            let id = u32::from_str_radix(entity_name, 16).map_err(|_| {
                StorageError::corrupt(format!("unparseable entity name {entity_name} in {scope}"))
            })?;
            ids.push(id);
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

/// Reads an entity file's header: `[8B BE payload length][32B digest]`.
/// Validates the payload length against the actual file size.
fn read_entity_header(path: &Path) -> Result<(Digest32, u64), StorageError> {
    let mut file = File::open(path)?;
    let size = file.metadata()?.len();
    if size < ENTITY_HEADER_BYTES as u64 {
        return Err(StorageError::corrupt(format!(
            "entity file {} is truncated",
            path.display()
        )));
    }
    let mut header = [0u8; ENTITY_HEADER_BYTES];
    file.read_exact(&mut header)?;
    let mut len = [0u8; 8];
    len.copy_from_slice(&header[..8]);
    let len = u64::from_be_bytes(len);
    let mut digest: Digest32 = [0; 32];
    digest.copy_from_slice(&header[8..]);
    if len != size - ENTITY_HEADER_BYTES as u64 {
        return Err(StorageError::corrupt(format!(
            "entity file {} length differs from its header",
            path.display()
        )));
    }
    Ok((digest, len))
}

/// Rehashes an entity file's payload and returns the recorded digest when
/// it still matches.
fn read_verified_digest(
    path: &Path,
    entity: EntityId,
    scope: &ScopePath,
) -> Result<Digest32, StorageError> {
    let (recorded, _len) = read_entity_header(path)?;
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(ENTITY_HEADER_BYTES as u64))?;
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
            "entity {entity} in {scope} fails its recorded digest"
        )));
    }
    Ok(recorded)
}

fn read_seal_record(path: &Path, scope: &ScopePath) -> Result<SealRecord, StorageError> {
    let bytes = fs::read(path)?;
    if bytes.len() != SEAL_RECORD_LEN {
        return Err(StorageError::corrupt(format!(
            "seal record of scope {scope} has wrong length"
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

/// Rebuilds the journal index from the log, truncating at most one torn
/// trailing record: bytes after the last checksummed record never belonged
/// to an append that returned success.
fn recover_journal(log_path: &Path) -> Result<JournalIndex, StorageError> {
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(log_path)?;
    let size = file.metadata()?.len();
    let mut index = JournalIndex::default();
    if size == 0 {
        Write::write_all(&mut file, JOURNAL_MAGIC)?;
        file.sync_all()?;
        if let Some(parent) = log_path.parent() {
            sync_dir(parent)?;
        }
        return Ok(index);
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    if bytes.len() < JOURNAL_MAGIC.len() || &bytes[..JOURNAL_MAGIC.len()] != JOURNAL_MAGIC {
        return Err(StorageError::corrupt("journal log carries a foreign magic"));
    }
    let mut position = JOURNAL_MAGIC.len();
    let mut last_good = position;
    while position < bytes.len() {
        match scan_record(&bytes, position, &mut index)? {
            Some(next) => {
                position = next;
                last_good = next;
            }
            None => break, // torn tail
        }
    }
    if last_good < bytes.len() {
        file.set_len(last_good as u64)?;
        file.sync_all()?;
    }
    Ok(index)
}

/// Validates the record at `position`, folding it into the index. Returns
/// the offset just past it, or `None` for a torn tail.
fn scan_record(
    bytes: &[u8],
    position: usize,
    index: &mut JournalIndex,
) -> Result<Option<usize>, StorageError> {
    if bytes.len() - position < RECORD_HEADER_BYTES + 32 {
        return Ok(None); // torn tail
    }
    let header = &bytes[position..position + RECORD_HEADER_BYTES];
    let record_type = header[0];
    let mut cursor_bytes = [0u8; 8];
    cursor_bytes.copy_from_slice(&header[1..9]);
    let cursor = u64::from_be_bytes(cursor_bytes);
    let mut id_len = [0u8; 4];
    id_len.copy_from_slice(&header[9..13]);
    let id_len = u32::from_be_bytes(id_len) as usize;
    let mut payload_len = [0u8; 4];
    payload_len.copy_from_slice(&header[13..17]);
    let payload_len = u32::from_be_bytes(payload_len) as usize;
    if (record_type != TYPE_INTENT && record_type != TYPE_COMMIT)
        || id_len == 0
        || id_len > MAX_INTENT_ID_BYTES
        || payload_len == 0
        || payload_len > MAX_RECORD_BYTES
    {
        return Err(StorageError::corrupt("journal record geometry is invalid"));
    }
    let end = position + RECORD_HEADER_BYTES + id_len + payload_len + 32;
    if end > bytes.len() {
        return Ok(None); // torn tail
    }
    let id = &bytes[position + RECORD_HEADER_BYTES..position + RECORD_HEADER_BYTES + id_len];
    let payload = &bytes[position + RECORD_HEADER_BYTES + id_len..end - 32];
    let recorded = &bytes[end - 32..end];
    let mut hasher = Sha256::new();
    hasher.update(header);
    hasher.update(id);
    hasher.update(payload);
    let checksum: Digest32 = hasher.finalize().into();
    if checksum[..] != recorded[..] {
        return Err(StorageError::corrupt("journal record checksum mismatch"));
    }
    if cursor != index.last_cursor + 1 {
        return Err(StorageError::corrupt("journal cursor sequence has a gap"));
    }
    index.last_cursor = cursor;
    let intent_id = std::str::from_utf8(id)
        .map_err(|_| StorageError::corrupt("journal intent id is not UTF-8"))?
        .to_string();
    match record_type {
        TYPE_INTENT => {
            if index.by_intent_id.contains_key(&intent_id) {
                return Err(StorageError::corrupt(format!(
                    "journal replays intent identifier {intent_id}"
                )));
            }
            let intent = Intent::new(&intent_id, payload)
                .map_err(|_| StorageError::corrupt("journal intent fails validation"))?;
            index.by_intent_id.insert(intent_id, index.intents.len());
            index.intents.push(IntentState {
                intent,
                cursor: Cursor(cursor),
                commit: None,
                commit_cursor: None,
            });
        }
        _ => {
            let Some(intent_index) = index.by_intent_id.get(&intent_id).copied() else {
                return Err(StorageError::corrupt(format!(
                    "journal commit has no uncommitted intent {intent_id}"
                )));
            };
            let entry = &mut index.intents[intent_index];
            if entry.commit.is_some() {
                return Err(StorageError::corrupt(format!(
                    "journal commit has no uncommitted intent {intent_id}"
                )));
            }
            entry.commit = Some(payload.to_vec());
            entry.commit_cursor = Some(Cursor(cursor));
        }
    }
    Ok(Some(end))
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
        let scope = ScopePath::root(0);
        let payload = b"hello pipestream";
        store.put(&scope, 7, payload, &sha256(payload)).unwrap();
        let mut reader = store.open_read(&scope, 7).unwrap();
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, payload);
    }

    #[test]
    fn journal_survives_reopen_with_commit_state() {
        let (dir, mut store) = store();
        let intent = Intent::new("work-1", b"admitted".to_vec()).unwrap();
        let cursor = store.append_intent(&intent).unwrap();
        assert_eq!(cursor, Cursor(1));
        store.commit("work-1", b"done").unwrap();
        drop(store);
        let mut store = FsStore::open(dir.path()).unwrap();
        let entries = store.replay(Cursor(0)).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].intent.id(), "work-1");
        assert!(entries[0].committed());
        assert_eq!(entries[0].commit.as_deref(), Some(b"done".as_slice()));
        // The commit consumed cursor 2; the next intent gets cursor 3.
        let next = store
            .append_intent(&Intent::new("work-2", b"more".to_vec()).unwrap())
            .unwrap();
        assert_eq!(next, Cursor(3));
    }

    #[test]
    fn seal_record_survives_reopen() {
        let (dir, mut store) = store();
        let scope = ScopePath::root(0).child(3).unwrap();
        let payload = b"sealed bytes";
        store.put(&scope, 1, payload, &sha256(payload)).unwrap();
        let digest = crate::layout::scope_digest(&[(1, sha256(payload))]).unwrap();
        store.seal_scope(&scope, &digest, 1).unwrap();
        drop(store);
        let store = FsStore::open(dir.path()).unwrap();
        let record = store.verify_scope(&scope).unwrap();
        assert_eq!(record.entity_count, 1);
        assert_eq!(record.digest, digest);
    }

    #[test]
    fn verify_scope_detects_tampered_entity_file() {
        let (dir, mut store) = store();
        let scope = ScopePath::root(0);
        let payload = b"integrity matters";
        store.put(&scope, 5, payload, &sha256(payload)).unwrap();
        let digest = crate::layout::scope_digest(&[(5, sha256(payload))]).unwrap();
        store.seal_scope(&scope, &digest, 1).unwrap();
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

    #[test]
    fn torn_journal_tail_is_truncated_at_open() {
        let (dir, mut store) = store();
        store
            .append_intent(&Intent::new("kept", b"record".to_vec()).unwrap())
            .unwrap();
        drop(store);
        // Simulate a killed writer mid-append: garbage bytes at the tail.
        let log_path = dir.path().join(JOURNAL_DIR).join(JOURNAL_LOG_NAME);
        let mut log = OpenOptions::new().append(true).open(&log_path).unwrap();
        Write::write_all(&mut log, &[0xaa, 0xbb, 0xcc]).unwrap();
        log.sync_all().unwrap();
        drop(log);
        let store = FsStore::open(dir.path()).unwrap();
        let entries = store.replay(Cursor(0)).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].intent.id(), "kept");
    }

    #[test]
    fn second_writer_is_refused() {
        let (_dir, _first) = store();
        match FsStore::open(_dir.path()) {
            Err(StorageError::Failed(_)) => {}
            other => panic!("expected Failed for a second writer, got {:?}", other.map(|_| ())),
        }
    }

    #[test]
    fn scratch_is_swept_on_open_and_close() {
        let (dir, store) = store();
        let scratch = dir
            .path()
            .join(OBJECTS_DIR)
            .join("write-abandoned.tmp");
        std::fs::write(&scratch, b"partial").unwrap();
        drop(store);
        assert!(!scratch.exists(), "drop sweeps scratch");
        std::fs::write(&scratch, b"partial").unwrap();
        let store = FsStore::open(dir.path()).unwrap();
        assert!(!scratch.exists(), "open sweeps scratch");
        store.close().unwrap();
    }

    #[test]
    fn retire_dedupes_buckets_shared_by_multiple_entities() {
        let (_dir, mut store) = store();
        let scope = ScopePath::root(0);
        // Entities 0 and 85 share fanout bucket "df" (pinned layout vector).
        assert_eq!(crate::layout::bucket_hex(0), "df");
        assert_eq!(crate::layout::bucket_hex(85), "df");
        store.put(&scope, 0, b"zero", &sha256(b"zero")).unwrap();
        store.put(&scope, 85, b"eighty-five", &sha256(b"eighty-five")).unwrap();
        assert_eq!(store.retire(&scope, 85).unwrap(), 2);
        assert!(store.list(&scope).unwrap().is_empty());
        assert!(!store.entity_path(&scope, 0).parent().unwrap().exists());
    }

    #[test]
    fn failed_append_truncates_back_to_the_committed_prefix() {
        let (dir, mut store) = store();
        store
            .append_intent(&Intent::new("first", b"one".to_vec()).unwrap())
            .unwrap();
        // Fail the second part write (the id bytes), mid-record.
        let log_path = dir.path().join(JOURNAL_DIR).join(JOURNAL_LOG_NAME);
        let real = OpenOptions::new().append(true).open(&log_path).unwrap();
        store.log = Box::new(FailingSink {
            inner: real,
            writes: 0,
            fail_at: 1,
        });
        match store.append_intent(&Intent::new("second", b"two".to_vec()).unwrap()) {
            Err(StorageError::Transient(_)) => {}
            other => panic!("expected Transient for the failed append, got {other:?}"),
        }
        drop(store);
        // Reopen: the committed prefix replays cleanly and the log keeps
        // working — the failed append left nothing behind.
        let mut store = FsStore::open(dir.path()).unwrap();
        let entries = store.replay(Cursor(0)).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].intent.id(), "first");
        assert_eq!(
            store
                .append_intent(&Intent::new("second", b"two".to_vec()).unwrap())
                .unwrap(),
            Cursor(2)
        );
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

/// A journal sink that fails one chosen write after writing only a prefix
/// of it, to prove a failed append truncates back to the pre-append offset.
#[cfg(test)]
struct FailingSink {
    inner: File,
    writes: usize,
    fail_at: usize,
}

#[cfg(test)]
impl LogIo for FailingSink {
    fn len(&mut self) -> std::io::Result<u64> {
        Ok(self.inner.metadata()?.len())
    }

    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.writes += 1;
        if self.writes == self.fail_at + 1 {
            // Torn write: part of the record lands, then the write fails.
            Write::write_all(&mut self.inner, &bytes[..bytes.len() / 2])?;
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "injected write failure",
            ));
        }
        Write::write_all(&mut self.inner, bytes)
    }

    fn sync(&mut self) -> std::io::Result<()> {
        self.inner.sync_all()
    }

    fn truncate(&mut self, len: u64) -> std::io::Result<()> {
        self.inner.set_len(len)
    }
}
