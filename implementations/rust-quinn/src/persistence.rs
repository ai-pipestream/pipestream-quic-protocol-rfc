//! Durable, compare-and-swap session persistence.

use crate::{ProtocolError, session::SESSION_FORMAT_VERSION, session::Session};
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use sha2::{Digest, Sha256};
use std::{
    error::Error,
    fmt, fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

mod binding;
mod maintenance;
pub use maintenance::PayloadMaintenance;
mod image;
pub use binding::{PAYLOAD_BINDING_BYTES, PayloadBinding, StoreIdentity};
#[cfg(test)]
mod index_delta_tests;
mod queue;
#[cfg(test)]
mod rehydration_tests;
mod schema;
pub use queue::{JobQueueLimits, JobQueueUsage, ReadyJob};
mod storage;
pub use storage::{StorageLimits, StorageUsage};
mod physical;
pub use physical::{PhysicalLimits, PhysicalUsage};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS pipestream_sessions (
    session_id TEXT PRIMARY KEY NOT NULL,
    image BLOB NOT NULL CHECK (length(image) > 104)
) STRICT;
CREATE TABLE IF NOT EXISTS pipestream_payload_binding (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    image BLOB NOT NULL CHECK (length(image) = 72)
) STRICT;
";

#[derive(Debug, Clone, PartialEq)]
pub struct VersionedSession {
    pub revision: u64,
    pub session: Session,
}

#[derive(Debug)]
pub enum StoreError {
    Database(rusqlite::Error),
    Io(std::io::Error),
    Codec(String),
    Corrupt(String),
    NotFound(String),
    Conflict { expected: u64, actual: u64 },
    Protocol(ProtocolError),
    Clock(String),
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(error) => write!(formatter, "session database error: {error}"),
            Self::Io(error) => write!(formatter, "session store I/O error: {error}"),
            Self::Codec(detail) => write!(formatter, "session state codec error: {detail}"),
            Self::Corrupt(detail) => write!(formatter, "session store corruption: {detail}"),
            Self::NotFound(session) => write!(formatter, "session not found: {session}"),
            Self::Conflict { expected, actual } => write!(
                formatter,
                "session revision conflict: expected {expected}, actual {actual}"
            ),
            Self::Protocol(error) => error.fmt(formatter),
            Self::Clock(detail) => write!(formatter, "system clock error: {detail}"),
        }
    }
}

impl Error for StoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Database(error) => Some(error),
            Self::Io(error) => Some(error),
            Self::Protocol(error) => Some(error),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for StoreError {
    fn from(error: rusqlite::Error) -> Self {
        if error.sqlite_error_code() == Some(rusqlite::ErrorCode::DiskFull) {
            Self::Protocol(ProtocolError::limit("SQLite storage capacity exhausted"))
        } else {
            Self::Database(error)
        }
    }
}

impl From<std::io::Error> for StoreError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

pub trait SessionStore: Send + Sync {
    fn create(&self, session: &Session) -> Result<VersionedSession, StoreError>;
    fn load(&self, session_id: &str) -> Result<Option<VersionedSession>, StoreError>;
    fn save(
        &self,
        expected_revision: u64,
        session: &Session,
    ) -> Result<VersionedSession, StoreError>;
}

#[derive(Debug, Clone)]
pub struct SqliteSessionStore {
    path: PathBuf,
    job_limits: JobQueueLimits,
    storage_limits: StorageLimits,
    physical: Arc<physical::Guard>,
    identity: Option<StoreIdentity>,
}

impl SqliteSessionStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, StoreError> {
        Self::open_configured(path.into(), None, None, None)
    }

    /// Queue limits are durable database policy. Reopening cannot silently replace them.
    pub fn open_with_job_limits(
        path: impl Into<PathBuf>,
        limits: JobQueueLimits,
    ) -> Result<Self, StoreError> {
        limits.validate()?;
        Self::open_configured(path.into(), Some(limits), None, None)
    }

    /// Configure persistent queue and serialized-state policy when creating a store.
    /// Reopening cannot silently change either policy or account an existing unbounded store.
    pub fn open_with_limits(
        path: impl Into<PathBuf>,
        jobs: JobQueueLimits,
        storage: StorageLimits,
    ) -> Result<Self, StoreError> {
        jobs.validate()?;
        storage.validate()?;
        Self::open_configured(path.into(), Some(jobs), Some(storage), None)
    }

    /// Set all durable budgets on creation. Existing policies cannot be replaced.
    pub fn open_with_all_limits(
        path: impl Into<PathBuf>,
        jobs: JobQueueLimits,
        storage: StorageLimits,
        physical: PhysicalLimits,
    ) -> Result<Self, StoreError> {
        jobs.validate()?;
        storage.validate()?;
        Self::open_configured(path.into(), Some(jobs), Some(storage), Some(physical))
    }

    fn open_configured(
        path: PathBuf,
        requested: Option<JobQueueLimits>,
        storage_limits: Option<StorageLimits>,
        physical_limits: Option<PhysicalLimits>,
    ) -> Result<Self, StoreError> {
        let physical = physical::Guard::open(&path, physical_limits)?;
        let path = physical.path.clone();
        let parent = path
            .parent()
            .ok_or_else(|| StoreError::Corrupt("missing database directory".into()))?
            .to_owned();
        let mut store = Self {
            path,
            job_limits: JobQueueLimits::default(),
            storage_limits: StorageLimits::default(),
            physical,
            identity: None,
        };
        let mut connection = store.connect()?;
        schema::initialize_root(&mut connection, SCHEMA)?;
        store.identity = Some(binding::read(&connection)?.database());
        store.job_limits = queue::initialize(&mut connection, requested)?;
        store.storage_limits = storage::initialize(&mut connection, storage_limits)?;
        drop(connection);
        sync_directory(&parent)?;
        Ok(store)
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn job_limits(&self) -> JobQueueLimits {
        self.job_limits
    }

    pub fn storage_limits(&self) -> StorageLimits {
        self.storage_limits
    }

    pub fn physical_limits(&self) -> PhysicalLimits {
        self.physical.limits
    }

    /// Current file lengths. Concurrent writes can change them between samples.
    pub fn physical_usage(&self) -> Result<PhysicalUsage, StoreError> {
        self.physical.usage()
    }

    pub fn storage_usage(&self) -> Result<StorageUsage, StoreError> {
        storage::usage(&self.connect()?, None)
    }

    pub fn principal_storage_usage(
        &self,
        principal: Option<&crate::authorization::PrincipalBinding>,
    ) -> Result<StorageUsage, StoreError> {
        storage::usage(&self.connect()?, Some(principal))
    }

    pub fn transact<T>(
        &self,
        session_id: &str,
        operation: impl FnOnce(&mut Session) -> Result<T, ProtocolError>,
    ) -> Result<(T, VersionedSession), StoreError> {
        let mut connection = self.connect()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = load_from(&transaction, session_id)?
            .ok_or_else(|| StoreError::NotFound(session_id.to_owned()))?;
        let mut session = current.session;
        let output = operation(&mut session).map_err(StoreError::Protocol)?;
        if session.session_id != session_id {
            return Err(StoreError::Protocol(ProtocolError::entity(
                "transaction changed session identity",
            )));
        }
        let next_revision = current
            .revision
            .checked_add(1)
            .ok_or_else(|| StoreError::Corrupt("session revision overflow".to_owned()))?;
        persist_update(&transaction, current.revision, next_revision, &session)?;
        transaction.commit()?;
        Ok((
            output,
            VersionedSession {
                revision: next_revision,
                session,
            },
        ))
    }

    pub fn integrity_check(&self) -> Result<(), StoreError> {
        let mut connection = self.connect()?;
        let transaction = connection.transaction()?;
        let result: String =
            transaction.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
        if result != "ok" {
            return Err(StoreError::Corrupt(format!(
                "SQLite integrity_check returned {result}"
            )));
        }
        queue::verify_index(&transaction)?;
        storage::verify_index(&transaction)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn checkpoint(&self) -> Result<(), StoreError> {
        let connection = self.connect()?;
        let busy: i32 =
            connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row.get(0))?;
        if busy != 0 {
            return Err(StoreError::Database(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
                Some("WAL checkpoint blocked; storage has not been reclaimed".into()),
            )));
        }
        Ok(())
    }

    pub fn list_session_ids(&self) -> Result<Vec<String>, StoreError> {
        let connection = self.connect()?;
        let mut statement = connection.prepare(schema::SESSION_IDS)?;
        let mut rows = statement.query([])?;
        let mut ids = Vec::new();
        while let Some(row) = rows.next()? {
            ids.push(schema::session_id(row, 0)?);
        }
        Ok(ids)
    }

    fn connect(&self) -> Result<Connection, StoreError> {
        self.physical.verify()?;
        let connection = Connection::open_with_flags_and_vfs(
            &self.path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            physical::VFS_NAME,
        )?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        connection.execute_batch("PRAGMA temp_store=MEMORY; PRAGMA mmap_size=0;")?;
        let page_size: u32 = connection.query_row("PRAGMA page_size", [], |row| row.get(0))?;
        if !(512..=65536).contains(&page_size) || !page_size.is_power_of_two() {
            return Err(StoreError::Corrupt("unsupported SQLite page size".into()));
        }
        let pages = self.physical.limits.database_bytes / u64::from(page_size);
        let actual: i64 =
            connection.query_row(&format!("PRAGMA max_page_count={pages}"), [], |row| {
                row.get(0)
            })?;
        if actual < 0 || actual as u64 != pages {
            return Err(StoreError::Corrupt(
                "SQLite pages exceed the physical database policy".into(),
            ));
        }
        connection.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=FULL;
             PRAGMA foreign_keys=ON;",
        )?;
        if let Some(identity) = self.identity
            && binding::read(&connection)?.database() != identity
        {
            return Err(StoreError::Corrupt(
                "retained database identity changed".into(),
            ));
        }
        Ok(connection)
    }
}

impl SessionStore for SqliteSessionStore {
    fn create(&self, session: &Session) -> Result<VersionedSession, StoreError> {
        if session.format_version != SESSION_FORMAT_VERSION {
            return Err(StoreError::Corrupt(format!(
                "unsupported in-memory session version {}",
                session.format_version
            )));
        }
        let mut connection = self.connect()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        storage::verify_index(&transaction)?;
        let (state, checksum) = encode_state(session, storage::read_limits(&transaction)?)?;
        queue::verify_index(&transaction)?;
        if let Some(previous) = load_from(&transaction, &session.session_id)? {
            return Err(StoreError::Conflict {
                expected: 0,
                actual: previous.revision,
            });
        }
        let capacity = state.len() + storage::completion_reservation(session, self.storage_limits)?;
        physical::protect(&transaction, session, capacity)?;
        image::write(
            &transaction,
            &session.session_id,
            0,
            1,
            &state,
            &checksum,
            capacity,
        )?;
        queue::replace_index(&transaction, session)?;
        storage::replace_index(&transaction, session, state.len(), &checksum)?;
        transaction.commit()?;
        Ok(VersionedSession {
            revision: 1,
            session: session.clone(),
        })
    }

    fn load(&self, session_id: &str) -> Result<Option<VersionedSession>, StoreError> {
        let mut connection = self.connect()?;
        let transaction = connection.transaction()?;
        let retained = load_from(&transaction, session_id)?;
        transaction.commit()?;
        Ok(retained)
    }

    fn save(
        &self,
        expected_revision: u64,
        session: &Session,
    ) -> Result<VersionedSession, StoreError> {
        let mut connection = self.connect()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let next_revision = expected_revision
            .checked_add(1)
            .ok_or_else(|| StoreError::Corrupt("session revision overflow".to_owned()))?;
        persist_update(&transaction, expected_revision, next_revision, session)?;
        transaction.commit()?;
        Ok(VersionedSession {
            revision: next_revision,
            session: session.clone(),
        })
    }
}

fn load_from(
    connection: &Connection,
    session_id: &str,
) -> Result<Option<VersionedSession>, StoreError> {
    let limit = storage::read_limits(connection)?.record_bytes;
    let Some(header) = image::header(connection, session_id, limit)? else {
        return Ok(None);
    };
    let state = image::read(connection, &header)?;
    let expected = header.checksum;
    let revision = header.revision;
    let session: Session =
        postcard::from_bytes(&state).map_err(|error| StoreError::Codec(error.to_string()))?;
    if session.format_version != SESSION_FORMAT_VERSION || session.session_id != session_id {
        return Err(StoreError::Corrupt(
            "stored session identity or version mismatch".to_owned(),
        ));
    }
    session
        .validate_jobs()
        .map_err(|error| StoreError::Corrupt(error.to_string()))?;
    session
        .validate_recovery()
        .map_err(|error| StoreError::Corrupt(error.to_string()))?;
    storage::validate_entry(connection, &session, state.len(), &expected)?;
    if state.len() + storage::completion_reservation(&session, storage::read_limits(connection)?)?
        > header.capacity
    {
        return Err(StoreError::Corrupt(
            "session image has unfunded completion growth".into(),
        ));
    }
    Ok(Some(VersionedSession { revision, session }))
}

fn persist_update(
    connection: &Connection,
    expected_revision: u64,
    next_revision: u64,
    session: &Session,
) -> Result<(), StoreError> {
    storage::verify_index(connection)?;
    queue::verify_index(connection)?;
    if let Some(previous) = load_from(connection, &session.session_id)? {
        session
            .validate_retained_jobs(&previous.session)
            .map_err(StoreError::Protocol)?;
        session
            .validate_retained_recovery(&previous.session)
            .map_err(StoreError::Protocol)?;
    }
    let limits = storage::read_limits(connection)?;
    let (state, checksum) = encode_state(session, limits)?;
    let capacity = state.len() + storage::completion_reservation(session, limits)?;
    physical::protect(connection, session, capacity)?;
    image::write(
        connection,
        &session.session_id,
        expected_revision,
        next_revision,
        &state,
        &checksum,
        capacity,
    )?;
    queue::replace_index(connection, session)?;
    storage::replace_index(connection, session, state.len(), &checksum)
}

fn encode_state(
    session: &Session,
    limits: StorageLimits,
) -> Result<(Vec<u8>, [u8; 32]), StoreError> {
    crate::session::validate_session_id(&session.session_id).map_err(StoreError::Protocol)?;
    if session.format_version != SESSION_FORMAT_VERSION {
        return Err(StoreError::Corrupt(format!(
            "unsupported in-memory session version {}",
            session.format_version
        )));
    }
    session.validate_jobs().map_err(StoreError::Protocol)?;
    session.validate_recovery().map_err(StoreError::Protocol)?;
    let limit = limits
        .record_bytes
        .checked_sub(storage::completion_reservation(session, limits)?)
        .ok_or_else(|| {
            StoreError::Protocol(ProtocolError::limit(
                "completion reservation exceeds record budget",
            ))
        })?;
    let state = storage::encode(session, limit)?;
    let checksum = Sha256::digest(&state).into();
    Ok((state, checksum))
}

fn now_micros() -> Result<i64, StoreError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| StoreError::Clock(error.to_string()))?;
    i64::try_from(elapsed.as_micros())
        .map_err(|_| StoreError::Clock("timestamp exceeds SQLite integer".to_owned()))
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<(), StoreError> {
    fs::File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<(), StoreError> {
    Ok(())
}

#[cfg(test)]
mod queue_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::{EntityState, NewEntity};

    fn session() -> Session {
        let mut session = Session::new("durable-1", 7, 128).unwrap();
        let root = session
            .add_root(NewEntity {
                entity_id: 1,
                layer: 0,
                payload_digest: [0x5a; 32],
                policy: None,
            })
            .unwrap();
        session.transition(root, EntityState::Processing).unwrap();
        session
    }

    #[test]
    fn wal_round_trip_survives_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("sessions.sqlite3");
        {
            let store = SqliteSessionStore::open(&path).unwrap();
            store.create(&session()).unwrap();
            store.checkpoint().unwrap();
        }
        let reopened = SqliteSessionStore::open(&path).unwrap();
        let loaded = reopened.load("durable-1").unwrap().unwrap();
        assert_eq!(1, loaded.revision);
        assert_eq!(session(), loaded.session);
        reopened.integrity_check().unwrap();
    }

    #[test]
    fn stale_writer_is_refused() {
        let directory = tempfile::tempdir().unwrap();
        let store = SqliteSessionStore::open(directory.path().join("sessions.sqlite3")).unwrap();
        let first = store.create(&session()).unwrap();
        let mut updated = first.session.clone();
        let root = *updated.entities.keys().next().unwrap();
        updated.transition(root, EntityState::Complete).unwrap();
        store.save(first.revision, &updated).unwrap();
        let error = store.save(first.revision, &first.session).unwrap_err();
        assert!(matches!(
            error,
            StoreError::Conflict {
                expected: 1,
                actual: 2
            }
        ));
    }

    #[test]
    fn transaction_commits_state_and_revision_together() {
        let directory = tempfile::tempdir().unwrap();
        let store = SqliteSessionStore::open(directory.path().join("sessions.sqlite3")).unwrap();
        store.create(&session()).unwrap();
        let root = *session().entities.keys().next().unwrap();
        let (_, saved) = store
            .transact("durable-1", |session| {
                session.transition(root, EntityState::Complete)
            })
            .unwrap();
        assert_eq!(2, saved.revision);
        assert_eq!(EntityState::Complete, saved.session.entities[&root].state);
    }
}
