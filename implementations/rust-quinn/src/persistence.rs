//! Durable, compare-and-swap session persistence.

use crate::{ProtocolError, session::SESSION_FORMAT_VERSION, session::Session};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use sha2::{Digest, Sha256};
use std::{
    error::Error,
    fmt, fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

mod queue;
pub use queue::{JobQueueLimits, ReadyJob};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS pipestream_sessions (
    session_id TEXT PRIMARY KEY NOT NULL,
    format_version INTEGER NOT NULL,
    revision INTEGER NOT NULL CHECK (revision > 0),
    state BLOB NOT NULL,
    checksum BLOB NOT NULL CHECK (length(checksum) = 32),
    updated_at_micros INTEGER NOT NULL
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
        Self::Database(error)
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
}

impl SqliteSessionStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, StoreError> {
        Self::open_configured(path.into(), None)
    }

    /// Queue limits are durable database policy. Reopening cannot silently replace them.
    pub fn open_with_job_limits(
        path: impl Into<PathBuf>,
        limits: JobQueueLimits,
    ) -> Result<Self, StoreError> {
        limits.validate()?;
        Self::open_configured(path.into(), Some(limits))
    }

    fn open_configured(
        path: PathBuf,
        requested: Option<JobQueueLimits>,
    ) -> Result<Self, StoreError> {
        let parent = path
            .parent()
            .filter(|value| !value.as_os_str().is_empty())
            .unwrap_or(Path::new("."))
            .to_path_buf();
        fs::create_dir_all(&parent)?;
        let mut store = Self {
            path,
            job_limits: JobQueueLimits::default(),
        };
        let mut connection = store.connect()?;
        connection.execute_batch(SCHEMA)?;
        store.job_limits = queue::initialize(&mut connection, requested)?;
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
        transaction.commit()?;
        Ok(())
    }

    pub fn checkpoint(&self) -> Result<(), StoreError> {
        let connection = self.connect()?;
        connection.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
        Ok(())
    }

    pub fn list_session_ids(&self) -> Result<Vec<String>, StoreError> {
        let connection = self.connect()?;
        let mut statement =
            connection.prepare("SELECT session_id FROM pipestream_sessions ORDER BY session_id")?;
        let rows = statement.query_map([], |row| row.get(0))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    fn connect(&self) -> Result<Connection, StoreError> {
        let connection = Connection::open(&self.path)?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=FULL;
             PRAGMA foreign_keys=ON;",
        )?;
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
        let (state, checksum) = encode_state(session)?;
        let inserted = transaction.execute(
            "INSERT OR IGNORE INTO pipestream_sessions
             (session_id, format_version, revision, state, checksum, updated_at_micros)
             VALUES (?1, ?2, 1, ?3, ?4, ?5)",
            params![
                session.session_id,
                i64::from(SESSION_FORMAT_VERSION),
                state,
                checksum.as_slice(),
                now_micros()?
            ],
        )?;
        if inserted != 1 {
            let actual =
                load_from(&transaction, &session.session_id)?.map_or(0, |value| value.revision);
            return Err(StoreError::Conflict {
                expected: 0,
                actual,
            });
        }
        queue::replace_index(&transaction, session)?;
        transaction.commit()?;
        Ok(VersionedSession {
            revision: 1,
            session: session.clone(),
        })
    }

    fn load(&self, session_id: &str) -> Result<Option<VersionedSession>, StoreError> {
        let connection = self.connect()?;
        load_from(&connection, session_id)
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
    let row = connection
        .query_row(
            "SELECT format_version, revision, state, checksum
             FROM pipestream_sessions WHERE session_id = ?1",
            [session_id],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                ))
            },
        )
        .optional()?;
    let Some((format_version, revision, state, checksum)) = row else {
        return Ok(None);
    };
    if format_version != i64::from(SESSION_FORMAT_VERSION) {
        return Err(StoreError::Corrupt(format!(
            "unsupported stored session version {format_version}"
        )));
    }
    let revision = u64::try_from(revision)
        .map_err(|_| StoreError::Corrupt("negative session revision".to_owned()))?;
    let expected: [u8; 32] = checksum
        .try_into()
        .map_err(|_| StoreError::Corrupt("invalid state checksum length".to_owned()))?;
    let actual: [u8; 32] = Sha256::digest(&state).into();
    if expected != actual {
        return Err(StoreError::Corrupt(
            "stored session checksum mismatch".to_owned(),
        ));
    }
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
    Ok(Some(VersionedSession { revision, session }))
}

fn persist_update(
    connection: &Connection,
    expected_revision: u64,
    next_revision: u64,
    session: &Session,
) -> Result<(), StoreError> {
    if let Some(previous) = load_from(connection, &session.session_id)? {
        session
            .validate_retained_jobs(&previous.session)
            .map_err(StoreError::Protocol)?;
        session
            .validate_retained_recovery(&previous.session)
            .map_err(StoreError::Protocol)?;
    }
    let expected = i64::try_from(expected_revision)
        .map_err(|_| StoreError::Corrupt("session revision exceeds SQLite integer".to_owned()))?;
    let next = i64::try_from(next_revision)
        .map_err(|_| StoreError::Corrupt("session revision exceeds SQLite integer".to_owned()))?;
    let (state, checksum) = encode_state(session)?;
    let changed = connection.execute(
        "UPDATE pipestream_sessions
         SET format_version = ?1, revision = ?2, state = ?3, checksum = ?4,
             updated_at_micros = ?5
         WHERE session_id = ?6 AND revision = ?7",
        params![
            i64::from(SESSION_FORMAT_VERSION),
            next,
            state,
            checksum.as_slice(),
            now_micros()?,
            session.session_id,
            expected
        ],
    )?;
    if changed != 1 {
        let actual: Option<i64> = connection
            .query_row(
                "SELECT revision FROM pipestream_sessions WHERE session_id = ?1",
                [&session.session_id],
                |row| row.get(0),
            )
            .optional()?;
        return match actual {
            Some(actual) => Err(StoreError::Conflict {
                expected: expected_revision,
                actual: u64::try_from(actual).unwrap_or(0),
            }),
            None => Err(StoreError::NotFound(session.session_id.clone())),
        };
    }
    queue::replace_index(connection, session)
}

fn encode_state(session: &Session) -> Result<(Vec<u8>, [u8; 32]), StoreError> {
    if session.format_version != SESSION_FORMAT_VERSION {
        return Err(StoreError::Corrupt(format!(
            "unsupported in-memory session version {}",
            session.format_version
        )));
    }
    session.validate_jobs().map_err(StoreError::Protocol)?;
    session.validate_recovery().map_err(StoreError::Protocol)?;
    let state =
        postcard::to_stdvec(session).map_err(|error| StoreError::Codec(error.to_string()))?;
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
