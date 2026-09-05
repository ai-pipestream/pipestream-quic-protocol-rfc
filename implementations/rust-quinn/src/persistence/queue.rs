use super::*;
use crate::{authorization::PrincipalBinding, execution::ExecutionKey, jobs::JobState};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS pipestream_job_limits (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    total INTEGER NOT NULL CHECK (total BETWEEN 1 AND 65536),
    per_principal INTEGER NOT NULL CHECK (per_principal BETWEEN 1 AND total)
) STRICT;
CREATE TABLE IF NOT EXISTS pipestream_jobs (
    session_id TEXT NOT NULL REFERENCES pipestream_sessions(session_id),
    execution_key BLOB NOT NULL,
    principal BLOB NOT NULL,
    ready_at_micros INTEGER,
    enqueued_at_micros INTEGER NOT NULL,
    PRIMARY KEY (session_id, execution_key)
) STRICT;
CREATE INDEX IF NOT EXISTS pipestream_jobs_ready ON pipestream_jobs
    (ready_at_micros, enqueued_at_micros, session_id, execution_key);
CREATE INDEX IF NOT EXISTS pipestream_jobs_principal ON pipestream_jobs (principal);
";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobQueueLimits {
    /// Queued plus running jobs, including revoked work retained for operator resolution.
    pub total: u32,
    pub per_principal: u32,
}

impl Default for JobQueueLimits {
    fn default() -> Self {
        Self {
            total: 128,
            per_principal: 32,
        }
    }
}

impl JobQueueLimits {
    pub(super) fn validate(self) -> Result<(), StoreError> {
        if self.total == 0
            || self.total > 65_536
            || self.per_principal == 0
            || self.per_principal > self.total
        {
            return Err(StoreError::Protocol(ProtocolError::limit(
                "invalid durable job queue limits",
            )));
        }
        Ok(())
    }
}

/// Discovery is a hint, not an execution grant. Acquire the job under a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadyJob {
    pub session_id: String,
    pub key: ExecutionKey,
    pub principal: Option<PrincipalBinding>,
}

pub(super) fn initialize(
    connection: &mut Connection,
    requested: Option<JobQueueLimits>,
) -> Result<JobQueueLimits, StoreError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let nonempty: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM pipestream_sessions)",
        [],
        |row| row.get(0),
    )?;
    let tables: u32 = transaction.query_row("SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name IN ('pipestream_jobs', 'pipestream_job_limits')", [], |row| row.get(0))?;
    if nonempty && tables != 2 {
        return Err(StoreError::Corrupt(
            "job queue schema is absent from a nonempty session store".into(),
        ));
    }
    if nonempty {
        let policy_exists: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM pipestream_job_limits WHERE singleton = 1)",
            [],
            |row| row.get(0),
        )?;
        if !policy_exists {
            return Err(StoreError::Corrupt(
                "durable job queue policy is missing".into(),
            ));
        }
    }
    transaction.execute_batch(SCHEMA)?;
    let initial = requested.unwrap_or_default();
    transaction.execute(
        "INSERT OR IGNORE INTO pipestream_job_limits VALUES (1, ?1, ?2)",
        params![initial.total, initial.per_principal],
    )?;
    let stored = read_limits(&transaction)?;
    if requested.is_some_and(|value| value != stored) {
        return Err(StoreError::Protocol(ProtocolError::limit(
            "durable job queue limits differ from stored policy",
        )));
    }
    transaction.commit()?;
    Ok(stored)
}

fn read_limits(connection: &Connection) -> Result<JobQueueLimits, StoreError> {
    Ok(connection.query_row(
        "SELECT total, per_principal FROM pipestream_job_limits WHERE singleton = 1",
        [],
        |row| {
            Ok(JobQueueLimits {
                total: row.get(0)?,
                per_principal: row.get(1)?,
            })
        },
    )?)
}

fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, StoreError> {
    postcard::to_stdvec(value).map_err(|error| StoreError::Codec(error.to_string()))
}

fn timestamp(value: u64) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(|_| {
        StoreError::Protocol(ProtocolError::limit("job timestamp exceeds SQLite integer"))
    })
}

fn ready_at(session: &Session, key: &ExecutionKey) -> Result<Option<i64>, StoreError> {
    let job = &session.jobs[key];
    let ready = match job.state {
        JobState::Queued => job.enqueued_at_micros,
        JobState::Running => {
            session
                .executions
                .get(key)
                .ok_or_else(|| StoreError::Corrupt("running job has no attempt".into()))?
                .expires_at_micros
        }
        _ => {
            return Err(StoreError::Corrupt(
                "finished job indexed as unfinished".into(),
            ));
        }
    };
    Ok(
        if session.owner.as_ref().is_some_and(|owner| owner.revoked) {
            None
        } else {
            Some(timestamp(ready)?)
        },
    )
}

/// Scan one session at a time in a stable read transaction, including missing index rows.
pub(super) fn verify_index(connection: &Connection) -> Result<(), StoreError> {
    let mut query =
        connection.prepare("SELECT session_id FROM pipestream_sessions ORDER BY session_id")?;
    let mut rows = query.query([])?;
    let mut expected = 0u64;
    while let Some(row) = rows.next()? {
        let id: String = row.get(0)?;
        let session = load_from(connection, &id)?
            .ok_or_else(|| StoreError::Corrupt("session disappeared during audit".into()))?
            .session;
        let principal = encode(&session.owner.as_ref().map(|owner| &owner.binding))?;
        for (key, job) in &session.jobs {
            if !job.state.is_unfinished() {
                continue;
            }
            let actual = connection.query_row("SELECT principal, ready_at_micros, enqueued_at_micros FROM pipestream_jobs WHERE session_id = ?1 AND execution_key = ?2", params![id, encode(key)?], |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Option<i64>>(1)?, row.get::<_, i64>(2)?))).optional()?;
            if actual.as_ref()
                != Some(&(
                    principal.clone(),
                    ready_at(&session, key)?,
                    timestamp(job.enqueued_at_micros)?,
                ))
            {
                return Err(StoreError::Corrupt(
                    "job index differs from retained session state".into(),
                ));
            }
            expected += 1;
        }
    }
    let actual: u32 =
        connection.query_row("SELECT count(*) FROM pipestream_jobs", [], |row| row.get(0))?;
    let limits = read_limits(connection)?;
    let oversized: u32 = connection.query_row("SELECT count(*) FROM (SELECT principal FROM pipestream_jobs GROUP BY principal HAVING count(*) > ?1)", [limits.per_principal], |row| row.get(0))?;
    if expected != u64::from(actual) || actual > limits.total || oversized != 0 {
        return Err(StoreError::Corrupt(
            "job index has extra rows or exceeds stored limits".into(),
        ));
    }
    Ok(())
}

pub(super) fn replace_index(connection: &Connection, session: &Session) -> Result<(), StoreError> {
    connection.execute(
        "DELETE FROM pipestream_jobs WHERE session_id = ?1",
        [&session.session_id],
    )?;
    let principal = encode(&session.owner.as_ref().map(|owner| &owner.binding))?;
    let count = session
        .jobs
        .values()
        .filter(|job| job.state.is_unfinished())
        .count() as u64;
    if count == 0 {
        return Ok(());
    }
    let limits = read_limits(connection)?;
    let total: u32 =
        connection.query_row("SELECT count(*) FROM pipestream_jobs", [], |row| row.get(0))?;
    let owned: u32 = connection.query_row(
        "SELECT count(*) FROM pipestream_jobs WHERE principal = ?1",
        [&principal],
        |row| row.get(0),
    )?;
    if u64::from(total) + count > u64::from(limits.total)
        || u64::from(owned) + count > u64::from(limits.per_principal)
    {
        return Err(StoreError::Protocol(ProtocolError::limit(
            "durable job queue is full",
        )));
    }
    let mut insert =
        connection.prepare("INSERT INTO pipestream_jobs VALUES (?1, ?2, ?3, ?4, ?5)")?;
    for (key, job) in &session.jobs {
        if !job.state.is_unfinished() {
            continue;
        }
        insert.execute(params![
            session.session_id,
            encode(key)?,
            principal,
            ready_at(session, key)?,
            timestamp(job.enqueued_at_micros)?
        ])?;
    }
    Ok(())
}

impl SqliteSessionStore {
    /// Bounded, indexed discovery. Running jobs become eligible only at lease expiry.
    pub fn ready_jobs(&self, now_micros: u64, limit: u32) -> Result<Vec<ReadyJob>, StoreError> {
        if limit == 0 || limit > self.job_limits.total {
            return Err(StoreError::Protocol(ProtocolError::limit(
                "invalid job discovery limit",
            )));
        }
        let connection = self.connect()?;
        let mut query = connection.prepare("SELECT session_id, execution_key, principal FROM pipestream_jobs WHERE ready_at_micros <= ?1 ORDER BY ready_at_micros, enqueued_at_micros, session_id, execution_key LIMIT ?2")?;
        let rows = query.query_map(params![timestamp(now_micros)?, limit], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, Vec<u8>>(2)?,
            ))
        })?;
        rows.map(|row| {
            let (session_id, key, principal) = row?;
            Ok(ReadyJob {
                session_id,
                key: postcard::from_bytes(&key)
                    .map_err(|error| StoreError::Corrupt(format!("job index key: {error}")))?,
                principal: postcard::from_bytes(&principal).map_err(|error| {
                    StoreError::Corrupt(format!("job index principal: {error}"))
                })?,
            })
        })
        .collect()
    }

    pub fn unfinished_job_count(&self) -> Result<u32, StoreError> {
        Ok(self
            .connect()?
            .query_row("SELECT count(*) FROM pipestream_jobs", [], |row| row.get(0))?)
    }
}
