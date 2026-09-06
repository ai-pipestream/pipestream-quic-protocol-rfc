use super::*;
use crate::{
    authorization::PrincipalBinding,
    execution::{ExecutionKey, ExecutionStage},
    jobs::JobState,
};
use std::collections::BTreeMap;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS pipestream_job_limits (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    version INTEGER NOT NULL CHECK (version = 2),
    total INTEGER NOT NULL CHECK (total BETWEEN 1 AND 65536),
    per_principal INTEGER NOT NULL CHECK (per_principal BETWEEN 1 AND total),
    rehydration_total INTEGER NOT NULL CHECK (rehydration_total BETWEEN 1 AND 65536),
    rehydration_per_principal INTEGER NOT NULL CHECK (rehydration_per_principal BETWEEN 1 AND rehydration_total)
) STRICT;
CREATE TABLE IF NOT EXISTS pipestream_jobs (
    session_id TEXT NOT NULL REFERENCES pipestream_sessions(session_id),
    execution_key BLOB NOT NULL,
    principal BLOB NOT NULL,
    ready_at_micros INTEGER,
    enqueued_at_micros INTEGER NOT NULL,
    rehydration INTEGER NOT NULL CHECK (rehydration IN (0, 1)),
    reserved INTEGER NOT NULL CHECK (reserved IN (0, 1) AND (reserved = 0 OR (rehydration = 1 AND ready_at_micros IS NULL))),
    PRIMARY KEY (session_id, execution_key)
) STRICT;
CREATE INDEX IF NOT EXISTS pipestream_jobs_ready ON pipestream_jobs
    (ready_at_micros, enqueued_at_micros, session_id, execution_key);
CREATE INDEX IF NOT EXISTS pipestream_jobs_principal ON pipestream_jobs (principal);
";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobQueueLimits {
    /// Queued plus running PROCESS/RESUME jobs, including revoked work.
    pub total: u32,
    pub per_principal: u32,
    /// Future plus queued/running REHYDRATE slots, separate from ordinary work.
    pub rehydration_total: u32,
    pub rehydration_per_principal: u32,
}

impl Default for JobQueueLimits {
    fn default() -> Self {
        Self {
            total: 128,
            per_principal: 32,
            rehydration_total: 65_536,
            rehydration_per_principal: 16_384,
        }
    }
}

impl JobQueueLimits {
    pub(super) fn validate(self) -> Result<(), StoreError> {
        if self.total == 0
            || self.total > 65_536
            || self.per_principal == 0
            || self.per_principal > self.total
            || self.rehydration_total == 0
            || self.rehydration_total > 65_536
            || self.rehydration_per_principal == 0
            || self.rehydration_per_principal > self.rehydration_total
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
    if (nonempty && tables != 2) || tables == 1 {
        return Err(StoreError::Corrupt(
            "job queue schema is absent from a nonempty session store".into(),
        ));
    }
    if tables == 2 {
        let versioned: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('pipestream_job_limits') WHERE name = 'version')",
            [], |row| row.get(0),
        )?;
        if !versioned {
            return Err(StoreError::Corrupt(
                "unsupported job reservation policy version".into(),
            ));
        }
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
        read_limits(&transaction)?;
    }
    transaction.execute_batch(SCHEMA)?;
    let initial = requested.unwrap_or_default();
    transaction.execute(
        "INSERT OR IGNORE INTO pipestream_job_limits VALUES (1, 2, ?1, ?2, ?3, ?4)",
        params![
            initial.total,
            initial.per_principal,
            initial.rehydration_total,
            initial.rehydration_per_principal
        ],
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
    let version: u32 = connection.query_row(
        "SELECT version FROM pipestream_job_limits WHERE singleton = 1",
        [],
        |row| row.get(0),
    )?;
    if version != 2 {
        return Err(StoreError::Corrupt(
            "unsupported job reservation policy version".into(),
        ));
    }
    let limits = connection.query_row(
        "SELECT total, per_principal, rehydration_total, rehydration_per_principal FROM pipestream_job_limits WHERE singleton = 1",
        [],
        |row| {
            Ok(JobQueueLimits {
                total: row.get(0)?,
                per_principal: row.get(1)?,
                rehydration_total: row.get(2)?,
                rehydration_per_principal: row.get(3)?,
            })
        },
    )?;
    limits
        .validate()
        .map_err(|error| StoreError::Corrupt(error.to_string()))?;
    Ok(limits)
}

#[derive(Debug, PartialEq, Eq)]
struct IndexEntry {
    ready: Option<i64>,
    enqueued: i64,
    rehydration: bool,
    reserved: bool,
}

fn entries(session: &Session) -> Result<BTreeMap<ExecutionKey, IndexEntry>, StoreError> {
    let mut entries = BTreeMap::new();
    for (key, job) in &session.jobs {
        if job.state.is_unfinished() {
            entries.insert(
                *key,
                IndexEntry {
                    ready: ready_at(session, key)?,
                    enqueued: timestamp(job.enqueued_at_micros)?,
                    rehydration: key.stage == ExecutionStage::Rehydrate,
                    reserved: false,
                },
            );
        }
    }
    for (key, process) in session.future_rehydrations() {
        entries.insert(
            key,
            IndexEntry {
                ready: None,
                enqueued: timestamp(process.enqueued_at_micros)?,
                rehydration: true,
                reserved: true,
            },
        );
    }
    Ok(entries)
}

/// Queue slots, including future rehydration held by waiting parents.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct JobQueueUsage {
    pub ordinary: u32,
    pub rehydration_reserved: u32,
    pub rehydration_active: u32,
}

fn usage(connection: &Connection, principal: Option<&[u8]>) -> Result<JobQueueUsage, StoreError> {
    usage_excluding(connection, principal, None)
}

fn usage_excluding(
    connection: &Connection,
    principal: Option<&[u8]>,
    session: Option<&str>,
) -> Result<JobQueueUsage, StoreError> {
    Ok(connection.query_row(
        "SELECT coalesce(sum(rehydration = 0), 0), coalesce(sum(reserved = 1), 0),
         coalesce(sum(rehydration = 1 AND reserved = 0), 0)
         FROM pipestream_jobs WHERE (?1 IS NULL OR principal = ?1)
         AND (?2 IS NULL OR session_id != ?2)",
        params![principal, session],
        |row| {
            Ok(JobQueueUsage {
                ordinary: row.get(0)?,
                rehydration_reserved: row.get(1)?,
                rehydration_active: row.get(2)?,
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
        if session.owner.as_ref().is_some_and(|owner| owner.revoked)
            || matches!(key.stage, crate::execution::ExecutionStage::Resume { claim_id } if session.revoked_claims.contains(&claim_id))
        {
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
        for (key, entry) in entries(&session)? {
            // Compare raw integer flags: bool decoding would coerce corrupt 2 to
            // true while SQL capacity sums using '= 1' would omit that row.
            let actual = connection.query_row("SELECT principal, ready_at_micros, enqueued_at_micros, rehydration, reserved FROM pipestream_jobs WHERE session_id = ?1 AND execution_key = ?2", params![id, encode(&key)?], |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Option<i64>>(1)?, row.get::<_, i64>(2)?, row.get::<_, i64>(3)?, row.get::<_, i64>(4)?))).optional()?;
            if actual
                != Some((
                    principal.clone(),
                    entry.ready,
                    entry.enqueued,
                    i64::from(entry.rehydration),
                    i64::from(entry.reserved),
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
    let global = usage(connection, None)?;
    let oversized: u32 = connection.query_row("SELECT count(*) FROM (SELECT principal FROM pipestream_jobs GROUP BY principal HAVING sum(rehydration = 0) > ?1 OR sum(rehydration = 1) > ?2)", params![limits.per_principal, limits.rehydration_per_principal], |row| row.get(0))?;
    if expected != u64::from(actual)
        || global.ordinary > limits.total
        || global.rehydration_reserved + global.rehydration_active > limits.rehydration_total
        || oversized != 0
    {
        return Err(StoreError::Corrupt(
            "job index has extra rows or exceeds stored limits".into(),
        ));
    }
    Ok(())
}

pub(super) fn replace_index(connection: &Connection, session: &Session) -> Result<(), StoreError> {
    let principal = encode(&session.owner.as_ref().map(|owner| &owner.binding))?;
    let entries = entries(session)?;
    let limits = read_limits(connection)?;
    let total = usage_excluding(connection, None, Some(&session.session_id))?;
    let owned = usage_excluding(connection, Some(&principal), Some(&session.session_id))?;
    let ordinary = entries.values().filter(|entry| !entry.rehydration).count() as u64;
    let rehydration = entries.len() as u64 - ordinary;
    if u64::from(total.ordinary) + ordinary > u64::from(limits.total)
        || u64::from(owned.ordinary) + ordinary > u64::from(limits.per_principal)
        || u64::from(total.rehydration_reserved + total.rehydration_active) + rehydration
            > u64::from(limits.rehydration_total)
        || u64::from(owned.rehydration_reserved + owned.rehydration_active) + rehydration
            > u64::from(limits.rehydration_per_principal)
    {
        return Err(StoreError::Protocol(ProtocolError::limit(
            "durable job queue is full",
        )));
    }
    // Callers audit the old index before changing authoritative session state.
    // Keep unchanged rows and their rowids; only actual state changes touch the
    // queue B-trees. In particular, one lease does not rewrite every sibling.
    let mut query = connection.prepare(
        "SELECT execution_key, principal, ready_at_micros, enqueued_at_micros, rehydration, reserved
         FROM pipestream_jobs WHERE session_id = ?1",
    )?;
    let existing = query
        .query_map([&session.session_id], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                (
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                ),
            ))
        })?
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let desired = entries
        .into_iter()
        .map(|(key, entry)| Ok((encode(&key)?, entry)))
        .collect::<Result<BTreeMap<_, _>, StoreError>>()?;
    let mut delete = connection
        .prepare("DELETE FROM pipestream_jobs WHERE session_id = ?1 AND execution_key = ?2")?;
    let mut update = connection.prepare(
        "UPDATE pipestream_jobs SET ready_at_micros = ?3, enqueued_at_micros = ?4,
         rehydration = ?5, reserved = ?6 WHERE session_id = ?1 AND execution_key = ?2",
    )?;
    let mut update_principal = connection.prepare(
        "UPDATE pipestream_jobs SET principal = ?3 WHERE session_id = ?1 AND execution_key = ?2",
    )?;
    let mut insert =
        connection.prepare("INSERT INTO pipestream_jobs VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)")?;
    // Release obsolete rows first so replacement work can reuse their pages.
    for key in existing.keys().filter(|key| !desired.contains_key(*key)) {
        delete.execute(params![session.session_id, key])?;
    }
    for (key, entry) in desired {
        let old = existing.get(&key);
        if old.is_some_and(|old| {
            old.0 == principal
                && old.1 == entry.ready
                && old.2 == entry.enqueued
                && old.3 == i64::from(entry.rehydration)
                && old.4 == i64::from(entry.reserved)
        }) {
            continue;
        }
        if let Some(old) = old {
            update.execute(params![
                session.session_id,
                key,
                entry.ready,
                entry.enqueued,
                entry.rehydration,
                entry.reserved
            ])?;
            if old.0 != principal {
                update_principal.execute(params![session.session_id, key, principal])?;
            }
        } else {
            insert.execute(params![
                session.session_id,
                key,
                principal,
                entry.ready,
                entry.enqueued,
                entry.rehydration,
                entry.reserved
            ])?;
        }
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
        // A larger completion queue must not hide every other principal behind
        // one principal's physical worker limit. Each page interleaves owners.
        let mut query = connection.prepare("SELECT session_id, execution_key, principal FROM (
            SELECT session_id, execution_key, principal, ready_at_micros, enqueued_at_micros,
                   row_number() OVER (PARTITION BY principal ORDER BY rehydration DESC,
                       ready_at_micros, enqueued_at_micros, session_id, execution_key) AS ordinal
            FROM pipestream_jobs WHERE reserved = 0 AND ready_at_micros <= ?1
        ) ORDER BY ordinal, ready_at_micros, enqueued_at_micros, session_id, execution_key LIMIT ?2")?;
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
        Ok(self.connect()?.query_row(
            "SELECT count(*) FROM pipestream_jobs WHERE reserved = 0",
            [],
            |row| row.get(0),
        )?)
    }

    /// Audit authoritative state and report actual/future slots in one snapshot.
    pub fn job_queue_usage(&self) -> Result<JobQueueUsage, StoreError> {
        let mut connection = self.connect()?;
        let transaction = connection.transaction()?;
        verify_index(&transaction)?;
        let result = usage(&transaction, None)?;
        transaction.commit()?;
        Ok(result)
    }
}
