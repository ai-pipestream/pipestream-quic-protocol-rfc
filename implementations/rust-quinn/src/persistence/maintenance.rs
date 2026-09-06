//! A writer-locked snapshot for offline, paired payload-store maintenance.

use super::*;
use crate::execution::{ExecutionKey, ExecutionStage};

/// Prevents admission and result publication while an exclusively owned payload
/// store audits its inputs and reclaims orphans. It never mutates lifecycle rows.
///
/// This is an offline storage operation, not an application execution context.
/// Callers must acquire exclusive payload-root ownership first, retain it until
/// this guard is dropped, and audit every session before removing anything.
/// Filesystem changes are not undone by SQLite rollback. Interrupted maintenance
/// must be replayable from durable file commitments.
pub struct PayloadMaintenance {
    connection: Connection,
    after: Option<String>,
    exhausted: bool,
}

impl SqliteSessionStore {
    /// Start maintenance against an already bound database/root pair. No pairing
    /// is created or repaired here. The SQLite writer lock lasts for the guard's
    /// entire lifetime, including after the session iterator reaches its end.
    pub fn payload_maintenance(
        &self,
        expected: PayloadBinding,
    ) -> Result<PayloadMaintenance, StoreError> {
        let connection = self.connect()?;
        connection.execute_batch("BEGIN IMMEDIATE")?;
        let guard = PayloadMaintenance {
            connection,
            after: None,
            exhausted: false,
        };
        if expected.payloads().is_none() || binding::read(&guard.connection)? != expected {
            return Err(StoreError::Protocol(ProtocolError::entity(
                "payload maintenance requires the retained database/root pair",
            )));
        }
        queue::verify_index(&guard.connection)?;
        storage::verify_index(&guard.connection)?;
        Ok(guard)
    }
}

impl PayloadMaintenance {
    /// Read and validate one session, including finished and refused inputs.
    /// The iterator retains only a bounded session-ID cursor; session decoding
    /// still requires memory proportional to the configured per-record limit.
    /// Caller-managed admission without an original PROCESS descriptor refuses:
    /// absence of a dispatch row is not evidence that its input is an orphan.
    pub fn next_session(&mut self) -> Result<Option<VersionedSession>, StoreError> {
        if self.exhausted {
            return Ok(None);
        }
        // The non-null empty string is below every valid session ID. Keeping a
        // simple range predicate lets SQLite seek instead of rescanning the
        // already audited prefix for each session.
        let mut statement = self.connection.prepare(
            "SELECT CASE WHEN length(CAST(session_id AS BLOB)) BETWEEN 1 AND 128
             THEN session_id ELSE NULL END FROM pipestream_sessions
             WHERE session_id > ?1 ORDER BY session_id LIMIT 1",
        )?;
        let mut rows = statement.query([self.after.as_deref().unwrap_or("")])?;
        let Some(row) = rows.next()? else {
            self.exhausted = true;
            return Ok(None);
        };
        let id = schema::session_id(row, 0)?;
        let retained = load_from(&self.connection, &id)?
            .ok_or_else(|| StoreError::Corrupt("maintenance session disappeared".into()))?;
        for entity in retained.session.entities.keys() {
            if !retained.session.jobs.contains_key(&ExecutionKey {
                entity: *entity,
                stage: ExecutionStage::Process,
            }) {
                return Err(StoreError::Protocol(ProtocolError::entity(
                    "payload maintenance refuses caller-managed admitted input",
                )));
            }
        }
        for (key, job) in &retained.session.jobs {
            retained
                .session
                .validate_job_input(*key, &job.input)
                .map_err(|error| StoreError::Corrupt(error.to_string()))?;
        }
        self.after = Some(id);
        Ok(Some(retained))
    }
}

impl Drop for PayloadMaintenance {
    fn drop(&mut self) {
        // No database writes are performed. Connection drop is also a rollback
        // fallback if SQLite refuses this explicit release.
        let _ = self.connection.execute_batch("ROLLBACK");
    }
}

#[cfg(test)]
mod tests;
