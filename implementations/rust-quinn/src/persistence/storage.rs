//! Durable quotas for serialized session state. These are not filesystem quotas.

use super::*;
use crate::authorization::PrincipalBinding;

mod completion;
pub(super) use completion::reserved_bytes as completion_reservation;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS pipestream_storage_limits (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    version INTEGER NOT NULL CHECK (version = 3),
    total_bytes INTEGER NOT NULL CHECK (total_bytes > 0),
    principal_bytes INTEGER NOT NULL CHECK (principal_bytes > 0),
    record_bytes INTEGER NOT NULL CHECK (record_bytes > 0),
    sessions INTEGER NOT NULL CHECK (sessions > 0),
    principal_sessions INTEGER NOT NULL CHECK (principal_sessions > 0),
    yield_token_bytes INTEGER NOT NULL CHECK (yield_token_bytes BETWEEN 1 AND 16777215)
) STRICT;
CREATE TABLE IF NOT EXISTS pipestream_storage_sessions (
    session_id TEXT PRIMARY KEY REFERENCES pipestream_sessions(session_id),
    principal BLOB NOT NULL,
    state_bytes INTEGER NOT NULL CHECK (state_bytes > 0),
    completion_bytes INTEGER NOT NULL CHECK (completion_bytes >= 0),
    checksum BLOB NOT NULL CHECK (length(checksum) = 32)
) STRICT;
CREATE INDEX IF NOT EXISTS pipestream_storage_principal ON
    pipestream_storage_sessions(principal);
";

/// Serialized-state and retained-session limits, including finished and revoked work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageLimits {
    pub total_bytes: u64,
    pub principal_bytes: u64,
    pub record_bytes: usize,
    pub sessions: u32,
    pub principal_sessions: u32,
    /// Maximum continuation-token bytes a processing callback may retain.
    /// Jobs negotiating Layer 2 reserve this capacity before execution.
    pub yield_token_bytes: usize,
}

impl Default for StorageLimits {
    fn default() -> Self {
        Self {
            total_bytes: 128 << 20,
            principal_bytes: 32 << 20,
            record_bytes: 8 << 20,
            sessions: 4096,
            principal_sessions: 1024,
            yield_token_bytes: 64 << 10,
        }
    }
}

impl StorageLimits {
    pub(super) fn validate(self) -> Result<(), StoreError> {
        if self.total_bytes == 0
            || self.total_bytes > 16 << 30
            || self.principal_bytes == 0
            || self.principal_bytes > self.total_bytes
            || self.record_bytes == 0
            || self.record_bytes > 16 << 20
            || self.record_bytes as u64 > self.principal_bytes
            || self.sessions == 0
            || self.sessions > 65_536
            || self.principal_sessions == 0
            || self.principal_sessions > self.sessions
            || self.yield_token_bytes == 0
            || self.yield_token_bytes > 0x00ff_ffff
        {
            return Err(limit("invalid durable storage limits"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StorageUsage {
    pub state_bytes: u64,
    pub completion_reserved_bytes: u64,
    pub sessions: u32,
}

impl StorageUsage {
    /// Serialized bytes plus the protected growth of already-admitted jobs.
    pub fn charged_bytes(self) -> u64 {
        self.state_bytes + self.completion_reserved_bytes
    }
}

fn limit(detail: &str) -> StoreError {
    StoreError::Protocol(ProtocolError::limit(detail))
}

pub(super) fn initialize(
    connection: &mut Connection,
    requested: Option<StorageLimits>,
) -> Result<StorageLimits, StoreError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let nonempty: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM pipestream_sessions)",
        [],
        |row| row.get(0),
    )?;
    let tables: u32 = transaction.query_row(
        "SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name IN ('pipestream_storage_limits', 'pipestream_storage_sessions')",
        [], |row| row.get(0),
    )?;
    if (nonempty && tables != 2) || tables == 1 {
        return Err(StoreError::Corrupt(
            "storage accounting is absent from a nonempty session store".into(),
        ));
    }
    if tables == 2 {
        // Do not recreate a missing policy with more permissive defaults.
        let retained = read_limits(&transaction)?;
        if requested.is_some_and(|value| value != retained) {
            return Err(limit("durable storage limits differ from stored policy"));
        }
    }
    transaction.execute_batch(SCHEMA)?;
    let initial = requested.unwrap_or_default();
    transaction.execute(
        "INSERT OR IGNORE INTO pipestream_storage_limits VALUES (1, 3, ?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            initial.total_bytes as i64,
            initial.principal_bytes as i64,
            initial.record_bytes as i64,
            initial.sessions,
            initial.principal_sessions,
            initial.yield_token_bytes as i64
        ],
    )?;
    let retained = read_limits(&transaction)?;
    if requested.is_some_and(|value| value != retained) {
        return Err(limit("durable storage limits differ from stored policy"));
    }
    transaction.commit()?;
    Ok(retained)
}

pub(super) fn read_limits(connection: &Connection) -> Result<StorageLimits, StoreError> {
    let version: u32 = connection.query_row(
        "SELECT version FROM pipestream_storage_limits WHERE singleton = 1",
        [],
        |row| row.get(0),
    )?;
    if version != 3 {
        return Err(StoreError::Corrupt(
            "unsupported storage reservation policy version".into(),
        ));
    }
    let (total, principal, record, sessions, principal_sessions, yield_token_bytes) = connection.query_row(
        "SELECT total_bytes, principal_bytes, record_bytes, sessions, principal_sessions, yield_token_bytes FROM pipestream_storage_limits WHERE singleton = 1 AND version = 3",
        [], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?,
            row.get::<_, u32>(2)?, row.get::<_, u32>(3)?, row.get::<_, u32>(4)?, row.get::<_, u32>(5)?)),
    )?;
    let limits = StorageLimits {
        total_bytes: nonnegative(total)?,
        principal_bytes: nonnegative(principal)?,
        record_bytes: record as usize,
        sessions,
        principal_sessions,
        yield_token_bytes: yield_token_bytes as usize,
    };
    limits
        .validate()
        .map_err(|e| StoreError::Corrupt(e.to_string()))?;
    Ok(limits)
}

fn principal_key(principal: Option<&PrincipalBinding>) -> Result<Vec<u8>, StoreError> {
    postcard::to_stdvec(&principal).map_err(|error| StoreError::Codec(error.to_string()))
}

fn nonnegative(value: i64) -> Result<u64, StoreError> {
    u64::try_from(value)
        .map_err(|_| StoreError::Corrupt("negative storage accounting value".into()))
}

fn charge_checksum(
    id: &str,
    principal: &[u8],
    bytes: usize,
    reserved: usize,
    state_checksum: &[u8],
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"pipestream-state-charge-v3");
    digest.update((id.len() as u64).to_be_bytes());
    digest.update(id.as_bytes());
    digest.update((principal.len() as u64).to_be_bytes());
    digest.update(principal);
    digest.update((bytes as u64).to_be_bytes());
    digest.update((reserved as u64).to_be_bytes());
    digest.update(state_checksum);
    digest.finalize().into()
}

pub(super) fn usage(
    connection: &Connection,
    principal: Option<Option<&PrincipalBinding>>,
) -> Result<StorageUsage, StoreError> {
    usage_excluding(connection, principal, None)
}

fn usage_excluding(
    connection: &Connection,
    principal: Option<Option<&PrincipalBinding>>,
    session: Option<&str>,
) -> Result<StorageUsage, StoreError> {
    let key = principal.map(principal_key).transpose()?;
    let (bytes, reserved, sessions) = connection.query_row(
        "SELECT coalesce(sum(state_bytes), 0), coalesce(sum(completion_bytes), 0), count(*)
         FROM pipestream_storage_sessions WHERE (?1 IS NULL OR principal = ?1)
         AND (?2 IS NULL OR session_id != ?2)",
        params![key, session],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, u32>(2)?,
            ))
        },
    )?;
    Ok(StorageUsage {
        state_bytes: nonnegative(bytes)?,
        completion_reserved_bytes: nonnegative(reserved)?,
        sessions,
    })
}

pub(super) fn replace_index(
    connection: &Connection,
    session: &Session,
    state_bytes: usize,
    state_checksum: &[u8; 32],
) -> Result<(), StoreError> {
    let limits = read_limits(connection)?;
    let reserved = completion::reserved_bytes(session, limits)?;
    let charge = state_bytes
        .checked_add(reserved)
        .ok_or_else(|| limit("completion charge overflow"))?;
    let principal = session.owner.as_ref().map(|owner| &owner.binding);
    let global = usage_excluding(connection, None, Some(&session.session_id))?;
    let owner = usage_excluding(connection, Some(principal), Some(&session.session_id))?;
    if charge > limits.record_bytes
        || global
            .charged_bytes()
            .checked_add(charge as u64)
            .is_none_or(|bytes| bytes > limits.total_bytes)
        || owner
            .charged_bytes()
            .checked_add(charge as u64)
            .is_none_or(|bytes| bytes > limits.principal_bytes)
        || global.sessions >= limits.sessions
        || owner.sessions >= limits.principal_sessions
    {
        return Err(limit("durable session byte or count budget exhausted"));
    }
    let principal = principal_key(principal)?;
    let checksum = charge_checksum(
        &session.session_id,
        &principal,
        state_bytes,
        reserved,
        state_checksum,
    );
    let previous = connection.query_row(
        "SELECT principal, state_bytes, completion_bytes, checksum FROM pipestream_storage_sessions WHERE session_id = ?1",
        [&session.session_id],
        |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?, row.get::<_, Vec<u8>>(3)?)),
    ).optional()?;
    if previous.as_ref().is_some_and(|old| {
        old.0 == principal
            && old.1 == state_bytes as i64
            && old.2 == reserved as i64
            && old.3.as_slice() == checksum
    }) {
        return Ok(());
    }
    if let Some(previous) = previous {
        connection.execute(
            "UPDATE pipestream_storage_sessions SET state_bytes = ?2,
            completion_bytes = ?3, checksum = ?4 WHERE session_id = ?1",
            params![
                session.session_id,
                state_bytes as i64,
                reserved as i64,
                checksum.as_slice()
            ],
        )?;
        if previous.0 != principal {
            connection.execute(
                "UPDATE pipestream_storage_sessions SET principal = ?2 WHERE session_id = ?1",
                params![session.session_id, principal],
            )?;
        }
    } else {
        connection.execute(
            "INSERT INTO pipestream_storage_sessions VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                session.session_id,
                principal,
                state_bytes as i64,
                reserved as i64,
                checksum.as_slice()
            ],
        )?;
    }
    Ok(())
}

pub(super) fn validate_entry(
    connection: &Connection,
    session: &Session,
    bytes: usize,
    state_checksum: &[u8; 32],
) -> Result<(), StoreError> {
    let stored = connection
        .query_row(
            "SELECT principal, state_bytes, completion_bytes, checksum FROM pipestream_storage_sessions WHERE session_id = ?1",
            [&session.session_id],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?, row.get::<_, Vec<u8>>(3)?)),
        )
        .optional()?;
    let principal = principal_key(session.owner.as_ref().map(|owner| &owner.binding))?;
    let reserved = completion::reserved_bytes(session, read_limits(connection)?)
        .map_err(|error| StoreError::Corrupt(error.to_string()))?;
    let checksum = charge_checksum(
        &session.session_id,
        &principal,
        bytes,
        reserved,
        state_checksum,
    );
    if stored != Some((principal, bytes as i64, reserved as i64, checksum.to_vec())) {
        return Err(StoreError::Corrupt(
            "storage accounting differs from retained session state".into(),
        ));
    }
    Ok(())
}

pub(super) fn verify_index(connection: &Connection) -> Result<(), StoreError> {
    let limits = read_limits(connection)?;
    let global = usage(connection, None)?;
    let actual: u32 =
        connection.query_row("SELECT count(*) FROM pipestream_sessions", [], |r| r.get(0))?;
    let oversized: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM pipestream_storage_sessions GROUP BY principal HAVING sum(state_bytes + completion_bytes) > ?1 OR count(*) > ?2)",
        params![limits.principal_bytes as i64, limits.principal_sessions], |r| r.get(0),
    )?;
    if global.sessions != actual
        || global.sessions > limits.sessions
        || global.charged_bytes() > limits.total_bytes
        || oversized
    {
        return Err(StoreError::Corrupt(
            "storage accounting exceeds policy or has extra rows".into(),
        ));
    }
    let mut query = connection.prepare(
        "SELECT s.session_id, length(s.state), s.checksum, a.principal, a.state_bytes, a.checksum, a.completion_bytes
         FROM pipestream_sessions s LEFT JOIN pipestream_storage_sessions a USING(session_id)",
    )?;
    let mut rows = query.query([])?;
    while let Some(row) = rows.next()? {
        let id: String = row.get(0)?;
        let bytes: u32 = row.get(1)?;
        let state_checksum: Vec<u8> = row.get(2)?;
        let principal: Option<Vec<u8>> = row.get(3)?;
        let recorded: Option<i64> = row.get(4)?;
        let checksum: Option<Vec<u8>> = row.get(5)?;
        let reserved: Option<i64> = row.get(6)?;
        let reserved = reserved
            .filter(|value| *value >= 0)
            .ok_or_else(|| StoreError::Corrupt("session lacks completion accounting".into()))?
            as u64;
        let Some(principal) = principal else {
            return Err(StoreError::Corrupt(
                "session lacks storage accounting".into(),
            ));
        };
        if u64::from(bytes)
            .checked_add(reserved)
            .is_none_or(|charge| charge > limits.record_bytes as u64)
            || state_checksum.len() != 32
            || recorded != Some(i64::from(bytes))
            || checksum.as_deref()
                != Some(
                    charge_checksum(
                        &id,
                        &principal,
                        bytes as usize,
                        reserved as usize,
                        &state_checksum,
                    )
                    .as_slice(),
                )
        {
            return Err(StoreError::Corrupt(
                "storage accounting checksum or length differs from session".into(),
            ));
        }
    }
    Ok(())
}

pub(super) fn encode(session: &Session, limit: usize) -> Result<Vec<u8>, StoreError> {
    postcard::serialize_with_flavor(
        session,
        BoundedBytes {
            bytes: Vec::new(),
            limit,
        },
    )
    .map_err(|error| match error {
        postcard::Error::SerializeBufferFull => {
            self::limit("session serialization exceeds record budget")
        }
        other => StoreError::Codec(other.to_string()),
    })
}

struct BoundedBytes {
    bytes: Vec<u8>,
    limit: usize,
}

impl postcard::ser_flavors::Flavor for BoundedBytes {
    type Output = Vec<u8>;

    fn try_push(&mut self, byte: u8) -> postcard::Result<()> {
        self.try_extend(&[byte])
    }

    fn try_extend(&mut self, data: &[u8]) -> postcard::Result<()> {
        let length = self
            .bytes
            .len()
            .checked_add(data.len())
            .filter(|&n| n <= self.limit)
            .ok_or(postcard::Error::SerializeBufferFull)?;
        if length > self.bytes.capacity() {
            let target = length
                .max(self.bytes.capacity().saturating_mul(2))
                .min(self.limit);
            self.bytes
                .try_reserve_exact(target - self.bytes.len())
                .map_err(|_| postcard::Error::SerializeBufferFull)?;
        }
        self.bytes.extend_from_slice(data);
        Ok(())
    }

    fn finalize(self) -> postcard::Result<Vec<u8>> {
        Ok(self.bytes)
    }
}

#[cfg(test)]
mod tests;
