//! Durable quotas for serialized session state. These are not filesystem quotas.

use super::*;
use crate::authorization::PrincipalBinding;
use std::collections::BTreeMap;

mod completion;
pub(super) use completion::reserved_bytes as completion_reservation;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS pipestream_storage_limits (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    version INTEGER NOT NULL CHECK (version = 4),
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
    image BLOB NOT NULL CHECK (length(image) = 56)
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
    /// Allocated state capacity, including padding retained after publication.
    pub allocated_state_bytes: u64,
    pub sessions: u32,
}

impl StorageUsage {
    /// Capacity stays charged even after unused logical growth is released.
    pub fn charged_bytes(self) -> u64 {
        (self.state_bytes + self.completion_reserved_bytes).max(self.allocated_state_bytes)
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
        schema::verify(&transaction, SCHEMA)?;
        transaction.commit()?;
        return Ok(retained);
    }
    transaction.execute_batch(SCHEMA)?;
    let initial = requested.unwrap_or_default();
    transaction.execute(
        "INSERT OR IGNORE INTO pipestream_storage_limits VALUES (1, 4, ?1, ?2, ?3, ?4, ?5, ?6)",
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
    if version != 4 {
        return Err(StoreError::Corrupt(
            "unsupported storage reservation policy version".into(),
        ));
    }
    let (total, principal, record, sessions, principal_sessions, yield_token_bytes) = connection.query_row(
        "SELECT total_bytes, principal_bytes, record_bytes, sessions, principal_sessions, yield_token_bytes FROM pipestream_storage_limits WHERE singleton = 1 AND version = 4",
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
    allocated: usize,
    state_checksum: &[u8],
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"pipestream-state-charge-v4");
    digest.update((id.len() as u64).to_be_bytes());
    digest.update(id.as_bytes());
    digest.update((principal.len() as u64).to_be_bytes());
    digest.update(principal);
    digest.update((bytes as u64).to_be_bytes());
    digest.update((reserved as u64).to_be_bytes());
    digest.update((allocated as u64).to_be_bytes());
    digest.update(state_checksum);
    digest.finalize().into()
}

#[derive(Debug)]
struct Charge {
    bytes: u64,
    reserved: u64,
    allocated: u64,
    checksum: [u8; 32],
}

impl Charge {
    fn read(bytes: &[u8]) -> Result<Self, StoreError> {
        if bytes.len() != 56 {
            return Err(StoreError::Corrupt(
                "invalid fixed accounting image length".into(),
            ));
        }
        let number = |offset: usize| {
            let mut value = [0; 8];
            value.copy_from_slice(&bytes[offset..offset + 8]);
            u64::from_be_bytes(value)
        };
        let mut checksum = [0; 32];
        checksum.copy_from_slice(&bytes[24..]);
        let charge = Self {
            bytes: number(0),
            reserved: number(8),
            allocated: number(16),
            checksum,
        };
        if charge.bytes == 0
            || charge.allocated > 16 << 20
            || charge
                .bytes
                .checked_add(charge.reserved)
                .is_none_or(|sum| sum > charge.allocated)
        {
            return Err(StoreError::Corrupt("invalid allocated state charge".into()));
        }
        Ok(charge)
    }

    fn image(&self) -> [u8; 56] {
        let mut image = [0; 56];
        image[..8].copy_from_slice(&self.bytes.to_be_bytes());
        image[8..16].copy_from_slice(&self.reserved.to_be_bytes());
        image[16..24].copy_from_slice(&self.allocated.to_be_bytes());
        image[24..].copy_from_slice(&self.checksum);
        image
    }

    fn add_to(&self, usage: &mut StorageUsage) -> Result<(), StoreError> {
        let invalid = || StoreError::Corrupt("storage accounting overflow".into());
        usage.state_bytes = usage
            .state_bytes
            .checked_add(self.bytes)
            .ok_or_else(invalid)?;
        usage.completion_reserved_bytes = usage
            .completion_reserved_bytes
            .checked_add(self.reserved)
            .ok_or_else(invalid)?;
        usage.allocated_state_bytes = usage
            .allocated_state_bytes
            .checked_add(self.allocated)
            .ok_or_else(invalid)?;
        usage.sessions = usage.sessions.checked_add(1).ok_or_else(invalid)?;
        Ok(())
    }
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
    let mut query = connection.prepare(
        "SELECT CASE WHEN length(image) = 56 THEN image ELSE NULL END
         FROM pipestream_storage_sessions WHERE (?1 IS NULL OR principal = ?1)
         AND (?2 IS NULL OR session_id != ?2)",
    )?;
    let mut rows = query.query(params![key, session])?;
    let mut usage = StorageUsage::default();
    while let Some(row) = rows.next()? {
        let image: Option<Vec<u8>> = row.get(0)?;
        let image =
            image.ok_or_else(|| StoreError::Corrupt("invalid accounting image length".into()))?;
        Charge::read(&image)?.add_to(&mut usage)?;
    }
    Ok(usage)
}

pub(super) fn replace_index(
    connection: &Connection,
    session: &Session,
    state_bytes: usize,
    state_checksum: &[u8; 32],
) -> Result<(), StoreError> {
    let limits = read_limits(connection)?;
    let reserved = completion::reserved_bytes(session, limits)?;
    let required = state_bytes
        .checked_add(reserved)
        .ok_or_else(|| limit("completion charge overflow"))?;
    let header = image::header(connection, &session.session_id, limits.record_bytes)?
        .ok_or_else(|| StoreError::Corrupt("charged session image is absent".into()))?;
    let allocated = header.capacity;
    let principal = session.owner.as_ref().map(|owner| &owner.binding);
    let global = usage_excluding(connection, None, Some(&session.session_id))?;
    let owner = usage_excluding(connection, Some(principal), Some(&session.session_id))?;
    if required > allocated
        || allocated > limits.record_bytes
        || global
            .charged_bytes()
            .checked_add(allocated as u64)
            .is_none_or(|bytes| bytes > limits.total_bytes)
        || owner
            .charged_bytes()
            .checked_add(allocated as u64)
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
        allocated,
        state_checksum,
    );
    let image = Charge {
        bytes: state_bytes as u64,
        reserved: reserved as u64,
        allocated: allocated as u64,
        checksum,
    }
    .image();
    let previous = connection
        .query_row(
            "SELECT rowid, principal, image FROM pipestream_storage_sessions WHERE session_id = ?1",
            [&session.session_id],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            },
        )
        .optional()?;
    if previous
        .as_ref()
        .is_some_and(|old| old.1 == principal && old.2 == image)
    {
        return Ok(());
    }
    if let Some(previous) = previous {
        if previous.1 != principal {
            return Err(StoreError::Protocol(ProtocolError::entity(
                "retained accounting principal changed",
            )));
        }
        let mut blob = connection.blob_open(
            "main",
            "pipestream_storage_sessions",
            "image",
            previous.0,
            false,
        )?;
        if blob.len() != image.len() {
            return Err(StoreError::Corrupt(
                "invalid accounting image length".into(),
            ));
        }
        blob.write_at(&image, 0)?;
        blob.close()?;
    } else {
        connection.execute(
            "INSERT INTO pipestream_storage_sessions VALUES (?1, ?2, ?3)",
            params![session.session_id, principal, image.as_slice()],
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
    let principal = principal_key(session.owner.as_ref().map(|owner| &owner.binding))?;
    let limits = read_limits(connection)?;
    let header = image::header(connection, &session.session_id, limits.record_bytes)?
        .ok_or_else(|| StoreError::Corrupt("charged session image is absent".into()))?;
    let reserved = completion::reserved_bytes(session, limits)
        .map_err(|error| StoreError::Corrupt(error.to_string()))?;
    let checksum = charge_checksum(
        &session.session_id,
        &principal,
        bytes,
        reserved,
        header.capacity,
        state_checksum,
    );
    let expected = Charge {
        bytes: bytes as u64,
        reserved: reserved as u64,
        allocated: header.capacity as u64,
        checksum,
    }
    .image();
    let matches: Option<bool> = connection.query_row(
        "SELECT coalesce(principal = ?2 AND image = ?3, 0) FROM pipestream_storage_sessions WHERE session_id = ?1",
        params![session.session_id, principal, expected.as_slice()], |r| r.get(0),
    ).optional()?;
    if matches != Some(true) {
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
    if global.sessions != actual
        || global.sessions > limits.sessions
        || global.charged_bytes() > limits.total_bytes
    {
        return Err(StoreError::Corrupt(
            "storage accounting exceeds policy or has extra rows".into(),
        ));
    }
    let mut query = connection.prepare(
        "SELECT CASE WHEN length(CAST(s.session_id AS BLOB)) BETWEEN 1 AND 128 THEN s.session_id ELSE NULL END,
         CASE WHEN length(a.principal) BETWEEN 1 AND 261 THEN a.principal ELSE NULL END,
         CASE WHEN length(a.image) = 56 THEN a.image ELSE NULL END
         FROM pipestream_sessions s LEFT JOIN pipestream_storage_sessions a USING(session_id)",
    )?;
    let mut rows = query.query([])?;
    let mut owners = BTreeMap::<Vec<u8>, StorageUsage>::new();
    while let Some(row) = rows.next()? {
        let id = schema::session_id(row, 0)?;
        let header = image::header(connection, &id, limits.record_bytes)?.ok_or_else(|| {
            StoreError::Corrupt("session disappeared during accounting audit".into())
        })?;
        let principal: Option<Vec<u8>> = row.get(1)?;
        let image: Option<Vec<u8>> = row.get(2)?;
        let charge =
            Charge::read(&image.ok_or_else(|| {
                StoreError::Corrupt("session lacks fixed accounting image".into())
            })?)?;
        let Some(principal) = principal else {
            return Err(StoreError::Corrupt(
                "session lacks storage accounting".into(),
            ));
        };
        if charge.bytes != header.state_bytes as u64
            || charge.allocated != header.capacity as u64
            || charge.checksum
                != charge_checksum(
                    &id,
                    &principal,
                    header.state_bytes,
                    charge.reserved as usize,
                    header.capacity,
                    &header.checksum,
                )
        {
            return Err(StoreError::Corrupt(
                "storage accounting checksum or length differs from session".into(),
            ));
        }
        let owner = owners.entry(principal).or_default();
        charge.add_to(owner)?;
        if owner.sessions > limits.principal_sessions
            || owner.charged_bytes() > limits.principal_bytes
        {
            return Err(StoreError::Corrupt(
                "principal storage accounting exceeds policy".into(),
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
