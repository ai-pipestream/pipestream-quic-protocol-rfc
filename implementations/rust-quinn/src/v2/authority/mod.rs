//! Durable processing-authority transactions for the current protocol.
//!
//! The caller supplies already authenticated identity and a live authorization
//! policy. This library does not authenticate a network connection. Persistent
//! identity, declarations and receipts are real SQLite transactions; endpoint
//! activation still requires the complete execution/result/storage contract.

use super::{
    codec::{self, Wire},
    *,
};
use crate::persistence::{GUARDED_VFS, PhysicalGuard, PhysicalLimits, PhysicalUsage};
use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, params,
};
use std::{path::Path, sync::Arc, time::Duration as Elapsed};

#[cfg(unix)]
mod admission;
#[cfg(unix)]
pub mod ingress;
mod jobs;
#[cfg(unix)]
pub mod payload;
mod records;
mod scopes;
mod sessions;
#[cfg(test)]
mod tests;

const APPLICATION_ID: i64 = 1_347_637_825;
const FORMAT: i64 = 6;
const SCHEMA: &str = include_str!("schema.sql");

#[derive(Debug)]
pub enum StoreError {
    Protocol(Error),
    Database(rusqlite::Error),
    Physical(crate::persistence::StoreError),
    Io(std::io::Error),
    Corrupt(&'static str),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Protocol(e) => e.fmt(f),
            Self::Database(e) => write!(f, "authority database: {e}"),
            Self::Physical(e) => e.fmt(f),
            Self::Io(e) => e.fmt(f),
            Self::Corrupt(s) => write!(f, "authority storage invalid: {s}"),
        }
    }
}
impl std::error::Error for StoreError {}
impl From<std::io::Error> for StoreError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<Error> for StoreError {
    fn from(e: Error) -> Self {
        Self::Protocol(e)
    }
}
impl From<rusqlite::Error> for StoreError {
    fn from(e: rusqlite::Error) -> Self {
        if e.sqlite_error_code() == Some(rusqlite::ErrorCode::DiskFull) {
            protocol(
                ErrorCode::LimitExceeded,
                "physical database capacity exhausted",
            )
        } else {
            Self::Database(e)
        }
    }
}
impl From<crate::persistence::StoreError> for StoreError {
    fn from(e: crate::persistence::StoreError) -> Self {
        Self::Physical(e)
    }
}

type Result<T> = std::result::Result<T, StoreError>;
fn protocol(code: ErrorCode, detail: &'static str) -> StoreError {
    Error::new(code, detail).into()
}

/// Permissions are checked under the transaction that exposes or changes state.
/// Policy implementations must be bounded, local, and nonblocking. Network policy
/// refresh belongs outside SQLite; revocation must be visible to this check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Permission {
    Inspect,
    Create,
    Declare,
    Admit,
}

pub trait Authorization: Send + Sync {
    fn permits(&self, owner: &IdentityLabel, permission: Permission) -> bool;
}

/// UTC trust is an operator/environment input. A monotonic process timer alone
/// does not justify timestamps or expiration across power loss.
#[derive(Debug, Clone, Copy)]
pub struct ClockReading {
    pub utc_ms: Number,
    pub trusted: bool,
}
pub trait Clock: Send + Sync {
    fn read(&self) -> ClockReading;
}

/// Persistent admission ceilings. Exhaustion refuses creation/declaration and
/// never evicts existing identities to make room. Session lifetimes are separate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorePolicy {
    pub owners: Id,
    pub sessions: Id,
    pub sessions_per_owner: Id,
    pub active_jobs: Id,
    pub active_jobs_per_owner: Id,
    pub session_limits: Limits,
}

impl Wire for StorePolicy {
    fn read(d: &mut minicbor::Decoder<'_>) -> std::result::Result<Self, Error> {
        codec::array(d, 6)?;
        let value = Self {
            owners: Id::read(d)?,
            sessions: Id::read(d)?,
            sessions_per_owner: Id::read(d)?,
            active_jobs: Id::read(d)?,
            active_jobs_per_owner: Id::read(d)?,
            session_limits: Limits::read(d)?,
        };
        value.check()?;
        Ok(value)
    }
    fn write(&self, w: &mut codec::Writer) {
        w.array(6);
        self.owners.write(w);
        self.sessions.write(w);
        self.sessions_per_owner.write(w);
        self.active_jobs.write(w);
        self.active_jobs_per_owner.write(w);
        self.session_limits.write(w);
    }
    fn check(&self) -> std::result::Result<(), Error> {
        self.owners.check()?;
        self.sessions.check()?;
        self.sessions_per_owner.check()?;
        self.active_jobs.check()?;
        self.active_jobs_per_owner.check()?;
        self.session_limits.check()?;
        require(
            self.sessions_per_owner <= self.sessions
                && self.active_jobs_per_owner <= self.active_jobs,
            "owner limit exceeds global limit",
        )
    }
}

/// A retained session binding. The caller's connection request ID is not part
/// of this immutable record; reconnect returns it under fresh correlation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binding {
    pub identity: SessionIdentity,
    pub creation_sequence: Id,
    pub policy: Policy,
    pub limits: Limits,
    pub results: bool,
    pub control_limit: ControlLimit,
    pub object_limit: Number,
}

impl Binding {
    pub fn response(&self, request: Id) -> Control {
        Control::Session(Session::Binding {
            request,
            authority: self.identity.authority.clone(),
            owner: self.identity.owner.clone(),
            generation: self.identity.generation,
            creation_sequence: self.creation_sequence,
            policy: self.policy.clone(),
            limits: self.limits.clone(),
        })
    }
}

/// Cloneable handles share an immutable physical policy, not a whole-session
/// in-memory image. Transactions use independent guarded SQLite connections.
#[derive(Clone)]
pub struct AuthorityStore {
    physical: Arc<PhysicalGuard>,
    authority: IdentityLabel,
    policy: StorePolicy,
    clock: Arc<dyn Clock>,
    authorization: Arc<dyn Authorization>,
}

impl AuthorityStore {
    /// Explicitly create a new issuing store. The path must not already exist.
    /// Operators must not use this to replace lost history under the same
    /// authority identity, or restore stale backups without anti-reuse proof.
    pub fn initialize(
        path: &Path,
        authority: IdentityLabel,
        policy: StorePolicy,
        physical_limits: PhysicalLimits,
        clock: Arc<dyn Clock>,
        authorization: Arc<dyn Authorization>,
    ) -> Result<Self> {
        Self::open_inner(
            path,
            authority,
            policy,
            physical_limits,
            clock,
            authorization,
            true,
        )
    }

    /// Open existing durable history. Missing, empty or incompatible stores
    /// fail closed; restart must never silently reset issuing counters.
    pub fn open(
        path: &Path,
        authority: IdentityLabel,
        policy: StorePolicy,
        physical_limits: PhysicalLimits,
        clock: Arc<dyn Clock>,
        authorization: Arc<dyn Authorization>,
    ) -> Result<Self> {
        Self::open_inner(
            path,
            authority,
            policy,
            physical_limits,
            clock,
            authorization,
            false,
        )
    }

    fn open_inner(
        path: &Path,
        authority: IdentityLabel,
        policy: StorePolicy,
        physical_limits: PhysicalLimits,
        clock: Arc<dyn Clock>,
        authorization: Arc<dyn Authorization>,
        initialize: bool,
    ) -> Result<Self> {
        authority.check()?;
        policy.check()?;
        if !initialize {
            let metadata = std::fs::symlink_metadata(path)?;
            if !metadata.is_file() || metadata.len() == 0 {
                return Err(StoreError::Corrupt(
                    "existing authority database is missing or empty",
                ));
            }
        }
        let physical = PhysicalGuard::open(path, Some(physical_limits))?;
        let store = Self {
            physical,
            authority,
            policy,
            clock,
            authorization,
        };
        if initialize {
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            options.open(&store.physical.path)?.sync_all()?;
        }
        let mut connection = store.connect()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let app_id: i64 = tx.query_row("PRAGMA application_id", [], |r| r.get(0))?;
        let version: i64 = tx.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        if initialize && app_id == 0 && version == 0 {
            let objects: i64 = tx.query_row(
                "SELECT count(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
                [],
                |r| r.get(0),
            )?;
            if objects != 0 {
                return Err(StoreError::Corrupt(
                    "database is not an empty authority store",
                ));
            }
            let reading = store.clock.read();
            reading.utc_ms.check()?;
            if !reading.trusted {
                return Err(protocol(
                    ErrorCode::ClockUnsafe,
                    "new authority needs trusted UTC",
                ));
            }
            tx.execute_batch(SCHEMA)?;
            tx.execute(
                "INSERT INTO authority(singleton,name,last_generation,clock,policy,store_id) VALUES(1, ?1, 0, zeroblob(?2), ?3, ?4)",
                params![
                    store.authority.0,
                    (records::HEADER_BYTES + records::CLOCK_CAPACITY) as i64,
                    pack(&store.policy)?,
                    crate::persistence::StoreIdentity::generate()?.as_bytes().as_slice()
                ],
            )?;
            records::initialize(
                &tx,
                records::CLOCK,
                &reading.utc_ms,
                records::CLOCK_CAPACITY,
                0,
            )?;
        } else if app_id != APPLICATION_ID || version != FORMAT {
            return Err(StoreError::Corrupt(
                "wrong authority application or storage format",
            ));
        }
        let (name, retained): (String, Vec<u8>) = tx.query_row(
            "SELECT name,policy FROM authority WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if name != store.authority.0 || unpack::<StorePolicy>(&retained)? != store.policy {
            return Err(StoreError::Corrupt(
                "authority identity or retained policy changed",
            ));
        }
        records::protect(&tx, 0, 0)?;
        records::verify(&tx)?;
        tx.commit()?;
        drop(connection);
        crate::persistence::sync_directory(
            store
                .physical
                .path
                .parent()
                .ok_or(StoreError::Corrupt("missing store directory"))?,
        )?;
        Ok(store)
    }

    fn connect(&self) -> Result<Connection> {
        self.physical.verify()?;
        let connection = Connection::open_with_flags_and_vfs(
            &self.physical.path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            GUARDED_VFS,
        )?;
        connection.busy_timeout(Elapsed::from_secs(5))?;
        connection.execute_batch("PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA trusted_schema=OFF; PRAGMA temp_store=MEMORY; PRAGMA cache_size=-2048;")?;
        Ok(connection)
    }

    fn authorize(&self, owner: &IdentityLabel, permission: Permission) -> Result<()> {
        owner.check()?;
        if !self.authorization.permits(owner, permission) {
            return Err(protocol(ErrorCode::Unauthorized, "authority access denied"));
        }
        Ok(())
    }

    fn trusted_now(&self, tx: &Transaction<'_>) -> Result<Number> {
        let now = self.check_clock(tx)?;
        self.remember_clock(tx, now)?;
        Ok(now)
    }

    /// Read trusted time without spending metadata capacity. A funded transition
    /// spends its record credit first, then persists this observation in the
    /// same transaction with remember_clock. No promise may escape between them.
    fn check_clock(&self, tx: &Transaction<'_>) -> Result<Number> {
        let reading = self.clock.read();
        reading.utc_ms.check()?;
        let (_, greatest): (_, Number) = records::read(tx, records::CLOCK)?;
        if !reading.trusted || reading.utc_ms < greatest {
            return Err(protocol(
                ErrorCode::ClockUnsafe,
                "authority UTC is untrusted or regressed",
            ));
        }
        Ok(reading.utc_ms)
    }

    fn remember_clock(&self, tx: &Transaction<'_>, now: Number) -> Result<()> {
        now.check()?;
        let (clock, greatest): (_, Number) = records::read(tx, records::CLOCK)?;
        if now < greatest {
            return Err(protocol(
                ErrorCode::ClockUnsafe,
                "authority clock observation regressed",
            ));
        }
        records::protect(tx, 0, 0)?;
        if now != greatest {
            records::replace(tx, records::CLOCK, clock.revision, &now, false)?;
        }
        Ok(())
    }

    fn authorize_session(
        &self,
        tx: &Transaction<'_>,
        identity: &SessionIdentity,
        permission: Permission,
    ) -> Result<Binding> {
        self.authorize(&identity.owner, permission)?;
        identity.authority.check()?;
        identity.generation.check()?;
        if identity.authority != self.authority {
            return Err(protocol(ErrorCode::Unauthorized, "authority access denied"));
        }
        // Compare ownership before decoding any retained policy or scope state.
        // Corruption in somebody else's records must not change the denial.
        let owned: Option<bool> = tx
            .query_row(
                "SELECT owner=?2 FROM sessions WHERE generation=?1",
                params![sql(identity.generation.0)?, identity.owner.0],
                |r| r.get(0),
            )
            .optional()?;
        match owned {
            Some(true) => {}
            Some(false) => {
                return Err(protocol(ErrorCode::Unauthorized, "authority access denied"));
            }
            None => return Err(protocol(ErrorCode::NotFound, "session not retained")),
        }
        let retained = sessions::load(tx, &self.authority, identity.generation)?;
        let Some((binding, revoked)) = retained else {
            return Err(protocol(ErrorCode::NotFound, "session not retained"));
        };
        if revoked || binding.identity.owner != identity.owner {
            return Err(protocol(ErrorCode::Unauthorized, "authority access denied"));
        }
        Ok(binding)
    }

    pub fn physical_usage(&self) -> Result<PhysicalUsage> {
        Ok(self.physical.usage()?)
    }

    pub fn integrity_check(&self) -> Result<()> {
        let mut connection = self.connect()?;
        let connection = connection.transaction()?;
        let result: String = connection.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
        if result != "ok" {
            return Err(StoreError::Corrupt("SQLite integrity check failed"));
        }
        let foreign: Option<i64> = connection
            .query_row("PRAGMA foreign_key_check", [], |r| r.get(1))
            .optional()?;
        if foreign.is_some() {
            return Err(StoreError::Corrupt("foreign key check failed"));
        }
        records::verify(&connection)?;
        Ok(())
    }
}

fn pack<T: Wire>(value: &T) -> Result<Vec<u8>> {
    Ok(codec::encode(value, MAX_CONTROL_LIMIT)?)
}
fn unpack<T: Wire>(bytes: &[u8]) -> Result<T> {
    codec::decode(bytes, MAX_CONTROL_LIMIT)
        .map_err(|_| StoreError::Corrupt("invalid retained typed record"))
}

fn increment(value: u64) -> Result<u64> {
    value
        .checked_add(1)
        .filter(|v| *v <= MAX_NUMBER)
        .ok_or_else(|| protocol(ErrorCode::LimitExceeded, "identity counter exhausted"))
}

fn add_duration(now: Number, duration: Duration) -> Result<Number> {
    now.check()?;
    duration.check()?;
    now.0
        .checked_add(duration.0)
        .filter(|n| *n <= MAX_NUMBER)
        .map(Number)
        .ok_or_else(|| {
            protocol(
                ErrorCode::LimitExceeded,
                "retention or execution deadline overflow",
            )
        })
}

fn selected(caps: &Capabilities) -> Result<()> {
    caps.check()?;
    require(
        caps.response.0 == 1,
        "authority needs negotiated capabilities",
    )?;
    if !caps.has(DURABLE_WORK) {
        return Err(protocol(
            ErrorCode::ExtensionUnsupported,
            "durable work not selected",
        ));
    }
    Ok(())
}

fn commit(tx: Transaction<'_>, boundary: &'static str) -> Result<()> {
    #[cfg(test)]
    tests::crash_boundary(boundary, "before");
    tx.commit()?;
    #[cfg(test)]
    tests::crash_boundary(boundary, "after");
    let _ = boundary;
    Ok(())
}

// SQLite INTEGER and the wire number domain share an exact signed-63-bit
// maximum. Never bind a REAL, round through floating point, or wrap a cast.
fn sql(value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| protocol(ErrorCode::LimitExceeded, "SQLite integer overflow"))
}

fn number(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<u64> {
    let value: i64 = row.get(index)?;
    u64::try_from(value).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(index, value))
}
