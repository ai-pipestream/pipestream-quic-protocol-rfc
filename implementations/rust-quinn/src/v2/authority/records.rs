//! Fixed-capacity authority records and persistent credits for their rewrites.
//!
//! A credit funds one overwrite of this record and one shared-clock overwrite
//! in the same transaction, not arbitrary SQL, payloads or an entire job.
//! Callers must separately fund every
//! other member of a transition's write set before acknowledging that promise.

use super::*;
use sha2::{Digest as _, Sha256};

const MAGIC: &[u8; 8] = b"PSREC003";
pub(super) const HEADER_BYTES: usize = 104;
pub(super) const WORK_CAPACITY: usize = 2048;
pub(super) const SCOPE_CAPACITY: usize = 1024;
pub(super) const SCOPE_CREDITS: u64 = 2;
pub(super) const CLOCK_CAPACITY: usize = 64;
pub(super) const CLOCK: Target = Target {
    table: Table::Clock,
    row: 1,
};
pub(super) const WORK_CREDITS: u64 = 2;
const MAX_SECTOR: u64 = 65536;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Table {
    Work,
    Scope,
    Clock,
    Job,
}
impl Table {
    fn name(self) -> &'static str {
        match self {
            Self::Work => "work",
            Self::Scope => "scopes",
            Self::Clock => "authority",
            Self::Job => "jobs",
        }
    }
    fn column(self) -> &'static str {
        match self {
            Self::Work => "view",
            Self::Scope => "state",
            Self::Clock => "clock",
            Self::Job => "state",
        }
    }
    fn inventory(self) -> &'static str {
        match self {
            Self::Work => "SELECT rowid,length(view),substr(view,1,104) FROM work ORDER BY rowid",
            Self::Scope => {
                "SELECT rowid,length(state),substr(state,1,104) FROM scopes ORDER BY rowid"
            }
            Self::Clock => {
                "SELECT rowid,length(clock),substr(clock,1,104) FROM authority ORDER BY rowid"
            }
            Self::Job => "SELECT rowid,length(state),substr(state,1,104) FROM jobs ORDER BY rowid",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Target {
    pub table: Table,
    pub row: i64,
}

#[derive(Debug)]
pub(super) struct Header {
    pub revision: Id,
    pub credits: u64,
    pub capacity: usize,
    used: usize,
    digest: [u8; 32],
}

fn checksum(target: Target, prefix: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"pipestream-authority-record-v3");
    hash.update([match target.table {
        Table::Work => 0,
        Table::Scope => 1,
        Table::Clock => 2,
        Table::Job => 3,
    }]);
    hash.update(target.row.to_be_bytes());
    hash.update(prefix);
    hash.finalize().into()
}

fn parse(target: Target, length: u64, bytes: &[u8]) -> Result<Header> {
    if bytes.len() != HEADER_BYTES || &bytes[..8] != MAGIC || target.row <= 0 {
        return Err(StoreError::Corrupt("invalid authority record header"));
    }
    let integer =
        |offset| u64::from_be_bytes(bytes[offset..offset + 8].try_into().expect("eight bytes"));
    let revision = integer(8);
    let credits = integer(16);
    let used = integer(24);
    let capacity = integer(32);
    if revision == 0
        || revision > MAX_NUMBER
        || credits > MAX_NUMBER
        || revision > MAX_NUMBER - credits
        || used == 0
        || used > capacity
        || capacity > MAX_CONTROL_LIMIT as u64
        || length != capacity + HEADER_BYTES as u64
        || checksum(target, &bytes[..72]).as_slice() != &bytes[72..]
    {
        return Err(StoreError::Corrupt(
            "authority record geometry or checksum changed",
        ));
    }
    Ok(Header {
        revision: Id(revision),
        credits,
        capacity: capacity as usize,
        used: used as usize,
        digest: bytes[40..72].try_into().expect("32 bytes"),
    })
}

pub(super) fn header(tx: &Connection, target: Target) -> Result<Header> {
    let blob = tx.blob_open(
        "main",
        target.table.name(),
        target.table.column(),
        target.row,
        true,
    )?;
    let mut bytes = [0; HEADER_BYTES];
    if blob.len() < HEADER_BYTES {
        return Err(StoreError::Corrupt("truncated authority record"));
    }
    blob.read_at_exact(&mut bytes, 0)?;
    parse(target, blob.len() as u64, &bytes)
}

fn read_bytes(tx: &Connection, target: Target) -> Result<(Header, Vec<u8>)> {
    let retained = header(tx, target)?;
    let blob = tx.blob_open(
        "main",
        target.table.name(),
        target.table.column(),
        target.row,
        true,
    )?;
    let mut bytes = vec![0; retained.used];
    blob.read_at_exact(&mut bytes, HEADER_BYTES)?;
    let mut offset = retained.used;
    let mut buffer = [0; 8192];
    while offset < retained.capacity {
        let count = buffer.len().min(retained.capacity - offset);
        blob.read_at_exact(&mut buffer[..count], HEADER_BYTES + offset)?;
        if buffer[..count].iter().any(|b| *b != 0) {
            return Err(StoreError::Corrupt("authority record padding changed"));
        }
        offset += count;
    }
    if Sha256::digest(&bytes).as_slice() != retained.digest {
        return Err(StoreError::Corrupt(
            "authority record body checksum changed",
        ));
    }
    Ok((retained, bytes))
}

pub(super) fn read<T: Wire>(tx: &Connection, target: Target) -> Result<(Header, T)> {
    let (retained, bytes) = read_bytes(tx, target)?;
    Ok((retained, unpack(&bytes)?))
}

fn exhausted() -> StoreError {
    protocol(
        ErrorCode::LimitExceeded,
        "authority record completion capacity exhausted",
    )
}

/// Record plus fixed shared-clock BLOB, in one transaction. Each allows a leaf
/// and conservative overflow pages. Incremental writes allocate no B-tree pages.
/// Pinned SQLite may duplicate the final commit frame and pad to a sector.
fn rewrite_bytes(capacity: usize, page: u64) -> Result<u64> {
    if capacity == 0 || capacity > MAX_CONTROL_LIMIT {
        return Err(exhausted());
    }
    let frame = page + 24;
    let pages = (capacity as u64 + HEADER_BYTES as u64).div_ceil(page - 4) + 1;
    let clock_pages = (CLOCK_CAPACITY as u64 + HEADER_BYTES as u64).div_ceil(page - 4) + 1;
    (pages + clock_pages + 1 + MAX_SECTOR.div_ceil(frame))
        .checked_mul(frame)
        .and_then(|n| n.checked_add(32))
        .ok_or_else(exhausted)
}

fn physical_error(error: crate::persistence::StoreError) -> StoreError {
    match error {
        crate::persistence::StoreError::Protocol(error)
            if error.code == crate::ERROR_LIMIT_EXCEEDED =>
        {
            exhausted()
        }
        other => StoreError::Physical(other),
    }
}

struct Forecast {
    bytes: u64,
    clock_credits: u64,
    clock_revision: Id,
}
impl Forecast {
    fn clock_room(&self, revision: Id, additional: u64) -> Result<()> {
        if self
            .clock_credits
            .checked_add(additional)
            .is_none_or(|credits| credits > MAX_NUMBER - revision.0)
        {
            return Err(exhausted());
        }
        Ok(())
    }
}

fn audit(
    tx: &Connection,
    page: u64,
    replacement: Option<(Target, usize, u64)>,
) -> Result<Forecast> {
    let mut reserved = 0u64;
    let mut clock_credits = 0u64;
    let mut replaced = replacement.is_none_or(|(target, _, _)| target == CLOCK);
    for table in [Table::Work, Table::Scope, Table::Job] {
        let mut statement = tx.prepare(table.inventory())?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let target = Target {
                table,
                row: row.get(0)?,
            };
            let retained = parse(target, number(row, 1)?, &row.get::<_, Vec<u8>>(2)?)?;
            let (capacity, credits) = if let Some((_, capacity, credits)) =
                replacement.filter(|(t, _, _)| *t == target)
            {
                replaced = true;
                (capacity, credits)
            } else {
                (retained.capacity, retained.credits)
            };
            clock_credits = clock_credits.checked_add(credits).ok_or_else(exhausted)?;
            reserved = reserved
                .checked_add(
                    credits
                        .checked_mul(rewrite_bytes(capacity, page)?)
                        .ok_or_else(exhausted)?,
                )
                .ok_or_else(exhausted)?;
        }
    }
    if !replaced {
        return Err(StoreError::Corrupt("completion replacement record missing"));
    }
    let clock = header(tx, CLOCK)?;
    if clock.capacity != CLOCK_CAPACITY || clock.credits != 0 {
        return Err(StoreError::Corrupt("invalid shared-clock record geometry"));
    }
    Ok(Forecast {
        bytes: reserved,
        clock_credits,
        clock_revision: clock.revision,
    })
}

/// Install the guarded WAL ceiling before unrelated writes, reserving retained
/// credits plus a bounded batch of new slots. The writer lock prevents races.
pub(super) fn protect(tx: &Transaction<'_>, capacity: usize, credits: u64) -> Result<()> {
    let page = crate::persistence::completion_geometry(tx).map_err(physical_error)?;
    let additional = if credits == 0 {
        0
    } else {
        credits
            .checked_mul(rewrite_bytes(capacity, page)?)
            .ok_or_else(exhausted)?
    };
    let forecast = audit(tx, page, None)?;
    forecast.clock_room(forecast.clock_revision, credits)?;
    let reserved = forecast
        .bytes
        .checked_add(additional)
        .ok_or_else(exhausted)?;
    crate::persistence::reserve_completion(tx, page, reserved).map_err(physical_error)
}

/// Restart verifies complete record bodies and their relational identities,
/// rather than trusting a well-formed charge header on corrupt contents. This
/// audit is streaming across records; payload/job reconciliation is separate.
pub(super) fn verify(tx: &Transaction<'_>) -> Result<()> {
    let (_, clock): (_, Number) = read(tx, CLOCK)?;
    let mut statement =
        tx.prepare("SELECT rowid,scope,producer,entity FROM work ORDER BY rowid")?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let (_, view): (_, WorkView) = read(
            tx,
            Target {
                table: Table::Work,
                row: row.get(0)?,
            },
        )?;
        let key = WorkKey {
            scope: Number(number(row, 1)?),
            producer: Producer(number(row, 2)?),
            entity: Id(number(row, 3)?),
        };
        if view.work != key {
            return Err(StoreError::Corrupt(
                "work record identity differs from its index",
            ));
        }
        if view.admitted_at.is_some_and(|time| time > clock)
            || view.terminal_at.is_some_and(|time| time > clock)
        {
            return Err(StoreError::Corrupt(
                "work timestamp exceeds retained shared clock",
            ));
        }
    }
    let mut statement =
        tx.prepare("SELECT rowid,scope,producer,parent FROM scopes ORDER BY rowid")?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let (_, state): (_, scopes::ScopeState) = read(
            tx,
            Target {
                table: Table::Scope,
                row: row.get(0)?,
            },
        )?;
        if state.revoked && number(row, 1)? != 0 {
            return Err(StoreError::Corrupt("revocation outside root scope"));
        }
        if let Some(summary) = state.summary {
            let parent: Option<WorkKey> = row
                .get::<_, Option<Vec<u8>>>(3)?
                .map(|b| unpack(&b))
                .transpose()?;
            if summary.scope.0 != number(row, 1)?
                || summary.producer.0 != number(row, 2)?
                || summary.parent != parent
                || summary.closed_at > clock
            {
                return Err(StoreError::Corrupt("scope summary differs from its index"));
            }
        }
    }
    jobs::verify(tx)
}

fn write<T: Wire>(
    tx: &Connection,
    target: Target,
    value: &T,
    revision: Id,
    capacity: usize,
    credits: u64,
) -> Result<()> {
    write_encoded(tx, target, &pack(value)?, revision, capacity, credits)
}

fn write_encoded(
    tx: &Connection,
    target: Target,
    encoded: &[u8],
    revision: Id,
    capacity: usize,
    credits: u64,
) -> Result<()> {
    revision.check()?;
    if credits > MAX_NUMBER
        || revision.0 > MAX_NUMBER - credits
        || capacity == 0
        || capacity > MAX_CONTROL_LIMIT
    {
        return Err(exhausted());
    }
    if encoded.is_empty() || encoded.len() > capacity {
        return Err(exhausted());
    }
    let mut bytes = [0; HEADER_BYTES];
    bytes[..8].copy_from_slice(MAGIC);
    bytes[8..16].copy_from_slice(&revision.0.to_be_bytes());
    bytes[16..24].copy_from_slice(&credits.to_be_bytes());
    bytes[24..32].copy_from_slice(&(encoded.len() as u64).to_be_bytes());
    bytes[32..40].copy_from_slice(&(capacity as u64).to_be_bytes());
    bytes[40..72].copy_from_slice(&Sha256::digest(encoded));
    let digest = checksum(target, &bytes[..72]);
    bytes[72..].copy_from_slice(&digest);
    let mut blob = tx.blob_open(
        "main",
        target.table.name(),
        target.table.column(),
        target.row,
        false,
    )?;
    if blob.len() != HEADER_BYTES + capacity {
        return Err(StoreError::Corrupt("authority slot capacity changed"));
    }
    blob.write_at(&bytes, 0)?;
    blob.write_at(encoded, HEADER_BYTES)?;
    let zeros = [0; 8192];
    let mut offset = encoded.len();
    while offset < capacity {
        let count = zeros.len().min(capacity - offset);
        blob.write_at(&zeros[..count], HEADER_BYTES + offset)?;
        offset += count;
    }
    blob.close()?;
    Ok(())
}

/// Caller first protects the entire insertion batch, then inserts zeroblobs of
/// this exact capacity. No scan may observe a partially initialized slot.
pub(super) fn initialize<T: Wire>(
    tx: &Transaction<'_>,
    target: Target,
    value: &T,
    capacity: usize,
    credits: u64,
) -> Result<()> {
    write(tx, target, value, Id(1), capacity, credits)
}

/// A protected ordinary rewrite retains its credits. A promised rewrite spends
/// exactly one, never enlarges the slot, and does not release another record's
/// credit. Revision, content and remaining funding commit as one BLOB update.
pub(super) fn replace<T: Wire>(
    tx: &Transaction<'_>,
    target: Target,
    expected: Id,
    value: &T,
    spend: bool,
) -> Result<Id> {
    let retained = header(tx, target)?;
    if retained.revision != expected {
        return Err(protocol(
            ErrorCode::Conflict,
            "authority record revision changed",
        ));
    }
    let revision = Id(increment(expected.0)?);
    // Refuse invalid/oversize content before lowering this connection's WAL
    // ceiling. A rejected replacement must not make an unspent credit usable.
    if pack(value)?.len() > retained.capacity {
        return Err(exhausted());
    }
    let credits = if spend {
        retained.credits.checked_sub(1).ok_or_else(exhausted)?
    } else {
        retained.credits
    };
    if revision.0 > MAX_NUMBER - credits {
        return Err(exhausted());
    }
    let page = crate::persistence::completion_geometry(tx).map_err(physical_error)?;
    let forecast = audit(tx, page, Some((target, retained.capacity, credits)))?;
    forecast.clock_room(
        if target == CLOCK {
            revision
        } else {
            forecast.clock_revision
        },
        0,
    )?;
    crate::persistence::reserve_completion(tx, page, forecast.bytes).map_err(physical_error)?;
    write(tx, target, value, revision, retained.capacity, credits)?;
    Ok(revision)
}

/// Expand a retained slot before accepting a larger promise. This is private
/// funding, not an observable work change: preserve the exact body and revision.
/// Existing capacity/credits cannot be taken away. The expanded credits are
/// protected before allocating pages, and the old record survives any refusal.
pub(super) fn grow(
    tx: &mut Transaction<'_>,
    target: Target,
    expected: Id,
    capacity: usize,
    credits: u64,
) -> Result<()> {
    let (retained, bytes) = read_bytes(tx, target)?;
    match target.table {
        Table::Work => {
            unpack::<WorkView>(&bytes)?;
        }
        Table::Scope => {
            unpack::<scopes::ScopeState>(&bytes)?;
        }
        Table::Clock => {
            unpack::<Number>(&bytes)?;
            if capacity != CLOCK_CAPACITY || credits != 0 {
                return Err(exhausted());
            }
        }
        Table::Job => {
            unpack::<jobs::JobRecord>(&bytes)?;
        }
    }
    if retained.revision != expected {
        return Err(protocol(
            ErrorCode::Conflict,
            "authority record revision changed",
        ));
    }
    if capacity < retained.capacity
        || capacity > MAX_CONTROL_LIMIT
        || credits < retained.credits
        || credits > MAX_NUMBER
        || expected.0 > MAX_NUMBER - credits
    {
        return Err(exhausted());
    }
    // Validate the writer even for a no-op; no caller may treat a stale read
    // snapshot as a reservation accepted under the authority writer lock.
    let page = crate::persistence::completion_geometry(tx).map_err(physical_error)?;
    let forecast = audit(tx, page, Some((target, capacity, credits)))?;
    forecast.clock_room(forecast.clock_revision, 0)?;
    crate::persistence::reserve_completion(tx, page, forecast.bytes).map_err(physical_error)?;
    if (capacity, credits) == (retained.capacity, retained.credits) {
        return Ok(());
    }
    let savepoint = tx.savepoint()?;
    if capacity != retained.capacity {
        let statement = match target.table {
            Table::Work => "UPDATE work SET view=zeroblob(?1) WHERE rowid=?2",
            Table::Scope => "UPDATE scopes SET state=zeroblob(?1) WHERE rowid=?2",
            Table::Clock => "UPDATE authority SET clock=zeroblob(?1) WHERE rowid=?2",
            Table::Job => "UPDATE jobs SET state=zeroblob(?1) WHERE rowid=?2",
        };
        if savepoint.execute(
            statement,
            params![(HEADER_BYTES + capacity) as i64, target.row],
        )? != 1
        {
            return Err(StoreError::Corrupt("authority growth target disappeared"));
        }
        #[cfg(test)]
        tests::growth_crash_point("resized");
    }
    write_encoded(&savepoint, target, &bytes, expected, capacity, credits)?;
    #[cfg(test)]
    tests::growth_crash_point("written");
    savepoint.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests;
