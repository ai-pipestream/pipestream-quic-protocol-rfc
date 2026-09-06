//! WAL funding for admitted jobs under the pinned bundled SQLite layout.

use super::*;
use crate::{
    jobs::JobState,
    persistence::{image, load_from, schema, storage},
    session::Session,
};
use rusqlite::Connection;

pub(super) const MAX_SECTOR_BYTES: u64 = 65536;
const WAL_HEADER_BYTES: u64 = 32;
const FRAME_HEADER_BYTES: u64 = 24;

fn exhausted() -> StoreError {
    StoreError::Protocol(ProtocolError::limit(
        "insufficient SQLite completion headroom",
    ))
}

fn stages(session: &Session) -> Result<u64, StoreError> {
    let mut stages = 0u64;
    for job in session.jobs.values() {
        let count = match job.state {
            JobState::Queued => 2,  // acquisition and publication
            JobState::Running => 1, // renewal must buy ordinary write capacity
            JobState::Finished(_) | JobState::Refused(_) => 0,
        };
        stages = stages.checked_add(count).ok_or_else(exhausted)?;
    }
    for _ in session.future_rehydrations() {
        // Convert the preallocated slot, then acquire and publish it.
        stages = stages.checked_add(3).ok_or_else(exhausted)?;
    }
    Ok(stages)
}

fn stage_bytes(capacity: usize, page_size: u64) -> Result<u64, StoreError> {
    let frame = page_size + FRAME_HEADER_BYTES;
    // Incremental BLOB writes do not change keys or allocate B-tree pages.
    // The session can occupy one leaf plus these overflow pages. A stage can
    // also change two 32-byte queue images and one 56-byte charge image, each
    // crossing at most two pages. A four-byte overflow link is not payload.
    let image_pages = (capacity as u64 + image::HEADER_BYTES as u64).div_ceil(page_size - 4) + 1;
    let dirty_pages = image_pages + 6;
    // SQLite 3.53.2 walFrames overwrites same-transaction spill frames, but
    // can repeat the final commit page and then pad to the sector boundary.
    let frames = dirty_pages + 1 + MAX_SECTOR_BYTES.div_ceil(frame);
    frames
        .checked_mul(frame)
        .and_then(|n| n.checked_add(WAL_HEADER_BYTES))
        .ok_or_else(exhausted)
}

fn session_bytes(session: &Session, capacity: usize, page_size: u64) -> Result<u64, StoreError> {
    stages(session)?
        .checked_mul(stage_bytes(capacity, page_size)?)
        .ok_or_else(exhausted)
}

pub(super) fn wal_ceiling(limits: PhysicalLimits, page_size: u64, reserved: u64) -> Option<u64> {
    if !(512..=65536).contains(&page_size) || !page_size.is_power_of_two() {
        return None;
    }
    // Pinned SQLite's first 32 KiB WAL-index region maps 4,062 frames;
    // subsequent regions map 4,096. The VFS rounds mappings to 64 KiB.
    let regions = limits.shared_memory_bytes / 32768;
    let frames = 4062u64.checked_add(regions.checked_sub(1)?.checked_mul(4096)?)?;
    let indexed_bytes =
        WAL_HEADER_BYTES.checked_add(frames.checked_mul(page_size + FRAME_HEADER_BYTES)?)?;
    limits
        .wal_bytes
        .min(indexed_bytes)
        .checked_sub(reserved)
        .filter(|n| *n >= WAL_HEADER_BYTES)
}

/// Called under the SQLite writer transaction, before the first state/index
/// write. The per-connection VFS ceiling remains installed through commit or
/// rollback, including failures before persist_update returns to its caller.
pub(in crate::persistence) fn protect(
    connection: &Connection,
    proposed: &Session,
    required_capacity: usize,
) -> Result<(), StoreError> {
    protect_proposed(connection, Some((proposed, required_capacity)))
}

/// Preserve every admitted execution allowance while writing unrelated metadata.
pub(in crate::persistence) fn protect_unchanged(connection: &Connection) -> Result<(), StoreError> {
    protect_proposed(connection, None)
}

fn protect_proposed(
    connection: &Connection,
    proposed: Option<(&Session, usize)>,
) -> Result<(), StoreError> {
    // SAFETY: this read-only SQLite call borrows the live connection handle.
    let transaction_state =
        unsafe { rusqlite::ffi::sqlite3_txn_state(connection.handle(), c"main".as_ptr()) };
    if transaction_state != rusqlite::ffi::SQLITE_TXN_WRITE {
        return Err(corrupt(
            "completion reservation requires a writer transaction",
        ));
    }
    // Changing SQLite requires revalidating the spill/commit and WAL-index
    // geometry, not silently inheriting a different engine's write behavior.
    if rusqlite::version_number() != 3_053_002 {
        return Err(corrupt(
            "completion reservation requires bundled SQLite 3.53.2",
        ));
    }
    let page_size =
        u64::from(connection.query_row("PRAGMA page_size", [], |r| r.get::<_, u32>(0))?);
    let mut page_reserve: i32 = -1;
    // SAFETY: the live connection owns the main database. -1 queries the
    // reserve setting without modifying it, using SQLite's documented ABI.
    let result = unsafe {
        rusqlite::ffi::sqlite3_file_control(
            connection.handle(),
            c"main".as_ptr(),
            rusqlite::ffi::SQLITE_FCNTL_RESERVE_BYTES,
            std::ptr::addr_of_mut!(page_reserve).cast(),
        )
    };
    if result != rusqlite::ffi::SQLITE_OK
        || page_reserve != 0
        || !(512..=65536).contains(&page_size)
        || !page_size.is_power_of_two()
    {
        return Err(corrupt("unsupported SQLite completion page geometry"));
    }
    let limits = storage::read_limits(connection)?;
    let mut capacity = proposed.map_or(0, |(_, capacity)| capacity);
    let mut reserved = 0u64;
    let mut query = connection.prepare(schema::SESSION_IDS)?;
    let mut rows = query.query([])?;
    while let Some(row) = rows.next()? {
        let id = schema::session_id(row, 0)?;
        let header = image::header(connection, &id, limits.record_bytes)?
            .ok_or_else(|| corrupt("session missing during completion audit"))?;
        if proposed.is_some_and(|(session, _)| id == session.session_id) {
            capacity = capacity.max(header.capacity);
        } else {
            let retained = load_from(connection, &id)?
                .ok_or_else(|| corrupt("session missing during completion audit"))?;
            reserved = reserved
                .checked_add(session_bytes(
                    &retained.session,
                    header.capacity,
                    page_size,
                )?)
                .ok_or_else(exhausted)?;
        }
    }
    if let Some((session, _)) = proposed {
        reserved = reserved
            .checked_add(session_bytes(session, capacity, page_size)?)
            .ok_or_else(exhausted)?;
    }
    vfs::reserve(connection, page_size, reserved)
}

#[cfg(test)]
mod cost_tests;
#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::{JobOutput, ProcessOutcome, tests::fixture};

    #[test]
    fn acquisition_spends_one_stage_but_expired_renewal_spends_none() {
        let (mut session, key, input) = fixture("stages", None);
        session.enqueue_job(key, input, 100).unwrap();
        assert_eq!(stages(&session).unwrap(), 5);
        session.acquire_job(None, key, 100, 50).unwrap().unwrap();
        assert_eq!(stages(&session).unwrap(), 4);
        let lease = session.acquire_job(None, key, 150, 50).unwrap().unwrap();
        assert_eq!(stages(&session).unwrap(), 4);
        session
            .publish_job(None, &lease, 151, |s| {
                s.begin_dehydrating(key.entity)?;
                Ok(JobOutput::Processed(ProcessOutcome::Dehydrate))
            })
            .unwrap();
        assert_eq!(
            stages(&session).unwrap(),
            3,
            "waiting parent retains conversion and execution"
        );
    }

    #[test]
    fn wal_ceiling_funds_shared_memory_and_rejects_unfunded_reservations() {
        let limits = PhysicalLimits {
            wal_bytes: 1 << 30,
            shared_memory_bytes: 64 << 10,
            ..PhysicalLimits::default()
        };
        for page in [512, 4096, 65536] {
            let indexed = 32 + (4062 + 4096) * (page + 24);
            assert_eq!(wal_ceiling(limits, page, 0), Some(indexed));
            assert_eq!(wal_ceiling(limits, page, indexed - 32), Some(32));
            assert_eq!(wal_ceiling(limits, page, indexed - 31), None);
        }
        assert_eq!(wal_ceiling(limits, 513, 0), None);
        assert_eq!(wal_ceiling(limits, 4096, u64::MAX), None);
    }
}
