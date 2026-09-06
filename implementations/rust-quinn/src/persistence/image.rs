//! Fixed-capacity SQLite images. Admission allocates result growth up front;
//! publications within that capacity only overwrite existing BLOB pages.

use super::*;

const MAGIC: &[u8; 8] = b"PSIMG001";
pub(super) const HEADER_BYTES: usize = 104;

#[derive(Debug)]
pub(super) struct Header {
    pub rowid: i64,
    pub revision: u64,
    pub state_bytes: usize,
    pub capacity: usize,
    pub checksum: [u8; 32],
}

fn checksum(id: &str, capacity: usize, header: &[u8]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"pipestream-session-image-v1");
    digest.update((id.len() as u64).to_be_bytes());
    digest.update(id.as_bytes());
    digest.update((capacity as u64).to_be_bytes());
    digest.update(header);
    digest.finalize().into()
}

pub(super) fn header(
    connection: &Connection,
    id: &str,
    limit: usize,
) -> Result<Option<Header>, StoreError> {
    let row = connection
        .query_row(
            "SELECT rowid, length(image) FROM pipestream_sessions WHERE session_id = ?1",
            [id],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()?;
    let Some((rowid, length)) = row else {
        return Ok(None);
    };
    let capacity = usize::try_from(length)
        .ok()
        .and_then(|length| length.checked_sub(HEADER_BYTES))
        .filter(|&capacity| capacity > 0 && capacity <= limit)
        .ok_or_else(|| StoreError::Corrupt("stored session image exceeds record budget".into()))?;
    let blob = connection.blob_open("main", "pipestream_sessions", "image", rowid, true)?;
    let mut bytes = [0; HEADER_BYTES];
    blob.read_at_exact(&mut bytes, 0)?;
    let number = |offset: usize| {
        let mut octets = [0; 8];
        octets.copy_from_slice(&bytes[offset..offset + 8]);
        u64::from_be_bytes(octets)
    };
    let revision = number(16);
    let state_bytes = usize::try_from(number(32))
        .map_err(|_| StoreError::Corrupt("session image length overflows address space".into()))?;
    if &bytes[..8] != MAGIC
        || revision == 0
        || revision > i64::MAX as u64
        || number(24) > i64::MAX as u64
        || state_bytes == 0
        || state_bytes > capacity
        || checksum(id, capacity, &bytes[..72]).as_slice() != &bytes[72..]
    {
        return Err(StoreError::Corrupt(
            "invalid session image header or checksum".into(),
        ));
    }
    if number(8) != u64::from(SESSION_FORMAT_VERSION) {
        return Err(StoreError::Corrupt(format!(
            "unsupported stored session version {}",
            number(8)
        )));
    }
    let mut state_checksum = [0; 32];
    state_checksum.copy_from_slice(&bytes[40..72]);
    Ok(Some(Header {
        rowid,
        revision,
        state_bytes,
        capacity,
        checksum: state_checksum,
    }))
}

pub(super) fn read(connection: &Connection, header: &Header) -> Result<Vec<u8>, StoreError> {
    let blob = connection.blob_open("main", "pipestream_sessions", "image", header.rowid, true)?;
    let mut state = vec![0; header.state_bytes];
    blob.read_at_exact(&mut state, HEADER_BYTES)?;
    // Reserved storage is capacity, never another serialized outcome. Refuse
    // changed padding so corruption cannot hide outside the logical checksum.
    let mut remaining = header.capacity - header.state_bytes;
    let mut buffer = [0; 8192];
    while remaining != 0 {
        let count = remaining.min(buffer.len());
        blob.read_at_exact(
            &mut buffer[..count],
            HEADER_BYTES + header.capacity - remaining,
        )?;
        if buffer[..count].iter().any(|&value| value != 0) {
            return Err(StoreError::Corrupt(
                "nonzero session reservation padding".into(),
            ));
        }
        remaining -= count;
    }
    if Sha256::digest(&state).as_slice() != header.checksum {
        return Err(StoreError::Corrupt(
            "stored session checksum mismatch".into(),
        ));
    }
    Ok(state)
}

pub(super) fn write(
    connection: &Connection,
    id: &str,
    expected: u64,
    revision: u64,
    state: &[u8],
    state_checksum: &[u8; 32],
    required: usize,
) -> Result<(), StoreError> {
    let limit = storage::read_limits(connection)?.record_bytes;
    if required < state.len() || required > limit || revision == 0 || revision > i64::MAX as u64 {
        return Err(StoreError::Protocol(ProtocolError::limit(
            "invalid session image capacity or revision",
        )));
    }
    let previous = header(connection, id, limit)?;
    let actual = previous.as_ref().map_or(0, |value| value.revision);
    if expected != 0 && previous.is_none() {
        return Err(StoreError::NotFound(id.to_owned()));
    }
    if expected != actual {
        return Err(StoreError::Conflict { expected, actual });
    }
    let capacity = previous
        .as_ref()
        .map_or(required, |value| value.capacity.max(required));
    let rowid = if let Some(previous) = previous {
        if capacity != previous.capacity {
            connection.execute(
                "UPDATE pipestream_sessions SET image = zeroblob(?2) WHERE rowid = ?1",
                params![previous.rowid, (capacity + HEADER_BYTES) as i64],
            )?;
        }
        previous.rowid
    } else {
        connection.execute(
            "INSERT INTO pipestream_sessions (session_id, image) VALUES (?1, zeroblob(?2))",
            params![id, (capacity + HEADER_BYTES) as i64],
        )?;
        connection.last_insert_rowid()
    };
    let mut bytes = [0; HEADER_BYTES];
    bytes[..8].copy_from_slice(MAGIC);
    bytes[8..16].copy_from_slice(&u64::from(SESSION_FORMAT_VERSION).to_be_bytes());
    bytes[16..24].copy_from_slice(&revision.to_be_bytes());
    bytes[24..32].copy_from_slice(&(now_micros()? as u64).to_be_bytes());
    bytes[32..40].copy_from_slice(&(state.len() as u64).to_be_bytes());
    bytes[40..72].copy_from_slice(state_checksum);
    let header_checksum = checksum(id, capacity, &bytes[..72]);
    bytes[72..].copy_from_slice(&header_checksum);
    let mut blob = connection.blob_open("main", "pipestream_sessions", "image", rowid, false)?;
    blob.write_at(&bytes, 0)?;
    blob.write_at(state, HEADER_BYTES)?;
    let zeros = [0; 8192];
    let mut remaining = capacity - state.len();
    while remaining != 0 {
        let count = remaining.min(zeros.len());
        blob.write_at(&zeros[..count], HEADER_BYTES + capacity - remaining)?;
        remaining -= count;
    }
    blob.close()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::{JobOutput, ProcessOutcome, tests::fixture};

    #[test]
    fn admitted_attempt_and_result_overwrite_the_same_allocated_image() {
        let dir = tempfile::tempdir().unwrap();
        let store = SqliteSessionStore::open(dir.path().join("images.sqlite3")).unwrap();
        let (mut session, key, input) = fixture("image", None);
        session.enqueue_job(key, input, 100).unwrap();
        store.create(&session).unwrap();
        let mut connection = store.connect().unwrap();
        let before = header(&connection, "image", store.storage_limits().record_bytes)
            .unwrap()
            .unwrap();
        assert!(before.capacity > before.state_bytes + (64 << 10));
        let pages: u32 = connection
            .query_row("PRAGMA page_count", [], |row| row.get(0))
            .unwrap();
        let page_cap: u32 = connection
            .query_row(&format!("PRAGMA max_page_count={pages}"), [], |r| r.get(0))
            .unwrap();
        assert_eq!(page_cap, pages);
        connection
            .execute_batch(
                "CREATE TEMP TRIGGER no_image_replacement BEFORE UPDATE ON main.pipestream_sessions
            BEGIN SELECT RAISE(ABORT, 'session image was replaced'); END;",
            )
            .unwrap();
        // Use this connection so its trigger is active on the actual writes.
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let retained = load_from(&tx, "image").unwrap().unwrap();
        let mut session = retained.session;
        let lease = session.acquire_job(None, key, 100, 1000).unwrap().unwrap();
        persist_update(&tx, retained.revision, retained.revision + 1, &session).unwrap();
        tx.commit().unwrap();
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let retained = load_from(&tx, "image").unwrap().unwrap();
        let mut session = retained.session;
        session
            .publish_job(None, &lease, 200, |s| {
                s.complete_entity(key.entity, [7; 32])?;
                Ok(JobOutput::Processed(ProcessOutcome::Complete))
            })
            .unwrap();
        persist_update(&tx, retained.revision, retained.revision + 1, &session).unwrap();
        tx.commit().unwrap();
        let after = header(&connection, "image", store.storage_limits().record_bytes)
            .unwrap()
            .unwrap();
        assert_eq!(
            (after.rowid, after.capacity),
            (before.rowid, before.capacity)
        );
        assert_eq!(after.revision, before.revision + 2);
        assert_eq!(
            connection
                .query_row("PRAGMA page_count", [], |r| r.get::<_, u32>(0))
                .unwrap(),
            pages
        );
        let reopened = SqliteSessionStore::open(store.path()).unwrap();
        assert_eq!(reopened.load("image").unwrap().unwrap().session, session);
        reopened.integrity_check().unwrap();
    }

    #[test]
    fn image_header_state_padding_and_capacity_corruption_are_refused() {
        for offset in [0, 8, 16, 24, 32, 40, 72, HEADER_BYTES, HEADER_BYTES + 4096] {
            let dir = tempfile::tempdir().unwrap();
            let store = SqliteSessionStore::open(dir.path().join("images.sqlite3")).unwrap();
            let (mut session, key, input) = fixture("image", None);
            session.enqueue_job(key, input, 100).unwrap();
            store.create(&session).unwrap();
            let connection = store.connect().unwrap();
            let mut bytes: Vec<u8> = connection
                .query_row("SELECT image FROM pipestream_sessions", [], |r| r.get(0))
                .unwrap();
            bytes[offset] ^= 1;
            connection
                .execute("UPDATE pipestream_sessions SET image = ?1", [&bytes])
                .unwrap();
            assert!(
                matches!(store.load("image"), Err(StoreError::Corrupt(_))),
                "offset {offset}"
            );
            assert!(store.integrity_check().is_err(), "offset {offset}");
        }
        let dir = tempfile::tempdir().unwrap();
        let store = SqliteSessionStore::open(dir.path().join("images.sqlite3")).unwrap();
        let (mut session, key, input) = fixture("image", None);
        session.enqueue_job(key, input, 100).unwrap();
        store.create(&session).unwrap();
        let connection = store.connect().unwrap();
        connection
            .execute(
                "UPDATE pipestream_sessions SET image = substr(image, 1, length(image) - 1)",
                [],
            )
            .unwrap();
        assert!(matches!(store.load("image"), Err(StoreError::Corrupt(_))));
    }

    #[test]
    fn image_growth_rollback_preserves_capacity_revision_and_retained_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let store = SqliteSessionStore::open(dir.path().join("images.sqlite3")).unwrap();
        let (session, key, input) = fixture("image", None);
        let before = store.create(&session).unwrap();
        let connection = store.connect().unwrap();
        let original: Vec<u8> = connection
            .query_row("SELECT image FROM pipestream_sessions", [], |r| r.get(0))
            .unwrap();
        connection
            .execute_batch(
                "CREATE INDEX reject_accounting ON pipestream_storage_sessions(length(image))",
            )
            .unwrap();
        assert!(
            store
                .transact("image", |s| s.enqueue_job(key, input, 100))
                .is_err()
        );
        assert_eq!(store.load("image").unwrap().unwrap(), before);
        assert_eq!(
            connection
                .query_row("SELECT image FROM pipestream_sessions", [], |r| r
                    .get::<_, Vec<u8>>(0))
                .unwrap(),
            original
        );
        store.integrity_check().unwrap();
    }

    #[test]
    fn image_wal_extent_stays_within_its_allocated_page_bound_even_when_cache_spills() {
        for page_size in [512u64, 4096, 65536] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("geometry.sqlite3");
            let guard = physical::Guard::open(&path, None).unwrap();
            let setup = Connection::open_with_flags_and_vfs(
                &path,
                OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
                physical::VFS_NAME,
            )
            .unwrap();
            setup
                .execute_batch(&format!(
                    "PRAGMA page_size={page_size}; CREATE TABLE geometry_probe (value INTEGER);"
                ))
                .unwrap();
            drop(setup);
            let store = SqliteSessionStore::open_with_limits(
                &path,
                JobQueueLimits::default(),
                StorageLimits {
                    yield_token_bytes: 1 << 20,
                    ..StorageLimits::default()
                },
            )
            .unwrap();
            let (mut session, key, input) = fixture("geometry", None);
            session.enqueue_job(key, input, 100).unwrap();
            store.create(&session).unwrap();
            let mut connection = store.connect().unwrap();
            store.checkpoint().unwrap();
            let reader = store.connect().unwrap();
            reader
                .execute_batch("BEGIN; SELECT count(*) FROM pipestream_sessions")
                .unwrap();
            connection
                .execute_batch("PRAGMA cache_size=2; PRAGMA cache_spill=1;")
                .unwrap();
            let before = header(&connection, "geometry", store.storage_limits().record_bytes)
                .unwrap()
                .unwrap();
            let state = read(&connection, &before).unwrap();
            let tx = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            write(
                &tx,
                "geometry",
                before.revision,
                before.revision + 1,
                &state,
                &before.checksum,
                before.capacity,
            )
            .unwrap();
            tx.commit().unwrap();
            // One leaf plus overflow pages, one repeated final commit page,
            // and complete-frame padding across a sector of at most 64 KiB.
            // These files are created with SQLite's zero reserved-byte geometry.
            let frame = page_size + 24;
            let pages = (HEADER_BYTES as u64 + before.capacity as u64).div_ceil(page_size - 4) + 1;
            let extent_bound = 32 + (pages + 1 + 65536u64.div_ceil(frame)) * frame;
            let actual = store.physical_usage().unwrap().wal_bytes;
            assert!(
                actual > 0 && actual <= extent_bound,
                "page_size={page_size}: {actual} > {extent_bound}"
            );
            eprintln!(
                "fixed image WAL: page_size={page_size}, capacity={}, extent={actual}, bound={extent_bound}",
                before.capacity
            );
            store.integrity_check().unwrap();
            drop(reader);
            drop(guard);
        }
    }
}
