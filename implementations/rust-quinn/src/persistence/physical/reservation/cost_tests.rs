use super::*;
use crate::{
    StoppingPointValidation,
    authorization::PrincipalBinding,
    jobs::{JobOutput, ProcessOutcome, tests::fixture},
    persistence::{
        JobQueueLimits, SessionStore, SqliteSessionStore, StorageLimits, TransactionBehavior,
        persist_update,
    },
};

fn measured<T>(
    store: &SqliteSessionStore,
    connection: &mut Connection,
    id: &str,
    page: u64,
    operation: impl FnOnce(&mut Session) -> Result<T, ProtocolError>,
) -> T {
    store.checkpoint().unwrap();
    let reader = store.connect().unwrap();
    reader
        .execute_batch("BEGIN; SELECT count(*) FROM pipestream_sessions")
        .unwrap();
    let before = image::header(connection, id, store.storage_limits().record_bytes)
        .unwrap()
        .unwrap();
    let pages: u32 = connection
        .query_row("PRAGMA page_count", [], |r| r.get(0))
        .unwrap();
    let capped: u32 = connection
        .query_row(&format!("PRAGMA max_page_count={pages}"), [], |r| r.get(0))
        .unwrap();
    assert_eq!(capped, pages);
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let current = load_from(&transaction, id).unwrap().unwrap();
    let mut session = current.session;
    let result = operation(&mut session).unwrap();
    persist_update(
        &transaction,
        current.revision,
        current.revision + 1,
        &session,
    )
    .unwrap();
    transaction.commit().unwrap();
    let after = image::header(connection, id, store.storage_limits().record_bytes)
        .unwrap()
        .unwrap();
    assert_eq!(
        (after.rowid, after.capacity),
        (before.rowid, before.capacity)
    );
    assert_eq!(
        connection
            .query_row("PRAGMA page_count", [], |r| r.get::<_, u32>(0))
            .unwrap(),
        pages
    );
    let actual = store.physical_usage().unwrap().wal_bytes;
    let bound = stage_bytes(before.capacity, page).unwrap();
    assert!(
        actual > 0 && actual <= bound,
        "page={page} capacity={}: {actual} exceeds {bound}",
        before.capacity
    );
    assert_eq!(store.load(id).unwrap().unwrap().session, session);
    eprintln!(
        "complete transaction: page={page} capacity={} WAL={actual} bound={bound}",
        before.capacity
    );
    store.integrity_check().unwrap();
    result
}

#[test]
fn complete_stage_bound_covers_spilling_acquisition_refusal_and_token_publication() {
    for page in [512u64, 4096, 65536] {
        for token in [127usize, 128, 4095, 4096, 65535, 65536, 1 << 20, 8 << 20] {
            for outcome in 0..3 {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("stage.sqlite3");
                let physical = PhysicalLimits {
                    wal_bytes: 128 << 20,
                    shared_memory_bytes: 4 << 20,
                    ..PhysicalLimits::default()
                };
                let guard = Guard::open(&path, Some(physical)).unwrap();
                let setup = Connection::open_with_flags_and_vfs(
                    &path,
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                        | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
                    super::super::VFS_NAME,
                )
                .unwrap();
                setup
                    .execute_batch(&format!(
                        "PRAGMA page_size={page}; CREATE TABLE geometry_probe(value INTEGER)"
                    ))
                    .unwrap();
                drop(setup);
                let limits = StorageLimits {
                    record_bytes: 16 << 20,
                    yield_token_bytes: token,
                    ..StorageLimits::default()
                };
                let store = SqliteSessionStore::open_with_all_limits(
                    &path,
                    JobQueueLimits::default(),
                    limits,
                    physical,
                )
                .unwrap();
                let id = "s".repeat(128);
                let owner = PrincipalBinding::new("a".repeat(128), "p".repeat(128)).unwrap();
                let (mut session, key, input) = fixture(&id, Some(owner.clone()));
                session.enqueue_job(key, input, 100).unwrap();
                store.create(&session).unwrap();
                let mut connection = store.connect().unwrap();
                connection
                    .execute_batch("PRAGMA cache_size=2; PRAGMA cache_spill=1")
                    .unwrap();
                // Fixed image mutations must not take SQL row-replacement paths.
                for table in [
                    "pipestream_sessions",
                    "pipestream_jobs",
                    "pipestream_storage_sessions",
                ] {
                    connection.execute_batch(&format!("CREATE TEMP TRIGGER no_update_{table} BEFORE UPDATE ON main.{table} BEGIN SELECT RAISE(ABORT, 'unexpected row replacement'); END;
                        CREATE TEMP TRIGGER no_delete_{table} BEFORE DELETE ON main.{table} BEGIN SELECT RAISE(ABORT, 'unexpected row deletion'); END;")).unwrap();
                }
                let lease = measured(&store, &mut connection, &id, page, |s| {
                    s.acquire_job(Some(&owner), key, 127, 100)
                })
                .unwrap();
                measured(&store, &mut connection, &id, page, |s| {
                    if outcome == 1 {
                        return s.refuse_job(
                            Some(&owner),
                            &lease,
                            128,
                            &ProtocolError::limit("x".repeat(512)),
                        );
                    }
                    s.publish_job(Some(&owner), &lease, 128, |s| {
                        let output = if outcome == 0 {
                            s.complete_entity(key.entity, [7; 32])?;
                            ProcessOutcome::Complete
                        } else {
                            s.defer_with_claim_id(
                                key.entity,
                                vec![255; token],
                                StoppingPointValidation {
                                    state_checksum: Some([255; 32]),
                                    bytes_processed: Some(u64::MAX),
                                    children_complete: Some(u64::MAX),
                                    children_total: Some(u64::MAX),
                                    is_resumable: Some(true),
                                    checkpoint_ref: Some("x".repeat(256)),
                                },
                                u64::MAX,
                                u64::MAX,
                                128,
                            )?;
                            ProcessOutcome::Deferred {
                                reason: 5,
                                claim_id: u64::MAX,
                            }
                        };
                        Ok(JobOutput::Processed(output))
                    })
                    .map(|_| ())
                });
                assert_eq!(store.unfinished_job_count().unwrap(), 0);
                drop(guard);
            }
        }
    }
}
