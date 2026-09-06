use super::*;
use crate::{
    LayerSupport,
    authorization::PrincipalBinding,
    execution::{ExecutionKey, ExecutionStage},
    jobs::{JobInput, JobOutput, ProcessOutcome, tests::fixture},
    session::{EntityKey, EntityState, NewEntity},
};
use std::collections::BTreeMap;

fn principal() -> PrincipalBinding {
    PrincipalBinding::new("issuer", "alice").unwrap()
}

fn add_job(session: &mut Session, id: u32) -> ExecutionKey {
    let (_, _, mut input) = fixture(&session.session_id, Some(principal()));
    let JobInput::Process {
        header,
        digest,
        layers,
        ..
    } = &mut input
    else {
        unreachable!()
    };
    header.entity_id = id;
    *layers = LayerSupport::LAYER1;
    let entity = session
        .add_root(NewEntity {
            entity_id: id,
            layer: 0,
            payload_digest: *digest,
            policy: None,
        })
        .unwrap();
    session.transition(entity, EntityState::Processing).unwrap();
    let key = ExecutionKey {
        entity,
        stage: ExecutionStage::Process,
    };
    session.enqueue_job(key, input, 100).unwrap();
    key
}

fn session(count: u32) -> Session {
    let mut session = Session::new("many", 7, 2048).unwrap();
    session.bind_owner(principal()).unwrap();
    for id in 1..=count {
        add_job(&mut session, id);
    }
    session
}

fn store(path: &Path, ordinary: u32) -> SqliteSessionStore {
    SqliteSessionStore::open_with_job_limits(
        path,
        JobQueueLimits {
            total: ordinary,
            per_principal: ordinary,
            rehydration_total: ordinary,
            rehydration_per_principal: ordinary,
        },
    )
    .unwrap()
}

fn rowids(connection: &Connection) -> BTreeMap<Vec<u8>, i64> {
    connection
        .prepare("SELECT execution_key, rowid FROM pipestream_jobs ORDER BY execution_key")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

fn audit(connection: &Connection) {
    connection.execute_batch("CREATE TABLE delta_audit (kind TEXT, action TEXT, key BLOB);
        CREATE TRIGGER queue_insert AFTER INSERT ON pipestream_jobs BEGIN INSERT INTO delta_audit VALUES('queue','insert',NEW.execution_key); END;
        CREATE TRIGGER queue_update AFTER UPDATE ON pipestream_jobs BEGIN INSERT INTO delta_audit VALUES('queue','update',NEW.execution_key); END;
        CREATE TRIGGER queue_delete AFTER DELETE ON pipestream_jobs BEGIN INSERT INTO delta_audit VALUES('queue','delete',OLD.execution_key); END;
        CREATE TRIGGER storage_insert AFTER INSERT ON pipestream_storage_sessions BEGIN INSERT INTO delta_audit VALUES('storage','insert',NULL); END;
        CREATE TRIGGER storage_update AFTER UPDATE ON pipestream_storage_sessions BEGIN INSERT INTO delta_audit VALUES('storage','update',NULL); END;
        CREATE TRIGGER storage_delete AFTER DELETE ON pipestream_storage_sessions BEGIN INSERT INTO delta_audit VALUES('storage','delete',NULL); END;").unwrap();
}

fn changes(connection: &Connection) -> Vec<(String, String, u32)> {
    connection.prepare("SELECT kind, action, count(*) FROM delta_audit GROUP BY kind, action ORDER BY kind, action").unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap()
        .collect::<Result<_, _>>().unwrap()
}

#[test]
fn noop_lease_completion_and_revocation_touch_only_changed_index_rows() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir.path().join("delta.sqlite3"), 128);
    let session = session(128);
    let key = ExecutionKey {
        entity: EntityKey {
            scope_id: 0,
            entity_id: 1,
        },
        stage: ExecutionStage::Process,
    };
    let saved = store.create(&session).unwrap();
    let connection = store.connect().unwrap();
    let ids = rowids(&connection);
    let storage_id: i64 = connection
        .query_row("SELECT rowid FROM pipestream_storage_sessions", [], |r| {
            r.get(0)
        })
        .unwrap();
    audit(&connection);
    store.save(saved.revision, &saved.session).unwrap();
    assert!(changes(&connection).is_empty());
    assert_eq!(rowids(&connection), ids);
    let lease = store
        .transact("many", |s| s.acquire_job(Some(&principal()), key, 100, 50))
        .unwrap()
        .0
        .unwrap();
    assert_eq!(
        changes(&connection),
        vec![
            ("queue".into(), "update".into(), 1),
            ("storage".into(), "update".into(), 1)
        ]
    );
    assert_eq!(rowids(&connection), ids);
    connection.execute("DELETE FROM delta_audit", []).unwrap();
    store
        .transact("many", |s| {
            s.publish_job(Some(&principal()), &lease, 110, |s| {
                s.complete_entity(key.entity, [7; 32])?;
                Ok(JobOutput::Processed(ProcessOutcome::Complete))
            })
        })
        .unwrap();
    assert_eq!(
        changes(&connection),
        vec![
            ("queue".into(), "delete".into(), 2),
            ("storage".into(), "update".into(), 1)
        ]
    );
    let remaining = rowids(&connection);
    assert_eq!(remaining.len(), 254);
    for (key, row) in &remaining {
        assert_eq!(ids.get(key), Some(row));
    }
    connection.execute("DELETE FROM delta_audit", []).unwrap();
    store.transact("many", Session::revoke_access).unwrap();
    assert_eq!(
        changes(&connection),
        vec![
            ("queue".into(), "update".into(), 127),
            ("storage".into(), "update".into(), 1)
        ]
    );
    assert_eq!(rowids(&connection), remaining);
    assert_eq!(
        connection
            .query_row("SELECT rowid FROM pipestream_storage_sessions", [], |r| r
                .get::<_, i64>(
                0
            ))
            .unwrap(),
        storage_id
    );
    assert!(store.ready_jobs(1000, 128).unwrap().is_empty());
    store.integrity_check().unwrap();
    drop(connection);
    let reopened = SqliteSessionStore::open(store.path()).unwrap();
    assert_eq!(rowids(&reopened.connect().unwrap()), remaining);
    assert_eq!(reopened.job_queue_usage().unwrap().ordinary, 127);
}

#[test]
fn replacement_at_full_quota_releases_obsolete_rows_before_admitting_new_work() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir.path().join("delta.sqlite3"), 1);
    store.create(&session(1)).unwrap();
    let key = ExecutionKey {
        entity: EntityKey {
            scope_id: 0,
            entity_id: 1,
        },
        stage: ExecutionStage::Process,
    };
    let lease = store
        .transact("many", |s| s.acquire_job(Some(&principal()), key, 100, 50))
        .unwrap()
        .0
        .unwrap();
    let connection = store.connect().unwrap();
    connection.execute_batch("CREATE TRIGGER no_transient_overbook AFTER INSERT ON pipestream_jobs
        WHEN (SELECT count(*) FROM pipestream_jobs) > 2 BEGIN SELECT RAISE(ABORT, 'transient overbooking'); END;").unwrap();
    store
        .transact("many", |s| {
            s.publish_job(Some(&principal()), &lease, 110, |s| {
                s.complete_entity(key.entity, [7; 32])?;
                Ok(JobOutput::Processed(ProcessOutcome::Complete))
            })?;
            add_job(s, 2);
            Ok(())
        })
        .unwrap();
    assert_eq!(
        store.job_queue_usage().unwrap(),
        JobQueueUsage {
            ordinary: 1,
            rehydration_reserved: 1,
            rehydration_active: 0
        }
    );
    assert_eq!(store.ready_jobs(110, 1).unwrap()[0].key.entity.entity_id, 2);
    store.integrity_check().unwrap();
}

#[test]
fn failure_after_selective_deletion_restores_session_rows_and_reservations() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir.path().join("delta.sqlite3"), 2);
    store.create(&session(2)).unwrap();
    let key = ExecutionKey {
        entity: EntityKey {
            scope_id: 0,
            entity_id: 1,
        },
        stage: ExecutionStage::Process,
    };
    let lease = store
        .transact("many", |s| s.acquire_job(Some(&principal()), key, 100, 50))
        .unwrap()
        .0
        .unwrap();
    let before = store.load("many").unwrap().unwrap();
    let quota = store.storage_usage().unwrap();
    let connection = store.connect().unwrap();
    let ids = rowids(&connection);
    connection
        .execute_batch(
            "CREATE TRIGGER reject_final_accounting BEFORE UPDATE ON pipestream_storage_sessions
        BEGIN SELECT RAISE(ABORT, 'accounting after queue delta failed'); END;",
        )
        .unwrap();
    assert!(
        store
            .transact("many", |s| s.publish_job(
                Some(&principal()),
                &lease,
                110,
                |s| {
                    s.complete_entity(key.entity, [7; 32])?;
                    Ok(JobOutput::Processed(ProcessOutcome::Complete))
                }
            ))
            .is_err()
    );
    assert_eq!(store.load("many").unwrap().unwrap(), before);
    assert_eq!(store.storage_usage().unwrap(), quota);
    assert_eq!(rowids(&connection), ids);
    store.integrity_check().unwrap();
}

#[test]
fn insertion_failure_after_selective_deletion_restores_existing_work() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir.path().join("delta.sqlite3"), 1);
    store.create(&session(1)).unwrap();
    let key = ExecutionKey {
        entity: EntityKey {
            scope_id: 0,
            entity_id: 1,
        },
        stage: ExecutionStage::Process,
    };
    let lease = store
        .transact("many", |s| s.acquire_job(Some(&principal()), key, 100, 50))
        .unwrap()
        .0
        .unwrap();
    let before = store.load("many").unwrap().unwrap();
    let quota = store.storage_usage().unwrap();
    let connection = store.connect().unwrap();
    let ids = rowids(&connection);
    // This failure is reachable only after both obsolete entries were deleted.
    connection
        .execute_batch(
            "CREATE TRIGGER reject_replacement BEFORE INSERT ON pipestream_jobs
             WHEN NOT EXISTS(SELECT 1 FROM pipestream_jobs)
             BEGIN SELECT RAISE(ABORT, 'replacement after deletion failed'); END;",
        )
        .unwrap();
    let result = store.transact("many", |s| {
        s.publish_job(Some(&principal()), &lease, 110, |s| {
            s.complete_entity(key.entity, [7; 32])?;
            Ok(JobOutput::Processed(ProcessOutcome::Complete))
        })?;
        add_job(s, 2);
        Ok(())
    });
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("replacement after deletion failed")
    );
    assert_eq!(store.load("many").unwrap().unwrap(), before);
    assert_eq!(store.storage_usage().unwrap(), quota);
    assert_eq!(rowids(&connection), ids);
    store.integrity_check().unwrap();
    connection
        .execute("DROP TRIGGER reject_replacement", [])
        .unwrap();
    store
        .transact("many", |s| {
            s.publish_job(Some(&principal()), &lease, 110, |s| {
                s.complete_entity(key.entity, [7; 32])?;
                Ok(JobOutput::Processed(ProcessOutcome::Complete))
            })?;
            add_job(s, 2);
            Ok(())
        })
        .unwrap();
    assert_eq!(store.ready_jobs(110, 1).unwrap()[0].key.entity.entity_id, 2);
    store.integrity_check().unwrap();
}

#[test]
fn first_index_insertion_failure_cannot_leave_an_unaccounted_session() {
    for table in ["pipestream_jobs", "pipestream_storage_sessions"] {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir.path().join("delta.sqlite3"), 1);
        let connection = store.connect().unwrap();
        connection
            .execute_batch(&format!(
                "CREATE TRIGGER reject_new_index BEFORE INSERT ON {table}
             BEGIN SELECT RAISE(ABORT, 'new index insertion failed'); END;"
            ))
            .unwrap();
        assert!(
            store
                .create(&session(1))
                .unwrap_err()
                .to_string()
                .contains("new index insertion failed")
        );
        assert!(store.load("many").unwrap().is_none());
        assert!(rowids(&connection).is_empty());
        assert_eq!(store.job_queue_usage().unwrap(), JobQueueUsage::default());
        assert_eq!(store.storage_usage().unwrap(), StorageUsage::default());
        store.integrity_check().unwrap();
        connection
            .execute("DROP TRIGGER reject_new_index", [])
            .unwrap();
        store.create(&session(1)).unwrap();
        store.integrity_check().unwrap();
    }
}

#[test]
fn unchanged_index_reconciliation_emits_no_wal_frames_but_full_replacement_does() {
    for count in [1, 128, 512] {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir.path().join("delta.sqlite3"), count);
        let session = session(count);
        store.create(&session).unwrap();
        let mut connection = store.connect().unwrap();
        store.checkpoint().unwrap();
        let reader = store.connect().unwrap();
        reader
            .execute_batch("BEGIN; SELECT count(*) FROM pipestream_jobs")
            .unwrap();
        let before = store.physical_usage().unwrap().wal_bytes;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        queue::replace_index(&transaction, &session).unwrap();
        let (bytes, checksum) = encode_state(&session, store.storage_limits()).unwrap();
        storage::replace_index(&transaction, &session, bytes.len(), &checksum).unwrap();
        transaction.commit().unwrap();
        assert_eq!(store.physical_usage().unwrap().wal_bytes, before);
        // Counterfactual for identical retained state: the old reconciliation
        // policy rebuilt all rows even when no indexed value changed.
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        transaction
            .execute("DELETE FROM pipestream_jobs WHERE session_id = 'many'", [])
            .unwrap();
        transaction
            .execute(
                "DELETE FROM pipestream_storage_sessions WHERE session_id = 'many'",
                [],
            )
            .unwrap();
        queue::replace_index(&transaction, &session).unwrap();
        storage::replace_index(&transaction, &session, bytes.len(), &checksum).unwrap();
        transaction.commit().unwrap();
        let rebuilt = store.physical_usage().unwrap().wal_bytes;
        assert!(rebuilt > before);
        eprintln!(
            "unchanged index reconciliation: jobs={count}, delta_wal=0, full_replacement_wal={}",
            rebuilt - before
        );
        store.integrity_check().unwrap();
    }
}
