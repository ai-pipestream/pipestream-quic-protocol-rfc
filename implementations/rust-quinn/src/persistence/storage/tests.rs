use super::*;
use crate::session::{EntityKey, NewEntity};

fn session(id: &str, owner: Option<&str>) -> Session {
    let mut session = Session::new(id, 7, 100).unwrap();
    if let Some(owner) = owner {
        session
            .bind_owner(PrincipalBinding::new("issuer", owner).unwrap())
            .unwrap();
    }
    session
}

fn size(session: &Session) -> usize {
    postcard::to_stdvec(session).unwrap().len()
}

fn add_root(session: &mut Session) -> Result<EntityKey, ProtocolError> {
    session.add_root(NewEntity {
        entity_id: 1,
        layer: 0,
        payload_digest: [1; 32],
        policy: None,
    })
}

fn is_limit(error: StoreError) {
    assert!(
        matches!(error, StoreError::Protocol(error) if error.code == crate::ERROR_LIMIT_EXCEEDED)
    );
}

#[test]
fn global_and_principal_bytes_survive_reopen_without_policy_reset() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite3");
    let alice = session("one", Some("alice"));
    let bytes = size(&alice);
    let limits = StorageLimits {
        total_bytes: (bytes * 3) as u64,
        principal_bytes: (bytes * 2) as u64,
        record_bytes: bytes,
        sessions: 10,
        principal_sessions: 10,
        ..StorageLimits::default()
    };
    let store =
        SqliteSessionStore::open_with_limits(&path, JobQueueLimits::default(), limits).unwrap();
    store.create(&alice).unwrap();
    store.create(&session("two", Some("alice"))).unwrap();
    is_limit(store.create(&session("tri", Some("alice"))).unwrap_err());
    assert!(store.load("tri").unwrap().is_none());
    store.create(&session("tri", Some("bobby"))).unwrap();
    is_limit(store.create(&session("end", Some("bobby"))).unwrap_err());
    assert_eq!(
        store.storage_usage().unwrap(),
        StorageUsage {
            state_bytes: (bytes * 3) as u64,
            completion_reserved_bytes: 0,
            sessions: 3
        }
    );
    assert_eq!(
        store
            .principal_storage_usage(Some(&PrincipalBinding::new("issuer", "alice").unwrap()))
            .unwrap(),
        StorageUsage {
            state_bytes: (bytes * 2) as u64,
            completion_reserved_bytes: 0,
            sessions: 2
        }
    );
    assert_eq!(
        store.principal_storage_usage(None).unwrap(),
        StorageUsage::default()
    );
    drop(store);
    let reopened = SqliteSessionStore::open(&path).unwrap();
    assert_eq!(reopened.storage_limits(), limits);
    is_limit(
        SqliteSessionStore::open_with_limits(
            &path,
            JobQueueLimits::default(),
            StorageLimits::default(),
        )
        .unwrap_err(),
    );
    assert_eq!(reopened.storage_usage().unwrap().sessions, 3);
    reopened.integrity_check().unwrap();
}

#[test]
fn anonymous_and_authority_qualified_session_counts_are_separate() {
    let dir = tempfile::tempdir().unwrap();
    let limits = StorageLimits {
        sessions: 3,
        principal_sessions: 1,
        ..StorageLimits::default()
    };
    let store = SqliteSessionStore::open_with_limits(
        dir.path().join("state.sqlite3"),
        JobQueueLimits::default(),
        limits,
    )
    .unwrap();
    store.create(&session("anonymous", None)).unwrap();
    is_limit(store.create(&session("another", None)).unwrap_err());
    store.create(&session("alice", Some("alice"))).unwrap();
    let mut other = Session::new("other-authority", 7, 100).unwrap();
    other
        .bind_owner(PrincipalBinding::new("other", "alice").unwrap())
        .unwrap();
    store.create(&other).unwrap();
    is_limit(store.create(&session("bobby", Some("bobby"))).unwrap_err());
    assert_eq!(store.principal_storage_usage(None).unwrap().sessions, 1);
    assert_eq!(store.storage_usage().unwrap().sessions, 3);
    store.integrity_check().unwrap();
}

#[test]
fn failed_growth_rolls_back_state_revision_and_accounting_for_save_and_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let original = session("grow", None);
    let mut grown = original.clone();
    add_root(&mut grown).unwrap();
    let total = (size(&original) + size(&grown) - 1) as u64;
    let limits = StorageLimits {
        total_bytes: total,
        principal_bytes: total,
        record_bytes: size(&grown),
        sessions: 10,
        principal_sessions: 10,
        ..StorageLimits::default()
    };
    let store = SqliteSessionStore::open_with_limits(
        dir.path().join("state.sqlite3"),
        JobQueueLimits::default(),
        limits,
    )
    .unwrap();
    let before = store.create(&original).unwrap();
    store.create(&session("keep", None)).unwrap();
    let usage = store.storage_usage().unwrap();
    is_limit(store.save(before.revision, &grown).unwrap_err());
    assert_eq!(store.load("grow").unwrap().unwrap(), before);
    is_limit(store.transact("grow", add_root).unwrap_err());
    assert_eq!(store.load("grow").unwrap().unwrap(), before);
    assert_eq!(store.storage_usage().unwrap(), usage);
    store.integrity_check().unwrap();
}

#[test]
fn concurrent_handles_cannot_overbook_retained_session_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite3");
    let limits = StorageLimits {
        sessions: 1,
        principal_sessions: 1,
        ..StorageLimits::default()
    };
    let store =
        SqliteSessionStore::open_with_limits(&path, JobQueueLimits::default(), limits).unwrap();
    let barrier = std::sync::Barrier::new(2);
    let results = std::thread::scope(|threads| {
        let handles: Vec<_> = ["one", "two"]
            .into_iter()
            .map(|id| {
                threads.spawn(|| {
                    let store = SqliteSessionStore::open(&path).unwrap();
                    barrier.wait();
                    store.create(&session(id, None))
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    for result in results {
        if let Err(error) = result {
            is_limit(error);
        }
    }
    assert_eq!(store.storage_usage().unwrap().sessions, 1);
    store.integrity_check().unwrap();
}

#[test]
fn accounting_failure_rolls_back_session_and_job_index_together() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite3");
    let store = SqliteSessionStore::open(&path).unwrap();
    let (mut session, key, input) = crate::jobs::tests::fixture("queued", None);
    let before = store.create(&session).unwrap();
    let usage = store.storage_usage().unwrap();
    let conn = Connection::open(path).unwrap();
    conn.execute_batch("CREATE TRIGGER reject_accounting BEFORE INSERT ON pipestream_storage_sessions BEGIN SELECT RAISE(ABORT, 'injected accounting failure'); END;").unwrap();
    session.enqueue_job(key, input, 20).unwrap();
    assert!(store.save(before.revision, &session).is_err());
    assert_eq!(store.load("queued").unwrap().unwrap(), before);
    assert_eq!(store.storage_usage().unwrap(), usage);
    assert_eq!(store.unfinished_job_count().unwrap(), 0);
    store.integrity_check().unwrap();
}

#[test]
fn missing_or_changed_accounting_is_corruption_not_free_capacity() {
    for alteration in [
        "DELETE FROM pipestream_storage_sessions",
        "UPDATE pipestream_storage_sessions SET state_bytes = state_bytes + 1",
        "UPDATE pipestream_storage_sessions SET principal = x'00ff'",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite3");
        let store = SqliteSessionStore::open(&path).unwrap();
        store.create(&session("one", None)).unwrap();
        Connection::open(path)
            .unwrap()
            .execute_batch(alteration)
            .unwrap();
        assert!(matches!(store.load("one"), Err(StoreError::Corrupt(_))));
        assert!(store.integrity_check().is_err());
        assert!(store.create(&session("new", None)).is_err());
        assert!(store.load("new").unwrap().is_none());
    }
}

#[test]
fn missing_policy_or_old_unaccounted_store_is_not_reinitialized() {
    for alteration in [
        "DELETE FROM pipestream_storage_limits",
        "DROP TABLE pipestream_storage_sessions",
        "DROP TABLE pipestream_storage_limits",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite3");
        let store = SqliteSessionStore::open(&path).unwrap();
        store.create(&session("one", None)).unwrap();
        drop(store);
        let conn = Connection::open(&path).unwrap();
        let before: Vec<u8> = conn
            .query_row("SELECT state FROM pipestream_sessions", [], |r| r.get(0))
            .unwrap();
        conn.execute_batch(alteration).unwrap();
        assert!(SqliteSessionStore::open(&path).is_err());
        let after: Vec<u8> = conn
            .query_row("SELECT state FROM pipestream_sessions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(before, after);
        let available: u32 = conn.query_row("SELECT count(*) FROM sqlite_schema WHERE type='table' AND name LIKE 'pipestream_storage_%'", [], |r| r.get(0)).unwrap();
        assert_eq!(
            available,
            if alteration.starts_with("DROP") { 1 } else { 2 }
        );
    }
}

#[test]
fn bounded_serializer_matches_existing_bytes_and_stops_before_growth() {
    use postcard::ser_flavors::Flavor;
    let session = session("bounded", Some("alice"));
    let expected = postcard::to_stdvec(&session).unwrap();
    assert_eq!(encode(&session, expected.len()).unwrap(), expected);
    is_limit(encode(&session, expected.len() - 1).unwrap_err());
    let mut bytes = BoundedBytes {
        bytes: Vec::new(),
        limit: 101,
    };
    for _ in 0..101 {
        bytes.try_push(7).unwrap();
    }
    let capacity = bytes.bytes.capacity();
    assert!(capacity <= 101);
    assert!(bytes.try_extend(&[0; 4096]).is_err());
    assert_eq!(bytes.bytes.len(), 101);
    assert_eq!(bytes.bytes.capacity(), capacity);
}

#[test]
fn oversized_stored_blob_is_refused_before_deserialization() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite3");
    let limits = StorageLimits {
        record_bytes: 1024,
        ..StorageLimits::default()
    };
    let store =
        SqliteSessionStore::open_with_limits(&path, JobQueueLimits::default(), limits).unwrap();
    store.create(&session("one", None)).unwrap();
    Connection::open(path)
        .unwrap()
        .execute(
            "UPDATE pipestream_sessions SET state = zeroblob(1048576)",
            [],
        )
        .unwrap();
    assert!(
        matches!(store.load("one"), Err(StoreError::Corrupt(detail)) if detail == "stored session exceeds record budget")
    );
}

#[test]
fn abrupt_exit_preserves_committed_capacity_charges() {
    const CHILD: &str = "PIPESTREAM_STORAGE_CRASH_DIR";
    if let Some(path) = std::env::var_os(CHILD) {
        let limits = StorageLimits {
            sessions: 1,
            principal_sessions: 1,
            ..StorageLimits::default()
        };
        let store = SqliteSessionStore::open_with_limits(
            PathBuf::from(path).join("state.sqlite3"),
            JobQueueLimits::default(),
            limits,
        )
        .unwrap();
        store.create(&session("retained", None)).unwrap();
        std::process::exit(0);
    }
    let dir = tempfile::tempdir().unwrap();
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "persistence::storage::tests::abrupt_exit_preserves_committed_capacity_charges",
            "--nocapture",
        ])
        .env(CHILD, dir.path())
        .output()
        .unwrap();
    assert!(
        child.status.success(),
        "{}",
        String::from_utf8_lossy(&child.stderr)
    );
    let store = SqliteSessionStore::open(dir.path().join("state.sqlite3")).unwrap();
    assert_eq!(store.storage_usage().unwrap().sessions, 1);
    is_limit(store.create(&session("extra", None)).unwrap_err());
    store.integrity_check().unwrap();
}

#[test]
fn concurrent_reads_observe_state_and_accounting_in_one_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite3");
    let store = SqliteSessionStore::open(&path).unwrap();
    store.create(&session("growing", None)).unwrap();
    let barrier = std::sync::Barrier::new(2);
    std::thread::scope(|threads| {
        let writer = threads.spawn(|| {
            let store = SqliteSessionStore::open(&path).unwrap();
            barrier.wait();
            for id in 1..=100 {
                store
                    .transact("growing", |session| {
                        session.add_root(NewEntity {
                            entity_id: id,
                            layer: 0,
                            payload_digest: [1; 32],
                            policy: None,
                        })
                    })
                    .unwrap();
            }
        });
        barrier.wait();
        for _ in 0..150 {
            store.load("growing").unwrap().unwrap();
        }
        writer.join().unwrap();
    });
    assert_eq!(
        store
            .load("growing")
            .unwrap()
            .unwrap()
            .session
            .entities
            .len(),
        100
    );
    store.integrity_check().unwrap();
}

#[test]
fn finished_and_revoked_work_remains_charged() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteSessionStore::open(dir.path().join("state.sqlite3")).unwrap();
    let owner = PrincipalBinding::new("issuer", "alice").unwrap();
    let (mut session, key, input) = crate::jobs::tests::fixture("finished", Some(owner.clone()));
    session.enqueue_job(key, input, 10).unwrap();
    store.create(&session).unwrap();
    let lease = store
        .transact("finished", |s| s.acquire_job(Some(&owner), key, 11, 100))
        .unwrap()
        .0
        .unwrap();
    store
        .transact("finished", |s| {
            s.publish_job(Some(&owner), &lease, 12, |s| {
                s.complete_entity(key.entity, [2; 32])?;
                Ok(crate::jobs::JobOutput::Processed(
                    crate::jobs::ProcessOutcome::Complete,
                ))
            })
        })
        .unwrap();
    store.transact("finished", Session::revoke_access).unwrap();
    assert_eq!(store.unfinished_job_count().unwrap(), 0);
    let retained = store.load("finished").unwrap().unwrap().session;
    assert_eq!(
        store.principal_storage_usage(Some(&owner)).unwrap(),
        StorageUsage {
            sessions: 1,
            completion_reserved_bytes: 0,
            state_bytes: size(&retained) as u64
        }
    );
    store.integrity_check().unwrap();
}

#[test]
fn invalid_limits_and_missing_empty_store_policy_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    for invalid in [
        StorageLimits {
            total_bytes: u64::MAX,
            ..StorageLimits::default()
        },
        StorageLimits {
            record_bytes: 0,
            ..StorageLimits::default()
        },
        StorageLimits {
            record_bytes: 17 << 20,
            ..StorageLimits::default()
        },
        StorageLimits {
            sessions: 0,
            ..StorageLimits::default()
        },
        StorageLimits {
            principal_sessions: 5000,
            ..StorageLimits::default()
        },
        StorageLimits {
            yield_token_bytes: 0,
            ..StorageLimits::default()
        },
        StorageLimits {
            yield_token_bytes: 0x0100_0000,
            ..StorageLimits::default()
        },
    ] {
        let path = dir.path().join("invalid.sqlite3");
        is_limit(
            SqliteSessionStore::open_with_limits(&path, JobQueueLimits::default(), invalid)
                .unwrap_err(),
        );
        assert!(!path.exists());
    }
    let path = dir.path().join("empty.sqlite3");
    SqliteSessionStore::open(&path).unwrap();
    let conn = Connection::open(&path).unwrap();
    conn.execute("DELETE FROM pipestream_storage_limits", [])
        .unwrap();
    assert!(SqliteSessionStore::open(&path).is_err());
    assert_eq!(
        conn.query_row("SELECT count(*) FROM pipestream_storage_limits", [], |r| {
            r.get::<_, u32>(0)
        })
        .unwrap(),
        0
    );
}
