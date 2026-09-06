use super::*;
use crate::{
    authorization::PrincipalBinding,
    jobs::{JobOutput, ProcessOutcome, tests::fixture},
    persistence::{JobQueueLimits, SessionStore, SqliteSessionStore, StorageLimits},
    session::Session,
};

fn limit(error: StoreError) {
    assert!(
        matches!(error, StoreError::Protocol(ref e) if e.code == crate::ERROR_LIMIT_EXCEEDED),
        "{error}"
    );
}

fn saturate(store: &SqliteSessionStore) {
    let mut other = store.load("other").unwrap().unwrap();
    for _ in 0..10000 {
        match store.save(other.revision, &other.session) {
            Ok(next) => other = next,
            Err(error) => {
                limit(error);
                assert_eq!(store.load("other").unwrap().unwrap(), other);
                return;
            }
        }
    }
    panic!("ordinary writes did not reach their reserved ceiling");
}

fn configured(path: &Path, wal_bytes: u64) -> SqliteSessionStore {
    SqliteSessionStore::open_with_all_limits(
        path,
        JobQueueLimits::default(),
        StorageLimits::default(),
        PhysicalLimits {
            database_bytes: 8 << 20,
            wal_bytes,
            journal_bytes: 1 << 20,
            shared_memory_bytes: 64 << 10,
        },
    )
    .unwrap()
}

#[test]
fn unfulfillable_completion_promise_is_refused_before_admission() {
    let dir = tempfile::tempdir().unwrap();
    let store = configured(&dir.path().join("unfunded.sqlite3"), 256 << 10);
    let (mut session, key, input) = fixture("unfunded", None);
    session.enqueue_job(key, input, 100).unwrap();
    limit(store.create(&session).unwrap_err());
    assert!(store.load("unfunded").unwrap().is_none());
    assert_eq!(store.unfinished_job_count().unwrap(), 0);
    assert_eq!(store.storage_usage().unwrap(), Default::default());
    store.integrity_check().unwrap();
}

#[test]
fn queued_work_for_two_owners_acquires_and_publishes_after_saturation_and_reopen() {
    for page_size in [512, 4096, 65536] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("owners.sqlite3");
        let wal_bytes = if page_size == 65536 { 8 << 20 } else { 2 << 20 };
        let guard = Guard::open(
            &path,
            Some(PhysicalLimits {
                database_bytes: 8 << 20,
                wal_bytes,
                journal_bytes: 1 << 20,
                shared_memory_bytes: 64 << 10,
            }),
        )
        .unwrap();
        let setup = rusqlite::Connection::open_with_flags_and_vfs(
            &path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            VFS_NAME,
        )
        .unwrap();
        setup
            .execute_batch(&format!(
                "PRAGMA page_size={page_size}; CREATE TABLE geometry_probe(value INTEGER)"
            ))
            .unwrap();
        drop(setup);
        let store = configured(&path, wal_bytes);
        let owners = [
            PrincipalBinding::new("issuer", "alice").unwrap(),
            PrincipalBinding::new("issuer", "bob").unwrap(),
        ];
        let mut keys = Vec::new();
        for owner in &owners {
            let (mut session, key, input) = fixture(&owner.principal, Some(owner.clone()));
            session.enqueue_job(key, input, 100).unwrap();
            store.create(&session).unwrap();
            keys.push(key);
        }
        store
            .create(&Session::new("other", 7, 32).unwrap())
            .unwrap();
        store.checkpoint().unwrap();
        let reader = store.connect().unwrap();
        reader
            .execute_batch("BEGIN; SELECT count(*) FROM pipestream_sessions")
            .unwrap();
        saturate(&store);
        drop(store);
        let store = SqliteSessionStore::open(&path).unwrap();
        for (owner, key) in owners.iter().zip(keys) {
            let lease = store
                .transact(&owner.principal, |s| {
                    s.acquire_job(Some(owner), key, 100, 1_000_000)
                })
                .unwrap()
                .0
                .unwrap();
            saturate(&store);
            store
                .transact(&owner.principal, |s| {
                    s.publish_job(Some(owner), &lease, 200, |s| {
                        // Materialize the full promised token, not just a tiny COMPLETE.
                        s.defer_with_claim_id(
                            key.entity,
                            vec![7; 64 << 10],
                            crate::StoppingPointValidation {
                                state_checksum: Some([7; 32]),
                                bytes_processed: Some(u64::MAX),
                                children_complete: Some(u64::MAX),
                                children_total: Some(u64::MAX),
                                is_resumable: Some(true),
                                checkpoint_ref: Some("x".repeat(256)),
                            },
                            u64::MAX,
                            u64::MAX,
                            200,
                        )?;
                        Ok(JobOutput::Processed(ProcessOutcome::Deferred {
                            reason: 5,
                            claim_id: u64::MAX,
                        }))
                    })
                })
                .unwrap();
            assert_eq!(
                store
                    .load(&owner.principal)
                    .unwrap()
                    .unwrap()
                    .session
                    .claims[&u64::MAX]
                    .token,
                vec![7; 64 << 10]
            );
            saturate(&store);
        }
        assert_eq!(store.unfinished_job_count().unwrap(), 0);
        assert!(store.physical_usage().unwrap().wal_bytes <= wal_bytes);
        store.integrity_check().unwrap();
        drop(reader);
        store.checkpoint().unwrap();
        drop(guard);
    }
}

#[test]
fn expired_reacquisition_cannot_spend_another_jobs_publication_credit() {
    let dir = tempfile::tempdir().unwrap();
    let store = configured(&dir.path().join("expiry.sqlite3"), 2 << 20);
    let mut keys = Vec::new();
    let mut leases = Vec::new();
    for (id, ttl) in [("expired", 50), ("valid", 1_000_000)] {
        let (mut session, key, input) = fixture(id, None);
        session.enqueue_job(key, input, 100).unwrap();
        store.create(&session).unwrap();
        keys.push(key);
        leases.push(
            store
                .transact(id, |s| s.acquire_job(None, key, 100, ttl))
                .unwrap()
                .0
                .unwrap(),
        );
    }
    store
        .create(&Session::new("other", 7, 32).unwrap())
        .unwrap();
    store.checkpoint().unwrap();
    let reader = store.connect().unwrap();
    reader
        .execute_batch("BEGIN; SELECT count(*) FROM pipestream_sessions")
        .unwrap();
    saturate(&store);
    let before = store.load("expired").unwrap();
    limit(
        store
            .transact("expired", |s| s.acquire_job(None, keys[0], 200, 100))
            .unwrap_err(),
    );
    assert_eq!(store.load("expired").unwrap(), before);
    store
        .transact("valid", |s| {
            s.publish_job(None, &leases[1], 200, |s| {
                s.complete_entity(keys[1].entity, [7; 32])?;
                Ok(JobOutput::Processed(ProcessOutcome::Complete))
            })
        })
        .unwrap();
    let lease = store
        .transact("expired", |s| s.acquire_job(None, keys[0], 200, 100))
        .unwrap()
        .0
        .unwrap();
    assert_eq!(lease.epoch(), 2);
    store
        .transact("expired", |s| {
            s.publish_job(None, &lease, 201, |s| {
                s.complete_entity(keys[0].entity, [7; 32])?;
                Ok(JobOutput::Processed(ProcessOutcome::Complete))
            })
        })
        .unwrap();
    store.integrity_check().unwrap();
}

#[test]
fn concurrent_handles_cannot_overbook_physical_completion_promises() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("concurrent.sqlite3");
    let store = configured(&path, 1 << 20);
    let start = std::sync::Barrier::new(2);
    let results = std::thread::scope(|scope| {
        let handles: Vec<_> = ["alice", "bob"]
            .into_iter()
            .map(|id| {
                let path = &path;
                let start = &start;
                scope.spawn(move || {
                    let owner = PrincipalBinding::new("issuer", id).unwrap();
                    let (mut session, key, input) = fixture(id, Some(owner));
                    session.enqueue_job(key, input, 100).unwrap();
                    let store = SqliteSessionStore::open(path).unwrap();
                    start.wait();
                    store.create(&session)
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
            limit(error);
        }
    }
    assert_eq!(store.unfinished_job_count().unwrap(), 1);
    store.integrity_check().unwrap();
}

#[test]
fn future_rehydration_converts_acquires_and_publishes_at_the_wal_ceiling() {
    use crate::{
        execution::{ExecutionKey, ExecutionStage},
        jobs::JobInput,
        session::{EntityState, NewEntity},
    };
    let dir = tempfile::tempdir().unwrap();
    let store = configured(&dir.path().join("rehydrate.sqlite3"), 1 << 20);
    let (mut session, process, input) = fixture("parent", None);
    session.enqueue_job(process, input, 100).unwrap();
    store.create(&session).unwrap();
    let lease = store
        .transact("parent", |s| s.acquire_job(None, process, 100, 1_000_000))
        .unwrap()
        .0
        .unwrap();
    store
        .transact("parent", |s| {
            s.publish_job(None, &lease, 200, |s| {
                s.begin_dehydrating(process.entity)?;
                Ok(JobOutput::Processed(ProcessOutcome::Dehydrate))
            })
        })
        .unwrap();
    store
        .transact("parent", |s| {
            s.open_child_scope(process.entity, 1, 1)?;
            let child = s.add_child(
                1,
                NewEntity {
                    entity_id: 1,
                    layer: 0,
                    payload_digest: [1; 32],
                    policy: None,
                },
            )?;
            s.transition(child, EntityState::Processing)?;
            s.complete_entity(child, [2; 32])
        })
        .unwrap();
    store
        .create(&Session::new("other", 7, 32).unwrap())
        .unwrap();
    store.checkpoint().unwrap();
    let reader = store.connect().unwrap();
    reader
        .execute_batch("BEGIN; SELECT count(*) FROM pipestream_sessions")
        .unwrap();
    saturate(&store);
    let key = ExecutionKey {
        stage: ExecutionStage::Rehydrate,
        ..process
    };
    let digest = store
        .transact("parent", |s| {
            let digest = s.close_scope(1)?;
            s.begin_rehydration(process.entity)?;
            s.enqueue_job(
                key,
                JobInput::Rehydrate {
                    digest: digest.clone(),
                },
                300,
            )?;
            Ok(digest)
        })
        .unwrap()
        .0;
    saturate(&store);
    let lease = store
        .transact("parent", |s| s.acquire_job(None, key, 300, 1_000_000))
        .unwrap()
        .0
        .unwrap();
    saturate(&store);
    store
        .transact("parent", |s| {
            s.publish_job(None, &lease, 301, |s| {
                s.complete_rehydration(key.entity, [7; 32])?;
                Ok(JobOutput::Rehydrated(digest))
            })
        })
        .unwrap();
    assert_eq!(store.unfinished_job_count().unwrap(), 0);
    assert_eq!(
        store.load("parent").unwrap().unwrap().session.entities[&key.entity].state,
        EntityState::Complete
    );
    store.integrity_check().unwrap();
}

#[test]
fn authenticated_resume_retains_its_receipt_and_publication_at_saturation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("resume.sqlite3");
    let store = configured(&path, 1 << 20);
    let owner = PrincipalBinding::new("issuer", "alice").unwrap();
    let (mut session, process, _) = fixture("resume", Some(owner.clone()));
    session
        .defer_with_claim_id(
            process.entity,
            vec![7; 64 << 10],
            crate::StoppingPointValidation {
                state_checksum: Some([7; 32]),
                bytes_processed: None,
                children_complete: None,
                children_total: None,
                is_resumable: Some(true),
                checkpoint_ref: None,
            },
            99,
            10_000,
            100,
        )
        .unwrap();
    store.create(&session).unwrap();
    let request = crate::recovery::RecoveryRequest {
        authority: owner.authority.clone(),
        session_id: "resume".into(),
        request_id: [1; 16],
        claim_id: 99,
        state_checksum: [7; 32],
    };
    let receipt = store
        .transact("resume", |s| s.accept_recovery(Some(&owner), &request, 200))
        .unwrap()
        .0;
    store
        .create(&Session::new("other", 7, 32).unwrap())
        .unwrap();
    store.checkpoint().unwrap();
    let reader = store.connect().unwrap();
    reader
        .execute_batch("BEGIN; SELECT count(*) FROM pipestream_sessions")
        .unwrap();
    saturate(&store);
    drop(store);
    let store = SqliteSessionStore::open(&path).unwrap();
    let key = receipt.execution_key();
    let lease = store
        .transact("resume", |s| {
            s.acquire_job(Some(&owner), key, 300, 1_000_000)
        })
        .unwrap()
        .0
        .unwrap();
    saturate(&store);
    store
        .transact("resume", |s| {
            s.publish_job(Some(&owner), &lease, 301, |s| {
                s.complete_entity(key.entity, [9; 32])?;
                Ok(JobOutput::Resumed)
            })
        })
        .unwrap();
    assert_eq!(
        store
            .transact("resume", |s| s.accept_recovery(Some(&owner), &request, 400))
            .unwrap()
            .0,
        receipt
    );
    let retained = store.load("resume").unwrap().unwrap().session;
    assert_eq!(retained.jobs.len(), 1);
    assert_eq!(
        retained.jobs[&key].state,
        crate::jobs::JobState::Finished(JobOutput::Resumed)
    );
    store.integrity_check().unwrap();
}

#[test]
fn abrupt_exit_after_admission_keeps_completion_funded_in_the_retained_wal() {
    const CHILD_DB: &str = "PIPESTREAM_COMPLETION_CRASH_DB";
    if let Some(path) = std::env::var_os(CHILD_DB) {
        let store = SqliteSessionStore::open(path).unwrap();
        let (mut session, key, input) = fixture("crashed", None);
        session.enqueue_job(key, input, 100).unwrap();
        store.create(&session).unwrap();
        std::process::exit(0);
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("crash.sqlite3");
    let store = configured(&path, 1 << 20);
    store
        .create(&Session::new("other", 7, 32).unwrap())
        .unwrap();
    store.checkpoint().unwrap();
    let reader = store.connect().unwrap();
    reader
        .execute_batch("BEGIN; SELECT count(*) FROM pipestream_sessions")
        .unwrap();
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact").arg("persistence::physical::reservation_tests::abrupt_exit_after_admission_keeps_completion_funded_in_the_retained_wal")
        .env(CHILD_DB, &path).output().unwrap();
    assert!(
        child.status.success(),
        "{}",
        String::from_utf8_lossy(&child.stderr)
    );
    assert!(store.physical_usage().unwrap().wal_bytes > 0);
    saturate(&store);
    let key = *store
        .load("crashed")
        .unwrap()
        .unwrap()
        .session
        .jobs
        .keys()
        .next()
        .unwrap();
    let lease = store
        .transact("crashed", |s| s.acquire_job(None, key, 200, 1_000_000))
        .unwrap()
        .0
        .unwrap();
    saturate(&store);
    store
        .transact("crashed", |s| {
            s.publish_job(None, &lease, 201, |s| {
                s.complete_entity(key.entity, [7; 32])?;
                Ok(JobOutput::Processed(ProcessOutcome::Complete))
            })
        })
        .unwrap();
    store.integrity_check().unwrap();
}

#[test]
fn unrelated_writes_cannot_spend_an_admitted_jobs_publication_space() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteSessionStore::open_with_all_limits(
        dir.path().join("completion.sqlite3"),
        JobQueueLimits::default(),
        StorageLimits::default(),
        PhysicalLimits {
            database_bytes: 1 << 20,
            // The Layer 2 fixture reserves five future writes of its maximum
            // token-bearing image. Admission now refuses the old 256 KiB cap;
            // fund that promise, then saturate ordinary writes as before.
            wal_bytes: 1 << 20,
            journal_bytes: 1 << 20,
            shared_memory_bytes: 64 << 10,
        },
    )
    .unwrap();
    let (mut session, key, input) = fixture("admitted", None);
    session.enqueue_job(key, input, 100).unwrap();
    store.create(&session).unwrap();
    let lease = store
        .transact("admitted", |s| s.acquire_job(None, key, 100, 1_000_000))
        .unwrap()
        .0
        .unwrap();
    let mut unrelated = store
        .create(&Session::new("other", 7, 32).unwrap())
        .unwrap();
    store.checkpoint().unwrap();
    let reader = store.connect().unwrap();
    reader
        .execute_batch("BEGIN; SELECT count(*) FROM pipestream_sessions")
        .unwrap();
    let mut refused = false;
    for _ in 0..1000 {
        match store.save(unrelated.revision, &unrelated.session) {
            Ok(next) => unrelated = next,
            Err(StoreError::Protocol(error)) if error.code == crate::ERROR_LIMIT_EXCEEDED => {
                refused = true;
                break;
            }
            Err(error) => panic!("unexpected admission error: {error}"),
        }
    }
    assert!(
        refused,
        "unrelated writes must stop before consuming completion credit"
    );
    assert_eq!(store.load("other").unwrap().unwrap(), unrelated);
    let before = store.physical_usage().unwrap();
    store
        .transact("admitted", |s| {
            s.publish_job(None, &lease, 200, |s| {
                s.complete_entity(key.entity, [7; 32])?;
                Ok(JobOutput::Processed(ProcessOutcome::Complete))
            })
        })
        .expect("admitted publication must fit while the WAL reader remains pinned");
    let after = store.physical_usage().unwrap();
    assert!(after.wal_bytes >= before.wal_bytes);
    assert!(after.wal_bytes <= store.physical_limits().wal_bytes);
    assert_eq!(store.unfinished_job_count().unwrap(), 0);
    store.integrity_check().unwrap();
    drop(reader);
    store.checkpoint().unwrap();
    let reopened = SqliteSessionStore::open(store.path()).unwrap();
    assert_eq!(
        reopened.load("admitted").unwrap(),
        store.load("admitted").unwrap()
    );
    reopened.integrity_check().unwrap();
}
