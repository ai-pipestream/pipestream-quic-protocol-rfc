use super::*;
use crate::{
    authorization::PrincipalBinding,
    jobs::{JobOutput, ProcessOutcome, tests::fixture},
};

fn queued(id: &str, principal: Option<PrincipalBinding>, now: u64) -> Session {
    let (mut session, key, input) = fixture(id, principal);
    session.enqueue_job(key, input, now).unwrap();
    session
}

#[test]
fn global_and_principal_queue_limits_are_atomic_and_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("queue.sqlite3");
    let limits = JobQueueLimits {
        total: 3,
        per_principal: 2,
        ..JobQueueLimits::default()
    };
    let store = SqliteSessionStore::open_with_job_limits(&path, limits).unwrap();
    let alice = PrincipalBinding::new("issuer", "alice").unwrap();
    let bob = PrincipalBinding::new("issuer", "bob").unwrap();
    store
        .create(&queued("alice-1", Some(alice.clone()), 100))
        .unwrap();
    store
        .create(&queued("alice-2", Some(alice.clone()), 100))
        .unwrap();
    let error = store
        .create(&queued("alice-overflow", Some(alice), 100))
        .unwrap_err();
    assert!(matches!(error, StoreError::Protocol(e) if e.code == crate::ERROR_LIMIT_EXCEEDED));
    assert!(store.load("alice-overflow").unwrap().is_none());
    store.create(&queued("bob-1", Some(bob), 100)).unwrap();
    assert!(
        store
            .create(&queued("anonymous-overflow", None, 100))
            .is_err()
    );
    assert_eq!(store.unfinished_job_count().unwrap(), 3);
    drop(store);
    let reopened = SqliteSessionStore::open(&path).unwrap();
    assert_eq!(reopened.job_limits(), limits);
    assert_eq!(reopened.ready_jobs(100, 3).unwrap().len(), 3);
    assert!(SqliteSessionStore::open_with_job_limits(&path, JobQueueLimits::default()).is_err());
    assert_eq!(reopened.unfinished_job_count().unwrap(), 3);
}

#[test]
fn admission_rolls_back_when_queue_is_full_for_create_save_and_transact() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteSessionStore::open_with_job_limits(
        dir.path().join("queue.sqlite3"),
        JobQueueLimits {
            total: 1,
            per_principal: 1,
            ..JobQueueLimits::default()
        },
    )
    .unwrap();
    store.create(&queued("occupant", None, 100)).unwrap();
    let (mut session, key, input) = fixture("not-admitted", None);
    let before = store.create(&session).unwrap();
    assert!(
        store
            .transact(&session.session_id, |s| s.enqueue_job(
                key,
                input.clone(),
                100
            ))
            .is_err()
    );
    assert_eq!(store.load(&session.session_id).unwrap().unwrap(), before);
    session.enqueue_job(key, input, 100).unwrap();
    assert!(store.save(before.revision, &session).is_err());
    assert_eq!(store.load(&session.session_id).unwrap().unwrap(), before);
    assert_eq!(store.unfinished_job_count().unwrap(), 1);
    assert_eq!(store.ready_jobs(100, 1).unwrap()[0].session_id, "occupant");
}

#[test]
fn acquisition_expiry_completion_and_revocation_update_discovery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("queue.sqlite3");
    let store = SqliteSessionStore::open(&path).unwrap();
    let alice = PrincipalBinding::new("issuer", "alice").unwrap();
    let (mut session, key, input) = fixture("work", Some(alice.clone()));
    session.enqueue_job(key, input, 100).unwrap();
    store.create(&session).unwrap();
    assert!(store.ready_jobs(99, 1).unwrap().is_empty());
    let ready = store.ready_jobs(100, 1).unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].key, key);
    assert_eq!(ready[0].principal.as_ref(), Some(&alice));
    let lease = store
        .transact("work", |s| s.acquire_job(Some(&alice), key, 100, 50))
        .unwrap()
        .0
        .unwrap();
    assert!(store.ready_jobs(149, 1).unwrap().is_empty());
    assert_eq!(store.ready_jobs(150, 1).unwrap(), ready);
    store
        .transact("work", |s| {
            s.publish_job(Some(&alice), &lease, 140, |s| {
                s.complete_entity(key.entity, [1; 32])?;
                Ok(JobOutput::Processed(ProcessOutcome::Complete))
            })
        })
        .unwrap();
    assert_eq!(store.unfinished_job_count().unwrap(), 0);
    assert!(store.ready_jobs(200, 1).unwrap().is_empty());
    store.create(&queued("revoked", Some(alice), 100)).unwrap();
    store.transact("revoked", Session::revoke_access).unwrap();
    drop(store);
    let store = SqliteSessionStore::open(&path).unwrap();
    assert!(store.ready_jobs(200, 1).unwrap().is_empty());
    assert_eq!(
        store.unfinished_job_count().unwrap(),
        1,
        "revocation does not erase outstanding work"
    );
}

#[test]
fn concurrent_handles_cannot_overbook_the_persistent_queue() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("queue.sqlite3");
    let store = SqliteSessionStore::open_with_job_limits(
        &path,
        JobQueueLimits {
            total: 3,
            per_principal: 2,
            ..JobQueueLimits::default()
        },
    )
    .unwrap();
    let start = std::sync::Barrier::new(8);
    let accepted = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..8)
            .map(|id| {
                let start = &start;
                let path = &path;
                scope.spawn(move || {
                    let store = SqliteSessionStore::open(path).unwrap();
                    start.wait();
                    store
                        .create(&queued(&format!("job-{id}"), None, 100))
                        .is_ok()
                })
            })
            .collect();
        handles
            .into_iter()
            .filter_map(|handle| handle.join().unwrap().then_some(()))
            .count()
    });
    assert_eq!(
        accepted, 2,
        "all anonymous connections share one principal budget"
    );
    assert_eq!(store.unfinished_job_count().unwrap(), 2);
    assert_eq!(store.list_session_ids().unwrap().len(), 2);
    store.integrity_check().unwrap();
}

#[test]
fn queue_write_failure_rolls_back_session_and_index_together() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("queue.sqlite3");
    let store = SqliteSessionStore::open(&path).unwrap();
    let session = queued("work", None, 100);
    let key = *session.jobs.keys().next().unwrap();
    let before = store.create(&session).unwrap();
    let ready = store.ready_jobs(100, 1).unwrap();
    let connection = Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TRIGGER fail_queue BEFORE INSERT ON pipestream_jobs BEGIN SELECT RAISE(ABORT, 'injected queue failure'); END;").unwrap();
    assert!(
        store
            .transact("work", |s| s.acquire_job(None, key, 100, 50))
            .is_err()
    );
    assert_eq!(store.load("work").unwrap().unwrap(), before);
    assert_eq!(store.ready_jobs(100, 1).unwrap(), ready);
    assert_eq!(store.unfinished_job_count().unwrap(), 1);
}

#[test]
fn bounded_discovery_and_invalid_timestamps_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteSessionStore::open(dir.path().join("queue.sqlite3")).unwrap();
    assert!(store.ready_jobs(1, 0).is_err());
    assert!(store.ready_jobs(1, store.job_limits().total + 1).is_err());
    assert!(store.ready_jobs(u64::MAX, 1).is_err());
    assert!(store.create(&queued("far-future", None, u64::MAX)).is_err());
    assert!(store.load("far-future").unwrap().is_none());
    assert_eq!(store.unfinished_job_count().unwrap(), 0);
    for id in 1..=3 {
        store
            .create(&queued(&format!("work-{id}"), None, id))
            .unwrap();
    }
    assert_eq!(store.ready_jobs(3, 1).unwrap().len(), 1);
    assert_eq!(store.ready_jobs(2, 3).unwrap().len(), 2);
}

#[test]
fn abrupt_process_exit_preserves_the_committed_job_and_fence() {
    const CHILD_DB: &str = "PIPESTREAM_JOB_CRASH_TEST_DB";
    if let Some(path) = std::env::var_os(CHILD_DB) {
        let store = SqliteSessionStore::open(path).unwrap();
        let session = queued("crashed", None, 100);
        let key = *session.jobs.keys().next().unwrap();
        store.create(&session).unwrap();
        store
            .transact("crashed", |s| s.acquire_job(None, key, 100, 50))
            .unwrap();
        std::process::exit(0);
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("queue.sqlite3");
    let result = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "persistence::queue_tests::abrupt_process_exit_preserves_the_committed_job_and_fence",
            "--nocapture",
        ])
        .env(CHILD_DB, &path)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let store = SqliteSessionStore::open(&path).unwrap();
    assert!(store.ready_jobs(149, 1).unwrap().is_empty());
    let ready = store.ready_jobs(150, 1).unwrap();
    assert_eq!(ready.len(), 1);
    let lease = store
        .transact("crashed", |s| s.acquire_job(None, ready[0].key, 150, 50))
        .unwrap()
        .0
        .unwrap();
    assert_eq!(lease.epoch(), 2);
    assert!(
        store
            .load("crashed")
            .unwrap()
            .unwrap()
            .session
            .final_lineage_digest()
            .is_err()
    );
    store.integrity_check().unwrap();
}

#[test]
fn integrity_audit_detects_missing_extra_and_changed_queue_rows() {
    for mutation in [
        "DELETE FROM pipestream_jobs",
        "UPDATE pipestream_jobs SET ready_at_micros = 101 WHERE reserved = 0",
        "UPDATE pipestream_jobs SET ready_at_micros = NULL",
        "UPDATE pipestream_jobs SET principal = x'00ff'",
        "INSERT INTO pipestream_jobs SELECT session_id, CAST(execution_key || x'ff' AS BLOB), principal, ready_at_micros, enqueued_at_micros, rehydration, reserved FROM pipestream_jobs",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.sqlite3");
        let store = SqliteSessionStore::open(&path).unwrap();
        store.create(&queued("work", None, 100)).unwrap();
        store.integrity_check().unwrap();
        Connection::open(&path)
            .unwrap()
            .execute_batch(mutation)
            .unwrap();
        assert!(
            matches!(store.integrity_check(), Err(StoreError::Corrupt(_))),
            "{mutation}"
        );
    }
}

#[test]
fn save_cannot_delete_jobs_change_inputs_or_rewrite_terminal_outcomes() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteSessionStore::open(dir.path().join("queue.sqlite3")).unwrap();
    let session = queued("work", None, 100);
    let key = *session.jobs.keys().next().unwrap();
    let initial = store.create(&session).unwrap();
    let mut removed = session.clone();
    removed.jobs.clear();
    assert!(store.save(initial.revision, &removed).is_err());
    let mut changed = session;
    changed.jobs.get_mut(&key).unwrap().enqueued_at_micros += 1;
    assert!(store.save(initial.revision, &changed).is_err());
    assert_eq!(store.load("work").unwrap().unwrap(), initial);
    let lease = store
        .transact("work", |s| s.acquire_job(None, key, 100, 50))
        .unwrap()
        .0
        .unwrap();
    let finished = store
        .transact("work", |s| {
            s.publish_job(None, &lease, 110, |s| {
                s.complete_entity(key.entity, [1; 32])?;
                Ok(JobOutput::Processed(ProcessOutcome::Complete))
            })
        })
        .unwrap()
        .1;
    let mut changed = finished.session.clone();
    changed.jobs.get_mut(&key).unwrap().state =
        crate::jobs::JobState::Finished(JobOutput::Processed(ProcessOutcome::Failed));
    assert!(store.save(finished.revision, &changed).is_err());
    assert_eq!(store.load("work").unwrap().unwrap(), finished);
    store.integrity_check().unwrap();
}

#[test]
fn missing_queue_schema_cannot_be_recreated_as_empty_over_existing_work() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("queue.sqlite3");
    let store = SqliteSessionStore::open(&path).unwrap();
    store.create(&queued("work", None, 100)).unwrap();
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch("DROP TABLE pipestream_jobs")
        .unwrap();
    assert!(matches!(
        SqliteSessionStore::open(&path),
        Err(StoreError::Corrupt(_))
    ));
    let count: u32 = connection
        .query_row(
            "SELECT count(*) FROM sqlite_schema WHERE name = 'pipestream_jobs'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 0, "refusal must not manufacture an empty queue");
    assert_eq!(store.load("work").unwrap().unwrap().session.jobs.len(), 1);

    let other = dir.path().join("missing-policy.sqlite3");
    let store = SqliteSessionStore::open(&other).unwrap();
    store.create(&queued("work", None, 100)).unwrap();
    let connection = Connection::open(&other).unwrap();
    connection
        .execute_batch("DELETE FROM pipestream_job_limits")
        .unwrap();
    assert!(matches!(
        SqliteSessionStore::open(&other),
        Err(StoreError::Corrupt(_))
    ));
    let count: u32 = connection
        .query_row("SELECT count(*) FROM pipestream_job_limits", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(
        count, 0,
        "missing policy must not be replaced with fresh default limits"
    );
}
