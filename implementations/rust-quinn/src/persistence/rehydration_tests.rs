use super::*;
use crate::{
    LayerSupport,
    authorization::PrincipalBinding,
    execution::{ExecutionKey, ExecutionStage},
    jobs::{JobInput, JobOutput, JobState, ProcessOutcome, tests::fixture},
    session::{EntityKey, EntityState, NewEntity},
};

fn queued(id: &str, owner: Option<PrincipalBinding>) -> (Session, ExecutionKey) {
    let (mut session, key, mut input) = fixture(id, owner);
    let JobInput::Process { layers, .. } = &mut input else {
        unreachable!()
    };
    *layers = LayerSupport::LAYER1;
    session.enqueue_job(key, input, 1).unwrap();
    (session, key)
}

fn waiting(id: &str, owner: Option<PrincipalBinding>, scope: u32) -> (Session, ExecutionKey) {
    let (mut session, key) = queued(id, owner.clone());
    let lease = session
        .acquire_job(owner.as_ref(), key, 1, 100)
        .unwrap()
        .unwrap();
    session
        .publish_job(owner.as_ref(), &lease, 2, |s| {
            s.begin_dehydrating(key.entity)?;
            Ok(JobOutput::Processed(ProcessOutcome::Dehydrate))
        })
        .unwrap();
    session.open_child_scope(key.entity, scope, 2).unwrap();
    let child = session
        .add_child(
            scope,
            NewEntity {
                entity_id: crate::MAX_ENTITY_ID,
                layer: 0,
                payload_digest: [1; 32],
                policy: None,
            },
        )
        .unwrap();
    session.transition(child, EntityState::Processing).unwrap();
    session.complete_entity(child, [2; 32]).unwrap();
    (
        session,
        ExecutionKey {
            stage: ExecutionStage::Rehydrate,
            ..key
        },
    )
}

fn close(session: &mut Session, scope: u32, key: ExecutionKey) -> Result<(), ProtocolError> {
    let digest = session.close_scope(scope)?;
    session.begin_rehydration(key.entity)?;
    session.enqueue_job(key, JobInput::Rehydrate { digest }, 128)
}

fn charge(session: &Session) -> usize {
    postcard::to_stdvec(session).unwrap().len()
        + storage::completion_reservation(session, StorageLimits::default()).unwrap()
}

fn assert_limit(error: StoreError) {
    assert!(
        matches!(error, StoreError::Protocol(error) if error.code == crate::ERROR_LIMIT_EXCEEDED)
    );
}

#[test]
fn scope_closure_and_publication_use_reserved_bytes_and_slots_at_capacity() {
    for scope in [127, 128, 16_384, u32::MAX] {
        let (session, key) = waiting("parent", None, scope);
        let (occupant, _) = queued("occupant", None);
        let total = charge(&session) + charge(&occupant);
        let dir = tempfile::tempdir().unwrap();
        let store = SqliteSessionStore::open_with_limits(
            dir.path().join("state.sqlite3"),
            JobQueueLimits {
                total: 1,
                per_principal: 1,
                rehydration_total: 2,
                rehydration_per_principal: 2,
            },
            StorageLimits {
                total_bytes: total as u64,
                principal_bytes: total as u64,
                record_bytes: charge(&session).max(charge(&occupant)),
                ..StorageLimits::default()
            },
        )
        .unwrap();
        store.create(&session).unwrap();
        store.create(&occupant).unwrap();
        let before = store.storage_usage().unwrap();
        assert_eq!(before.charged_bytes(), total as u64);
        assert_eq!(
            store.job_queue_usage().unwrap(),
            JobQueueUsage {
                ordinary: 1,
                rehydration_reserved: 2,
                rehydration_active: 0,
            }
        );
        assert_limit(store.create(&queued("overflow", None).0).unwrap_err());
        store.transact("parent", |s| close(s, scope, key)).unwrap();
        assert_eq!(
            store.job_queue_usage().unwrap(),
            JobQueueUsage {
                ordinary: 1,
                rehydration_reserved: 1,
                rehydration_active: 1,
            }
        );
        assert_eq!(store.unfinished_job_count().unwrap(), 2);
        assert!(store.storage_usage().unwrap().charged_bytes() <= before.charged_bytes());
        assert_eq!(store.ready_jobs(128, 1).unwrap()[0].key, key);
        let lease = store
            .transact("parent", |s| s.acquire_job(None, key, 128, 100))
            .unwrap()
            .0
            .unwrap();
        store
            .transact("parent", |s| {
                s.publish_job(None, &lease, 129, |s| {
                    let JobInput::Rehydrate { digest } = s.jobs[&key].input.clone() else {
                        unreachable!()
                    };
                    s.complete_rehydration(key.entity, [255; 32])?;
                    Ok(JobOutput::Rehydrated(digest))
                })
            })
            .unwrap();
        assert!(store.storage_usage().unwrap().charged_bytes() <= before.charged_bytes());
        assert_eq!(
            store.job_queue_usage().unwrap(),
            JobQueueUsage {
                ordinary: 1,
                rehydration_reserved: 1,
                rehydration_active: 0,
            }
        );
        store.integrity_check().unwrap();
    }
}

#[test]
fn future_slot_limits_are_atomic_per_owner_and_retain_revoked_work() {
    let dir = tempfile::tempdir().unwrap();
    let limits = JobQueueLimits {
        rehydration_total: 2,
        rehydration_per_principal: 1,
        ..JobQueueLimits::default()
    };
    let store =
        SqliteSessionStore::open_with_job_limits(dir.path().join("state.sqlite3"), limits).unwrap();
    let alice = PrincipalBinding::new("issuer", "alice").unwrap();
    let barrier = std::sync::Barrier::new(2);
    let accepted = std::thread::scope(|threads| {
        let handles: Vec<_> = ["alice1", "alice2"]
            .into_iter()
            .map(|id| {
                let barrier = &barrier;
                let path = store.path();
                let alice = &alice;
                threads.spawn(move || {
                    let store = SqliteSessionStore::open(path).unwrap();
                    barrier.wait();
                    store.create(&waiting(id, Some(alice.clone()), 1).0)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(accepted.iter().filter(|value| value.is_ok()).count(), 1);
    let retained = accepted.into_iter().find_map(Result::ok).unwrap();
    store
        .transact(&retained.session.session_id, Session::revoke_access)
        .unwrap();
    assert_limit(store.create(&queued("alice3", Some(alice)).0).unwrap_err());
    store
        .create(
            &waiting(
                "bob",
                Some(PrincipalBinding::new("issuer", "bob").unwrap()),
                1,
            )
            .0,
        )
        .unwrap();
    assert_limit(store.create(&queued("carol", None).0).unwrap_err());
    assert_eq!(
        store.job_queue_usage().unwrap(),
        JobQueueUsage {
            ordinary: 0,
            rehydration_reserved: 2,
            rehydration_active: 0,
        }
    );
    let reopened = SqliteSessionStore::open(store.path()).unwrap();
    assert_eq!(reopened.job_limits(), limits);
    assert_eq!(
        reopened.job_queue_usage().unwrap(),
        store.job_queue_usage().unwrap()
    );
}

#[test]
fn closure_failure_rolls_back_reservation_conversion_and_retry_can_finish() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteSessionStore::open(dir.path().join("state.sqlite3")).unwrap();
    let (session, key) = waiting("parent", None, 1);
    let before = store.create(&session).unwrap();
    let usage = store.job_queue_usage().unwrap();
    let bytes = store.storage_usage().unwrap();
    let connection = Connection::open(store.path()).unwrap();
    connection.execute_batch("CREATE TRIGGER fail_rehydration BEFORE INSERT ON pipestream_jobs
        WHEN NEW.rehydration = 1 AND NEW.reserved = 0 BEGIN SELECT RAISE(ABORT, 'injected closure failure'); END;").unwrap();
    assert!(store.transact("parent", |s| close(s, 1, key)).is_err());
    assert_eq!(store.load("parent").unwrap().unwrap(), before);
    assert_eq!(store.job_queue_usage().unwrap(), usage);
    assert_eq!(store.storage_usage().unwrap(), bytes);
    connection
        .execute_batch("DROP TRIGGER fail_rehydration")
        .unwrap();
    store.transact("parent", |s| close(s, 1, key)).unwrap();
    store.integrity_check().unwrap();
}

#[test]
fn missing_or_altered_future_slots_cannot_admit_unrelated_work() {
    for mutation in [
        "DELETE FROM pipestream_jobs WHERE reserved = 1",
        "UPDATE pipestream_jobs SET reserved = 0 WHERE reserved = 1",
        "UPDATE pipestream_jobs SET principal = x'00ff' WHERE reserved = 1",
        "UPDATE pipestream_jobs SET enqueued_at_micros = 99 WHERE reserved = 1",
        "PRAGMA ignore_check_constraints = ON; UPDATE pipestream_jobs SET reserved = 2 WHERE reserved = 1; PRAGMA ignore_check_constraints = OFF;",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let store = SqliteSessionStore::open(dir.path().join("state.sqlite3")).unwrap();
        let (session, _) = waiting("parent", None, 1);
        let before = store.create(&session).unwrap();
        Connection::open(store.path())
            .unwrap()
            .execute_batch(mutation)
            .unwrap();
        assert!(
            matches!(store.job_queue_usage(), Err(StoreError::Corrupt(_))),
            "{mutation}"
        );
        assert!(
            matches!(
                store.create(&queued("intruder", None).0),
                Err(StoreError::Corrupt(_))
            ),
            "{mutation}"
        );
        assert!(store.load("intruder").unwrap().is_none());
        assert_eq!(store.load("parent").unwrap().unwrap(), before);
    }
}

#[test]
fn abrupt_exit_retains_future_or_converted_rehydration_without_double_charge() {
    const CHILD: &str = "PIPESTREAM_FUTURE_REHYDRATION_CRASH_DB";
    const ACQUIRED: &str = "PIPESTREAM_FUTURE_REHYDRATION_ACQUIRED";
    if let Some(path) = std::env::var_os(CHILD) {
        let store = SqliteSessionStore::open(path).unwrap();
        let (session, key) = waiting("parent", None, 1);
        store.create(&session).unwrap();
        if std::env::var_os(ACQUIRED).is_some() {
            store.transact("parent", |s| close(s, 1, key)).unwrap();
            store
                .transact("parent", |s| s.acquire_job(None, key, 128, 100))
                .unwrap();
        }
        std::process::exit(37);
    }
    for acquired in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite3");
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command.args(["--exact", "persistence::rehydration_tests::abrupt_exit_retains_future_or_converted_rehydration_without_double_charge", "--nocapture"]).env(CHILD, &path);
        if acquired {
            command.env(ACQUIRED, "1");
        }
        let output = command.output().unwrap();
        assert_eq!(
            output.status.code(),
            Some(37),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let store = SqliteSessionStore::open(path).unwrap();
        let key = ExecutionKey {
            entity: EntityKey {
                scope_id: 0,
                entity_id: 1,
            },
            stage: ExecutionStage::Rehydrate,
        };
        assert_eq!(
            store.job_queue_usage().unwrap(),
            JobQueueUsage {
                ordinary: 0,
                rehydration_reserved: u32::from(!acquired),
                rehydration_active: u32::from(acquired),
            }
        );
        let before = store.storage_usage().unwrap();
        if !acquired {
            store.transact("parent", |s| close(s, 1, key)).unwrap();
        }
        let lease = store
            .transact("parent", |s| s.acquire_job(None, key, 228, 100))
            .unwrap()
            .0
            .unwrap();
        assert_eq!(lease.epoch(), if acquired { 2 } else { 1 });
        store
            .transact("parent", |s| {
                s.refuse_job(None, &lease, 229, &ProtocolError::limit("x".repeat(512)))
            })
            .unwrap();
        assert!(store.storage_usage().unwrap().charged_bytes() <= before.charged_bytes());
        assert_eq!(store.job_queue_usage().unwrap(), JobQueueUsage::default());
        let state = store.load("parent").unwrap().unwrap().session;
        assert_eq!(state.entities[&key.entity].state, EntityState::Rehydrating);
        assert!(state.final_lineage_digest().is_err());
        assert!(matches!(state.jobs[&key].state, JobState::Refused(_)));
    }
}

#[test]
fn discovery_interleaves_principals_before_filling_a_bounded_page() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteSessionStore::open(dir.path().join("state.sqlite3")).unwrap();
    for (id, principal) in [("alice1", "alice"), ("alice2", "alice"), ("bob", "bob")] {
        let (mut session, key) = waiting(
            id,
            Some(PrincipalBinding::new("issuer", principal).unwrap()),
            1,
        );
        close(&mut session, 1, key).unwrap();
        store.create(&session).unwrap();
    }
    let ready = store.ready_jobs(128, 2).unwrap();
    assert_eq!(ready.len(), 2);
    assert_eq!(ready[0].principal.as_ref().unwrap().principal, "alice");
    assert_eq!(ready[1].principal.as_ref().unwrap().principal, "bob");
}

#[test]
fn unfunded_future_bytes_refuse_admission_for_create_save_and_transaction() {
    let (mut session, key, mut input) = fixture("unfunded", None);
    let JobInput::Process { layers, .. } = &mut input else {
        unreachable!()
    };
    *layers = LayerSupport::LAYER1;
    let capacity = postcard::to_stdvec(&session).unwrap().len();
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteSessionStore::open_with_limits(
        dir.path().join("state.sqlite3"),
        JobQueueLimits::default(),
        StorageLimits {
            total_bytes: capacity as u64,
            principal_bytes: capacity as u64,
            record_bytes: capacity,
            ..StorageLimits::default()
        },
    )
    .unwrap();
    let before = store.create(&session).unwrap();
    assert_limit(
        store
            .transact("unfunded", |s| s.enqueue_job(key, input.clone(), 1))
            .unwrap_err(),
    );
    session.enqueue_job(key, input, 1).unwrap();
    assert_limit(store.save(before.revision, &session).unwrap_err());
    assert_eq!(store.load("unfunded").unwrap().unwrap(), before);
    assert_eq!(store.job_queue_usage().unwrap(), JobQueueUsage::default());
    let other = SqliteSessionStore::open_with_limits(
        dir.path().join("create.sqlite3"),
        JobQueueLimits::default(),
        store.storage_limits(),
    )
    .unwrap();
    assert_limit(other.create(&session).unwrap_err());
    assert!(other.load("unfunded").unwrap().is_none());
}

#[test]
fn older_queue_and_storage_policies_are_refused_without_conversion() {
    for queue_policy in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite3");
        let store = SqliteSessionStore::open(&path).unwrap();
        store.create(&waiting("parent", None, 1).0).unwrap();
        let connection = Connection::open(&path).unwrap();
        if queue_policy {
            connection.execute_batch("ALTER TABLE pipestream_job_limits RENAME TO previous_job_limits;
                CREATE TABLE pipestream_job_limits (singleton INTEGER PRIMARY KEY, total INTEGER NOT NULL, per_principal INTEGER NOT NULL) STRICT;
                INSERT INTO pipestream_job_limits SELECT singleton, total, per_principal FROM previous_job_limits;
                DROP TABLE previous_job_limits;").unwrap();
        } else {
            connection
                .execute_batch(
                    "PRAGMA ignore_check_constraints = ON;
                UPDATE pipestream_storage_limits SET version = 2;
                PRAGMA ignore_check_constraints = OFF;",
                )
                .unwrap();
        }
        let snapshot = || -> (Vec<u8>, i64, Vec<(String, String)>) {
            let (state, revision) = connection
                .query_row(
                    "SELECT state, revision FROM pipestream_sessions",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            let schema = connection
                .prepare("SELECT name, sql FROM sqlite_schema WHERE sql IS NOT NULL ORDER BY name")
                .unwrap()
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            (state, revision, schema)
        };
        let before = snapshot();
        drop(store);
        assert!(matches!(
            SqliteSessionStore::open(&path),
            Err(StoreError::Corrupt(_))
        ));
        assert_eq!(snapshot(), before);
    }
}

#[test]
fn many_rehydrations_convert_map_prefix_credit_once() {
    let (mut session, first, mut input) = fixture("map-conversion", None);
    let JobInput::Process { layers, .. } = &mut input else {
        unreachable!()
    };
    *layers = LayerSupport::LAYER1;
    for id in 1..=64 {
        let key = ExecutionKey {
            entity: EntityKey {
                scope_id: 0,
                entity_id: id,
            },
            ..first
        };
        if id != 1 {
            session
                .add_root(NewEntity {
                    entity_id: id,
                    layer: 0,
                    payload_digest: session.entities[&first.entity].payload_digest,
                    policy: None,
                })
                .unwrap();
            session
                .transition(key.entity, EntityState::Processing)
                .unwrap();
        }
        let mut input = input.clone();
        let JobInput::Process { header, .. } = &mut input else {
            unreachable!()
        };
        header.entity_id = id;
        session.enqueue_job(key, input, 1).unwrap();
        let lease = session.acquire_job(None, key, 1, 100).unwrap().unwrap();
        session
            .publish_job(None, &lease, 2, |s| {
                s.begin_dehydrating(key.entity)?;
                Ok(JobOutput::Processed(ProcessOutcome::Dehydrate))
            })
            .unwrap();
        session.open_child_scope(key.entity, id, 2).unwrap();
        let child = session
            .add_child(
                id,
                NewEntity {
                    entity_id: 1,
                    layer: 0,
                    payload_digest: [1; 32],
                    policy: None,
                },
            )
            .unwrap();
        session.transition(child, EntityState::Processing).unwrap();
        session.complete_entity(child, [2; 32]).unwrap();
    }
    let mut before = charge(&session);
    for id in 1..=64 {
        let key = ExecutionKey {
            entity: EntityKey {
                scope_id: 0,
                entity_id: id,
            },
            stage: ExecutionStage::Rehydrate,
        };
        close(&mut session, id, key).unwrap();
        assert!(charge(&session) <= before);
        before = charge(&session);
        let lease = session.acquire_job(None, key, 128, 100).unwrap().unwrap();
        assert_eq!(charge(&session), before);
        session
            .publish_job(None, &lease, 129, |s| {
                let digest = s.scope_digest(id)?;
                s.complete_rehydration(key.entity, [255; 32])?;
                Ok(JobOutput::Rehydrated(digest))
            })
            .unwrap();
        assert!(charge(&session) <= before);
        before = charge(&session);
    }
    assert_eq!(session.jobs.len(), 128);
    assert_eq!(session.executions.len(), 128);
    assert_eq!(session.future_rehydrations().count(), 0);
    assert_eq!(
        storage::completion_reservation(&session, StorageLimits::default()).unwrap(),
        0
    );
    session.validate_jobs().unwrap();
}

#[test]
fn future_queue_policy_bounds_cannot_be_disabled_or_changed_on_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite3");
    for (total, principal) in [(0, 1), (65_537, 1), (10, 0), (10, 11)] {
        assert_limit(
            SqliteSessionStore::open_with_job_limits(
                &path,
                JobQueueLimits {
                    rehydration_total: total,
                    rehydration_per_principal: principal,
                    ..JobQueueLimits::default()
                },
            )
            .unwrap_err(),
        );
        assert!(!path.exists());
    }
    let store = SqliteSessionStore::open(&path).unwrap();
    let before = store.create(&waiting("parent", None, 1).0).unwrap();
    assert_limit(
        SqliteSessionStore::open_with_job_limits(
            &path,
            JobQueueLimits {
                rehydration_per_principal: 1,
                ..JobQueueLimits::default()
            },
        )
        .unwrap_err(),
    );
    assert_eq!(store.load("parent").unwrap().unwrap(), before);
}
