use super::*;
use crate::{
    ERROR_ENTITY_INVALID, ERROR_UNAUTHORIZED,
    persistence::{SessionStore, SqliteSessionStore, StoreError},
    session::NewEntity,
};

fn fixture() -> (Session, PrincipalBinding, ExecutionKey) {
    let principal = PrincipalBinding::new("issuer", "alice").unwrap();
    let mut session = Session::new("execution-1", 7, 100).unwrap();
    session.bind_owner(principal.clone()).unwrap();
    let entity = session
        .add_root(NewEntity {
            entity_id: 1,
            layer: 0,
            payload_digest: [1; 32],
            policy: None,
        })
        .unwrap();
    session.transition(entity, EntityState::Processing).unwrap();
    (
        session,
        principal,
        ExecutionKey {
            entity,
            stage: ExecutionStage::Process,
        },
    )
}

#[test]
fn expired_attempt_is_fenced_after_reopen_and_reacquisition() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite3");
    let (session, caller, key) = fixture();
    let store = SqliteSessionStore::open(&path).unwrap();
    store.create(&session).unwrap();
    let old = store
        .transact("execution-1", |s| {
            s.acquire_execution(Some(&caller), key, 100, 50)
        })
        .unwrap()
        .0
        .unwrap();
    assert_eq!(old.epoch(), 1);
    assert!(
        store
            .transact("execution-1", |s| s.acquire_execution(
                Some(&caller),
                key,
                149,
                50
            ))
            .unwrap()
            .0
            .is_none()
    );
    drop(store);
    let store = SqliteSessionStore::open(&path).unwrap();
    let new = store
        .transact("execution-1", |s| {
            s.acquire_execution(Some(&caller), key, 150, 50)
        })
        .unwrap()
        .0
        .unwrap();
    assert_eq!(new.epoch(), 2);
    let before = store.load("execution-1").unwrap().unwrap();
    for now in [149, 150, 160] {
        let error = store
            .transact("execution-1", |s| {
                s.publish_execution(Some(&caller), &old, now, |s| {
                    s.complete_entity(key.entity, [9; 32])
                })
            })
            .unwrap_err();
        assert!(matches!(error, StoreError::Protocol(e) if e.code == ERROR_ENTITY_INVALID));
        assert_eq!(store.load("execution-1").unwrap().unwrap(), before);
    }
    store
        .transact("execution-1", |s| {
            s.publish_execution(Some(&caller), &new, 160, |s| {
                s.complete_entity(key.entity, [2; 32])
            })
        })
        .unwrap();
    drop(store);
    let store = SqliteSessionStore::open(&path).unwrap();
    let completed = store.load("execution-1").unwrap().unwrap();
    assert_eq!(
        completed.session.entities[&key.entity].output_digest,
        Some([2; 32])
    );
    assert_eq!(
        completed.session.executions[&key].completed_at_micros,
        Some(160)
    );
    assert!(
        store
            .transact("execution-1", |s| s.publish_execution(
                Some(&caller),
                &new,
                170,
                |_| Ok(())
            ))
            .is_err()
    );
    assert_eq!(completed, store.load("execution-1").unwrap().unwrap());
    assert!(
        store
            .transact("execution-1", |s| s.acquire_execution(
                Some(&caller),
                key,
                300,
                50
            ))
            .unwrap()
            .0
            .is_none()
    );
    store.integrity_check().unwrap();
}

#[test]
fn expired_lease_cannot_publish_even_without_a_replacement() {
    let (mut session, caller, key) = fixture();
    let lease = session
        .acquire_execution(Some(&caller), key, 100, 50)
        .unwrap()
        .unwrap();
    let before = session.clone();
    for now in [99, 150, 151] {
        assert!(
            session
                .publish_execution(Some(&caller), &lease, now, |s| s
                    .complete_entity(key.entity, [2; 32]))
                .is_err()
        );
        assert_eq!(session, before);
    }
}

#[test]
fn caller_authority_session_and_revocation_are_checked_on_both_sides() {
    let (mut session, caller, key) = fixture();
    let lease = session
        .acquire_execution(Some(&caller), key, 100, 50)
        .unwrap()
        .unwrap();
    let bob = PrincipalBinding::new("issuer", "bob").unwrap();
    let elsewhere = PrincipalBinding::new("elsewhere", "alice").unwrap();
    let before = session.clone();
    for identity in [None, Some(&bob), Some(&elsewhere)] {
        assert_eq!(
            session
                .acquire_execution(identity, key, 150, 50)
                .unwrap_err()
                .code,
            ERROR_UNAUTHORIZED
        );
        assert_eq!(
            session
                .publish_execution(identity, &lease, 120, |_| Ok(()))
                .unwrap_err()
                .code,
            ERROR_UNAUTHORIZED
        );
        assert_eq!(session, before);
    }
    let mut other_session = session.clone();
    other_session.session_id = "other-session".into();
    assert!(
        other_session
            .publish_execution(Some(&caller), &lease, 120, |_| Ok(()))
            .is_err()
    );
    session.revoke_access().unwrap();
    let revoked = session.clone();
    assert_eq!(
        session
            .publish_execution(Some(&caller), &lease, 120, |_| Ok(()))
            .unwrap_err()
            .code,
        ERROR_UNAUTHORIZED
    );
    assert_eq!(
        session
            .acquire_execution(Some(&caller), key, 150, 50)
            .unwrap_err()
            .code,
        ERROR_UNAUTHORIZED
    );
    assert_eq!(session, revoked);
}

#[test]
fn failed_publication_rolls_back_both_the_result_and_attempt_completion() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteSessionStore::open(dir.path().join("state.sqlite3")).unwrap();
    let (session, caller, key) = fixture();
    store.create(&session).unwrap();
    let lease = store
        .transact("execution-1", |s| {
            s.acquire_execution(Some(&caller), key, 100, 50)
        })
        .unwrap()
        .0
        .unwrap();
    let before = store.load("execution-1").unwrap().unwrap();
    let failed: Result<_, StoreError> = store.transact("execution-1", |s| {
        s.publish_execution(Some(&caller), &lease, 120, |s| {
            s.complete_entity(key.entity, [2; 32])?;
            Err::<(), _>(ProtocolError::entity("injected publication failure"))
        })
    });
    assert!(failed.is_err());
    assert_eq!(store.load("execution-1").unwrap().unwrap(), before);
    store
        .transact("execution-1", |s| {
            s.publish_execution(Some(&caller), &lease, 125, |s| {
                s.complete_entity(key.entity, [3; 32])
            })
        })
        .unwrap();
}

#[test]
fn concurrent_store_handles_grant_exactly_one_live_attempt() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite3");
    let (session, caller, key) = fixture();
    SqliteSessionStore::open(&path)
        .unwrap()
        .create(&session)
        .unwrap();
    let barrier = std::sync::Barrier::new(8);
    let leases = std::thread::scope(|scope| {
        let threads: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    let store = SqliteSessionStore::open(&path).unwrap();
                    barrier.wait();
                    store
                        .transact("execution-1", |s| {
                            s.acquire_execution(Some(&caller), key, 100, 50)
                        })
                        .unwrap()
                        .0
                })
            })
            .collect();
        threads
            .into_iter()
            .filter_map(|t| t.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].epoch(), 1);
}

#[test]
fn stage_mismatch_clock_overflow_and_epoch_exhaustion_do_not_mutate() {
    let (mut session, caller, key) = fixture();
    let before = session.clone();
    for stage in [
        ExecutionStage::Rehydrate,
        ExecutionStage::Resume { claim_id: 99 },
    ] {
        assert!(
            session
                .acquire_execution(Some(&caller), ExecutionKey { stage, ..key }, 100, 50)
                .is_err()
        );
        assert_eq!(session, before);
    }
    for (now, duration) in [
        (100, 0),
        (100, MAX_EXECUTION_LEASE_MICROS + 1),
        (u64::MAX, 1),
    ] {
        assert!(
            session
                .acquire_execution(Some(&caller), key, now, duration)
                .is_err()
        );
        assert_eq!(session, before);
    }
    session
        .acquire_execution(Some(&caller), key, 100, 50)
        .unwrap();
    session.executions.get_mut(&key).unwrap().epoch = u64::MAX;
    let before = session.clone();
    assert!(
        session
            .acquire_execution(Some(&caller), key, 150, 50)
            .is_err()
    );
    assert_eq!(session, before);
}
