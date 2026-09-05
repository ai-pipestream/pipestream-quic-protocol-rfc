use super::*;
use crate::{
    StoppingPointValidation,
    jobs::{JobOutput, JobState},
    persistence::{SessionStore, SqliteSessionStore},
    session::{EntityState, NewEntity},
};

fn fixture() -> (Session, PrincipalBinding, RecoveryRequest) {
    let owner = PrincipalBinding::new("issuer", "alice").unwrap();
    let mut session = Session::new("recovery", 7, 100).unwrap();
    session.bind_owner(owner.clone()).unwrap();
    let entity = session
        .add_root(NewEntity {
            entity_id: 1,
            layer: 0,
            payload_digest: [1; 32],
            policy: None,
        })
        .unwrap();
    session.transition(entity, EntityState::Processing).unwrap();
    session
        .defer_with_claim_id(
            entity,
            b"resume".to_vec(),
            StoppingPointValidation {
                state_checksum: Some([2; 32]),
                bytes_processed: None,
                children_complete: None,
                children_total: None,
                is_resumable: Some(true),
                checkpoint_ref: None,
            },
            99,
            1_000,
            10,
        )
        .unwrap();
    let request = RecoveryRequest {
        authority: owner.authority.clone(),
        session_id: session.session_id.clone(),
        request_id: [3; 16],
        claim_id: 99,
        state_checksum: [2; 32],
    };
    (session, owner, request)
}

#[test]
fn receipt_replay_after_reopen_does_not_redeem_or_enqueue_again() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("session.sqlite3");
    let (session, owner, request) = fixture();
    let store = SqliteSessionStore::open(&path).unwrap();
    store.create(&session).unwrap();
    let first = store
        .transact("recovery", |s| {
            s.accept_recovery(Some(&owner), &request, 20)
        })
        .unwrap()
        .0;
    drop(store);
    let store = SqliteSessionStore::open(&path).unwrap();
    let (replayed, current) = store
        .transact("recovery", |s| {
            s.accept_recovery(Some(&owner), &request, 2_000)
        })
        .unwrap();
    assert_eq!(replayed, first); // Claim lifetime ended; only the retained receipt is replayed.
    assert_eq!(current.session.jobs.len(), 1);
    assert_eq!(current.session.claims[&99].redeemed_at_micros, Some(20));
    let key = first.execution_key();
    let lease = store
        .transact("recovery", |s| s.acquire_job(Some(&owner), key, 2_000, 100))
        .unwrap()
        .0
        .unwrap();
    store
        .transact("recovery", |s| {
            s.publish_job(Some(&owner), &lease, 2_001, |s| {
                s.complete_entity(key.entity, [4; 32])?;
                Ok(JobOutput::Resumed)
            })
        })
        .unwrap();
    assert_eq!(
        store
            .transact("recovery", |s| s.accept_recovery(
                Some(&owner),
                &request,
                3_000
            ))
            .unwrap()
            .0,
        first
    );
    assert_eq!(store.unfinished_job_count().unwrap(), 0);
    store.integrity_check().unwrap();
}

#[test]
fn unauthorized_changed_expired_and_revoked_replays_do_not_mutate_state() {
    let (mut session, owner, request) = fixture();
    let first = session.accept_recovery(Some(&owner), &request, 20).unwrap();
    let before = session.clone();
    for caller in [
        None,
        Some(PrincipalBinding::new("issuer", "bob").unwrap()),
        Some(PrincipalBinding::new("elsewhere", "alice").unwrap()),
    ] {
        assert_eq!(
            session
                .accept_recovery(caller.as_ref(), &request, 30)
                .unwrap_err()
                .code,
            crate::ERROR_UNAUTHORIZED
        );
        assert_eq!(session, before);
    }
    let mut changed = request.clone();
    changed.state_checksum = [5; 32];
    assert_eq!(
        session
            .accept_recovery(Some(&owner), &changed, 30)
            .unwrap_err()
            .code,
        crate::ERROR_ENTITY_INVALID
    );
    assert!(session.accept_recovery(Some(&owner), &request, 19).is_err());
    assert_eq!(
        session
            .accept_recovery(Some(&owner), &request, first.acceptance.retain_until_micros)
            .unwrap_err()
            .code,
        crate::ERROR_CLAIM_EXPIRED
    );
    assert_eq!(session, before);
    session.revoke_claim(99).unwrap();
    let revoked = session.clone();
    assert_eq!(
        session
            .accept_recovery(Some(&owner), &request, 30)
            .unwrap_err()
            .code,
        crate::ERROR_UNAUTHORIZED
    );
    assert_eq!(session, revoked);
    assert_eq!(session.jobs[&first.execution_key()].state, JobState::Queued);
}

#[test]
fn concurrent_requests_have_one_receipt_and_one_job() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite3");
    let (session, owner, request) = fixture();
    SqliteSessionStore::open(&path)
        .unwrap()
        .create(&session)
        .unwrap();
    let barrier = std::sync::Barrier::new(2);
    let receipts = std::thread::scope(|threads| {
        let handles: Vec<_> = (0..2)
            .map(|_| {
                threads.spawn(|| {
                    let store = SqliteSessionStore::open(&path).unwrap();
                    barrier.wait();
                    store
                        .transact("recovery", |s| {
                            s.accept_recovery(Some(&owner), &request, 20)
                        })
                        .unwrap()
                        .0
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(receipts[0], receipts[1]);
    let store = SqliteSessionStore::open(path).unwrap();
    assert_eq!(store.unfinished_job_count().unwrap(), 1);
    store.integrity_check().unwrap();
}

#[test]
fn revocation_fences_running_resume_and_cannot_be_erased_by_save() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite3");
    let (session, owner, request) = fixture();
    let store = SqliteSessionStore::open(&path).unwrap();
    store.create(&session).unwrap();
    let receipt = store
        .transact("recovery", |s| {
            s.accept_recovery(Some(&owner), &request, 20)
        })
        .unwrap()
        .0;
    let key = receipt.execution_key();
    let lease = store
        .transact("recovery", |s| s.acquire_job(Some(&owner), key, 21, 100))
        .unwrap()
        .0
        .unwrap();
    store.transact("recovery", |s| s.revoke_claim(99)).unwrap();
    assert!(store.ready_jobs(500, 10).unwrap().is_empty());
    assert_eq!(store.unfinished_job_count().unwrap(), 1);
    assert!(
        store
            .transact("recovery", |s| s.publish_job(
                Some(&owner),
                &lease,
                22,
                |_| unreachable!()
            ))
            .is_err()
    );
    let before = store.load("recovery").unwrap().unwrap();
    for mutation in 0..4 {
        let mut changed = before.session.clone();
        match mutation {
            0 => changed.recovery_receipts.clear(),
            1 => changed.revoked_claims.clear(),
            2 => changed.owner.as_mut().unwrap().binding.principal = "bob".into(),
            _ => {
                changed
                    .recovery_receipts
                    .get_mut(&request.request_id)
                    .unwrap()
                    .acceptance
                    .retain_until_micros += 1
            }
        }
        assert!(store.save(before.revision, &changed).is_err());
        assert_eq!(store.load("recovery").unwrap().unwrap(), before);
    }
    store.integrity_check().unwrap();
}

#[test]
fn wire_receipt_and_request_are_distinct_and_strict() {
    let (mut session, owner, request) = fixture();
    let receipt = session.accept_recovery(Some(&owner), &request, 20).unwrap();
    for frame in [
        RecoveryFrame::Request(request),
        RecoveryFrame::Receipt(receipt),
    ] {
        let bytes = encode(&frame).unwrap();
        let (kind, body) = crate::decode_ucf(&bytes).unwrap();
        assert_eq!(kind, FRAME_RECOVERY);
        assert_eq!(decode(body).unwrap(), frame);
        let mut trailing = body.to_vec();
        trailing.push(0);
        assert!(decode(&trailing).is_err());
        let mut flags = body.to_vec();
        flags[7] = 2;
        assert!(decode(&flags).is_err());
    }
}

#[test]
fn independent_frozen_recovery_vectors_pin_encodings_and_named_refusals() {
    for row in include_str!("../../../../test-vectors/authenticated-recovery.tsv")
        .lines()
        .skip(1)
    {
        let fields: Vec<_> = row.split('\t').collect();
        assert_eq!(fields.len(), 3);
        let bytes: Vec<u8> = fields[2]
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect();
        let (kind, body) = crate::decode_ucf(&bytes).unwrap();
        assert_eq!(kind, FRAME_RECOVERY);
        let result = decode(body);
        if fields[1] == "ok" {
            let frame = result.unwrap();
            assert_eq!(encode(&frame).unwrap(), bytes, "{}", fields[0]);
            if fields[0] == "request" {
                assert_eq!(frame, RecoveryFrame::Request(fixture().2));
            }
        } else {
            assert_eq!(result.unwrap_err().name, fields[1], "{}", fields[0]);
        }
    }
}

#[test]
fn retained_refusal_is_not_success_even_with_a_zero_diagnostic_code() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite3");
    let (session, owner, request) = fixture();
    let store = SqliteSessionStore::open(&path).unwrap();
    store.create(&session).unwrap();
    let receipt = store
        .transact("recovery", |s| {
            s.accept_recovery(Some(&owner), &request, 20)
        })
        .unwrap()
        .0;
    let key = receipt.execution_key();
    let lease = store
        .transact("recovery", |s| s.acquire_job(Some(&owner), key, 21, 100))
        .unwrap()
        .0
        .unwrap();
    let error = ProtocolError::new(0, "APPLICATION_REFUSAL", "operation declined");
    store
        .transact("recovery", |s| {
            s.refuse_job(Some(&owner), &lease, 22, &error)
        })
        .unwrap();
    drop(store);
    let store = SqliteSessionStore::open(path).unwrap();
    let (replayed, retained) = store
        .transact("recovery", |s| {
            s.accept_recovery(Some(&owner), &request, 30)
        })
        .unwrap();
    assert_eq!(replayed, receipt);
    let JobState::Refused(failure) = &retained.session.jobs[&key].state else {
        panic!("expected retained refusal");
    };
    assert_eq!(failure.code, 0);
    let outcome = RecoveryFrame::Outcome {
        receipt,
        outcome: RecoveryOutcome::Refused(failure.clone()),
    };
    assert_eq!(
        decode(crate::decode_ucf(&encode(&outcome).unwrap()).unwrap().1).unwrap(),
        outcome
    );
    assert_ne!(
        retained.session.entities[&key.entity].state,
        EntityState::Complete
    );
    for state in [
        JobState::Finished(JobOutput::Resumed),
        JobState::Refused(crate::jobs::JobFailure {
            code: 1,
            detail: "changed".into(),
        }),
    ] {
        let mut changed = retained.session.clone();
        changed.jobs.get_mut(&key).unwrap().state = state;
        assert!(store.save(retained.revision, &changed).is_err());
        assert_eq!(store.load("recovery").unwrap().unwrap(), retained);
    }
    assert!(store.ready_jobs(200, 1).unwrap().is_empty());
    store.integrity_check().unwrap();
}

#[test]
fn abrupt_exit_retains_acceptance_and_queued_resume_as_one_transaction() {
    const CHILD: &str = "PIPESTREAM_RECOVERY_CRASH_DIR";
    let (session, owner, request) = fixture();
    if let Some(path) = std::env::var_os(CHILD) {
        let store =
            SqliteSessionStore::open(std::path::Path::new(&path).join("state.sqlite3")).unwrap();
        store.create(&session).unwrap();
        store
            .transact("recovery", |s| {
                s.accept_recovery(Some(&owner), &request, 20)
            })
            .unwrap();
        std::process::exit(0);
    }
    let dir = tempfile::tempdir().unwrap();
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "recovery::tests::abrupt_exit_retains_acceptance_and_queued_resume_as_one_transaction",
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
    let before = store.load("recovery").unwrap().unwrap();
    assert_eq!(before.session.recovery_receipts.len(), 1);
    let receipt = store
        .transact("recovery", |s| {
            s.accept_recovery(Some(&owner), &request, 30)
        })
        .unwrap()
        .0;
    assert_eq!(
        receipt,
        before.session.recovery_receipts[&request.request_id]
    );
    assert_eq!(
        before.session.jobs[&receipt.execution_key()].state,
        JobState::Queued
    );
    assert_eq!(
        store.ready_jobs(30, 1).unwrap()[0].key,
        receipt.execution_key()
    );
    store.integrity_check().unwrap();
}

#[test]
fn queue_exhaustion_rolls_back_redemption_receipt_and_job() {
    let (mut session, owner, first) = fixture();
    let entity = session
        .add_root(NewEntity {
            entity_id: 2,
            layer: 0,
            payload_digest: [1; 32],
            policy: None,
        })
        .unwrap();
    session.transition(entity, EntityState::Processing).unwrap();
    session
        .defer_with_claim_id(
            entity,
            b"second".to_vec(),
            session.claims[&99].validation.clone(),
            100,
            1000,
            10,
        )
        .unwrap();
    let second = RecoveryRequest {
        claim_id: 100,
        request_id: [4; 16],
        ..first.clone()
    };
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteSessionStore::open_with_job_limits(
        dir.path().join("state.sqlite3"),
        crate::persistence::JobQueueLimits {
            total: 1,
            per_principal: 1,
        },
    )
    .unwrap();
    store.create(&session).unwrap();
    store
        .transact("recovery", |s| s.accept_recovery(Some(&owner), &first, 20))
        .unwrap();
    let before = store.load("recovery").unwrap().unwrap();
    assert!(
        store
            .transact("recovery", |s| s.accept_recovery(Some(&owner), &second, 21))
            .unwrap_err()
            .to_string()
            .contains("PIPESTREAM_LIMIT_EXCEEDED")
    );
    assert_eq!(store.load("recovery").unwrap().unwrap(), before);
    assert!(before.session.claims[&100].redeemed_at_micros.is_none());
    assert_eq!(before.session.recovery_receipts.len(), 1);
    assert_eq!(store.unfinished_job_count().unwrap(), 1);
    store.integrity_check().unwrap();
}

#[test]
fn new_expired_corrupt_or_overflowing_requests_cannot_be_accepted() {
    let (mut session, owner, request) = fixture();
    let before = session.clone();
    assert_eq!(
        session
            .accept_recovery(Some(&owner), &request, 1_000)
            .unwrap_err()
            .code,
        crate::ERROR_CLAIM_EXPIRED
    );
    assert_eq!(session, before);
    let mut corrupt = request.clone();
    corrupt.state_checksum = [0; 32];
    assert_eq!(
        session
            .accept_recovery(Some(&owner), &corrupt, 20)
            .unwrap_err()
            .code,
        crate::ERROR_INTEGRITY
    );
    assert_eq!(session, before);
    assert_eq!(
        session
            .accept_recovery(Some(&owner), &request, u64::MAX)
            .unwrap_err()
            .code,
        crate::ERROR_LIMIT_EXCEEDED
    );
    assert_eq!(session, before);
    session.revoke_access().unwrap();
    let revoked = session.clone();
    assert_eq!(
        session
            .accept_recovery(Some(&owner), &request, 20)
            .unwrap_err()
            .code,
        crate::ERROR_UNAUTHORIZED
    );
    assert_eq!(session, revoked);
}

#[test]
fn receipt_capacity_does_not_evict_replay_history_or_accept_more_work() {
    let (mut session, owner, first) = fixture();
    session.max_entities_per_scope = (MAX_RECOVERY_RECEIPTS + 1) as u32;
    let initial = session.accept_recovery(Some(&owner), &first, 20).unwrap();
    let mut extra = first.clone();
    for index in 1..=MAX_RECOVERY_RECEIPTS {
        let key = session
            .add_root(NewEntity {
                entity_id: index as u32 + 1,
                layer: 0,
                payload_digest: [1; 32],
                policy: None,
            })
            .unwrap();
        session.transition(key, EntityState::Processing).unwrap();
        session
            .defer_with_claim_id(
                key,
                b"resume".to_vec(),
                session.claims[&99].validation.clone(),
                99 + index as u64,
                1_000,
                10,
            )
            .unwrap();
        extra = RecoveryRequest {
            request_id: (index as u128).to_be_bytes(),
            claim_id: 99 + index as u64,
            ..first.clone()
        };
        if index < MAX_RECOVERY_RECEIPTS {
            session.accept_recovery(Some(&owner), &extra, 20).unwrap();
        }
    }
    let before = session.clone();
    assert_eq!(
        session
            .accept_recovery(Some(&owner), &extra, 20)
            .unwrap_err()
            .code,
        crate::ERROR_LIMIT_EXCEEDED
    );
    assert_eq!(session, before);
    assert_eq!(
        session.accept_recovery(Some(&owner), &first, 30).unwrap(),
        initial
    );
    session.validate_recovery().unwrap();
    assert_eq!(session.recovery_receipts.len(), MAX_RECOVERY_RECEIPTS);
}
