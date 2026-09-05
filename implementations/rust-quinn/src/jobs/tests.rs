use super::*;
use crate::{
    persistence::{SessionStore, SqliteSessionStore},
    session::{EntityKey, NewEntity},
};
use sha2::{Digest, Sha256};

pub(crate) fn fixture(
    id: &str,
    principal: Option<PrincipalBinding>,
) -> (Session, ExecutionKey, JobInput) {
    let mut session = Session::new(id, 7, 100).unwrap();
    if let Some(principal) = principal {
        session.bind_owner(principal).unwrap();
    }
    let digest = Sha256::digest(b"job").into();
    let key = ExecutionKey {
        entity: EntityKey {
            scope_id: 0,
            entity_id: 1,
        },
        stage: ExecutionStage::Process,
    };
    session
        .add_root(NewEntity {
            entity_id: 1,
            layer: 0,
            payload_digest: digest,
            policy: None,
        })
        .unwrap();
    session
        .transition(key.entity, EntityState::Processing)
        .unwrap();
    let header = EntityHeader {
        entity_id: 1,
        parent_id: None,
        parent_scope_id: None,
        scope_id: None,
        layer: 0,
        content_type: Some("application/octet-stream".into()),
        payload_length: Some(3),
        checksum: Some(digest),
        metadata: Default::default(),
        chunk_info: None,
        completion_policy: None,
    };
    (
        session,
        key,
        JobInput::Process {
            header,
            length: 3,
            digest,
            layers: LayerSupport::LAYER2,
        },
    )
}

#[test]
fn job_input_identity_is_validated_and_replay_cannot_replace_it() {
    let (mut session, key, input) = fixture("job-identity", None);
    let before = session.clone();
    let JobInput::Process {
        header,
        length,
        digest,
        layers,
    } = input.clone()
    else {
        unreachable!()
    };
    for invalid in [
        JobInput::Process {
            header: header.clone(),
            length: length + 1,
            digest,
            layers,
        },
        JobInput::Process {
            header: header.clone(),
            length,
            digest: [0; 32],
            layers,
        },
        JobInput::Process {
            header: EntityHeader {
                entity_id: 2,
                ..header.clone()
            },
            length,
            digest,
            layers,
        },
        JobInput::Resume { claim_id: 2 },
    ] {
        assert!(session.enqueue_job(key, invalid, 10).is_err());
        assert_eq!(session, before);
    }
    session.enqueue_job(key, input.clone(), 10).unwrap();
    let queued = session.clone();
    session.enqueue_job(key, input, 999).unwrap();
    assert_eq!(session, queued);
    let mut changed = header;
    changed.metadata.insert("action".into(), "different".into());
    assert!(
        session
            .enqueue_job(
                key,
                JobInput::Process {
                    header: changed,
                    length,
                    digest,
                    layers
                },
                10
            )
            .is_err()
    );
    assert_eq!(session, queued);
}

#[test]
fn fenced_result_and_retained_outcome_commit_together() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite3");
    let caller = PrincipalBinding::new("issuer", "alice").unwrap();
    let (mut session, key, input) = fixture("job-result", Some(caller.clone()));
    session.enqueue_job(key, input.clone(), 100).unwrap();
    let store = SqliteSessionStore::open(&path).unwrap();
    store.create(&session).unwrap();
    let first = store
        .transact(&session.session_id, |s| {
            s.acquire_job(Some(&caller), key, 100, 50)
        })
        .unwrap()
        .0
        .unwrap();
    drop(store);
    let store = SqliteSessionStore::open(&path).unwrap();
    let second = store
        .transact(&session.session_id, |s| {
            s.acquire_job(Some(&caller), key, 150, 50)
        })
        .unwrap()
        .0
        .unwrap();
    let running = store.load(&session.session_id).unwrap().unwrap();
    assert!(
        store
            .transact(&session.session_id, |s| s.publish_job(
                Some(&caller),
                &first,
                160,
                |_| unreachable!()
            ))
            .is_err()
    );
    assert_eq!(running, store.load(&session.session_id).unwrap().unwrap());
    for output in [
        JobOutput::Resumed,
        JobOutput::Processed(ProcessOutcome::Complete),
    ] {
        assert!(
            store
                .transact(&session.session_id, |s| s.publish_job(
                    Some(&caller),
                    &second,
                    160,
                    |_| Ok(output)
                ))
                .is_err()
        );
        assert_eq!(running, store.load(&session.session_id).unwrap().unwrap());
    }
    store
        .transact(&session.session_id, |s| {
            s.publish_job(Some(&caller), &second, 160, |s| {
                s.complete_entity(key.entity, [9; 32])?;
                Ok(JobOutput::Processed(ProcessOutcome::Complete))
            })
        })
        .unwrap();
    drop(store);
    let store = SqliteSessionStore::open(&path).unwrap();
    let done = store.load(&session.session_id).unwrap().unwrap();
    assert_eq!(
        done.session.jobs[&key].state,
        JobState::Finished(JobOutput::Processed(ProcessOutcome::Complete))
    );
    assert_eq!(
        done.session.entities[&key.entity].output_digest,
        Some([9; 32])
    );
    assert_eq!(store.unfinished_job_count().unwrap(), 0);
    assert!(
        store
            .transact(&session.session_id, |s| s.acquire_job(
                Some(&caller),
                key,
                201,
                50
            ))
            .unwrap()
            .0
            .is_none()
    );
    store
        .transact(&session.session_id, |s| s.enqueue_job(key, input, 999))
        .unwrap();
    assert_eq!(
        store.load(&session.session_id).unwrap().unwrap().session,
        done.session
    );
}

#[test]
fn refusal_is_retained_bounded_and_does_not_complete_work() {
    let (mut session, key, input) = fixture("job-refusal", None);
    session.enqueue_job(key, input, 100).unwrap();
    let lease = session.acquire_job(None, key, 100, 50).unwrap().unwrap();
    let error = ProtocolError::integrity("bad payload ".repeat(100) + &"é".repeat(100));
    session.refuse_job(None, &lease, 120, &error).unwrap();
    let JobState::Refused(ref failure) = session.jobs[&key].state else {
        panic!("missing refusal")
    };
    assert!(failure.detail.len() <= 512);
    assert_eq!(failure.protocol_error().code, crate::ERROR_INTEGRITY);
    assert_eq!(session.entities[&key.entity].state, EntityState::Processing);
    assert!(session.final_lineage_digest().is_err());
    assert!(session.acquire_job(None, key, 200, 50).unwrap().is_none());
    let multibyte = JobFailure::new(&ProtocolError::frame("a".to_owned() + &"é".repeat(300)));
    assert_eq!(multibyte.detail.len(), 511);
    session.validate_jobs().unwrap();
}

#[test]
fn ownership_and_revocation_guard_job_execution_and_publication() {
    let alice = PrincipalBinding::new("issuer", "alice").unwrap();
    let bob = PrincipalBinding::new("issuer", "bob").unwrap();
    let (mut session, key, input) = fixture("job-owner", Some(alice.clone()));
    session.enqueue_job(key, input, 100).unwrap();
    assert_eq!(
        session
            .acquire_job(Some(&bob), key, 100, 50)
            .unwrap_err()
            .code,
        crate::ERROR_UNAUTHORIZED
    );
    let lease = session
        .acquire_job(Some(&alice), key, 100, 50)
        .unwrap()
        .unwrap();
    session.revoke_access().unwrap();
    let before = session.clone();
    assert_eq!(
        session
            .publish_job(Some(&alice), &lease, 110, |_| unreachable!())
            .unwrap_err()
            .code,
        crate::ERROR_UNAUTHORIZED
    );
    assert_eq!(
        session
            .refuse_job(Some(&alice), &lease, 110, &ProtocolError::frame("refused"))
            .unwrap_err()
            .code,
        crate::ERROR_UNAUTHORIZED
    );
    assert_eq!(session, before);
}

#[test]
fn rehydrate_and_resume_inputs_reconstruct_the_correct_operation() {
    let (mut session, process, _) = fixture("job-stages", None);
    session.begin_dehydrating(process.entity).unwrap();
    session.open_child_scope(process.entity, 1, 10).unwrap();
    let child = session
        .add_child(
            1,
            NewEntity {
                entity_id: 1,
                layer: 0,
                payload_digest: [4; 32],
                policy: None,
            },
        )
        .unwrap();
    session.transition(child, EntityState::Processing).unwrap();
    session.complete_entity(child, [5; 32]).unwrap();
    let digest = session.close_scope(1).unwrap();
    session.begin_rehydration(process.entity).unwrap();
    let rehydrate = ExecutionKey {
        stage: ExecutionStage::Rehydrate,
        ..process
    };
    let before = session.clone();
    let mut wrong = digest.clone();
    wrong.entities_succeeded += 1;
    assert!(
        session
            .enqueue_job(rehydrate, JobInput::Rehydrate { digest: wrong }, 100)
            .is_err()
    );
    assert_eq!(session, before);
    session
        .enqueue_job(
            rehydrate,
            JobInput::Rehydrate {
                digest: digest.clone(),
            },
            100,
        )
        .unwrap();
    let lease = session
        .acquire_job(None, rehydrate, 100, 50)
        .unwrap()
        .unwrap();
    session
        .publish_job(None, &lease, 110, |s| {
            s.complete_rehydration(process.entity, [8; 32])?;
            Ok(JobOutput::Rehydrated(digest))
        })
        .unwrap();
    session.validate_jobs().unwrap();

    let (mut session, process, _) = fixture("job-resume", None);
    let validation = crate::StoppingPointValidation {
        state_checksum: Some([1; 32]),
        bytes_processed: None,
        children_complete: None,
        children_total: None,
        is_resumable: Some(true),
        checkpoint_ref: None,
    };
    session
        .defer_with_claim_id(process.entity, b"resume".to_vec(), validation, 1, 200, 100)
        .unwrap();
    session.redeem_claim(1, [1; 32], 110).unwrap();
    let resume = ExecutionKey {
        stage: ExecutionStage::Resume { claim_id: 1 },
        ..process
    };
    session
        .enqueue_job(resume, JobInput::Resume { claim_id: 1 }, 110)
        .unwrap();
    let lease = session.acquire_job(None, resume, 110, 50).unwrap().unwrap();
    session
        .publish_job(None, &lease, 120, |s| {
            s.complete_entity(resume.entity, [8; 32])?;
            Ok(JobOutput::Resumed)
        })
        .unwrap();
    session.validate_jobs().unwrap();
}
