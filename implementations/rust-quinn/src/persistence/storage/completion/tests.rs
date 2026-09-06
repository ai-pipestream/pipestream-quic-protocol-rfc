use super::*;
use crate::{
    LayerSupport,
    execution::{ExecutionKey, ExecutionStage},
    jobs::tests::fixture,
    session::{EntityState, NewEntity},
};

fn validation() -> StoppingPointValidation {
    StoppingPointValidation {
        state_checksum: Some([255; 32]),
        bytes_processed: Some(u64::MAX),
        children_complete: Some(u64::MAX),
        children_total: Some(u64::MAX),
        is_resumable: Some(true),
        checkpoint_ref: Some("x".repeat(256)),
    }
}

fn queued(layers: LayerSupport) -> (Session, ExecutionKey) {
    let (mut session, key, mut input) = fixture("reserved", None);
    let JobInput::Process {
        layers: selected, ..
    } = &mut input
    else {
        unreachable!()
    };
    *selected = layers;
    session.enqueue_job(key, input, 1).unwrap();
    (session, key)
}

fn charge(session: &Session, limits: StorageLimits) -> usize {
    postcard::to_stdvec(session).unwrap().len() + reserved_bytes(session, limits).unwrap()
}

fn tight_store(session: &Session) -> (tempfile::TempDir, SqliteSessionStore) {
    let dir = tempfile::tempdir().unwrap();
    let bytes = charge(session, StorageLimits::default());
    let limits = StorageLimits {
        total_bytes: bytes as u64,
        principal_bytes: bytes as u64,
        record_bytes: bytes,
        ..StorageLimits::default()
    };
    let store = SqliteSessionStore::open_with_limits(
        dir.path().join("state.sqlite3"),
        JobQueueLimits::default(),
        limits,
    )
    .unwrap();
    store.create(session).unwrap();
    (dir, store)
}

fn assert_limit(error: StoreError) {
    assert!(
        matches!(error, StoreError::Protocol(error) if error.code == crate::ERROR_LIMIT_EXCEEDED)
    );
}

#[test]
fn claim_bound_matches_real_postcard_bytes_at_length_boundaries() {
    let entity = EntityKey {
        scope_id: u32::MAX,
        entity_id: crate::MAX_ENTITY_ID,
    };
    for length in [1, 127, 128, 16_383, 16_384, 65_536, 0x00ff_ffff] {
        let claim = ClaimRecord {
            claim_id: u64::MAX,
            entity,
            expiry_timestamp_micros: u64::MAX,
            token: vec![255; length],
            validation: validation(),
            redeemed_at_micros: None,
        };
        assert_eq!(
            maximum_claim_bytes(entity, length).unwrap(),
            postcard::to_stdvec(&(u64::MAX, claim)).unwrap().len()
        );
    }
}

#[test]
fn every_processing_outcome_fits_its_admission_charge_at_capacity() {
    for layers in [
        LayerSupport::LAYER0,
        LayerSupport::LAYER1,
        LayerSupport::LAYER2,
    ] {
        for outcome in 0..5 {
            if outcome == 4 && !layers.layer2_resilience {
                continue;
            }
            let (session, key) = queued(layers);
            let (_dir, store) = tight_store(&session);
            let before = store.storage_usage().unwrap();
            assert!(before.completion_reserved_bytes > 0);
            assert_eq!(before.charged_bytes(), store.storage_limits().total_bytes);
            assert_limit(
                store
                    .create(&Session::new("unrelated", 7, 100).unwrap())
                    .unwrap_err(),
            );
            assert!(store.load("unrelated").unwrap().is_none());
            let lease = store
                .transact("reserved", |s| s.acquire_job(None, key, 127, 100))
                .unwrap()
                .0
                .unwrap();
            assert_eq!(
                before.charged_bytes(),
                store.storage_usage().unwrap().charged_bytes()
            );
            if outcome == 3 {
                store
                    .transact("reserved", |s| {
                        s.refuse_job(None, &lease, 128, &ProtocolError::limit("x".repeat(512)))
                    })
                    .unwrap();
            } else {
                store
                    .transact("reserved", |s| {
                        s.publish_job(None, &lease, 128, |s| {
                            Ok(JobOutput::Processed(match outcome {
                                0 => {
                                    s.complete_entity(key.entity, [255; 32])?;
                                    ProcessOutcome::Complete
                                }
                                1 => {
                                    s.begin_dehydrating(key.entity)?;
                                    ProcessOutcome::Dehydrate
                                }
                                2 => {
                                    s.transition(key.entity, EntityState::Failed)?;
                                    ProcessOutcome::Failed
                                }
                                4 => {
                                    s.defer_with_claim_id(
                                        key.entity,
                                        vec![255; 64 << 10],
                                        validation(),
                                        u64::MAX,
                                        u64::MAX,
                                        128,
                                    )?;
                                    ProcessOutcome::Deferred {
                                        reason: 5,
                                        claim_id: u64::MAX,
                                    }
                                }
                                _ => unreachable!(),
                            }))
                        })
                    })
                    .unwrap();
            }
            let after = store.storage_usage().unwrap();
            if outcome == 1 {
                assert!(
                    after.completion_reserved_bytes > 0,
                    "waiting parents retain future credit"
                );
            } else {
                assert_eq!(after.completion_reserved_bytes, 0);
            }
            assert!(after.state_bytes > before.state_bytes);
            assert!(after.charged_bytes() <= before.charged_bytes());
            assert_eq!(store.unfinished_job_count().unwrap(), 0);
            store.integrity_check().unwrap();
        }
    }
}

#[test]
fn recovery_and_rehydration_publications_fit_at_capacity() {
    for resume in [false, true] {
        let (mut session, process, _) = fixture("reserved", None);
        let (key, input, output) = if resume {
            session
                .defer_with_claim_id(
                    process.entity,
                    vec![1; 64 << 10],
                    validation(),
                    u64::MAX,
                    1000,
                    1,
                )
                .unwrap();
            session.redeem_claim(u64::MAX, [255; 32], 2).unwrap();
            (
                ExecutionKey {
                    stage: ExecutionStage::Resume { claim_id: u64::MAX },
                    ..process
                },
                JobInput::Resume { claim_id: u64::MAX },
                JobOutput::Resumed,
            )
        } else {
            session.begin_dehydrating(process.entity).unwrap();
            session
                .open_child_scope(process.entity, u32::MAX, 1)
                .unwrap();
            let child = session
                .add_child(
                    u32::MAX,
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
            let digest = session.close_scope(u32::MAX).unwrap();
            session.begin_rehydration(process.entity).unwrap();
            (
                ExecutionKey {
                    stage: ExecutionStage::Rehydrate,
                    ..process
                },
                JobInput::Rehydrate {
                    digest: digest.clone(),
                },
                JobOutput::Rehydrated(digest),
            )
        };
        session.enqueue_job(key, input, 3).unwrap();
        let (_dir, store) = tight_store(&session);
        let before = store.storage_usage().unwrap();
        let lease = store
            .transact("reserved", |s| s.acquire_job(None, key, 127, 100))
            .unwrap()
            .0
            .unwrap();
        assert_eq!(
            before.charged_bytes(),
            store.storage_usage().unwrap().charged_bytes()
        );
        store
            .transact("reserved", |s| {
                s.publish_job(None, &lease, 128, |s| {
                    if resume {
                        s.complete_entity(key.entity, [3; 32])?;
                    } else {
                        s.complete_rehydration(key.entity, [3; 32])?;
                    }
                    Ok(output)
                })
            })
            .unwrap();
        assert_eq!(store.storage_usage().unwrap().completion_reserved_bytes, 0);
        assert!(store.storage_usage().unwrap().charged_bytes() <= before.charged_bytes());
        store.integrity_check().unwrap();
    }
}

#[test]
fn record_limit_refuses_unfunded_admission_and_preserves_existing_state() {
    let (queued, key) = queued(LayerSupport::LAYER2);
    let mut unqueued = queued.clone();
    unqueued.jobs.clear();
    let dir = tempfile::tempdir().unwrap();
    let limits = StorageLimits {
        record_bytes: charge(&queued, StorageLimits::default()) - 1,
        ..StorageLimits::default()
    };
    let store = SqliteSessionStore::open_with_limits(
        dir.path().join("state.sqlite3"),
        JobQueueLimits::default(),
        limits,
    )
    .unwrap();
    let original = store.create(&unqueued).unwrap();
    assert_limit(store.save(original.revision, &queued).unwrap_err());
    assert_limit(
        store
            .transact("reserved", |s| {
                s.enqueue_job(key, queued.jobs[&key].input.clone(), 1)
            })
            .unwrap_err(),
    );
    assert_eq!(store.load("reserved").unwrap().unwrap(), original);
    assert_eq!(store.storage_usage().unwrap().completion_reserved_bytes, 0);
    assert_eq!(store.unfinished_job_count().unwrap(), 0);
}

#[test]
fn oversized_yield_rolls_back_claim_and_can_publish_a_reserved_refusal() {
    let (session, key) = queued(LayerSupport::LAYER2);
    let (_dir, store) = tight_store(&session);
    let lease = store
        .transact("reserved", |s| s.acquire_job(None, key, 1, 100))
        .unwrap()
        .0
        .unwrap();
    let before = store.load("reserved").unwrap().unwrap();
    assert_limit(
        store
            .transact("reserved", |s| {
                s.publish_job(None, &lease, 2, |s| {
                    s.defer_with_claim_id(
                        key.entity,
                        vec![0; (64 << 10) + 1],
                        validation(),
                        1,
                        100,
                        2,
                    )?;
                    Ok(JobOutput::Processed(ProcessOutcome::Deferred {
                        reason: 1,
                        claim_id: 1,
                    }))
                })
            })
            .unwrap_err(),
    );
    assert_eq!(store.load("reserved").unwrap().unwrap(), before);
    store
        .transact("reserved", |s| {
            s.refuse_job(None, &lease, 3, &ProtocolError::limit("over budget"))
        })
        .unwrap();
    let retained = store.load("reserved").unwrap().unwrap().session;
    assert!(retained.claims.is_empty());
    assert_eq!(
        retained.entities[&key.entity].state,
        EntityState::Processing
    );
    assert!(matches!(retained.jobs[&key].state, JobState::Refused(_)));
    store.integrity_check().unwrap();
}

#[test]
fn reservation_corruption_cannot_create_capacity_for_another_session() {
    for alteration in [
        "UPDATE pipestream_storage_sessions SET completion_bytes=0",
        "DELETE FROM pipestream_storage_sessions",
    ] {
        let (session, _) = queued(LayerSupport::LAYER2);
        let (_dir, store) = tight_store(&session);
        Connection::open(store.path())
            .unwrap()
            .execute_batch(alteration)
            .unwrap();
        assert!(matches!(
            store.load("reserved"),
            Err(StoreError::Corrupt(_))
        ));
        assert!(matches!(
            store.create(&Session::new("unrelated", 7, 100).unwrap()),
            Err(StoreError::Corrupt(_))
        ));
        assert!(store.load("unrelated").unwrap().is_none());
        assert!(store.integrity_check().is_err());
    }
}

#[test]
fn abrupt_exit_retains_reservation_and_expired_attempt_growth() {
    const CHILD: &str = "PIPESTREAM_COMPLETION_CRASH_DIR";
    if let Some(directory) = std::env::var_os(CHILD) {
        let (session, key) = queued(LayerSupport::LAYER2);
        let store =
            SqliteSessionStore::open(PathBuf::from(directory).join("state.sqlite3")).unwrap();
        store.create(&session).unwrap();
        store
            .transact("reserved", |s| s.acquire_job(None, key, 1, 100))
            .unwrap();
        std::process::exit(37);
    }
    let dir = tempfile::tempdir().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "persistence::storage::completion::tests::abrupt_exit_retains_reservation_and_expired_attempt_growth", "--nocapture"])
        .env(CHILD, dir.path()).status().unwrap();
    assert_eq!(status.code(), Some(37));
    let store = SqliteSessionStore::open(dir.path().join("state.sqlite3")).unwrap();
    let (session, key) = queued(LayerSupport::LAYER2);
    assert_eq!(
        store.storage_usage().unwrap().charged_bytes(),
        charge(&session, store.storage_limits()) as u64
    );
    let lease = store
        .transact("reserved", |s| s.acquire_job(None, key, 128, 100))
        .unwrap()
        .0
        .unwrap();
    assert_eq!(lease.epoch(), 2);
    store
        .transact("reserved", |s| {
            s.refuse_job(None, &lease, 129, &ProtocolError::limit("x".repeat(512)))
        })
        .unwrap();
    assert_eq!(store.storage_usage().unwrap().completion_reserved_bytes, 0);
    store.integrity_check().unwrap();
}

#[test]
fn old_policy_and_changed_yield_budget_are_refused_without_conversion() {
    let (session, _) = queued(LayerSupport::LAYER2);
    let (_dir, store) = tight_store(&session);
    let before = store.load("reserved").unwrap().unwrap();
    assert_limit(
        SqliteSessionStore::open_with_limits(
            store.path(),
            store.job_limits(),
            StorageLimits {
                yield_token_bytes: 1024,
                ..store.storage_limits()
            },
        )
        .unwrap_err(),
    );
    assert_eq!(store.load("reserved").unwrap().unwrap(), before);
    store.checkpoint().unwrap();
    let connection = Connection::open(store.path()).unwrap();
    connection
        .execute_batch(
            "PRAGMA ignore_check_constraints=ON; UPDATE pipestream_storage_limits SET version=1;",
        )
        .unwrap();
    let state: Vec<u8> = connection
        .query_row("SELECT state FROM pipestream_sessions", [], |r| r.get(0))
        .unwrap();
    assert!(matches!(
        SqliteSessionStore::open(store.path()),
        Err(StoreError::Corrupt(_))
    ));
    assert_eq!(
        connection
            .query_row("SELECT version FROM pipestream_storage_limits", [], |r| r
                .get::<_, i64>(
                0
            ))
            .unwrap(),
        1
    );
    assert_eq!(
        connection
            .query_row("SELECT state FROM pipestream_sessions", [], |r| r
                .get::<_, Vec<u8>>(0))
            .unwrap(),
        state
    );
}

#[test]
fn map_prefix_growth_remains_reserved_across_many_attempts_and_claims() {
    let (mut session, first, input) = fixture("map-prefixes", None);
    session.max_entities_per_scope = 128;
    session.enqueue_job(first, input.clone(), 1).unwrap();
    for id in 2..=128 {
        let key = session
            .add_root(NewEntity {
                entity_id: id,
                layer: 0,
                payload_digest: session.entities[&first.entity].payload_digest,
                policy: None,
            })
            .unwrap();
        session.transition(key, EntityState::Processing).unwrap();
        let mut input = input.clone();
        let JobInput::Process { header, .. } = &mut input else {
            unreachable!()
        };
        header.entity_id = id;
        session
            .enqueue_job(
                ExecutionKey {
                    entity: key,
                    stage: ExecutionStage::Process,
                },
                input,
                1,
            )
            .unwrap();
    }
    let limits = StorageLimits {
        yield_token_bytes: 1,
        ..StorageLimits::default()
    };
    let initial = charge(&session, limits);
    let keys: Vec<_> = session.jobs.keys().copied().collect();
    let leases: Vec<_> = keys
        .iter()
        .map(|key| {
            let lease = session.acquire_job(None, *key, 127, 100).unwrap().unwrap();
            assert_eq!(charge(&session, limits), initial);
            lease
        })
        .collect();
    for (index, lease) in leases.iter().enumerate() {
        let before = charge(&session, limits);
        session
            .publish_job(None, lease, 128, |s| {
                let claim_id = u64::MAX - index as u64;
                s.defer_with_claim_id(
                    lease.key().entity,
                    vec![255],
                    validation(),
                    claim_id,
                    u64::MAX,
                    128,
                )?;
                Ok(JobOutput::Processed(ProcessOutcome::Deferred {
                    reason: 5,
                    claim_id,
                }))
            })
            .unwrap();
        assert!(charge(&session, limits) <= before);
    }
    assert_eq!(session.executions.len(), 128);
    assert_eq!(session.claims.len(), 128);
    assert_eq!(reserved_bytes(&session, limits).unwrap(), 0);
}

#[test]
fn principal_reservations_survive_revocation_and_cannot_be_overbooked_by_other_handles() {
    let prepared = |id: &str, owner: &str| {
        let (mut session, key, input) =
            fixture(id, Some(PrincipalBinding::new("issuer", owner).unwrap()));
        session.enqueue_job(key, input, 1).unwrap();
        session
    };
    let first = prepared("one", "alice");
    let bytes = charge(&first, StorageLimits::default());
    let limits = StorageLimits {
        total_bytes: 2 * bytes as u64,
        principal_bytes: bytes as u64,
        record_bytes: bytes,
        ..StorageLimits::default()
    };
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteSessionStore::open_with_limits(
        dir.path().join("state.sqlite3"),
        JobQueueLimits::default(),
        limits,
    )
    .unwrap();
    let barrier = std::sync::Barrier::new(2);
    let results = std::thread::scope(|threads| {
        let handles: Vec<_> = ["one", "two"]
            .into_iter()
            .map(|id| {
                let store = &store;
                let barrier = &barrier;
                let prepared = &prepared;
                threads.spawn(move || {
                    let reopened = SqliteSessionStore::open(store.path()).unwrap();
                    barrier.wait();
                    reopened.create(&prepared(id, "alice"))
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    for result in &results {
        if let Err(error) = result {
            assert!(
                matches!(error, StoreError::Protocol(error) if error.code == crate::ERROR_LIMIT_EXCEEDED)
            );
        }
    }
    let retained = results.into_iter().find_map(Result::ok).unwrap();
    let before = store.storage_usage().unwrap();
    store
        .transact(&retained.session.session_id, Session::revoke_access)
        .unwrap();
    assert_eq!(before, store.storage_usage().unwrap());
    assert!(store.ready_jobs(2, 10).unwrap().is_empty());
    store.create(&prepared("tri", "bobby")).unwrap();
    assert_eq!(
        store.storage_usage().unwrap().charged_bytes(),
        2 * bytes as u64
    );
    assert_limit(store.create(&prepared("end", "carol")).unwrap_err());
    let reopened = SqliteSessionStore::open(store.path()).unwrap();
    assert_eq!(
        reopened.storage_usage().unwrap(),
        store.storage_usage().unwrap()
    );
    assert_eq!(
        reopened
            .principal_storage_usage(Some(&PrincipalBinding::new("issuer", "alice").unwrap()))
            .unwrap(),
        before
    );
    reopened.integrity_check().unwrap();
}
