use super::*;
use pipestream_core::persistence::{JobQueueLimits, PhysicalLimits, StorageLimits, StoreError};

struct ReleasePublication(Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>);
impl Drop for ReleasePublication {
    fn drop(&mut self) {
        *self.0.0.lock().unwrap() = true;
        self.0.1.notify_all();
    }
}

#[tokio::test]
async fn authenticated_callback_publishes_while_unrelated_writes_saturate_reserved_wal()
-> Result<()> {
    let mut fixture = AuthFixture::new()?;
    fixture.options.state_database = fixture._dir.path().join("publication.sqlite3");
    let physical = PhysicalLimits {
        database_bytes: 2 << 20,
        wal_bytes: 1 << 20,
        journal_bytes: 1 << 20,
        shared_memory_bytes: 64 << 10,
    };
    fixture.store = Arc::new(SqliteSessionStore::open_with_all_limits(
        &fixture.options.state_database,
        JobQueueLimits::default(),
        StorageLimits::default(),
        physical,
    )?);
    let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let release = ReleasePublication(gate.clone());
    fixture.processor = Arc::new(Processor {
        process_gate: Some(gate),
        token_bytes: Some(64 << 10),
        expected_token_budget: Some(64 << 10),
        ..Processor::default()
    });
    let options = fixture.listen(Some("issuer-a"), Some(0))?;
    let worker_options = options.clone();
    let pending =
        tokio::spawn(async move { begin_durable_yield(&worker_options, "wal-yield").await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while fixture.processor.processed.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await?;
    let before = fixture.store.load("wal-yield")?.unwrap();
    let mut filler = fixture
        .store
        .create(&pipestream_core::session::Session::new(
            "wal-filler",
            7,
            32,
        )?)?;
    fixture.store.checkpoint()?;
    let reader = rusqlite::Connection::open_with_flags(
        &fixture.options.state_database,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    reader.execute_batch("BEGIN; SELECT count(*) FROM pipestream_sessions")?;
    let mut saturated = false;
    for _ in 0..1000 {
        match fixture.store.save(filler.revision, &filler.session) {
            Ok(next) => filler = next,
            Err(StoreError::Protocol(error)) if error.code == ERROR_LIMIT_EXCEEDED => {
                saturated = true;
                break;
            }
            Err(error) => return Err(error.into()),
        }
    }
    assert!(saturated);
    assert_eq!(fixture.store.load("wal-filler")?, Some(filler));
    assert_eq!(fixture.store.load("wal-yield")?, Some(before));
    drop(release);
    let claim = tokio::time::timeout(Duration::from_secs(5), pending).await???;
    let retained = fixture.store.load("wal-yield")?.unwrap().session;
    assert_eq!(retained.claims[&claim.claim_id].token, vec![255; 64 << 10]);
    assert_eq!(fixture.store.unfinished_job_count()?, 0);
    assert!(fixture.store.physical_usage()?.wal_bytes <= physical.wal_bytes);
    fixture.store.integrity_check()?;
    drop(reader);
    fixture.store.checkpoint()?;
    Ok(())
}

#[tokio::test]
async fn authenticated_yield_uses_its_reservation_after_other_admissions_fill_the_store()
-> Result<()> {
    let mut fixture = fixture(StorageLimits {
        total_bytes: 8192,
        principal_bytes: 8192,
        record_bytes: 8192,
        yield_token_bytes: 4096,
        ..StorageLimits::default()
    })?;
    let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let release = ReleasePublication(gate.clone());
    fixture.processor = Arc::new(Processor {
        process_gate: Some(gate),
        token_bytes: Some(4096),
        expected_token_budget: Some(4096),
        ..Processor::default()
    });
    let options = fixture.listen(Some("issuer-a"), Some(0))?;
    let worker_options = options.clone();
    let pending_work =
        tokio::spawn(async move { begin_durable_yield(&worker_options, "reserved-yield").await });
    tokio::time::timeout(Duration::from_secs(3), async {
        while fixture.processor.processed.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await?;
    let before = fixture.store.load("reserved-yield")?.unwrap();
    let usage = fixture.store.storage_usage()?;
    assert!(usage.completion_reserved_bytes > 4096);
    let mut filler = pipestream_core::session::Session::new("filler", 7, 1000)?;
    filler.bind_owner(before.session.owner.as_ref().unwrap().binding.clone())?;
    fixture.store.create(&filler)?;
    let mut refused_growth = false;
    for id in 1..1000 {
        match fixture.store.transact("filler", |s| {
            s.add_root(pipestream_core::session::NewEntity {
                entity_id: id,
                layer: 0,
                payload_digest: [0; 32],
                policy: None,
            })
        }) {
            Ok(_) => {}
            Err(StoreError::Protocol(error)) if error.code == ERROR_LIMIT_EXCEEDED => {
                refused_growth = true;
                break;
            }
            Err(error) => return Err(error.into()),
        }
    }
    assert!(refused_growth);
    assert!(
        !fixture
            .store
            .load("filler")?
            .unwrap()
            .session
            .entities
            .is_empty()
    );
    let full = fixture.store.storage_usage()?;
    assert_eq!(
        full.completion_reserved_bytes,
        usage.completion_reserved_bytes
    );
    let mut rejected = RecursiveClient::connect_sealed(&options).await?;
    let error = rejected
        .declare_work(&work("cannot-spend-reserve", 0, vec![1], Some(&[1])))
        .await
        .unwrap_err();
    assert!(
        format!("{error:#}").contains("PIPESTREAM_LIMIT_EXCEEDED"),
        "{error:#}"
    );
    assert!(fixture.store.load("cannot-spend-reserve")?.is_none());
    assert_eq!(fixture.store.load("reserved-yield")?, Some(before));
    drop(release);
    let claim = tokio::time::timeout(Duration::from_secs(3), pending_work).await???;
    let retained = fixture.store.load("reserved-yield")?.unwrap().session;
    assert_eq!(retained.claims[&claim.claim_id].token, vec![255; 4096]);
    assert_eq!(
        retained.entities[&retained.claims[&claim.claim_id].entity].state,
        pipestream_core::session::EntityState::Deferred
    );
    assert_eq!(fixture.store.storage_usage()?.completion_reserved_bytes, 0);
    assert!(fixture.store.storage_usage()?.charged_bytes() <= full.charged_bytes());
    fixture.store.integrity_check()?;
    Ok(())
}

#[tokio::test]
async fn callback_yield_budget_is_explicit_and_oversized_results_are_retained_refusals()
-> Result<()> {
    let frame_budget = MAX_CONTROL_FRAME - 24;
    for (policy, budget, bytes) in [
        (32, 32, 32),
        (32, 32, 33),
        (2 << 20, frame_budget, frame_budget),
        (2 << 20, frame_budget, frame_budget + 1),
    ] {
        let mut fixture = fixture(StorageLimits {
            yield_token_bytes: policy,
            ..StorageLimits::default()
        })?;
        fixture.processor = Arc::new(Processor {
            token_bytes: Some(bytes),
            expected_token_budget: Some(budget),
            ..Processor::default()
        });
        let options = fixture.listen(Some("issuer-a"), Some(0))?;
        let outcome = begin_durable_yield(&options, "bounded-yield").await;
        if bytes == budget {
            let claim = outcome?;
            assert_eq!(
                fixture.store.load("bounded-yield")?.unwrap().session.claims[&claim.claim_id]
                    .token
                    .len(),
                bytes
            );
            let mut client = RecursiveClient::connect_recovery(&options).await?;
            let request = pipestream_core::recovery::RecoveryRequest {
                authority: "issuer-a".into(),
                session_id: claim.session_id,
                request_id: [3; 16],
                claim_id: claim.claim_id,
                state_checksum: claim.state_checksum,
            };
            let receipt = client.accept_recovery(&request).await?;
            assert_eq!(
                client.wait_recovery(&receipt).await?,
                pipestream_core::recovery::RecoveryOutcome::Complete
            );
            client.disconnect_gracefully().await;
            assert_eq!(fixture.processor.resumed.load(Ordering::SeqCst), 1);
        } else {
            let error = outcome.unwrap_err();
            assert!(
                format!("{error:#}").contains("PIPESTREAM_LIMIT_EXCEEDED"),
                "{error:#}"
            );
            let retained = fixture.store.load("bounded-yield")?.unwrap().session;
            assert!(retained.claims.is_empty());
            assert!(retained.jobs.values().all(|job| matches!(&job.state, pipestream_core::jobs::JobState::Refused(failure) if failure.code == ERROR_LIMIT_EXCEEDED)));
            assert!(retained.entities.values().all(|entity| entity.state == pipestream_core::session::EntityState::Processing));
        }
        assert_eq!(fixture.store.storage_usage()?.completion_reserved_bytes, 0);
        fixture.store.integrity_check()?;
    }
    Ok(())
}

fn fixture(limits: StorageLimits) -> Result<AuthFixture> {
    let mut fixture = AuthFixture::new()?;
    fixture.options.state_database = fixture._dir.path().join("quota.sqlite3");
    fixture.store = Arc::new(SqliteSessionStore::open_with_limits(
        &fixture.options.state_database,
        JobQueueLimits::default(),
        limits,
    )?);
    Ok(fixture)
}

fn work(id: &str, sequence: u64, ids: Vec<u32>, all: Option<&[u32]>) -> WorkSetFrame {
    WorkSetFrame {
        session_id: id.into(),
        producer_id: [1; 16],
        scope_id: 0,
        parent: None,
        sequence,
        entity_ids: ids,
        flags: if all.is_some() { work_set::SEAL } else { 0 },
        seal_digest: all
            .map(|all| work_set::seal_digest(id, [1; 16], 0, None, &all.iter().copied().collect())),
    }
}

#[tokio::test]
async fn full_store_refuses_new_sessions_but_replays_retained_declarations() -> Result<()> {
    let mut fixture = fixture(StorageLimits {
        sessions: 2,
        principal_sessions: 1,
        ..StorageLimits::default()
    })?;
    let alice = fixture.listen(Some("issuer-a"), Some(0))?;
    let bob = fixture.listen(Some("issuer-a"), Some(2))?;
    let request = work("alice-set", 0, vec![1], Some(&[1]));
    let mut client = RecursiveClient::connect_sealed(&alice).await?;
    client.declare_work(&request).await?;
    client.disconnect();
    let before = fixture.store.load("alice-set")?.unwrap();
    let mut denied = RecursiveClient::connect_sealed(&alice).await?;
    assert!(
        format!(
            "{:#}",
            denied
                .declare_work(&work("alice-over", 0, vec![1], Some(&[1])))
                .await
                .unwrap_err()
        )
        .contains("PIPESTREAM_LIMIT_EXCEEDED")
    );
    assert!(fixture.store.load("alice-over")?.is_none());
    let mut client = RecursiveClient::connect_sealed(&bob).await?;
    client
        .declare_work(&work("bob-set", 0, vec![1], Some(&[1])))
        .await?;
    client.disconnect();
    assert_eq!(fixture.store.storage_usage()?.sessions, 2);
    let other_authority = fixture.listen(Some("issuer-b"), Some(0))?;
    let mut global_denied = RecursiveClient::connect_sealed(&other_authority).await?;
    assert!(
        format!(
            "{:#}",
            global_denied
                .declare_work(&work("global-over", 0, vec![1], Some(&[1])))
                .await
                .unwrap_err()
        )
        .contains("PIPESTREAM_LIMIT_EXCEEDED")
    );
    assert!(fixture.store.load("global-over")?.is_none());
    // Reopen under the same durable policy and rotate Alice's client certificate.
    while let Some(server) = fixture.servers.pop() {
        server.abort();
        let _ = server.await;
    }
    fixture.store = Arc::new(SqliteSessionStore::open(&fixture.options.state_database)?);
    let rotated = fixture.listen(Some("issuer-a"), Some(1))?;
    let mut replay = RecursiveClient::connect_sealed(&rotated).await?;
    replay.declare_work(&request).await?;
    assert_eq!(
        fixture.store.load("alice-set")?.unwrap().session,
        before.session
    );
    assert_eq!(fixture.store.storage_usage()?.sessions, 2);
    assert_eq!(fixture.processor.processed.load(Ordering::SeqCst), 0);
    fixture.store.integrity_check()?;
    replay.disconnect();
    Ok(())
}

#[tokio::test]
async fn record_exhaustion_cannot_extend_or_seal_an_acknowledged_work_set() -> Result<()> {
    let mut fixture = fixture(StorageLimits {
        record_bytes: 256,
        ..StorageLimits::default()
    })?;
    let options = fixture.listen(Some("issuer-a"), Some(0))?;
    let request = work("growing-set", 0, vec![1], None);
    let mut client = RecursiveClient::connect_sealed(&options).await?;
    client.declare_work(&request).await?;
    let before = fixture.store.load("growing-set")?.unwrap();
    let usage = fixture.store.storage_usage()?;
    let all: Vec<_> = (1..=100).collect();
    let more = work("growing-set", 1, (2..=100).collect(), Some(&all));
    let error = client.declare_work(&more).await.unwrap_err();
    assert!(
        format!("{error:#}").contains("PIPESTREAM_LIMIT_EXCEEDED"),
        "{error:#}"
    );
    assert_eq!(fixture.store.load("growing-set")?.unwrap(), before);
    assert_eq!(fixture.store.storage_usage()?, usage);
    let mut replay = RecursiveClient::connect_sealed(&options).await?;
    replay.declare_work(&request).await?;
    let state = fixture.store.load("growing-set")?.unwrap().session;
    let scope = &state.work_sets.as_ref().unwrap().scopes[&0];
    assert_eq!(scope.ids, std::collections::BTreeSet::from([1]));
    assert!(scope.seal_digest.is_none());
    assert!(!state.work_scope_ready(0));
    fixture.store.integrity_check()?;
    replay.disconnect();
    Ok(())
}

#[tokio::test]
async fn wal_exhaustion_is_a_named_wire_refusal_and_retained_work_resumes_after_checkpoint()
-> Result<()> {
    let mut fixture = AuthFixture::new()?;
    fixture.options.state_database = fixture._dir.path().join("physical.sqlite3");
    let physical = PhysicalLimits {
        database_bytes: 256 << 10,
        wal_bytes: 128 << 10,
        journal_bytes: 256 << 10,
        shared_memory_bytes: 64 << 10,
    };
    fixture.store = Arc::new(SqliteSessionStore::open_with_all_limits(
        &fixture.options.state_database,
        JobQueueLimits::default(),
        StorageLimits::default(),
        physical,
    )?);
    let options = fixture.listen(Some("issuer-a"), Some(0))?;
    let first = work("physical-set", 0, vec![1], None);
    let mut client = RecursiveClient::connect_sealed(&options).await?;
    client.declare_work(&first).await?;
    // A read-only fault-injection connection pins a WAL snapshot. All writes
    // still go through the guarded production store, including wire admission.
    let reader = rusqlite::Connection::open_with_flags(
        &fixture.options.state_database,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    reader.execute_batch("BEGIN; SELECT count(*) FROM pipestream_sessions;")?;
    let mut before = fixture.store.load("physical-set")?.unwrap();
    let mut full = false;
    for _ in 0..100 {
        match fixture.store.save(before.revision, &before.session) {
            Ok(next) => before = next,
            Err(StoreError::Protocol(error))
                if error.code == pipestream_core::ERROR_LIMIT_EXCEEDED =>
            {
                full = true;
                break;
            }
            Err(error) => return Err(error.into()),
        }
    }
    assert!(full);
    let final_batch = work("physical-set", 1, vec![2], Some(&[1, 2]));
    let error = tokio::time::timeout(Duration::from_secs(5), client.declare_work(&final_batch))
        .await?
        .unwrap_err();
    assert!(
        format!("{error:#}").contains("PIPESTREAM_LIMIT_EXCEEDED"),
        "{error:#}"
    );
    assert_eq!(fixture.store.load("physical-set")?.unwrap(), before);
    assert_eq!(fixture.store.unfinished_job_count()?, 0);
    assert!(fixture.store.physical_usage()?.wal_bytes <= physical.wal_bytes);
    drop(reader);
    fixture.store.checkpoint()?;
    let mut replay = RecursiveClient::connect_sealed(&options).await?;
    replay.declare_work(&first).await?;
    replay.declare_work(&final_batch).await?;
    let retained = fixture.store.load("physical-set")?.unwrap().session;
    assert_eq!(
        retained.work_sets.as_ref().unwrap().scopes[&0].ids,
        std::collections::BTreeSet::from([1, 2])
    );
    assert!(!retained.work_scope_ready(0));
    fixture.store.integrity_check()?;
    replay.disconnect();
    Ok(())
}

#[tokio::test]
async fn retained_payload_quota_preserves_declared_work_and_allows_another_principal() -> Result<()>
{
    let mut fixture = AuthFixture::new()?;
    let files = FileEntityStore::open_with_limits(
        &fixture.options.entity_directory,
        spool::SpoolLimits::default(),
        RetainedLimits {
            // One payload (512 metadata + 32 receipt + 1 body) and one
            // final-lineage allowance (1120), for each of two principals.
            bytes: 3330,
            principal_bytes: 1665,
            objects: 4,
            principal_objects: 2,
            staging_bytes: 1,
            staging_objects: 2,
            principals: 4,
        },
    )?;
    let alice = fixture.listen(Some("issuer-a"), Some(0))?;
    let mut client = RecursiveClient::connect_sealed(&alice).await?;
    let request = work("payload-alice", 0, vec![1, 2], Some(&[1, 2]));
    client.declare_work(&request).await?;
    let mut header = EntityHeader {
        entity_id: 1,
        parent_id: None,
        scope_id: None,
        parent_scope_id: None,
        layer: 0,
        content_type: None,
        payload_length: Some(1),
        checksum: None,
        metadata: BTreeMap::from([
            (SESSION_METADATA_KEY.to_owned(), "payload-alice".to_owned()),
            (ACTION_METADATA_KEY.to_owned(), "complete".to_owned()),
        ]),
        chunk_info: None,
        completion_policy: None,
    };
    assert_eq!(
        client
            .send_entity(&header, b"x", 0)
            .await?
            .last()
            .unwrap()
            .status
            .state,
        STATUS_COMPLETE
    );
    header.entity_id = 2;
    let error = tokio::time::timeout(Duration::from_secs(5), client.send_entity(&header, b"x", 0))
        .await?
        .unwrap_err();
    assert!(
        format!("{error:#}").contains("PIPESTREAM_LIMIT_EXCEEDED"),
        "{error:#}"
    );
    let retained = fixture.store.load("payload-alice")?.unwrap().session;
    assert_eq!(retained.entities.len(), 1);
    assert_eq!(retained.jobs.len(), 1);
    assert!(!retained.work_scope_ready(0));
    assert_eq!(
        retained.work_sets.as_ref().unwrap().scopes[&0].ids,
        std::collections::BTreeSet::from([1, 2])
    );
    assert!(
        !fixture
            .options
            .entity_directory
            .join("payload-alice/scope-0/entity-2.bin")
            .exists()
    );
    let bob = fixture.listen(Some("issuer-a"), Some(2))?;
    let mut client = RecursiveClient::connect_sealed(&bob).await?;
    client
        .declare_work(&work("payload-bob", 0, vec![1], Some(&[1])))
        .await?;
    header.entity_id = 1;
    header
        .metadata
        .insert(SESSION_METADATA_KEY.to_owned(), "payload-bob".into());
    assert_eq!(
        client
            .send_entity(&header, b"x", 0)
            .await?
            .last()
            .unwrap()
            .status
            .state,
        STATUS_COMPLETE
    );
    let full = files.retained_usage()?;
    assert_eq!(full.objects, 4);
    assert_eq!(full.bytes, 3330);
    assert_eq!(full.lineage_reservations, 2);
    assert_eq!(files.retained_usage()?.staging_objects, 0);
    assert!(
        !fixture
            .options
            .entity_directory
            .join("payload-alice/lineage.sha256")
            .exists()
    );
    let mut cp = checkpoint(2000);
    cp.checkpoint_entity_id = 1;
    assert_eq!(client.checkpoint(&cp).await?.flags, CHECKPOINT_ACK);
    client.goaway(1).await?;
    let bob_state = fixture.store.load("payload-bob")?.unwrap().session;
    assert!(bob_state.checkpoints[&(0, 1)].acknowledged);
    assert_eq!(
        fs::read(
            fixture
                .options
                .entity_directory
                .join("payload-bob/lineage.sha256")
        )?,
        bob_state.final_lineage_digest()?
    );
    assert_eq!(files.retained_usage()?, full);
    let mut replay = RecursiveClient::connect_sealed(&alice).await?;
    replay.declare_work(&request).await?;
    assert_eq!(files.retained_usage()?, full);
    let error = replay.checkpoint(&checkpoint(50)).await.unwrap_err();
    assert!(
        format!("{error:#}").contains("PIPESTREAM_CHECKPOINT_TIMEOUT"),
        "{error:#}"
    );
    let alice_state = fixture.store.load("payload-alice")?.unwrap().session;
    assert!(
        alice_state
            .checkpoints
            .values()
            .all(|checkpoint| !checkpoint.acknowledged)
    );
    assert!(
        !fixture
            .options
            .entity_directory
            .join("payload-alice/lineage.sha256")
            .exists()
    );
    fixture.store.integrity_check()?;
    replay.disconnect();
    Ok(())
}

#[tokio::test]
async fn admitted_callbacks_keep_lineage_credit_while_other_principals_fill_retained_storage()
-> Result<()> {
    let mut fixture = AuthFixture::new()?;
    let files = FileEntityStore::open_with_limits(
        &fixture.options.entity_directory,
        spool::SpoolLimits::default(),
        RetainedLimits {
            bytes: 3330,
            principal_bytes: 1665,
            objects: 4,
            principal_objects: 2,
            staging_bytes: 2,
            staging_objects: 2,
            principals: 2,
        },
    )?;
    let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let release = ReleasePublication(gate.clone());
    fixture.processor = Arc::new(Processor {
        process_gate: Some(gate),
        ..Processor::default()
    });
    let alice = fixture.listen(Some("issuer-a"), Some(0))?;
    let bob = fixture.listen(Some("issuer-a"), Some(2))?;
    let mut callbacks = Vec::new();
    for (options, session) in [(alice.clone(), "held-alice"), (bob, "held-bob")] {
        callbacks.push(tokio::spawn(async move {
            let mut client = RecursiveClient::connect_sealed(&options).await?;
            client
                .declare_work(&work(session, 0, vec![1], Some(&[1])))
                .await?;
            let header = EntityHeader {
                entity_id: 1,
                parent_id: None,
                scope_id: None,
                parent_scope_id: None,
                layer: 0,
                content_type: None,
                payload_length: Some(1),
                checksum: None,
                metadata: BTreeMap::from([
                    (SESSION_METADATA_KEY.to_owned(), session.to_owned()),
                    (ACTION_METADATA_KEY.to_owned(), "complete".to_owned()),
                ]),
                chunk_info: None,
                completion_policy: None,
            };
            assert_eq!(
                client
                    .send_entity(&header, b"x", 0)
                    .await?
                    .last()
                    .unwrap()
                    .status
                    .state,
                STATUS_COMPLETE
            );
            let mut cp = checkpoint(2000);
            cp.checkpoint_entity_id = 1;
            assert_eq!(client.checkpoint(&cp).await?.flags, CHECKPOINT_ACK);
            client.goaway(1).await?;
            Result::<()>::Ok(())
        }));
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        while fixture.processor.processed.load(Ordering::SeqCst) != 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await?;
    let full = files.retained_usage()?;
    assert_eq!(full.bytes, 3330);
    assert_eq!(full.objects, 4);
    assert_eq!(full.lineage_reservations, 2);
    for session in ["held-alice", "held-bob"] {
        let retained = fixture.store.load(session)?.unwrap().session;
        assert_eq!(retained.entities.len(), 1);
        assert!(!retained.work_scope_ready(0));
        assert!(
            !fixture
                .options
                .entity_directory
                .join(session)
                .join("lineage.sha256")
                .exists()
        );
    }
    // No new session can borrow the admitted work's unused completion credit.
    let owner = PrincipalBinding::new("issuer-a", "alice")?;
    let refused = files
        .reserve_lineage(Some(&owner), "cannot-borrow")
        .unwrap_err();
    assert!(format!("{refused:#}").contains("PIPESTREAM_LIMIT_EXCEEDED"));
    assert_eq!(files.retained_usage()?, full);
    drop(release);
    for callback in callbacks {
        tokio::time::timeout(Duration::from_secs(5), callback).await???;
    }
    for session in ["held-alice", "held-bob"] {
        let retained = fixture.store.load(session)?.unwrap().session;
        assert!(retained.checkpoints[&(0, 1)].acknowledged);
        assert_eq!(
            fs::read(
                fixture
                    .options
                    .entity_directory
                    .join(session)
                    .join("lineage.sha256")
            )?,
            retained.final_lineage_digest()?
        );
    }
    assert_eq!(files.retained_usage()?, full);
    fixture.store.integrity_check()?;
    Ok(())
}
