use super::*;

struct ReentrantProcessor {
    store: Arc<SqliteSessionStore>,
    calls: [AtomicU64; 3],
}

impl ReentrantProcessor {
    fn probe(&self, lease: &ExecutionLease, stage: usize) {
        // This writer would time out if the service held its callback transaction.
        self.store
            .transact(lease.session_id(), |session| {
                let record = &session.executions[&lease.key()];
                assert_eq!(record.epoch, lease.epoch());
                assert_eq!(record.executor, lease.executor());
                assert!(record.completed_at_micros.is_none());
                Ok(())
            })
            .unwrap();
        self.calls[stage].fetch_add(1, Ordering::SeqCst);
    }
}

impl EntityProcessor for ReentrantProcessor {
    fn process(&self, context: ProcessContext<'_>) -> ProcessingDisposition {
        self.probe(context.execution, 0);
        ExemplarProcessor::default().process(context)
    }
    fn rehydrate(&self, context: RehydrateContext<'_>) -> [u8; 32] {
        self.probe(context.execution, 1);
        ExemplarProcessor::default().rehydrate(context)
    }
    fn resume(&self, context: ResumeContext<'_>) -> [u8; 32] {
        self.probe(context.execution, 2);
        ExemplarProcessor::default().resume(context)
    }
}

#[tokio::test]
async fn every_callback_can_reenter_store_and_has_a_durable_fence() {
    let dir = tempfile::tempdir().unwrap();
    let options = tests::test_options(dir.path(), "reentrant");
    let store = Arc::new(SqliteSessionStore::open(&options.state_database).unwrap());
    let processor = Arc::new(ReentrantProcessor {
        store: store.clone(),
        calls: std::array::from_fn(|_| AtomicU64::new(0)),
    });
    let service = RecursiveService::new(
        store.clone(),
        Arc::new(FileEntityStore::open(&options.entity_directory).unwrap()),
        processor.clone(),
        7,
        1_000,
    )
    .unwrap();
    let server = RecursiveServer::bind(&options, service).unwrap();
    let client = tests::client_options(&options, server.local_addr().unwrap());
    let task = tokio::spawn(server.run(false));
    run_recursive_scenario(&client, "reentrant-tree")
        .await
        .unwrap();
    let claim = begin_durable_yield(&client, "reentrant-claim")
        .await
        .unwrap();
    finish_durable_yield(&client, &claim).await.unwrap();
    assert_eq!(processor.calls[0].load(Ordering::SeqCst), 7);
    assert_eq!(processor.calls[1].load(Ordering::SeqCst), 2);
    assert_eq!(processor.calls[2].load(Ordering::SeqCst), 1);
    for id in ["reentrant-tree", "reentrant-claim"] {
        let retained = store.load(id).unwrap().unwrap();
        assert!(
            retained
                .session
                .executions
                .values()
                .all(|r| r.completed_at_micros.is_some())
        );
        assert!(retained.session.final_lineage_digest().is_ok());
    }
    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn overlong_callback_cannot_publish_a_successful_protocol_result() {
    struct Slow;
    impl EntityProcessor for Slow {
        fn process(&self, context: ProcessContext<'_>) -> ProcessingDisposition {
            std::thread::sleep(Duration::from_millis(25));
            ExemplarProcessor::default().process(context)
        }
        fn rehydrate(&self, _: RehydrateContext<'_>) -> [u8; 32] {
            unreachable!()
        }
        fn resume(&self, _: ResumeContext<'_>) -> [u8; 32] {
            unreachable!()
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let options = tests::test_options(dir.path(), "expired-executor");
    let store = Arc::new(SqliteSessionStore::open(&options.state_database).unwrap());
    let service = RecursiveService::new(
        store.clone(),
        Arc::new(FileEntityStore::open(&options.entity_directory).unwrap()),
        Arc::new(Slow),
        7,
        1_000,
    )
    .unwrap()
    .with_execution_lease(Duration::from_millis(1))
    .unwrap();
    let server = RecursiveServer::bind(&options, service).unwrap();
    let client = tests::client_options(&options, server.local_addr().unwrap());
    let task = tokio::spawn(server.run(true));
    let error = begin_durable_yield(&client, "overlong-callback")
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("execution lease is stale or expired"));
    assert!(task.await.unwrap().is_err());
    let retained = store.load("overlong-callback").unwrap().unwrap();
    assert!(retained.session.claims.is_empty());
    assert!(
        retained
            .session
            .executions
            .values()
            .all(|r| r.completed_at_micros.is_none())
    );
    assert!(
        retained
            .session
            .entities
            .values()
            .all(|r| r.state == EntityState::Processing && r.output_digest.is_none())
    );
}

#[test]
fn panic_leaves_durable_resume_attempt_for_expiry_and_reacquisition() {
    struct Interrupted(AtomicU64);
    impl EntityProcessor for Interrupted {
        fn process(&self, _: ProcessContext<'_>) -> ProcessingDisposition {
            unreachable!()
        }
        fn rehydrate(&self, _: RehydrateContext<'_>) -> [u8; 32] {
            unreachable!()
        }
        fn resume(&self, context: ResumeContext<'_>) -> [u8; 32] {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("injected callback interruption before publication");
            }
            assert_eq!(context.execution.epoch(), 2);
            ExemplarProcessor::default().resume(context)
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite3");
    let store = Arc::new(SqliteSessionStore::open(&path).unwrap());
    let mut session = Session::new("interrupted-resume", 7, 100).unwrap();
    let entity = session
        .add_root(NewEntity {
            entity_id: 1,
            layer: 0,
            payload_digest: [1; 32],
            policy: None,
        })
        .unwrap();
    session.transition(entity, EntityState::Processing).unwrap();
    let now = now_micros().unwrap();
    session
        .defer_with_claim_id(
            entity,
            b"continuation".to_vec(),
            StoppingPointValidation {
                state_checksum: Some([2; 32]),
                bytes_processed: None,
                children_complete: None,
                children_total: None,
                is_resumable: Some(true),
                checkpoint_ref: None,
            },
            99,
            now + 60_000_000,
            now,
        )
        .unwrap();
    session.redeem_claim(99, [2; 32], now).unwrap();
    store.create(&session).unwrap();
    let processor = Arc::new(Interrupted(AtomicU64::new(0)));
    let entities = Arc::new(FileEntityStore::open(dir.path().join("entities")).unwrap());
    let service = RecursiveService::new(store.clone(), entities.clone(), processor.clone(), 7, 100)
        .unwrap()
        .with_execution_lease(Duration::from_millis(50))
        .unwrap();
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        service.recover_interrupted_resumptions()
    }));
    assert!(failed.is_err());
    let interrupted = store.load("interrupted-resume").unwrap().unwrap();
    let key = ExecutionKey {
        entity,
        stage: ExecutionStage::Resume { claim_id: 99 },
    };
    assert_eq!(
        interrupted.session.entities[&entity].state,
        EntityState::Processing
    );
    assert_eq!(interrupted.session.executions[&key].epoch, 1);
    assert!(
        interrupted.session.executions[&key]
            .completed_at_micros
            .is_none()
    );
    drop(service);
    drop(store);
    std::thread::sleep(Duration::from_millis(60));
    let store = Arc::new(SqliteSessionStore::open(&path).unwrap());
    let service =
        RecursiveService::new(store.clone(), entities, processor.clone(), 7, 100).unwrap();
    assert_eq!(service.recover_interrupted_resumptions().unwrap(), 1);
    assert_eq!(service.recover_interrupted_resumptions().unwrap(), 0);
    let completed = store.load("interrupted-resume").unwrap().unwrap();
    assert_eq!(
        completed.session.entities[&entity].state,
        EntityState::Complete
    );
    assert_eq!(completed.session.executions[&key].epoch, 2);
    assert!(
        completed.session.executions[&key]
            .completed_at_micros
            .is_some()
    );
    assert_eq!(processor.0.load(Ordering::SeqCst), 2);
}
