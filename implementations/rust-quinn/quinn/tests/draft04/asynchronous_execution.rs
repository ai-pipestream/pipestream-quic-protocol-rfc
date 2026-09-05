use super::*;
use pipestream_core::{
    jobs::JobState,
    persistence::{JobQueueLimits, SessionStore},
    session::{EntityKey, EntityState},
};
use std::sync::{
    Condvar, Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Default)]
struct HeldProcessor {
    release: Mutex<bool>,
    changed: Condvar,
    started: AtomicUsize,
    active: AtomicUsize,
    peak: AtomicUsize,
}

impl HeldProcessor {
    fn hold_stage(&self) {
        self.active.fetch_add(1, Ordering::SeqCst);
        self.started.fetch_add(1, Ordering::SeqCst);
        let (_release, timeout) = self
            .changed
            .wait_timeout_while(
                self.release.lock().unwrap(),
                Duration::from_secs(5),
                |release| !*release,
            )
            .unwrap();
        assert!(!timeout.timed_out(), "test did not release callback");
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
    fn release(&self) {
        *self.release.lock().unwrap() = true;
        self.changed.notify_all();
    }

    async fn started(&self, count: usize) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(2), async {
            while self.started.load(Ordering::SeqCst) < count {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await?;
        Ok(())
    }
}

struct ReleaseOnDrop(Arc<HeldProcessor>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

impl EntityProcessor for HeldProcessor {
    fn process(&self, context: ProcessContext<'_>) -> Result<ProcessingDisposition, ProtocolError> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        self.started.fetch_add(1, Ordering::SeqCst);
        if context
            .header
            .metadata
            .get(ACTION_METADATA_KEY)
            .is_some_and(|action| action == "hold")
        {
            let (_release, timed_out) = self
                .changed
                .wait_timeout_while(
                    self.release.lock().unwrap(),
                    Duration::from_secs(5),
                    |release| !*release,
                )
                .unwrap();
            assert!(!timed_out.timed_out(), "test did not release callback");
        }
        self.active.fetch_sub(1, Ordering::SeqCst);
        Ok(ProcessingDisposition::Complete {
            output_digest: context.payload.digest(),
        })
    }
    fn rehydrate(&self, context: RehydrateContext<'_>) -> [u8; 32] {
        ExemplarProcessor::default().rehydrate(context)
    }
    fn resume(&self, context: ResumeContext<'_>) -> [u8; 32] {
        ExemplarProcessor::default().resume(context)
    }
}

#[tokio::test]
async fn stopping_listener_cancels_ingress_but_retains_admitted_execution() -> Result<()> {
    for once in [false, true] {
        let processor = Arc::new(HeldProcessor::default());
        let _release = ReleaseOnDrop(processor.clone());
        let mut peer = Fixture::with_runtime_mode(
            offer(LayerSupport::LAYER2, 7),
            processor.clone(),
            spool::SpoolLimits::default(),
            JobQueueLimits::default(),
            executor::ExecutionLimits::default(),
            once,
        )
        .await?;
        peer.entity(1, false, "hold", LayerSupport::LAYER2).await?;
        assert_eq!(peer.statuses(1).await?, [STATUS_PROCESSING]);
        processor.started(1).await?;

        let (mut header, _) = decode_entity(&entity(2, b"abc", "text/plain")?)?;
        header
            .metadata
            .insert(SESSION_METADATA_KEY.into(), "review-session".into());
        let encoded = encode_entity_header_for(&header, LayerSupport::LAYER2)?;
        let mut stream = peer.connection.open_uni().await?;
        stream
            .write_all(&(encoded.len() as u32).to_be_bytes())
            .await?;
        stream.write_all(&encoded).await?;
        stream.write_all(b"a").await?;
        // Keep the second payload incomplete while stopping the listener.
        let entities = FileEntityStore::open(&peer.options.entity_directory)?;
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let usage = entities.spool().usage().unwrap();
                if usage.files == 1 && usage.bytes == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await?;

        peer.server.abort();
        assert!((&mut peer.server).await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(2), async {
            while entities.spool().usage().unwrap().files != 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await?;
        assert_eq!(entities.spool().usage()?.bytes, 0);
        assert!(
            tokio::time::timeout(Duration::from_secs(2), read(&mut peer.recv))
                .await?
                .is_err()
        );
        assert_eq!(processor.active.load(Ordering::SeqCst), 1);

        let store = SqliteSessionStore::open(&peer.options.state_database)?;
        let key = EntityKey {
            scope_id: 0,
            entity_id: 1,
        };
        let session = store.load("review-session")?.unwrap().session;
        assert_eq!(session.entities.len(), 1);
        assert_eq!(session.entities[&key].state, EntityState::Processing);
        assert_eq!(session.jobs.len(), 1);
        assert_eq!(store.unfinished_job_count()?, 1);

        // A started callback may still publish under its valid fence after dispatch stops.
        processor.release();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let session = store.load("review-session").unwrap().unwrap().session;
                if session.entities[&key].state == EntityState::Complete {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await?;
        assert_eq!(store.unfinished_job_count()?, 0);
        assert_eq!(processor.started.load(Ordering::SeqCst), 1);
        store.integrity_check()?;
    }
    Ok(())
}

#[tokio::test]
async fn slow_callback_does_not_block_another_job_on_the_same_quic_connection() -> Result<()> {
    let processor = Arc::new(HeldProcessor::default());
    let _release = ReleaseOnDrop(processor.clone());
    let mut peer = Fixture::with_processor(LayerSupport::LAYER2, 7, processor.clone()).await?;
    peer.entity(1, false, "hold", LayerSupport::LAYER2).await?;
    assert_eq!(peer.statuses(1).await?, [STATUS_PROCESSING]);
    processor.started(1).await?;
    peer.entity(2, false, "complete", LayerSupport::LAYER2)
        .await?;
    assert_eq!(
        peer.statuses(2).await?,
        [STATUS_PROCESSING, STATUS_COMPLETE]
    );
    let retained = SqliteSessionStore::open(&peer.options.state_database)?
        .load("review-session")?
        .unwrap();
    assert_eq!(
        retained.session.entities[&EntityKey {
            scope_id: 0,
            entity_id: 1
        }]
            .state,
        EntityState::Processing
    );
    assert_eq!(
        retained.session.entities[&EntityKey {
            scope_id: 0,
            entity_id: 2
        }]
            .state,
        EntityState::Complete
    );
    assert_eq!(processor.active.load(Ordering::SeqCst), 1);
    processor.release();
    assert_eq!(peer.statuses(1).await?, [STATUS_COMPLETE]);
    assert!(
        retained
            .session
            .jobs
            .values()
            .any(|job| job.state == JobState::Running)
    );
    Ok(())
}

#[tokio::test]
async fn checkpoint_deadline_progresses_while_callback_is_stalled() -> Result<()> {
    let processor = Arc::new(HeldProcessor::default());
    let _release = ReleaseOnDrop(processor.clone());
    let mut peer = Fixture::with_processor(LayerSupport::LAYER2, 7, processor.clone()).await?;
    peer.entity(1, false, "hold", LayerSupport::LAYER2).await?;
    assert_eq!(peer.statuses(1).await?, [STATUS_PROCESSING]);
    processor.started(1).await?;
    let started = tokio::time::Instant::now();
    peer.send
        .write_all(&encode_checkpoint(&checkpoint(20))?)
        .await?;
    let reason = peer.refused().await?;
    assert!(reason.contains("PIPESTREAM_CHECKPOINT_TIMEOUT"), "{reason}");
    assert!(started.elapsed() < Duration::from_millis(500));
    assert_eq!(processor.active.load(Ordering::SeqCst), 1);
    let retained = SqliteSessionStore::open(&peer.options.state_database)?
        .load("review-session")?
        .unwrap();
    assert!(
        retained
            .session
            .checkpoints
            .values()
            .all(|checkpoint| !checkpoint.acknowledged)
    );
    processor.release();
    Ok(())
}

#[tokio::test]
async fn invalid_control_is_refused_without_waiting_for_callback_completion() -> Result<()> {
    let processor = Arc::new(HeldProcessor::default());
    let _release = ReleaseOnDrop(processor.clone());
    let mut peer = Fixture::with_processor(LayerSupport::LAYER2, 7, processor.clone()).await?;
    peer.entity(1, false, "hold", LayerSupport::LAYER2).await?;
    peer.statuses(1).await?;
    processor.started(1).await?;
    peer.send
        .write_all(&encode_status(Status {
            state: STATUS_COMPLETE,
            entity_id: 2,
            scope_id: 0,
            cursor: None,
            depth: 0,
        })?)
        .await?;
    assert!(peer.refused().await?.contains("PIPESTREAM_ENTITY_INVALID"));
    assert_eq!(processor.active.load(Ordering::SeqCst), 1);
    processor.release();
    Ok(())
}

#[tokio::test]
async fn queue_overload_refuses_admission_without_losing_earlier_jobs() -> Result<()> {
    let processor = Arc::new(HeldProcessor::default());
    let _release = ReleaseOnDrop(processor.clone());
    let mut peer = Fixture::with_runtime_limits(
        offer(LayerSupport::LAYER2, 7),
        processor.clone(),
        spool::SpoolLimits::default(),
        JobQueueLimits {
            total: 2,
            per_principal: 2,
        },
        executor::ExecutionLimits {
            workers: 1,
            workers_per_principal: 1,
        },
    )
    .await?;
    peer.entity(1, false, "hold", LayerSupport::LAYER2).await?;
    peer.statuses(1).await?;
    processor.started(1).await?;
    peer.entity(2, false, "complete", LayerSupport::LAYER2)
        .await?;
    assert_eq!(peer.statuses(1).await?, [STATUS_PROCESSING]);
    peer.entity(3, false, "complete", LayerSupport::LAYER2)
        .await?;
    let reason = peer.refused().await?;
    assert!(
        reason.contains("PIPESTREAM_LIMIT_EXCEEDED")
            && reason.contains("durable job queue is full"),
        "{reason}"
    );
    let store = SqliteSessionStore::open(&peer.options.state_database)?;
    let state = store.load("review-session")?.unwrap().session;
    assert_eq!(state.jobs.len(), 2);
    assert_eq!(store.unfinished_job_count()?, 2);
    assert!(!state.entities.contains_key(&EntityKey {
        scope_id: 0,
        entity_id: 3
    }));
    assert_eq!(processor.started.load(Ordering::SeqCst), 1);
    assert_eq!(processor.peak.load(Ordering::SeqCst), 1);
    processor.release();
    Ok(())
}

#[tokio::test]
async fn callback_panic_has_a_retained_refusal_not_an_automatic_retry_loop() -> Result<()> {
    struct Panics;
    impl EntityProcessor for Panics {
        fn process(&self, _: ProcessContext<'_>) -> Result<ProcessingDisposition, ProtocolError> {
            panic!("injected application panic");
        }
        fn rehydrate(&self, _: RehydrateContext<'_>) -> [u8; 32] {
            unreachable!()
        }
        fn resume(&self, _: ResumeContext<'_>) -> [u8; 32] {
            unreachable!()
        }
    }
    let mut peer = Fixture::with_processor(LayerSupport::LAYER2, 7, Arc::new(Panics)).await?;
    peer.entity(1, false, "complete", LayerSupport::LAYER2)
        .await?;
    assert_eq!(peer.statuses(1).await?, [STATUS_PROCESSING]);
    assert!(
        peer.refused()
            .await?
            .contains("application callback panicked")
    );
    let store = SqliteSessionStore::open(&peer.options.state_database)?;
    let state = store.load("review-session")?.unwrap().session;
    assert!(
        state
            .jobs
            .values()
            .all(|job| matches!(job.state, JobState::Refused(_)))
    );
    assert!(state.final_lineage_digest().is_err());
    assert_eq!(store.unfinished_job_count()?, 0);
    store.integrity_check()?;
    Ok(())
}

#[tokio::test]
async fn invalid_application_decision_is_refused_without_waiting_for_lease_expiry() -> Result<()> {
    struct InvalidYield;
    impl EntityProcessor for InvalidYield {
        fn process(
            &self,
            context: ProcessContext<'_>,
        ) -> Result<ProcessingDisposition, ProtocolError> {
            let ProcessingDisposition::Yield {
                continuation_token,
                validation,
                expires_at_micros,
                ..
            } = ExemplarProcessor::default().process(context)?
            else {
                unreachable!()
            };
            Ok(ProcessingDisposition::Yield {
                reason: 0,
                continuation_token,
                validation,
                expires_at_micros,
            })
        }
        fn rehydrate(&self, _: RehydrateContext<'_>) -> [u8; 32] {
            unreachable!()
        }
        fn resume(&self, _: ResumeContext<'_>) -> [u8; 32] {
            unreachable!()
        }
    }
    let mut peer = Fixture::with_processor(LayerSupport::LAYER2, 7, Arc::new(InvalidYield)).await?;
    peer.entity(1, false, "yield", LayerSupport::LAYER2).await?;
    peer.statuses(1).await?;
    assert!(peer.refused().await?.contains("yield reason is unassigned"));
    let store = SqliteSessionStore::open(&peer.options.state_database)?;
    let session = store.load("review-session")?.unwrap().session;
    assert!(session.claims.is_empty());
    assert!(
        session
            .jobs
            .values()
            .all(|job| matches!(job.state, JobState::Refused(_)))
    );
    assert_eq!(store.unfinished_job_count()?, 0);
    Ok(())
}

#[tokio::test]
async fn rehydration_and_resume_callbacks_do_not_hold_checkpoint_deadlines() -> Result<()> {
    struct HeldStage {
        gate: Arc<HeldProcessor>,
        resume: bool,
    }
    impl EntityProcessor for HeldStage {
        fn process(
            &self,
            context: ProcessContext<'_>,
        ) -> Result<ProcessingDisposition, ProtocolError> {
            ExemplarProcessor::default().process(context)
        }
        fn rehydrate(&self, context: RehydrateContext<'_>) -> [u8; 32] {
            if !self.resume {
                self.gate.hold_stage();
            }
            ExemplarProcessor::default().rehydrate(context)
        }
        fn resume(&self, context: ResumeContext<'_>) -> [u8; 32] {
            if self.resume {
                self.gate.hold_stage();
            }
            ExemplarProcessor::default().resume(context)
        }
    }
    for resume in [false, true] {
        let gate = Arc::new(HeldProcessor::default());
        let _release = ReleaseOnDrop(gate.clone());
        let mut peer = Fixture::with_processor(
            LayerSupport::LAYER2,
            7,
            Arc::new(HeldStage {
                gate: gate.clone(),
                resume,
            }),
        )
        .await?;
        if resume {
            peer.entity(1, false, "yield", LayerSupport::LAYER2).await?;
            assert_eq!(
                peer.statuses(3).await?,
                [STATUS_PROCESSING, STATUS_YIELDED, STATUS_DEFERRED]
            );
            let store = SqliteSessionStore::open(&peer.options.state_database)?;
            let state = store.load("review-session")?.unwrap().session;
            let claim = state.claims.values().next().unwrap();
            peer.send
                .write_all(&encode_claim_redemption(&ClaimRedemption {
                    session_id: "review-session".into(),
                    claim_id: claim.claim_id,
                    state_checksum: claim.validation.state_checksum.unwrap(),
                    acknowledged: false,
                })?)
                .await?;
        } else {
            peer.entity(1, false, "dehydrate", LayerSupport::LAYER2)
                .await?;
            peer.statuses(2).await?;
            peer.entity(1, true, "complete", LayerSupport::LAYER2)
                .await?;
            peer.statuses(2).await?;
            let digest = ScopeDigest {
                scope_id: 1,
                entities_processed: 1,
                entities_succeeded: 1,
                entities_failed: 0,
                entities_deferred: 0,
                merkle_root: pipestream_core::session::merkle_root(&[(1, EntityState::Complete)])?,
            };
            peer.send.write_all(&encode_scope_digest(&digest)?).await?;
        }
        gate.started(1).await?;
        let started = tokio::time::Instant::now();
        peer.send
            .write_all(&encode_checkpoint(&checkpoint(20))?)
            .await?;
        assert!(
            peer.refused()
                .await?
                .contains("PIPESTREAM_CHECKPOINT_TIMEOUT")
        );
        assert!(started.elapsed() < Duration::from_millis(500));
        assert_eq!(gate.active.load(Ordering::SeqCst), 1);
        gate.release();
    }
    Ok(())
}
