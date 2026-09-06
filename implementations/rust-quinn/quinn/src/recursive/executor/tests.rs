use super::*;
use std::sync::{Condvar, atomic::AtomicUsize};

#[derive(Default)]
struct Counting {
    processed: AtomicUsize,
    rehydrated: AtomicUsize,
    resumed: AtomicUsize,
}

impl EntityProcessor for Counting {
    fn process(&self, context: ProcessContext<'_>) -> Result<ProcessingDisposition, ProtocolError> {
        self.processed.fetch_add(1, Ordering::SeqCst);
        ExemplarProcessor::default().process(context)
    }
    fn rehydrate(&self, context: RehydrateContext<'_>) -> [u8; 32] {
        self.rehydrated.fetch_add(1, Ordering::SeqCst);
        ExemplarProcessor::default().rehydrate(context)
    }
    fn resume(&self, context: ResumeContext<'_>) -> [u8; 32] {
        self.resumed.fetch_add(1, Ordering::SeqCst);
        ExemplarProcessor::default().resume(context)
    }
}

fn service<P: EntityProcessor>(dir: &Path, processor: Arc<P>) -> RecursiveService<P> {
    RecursiveService::new(
        Arc::new(SqliteSessionStore::open(dir.join("state.sqlite3")).unwrap()),
        Arc::new(FileEntityStore::open(dir.join("entities")).unwrap()),
        processor,
        7,
        100,
    )
    .unwrap()
}

async fn enqueue<P: EntityProcessor>(
    service: &RecursiveService<P>,
    id: &str,
    action: &str,
) -> ExecutionKey {
    let header = exemplar_header(id, 1, None, None, action, b"retained input");
    let payload = service
        .entities
        .spool()
        .connection(None, 1024)
        .unwrap()
        .create()
        .await
        .unwrap()
        .append(b"retained input")
        .await
        .unwrap()
        .finish()
        .await
        .unwrap();
    let pending = StatusFrame {
        status: Status {
            state: pipestream_core::STATUS_PENDING,
            entity_id: 1,
            scope_id: 0,
            cursor: None,
            depth: 0,
        },
        extension: None,
    };
    let mut capabilities = service.capabilities.clone();
    capabilities.extensions = Default::default();
    service
        .prepare_entity(None, &pending, &header, &payload, &capabilities)
        .unwrap()
        .1
}

async fn terminal(store: &SqliteSessionStore, id: &str, key: ExecutionKey) -> JobState {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let state = store.load(id).unwrap().unwrap().session.jobs[&key]
                .state
                .clone();
            if !state.is_unfinished() {
                return state;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("job did not reach a retained outcome")
}

#[tokio::test]
async fn process_input_survives_abrupt_exit_and_is_executed_after_reopen() {
    const CHILD_DIR: &str = "PIPESTREAM_EXECUTOR_RESTART_DIR";
    if let Some(dir) = std::env::var_os(CHILD_DIR) {
        let service = service(Path::new(&dir), Arc::new(Counting::default()));
        enqueue(&service, "crashed-process", "complete").await;
        std::process::exit(0);
    }
    let dir = tempfile::tempdir().unwrap();
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "recursive::executor::tests::process_input_survives_abrupt_exit_and_is_executed_after_reopen", "--nocapture"])
        .env(CHILD_DIR, dir.path()).output().unwrap();
    assert!(
        child.status.success(),
        "{}",
        String::from_utf8_lossy(&child.stderr)
    );
    let processor = Arc::new(Counting::default());
    let service = service(dir.path(), processor.clone());
    let ready = service.store.ready_jobs(now_micros().unwrap(), 1).unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(processor.processed.load(Ordering::SeqCst), 0);
    let executor = service.start_executor().unwrap();
    assert_eq!(
        terminal(&service.store, "crashed-process", ready[0].key).await,
        JobState::Finished(JobOutput::Processed(ProcessOutcome::Complete))
    );
    assert_eq!(processor.processed.load(Ordering::SeqCst), 1);
    assert_eq!(executor.shutdown(Duration::from_secs(1)).await.unwrap(), 0);
    service.store.integrity_check().unwrap();
    assert_eq!(
        fs::read(
            dir.path()
                .join("entities/crashed-process/scope-0/entity-1.bin")
        )
        .unwrap(),
        b"retained input"
    );
}

#[tokio::test]
async fn missing_and_corrupt_retained_inputs_are_refused_before_callback() {
    for missing in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let processor = Arc::new(Counting::default());
        let service = service(dir.path(), processor.clone());
        let key = enqueue(&service, "invalid-input", "complete").await;
        let path = dir
            .path()
            .join("entities/invalid-input/scope-0/entity-1.bin");
        if missing {
            fs::remove_file(&path).unwrap();
        } else {
            fs::write(&path, b"tampered input").unwrap();
        }
        let executor = service.start_executor().unwrap();
        let JobState::Refused(failure) = terminal(&service.store, "invalid-input", key).await
        else {
            panic!("corrupt input was not refused")
        };
        assert_eq!(failure.code, pipestream_core::ERROR_INTEGRITY);
        assert_eq!(processor.processed.load(Ordering::SeqCst), 0);
        assert!(
            service
                .store
                .load("invalid-input")
                .unwrap()
                .unwrap()
                .session
                .final_lineage_digest()
                .is_err()
        );
        executor.shutdown(Duration::from_secs(1)).await.unwrap();
    }
}

#[tokio::test]
async fn rehydration_and_redemption_are_recovered_without_an_attached_connection() {
    let dir = tempfile::tempdir().unwrap();
    let processor = Arc::new(Counting::default());
    let first = service(dir.path(), processor.clone());
    let process = enqueue(&first, "resume-input", "yield").await;
    let executor = first.start_executor().unwrap();
    let JobState::Finished(JobOutput::Processed(ProcessOutcome::Deferred { claim_id, .. })) =
        terminal(&first.store, "resume-input", process).await
    else {
        panic!("yield did not retain a claim")
    };
    executor.shutdown(Duration::from_secs(1)).await.unwrap();
    let claim = first
        .store
        .load("resume-input")
        .unwrap()
        .unwrap()
        .session
        .claims[&claim_id]
        .clone();
    let resume = first
        .enqueue_redemption(&ClaimRedemption {
            session_id: "resume-input".into(),
            claim_id,
            state_checksum: claim.validation.state_checksum.unwrap(),
            acknowledged: false,
        })
        .unwrap();

    let mut session = Session::new("rehydrate-input", 7, 100).unwrap();
    let root = session
        .add_root(NewEntity {
            entity_id: 1,
            layer: 0,
            payload_digest: [1; 32],
            policy: None,
        })
        .unwrap();
    session.transition(root, EntityState::Processing).unwrap();
    session.begin_dehydrating(root).unwrap();
    session
        .open_child_scope(root, 1, now_micros().unwrap())
        .unwrap();
    let child = session
        .add_child(
            1,
            NewEntity {
                entity_id: 2,
                layer: 0,
                payload_digest: [2; 32],
                policy: None,
            },
        )
        .unwrap();
    session.transition(child, EntityState::Processing).unwrap();
    session.complete_entity(child, [3; 32]).unwrap();
    let digest = session.scope_digest(1).unwrap();
    first.store.create(&session).unwrap();
    let rehydrate = first
        .enqueue_rehydration("rehydrate-input", digest.clone())
        .unwrap();
    drop(first);
    let reopened = service(dir.path(), processor.clone());
    let executor = reopened.start_executor().unwrap();
    assert_eq!(
        terminal(&reopened.store, "resume-input", resume).await,
        JobState::Finished(JobOutput::Resumed)
    );
    assert_eq!(
        terminal(&reopened.store, "rehydrate-input", rehydrate).await,
        JobState::Finished(JobOutput::Rehydrated(digest))
    );
    assert_eq!(processor.resumed.load(Ordering::SeqCst), 1);
    assert_eq!(processor.rehydrated.load(Ordering::SeqCst), 1);
    assert_eq!(executor.shutdown(Duration::from_secs(1)).await.unwrap(), 0);
    reopened.store.integrity_check().unwrap();
}

#[test]
fn worker_permits_are_shared_by_store_and_bound_principal_and_global_execution() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteSessionStore::open(dir.path().join("state.sqlite3")).unwrap();
    let limits = ExecutionLimits {
        workers: 2,
        workers_per_principal: 1,
    };
    let pool = WorkerPool::open(store.path(), limits).unwrap();
    let alias = WorkerPool::open(store.path(), limits).unwrap();
    assert!(Arc::ptr_eq(&pool, &alias));
    assert!(WorkerPool::open(store.path(), ExecutionLimits::default()).is_err());
    let alice = PrincipalBinding::new("issuer", "alice").unwrap();
    let bob = PrincipalBinding::new("issuer", "bob").unwrap();
    let key = ExecutionKey {
        entity: EntityKey {
            entity_id: 1,
            scope_id: 0,
        },
        stage: ExecutionStage::Process,
    };
    let first = pool.acquire(Some(&alice), "alice-1", key).unwrap().unwrap();
    assert!(
        alias
            .acquire(Some(&alice), "alice-2", key)
            .unwrap()
            .is_none()
    );
    let second = alias.acquire(Some(&bob), "bob-1", key).unwrap().unwrap();
    assert!(pool.acquire(None, "anonymous", key).unwrap().is_none());
    drop(first);
    assert!(
        pool.acquire(Some(&alice), "alice-2", key)
            .unwrap()
            .is_some()
    );
    drop(second);
    assert_eq!(pool.active_count().unwrap(), 0);
}

#[derive(Default)]
struct Held {
    release: Mutex<bool>,
    changed: Condvar,
    calls: AtomicUsize,
}

impl Held {
    fn wait(&self) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let (_release, timeout) = self
            .changed
            .wait_timeout_while(
                self.release.lock().unwrap(),
                Duration::from_secs(5),
                |release| !*release,
            )
            .unwrap();
        assert!(
            !timeout.timed_out(),
            "test did not release blocking operation"
        );
    }

    fn release(&self) {
        *self.release.lock().unwrap() = true;
        self.changed.notify_all();
    }
}
struct Release(Arc<Held>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}
impl EntityProcessor for Held {
    fn process(&self, context: ProcessContext<'_>) -> Result<ProcessingDisposition, ProtocolError> {
        self.wait();
        ExemplarProcessor::default().process(context)
    }
    fn rehydrate(&self, _: RehydrateContext<'_>) -> [u8; 32] {
        unreachable!()
    }
    fn resume(&self, _: ResumeContext<'_>) -> [u8; 32] {
        unreachable!()
    }
}

struct HeldInstallation {
    files: FileEntityStore,
    hold: Arc<Held>,
    entity_id: u32,
}

impl EntityStore for HeldInstallation {
    fn bind_session_store(
        &self,
        store: &SqliteSessionStore,
    ) -> std::result::Result<(), StoreError> {
        self.files.bind_session_store(store)
    }

    fn put(&self, id: &str, key: EntityKey, payload: &[u8]) -> std::io::Result<()> {
        self.files.put(id, key, payload)
    }
    fn put_payload(
        &self,
        principal: Option<&PrincipalBinding>,
        id: &str,
        key: EntityKey,
        payload: &Payload,
    ) -> std::io::Result<()> {
        if key.entity_id == self.entity_id {
            self.hold.wait();
        }
        self.files.put_payload(principal, id, key, payload)
    }
    fn spool(&self) -> &Arc<SpoolStore> {
        self.files.spool()
    }
    fn load_payload(
        &self,
        principal: Option<&PrincipalBinding>,
        id: &str,
        key: EntityKey,
        length: u64,
        digest: [u8; 32],
    ) -> std::io::Result<Payload> {
        self.files.load_payload(principal, id, key, length, digest)
    }
    fn put_lineage(
        &self,
        principal: Option<&PrincipalBinding>,
        id: &str,
        digest: [u8; 32],
    ) -> std::io::Result<()> {
        self.files.put_lineage(principal, id, digest)
    }
}

#[tokio::test]
async fn pipelined_roots_wait_for_first_admission_without_serializing_processing() {
    let dir = tempfile::tempdir().unwrap();
    let options = super::super::tests::test_options(dir.path(), "held-installation");
    let store = Arc::new(SqliteSessionStore::open(&options.state_database).unwrap());
    let hold = Arc::new(Held::default());
    let _release = Release(hold.clone());
    let entities = Arc::new(HeldInstallation {
        files: FileEntityStore::open(&options.entity_directory).unwrap(),
        hold: hold.clone(),
        entity_id: 1,
    });
    let service = RecursiveService::new(
        store.clone(),
        entities.clone(),
        Arc::new(Counting::default()),
        7,
        100,
    )
    .unwrap();
    let server = RecursiveServer::bind(&options, service).unwrap();
    let address = server.local_addr().unwrap();
    let task = tokio::spawn(server.run(false));
    let mut client =
        RecursiveClient::connect(&super::super::tests::client_options(&options, address))
            .await
            .unwrap();
    let first = exemplar_header("pipelined", 1, None, None, "complete", b"x");
    client.write_entity_stream(&first, b"x").await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while hold.calls.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let second = exemplar_header("pipelined", 2, None, None, "complete", b"y");
    client.write_entity_stream(&second, b"y").await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), client.connection.closed())
            .await
            .is_err()
    );
    assert!(store.load("pipelined").unwrap().is_none());
    assert_eq!(entities.spool().usage().unwrap().files, 2);
    hold.release();

    let mut observed: BTreeMap<u32, Vec<u8>> = BTreeMap::new();
    tokio::time::timeout(Duration::from_secs(2), async {
        for _ in 0..4 {
            let (kind, bytes) = client.read_response().await.unwrap();
            assert_eq!(kind, FRAME_STATUS);
            let status = decode_status_frame(&bytes, client.layers).unwrap().status;
            observed
                .entry(status.entity_id)
                .or_default()
                .push(status.state);
        }
    })
    .await
    .unwrap();
    for id in [1, 2] {
        assert_eq!(
            observed[&id],
            [
                pipestream_core::STATUS_PROCESSING,
                pipestream_core::STATUS_COMPLETE
            ]
        );
    }
    assert_eq!(
        store.load("pipelined").unwrap().unwrap().session.jobs.len(),
        2
    );
    assert_eq!(store.unfinished_job_count().unwrap(), 0);
    store.integrity_check().unwrap();
    client.disconnect();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn checkpoint_waits_for_received_but_not_yet_admitted_payload() {
    let dir = tempfile::tempdir().unwrap();
    let options = super::super::tests::test_options(dir.path(), "checkpoint-installation");
    let store = Arc::new(SqliteSessionStore::open(&options.state_database).unwrap());
    let hold = Arc::new(Held::default());
    let _release = Release(hold.clone());
    let entities = Arc::new(HeldInstallation {
        files: FileEntityStore::open(&options.entity_directory).unwrap(),
        hold: hold.clone(),
        entity_id: 2,
    });
    let service = RecursiveService::new(
        store.clone(),
        entities,
        Arc::new(Counting::default()),
        7,
        100,
    )
    .unwrap();
    let server = RecursiveServer::bind(&options, service).unwrap();
    let address = server.local_addr().unwrap();
    let task = tokio::spawn(server.run(false));
    let mut client =
        RecursiveClient::connect(&super::super::tests::client_options(&options, address))
            .await
            .unwrap();
    let first = exemplar_header("checkpoint-installation", 1, None, None, "complete", b"x");
    client.send_entity(&first, b"x", 0).await.unwrap();
    let second = exemplar_header("checkpoint-installation", 2, None, None, "complete", b"y");
    // No PENDING: the received header itself makes this entity known to the receiver.
    client.write_entity_stream(&second, b"y").await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while hold.calls.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let request = Checkpoint {
        checkpoint_id: "installing".into(),
        sequence_number: 1,
        checkpoint_entity_id: 3,
        scope_id: None,
        flags: 0,
        timeout_ms: Some(20),
    };
    write_control(
        &mut client.control_send,
        &pipestream_core::encode_checkpoint(&request).unwrap(),
    )
    .await
    .unwrap();
    let error = tokio::time::timeout(Duration::from_millis(500), client.connection.closed())
        .await
        .unwrap();
    assert!(
        error.to_string().contains("PIPESTREAM_CHECKPOINT_TIMEOUT"),
        "{error}"
    );
    let state = store
        .load("checkpoint-installation")
        .unwrap()
        .unwrap()
        .session;
    assert_eq!(state.entities.len(), 1);
    assert!(
        state
            .checkpoints
            .values()
            .all(|checkpoint| !checkpoint.acknowledged)
    );
    hold.release();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let state = store
                .load("checkpoint-installation")
                .unwrap()
                .unwrap()
                .session;
            if state.entities.len() == 2 && store.unfinished_job_count().unwrap() == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn shutdown_does_not_free_a_stalled_callbacks_physical_worker_slot() {
    let dir = tempfile::tempdir().unwrap();
    let processor = Arc::new(Held::default());
    let _release = Release(processor.clone());
    let service = service(dir.path(), processor.clone())
        .with_execution_limits(ExecutionLimits {
            workers: 1,
            workers_per_principal: 1,
        })
        .unwrap()
        .with_execution_lease(Duration::from_millis(100))
        .unwrap();
    let key = enqueue(&service, "held", "complete").await;
    let first = service.start_executor().unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while processor.calls.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(first.shutdown(Duration::ZERO).await.unwrap(), 1);
    let second = service.start_executor().unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(processor.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        service
            .store
            .load("held")
            .unwrap()
            .unwrap()
            .session
            .executions[&key]
            .epoch,
        1
    );
    processor.release();
    assert_eq!(
        terminal(&service.store, "held", key).await,
        JobState::Finished(JobOutput::Processed(ProcessOutcome::Complete))
    );
    assert_eq!(processor.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        service
            .store
            .load("held")
            .unwrap()
            .unwrap()
            .session
            .executions[&key]
            .epoch,
        2
    );
    assert_eq!(second.shutdown(Duration::from_secs(1)).await.unwrap(), 0);
}
