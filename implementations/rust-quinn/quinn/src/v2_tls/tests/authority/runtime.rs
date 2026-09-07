//! Real authority workers and lifecycle maintenance; control dispatch remains
//! local. These tests do not claim a complete public durable-profile listener.
use super::*;
use crate::v2_authority::runtime::{Options, Runtime};
use pipestream_core::v2::authority::execution::{Application, ApplicationOutcome, WorkContext};

fn options() -> Options {
    let mut options = Options::default();
    options.workers.workers = 1;
    options.workers.workers_per_owner = 1;
    options.workers.scan_batch = 1;
    options.maintenance_batch = 1;
    options
}
fn start(db: &Database, apps: Arc<Applications>) -> Runtime {
    db.authority
        .start_runtime(
            apps,
            ResultEndpoint::new("localhost:7443".into()).unwrap(),
            caps(),
            options(),
        )
        .unwrap()
}
async fn until(mut ready: impl FnMut() -> bool) {
    tokio::time::timeout(HANDSHAKE, async {
        while !ready() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
async fn stop(runtime: Runtime) {
    runtime.request_stop();
    until(|| runtime.is_finished()).await;
    let snapshot = runtime.shutdown().unwrap();
    assert!(snapshot.finished);
    assert!(!snapshot.execution.unwrap().faulted);
    assert!(snapshot.maintenance.unwrap().fault.is_none());
}
async fn admit(
    db: &Database,
    connection: &Connection,
    wire: &Exchange,
    apps: Arc<Applications>,
    entity: u64,
) -> SessionIdentity {
    let inputs =
        crate::v2_authority::input::Inputs::new(db.authority.clone(), apps, Default::default())
            .unwrap();
    let identity = connection.input().unwrap().binding().identity.clone();
    let mut header = inputs::header(b"abc");
    header.generation = identity.generation;
    header.parameters.work.entity = Id(entity);
    header.operation = OperationId([(entity + 1) as u8; 16]);
    let mut bytes = header.encode_framed().unwrap();
    bytes.extend_from_slice(b"abc");
    let (reply, _) =
        inputs::transfer(&inputs, connection, wire.client.as_ref().unwrap(), bytes).await;
    assert!(matches!(
        reply.control(),
        Control::Work(Work::Admitted { .. })
    ));
    identity
}
async fn published(db: &Database, identity: &SessionIdentity) -> Manifest {
    until(|| {
        db.store
            .work_view(identity, &key(), Number(0))
            .unwrap()
            .1
            .state
            == State::SUCCEEDED
    })
    .await;
    db.store
        .work_view(identity, &key(), Number(0))
        .unwrap()
        .1
        .manifest
        .unwrap()
}
async fn pin(connection: &Connection, manifest: &Manifest) -> Response {
    response(
        connection,
        Control::Result(ResultMessage::Read {
            request: Id(3),
            work: manifest.work.clone(),
            attempt: manifest.attempt,
            index: OutputIndex(0),
            expected_sha256: manifest.outputs[0].sha256,
        }),
    )
    .await
}

#[tokio::test]
async fn runtime_discovers_admitted_work_and_retires_only_after_read_pins_and_safe_time() {
    let db = Database::new();
    let tls = Fixture::new();
    // This test releases the read explicitly; do not let a short idle expiry
    // race the clock/retirement assertions. Expiry has its own held-worker test.
    let mut selected = caps();
    selected.stream_idle_ms = IdleMs(30000);
    selected.stream_lifetime_ms = LifetimeMs(30000);
    let (connection, wire) = db.connect(&tls, selected, Some(0)).await;
    control(&connection, create(1)).await;
    control(&connection, declare(2, vec![Id(1)], true)).await;
    let apps = inputs::applications();
    let identity = admit(&db, &connection, &wire, apps.clone(), 1).await;
    // Admission predates runtime startup. No queue replay hint or direct run call.
    let runtime = start(&db, apps);
    let manifest = published(&db, &identity).await;
    let pinned = pin(&connection, &manifest).await;
    until(|| {
        runtime
            .snapshot()
            .execution
            .is_some_and(|s| s.closed_scopes > 0)
    })
    .await;
    db.clock.0.store(0, Ordering::SeqCst);
    until(|| {
        runtime.snapshot().maintenance.is_some_and(|s| {
            assert!(s.fault.is_none());
            s.last_refusal[1] == Some(ErrorCode::ClockUnsafe)
                && s.last_refusal[2] == Some(ErrorCode::ClockUnsafe)
        })
    })
    .await;
    assert_eq!(
        db.store
            .work_view(&identity, &key(), Number(0))
            .unwrap()
            .1
            .state,
        State::SUCCEEDED
    );
    db.clock.0.store(40000, Ordering::SeqCst);
    until(|| {
        runtime.snapshot().maintenance.is_some_and(|s| {
            assert_eq!(s.sessions_retired, 0);
            s.files_removed > 0
        })
    })
    .await;
    // Abort the pending read. Neither expiry nor cleanup may free its bytes first.
    drop(pinned);
    until(|| {
        runtime
            .snapshot()
            .maintenance
            .is_some_and(|s| s.sessions_retired == 1)
    })
    .await;
    assert!(matches!(
        db.store.attach_session(&identity.owner, &identity, &caps()),
        Err(store::StoreError::Protocol(Error {
            code: ErrorCode::NotFound,
            ..
        }))
    ));
    assert_eq!(db.store.next_creation(&identity.owner).unwrap(), Id(2));
    assert!(matches!(
        db.store
            .create_session(&identity.owner, Id(1), &policy(), &caps()),
        Err(store::StoreError::Protocol(Error {
            code: ErrorCode::Expired,
            ..
        }))
    ));
    stop(runtime).await;
}

struct Gate {
    pause: AtomicBool,
    entered: Notify,
    release: (Mutex<bool>, Condvar),
}
impl Gate {
    fn release(&self) {
        *self.release.0.lock().unwrap() = true;
        self.release.1.notify_all();
    }
}
struct Release(Arc<Gate>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}
impl Application for Gate {
    fn execute(&self, context: &mut WorkContext) -> Result<ApplicationOutcome, store::StoreError> {
        if self.pause.load(Ordering::SeqCst) {
            self.entered.notify_one();
            let (released, _) = self
                .release
                .1
                .wait_timeout_while(self.release.0.lock().unwrap(), HANDSHAKE, |released| {
                    !*released
                })
                .unwrap();
            assert!(*released, "test must release held application callback");
        }
        CopyApplication.execute(context)
    }
}

#[tokio::test]
async fn runtime_expires_reads_and_requests_stop_while_a_callback_is_blocked() {
    let db = Database::new();
    let tls = Fixture::new();
    let (connection, wire) = db.connect(&tls, caps(), Some(0)).await;
    control(&connection, create(1)).await;
    control(&connection, declare(2, vec![Id(1), Id(2)], true)).await;
    let gate = Arc::new(Gate {
        pause: AtomicBool::new(false),
        entered: Notify::new(),
        release: (Mutex::new(false), Condvar::new()),
    });
    let _release = Release(gate.clone());
    let mut apps = Applications::default();
    apps.register(
        ApplicationLabel("copy/v1".into()),
        vec![Mode(0)],
        RestartSafety::Pure,
        gate.clone(),
    )
    .unwrap();
    let apps = Arc::new(apps);
    let runtime = start(&db, apps.clone());
    let identity = admit(&db, &connection, &wire, apps.clone(), 1).await;
    let manifest = published(&db, &identity).await;
    let mut pinned = pin(&connection, &manifest).await;
    gate.pause.store(true, Ordering::SeqCst);
    admit(&db, &connection, &wire, apps, 2).await;
    tokio::time::timeout(HANDSHAKE, gate.entered.notified())
        .await
        .unwrap();
    until(|| {
        runtime
            .snapshot()
            .maintenance
            .is_some_and(|s| s.read_leases_closed == 1)
    })
    .await;
    let ResponseBody::Result(read) = pinned.body() else {
        panic!("expected pinned read")
    };
    assert!(matches!(
        read.next_deadline(),
        Err(store::StoreError::Protocol(Error {
            code: ErrorCode::LimitExceeded,
            ..
        }))
    ));
    until(|| runtime.snapshot().execution.is_some_and(|s| s.active == 1)).await;
    runtime.request_stop();
    assert!(!runtime.is_finished());
    gate.release();
    stop(runtime).await;
    assert_eq!(
        db.store
            .work_view(
                &identity,
                &WorkKey {
                    entity: Id(2),
                    ..key()
                },
                Number(0)
            )
            .unwrap()
            .1
            .state,
        State::SUCCEEDED
    );
}

#[tokio::test]
async fn runtime_rejects_invalid_limits_and_duplicate_execution_ownership() {
    let db = Database::new();
    for options in [
        Options {
            maintenance_batch: 0,
            ..options()
        },
        Options {
            maintenance_batch: 257,
            ..options()
        },
        Options {
            maintenance_interval: Duration::ZERO,
            ..options()
        },
        Options {
            maintenance_interval: Duration::from_secs(61),
            ..options()
        },
    ] {
        assert!(matches!(
            db.authority.start_runtime(
                inputs::applications(),
                ResultEndpoint::new("localhost:7443".into()).unwrap(),
                caps(),
                options
            ),
            Err(Error {
                code: ErrorCode::LimitExceeded,
                ..
            })
        ));
    }
    let runtime = start(&db, inputs::applications());
    assert!(matches!(
        db.authority.start_runtime(
            inputs::applications(),
            ResultEndpoint::new("localhost:7443".into()).unwrap(),
            caps(),
            options()
        ),
        Err(Error {
            code: ErrorCode::Conflict,
            ..
        })
    ));
    stop(runtime).await;
    stop(start(&db, inputs::applications())).await;
}
