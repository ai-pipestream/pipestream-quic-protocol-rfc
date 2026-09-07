//! Dispatcher tests with actual authenticated TLS peers and guarded on-disk
//! authority storage. Control calls below are local, not V2 wire conformance.
use super::*;
use crate::v2_authority::{Authority, Connection, Pending, Response, ResponseBody, Submission};
use pipestream_core::v2::Digest;
use pipestream_core::{
    persistence::PhysicalLimits,
    v2::authority::{
        self as store, AuthorityStore, Authorization, ClockReading, Permission, StorePolicy,
        execution::{CopyApplication, Executor, ResultEndpoint},
        ingress::{Applications, InputPreparation, InputReception, RestartSafety},
        payload::{PayloadPolicy, PayloadStore},
    },
};
use sha2::Digest as _;
use std::sync::{Condvar, atomic::AtomicBool};
use tokio::sync::Notify;

struct StoreClock;
impl store::Clock for StoreClock {
    fn read(&self) -> ClockReading {
        ClockReading {
            utc_ms: Number(1000),
            trusted: true,
        }
    }
}
struct Access {
    allowed: AtomicBool,
    pause: AtomicBool,
    pause_cancel: AtomicBool,
    entered: Notify,
    release: (Mutex<bool>, Condvar),
}
impl Default for Access {
    fn default() -> Self {
        Self {
            allowed: AtomicBool::new(true),
            pause: AtomicBool::new(false),
            pause_cancel: AtomicBool::new(false),
            entered: Notify::new(),
            release: (Mutex::new(false), Condvar::new()),
        }
    }
}
impl Access {
    fn release(&self) {
        *self.release.0.lock().unwrap() = true;
        self.release.1.notify_all();
    }
}
struct ReleaseOnDrop(Arc<Access>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}
impl Authorization for Access {
    fn permits(&self, owner: &IdentityLabel, permission: Permission) -> bool {
        if (permission == Permission::Create && self.pause.swap(false, Ordering::SeqCst))
            || (permission == Permission::Cancel && self.pause_cancel.swap(false, Ordering::SeqCst))
        {
            self.entered.notify_one();
            let (released, _) = self
                .release
                .1
                .wait_timeout_while(self.release.0.lock().unwrap(), HANDSHAKE, |released| {
                    !*released
                })
                .unwrap();
            assert!(
                *released,
                "test must release the deliberately blocked storage operation"
            );
        }
        self.allowed.load(Ordering::SeqCst) && matches!(owner.0.as_str(), "alice" | "bob")
    }
}
struct Database {
    _directory: tempfile::TempDir,
    store: AuthorityStore,
    payloads: PayloadStore,
    access: Arc<Access>,
    authority: Authority,
}
impl Database {
    fn new() -> Self {
        Self::with_authority("issuer-a")
    }
    fn with_authority(name: &str) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let access = Arc::new(Access::default());
        let store = AuthorityStore::initialize(
            &directory.path().join("authority.sqlite"),
            IdentityLabel(name.into()),
            StorePolicy {
                owners: Id(2),
                sessions: Id(8),
                sessions_per_owner: Id(4),
                active_jobs: Id(8),
                active_jobs_per_owner: Id(4),
                session_limits: Limits {
                    scopes: Id(8),
                    entities: Id(1000),
                    operations: Id(64),
                    active_jobs: Id(4),
                    retained_input_bytes: Number(1 << 20),
                    retained_output_bytes: Number(1 << 20),
                },
            },
            PhysicalLimits::default(),
            Arc::new(StoreClock),
            access.clone(),
        )
        .unwrap();
        let payloads = PayloadStore::initialize(
            &directory.path().join("objects"),
            store.payload_identity().unwrap(),
            PayloadPolicy {
                objects: Id(32),
                bytes: Number(4 << 20),
                owner_objects: Id(32),
                owner_bytes: Number(4 << 20),
                chunk_bytes: Id(8192),
                handles: Id(16),
                owner_handles: Id(16),
            },
        )
        .unwrap();
        store.bind_payloads(&payloads).unwrap();
        let authority = Authority::new(store.clone(), payloads.clone(), 1).unwrap();
        Self {
            _directory: directory,
            store,
            payloads,
            access,
            authority,
        }
    }
    async fn connect(
        &self,
        tls: &Fixture,
        caps: Capabilities,
        principal: Option<usize>,
    ) -> (Connection, Exchange) {
        let mut exchange = tls.connect(tls.config(principal), "localhost").await;
        let connection = self.adapter(tls, &mut exchange, caps).unwrap();
        (connection, exchange)
    }
    fn adapter(
        &self,
        tls: &Fixture,
        exchange: &mut Exchange,
        caps: Capabilities,
    ) -> Result<Connection, Error> {
        let peer = std::mem::replace(
            &mut exchange.server,
            Err(anyhow::anyhow!("peer transferred")),
        )
        .unwrap();
        let security = ServerSecurity::new(
            vec![tls.certificate.der.clone()],
            tls.certificate.key.clone_key(),
            Some(tls.policy.clone()),
        )
        .unwrap();
        self.authority
            .connection(Arc::new(peer), Arc::new(security), caps)
    }
}
fn caps() -> Capabilities {
    let mut caps = offer(true);
    caps.response = ResponseFlag(1);
    caps.control_limit = ControlLimit(8192);
    caps.object_limit = Number(1 << 20);
    caps.pending_limit = ConcurrencyLimit(4);
    caps
}
fn policy() -> Policy {
    Policy {
        execution_limit_ms: pipestream_core::v2::Duration(10000),
        output_retention_ms: pipestream_core::v2::Duration(20000),
        receipt_retention_ms: pipestream_core::v2::Duration(30000),
    }
}
fn create(request: u64) -> Control {
    Control::Session(Session::Create {
        request: Id(request),
        creation_sequence: Id(1),
        policy: policy(),
    })
}
fn next(request: u64) -> Control {
    Control::Session(Session::NextSequence {
        request: Id(request),
    })
}
fn attach(request: u64, owner: &str, generation: u64) -> Control {
    Control::Session(Session::Attach {
        request: Id(request),
        authority: IdentityLabel("issuer-a".into()),
        owner: IdentityLabel(owner.into()),
        generation: Id(generation),
    })
}
fn declare(request: u64, entities: Vec<Id>, seal: bool) -> Control {
    Control::Scope(Scope::Declare {
        request: Id(request),
        operation: OperationId([1; 16]),
        scope: Number(0),
        entity_ids: entities,
        seal,
    })
}
fn key() -> WorkKey {
    WorkKey {
        scope: Number(0),
        producer: Producer(0),
        entity: Id(1),
    }
}
fn pending(connection: &Connection, message: Control) -> Pending {
    match connection.submit(message).unwrap() {
        Submission::Pending(pending) => pending,
        Submission::Refused(refusal) => panic!("unexpected {refusal:?}"),
    }
}
async fn response(connection: &Connection, message: Control) -> Response {
    tokio::time::timeout(HANDSHAKE, pending(connection, message).run())
        .await
        .unwrap()
        .unwrap()
}
async fn control(connection: &Connection, message: Control) -> Control {
    match response(connection, message).await.body() {
        ResponseBody::Control(control) => control.clone(),
        ResponseBody::Result(_) => panic!("unexpected result transfer"),
    }
}
fn refused(control: &Control, request: u64, expected: ErrorCode) {
    assert!(
        matches!(control, Control::Refusal(Refusal { request: RequestTag::Control { request: id }, code, .. }) if id.0 == request && *code == expected),
        "{control:?}"
    );
}
fn immediate(connection: &Connection, message: Control, request: u64, code: ErrorCode) {
    match connection.submit(message).unwrap() {
        Submission::Refused(control) => refused(&control, request, code),
        Submission::Pending(_) => panic!("unexpected acceptance"),
    }
}
async fn root(connection: &Connection, request: u64, seal: Digest) -> ScopeSummary {
    match control(
        connection,
        Control::Scope(Scope::Checkpoint {
            request: Id(request),
            scope: Number(0),
            seal,
            wait_ms: WaitMs(0),
        }),
    )
    .await
    {
        Control::Scope(Scope::CheckpointResponse { summary, .. }) => summary,
        other => panic!("unexpected {other:?}"),
    }
}
fn seal(control: Control) -> Digest {
    let Control::Scope(Scope::Declared { receipt, .. }) = control else {
        panic!("declaration expected")
    };
    let Outcome::Declared {
        seal: Some(seal), ..
    } = receipt.body
    else {
        panic!("seal expected")
    };
    seal
}

#[tokio::test]
async fn dispatcher_creation_replay_attachment_and_single_binding() {
    let db = Database::new();
    let tls = Fixture::new();
    let (first, _first_wire) = db.connect(&tls, caps(), Some(0)).await;
    assert_eq!(
        control(&first, next(1)).await,
        Control::Session(Session::Sequence {
            request: Id(1),
            next_creation_sequence: Id(1)
        })
    );
    let creating = pending(&first, create(2));
    immediate(&first, create(3), 3, ErrorCode::Conflict);
    assert!(matches!(
        creating.run().await.unwrap().body(),
        ResponseBody::Control(Control::Session(Session::Binding {
            generation: Id(1),
            ..
        }))
    ));
    immediate(&first, attach(4, "alice", 1), 4, ErrorCode::Conflict);
    let (replay, _replay_wire) = db.connect(&tls, caps(), Some(1)).await;
    assert!(matches!(
        control(&replay, create(1)).await,
        Control::Session(Session::Binding {
            generation: Id(1),
            creation_sequence: Id(1),
            ..
        })
    ));
    let (other, _other_wire) = db.connect(&tls, caps(), Some(0)).await;
    refused(
        &control(&other, attach(1, "bob", 1)).await,
        1,
        ErrorCode::Unauthorized,
    );
    refused(
        &control(&other, attach(2, "alice", 99)).await,
        2,
        ErrorCode::NotFound,
    );
    assert!(matches!(
        control(&other, attach(3, "alice", 1)).await,
        Control::Session(Session::Binding {
            generation: Id(1),
            ..
        })
    ));
}

#[tokio::test]
async fn dispatcher_wrong_direction_and_duplicate_ids_are_fatal_but_refusals_consume_ids() {
    let db = Database::new();
    let tls = Fixture::new();
    let (connection, _wire) = db.connect(&tls, caps(), Some(0)).await;
    immediate(
        &connection,
        declare(1, vec![], true),
        1,
        ErrorCode::NotReady,
    );
    assert!(matches!(
        connection.submit(next(1)),
        Err(Error {
            code: ErrorCode::FrameError,
            ..
        })
    ));
    assert!(matches!(
        connection.submit(Control::Drain(Drain::Detached { request: Id(2) })),
        Err(Error {
            code: ErrorCode::FrameError,
            ..
        })
    ));
    assert!(matches!(
        connection.submit(Control::Capabilities(caps())),
        Err(Error {
            code: ErrorCode::FrameError,
            ..
        })
    ));
    assert_eq!(
        control(&connection, next(7)).await,
        Control::Session(Session::Sequence {
            request: Id(7),
            next_creation_sequence: Id(1)
        })
    );
    let (fresh, _fresh_wire) = db.connect(&tls, caps(), Some(0)).await;
    assert!(matches!(
        fresh.submit(next(2)),
        Err(Error {
            code: ErrorCode::FrameError,
            ..
        })
    ));
}

#[tokio::test]
async fn dispatcher_pending_slots_include_unsent_responses_and_cloned_input_jobs() {
    let db = Database::new();
    let tls = Fixture::new();
    let mut caps = caps();
    caps.pending_limit = ConcurrencyLimit(2);
    caps.stream_limit = ConcurrencyLimit(1);
    let (connection, _wire) = db.connect(&tls, caps, Some(0)).await;
    control(&connection, create(1)).await;
    let input = connection.input().unwrap();
    let cloned_job = input.clone();
    drop(input);
    assert!(matches!(
        connection.input(),
        Err(Error {
            code: ErrorCode::LimitExceeded,
            ..
        })
    ));
    let held = response(&connection, next(2)).await;
    immediate(&connection, next(3), 3, ErrorCode::LimitExceeded);
    drop(held);
    assert!(matches!(
        control(&connection, next(4)).await,
        Control::Session(Session::Sequence { .. })
    ));
    drop(cloned_job);
    assert!(connection.input().is_ok());
}

#[tokio::test]
async fn dispatcher_watch_and_checkpoint_waits_release_metadata_capacity() {
    let db = Database::new();
    let tls = Fixture::new();
    let (connection, _wire) = db.connect(&tls, caps(), Some(0)).await;
    control(&connection, create(1)).await;
    let digest = seal(control(&connection, declare(2, vec![Id(1)], true)).await);
    let mut watch = tokio::spawn(
        pending(
            &connection,
            Control::Work(Work::Watch {
                request: Id(3),
                work: key(),
                after_revision: Number(1),
                wait_ms: WaitMs(1000),
            }),
        )
        .run(),
    );
    // Let the first snapshot finish; this poll observes the same running handle.
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut watch)
            .await
            .is_err()
    );
    db.access.pause_cancel.store(true, Ordering::SeqCst);
    let _release_on_failure = ReleaseOnDrop(db.access.clone());
    let cancelling = tokio::spawn(
        pending(
            &connection,
            Control::Work(Work::Cancel {
                request: Id(4),
                operation: OperationId([4; 16]),
                work: key(),
            }),
        )
        .run(),
    );
    tokio::time::timeout(HANDSHAKE, db.access.entered.notified())
        .await
        .unwrap();
    let early = tokio::time::timeout(Duration::from_millis(75), &mut watch).await;
    if let Ok(result) = early {
        let mut response = result.unwrap().unwrap();
        match response.body() {
            ResponseBody::Control(control) => {
                panic!("watch ended while cancellation held metadata: {control:?}")
            }
            _ => panic!("unexpected object response"),
        }
    }
    db.access.release();
    let mut cancelled = tokio::time::timeout(HANDSHAKE, cancelling)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let ResponseBody::Control(cancelled) = cancelled.body() else {
        panic!("cancellation response expected")
    };
    assert!(matches!(cancelled, Control::Work(Work::Cancelled { .. })));
    let mut watched = tokio::time::timeout(HANDSHAKE, watch)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        matches!(watched.body(), ResponseBody::Control(Control::Work(Work::View { revision, work, .. })) if revision.0 > 1 && work.state == State::CANCELLED)
    );
    drop(watched);
    // A closed work item need not yet have a materialized scope summary.
    refused(
        &control(
            &connection,
            Control::Scope(Scope::Checkpoint {
                request: Id(5),
                scope: Number(0),
                seal: digest,
                wait_ms: WaitMs(0),
            }),
        )
        .await,
        5,
        ErrorCode::WaitTimeout,
    );
    let mut cursor = store::ReconcileCursor::default();
    for _ in 0..4 {
        db.store.reconcile(&mut cursor, 8).unwrap();
    }
    let summary = root(&connection, 6, digest).await;
    assert_eq!(summary.counts.cancelled, Number(1));
    let snapshot = control(
        &connection,
        Control::Work(Work::Watch {
            request: Id(7),
            work: key(),
            after_revision: Number(0),
            wait_ms: WaitMs(30000),
        }),
    )
    .await;
    let Control::Work(Work::View { revision, .. }) = snapshot else {
        panic!("view expected")
    };
    assert!(
        matches!(control(&connection, Control::Work(Work::Watch { request: Id(8), work: key(), after_revision: Number(revision.0), wait_ms: WaitMs(30) })).await,
        Control::Work(Work::View { revision: actual, .. }) if actual == revision)
    );
    refused(
        &control(
            &connection,
            Control::Work(Work::Watch {
                request: Id(9),
                work: key(),
                after_revision: Number(revision.0 + 1),
                wait_ms: WaitMs(0),
            }),
        )
        .await,
        9,
        ErrorCode::Conflict,
    );
}

#[tokio::test]
async fn dispatcher_complete_checks_exact_root_and_all_connection_work() {
    let db = Database::new();
    let tls = Fixture::new();
    let (connection, _wire) = db.connect(&tls, caps(), Some(0)).await;
    control(&connection, create(1)).await;
    let digest = seal(control(&connection, declare(2, vec![], true)).await);
    let summary = root(&connection, 3, digest).await;
    let complete = |request, summary| {
        Control::Drain(Drain::Complete {
            request: Id(request),
            generation: Id(1),
            root_summary: summary,
        })
    };
    let input = connection.input().unwrap();
    immediate(
        &connection,
        complete(4, summary.clone()),
        4,
        ErrorCode::NotReady,
    );
    drop(input);
    let mut changed = summary.clone();
    changed.closed_at.0 += 1;
    refused(
        &control(&connection, complete(5, changed)).await,
        5,
        ErrorCode::Conflict,
    );
    let mut child = summary.clone();
    child.scope = Number(1);
    child.parent = Some(key());
    // A structurally valid child cut is still not a completed-session cut.
    assert!(matches!(
        db.store.complete_session(
            &connection.input().unwrap().binding().identity,
            Id(1),
            &child
        ),
        Err(store::StoreError::Protocol(Error {
            code: ErrorCode::Conflict,
            ..
        }))
    ));
    let completed = response(&connection, complete(6, summary.clone())).await;
    immediate(&connection, next(7), 7, ErrorCode::NotReady);
    assert!(matches!(
        connection.input(),
        Err(Error {
            code: ErrorCode::NotReady,
            ..
        })
    ));
    let mut completed = completed;
    assert!(
        matches!(completed.body(), ResponseBody::Control(Control::Drain(Drain::Completed { root_summary, .. })) if *root_summary == summary)
    );
    drop(completed);
    // The acknowledgment neither retires nor changes the root.
    assert_eq!(root(&connection, 8, digest).await, summary);
}

#[tokio::test]
async fn dispatcher_detach_waits_for_existing_work_and_has_an_exclusive_deadline() {
    let db = Database::new();
    let tls = Fixture::new();
    let (connection, _wire) = db.connect(&tls, caps(), Some(0)).await;
    control(&connection, create(1)).await;
    let input = connection.input().unwrap();
    let mut draining = tokio::spawn(
        pending(
            &connection,
            Control::Drain(Drain::Detach { request: Id(2) }),
        )
        .run(),
    );
    immediate(&connection, next(3), 3, ErrorCode::NotReady);
    immediate(
        &connection,
        Control::Drain(Drain::Detach { request: Id(4) }),
        4,
        ErrorCode::NotReady,
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut draining)
            .await
            .is_err()
    );
    drop(input);
    assert!(matches!(
        tokio::time::timeout(HANDSHAKE, draining)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .body(),
        ResponseBody::Control(Control::Drain(Drain::Detached { request: Id(2) }))
    ));
    let mut short = caps();
    short.stream_idle_ms = IdleMs(1000);
    short.stream_lifetime_ms = LifetimeMs(1000);
    let (second, _second_wire) = db.connect(&tls, short, Some(0)).await;
    let held = response(&second, next(1)).await;
    assert!(matches!(
        pending(&second, Control::Drain(Drain::Detach { request: Id(2) }))
            .run()
            .await,
        Err(Error {
            code: ErrorCode::LimitExceeded,
            ..
        })
    ));
    drop(held);
}

#[tokio::test]
async fn dispatcher_cancelled_waiter_cannot_release_a_still_running_commit() {
    let db = Database::new();
    let tls = Fixture::new();
    let (connection, _wire) = db.connect(&tls, caps(), Some(0)).await;
    let (other, _other_wire) = db.connect(&tls, caps(), Some(1)).await;
    db.access.pause.store(true, Ordering::SeqCst);
    let _release_on_failure = ReleaseOnDrop(db.access.clone());
    let creating = tokio::spawn(pending(&connection, create(1)).run());
    tokio::time::timeout(HANDSHAKE, db.access.entered.notified())
        .await
        .unwrap();
    creating.abort();
    assert!(matches!(creating.await, Err(error) if error.is_cancelled()));
    refused(&control(&other, next(1)).await, 1, ErrorCode::LimitExceeded);
    let mut draining = tokio::spawn(
        pending(
            &connection,
            Control::Drain(Drain::Detach { request: Id(2) }),
        )
        .run(),
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(40), &mut draining)
            .await
            .is_err()
    );
    db.access.release();
    let mut detached = tokio::time::timeout(HANDSHAKE, draining)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(
        detached.body(),
        ResponseBody::Control(Control::Drain(Drain::Detached { .. }))
    ));
    assert_eq!(
        control(&other, next(2)).await,
        Control::Session(Session::Sequence {
            request: Id(2),
            next_creation_sequence: Id(2)
        })
    );
    assert!(matches!(
        control(&other, create(3)).await,
        Control::Session(Session::Binding {
            generation: Id(1),
            ..
        })
    ));
}

#[tokio::test]
async fn dispatcher_rechecks_credentials_and_retained_authorization_before_lookup() {
    let db = Database::new();
    let tls = Fixture::new();
    let (connection, _wire) = db.connect(&tls, caps(), Some(0)).await;
    control(&connection, create(1)).await;
    db.access.allowed.store(false, Ordering::SeqCst);
    refused(
        &control(
            &connection,
            Control::Work(Work::Operation {
                request: Id(2),
                operation: OperationId([99; 16]),
            }),
        )
        .await,
        2,
        ErrorCode::Unauthorized,
    );
    db.access.allowed.store(true, Ordering::SeqCst);
    let queued = pending(&connection, next(3));
    tls.clock.0.store(at(2032, 1, 1), Ordering::SeqCst);
    let mut response = queued.run().await.unwrap();
    let ResponseBody::Control(control) = response.body() else {
        panic!("refusal expected")
    };
    refused(control, 3, ErrorCode::Unauthorized);
    immediate(&connection, next(4), 4, ErrorCode::Unauthorized);
}

mod results;

#[tokio::test]
async fn dispatcher_scope_cancellation_and_skip_are_real_fences_and_replay_receipts() {
    let db = Database::new();
    let tls = Fixture::new();
    let (connection, _wire) = db.connect(&tls, caps(), Some(0)).await;
    control(&connection, create(1)).await;
    control(&connection, declare(2, vec![Id(1), Id(2)], false)).await;
    let skipped = control(
        &connection,
        Control::Work(Work::Skip {
            request: Id(3),
            operation: OperationId([3; 16]),
            work: key(),
        }),
    )
    .await;
    assert!(matches!(skipped, Control::Work(Work::Skipped { .. })));
    let cancel = |request| {
        Control::Scope(Scope::Cancel {
            request: Id(request),
            operation: OperationId([4; 16]),
            scope: Number(0),
        })
    };
    let Control::Scope(Scope::Cancelled { receipt, .. }) = control(&connection, cancel(4)).await
    else {
        panic!("scope cancellation expected")
    };
    assert!(
        matches!(control(&connection, cancel(5)).await, Control::Scope(Scope::Cancelled { receipt: found, .. }) if found == receipt)
    );
    let mut cursor = store::ReconcileCursor::default();
    for _ in 0..8 {
        db.store.reconcile(&mut cursor, 8).unwrap();
    }
    let Control::Scope(Scope::PageResponse {
        seal: Some(seal), ..
    }) = control(
        &connection,
        Control::Scope(Scope::Page {
            request: Id(6),
            scope: Number(0),
            after_entity: Number(0),
            limit: PageLimit(2),
        }),
    )
    .await
    else {
        panic!("sealed page expected")
    };
    let summary = root(&connection, 7, seal).await;
    assert_eq!(summary.counts.skipped, Number(1));
    assert_eq!(summary.counts.cancelled, Number(1));
}

#[tokio::test]
async fn dispatcher_configuration_requires_mapped_identity_and_matching_authority() {
    let db = Database::new();
    let tls = Fixture::new();
    for jobs in [0, 65] {
        assert!(matches!(
            Authority::new(db.store.clone(), db.payloads.clone(), jobs),
            Err(Error {
                code: ErrorCode::LimitExceeded,
                ..
            })
        ));
    }
    for principal in [None, Some(2)] {
        let mut exchange = tls.connect(tls.config(principal), "localhost").await;
        assert!(matches!(
            db.adapter(&tls, &mut exchange, caps()),
            Err(Error {
                code: ErrorCode::Unauthorized,
                ..
            })
        ));
    }
    let wrong = Database::with_authority("other-authority");
    let mut exchange = tls.connect(tls.config(Some(0)), "localhost").await;
    assert!(matches!(
        wrong.adapter(&tls, &mut exchange, caps()),
        Err(Error {
            code: ErrorCode::Unauthorized,
            ..
        })
    ));
    let mut core = caps();
    core.supported.clear();
    core.required.clear();
    let mut exchange = tls.connect(tls.config(Some(0)), "localhost").await;
    assert!(matches!(
        db.adapter(&tls, &mut exchange, core),
        Err(Error {
            code: ErrorCode::ExtensionUnsupported,
            ..
        })
    ));
}

#[tokio::test]
async fn dispatcher_profile_combination_cannot_change_on_attachment() {
    let db = Database::new();
    let tls = Fixture::new();
    let mut work_only = caps();
    work_only
        .supported
        .retain(|id| id.0 == u64::from(DURABLE_WORK));
    let (connection, _wire) = db.connect(&tls, work_only, Some(0)).await;
    control(&connection, create(1)).await;
    immediate(
        &connection,
        Control::Result(ResultMessage::GetManifest {
            request: Id(2),
            work: key(),
            attempt: Id(1),
        }),
        2,
        ErrorCode::ExtensionUnsupported,
    );
    let (resumed, _resumed_wire) = db.connect(&tls, caps(), Some(1)).await;
    refused(
        &control(&resumed, attach(1, "alice", 1)).await,
        1,
        ErrorCode::ExtensionUnsupported,
    );
    let identity = connection.input().unwrap().binding().identity.clone();
    db.store.revoke_session(&identity).unwrap();
    refused(
        &control(
            &connection,
            Control::Scope(Scope::Page {
                request: Id(3),
                scope: Number(0),
                after_entity: Number(0),
                limit: PageLimit(1),
            }),
        )
        .await,
        3,
        ErrorCode::Unauthorized,
    );
}
