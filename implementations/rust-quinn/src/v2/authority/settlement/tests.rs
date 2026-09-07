use super::*;
use crate::v2::authority::{execution::*, ingress::*, payload::*};
use sha2::{Digest as _, Sha256};
use std::{
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::Instant,
};

struct TestClock {
    now: AtomicU64,
    trusted: AtomicBool,
}
impl TestClock {
    fn new(now: u64) -> Self {
        Self {
            now: AtomicU64::new(now),
            trusted: AtomicBool::new(true),
        }
    }
    fn set(&self, now: u64) {
        self.now.store(now, Ordering::SeqCst);
    }
}
impl Clock for TestClock {
    fn read(&self) -> ClockReading {
        ClockReading {
            utc_ms: Number(self.now.load(Ordering::SeqCst)),
            trusted: self.trusted.load(Ordering::SeqCst),
        }
    }
}
struct Auth {
    caller: AtomicBool,
    skip: AtomicBool,
    revoke: AtomicBool,
}
impl Auth {
    fn new() -> Self {
        Self {
            caller: AtomicBool::new(true),
            skip: AtomicBool::new(true),
            revoke: AtomicBool::new(true),
        }
    }
}
impl Authorization for Auth {
    fn permits(&self, owner: &IdentityLabel, permission: Permission) -> bool {
        owner.0 == "alice"
            && match permission {
                Permission::Revoke => self.revoke.load(Ordering::SeqCst),
                Permission::Skip => {
                    self.caller.load(Ordering::SeqCst) && self.skip.load(Ordering::SeqCst)
                }
                _ => self.caller.load(Ordering::SeqCst),
            }
    }
}
fn op(value: u64) -> OperationId {
    let mut bytes = [0; 16];
    bytes[..8].copy_from_slice(&value.to_be_bytes());
    OperationId(bytes)
}
fn key(scope: u64, entity: u64) -> WorkKey {
    WorkKey {
        scope: Number(scope),
        producer: Producer(0),
        entity: Id(entity),
    }
}
fn caps() -> Capabilities {
    Capabilities {
        response: ResponseFlag(1),
        supported: vec![
            ProfileId(DURABLE_WORK.into()),
            ProfileId(RESULT_DELIVERY.into()),
        ],
        required: vec![],
        control_limit: ControlLimit(8192),
        object_limit: Number(1 << 20),
        stream_limit: ConcurrencyLimit(8),
        pending_limit: ConcurrencyLimit(8),
        stream_idle_ms: IdleMs(1000),
        stream_lifetime_ms: LifetimeMs(10000),
    }
}
fn payload_policy() -> PayloadPolicy {
    PayloadPolicy {
        objects: Id(1024),
        bytes: Number(16 << 20),
        owner_objects: Id(1024),
        owner_bytes: Number(16 << 20),
        chunk_bytes: Id(16384),
        handles: Id(8),
        owner_handles: Id(8),
    }
}
fn apps(application: Arc<dyn Application>) -> Arc<Applications> {
    let mut apps = Applications::default();
    apps.register(
        ApplicationLabel("test/v1".into()),
        vec![Mode(0), Mode(1), Mode(2)],
        RestartSafety::Pure,
        application,
    )
    .unwrap();
    Arc::new(apps)
}
struct Fixture {
    directory: tempfile::TempDir,
    store: AuthorityStore,
    binding: Binding,
    payloads: PayloadStore,
    applications: Arc<Applications>,
    executor: Executor,
    clock: Arc<TestClock>,
    auth: Arc<Auth>,
    next_op: AtomicU64,
}
impl Fixture {
    fn new() -> Self {
        Self::configured(Arc::new(CopyApplication), PhysicalLimits::default())
    }
    fn configured(application: Arc<dyn Application>, physical: PhysicalLimits) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let clock = Arc::new(TestClock::new(1000));
        let auth = Arc::new(Auth::new());
        let store = AuthorityStore::initialize(
            &directory.path().join("authority.sqlite"),
            IdentityLabel("test-authority".into()),
            super::super::tests::policy(),
            physical,
            clock.clone(),
            auth.clone(),
        )
        .unwrap();
        let binding = store
            .create_session(
                &IdentityLabel("alice".into()),
                Id(1),
                &Policy {
                    execution_limit_ms: Duration(10000),
                    output_retention_ms: Duration(20000),
                    receipt_retention_ms: Duration(30000),
                },
                &caps(),
            )
            .unwrap();
        let payloads = PayloadStore::initialize(
            &directory.path().join("objects"),
            store.payload_identity().unwrap(),
            payload_policy(),
        )
        .unwrap();
        store.bind_payloads(&payloads).unwrap();
        let applications = apps(application);
        let executor = Executor::new(
            store.clone(),
            payloads.clone(),
            applications.clone(),
            ResultEndpoint::new("results.example:7443".into()).unwrap(),
            caps(),
            Duration(100),
        )
        .unwrap();
        Self {
            directory,
            store,
            binding,
            payloads,
            applications,
            executor,
            clock,
            auth,
            next_op: AtomicU64::new(1),
        }
    }
    fn operation(&self) -> OperationId {
        op(self.next_op.fetch_add(1, Ordering::SeqCst))
    }
    fn declare(&self, scope: u64, entities: &[u64], seal: bool) {
        self.store
            .declare(
                &self.binding.identity,
                self.operation(),
                Number(scope),
                &entities.iter().copied().map(Id).collect::<Vec<_>>(),
                seal,
            )
            .unwrap();
    }
    fn admit(&self, work: WorkKey, mode: u64, execution_ms: u64) -> WorkView {
        let prepared = self.prepare(work.clone(), mode, execution_ms);
        self.store
            .admit_input(prepared, &caps(), &self.applications)
            .unwrap();
        self.view(&work)
    }
    fn prepare(&self, work: WorkKey, mode: u64, execution_ms: u64) -> PreparedInput {
        let header = InputHeader {
            kind: Literal,
            generation: self.binding.identity.generation,
            operation: self.operation(),
            parameters: AdmitParameters {
                work: work.clone(),
                input: Input {
                    length: Number(3),
                    sha256: Digest(Sha256::digest(b"abc").into()),
                    content_type: ApplicationLabel("text/plain".into()),
                },
                application: ApplicationLabel("test/v1".into()),
                mode: Mode(mode),
                execution_ms: Duration(execution_ms),
                outputs: OutputBudget {
                    count: BatchCount(1),
                    total_bytes: Number(3),
                },
            },
        };
        let now = Instant::now();
        let InputReception::Receiving(mut receiving) = self
            .store
            .receive_input(
                &self.binding.identity,
                &header,
                &caps(),
                &self.payloads,
                &self.applications,
                now,
            )
            .unwrap()
        else {
            panic!("unexpected replay")
        };
        receiving.receive(b"abc", now).unwrap();
        let InputPreparation::Ready(prepared) = self
            .store
            .prepare_input(receiving.finish(now).unwrap(), &caps(), &self.applications)
            .unwrap()
        else {
            panic!("unexpected replay")
        };
        *prepared
    }
    fn view(&self, work: &WorkKey) -> WorkView {
        self.store
            .work_view(&self.binding.identity, work, Number(0))
            .unwrap()
            .1
    }
    fn raw_view(&self, work: &WorkKey) -> WorkView {
        let mut connection = self.store.connect().unwrap();
        let tx = connection.transaction().unwrap();
        scopes::work(&tx, self.binding.identity.generation, work)
            .unwrap()
            .1
    }
    fn job(&self, work: &WorkKey) -> jobs::JobRecord {
        let mut connection = self.store.connect().unwrap();
        let tx = connection.transaction().unwrap();
        let (row, _, _) = find_work(&tx, &self.binding.identity, work).unwrap();
        records::read(&tx, target(records::Table::Job, row))
            .unwrap()
            .1
    }
    fn summary(&self, scope: u64) -> Option<ScopeSummary> {
        let mut connection = self.store.connect().unwrap();
        let tx = connection.transaction().unwrap();
        scopes::closed(&tx, self.binding.identity.generation, Number(scope)).unwrap()
    }
    fn close(&self, limit: usize) -> ScopeSummary {
        let mut cursor = ReconcileCursor::default();
        for _ in 0..5000 {
            if let Some(summary) = self.summary(0) {
                return summary;
            }
            let progress = self.store.reconcile(&mut cursor, limit).unwrap();
            assert!(progress.inspected_work <= limit && progress.inspected_members <= limit);
        }
        panic!("root did not close");
    }
    fn reopen(&self) -> Result<AuthorityStore> {
        AuthorityStore::open(
            &self.directory.path().join("authority.sqlite"),
            self.store.authority.clone(),
            self.store.policy.clone(),
            self.store.physical.limits,
            self.clock.clone(),
            self.auth.clone(),
        )
    }
    fn run(&self, work: &WorkKey) -> Result<WorkView> {
        self.executor.run(&self.binding.identity, work)
    }
}
fn refuse<T>(result: Result<T>, code: ErrorCode) {
    match result {
        Err(StoreError::Protocol(error)) => assert_eq!(error.code, code),
        Err(error) => panic!("expected {code:?}, got {error:?}"),
        Ok(_) => panic!("expected {code:?}"),
    }
}

#[test]
fn declared_cancel_skip_and_terminal_receipts_replay_without_changing_outcomes() {
    let fixture = Fixture::new();
    fixture.declare(0, &[1, 2, 3, 4], true);
    let cancel = fixture
        .store
        .cancel_work(&fixture.binding.identity, op(100), &key(0, 1))
        .unwrap();
    assert!(matches!(
        cancel.body,
        Outcome::Cancelled {
            disposition: Disposition(0),
            state_at_commit: State::CANCELLED,
            ..
        }
    ));
    let view = fixture.view(&key(0, 1));
    assert_eq!(view.attempt, Number(0));
    assert!(view.input.is_none() && view.deadline.is_none() && view.manifest.is_none());
    fixture.clock.set(1001);
    assert_eq!(
        fixture
            .store
            .cancel_work(&fixture.binding.identity, op(100), &key(0, 1))
            .unwrap(),
        cancel
    );
    refuse(
        fixture
            .store
            .skip_work(&fixture.binding.identity, op(100), &key(0, 1)),
        ErrorCode::Conflict,
    );
    let no_op = fixture
        .store
        .skip_work(&fixture.binding.identity, op(101), &key(0, 1))
        .unwrap();
    assert!(matches!(
        no_op.body,
        Outcome::Skipped {
            disposition: Disposition(1),
            state_at_commit: State::CANCELLED,
            ..
        }
    ));
    assert_eq!(fixture.view(&key(0, 1)), view);
    fixture.auth.skip.store(false, Ordering::SeqCst);
    refuse(
        fixture
            .store
            .skip_work(&fixture.binding.identity, op(102), &key(0, 2)),
        ErrorCode::Unauthorized,
    );
    refuse(
        fixture.store.operation(&fixture.binding.identity, op(102)),
        ErrorCode::NotFound,
    );
    fixture.auth.skip.store(true, Ordering::SeqCst);
    fixture
        .store
        .skip_work(&fixture.binding.identity, op(102), &key(0, 2))
        .unwrap();
    fixture.admit(key(0, 3), 0, 1000);
    let succeeded = fixture.run(&key(0, 3)).unwrap();
    let no_op = fixture
        .store
        .cancel_work(&fixture.binding.identity, op(103), &key(0, 3))
        .unwrap();
    assert!(matches!(
        no_op.body,
        Outcome::Cancelled {
            disposition: Disposition(1),
            state_at_commit: State::SUCCEEDED,
            ..
        }
    ));
    assert_eq!(fixture.view(&key(0, 3)), succeeded);
    fixture.admit(key(0, 4), 0, 1000);
    fixture.clock.set(2001);
    let summary = fixture.close(1);
    assert_eq!(
        summary.counts,
        Counts {
            success: Number(1),
            failure: Number(1),
            cancelled: Number(1),
            skipped: Number(1)
        }
    );
    let mut status = StatusRoot::default();
    for (entity, state, attempt, manifest_digest) in [
        (1, State::CANCELLED, 0, None),
        (2, State::SKIPPED, 0, None),
        (
            3,
            State::SUCCEEDED,
            1,
            Some(succeeded.manifest.unwrap().digest().unwrap()),
        ),
        (4, State::FAILED, 1, None),
    ] {
        status
            .push(
                StatusLeaf {
                    work: key(0, entity),
                    state,
                    attempt: Number(attempt),
                    manifest_digest,
                    child_status_root: None,
                }
                .digest()
                .unwrap(),
            )
            .unwrap();
    }
    assert_eq!(summary.status_root, status.finish());
    fixture.reopen().unwrap().integrity_check().unwrap();
}

#[test]
fn nested_first_skip_fence_survives_parent_cancel_deadline_and_restart() {
    let fixture = Fixture::new();
    fixture.declare(0, &[1], true);
    fixture.admit(key(0, 1), 1, 1000);
    fixture.declare(1, &[1, 2], false);
    fixture.admit(key(1, 1), 1, 1000);
    fixture.declare(2, &[1, 2], false);
    let skip = fixture
        .store
        .skip_work(&fixture.binding.identity, op(100), &key(1, 1))
        .unwrap();
    assert!(matches!(
        skip.body,
        Outcome::Skipped {
            state_at_commit: State::CANCELLING,
            ..
        }
    ));
    refuse(
        fixture
            .store
            .cancel_work(&fixture.binding.identity, op(101), &key(1, 1)),
        ErrorCode::Cancelled,
    );
    fixture
        .store
        .cancel_work(&fixture.binding.identity, op(101), &key(0, 1))
        .unwrap();
    assert_eq!(fixture.view(&key(0, 1)).state, State::CANCELLING);
    refuse(
        fixture.store.declare(
            &fixture.binding.identity,
            op(102),
            Number(2),
            &[Id(3)],
            false,
        ),
        ErrorCode::Cancelled,
    );
    fixture.clock.set(2000);
    let reopened = fixture.reopen().unwrap();
    let mut cursor = ReconcileCursor::default();
    for _ in 0..100 {
        let progress = reopened.reconcile(&mut cursor, 1).unwrap();
        assert!(progress.inspected_work <= 1 && progress.inspected_members <= 1);
        if fixture.summary(0).is_some() {
            break;
        }
    }
    assert_eq!(fixture.view(&key(0, 1)).state, State::CANCELLED);
    assert_eq!(fixture.view(&key(1, 1)).state, State::SKIPPED);
    for work in [key(1, 2), key(2, 1), key(2, 2)] {
        assert_eq!(fixture.view(&work).state, State::CANCELLED);
    }
    assert_eq!(fixture.summary(1).unwrap().counts.skipped, Number(1));
    assert_eq!(fixture.summary(2).unwrap().counts.cancelled, Number(2));
    assert_eq!(fixture.summary(0).unwrap().counts.cancelled, Number(1));
    let root_leaf = StatusLeaf {
        work: key(0, 1),
        state: State::CANCELLED,
        attempt: Number(1),
        manifest_digest: None,
        child_status_root: Some(fixture.summary(1).unwrap().status_root),
    }
    .digest()
    .unwrap();
    assert_eq!(fixture.summary(0).unwrap().status_root, root_leaf);
    assert_eq!(
        reopened
            .skip_work(&fixture.binding.identity, op(100), &key(1, 1))
            .unwrap(),
        skip
    );
    reopened.integrity_check().unwrap();
}

struct Retryable;
impl Application for Retryable {
    fn execute(&self, _: &mut WorkContext) -> Result<ApplicationOutcome> {
        Ok(ApplicationOutcome::Retryable(Diagnostic {
            code: DiagnosticCode(0),
            detail: Detail("retryable application failure".into()),
        }))
    }
}
#[test]
fn deadline_settles_active_and_awaiting_retry_without_extending_attempt_or_retention() {
    for retryable in [false, true] {
        let fixture = Fixture::configured(
            if retryable {
                Arc::new(Retryable)
            } else {
                Arc::new(CopyApplication)
            },
            PhysicalLimits::default(),
        );
        fixture.declare(0, &[1], true);
        let admitted = fixture.admit(key(0, 1), 0, 1000);
        if retryable {
            assert_eq!(
                fixture.run(&key(0, 1)).unwrap().state,
                State::AWAITING_RETRY
            );
        }
        fixture.clock.set(2000);
        let summary = fixture.close(1);
        let view = fixture.view(&key(0, 1));
        assert_eq!(view.state, State::FAILED);
        assert_eq!(
            (view.attempt, view.admitted_at, view.deadline),
            (admitted.attempt, admitted.admitted_at, admitted.deadline)
        );
        assert_eq!(
            view.diagnostic.unwrap().code,
            DiagnosticCode(ErrorCode::DeadlineExceeded as u64)
        );
        assert_eq!(view.receipt_until, Some(Number(32000)));
        assert_eq!(summary.counts.failure, Number(1));
        refuse(fixture.run(&key(0, 1)), ErrorCode::AlreadyTerminal);
        let job = fixture.job(&key(0, 1));
        assert!(!job.executor_live && job.input_live && job.outputs_live);
        fixture.reopen().unwrap().integrity_check().unwrap();
    }
}

#[test]
fn strict_child_failure_settles_parent_but_parent_deadline_does_not_cancel_children() {
    for parent_expired in [false, true] {
        let fixture = Fixture::new();
        fixture.declare(0, &[1], true);
        fixture.admit(key(0, 1), 1, 1000);
        fixture.declare(1, &[1], true);
        if parent_expired {
            fixture.clock.set(2000);
            fixture
                .store
                .reconcile(&mut ReconcileCursor::default(), 256)
                .unwrap();
            assert_eq!(fixture.view(&key(0, 1)).state, State::FAILED);
            assert_eq!(fixture.view(&key(1, 1)).state, State::DECLARED);
            assert!(fixture.summary(0).is_none());
        }
        fixture
            .store
            .skip_work(&fixture.binding.identity, op(100), &key(1, 1))
            .unwrap();
        let summary = fixture.close(1);
        assert_eq!(summary.counts.failure, Number(1));
        assert_eq!(fixture.summary(1).unwrap().counts.skipped, Number(1));
        let view = fixture.view(&key(0, 1));
        assert_eq!(view.state, State::FAILED);
        if parent_expired {
            assert_eq!(
                view.diagnostic.unwrap().code,
                DiagnosticCode(ErrorCode::DeadlineExceeded as u64)
            );
        } else {
            assert_eq!(
                view.diagnostic.unwrap().detail.0,
                "STRICT child scope contains nonsuccessful work"
            );
        }
        fixture.reopen().unwrap().integrity_check().unwrap();
    }
}

#[test]
fn revocation_uses_operator_permission_and_settles_without_caller_authorization() {
    let fixture = Fixture::new();
    fixture.declare(0, &[1, 2], false);
    fixture.admit(key(0, 1), 1, 1000);
    fixture.declare(1, &[1], false);
    let held = fixture
        .executor
        .claim(&fixture.binding.identity, &key(0, 2));
    refuse(held, ErrorCode::NotReady);
    fixture.auth.revoke.store(false, Ordering::SeqCst);
    refuse(
        fixture.store.revoke_session(&fixture.binding.identity),
        ErrorCode::Unauthorized,
    );
    fixture.auth.revoke.store(true, Ordering::SeqCst);
    fixture.auth.caller.store(false, Ordering::SeqCst);
    fixture
        .store
        .revoke_session(&fixture.binding.identity)
        .unwrap();
    fixture.auth.caller.store(true, Ordering::SeqCst);
    refuse(
        fixture.store.operation(&fixture.binding.identity, op(1)),
        ErrorCode::Unauthorized,
    );
    refuse(
        fixture
            .store
            .work_view(&fixture.binding.identity, &key(0, 1), Number(0)),
        ErrorCode::Unauthorized,
    );
    refuse(
        fixture.store.declare(
            &fixture.binding.identity,
            op(100),
            Number(0),
            &[Id(3)],
            true,
        ),
        ErrorCode::Unauthorized,
    );
    fixture.auth.caller.store(false, Ordering::SeqCst);
    let summary = fixture.close(1);
    assert_eq!(summary.counts.cancelled, Number(2));
    assert_eq!(fixture.raw_view(&key(1, 1)).state, State::CANCELLED);
    fixture
        .store
        .revoke_session(&fixture.binding.identity)
        .unwrap();
    fixture.reopen().unwrap().integrity_check().unwrap();
}

#[test]
fn unsafe_clock_rejects_new_fences_and_settlement_but_not_accepted_receipt_replay() {
    let fixture = Fixture::new();
    fixture.declare(0, &[1, 2], true);
    let receipt = fixture
        .store
        .cancel_work(&fixture.binding.identity, op(100), &key(0, 1))
        .unwrap();
    for untrusted in [false, true] {
        fixture.clock.set(if untrusted { 1000 } else { 999 });
        fixture.clock.trusted.store(!untrusted, Ordering::SeqCst);
        refuse(
            fixture
                .store
                .cancel_work(&fixture.binding.identity, op(101), &key(0, 2)),
            ErrorCode::ClockUnsafe,
        );
        refuse(
            fixture
                .store
                .cancel_scope(&fixture.binding.identity, op(101), Number(0)),
            ErrorCode::ClockUnsafe,
        );
        refuse(
            fixture
                .store
                .reconcile(&mut ReconcileCursor::default(), 256),
            ErrorCode::ClockUnsafe,
        );
        assert_eq!(
            fixture
                .store
                .cancel_work(&fixture.binding.identity, op(100), &key(0, 1))
                .unwrap(),
            receipt
        );
        assert_eq!(fixture.view(&key(0, 2)).state, State::DECLARED);
        refuse(
            fixture.store.operation(&fixture.binding.identity, op(101)),
            ErrorCode::NotFound,
        );
    }
    fixture.clock.set(1000);
    fixture.clock.trusted.store(true, Ordering::SeqCst);
    fixture
        .store
        .cancel_scope(&fixture.binding.identity, op(101), Number(0))
        .unwrap();
    fixture.close(1);
    fixture.reopen().unwrap();
}

#[test]
fn scope_freeze_is_immediate_but_large_seal_and_status_fold_are_bounded_and_restartable() {
    let fixture = Fixture::configured(
        Arc::new(CopyApplication),
        PhysicalLimits {
            wal_bytes: 256 << 20,
            ..PhysicalLimits::default()
        },
    );
    for batch in 0..3 {
        fixture.declare(
            0,
            &(batch * 200 + 1..=(batch + 1) * 200).collect::<Vec<_>>(),
            false,
        );
    }
    let receipt = fixture
        .store
        .cancel_scope(&fixture.binding.identity, op(100), Number(0))
        .unwrap();
    refuse(
        fixture.store.declare(
            &fixture.binding.identity,
            op(101),
            Number(0),
            &[Id(601)],
            true,
        ),
        ErrorCode::Cancelled,
    );
    let mut cursor = ReconcileCursor::default();
    let progress = fixture.store.reconcile(&mut cursor, 73).unwrap();
    assert_eq!(progress.inspected_work, 73);
    assert_eq!(progress.inspected_members, 73);
    assert_eq!(progress.sealed_scopes, 0);
    assert!(matches!(
        fixture
            .store
            .scope_page(
                &fixture.binding.identity,
                Id(1),
                Number(0),
                Number(0),
                PageLimit(1)
            )
            .unwrap(),
        Control::Scope(Scope::PageResponse {
            sealed: false,
            seal: None,
            declared: Number(600),
            more: true,
            ..
        })
    ));
    let reopened = fixture.reopen().unwrap();
    let mut cursor = ReconcileCursor::default();
    for _ in 0..100 {
        let progress = reopened.reconcile(&mut cursor, 127).unwrap();
        assert!(progress.inspected_work <= 127 && progress.inspected_members <= 127);
        if fixture.summary(0).is_some() {
            break;
        }
    }
    let summary = fixture.summary(0).unwrap();
    assert_eq!(summary.declared, Number(600));
    assert_eq!(summary.counts.cancelled, Number(600));
    let mut seal = ScopeSeal::new(
        &fixture.binding.identity,
        Number(0),
        Producer(0),
        None,
        Number(600),
    )
    .unwrap();
    let mut root = StatusRoot::default();
    for entity in 1..=600 {
        seal.push(Id(entity)).unwrap();
        root.push(
            StatusLeaf {
                work: key(0, entity),
                state: State::CANCELLED,
                attempt: Number(0),
                manifest_digest: None,
                child_status_root: None,
            }
            .digest()
            .unwrap(),
        )
        .unwrap();
    }
    assert_eq!(summary.seal, seal.finish().unwrap());
    assert_eq!(summary.status_root, root.finish());
    assert!(
        matches!(reopened.scope_page(&fixture.binding.identity, Id(2), Number(0), Number(0), PageLimit(1)).unwrap(),
        Control::Scope(Scope::PageResponse { sealed: true, seal: Some(seal), .. }) if seal == summary.seal)
    );
    assert_eq!(
        reopened
            .cancel_scope(&fixture.binding.identity, op(100), Number(0))
            .unwrap(),
        receipt
    );
    fixture.clock.set(1010);
    for _ in 0..20 {
        reopened.reconcile(&mut cursor, 256).unwrap();
    }
    assert_eq!(fixture.summary(0).unwrap(), summary);
    reopened.integrity_check().unwrap();
}

#[test]
fn empty_cancelled_scope_closes_and_cursor_cannot_cross_authority_stores() {
    let fixture = Fixture::new();
    fixture
        .store
        .cancel_scope(&fixture.binding.identity, op(100), Number(0))
        .unwrap();
    let summary = fixture.close(1);
    assert_eq!(summary.declared, Number(0));
    assert_eq!(summary.status_root, StatusRoot::default().finish());
    let mut cursor = ReconcileCursor::default();
    fixture.store.reconcile(&mut cursor, 1).unwrap();
    let other = Fixture::new();
    refuse(other.store.reconcile(&mut cursor, 1), ErrorCode::Conflict);
    refuse(
        other.store.reconcile(&mut ReconcileCursor::default(), 0),
        ErrorCode::LimitExceeded,
    );
    refuse(
        other.store.reconcile(&mut ReconcileCursor::default(), 257),
        ErrorCode::LimitExceeded,
    );
    fixture.reopen().unwrap();
}

struct HeldCopy {
    ready: std::sync::mpsc::SyncSender<()>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}
impl Application for HeldCopy {
    fn execute(&self, context: &mut WorkContext) -> Result<ApplicationOutcome> {
        CopyApplication.execute(context)?;
        self.ready.send(()).unwrap();
        self.release
            .lock()
            .unwrap()
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap();
        Ok(ApplicationOutcome::Succeeded)
    }
}
fn held_copy() -> (
    Arc<HeldCopy>,
    std::sync::mpsc::Receiver<()>,
    std::sync::mpsc::SyncSender<()>,
) {
    let (ready, started) = std::sync::mpsc::sync_channel(1);
    let (release, released) = std::sync::mpsc::sync_channel(1);
    (
        Arc::new(HeldCopy {
            ready,
            release: std::sync::Mutex::new(released),
        }),
        started,
        release,
    )
}

#[test]
fn accepted_fences_beat_inflight_publication_without_refunding_live_payloads() {
    for action in ["cancel", "skip", "scope", "revoke"] {
        let (application, started, release) = held_copy();
        let fixture = Fixture::configured(application, PhysicalLimits::default());
        fixture.declare(0, &[1], true);
        fixture.admit(key(0, 1), 0, 1000);
        let executor = fixture.executor.clone();
        let identity = fixture.binding.identity.clone();
        let worker = std::thread::spawn(move || executor.run(&identity, &key(0, 1)));
        started
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap();
        let usage = fixture.payloads.usage(None).unwrap();
        match action {
            "cancel" => {
                fixture
                    .store
                    .cancel_work(&fixture.binding.identity, op(100), &key(0, 1))
                    .unwrap();
            }
            "skip" => {
                fixture
                    .store
                    .skip_work(&fixture.binding.identity, op(100), &key(0, 1))
                    .unwrap();
            }
            "scope" => {
                fixture
                    .store
                    .cancel_scope(&fixture.binding.identity, op(100), Number(0))
                    .unwrap();
            }
            _ => {
                fixture
                    .store
                    .revoke_session(&fixture.binding.identity)
                    .unwrap();
            }
        }
        fixture.close(1);
        assert_eq!(fixture.payloads.usage(None).unwrap(), usage);
        let job = fixture.job(&key(0, 1));
        assert!(job.input_live && job.outputs_live && !job.executor_live);
        release.send(()).unwrap();
        refuse(
            worker.join().unwrap(),
            match action {
                "scope" => ErrorCode::Cancelled,
                "revoke" => ErrorCode::Unauthorized,
                _ => ErrorCode::AlreadyTerminal,
            },
        );
        let view = fixture.raw_view(&key(0, 1));
        assert_eq!(
            view.state,
            if action == "skip" {
                State::SKIPPED
            } else {
                State::CANCELLED
            }
        );
        assert!(view.manifest.is_none() && view.output_until.is_none());
        fixture.reopen().unwrap().integrity_check().unwrap();
    }
}

#[test]
fn maintenance_settles_expiry_and_closes_scope_while_every_callback_worker_is_held() {
    let (application, started, release) = held_copy();
    let fixture = Fixture::configured(application, PhysicalLimits::default());
    fixture.declare(0, &[1], true);
    fixture.admit(key(0, 1), 0, 1000);
    let pool = fixture
        .executor
        .start_workers(PoolConfig {
            workers: 1,
            workers_per_owner: 1,
            scan_batch: 1,
            idle_poll_ms: 2,
        })
        .unwrap();
    started
        .recv_timeout(std::time::Duration::from_secs(10))
        .unwrap();
    assert_eq!(pool.snapshot().active, 1);
    fixture.clock.set(2000);
    pool.wake();
    let start = Instant::now();
    while fixture.summary(0).is_none() {
        assert!(
            start.elapsed() < std::time::Duration::from_secs(10),
            "maintenance did not close scope: {:?}",
            pool.snapshot()
        );
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    let snapshot = pool.snapshot();
    assert_eq!(snapshot.active, 1);
    assert_eq!(fixture.view(&key(0, 1)).state, State::FAILED);
    assert!(!snapshot.faulted);
    release.send(()).unwrap();
    let snapshot = pool.shutdown().unwrap();
    assert_eq!(snapshot.settled, 1);
    assert_eq!(snapshot.closed_scopes, 1);
    assert_eq!(snapshot.completed, 0);
    fixture.reopen().unwrap();
}

#[test]
fn settlement_crash_child() {
    let Some(path) = std::env::var_os("PIPESTREAM_SETTLEMENT_DIRECTORY") else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    let action = std::env::var("PIPESTREAM_SETTLEMENT_ACTION").unwrap();
    let store = AuthorityStore::open(
        &path.join("authority.sqlite"),
        IdentityLabel("test-authority".into()),
        super::super::tests::policy(),
        PhysicalLimits::default(),
        Arc::new(TestClock::new(2000)),
        Arc::new(Auth::new()),
    )
    .unwrap();
    let identity = SessionIdentity {
        authority: IdentityLabel("test-authority".into()),
        owner: IdentityLabel("alice".into()),
        generation: Id(1),
    };
    match action.as_str() {
        "work-fence" => {
            store.skip_work(&identity, op(100), &key(0, 1)).unwrap();
        }
        "scope-fence" => {
            store.cancel_scope(&identity, op(100), Number(0)).unwrap();
        }
        "session-revoke" => {
            store.revoke_session(&identity).unwrap();
        }
        _ => {
            store
                .reconcile(&mut ReconcileCursor::default(), 256)
                .unwrap();
        }
    }
    panic!("settlement crash boundary did not fire");
}

#[test]
fn actual_process_death_preserves_atomic_fences_settlement_seals_and_closure() {
    for action in [
        "work-fence",
        "scope-fence",
        "session-revoke",
        "settlement-work",
        "seal",
        "closure",
    ] {
        for phase in ["before", "after"] {
            let fixture = Fixture::new();
            fixture.declare(0, &[1], false);
            fixture.admit(key(0, 1), 0, 1000);
            if matches!(action, "seal" | "closure") {
                fixture
                    .store
                    .cancel_scope(&fixture.binding.identity, op(100), Number(0))
                    .unwrap();
                if action == "closure" {
                    fixture
                        .store
                        .reconcile(&mut ReconcileCursor::default(), 256)
                        .unwrap();
                    assert!(fixture.summary(0).is_none());
                }
            }
            let boundary = if matches!(action, "seal" | "closure") {
                "settlement-scope"
            } else {
                action
            };
            // Simulate complete owner shutdown before recovery. A second live
            // PayloadStore would correctly refuse the exclusively owned root.
            let Fixture {
                directory,
                store,
                binding,
                payloads,
                applications,
                executor,
                clock,
                auth,
                next_op,
            } = fixture;
            drop(executor);
            drop(payloads);
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "v2::authority::settlement::tests::settlement_crash_child",
                    "--nocapture",
                ])
                .env("PIPESTREAM_SETTLEMENT_DIRECTORY", directory.path())
                .env("PIPESTREAM_SETTLEMENT_ACTION", action)
                .env(
                    "PIPESTREAM_TEST_AUTHORITY_CRASH",
                    format!("{boundary}:{phase}"),
                )
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(86), "{action}:{phase}");
            let payloads = PayloadStore::open(
                &directory.path().join("objects"),
                store.payload_identity().unwrap(),
                payload_policy(),
            )
            .unwrap();
            let executor = Executor::new(
                store.clone(),
                payloads.clone(),
                applications.clone(),
                ResultEndpoint::new("results.example:7443".into()).unwrap(),
                caps(),
                Duration(100),
            )
            .unwrap();
            let fixture = Fixture {
                directory,
                store,
                binding,
                payloads,
                applications,
                executor,
                clock,
                auth,
                next_op,
            };
            fixture.clock.set(2000);
            let reopened = fixture.reopen().unwrap();
            let committed = phase == "after";
            if matches!(action, "work-fence" | "scope-fence") {
                let receipt = reopened.operation(&fixture.binding.identity, op(100));
                if committed {
                    receipt.unwrap();
                } else {
                    refuse(receipt, ErrorCode::NotFound);
                }
            }
            match action {
                "work-fence" => assert_eq!(
                    fixture.raw_view(&key(0, 1)).state,
                    if committed {
                        State::SKIPPED
                    } else {
                        State::ACTIVE
                    }
                ),
                "settlement-work" => assert_eq!(
                    fixture.raw_view(&key(0, 1)).state,
                    if committed {
                        State::FAILED
                    } else {
                        State::ACTIVE
                    }
                ),
                "session-revoke" if committed => refuse(
                    reopened.work_view(&fixture.binding.identity, &key(0, 1), Number(0)),
                    ErrorCode::Unauthorized,
                ),
                "closure" => assert_eq!(fixture.summary(0).is_some(), committed),
                "seal" => {
                    let mut connection = reopened.connect().unwrap();
                    let tx = connection.transaction().unwrap();
                    assert_eq!(
                        scopes::load(&tx, fixture.binding.identity.generation, Number(0))
                            .unwrap()
                            .seal
                            .is_some(),
                        committed
                    );
                }
                _ => (),
            }
            // Recovery uses the same bytes and charged input/output reservations.
            let job = fixture.job(&key(0, 1));
            assert!(job.input_live && job.outputs_live);
            let mut input = fixture
                .payloads
                .open_object(
                    &job.input_key.0,
                    &fixture.binding.identity.owner,
                    &job.parameters.input,
                )
                .unwrap();
            let mut bytes = [0; 3];
            assert_eq!(input.read_chunk(&mut bytes).unwrap(), 3);
            assert_eq!(&bytes, b"abc");
            reopened.revoke_session(&fixture.binding.identity).unwrap();
            fixture.close(1);
            reopened.integrity_check().unwrap();
        }
    }
}

#[test]
fn autonomous_settlement_fits_reserved_wal_without_row_replacement_or_page_growth() {
    for cancelled in [false, true] {
        let physical = PhysicalLimits {
            wal_bytes: 4 << 20,
            ..PhysicalLimits::default()
        };
        let fixture = Fixture::configured(Arc::new(CopyApplication), physical);
        fixture.declare(0, &[1], !cancelled);
        fixture.admit(key(0, 1), 0, 1000);
        if cancelled {
            fixture
                .store
                .cancel_scope(&fixture.binding.identity, op(100), Number(0))
                .unwrap();
        }
        let mut connection = fixture.store.connect().unwrap();
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        records::protect(&tx, 0, 0).unwrap();
        tx.execute_batch("CREATE TABLE settlement_fill(body BLOB);
            CREATE TRIGGER forbid_work_update BEFORE UPDATE ON work BEGIN SELECT RAISE(ABORT,'work row replacement'); END;
            CREATE TRIGGER forbid_job_update BEFORE UPDATE ON jobs BEGIN SELECT RAISE(ABORT,'job row replacement'); END;
            CREATE TRIGGER forbid_scope_update BEFORE UPDATE ON scopes BEGIN SELECT RAISE(ABORT,'scope row replacement'); END;
            CREATE TRIGGER forbid_clock_update BEFORE UPDATE ON authority BEGIN SELECT RAISE(ABORT,'clock row replacement'); END;").unwrap();
        tx.commit().unwrap();
        let mut reader = fixture.store.connect().unwrap();
        let snapshot = reader.transaction().unwrap();
        snapshot
            .query_row("SELECT count(*) FROM work", [], |row| row.get::<_, i64>(0))
            .unwrap();
        let mut filled = 0;
        loop {
            let result = (|| -> Result<()> {
                let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                records::protect(&tx, 0, 0)?;
                tx.execute("INSERT INTO settlement_fill VALUES(zeroblob(4096))", [])?;
                tx.commit()?;
                Ok(())
            })();
            if result.is_err() {
                refuse(result, ErrorCode::LimitExceeded);
                break;
            }
            filled += 1;
            assert!(filled < 4096);
        }
        assert!(filled > 0);
        let pages: i64 = connection
            .query_row("PRAGMA page_count", [], |row| row.get(0))
            .unwrap();
        let before = fixture.store.physical_usage().unwrap();
        fixture.clock.set(2000);
        let summary = fixture.close(1);
        let after = fixture.store.physical_usage().unwrap();
        assert_eq!(
            fixture.view(&key(0, 1)).state,
            if cancelled {
                State::CANCELLED
            } else {
                State::FAILED
            }
        );
        assert_eq!(summary.closed_at, Number(2000));
        assert_eq!(
            connection
                .query_row("PRAGMA page_count", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            pages
        );
        assert!(after.wal_bytes > before.wal_bytes && after.wal_bytes <= physical.wal_bytes);
        eprintln!(
            "settlement: cancelled={cancelled} fill={filled} pages={pages} WAL={} -> {} cap={}",
            before.wal_bytes, after.wal_bytes, physical.wal_bytes
        );
        fixture.store.integrity_check().unwrap();
    }
}

#[test]
fn reopening_refuses_prior_format_or_fence_whose_immutable_receipt_was_changed() {
    for corruption in ["format", "receipt", "time"] {
        let fixture = Fixture::new();
        fixture.declare(0, &[1], true);
        let receipt = fixture
            .store
            .skip_work(&fixture.binding.identity, op(100), &key(0, 1))
            .unwrap();
        let connection = fixture.store.connect().unwrap();
        if corruption == "format" {
            connection.execute_batch("PRAGMA user_version = 6").unwrap();
        } else {
            let mut changed = receipt;
            let Outcome::Skipped {
                disposition,
                accepted_at,
                ..
            } = &mut changed.body
            else {
                unreachable!()
            };
            if corruption == "time" {
                fixture.clock.set(1001);
                fixture
                    .store
                    .cancel_work(&fixture.binding.identity, op(101), &key(0, 1))
                    .unwrap();
                *accepted_at = Number(1001); // below shared clock, but after terminal settlement
            } else {
                *disposition = Disposition(1);
            }
            connection
                .execute(
                    "UPDATE operations SET receipt=?1 WHERE operation=?2",
                    params![pack(&changed).unwrap(), op(100).0.as_slice()],
                )
                .unwrap();
        }
        assert!(
            matches!(fixture.reopen(), Err(StoreError::Corrupt(_))),
            "{corruption}"
        );
    }
}

#[test]
fn ancestor_fence_rejects_prepared_admission_retry_and_publication_before_materialization() {
    let fixture = Fixture::new();
    fixture.declare(0, &[1], true);
    fixture.admit(key(0, 1), 1, 1000);
    fixture.declare(1, &[1, 2], false);
    fixture.admit(key(1, 1), 0, 1000);
    let execution = fixture
        .executor
        .claim(&fixture.binding.identity, &key(1, 1))
        .unwrap();
    let prepared = fixture.prepare(key(1, 2), 0, 1000);
    fixture
        .store
        .cancel_work(&fixture.binding.identity, op(100), &key(0, 1))
        .unwrap();
    // No reconciler has run: the ancestor's single durable fence suffices.
    assert_eq!(fixture.view(&key(1, 1)).state, State::ACTIVE);
    assert_eq!(fixture.view(&key(1, 2)).state, State::DECLARED);
    refuse(
        fixture
            .store
            .admit_input(prepared, &caps(), &fixture.applications),
        ErrorCode::Cancelled,
    );
    refuse(
        fixture
            .store
            .retry_work(&fixture.binding.identity, op(101), &key(1, 1), Id(1)),
        ErrorCode::Cancelled,
    );
    refuse(execution.run(), ErrorCode::Cancelled);
    assert!(fixture.view(&key(1, 2)).input.is_none());
    fixture.close(1);
    fixture.reopen().unwrap();
}

#[test]
fn owner_scope_cancellation_can_close_an_authority_producer_scope() {
    let fixture = Fixture::new();
    fixture.declare(0, &[1], true);
    let view = fixture.admit(key(0, 1), 2, 1000);
    assert_eq!(view.child.unwrap().producer, Producer(1));
    refuse(
        fixture.store.declare(
            &fixture.binding.identity,
            op(100),
            Number(1),
            &[Id(1)],
            false,
        ),
        ErrorCode::Unauthorized,
    );
    fixture
        .store
        .cancel_scope(&fixture.binding.identity, op(100), Number(1))
        .unwrap();
    fixture
        .store
        .skip_work(&fixture.binding.identity, op(101), &key(0, 1))
        .unwrap();
    fixture.close(1);
    let summary = fixture.summary(1).unwrap();
    assert_eq!(summary.producer, Producer(1));
    assert_eq!(summary.declared, Number(0));
    fixture.reopen().unwrap();
}

struct WithdrawOnCommit(AtomicU64);
impl Authorization for WithdrawOnCommit {
    fn permits(&self, _: &IdentityLabel, permission: Permission) -> bool {
        permission != Permission::Cancel || self.0.fetch_add(1, Ordering::SeqCst) == 0
    }
}
#[test]
fn last_moment_authorization_denial_rolls_back_fence_outcome_receipt_and_clock() {
    for scope in [false, true] {
        let fixture = Fixture::new();
        fixture.declare(0, &[1], false);
        let before = fixture.view(&key(0, 1));
        let mut store = fixture.store.clone();
        store.authorization = Arc::new(WithdrawOnCommit(AtomicU64::new(0)));
        fixture.clock.set(1001);
        let result = if scope {
            store.cancel_scope(&fixture.binding.identity, op(100), Number(0))
        } else {
            store.cancel_work(&fixture.binding.identity, op(100), &key(0, 1))
        };
        refuse(result, ErrorCode::Unauthorized);
        fixture.clock.set(1000); // failed mutation did not advance the durable clock
        assert_eq!(fixture.view(&key(0, 1)), before);
        refuse(
            fixture.store.operation(&fixture.binding.identity, op(100)),
            ErrorCode::NotFound,
        );
        fixture.declare(0, &[2], false);
        fixture.reopen().unwrap();
    }
}
