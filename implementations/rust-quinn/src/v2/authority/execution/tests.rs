use super::*;
use crate::v2::authority::{ingress::*, payload::*};
use sha2::{Digest as _, Sha256};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

struct TestClock(AtomicU64);
impl Clock for TestClock {
    fn read(&self) -> ClockReading {
        ClockReading {
            utc_ms: Number(self.0.load(Ordering::SeqCst)),
            trusted: true,
        }
    }
}
struct Auth(AtomicBool);
impl Authorization for Auth {
    fn permits(&self, owner: &IdentityLabel, _: Permission) -> bool {
        owner.0 == "alice" && self.0.load(Ordering::SeqCst)
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
fn applications(application: Arc<dyn Application>) -> Arc<Applications> {
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
    payloads: PayloadStore,
    binding: Binding,
    clock: Arc<TestClock>,
    auth: Arc<Auth>,
    executor: Executor,
}
impl Fixture {
    fn new(application: Arc<dyn Application>) -> Self {
        Self::configured(application, caps(), PhysicalLimits::default())
    }
    fn configured(
        application: Arc<dyn Application>,
        caps: Capabilities,
        physical: PhysicalLimits,
    ) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let clock = Arc::new(TestClock(AtomicU64::new(1000)));
        let auth = Arc::new(Auth(AtomicBool::new(true)));
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
                &caps,
            )
            .unwrap();
        store
            .declare(
                &binding.identity,
                OperationId([1; 16]),
                Number(0),
                &[Id(1)],
                true,
            )
            .unwrap();
        let payloads = PayloadStore::initialize(
            &directory.path().join("objects"),
            store.payload_identity().unwrap(),
            payload_policy(),
        )
        .unwrap();
        store.bind_payloads(&payloads).unwrap();
        let executor = Executor::new(
            store.clone(),
            payloads.clone(),
            applications(application),
            ResultEndpoint::new("results.example:7443".into()).unwrap(),
            caps,
            Duration(100),
        )
        .unwrap();
        Self {
            directory,
            store,
            payloads,
            binding,
            clock,
            auth,
            executor,
        }
    }
    fn key(&self) -> WorkKey {
        WorkKey {
            scope: Number(0),
            producer: Producer(0),
            entity: Id(1),
        }
    }
    fn admit(&self, mode: u64, count: u64, bytes: u64) {
        let header = InputHeader {
            kind: Literal,
            generation: self.binding.identity.generation,
            operation: OperationId([2; 16]),
            parameters: AdmitParameters {
                work: self.key(),
                input: Input {
                    length: Number(3),
                    sha256: Digest(Sha256::digest(b"abc").into()),
                    content_type: ApplicationLabel("text/plain".into()),
                },
                application: ApplicationLabel("test/v1".into()),
                mode: Mode(mode),
                execution_ms: Duration(1000),
                outputs: OutputBudget {
                    count: BatchCount(count),
                    total_bytes: Number(bytes),
                },
            },
        };
        let now = Instant::now();
        let InputReception::Receiving(mut receiving) = self
            .store
            .receive_input(
                &self.binding.identity,
                &header,
                &self.executor.caps,
                &self.payloads,
                &self.executor.applications,
                now,
            )
            .unwrap()
        else {
            panic!("unexpected replay")
        };
        receiving.receive(b"abc", now).unwrap();
        let InputPreparation::Ready(prepared) = self
            .store
            .prepare_input(
                receiving.finish(now).unwrap(),
                &self.executor.caps,
                &self.executor.applications,
            )
            .unwrap()
        else {
            panic!("unexpected replay")
        };
        self.store
            .admit_input(*prepared, &self.executor.caps, &self.executor.applications)
            .unwrap();
    }
    fn view(&self) -> WorkView {
        self.store
            .work_view(&self.binding.identity, &self.key(), Number(0))
            .unwrap()
            .1
    }
    fn job(&self) -> jobs::JobRecord {
        let mut connection = self.store.connect().unwrap();
        let tx = connection.transaction().unwrap();
        load(&tx, &self.binding.identity, &self.key()).unwrap().1
    }
    fn run(&self) -> Result<WorkView> {
        self.executor.run(&self.binding.identity, &self.key())
    }
    fn reopen(&self) -> AuthorityStore {
        AuthorityStore::open(
            &self.directory.path().join("authority.sqlite"),
            self.store.authority.clone(),
            self.store.policy.clone(),
            self.store.physical.limits,
            self.clock.clone(),
            self.auth.clone(),
        )
        .unwrap()
    }
}
fn refuse<T>(result: Result<T>, code: ErrorCode) {
    match result {
        Err(StoreError::Protocol(error)) => assert_eq!(error.code, code),
        Err(other) => panic!("expected {code:?}: {other:?}"),
        Ok(_) => panic!("expected {code:?}"),
    }
}
struct CountCopy(Arc<AtomicUsize>);
impl Application for CountCopy {
    fn execute(&self, context: &mut WorkContext) -> Result<ApplicationOutcome> {
        self.0.fetch_add(1, Ordering::SeqCst);
        CopyApplication.execute(context)
    }
}

#[test]
fn real_callback_publishes_exact_immutable_manifest_and_never_runs_from_admission_or_replay() {
    let calls = Arc::new(AtomicUsize::new(0));
    let fixture = Fixture::new(Arc::new(CountCopy(calls.clone())));
    fixture.admit(0, 1, 3);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let view = fixture.run().unwrap();
    assert_eq!((view.state, view.attempt), (State::SUCCEEDED, Number(1)));
    let manifest = view.manifest.as_ref().unwrap();
    assert_eq!(manifest.input_sha256, Digest(Sha256::digest(b"abc").into()));
    assert_eq!(manifest.outputs.len(), 1);
    assert_eq!(manifest.outputs[0].sha256, manifest.input_sha256);
    assert_eq!(manifest.outputs[0].length, Number(3));
    let mut reader = fixture
        .payloads
        .open_output(
            &fixture.job().reservation_key.0,
            OutputIndex(0),
            &fixture.binding.identity.owner,
            &Input {
                length: Number(3),
                sha256: manifest.outputs[0].sha256,
                content_type: manifest.outputs[0].content_type.clone(),
            },
        )
        .unwrap();
    let mut bytes = [0; 3];
    assert_eq!(reader.read_chunk(&mut bytes).unwrap(), 3);
    assert_eq!(reader.read_chunk(&mut bytes).unwrap(), 0);
    assert!(reader.verified());
    assert_eq!(&bytes, b"abc");
    assert_eq!(
        manifest.outputs[0].locator.target().unwrap().host,
        "results.example"
    );
    assert_eq!(
        fixture
            .reopen()
            .work_view(&fixture.binding.identity, &fixture.key(), Number(0))
            .unwrap()
            .1,
        view
    );
    refuse(fixture.run(), ErrorCode::AlreadyTerminal);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(!fixture.job().executor_live);
    assert!(fixture.job().input_live && fixture.job().outputs_live);
    fixture.store.integrity_check().unwrap();
}

#[test]
fn lease_expiry_cannot_publish_or_recycle_live_worker_outputs_and_recovery_keeps_attempt() {
    let fixture = Fixture::new(Arc::new(CopyApplication));
    fixture.admit(0, 1, 3);
    let mut old = fixture
        .executor
        .claim(&fixture.binding.identity, &fixture.key())
        .unwrap();
    CopyApplication.execute(&mut old.context).unwrap();
    assert_eq!((old.lease(), old.attempt()), (Number(1), Id(1)));
    refuse(
        fixture
            .executor
            .claim(&fixture.binding.identity, &fixture.key()),
        ErrorCode::NotReady,
    );
    fixture.clock.0.store(1100, Ordering::SeqCst);
    refuse(
        fixture
            .executor
            .claim(&fixture.binding.identity, &fixture.key()),
        ErrorCode::NotReady,
    );
    refuse(
        old.context.publish(ApplicationOutcome::Succeeded),
        ErrorCode::Conflict,
    );
    let recovered = fixture
        .executor
        .claim(&fixture.binding.identity, &fixture.key())
        .unwrap();
    assert_eq!((recovered.lease(), recovered.attempt()), (Number(2), Id(1)));
    assert_eq!(recovered.context.reservation.usage().unwrap().outputs, 0);
    let view = recovered.run().unwrap();
    assert_eq!(view.state, State::SUCCEEDED);
    assert_eq!(view.manifest.unwrap().attempt, Id(1));
    fixture.store.integrity_check().unwrap();
}

#[test]
fn expired_or_unauthorized_execution_never_invokes_application() {
    for case in ["deadline", "owner", "scope"] {
        let calls = Arc::new(AtomicUsize::new(0));
        let fixture = Fixture::new(Arc::new(CountCopy(calls.clone())));
        fixture.admit(0, 1, 3);
        let code = match case {
            "deadline" => {
                fixture.clock.0.store(2000, Ordering::SeqCst);
                ErrorCode::DeadlineExceeded
            }
            "owner" => {
                fixture.auth.0.store(false, Ordering::SeqCst);
                ErrorCode::Unauthorized
            }
            _ => {
                super::super::tests::set_scope_fence(
                    &fixture.store,
                    fixture.binding.identity.generation,
                    false,
                );
                ErrorCode::Cancelled
            }
        };
        refuse(fixture.run(), code);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

struct BadOutput;
impl Application for BadOutput {
    fn execute(&self, context: &mut WorkContext) -> Result<ApplicationOutcome> {
        context.begin_output(Number(1), ApplicationLabel("text/plain".into()))?;
        let _ = context.write_output(b"too large"); // deliberate application bug
        let _ = context.finish_output();
        Ok(ApplicationOutcome::Succeeded)
    }
}
#[test]
fn ignored_output_error_commits_failure_not_truncated_success() {
    let fixture = Fixture::new(Arc::new(BadOutput));
    fixture.admit(0, 1, 3);
    let view = fixture.run().unwrap();
    assert_eq!(view.state, State::FAILED);
    assert_eq!(
        view.diagnostic.unwrap().code,
        DiagnosticCode(ErrorCode::LimitExceeded as u64)
    );
    assert!(view.manifest.is_none() && view.output_until.is_none());
    fixture.store.integrity_check().unwrap();
}

struct Retryable;
impl Application for Retryable {
    fn execute(&self, _: &mut WorkContext) -> Result<ApplicationOutcome> {
        Ok(ApplicationOutcome::Retryable(diag(
            ErrorCode::InternalError,
            "retry requested by application",
        )))
    }
}
#[test]
fn retryable_failure_stays_nonterminal_and_does_not_automatically_execute_again() {
    let fixture = Fixture::new(Arc::new(Retryable));
    fixture.admit(0, 0, 0);
    let view = fixture.run().unwrap();
    assert_eq!(view.state, State::AWAITING_RETRY);
    assert!(view.diagnostic.is_some() && view.terminal_at.is_none() && view.manifest.is_none());
    assert!(fixture.job().executor_live);
    refuse(fixture.run(), ErrorCode::NotReady);
    fixture.store.integrity_check().unwrap();
}

#[test]
fn caller_branch_waits_for_real_sealed_child_closure_before_rehydration() {
    let fixture = Fixture::new(Arc::new(CopyApplication));
    fixture.admit(1, 1, 3);
    refuse(fixture.run(), ErrorCode::NotReady);
    fixture
        .store
        .declare(
            &fixture.binding.identity,
            OperationId([3; 16]),
            Number(1),
            &[],
            true,
        )
        .unwrap();
    let view = fixture.run().unwrap();
    assert_eq!(view.state, State::SUCCEEDED);
    assert_eq!(view.child.unwrap().scope, Id(1));
    fixture.store.integrity_check().unwrap();
}

#[test]
fn retry_fences_live_worker_once_preserves_input_and_deadline_and_refunds_no_live_bytes() {
    let fixture = Fixture::new(Arc::new(CopyApplication));
    fixture.admit(0, 1, 3);
    let mut old = fixture
        .executor
        .claim(&fixture.binding.identity, &fixture.key())
        .unwrap();
    CopyApplication.execute(&mut old.context).unwrap();
    let before = fixture.view();
    let receipt = fixture
        .store
        .retry_work(
            &fixture.binding.identity,
            OperationId([3; 16]),
            &fixture.key(),
            Id(1),
        )
        .unwrap();
    assert_eq!(
        fixture
            .store
            .retry_work(
                &fixture.binding.identity,
                OperationId([3; 16]),
                &fixture.key(),
                Id(1)
            )
            .unwrap(),
        receipt
    );
    assert!(matches!(
        receipt.body,
        Outcome::Retried {
            expected_attempt: Id(1),
            replacement_attempt: Id(2),
            ..
        }
    ));
    let view = fixture.view();
    assert_eq!(
        (view.input, view.admitted_at, view.deadline, view.child),
        (
            before.input,
            before.admitted_at,
            before.deadline,
            before.child
        )
    );
    refuse(fixture.run(), ErrorCode::NotReady); // old output handles have not drained
    refuse(
        old.context.publish(ApplicationOutcome::Succeeded),
        ErrorCode::Conflict,
    );
    let complete = fixture.run().unwrap();
    assert_eq!(
        (complete.state, complete.attempt),
        (State::SUCCEEDED, Number(2))
    );
    refuse(
        fixture.store.retry_work(
            &fixture.binding.identity,
            OperationId([4; 16]),
            &fixture.key(),
            Id(2),
        ),
        ErrorCode::AlreadyTerminal,
    );
    fixture.store.integrity_check().unwrap();
}

#[test]
fn retry_refuses_stale_expected_attempt_deadline_and_changed_operation() {
    let fixture = Fixture::new(Arc::new(Retryable));
    fixture.admit(0, 0, 0);
    fixture.run().unwrap();
    refuse(
        fixture.store.retry_work(
            &fixture.binding.identity,
            OperationId([3; 16]),
            &fixture.key(),
            Id(2),
        ),
        ErrorCode::Conflict,
    );
    fixture
        .store
        .retry_work(
            &fixture.binding.identity,
            OperationId([3; 16]),
            &fixture.key(),
            Id(1),
        )
        .unwrap();
    refuse(
        fixture.store.retry_work(
            &fixture.binding.identity,
            OperationId([3; 16]),
            &fixture.key(),
            Id(2),
        ),
        ErrorCode::Conflict,
    );
    fixture.clock.0.store(2000, Ordering::SeqCst);
    refuse(
        fixture.store.retry_work(
            &fixture.binding.identity,
            OperationId([4; 16]),
            &fixture.key(),
            Id(2),
        ),
        ErrorCode::DeadlineExceeded,
    );
    assert_eq!(fixture.view().attempt, Number(2));
}

#[test]
fn renewal_preserves_lease_identity_and_cannot_resurrect_an_expired_lease() {
    let fixture = Fixture::new(Arc::new(CopyApplication));
    fixture.admit(0, 1, 3);
    let mut execution = fixture
        .executor
        .claim(&fixture.binding.identity, &fixture.key())
        .unwrap();
    fixture.clock.0.store(1050, Ordering::SeqCst);
    execution.context.renew().unwrap();
    assert_eq!(
        (fixture.job().lease, fixture.job().lease_until),
        (Number(1), Some(Number(1150)))
    );
    fixture.clock.0.store(1100, Ordering::SeqCst);
    execution.context.check().unwrap();
    fixture.clock.0.store(1150, Ordering::SeqCst);
    refuse(execution.context.renew(), ErrorCode::Conflict);
    assert_eq!(fixture.job().lease, Number(1));
}

struct WithdrawAtPublication;
impl Application for WithdrawAtPublication {
    fn execute(&self, context: &mut WorkContext) -> Result<ApplicationOutcome> {
        CopyApplication.execute(context)?;
        super::super::tests::set_scope_fence(
            &context.executor.store,
            context.identity.generation,
            false,
        );
        Ok(ApplicationOutcome::Succeeded)
    }
}
#[test]
fn actual_callback_runs_outside_writer_and_a_late_scope_fence_wins_publication() {
    let fixture = Fixture::new(Arc::new(WithdrawAtPublication));
    fixture.admit(0, 1, 3);
    refuse(fixture.run(), ErrorCode::Cancelled);
    assert!(fixture.view().manifest.is_none());
    assert_eq!(fixture.view().state, State::ACTIVE);
}

struct Broken(u8);
impl Application for Broken {
    fn execute(&self, context: &mut WorkContext) -> Result<ApplicationOutcome> {
        match self.0 {
            0 => panic!("deliberate callback panic"),
            1 => {
                context.begin_output(Number(3), ApplicationLabel("text/plain".into()))?;
                context.write_output(b"abc")?;
                Ok(ApplicationOutcome::Succeeded)
            }
            _ => Ok(ApplicationOutcome::Failed(Diagnostic {
                code: DiagnosticCode(1),
                detail: Detail("d".repeat(513)),
            })),
        }
    }
}
#[test]
fn callback_panic_unfinished_output_and_invalid_diagnostic_settle_as_failure() {
    for case in 0..3 {
        let fixture = Fixture::new(Arc::new(Broken(case)));
        fixture.admit(0, 1, 3);
        let view = fixture.run().unwrap();
        assert_eq!(view.state, State::FAILED);
        assert!(view.manifest.is_none() && view.diagnostic.is_some());
        fixture.store.integrity_check().unwrap();
    }
}

#[test]
fn execution_crash_child() {
    let Some(path) = std::env::var_os("PIPESTREAM_EXECUTION_CHILD_DIRECTORY") else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    let now = std::env::var("PIPESTREAM_EXECUTION_NOW")
        .unwrap_or("1000".into())
        .parse()
        .unwrap();
    let store = AuthorityStore::open(
        &path.join("authority.sqlite"),
        IdentityLabel("test-authority".into()),
        super::super::tests::policy(),
        PhysicalLimits::default(),
        Arc::new(TestClock(AtomicU64::new(now))),
        Arc::new(Auth(AtomicBool::new(true))),
    )
    .unwrap();
    let identity = SessionIdentity {
        authority: IdentityLabel("test-authority".into()),
        owner: IdentityLabel("alice".into()),
        generation: Id(1),
    };
    let key = WorkKey {
        scope: Number(0),
        producer: Producer(0),
        entity: Id(1),
    };
    if std::env::var("PIPESTREAM_TEST_AUTHORITY_CRASH")
        .unwrap()
        .starts_with("worker-retry")
    {
        store
            .retry_work(&identity, OperationId([3; 16]), &key, Id(1))
            .unwrap();
    } else {
        let payloads = PayloadStore::open(
            &path.join("objects"),
            store.payload_identity().unwrap(),
            payload_policy(),
        )
        .unwrap();
        Executor::new(
            store,
            payloads,
            applications(Arc::new(CopyApplication)),
            ResultEndpoint::new("results.example:7443".into()).unwrap(),
            caps(),
            Duration(100),
        )
        .unwrap()
        .run(&identity, &key)
        .unwrap();
    }
    panic!("worker crash boundary did not fire");
}
fn child(path: &std::path::Path, boundary: &str, now: u64) {
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "v2::authority::execution::tests::execution_crash_child",
            "--nocapture",
        ])
        .env("PIPESTREAM_EXECUTION_CHILD_DIRECTORY", path)
        .env("PIPESTREAM_TEST_AUTHORITY_CRASH", boundary)
        .env("PIPESTREAM_EXECUTION_NOW", now.to_string())
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(86), "{boundary}");
}

#[test]
fn process_death_brackets_claim_publication_retry_and_unpublished_output_reclamation() {
    for boundary in [
        "worker-claim:before",
        "worker-claim:after",
        "worker-publish:before",
        "worker-publish:after",
        "worker-retry:before",
        "worker-retry:after",
        "worker-cleanup:unlinked",
    ] {
        let retry = boundary.starts_with("worker-retry");
        let fixture = Fixture::new(if retry {
            Arc::new(Retryable)
        } else {
            Arc::new(CopyApplication)
        });
        fixture.admit(0, 1, 3);
        if retry {
            fixture.run().unwrap();
        }
        let Fixture {
            directory,
            store,
            payloads,
            binding,
            clock,
            auth,
            executor,
        } = fixture;
        drop(executor);
        drop(payloads);
        if boundary == "worker-cleanup:unlinked" {
            child(directory.path(), "worker-publish:before", 1000);
        }
        child(
            directory.path(),
            boundary,
            if boundary == "worker-cleanup:unlinked" {
                1200
            } else {
                1000
            },
        );
        clock.0.store(1400, Ordering::SeqCst);
        let store = AuthorityStore::open(
            &directory.path().join("authority.sqlite"),
            store.authority.clone(),
            store.policy.clone(),
            PhysicalLimits::default(),
            clock,
            auth,
        )
        .unwrap();
        let payloads = PayloadStore::open(
            &directory.path().join("objects"),
            store.payload_identity().unwrap(),
            payload_policy(),
        )
        .unwrap();
        let key = WorkKey {
            scope: Number(0),
            producer: Producer(0),
            entity: Id(1),
        };
        let before = store
            .work_view(&binding.identity, &key, Number(0))
            .unwrap()
            .1;
        if retry {
            assert_eq!(
                before.attempt,
                Number(if boundary.ends_with("after") { 2 } else { 1 })
            );
            let receipt = store
                .retry_work(&binding.identity, OperationId([3; 16]), &key, Id(1))
                .unwrap();
            assert!(matches!(
                receipt.body,
                Outcome::Retried {
                    replacement_attempt: Id(2),
                    ..
                }
            ));
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let executor = Executor::new(
            store.clone(),
            payloads.clone(),
            applications(Arc::new(CountCopy(calls.clone()))),
            ResultEndpoint::new("results.example:7443".into()).unwrap(),
            caps(),
            Duration(100),
        )
        .unwrap();
        let complete = if boundary == "worker-publish:after" {
            assert_eq!(before.state, State::SUCCEEDED);
            refuse(
                executor.run(&binding.identity, &key),
                ErrorCode::AlreadyTerminal,
            );
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            before
        } else {
            assert!(before.manifest.is_none());
            let view = executor.run(&binding.identity, &key).unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            view
        };
        assert_eq!(complete.attempt, Number(if retry { 2 } else { 1 }));
        let mut connection = store.connect().unwrap();
        let tx = connection.transaction().unwrap();
        let job = load(&tx, &binding.identity, &key).unwrap().1;
        drop(tx);
        let output = &complete.manifest.unwrap().outputs[0];
        let mut reader = payloads
            .open_output(
                &job.reservation_key.0,
                OutputIndex(0),
                &binding.identity.owner,
                &Input {
                    length: output.length,
                    sha256: output.sha256,
                    content_type: output.content_type.clone(),
                },
            )
            .unwrap();
        let mut bytes = [0; 3];
        assert_eq!(reader.read_chunk(&mut bytes).unwrap(), 3);
        assert_eq!(reader.read_chunk(&mut bytes).unwrap(), 0);
        assert!(reader.verified());
        assert_eq!(&bytes, b"abc");
        store.integrity_check().unwrap();
    }
}

struct EmptyOutputs(usize);
impl Application for EmptyOutputs {
    fn execute(&self, context: &mut WorkContext) -> Result<ApplicationOutcome> {
        for _ in 0..self.0 {
            context.begin_output(
                Number(0),
                ApplicationLabel("application/octet-stream".into()),
            )?;
            context.finish_output()?;
        }
        Ok(ApplicationOutcome::Succeeded)
    }
}
#[test]
fn full_publication_fits_reserved_journal_without_row_replacement_or_page_growth() {
    for count in [0, 1, 256] {
        let mut selected = caps();
        selected.control_limit = ControlLimit(1 << 20);
        let physical = PhysicalLimits {
            wal_bytes: 4 << 20,
            ..PhysicalLimits::default()
        };
        let fixture = Fixture::configured(Arc::new(EmptyOutputs(count)), selected, physical);
        fixture.admit(0, count as u64, 0);
        let mut execution = fixture
            .executor
            .claim(&fixture.binding.identity, &fixture.key())
            .unwrap();
        let outcome = execution
            .application
            .execute(&mut execution.context)
            .unwrap();
        let mut connection = fixture.store.connect().unwrap();
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        records::protect(&tx, 0, 0).unwrap();
        tx.execute_batch("CREATE TABLE worker_fill(body BLOB);
            CREATE TRIGGER forbid_work_update BEFORE UPDATE ON work BEGIN SELECT RAISE(ABORT,'work row replacement'); END;
            CREATE TRIGGER forbid_job_update BEFORE UPDATE ON jobs BEGIN SELECT RAISE(ABORT,'job row replacement'); END;
            CREATE TRIGGER forbid_clock_update BEFORE UPDATE ON authority BEGIN SELECT RAISE(ABORT,'clock row replacement'); END;").unwrap();
        tx.commit().unwrap();
        let mut reader = fixture.store.connect().unwrap();
        let snapshot = reader.transaction().unwrap();
        snapshot
            .query_row("SELECT count(*) FROM work", [], |r| r.get::<_, i64>(0))
            .unwrap();
        let mut filled = 0;
        loop {
            let result = (|| -> Result<()> {
                let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                records::protect(&tx, 0, 0)?;
                tx.execute("INSERT INTO worker_fill VALUES(zeroblob(4096))", [])?;
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
        let pages: i64 = connection
            .query_row("PRAGMA page_count", [], |r| r.get(0))
            .unwrap();
        let before = fixture.store.physical_usage().unwrap();
        fixture.clock.0.store(1001, Ordering::SeqCst);
        let view = execution.context.publish(outcome).unwrap();
        let after = fixture.store.physical_usage().unwrap();
        assert_eq!(view.state, State::SUCCEEDED);
        assert_eq!(view.manifest.as_ref().unwrap().outputs.len(), count);
        assert_eq!(view.terminal_at, Some(Number(1001)));
        assert_eq!(
            connection
                .query_row("PRAGMA page_count", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            pages
        );
        assert!(after.wal_bytes > before.wal_bytes && after.wal_bytes <= physical.wal_bytes);
        eprintln!(
            "worker publication: outputs={count} fill={filled} pages={pages} WAL={} -> {} cap={}",
            before.wal_bytes, after.wal_bytes, physical.wal_bytes
        );
        fixture.store.integrity_check().unwrap();
    }
}

#[test]
fn success_without_result_profile_has_no_manifest_and_cannot_silently_discard_outputs() {
    for produces_output in [false, true] {
        let mut selected = caps();
        selected.supported = vec![ProfileId(DURABLE_WORK.into())];
        let fixture = Fixture::configured(
            Arc::new(EmptyOutputs(usize::from(produces_output))),
            selected,
            PhysicalLimits::default(),
        );
        fixture.admit(0, 0, 0);
        let view = fixture.run().unwrap();
        assert_eq!(
            view.state,
            if produces_output {
                State::FAILED
            } else {
                State::SUCCEEDED
            }
        );
        assert!(view.manifest.is_none() && view.output_until.is_none());
        fixture.store.integrity_check().unwrap();
    }
}

#[test]
fn reopening_refuses_a_checksummed_manifest_rebound_to_another_owner() {
    let fixture = Fixture::new(Arc::new(CopyApplication));
    fixture.admit(0, 1, 3);
    fixture.run().unwrap();
    let mut connection = fixture.store.connect().unwrap();
    let tx = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let (row, _, _, mut view, revision) =
        load(&tx, &fixture.binding.identity, &fixture.key()).unwrap();
    view.manifest.as_mut().unwrap().owner = IdentityLabel("bob".into());
    records::replace(&tx, work_target(row), revision, &view, false).unwrap();
    tx.commit().unwrap();
    assert!(matches!(
        fixture.store.integrity_check(),
        Err(StoreError::Corrupt(_))
    ));
    assert!(matches!(
        AuthorityStore::open(
            &fixture.directory.path().join("authority.sqlite"),
            fixture.store.authority.clone(),
            fixture.store.policy.clone(),
            PhysicalLimits::default(),
            fixture.clock.clone(),
            fixture.auth.clone()
        ),
        Err(StoreError::Corrupt(_))
    ));
}
