use super::*;

struct UppercaseScatter;
impl Application for UppercaseScatter {
    fn expansion(&self) -> Option<&dyn Expansion> {
        Some(self)
    }
    fn execute(&self, context: &mut WorkContext) -> Result<ApplicationOutcome> {
        let input = context.input_descriptor().clone();
        context.begin_output(input.length, input.content_type)?;
        let mut bytes = [0; 2];
        if context.mode() == Mode(0) {
            loop {
                let n = context.read_input(&mut bytes)?;
                if n == 0 {
                    break;
                }
                bytes[..n].make_ascii_uppercase();
                context.write_output(&bytes[..n])?;
            }
        } else {
            let mut after = Number(0);
            loop {
                let page = context.children(after, PageLimit(1))?;
                for child in page.members {
                    context.begin_child_output(child.entity, OutputIndex(0))?;
                    loop {
                        let n = context.read_child_output(&mut bytes)?;
                        if n == 0 {
                            break;
                        }
                        context.write_output(&bytes[..n])?;
                    }
                    context.finish_child_output()?;
                    after = Number(child.entity.0);
                }
                if !page.more {
                    break;
                }
            }
        }
        context.finish_output()?;
        Ok(ApplicationOutcome::Succeeded)
    }
}
fn parameters(scope: Id, entity: Id, producer: Producer, bytes: &[u8]) -> AdmitParameters {
    AdmitParameters {
        work: WorkKey {
            scope: Number(scope.0),
            producer,
            entity,
        },
        input: Input {
            length: Number(bytes.len() as u64),
            sha256: Digest(Sha256::digest(bytes).into()),
            content_type: ApplicationLabel("text/plain".into()),
        },
        application: ApplicationLabel("test/v1".into()),
        mode: Mode(0),
        execution_ms: Duration(1000),
        outputs: OutputBudget {
            count: BatchCount(1),
            total_bytes: Number(bytes.len() as u64),
        },
    }
}
impl Expansion for UppercaseScatter {
    fn expand(&self, context: &mut ExpansionContext<'_>) -> Result<ExpansionOutcome> {
        context.declare(context.operation(Id(1))?, &[Id(1), Id(2)], true)?;
        let mut bytes = [0; 2];
        for entity in 1..=2 {
            let n = context.read_input(&mut bytes)?;
            assert!(n > 0);
            let input = parameters(context.child_scope(), Id(entity), Producer(1), &bytes[..n]);
            let now = Instant::now();
            match context.receive_input(context.operation(Id(entity + 1))?, input, now)? {
                InputReception::Replay(_) => (),
                InputReception::Receiving(mut receiving) => {
                    receiving.receive(&bytes[..n], now)?;
                    match context.prepare_input(receiving.finish(now)?)? {
                        InputPreparation::Replay(_) => (),
                        InputPreparation::Ready(prepared) => {
                            context.admit_input(*prepared)?;
                        }
                    }
                }
            }
        }
        assert_eq!(context.read_input(&mut bytes)?, 0);
        Ok(ExpansionOutcome::Complete)
    }
}
fn reconcile_until(fixture: &Fixture, scope: Number) -> ScopeSummary {
    let mut cursor = ReconcileCursor::default();
    for _ in 0..100 {
        fixture.store.reconcile(&mut cursor, 1).unwrap();
        let mut connection = fixture.store.connect().unwrap();
        let tx = connection.transaction().unwrap();
        if let Some(summary) =
            scopes::closed(&tx, fixture.binding.identity.generation, scope).unwrap()
        {
            return summary;
        }
    }
    panic!("scope did not close");
}
fn output_bytes(fixture: &Fixture) -> Vec<u8> {
    let view = fixture.view();
    let output = &view.manifest.unwrap().outputs[0];
    let mut reader = fixture
        .payloads
        .open_output(
            &fixture.job().reservation_key.0,
            OutputIndex(0),
            &fixture.binding.identity.owner,
            &Input {
                length: output.length,
                sha256: output.sha256,
                content_type: output.content_type.clone(),
            },
        )
        .unwrap();
    let mut result = Vec::new();
    let mut bytes = [0; 2];
    loop {
        let n = reader.read_chunk(&mut bytes).unwrap();
        if n == 0 {
            break;
        }
        result.extend_from_slice(&bytes[..n]);
    }
    result
}
fn execute_children(fixture: &Fixture, producer: Producer) {
    for entity in 1..=2 {
        let work = WorkKey {
            scope: Number(1),
            producer,
            entity: Id(entity),
        };
        assert_eq!(
            fixture
                .executor
                .run(&fixture.binding.identity, &work)
                .unwrap()
                .state,
            State::SUCCEEDED
        );
    }
    assert_eq!(
        reconcile_until(fixture, Number(1)).counts.success,
        Number(2)
    );
}

#[test]
fn authority_expansion_commits_real_children_and_reassembles_their_transformed_outputs() {
    let fixture = Fixture::new(Arc::new(UppercaseScatter));
    fixture.admit(2, 1, 3);
    assert_eq!(fixture.run().unwrap().state, State::WAITING_CHILDREN);
    refuse(fixture.run(), ErrorCode::NotReady);
    execute_children(&fixture, Producer(1));
    assert_eq!(fixture.run().unwrap().state, State::SUCCEEDED);
    assert_eq!(output_bytes(&fixture), b"ABC");
    assert_eq!(
        reconcile_until(&fixture, Number(0)).counts.success,
        Number(1)
    );
    fixture.reopen().integrity_check().unwrap();
}

#[test]
fn one_worker_can_expand_run_children_and_reassemble_without_a_thread_per_branch() {
    let fixture = Fixture::new(Arc::new(UppercaseScatter));
    fixture.admit(2, 1, 3);
    let pool = fixture
        .executor
        .start_workers(PoolConfig {
            workers: 1,
            workers_per_owner: 1,
            scan_batch: 1,
            ..pool_config()
        })
        .unwrap();
    wait_until(|| fixture.view().state.is_terminal());
    let status = pool.shutdown().unwrap();
    assert_eq!(fixture.view().state, State::SUCCEEDED);
    assert_eq!(output_bytes(&fixture), b"ABC");
    assert_eq!(status.completed, 3);
    assert_eq!(status.waiting_children, 1);
    assert!(!status.faulted);
    fixture.reopen().integrity_check().unwrap();
}

#[test]
fn declared_authority_children_cannot_be_supplied_by_an_external_input_or_lookup_namespace() {
    let fixture = Fixture::new(Arc::new(UppercaseScatter));
    fixture.admit(2, 1, 3);
    fixture.run().unwrap();
    let header = InputHeader {
        kind: Literal,
        generation: fixture.binding.identity.generation,
        operation: OperationId([7; 16]),
        parameters: parameters(Id(1), Id(1), Producer(1), b"ab"),
    };
    refuse(
        fixture.store.receive_input(
            &fixture.binding.identity,
            &header,
            &caps(),
            &fixture.payloads,
            &fixture.executor.applications,
            Instant::now(),
        ),
        ErrorCode::Unauthorized,
    );
    refuse(
        fixture.store.declare(
            &fixture.binding.identity,
            OperationId([7; 16]),
            Number(1),
            &[Id(3)],
            true,
        ),
        ErrorCode::Unauthorized,
    );
    let mut connection = fixture.store.connect().unwrap();
    let tx = connection.transaction().unwrap();
    let (_, job, _, _, _) = load(&tx, &fixture.binding.identity, &header.parameters.work).unwrap();
    assert_eq!(job.originator, Producer(1));
    refuse(
        fixture
            .store
            .operation(&fixture.binding.identity, job.operation),
        ErrorCode::NotFound,
    );
    assert!(
        scopes::operation(
            &tx,
            fixture.binding.identity.generation,
            Producer(1),
            job.operation
        )
        .unwrap()
        .is_some()
    );
    fixture.reopen().integrity_check().unwrap();
}

#[test]
fn branch_claim_reserves_child_reader_so_other_reads_cannot_steal_it() {
    let fixture = Fixture::new(Arc::new(UppercaseScatter));
    fixture.admit(2, 1, 3);
    fixture.run().unwrap();
    execute_children(&fixture, Producer(1));
    let execution = fixture
        .executor
        .claim(&fixture.binding.identity, &fixture.key())
        .unwrap();
    let job = fixture.job();
    let mut held = Vec::new();
    loop {
        match fixture.payloads.open_object(
            &job.input_key.0,
            &fixture.binding.identity.owner,
            &job.parameters.input,
        ) {
            Ok(reader) => held.push(reader),
            Err(error) => {
                refuse::<()>(Err(error), ErrorCode::LimitExceeded);
                break;
            }
        }
    }
    assert_eq!(held.len(), 4); // input, reservation, output I/O and child reader prepaid
    assert_eq!(execution.run().unwrap().state, State::SUCCEEDED);
    drop(held);
    assert_eq!(output_bytes(&fixture), b"ABC");
    fixture.reopen().integrity_check().unwrap();
}

#[test]
fn registering_authority_mode_without_an_expander_is_refused_before_admission() {
    let mut registry = Applications::default();
    refuse(
        registry.register(
            ApplicationLabel("copy/v1".into()),
            vec![Mode(0), Mode(2)],
            RestartSafety::Pure,
            Arc::new(CopyApplication),
        ),
        ErrorCode::ApplicationUnsupported,
    );
}

struct YieldAfterSeal(AtomicBool);
impl Application for YieldAfterSeal {
    fn expansion(&self) -> Option<&dyn Expansion> {
        Some(self)
    }
    fn execute(&self, context: &mut WorkContext) -> Result<ApplicationOutcome> {
        UppercaseScatter.execute(context)
    }
}
impl Expansion for YieldAfterSeal {
    fn expand(&self, context: &mut ExpansionContext<'_>) -> Result<ExpansionOutcome> {
        context.declare(context.operation(Id(1))?, &[Id(1), Id(2)], true)?;
        if self.0.swap(false, Ordering::SeqCst) {
            return Ok(ExpansionOutcome::Yield);
        }
        UppercaseScatter.expand(context)
    }
}
#[test]
fn a_seal_does_not_mean_expansion_finished_or_child_inputs_were_admitted() {
    let fixture = Fixture::new(Arc::new(YieldAfterSeal(AtomicBool::new(true))));
    fixture.admit(2, 1, 3);
    assert_eq!(fixture.run().unwrap().state, State::ACTIVE);
    fixture.reopen().integrity_check().unwrap();
    assert_eq!(fixture.run().unwrap().state, State::WAITING_CHILDREN);
    execute_children(&fixture, Producer(1));
    fixture.run().unwrap();
    assert_eq!(output_bytes(&fixture), b"ABC");
}

fn admit_external_child(fixture: &Fixture, entity: u64, bytes: &[u8]) {
    let now = Instant::now();
    let header = InputHeader {
        kind: Literal,
        generation: fixture.binding.identity.generation,
        operation: OperationId([entity as u8 + 20; 16]),
        parameters: parameters(Id(1), Id(entity), Producer(0), bytes),
    };
    let InputReception::Receiving(mut receiving) = fixture
        .store
        .receive_input(
            &fixture.binding.identity,
            &header,
            &caps(),
            &fixture.payloads,
            &fixture.executor.applications,
            now,
        )
        .unwrap()
    else {
        panic!("unexpected replay")
    };
    receiving.receive(bytes, now).unwrap();
    let InputPreparation::Ready(prepared) = fixture
        .store
        .prepare_input(
            receiving.finish(now).unwrap(),
            &caps(),
            &fixture.executor.applications,
        )
        .unwrap()
    else {
        panic!("unexpected replay")
    };
    fixture
        .store
        .admit_input(*prepared, &caps(), &fixture.executor.applications)
        .unwrap();
}

#[test]
fn caller_expanded_branch_uses_the_same_reassembly_and_dependency_reads_survive_external_expiry() {
    let fixture = Fixture::setup(
        Arc::new(UppercaseScatter),
        caps(),
        PhysicalLimits::default(),
        1,
        Policy {
            execution_limit_ms: Duration(10000),
            output_retention_ms: Duration(1),
            receipt_retention_ms: Duration(30000),
        },
        payload_policy(),
    );
    fixture.admit(1, 1, 3);
    fixture
        .store
        .declare(
            &fixture.binding.identity,
            OperationId([10; 16]),
            Number(1),
            &[Id(1), Id(2)],
            true,
        )
        .unwrap();
    admit_external_child(&fixture, 1, b"ab");
    admit_external_child(&fixture, 2, b"c");
    execute_children(&fixture, Producer(0));
    fixture.clock.0.store(1002, Ordering::SeqCst);
    super::retention_tests::sweep(&fixture.store, &fixture.payloads);
    for entity in 1..=2 {
        let mut connection = fixture.store.connect().unwrap();
        let tx = connection.transaction().unwrap();
        let (_, child, _, _, _) = load(
            &tx,
            &fixture.binding.identity,
            &WorkKey {
                scope: Number(1),
                producer: Producer(0),
                entity: Id(entity),
            },
        )
        .unwrap();
        assert!(!child.input_live && child.outputs_live);
        assert!(
            !child.release.unwrap().outputs,
            "active parent still needs expired child output"
        );
    }
    fixture.run().unwrap();
    assert_eq!(output_bytes(&fixture), b"ABC");
    super::retention_tests::sweep(&fixture.store, &fixture.payloads);
    assert!(
        fixture.job().outputs_live,
        "parent output has a new independent deadline"
    );
    assert_eq!(fixture.payloads.usage(None).unwrap().objects, 2);
    fixture.clock.0.store(1003, Ordering::SeqCst);
    super::retention_tests::sweep(&fixture.store, &fixture.payloads);
    assert_eq!(fixture.payloads.usage(None).unwrap().objects, 0);
    fixture.store.audit_payloads(&fixture.payloads).unwrap();
    fixture.reopen().integrity_check().unwrap();
}

struct CapturePreparation {
    prepared: Arc<std::sync::Mutex<Option<PreparedInput>>>,
    ready: std::sync::mpsc::SyncSender<()>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}
impl Application for CapturePreparation {
    fn expansion(&self) -> Option<&dyn Expansion> {
        Some(self)
    }
    fn execute(&self, context: &mut WorkContext) -> Result<ApplicationOutcome> {
        UppercaseScatter.execute(context)
    }
}
impl Expansion for CapturePreparation {
    fn expand(&self, context: &mut ExpansionContext<'_>) -> Result<ExpansionOutcome> {
        context.declare(context.operation(Id(1))?, &[Id(1)], true)?;
        let now = Instant::now();
        let InputReception::Receiving(mut receiving) = context.receive_input(
            context.operation(Id(2))?,
            parameters(context.child_scope(), Id(1), Producer(1), b"abc"),
            now,
        )?
        else {
            panic!("unexpected replay")
        };
        receiving.receive(b"abc", now)?;
        let InputPreparation::Ready(prepared) = context.prepare_input(receiving.finish(now)?)?
        else {
            panic!("unexpected replay")
        };
        *self.prepared.lock().unwrap() = Some(*prepared);
        self.ready.send(()).unwrap();
        self.release
            .lock()
            .unwrap()
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap();
        Ok(ExpansionOutcome::Yield)
    }
}

#[test]
fn escaped_preparation_rechecks_parent_lease_attempt_authorization_deadline_and_fences() {
    for action in [
        "retry", "lease", "deadline", "cancel", "scope", "revoke", "auth",
    ] {
        let prepared = Arc::new(std::sync::Mutex::new(None));
        let (ready, started) = std::sync::mpsc::sync_channel(1);
        let (release, released) = std::sync::mpsc::sync_channel(1);
        let fixture = Fixture::new(Arc::new(CapturePreparation {
            prepared: prepared.clone(),
            ready,
            release: std::sync::Mutex::new(released),
        }));
        fixture.admit(2, 1, 3);
        let executor = fixture.executor.clone();
        let identity = fixture.binding.identity.clone();
        let key = fixture.key();
        let worker = std::thread::spawn(move || executor.run(&identity, &key));
        started
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap();
        let code = match action {
            "retry" => {
                fixture
                    .store
                    .retry_work(
                        &fixture.binding.identity,
                        OperationId([40; 16]),
                        &fixture.key(),
                        Id(1),
                    )
                    .unwrap();
                ErrorCode::Conflict
            }
            "lease" => {
                fixture.clock.0.store(1100, Ordering::SeqCst);
                ErrorCode::Conflict
            }
            "deadline" => {
                fixture.clock.0.store(2000, Ordering::SeqCst);
                ErrorCode::DeadlineExceeded
            }
            "cancel" => {
                fixture
                    .store
                    .cancel_work(
                        &fixture.binding.identity,
                        OperationId([40; 16]),
                        &fixture.key(),
                    )
                    .unwrap();
                ErrorCode::Cancelled
            }
            "scope" => {
                fixture
                    .store
                    .cancel_scope(&fixture.binding.identity, OperationId([40; 16]), Number(0))
                    .unwrap();
                ErrorCode::Cancelled
            }
            "revoke" => {
                fixture
                    .store
                    .revoke_session(&fixture.binding.identity)
                    .unwrap();
                ErrorCode::Unauthorized
            }
            _ => {
                fixture.auth.0.store(false, Ordering::SeqCst);
                ErrorCode::Unauthorized
            }
        };
        let prepared = prepared.lock().unwrap().take().unwrap();
        let operation = prepared.input.header.operation;
        refuse(
            fixture
                .store
                .admit_input(prepared, &caps(), &fixture.executor.applications),
            code,
        );
        release.send(()).unwrap();
        refuse(worker.join().unwrap(), code);
        let mut connection = fixture.store.connect().unwrap();
        let tx = connection.transaction().unwrap();
        assert_eq!(
            scopes::work(
                &tx,
                fixture.binding.identity.generation,
                &WorkKey {
                    scope: Number(1),
                    producer: Producer(1),
                    entity: Id(1)
                }
            )
            .unwrap()
            .1
            .state,
            State::DECLARED
        );
        assert!(
            scopes::operation(
                &tx,
                fixture.binding.identity.generation,
                Producer(1),
                operation
            )
            .unwrap()
            .is_none()
        );
        fixture.reopen().integrity_check().unwrap();
    }
}

struct IncompleteRead;
impl Application for IncompleteRead {
    fn expansion(&self) -> Option<&dyn Expansion> {
        Some(&UppercaseScatter)
    }
    fn execute(&self, context: &mut WorkContext) -> Result<ApplicationOutcome> {
        if context.mode() == Mode(0) {
            return UppercaseScatter.execute(context);
        }
        context.begin_child_output(Id(1), OutputIndex(0))?;
        let mut bytes = [0; 1];
        context.read_child_output(&mut bytes)?;
        context.begin_output(Number(1), ApplicationLabel("text/plain".into()))?;
        context.write_output(&bytes)?;
        context.finish_output()?;
        Ok(ApplicationOutcome::Succeeded) // bug: unverified child bytes are not a result
    }
}
#[test]
fn unverified_child_read_cannot_publish_success_even_if_application_ignores_eof() {
    let fixture = Fixture::new(Arc::new(IncompleteRead));
    fixture.admit(2, 1, 3);
    fixture.run().unwrap();
    execute_children(&fixture, Producer(1));
    let view = fixture.run().unwrap();
    assert_eq!(view.state, State::FAILED);
    assert_eq!(
        view.diagnostic.unwrap().code,
        DiagnosticCode(ErrorCode::IntegrityError as u64)
    );
    assert!(view.manifest.is_none());
    fixture.reopen().integrity_check().unwrap();
}

#[test]
fn branches_refuse_permanently_impossible_handle_policies_before_admission() {
    for (mode, handles) in [(1, 3), (2, 4)] {
        let policy = PayloadPolicy {
            handles: Id(handles),
            owner_handles: Id(handles),
            ..payload_policy()
        };
        let fixture = Fixture::setup(
            Arc::new(UppercaseScatter),
            caps(),
            PhysicalLimits::default(),
            1,
            Policy {
                execution_limit_ms: Duration(10000),
                output_retention_ms: Duration(20000),
                receipt_retention_ms: Duration(30000),
            },
            policy,
        );
        let now = Instant::now();
        let header = InputHeader {
            kind: Literal,
            generation: fixture.binding.identity.generation,
            operation: OperationId([2; 16]),
            parameters: AdmitParameters {
                work: fixture.key(),
                mode: Mode(mode),
                ..parameters(Id(1), Id(1), Producer(0), b"abc")
            },
        };
        let InputReception::Receiving(mut receiving) = fixture
            .store
            .receive_input(
                &fixture.binding.identity,
                &header,
                &caps(),
                &fixture.payloads,
                &fixture.executor.applications,
                now,
            )
            .unwrap()
        else {
            panic!("unexpected replay")
        };
        receiving.receive(b"abc", now).unwrap();
        let InputPreparation::Ready(prepared) = fixture
            .store
            .prepare_input(
                receiving.finish(now).unwrap(),
                &caps(),
                &fixture.executor.applications,
            )
            .unwrap()
        else {
            panic!("unexpected replay")
        };
        refuse(
            fixture
                .store
                .admit_input(*prepared, &caps(), &fixture.executor.applications),
            ErrorCode::LimitExceeded,
        );
        assert_eq!(fixture.view().state, State::DECLARED);
        refuse(
            fixture
                .store
                .operation(&fixture.binding.identity, header.operation),
            ErrorCode::NotFound,
        );
    }
}

#[test]
fn expansion_crash_child() {
    let Some(path) = std::env::var_os("PIPESTREAM_EXPANSION_CHILD_DIRECTORY") else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    let store = AuthorityStore::open(
        &path.join("authority.sqlite"),
        IdentityLabel("test-authority".into()),
        super::super::super::tests::policy(),
        PhysicalLimits::default(),
        Arc::new(TestClock(AtomicU64::new(1000))),
        Arc::new(Auth(AtomicBool::new(true))),
    )
    .unwrap();
    let payloads = PayloadStore::open(
        &path.join("objects"),
        store.payload_identity().unwrap(),
        payload_policy(),
    )
    .unwrap();
    let executor = Executor::new(
        store,
        payloads,
        applications(Arc::new(UppercaseScatter)),
        ResultEndpoint::new("results.example:7443".into()).unwrap(),
        caps(),
        Duration(100),
    )
    .unwrap();
    let pool = executor
        .start_workers(PoolConfig {
            workers: 1,
            workers_per_owner: 1,
            scan_batch: 1,
            ..pool_config()
        })
        .unwrap();
    wait_until(|| pool.snapshot().completed > 0 || pool.snapshot().faulted);
    pool.shutdown().unwrap();
    panic!("expansion crash boundary did not fire");
}

#[test]
fn process_death_recovers_partial_expansion_and_lost_local_acknowledgments() {
    for boundary in [
        "declare:before",
        "declare:after",
        "admit-input:before",
        "admit-input:after",
        "worker-expansion:before",
        "worker-expansion:after",
    ] {
        let fixture = Fixture::new(Arc::new(UppercaseScatter));
        fixture.admit(2, 1, 3);
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
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "v2::authority::execution::tests::branch_tests::expansion_crash_child",
                "--nocapture",
            ])
            .env("PIPESTREAM_EXPANSION_CHILD_DIRECTORY", directory.path())
            .env("PIPESTREAM_TEST_AUTHORITY_CRASH", boundary)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(86), "{boundary}");
        clock.0.store(1100, Ordering::SeqCst);
        let store = AuthorityStore::open(
            &directory.path().join("authority.sqlite"),
            store.authority.clone(),
            store.policy.clone(),
            PhysicalLimits::default(),
            clock.clone(),
            auth.clone(),
        )
        .unwrap();
        let payloads = PayloadStore::open(
            &directory.path().join("objects"),
            store.payload_identity().unwrap(),
            payload_policy(),
        )
        .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let executor = Executor::new(
            store.clone(),
            payloads.clone(),
            applications(Arc::new(CountExpansion(calls.clone()))),
            ResultEndpoint::new("results.example:7443".into()).unwrap(),
            caps(),
            Duration(100),
        )
        .unwrap();
        let fixture = Fixture {
            directory,
            store,
            payloads,
            binding,
            clock,
            auth,
            executor,
        };
        assert_eq!(
            fixture.job().expansion_complete,
            boundary == "worker-expansion:after"
        );
        let pool = fixture
            .executor
            .start_workers(PoolConfig {
                workers: 1,
                workers_per_owner: 1,
                scan_batch: 1,
                ..pool_config()
            })
            .unwrap();
        wait_until(|| fixture.view().state.is_terminal() || pool.snapshot().faulted);
        assert!(!pool.shutdown().unwrap().faulted, "{boundary}");
        assert_eq!(fixture.view().state, State::SUCCEEDED, "{boundary}");
        assert_eq!(fixture.view().attempt, Number(1));
        assert_eq!(output_bytes(&fixture), b"ABC");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            usize::from(boundary != "worker-expansion:after")
        );
        let connection = fixture.store.connect().unwrap();
        let jobs: i64 = connection
            .query_row("SELECT count(*) FROM jobs", [], |r| r.get(0))
            .unwrap();
        let operations: i64 = connection
            .query_row(
                "SELECT count(*) FROM operations WHERE originator=1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!((jobs, operations), (3, 3), "{boundary}");
        fixture.reopen().integrity_check().unwrap();
    }
}

struct CountExpansion(Arc<AtomicUsize>);
impl Application for CountExpansion {
    fn expansion(&self) -> Option<&dyn Expansion> {
        Some(self)
    }
    fn execute(&self, context: &mut WorkContext) -> Result<ApplicationOutcome> {
        UppercaseScatter.execute(context)
    }
}
impl Expansion for CountExpansion {
    fn expand(&self, context: &mut ExpansionContext<'_>) -> Result<ExpansionOutcome> {
        self.0.fetch_add(1, Ordering::SeqCst);
        UppercaseScatter.expand(context)
    }
}

struct RetryBranch {
    expansion_failure: bool,
    calls: Arc<AtomicUsize>,
    retried: AtomicBool,
}
impl Application for RetryBranch {
    fn expansion(&self) -> Option<&dyn Expansion> {
        Some(self)
    }
    fn execute(&self, context: &mut WorkContext) -> Result<ApplicationOutcome> {
        if !self.expansion_failure
            && context.mode() != Mode(0)
            && !self.retried.swap(true, Ordering::SeqCst)
        {
            // Make an unpublished partial output; retry must not return it.
            context.begin_output(Number(1), ApplicationLabel("text/plain".into()))?;
            context.write_output(b"x")?;
            context.finish_output()?;
            return Ok(ApplicationOutcome::Retryable(diag(
                ErrorCode::InternalError,
                "retry reassembly",
            )));
        }
        UppercaseScatter.execute(context)
    }
}
impl Expansion for RetryBranch {
    fn expand(&self, context: &mut ExpansionContext<'_>) -> Result<ExpansionOutcome> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let outcome = UppercaseScatter.expand(context)?;
        if self.expansion_failure && !self.retried.swap(true, Ordering::SeqCst) {
            return Ok(ExpansionOutcome::Retryable(diag(
                ErrorCode::InternalError,
                "retry expansion",
            )));
        }
        Ok(outcome)
    }
}

#[test]
fn authorized_retry_preserves_children_and_does_not_repeat_completed_expansion() {
    for expansion_failure in [true, false] {
        let calls = Arc::new(AtomicUsize::new(0));
        let fixture = Fixture::new(Arc::new(RetryBranch {
            expansion_failure,
            calls: calls.clone(),
            retried: AtomicBool::new(false),
        }));
        fixture.admit(2, 1, 3);
        fixture.run().unwrap();
        execute_children(&fixture, Producer(1));
        if !expansion_failure {
            fixture.run().unwrap();
        }
        assert_eq!(fixture.view().state, State::AWAITING_RETRY);
        fixture.reopen().integrity_check().unwrap();
        let original_child = fixture.view().child;
        let child_before: Vec<_> = (1..=2)
            .map(|entity| {
                fixture
                    .store
                    .work_view(
                        &fixture.binding.identity,
                        &WorkKey {
                            scope: Number(1),
                            producer: Producer(1),
                            entity: Id(entity),
                        },
                        Number(0),
                    )
                    .unwrap()
            })
            .collect();
        fixture
            .store
            .retry_work(
                &fixture.binding.identity,
                OperationId([99; 16]),
                &fixture.key(),
                Id(1),
            )
            .unwrap();
        if expansion_failure {
            assert_eq!(fixture.run().unwrap().state, State::WAITING_CHILDREN);
        }
        assert_eq!(fixture.run().unwrap().state, State::SUCCEEDED);
        assert_eq!(output_bytes(&fixture), b"ABC");
        assert_eq!(fixture.view().attempt, Number(2));
        assert_eq!(fixture.view().child, original_child);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            if expansion_failure { 2 } else { 1 }
        );
        for (entity, before) in (1..=2).zip(child_before) {
            assert_eq!(
                fixture
                    .store
                    .work_view(
                        &fixture.binding.identity,
                        &WorkKey {
                            scope: Number(1),
                            producer: Producer(1),
                            entity: Id(entity)
                        },
                        Number(0)
                    )
                    .unwrap(),
                before
            );
        }
        fixture.reopen().integrity_check().unwrap();
    }
}

#[test]
fn open_child_output_does_not_bypass_parent_authorization_lease_deadline_or_cancellation() {
    for cause in ["lease", "deadline", "cancel", "scope", "revoke", "auth"] {
        let fixture = Fixture::new(Arc::new(UppercaseScatter));
        fixture.admit(2, 1, 3);
        fixture.run().unwrap();
        execute_children(&fixture, Producer(1));
        let mut execution = fixture
            .executor
            .claim(&fixture.binding.identity, &fixture.key())
            .unwrap();
        execution
            .context
            .begin_child_output(Id(1), OutputIndex(0))
            .unwrap();
        let expected = match cause {
            "lease" => {
                fixture.clock.0.store(1100, Ordering::SeqCst);
                ErrorCode::Conflict
            }
            "deadline" => {
                fixture.clock.0.store(2000, Ordering::SeqCst);
                ErrorCode::DeadlineExceeded
            }
            "cancel" => {
                fixture
                    .store
                    .cancel_work(
                        &fixture.binding.identity,
                        OperationId([90; 16]),
                        &fixture.key(),
                    )
                    .unwrap();
                // All descendants have already closed, so cancellation can
                // settle this parent immediately rather than leave CANCELLING.
                assert_eq!(fixture.view().state, State::CANCELLED);
                ErrorCode::AlreadyTerminal
            }
            "scope" => {
                fixture
                    .store
                    .cancel_scope(&fixture.binding.identity, OperationId([90; 16]), Number(0))
                    .unwrap();
                ErrorCode::Cancelled
            }
            "revoke" => {
                fixture
                    .store
                    .revoke_session(&fixture.binding.identity)
                    .unwrap();
                ErrorCode::Unauthorized
            }
            "auth" => {
                fixture.auth.0.store(false, Ordering::SeqCst);
                ErrorCode::Unauthorized
            }
            _ => unreachable!(),
        };
        refuse(execution.context.read_child_output(&mut [0; 2]), expected);
        refuse(
            execution.context.publish(ApplicationOutcome::Succeeded),
            expected,
        );
        fixture.auth.0.store(true, Ordering::SeqCst);
        fixture.reopen().integrity_check().unwrap();
    }
}

#[test]
fn live_child_reader_keeps_its_credit_after_worker_ownership_drops() {
    for per_owner in [false, true] {
        let mut policy = payload_policy();
        if per_owner {
            policy.handles = Id(16);
        }
        let fixture = Fixture::setup(
            Arc::new(UppercaseScatter),
            caps(),
            PhysicalLimits::default(),
            1,
            Policy {
                execution_limit_ms: Duration(10000),
                output_retention_ms: Duration(20000),
                receipt_retention_ms: Duration(30000),
            },
            policy,
        );
        fixture.admit(0, 1, 3);
        fixture.run().unwrap();
        let job = fixture.job();
        let output = &fixture.view().manifest.unwrap().outputs[0];
        let descriptor = Input {
            length: output.length,
            sha256: output.sha256,
            content_type: output.content_type.clone(),
        };
        let owner = &fixture.binding.identity.owner;
        let credit = fixture.payloads.reserve_reader(owner).unwrap();
        let reader = credit
            .open_output(&job.reservation_key.0, OutputIndex(0), owner, &descriptor)
            .unwrap();
        refuse(
            credit.open_output(&job.reservation_key.0, OutputIndex(0), owner, &descriptor),
            ErrorCode::LimitExceeded,
        );
        let open = || {
            fixture
                .payloads
                .open_object(&job.input_key.0, owner, &job.parameters.input)
        };
        let mut held = Vec::new();
        loop {
            match open() {
                Ok(reader) => held.push(reader),
                Err(error) => {
                    refuse::<()>(Err(error), ErrorCode::LimitExceeded);
                    break;
                }
            }
        }
        drop(credit);
        refuse(open(), ErrorCode::LimitExceeded);
        drop(reader);
        let reclaimed = open().unwrap();
        refuse(open(), ErrorCode::LimitExceeded);
        drop(reclaimed);
        drop(held);
        fixture.reopen().integrity_check().unwrap();
    }
}

struct GatedBranch {
    expansion: bool,
    ready: std::sync::mpsc::SyncSender<()>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}
impl GatedBranch {
    fn pause(&self) {
        self.ready.send(()).unwrap();
        self.release
            .lock()
            .unwrap()
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap();
    }
}
impl Application for GatedBranch {
    fn expansion(&self) -> Option<&dyn Expansion> {
        Some(self)
    }
    fn execute(&self, context: &mut WorkContext) -> Result<ApplicationOutcome> {
        let outcome = UppercaseScatter.execute(context)?;
        if !self.expansion && context.mode() != Mode(0) {
            self.pause();
        }
        Ok(outcome)
    }
}
impl Expansion for GatedBranch {
    fn expand(&self, context: &mut ExpansionContext<'_>) -> Result<ExpansionOutcome> {
        let outcome = UppercaseScatter.expand(context)?;
        if self.expansion {
            self.pause();
        }
        Ok(outcome)
    }
}

#[test]
fn branch_completion_fits_reserved_wal_without_row_replacement_or_page_growth() {
    for expansion in [true, false] {
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let physical = PhysicalLimits {
            wal_bytes: 4 << 20,
            ..PhysicalLimits::default()
        };
        let fixture = Fixture::configured(
            Arc::new(GatedBranch {
                expansion,
                ready: ready_tx,
                release: std::sync::Mutex::new(release_rx),
            }),
            caps(),
            physical,
        );
        fixture.admit(2, 1, 3);
        if !expansion {
            fixture.run().unwrap();
            execute_children(&fixture, Producer(1));
        }
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| fixture.run());
            ready_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            let mut connection = fixture.store.connect().unwrap();
            let tx = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            records::protect(&tx, 0, 0).unwrap();
            tx.execute_batch("CREATE TABLE branch_fill(body BLOB);
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
                    let tx =
                        connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                    records::protect(&tx, 0, 0)?;
                    tx.execute("INSERT INTO branch_fill VALUES(zeroblob(4096))", [])?;
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
            release_tx.send(()).unwrap();
            let view = worker.join().unwrap().unwrap();
            let after = fixture.store.physical_usage().unwrap();
            assert_eq!(
                view.state,
                if expansion {
                    State::WAITING_CHILDREN
                } else {
                    State::SUCCEEDED
                }
            );
            assert_eq!(
                connection
                    .query_row("PRAGMA page_count", [], |r| r.get::<_, i64>(0))
                    .unwrap(),
                pages
            );
            assert!(after.wal_bytes > before.wal_bytes && after.wal_bytes <= physical.wal_bytes);
            eprintln!(
                "branch expansion={expansion} fill={filled} pages={pages} WAL={} -> {} cap={}",
                before.wal_bytes, after.wal_bytes, physical.wal_bytes
            );
            fixture.store.integrity_check().unwrap();
        });
    }
}

#[test]
fn reopen_refuses_prior_branch_format_and_success_without_completed_expansion() {
    for old_format in [true, false] {
        let fixture = Fixture::new(Arc::new(UppercaseScatter));
        fixture.admit(2, 1, 3);
        fixture.run().unwrap();
        execute_children(&fixture, Producer(1));
        fixture.run().unwrap();
        let mut connection = fixture.store.connect().unwrap();
        if old_format {
            connection.pragma_update(None, "user_version", 7).unwrap();
        } else {
            let tx = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            let (row, mut job, revision, _, _) =
                load(&tx, &fixture.binding.identity, &fixture.key()).unwrap();
            job.expansion_complete = false;
            // Deliberately preserve the fixed record checksum: reopening must
            // check the semantic cross-record invariant as well as its bytes.
            records::replace(&tx, job_target(row), revision, &job, false).unwrap();
            tx.commit().unwrap();
        }
        assert!(matches!(
            AuthorityStore::open(
                &fixture.directory.path().join("authority.sqlite"),
                fixture.store.authority.clone(),
                fixture.store.policy.clone(),
                PhysicalLimits::default(),
                fixture.clock.clone(),
                fixture.auth.clone(),
            ),
            Err(StoreError::Corrupt(_))
        ));
    }
}

struct YieldOnCapacity;
impl Application for YieldOnCapacity {
    fn expansion(&self) -> Option<&dyn Expansion> {
        Some(self)
    }
    fn execute(&self, context: &mut WorkContext) -> Result<ApplicationOutcome> {
        UppercaseScatter.execute(context)
    }
}
impl Expansion for YieldOnCapacity {
    fn expand(&self, context: &mut ExpansionContext<'_>) -> Result<ExpansionOutcome> {
        match UppercaseScatter.expand(context) {
            Err(StoreError::Protocol(Error {
                code: ErrorCode::LimitExceeded,
                ..
            })) => Ok(ExpansionOutcome::Yield),
            other => other,
        }
    }
}

#[test]
fn child_admission_pressure_yields_without_losing_declarations_or_spending_terminal_credits() {
    let fixture = Fixture::new(Arc::new(YieldOnCapacity));
    fixture.admit(2, 1, 3);
    let execution = fixture
        .executor
        .claim(&fixture.binding.identity, &fixture.key())
        .unwrap();
    let job = fixture.job();
    let mut held = Vec::new();
    loop {
        match fixture.payloads.open_object(
            &job.input_key.0,
            &fixture.binding.identity.owner,
            &job.parameters.input,
        ) {
            Ok(reader) => held.push(reader),
            Err(error) => {
                refuse::<()>(Err(error), ErrorCode::LimitExceeded);
                break;
            }
        }
    }
    assert_eq!(held.len(), 5);
    assert_eq!(execution.run().unwrap().state, State::ACTIVE);
    assert!(!fixture.job().expansion_complete);
    assert_eq!(fixture.view().attempt, Number(1));
    let mut connection = fixture.store.connect().unwrap();
    let tx = connection.transaction().unwrap();
    let (row, ..) = load(&tx, &fixture.binding.identity, &fixture.key()).unwrap();
    assert_eq!(
        records::header(&tx, job_target(row)).unwrap().credits,
        jobs::CREDITS
    );
    assert_eq!(
        records::header(&tx, work_target(row)).unwrap().credits,
        jobs::WORK_CREDITS
    );
    assert!(
        scopes::load(&tx, fixture.binding.identity.generation, Number(1))
            .unwrap()
            .seal
            .is_some()
    );
    assert_eq!(
        tx.query_row("SELECT count(*) FROM jobs", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
    drop(tx);
    drop(held);
    assert_eq!(fixture.run().unwrap().state, State::WAITING_CHILDREN);
    execute_children(&fixture, Producer(1));
    fixture.run().unwrap();
    assert_eq!(output_bytes(&fixture), b"ABC");
    fixture.reopen().integrity_check().unwrap();
}

#[test]
fn authority_branch_executes_at_its_minimum_handle_policy() {
    let mut policy = payload_policy();
    policy.handles = Id(5);
    policy.owner_handles = Id(5);
    let fixture = Fixture::setup(
        Arc::new(UppercaseScatter),
        caps(),
        PhysicalLimits::default(),
        1,
        Policy {
            execution_limit_ms: Duration(10000),
            output_retention_ms: Duration(20000),
            receipt_retention_ms: Duration(30000),
        },
        policy,
    );
    fixture.admit(2, 1, 3);
    assert_eq!(fixture.run().unwrap().state, State::WAITING_CHILDREN);
    execute_children(&fixture, Producer(1));
    assert_eq!(fixture.run().unwrap().state, State::SUCCEEDED);
    assert_eq!(output_bytes(&fixture), b"ABC");
}
