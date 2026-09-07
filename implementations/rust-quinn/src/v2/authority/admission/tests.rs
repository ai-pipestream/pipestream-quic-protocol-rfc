use super::*;
use crate::v2::authority::{ingress::*, payload::*};
use sha2::{Digest as _, Sha256};
use std::time::Instant;

fn caps() -> Capabilities {
    Capabilities {
        response: ResponseFlag(1),
        supported: vec![
            ProfileId(DURABLE_WORK.into()),
            ProfileId(RESULT_DELIVERY.into()),
        ],
        required: vec![],
        control_limit: ControlLimit(8192),
        stream_limit: ConcurrencyLimit(8),
        pending_limit: ConcurrencyLimit(8),
        object_limit: Number(1 << 20),
        stream_idle_ms: IdleMs(1000),
        stream_lifetime_ms: LifetimeMs(10000),
    }
}
fn payload_policy() -> PayloadPolicy {
    PayloadPolicy {
        objects: Id(64),
        bytes: Number(8 << 20),
        owner_objects: Id(32),
        owner_bytes: Number(4 << 20),
        chunk_bytes: Id(65536),
        handles: Id(64),
        owner_handles: Id(32),
    }
}
fn applications() -> Applications {
    let mut apps = Applications::default();
    apps.register(
        ApplicationLabel("copy/v1".into()),
        vec![Mode(0), Mode(1), Mode(2)],
        RestartSafety::Pure,
        super::super::execution::fixture_application(Arc::new(
            super::super::execution::CopyApplication,
        )),
    )
    .unwrap();
    apps
}
fn header(binding: &Binding, entity: u64, mode: u64) -> InputHeader {
    InputHeader {
        kind: Literal,
        generation: binding.identity.generation,
        operation: OperationId([entity as u8 + 10; 16]),
        parameters: AdmitParameters {
            work: WorkKey {
                scope: Number(0),
                producer: Producer(0),
                entity: Id(entity),
            },
            input: Input {
                length: Number(3),
                sha256: Digest(Sha256::digest(b"abc").into()),
                content_type: ApplicationLabel("text/plain".into()),
            },
            application: ApplicationLabel("copy/v1".into()),
            mode: Mode(mode),
            execution_ms: Duration(1000),
            outputs: OutputBudget {
                count: BatchCount(1),
                total_bytes: Number(8),
            },
        },
    }
}
fn prepared(
    store: &AuthorityStore,
    binding: &Binding,
    payloads: &PayloadStore,
    header: &InputHeader,
) -> PreparedInput {
    let now = Instant::now();
    let InputReception::Receiving(mut input) = store
        .receive_input(
            &binding.identity,
            header,
            &caps(),
            payloads,
            &applications(),
            now,
        )
        .unwrap()
    else {
        panic!("unexpected replay")
    };
    input.receive(b"abc", now).unwrap();
    let InputPreparation::Ready(input) = store
        .prepare_input(input.finish(now).unwrap(), &caps(), &applications())
        .unwrap()
    else {
        panic!("unexpected replay")
    };
    *input
}
struct Fixture {
    authority: super::super::tests::Fixture,
    binding: Binding,
    payloads: PayloadStore,
}
impl Fixture {
    fn new(policy: StorePolicy) -> Self {
        Self::with_physical(policy, PhysicalLimits::default())
    }
    fn with_physical(policy: StorePolicy, physical: PhysicalLimits) -> Self {
        Self::with_payload_policy(policy, physical, payload_policy())
    }
    fn with_payload_policy(
        policy: StorePolicy,
        physical: PhysicalLimits,
        payload_policy: PayloadPolicy,
    ) -> Self {
        let authority = super::super::tests::Fixture::with_physical(policy, physical);
        let binding = authority.create();
        authority
            .store
            .declare(
                &binding.identity,
                OperationId([1; 16]),
                Number(0),
                &[Id(1), Id(2)],
                false,
            )
            .unwrap();
        let payloads = PayloadStore::initialize(
            &authority.directory.path().join("objects"),
            authority.store.payload_identity().unwrap(),
            payload_policy,
        )
        .unwrap();
        authority.store.bind_payloads(&payloads).unwrap();
        Self {
            authority,
            binding,
            payloads,
        }
    }
    fn prepare(&self, entity: u64, mode: u64) -> PreparedInput {
        prepared(
            &self.authority.store,
            &self.binding,
            &self.payloads,
            &header(&self.binding, entity, mode),
        )
    }
    fn job(&self, entity: u64) -> jobs::JobRecord {
        let connection = self.authority.store.connect().unwrap();
        let row = connection
            .query_row(
                "SELECT row_id FROM work WHERE generation=?1 AND scope=0 AND entity=?2",
                params![
                    sql(self.binding.identity.generation.0).unwrap(),
                    sql(entity).unwrap()
                ],
                |r| r.get(0),
            )
            .unwrap();
        records::read::<jobs::JobRecord>(
            &connection,
            records::Target {
                table: records::Table::Job,
                row,
            },
        )
        .unwrap()
        .1
    }
    fn unadmitted(&self, entity: u64) {
        let h = header(&self.binding, entity, 0);
        let (revision, view) = self
            .authority
            .store
            .work_view(&self.binding.identity, &h.parameters.work, Number(0))
            .unwrap();
        assert_eq!(
            (revision, view.state, view.attempt),
            (Id(1), State::DECLARED, Number(0))
        );
        refuse(
            self.authority
                .store
                .operation(&self.binding.identity, h.operation),
            ErrorCode::NotFound,
        );
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
fn admission_commits_real_input_job_receipt_and_exactly_one_child_for_each_mode() {
    for mode in 0..=2 {
        let fixture = Fixture::new(super::super::tests::policy());
        let input = fixture.prepare(1, mode);
        let input_key = input.input().payload().key().to_owned();
        let reservation = input.outputs().key().to_owned();
        let receipt = fixture
            .authority
            .store
            .admit_input(input, &caps(), &applications())
            .unwrap();
        let Outcome::Admitted {
            attempt,
            admitted_at,
            deadline,
            child,
            ..
        } = &receipt.body
        else {
            panic!("not admitted")
        };
        assert_eq!(
            (*attempt, *admitted_at, *deadline),
            (Id(1), Number(1000), Number(2000))
        );
        assert_eq!(
            *child,
            (mode != 0).then(|| ChildScope {
                scope: Id(1),
                producer: Producer(mode - 1)
            })
        );
        let job = fixture.job(1);
        assert_eq!(job.input_key.0, input_key);
        assert_eq!(job.reservation_key.0, reservation);
        assert_eq!(job.stage, Number(if mode == 1 { 2 } else { 0 }));
        assert_eq!(
            (job.attempt, job.lease, job.lease_until),
            (Id(1), Number(0), None)
        );
        let (revision, view) = fixture
            .authority
            .store
            .work_view(&fixture.binding.identity, &job.parameters.work, Number(0))
            .unwrap();
        assert_eq!(revision, Id(2));
        assert_eq!(view.child, *child);
        assert_eq!(
            view.state,
            if mode == 1 {
                State::WAITING_CHILDREN
            } else {
                State::ACTIVE
            }
        );
        for _ in 0..2 {
            assert_eq!(
                fixture
                    .authority
                    .store
                    .collect_payload_orphans(&fixture.payloads, None, 256)
                    .unwrap()
                    .removed,
                0
            );
        }
        let mut reader = fixture
            .payloads
            .open_object(
                &input_key,
                &fixture.binding.identity.owner,
                &job.parameters.input,
            )
            .unwrap();
        let mut bytes = [0; 3];
        assert_eq!(reader.read_chunk(&mut bytes).unwrap(), 3);
        assert_eq!(reader.read_chunk(&mut bytes).unwrap(), 0);
        assert!(reader.verified());
        assert_eq!(&bytes, b"abc");
        assert_eq!(
            fixture
                .authority
                .store
                .operation(&fixture.binding.identity, receipt.operation)
                .unwrap(),
            receipt
        );
        fixture.authority.store.integrity_check().unwrap();
    }
}

#[test]
fn admission_refuses_an_immutable_handle_ceiling_that_cannot_ever_run_its_outputs() {
    for (global, owner, outputs) in [(2, 2, true), (8, 2, true), (2, 2, false)] {
        let mut policy = payload_policy();
        policy.handles = Id(global);
        policy.owner_handles = Id(owner);
        let fixture = Fixture::with_payload_policy(
            super::super::tests::policy(),
            PhysicalLimits::default(),
            policy,
        );
        let mut requested = header(&fixture.binding, 1, 0);
        if !outputs {
            requested.parameters.outputs = OutputBudget {
                count: BatchCount(0),
                total_bytes: Number(0),
            };
        }
        let prepared = prepared(
            &fixture.authority.store,
            &fixture.binding,
            &fixture.payloads,
            &requested,
        );
        let result = fixture
            .authority
            .store
            .admit_input(prepared, &caps(), &applications());
        if outputs {
            match result {
                Err(StoreError::Protocol(error)) => {
                    assert_eq!(error.code, ErrorCode::LimitExceeded)
                }
                other => panic!("expected impossible admission refusal, got {other:?}"),
            }
            assert_eq!(
                fixture
                    .authority
                    .store
                    .work_view(
                        &fixture.binding.identity,
                        &requested.parameters.work,
                        Number(0)
                    )
                    .unwrap()
                    .1
                    .state,
                State::DECLARED
            );
            let jobs: i64 = fixture
                .authority
                .store
                .connect()
                .unwrap()
                .query_row("SELECT count(*) FROM jobs", [], |row| row.get(0))
                .unwrap();
            assert_eq!(jobs, 0);
            assert!(
                fixture
                    .authority
                    .store
                    .operation(&fixture.binding.identity, requested.operation)
                    .is_err()
            );
        } else {
            result.unwrap();
            assert_eq!(fixture.job(1).parameters.outputs.count, BatchCount(0));
        }
    }
}

#[test]
fn duplicate_prepared_inputs_serialize_to_one_job_and_immutable_receipt() {
    let fixture = Fixture::new(super::super::tests::policy());
    let first = fixture.prepare(1, 2);
    let second = fixture.prepare(1, 2);
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let handles: Vec<_> = [first, second]
        .into_iter()
        .map(|input| {
            let store = fixture.authority.store.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                store.admit_input(input, &caps(), &applications()).unwrap()
            })
        })
        .collect();
    let receipts: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(receipts[0], receipts[1]);
    let connection = fixture.authority.store.connect().unwrap();
    let counts: (i64, i64, i64) = connection.query_row("SELECT (SELECT count(*) FROM jobs),(SELECT count(*) FROM scopes),(SELECT count(*) FROM payload_refs)", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap();
    assert_eq!(counts, (1, 2, 2));
    let mut changed = header(&fixture.binding, 1, 2);
    changed.parameters.execution_ms = Duration(999);
    refuse(
        fixture.authority.store.receive_input(
            &fixture.binding.identity,
            &changed,
            &caps(),
            &fixture.payloads,
            &applications(),
            Instant::now(),
        ),
        ErrorCode::Conflict,
    );
    fixture.authority.store.integrity_check().unwrap();
}

#[test]
fn aggregate_limits_refuse_without_accepting_the_second_job() {
    for quota in [
        "global",
        "owner",
        "session",
        "inputs",
        "outputs",
        "operations",
        "scopes",
    ] {
        let mut policy = super::super::tests::policy();
        match quota {
            "global" => {
                policy.active_jobs = Id(1);
                policy.active_jobs_per_owner = Id(1);
            }
            "owner" => policy.active_jobs_per_owner = Id(1),
            "session" => policy.session_limits.active_jobs = Id(1),
            "inputs" => policy.session_limits.retained_input_bytes = Number(5),
            "outputs" => policy.session_limits.retained_output_bytes = Number(15),
            "operations" => policy.session_limits.operations = Id(2),
            "scopes" => policy.session_limits.scopes = Id(2),
            _ => unreachable!(),
        }
        let fixture = Fixture::new(policy);
        let first = fixture.prepare(1, 1);
        let second = fixture.prepare(2, 1);
        fixture
            .authority
            .store
            .admit_input(first, &caps(), &applications())
            .unwrap();
        refuse(
            fixture
                .authority
                .store
                .admit_input(second, &caps(), &applications()),
            ErrorCode::LimitExceeded,
        );
        fixture.unadmitted(2);
        assert_eq!(
            fixture
                .authority
                .store
                .connect()
                .unwrap()
                .query_row("SELECT count(*) FROM jobs", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1,
            "{quota}"
        );
        fixture.authority.store.integrity_check().unwrap();
    }
}

#[test]
fn committed_reservation_pins_installed_outputs_without_a_process_handle() {
    let fixture = Fixture::new(super::super::tests::policy());
    let input = fixture.prepare(1, 0);
    // Storage only: these bytes are not a published result or callback outcome.
    let now = Instant::now();
    let mut output = input
        .outputs()
        .stage(
            OutputIndex(0),
            Number(3),
            ApplicationLabel("text/plain".into()),
            &caps(),
            now,
        )
        .unwrap();
    output.write(b"xyz", now).unwrap();
    let installed = output.finish(now).unwrap();
    let key = installed.key().to_owned();
    let descriptor = installed.descriptor().clone();
    drop(installed);
    fixture
        .authority
        .store
        .admit_input(input, &caps(), &applications())
        .unwrap();
    assert_eq!(
        fixture
            .authority
            .store
            .collect_payload_orphans(&fixture.payloads, None, 256)
            .unwrap()
            .removed,
        0
    );
    let mut read = fixture
        .payloads
        .open_object(&key, &fixture.binding.identity.owner, &descriptor)
        .unwrap();
    let mut bytes = [0; 3];
    assert_eq!(read.read_chunk(&mut bytes).unwrap(), 3);
    assert_eq!(&bytes, b"xyz");
}

#[test]
fn global_and_owner_executor_limits_span_session_boundaries() {
    for same_owner in [true, false] {
        let mut policy = super::super::tests::policy();
        policy.active_jobs_per_owner = Id(1);
        if !same_owner {
            policy.active_jobs = Id(1);
        }
        let fixture = Fixture::new(policy);
        let other = fixture
            .authority
            .store
            .create_session(
                &IdentityLabel(if same_owner { "alice" } else { "bob" }.into()),
                Id(if same_owner { 2 } else { 1 }),
                &fixture.binding.policy,
                &caps(),
            )
            .unwrap();
        fixture
            .authority
            .store
            .declare(
                &other.identity,
                OperationId([1; 16]),
                Number(0),
                &[Id(1)],
                false,
            )
            .unwrap();
        let first = fixture.prepare(1, 0);
        let second = prepared(
            &fixture.authority.store,
            &other,
            &fixture.payloads,
            &header(&other, 1, 0),
        );
        fixture
            .authority
            .store
            .admit_input(first, &caps(), &applications())
            .unwrap();
        refuse(
            fixture
                .authority
                .store
                .admit_input(second, &caps(), &applications()),
            ErrorCode::LimitExceeded,
        );
        let view = fixture
            .authority
            .store
            .work_view(
                &other.identity,
                &header(&other, 1, 0).parameters.work,
                Number(0),
            )
            .unwrap()
            .1;
        assert_eq!(view.state, State::DECLARED);
        fixture.authority.store.integrity_check().unwrap();
    }
}

struct FixedClock {
    now: u64,
    trusted: bool,
}
impl Clock for FixedClock {
    fn read(&self) -> ClockReading {
        ClockReading {
            utc_ms: Number(self.now),
            trusted: self.trusted,
        }
    }
}
struct Allow;
impl Authorization for Allow {
    fn permits(&self, owner: &IdentityLabel, _: Permission) -> bool {
        owner.0 == "alice"
    }
}

#[test]
fn late_authorization_denial_rolls_back_job_child_receipt_and_funding() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Revoke(AtomicUsize);
    impl Authorization for Revoke {
        fn permits(&self, _: &IdentityLabel, permission: Permission) -> bool {
            permission != Permission::Admit || self.0.fetch_add(1, Ordering::SeqCst) < 2
        }
    }
    let mut fixture = Fixture::new(super::super::tests::policy());
    let input = fixture.prepare(1, 2);
    let before = fixture.authority.store.physical_usage().unwrap();
    fixture.authority.store.authorization = Arc::new(Revoke(AtomicUsize::new(0)));
    refuse(
        fixture
            .authority
            .store
            .admit_input(input, &caps(), &applications()),
        ErrorCode::Unauthorized,
    );
    fixture.unadmitted(1);
    let connection = fixture.authority.store.connect().unwrap();
    let counts: (i64,i64,i64,i64) = connection.query_row("SELECT (SELECT count(*) FROM jobs),(SELECT count(*) FROM scopes),(SELECT count(*) FROM payload_refs),(SELECT last_scope FROM sessions)", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).unwrap();
    assert_eq!(counts, (0, 1, 0, 0));
    let row = connection
        .query_row("SELECT row_id FROM work WHERE entity=1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        records::header(
            &connection,
            records::Target {
                table: records::Table::Work,
                row
            }
        )
        .unwrap()
        .credits,
        records::WORK_CREDITS
    );
    assert!(
        fixture
            .authority
            .store
            .physical_usage()
            .unwrap()
            .database_bytes
            >= before.database_bytes
    );
    for _ in 0..2 {
        fixture
            .authority
            .store
            .collect_payload_orphans(&fixture.payloads, None, 256)
            .unwrap();
    }
    assert_eq!(fixture.payloads.usage(None).unwrap().objects, 0);
    fixture.authority.store.integrity_check().unwrap();
}

#[test]
fn admission_revalidates_prepared_evidence_fences_clock_and_limits() {
    for case in [
        "scope",
        "revoked",
        "application",
        "object",
        "clock",
        "regression",
        "overflow",
        "child-counter",
    ] {
        let mut fixture = Fixture::new(super::super::tests::policy());
        let input = fixture.prepare(1, 2);
        let mut selected = caps();
        let mut apps = applications();
        let error = match case {
            "scope" | "revoked" => {
                super::super::tests::set_scope_fence(
                    &fixture.authority.store,
                    fixture.binding.identity.generation,
                    case == "revoked",
                );
                if case == "revoked" {
                    ErrorCode::Unauthorized
                } else {
                    ErrorCode::Cancelled
                }
            }
            "application" => {
                apps = Applications::default();
                ErrorCode::ApplicationUnsupported
            }
            "object" => {
                selected.object_limit = Number(2);
                ErrorCode::LimitExceeded
            }
            "clock" | "regression" => {
                fixture.authority.store.clock = Arc::new(FixedClock {
                    now: if case == "regression" { 999 } else { 1000 },
                    trusted: case != "clock",
                });
                ErrorCode::ClockUnsafe
            }
            "overflow" => {
                fixture.authority.store.clock = Arc::new(FixedClock {
                    now: MAX_NUMBER - 1500,
                    trusted: true,
                });
                ErrorCode::LimitExceeded
            }
            _ => {
                fixture
                    .authority
                    .store
                    .connect()
                    .unwrap()
                    .execute(
                        "UPDATE sessions SET last_scope=?1",
                        [sql(MAX_NUMBER).unwrap()],
                    )
                    .unwrap();
                ErrorCode::LimitExceeded
            }
        };
        refuse(
            fixture.authority.store.admit_input(input, &selected, &apps),
            error,
        );
        assert_eq!(
            fixture
                .authority
                .store
                .connect()
                .unwrap()
                .query_row("SELECT count(*) FROM jobs", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0,
            "{case}"
        );
        if case != "revoked" {
            fixture.unadmitted(1);
        }
    }
}

#[test]
fn admission_crash_child() {
    let Some(directory) = std::env::var_os("PIPESTREAM_ADMISSION_CHILD_DIRECTORY") else {
        return;
    };
    let path = std::path::PathBuf::from(directory);
    let store = AuthorityStore::open(
        &path.join("authority.sqlite"),
        IdentityLabel("test-authority".into()),
        super::super::tests::policy(),
        PhysicalLimits::default(),
        Arc::new(FixedClock {
            now: 1000,
            trusted: true,
        }),
        Arc::new(Allow),
    )
    .unwrap();
    let identity = SessionIdentity {
        authority: IdentityLabel("test-authority".into()),
        owner: IdentityLabel("alice".into()),
        generation: Id(1),
    };
    let binding = store
        .attach_session(&identity.owner, &identity, &caps())
        .unwrap();
    let payloads = PayloadStore::open(
        &path.join("objects"),
        store.payload_identity().unwrap(),
        payload_policy(),
    )
    .unwrap();
    let input = prepared(&store, &binding, &payloads, &header(&binding, 1, 2));
    store.admit_input(input, &caps(), &applications()).unwrap();
    panic!("admission crash boundary did not fire");
}

#[test]
fn process_death_before_or_after_admission_has_no_partial_job_or_lost_receipt() {
    for boundary in ["admit-input:before", "admit-input:after"] {
        let Fixture {
            authority,
            binding,
            payloads,
        } = Fixture::new(super::super::tests::policy());
        drop(payloads); // release the parent's exclusive payload-root ownership
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "v2::authority::admission::tests::admission_crash_child",
                "--nocapture",
            ])
            .env(
                "PIPESTREAM_ADMISSION_CHILD_DIRECTORY",
                authority.directory.path(),
            )
            .env("PIPESTREAM_TEST_AUTHORITY_CRASH", boundary)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(86));
        let store = AuthorityStore::open(
            &authority.directory.path().join("authority.sqlite"),
            authority.store.authority.clone(),
            authority.store.policy.clone(),
            PhysicalLimits::default(),
            authority.store.clock.clone(),
            authority.store.authorization.clone(),
        )
        .unwrap();
        let payloads = PayloadStore::open(
            &authority.directory.path().join("objects"),
            store.payload_identity().unwrap(),
            payload_policy(),
        )
        .unwrap();
        let h = header(&binding, 1, 2);
        let (revision, view) = store
            .work_view(&binding.identity, &h.parameters.work, Number(0))
            .unwrap();
        if boundary.ends_with("before") {
            assert_eq!(
                (revision, view.state, view.attempt),
                (Id(1), State::DECLARED, Number(0))
            );
            refuse(
                store.operation(&binding.identity, h.operation),
                ErrorCode::NotFound,
            );
            for _ in 0..2 {
                store.collect_payload_orphans(&payloads, None, 256).unwrap();
            }
            assert_eq!(payloads.usage(None).unwrap().objects, 0);
            let input = prepared(&store, &binding, &payloads, &h);
            store.admit_input(input, &caps(), &applications()).unwrap();
        } else {
            assert_eq!(
                (revision, view.state, view.attempt),
                (Id(2), State::ACTIVE, Number(1))
            );
            let original = store.operation(&binding.identity, h.operation).unwrap();
            let InputReception::Replay(replay) = store
                .receive_input(
                    &binding.identity,
                    &h,
                    &caps(),
                    &payloads,
                    &applications(),
                    Instant::now(),
                )
                .unwrap()
            else {
                panic!("lost ack did not replay")
            };
            assert_eq!(replay, original);
        }
        assert_eq!(
            store
                .collect_payload_orphans(&payloads, None, 256)
                .unwrap()
                .removed,
            0
        );
        let connection = store.connect().unwrap();
        let row = connection
            .query_row("SELECT work_row FROM jobs", [], |r| r.get(0))
            .unwrap();
        let (_, job): (_, jobs::JobRecord) = records::read(
            &connection,
            records::Target {
                table: records::Table::Job,
                row,
            },
        )
        .unwrap();
        assert_eq!(
            (job.attempt, job.lease, job.stage),
            (Id(1), Number(0), Number(0))
        );
        let mut read = payloads
            .open_object(
                &job.input_key.0,
                &binding.identity.owner,
                &job.parameters.input,
            )
            .unwrap();
        let mut bytes = [0; 3];
        assert_eq!(read.read_chunk(&mut bytes).unwrap(), 3);
        assert_eq!(read.read_chunk(&mut bytes).unwrap(), 0);
        assert!(read.verified());
        assert_eq!(&bytes, b"abc");
        let reservation = payloads
            .open_reservation(
                &job.reservation_key.0,
                &binding.identity.owner,
                &job.parameters.outputs,
            )
            .unwrap();
        assert_eq!(reservation.budget(), &h.parameters.outputs);
        store.integrity_check().unwrap();
    }
}

#[test]
fn pinned_journal_exhaustion_preserves_declared_work_and_existing_receipts() {
    let physical = PhysicalLimits {
        database_bytes: 128 << 10,
        wal_bytes: 4 << 20,
        journal_bytes: 128 << 10,
        shared_memory_bytes: 64 << 10,
    };
    let fixture = Fixture::with_physical(super::super::tests::policy(), physical);
    let input = fixture.prepare(1, 2);
    // Fill unrelated test metadata through the same guarded writer. Do not
    // lower or bypass the completion reservations already owed to declarations.
    let mut connection = fixture.authority.store.connect().unwrap();
    let tx = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    records::protect(&tx, 0, 0).unwrap();
    tx.execute_batch("CREATE TABLE admission_test_fill(body BLOB NOT NULL)")
        .unwrap();
    tx.commit().unwrap();
    let mut reader = fixture.authority.store.connect().unwrap();
    let snapshot = reader.transaction().unwrap();
    snapshot
        .query_row("SELECT count(*) FROM work", [], |r| r.get::<_, i64>(0))
        .unwrap();
    let mut filled = 0;
    loop {
        let result = (|| -> Result<()> {
            let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            records::protect(&tx, 0, 0)?;
            tx.execute("INSERT INTO admission_test_fill VALUES(zeroblob(4096))", [])?;
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
    refuse(
        fixture
            .authority
            .store
            .admit_input(input, &caps(), &applications()),
        ErrorCode::LimitExceeded,
    );
    fixture.unadmitted(1);
    let receipt = fixture
        .authority
        .store
        .operation(&fixture.binding.identity, OperationId([1; 16]))
        .unwrap();
    assert!(matches!(
        receipt.body,
        Outcome::Declared {
            declared: Number(2),
            ..
        }
    ));
    let counts: (i64, i64, i64) = connection.query_row("SELECT (SELECT count(*) FROM jobs),(SELECT count(*) FROM scopes),(SELECT count(*) FROM payload_refs)", [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap();
    assert_eq!(counts, (0, 1, 0));
    let usage = fixture.authority.store.physical_usage().unwrap();
    assert!(
        usage.database_bytes <= physical.database_bytes && usage.wal_bytes <= physical.wal_bytes
    );
    eprintln!(
        "admission refusal: filled={filled} DB={} WAL={} caps={}/{}",
        usage.database_bytes, usage.wal_bytes, physical.database_bytes, physical.wal_bytes
    );
    fixture.authority.store.integrity_check().unwrap();
}

#[test]
fn restart_refuses_missing_or_changed_job_admission_evidence() {
    for corruption in ["job", "input", "receipt", "parameters", "stage"] {
        let fixture = Fixture::new(super::super::tests::policy());
        fixture
            .authority
            .store
            .admit_input(fixture.prepare(1, 0), &caps(), &applications())
            .unwrap();
        let mut connection = fixture.authority.store.connect().unwrap();
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        records::protect(&tx, 0, 0).unwrap();
        match corruption {
            "job" => {
                tx.execute("DELETE FROM jobs", []).unwrap();
            }
            "input" => {
                tx.execute("DELETE FROM payload_refs WHERE purpose=0", [])
                    .unwrap();
            }
            "receipt" => {
                tx.execute(
                    "DELETE FROM operations WHERE operation=?1",
                    [header(&fixture.binding, 1, 0).operation.0.as_slice()],
                )
                .unwrap();
            }
            _ => {
                let row = tx
                    .query_row("SELECT work_row FROM jobs", [], |r| r.get(0))
                    .unwrap();
                let target = records::Target {
                    table: records::Table::Job,
                    row,
                };
                let (retained, mut job): (_, jobs::JobRecord) = records::read(&tx, target).unwrap();
                if corruption == "parameters" {
                    job.parameters.application = ApplicationLabel("changed/v1".into());
                } else {
                    job.stage = Number(2);
                }
                records::replace(&tx, target, retained.revision, &job, false).unwrap();
            }
        }
        tx.commit().unwrap();
        assert!(
            matches!(
                fixture.authority.store.integrity_check(),
                Err(StoreError::Corrupt(_))
            ),
            "{corruption}"
        );
        assert!(
            matches!(
                AuthorityStore::open(
                    &fixture.authority.directory.path().join("authority.sqlite"),
                    fixture.authority.store.authority.clone(),
                    fixture.authority.store.policy.clone(),
                    PhysicalLimits::default(),
                    fixture.authority.store.clock.clone(),
                    fixture.authority.store.authorization.clone()
                ),
                Err(StoreError::Corrupt(_))
            ),
            "{corruption}"
        );
    }
}

#[test]
fn admission_retains_response_limits_and_exact_large_timestamps_on_reconnection() {
    let mut fixture = Fixture::new(super::super::tests::policy());
    let now = (1u64 << 53) + 17;
    fixture.authority.store.clock = Arc::new(FixedClock { now, trusted: true });
    let mut h = header(&fixture.binding, 1, 0);
    h.parameters.outputs = OutputBudget {
        count: BatchCount(2),
        total_bytes: Number(100),
    };
    let input = prepared(
        &fixture.authority.store,
        &fixture.binding,
        &fixture.payloads,
        &h,
    );
    let duplicate = prepared(
        &fixture.authority.store,
        &fixture.binding,
        &fixture.payloads,
        &h,
    );
    let receipt = fixture
        .authority
        .store
        .admit_input(input, &caps(), &applications())
        .unwrap();
    assert!(
        matches!(receipt.body, Outcome::Admitted { admitted_at: Number(n), deadline: Number(d), .. } if n == now && d == now + 1000)
    );
    for limit in ["object", "control"] {
        let mut low = caps();
        if limit == "object" {
            low.object_limit = Number(99);
        } else {
            low.control_limit = ControlLimit(4096);
        }
        refuse(
            fixture.authority.store.attach_session(
                &fixture.binding.identity.owner,
                &fixture.binding.identity,
                &low,
            ),
            ErrorCode::LimitExceeded,
        );
    }
    fixture.authority.store.clock = Arc::new(FixedClock {
        now: 1000,
        trusted: false,
    });
    assert_eq!(
        fixture
            .authority
            .store
            .admit_input(duplicate, &caps(), &applications())
            .unwrap(),
        receipt
    );
    fixture.authority.store.integrity_check().unwrap();
}

#[test]
fn maximum_job_representation_fits_the_preallocated_record() {
    let fixture = Fixture::new(super::super::tests::policy());
    fixture
        .authority
        .store
        .admit_input(fixture.prepare(1, 0), &caps(), &applications())
        .unwrap();
    let mut job = fixture.job(1);
    job.parameters.work = WorkKey {
        scope: Number(MAX_NUMBER),
        producer: Producer(1),
        entity: Id(MAX_NUMBER),
    };
    job.parameters.input.length = Number(MAX_NUMBER);
    job.parameters.input.content_type = ApplicationLabel("i".repeat(128));
    job.parameters.application = ApplicationLabel("a".repeat(128));
    job.parameters.execution_ms = Duration(MAX_DURATION);
    job.parameters.outputs = OutputBudget {
        count: BatchCount(256),
        total_bytes: Number(MAX_NUMBER),
    };
    job.attempt = Id(MAX_NUMBER);
    job.lease = Number(MAX_NUMBER);
    job.lease_until = Some(Number(MAX_NUMBER));
    job.stage = Number(1);
    job.object_limit = Number(MAX_NUMBER);
    let encoded = pack(&job).unwrap();
    assert!(encoded.len() < jobs::CAPACITY);
    assert_eq!(unpack::<jobs::JobRecord>(&encoded).unwrap(), job);
}
