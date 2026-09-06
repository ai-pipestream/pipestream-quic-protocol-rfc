use super::*;
use std::sync::{
    Barrier,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

struct TestClock {
    now: AtomicU64,
    trusted: AtomicBool,
}
impl TestClock {
    fn new() -> Self {
        Self {
            now: AtomicU64::new(1_000),
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
struct TestAuthorization {
    allowed: AtomicBool,
}
impl Authorization for TestAuthorization {
    fn permits(&self, owner: &IdentityLabel, _: Permission) -> bool {
        self.allowed.load(Ordering::SeqCst) && matches!(owner.0.as_str(), "alice" | "bob")
    }
}
fn policy() -> StorePolicy {
    StorePolicy {
        owners: Id(2),
        sessions: Id(10),
        sessions_per_owner: Id(5),
        session_limits: Limits {
            scopes: Id(100),
            entities: Id(1000),
            operations: Id(100),
            retained_input_bytes: Number(1 << 20),
            retained_output_bytes: Number(1 << 20),
            active_jobs: Id(100),
        },
    }
}
fn retention() -> Policy {
    Policy {
        execution_limit_ms: Duration(10000),
        output_retention_ms: Duration(20000),
        receipt_retention_ms: Duration(30000),
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
        stream_limit: ConcurrencyLimit(8),
        pending_limit: ConcurrencyLimit(8),
        object_limit: Number(1 << 20),
        stream_idle_ms: IdleMs(1000),
        stream_lifetime_ms: LifetimeMs(10000),
    }
}
fn owner(name: &str) -> IdentityLabel {
    IdentityLabel(name.to_owned())
}
fn op(value: u8) -> OperationId {
    OperationId([value; 16])
}
fn key(entity: u64) -> WorkKey {
    WorkKey {
        scope: Number(0),
        producer: Producer(0),
        entity: Id(entity),
    }
}
fn refuse<T: std::fmt::Debug>(result: Result<T>, code: ErrorCode) {
    match result.unwrap_err() {
        StoreError::Protocol(error) => assert_eq!(error.code, code),
        other => panic!("expected {code:?}, got {other:?}"),
    }
}
struct Fixture {
    directory: tempfile::TempDir,
    store: AuthorityStore,
    clock: Arc<TestClock>,
    authorization: Arc<TestAuthorization>,
}
impl Fixture {
    fn with_policy(policy: StorePolicy) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let clock = Arc::new(TestClock::new());
        let authorization = Arc::new(TestAuthorization {
            allowed: AtomicBool::new(true),
        });
        let store = AuthorityStore::initialize(
            &directory.path().join("authority.sqlite"),
            owner("test-authority"),
            policy,
            PhysicalLimits::default(),
            clock.clone(),
            authorization.clone(),
        )
        .unwrap();
        Self {
            directory,
            store,
            clock,
            authorization,
        }
    }
    fn new() -> Self {
        Self::with_policy(policy())
    }
    fn create(&self) -> Binding {
        self.store
            .create_session(&owner("alice"), Id(1), &retention(), &caps())
            .unwrap()
    }
    fn reopen(&self) -> AuthorityStore {
        AuthorityStore::open(
            &self.directory.path().join("authority.sqlite"),
            owner("test-authority"),
            self.store.policy.clone(),
            PhysicalLimits::default(),
            self.clock.clone(),
            self.authorization.clone(),
        )
        .unwrap()
    }
}

#[test]
fn creation_replays_after_reopen_without_reissuing_identity() {
    let fixture = Fixture::new();
    assert_eq!(fixture.store.next_creation(&owner("alice")).unwrap(), Id(1));
    let binding = fixture.create();
    let reopened = fixture.reopen();
    assert_eq!(
        reopened
            .create_session(&owner("alice"), Id(1), &retention(), &caps())
            .unwrap(),
        binding
    );
    assert_eq!(reopened.next_creation(&owner("alice")).unwrap(), Id(2));
    let bob = reopened
        .create_session(&owner("bob"), Id(1), &retention(), &caps())
        .unwrap();
    assert_eq!(bob.identity.generation.0, binding.identity.generation.0 + 1);
    let second = reopened
        .create_session(&owner("alice"), Id(2), &retention(), &caps())
        .unwrap();
    assert_eq!(second.identity.generation.0, bob.identity.generation.0 + 1);
    reopened.integrity_check().unwrap();
}

#[test]
fn creation_refuses_changed_policy_profile_and_future_sequence() {
    let fixture = Fixture::new();
    let binding = fixture.create();
    let mut changed = retention();
    changed.receipt_retention_ms = Duration(999);
    refuse(
        fixture
            .store
            .create_session(&owner("alice"), Id(1), &changed, &caps()),
        ErrorCode::Conflict,
    );
    refuse(
        fixture
            .store
            .create_session(&owner("alice"), Id(3), &retention(), &caps()),
        ErrorCode::Conflict,
    );
    let mut no_results = caps();
    no_results.supported.pop();
    refuse(
        fixture
            .store
            .create_session(&owner("alice"), Id(1), &retention(), &no_results),
        ErrorCode::ExtensionUnsupported,
    );
    refuse(
        fixture
            .store
            .attach_session(&owner("alice"), &binding.identity, &no_results),
        ErrorCode::ExtensionUnsupported,
    );
    let mut smaller = caps();
    smaller.control_limit = ControlLimit(4096);
    smaller.object_limit = Number(0);
    assert_eq!(
        fixture
            .store
            .attach_session(&owner("alice"), &binding.identity, &smaller)
            .unwrap(),
        binding
    );
    assert_eq!(fixture.store.next_creation(&owner("alice")).unwrap(), Id(2));
}

#[test]
fn authorization_precedes_lookup_and_revocation_denies_replay() {
    let fixture = Fixture::new();
    let binding = fixture.create();
    refuse(
        fixture
            .store
            .attach_session(&owner("bob"), &binding.identity, &caps()),
        ErrorCode::Unauthorized,
    );
    let mut wrong = binding.identity.clone();
    wrong.owner = owner("bob");
    refuse(
        fixture.store.attach_session(&owner("bob"), &wrong, &caps()),
        ErrorCode::Unauthorized,
    );
    fixture.authorization.allowed.store(false, Ordering::SeqCst);
    refuse(
        fixture.store.next_creation(&owner("alice")),
        ErrorCode::Unauthorized,
    );
    refuse(
        fixture
            .store
            .create_session(&owner("alice"), Id(1), &retention(), &caps()),
        ErrorCode::Unauthorized,
    );
    wrong.generation = Id(999);
    refuse(
        fixture.store.operation(&wrong, op(1)),
        ErrorCode::Unauthorized,
    );
    fixture.authorization.allowed.store(true, Ordering::SeqCst);
    fixture
        .store
        .connect()
        .unwrap()
        .execute(
            "UPDATE sessions SET revoked=1 WHERE generation=?1",
            [sql(binding.identity.generation.0).unwrap()],
        )
        .unwrap();
    refuse(
        fixture
            .store
            .create_session(&owner("alice"), Id(1), &retention(), &caps()),
        ErrorCode::Unauthorized,
    );
    refuse(
        fixture
            .store
            .attach_session(&owner("alice"), &binding.identity, &caps()),
        ErrorCode::Unauthorized,
    );
}

#[test]
fn clock_rollback_refuses_mutation_but_preserves_retained_evidence() {
    let fixture = Fixture::new();
    let binding = fixture.create();
    fixture.clock.set(2000);
    let receipt = fixture
        .store
        .declare(&binding.identity, op(1), Number(0), &[Id(5)], false)
        .unwrap();
    fixture.clock.set(1999);
    refuse(
        fixture
            .store
            .declare(&binding.identity, op(2), Number(0), &[Id(6)], false),
        ErrorCode::ClockUnsafe,
    );
    refuse(
        fixture
            .store
            .create_session(&owner("alice"), Id(2), &retention(), &caps()),
        ErrorCode::ClockUnsafe,
    );
    assert_eq!(
        fixture
            .store
            .declare(&binding.identity, op(1), Number(0), &[Id(5)], false)
            .unwrap(),
        receipt
    );
    assert_eq!(
        fixture
            .reopen()
            .operation(&binding.identity, op(1))
            .unwrap(),
        receipt
    );
    fixture.clock.trusted.store(false, Ordering::SeqCst);
    fixture.clock.set(3000);
    refuse(
        fixture
            .store
            .declare(&binding.identity, op(2), Number(0), &[Id(6)], false),
        ErrorCode::ClockUnsafe,
    );
    assert_eq!(
        fixture
            .store
            .work_view(&binding.identity, &key(5), Number(0))
            .unwrap()
            .0,
        Id(1)
    );
    fixture.clock.trusted.store(true, Ordering::SeqCst);
    fixture
        .store
        .declare(&binding.identity, op(2), Number(0), &[Id(6)], false)
        .unwrap();
}

#[test]
fn declaration_replay_pages_and_missing_input_checkpoint_are_durable() {
    let fixture = Fixture::new();
    let binding = fixture.create();
    let receipt = fixture
        .store
        .declare(
            &binding.identity,
            op(1),
            Number(0),
            &[Id(1), Id(5), Id(9)],
            true,
        )
        .unwrap();
    assert_eq!(
        fixture
            .reopen()
            .declare(
                &binding.identity,
                op(1),
                Number(0),
                &[Id(1), Id(5), Id(9)],
                true
            )
            .unwrap(),
        receipt
    );
    let Outcome::Declared {
        seal: Some(seal),
        declared: Number(3),
        ..
    } = receipt.body
    else {
        panic!("wrong receipt");
    };
    assert_eq!(
        seal,
        scope_seal(
            &binding.identity,
            Number(0),
            Producer(0),
            None,
            Number(3),
            [Id(1), Id(5), Id(9)]
        )
        .unwrap()
    );
    assert_eq!(
        fixture
            .store
            .checkpoint(&binding.identity, Number(0), seal)
            .unwrap(),
        None
    );
    refuse(
        fixture
            .store
            .checkpoint(&binding.identity, Number(0), Digest([0; 32])),
        ErrorCode::IntegrityError,
    );
    refuse(
        fixture
            .store
            .declare(&binding.identity, op(2), Number(0), &[Id(10)], false),
        ErrorCode::Conflict,
    );
    let page = fixture
        .store
        .scope_page(
            &binding.identity,
            Id(12),
            Number(0),
            Number(1),
            PageLimit(1),
        )
        .unwrap();
    let Control::Scope(Scope::PageResponse {
        entries,
        more: true,
        declared: Number(3),
        ..
    }) = page
    else {
        panic!("wrong page");
    };
    assert_eq!(
        entries,
        [ScopeEntry {
            entity: Id(5),
            state: State::DECLARED
        }]
    );
    let page = fixture
        .store
        .scope_page(
            &binding.identity,
            Id(13),
            Number(0),
            Number(9),
            PageLimit(1),
        )
        .unwrap();
    assert!(
        matches!(page, Control::Scope(Scope::PageResponse { entries, more: false, .. }) if entries.is_empty())
    );
    let (revision, view) = fixture
        .store
        .work_view(&binding.identity, &key(9), Number(1))
        .unwrap();
    assert_eq!(revision, Id(1));
    assert_eq!(view.attempt, Number(0));
    assert!(view.input.is_none());
    refuse(
        fixture
            .store
            .work_view(&binding.identity, &key(9), Number(2)),
        ErrorCode::Conflict,
    );
    fixture.store.integrity_check().unwrap();
}

#[test]
fn batching_does_not_change_seal_and_empty_seal_closes_immutably() {
    let fixture = Fixture::new();
    let binding = fixture.create();
    let ids: Vec<Id> = (1..=600).map(Id).collect();
    for (i, batch) in ids.chunks(256).enumerate() {
        fixture
            .store
            .declare(&binding.identity, op(i as u8 + 1), Number(0), batch, false)
            .unwrap();
    }
    let receipt = fixture
        .store
        .declare(&binding.identity, op(4), Number(0), &[], true)
        .unwrap();
    let Outcome::Declared {
        seal: Some(seal), ..
    } = receipt.body
    else {
        panic!("no seal");
    };
    assert_eq!(
        seal,
        scope_seal(
            &binding.identity,
            Number(0),
            Producer(0),
            None,
            Number(600),
            ids
        )
        .unwrap()
    );
    let empty = fixture
        .store
        .create_session(&owner("alice"), Id(2), &retention(), &caps())
        .unwrap();
    let receipt = fixture
        .store
        .declare(&empty.identity, op(1), Number(0), &[], true)
        .unwrap();
    let Outcome::Declared {
        seal: Some(seal), ..
    } = receipt.body
    else {
        panic!("no seal");
    };
    let summary = fixture
        .store
        .checkpoint(&empty.identity, Number(0), seal)
        .unwrap()
        .unwrap();
    assert_eq!(summary.counts.total().unwrap(), 0);
    assert_eq!(summary.status_root, empty_status_root());
    fixture.clock.set(9000);
    assert_eq!(
        fixture
            .reopen()
            .checkpoint(&empty.identity, Number(0), seal)
            .unwrap(),
        Some(summary)
    );
}

#[test]
fn operations_are_immutable_and_refusals_do_not_consume_ids_or_capacity() {
    let mut policy = policy();
    policy.session_limits.entities = Id(2);
    policy.session_limits.operations = Id(2);
    let fixture = Fixture::with_policy(policy);
    let binding = fixture.create();
    refuse(
        fixture
            .store
            .declare(&binding.identity, op(1), Number(0), &[Id(2), Id(1)], false),
        ErrorCode::FrameError,
    );
    let first = fixture
        .store
        .declare(&binding.identity, op(1), Number(0), &[Id(2)], false)
        .unwrap();
    refuse(
        fixture
            .store
            .declare(&binding.identity, op(1), Number(0), &[Id(3)], false),
        ErrorCode::Conflict,
    );
    refuse(
        fixture
            .store
            .declare(&binding.identity, op(2), Number(0), &[Id(2)], false),
        ErrorCode::Conflict,
    );
    refuse(
        fixture
            .store
            .declare(&binding.identity, op(2), Number(0), &[Id(3), Id(4)], false),
        ErrorCode::LimitExceeded,
    );
    refuse(
        fixture.store.operation(&binding.identity, op(2)),
        ErrorCode::NotFound,
    );
    fixture
        .store
        .declare(&binding.identity, op(2), Number(0), &[Id(3)], true)
        .unwrap();
    assert_eq!(
        fixture.store.operation(&binding.identity, op(1)).unwrap(),
        first
    );
    assert_eq!(
        fixture
            .store
            .declare(&binding.identity, op(1), Number(0), &[Id(2)], false)
            .unwrap(),
        first
    );
}

#[test]
fn simultaneous_creation_and_declaration_commit_once() {
    let fixture = Fixture::new();
    let barrier = Arc::new(Barrier::new(8));
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let store = fixture.store.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                let binding = store
                    .create_session(&owner("alice"), Id(1), &retention(), &caps())
                    .unwrap();
                let receipt = store
                    .declare(&binding.identity, op(1), Number(0), &[Id(1)], true)
                    .unwrap();
                (binding, receipt)
            })
        })
        .collect();
    let values: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert!(values.windows(2).all(|w| w[0] == w[1]));
    let connection = fixture.store.connect().unwrap();
    let counts: (u64, u64, u64, u64) = connection.query_row("SELECT (SELECT count(*) FROM work),(SELECT count(*) FROM operations),entities,operations FROM sessions", [],
        |r| Ok((number(r, 0)?, number(r, 1)?, number(r, 2)?, number(r, 3)?))).unwrap();
    assert_eq!(counts, (1, 1, 1, 1));
}

#[test]
fn counter_exhaustion_and_retired_creation_never_reuse_identity() {
    let fixture = Fixture::new();
    fixture
        .store
        .connect()
        .unwrap()
        .execute(
            "UPDATE authority SET last_generation=?1",
            [sql(MAX_NUMBER).unwrap()],
        )
        .unwrap();
    refuse(
        fixture
            .store
            .create_session(&owner("alice"), Id(1), &retention(), &caps()),
        ErrorCode::LimitExceeded,
    );
    assert_eq!(fixture.store.next_creation(&owner("alice")).unwrap(), Id(1));
    // Synthetic post-retirement history: no live promises are deleted here.
    fixture
        .store
        .connect()
        .unwrap()
        .execute("INSERT INTO owners VALUES('alice', 7)", [])
        .unwrap();
    refuse(
        fixture
            .store
            .create_session(&owner("alice"), Id(7), &retention(), &caps()),
        ErrorCode::Expired,
    );
    assert_eq!(fixture.store.next_creation(&owner("alice")).unwrap(), Id(8));
    fixture
        .store
        .connect()
        .unwrap()
        .execute(
            "UPDATE owners SET last_creation=?1",
            [sql(MAX_NUMBER).unwrap()],
        )
        .unwrap();
    refuse(
        fixture.store.next_creation(&owner("alice")),
        ErrorCode::LimitExceeded,
    );
}

pub(super) fn crash_boundary(boundary: &str, side: &str) {
    if std::env::var("PIPESTREAM_TEST_AUTHORITY_CRASH")
        .ok()
        .as_deref()
        == Some(&format!("{boundary}:{side}"))
    {
        // No Rust destructors run: SQLite connections and transactions are
        // abandoned like process death, not gracefully rolled back by Drop.
        std::process::exit(86);
    }
}

#[test]
fn open_never_initializes_missing_empty_or_wrong_authority_history() {
    let fixture = Fixture::new();
    let binding = fixture.create();
    let missing = fixture.directory.path().join("missing.sqlite");
    assert!(
        AuthorityStore::open(
            &missing,
            owner("test-authority"),
            policy(),
            PhysicalLimits::default(),
            fixture.clock.clone(),
            fixture.authorization.clone()
        )
        .is_err()
    );
    assert!(!missing.exists());
    std::fs::File::create_new(&missing).unwrap();
    assert!(
        AuthorityStore::open(
            &missing,
            owner("test-authority"),
            policy(),
            PhysicalLimits::default(),
            fixture.clock.clone(),
            fixture.authorization.clone()
        )
        .is_err()
    );
    let path = fixture.directory.path().join("authority.sqlite");
    assert!(
        AuthorityStore::initialize(
            &path,
            owner("test-authority"),
            policy(),
            PhysicalLimits::default(),
            fixture.clock.clone(),
            fixture.authorization.clone()
        )
        .is_err()
    );
    assert!(
        AuthorityStore::open(
            &path,
            owner("different-authority"),
            policy(),
            PhysicalLimits::default(),
            fixture.clock.clone(),
            fixture.authorization.clone()
        )
        .is_err()
    );
    assert_eq!(
        fixture
            .reopen()
            .create_session(&owner("alice"), Id(1), &retention(), &caps())
            .unwrap(),
        binding
    );
}

#[test]
fn sqlite_preserves_large_wire_integers_without_float_rounding() {
    let fixture = Fixture::new();
    fixture
        .store
        .connect()
        .unwrap()
        .execute(
            "UPDATE authority SET last_generation=?1",
            [sql((1 << 53) + 1).unwrap()],
        )
        .unwrap();
    let binding = fixture.create();
    assert_eq!(binding.identity.generation, Id((1 << 53) + 2));
    let entities = [Id((1 << 53) + 1), Id((1 << 53) + 2), Id(MAX_NUMBER)];
    let receipt = fixture
        .store
        .declare(&binding.identity, op(1), Number(0), &entities, true)
        .unwrap();
    assert_eq!(
        fixture
            .reopen()
            .operation(&binding.identity, op(1))
            .unwrap(),
        receipt
    );
    for entity in entities {
        assert_eq!(
            fixture
                .store
                .work_view(&binding.identity, &key(entity.0), Number(0))
                .unwrap()
                .1
                .work
                .entity,
            entity
        );
    }
    refuse(sql(MAX_NUMBER + 1), ErrorCode::LimitExceeded);
}

#[test]
fn operation_and_session_capacity_refuse_new_state_but_not_replay() {
    let mut limits = policy();
    limits.session_limits.operations = Id(1);
    limits.sessions_per_owner = Id(1);
    let fixture = Fixture::with_policy(limits);
    let binding = fixture.create();
    let receipt = fixture
        .store
        .declare(&binding.identity, op(1), Number(0), &[Id(1)], false)
        .unwrap();
    refuse(
        fixture
            .store
            .declare(&binding.identity, op(2), Number(0), &[Id(2)], true),
        ErrorCode::LimitExceeded,
    );
    refuse(
        fixture
            .store
            .create_session(&owner("alice"), Id(2), &retention(), &caps()),
        ErrorCode::LimitExceeded,
    );
    assert_eq!(fixture.store.next_creation(&owner("alice")).unwrap(), Id(2));
    assert_eq!(
        fixture.store.operation(&binding.identity, op(1)).unwrap(),
        receipt
    );
    assert_eq!(fixture.create(), binding);
    refuse(
        fixture
            .store
            .work_view(&binding.identity, &key(2), Number(0)),
        ErrorCode::NotFound,
    );
}

#[test]
fn policy_withdrawal_before_commit_rolls_back_mutation_and_receipt() {
    struct WithdrawOnRecheck(AtomicU64);
    impl Authorization for WithdrawOnRecheck {
        fn permits(&self, _: &IdentityLabel, permission: Permission) -> bool {
            permission != Permission::Declare || self.0.fetch_add(1, Ordering::SeqCst) == 0
        }
    }
    let fixture = Fixture::new();
    let binding = fixture.create();
    let mut withdrawing = fixture.store.clone();
    withdrawing.authorization = Arc::new(WithdrawOnRecheck(AtomicU64::new(0)));
    refuse(
        withdrawing.declare(&binding.identity, op(1), Number(0), &[Id(1)], true),
        ErrorCode::Unauthorized,
    );
    refuse(
        fixture.store.operation(&binding.identity, op(1)),
        ErrorCode::NotFound,
    );
    refuse(
        fixture
            .store
            .work_view(&binding.identity, &key(1), Number(0)),
        ErrorCode::NotFound,
    );
    fixture
        .store
        .declare(&binding.identity, op(1), Number(0), &[Id(1)], true)
        .unwrap();
}

#[test]
fn different_concurrent_parameters_cannot_share_an_operation() {
    let fixture = Fixture::new();
    let binding = fixture.create();
    let barrier = Arc::new(Barrier::new(2));
    let handles: Vec<_> = [1, 2]
        .into_iter()
        .map(|entity| {
            let store = fixture.store.clone();
            let identity = binding.identity.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                store.declare(&identity, op(1), Number(0), &[Id(entity)], false)
            })
        })
        .collect();
    let mut successes = 0;
    let mut conflicts = 0;
    for handle in handles {
        match handle.join().unwrap() {
            Ok(_) => successes += 1,
            Err(StoreError::Protocol(Error {
                code: ErrorCode::Conflict,
                ..
            })) => conflicts += 1,
            other => panic!("unexpected concurrent outcome: {other:?}"),
        }
    }
    assert_eq!((successes, conflicts), (1, 1));
    let page = fixture
        .store
        .scope_page(
            &binding.identity,
            Id(1),
            Number(0),
            Number(0),
            PageLimit(256),
        )
        .unwrap();
    assert!(
        matches!(page, Control::Scope(Scope::PageResponse { declared: Number(1), entries, more: false, .. }) if entries.len() == 1)
    );
}

#[test]
fn physical_exhaustion_rolls_back_whole_batch_and_preserves_replay() {
    let directory = tempfile::tempdir().unwrap();
    let physical = PhysicalLimits {
        database_bytes: 128 << 10,
        wal_bytes: 128 << 10,
        journal_bytes: 128 << 10,
        shared_memory_bytes: 64 << 10,
    };
    let mut limits = policy();
    limits.session_limits.entities = Id(10000);
    limits.session_limits.operations = Id(1000);
    let clock = Arc::new(TestClock::new());
    let authorization = Arc::new(TestAuthorization {
        allowed: AtomicBool::new(true),
    });
    let store = AuthorityStore::initialize(
        &directory.path().join("authority.sqlite"),
        owner("test-authority"),
        limits.clone(),
        physical,
        clock.clone(),
        authorization.clone(),
    )
    .unwrap();
    let binding = store
        .create_session(&owner("alice"), Id(1), &retention(), &caps())
        .unwrap();
    let mut accepted = 0;
    let mut refusal_seen = false;
    for operation in 1..=200u8 {
        let entities: Vec<Id> = ((accepted + 1)..=(accepted + 16)).map(Id).collect();
        match store.declare(
            &binding.identity,
            op(operation),
            Number(0),
            &entities,
            false,
        ) {
            Ok(_) => accepted += 16,
            Err(error) => {
                refuse::<()>(Err(error), ErrorCode::LimitExceeded);
                refusal_seen = true;
                refuse(
                    store.operation(&binding.identity, op(operation)),
                    ErrorCode::NotFound,
                );
                break;
            }
        }
    }
    assert!(refusal_seen && accepted > 0 && accepted < limits.session_limits.entities.0);
    let usage = store.physical_usage().unwrap();
    assert!(
        usage.database_bytes <= physical.database_bytes
            && usage.wal_bytes <= physical.wal_bytes
            && usage.journal_bytes <= physical.journal_bytes
            && usage.shared_memory_bytes <= physical.shared_memory_bytes
    );
    let reopened = AuthorityStore::open(
        &directory.path().join("authority.sqlite"),
        owner("test-authority"),
        limits,
        physical,
        clock,
        authorization,
    )
    .unwrap();
    let count = reopened
        .connect()
        .unwrap()
        .query_row("SELECT count(*) FROM work", [], |r| number(r, 0))
        .unwrap();
    assert_eq!(count, accepted);
    assert_eq!(
        reopened.operation(&binding.identity, op(1)).unwrap(),
        reopened
            .declare(
                &binding.identity,
                op(1),
                Number(0),
                &(1..=16).map(Id).collect::<Vec<_>>(),
                false
            )
            .unwrap()
    );
    reopened.integrity_check().unwrap();
}

#[test]
fn empty_root_closure_refuses_unrepresentable_receipt_retention() {
    let fixture = Fixture::new();
    let binding = fixture.create();
    fixture
        .clock
        .set(MAX_NUMBER - retention().receipt_retention_ms.0 + 1);
    refuse(
        fixture
            .store
            .declare(&binding.identity, op(1), Number(0), &[], true),
        ErrorCode::LimitExceeded,
    );
    refuse(
        fixture.store.operation(&binding.identity, op(1)),
        ErrorCode::NotFound,
    );
    fixture
        .clock
        .set(MAX_NUMBER - retention().receipt_retention_ms.0);
    let receipt = fixture
        .store
        .declare(&binding.identity, op(1), Number(0), &[], true)
        .unwrap();
    let Outcome::Declared {
        seal: Some(seal), ..
    } = receipt.body
    else {
        panic!("missing seal");
    };
    assert_eq!(
        fixture
            .store
            .checkpoint(&binding.identity, Number(0), seal)
            .unwrap()
            .unwrap()
            .closed_at
            .0,
        MAX_NUMBER - retention().receipt_retention_ms.0
    );
}

#[test]
fn authority_crash_child() {
    let Some(path) = std::env::var_os("PIPESTREAM_TEST_AUTHORITY_PATH") else {
        return;
    };
    let store = AuthorityStore::open(
        Path::new(&path),
        owner("test-authority"),
        policy(),
        PhysicalLimits::default(),
        Arc::new(TestClock::new()),
        Arc::new(TestAuthorization {
            allowed: AtomicBool::new(true),
        }),
    )
    .unwrap();
    let binding = store
        .create_session(&owner("alice"), Id(1), &retention(), &caps())
        .unwrap();
    store
        .declare(&binding.identity, op(1), Number(0), &[Id(1)], true)
        .unwrap();
    panic!("crash boundary was not reached");
}

#[test]
fn process_crash_on_each_side_of_creation_and_declaration_commit() {
    for boundary in [
        "create:before",
        "create:after",
        "declare:before",
        "declare:after",
    ] {
        let fixture = Fixture::new();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "v2::authority::tests::authority_crash_child",
                "--nocapture",
            ])
            .env(
                "PIPESTREAM_TEST_AUTHORITY_PATH",
                fixture.directory.path().join("authority.sqlite"),
            )
            .env("PIPESTREAM_TEST_AUTHORITY_CRASH", boundary)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(86), "{boundary}");
        let reopened = fixture.reopen();
        let expected_next = if boundary == "create:before" {
            Id(1)
        } else {
            Id(2)
        };
        assert_eq!(
            reopened.next_creation(&owner("alice")).unwrap(),
            expected_next,
            "{boundary}"
        );
        let binding = reopened
            .create_session(&owner("alice"), Id(1), &retention(), &caps())
            .unwrap();
        assert_eq!(binding.identity.generation, Id(1));
        let before_retry = reopened.operation(&binding.identity, op(1));
        if boundary == "declare:after" {
            assert!(before_retry.is_ok());
        } else {
            refuse(before_retry, ErrorCode::NotFound);
        }
        let receipt = reopened
            .declare(&binding.identity, op(1), Number(0), &[Id(1)], true)
            .unwrap();
        assert_eq!(
            reopened.operation(&binding.identity, op(1)).unwrap(),
            receipt
        );
        reopened.integrity_check().unwrap();
    }
}
