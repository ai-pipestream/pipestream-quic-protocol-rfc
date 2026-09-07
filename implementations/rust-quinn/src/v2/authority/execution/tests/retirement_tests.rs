use super::{result_tests::*, retention_tests::sweep, *};

fn close(fixture: &Fixture) {
    let mut cursor = ReconcileCursor::default();
    for _ in 0..80 {
        fixture.store.reconcile(&mut cursor, 1).unwrap();
    }
    let mut connection = fixture.store.connect().unwrap();
    let tx = connection.transaction().unwrap();
    assert!(
        scopes::closed(&tx, fixture.binding.identity.generation, Number(0))
            .unwrap()
            .is_some()
    );
}
fn count(store: &AuthorityStore, table: &str) -> u64 {
    assert!(
        [
            "sessions",
            "retirements",
            "jobs",
            "work",
            "scopes",
            "operations",
            "payload_refs"
        ]
        .contains(&table)
    );
    store
        .connect()
        .unwrap()
        .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| {
            number(r, 0)
        })
        .unwrap()
}
fn ready() -> Fixture {
    let fixture = published();
    close(&fixture);
    fixture.clock.0.store(31000, Ordering::SeqCst);
    sweep(&fixture.store, &fixture.payloads);
    fixture
}
fn finish(store: &AuthorityStore, payloads: &PayloadStore) {
    let mut cursor = RetirementCursor::default();
    for _ in 0..256 {
        let report = store.retire(payloads, &mut cursor, 1).unwrap();
        assert!(report.deleted_work + report.deleted_scopes + report.deleted_operations <= 1);
        store.integrity_check().unwrap();
        store.audit_payloads(payloads).unwrap();
        if count(store, "sessions") == 0 {
            return;
        }
        assert_eq!(count(store, "retirements"), 1);
        assert!(
            count(store, "scopes") >= 1,
            "closed root must survive partial deletion"
        );
        // Other local maintenance can run between retirement batches.
        store.reconcile(&mut ReconcileCursor::default(), 1).unwrap();
        store
            .reclaim(payloads, &mut RetentionCursor::default(), 1)
            .unwrap();
    }
    panic!("session retirement did not complete");
}

#[test]
fn retirement_fences_access_before_bounded_deletion_and_preserves_nonreuse_history() {
    let fixture = ready();
    let report = fixture
        .store
        .retire(&fixture.payloads, &mut RetirementCursor::default(), 1)
        .unwrap();
    assert!(report.started && !report.completed);
    assert_eq!(count(&fixture.store, "work"), 1);
    assert_eq!(count(&fixture.store, "sessions"), 1);
    for result in [
        fixture.store.create_session(
            &fixture.binding.identity.owner,
            Id(1),
            &fixture.binding.policy,
            &caps(),
        ),
        fixture.store.attach_session(
            &fixture.binding.identity.owner,
            &fixture.binding.identity,
            &caps(),
        ),
    ] {
        refuse(result, ErrorCode::Expired);
    }
    refuse(
        fixture
            .store
            .work_view(&fixture.binding.identity, &fixture.key(), Number(0)),
        ErrorCode::Expired,
    );
    refuse(
        fixture
            .store
            .operation(&fixture.binding.identity, OperationId([2; 16])),
        ErrorCode::Expired,
    );
    refuse(
        service(&fixture).manifest(&fixture.binding.identity, &fixture.key(), Id(1), &caps()),
        ErrorCode::Expired,
    );
    refuse(fixture.run(), ErrorCode::Expired);
    let mut wrong = fixture.binding.identity.clone();
    wrong.owner = IdentityLabel("bob".into());
    refuse(
        fixture.store.attach_session(&wrong.owner, &wrong, &caps()),
        ErrorCode::Unauthorized,
    );
    fixture.auth.0.store(false, Ordering::SeqCst);
    refuse(
        fixture.store.attach_session(
            &fixture.binding.identity.owner,
            &fixture.binding.identity,
            &caps(),
        ),
        ErrorCode::Unauthorized,
    );
    finish(&fixture.store, &fixture.payloads); // Local cleanup is still allowed.
    fixture.auth.0.store(true, Ordering::SeqCst);
    for table in [
        "sessions",
        "retirements",
        "jobs",
        "work",
        "scopes",
        "operations",
        "payload_refs",
    ] {
        assert_eq!(count(&fixture.store, table), 0, "{table}");
    }
    let store = fixture.reopen();
    assert_eq!(
        store
            .next_creation(&fixture.binding.identity.owner)
            .unwrap(),
        Id(2)
    );
    refuse(
        store.create_session(
            &fixture.binding.identity.owner,
            Id(1),
            &fixture.binding.policy,
            &caps(),
        ),
        ErrorCode::Expired,
    );
    let next = store
        .create_session(
            &fixture.binding.identity.owner,
            Id(2),
            &fixture.binding.policy,
            &caps(),
        )
        .unwrap();
    assert_eq!(next.identity.generation, Id(2));
    assert_ne!(next.identity, fixture.binding.identity);
    refuse(
        store.attach_session(
            &fixture.binding.identity.owner,
            &fixture.binding.identity,
            &caps(),
        ),
        ErrorCode::NotFound,
    );
}

#[test]
fn retirement_requires_closed_root_and_the_full_creation_receipt_interval() {
    let fixture = published();
    fixture.clock.0.store(50000, Ordering::SeqCst);
    sweep(&fixture.store, &fixture.payloads);
    assert!(
        !fixture
            .store
            .retire(&fixture.payloads, &mut RetirementCursor::default(), 1)
            .unwrap()
            .started
    );
    assert_eq!(count(&fixture.store, "retirements"), 0);
    close(&fixture); // Closure at 50000, not at the earlier terminal commit.
    fixture.clock.0.store(79999, Ordering::SeqCst);
    assert!(
        !fixture
            .store
            .retire(&fixture.payloads, &mut RetirementCursor::default(), 1)
            .unwrap()
            .started
    );
    fixture.clock.0.store(80000, Ordering::SeqCst);
    assert!(
        fixture
            .store
            .retire(&fixture.payloads, &mut RetirementCursor::default(), 1)
            .unwrap()
            .started
    );
    finish(&fixture.store, &fixture.payloads);
}

#[test]
fn longer_output_and_live_read_promises_prevent_retirement_after_receipt_expiry() {
    let fixture = Fixture::setup(
        Arc::new(CopyApplication),
        caps(),
        PhysicalLimits::default(),
        1,
        Policy {
            execution_limit_ms: Duration(10000),
            output_retention_ms: Duration(20000),
            receipt_retention_ms: Duration(1),
        },
        payload_policy(),
    );
    fixture.admit(0, 1, 3);
    fixture.run().unwrap();
    close(&fixture);
    let results = service(&fixture);
    let now = Instant::now();
    let read = results
        .begin_read(&fixture.binding.identity, &request(&fixture), &caps(), now)
        .unwrap();
    fixture.clock.0.store(1001, Ordering::SeqCst);
    sweep(&fixture.store, &fixture.payloads);
    assert!(
        !fixture
            .store
            .retire(&fixture.payloads, &mut RetirementCursor::default(), 1)
            .unwrap()
            .started
    );
    fixture.clock.0.store(21000, Ordering::SeqCst);
    sweep(&fixture.store, &fixture.payloads);
    assert!(fixture.job().outputs_live);
    assert!(
        !fixture
            .store
            .retire(&fixture.payloads, &mut RetirementCursor::default(), 1)
            .unwrap()
            .started
    );
    assert_eq!(drain(read, now), b"abc");
    sweep(&fixture.store, &fixture.payloads);
    finish(&fixture.store, &fixture.payloads);
}

#[test]
fn empty_and_never_admitted_skipped_sessions_retire_without_fabricated_jobs() {
    for members in [0, 1] {
        let fixture = Fixture::with_members(
            Arc::new(CopyApplication),
            caps(),
            PhysicalLimits::default(),
            members,
        );
        if members == 1 {
            fixture
                .store
                .skip_work(
                    &fixture.binding.identity,
                    OperationId([99; 16]),
                    &fixture.key(),
                )
                .unwrap();
        }
        close(&fixture);
        assert_eq!(count(&fixture.store, "jobs"), 0);
        fixture.clock.0.store(31000, Ordering::SeqCst);
        finish(&fixture.store, &fixture.payloads);
        assert_eq!(
            fixture
                .store
                .next_creation(&fixture.binding.identity.owner)
                .unwrap(),
            Id(2)
        );
    }
}

struct Both;
impl Authorization for Both {
    fn permits(&self, owner: &IdentityLabel, _: Permission) -> bool {
        matches!(owner.0.as_str(), "alice" | "bob")
    }
}
#[test]
fn retirement_preserves_another_owners_active_session_and_advances_generation_globally() {
    let fixture = ready();
    let mut store = fixture.store.clone();
    store.authorization = Arc::new(Both);
    let bob = store
        .create_session(
            &IdentityLabel("bob".into()),
            Id(1),
            &fixture.binding.policy,
            &caps(),
        )
        .unwrap();
    let mut cursor = RetirementCursor::default();
    for _ in 0..64 {
        store.retire(&fixture.payloads, &mut cursor, 1).unwrap();
    }
    assert_eq!(count(&store, "sessions"), 1);
    assert_eq!(
        store
            .attach_session(&bob.identity.owner, &bob.identity, &caps())
            .unwrap(),
        bob
    );
    let mut wrong = fixture.binding.identity.clone();
    wrong.owner = bob.identity.owner.clone();
    refuse(
        store.attach_session(&wrong.owner, &wrong, &caps()),
        ErrorCode::NotFound,
    );
    let alice = store
        .create_session(
            &fixture.binding.identity.owner,
            Id(2),
            &fixture.binding.policy,
            &caps(),
        )
        .unwrap();
    assert_eq!(alice.identity.generation, Id(3));
    store.integrity_check().unwrap();
}

struct Unsafe;
impl Clock for Unsafe {
    fn read(&self) -> ClockReading {
        ClockReading {
            utc_ms: Number(50000),
            trusted: false,
        }
    }
}
#[test]
fn unsafe_clock_bad_limits_and_foreign_cursor_never_start_or_resume_retirement() {
    let fixture = ready();
    for limit in [0, 257] {
        refuse(
            fixture
                .store
                .retire(&fixture.payloads, &mut RetirementCursor::default(), limit),
            ErrorCode::LimitExceeded,
        );
    }
    let other = ready();
    let mut cursor = RetirementCursor::default();
    other.store.retire(&other.payloads, &mut cursor, 1).unwrap();
    refuse(
        fixture.store.retire(&fixture.payloads, &mut cursor, 1),
        ErrorCode::Conflict,
    );
    for started in [false, true] {
        if started {
            fixture
                .store
                .retire(&fixture.payloads, &mut RetirementCursor::default(), 1)
                .unwrap();
        }
        let before = count(&fixture.store, "work");
        let mut unsafe_store = fixture.store.clone();
        unsafe_store.clock = Arc::new(Unsafe);
        refuse(
            unsafe_store.retire(&fixture.payloads, &mut RetirementCursor::default(), 1),
            ErrorCode::ClockUnsafe,
        );
        fixture.clock.0.store(999, Ordering::SeqCst);
        refuse(
            fixture
                .store
                .retire(&fixture.payloads, &mut RetirementCursor::default(), 1),
            ErrorCode::ClockUnsafe,
        );
        fixture.clock.0.store(31000, Ordering::SeqCst);
        assert_eq!(count(&fixture.store, "work"), before);
    }
    finish(&fixture.store, &fixture.payloads);
}

#[test]
fn retirement_crash_child() {
    let Some(path) = std::env::var_os("PIPESTREAM_RETIREMENT_CHILD_DIRECTORY") else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    let store = AuthorityStore::open(
        &path.join("authority.sqlite"),
        IdentityLabel("test-authority".into()),
        super::super::super::tests::policy(),
        PhysicalLimits::default(),
        Arc::new(TestClock(AtomicU64::new(31000))),
        Arc::new(Auth(AtomicBool::new(true))),
    )
    .unwrap();
    let payloads = PayloadStore::open(
        &path.join("objects"),
        store.payload_identity().unwrap(),
        payload_policy(),
    )
    .unwrap();
    finish(&store, &payloads);
    panic!("retirement crash hook did not fire");
}

#[test]
fn retiring_session_slot_is_not_refunded_until_final_metadata_commit() {
    let fixture = ready();
    fixture
        .store
        .retire(&fixture.payloads, &mut RetirementCursor::default(), 1)
        .unwrap();
    for sequence in 2..=5 {
        fixture
            .store
            .create_session(
                &fixture.binding.identity.owner,
                Id(sequence),
                &fixture.binding.policy,
                &caps(),
            )
            .unwrap();
    }
    refuse(
        fixture.store.create_session(
            &fixture.binding.identity.owner,
            Id(6),
            &fixture.binding.policy,
            &caps(),
        ),
        ErrorCode::LimitExceeded,
    );
    let mut cursor = RetirementCursor::default();
    for _ in 0..128 {
        fixture
            .store
            .retire(&fixture.payloads, &mut cursor, 1)
            .unwrap();
    }
    assert_eq!(count(&fixture.store, "sessions"), 4);
    assert_eq!(
        fixture
            .store
            .create_session(
                &fixture.binding.identity.owner,
                Id(6),
                &fixture.binding.policy,
                &caps()
            )
            .unwrap()
            .identity
            .generation,
        Id(6)
    );
    fixture.reopen().integrity_check().unwrap();
}

#[test]
fn pinned_wal_refusal_preserves_retirement_state_then_resumes_after_reader_release() {
    for already_started in [false, true] {
        let physical = PhysicalLimits {
            wal_bytes: 4 << 20,
            ..PhysicalLimits::default()
        };
        let fixture = Fixture::configured(Arc::new(CopyApplication), caps(), physical);
        fixture.admit(0, 1, 3);
        fixture.run().unwrap();
        close(&fixture);
        fixture.clock.0.store(31000, Ordering::SeqCst);
        sweep(&fixture.store, &fixture.payloads);
        if already_started {
            fixture
                .store
                .retire(&fixture.payloads, &mut RetirementCursor::default(), 1)
                .unwrap();
        }
        let mut connection = fixture.store.connect().unwrap();
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        records::protect(&tx, 0, 0).unwrap();
        tx.execute_batch("CREATE TABLE retirement_fill(body BLOB);
            CREATE TRIGGER protect_generation BEFORE UPDATE OF last_generation ON authority BEGIN SELECT RAISE(ABORT,'generation changed'); END;
            CREATE TRIGGER protect_authority BEFORE DELETE ON authority BEGIN SELECT RAISE(ABORT,'authority deleted'); END;
            CREATE TRIGGER protect_owner BEFORE DELETE ON owners BEGIN SELECT RAISE(ABORT,'owner deleted'); END;
            CREATE TRIGGER protect_creation BEFORE UPDATE ON owners BEGIN SELECT RAISE(ABORT,'creation changed'); END;").unwrap();
        tx.commit().unwrap();
        let mut reader = fixture.store.connect().unwrap();
        let snapshot = reader.transaction().unwrap();
        snapshot
            .query_row("SELECT count(*) FROM sessions", [], |r| r.get::<_, i64>(0))
            .unwrap();
        let mut filled = 0;
        loop {
            let result = (|| -> Result<()> {
                let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                records::protect(&tx, 0, 0)?;
                tx.execute("INSERT INTO retirement_fill VALUES(zeroblob(4096))", [])?;
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
        loop {
            fixture.clock.0.fetch_add(1, Ordering::SeqCst);
            let result = (|| -> Result<()> {
                let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let now = fixture.store.check_clock(&tx)?;
                fixture.store.remember_clock(&tx, now)?;
                tx.commit()?;
                Ok(())
            })();
            if result.is_err() {
                refuse(result, ErrorCode::LimitExceeded);
                break;
            }
            assert!(fixture.clock.0.load(Ordering::SeqCst) < 32000);
        }
        let before = fixture.store.physical_usage().unwrap();
        refuse(
            fixture
                .store
                .retire(&fixture.payloads, &mut RetirementCursor::default(), 1),
            ErrorCode::LimitExceeded,
        );
        assert_eq!(
            count(&fixture.store, "retirements"),
            u64::from(already_started)
        );
        assert_eq!(count(&fixture.store, "work"), 1);
        assert_eq!(count(&fixture.store, "sessions"), 1);
        fixture.store.integrity_check().unwrap();
        let refused = fixture.store.physical_usage().unwrap();
        assert!(refused.wal_bytes <= physical.wal_bytes);
        refuse(fixture.store.checkpoint_storage(), ErrorCode::LimitExceeded);
        drop(snapshot);
        drop(reader);
        fixture.store.checkpoint_storage().unwrap();
        finish(&fixture.store, &fixture.payloads);
        fixture.reopen().integrity_check().unwrap();
        eprintln!(
            "retirement already_started={already_started} fill={filled} WAL_before={} WAL_refusal={} cap={}",
            before.wal_bytes, refused.wal_bytes, physical.wal_bytes
        );
    }
}

#[test]
fn actual_process_death_recovers_every_retirement_phase_without_reusing_identity() {
    for phase in [
        "retirement-intent",
        "retirement-work",
        "retirement-scope",
        "retirement-operation",
        "retirement-finish",
    ] {
        for side in ["before", "after"] {
            let fixture = Fixture::new(Arc::new(branch_tests::UppercaseScatter));
            fixture.admit(2, 1, 3);
            fixture.run().unwrap();
            branch_tests::execute_children(&fixture, Producer(1));
            fixture.run().unwrap();
            close(&fixture);
            fixture.clock.0.store(31000, Ordering::SeqCst);
            sweep(&fixture.store, &fixture.payloads);
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
            let boundary = format!("{phase}:{side}");
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "v2::authority::execution::tests::retirement_tests::retirement_crash_child",
                    "--nocapture",
                ])
                .env("PIPESTREAM_RETIREMENT_CHILD_DIRECTORY", directory.path())
                .env("PIPESTREAM_TEST_AUTHORITY_CRASH", &boundary)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(86), "{boundary}");
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
            store.audit_payloads(&payloads).unwrap();
            finish(&store, &payloads);
            assert_eq!(store.next_creation(&binding.identity.owner).unwrap(), Id(2));
            refuse(
                store.create_session(&binding.identity.owner, Id(1), &binding.policy, &caps()),
                ErrorCode::Expired,
            );
            assert_eq!(
                store
                    .create_session(&binding.identity.owner, Id(2), &binding.policy, &caps())
                    .unwrap()
                    .identity
                    .generation,
                Id(2)
            );
        }
    }
}
