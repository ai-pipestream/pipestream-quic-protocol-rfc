use super::{result_tests::*, *};

pub(super) fn sweep(store: &AuthorityStore, payloads: &PayloadStore) {
    let mut cursor = RetentionCursor::default();
    // Separate file and metadata passes, one inspected item per pass. Repeated
    // calls intentionally exercise the wrap-around case, not only limit=256.
    for _ in 0..80 {
        let report = store.reclaim(payloads, &mut cursor, 1).unwrap();
        assert!(report.inspected_jobs <= 1 && report.inspected_files <= 1);
    }
}

#[test]
fn reclamation_keeps_manifests_and_receipts_and_releases_input_before_output_expiry() {
    let fixture = published();
    let before = fixture.view();
    let mut cursor = RetentionCursor::default();
    let first = fixture
        .store
        .reclaim(&fixture.payloads, &mut cursor, 256)
        .unwrap();
    assert_eq!((first.committed_intents, first.removed_files), (1, 1));
    assert!(
        fixture.job().input_live,
        "deletion must precede quota refund"
    );
    assert_eq!(fixture.payloads.usage(None).unwrap().objects, 2);
    fixture.store.audit_payloads(&fixture.payloads).unwrap();
    fixture.reopen().integrity_check().unwrap();
    sweep(&fixture.store, &fixture.payloads);
    assert!(!fixture.job().input_live && fixture.job().outputs_live);
    assert_eq!(fixture.view(), before);
    let results = service(&fixture);
    assert_eq!(
        drain(
            results
                .begin_read(
                    &fixture.binding.identity,
                    &request(&fixture),
                    &caps(),
                    Instant::now()
                )
                .unwrap(),
            Instant::now()
        ),
        b"abc"
    );
    fixture.clock.0.store(20999, Ordering::SeqCst);
    sweep(&fixture.store, &fixture.payloads);
    assert!(fixture.job().outputs_live);
    fixture.clock.0.store(21000, Ordering::SeqCst);
    sweep(&fixture.store, &fixture.payloads);
    assert!(!fixture.job().outputs_live);
    assert_eq!(fixture.payloads.usage(None).unwrap().objects, 0);
    assert_eq!(fixture.payloads.usage(None).unwrap().charged_bytes, 0);
    assert_eq!(fixture.view(), before);
    assert_eq!(
        results
            .manifest(&fixture.binding.identity, &fixture.key(), Id(1), &caps())
            .unwrap(),
        before.manifest.unwrap()
    );
    refuse(
        results.begin_read(
            &fixture.binding.identity,
            &request(&fixture),
            &caps(),
            Instant::now(),
        ),
        ErrorCode::Expired,
    );
    sweep(&fixture.store, &fixture.payloads);
    fixture.store.audit_payloads(&fixture.payloads).unwrap();
    fixture.reopen().integrity_check().unwrap();
}

#[test]
fn a_read_pin_outliving_external_expiry_holds_actual_bytes_and_logical_output_capacity() {
    let fixture = published();
    let results = service(&fixture);
    let now = Instant::now();
    let read = results
        .begin_read(&fixture.binding.identity, &request(&fixture), &caps(), now)
        .unwrap();
    fixture.clock.0.store(21000, Ordering::SeqCst);
    sweep(&fixture.store, &fixture.payloads);
    assert!(!fixture.job().input_live && fixture.job().outputs_live);
    assert!(fixture.job().release.unwrap().outputs);
    assert_eq!(fixture.payloads.usage(None).unwrap().objects, 2);
    let mut binding = fixture.binding.clone();
    binding.limits.retained_output_bytes = Number(3);
    let check = || {
        let mut connection = fixture.store.connect().unwrap();
        let tx = connection.transaction().unwrap();
        jobs::capacity(
            &tx,
            &fixture.store.policy,
            &binding,
            &fixture.job().parameters,
        )
    };
    refuse(check(), ErrorCode::LimitExceeded);
    assert_eq!(drain(read, now), b"abc");
    sweep(&fixture.store, &fixture.payloads);
    assert!(!fixture.job().outputs_live);
    check().unwrap();
    fixture.store.audit_payloads(&fixture.payloads).unwrap();
}

#[test]
fn a_cancelled_callback_keeps_its_input_and_output_reservation_until_handles_drop() {
    let fixture = Fixture::new(Arc::new(CopyApplication));
    fixture.admit(0, 1, 3);
    let mut execution = fixture
        .executor
        .claim(&fixture.binding.identity, &fixture.key())
        .unwrap();
    execution
        .context
        .begin_output(Number(3), ApplicationLabel("text/plain".into()))
        .unwrap();
    execution.context.write_output(b"ab").unwrap();
    fixture
        .store
        .cancel_scope(&fixture.binding.identity, OperationId([89; 16]), Number(0))
        .unwrap();
    let mut cursor = ReconcileCursor::default();
    for _ in 0..20 {
        fixture.store.reconcile(&mut cursor, 1).unwrap();
    }
    assert_eq!(fixture.view().state, State::CANCELLED);
    sweep(&fixture.store, &fixture.payloads);
    assert!(fixture.job().input_live && fixture.job().outputs_live);
    assert!(fixture.payloads.usage(None).unwrap().objects > 0);
    refuse(execution.context.write_output(b"c"), ErrorCode::Cancelled);
    drop(execution);
    sweep(&fixture.store, &fixture.payloads);
    assert!(!fixture.job().input_live && !fixture.job().outputs_live);
    assert_eq!(fixture.payloads.usage(None).unwrap().objects, 0);
    fixture.reopen().integrity_check().unwrap();
}

#[test]
fn elapsed_deadlines_do_not_clean_unsettled_work_or_missing_descendants() {
    for mode in [0, 1] {
        let fixture = Fixture::new(Arc::new(CopyApplication));
        fixture.admit(mode, 1, 3);
        if mode == 1 {
            fixture
                .store
                .declare(
                    &fixture.binding.identity,
                    OperationId([87; 16]),
                    Number(1),
                    &[Id(1)],
                    true,
                )
                .unwrap();
        }
        fixture.clock.0.store(50000, Ordering::SeqCst);
        let before = fixture.payloads.usage(None).unwrap();
        sweep(&fixture.store, &fixture.payloads);
        assert!(fixture.job().release.is_none());
        assert_eq!(fixture.payloads.usage(None).unwrap(), before);
        if mode == 1 {
            // A fenced parent can be terminal before all named missing children
            // are settled. Until child closure the input cannot be reclaimed.
            let mut cursor = ReconcileCursor::default();
            fixture.store.reconcile(&mut cursor, 1).unwrap();
            sweep(&fixture.store, &fixture.payloads);
            let mut connection = fixture.store.connect().unwrap();
            let tx = connection.transaction().unwrap();
            assert!(
                scopes::closed(&tx, fixture.binding.identity.generation, Number(1))
                    .unwrap()
                    .is_none()
            );
            assert!(fixture.job().release.is_none());
        }
    }
}

struct UnsafeClock;
impl Clock for UnsafeClock {
    fn read(&self) -> ClockReading {
        ClockReading {
            utc_ms: Number(50000),
            trusted: false,
        }
    }
}
#[test]
fn destructive_cleanup_requires_safe_utc_but_not_current_caller_authorization() {
    let mut fixture = published();
    let before = fixture.payloads.usage(None).unwrap();
    let safe = fixture.store.clock.clone();
    fixture.store.clock = Arc::new(UnsafeClock);
    refuse(
        fixture
            .store
            .reclaim(&fixture.payloads, &mut RetentionCursor::default(), 1),
        ErrorCode::ClockUnsafe,
    );
    refuse(
        fixture
            .store
            .collect_payload_orphans(&fixture.payloads, None, 256),
        ErrorCode::ClockUnsafe,
    );
    fixture.store.audit_payloads(&fixture.payloads).unwrap();
    assert_eq!(fixture.payloads.usage(None).unwrap(), before);
    fixture.store.clock = safe;
    fixture.clock.0.store(999, Ordering::SeqCst);
    refuse(
        fixture
            .store
            .reclaim(&fixture.payloads, &mut RetentionCursor::default(), 1),
        ErrorCode::ClockUnsafe,
    );
    fixture.clock.0.store(21000, Ordering::SeqCst);
    fixture.auth.0.store(false, Ordering::SeqCst);
    sweep(&fixture.store, &fixture.payloads);
    assert_eq!(fixture.payloads.usage(None).unwrap().objects, 0);
    fixture.reopen().integrity_check().unwrap();
}

#[test]
fn invalid_batch_or_cross_authority_cursor_refuses_without_mutation() {
    let fixture = published();
    let before = fixture.job();
    for limit in [0, 257] {
        refuse(
            fixture
                .store
                .reclaim(&fixture.payloads, &mut RetentionCursor::default(), limit),
            ErrorCode::LimitExceeded,
        );
    }
    assert_eq!(fixture.job(), before);
    let other = published();
    let mut cursor = RetentionCursor::default();
    other
        .store
        .reclaim(&other.payloads, &mut cursor, 1)
        .unwrap();
    refuse(
        fixture.store.reclaim(&fixture.payloads, &mut cursor, 1),
        ErrorCode::Conflict,
    );
    assert_eq!(fixture.job(), before);
}

#[test]
fn new_admitted_jobs_cannot_keep_the_metadata_cursor_above_an_older_due_job() {
    let fixture = Fixture::with_members(
        Arc::new(CopyApplication),
        caps(),
        PhysicalLimits::default(),
        12,
    );
    fixture.admit(0, 1, 3);
    fixture.run().unwrap();
    let mut cursor = RetentionCursor::default();
    fixture
        .store
        .reclaim(&fixture.payloads, &mut cursor, 1)
        .unwrap();
    fixture.clock.0.store(21000, Ordering::SeqCst);
    for entity in 2..=12 {
        fixture.admit_entity(entity, 0, 1, 3);
        fixture
            .store
            .reclaim(&fixture.payloads, &mut cursor, 1)
            .unwrap();
    }
    assert!(fixture.job().release.unwrap().outputs);
    fixture.reopen().integrity_check().unwrap();
}

#[test]
fn missing_required_files_fail_closed_at_rebind_executor_start_and_fresh_input() {
    for kind in ["input", "reservation", "output"] {
        let fixture = Fixture::with_members(
            Arc::new(CopyApplication),
            caps(),
            PhysicalLimits::default(),
            2,
        );
        fixture.admit(0, 1, 3);
        fixture.run().unwrap();
        let job = fixture.job();
        let path = match kind {
            "input" => fixture
                .payloads
                .path()
                .join(format!("object-{}", job.input_key.0)),
            "reservation" => fixture
                .payloads
                .path()
                .join(format!("reserve-{}", job.reservation_key.0)),
            _ => std::fs::read_dir(fixture.payloads.path())
                .unwrap()
                .map(|e| e.unwrap().path())
                .find(|p| {
                    p.file_name()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .starts_with("object-")
                        && *p
                            != fixture
                                .payloads
                                .path()
                                .join(format!("object-{}", job.input_key.0))
                })
                .unwrap(),
        };
        std::fs::remove_file(path).unwrap();
        assert!(
            matches!(
                fixture.store.bind_payloads(&fixture.payloads),
                Err(StoreError::Corrupt(_))
            ),
            "{kind}"
        );
        assert!(
            matches!(
                Executor::new(
                    fixture.store.clone(),
                    fixture.payloads.clone(),
                    fixture.executor.applications.clone(),
                    fixture.executor.endpoint.clone(),
                    caps(),
                    Duration(100)
                ),
                Err(StoreError::Corrupt(_))
            ),
            "{kind}"
        );
        let header = InputHeader {
            kind: Literal,
            generation: fixture.binding.identity.generation,
            operation: OperationId([3; 16]),
            parameters: AdmitParameters {
                work: WorkKey {
                    entity: Id(2),
                    ..fixture.key()
                },
                ..job.parameters
            },
        };
        assert!(
            matches!(
                fixture.store.receive_input(
                    &fixture.binding.identity,
                    &header,
                    &caps(),
                    &fixture.payloads,
                    &fixture.executor.applications,
                    Instant::now()
                ),
                Err(StoreError::Corrupt(_))
            ),
            "{kind}"
        );
        fixture.reopen().integrity_check().unwrap(); // Metadata validity is distinct.
    }
}

#[test]
fn retention_crash_child() {
    let Some(path) = std::env::var_os("PIPESTREAM_RETENTION_CHILD_DIRECTORY") else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    let store = AuthorityStore::open(
        &path.join("authority.sqlite"),
        IdentityLabel("test-authority".into()),
        super::super::super::tests::policy(),
        PhysicalLimits::default(),
        Arc::new(TestClock(AtomicU64::new(21000))),
        Arc::new(Auth(AtomicBool::new(true))),
    )
    .unwrap();
    let payloads = PayloadStore::open(
        &path.join("objects"),
        store.payload_identity().unwrap(),
        payload_policy(),
    )
    .unwrap();
    store.audit_payloads(&payloads).unwrap();
    sweep(&store, &payloads);
    panic!("retention crash point did not fire");
}

#[test]
fn checksummed_early_or_future_release_and_prior_format_refuse_on_reopen() {
    for case in ["early", "future", "before-terminal", "old-format"] {
        let fixture = published();
        let mut connection = fixture.store.connect().unwrap();
        if case == "old-format" {
            connection.pragma_update(None, "user_version", 8).unwrap();
        } else {
            let tx = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            let (row, mut job, revision, _, _) =
                load(&tx, &fixture.binding.identity, &fixture.key()).unwrap();
            job.release = Some(jobs::Release {
                input: true,
                outputs: case == "early",
                at: Number(match case {
                    "future" => 1001,
                    "before-terminal" => 999,
                    _ => 1000,
                }),
            });
            records::replace(&tx, job_target(row), revision, &job, false).unwrap();
            tx.commit().unwrap();
            // Cleanup cannot repair a forged early intent by replacing its
            // timestamp with today's otherwise valid expiry timestamp.
            fixture.clock.0.store(21000, Ordering::SeqCst);
            assert!(
                matches!(
                    fixture
                        .store
                        .reclaim(&fixture.payloads, &mut RetentionCursor::default(), 1),
                    Err(StoreError::Corrupt(_))
                ),
                "{case}"
            );
        }
        assert!(
            matches!(
                AuthorityStore::open(
                    &fixture.directory.path().join("authority.sqlite"),
                    fixture.store.authority.clone(),
                    fixture.store.policy.clone(),
                    PhysicalLimits::default(),
                    fixture.clock.clone(),
                    fixture.auth.clone()
                ),
                Err(StoreError::Corrupt(_))
            ),
            "{case}"
        );
    }
}

#[test]
fn missing_input_after_exclusive_reopen_is_not_free_capacity() {
    let fixture = published();
    let input = fixture.job().input_key.0;
    let Fixture {
        directory,
        store,
        payloads,
        executor,
        ..
    } = fixture;
    drop(executor);
    drop(payloads);
    std::fs::remove_file(
        directory
            .path()
            .join("objects")
            .join(format!("object-{input}")),
    )
    .unwrap();
    let payloads = PayloadStore::open(
        &directory.path().join("objects"),
        store.payload_identity().unwrap(),
        payload_policy(),
    )
    .unwrap();
    assert!(matches!(
        store.audit_payloads(&payloads),
        Err(StoreError::Corrupt(_))
    ));
}

#[test]
fn retained_job_cleanup_uses_reserved_wal_without_row_replacement_or_page_growth() {
    let physical = PhysicalLimits {
        wal_bytes: 4 << 20,
        ..PhysicalLimits::default()
    };
    let fixture = Fixture::configured(Arc::new(CopyApplication), caps(), physical);
    fixture.admit(0, 1, 3);
    fixture.run().unwrap();
    let mut connection = fixture.store.connect().unwrap();
    let tx = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    records::protect(&tx, 0, 0).unwrap();
    tx.execute_batch("CREATE TABLE retention_fill(body BLOB);
        CREATE TRIGGER forbid_work_update BEFORE UPDATE ON work BEGIN SELECT RAISE(ABORT,'work row replacement'); END;
        CREATE TRIGGER forbid_job_update BEFORE UPDATE ON jobs BEGIN SELECT RAISE(ABORT,'job row replacement'); END;
        CREATE TRIGGER forbid_clock_update BEFORE UPDATE ON authority BEGIN SELECT RAISE(ABORT,'clock row replacement'); END;").unwrap();
    tx.commit().unwrap();
    let mut reader = fixture.store.connect().unwrap();
    let snapshot = reader.transaction().unwrap();
    snapshot
        .query_row("SELECT count(*) FROM jobs", [], |r| r.get::<_, i64>(0))
        .unwrap();
    let mut filled = 0;
    loop {
        let result = (|| -> Result<()> {
            let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            records::protect(&tx, 0, 0)?;
            tx.execute("INSERT INTO retention_fill VALUES(zeroblob(4096))", [])?;
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
    // Fill the smaller ordinary clock-rewrite shape too. A refused insert
    // alone does not prove no smaller ordinary transaction still fits.
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
        assert!(fixture.clock.0.load(Ordering::SeqCst) < 2000);
    }
    let pages: i64 = connection
        .query_row("PRAGMA page_count", [], |r| r.get(0))
        .unwrap();
    let before = fixture.store.physical_usage().unwrap();
    // Exercise four separate funded transitions and distinct clock updates:
    // input intent, input finish, output intent, output finish.
    for _ in 0..8 {
        fixture.clock.0.fetch_add(1, Ordering::SeqCst);
        fixture
            .store
            .reclaim(&fixture.payloads, &mut RetentionCursor::default(), 256)
            .unwrap();
    }
    assert!(!fixture.job().input_live && fixture.job().outputs_live);
    fixture.clock.0.store(21000, Ordering::SeqCst);
    for _ in 0..8 {
        fixture.clock.0.fetch_add(1, Ordering::SeqCst);
        fixture
            .store
            .reclaim(&fixture.payloads, &mut RetentionCursor::default(), 256)
            .unwrap();
    }
    let after = fixture.store.physical_usage().unwrap();
    assert!(!fixture.job().outputs_live);
    assert_eq!(fixture.payloads.usage(None).unwrap().objects, 0);
    assert_eq!(
        connection
            .query_row("PRAGMA page_count", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        pages
    );
    assert!(after.wal_bytes > before.wal_bytes && after.wal_bytes <= physical.wal_bytes);
    eprintln!(
        "retention fill={filled} pages={pages} WAL={} -> {} cap={}",
        before.wal_bytes, after.wal_bytes, physical.wal_bytes
    );
    fixture.store.integrity_check().unwrap();
}

#[test]
fn actual_process_death_recovers_intent_unlink_and_quota_completion_independently() {
    for (variable, point, exit) in [
        (
            "PIPESTREAM_TEST_AUTHORITY_CRASH",
            "retention-intent:before",
            86,
        ),
        (
            "PIPESTREAM_TEST_AUTHORITY_CRASH",
            "retention-intent:after",
            86,
        ),
        ("PIPESTREAM_PAYLOAD_TEST_CRASH", "cleanup-unlinked", 87),
        ("PIPESTREAM_OUTPUT_TEST_CRASH", "reserve-unlinked", 87),
        (
            "PIPESTREAM_TEST_AUTHORITY_CRASH",
            "retention-finish:before",
            86,
        ),
        (
            "PIPESTREAM_TEST_AUTHORITY_CRASH",
            "retention-finish:after",
            86,
        ),
    ] {
        let fixture = published();
        let before = fixture.view();
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
                "v2::authority::execution::tests::retention_tests::retention_crash_child",
                "--nocapture",
            ])
            .env("PIPESTREAM_RETENTION_CHILD_DIRECTORY", directory.path())
            .env(variable, point)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(exit), "{point}");
        clock.0.store(21000, Ordering::SeqCst);
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
        store.bind_payloads(&payloads).unwrap();
        sweep(&store, &payloads);
        assert_eq!(payloads.usage(None).unwrap().objects, 0, "{point}");
        assert_eq!(
            store
                .work_view(&binding.identity, &before.work, Number(0))
                .unwrap()
                .1,
            before,
            "{point}"
        );
        store.integrity_check().unwrap();
        store.audit_payloads(&payloads).unwrap();
    }
}
