use super::*;

fn work_slot(store: &AuthorityStore, generation: Id, entity: Id) -> Target {
    Target {
        table: Table::Work,
        row: store
            .connect()
            .unwrap()
            .query_row(
                "SELECT rowid FROM work WHERE generation=?1 AND scope=0 AND entity=?2",
                params![sql(generation.0).unwrap(), sql(entity.0).unwrap()],
                |r| r.get(0),
            )
            .unwrap(),
    }
}

fn declared() -> (super::super::tests::Fixture, Binding, Target) {
    let fixture = super::super::tests::Fixture::new();
    let binding = fixture.create();
    fixture
        .store
        .declare(
            &binding.identity,
            OperationId([1; 16]),
            Number(0),
            &[Id(1)],
            false,
        )
        .unwrap();
    let target = work_slot(&fixture.store, binding.identity.generation, Id(1));
    (fixture, binding, target)
}

#[test]
fn declared_records_preallocate_capacity_and_preserve_it_across_rewrite() {
    let (fixture, binding, target) = declared();
    let mut connection = fixture.store.connect().unwrap();
    let tx = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let (before, mut view): (_, WorkView) = read(&tx, target).unwrap();
    assert_eq!(before.capacity, WORK_CAPACITY);
    assert_eq!(before.credits, WORK_CREDITS);
    assert_eq!(before.revision, Id(1));
    let pages: u64 = tx
        .query_row("PRAGMA page_count", [], |r| number(r, 0))
        .unwrap();
    view.state = State::CANCELLING;
    assert_eq!(replace(&tx, target, Id(1), &view, true).unwrap(), Id(2));
    let (after, actual): (_, WorkView) = read(&tx, target).unwrap();
    assert_eq!(actual, view);
    assert_eq!(after.capacity, before.capacity);
    assert_eq!(after.credits, before.credits - 1);
    assert_eq!(
        tx.query_row("PRAGMA page_count", [], |r| number(r, 0))
            .unwrap(),
        pages
    );
    tx.commit().unwrap();
    assert_eq!(
        fixture
            .store
            .work_view(&binding.identity, &view.work, Number(0))
            .unwrap(),
        (Id(2), view)
    );
}

#[test]
fn ordinary_rewrite_never_spends_a_future_credit_and_rollback_restores_spending() {
    let (fixture, _, target) = declared();
    let mut connection = fixture.store.connect().unwrap();
    let tx = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let (_, view): (_, WorkView) = read(&tx, target).unwrap();
    replace(&tx, target, Id(1), &view, false).unwrap();
    assert_eq!(header(&tx, target).unwrap().credits, WORK_CREDITS);
    replace(&tx, target, Id(2), &view, true).unwrap();
    assert_eq!(header(&tx, target).unwrap().credits, WORK_CREDITS - 1);
    tx.rollback().unwrap();
    let tx = connection.transaction().unwrap();
    let retained = header(&tx, target).unwrap();
    assert_eq!((retained.revision, retained.credits), (Id(1), WORK_CREDITS));
}

#[test]
fn ordinary_revisions_cannot_exhaust_counters_reserved_for_promised_updates() {
    let (fixture, _, target) = declared();
    let mut connection = fixture.store.connect().unwrap();
    let tx = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let (_, view): (_, WorkView) = read(&tx, target).unwrap();
    // Place the private record at the last safe ordinary revision without
    // iterating the 63-bit space; public revision reads still use exact integers.
    write(&tx, target, &view, Id(MAX_NUMBER - 2), WORK_CAPACITY, 2).unwrap();
    let result = replace(&tx, target, Id(MAX_NUMBER - 2), &view, false);
    assert!(
        matches!(
            result,
            Err(StoreError::Protocol(Error {
                code: ErrorCode::LimitExceeded,
                ..
            }))
        ),
        "ordinary changes must preserve two remaining revision increments: {result:?}"
    );
    assert_eq!(
        replace(&tx, target, Id(MAX_NUMBER - 2), &view, true).unwrap(),
        Id(MAX_NUMBER - 1)
    );
    assert_eq!(
        replace(&tx, target, Id(MAX_NUMBER - 1), &view, true).unwrap(),
        Id(MAX_NUMBER)
    );
    assert_eq!(header(&tx, target).unwrap().credits, 0);
}

#[test]
fn scope_closure_uses_its_preallocated_summary_without_allocating_a_new_record() {
    let fixture = super::super::tests::Fixture::new();
    let binding = fixture.create();
    let target = Target {
        table: Table::Scope,
        row: fixture
            .store
            .connect()
            .unwrap()
            .query_row("SELECT rowid FROM scopes", [], |r| r.get(0))
            .unwrap(),
    };
    let mut connection = fixture.store.connect().unwrap();
    let tx = connection.transaction().unwrap();
    let (before, empty): (_, scopes::ScopeState) = read(&tx, target).unwrap();
    assert!(empty.summary.is_none());
    assert_eq!(before.credits, SCOPE_CREDITS);
    drop(tx);
    fixture
        .store
        .declare(
            &binding.identity,
            OperationId([2; 16]),
            Number(0),
            &[],
            true,
        )
        .unwrap();
    let tx = connection.transaction().unwrap();
    let (after, state): (_, scopes::ScopeState) = read(&tx, target).unwrap();
    assert_eq!(after.capacity, before.capacity);
    assert_eq!((after.revision, after.credits), (Id(2), SCOPE_CREDITS - 1));
    assert_eq!(state.summary.unwrap().counts.total().unwrap(), 0);
}

#[test]
fn corrupt_header_body_padding_and_cross_row_copy_fail_closed() {
    for corruption in ["header", "body", "padding", "row"] {
        let (fixture, _, target) = declared();
        let mut connection = fixture.store.connect().unwrap();
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let retained = header(&tx, target).unwrap();
        let mut blob = tx
            .blob_open("main", "work", "view", target.row, false)
            .unwrap();
        let offset = match corruption {
            "header" => 16,
            "body" => HEADER_BYTES,
            "padding" => HEADER_BYTES + retained.used,
            _ => 0,
        };
        if corruption == "row" {
            let mut bytes = [0; HEADER_BYTES];
            blob.read_at_exact(&mut bytes, 0).unwrap();
            let hash = checksum(
                Target {
                    table: Table::Work,
                    row: target.row + 1,
                },
                &bytes[..72],
            );
            bytes[72..].copy_from_slice(&hash);
            blob.write_at(&bytes, 0).unwrap();
        } else {
            let mut byte = [0];
            blob.read_at_exact(&mut byte, offset).unwrap();
            byte[0] ^= 1;
            blob.write_at(&byte, offset).unwrap();
        }
        blob.close().unwrap();
        assert!(
            matches!(read::<WorkView>(&tx, target), Err(StoreError::Corrupt(_))),
            "{corruption}"
        );
    }
}

#[test]
fn stale_revision_oversize_value_and_exhausted_credits_do_not_change_the_record() {
    let (fixture, _, target) = declared();
    let mut connection = fixture.store.connect().unwrap();
    let tx = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let (_, view): (_, WorkView) = read(&tx, target).unwrap();
    assert!(matches!(
        replace(&tx, target, Id(2), &view, true),
        Err(StoreError::Protocol(Error {
            code: ErrorCode::Conflict,
            ..
        }))
    ));
    // A private typed record cannot exceed its allocated slot, even when the
    // CBOR value itself remains below the maximum control bound.
    let values = vec![Number(MAX_NUMBER); 256];
    assert!(matches!(
        replace(&tx, target, Id(1), &values, true),
        Err(StoreError::Protocol(Error {
            code: ErrorCode::LimitExceeded,
            ..
        }))
    ));
    assert_eq!(header(&tx, target).unwrap().revision, Id(1));
    replace(&tx, target, Id(1), &view, true).unwrap();
    replace(&tx, target, Id(2), &view, true).unwrap();
    assert!(matches!(
        replace(&tx, target, Id(3), &view, true),
        Err(StoreError::Protocol(Error {
            code: ErrorCode::LimitExceeded,
            ..
        }))
    ));
    assert_eq!(header(&tx, target).unwrap().revision, Id(3));
}

#[test]
fn pinned_wal_exhaustion_preserves_both_records_promised_rewrites() {
    let fixture = super::super::tests::Fixture::with_physical(
        super::super::tests::policy(),
        PhysicalLimits {
            wal_bytes: 1 << 20,
            ..PhysicalLimits::default()
        },
    );
    let binding = fixture.create();
    fixture
        .store
        .declare(
            &binding.identity,
            OperationId([1; 16]),
            Number(0),
            &[Id(1), Id(2)],
            false,
        )
        .unwrap();
    let targets = [
        work_slot(&fixture.store, binding.identity.generation, Id(1)),
        work_slot(&fixture.store, binding.identity.generation, Id(2)),
    ];
    let mut writer = fixture.store.connect().unwrap();
    let mut tx = writer
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    grow(&mut tx, targets[0], Id(1), 16384, WORK_CREDITS).unwrap();
    tx.commit().unwrap();
    // A test-only ordinary record supplies unrelated pressure, through the
    // same protected transaction and VFS as real authority metadata writers.
    writer
        .execute_batch(
            "CREATE TABLE ordinary_write_probe(id INTEGER PRIMARY KEY, value BLOB);
        INSERT INTO ordinary_write_probe VALUES(1,zeroblob(8192)); PRAGMA wal_checkpoint(TRUNCATE);
        PRAGMA wal_autocheckpoint=0;",
        )
        .unwrap();
    let mut reader = fixture.store.connect().unwrap();
    let snapshot = reader.transaction().unwrap();
    let _: i64 = snapshot
        .query_row("SELECT count(*) FROM work", [], |r| r.get(0))
        .unwrap();
    let mut accepted = 0;
    let mut refused = false;
    for iteration in 0..2000 {
        let tx = writer
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let result = protect(&tx, 0, 0).and_then(|_| {
            // Different bytes force a real write; repeatedly storing the same
            // zeroblob can be optimized away by SQLite and is not pressure.
            let bytes = vec![(iteration % 250 + 1) as u8; 8192];
            tx.execute(
                "UPDATE ordinary_write_probe SET value=?1 WHERE id=1",
                [bytes],
            )?;
            tx.commit().map_err(StoreError::from)
        });
        match result {
            Ok(()) => accepted += 1,
            Err(StoreError::Protocol(Error {
                code: ErrorCode::LimitExceeded,
                ..
            })) => {
                refused = true;
                break;
            }
            other => panic!("unexpected fill result: {other:?}"),
        }
    }
    assert!(
        accepted > 0 && refused,
        "ordinary writes must reach the protected ceiling"
    );
    let mut tx = writer
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    assert!(matches!(
        grow(&mut tx, targets[1], Id(1), MAX_CONTROL_LIMIT, WORK_CREDITS),
        Err(StoreError::Protocol(Error {
            code: ErrorCode::LimitExceeded,
            ..
        }))
    ));
    assert_eq!(header(&tx, targets[1]).unwrap().capacity, WORK_CAPACITY);
    tx.rollback().unwrap();
    let pages: u64 = writer
        .query_row("PRAGMA page_count", [], |r| number(r, 0))
        .unwrap();
    writer
        .pragma_update(None, "max_page_count", sql(pages).unwrap())
        .unwrap();
    let start = fixture.store.physical_usage().unwrap().wal_bytes;
    for target in targets {
        for step in 0..WORK_CREDITS {
            let tx = writer
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            let (old, view): (_, WorkView) = read(&tx, target).unwrap();
            assert_eq!(old.credits, WORK_CREDITS - step);
            replace(&tx, target, old.revision, &view, true).unwrap();
            let (clock, previous): (_, Number) = read(&tx, CLOCK).unwrap();
            replace(&tx, CLOCK, clock.revision, &Number(previous.0 + 1), false).unwrap();
            tx.commit().unwrap();
        }
    }
    let usage = fixture.store.physical_usage().unwrap();
    assert!(usage.wal_bytes > start && usage.wal_bytes <= 1 << 20);
    assert_eq!(
        writer
            .query_row("PRAGMA page_count", [], |r| number(r, 0))
            .unwrap(),
        pages
    );
    let before_release = snapshot
        .query_row("SELECT count(*) FROM work", [], |r| number(r, 0))
        .unwrap();
    assert_eq!(before_release, 2);
    eprintln!(
        "record escrow: ordinary_commits={accepted} pinned_wal_before={start} after={} cap={}",
        usage.wal_bytes,
        1 << 20
    );
}

#[test]
fn record_rewrite_cost_bound_covers_page_sizes_padding_and_spilling() {
    for page in [512u64, 4096, 65536] {
        for capacity in [512usize, 2048, 4096, 8192, 65536, MAX_CONTROL_LIMIT] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("geometry.sqlite");
            let guard = PhysicalGuard::open(
                &path,
                Some(PhysicalLimits {
                    wal_bytes: 128 << 20,
                    shared_memory_bytes: 4 << 20,
                    ..PhysicalLimits::default()
                }),
            )
            .unwrap();
            let mut connection = Connection::open_with_flags_and_vfs(
                &path,
                OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
                GUARDED_VFS,
            )
            .unwrap();
            connection
                .pragma_update(None, "page_size", sql(page).unwrap())
                .unwrap();
            connection.execute_batch(SCHEMA).unwrap();
            connection.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA cache_size=2; PRAGMA cache_spill=1;").unwrap();
            // This is a physical-record fixture using the real table layout,
            // not an admitted protocol job or a logical cancellation test.
            let tx = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            tx.execute("INSERT INTO owners VALUES('alice',1)", [])
                .unwrap();
            let policy = super::super::tests::policy();
            tx.execute("INSERT INTO authority(singleton,name,last_generation,clock,policy,store_id,payload_path) VALUES(1,?1,?2,zeroblob(?3),?4,?5,?6)",
                params!["a".repeat(128), sql(MAX_NUMBER).unwrap(), (HEADER_BYTES+CLOCK_CAPACITY) as i64, pack(&policy).unwrap(), crate::persistence::StoreIdentity::generate().unwrap().as_bytes().as_slice(), "p".repeat(4096)]).unwrap();
            initialize(&tx, CLOCK, &Number(0), CLOCK_CAPACITY, 0).unwrap();
            let retention = Policy {
                execution_limit_ms: Duration(1000),
                output_retention_ms: Duration(1000),
                receipt_retention_ms: Duration(1000),
            };
            tx.execute("INSERT INTO sessions(generation,owner,creation_sequence,policy,limits,results,control_limit,object_limit) VALUES(1,'alice',1,?1,?2,1,8192,1048576)",
                params![pack(&retention).unwrap(),pack(&policy.session_limits).unwrap()]).unwrap();
            protect(&tx, SCOPE_CAPACITY, SCOPE_CREDITS).unwrap();
            tx.execute(
                "INSERT INTO scopes(generation,scope,producer,state) VALUES(1,0,0,zeroblob(?1))",
                [(HEADER_BYTES + SCOPE_CAPACITY) as i64],
            )
            .unwrap();
            initialize(
                &tx,
                Target {
                    table: Table::Scope,
                    row: tx.last_insert_rowid(),
                },
                &scopes::ScopeState::empty(),
                SCOPE_CAPACITY,
                SCOPE_CREDITS,
            )
            .unwrap();
            protect(&tx, capacity.max(FENCE_CAPACITY), 2 + FENCE_CREDITS).unwrap();
            tx.execute("INSERT INTO work(generation,scope,producer,entity,view,fence) VALUES(1,0,0,1,zeroblob(?1),zeroblob(?2))",
                [(capacity+HEADER_BYTES) as i64, (FENCE_CAPACITY+HEADER_BYTES) as i64]).unwrap();
            initialize(
                &tx,
                Target {
                    table: Table::WorkFence,
                    row: tx.last_insert_rowid(),
                },
                &None::<settlement::WorkFence>,
                FENCE_CAPACITY,
                FENCE_CREDITS,
            )
            .unwrap();
            let target = Target {
                table: Table::Work,
                row: tx.last_insert_rowid(),
            };
            initialize(&tx, target, &Number(0), capacity, 2).unwrap();
            tx.commit().unwrap();
            connection
                .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
                .unwrap();
            let reader = Connection::open_with_flags_and_vfs(
                &path,
                OpenFlags::SQLITE_OPEN_READ_WRITE,
                GUARDED_VFS,
            )
            .unwrap();
            reader
                .execute_batch("BEGIN; SELECT count(*) FROM work")
                .unwrap();
            let pages: u64 = connection
                .query_row("PRAGMA page_count", [], |r| number(r, 0))
                .unwrap();
            connection
                .pragma_update(None, "max_page_count", sql(pages).unwrap())
                .unwrap();
            connection.execute_batch("CREATE TEMP TRIGGER forbid_record_update BEFORE UPDATE ON main.work BEGIN SELECT RAISE(ABORT,'row replacement forbidden'); END").unwrap();
            let tx = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            replace(&tx, target, Id(1), &Number(MAX_NUMBER), true).unwrap();
            replace(&tx, CLOCK, Id(1), &Number(1), false).unwrap();
            tx.commit().unwrap();
            let actual = guard.usage().unwrap().wal_bytes;
            let bound = rewrite_bytes(capacity, page).unwrap();
            assert!(
                actual > 0 && actual <= bound,
                "page={page} capacity={capacity} actual={actual} bound={bound}"
            );
            assert_eq!(
                connection
                    .query_row("PRAGMA page_count", [], |r| number(r, 0))
                    .unwrap(),
                pages
            );
            let tx = connection.transaction().unwrap();
            let (retained, value): (_, Number) = read(&tx, target).unwrap();
            assert_eq!((retained.credits, value), (1, Number(MAX_NUMBER)));
            eprintln!("record rewrite: page={page} capacity={capacity} WAL={actual} bound={bound}");
        }
    }
}

#[test]
fn growth_preserves_exact_body_revision_and_other_records_across_restart() {
    let (fixture, binding, target) = declared();
    fixture
        .store
        .declare(
            &binding.identity,
            OperationId([2; 16]),
            Number(0),
            &[Id(2)],
            false,
        )
        .unwrap();
    let other = work_slot(&fixture.store, binding.identity.generation, Id(2));
    let mut reader = fixture.store.connect().unwrap();
    let snapshot = reader.transaction().unwrap();
    let (old, expected): (_, WorkView) = read(&snapshot, target).unwrap();
    let mut writer = fixture.store.connect().unwrap();
    let mut tx = writer
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let capacity = super::super::ingress::response_capacity(BatchCount(256)).unwrap() as usize;
    grow(&mut tx, target, Id(1), capacity, 4).unwrap();
    // Identical preparation consumes neither another revision nor another credit.
    grow(&mut tx, target, Id(1), capacity, 4).unwrap();
    let (funded, actual): (_, WorkView) = read(&tx, target).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(
        (funded.revision, funded.capacity, funded.credits),
        (old.revision, capacity, 4)
    );
    assert_eq!(header(&tx, other).unwrap().capacity, WORK_CAPACITY);
    assert_eq!(header(&tx, other).unwrap().credits, WORK_CREDITS);
    tx.commit().unwrap();
    assert_eq!(header(&snapshot, target).unwrap().capacity, WORK_CAPACITY);
    drop(snapshot);
    let reopened = reopen(&fixture.directory.path().join("authority.sqlite"));
    let connection = reopened.connect().unwrap();
    let (retained, actual): (_, WorkView) = read(&connection, target).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(
        (retained.revision, retained.capacity, retained.credits),
        (Id(1), capacity, 4)
    );
    reopened.integrity_check().unwrap();
}

#[test]
fn growth_refuses_stale_revision_released_promises_overflow_and_corrupt_source() {
    let (fixture, _, target) = declared();
    let mut connection = fixture.store.connect().unwrap();
    let mut tx = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    for (revision, capacity, credits, code) in [
        (Id(2), 4096, 2, ErrorCode::Conflict),
        (Id(1), WORK_CAPACITY - 1, 2, ErrorCode::LimitExceeded),
        (Id(1), 4096, 1, ErrorCode::LimitExceeded),
        (Id(1), MAX_CONTROL_LIMIT + 1, 2, ErrorCode::LimitExceeded),
        (Id(1), 4096, MAX_NUMBER, ErrorCode::LimitExceeded),
    ] {
        let result = grow(&mut tx, target, revision, capacity, credits);
        assert!(
            matches!(result, Err(StoreError::Protocol(Error {code: actual, ..})) if actual == code)
        );
        let retained = header(&tx, target).unwrap();
        assert_eq!(
            (retained.revision, retained.capacity, retained.credits),
            (Id(1), WORK_CAPACITY, WORK_CREDITS)
        );
    }
    let mut blob = tx
        .blob_open("main", "work", "view", target.row, false)
        .unwrap();
    blob.write_at(&[0xff], HEADER_BYTES).unwrap();
    blob.close().unwrap();
    assert!(matches!(
        grow(&mut tx, target, Id(1), 4096, 2),
        Err(StoreError::Corrupt(_))
    ));
    tx.rollback().unwrap();
    fixture.store.integrity_check().unwrap();
}

#[test]
fn failed_growth_after_resize_restores_the_original_record_even_if_caller_commits() {
    let (fixture, _, target) = declared();
    let mut connection = fixture.store.connect().unwrap();
    // The SQL resize succeeds, then this test-only trigger changes its length.
    // The subsequent BLOB initialization fails. The API must roll back both.
    connection.execute_batch("CREATE TEMP TRIGGER disrupt_growth AFTER UPDATE OF view ON main.work BEGIN UPDATE work SET view=zeroblob(105) WHERE rowid=NEW.rowid; END").unwrap();
    let mut tx = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let (_, before): (_, WorkView) = read(&tx, target).unwrap();
    assert!(matches!(
        grow(&mut tx, target, Id(1), 16384, 4),
        Err(StoreError::Corrupt(_))
    ));
    let (retained, after): (_, WorkView) = read(&tx, target).unwrap();
    assert_eq!(after, before);
    assert_eq!(
        (retained.revision, retained.capacity, retained.credits),
        (Id(1), WORK_CAPACITY, WORK_CREDITS)
    );
    tx.commit().unwrap();
    connection
        .execute_batch("DROP TRIGGER disrupt_growth")
        .unwrap();
    let mut tx = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    grow(&mut tx, target, Id(1), 16384, 4).unwrap();
    tx.commit().unwrap();
    fixture.store.integrity_check().unwrap();
}

#[test]
fn physical_growth_exhaustion_keeps_the_prior_committed_reservation() {
    let (fixture, _, target) = declared();
    let mut connection = fixture.store.connect().unwrap();
    let pages: u64 = connection
        .query_row("PRAGMA page_count", [], |r| number(r, 0))
        .unwrap();
    connection
        .pragma_update(None, "max_page_count", sql(pages).unwrap())
        .unwrap();
    let mut tx = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let result = grow(&mut tx, target, Id(1), MAX_CONTROL_LIMIT, 4);
    assert!(
        matches!(
            result,
            Err(StoreError::Protocol(Error {
                code: ErrorCode::LimitExceeded,
                ..
            }))
        ),
        "{result:?}"
    );
    drop(tx); // SQLite may have rolled back the outer transaction on SQLITE_FULL.
    let connection = fixture.store.connect().unwrap();
    let retained = header(&connection, target).unwrap();
    assert_eq!(
        (retained.revision, retained.capacity, retained.credits),
        (Id(1), WORK_CAPACITY, WORK_CREDITS)
    );
    fixture.store.integrity_check().unwrap();
}

pub(super) fn growth_crash_point(point: &str) {
    if std::env::var("PIPESTREAM_RECORD_TEST_CRASH")
        .ok()
        .as_deref()
        == Some(&format!("grow-{point}"))
    {
        std::process::exit(87);
    }
}

struct FixedClock;
impl Clock for FixedClock {
    fn read(&self) -> ClockReading {
        ClockReading {
            utc_ms: Number(1000),
            trusted: true,
        }
    }
}
struct Allowed;
impl Authorization for Allowed {
    fn permits(&self, owner: &IdentityLabel, _: Permission) -> bool {
        owner.0 == "alice"
    }
}

fn try_reopen(path: &std::path::Path) -> Result<AuthorityStore> {
    AuthorityStore::open(
        path,
        IdentityLabel("test-authority".into()),
        super::super::tests::policy(),
        PhysicalLimits::default(),
        Arc::new(FixedClock),
        Arc::new(Allowed),
    )
}

fn reopen(path: &std::path::Path) -> AuthorityStore {
    try_reopen(path).unwrap()
}

#[test]
fn restart_refuses_corrupt_bodies_even_with_intact_charge_headers() {
    for offset in [HEADER_BYTES, HEADER_BYTES + WORK_CAPACITY - 1] {
        let (fixture, _, target) = declared();
        let mut connection = fixture.store.connect().unwrap();
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let mut blob = tx
            .blob_open("main", "work", "view", target.row, false)
            .unwrap();
        let mut byte = [0];
        blob.read_at_exact(&mut byte, offset).unwrap();
        byte[0] ^= 1;
        blob.write_at(&byte, offset).unwrap();
        blob.close().unwrap();
        tx.commit().unwrap();
        assert!(matches!(
            try_reopen(&fixture.directory.path().join("authority.sqlite")),
            Err(StoreError::Corrupt(_))
        ));
        assert!(matches!(
            fixture.store.integrity_check(),
            Err(StoreError::Corrupt(_))
        ));
    }
}

#[test]
fn record_crash_child() {
    let Ok(boundary) = std::env::var("PIPESTREAM_RECORD_TEST_CRASH") else {
        return;
    };
    let path = std::path::PathBuf::from(std::env::var_os("PIPESTREAM_RECORD_TEST_PATH").unwrap());
    let store = reopen(&path);
    let target = work_slot(&store, Id(1), Id(1));
    let mut connection = store.connect().unwrap();
    let mut tx = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let (header, view): (_, WorkView) = read(&tx, target).unwrap();
    if boundary.starts_with("scope-") {
        let scope = Target {
            table: Table::Scope,
            row: tx
                .query_row(
                    "SELECT rowid FROM scopes WHERE generation=1 AND scope=0",
                    [],
                    |r| r.get(0),
                )
                .unwrap(),
        };
        let (retained, mut state): (_, scopes::ScopeState) = read(&tx, scope).unwrap();
        state.cancelled = true;
        state.revoked = true;
        replace(&tx, scope, retained.revision, &state, true).unwrap();
        let (clock, previous): (_, Number) = read(&tx, CLOCK).unwrap();
        replace(&tx, CLOCK, clock.revision, &Number(previous.0 + 1), false).unwrap();
        if boundary == "scope-before" {
            std::process::exit(87);
        }
        tx.commit().unwrap();
        if boundary == "scope-after" {
            std::process::exit(87);
        }
        panic!("scope crash boundary not reached");
    }
    if boundary.starts_with("grow-") {
        grow(&mut tx, target, header.revision, 16384, 4).unwrap();
        growth_crash_point("before-commit");
        tx.commit().unwrap();
        growth_crash_point("after-commit");
        panic!("growth crash boundary not reached");
    }
    replace(&tx, target, header.revision, &view, true).unwrap();
    let (clock, previous): (_, Number) = read(&tx, CLOCK).unwrap();
    replace(&tx, CLOCK, clock.revision, &Number(previous.0 + 1), false).unwrap();
    if boundary == "before" {
        std::process::exit(87);
    }
    tx.commit().unwrap();
    std::process::exit(87);
}

#[test]
fn crash_brackets_credit_spending_and_reopen_reconstructs_remaining_funding() {
    for boundary in ["before", "after"] {
        let (fixture, _, target) = declared();
        let path = fixture.directory.path().join("authority.sqlite");
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "v2::authority::records::tests::record_crash_child",
                "--nocapture",
            ])
            .env("PIPESTREAM_RECORD_TEST_CRASH", boundary)
            .env("PIPESTREAM_RECORD_TEST_PATH", &path)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(87));
        let store = reopen(&path);
        let mut connection = store.connect().unwrap();
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let (retained, view): (_, WorkView) = read(&tx, target).unwrap();
        let spent = u64::from(boundary == "after");
        let (clock, time): (_, Number) = read(&tx, CLOCK).unwrap();
        assert_eq!(
            (clock.revision, time),
            (Id(1 + spent), Number(1000 + spent))
        );
        assert_eq!(
            (retained.revision, retained.credits),
            (Id(1 + spent), WORK_CREDITS - spent)
        );
        protect(&tx, 0, 0).unwrap();
        replace(&tx, target, retained.revision, &view, true).unwrap();
        tx.commit().unwrap();
        store.integrity_check().unwrap();
    }
}

#[test]
fn crash_during_growth_never_exposes_a_half_initialized_or_unfunded_record() {
    for boundary in [
        "grow-resized",
        "grow-written",
        "grow-before-commit",
        "grow-after-commit",
    ] {
        let (fixture, _, target) = declared();
        let path = fixture.directory.path().join("authority.sqlite");
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "v2::authority::records::tests::record_crash_child",
                "--nocapture",
            ])
            .env("PIPESTREAM_RECORD_TEST_CRASH", boundary)
            .env("PIPESTREAM_RECORD_TEST_PATH", &path)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(87), "{boundary}");
        let store = reopen(&path);
        let connection = store.connect().unwrap();
        let (retained, view): (_, WorkView) = read(&connection, target).unwrap();
        assert_eq!((retained.revision, view.state), (Id(1), State::DECLARED));
        assert_eq!(
            (retained.capacity, retained.credits),
            if boundary == "grow-after-commit" {
                (16384, 4)
            } else {
                (WORK_CAPACITY, WORK_CREDITS)
            },
            "{boundary}"
        );
        store.integrity_check().unwrap();
    }
}

#[test]
fn shared_clock_counter_preserves_every_promised_record_observation() {
    let (fixture, _, work) = declared();
    let mut connection = fixture.store.connect().unwrap();
    let mut tx = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let scope = Target {
        table: Table::Scope,
        row: tx
            .query_row("SELECT rowid FROM scopes", [], |r| r.get(0))
            .unwrap(),
    };
    let fence = Target {
        table: Table::WorkFence,
        row: work.row,
    };
    let promised = WORK_CREDITS + SCOPE_CREDITS + FENCE_CREDITS;
    write(
        &tx,
        CLOCK,
        &Number(1000),
        Id(MAX_NUMBER - promised),
        CLOCK_CAPACITY,
        0,
    )
    .unwrap();
    assert!(matches!(
        protect(&tx, WORK_CAPACITY, 1),
        Err(StoreError::Protocol(Error {
            code: ErrorCode::LimitExceeded,
            ..
        }))
    ));
    assert!(matches!(
        grow(&mut tx, work, Id(1), WORK_CAPACITY, WORK_CREDITS + 1),
        Err(StoreError::Protocol(Error {
            code: ErrorCode::LimitExceeded,
            ..
        }))
    ));
    assert!(matches!(
        replace(&tx, CLOCK, Id(MAX_NUMBER - promised), &Number(1001), false),
        Err(StoreError::Protocol(Error {
            code: ErrorCode::LimitExceeded,
            ..
        }))
    ));
    for (index, target) in std::iter::repeat_n(work, WORK_CREDITS as usize)
        .chain(std::iter::repeat_n(scope, SCOPE_CREDITS as usize))
        .chain(std::iter::repeat_n(fence, FENCE_CREDITS as usize))
        .enumerate()
    {
        match target.table {
            Table::Work => {
                let (old, view): (_, WorkView) = read(&tx, target).unwrap();
                replace(&tx, target, old.revision, &view, true).unwrap();
            }
            Table::Scope => {
                let (old, state): (_, scopes::ScopeState) = read(&tx, target).unwrap();
                replace(&tx, target, old.revision, &state, true).unwrap();
            }
            Table::WorkFence => {
                let (old, fence): (_, Option<settlement::WorkFence>) = read(&tx, target).unwrap();
                replace(&tx, target, old.revision, &fence, true).unwrap();
            }
            Table::Clock | Table::Job | Table::Retirement => unreachable!(),
        }
        let clock = header(&tx, CLOCK).unwrap();
        let revision = replace(
            &tx,
            CLOCK,
            clock.revision,
            &Number(1001 + index as u64),
            false,
        )
        .unwrap();
        assert_eq!(revision, Id(MAX_NUMBER - promised + index as u64 + 1));
    }
    assert_eq!(header(&tx, CLOCK).unwrap().revision, Id(MAX_NUMBER));
    tx.commit().unwrap();
    fixture.store.integrity_check().unwrap();
}

struct AtTime(u64);
impl Clock for AtTime {
    fn read(&self) -> ClockReading {
        ClockReading {
            utc_ms: Number(self.0),
            trusted: true,
        }
    }
}

#[test]
fn empty_scope_closure_spends_its_clock_reservation_at_counter_exhaustion() {
    let mut fixture = super::super::tests::Fixture::new();
    let binding = fixture.create();
    let mut connection = fixture.store.connect().unwrap();
    let tx = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    write(
        &tx,
        CLOCK,
        &Number(1000),
        Id(MAX_NUMBER - SCOPE_CREDITS),
        CLOCK_CAPACITY,
        0,
    )
    .unwrap();
    tx.commit().unwrap();
    fixture.store.clock = Arc::new(AtTime(1001));
    let receipt = fixture
        .store
        .declare(
            &binding.identity,
            OperationId([1; 16]),
            Number(0),
            &[],
            true,
        )
        .unwrap();
    let Outcome::Declared {
        seal: Some(seal), ..
    } = receipt.body
    else {
        panic!("missing committed seal");
    };
    let summary = fixture
        .store
        .checkpoint(&binding.identity, Number(0), seal)
        .unwrap()
        .unwrap();
    assert_eq!(summary.closed_at, Number(1001));
    let (clock, time): (_, Number) = read(&connection, CLOCK).unwrap();
    assert_eq!(
        (clock.revision, time),
        (Id(MAX_NUMBER - SCOPE_CREDITS + 1), Number(1001))
    );
    fixture.store.integrity_check().unwrap();
}

#[test]
fn scope_fence_and_clock_updates_need_no_page_growth_or_sql_row_replacement() {
    let (mut fixture, binding, _) = declared();
    fixture.store.clock = Arc::new(AtTime(1001));
    let mut connection = fixture.store.connect().unwrap();
    let pages: u64 = connection
        .query_row("PRAGMA page_count", [], |r| number(r, 0))
        .unwrap();
    connection
        .pragma_update(None, "max_page_count", sql(pages).unwrap())
        .unwrap();
    connection.execute_batch("CREATE TEMP TRIGGER forbid_scope_row BEFORE UPDATE ON main.scopes BEGIN SELECT RAISE(ABORT,'scope row replacement forbidden'); END;
        CREATE TEMP TRIGGER forbid_clock_row BEFORE UPDATE ON main.authority BEGIN SELECT RAISE(ABORT,'clock row replacement forbidden'); END;").unwrap();
    let tx = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let target = Target {
        table: Table::Scope,
        row: tx
            .query_row("SELECT rowid FROM scopes", [], |r| r.get(0))
            .unwrap(),
    };
    let (before, mut state): (_, scopes::ScopeState) = read(&tx, target).unwrap();
    let now = fixture.store.check_clock(&tx).unwrap();
    let mut seal =
        ScopeSeal::new(&binding.identity, Number(0), Producer(0), None, Number(1)).unwrap();
    seal.push(Id(1)).unwrap();
    state.seal = Some(seal.finish().unwrap());
    state.cancelled = true;
    state.revoked = true;
    // Private storage transition only, not descendant settlement or a revoke RPC.
    replace(&tx, target, before.revision, &state, true).unwrap();
    fixture.store.remember_clock(&tx, now).unwrap();
    tx.commit().unwrap();
    assert_eq!(
        connection
            .query_row("PRAGMA page_count", [], |r| number(r, 0))
            .unwrap(),
        pages
    );
    let (after, actual): (_, scopes::ScopeState) = read(&connection, target).unwrap();
    assert_eq!(actual, state);
    assert_eq!(after.credits, before.credits - 1);
    assert!(matches!(
        fixture
            .store
            .operation(&binding.identity, OperationId([1; 16])),
        Err(StoreError::Protocol(Error {
            code: ErrorCode::Unauthorized,
            ..
        }))
    ));
    fixture.store.integrity_check().unwrap();
}

#[test]
fn restart_refuses_clock_corruption_or_scope_timestamps_ahead_of_its_clock() {
    for corruption in ["header", "body", "past"] {
        let fixture = super::super::tests::Fixture::new();
        let binding = fixture.create();
        fixture
            .store
            .declare(
                &binding.identity,
                OperationId([1; 16]),
                Number(0),
                &[],
                true,
            )
            .unwrap();
        let mut connection = fixture.store.connect().unwrap();
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        if corruption == "past" {
            write(&tx, CLOCK, &Number(999), Id(1), CLOCK_CAPACITY, 0).unwrap();
        } else {
            let mut blob = tx
                .blob_open("main", "authority", "clock", 1, false)
                .unwrap();
            let offset = if corruption == "header" {
                8
            } else {
                HEADER_BYTES
            };
            let mut byte = [0];
            blob.read_at_exact(&mut byte, offset).unwrap();
            byte[0] ^= 1;
            blob.write_at(&byte, offset).unwrap();
            blob.close().unwrap();
        }
        tx.commit().unwrap();
        assert!(
            matches!(fixture.store.integrity_check(), Err(StoreError::Corrupt(_))),
            "{corruption}"
        );
        assert!(
            matches!(
                try_reopen(&fixture.directory.path().join("authority.sqlite")),
                Err(StoreError::Corrupt(_))
            ),
            "{corruption}"
        );
    }
}

#[test]
fn scope_state_capacity_includes_maximum_summary_and_fence_fields() {
    let state = scopes::ScopeState {
        last_entity: Number(MAX_NUMBER),
        declared: Number(MAX_NUMBER),
        seal: Some(Digest([9; 32])),
        cancelled: true,
        revoked: false,
        summary: Some(ScopeSummary {
            scope: Number(MAX_NUMBER),
            producer: Producer(1),
            parent: Some(WorkKey {
                scope: Number(MAX_NUMBER - 1),
                producer: Producer(1),
                entity: Id(MAX_NUMBER),
            }),
            seal: Digest([9; 32]),
            declared: Number(MAX_NUMBER),
            counts: Counts {
                success: Number(MAX_NUMBER),
                failure: Number(0),
                cancelled: Number(0),
                skipped: Number(0),
            },
            status_root: Digest([8; 32]),
            closed_at: Number(MAX_NUMBER),
        }),
    };
    let bytes = pack(&state).unwrap();
    assert!(bytes.len() < SCOPE_CAPACITY);
    assert_eq!(unpack::<scopes::ScopeState>(&bytes).unwrap(), state);
}

#[test]
fn scope_fence_and_clock_commit_or_rollback_together_across_process_death() {
    for boundary in ["scope-before", "scope-after"] {
        let (fixture, _, work) = declared();
        let path = fixture.directory.path().join("authority.sqlite");
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "v2::authority::records::tests::record_crash_child",
                "--nocapture",
            ])
            .env("PIPESTREAM_RECORD_TEST_CRASH", boundary)
            .env("PIPESTREAM_RECORD_TEST_PATH", &path)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(87));
        let store = reopen(&path);
        let connection = store.connect().unwrap();
        let target = Target {
            table: Table::Scope,
            row: connection
                .query_row("SELECT rowid FROM scopes", [], |r| r.get(0))
                .unwrap(),
        };
        let (header, state): (_, scopes::ScopeState) = read(&connection, target).unwrap();
        let (clock, time): (_, Number) = read(&connection, CLOCK).unwrap();
        let spent = u64::from(boundary == "scope-after");
        assert_eq!((state.cancelled, state.revoked), (spent == 1, spent == 1));
        assert_eq!(
            (header.revision, header.credits),
            (Id(2 + spent), SCOPE_CREDITS - spent)
        );
        assert_eq!(
            (clock.revision, time),
            (Id(1 + spent), Number(1000 + spent))
        );
        assert_eq!(
            self::header(&connection, work).unwrap().credits,
            WORK_CREDITS
        );
        store.integrity_check().unwrap();
    }
}
