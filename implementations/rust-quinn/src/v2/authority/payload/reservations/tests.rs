use super::*;

fn policy() -> PayloadPolicy {
    PayloadPolicy {
        objects: Id(8),
        bytes: Number(4 << 20),
        owner_objects: Id(4),
        owner_bytes: Number(2 << 20),
        chunk_bytes: Id(65536),
        handles: Id(16),
        owner_handles: Id(8),
    }
}
fn owner(name: &str) -> IdentityLabel {
    IdentityLabel(name.into())
}
fn budget() -> OutputBudget {
    OutputBudget {
        count: BatchCount(2),
        total_bytes: Number(1024),
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
        object_limit: Number(2 << 20),
        stream_idle_ms: IdleMs(1000),
        stream_lifetime_ms: LifetimeMs(10000),
    }
}
fn input(bytes: &[u8]) -> Input {
    Input {
        length: Number(bytes.len() as u64),
        sha256: Digest(Sha256::digest(bytes).into()),
        content_type: ApplicationLabel("application/octet-stream".into()),
    }
}
fn refuse<T>(result: Result<T>, code: ErrorCode) {
    match result {
        Err(StoreError::Protocol(error)) => assert_eq!(error.code, code),
        Err(e) => panic!("wrong refusal {e:?}"),
        Ok(_) => panic!("expected {code:?}"),
    }
}
struct Fixture {
    directory: tempfile::TempDir,
    store: PayloadStore,
    binding: StoreIdentity,
}
impl Fixture {
    fn new() -> Self {
        Self::with_policy(policy())
    }
    fn with_policy(policy: PayloadPolicy) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let binding = StoreIdentity::generate().unwrap();
        let store =
            PayloadStore::initialize(&directory.path().join("objects"), binding, policy).unwrap();
        Self {
            directory,
            store,
            binding,
        }
    }
    fn reserve(&self) -> OutputReservation {
        self.store
            .reserve_outputs(&owner("alice"), &budget())
            .unwrap()
    }
}
fn output(
    reservation: &OutputReservation,
    index: u64,
    maximum: u64,
    bytes: &[u8],
) -> InstalledPayload {
    let now = Instant::now();
    let mut stage = reservation
        .stage(
            OutputIndex(index),
            Number(maximum),
            input(bytes).content_type,
            &caps(),
            now,
        )
        .unwrap();
    for chunk in bytes.chunks(65536) {
        stage.write(chunk, now).unwrap();
    }
    stage.finish(now).unwrap()
}
fn collect_all(store: &PayloadStore) {
    for _ in 0..4 {
        store.collect(None, 256, |_| Ok(false)).unwrap();
    }
    assert_eq!(store.usage(None).unwrap().objects, 0);
}

#[test]
fn output_reservation_is_durable_and_unknown_length_output_installs_exactly() {
    let fixture = Fixture::new();
    let reservation = fixture.reserve();
    let key = reservation.key().to_owned();
    let promised = fixture.store.usage(None).unwrap();
    assert_eq!(
        (promised.objects, promised.charged_bytes),
        (3, 1024 + 3 * OVERHEAD)
    );
    let installed = output(&reservation, 0, 1024, b"hello");
    assert_eq!(installed.descriptor(), &input(b"hello"));
    let object = installed.key().to_owned();
    assert_eq!(
        reservation.usage().unwrap(),
        ReservationUsage {
            outputs: 1,
            allocated_bytes: 5
        }
    );
    assert_eq!(fixture.store.usage(None).unwrap(), promised);
    drop(installed);
    drop(reservation);
    let root = fixture.directory.path().join("objects");
    drop(fixture.store);
    let store = PayloadStore::open(&root, fixture.binding, policy()).unwrap();
    assert_eq!(store.usage(None).unwrap(), promised);
    let retained = store
        .open_reservation(&key, &owner("alice"), &budget())
        .unwrap();
    assert_eq!(
        retained.usage().unwrap(),
        ReservationUsage {
            outputs: 1,
            allocated_bytes: 5
        }
    );
    let mut reader = store
        .open_object(&object, &owner("alice"), &input(b"hello"))
        .unwrap();
    let mut bytes = [0; 5];
    assert_eq!(reader.read_chunk(&mut bytes).unwrap(), 5);
    assert_eq!(&bytes, b"hello");
    assert_eq!(reader.read_chunk(&mut bytes).unwrap(), 0);
    assert!(reader.verified());
    drop(reader);
    drop(retained);
    collect_all(&store);
}

#[test]
fn ordinary_uploads_cannot_consume_reserved_bytes_or_slots() {
    let fixture = Fixture::new();
    let reservation = fixture.reserve();
    let now = Instant::now();
    let maximum = (2 << 20) - (1024 + 3 * OVERHEAD) - OVERHEAD;
    let descriptor = Input {
        length: Number(maximum),
        ..input(b"")
    };
    let upload = fixture
        .store
        .stage(&owner("alice"), &descriptor, &caps(), now)
        .unwrap();
    refuse(
        fixture
            .store
            .stage(&owner("alice"), &input(b""), &caps(), now),
        ErrorCode::LimitExceeded,
    );
    // Promised production uses existing capacity even while ordinary upload
    // capacity is exhausted. It is not charged a second time.
    let first = output(&reservation, 0, 512, &[7; 512]);
    let second = output(&reservation, 1, 512, &[8; 512]);
    assert_eq!(reservation.usage().unwrap().allocated_bytes, 1024);
    drop(first);
    drop(second);
    drop(upload);
    drop(reservation);
    collect_all(&fixture.store);
}

#[test]
fn partial_outputs_hold_their_maximum_and_unused_bytes_stay_inside_the_reservation() {
    let fixture = Fixture::new();
    let reservation = fixture.reserve();
    let now = Instant::now();
    let mut stage = reservation
        .stage(
            OutputIndex(0),
            Number(1000),
            input(b"").content_type,
            &caps(),
            now,
        )
        .unwrap();
    stage.write(b"abc", now).unwrap();
    assert_eq!(reservation.usage().unwrap().allocated_bytes, 1000);
    refuse(
        reservation.stage(
            OutputIndex(1),
            Number(25),
            input(b"").content_type,
            &caps(),
            now,
        ),
        ErrorCode::LimitExceeded,
    );
    let installed = stage.finish(now).unwrap();
    let second = reservation
        .stage(
            OutputIndex(1),
            Number(1021),
            input(b"").content_type,
            &caps(),
            now,
        )
        .unwrap();
    assert_eq!(reservation.usage().unwrap().allocated_bytes, 1024);
    assert_eq!(
        fixture.store.usage(None).unwrap().charged_bytes,
        1024 + 3 * OVERHEAD
    );
    drop(second);
    drop(installed);
    drop(reservation);
    collect_all(&fixture.store);
}

#[test]
fn output_errors_poison_staging_and_never_install_a_successful_prefix() {
    for reason in ["bytes", "chunk", "idle", "lifetime", "clock"] {
        let fixture = Fixture::new();
        let reservation = fixture.reserve();
        let now = Instant::now();
        let mut stage = reservation
            .stage(
                OutputIndex(0),
                Number(2),
                input(b"").content_type,
                &caps(),
                now,
            )
            .unwrap();
        let result = match reason {
            "bytes" => stage.write(b"abc", now),
            "chunk" => stage.write(&vec![0; 65537], now),
            "idle" => stage.check_deadline(now + Elapsed::from_millis(1000)),
            "lifetime" => stage.check_deadline(now + Elapsed::from_millis(10000)),
            _ => stage.check_deadline(now - Elapsed::from_millis(1)),
        };
        assert!(result.is_err(), "{reason}");
        refuse(stage.write(b"", now), ErrorCode::IntegrityError);
        refuse(stage.finish(now), ErrorCode::IntegrityError);
        assert_eq!(reservation.usage().unwrap().outputs, 0);
        assert_eq!(fixture.store.usage(None).unwrap().objects, 3);
    }
}

#[test]
fn retained_binding_slot_uniqueness_and_live_pins_exclude_conflicting_use_and_cleanup() {
    let fixture = Fixture::new();
    let reservation = fixture.reserve();
    let now = Instant::now();
    refuse(
        fixture
            .store
            .open_reservation(reservation.key(), &owner("bob"), &budget()),
        ErrorCode::OutputUnavailable,
    );
    refuse(
        fixture.store.open_reservation(
            reservation.key(),
            &owner("alice"),
            &OutputBudget {
                count: BatchCount(2),
                total_bytes: Number(1025),
            },
        ),
        ErrorCode::OutputUnavailable,
    );
    let first = output(&reservation, 0, 10, b"abc");
    drop(first);
    assert_eq!(
        fixture
            .store
            .collect(None, 256, |_| Ok(false))
            .unwrap()
            .removed,
        0
    );
    refuse(
        reservation.stage(
            OutputIndex(0),
            Number(1),
            input(b"").content_type,
            &caps(),
            now,
        ),
        ErrorCode::Conflict,
    );
    refuse(
        reservation.stage(
            OutputIndex(2),
            Number(1),
            input(b"").content_type,
            &caps(),
            now,
        ),
        ErrorCode::LimitExceeded,
    );
    let key = reservation.key().to_owned();
    drop(reservation);
    // A committed reservation pins its outputs across process-handle loss.
    // Reclaiming unpublished outputs for a replacement worker needs a separate
    // lease-fenced operation, not the orphan collector.
    fixture
        .store
        .collect(None, 256, |candidate| Ok(candidate == key))
        .unwrap();
    assert_eq!(fixture.store.usage(None).unwrap().objects, 3);
    let reopened = fixture
        .store
        .open_reservation(&key, &owner("alice"), &budget())
        .unwrap();
    assert_eq!(reopened.usage().unwrap().outputs, 1);
    drop(reopened);
    collect_all(&fixture.store);
}

#[test]
fn zero_length_outputs_are_real_objects_and_zero_count_reservations_produce_none() {
    let fixture = Fixture::new();
    let budget = OutputBudget {
        count: BatchCount(2),
        total_bytes: Number(0),
    };
    let reservation = fixture
        .store
        .reserve_outputs(&owner("alice"), &budget)
        .unwrap();
    let first = output(&reservation, 0, 0, b"");
    let second = output(&reservation, 1, 0, b"");
    assert_eq!(first.descriptor().sha256, input(b"").sha256);
    assert_eq!(
        reservation.usage().unwrap(),
        ReservationUsage {
            outputs: 2,
            allocated_bytes: 0
        }
    );
    drop(first);
    drop(second);
    drop(reservation);
    collect_all(&fixture.store);
    let empty = fixture
        .store
        .reserve_outputs(
            &owner("alice"),
            &OutputBudget {
                count: BatchCount(0),
                total_bytes: Number(0),
            },
        )
        .unwrap();
    refuse(
        empty.stage(
            OutputIndex(0),
            Number(0),
            input(b"").content_type,
            &caps(),
            Instant::now(),
        ),
        ErrorCode::LimitExceeded,
    );
}

#[test]
fn handles_and_global_and_per_owner_forecasts_are_bounded() {
    let mut limits = policy();
    limits.handles = Id(2);
    limits.owner_handles = Id(2);
    let fixture = Fixture::with_policy(limits);
    let reservation = fixture.reserve();
    let second = fixture
        .store
        .open_reservation(reservation.key(), &owner("alice"), &budget())
        .unwrap();
    refuse(
        reservation.stage(
            OutputIndex(0),
            Number(1),
            input(b"").content_type,
            &caps(),
            Instant::now(),
        ),
        ErrorCode::LimitExceeded,
    );
    drop(second);
    let stage = reservation
        .stage(
            OutputIndex(0),
            Number(1),
            input(b"").content_type,
            &caps(),
            Instant::now(),
        )
        .unwrap();
    drop(stage);
    refuse(
        fixture.store.reserve_outputs(&owner("alice"), &budget()),
        ErrorCode::LimitExceeded,
    );
    let bob = fixture
        .store
        .reserve_outputs(&owner("bob"), &budget())
        .unwrap();
    refuse(
        fixture.store.reserve_outputs(&owner("carol"), &budget()),
        ErrorCode::LimitExceeded,
    );
    drop(bob);
    drop(reservation);
    collect_all(&fixture.store);
}

#[test]
fn restart_refuses_missing_or_corrupt_funding_without_adopting_its_outputs() {
    for tamper in ["missing", "checksum", "alias"] {
        let fixture = Fixture::new();
        let reservation = fixture.reserve();
        let installed = output(&reservation, 0, 3, b"abc");
        let path = path(&fixture.store.root, reservation.key(), false);
        drop(installed);
        drop(reservation);
        let root = fixture.directory.path().join("objects");
        drop(fixture.store);
        match tamper {
            "missing" => {
                fs::remove_file(&path).unwrap();
            }
            "checksum" => {
                let mut file = OpenOptions::new().write(true).open(&path).unwrap();
                file.write_all(b"bad").unwrap();
                file.sync_all().unwrap();
            }
            _ => {
                fs::hard_link(&path, fixture.directory.path().join("alias")).unwrap();
            }
        }
        assert!(
            PayloadStore::open(&root, fixture.binding, policy()).is_err(),
            "{tamper}"
        );
        assert_eq!(
            fs::read_dir(&root)
                .unwrap()
                .filter(|e| e
                    .as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with("object-"))
                .count(),
            1
        );
    }
}

pub(in crate::v2::authority::payload) fn crash_point(point: &str) {
    if std::env::var("PIPESTREAM_OUTPUT_TEST_CRASH")
        .ok()
        .as_deref()
        == Some(point)
    {
        std::process::exit(87);
    }
}

#[test]
fn maximum_output_count_and_labels_fit_with_only_two_live_handles() {
    let mut limits = policy();
    limits.objects = Id(300);
    limits.owner_objects = Id(300);
    limits.handles = Id(2);
    limits.owner_handles = Id(2);
    let fixture = Fixture::with_policy(limits.clone());
    let owner = owner(&"a".repeat(128));
    let budget = OutputBudget {
        count: BatchCount(256),
        total_bytes: Number(0),
    };
    let reservation = fixture.store.reserve_outputs(&owner, &budget).unwrap();
    let content_type = ApplicationLabel("b".repeat(128));
    let mut last = String::new();
    for index in 0..256 {
        let now = Instant::now();
        let installed = reservation
            .stage(
                OutputIndex(index),
                Number(0),
                content_type.clone(),
                &caps(),
                now,
            )
            .unwrap()
            .finish(now)
            .unwrap();
        last = installed.key().to_owned();
        drop(installed);
    }
    assert_eq!(
        reservation.usage().unwrap(),
        ReservationUsage {
            outputs: 256,
            allocated_bytes: 0
        }
    );
    assert_eq!(
        fixture
            .store
            .collect(None, 256, |_| Ok(false))
            .unwrap()
            .removed,
        0
    );
    let key = reservation.key().to_owned();
    drop(reservation);
    let root = fixture.directory.path().join("objects");
    drop(fixture.store);
    let store = PayloadStore::open(&root, fixture.binding, limits).unwrap();
    let retained = store.open_reservation(&key, &owner, &budget).unwrap();
    let descriptor = Input {
        content_type,
        ..input(b"")
    };
    let mut reader = store.open_object(&last, &owner, &descriptor).unwrap();
    assert_eq!(reader.read_chunk(&mut [0; 1]).unwrap(), 0);
    assert!(reader.verified());
    drop(reader);
    drop(retained);
    collect_all(&store);
}

#[test]
fn progress_never_extends_output_lifetime_and_empty_writes_do_not_extend_idle() {
    let fixture = Fixture::new();
    let reservation = fixture.reserve();
    let now = Instant::now();
    let mut stage = reservation
        .stage(
            OutputIndex(0),
            Number(1000),
            input(b"").content_type,
            &caps(),
            now,
        )
        .unwrap();
    for i in 1..=11 {
        stage
            .write(&[1], now + Elapsed::from_millis(900 * i))
            .unwrap();
    }
    refuse(
        stage.finish(now + Elapsed::from_millis(10000)),
        ErrorCode::LimitExceeded,
    );
    let mut stage = reservation
        .stage(
            OutputIndex(0),
            Number(0),
            input(b"").content_type,
            &caps(),
            now,
        )
        .unwrap();
    stage.write(b"", now + Elapsed::from_millis(999)).unwrap();
    refuse(
        stage.finish(now + Elapsed::from_millis(1000)),
        ErrorCode::LimitExceeded,
    );
    assert_eq!(reservation.usage().unwrap().outputs, 0);
}

#[test]
fn sqlite_references_preserve_reservation_and_referenced_bytes_until_each_reference_retires() {
    let authority = super::super::super::tests::Fixture::new();
    let binding = authority.create();
    authority
        .store
        .declare(
            &binding.identity,
            OperationId([1; 16]),
            Number(0),
            &[Id(1)],
            true,
        )
        .unwrap();
    let store = PayloadStore::initialize(
        &authority.directory.path().join("objects"),
        authority.store.payload_identity().unwrap(),
        policy(),
    )
    .unwrap();
    authority.store.bind_payloads(&store).unwrap();
    let reserve = store.reserve_outputs(&owner("alice"), &budget()).unwrap();
    let installed = output(&reserve, 0, 3, b"abc");
    let reservation = reserve.key().to_owned();
    let object = installed.key().to_owned();
    // Storage reference transactions, not a fabricated admission or manifest.
    let mut connection = authority.store.connect().unwrap();
    let tx = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    super::super::super::records::protect(&tx, 0, 0).unwrap();
    tx.execute(
        "INSERT INTO payload_refs VALUES(?1,?2,0,1,2)",
        params![reservation, sql(binding.identity.generation.0).unwrap()],
    )
    .unwrap();
    tx.execute(
        "INSERT INTO payload_refs VALUES(?1,?2,0,1,1)",
        params![object, sql(binding.identity.generation.0).unwrap()],
    )
    .unwrap();
    tx.commit().unwrap();
    drop(installed);
    drop(reserve);
    assert_eq!(
        authority
            .store
            .collect_payload_orphans(&store, None, 256)
            .unwrap()
            .removed,
        0
    );
    connection
        .execute(
            "DELETE FROM payload_refs WHERE object_key=?1",
            [&reservation],
        )
        .unwrap();
    assert_eq!(
        authority
            .store
            .collect_payload_orphans(&store, None, 256)
            .unwrap()
            .removed,
        0,
        "referenced output still needs its funding record"
    );
    connection
        .execute("DELETE FROM payload_refs WHERE object_key=?1", [&object])
        .unwrap();
    for _ in 0..3 {
        authority
            .store
            .collect_payload_orphans(&store, None, 256)
            .unwrap();
    }
    assert_eq!(store.usage(None).unwrap().objects, 0);
    assert_eq!(
        authority
            .store
            .work_view(
                &binding.identity,
                &WorkKey {
                    scope: Number(0),
                    producer: Producer(0),
                    entity: Id(1)
                },
                Number(0)
            )
            .unwrap()
            .1
            .state,
        State::DECLARED
    );
}

#[test]
fn filesystem_space_and_quota_exhaustion_have_the_named_capacity_refusal() {
    for code in [rustix::io::Errno::NOSPC, rustix::io::Errno::DQUOT] {
        refuse::<()>(
            Err(io(std::io::Error::from_raw_os_error(code.raw_os_error()))),
            ErrorCode::LimitExceeded,
        );
    }
}

#[test]
fn failed_namespace_installation_quarantines_the_root_until_exclusive_reopen() {
    let fixture = Fixture::new();
    let reservation = fixture.reserve();
    let now = Instant::now();
    let mut stage = reservation
        .stage(
            OutputIndex(0),
            Number(3),
            input(b"").content_type,
            &caps(),
            now,
        )
        .unwrap();
    stage.write(b"abc", now).unwrap();
    // An actual conflicting directory makes the filesystem rename fail. The
    // implementation treats namespace errors conservatively as uncertain.
    let conflict = fixture.store.root.path(&stage.key, false);
    fs::create_dir(&conflict).unwrap();
    assert!(stage.finish(now).is_err());
    assert!(
        fixture.store.root.owned().is_err(),
        "uncertain installation must quarantine the live root"
    );
    assert!(
        fixture
            .store
            .stage(&owner("alice"), &input(b""), &caps(), now)
            .is_err()
    );
    // Repair only this test's injected empty directory, then reopen under a
    // fresh exclusive lock. No installed object or accepted reference is deleted.
    fs::remove_dir(&conflict).unwrap();
    drop(reservation);
    let root = fixture.directory.path().join("objects");
    drop(fixture.store);
    let reopened = PayloadStore::open(&root, fixture.binding, policy()).unwrap();
    assert_eq!(reopened.usage(None).unwrap().objects, 3);
    let key = reopened
        .root
        .entries()
        .unwrap()
        .reservations
        .keys()
        .next()
        .unwrap()
        .clone();
    let reservation = reopened
        .open_reservation(&key, &owner("alice"), &budget())
        .unwrap();
    assert_eq!(reservation.usage().unwrap().outputs, 0);
}

#[test]
fn output_crash_child() {
    let Some(root) = std::env::var_os("PIPESTREAM_OUTPUT_TEST_PATH") else {
        return;
    };
    let root = Path::new(&root);
    let bytes = fs::read(root.join(CONFIG)).unwrap();
    let binding = StoreIdentity::from_bytes(bytes[8..24].try_into().unwrap()).unwrap();
    let store = PayloadStore::open(root, binding, policy()).unwrap();
    let point = std::env::var("PIPESTREAM_OUTPUT_TEST_CRASH").unwrap();
    if point == "reserve-unlinked" {
        store.collect(None, 256, |_| Ok(false)).unwrap();
    } else if point.starts_with("reserve-") {
        store.reserve_outputs(&owner("alice"), &budget()).unwrap();
    } else {
        let key = store
            .root
            .entries()
            .unwrap()
            .reservations
            .keys()
            .next()
            .unwrap()
            .clone();
        let reserve = store
            .open_reservation(&key, &owner("alice"), &budget())
            .unwrap();
        output(&reserve, 0, 3, b"abc");
    }
    panic!("crash boundary not reached");
}

#[test]
fn process_death_preserves_promises_and_distinguishes_installed_outputs_from_stages() {
    for point in [
        "reserve-created",
        "reserve-synced",
        "reserve-renamed",
        "reserve-directory-synced",
        "reserve-unlinked",
        "output-created",
        "output-header-slot",
        "output-written",
        "output-synced",
        "output-renamed",
        "output-directory-synced",
    ] {
        let fixture = Fixture::new();
        if point.starts_with("output-") || point == "reserve-unlinked" {
            drop(fixture.reserve());
        }
        let root = fixture.directory.path().join("objects");
        drop(fixture.store);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "v2::authority::payload::reservations::tests::output_crash_child",
                "--nocapture",
            ])
            .env("PIPESTREAM_OUTPUT_TEST_PATH", &root)
            .env("PIPESTREAM_OUTPUT_TEST_CRASH", point)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(87), "{point}");
        let store = PayloadStore::open(&root, fixture.binding, policy()).unwrap();
        let retained = point.starts_with("output-")
            || matches!(point, "reserve-renamed" | "reserve-directory-synced");
        assert_eq!(
            store.usage(None).unwrap().objects,
            if retained { 3 } else { 0 },
            "{point}"
        );
        if point.starts_with("output-") {
            let key = store
                .root
                .entries()
                .unwrap()
                .reservations
                .keys()
                .next()
                .unwrap()
                .clone();
            let reserve = store
                .open_reservation(&key, &owner("alice"), &budget())
                .unwrap();
            let installed = matches!(point, "output-renamed" | "output-directory-synced");
            assert_eq!(
                reserve.usage().unwrap().outputs,
                u64::from(installed),
                "{point}"
            );
            if installed {
                let object = store.root.entries().unwrap().keys().next().unwrap().clone();
                let mut reader = store
                    .open_object(&object, &owner("alice"), &input(b"abc"))
                    .unwrap();
                let mut bytes = [0; 3];
                assert_eq!(reader.read_chunk(&mut bytes).unwrap(), 3);
                assert_eq!(&bytes, b"abc");
                assert_eq!(reader.read_chunk(&mut bytes).unwrap(), 0);
                assert!(reader.verified());
            }
            drop(reserve);
        }
        collect_all(&store);
    }
}
