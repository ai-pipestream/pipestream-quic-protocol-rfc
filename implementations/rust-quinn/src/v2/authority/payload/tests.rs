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
    IdentityLabel(name.to_owned())
}
fn descriptor(bytes: &[u8]) -> Input {
    Input {
        length: Number(bytes.len() as u64),
        sha256: Digest(Sha256::digest(bytes).into()),
        content_type: ApplicationLabel("application/octet-stream".into()),
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
fn refuse<T>(result: Result<T>, expected: ErrorCode) {
    match result {
        Err(StoreError::Protocol(error)) => assert_eq!(error.code, expected),
        Err(other) => panic!("unexpected {other:?}"),
        Ok(_) => panic!("expected {expected:?}"),
    }
}
struct Fixture {
    directory: tempfile::TempDir,
    store: PayloadStore,
    binding: StoreIdentity,
}
impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let binding = StoreIdentity::generate().unwrap();
        let store =
            PayloadStore::initialize(&directory.path().join("objects"), binding, policy()).unwrap();
        Self {
            directory,
            store,
            binding,
        }
    }
    fn install(&self, bytes: &[u8]) -> InstalledPayload {
        let now = Instant::now();
        let mut stage = self
            .store
            .stage(&owner("alice"), &descriptor(bytes), &caps(), now)
            .unwrap();
        for chunk in bytes.chunks(65536) {
            stage.receive(chunk, now).unwrap();
        }
        stage.finish(now).unwrap()
    }
}

#[test]
fn streamed_install_is_pinned_verified_and_reconstructed_after_reopen() {
    let fixture = Fixture::new();
    let bytes: Vec<u8> = (0..200000).map(|i| (i % 251) as u8).collect();
    let installed = fixture.install(&bytes);
    let key = installed.key().to_owned();
    assert_eq!(installed.descriptor(), &descriptor(&bytes));
    assert_eq!(installed.binding(), fixture.binding);
    let mut reader = fixture
        .store
        .open_object(&key, &owner("alice"), &descriptor(&bytes))
        .unwrap();
    let mut output = Vec::new();
    let mut buffer = [0u8; 4096];
    while !reader.verified() {
        let n = reader.read_chunk(&mut buffer).unwrap();
        output.extend_from_slice(&buffer[..n]);
    }
    assert_eq!(output, bytes);
    assert_eq!(
        fixture
            .store
            .collect(None, 256, |_| Ok(false))
            .unwrap()
            .removed,
        0
    );
    drop(reader);
    drop(installed);
    let path = fixture.directory.path().join("objects");
    let binding = fixture.binding;
    drop(fixture.store);
    let reopened = PayloadStore::open(&path, binding, policy()).unwrap();
    assert_eq!(
        reopened.usage(None).unwrap().charged_bytes,
        bytes.len() as u64 + OVERHEAD
    );
    let mut reader = reopened
        .open_object(&key, &owner("alice"), &descriptor(&bytes))
        .unwrap();
    while reader.read_chunk(&mut buffer).unwrap() != 0 {}
    assert!(reader.verified());
    drop(reader);
    assert_eq!(
        reopened.collect(None, 256, |_| Ok(true)).unwrap().removed,
        0
    );
    assert_eq!(
        reopened.collect(None, 256, |_| Ok(false)).unwrap().removed,
        1
    );
    assert_eq!(reopened.usage(None).unwrap().objects, 0);
}

#[test]
fn partial_bad_digest_extra_bytes_and_expired_fin_never_install() {
    for case in 0..4 {
        let fixture = Fixture::new();
        let now = Instant::now();
        let mut stage = fixture
            .store
            .stage(&owner("alice"), &descriptor(b"abc"), &caps(), now)
            .unwrap();
        match case {
            0 => {
                stage.receive(b"ab", now).unwrap();
                refuse(stage.finish(now), ErrorCode::IntegrityError);
            }
            1 => {
                stage.receive(b"xyz", now).unwrap();
                refuse(stage.finish(now), ErrorCode::IntegrityError);
            }
            2 => {
                refuse(stage.receive(b"abcd", now), ErrorCode::IntegrityError);
                drop(stage);
            }
            _ => {
                stage.receive(b"abc", now).unwrap();
                refuse(
                    stage.finish(now + Elapsed::from_millis(1000)),
                    ErrorCode::LimitExceeded,
                );
            }
        }
        assert_eq!(fixture.store.usage(None).unwrap().objects, 0);
        assert_eq!(
            fs::read_dir(fixture.directory.path().join("objects"))
                .unwrap()
                .count(),
            2
        );
    }
}

#[test]
fn full_length_is_reserved_before_receiving_and_owner_limits_are_independent() {
    let fixture = Fixture::new();
    let now = Instant::now();
    let mut stages = Vec::new();
    for _ in 0..4 {
        stages.push(
            fixture
                .store
                .stage(&owner("alice"), &descriptor(b""), &caps(), now)
                .unwrap(),
        );
    }
    refuse(
        fixture
            .store
            .stage(&owner("alice"), &descriptor(b""), &caps(), now),
        ErrorCode::LimitExceeded,
    );
    assert_eq!(
        fixture
            .store
            .usage(Some(&owner("alice")))
            .unwrap()
            .charged_bytes,
        4 * OVERHEAD
    );
    for _ in 0..4 {
        stages.push(
            fixture
                .store
                .stage(&owner("bob"), &descriptor(b""), &caps(), now)
                .unwrap(),
        );
    }
    refuse(
        fixture
            .store
            .stage(&owner("carol"), &descriptor(b""), &caps(), now),
        ErrorCode::LimitExceeded,
    );
    drop(stages);
    assert_eq!(fixture.store.usage(None).unwrap().objects, 0);
    let large = Input {
        length: Number((2 << 20) - OVERHEAD),
        sha256: Digest([0; 32]),
        content_type: ApplicationLabel("x".into()),
    };
    let stage = fixture
        .store
        .stage(&owner("alice"), &large, &caps(), now)
        .unwrap();
    assert_eq!(fixture.store.usage(None).unwrap().charged_bytes, 2 << 20);
    refuse(
        fixture
            .store
            .stage(&owner("alice"), &descriptor(b""), &caps(), now),
        ErrorCode::LimitExceeded,
    );
    drop(stage);
}

#[test]
fn corrupt_retained_bytes_are_unavailable_not_verified() {
    let fixture = Fixture::new();
    let installed = fixture.install(b"abc");
    let key = installed.key().to_owned();
    drop(installed);
    let path = fixture.store.root.path(&key, false);
    let (_, offset) = read_header(&path, false).unwrap();
    let mut file = OpenOptions::new().write(true).open(path).unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(b"xyz").unwrap();
    file.sync_all().unwrap();
    let mut reader = fixture
        .store
        .open_object(&key, &owner("alice"), &descriptor(b"abc"))
        .unwrap();
    let mut buf = [0u8; 3];
    assert_eq!(reader.read_chunk(&mut buf).unwrap(), 3);
    refuse(reader.read_chunk(&mut buf), ErrorCode::OutputUnavailable);
    assert!(!reader.verified());
    refuse(
        fixture
            .store
            .open_object(&key, &owner("bob"), &descriptor(b"abc")),
        ErrorCode::OutputUnavailable,
    );
    refuse(
        fixture
            .store
            .open_object(&key, &owner("alice"), &descriptor(b"def")),
        ErrorCode::OutputUnavailable,
    );
}

#[test]
fn live_handles_own_root_and_wrong_bindings_never_adopt_it() {
    let fixture = Fixture::new();
    let path = fixture.directory.path().join("objects");
    refuse(
        PayloadStore::open(&path, fixture.binding, policy()),
        ErrorCode::LimitExceeded,
    );
    let installed = fixture.install(b"abc");
    let binding = fixture.binding;
    drop(fixture.store);
    refuse(
        PayloadStore::open(&path, binding, policy()),
        ErrorCode::LimitExceeded,
    );
    drop(installed);
    assert!(PayloadStore::open(&path, StoreIdentity::generate().unwrap(), policy()).is_err());
    let mut changed = policy();
    changed.bytes = Number(8 << 20);
    assert!(PayloadStore::open(&path, binding, changed).is_err());
    PayloadStore::open(&path, binding, policy()).unwrap();
}

#[test]
fn restart_reclaims_torn_stages_but_retains_installed_orphans() {
    let fixture = Fixture::new();
    let installed = fixture.install(b"abc");
    let key = installed.key().to_owned();
    drop(installed);
    let path = fixture.directory.path().join("objects");
    let binding = fixture.binding;
    drop(fixture.store);
    // A crash can leave the file before even the fixed header was written.
    let mut torn = new_file(&path.join("stage-0123456789abcdef0123456789abcdef")).unwrap();
    torn.write_all(b"PS").unwrap();
    torn.sync_all().unwrap();
    drop(torn);
    let reopened = PayloadStore::open(&path, binding, policy()).unwrap();
    assert_eq!(reopened.recovered_stages(), 1);
    assert_eq!(reopened.usage(None).unwrap().objects, 1);
    assert!(reopened.root.path(&key, false).exists());
    assert_eq!(reopened.collect(None, 1, |_| Ok(false)).unwrap().removed, 1);
}

#[test]
fn unexpected_entries_and_file_aliases_fail_closed_without_deletion() {
    let fixture = Fixture::new();
    let path = fixture.directory.path().join("objects");
    let binding = fixture.binding;
    drop(fixture.store);
    let unknown = path.join("keep-me");
    fs::write(&unknown, b"untouched").unwrap();
    assert!(PayloadStore::open(&path, binding, policy()).is_err());
    assert_eq!(fs::read(&unknown).unwrap(), b"untouched");
    fs::rename(&unknown, fixture.directory.path().join("saved-unknown")).unwrap();
    std::os::unix::fs::symlink(
        "binding",
        path.join("object-0123456789abcdef0123456789abcdef"),
    )
    .unwrap();
    assert!(PayloadStore::open(&path, binding, policy()).is_err());
    assert!(
        fs::symlink_metadata(path.join("object-0123456789abcdef0123456789abcdef"))
            .unwrap()
            .file_type()
            .is_symlink()
    );
}

#[test]
fn chunk_bounds_and_cleanup_batches_bound_work_without_releasing_live_pins() {
    let fixture = Fixture::new();
    let now = Instant::now();
    let mut stage = fixture
        .store
        .stage(&owner("alice"), &descriptor(&vec![0; 65537]), &caps(), now)
        .unwrap();
    refuse(
        stage.receive(&vec![0; 65537], now),
        ErrorCode::LimitExceeded,
    );
    refuse(stage.receive(b"x", now), ErrorCode::IntegrityError);
    drop(stage);
    let mut installed = Vec::new();
    for _ in 0..4 {
        installed.push(fixture.install(b""));
    }
    let first = fixture.store.collect(None, 2, |_| Ok(false)).unwrap();
    assert_eq!(first.inspected, 2);
    assert_eq!(first.removed, 0);
    let second = fixture
        .store
        .collect(first.next.as_deref(), 2, |_| Ok(false))
        .unwrap();
    assert_eq!(second.inspected, 2);
    drop(installed);
    assert_eq!(
        fixture
            .store
            .collect(None, 2, |_| Ok(false))
            .unwrap()
            .removed,
        2
    );
    assert_eq!(fixture.store.usage(None).unwrap().objects, 2);
}

#[test]
fn retained_read_handles_have_global_and_per_owner_bounds() {
    let fixture = Fixture::new();
    let installed = fixture.install(b"abc");
    let key = installed.key().to_owned();
    drop(installed);
    let mut readers = Vec::new();
    for _ in 0..policy().owner_handles.0 {
        readers.push(
            fixture
                .store
                .open_object(&key, &owner("alice"), &descriptor(b"abc"))
                .unwrap(),
        );
    }
    refuse(
        fixture
            .store
            .open_object(&key, &owner("alice"), &descriptor(b"abc")),
        ErrorCode::LimitExceeded,
    );
    drop(readers.pop());
    readers.push(
        fixture
            .store
            .open_object(&key, &owner("alice"), &descriptor(b"abc"))
            .unwrap(),
    );
    drop(readers);
    assert_eq!(
        fixture
            .store
            .collect(None, 256, |_| Ok(false))
            .unwrap()
            .removed,
        1
    );
}

#[test]
fn authority_pairing_and_transactional_references_exclude_cleanup() {
    let fixture = super::super::tests::Fixture::new();
    let session = fixture.create();
    fixture
        .store
        .declare(
            &session.identity,
            OperationId([1; 16]),
            Number(0),
            &[Id(1)],
            true,
        )
        .unwrap();
    let payloads = PayloadStore::initialize(
        &fixture.directory.path().join("objects"),
        fixture.store.payload_identity().unwrap(),
        policy(),
    )
    .unwrap();
    fixture.store.bind_payloads(&payloads).unwrap();
    let now = Instant::now();
    let mut stage = payloads
        .stage(&owner("alice"), &descriptor(b"abc"), &caps(), now)
        .unwrap();
    stage.receive(b"abc", now).unwrap();
    let installed = stage.finish(now).unwrap();
    let key = installed.key().to_owned();
    assert_eq!(
        fixture
            .store
            .collect_payload_orphans(&payloads, None, 256)
            .unwrap()
            .removed,
        0
    );
    // Storage-level reference commit only; not a fabricated work admission.
    let mut connection = fixture.store.connect().unwrap();
    let tx = connection.transaction().unwrap();
    tx.execute(
        "INSERT INTO payload_refs VALUES(?1,?2,0,1,0)",
        params![key, sql(session.identity.generation.0).unwrap()],
    )
    .unwrap();
    tx.commit().unwrap();
    drop(installed);
    assert_eq!(
        fixture
            .store
            .collect_payload_orphans(&payloads, None, 256)
            .unwrap()
            .removed,
        0
    );
    let other_root = PayloadStore::initialize(
        &fixture.directory.path().join("other-objects"),
        fixture.store.payload_identity().unwrap(),
        policy(),
    )
    .unwrap();
    assert!(fixture.store.bind_payloads(&other_root).is_err());
    assert!(
        fixture
            .store
            .collect_payload_orphans(&other_root, None, 256)
            .is_err()
    );
    let other_authority = super::super::tests::Fixture::new();
    assert!(other_authority.store.bind_payloads(&payloads).is_err());
    fixture
        .store
        .connect()
        .unwrap()
        .execute("DELETE FROM payload_refs WHERE object_key=?1", [&key])
        .unwrap();
    assert_eq!(
        fixture
            .store
            .collect_payload_orphans(&payloads, None, 256)
            .unwrap()
            .removed,
        1
    );
    fixture.store.integrity_check().unwrap();
}

pub(super) fn crash_point(point: &str) {
    if std::env::var("PIPESTREAM_PAYLOAD_TEST_CRASH")
        .ok()
        .as_deref()
        == Some(point)
    {
        std::process::exit(87);
    }
}

#[test]
fn payload_process_child() {
    let Some(path) = std::env::var_os("PIPESTREAM_PAYLOAD_TEST_PATH") else {
        return;
    };
    let root = Path::new(&path);
    let config = fs::read(root.join(CONFIG)).unwrap();
    let binding = StoreIdentity::from_bytes(config[8..24].try_into().unwrap()).unwrap();
    let store = PayloadStore::open(root, binding, policy()).unwrap();
    if std::env::var("PIPESTREAM_PAYLOAD_TEST_CRASH").unwrap() == "cleanup-unlinked" {
        store.collect(None, 256, |_| Ok(false)).unwrap();
    } else {
        let now = Instant::now();
        let mut stage = store
            .stage(&owner("alice"), &descriptor(b"abc"), &caps(), now)
            .unwrap();
        stage.receive(b"abc", now).unwrap();
        stage.finish(now).unwrap();
    }
    panic!("crash boundary not reached");
}

#[test]
fn process_death_brackets_staging_installation_and_orphan_cleanup() {
    for point in [
        "stage-created",
        "stage-header",
        "object-synced",
        "object-renamed",
        "directory-synced",
        "cleanup-unlinked",
    ] {
        let fixture = Fixture::new();
        if point == "cleanup-unlinked" {
            drop(fixture.install(b"abc"));
        }
        let path = fixture.directory.path().join("objects");
        let binding = fixture.binding;
        drop(fixture.store);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "v2::authority::payload::tests::payload_process_child",
                "--nocapture",
            ])
            .env("PIPESTREAM_PAYLOAD_TEST_PATH", &path)
            .env("PIPESTREAM_PAYLOAD_TEST_CRASH", point)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(87), "{point}");
        let store = PayloadStore::open(&path, binding, policy()).unwrap();
        let installed = matches!(point, "object-renamed" | "directory-synced");
        assert_eq!(
            store.usage(None).unwrap().objects,
            u64::from(installed),
            "{point}"
        );
        if installed {
            let key = store.root.entries().unwrap().keys().next().unwrap().clone();
            let mut reader = store
                .open_object(&key, &owner("alice"), &descriptor(b"abc"))
                .unwrap();
            let mut bytes = [0u8; 3];
            assert_eq!(reader.read_chunk(&mut bytes).unwrap(), 3);
            assert_eq!(&bytes, b"abc");
            assert_eq!(reader.read_chunk(&mut bytes).unwrap(), 0);
            assert!(reader.verified());
            drop(reader);
            assert_eq!(store.collect(None, 256, |_| Ok(false)).unwrap().removed, 1);
        }
        assert_eq!(store.usage(None).unwrap().objects, 0);
    }
}
