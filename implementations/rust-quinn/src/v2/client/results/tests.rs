use super::*;
use crate::{
    persistence::PhysicalLimits,
    v2::client::{Creation, Journal, JournalLimits},
};
use std::{
    fs,
    io::{Seek, Write},
    process::Command,
};

pub(crate) fn crash_point(point: &str) {
    if std::env::var("PIPESTREAM_LOCAL_RESULT_CRASH")
        .ok()
        .as_deref()
        == Some(point)
    {
        std::process::exit(77);
    }
}
thread_local! { static FAIL_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
pub(crate) fn fail_sync() -> bool {
    FAIL_SYNC.with(|flag| flag.replace(false))
}

#[test]
fn failed_directory_sync_quarantines_installed_copy_until_exclusive_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("copies");
    let reference = reference(directory.path(), "alice", b"abc");
    let store = open(&path, true);
    let mut pending = store
        .stage(reference.clone(), &caps(), Instant::now())
        .unwrap();
    pending.receive(b"abc", Instant::now()).unwrap();
    FAIL_SYNC.with(|flag| flag.set(true));
    assert!(pending.finish(&header(&reference), Instant::now()).is_err());
    assert!(
        store.find(&reference).is_err(),
        "ambiguous namespace was usable without an audited reopen"
    );
    assert!(store.usage().is_err());
    drop(store);
    let store = open(&path, false);
    assert_eq!(read(&store, &reference), b"abc");
    assert_eq!(store.usage().unwrap().objects, 1);
}
fn policy() -> ResultPolicy {
    ResultPolicy {
        objects: Id(2),
        bytes: Number(4096),
        owner_objects: Id(2),
        owner_bytes: Number(4096),
        chunk_bytes: Id(1024),
        handles: Id(4),
        owner_handles: Id(4),
    }
}
fn caps() -> Capabilities {
    Capabilities {
        response: ResponseFlag(1),
        supported: vec![
            ProfileId(DURABLE_WORK.into()),
            ProfileId(RESULT_DELIVERY.into()),
        ],
        required: vec![
            ProfileId(DURABLE_WORK.into()),
            ProfileId(RESULT_DELIVERY.into()),
        ],
        control_limit: ControlLimit(65536),
        stream_limit: ConcurrencyLimit(4),
        pending_limit: ConcurrencyLimit(16),
        object_limit: Number(1 << 20),
        stream_idle_ms: IdleMs(5000),
        stream_lifetime_ms: LifetimeMs(30000),
    }
}
pub(super) fn reference(directory: &Path, owner: &str, bytes: &[u8]) -> RetainedReference {
    let creation = Creation {
        authority: IdentityLabel("issuer-a".into()),
        owner: IdentityLabel(owner.into()),
        creation_sequence: Id(1),
        policy: Policy {
            execution_limit_ms: Duration(60000),
            output_retention_ms: Duration(120000),
            receipt_retention_ms: Duration(180000),
        },
        results: true,
    };
    let path = directory.join(format!("{owner}.sqlite"));
    let open = if path.exists() {
        Journal::open
    } else {
        Journal::initialize
    };
    let journal = open(
        &path,
        creation.clone(),
        JournalLimits::default(),
        PhysicalLimits::default(),
    )
    .unwrap();
    journal
        .record_binding(
            &Control::Session(Session::Binding {
                request: Id(1),
                authority: creation.authority.clone(),
                owner: creation.owner.clone(),
                generation: Id(7),
                creation_sequence: Id(1),
                policy: creation.policy.clone(),
                limits: Limits {
                    scopes: Id(100),
                    entities: Id(100),
                    operations: Id(100),
                    active_jobs: Id(16),
                    retained_input_bytes: Number(1 << 20),
                    retained_output_bytes: Number(1 << 20),
                },
            }),
            &caps(),
        )
        .unwrap();
    journal.remember_reference(&Manifest { version: Literal, authority: creation.authority,
        owner: creation.owner, generation: Id(7), work: WorkKey { scope: Number(0), producer: Producer(0), entity: Id(1) },
        attempt: Id(1), input_sha256: Digest([8; 32]), committed_at: Number(200), available_until: Number(120200),
        outputs: vec![Output { index: OutputIndex(0), length: Number(bytes.len() as u64),
            sha256: Digest(Sha256::digest(bytes).into()), content_type: ApplicationLabel("text/plain".into()),
            locator: ResultLocator("pipestream://untrusted.invalid:7443/v2/sessions/7/scopes/0/producers/0/entities/1/attempts/1/outputs/0".into()) }],
    }, OutputIndex(0)).unwrap()
}
fn header(reference: &RetainedReference) -> ResultHeader {
    let manifest = reference.manifest();
    ResultHeader {
        kind: Literal,
        request: Id(1),
        generation: manifest.generation,
        work: manifest.work.clone(),
        attempt: manifest.attempt,
        index: reference.index(),
        length: manifest.outputs[0].length,
        sha256: manifest.outputs[0].sha256,
    }
}
pub(super) fn open(path: &Path, fresh: bool) -> ResultStore {
    let open = if fresh {
        ResultStore::initialize
    } else {
        ResultStore::open
    };
    open(
        path,
        IdentityLabel("issuer-a".into()),
        IdentityLabel("alice".into()),
        policy(),
    )
    .unwrap()
}
pub(super) fn fill(
    store: &ResultStore,
    reference: &RetainedReference,
    bytes: &[u8],
) -> InstalledPayload {
    let mut pending = store
        .stage(reference.clone(), &caps(), Instant::now())
        .unwrap();
    for chunk in bytes.chunks(store.chunk_limit()) {
        pending.receive(chunk, Instant::now()).unwrap();
    }
    pending.finish(&header(reference), Instant::now()).unwrap()
}
fn check<T>(value: Result<T>, code: ErrorCode) {
    assert!(matches!(value, Err(StoreError::Protocol(e)) if e.code == code));
}
fn read(store: &ResultStore, reference: &RetainedReference) -> Vec<u8> {
    let mut reader = store.find(reference).unwrap().unwrap();
    assert!(!reader.verified());
    let mut data = Vec::new();
    let mut chunk = [0; 1024];
    loop {
        let n = reader.read_chunk(&mut chunk).unwrap();
        if n == 0 {
            break;
        }
        data.extend_from_slice(&chunk[..n]);
    }
    assert!(reader.verified());
    data
}

#[test]
fn local_result_round_trip_reopen_and_empty_objects_require_verified_eof() {
    for bytes in [vec![], vec![0xc3; 1537]] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("copies");
        let reference = reference(directory.path(), "alice", &bytes);
        let store = open(&path, true);
        let pending = store
            .stage(reference.clone(), &caps(), Instant::now())
            .unwrap();
        assert!(store.find(&reference).unwrap().is_none());
        drop(pending);
        let installed = fill(&store, &reference, &bytes);
        assert_eq!(store.usage().unwrap().objects, 1);
        drop(installed);
        drop(store);
        let store = open(&path, false);
        assert_eq!(read(&store, &reference), bytes);
    }
}

#[test]
fn all_transfers_share_byte_object_and_live_handle_quotas() {
    let directory = tempfile::tempdir().unwrap();
    let reference = reference(directory.path(), "alice", &[0xab; 1537]);
    let store = open(&directory.path().join("copies"), true);
    let first = store
        .stage(reference.clone(), &caps(), Instant::now())
        .unwrap();
    // Each stage reserves its complete length plus 524 header bytes, not bytes received.
    assert_eq!(store.usage().unwrap().charged_bytes, 2061);
    check(
        store.stage(reference.clone(), &caps(), Instant::now()),
        ErrorCode::LimitExceeded,
    );
    drop(first);
    assert_eq!(store.usage().unwrap().charged_bytes, 0);
    let installed = fill(&store, &reference, &[0xab; 1537]);
    let mut pins = vec![];
    for _ in 0..3 {
        pins.push(store.find(&reference).unwrap().unwrap());
    }
    check(store.find(&reference), ErrorCode::LimitExceeded);
    check(store.remove(installed.key()), ErrorCode::Conflict);
    drop(pins);
    let key = installed.key().to_owned();
    drop(installed);
    assert!(store.remove(&key).unwrap());
    assert!(!store.remove(&key).unwrap());
    assert_eq!(store.usage().unwrap().charged_bytes, 0);
    check(store.remove("../binding"), ErrorCode::Conflict);

    let empty_directory = tempfile::tempdir().unwrap();
    let empty = reference_for_empty(empty_directory.path());
    let empty_store = open(&empty_directory.path().join("copies"), true);
    let a = fill(&empty_store, &empty, &[]);
    let b = fill(&empty_store, &empty, &[]);
    check(
        empty_store.stage(empty, &caps(), Instant::now()),
        ErrorCode::LimitExceeded,
    );
    drop((a, b));
}
fn reference_for_empty(path: &Path) -> RetainedReference {
    reference(path, "alice", &[])
}

#[test]
fn wrong_commitment_and_corrupt_body_never_become_verified_local_output() {
    let directory = tempfile::tempdir().unwrap();
    let reference = reference(directory.path(), "alice", b"abc");
    let path = directory.path().join("copies");
    let store = open(&path, true);
    let mut pending = store
        .stage(reference.clone(), &caps(), Instant::now())
        .unwrap();
    pending.receive(b"abc", Instant::now()).unwrap();
    let mut wrong = header(&reference);
    wrong.attempt = Id(2);
    check(
        pending.finish(&wrong, Instant::now()),
        ErrorCode::IntegrityError,
    );
    assert_eq!(store.usage().unwrap().objects, 0);
    let mut pending = store
        .stage(reference.clone(), &caps(), Instant::now())
        .unwrap();
    pending.receive(b"abd", Instant::now()).unwrap();
    assert!(pending.finish(&header(&reference), Instant::now()).is_err());
    assert!(store.find(&reference).unwrap().is_none());
    let installed = fill(&store, &reference, b"abc");
    let mut file = fs::OpenOptions::new()
        .write(true)
        .open(path.join(format!("object-{}", installed.key())))
        .unwrap();
    file.seek(std::io::SeekFrom::End(-1)).unwrap();
    file.write_all(b"d").unwrap();
    drop(file);
    let mut reader = store.find(&reference).unwrap().unwrap();
    let mut bytes = [0; 3];
    assert_eq!(reader.read_chunk(&mut bytes).unwrap(), 3);
    check(reader.read_chunk(&mut bytes), ErrorCode::OutputUnavailable);
    assert!(!reader.verified());
}

#[test]
fn exclusive_owner_binding_and_unknown_files_prevent_unsafe_adoption_or_cleanup() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("copies");
    let store = open(&path, true);
    let alice = IdentityLabel("alice".into());
    let issuer = IdentityLabel("issuer-a".into());
    assert!(ResultStore::initialize(&path, issuer.clone(), alice.clone(), policy()).is_err());
    check(
        ResultStore::open(&path, issuer.clone(), alice.clone(), policy()),
        ErrorCode::LimitExceeded,
    );
    let foreign = reference(directory.path(), "bob", b"abc");
    check(
        store.stage(foreign.clone(), &caps(), Instant::now()),
        ErrorCode::Unauthorized,
    );
    check(store.find(&foreign), ErrorCode::Unauthorized);
    drop(store);
    assert!(
        ResultStore::open(&path, issuer.clone(), IdentityLabel("bob".into()), policy()).is_err()
    );
    let mut changed = policy();
    changed.bytes = Number(8192);
    assert!(ResultStore::open(&path, issuer.clone(), alice.clone(), changed).is_err());
    fs::write(path.join("unrelated"), b"keep").unwrap();
    assert!(ResultStore::open(&path, issuer, alice, policy()).is_err());
    assert_eq!(fs::read(path.join("unrelated")).unwrap(), b"keep");
}

#[test]
fn process_death_reclaims_only_incomplete_copies_and_preserves_committed_bytes() {
    for point in ["before-finish", "after-finish", "removed"] {
        let directory = tempfile::tempdir().unwrap();
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "v2::client::results::tests::crash_child",
                "--nocapture",
            ])
            .env("PIPESTREAM_LOCAL_RESULT_CRASH", point)
            .env("PIPESTREAM_LOCAL_RESULT_DIR", directory.path())
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(77), "{output:?}");
        let reference = reference(directory.path(), "alice", b"abc");
        let store = open(&directory.path().join("copies"), false);
        match point {
            "before-finish" => {
                assert_eq!(store.recovered_stages(), 1);
                assert!(store.find(&reference).unwrap().is_none());
            }
            "after-finish" => assert_eq!(read(&store, &reference), b"abc"),
            "removed" => assert!(store.find(&reference).unwrap().is_none()),
            _ => unreachable!(),
        }
    }
}

#[test]
fn crash_child() {
    let Ok(directory) = std::env::var("PIPESTREAM_LOCAL_RESULT_DIR") else {
        return;
    };
    let directory = Path::new(&directory);
    let reference = reference(directory, "alice", b"abc");
    let store = open(&directory.join("copies"), true);
    let installed = fill(&store, &reference, b"abc");
    let key = installed.key().to_owned();
    drop(installed);
    store.remove(&key).unwrap();
    panic!("selected crash point was not reached");
}
