//! Characterization tests pinning the payload store's on-disk grammar:
//! `binding` / `root.lock`, `stage-*` → `object-*` object files, and
//! `funding-*` → `reserve-*` reservation files. Any later layout change
//! must fail these tests red before it can ship silently.
//!
//! Reference: `docs/standards/storage-spi-adoption-plan-rust.md` §6.

use super::*;
use std::fs;

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

fn fresh_root() -> (tempfile::TempDir, PayloadStore, StoreIdentity) {
    let directory = tempfile::tempdir().unwrap();
    let binding = StoreIdentity::generate().unwrap();
    let store = PayloadStore::initialize(&directory.path().join("objects"), binding, policy())
        .unwrap();
    (directory, store, binding)
}

fn root_entries(store: &PayloadStore) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(store.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort_unstable();
    names
}

#[test]
fn initialize_creates_exactly_the_binding_config_and_empty_lock() {
    let (_dir, store, binding) = fresh_root();
    assert_eq!(root_entries(&store), vec!["binding", "root.lock"]);

    let config = fs::read(store.path().join("binding")).unwrap();
    // [PSROOT04][16B store identity][policy CBOR][32B sha256 of the prefix]
    assert_eq!(&config[..8], ROOT_MAGIC);
    assert_eq!(&config[8..24], binding.as_bytes());
    let (body, checksum) = config.split_at(config.len() - 32);
    assert_eq!(Sha256::digest(body).as_slice(), checksum);
    assert_eq!(unpack::<PayloadPolicy>(&body[24..]).unwrap(), policy());

    let lock = fs::metadata(store.path().join("root.lock")).unwrap();
    assert!(lock.is_file());
    assert_eq!(lock.len(), 0);
}

#[test]
fn stage_and_object_files_follow_the_pinned_grammar() {
    let (_dir, store, _binding) = fresh_root();
    let now = Instant::now();
    let payload = b"grammar-pinned payload".to_vec();
    let input = descriptor(&payload);
    let mut staged = store
        .stage(&IdentityLabel("alice".into()), &input, &caps(), now)
        .unwrap();
    staged.receive(&payload, now).unwrap();

    // Exactly one stage-<32 lowercase hex> file, carrying
    // [PSOBJ004][u32 BE header length][header CBOR][zero padding to 512].
    let staged_names = root_entries(&store);
    assert_eq!(staged_names.len(), 3); // binding, root.lock, stage-*
    let stage_name = staged_names
        .iter()
        .find(|name| name.starts_with("stage-"))
        .expect("stage file")
        .clone();
    let key = stage_name.strip_prefix("stage-").unwrap();
    assert_eq!(key.len(), 32);
    assert!(key.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)));
    let stage_bytes = fs::read(store.path().join(&stage_name)).unwrap();
    assert_eq!(&stage_bytes[..8], MAGIC);
    let header_len = u32::from_be_bytes(stage_bytes[8..12].try_into().unwrap()) as usize;
    assert!(header_len <= HEADER_LIMIT);
    let header: Header =
        codec::decode(&stage_bytes[12..12 + header_len], HEADER_LIMIT).unwrap();
    assert_eq!(header.owner, IdentityLabel("alice".into()));
    assert_eq!(header.input, input);
    assert!(header.funding.is_none());
    assert!(stage_bytes[12 + header_len..OVERHEAD as usize].iter().all(|b| *b == 0));

    // finish() renames stage-* to object-* under the same key, payload at
    // the fixed offset.
    let installed = staged.finish(now).unwrap();
    assert_eq!(installed.key(), key);
    let names = root_entries(&store);
    assert!(!names.iter().any(|n| n.starts_with("stage-")));
    let object_name = format!("object-{key}");
    assert!(names.contains(&object_name));
    let object_bytes = fs::read(store.path().join(&object_name)).unwrap();
    assert_eq!(&object_bytes[..8], MAGIC);
    assert_eq!(&object_bytes[OVERHEAD as usize..], payload.as_slice());
    // Object files are private.
    let mode = fs::metadata(store.path().join(&object_name))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o600);
}

#[test]
fn reservation_files_follow_the_pinned_grammar() {
    let (_dir, store, _binding) = fresh_root();
    let budget = OutputBudget {
        count: BatchCount(2),
        total_bytes: Number(4096),
    };
    let reservation = store
        .reserve_outputs(&IdentityLabel("alice".into()), &budget)
        .unwrap();
    let key = reservation.key().to_owned();
    // Installation renamed funding-* to reserve-*.
    let names = root_entries(&store);
    assert!(!names.iter().any(|n| n.starts_with("funding-")));
    let reserve_name = format!("reserve-{key}");
    assert!(names.contains(&reserve_name));
    // [PSBUD004][u32 BE length][reservation CBOR][32B sha256 of the prefix]
    let bytes = fs::read(store.path().join(&reserve_name)).unwrap();
    assert_eq!(&bytes[..8], b"PSBUD004");
    let (body, checksum) = bytes.split_at(bytes.len() - 32);
    assert_eq!(Sha256::digest(body).as_slice(), checksum);
    let encoded_len = u32::from_be_bytes(bytes[8..12].try_into().unwrap()) as usize;
    assert_eq!(encoded_len + 44, bytes.len());
    let decoded: ReservationEntry =
        codec::decode(&bytes[12..12 + encoded_len], HEADER_LIMIT).unwrap();
    assert_eq!(decoded.owner, IdentityLabel("alice".into()));
    assert_eq!(decoded.budget, budget);
    drop(reservation);
}

#[test]
fn reopen_rejects_entries_outside_the_grammar() {
    let (dir, store, binding) = fresh_root();
    drop(store);
    let root = dir.path().join("objects");
    for bogus in ["junk", "junk-0123456789abcdef0123456789abcdef", "stage-short"] {
        fs::write(root.join(bogus), b"x").unwrap();
        assert!(
            PayloadStore::open(&root, binding, policy()).is_err(),
            "entry {bogus} must be rejected"
        );
        fs::remove_file(root.join(bogus)).unwrap();
    }
    // Duplicate identity across grammars: one key as both stage and object.
    let key = "0123456789abcdef0123456789abcdef";
    fs::write(root.join(format!("stage-{key}")), b"x").unwrap();
    fs::write(root.join(format!("object-{key}")), b"x").unwrap();
    assert!(PayloadStore::open(&root, binding, policy()).is_err());
}

#[test]
fn abandoned_stage_is_reclaimed_at_open() {
    let (dir, store, binding) = fresh_root();
    drop(store);
    // Simulate a killed writer: a stage-<32hex> file left behind.
    let root = dir.path().join("objects");
    fs::write(
        root.join("stage-0123456789abcdef0123456789abcdef"),
        b"partial reception",
    )
    .unwrap();
    let reopened = PayloadStore::open(&root, binding, policy()).unwrap();
    assert_eq!(reopened.recovered_stages(), 1);
    assert!(!root_entries(&reopened).iter().any(|n| n.starts_with("stage-")));
}
