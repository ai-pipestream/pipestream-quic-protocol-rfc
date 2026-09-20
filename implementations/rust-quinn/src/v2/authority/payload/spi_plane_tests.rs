//! Tests for the SPI payload plane (PayloadBackendKind::SpiFilesystem):
//! installed content lives in the SPI store under `<root>/spi`, local object
//! files are header-only stubs, and the kill windows reconcile at open.

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

fn spi_root() -> (tempfile::TempDir, PayloadStore, StoreIdentity) {
    let directory = tempfile::tempdir().unwrap();
    let binding = StoreIdentity::generate().unwrap();
    let store = PayloadStore::initialize_with_backend(
        &directory.path().join("objects"),
        binding,
        policy(),
        PayloadBackendKind::SpiFilesystem,
    )
    .unwrap();
    (directory, store, binding)
}

fn install(store: &PayloadStore, bytes: &[u8]) -> InstalledPayload {
    let now = Instant::now();
    let mut stage = store
        .stage(&IdentityLabel("alice".into()), &descriptor(bytes), &caps(), now)
        .unwrap();
    for chunk in bytes.chunks(65536) {
        stage.receive(chunk, now).unwrap();
    }
    stage.finish(now).unwrap()
}

fn read_all(store: &PayloadStore, key: &str, bytes: &[u8]) -> Vec<u8> {
    let mut reader = store
        .open_object(key, &IdentityLabel("alice".into()), &descriptor(bytes))
        .unwrap();
    let mut output = Vec::new();
    let mut buffer = [0u8; 4096];
    loop {
        let n = reader.read_chunk(&mut buffer).unwrap();
        output.extend_from_slice(&buffer[..n]);
        if n == 0 {
            break;
        }
    }
    assert!(reader.verified());
    output
}

fn spi_entities(store: &PayloadStore, dir: &tempfile::TempDir) -> Vec<u32> {
    let _ = dir;
    store
        .root
        .spi
        .as_ref()
        .unwrap()
        .lock()
        .unwrap()
        .list(&plane_scope())
        .unwrap()
}

#[test]
fn spi_plane_installs_content_in_the_store_and_a_stub_locally() {
    let (dir, store, _binding) = spi_root();
    let payload: Vec<u8> = (0..100_000).map(|i| (i % 251) as u8).collect();
    let installed = install(&store, &payload);
    let key = installed.key().to_owned();

    // The local object file is a header-only stub.
    let stub = dir.path().join("objects").join(format!("object-{key}"));
    let stub_len = fs::metadata(&stub).unwrap().len();
    assert_eq!(stub_len, OVERHEAD, "local object file is a header-only stub");
    assert!(!dir.path().join("objects").join(format!("stage-{key}")).exists());

    // The content lives in the SPI plane and reads back verified.
    assert_eq!(spi_entities(&store, &dir), vec![plane_entity(&key)]);
    assert_eq!(read_all(&store, &key, &payload), payload);
}

#[test]
fn spi_plane_content_survives_reopen() {
    let (dir, store, binding) = spi_root();
    let payload = b"restart-proof content".to_vec();
    let installed = install(&store, &payload);
    let key = installed.key().to_owned();
    drop(installed);
    drop(store);
    let reopened = PayloadStore::open_with_backend(
        &dir.path().join("objects"),
        binding,
        policy(),
        PayloadBackendKind::SpiFilesystem,
    )
    .unwrap();
    assert_eq!(read_all(&reopened, &key, &payload), payload);
}

#[test]
fn spi_plane_sweeps_orphaned_entities_at_open() {
    let (dir, store, binding) = spi_root();
    let payload = b"committed content".to_vec();
    let installed = install(&store, &payload);
    let key = installed.key().to_owned();
    drop(installed);
    drop(store);
    // Simulate a kill after the SPI put but before the stub write: put an
    // entity with no local object stub.
    let mut plane =
        pipestream_storage::FsStore::open(dir.path().join("objects").join(SPI_DIR)).unwrap();
    let orphan = b"orphaned bytes".to_vec();
    plane
        .put(
            &plane_scope(),
            0x0bad_f00d,
            &orphan,
            &pipestream_storage::sha256(&orphan),
        )
        .unwrap();
    drop(plane);
    let reopened = PayloadStore::open_with_backend(
        &dir.path().join("objects"),
        binding,
        policy(),
        PayloadBackendKind::SpiFilesystem,
    )
    .unwrap();
    assert_eq!(spi_entities(&reopened, &dir), vec![plane_entity(&key)]);
    assert_eq!(read_all(&reopened, &key, &payload), payload);
}

#[test]
fn spi_plane_collection_removes_stub_and_content() {
    let (dir, store, _binding) = spi_root();
    let payload = b"collected content".to_vec();
    let installed = install(&store, &payload);
    let key = installed.key().to_owned();
    drop(installed);
    let progress = store.collect(None, 256, |_| Ok(false)).unwrap();
    assert_eq!(progress.removed, 1);
    assert!(!dir.path().join("objects").join(format!("object-{key}")).exists());
    assert!(spi_entities(&store, &dir).is_empty());
}

#[test]
fn spi_plane_reopen_with_the_filesystem_backend_is_refused() {
    let (dir, store, binding) = spi_root();
    drop(store);
    assert!(
        PayloadStore::open_with_backend(
            &dir.path().join("objects"),
            binding,
            policy(),
            PayloadBackendKind::Filesystem,
        )
        .is_err()
    );
}

#[test]
fn spi_plane_abandoned_stage_leaves_no_plane_entity() {
    let (dir, store, binding) = spi_root();
    drop(store);
    // A killed reception leaves a stage file; nothing reached the plane.
    fs::write(
        dir.path()
            .join("objects")
            .join("stage-0123456789abcdef0123456789abcdef"),
        b"partial reception",
    )
    .unwrap();
    let reopened = PayloadStore::open_with_backend(
        &dir.path().join("objects"),
        binding,
        policy(),
        PayloadBackendKind::SpiFilesystem,
    )
    .unwrap();
    assert_eq!(reopened.recovered_stages(), 1);
    assert!(spi_entities(&reopened, &dir).is_empty());
}
