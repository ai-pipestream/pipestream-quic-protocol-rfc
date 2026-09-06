use pipestream_core::persistence::SqliteSessionStore;
use pipestream_core::*;
use pipestream_quic::recursive::*;
use quinn::crypto::rustls::QuicClientConfig;
use sha2::{Digest, Sha256};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    collections::BTreeMap,
    fs,
    io::Read,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

// This binary contains one test so other tests cannot contaminate its live-allocation gate.
struct MeasuredAllocator;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static LARGEST: AtomicUsize = AtomicUsize::new(0);

fn acquired(size: usize) {
    let live = LIVE.fetch_add(size, Ordering::SeqCst) + size;
    PEAK.fetch_max(live, Ordering::SeqCst);
    LARGEST.fetch_max(size, Ordering::SeqCst);
}

// Delegate allocation and layout handling unchanged to the system allocator.
unsafe impl GlobalAlloc for MeasuredAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            acquired(layout.size());
        }
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            acquired(layout.size());
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe {
            System.dealloc(pointer, layout);
        }
        LIVE.fetch_sub(layout.size(), Ordering::SeqCst);
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(pointer, layout, size) };
        if !result.is_null() {
            LIVE.fetch_sub(layout.size(), Ordering::SeqCst);
            acquired(size);
        }
        result
    }
}

#[global_allocator]
static ALLOCATOR: MeasuredAllocator = MeasuredAllocator;

async fn frame(recv: &mut quinn::RecvStream) -> (u8, Vec<u8>) {
    let mut prefix = [0; 5];
    recv.read_exact(&mut prefix).await.unwrap();
    let length = u32::from_be_bytes(prefix[1..].try_into().unwrap()) as usize;
    assert!(length <= 65536);
    let mut body = vec![0; length];
    recv.read_exact(&mut body).await.unwrap();
    (prefix[0], body)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn thirty_two_mib_transfer_uses_bounded_heap_and_verified_file_backed_processing() {
    tokio::time::timeout(Duration::from_secs(20), transfer())
        .await
        .unwrap();
}

async fn transfer() {
    let dir = tempfile::tempdir().unwrap();
    let certified = rcgen::generate_simple_self_signed(["localhost".into()]).unwrap();
    let cert = dir.path().join("server.crt");
    let key = dir.path().join("server.key");
    fs::write(&cert, certified.cert.pem()).unwrap();
    fs::write(&key, certified.signing_key.serialize_pem()).unwrap();
    let options = RecursiveServerOptions {
        bind: "127.0.0.1:0".parse().unwrap(),
        certificate: cert,
        private_key: key,
        state_database: dir.path().join("state.sqlite3"),
        entity_directory: dir.path().join("entities"),
        ready_file: None,
        once: true,
        max_scope_depth: 7,
        max_entities_per_scope: 100,
        max_entity_bytes: MAX_PAYLOAD,
        max_chunks_per_entity: 16,
        max_concurrent_connections: 1,
    };
    let entities = Arc::new(FileEntityStore::open(&options.entity_directory).unwrap());
    let service = RecursiveService::new(
        Arc::new(SqliteSessionStore::open(&options.state_database).unwrap()),
        entities.clone(),
        Arc::new(ExemplarProcessor::default()),
        7,
        100,
    )
    .unwrap();
    let server = RecursiveServer::bind(&options, service).unwrap();
    let address = server.local_addr().unwrap();
    let server = tokio::spawn(server.run(true));
    let mut roots = rustls::RootCertStore::empty();
    roots.add(certified.cert.der().clone()).unwrap();
    let mut tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls.alpn_protocols = vec![ALPN.to_vec()];
    let mut config = quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(tls).unwrap()));
    let mut transport = quinn::TransportConfig::default();
    transport.send_window(1 << 20);
    config.transport_config(Arc::new(transport));
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(config);
    let connection = endpoint
        .connect(address, "localhost")
        .unwrap()
        .await
        .unwrap();
    let (mut send, mut recv) = connection.open_bi().await.unwrap();
    send.write_all(&encode_capabilities(&Capabilities::default()).unwrap())
        .await
        .unwrap();
    assert_eq!(frame(&mut recv).await.0, FRAME_CAPABILITIES);
    assert_eq!(frame(&mut recv).await.0, FRAME_STATUS);

    let block = [0x5a; 8192];
    let length = 32 << 20;
    let mut digest = Sha256::new();
    for _ in 0..length / block.len() {
        digest.update(block);
    }
    let checksum: [u8; 32] = digest.finalize().into();
    let header = EntityHeader {
        entity_id: 1,
        parent_id: None,
        scope_id: None,
        parent_scope_id: None,
        layer: 0,
        content_type: None,
        payload_length: Some(length as u64),
        checksum: Some(checksum),
        metadata: BTreeMap::from([(SESSION_METADATA_KEY.into(), "large-spool".into())]),
        chunk_info: None,
        completion_policy: None,
    };
    let encoded = encode_entity_header_for(&header, LayerSupport::LAYER0).unwrap();
    let baseline = LIVE.load(Ordering::SeqCst);
    PEAK.store(baseline, Ordering::SeqCst);
    LARGEST.store(0, Ordering::SeqCst);
    let mut stream = connection.open_uni().await.unwrap();
    stream
        .write_all(&(encoded.len() as u32).to_be_bytes())
        .await
        .unwrap();
    stream.write_all(&encoded).await.unwrap();
    for _ in 0..length / block.len() {
        stream.write_all(&block).await.unwrap();
    }
    stream.finish().unwrap();
    for expected in [STATUS_PROCESSING, STATUS_COMPLETE] {
        let (kind, body) = frame(&mut recv).await;
        assert_eq!(kind, FRAME_STATUS);
        assert_eq!(
            decode_status_frame(&body, LayerSupport::LAYER0)
                .unwrap()
                .status
                .state,
            expected
        );
    }
    let peak = PEAK.load(Ordering::SeqCst).saturating_sub(baseline);
    let largest = LARGEST.load(Ordering::SeqCst);
    println!(
        "32 MiB streamed transfer: heap increase {peak} bytes, largest allocation {largest} bytes"
    );
    assert!(peak < 12 << 20, "heap grew by {peak} bytes");
    assert!(
        largest < 4 << 20,
        "whole-payload-sized allocation: {largest}"
    );
    let usage = entities.spool().usage().unwrap();
    assert_eq!(usage.peak_bytes, length as u64);
    assert_eq!(usage.peak_files, 1);
    assert_eq!((usage.bytes, usage.files), (0, 0));
    let mut payload = fs::File::open(
        options
            .entity_directory
            .join("large-spool/scope-0/entity-1.bin"),
    )
    .unwrap();
    assert_eq!(payload.metadata().unwrap().len(), length as u64);
    let mut scratch = [0; 8192];
    let mut digest = Sha256::new();
    loop {
        let n = payload.read(&mut scratch).unwrap();
        if n == 0 {
            break;
        }
        digest.update(&scratch[..n]);
    }
    assert_eq!(<[u8; 32]>::from(digest.finalize()), checksum);
    send.write_all(
        &encode_checkpoint_for(
            &Checkpoint {
                checkpoint_id: "large-cut".into(),
                sequence_number: 1,
                checkpoint_entity_id: 2,
                scope_id: None,
                flags: 0,
                timeout_ms: Some(1000),
            },
            LayerSupport::LAYER0,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(frame(&mut recv).await.0, FRAME_CHECKPOINT);
    send.write_all(&encode_goaway(1).unwrap()).await.unwrap();
    assert_eq!(frame(&mut recv).await.0, FRAME_GOAWAY);
    connection.close(0u32.into(), b"resource test complete");
    endpoint.close(0u32.into(), b"resource test complete");
    server.await.unwrap().unwrap();
    reconcile_large_input(
        dir.path(),
        options
            .entity_directory
            .join("large-spool/scope-0/entity-1.bin"),
        length as u64,
        checksum,
    );
}

fn reconcile_large_input(
    directory: &std::path::Path,
    source: std::path::PathBuf,
    length: u64,
    digest: [u8; 32],
) {
    let root = directory.join("orphan-payloads");
    let sessions = SqliteSessionStore::open(directory.join("orphan-state.sqlite3")).unwrap();
    let files = FileEntityStore::open(&root).unwrap();
    files.bind_session_store(&sessions).unwrap();
    let payload = spool::Payload::open_retained(source, length, digest).unwrap();
    let key = pipestream_core::session::EntityKey {
        scope_id: 0,
        entity_id: 1,
    };
    let baseline = LIVE.load(Ordering::SeqCst);
    PEAK.store(baseline, Ordering::SeqCst);
    LARGEST.store(0, Ordering::SeqCst);
    files.put_payload(None, "orphan", key, &payload).unwrap();
    drop(files);
    let report =
        FileEntityStore::reconcile(&root, spool::SpoolLimits::default(), &sessions).unwrap();
    assert_eq!(report.orphan_bodies_removed, 1);
    assert_eq!(report.after.bytes, 1120 + 512);
    let files = FileEntityStore::open(&root).unwrap();
    files.put_payload(None, "orphan", key, &payload).unwrap();
    let restored = files
        .load_payload(None, "orphan", key, length, digest)
        .unwrap();
    assert_eq!(restored.digest(), digest);
    assert_eq!(restored.len(), length);
    let peak = PEAK.load(Ordering::SeqCst).saturating_sub(baseline);
    let largest = LARGEST.load(Ordering::SeqCst);
    println!(
        "32 MiB orphan installation/reclamation/restoration: heap increase {peak} bytes, largest allocation {largest} bytes"
    );
    assert!(peak < 1 << 20, "reconciliation heap grew by {peak} bytes");
    assert!(
        largest < 1 << 20,
        "payload-sized reconciliation allocation: {largest}"
    );
}
