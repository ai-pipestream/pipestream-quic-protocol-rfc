#![cfg(unix)]

use pipestream_core::{
    persistence::StoreIdentity,
    v2::{
        authority::payload::{PayloadPolicy, PayloadStore},
        *,
    },
};
use sha2::{Digest as _, Sha256};
use std::{fs, os::unix::fs::MetadataExt, time::Instant};

#[path = "support/heap.rs"]
mod heap;

#[test]
fn thirty_two_mib_v2_payload_install_and_read_have_constant_buffering() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("objects");
    let length = 32u64 << 20;
    let policy = PayloadPolicy {
        objects: Id(4),
        bytes: Number(length + 4096),
        owner_objects: Id(4),
        owner_bytes: Number(length + 4096),
        chunk_bytes: Id(65536),
        handles: Id(8),
        owner_handles: Id(8),
    };
    let store =
        PayloadStore::initialize(&root, StoreIdentity::generate().unwrap(), policy).unwrap();
    let caps = Capabilities {
        response: ResponseFlag(1),
        supported: vec![
            ProfileId(DURABLE_WORK.into()),
            ProfileId(RESULT_DELIVERY.into()),
        ],
        required: vec![],
        control_limit: ControlLimit(8192),
        stream_limit: ConcurrencyLimit(8),
        pending_limit: ConcurrencyLimit(8),
        object_limit: Number(length),
        stream_idle_ms: IdleMs(10000),
        stream_lifetime_ms: LifetimeMs(60000),
    };
    let owner = IdentityLabel("resource-owner".into());
    let block = [0x5au8; 16384];
    let mut hash = Sha256::new();
    for _ in 0..length / block.len() as u64 {
        hash.update(block);
    }
    let input = Input {
        length: Number(length),
        sha256: Digest(hash.finalize().into()),
        content_type: ApplicationLabel("application/octet-stream".into()),
    };
    let sample = heap::Sample::start();
    let started = Instant::now();
    let mut stage = store.stage(&owner, &input, &caps, started).unwrap();
    for _ in 0..length / block.len() as u64 {
        stage.receive(&block, Instant::now()).unwrap();
    }
    let installed = stage.finish(Instant::now()).unwrap();
    let mut reader = store.open_object(installed.key(), &owner, &input).unwrap();
    let mut buffer = [0u8; 16384];
    let mut read = 0u64;
    loop {
        let n = reader.read_chunk(&mut buffer).unwrap();
        if n == 0 {
            break;
        }
        assert!(buffer[..n].iter().all(|b| *b == 0x5a));
        read += n as u64;
    }
    assert_eq!(read, length);
    assert!(reader.verified());
    let (peak, largest) = sample.finish();
    assert!(peak < 256 << 10, "Rust heap increase {peak} bytes");
    assert!(
        largest < 64 << 10,
        "largest Rust allocation {largest} bytes"
    );
    let (mut file_bytes, mut allocated_bytes) = (0u64, 0u64);
    for entry in fs::read_dir(&root).unwrap() {
        let metadata = entry.unwrap().metadata().unwrap();
        file_bytes += metadata.len();
        allocated_bytes += metadata.blocks() * 512;
    }
    assert!(file_bytes >= length && file_bytes <= length + 4096);
    println!(
        "V2 payload storage: input_bytes={length} rust_heap_peak_increase={peak} largest_rust_allocation={largest} file_length_bytes={file_bytes} allocated_block_bytes={allocated_bytes} elapsed_ms={}",
        started.elapsed().as_millis()
    );
    #[cfg(target_os = "linux")]
    for line in fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .filter(|line| line.starts_with("VmRSS:") || line.starts_with("VmHWM:"))
    {
        println!("process {line}");
    }
}
