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
fn thirty_two_mib_unknown_length_output_uses_bounded_buffers_and_reserved_capacity() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("objects");
    let length = 32u64 << 20;
    let store = PayloadStore::initialize(
        &root,
        StoreIdentity::generate().unwrap(),
        PayloadPolicy {
            objects: Id(2),
            bytes: Number(length + 4096),
            owner_objects: Id(2),
            owner_bytes: Number(length + 4096),
            chunk_bytes: Id(65536),
            handles: Id(4),
            owner_handles: Id(4),
        },
    )
    .unwrap();
    let caps = Capabilities {
        response: ResponseFlag(1),
        supported: vec![
            ProfileId(DURABLE_WORK.into()),
            ProfileId(RESULT_DELIVERY.into()),
        ],
        required: vec![],
        control_limit: ControlLimit(8192),
        stream_limit: ConcurrencyLimit(4),
        pending_limit: ConcurrencyLimit(4),
        object_limit: Number(length),
        stream_idle_ms: IdleMs(10000),
        stream_lifetime_ms: LifetimeMs(60000),
    };
    let owner = IdentityLabel("resource-owner".into());
    let block = [0x5au8; 16384];
    let budget = OutputBudget {
        count: BatchCount(1),
        total_bytes: Number(length),
    };
    let sample = heap::Sample::start();
    let started = Instant::now();
    let reserve = store.reserve_outputs(&owner, &budget).unwrap();
    let charge = store.usage(None).unwrap();
    let mut stage = reserve
        .stage(
            OutputIndex(0),
            Number(length),
            ApplicationLabel("application/octet-stream".into()),
            &caps,
            started,
        )
        .unwrap();
    let mut expected = Sha256::new();
    for _ in 0..length / block.len() as u64 {
        stage.write(&block, Instant::now()).unwrap();
        expected.update(block);
    }
    let installed = stage.finish(Instant::now()).unwrap();
    assert_eq!(installed.descriptor().length, Number(length));
    assert_eq!(
        installed.descriptor().sha256,
        Digest(expected.finalize().into())
    );
    assert_eq!(
        store.usage(None).unwrap(),
        charge,
        "materialization does not double charge the promise"
    );
    let mut reader = store
        .open_object(installed.key(), &owner, installed.descriptor())
        .unwrap();
    let mut buffer = [0u8; 16384];
    let mut received = 0u64;
    loop {
        let count = reader.read_chunk(&mut buffer).unwrap();
        if count == 0 {
            break;
        }
        assert!(buffer[..count].iter().all(|b| *b == 0x5a));
        received += count as u64;
    }
    assert_eq!(received, length);
    assert!(reader.verified());
    let (peak, largest) = sample.finish();
    assert!(peak < 256 << 10, "Rust heap increase {peak} bytes");
    assert!(largest < 64 << 10, "largest allocation {largest} bytes");
    let (mut file_bytes, mut blocks) = (0u64, 0u64);
    for entry in fs::read_dir(&root).unwrap() {
        let meta = entry.unwrap().metadata().unwrap();
        file_bytes += meta.len();
        blocks += meta.blocks() * 512;
    }
    assert!(file_bytes >= length && file_bytes <= length + 4096);
    println!(
        "V2 output storage: output_bytes={length} charged_bytes={} rust_heap_peak_increase={peak} largest_rust_allocation={largest} file_length_bytes={file_bytes} allocated_block_bytes={blocks} elapsed_ms={}",
        charge.charged_bytes,
        started.elapsed().as_millis()
    );
    #[cfg(target_os = "linux")]
    for line in fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .filter(|l| l.starts_with("VmRSS:") || l.starts_with("VmHWM:"))
    {
        println!("process {line}");
    }
}
