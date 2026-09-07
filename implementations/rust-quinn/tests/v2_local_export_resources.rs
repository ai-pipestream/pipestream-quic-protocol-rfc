//! Isolated local storage gate. This uses trusted fixture metadata, not a wire
//! oracle, and measures Rust allocations separately from observed process RSS.
#![cfg(unix)]
use pipestream_core::{
    persistence::PhysicalLimits,
    v2::{
        client::{
            Creation, Journal, JournalLimits,
            results::{
                ResultPolicy, ResultStore,
                exports::{ExportPolicy, ExportStore},
            },
        },
        *,
    },
};
use sha2::{Digest as _, Sha256};
use std::{fs, io::Read, time::Instant};
#[path = "support/heap.rs"]
mod heap;

#[test]
fn thirty_two_mib_local_copy_export_replay_and_cleanup_have_bounded_rust_heap() {
    const LENGTH: u64 = 32 << 20;
    let directory = tempfile::tempdir().unwrap();
    let labels = (
        IdentityLabel("issuer-a".into()),
        IdentityLabel("alice".into()),
    );
    let caps = Capabilities {
        response: ResponseFlag(1),
        supported: vec![
            ProfileId(DURABLE_WORK.into()),
            ProfileId(RESULT_DELIVERY.into()),
        ],
        required: vec![],
        control_limit: ControlLimit(65536),
        stream_limit: ConcurrencyLimit(4),
        pending_limit: ConcurrencyLimit(4),
        object_limit: Number(LENGTH),
        stream_idle_ms: IdleMs(10000),
        stream_lifetime_ms: LifetimeMs(60000),
    };
    let policy = Policy {
        execution_limit_ms: Duration(60000),
        output_retention_ms: Duration(120000),
        receipt_retention_ms: Duration(180000),
    };
    let journal = Journal::initialize(
        &directory.path().join("client.sqlite"),
        Creation {
            authority: labels.0.clone(),
            owner: labels.1.clone(),
            creation_sequence: Id(1),
            policy: policy.clone(),
            results: true,
        },
        JournalLimits::default(),
        PhysicalLimits::default(),
    )
    .unwrap();
    journal
        .record_binding(
            &Control::Session(Session::Binding {
                request: Id(1),
                authority: labels.0.clone(),
                owner: labels.1.clone(),
                generation: Id(1),
                creation_sequence: Id(1),
                policy,
                limits: Limits {
                    scopes: Id(1),
                    entities: Id(1),
                    operations: Id(2),
                    active_jobs: Id(1),
                    retained_input_bytes: Number(0),
                    retained_output_bytes: Number(LENGTH),
                },
            }),
            &caps,
        )
        .unwrap();
    let block = [0x65; 8192];
    let mut hash = Sha256::new();
    for _ in 0..LENGTH / block.len() as u64 {
        hash.update(block);
    }
    let hash = Digest(hash.finalize().into());
    let work = WorkKey {
        scope: Number(0),
        producer: Producer(0),
        entity: Id(1),
    };
    let manifest = Manifest { version: Literal, authority: labels.0.clone(), owner: labels.1.clone(), generation: Id(1), work: work.clone(), attempt: Id(1),
        input_sha256: Digest([0; 32]), committed_at: Number(100), available_until: Number(120100),
        outputs: vec![Output { index: OutputIndex(0), length: Number(LENGTH), sha256: hash, content_type: ApplicationLabel("application/octet-stream".into()),
            locator: ResultLocator("pipestream://localhost:7443/v2/sessions/1/scopes/0/producers/0/entities/1/attempts/1/outputs/0".into()) }],
    };
    let reference = journal
        .remember_reference(&manifest, OutputIndex(0))
        .unwrap();
    let path = directory.path().join("copies");
    let copies = ResultStore::initialize(
        &path,
        labels.0.clone(),
        labels.1.clone(),
        ResultPolicy {
            objects: Id(1),
            bytes: Number(LENGTH + 524),
            owner_objects: Id(1),
            owner_bytes: Number(LENGTH + 524),
            chunk_bytes: Id(8192),
            handles: Id(4),
            owner_handles: Id(4),
        },
    )
    .unwrap();
    let export_path = directory.path().join("exports");
    let export_policy = ExportPolicy {
        objects: 1,
        bytes: LENGTH + 112,
    };
    let started = Instant::now();
    let sample = heap::Sample::start();
    let mut stage = copies
        .stage(reference.clone(), &caps, Instant::now())
        .unwrap();
    for _ in 0..LENGTH / block.len() as u64 {
        stage.receive(&block, Instant::now()).unwrap();
    }
    let installed = stage
        .finish(
            &ResultHeader {
                kind: Literal,
                request: Id(1),
                generation: Id(1),
                work,
                attempt: Id(1),
                index: OutputIndex(0),
                length: Number(LENGTH),
                sha256: hash,
            },
            Instant::now(),
        )
        .unwrap();
    let key = installed.key().to_owned();
    drop(installed);
    let exports = ExportStore::initialize(
        &export_path,
        labels.0.clone(),
        labels.1.clone(),
        export_policy,
    )
    .unwrap();
    let id = OperationId([1; 16]);
    let first = exports
        .export(
            id,
            &reference,
            &mut copies.find(&reference).unwrap().unwrap(),
        )
        .unwrap();
    assert!(!first.replayed);
    assert_eq!(exports.usage().unwrap().charged_bytes, LENGTH + 112);
    assert_eq!(copies.usage().unwrap().charged_bytes, LENGTH + 524);
    let file_bytes = fs::read_dir(&export_path)
        .unwrap()
        .map(|entry| entry.unwrap().metadata().unwrap().len())
        .sum::<u64>();
    assert_eq!(file_bytes, LENGTH + 112 + 72);
    assert!(
        exports
            .export(
                OperationId([2; 16]),
                &reference,
                &mut copies.find(&reference).unwrap().unwrap()
            )
            .is_err()
    );
    drop(exports);
    let exports = ExportStore::open(&export_path, labels.0, labels.1, export_policy).unwrap();
    assert!(
        exports
            .export(
                id,
                &reference,
                &mut copies.find(&reference).unwrap().unwrap()
            )
            .unwrap()
            .replayed
    );
    // Independently inspect the raw file without allocating an object-sized buffer.
    let mut file = fs::File::open(first.path).unwrap();
    let mut actual = [0; 8192];
    let mut count = 0;
    loop {
        let n = file.read(&mut actual).unwrap();
        if n == 0 {
            break;
        }
        assert!(actual[..n].iter().all(|b| *b == 0x65));
        count += n as u64;
    }
    assert_eq!(count, LENGTH);
    drop(file);
    exports.remove(id).unwrap();
    copies.remove(&key).unwrap();
    assert_eq!(exports.usage().unwrap().charged_bytes, 0);
    assert_eq!(copies.usage().unwrap().charged_bytes, 0);
    let (peak, largest) = sample.finish();
    assert!(peak < 256 << 10, "Rust heap increase {peak}");
    assert!(largest < 64 << 10, "largest Rust allocation {largest}");
    println!(
        "V2 local export: bytes={LENGTH} chunk_bytes=8192 rust_heap_peak_increase={peak} largest_rust_allocation={largest} export_file_bytes={file_bytes} replay_verified=true cleanup_charged_bytes=0 elapsed_ms={}",
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
