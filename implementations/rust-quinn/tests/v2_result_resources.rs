#![cfg(unix)]

use pipestream_core::{
    persistence::PhysicalLimits,
    v2::{
        authority::{
            execution::{Application, ApplicationOutcome, Executor, ResultEndpoint, WorkContext},
            ingress::{Applications, InputPreparation, InputReception, RestartSafety},
            payload::{PayloadPolicy, PayloadStore},
            results::{ReadCursor, ResultService},
            *,
        },
        *,
    },
};
use sha2::{Digest as _, Sha256};
use std::{
    fs,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

#[path = "support/heap.rs"]
mod heap;
const LENGTH: u64 = 32 << 20;
struct TestPolicy;
struct TestClock(AtomicU64);
impl Clock for TestClock {
    fn read(&self) -> ClockReading {
        ClockReading {
            utc_ms: Number(self.0.load(Ordering::SeqCst)),
            trusted: true,
        }
    }
}
impl Authorization for TestPolicy {
    fn permits(&self, owner: &IdentityLabel, _: Permission) -> bool {
        owner.0 == "resource-owner"
    }
}
struct Produce;
impl Application for Produce {
    fn execute(&self, context: &mut WorkContext) -> Result<ApplicationOutcome, StoreError> {
        context.begin_output(
            Number(LENGTH),
            ApplicationLabel("application/octet-stream".into()),
        )?;
        let block = [0x5au8; 16384];
        for _ in 0..LENGTH / block.len() as u64 {
            context.write_output(&block)?;
        }
        context.finish_output()?;
        Ok(ApplicationOutcome::Succeeded)
    }
}

#[test]
fn thirty_two_mib_result_delivery_has_bounded_heap_handles_and_no_database_growth() {
    let directory = tempfile::tempdir().unwrap();
    let clock = Arc::new(TestClock(AtomicU64::new(1000)));
    let store = AuthorityStore::initialize(
        &directory.path().join("authority.sqlite"),
        IdentityLabel("resource-authority".into()),
        StorePolicy {
            owners: Id(1),
            sessions: Id(1),
            sessions_per_owner: Id(1),
            active_jobs: Id(1),
            active_jobs_per_owner: Id(1),
            session_limits: Limits {
                scopes: Id(1),
                entities: Id(1),
                operations: Id(2),
                active_jobs: Id(1),
                retained_input_bytes: Number(0),
                retained_output_bytes: Number(LENGTH),
            },
        },
        PhysicalLimits::default(),
        clock.clone(),
        Arc::new(TestPolicy),
    )
    .unwrap();
    let payloads = PayloadStore::initialize(
        &directory.path().join("objects"),
        store.payload_identity().unwrap(),
        PayloadPolicy {
            objects: Id(3),
            bytes: Number(LENGTH + 16384),
            owner_objects: Id(3),
            owner_bytes: Number(LENGTH + 16384),
            chunk_bytes: Id(16384),
            handles: Id(8),
            owner_handles: Id(8),
        },
    )
    .unwrap();
    store.bind_payloads(&payloads).unwrap();
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
        object_limit: Number(LENGTH),
        stream_idle_ms: IdleMs(10000),
        stream_lifetime_ms: LifetimeMs(60000),
    };
    let binding = store
        .create_session(
            &IdentityLabel("resource-owner".into()),
            Id(1),
            &Policy {
                execution_limit_ms: Duration(1000),
                output_retention_ms: Duration(20000),
                receipt_retention_ms: Duration(30000),
            },
            &caps,
        )
        .unwrap();
    let work = WorkKey {
        scope: Number(0),
        producer: Producer(0),
        entity: Id(1),
    };
    store
        .declare(
            &binding.identity,
            OperationId([1; 16]),
            Number(0),
            &[Id(1)],
            true,
        )
        .unwrap();
    let mut apps = Applications::default();
    apps.register(
        ApplicationLabel("produce/v1".into()),
        vec![Mode(0)],
        RestartSafety::Pure,
        Arc::new(Produce),
    )
    .unwrap();
    let apps = Arc::new(apps);
    let header = InputHeader {
        kind: Literal,
        generation: binding.identity.generation,
        operation: OperationId([2; 16]),
        parameters: AdmitParameters {
            work: work.clone(),
            input: Input {
                length: Number(0),
                sha256: Digest(Sha256::digest([]).into()),
                content_type: ApplicationLabel("application/octet-stream".into()),
            },
            application: ApplicationLabel("produce/v1".into()),
            mode: Mode(0),
            execution_ms: Duration(1000),
            outputs: OutputBudget {
                count: BatchCount(1),
                total_bytes: Number(LENGTH),
            },
        },
    };
    let now = Instant::now();
    let InputReception::Receiving(receiving) = store
        .receive_input(&binding.identity, &header, &caps, &payloads, &apps, now)
        .unwrap()
    else {
        panic!("unexpected replay")
    };
    let InputPreparation::Ready(prepared) = store
        .prepare_input(receiving.finish(now).unwrap(), &caps, &apps)
        .unwrap()
    else {
        panic!("unexpected replay")
    };
    store.admit_input(*prepared, &caps, &apps).unwrap();
    let executor = Executor::new(
        store.clone(),
        payloads.clone(),
        apps,
        ResultEndpoint::new("results.example:7443".into()).unwrap(),
        caps.clone(),
        Duration(100),
    )
    .unwrap();
    let completed = executor.run(&binding.identity, &work).unwrap();
    let manifest = completed.manifest.unwrap();
    let initial_usage = payloads.usage(None).unwrap();
    let before = store.physical_usage().unwrap();
    let sample = heap::Sample::start();
    let results = ResultService::new(store.clone(), payloads.clone()).unwrap();
    let now = Instant::now();
    let mut held = Vec::new();
    for n in 1..=8 {
        held.push(
            results
                .begin_read(
                    &binding.identity,
                    &ResultMessage::Read {
                        request: Id(n),
                        work: work.clone(),
                        attempt: Id(1),
                        index: OutputIndex(0),
                        expected_sha256: manifest.outputs[0].sha256,
                    },
                    &caps,
                    now,
                )
                .unwrap(),
        );
    }
    let refused = results.begin_read(
        &binding.identity,
        &ResultMessage::Read {
            request: Id(9),
            work: work.clone(),
            attempt: Id(1),
            index: OutputIndex(0),
            expected_sha256: manifest.outputs[0].sha256,
        },
        &caps,
        now,
    );
    assert!(matches!(
        refused,
        Err(StoreError::Protocol(Error {
            code: ErrorCode::LimitExceeded,
            ..
        }))
    ));
    let mut read = held.pop().unwrap();
    let header = read.start(Instant::now()).unwrap();
    let mut receiver =
        PayloadReceiver::new(header.length, header.sha256, &caps, Instant::now()).unwrap();
    let mut bytes = [0; 16384];
    let mut received = 0u64;
    loop {
        let n = read.read_chunk(&mut bytes, Instant::now()).unwrap();
        if n == 0 {
            break;
        }
        assert!(bytes[..n].iter().all(|b| *b == 0x5a));
        read.check_deadline(Instant::now()).unwrap();
        receiver.receive(&bytes[..n], Instant::now()).unwrap();
        read.sent(n, Instant::now()).unwrap();
        received += n as u64;
    }
    assert_eq!(
        receiver.finish(Instant::now()).unwrap().sha256(),
        manifest.outputs[0].sha256
    );
    read.check_deadline(Instant::now()).unwrap();
    read.finish(Instant::now()).unwrap();
    assert_eq!(received, LENGTH);
    let report = results
        .maintain(
            &mut ReadCursor::default(),
            8,
            now + std::time::Duration::from_secs(60),
        )
        .unwrap();
    assert_eq!(report.closed, 7); // Idle handles need not be dropped by the caller.
    assert_eq!(results.pending().unwrap(), 0);
    let (peak, largest) = sample.finish();
    assert!(peak < 256 << 10, "Rust heap increase {peak} bytes");
    assert!(
        largest < 64 << 10,
        "largest Rust allocation {largest} bytes"
    );
    assert_eq!(payloads.usage(None).unwrap(), initial_usage);
    let after = store.physical_usage().unwrap();
    assert_eq!(before.database_bytes, after.database_bytes);
    assert_eq!(before.wal_bytes, after.wal_bytes);
    println!(
        "V2 result delivery: bytes={received} pending_limit=8 chunk_bytes={} rust_heap_peak_increase={peak} largest_rust_allocation={largest} database_bytes={} wal_bytes={} elapsed_ms={}",
        bytes.len(),
        after.database_bytes,
        after.wal_bytes,
        now.elapsed().as_millis()
    );
    #[cfg(target_os = "linux")]
    for line in fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .filter(|l| l.starts_with("VmRSS:") || l.starts_with("VmHWM:"))
    {
        println!("process {line}");
    }
    store.integrity_check().unwrap();

    let pinned = results
        .begin_read(
            &binding.identity,
            &ResultMessage::Read {
                request: Id(10),
                work,
                attempt: Id(1),
                index: OutputIndex(0),
                expected_sha256: manifest.outputs[0].sha256,
            },
            &caps,
            Instant::now(),
        )
        .unwrap();
    clock.0.store(21000, Ordering::SeqCst);
    let before = store.physical_usage().unwrap();
    let cleanup_started = Instant::now();
    let sample = heap::Sample::start();
    let mut cursor = RetentionCursor::default();
    let mut files = 0;
    store.audit_payloads(&payloads).unwrap();
    for _ in 0..40 {
        let report = store.reclaim(&payloads, &mut cursor, 1).unwrap();
        assert!(report.inspected_files <= 1 && report.inspected_jobs <= 1);
        files += report.removed_files;
    }
    assert_eq!(
        files, 1,
        "only the input can be deleted while the result is pinned"
    );
    assert!(payloads.usage(None).unwrap().charged_bytes >= LENGTH);
    drop(pinned);
    for _ in 0..40 {
        let report = store.reclaim(&payloads, &mut cursor, 1).unwrap();
        files += report.removed_files;
    }
    store.audit_payloads(&payloads).unwrap();
    let (peak, largest) = sample.finish();
    assert!(peak < 256 << 10, "cleanup Rust heap increase {peak}");
    assert!(
        largest < 64 << 10,
        "cleanup largest Rust allocation {largest}"
    );
    assert_eq!(files, 3);
    assert_eq!(payloads.usage(None).unwrap().charged_bytes, 0);
    let after = store.physical_usage().unwrap();
    assert_eq!(before.database_bytes, after.database_bytes);
    println!(
        "V2 result reclamation: bytes={LENGTH} batch=1 removed_files={files} rust_heap_peak_increase={peak} largest_rust_allocation={largest} database_bytes={} wal_bytes={} elapsed_ms={}",
        after.database_bytes,
        after.wal_bytes,
        cleanup_started.elapsed().as_millis()
    );
    store.integrity_check().unwrap();

    let mut closure = ReconcileCursor::default();
    for _ in 0..16 {
        store.reconcile(&mut closure, 1).unwrap();
    }
    clock.0.store(51000, Ordering::SeqCst);
    store.checkpoint_storage().unwrap();
    let before = store.physical_usage().unwrap();
    let retirement_started = Instant::now();
    let sample = heap::Sample::start();
    let mut cursor = RetirementCursor::default();
    let mut completed = false;
    let mut deleted = 0;
    for _ in 0..32 {
        let report = store.retire(&payloads, &mut cursor, 1).unwrap();
        let rows = report.deleted_work + report.deleted_scopes + report.deleted_operations;
        assert!(rows <= 1);
        deleted += rows;
        store.integrity_check().unwrap();
        if report.completed {
            completed = true;
            break;
        }
    }
    let (peak, largest) = sample.finish();
    assert!(completed);
    assert!(peak < 256 << 10, "retirement Rust heap increase {peak}");
    assert!(
        largest < 64 << 10,
        "retirement largest Rust allocation {largest}"
    );
    let after = store.physical_usage().unwrap();
    assert_eq!(before.database_bytes, after.database_bytes);
    println!(
        "V2 session retirement: batch=1 deleted_units={deleted} rust_heap_peak_increase={peak} largest_rust_allocation={largest} database_bytes={} wal_bytes={} elapsed_ms={}",
        after.database_bytes,
        after.wal_bytes,
        retirement_started.elapsed().as_millis()
    );
    assert!(matches!(
        store.create_session(&binding.identity.owner, Id(1), &binding.policy, &caps),
        Err(StoreError::Protocol(Error {
            code: ErrorCode::Expired,
            ..
        }))
    ));
    let next = store
        .create_session(&binding.identity.owner, Id(2), &binding.policy, &caps)
        .unwrap();
    assert_eq!(next.identity.generation, Id(2));
}
