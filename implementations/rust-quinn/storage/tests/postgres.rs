//! Docker-based contract run of the PostgreSQL backend.
//!
//! Skips silently when no docker daemon is available. Uses a real
//! `postgres:18.4-alpine` container; every `fresh()` gets a brand-new
//! database so journal identity columns start at 1 as the contract expects.

mod common;

use pipestream_storage::PostgresStore;
use pipestream_storage::contract;

#[test]
fn postgres_backend_passes_the_spi_contract() {
    let Some(pg) = common::PostgresContainer::start() else {
        eprintln!("docker unavailable: skipping postgres contract run");
        return;
    };
    let fresh = || PostgresStore::open(&pg.fresh_database()).expect("open fresh store");
    let durable_db = pg.fresh_database();
    let reopen = || PostgresStore::open(&durable_db).expect("reopen durable store");
    contract::run_all(fresh, reopen);
}

#[test]
fn postgres_verify_detects_tampered_payload() {
    use pipestream_storage::{ObjectStore, ScopePath, StorageError, layout, sha256};
    let Some(pg) = common::PostgresContainer::start() else {
        eprintln!("docker unavailable: skipping postgres tamper test");
        return;
    };
    let db = pg.fresh_database();
    let mut store = PostgresStore::open(&db).expect("open store");
    let scope = ScopePath::root(0);
    let payload = b"postgres integrity";
    store
        .put(&scope, 3, payload, &sha256(payload))
        .expect("put");
    let digest = layout::scope_digest(&[(3, sha256(payload))]).expect("scope digest");
    store.seal_scope(&scope, &digest, 1).expect("seal");
    // Tamper behind the backend's back.
    let mut client = postgres::Client::connect(&db, postgres::NoTls).expect("connect");
    client
        .execute(
            "UPDATE pipestream_objects SET payload = $1 WHERE scope_path = $2 AND entity_id = 3",
            &[&b"forged".as_slice(), &scope.to_path_string()],
        )
        .expect("tamper");
    match store.verify_scope(&scope) {
        Err(StorageError::Corrupt(_)) => {}
        other => panic!("expected Corrupt after tampering, got {other:?}"),
    }
}

fn probe_vectors() -> (String, String) {
    let base = concat!(env!("CARGO_MANIFEST_DIR"), "/../../../test-vectors/storage-spi");
    let script = std::fs::read_to_string(format!("{base}/probe-script-v1.txt")).expect("script");
    let expected =
        std::fs::read_to_string(format!("{base}/expected-report-v1.txt")).expect("expected");
    (script, expected)
}

#[test]
fn postgres_probe_matches_expected_report_byte_for_byte() {
    use pipestream_storage::probe::{self, RoleSelection};
    let Some(pg) = common::PostgresContainer::start() else {
        eprintln!("docker unavailable: skipping postgres probe");
        return;
    };
    let mut store = PostgresStore::open(&pg.fresh_database()).expect("open store");
    let (script, expected) = probe_vectors();
    let report = probe::run(&mut store, RoleSelection::BOTH, &script).expect("probe");
    assert_eq!(report, expected);
}

/// Bounded-memory proof for the streaming writer: a 64 MiB payload written
/// in 64 KiB calls must not grow RSS by more than the payload size.
#[test]
fn postgres_streaming_put_stays_bounded_on_large_payloads() {
    use pipestream_storage::{ObjectStore, PutWriter, ScopePath, sha256};
    use std::io::Write as _;
    let Some(pg) = common::PostgresContainer::start() else {
        eprintln!("docker unavailable: skipping bounded-memory test");
        return;
    };
    let db = pg.fresh_database();
    let mut store = PostgresStore::open(&db).expect("open store");
    let scope = ScopePath::root(1);
    let total = 64usize << 20;
    let payload: Vec<u8> = (0..total).map(|i| ((i / 4096) % 251) as u8).collect();
    let digest = sha256(&payload);
    let rss_before = vm_rss_kib();
    {
        let mut writer = store.begin_put(&scope, 1, &digest).expect("begin_put");
        for chunk in payload.chunks(64 * 1024) {
            writer.write_all(chunk).expect("write");
        }
        writer.finish().expect("finish");
    }
    let rss_after = vm_rss_kib();
    // The whole payload was resident for the digest computation, but the
    // writer must not retain a second copy: growth well under one payload.
    let budget = 48 * 1024;
    assert!(
        rss_after.saturating_sub(rss_before) < budget,
        "RSS grew {} KiB (budget {budget} KiB)",
        rss_after.saturating_sub(rss_before)
    );
    // Read back and verify, including the chunked read path.
    let mut reader = store.open_read(&scope, 1).expect("open_read");
    let mut back = Vec::with_capacity(total);
    std::io::Read::read_to_end(&mut reader, &mut back).expect("read");
    assert_eq!(sha256(&back), digest);
    let canonical = pipestream_storage::layout::scope_digest(&[(1, sha256(&payload))]).unwrap();
    store.seal_scope(&scope, &canonical, 1).expect("seal");
    store.verify_scope(&scope).expect("verify_scope");
    drop(store);
    // Reopen: chunked content survives and re-verifies.
    let store = PostgresStore::open(&db).unwrap();
    store.verify_scope(&scope).expect("verify after reopen");
    let mut reader = store.open_read(&scope, 1).unwrap();
    let mut back = Vec::new();
    std::io::Read::read_to_end(&mut reader, &mut back).unwrap();
    assert_eq!(back.len(), total);
}

fn vm_rss_kib() -> usize {
    let status = std::fs::read_to_string("/proc/self/status").expect("status");
    status
        .lines()
        .find(|line| line.starts_with("VmRSS:"))
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse().ok())
        .expect("VmRSS")
}
