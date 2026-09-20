//! Byte-identity gate for the cross-language probe: the pinned script in
//! `test-vectors/storage-spi/probe-script-v1.txt` must render exactly
//! `expected-report-v1.txt` on every local backend (the docker-backed
//! variants assert the same file from their own integration tests, sliced
//! per role per the README).

use pipestream_storage::probe::{self, RoleSelection};
use pipestream_storage::{FsStore, MemoryStore};

fn script() -> String {
    std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../test-vectors/storage-spi/probe-script-v1.txt"
    ))
    .expect("probe script")
}

fn expected() -> String {
    std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../test-vectors/storage-spi/expected-report-v1.txt"
    ))
    .expect("expected report")
}

#[test]
fn memory_probe_matches_expected_report_byte_for_byte() {
    let mut store = MemoryStore::new();
    let report = probe::run(&mut store, RoleSelection::BOTH, &script()).expect("probe");
    assert_eq!(report, expected());
}

#[test]
fn filesystem_probe_matches_expected_report_byte_for_byte() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut store = FsStore::open(dir.path()).expect("fs store");
    let report = probe::run(&mut store, RoleSelection::BOTH, &script()).expect("probe");
    assert_eq!(report, expected());
}
