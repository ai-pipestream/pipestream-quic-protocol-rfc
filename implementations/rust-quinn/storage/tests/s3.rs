//! Docker-based contract run of the S3 backend against MinIO.
//!
//! Skips silently when no docker daemon is available. The S3 backend
//! implements the ObjectStore role only, so it certifies against
//! `contract::run_object_store`; the journal role is exercised by the
//! log-capable backends.

mod common;

use pipestream_storage::S3Store;
use pipestream_storage::contract;

#[test]
fn s3_backend_passes_the_object_store_contract() {
    let Some(minio) = common::MinioContainer::start() else {
        eprintln!("docker unavailable: skipping s3 contract run");
        return;
    };
    // Bucket creation can race MinIO startup despite a live TCP port;
    // retry opening until the service answers.
    let open = |config: &pipestream_storage::S3Config| {
        let mut attempt = 0;
        loop {
            match S3Store::open(config) {
                Ok(store) => return store,
                Err(_) if attempt < 50 => {
                    attempt += 1;
                    std::thread::sleep(std::time::Duration::from_millis(200));
                }
                Err(error) => panic!("open s3 store: {error}"),
            }
        }
    };
    let fresh = || open(&minio.fresh_config());
    let durable_config = minio.fresh_config();
    let reopen = || open(&durable_config);
    contract::run_object_store(fresh, reopen);
}


/// The S3 backend has no Journal role: its report is the expected file
/// without the journal section (per the README's slicing rule).
#[test]
fn s3_probe_matches_expected_report_without_journal() {
    use pipestream_storage::probe::RoleSelection;
    let Some(minio) = common::MinioContainer::start() else {
        eprintln!("docker unavailable: skipping s3 probe");
        return;
    };
    let config = minio.fresh_config();
    let mut store = {
        let mut attempt = 0;
        loop {
            match S3Store::open(&config) {
                Ok(store) => break pipestream_storage::probe::StoreOnly(store),
                Err(_) if attempt < 50 => {
                    attempt += 1;
                    std::thread::sleep(std::time::Duration::from_millis(200));
                }
                Err(error) => panic!("open s3 store: {error}"),
            }
        }
    };
    let base = concat!(env!("CARGO_MANIFEST_DIR"), "/../../../test-vectors/storage-spi");
    let script = std::fs::read_to_string(format!("{base}/probe-script-v1.txt")).expect("script");
    let expected =
        std::fs::read_to_string(format!("{base}/expected-report-v1.txt")).expect("expected");
    let report = pipestream_storage::probe::run(&mut store, RoleSelection::STORE_ONLY, &script)
        .expect("probe");
    let cut = expected.find("journal\n").expect("journal section");
    assert_eq!(report, expected[..cut]);
}
