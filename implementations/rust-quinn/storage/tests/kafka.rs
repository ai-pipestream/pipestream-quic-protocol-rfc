//! Docker-based contract run of the Kafka journal backend.
//!
//! Skips silently when no docker daemon is available. The Kafka backend
//! implements the Journal role only (payloads do not belong in the log),
//! so it certifies against `contract::run_journal`.

mod common;

use pipestream_storage::contract::{self};
use pipestream_storage::{KafkaConfig, KafkaJournal};

fn open_with_retry(config: &KafkaConfig) -> KafkaJournal {
    let mut attempt = 0;
    loop {
        match KafkaJournal::open(config) {
            Ok(journal) => return journal,
            Err(_) if attempt < 150 => {
                attempt += 1;
                std::thread::sleep(std::time::Duration::from_millis(200));
            }
            Err(error) => panic!("open kafka journal: {error}"),
        }
    }
}

#[test]
fn kafka_backend_passes_the_journal_contract() {
    let Some(kafka) = common::KafkaContainer::start() else {
        eprintln!("docker unavailable: skipping kafka contract run");
        return;
    };
    let fresh = || open_with_retry(&kafka.fresh_config());
    let durable_config = kafka.fresh_config();
    let reopen = || open_with_retry(&durable_config);
    contract::run_journal(fresh, reopen);
}


/// The Kafka backend has no ObjectStore role: its report is the expected
/// file from the journal marker onward (per the README's slicing rule).
#[test]
fn kafka_probe_matches_expected_report_without_scopes() {
    use pipestream_storage::probe::RoleSelection;
    let Some(kafka) = common::KafkaContainer::start() else {
        eprintln!("docker unavailable: skipping kafka probe");
        return;
    };
    let mut journal =
        pipestream_storage::probe::JournalOnly(open_with_retry(&kafka.fresh_config()));
    let base = concat!(env!("CARGO_MANIFEST_DIR"), "/../../../test-vectors/storage-spi");
    let script = std::fs::read_to_string(format!("{base}/probe-script-v1.txt")).expect("script");
    let expected =
        std::fs::read_to_string(format!("{base}/expected-report-v1.txt")).expect("expected");
    let report = pipestream_storage::probe::run(&mut journal, RoleSelection::JOURNAL_ONLY, &script)
        .expect("probe");
    let cut = expected.find("journal\n").expect("journal section");
    assert_eq!(report, expected[cut..]);
}
