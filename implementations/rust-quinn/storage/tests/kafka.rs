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
