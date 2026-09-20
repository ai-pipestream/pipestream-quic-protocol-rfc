//! Docker-based contract run of the MongoDB backend (both roles).
//!
//! Skips silently when no docker daemon is available. Uses a real
//! `mongo:8.2` container; every `fresh()` gets a brand-new database.

mod common;

use pipestream_storage::contract::{self};
use pipestream_storage::{MongoConfig, MongoStore};

fn open_with_retry(config: &MongoConfig) -> MongoStore {
    let mut attempt = 0;
    loop {
        match MongoStore::open(config) {
            Ok(store) => return store,
            Err(_) if attempt < 100 => {
                attempt += 1;
                std::thread::sleep(std::time::Duration::from_millis(200));
            }
            Err(error) => panic!("open mongo store: {error}"),
        }
    }
}

#[test]
fn mongo_backend_passes_the_spi_contract() {
    let Some(mongo) = common::MongoContainer::start() else {
        eprintln!("docker unavailable: skipping mongo contract run");
        return;
    };
    let fresh = || open_with_retry(&mongo.fresh_config());
    let durable_config = mongo.fresh_config();
    let reopen = || open_with_retry(&durable_config);
    contract::run_all(fresh, reopen);
}
