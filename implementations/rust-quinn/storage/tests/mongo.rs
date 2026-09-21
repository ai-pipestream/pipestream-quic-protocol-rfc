//! Docker-based contract run of the MongoDB backend (both roles).
//!
//! Skips silently when no docker daemon is available. Uses a real
//! `mongo:8.2` container; every `fresh()` gets a brand-new database.

mod common;

use pipestream_storage::contract::{self};
use pipestream_storage::{MongoConfig, MongoStore, ObjectStore, ScopePath, sha256};

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


fn probe_vectors() -> (String, String) {
    let base = concat!(env!("CARGO_MANIFEST_DIR"), "/../../../test-vectors/storage-spi");
    let script = std::fs::read_to_string(format!("{base}/probe-script-v1.txt")).expect("script");
    let expected =
        std::fs::read_to_string(format!("{base}/expected-report-v1.txt")).expect("expected");
    (script, expected)
}

#[test]
fn mongo_probe_matches_expected_report_byte_for_byte() {
    use pipestream_storage::probe::RoleSelection;
    let Some(mongo) = common::MongoContainer::start() else {
        eprintln!("docker unavailable: skipping mongo probe");
        return;
    };
    let mut store = open_with_retry(&mongo.fresh_config());
    let (script, expected) = probe_vectors();
    let report = pipestream_storage::probe::run(&mut store, RoleSelection::BOTH, &script)
        .expect("probe");
    assert_eq!(report, expected);
}

#[test]
fn mongo_streaming_put_minimal() {
    use pipestream_storage::{ObjectStore, PutWriter, ScopePath, sha256};
    use std::io::Write as _;
    let Some(mongo) = common::MongoContainer::start() else {
        eprintln!("docker unavailable: skipping");
        return;
    };
    let mut store = open_with_retry(&mongo.fresh_config());
    let scope = ScopePath::root(1);
    let payload = b"streamed payload".to_vec();
    let digest = sha256(&payload);
    let mut writer = store.begin_put(&scope, 1, &digest).expect("begin_put");
    writer.write_all(&payload).expect("write");
    writer.finish().expect("finish");
    let mut reader = store.open_read(&scope, 1).expect("open_read");
    let mut bytes = Vec::new();
    std::io::Read::read_to_end(&mut reader, &mut bytes).expect("read");
    assert_eq!(bytes, payload);
}


/// Chunked roundtrip: a payload above the 1 MiB chunk size lands as chunk
/// documents and streams back through the lazy reader, bounded in memory.
#[test]
fn mongo_chunked_streaming_put_roundtrip() {
    use pipestream_storage::{ObjectStore, PutWriter, ScopePath, sha256};
    use std::io::Write as _;
    let Some(mongo) = common::MongoContainer::start() else {
        eprintln!("docker unavailable: skipping chunked test");
        return;
    };
    let config = mongo.fresh_config();
    let mut store = open_with_retry(&config);
    let scope = ScopePath::root(1);
    let total = 8usize << 20;
    let payload: Vec<u8> = (0..total).map(|i| ((i / 4096) % 251) as u8).collect();
    let digest = sha256(&payload);
    {
        let mut writer = store.begin_put(&scope, 1, &digest).expect("begin_put");
        for chunk in payload.chunks(64 * 1024) {
            writer.write_all(chunk).expect("write");
        }
        writer.finish().expect("finish");
    }
    let mut reader = store.open_read(&scope, 1).expect("open_read");
    let mut back = Vec::with_capacity(total);
    std::io::Read::read_to_end(&mut reader, &mut back).expect("read");
    assert_eq!(back.len(), total);
    assert_eq!(sha256(&back), digest);
    let canonical = pipestream_storage::layout::scope_digest(&[(1, sha256(&payload))]).unwrap();
    store.seal_scope(&scope, &canonical, 1).expect("seal");
    store.verify_scope(&scope).expect("verify_scope");
    drop(store);
    let store = open_with_retry(&config);
    store.verify_scope(&scope).expect("verify after reopen");
    let mut reader = store.open_read(&scope, 1).unwrap();
    let mut back = Vec::new();
    std::io::Read::read_to_end(&mut reader, &mut back).unwrap();
    assert_eq!(back.len(), total);
}


/// Schema-collision regression: a database shared with the Java lane holds
/// Java-shaped documents (ObjectId _id, scope_path as a top-level string).
/// list_scopes must enumerate only Rust-schema documents and never error.
#[test]
fn list_scopes_ignores_java_shaped_documents() {
    let Some(mongo) = common::MongoContainer::start() else {
        eprintln!("docker unavailable: skipping schema-collision test");
        return;
    };
    let config = mongo.fresh_config();
    let mut store = open_with_retry(&config);
    // One Rust-shaped entity.
    let scope = ScopePath::root(0);
    let payload = b"rust-shaped".to_vec();
    store.put(&scope, 1, &payload, &sha256(&payload)).unwrap();
    // Java-shaped documents in the same collections.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let client = mongodb::Client::with_uri_str(&config.uri).await.unwrap();
        let db = client.database(&config.database);
        db.collection::<mongodb::bson::Document>("objects")
            .insert_one(mongodb::bson::doc! {
                "scope_path": "9",
                "bucket": "ab",
                "entity_id": 1_i64,
                "digest": mongodb::bson::Binary {
                    subtype: mongodb::bson::spec::BinarySubtype::Generic,
                    bytes: vec![0u8; 32],
                },
                "payload": mongodb::bson::Binary {
                    subtype: mongodb::bson::spec::BinarySubtype::Generic,
                    bytes: b"java-shaped".to_vec(),
                },
            })
            .await
            .unwrap();
        db.collection::<mongodb::bson::Document>("seals")
            .insert_one(mongodb::bson::doc! {
                "scope_path": "9",
                "digest": mongodb::bson::Binary {
                    subtype: mongodb::bson::spec::BinarySubtype::Generic,
                    bytes: vec![0u8; 32],
                },
                "entity_count": 1_i64,
            })
            .await
            .unwrap();
    });
    let scopes = store.list_scopes().expect("list_scopes");
    assert_eq!(scopes, vec![scope]);
}
