use super::*;

#[test]
fn identity_and_pair_are_stable_and_cannot_be_reassigned() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sessions.sqlite3");
    let store = SqliteSessionStore::open(&path).unwrap();
    let initial = store.payload_binding().unwrap();
    assert!(initial.payloads().is_none());
    assert_eq!(
        SqliteSessionStore::open(&path)
            .unwrap()
            .payload_binding()
            .unwrap(),
        initial
    );
    assert!(matches!(
        store.bind_payload_store(initial),
        Err(StoreError::Protocol(_))
    ));
    let expected = PayloadBinding::new(initial.database(), StoreIdentity::generate().unwrap());
    store.bind_payload_store(expected).unwrap();
    store.bind_payload_store(expected).unwrap();
    assert_eq!(
        SqliteSessionStore::open(&path)
            .unwrap()
            .payload_binding()
            .unwrap(),
        expected
    );
    let another = PayloadBinding::new(initial.database(), StoreIdentity::generate().unwrap());
    assert!(matches!(
        store.bind_payload_store(another),
        Err(StoreError::Protocol(_))
    ));
    let foreign = PayloadBinding::new(
        StoreIdentity::generate().unwrap(),
        expected.payloads().unwrap(),
    );
    assert!(matches!(
        store.bind_payload_store(foreign),
        Err(StoreError::Protocol(_))
    ));
    assert_eq!(store.payload_binding().unwrap(), expected);
    store.integrity_check().unwrap();
}

#[test]
fn encoded_binding_refuses_corruption_reserved_ids_and_wrong_geometry() {
    let binding = PayloadBinding::new(
        StoreIdentity::generate().unwrap(),
        StoreIdentity::generate().unwrap(),
    );
    let original = binding.encode();
    assert_eq!(PayloadBinding::decode(&original).unwrap(), binding);
    for offset in 0..original.len() {
        let mut changed = original;
        changed[offset] ^= 1;
        assert!(matches!(
            PayloadBinding::decode(&changed),
            Err(StoreError::Corrupt(_))
        ));
    }
    for length in 0..original.len() {
        assert!(PayloadBinding::decode(&original[..length]).is_err());
    }
    assert!(PayloadBinding::decode(&[0; 73]).is_err());
    let mut zero = original;
    zero[8..24].fill(0);
    let checksum = Sha256::digest(&zero[..40]);
    zero[40..].copy_from_slice(&checksum);
    assert!(PayloadBinding::decode(&zero).is_err());
    assert!(StoreIdentity::from_bytes([0; 16]).is_err());
}

#[test]
fn failed_blob_write_rolls_back_binding_and_exact_retry_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteSessionStore::open(dir.path().join("sessions.sqlite3")).unwrap();
    let initial = store.payload_binding().unwrap();
    let expected = PayloadBinding::new(initial.database(), StoreIdentity::generate().unwrap());
    let connection = store.connect().unwrap();
    connection
        .execute_batch("CREATE INDEX prevent_binding_blob ON pipestream_payload_binding(image)")
        .unwrap();
    assert!(matches!(
        store.bind_payload_store(expected),
        Err(StoreError::Database(_))
    ));
    assert_eq!(store.payload_binding().unwrap(), initial);
    connection
        .execute_batch("DROP INDEX prevent_binding_blob")
        .unwrap();
    store.bind_payload_store(expected).unwrap();
    assert_eq!(store.payload_binding().unwrap(), expected);
}

#[test]
fn competing_connections_can_only_commit_one_payload_identity() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sessions.sqlite3");
    let store = SqliteSessionStore::open(&path).unwrap();
    let database = store.payload_binding().unwrap().database();
    let start = std::sync::Arc::new(std::sync::Barrier::new(4));
    let attempts: Vec<_> = (0..4)
        .map(|_| {
            let store = SqliteSessionStore::open(&path).unwrap();
            let start = start.clone();
            std::thread::spawn(move || {
                let pair = PayloadBinding::new(database, StoreIdentity::generate().unwrap());
                start.wait();
                (pair, store.bind_payload_store(pair))
            })
        })
        .collect();
    let results: Vec<_> = attempts.into_iter().map(|t| t.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|(_, r)| r.is_ok()).count(), 1);
    let winner = results.iter().find(|(_, r)| r.is_ok()).unwrap().0;
    for (_, result) in results {
        if let Err(error) = result {
            assert!(matches!(error, StoreError::Protocol(_)));
        }
    }
    assert_eq!(store.payload_binding().unwrap(), winner);
}

#[test]
fn malformed_or_missing_binding_refuses_without_schema_repair() {
    for alteration in [
        "UPDATE pipestream_payload_binding SET image=zeroblob(72)",
        "DELETE FROM pipestream_payload_binding",
        "DROP TABLE pipestream_payload_binding",
        "PRAGMA ignore_check_constraints=ON; UPDATE pipestream_payload_binding SET image=zeroblob(5000000)",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.sqlite3");
        let store = SqliteSessionStore::open(&path).unwrap();
        let connection = store.connect().unwrap();
        connection.execute_batch(alteration).unwrap();
        connection
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        let before = fs::read(&path).unwrap();
        assert!(store.payload_binding().is_err(), "{alteration}");
        assert!(store.integrity_check().is_err(), "{alteration}");
        assert!(
            matches!(SqliteSessionStore::open(&path), Err(StoreError::Corrupt(_))),
            "{alteration}"
        );
        assert_eq!(fs::read(&path).unwrap(), before, "{alteration}");
    }
}

#[test]
fn live_handle_refuses_a_replaced_database_identity() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteSessionStore::open(dir.path().join("sessions.sqlite3")).unwrap();
    let other = PayloadBinding::new(
        StoreIdentity::generate().unwrap(),
        StoreIdentity::generate().unwrap(),
    );
    store
        .connect()
        .unwrap()
        .execute(
            "UPDATE pipestream_payload_binding SET image=?1",
            [other.encode().as_slice()],
        )
        .unwrap();
    assert!(matches!(
        store.payload_binding(),
        Err(StoreError::Corrupt(_))
    ));
    assert!(matches!(
        store.create(&Session::new("not-admitted", 7, 100).unwrap()),
        Err(StoreError::Corrupt(_))
    ));
}

#[test]
fn previous_physical_policy_is_not_converted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sessions.sqlite3");
    drop(SqliteSessionStore::open(&path).unwrap());
    let policy = dir.path().join("sessions.sqlite3.pslimits");
    let mut bytes = fs::read(&policy).unwrap();
    bytes[..8].copy_from_slice(b"PSDBL002");
    let checksum = Sha256::digest(&bytes[..40]);
    bytes[40..].copy_from_slice(&checksum);
    fs::write(&policy, &bytes).unwrap();
    assert!(matches!(
        SqliteSessionStore::open(&path),
        Err(StoreError::Corrupt(_))
    ));
    assert_eq!(fs::read(policy).unwrap(), bytes);
}
