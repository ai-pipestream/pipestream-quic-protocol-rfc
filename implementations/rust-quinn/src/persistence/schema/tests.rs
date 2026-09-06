use super::*;

fn schema_image(connection: &Connection) -> Vec<(String, Option<String>)> {
    connection
        .prepare("SELECT name, sql FROM sqlite_schema ORDER BY name")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

#[test]
fn reopen_refuses_missing_or_changed_owned_schema_without_repair() {
    for alteration in [
        "DROP INDEX pipestream_jobs_principal",
        "DROP INDEX pipestream_storage_principal",
        "DROP INDEX pipestream_jobs_principal; CREATE INDEX pipestream_jobs_principal ON pipestream_jobs (execution_key)",
        "ALTER TABLE pipestream_jobs ADD COLUMN unexpected INTEGER",
        "ALTER TABLE pipestream_sessions ADD COLUMN unexpected INTEGER",
        "DROP TABLE pipestream_jobs",
        "DROP TABLE pipestream_storage_sessions",
        "DROP TABLE pipestream_sessions",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("schema.sqlite3");
        let store = SqliteSessionStore::open(&path).unwrap();
        let (mut session, key, input) = crate::jobs::tests::fixture("retained", None);
        session.enqueue_job(key, input, 100).unwrap();
        store.create(&session).unwrap();
        // Keep a handle alive so last-close checkpointing cannot obscure writes.
        let connection = store.connect().unwrap();
        connection
            .execute_batch("PRAGMA foreign_keys=OFF;")
            .unwrap();
        connection.execute_batch(alteration).unwrap();
        // The public checkpoint now checks the root ownership schema too. Use
        // the already-held diagnostic connection to snapshot deliberate corruption.
        connection
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        let before = schema_image(&connection);
        let database = fs::read(&path).unwrap();
        let wal_path = path.with_file_name("schema.sqlite3-wal");
        let wal = fs::read(&wal_path).unwrap();
        assert!(
            matches!(SqliteSessionStore::open(&path), Err(StoreError::Corrupt(_))),
            "accepted changed schema: {alteration}"
        );
        assert_eq!(schema_image(&connection), before, "{alteration}");
        assert_eq!(fs::read(&path).unwrap(), database, "{alteration}");
        assert_eq!(fs::read(&wal_path).unwrap(), wal, "{alteration}");
    }
}

#[test]
fn session_identity_is_bounded_before_audit_materialization_and_admission() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteSessionStore::open(dir.path().join("ids.sqlite3")).unwrap();
    let original = Session::new("retained", 7, 100).unwrap();
    store.create(&original).unwrap();
    for id in ["a".repeat(129), "non ASCII \u{00e9}".into(), "".into()] {
        let mut invalid = original.clone();
        invalid.session_id = id;
        assert!(matches!(
            store.create(&invalid),
            Err(StoreError::Protocol(_))
        ));
        assert_eq!(store.list_session_ids().unwrap(), ["retained"]);
    }
    let connection = store.connect().unwrap();
    connection
        .execute_batch("PRAGMA foreign_keys=OFF;")
        .unwrap();
    connection
        .execute(
            "UPDATE pipestream_sessions SET session_id = ?1",
            ["a".repeat(1 << 20)],
        )
        .unwrap();
    assert!(matches!(
        store.list_session_ids(),
        Err(StoreError::Corrupt(_))
    ));
    assert!(matches!(
        store.integrity_check(),
        Err(StoreError::Corrupt(_))
    ));
    assert!(matches!(
        store.create(&original),
        Err(StoreError::Corrupt(_))
    ));
}
