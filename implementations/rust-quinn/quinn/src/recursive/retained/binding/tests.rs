use super::*;
use crate::recursive::{EntityStore, ExemplarProcessor, FileEntityStore, RecursiveService};
use pipestream_core::persistence::SessionStore;

#[test]
fn file_store_and_database_pair_before_service_start_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let sessions = Arc::new(SqliteSessionStore::open(dir.path().join("sessions.sqlite3")).unwrap());
    let files = Arc::new(FileEntityStore::open(dir.path().join("payloads")).unwrap());
    let initial = sessions.payload_binding().unwrap();
    assert!(initial.payloads().is_none());
    let usage = files.retained_usage().unwrap();
    let service = RecursiveService::new(
        sessions.clone(),
        files.clone(),
        Arc::new(ExemplarProcessor::default()),
        7,
        100,
    )
    .unwrap();
    let pair = sessions.payload_binding().unwrap();
    assert_eq!(pair.payloads(), Some(files.retained.identity));
    assert_eq!(
        read_claim(files.root(), files.retained.identity).unwrap(),
        Some(pair)
    );
    assert_eq!(files.retained_usage().unwrap(), usage);
    assert!(sessions.list_session_ids().unwrap().is_empty());
    drop(service);
    drop(files);
    let reopened = FileEntityStore::open(dir.path().join("payloads")).unwrap();
    reopened.bind_session_store(&sessions).unwrap();
    assert_eq!(sessions.payload_binding().unwrap(), pair);
}

#[test]
fn mismatched_roots_or_databases_are_refused_before_service_admission() {
    let dir = tempfile::tempdir().unwrap();
    let a = Arc::new(SqliteSessionStore::open(dir.path().join("a.sqlite3")).unwrap());
    let b = Arc::new(SqliteSessionStore::open(dir.path().join("b.sqlite3")).unwrap());
    let files = Arc::new(FileEntityStore::open(dir.path().join("payloads")).unwrap());
    files.bind_session_store(&a).unwrap();
    assert!(
        RecursiveService::new(
            b.clone(),
            files.clone(),
            Arc::new(ExemplarProcessor::default()),
            7,
            100
        )
        .is_err()
    );
    assert!(b.payload_binding().unwrap().payloads().is_none());
    let foreign = Arc::new(FileEntityStore::open(dir.path().join("foreign")).unwrap());
    assert!(
        RecursiveService::new(
            a.clone(),
            foreign.clone(),
            Arc::new(ExemplarProcessor::default()),
            7,
            100
        )
        .is_err()
    );
    assert!(!foreign.root().join(BINDING_FILE).exists());
    assert!(a.list_session_ids().unwrap().is_empty());
}

#[test]
fn file_claim_survives_database_failure_and_retries_only_with_the_same_pair() {
    let dir = tempfile::tempdir().unwrap();
    let database = dir.path().join("sessions.sqlite3");
    let sessions = SqliteSessionStore::open(&database).unwrap();
    let files = FileEntityStore::open(dir.path().join("payloads")).unwrap();
    let connection = rusqlite::Connection::open(&database).unwrap();
    connection
        .execute_batch("CREATE INDEX prevent_binding_blob ON pipestream_payload_binding(image)")
        .unwrap();
    assert!(files.bind_session_store(&sessions).is_err());
    let original = fs::read(files.root().join(BINDING_FILE)).unwrap();
    assert!(sessions.payload_binding().unwrap().payloads().is_none());
    let other = SqliteSessionStore::open(dir.path().join("other.sqlite3")).unwrap();
    assert!(files.bind_session_store(&other).is_err());
    assert!(other.payload_binding().unwrap().payloads().is_none());
    drop(files);
    connection
        .execute_batch("DROP INDEX prevent_binding_blob")
        .unwrap();
    let reopened = FileEntityStore::open(dir.path().join("payloads")).unwrap();
    reopened.bind_session_store(&sessions).unwrap();
    assert_eq!(
        fs::read(reopened.root().join(BINDING_FILE)).unwrap(),
        original
    );
    assert_eq!(
        PayloadBinding::decode(&original).unwrap(),
        sessions.payload_binding().unwrap()
    );
}

#[test]
fn lost_claim_or_identity_cannot_be_recreated_for_a_bound_store() {
    for file in [BINDING_FILE, IDENTITY_FILE] {
        let dir = tempfile::tempdir().unwrap();
        let sessions = SqliteSessionStore::open(dir.path().join("sessions.sqlite3")).unwrap();
        let root = dir.path().join("payloads");
        let files = FileEntityStore::open(&root).unwrap();
        files.bind_session_store(&sessions).unwrap();
        let original = sessions.payload_binding().unwrap();
        fs::remove_file(root.join(file)).unwrap();
        assert!(files.bind_session_store(&sessions).is_err());
        assert!(
            files
                .put(
                    "no-input",
                    EntityKey {
                        scope_id: 0,
                        entity_id: 1
                    },
                    b"x"
                )
                .is_err()
        );
        drop(files);
        match FileEntityStore::open(&root) {
            Ok(reopened) => assert!(reopened.bind_session_store(&sessions).is_err()),
            Err(_) => assert_eq!(file, IDENTITY_FILE),
        }
        assert!(!root.join(file).exists());
        assert_eq!(sessions.payload_binding().unwrap(), original);
    }
}

#[test]
fn corrupt_partial_oversized_or_aliased_claims_refuse_without_repair() {
    for fault in ["partial", "corrupt", "oversized", "symlink", "foreign"] {
        let dir = tempfile::tempdir().unwrap();
        let sessions = SqliteSessionStore::open(dir.path().join("sessions.sqlite3")).unwrap();
        let root = dir.path().join("payloads");
        let files = FileEntityStore::open(&root).unwrap();
        files.bind_session_store(&sessions).unwrap();
        let path = root.join(BINDING_FILE);
        let mut bytes = fs::read(&path).unwrap();
        drop(files);
        match fault {
            "partial" => bytes.truncate(37),
            "corrupt" => bytes[15] ^= 1,
            "oversized" => bytes.push(0),
            "foreign" => {
                bytes = PayloadBinding::new(
                    sessions.payload_binding().unwrap().database(),
                    StoreIdentity::generate().unwrap(),
                )
                .encode()
                .to_vec()
            }
            "symlink" => {
                fs::rename(&path, root.join("outside-claim")).unwrap();
                #[cfg(unix)]
                std::os::unix::fs::symlink(root.join("outside-claim"), &path).unwrap();
            }
            _ => unreachable!(),
        }
        if fault != "symlink" {
            fs::write(&path, &bytes).unwrap();
        }
        assert!(FileEntityStore::open(&root).is_err(), "{fault}");
        assert_eq!(fs::read(path).unwrap(), bytes);
    }
}

#[test]
fn competing_roots_cannot_share_one_database() {
    let dir = tempfile::tempdir().unwrap();
    let sessions = Arc::new(SqliteSessionStore::open(dir.path().join("sessions.sqlite3")).unwrap());
    let files: Vec<_> = (0..2)
        .map(|i| Arc::new(FileEntityStore::open(dir.path().join(format!("root-{i}"))).unwrap()))
        .collect();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let attempts: Vec<_> = files
        .iter()
        .map(|files| {
            let files = files.clone();
            let sessions = sessions.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                files.bind_session_store(&sessions)
            })
        })
        .collect();
    let results: Vec<_> = attempts.into_iter().map(|t| t.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    let winner = results.iter().position(Result::is_ok).unwrap();
    assert_eq!(
        sessions.payload_binding().unwrap().payloads(),
        Some(files[winner].retained.identity)
    );
    assert!(sessions.load("absent").unwrap().is_none());
}

#[test]
fn previous_retained_policy_is_refused_without_conversion() {
    let dir = tempfile::tempdir().unwrap();
    drop(FileEntityStore::open(dir.path()).unwrap());
    let path = dir.path().join(".retained-policy");
    let mut bytes = fs::read(&path).unwrap();
    bytes[..8].copy_from_slice(b"PSRET002");
    let checksum = Sha256::digest(&bytes[..64]);
    bytes[64..].copy_from_slice(&checksum);
    fs::write(&path, &bytes).unwrap();
    assert!(FileEntityStore::open(dir.path()).is_err());
    assert_eq!(fs::read(path).unwrap(), bytes);
}

#[test]
fn binding_waits_for_the_database_writer_without_discarding_its_file_claim() {
    let dir = tempfile::tempdir().unwrap();
    let database = dir.path().join("sessions.sqlite3");
    let sessions = Arc::new(SqliteSessionStore::open(&database).unwrap());
    let files = Arc::new(FileEntityStore::open(dir.path().join("payloads")).unwrap());
    let writer = rusqlite::Connection::open(&database).unwrap();
    writer.execute_batch("BEGIN IMMEDIATE").unwrap();
    let (sender, receiver) = std::sync::mpsc::channel();
    let operation = {
        let sessions = sessions.clone();
        let files = files.clone();
        std::thread::spawn(move || sender.send(files.bind_session_store(&sessions)).unwrap())
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    while !files.root().join(BINDING_FILE).exists() && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    let published = files.root().join(BINDING_FILE).exists();
    let held = matches!(
        receiver.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    );
    writer.execute_batch("ROLLBACK").unwrap();
    assert!(published);
    assert!(held);
    receiver
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap()
        .unwrap();
    operation.join().unwrap();
    assert_eq!(
        read_claim(files.root(), files.retained.identity).unwrap(),
        Some(sessions.payload_binding().unwrap())
    );
}

#[test]
fn abrupt_exit_after_file_claim_replays_without_invented_admission() {
    const CHILD: &str = "PIPESTREAM_RUST_BINDING_CRASH_CHILD";
    if let Some(path) = std::env::var_os(CHILD) {
        let dir = PathBuf::from(path);
        let database = dir.join("sessions.sqlite3");
        let sessions = SqliteSessionStore::open(&database).unwrap();
        let files = FileEntityStore::open(dir.join("payloads")).unwrap();
        let connection = rusqlite::Connection::open(database).unwrap();
        connection
            .execute_batch("CREATE INDEX prevent_binding_blob ON pipestream_payload_binding(image)")
            .unwrap();
        assert!(files.bind_session_store(&sessions).is_err());
        assert!(sessions.payload_binding().unwrap().payloads().is_none());
        assert!(files.root().join(BINDING_FILE).is_file());
        std::process::exit(73);
    }
    let dir = tempfile::tempdir().unwrap();
    let log = fs::File::create(dir.path().join("child.log")).unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "recursive::retained::binding::tests::abrupt_exit_after_file_claim_replays_without_invented_admission", "--nocapture"])
        .env(CHILD, dir.path()).stdout(log.try_clone().unwrap()).stderr(log).spawn().unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if std::time::Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("binding crash child timed out");
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    };
    assert_eq!(
        status.code(),
        Some(73),
        "{}",
        fs::read_to_string(dir.path().join("child.log")).unwrap()
    );
    let database = dir.path().join("sessions.sqlite3");
    let sessions = SqliteSessionStore::open(&database).unwrap();
    assert!(sessions.payload_binding().unwrap().payloads().is_none());
    assert!(sessions.list_session_ids().unwrap().is_empty());
    let claim = fs::read(dir.path().join("payloads").join(BINDING_FILE)).unwrap();
    rusqlite::Connection::open(database)
        .unwrap()
        .execute_batch("DROP INDEX prevent_binding_blob")
        .unwrap();
    let files = FileEntityStore::open(dir.path().join("payloads")).unwrap();
    files.bind_session_store(&sessions).unwrap();
    assert_eq!(
        sessions.payload_binding().unwrap(),
        PayloadBinding::decode(&claim).unwrap()
    );
    assert_eq!(fs::read(files.root().join(BINDING_FILE)).unwrap(), claim);
    assert_eq!(sessions.unfinished_job_count().unwrap(), 0);
}
