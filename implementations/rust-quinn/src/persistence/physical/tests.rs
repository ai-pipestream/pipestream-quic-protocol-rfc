use super::*;
use crate::{
    jobs::tests::fixture,
    persistence::{JobQueueLimits, SessionStore, SqliteSessionStore, StorageLimits},
};
use rusqlite::{Connection, ffi};

fn limits() -> PhysicalLimits {
    PhysicalLimits {
        database_bytes: 256 << 10,
        wal_bytes: 128 << 10,
        journal_bytes: 256 << 10,
        shared_memory_bytes: 64 << 10,
    }
}

fn store(path: &Path) -> SqliteSessionStore {
    SqliteSessionStore::open_with_all_limits(
        path,
        JobQueueLimits::default(),
        StorageLimits::default(),
        limits(),
    )
    .unwrap()
}

fn assert_limit(error: StoreError) {
    assert!(
        matches!(error, StoreError::Protocol(ref e) if e.code == crate::ERROR_LIMIT_EXCEEDED),
        "{error:?}"
    );
}

fn pinned_reader(store: &SqliteSessionStore) -> Connection {
    let reader = store.connect().unwrap();
    reader
        .execute_batch("BEGIN; SELECT count(*) FROM pipestream_sessions;")
        .unwrap();
    reader
}

#[test]
fn held_reader_bounds_wal_and_rolls_back_session_and_job_admission() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite3");
    let store = store(&path);
    let (session, key, input) = fixture("pending", None);
    let mut saved = store.create(&session).unwrap();
    let before_usage = store.storage_usage().unwrap();
    let reader = pinned_reader(&store);
    let other = SqliteSessionStore::open(&path).unwrap();
    let mut exhausted = false;
    for _ in 0..100 {
        match other.save(saved.revision, &saved.session) {
            Ok(next) => saved = next,
            Err(error) => {
                assert_limit(error);
                exhausted = true;
                break;
            }
        }
        assert!(store.physical_usage().unwrap().wal_bytes <= limits().wal_bytes);
    }
    assert!(
        exhausted,
        "a pinned reader must not allow WAL growth past the cap"
    );
    assert_eq!(store.load("pending").unwrap().unwrap(), saved);
    assert_limit(
        store
            .transact("pending", |s| s.enqueue_job(key, input, 100))
            .unwrap_err(),
    );
    assert_eq!(store.load("pending").unwrap().unwrap(), saved);
    assert_eq!(store.storage_usage().unwrap(), before_usage);
    assert_eq!(store.unfinished_job_count().unwrap(), 0);
    store.integrity_check().unwrap();
    assert!(
        matches!(store.checkpoint(), Err(StoreError::Database(error))
        if error.sqlite_error_code() == Some(rusqlite::ErrorCode::DatabaseBusy))
    );
    drop(reader);
    store.checkpoint().unwrap();
    assert_eq!(store.physical_usage().unwrap().wal_bytes, 0);
    let next = store.save(saved.revision, &saved.session).unwrap();
    drop(other);
    drop(store);
    let reopened = SqliteSessionStore::open(&path).unwrap();
    assert_eq!(reopened.load("pending").unwrap().unwrap(), next);
    assert_eq!(reopened.physical_limits(), limits());
    reopened.integrity_check().unwrap();
}

#[test]
fn main_page_limit_refuses_before_creating_an_uncheckpointable_database() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir.path().join("state.sqlite3"));
    let connection = store.connect().unwrap();
    connection
        .execute_batch("CREATE TABLE probe(value BLOB); PRAGMA wal_autocheckpoint=1;")
        .unwrap();
    let mut committed = 0;
    loop {
        match connection.execute("INSERT INTO probe VALUES(zeroblob(8192))", []) {
            Ok(_) => committed += 1,
            Err(error) => {
                assert_limit(error.into());
                break;
            }
        }
        assert!(committed < 100);
        assert!(store.physical_usage().unwrap().database_bytes <= limits().database_bytes);
    }
    let retained: i32 = connection
        .query_row("SELECT count(*) FROM probe", [], |r| r.get(0))
        .unwrap();
    assert_eq!(retained, committed);
    store.checkpoint().unwrap();
    store.integrity_check().unwrap();
}

#[test]
fn rollback_journal_growth_is_bounded_and_failure_restores_prior_rows() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteSessionStore::open_with_all_limits(
        dir.path().join("state.sqlite3"),
        JobQueueLimits::default(),
        StorageLimits::default(),
        PhysicalLimits {
            journal_bytes: 65536,
            ..limits()
        },
    )
    .unwrap();
    let connection = store.connect().unwrap();
    connection
        .execute_batch("PRAGMA journal_mode=DELETE; CREATE TABLE probe(value BLOB);")
        .unwrap();
    for _ in 0..16 {
        connection
            .execute("INSERT INTO probe VALUES(zeroblob(8192))", [])
            .unwrap();
    }
    assert_limit(
        connection
            .execute("UPDATE probe SET value=randomblob(8192)", [])
            .unwrap_err()
            .into(),
    );
    let unchanged: i32 = connection
        .query_row(
            "SELECT count(*) FROM probe WHERE value=zeroblob(8192)",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(unchanged, 16);
    assert!(store.physical_usage().unwrap().journal_bytes <= 65536);
    drop(connection);
    store.integrity_check().unwrap();
}

#[test]
fn wal_index_growth_refuses_at_the_shared_memory_cap() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("small-pages.sqlite3");
    let physical = PhysicalLimits {
        database_bytes: 1 << 20,
        wal_bytes: 8 << 20,
        journal_bytes: 1 << 20,
        shared_memory_bytes: 65536,
    };
    let guard = Guard::open(&path, Some(physical)).unwrap();
    let prepare = Connection::open_with_flags_and_vfs(
        &path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        VFS_NAME,
    )
    .unwrap();
    prepare
        .execute_batch("PRAGMA page_size=512; CREATE TABLE probe(value BLOB);")
        .unwrap();
    drop(prepare);
    let store = SqliteSessionStore::open(&path).unwrap();
    let connection = store.connect().unwrap();
    connection
        .execute_batch("PRAGMA wal_autocheckpoint=0; INSERT INTO probe VALUES(zeroblob(131072));")
        .unwrap();
    let reader = pinned_reader(&store);
    let mut full = false;
    for _ in 0..80 {
        match connection.execute("UPDATE probe SET value=randomblob(131072)", []) {
            Ok(_) => {}
            Err(error) => {
                assert_limit(error.into());
                full = true;
                break;
            }
        }
        assert!(
            store.physical_usage().unwrap().shared_memory_bytes <= physical.shared_memory_bytes
        );
    }
    assert!(full);
    let usage = store.physical_usage().unwrap();
    assert_eq!(usage.shared_memory_bytes, physical.shared_memory_bytes);
    assert!(
        usage.wal_bytes < physical.wal_bytes,
        "the WAL file cap must not be the refusing limit"
    );
    drop(reader);
    drop(connection);
    store.checkpoint().unwrap();
    store.integrity_check().unwrap();
    drop(guard);
}

#[test]
fn growth_controls_cannot_bypass_the_write_cap() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir.path().join("state.sqlite3"));
    let connection = store.connect().unwrap();
    let before = store.physical_usage().unwrap();
    // SAFETY: SQLite returns the live file pointer for this open connection;
    // argument types follow each documented file-control opcode. Every direct
    // mutating probe is outside the allowed range and must not reach Unix I/O.
    unsafe {
        let db = connection.handle();
        let mut file: *mut ffi::sqlite3_file = std::ptr::null_mut();
        assert_eq!(
            ffi::sqlite3_file_control(
                db,
                c"main".as_ptr(),
                ffi::SQLITE_FCNTL_FILE_POINTER,
                std::ptr::addr_of_mut!(file).cast()
            ),
            ffi::SQLITE_OK
        );
        assert!(!file.is_null());
        let methods = &*(*file).pMethods;
        let max = limits().database_bytes as i64;
        for size in [-1, max + 1, i64::MAX] {
            assert_eq!(methods.xTruncate.unwrap()(file, size), ffi::SQLITE_FULL);
        }
        let byte = [1u8];
        for (offset, count) in [(max, 1), (i64::MAX, 1), (-1, 1), (0, -1)] {
            assert_eq!(
                methods.xWrite.unwrap()(file, byte.as_ptr().cast(), count, offset),
                ffi::SQLITE_FULL
            );
        }
        let mut hint = max + 1;
        assert_eq!(
            ffi::sqlite3_file_control(
                db,
                c"main".as_ptr(),
                ffi::SQLITE_FCNTL_SIZE_HINT,
                std::ptr::addr_of_mut!(hint).cast()
            ),
            ffi::SQLITE_FULL
        );
        let mut chunk = i32::MAX;
        assert_eq!(
            ffi::sqlite3_file_control(
                db,
                c"main".as_ptr(),
                ffi::SQLITE_FCNTL_CHUNK_SIZE,
                std::ptr::addr_of_mut!(chunk).cast()
            ),
            ffi::SQLITE_OK
        );
        hint = max - 1;
        assert_eq!(
            ffi::sqlite3_file_control(
                db,
                c"main".as_ptr(),
                ffi::SQLITE_FCNTL_SIZE_HINT,
                std::ptr::addr_of_mut!(hint).cast()
            ),
            ffi::SQLITE_OK
        );
        let mut mmap = i64::MAX;
        assert_eq!(
            ffi::sqlite3_file_control(
                db,
                c"main".as_ptr(),
                ffi::SQLITE_FCNTL_MMAP_SIZE,
                std::ptr::addr_of_mut!(mmap).cast()
            ),
            ffi::SQLITE_OK
        );
        assert_eq!(mmap, 0);
        let mut mapped = std::ptr::null_mut();
        assert_eq!(
            methods.xShmMap.unwrap()(file, 2, 32768, 1, &mut mapped),
            ffi::SQLITE_FULL
        );
        assert!(mapped.is_null());
        assert_eq!(methods.iVersion, 2);
        assert!(methods.xFetch.is_none());
    }
    assert_eq!(store.physical_usage().unwrap(), before);
    store.integrity_check().unwrap();
}

#[test]
fn policy_is_immutable_and_checked_even_with_an_existing_handle() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite3");
    let store = store(&path);
    let changed = PhysicalLimits {
        wal_bytes: 2 * limits().wal_bytes,
        ..limits()
    };
    assert!(
        SqliteSessionStore::open_with_all_limits(
            &path,
            JobQueueLimits::default(),
            StorageLimits::default(),
            changed
        )
        .is_err()
    );
    let policy = path.with_file_name("state.sqlite3.pslimits");
    let mut bytes = fs::read(&policy).unwrap();
    bytes[20] ^= 1;
    fs::write(&policy, bytes).unwrap();
    assert!(store.load("anything").is_err());
    assert!(SqliteSessionStore::open(&path).is_err());
}

#[test]
fn unguarded_database_and_oversized_or_truncated_policy_are_refused_without_conversion() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("old.sqlite3");
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch("CREATE TABLE old(value);")
        .unwrap();
    drop(connection);
    let before = fs::read(&path).unwrap();
    assert!(SqliteSessionStore::open(&path).is_err());
    assert_eq!(fs::read(&path).unwrap(), before);
    assert!(!path.with_file_name("old.sqlite3.pslimits").exists());
    for count in [0, 71, 73, 4096] {
        let path = dir.path().join(format!("bad-{count}.sqlite3"));
        let policy = dir.path().join(format!("bad-{count}.sqlite3.pslimits"));
        fs::write(policy, vec![0; count]).unwrap();
        assert!(SqliteSessionStore::open(&path).is_err());
        assert!(!path.exists());
    }
}

#[cfg(unix)]
#[test]
fn symlinks_hardlinks_and_reserved_names_cannot_open_an_unbounded_alias() {
    use std::os::unix::fs::symlink;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite3");
    let store = store(&path);
    let alias = dir.path().join("alias.sqlite3");
    symlink(&path, &alias).unwrap();
    assert!(SqliteSessionStore::open(&alias).is_err());
    let link = dir.path().join("hard.sqlite3");
    fs::hard_link(&path, &link).unwrap();
    assert!(store.load("anything").is_err());
    assert!(SqliteSessionStore::open(&link).is_err());
    fs::remove_file(&link).unwrap();
    for suffix in ["-wal", "-shm", "-journal", ".pslimits"] {
        assert!(SqliteSessionStore::open(dir.path().join(format!("other{suffix}"))).is_err());
    }
    store.integrity_check().unwrap();
}

#[test]
fn external_oversize_is_refused_instead_of_silently_increasing_the_budget() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite3");
    let store = store(&path);
    let wal = dir.path().join("state.sqlite3-wal");
    File::create(&wal)
        .unwrap()
        .set_len(limits().wal_bytes + 1)
        .unwrap();
    assert!(store.load("anything").is_err());
    assert!(SqliteSessionStore::open(&path).is_err());
    assert_eq!(fs::metadata(wal).unwrap().len(), limits().wal_bytes + 1);
}

#[test]
fn concurrent_connection_close_does_not_misclassify_unlinked_sidecars_as_aliases() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(store(&dir.path().join("churn.sqlite3")));
    let (session, _, _) = fixture("retained", None);
    let expected = store.create(&session).unwrap();
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let store = &store;
            let expected = &expected;
            scope.spawn(move || {
                for _ in 0..100 {
                    assert_eq!(store.load("retained").unwrap().as_ref(), Some(expected));
                    store.physical_usage().unwrap();
                }
            });
        }
    });
    store.integrity_check().unwrap();
}

#[test]
fn quota_failure_survives_abrupt_process_exit_and_wal_recovery() {
    const CHILD: &str = "PIPESTREAM_PHYSICAL_CRASH_TEST";
    if let Some(path) = std::env::var_os(CHILD) {
        let store = store(Path::new(&path));
        let (session, _, _) = fixture("crash", None);
        let mut saved = store.create(&session).unwrap();
        let _reader = pinned_reader(&store);
        for _ in 0..100 {
            match store.save(saved.revision, &saved.session) {
                Ok(next) => saved = next,
                Err(error) => {
                    assert_limit(error);
                    println!("RETAINED_REVISION={}", saved.revision);
                    std::process::exit(42);
                }
            }
        }
        panic!("WAL quota did not refuse growth");
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("crash.sqlite3");
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "persistence::physical::tests::quota_failure_survives_abrupt_process_exit_and_wal_recovery", "--nocapture"])
        .env(CHILD, &path).output().unwrap();
    assert_eq!(
        child.status.code(),
        Some(42),
        "{}",
        String::from_utf8_lossy(&child.stderr)
    );
    let stdout = String::from_utf8(child.stdout).unwrap();
    let revision: u64 = stdout
        .lines()
        .find_map(|s| s.strip_prefix("RETAINED_REVISION="))
        .unwrap()
        .parse()
        .unwrap();
    assert!(revision > 1);
    assert!(
        fs::metadata(dir.path().join("crash.sqlite3-wal"))
            .unwrap()
            .len()
            <= limits().wal_bytes
    );
    let reopened = SqliteSessionStore::open(&path).unwrap();
    let loaded = reopened.load("crash").unwrap().unwrap();
    assert_eq!(loaded.revision, revision);
    reopened.integrity_check().unwrap();
    reopened.checkpoint().unwrap();
    assert!(reopened.save(revision, &loaded.session).is_ok());
}
