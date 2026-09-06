use super::*;
use crate::jobs::{JobInput, JobOutput, ProcessOutcome, tests::fixture};

fn paired(store: &SqliteSessionStore) -> PayloadBinding {
    let pair = PayloadBinding::new(
        store.payload_binding().unwrap().database(),
        StoreIdentity::generate().unwrap(),
    );
    store.bind_payload_store(pair).unwrap();
    pair
}

#[test]
fn maintenance_holds_writer_through_exhaustion_and_drop_releases_without_changes() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteSessionStore::open(dir.path().join("sessions.sqlite3")).unwrap();
    let pair = paired(&store);
    for id in ["z", "a", "m"] {
        store.create(&Session::new(id, 7, 100).unwrap()).unwrap();
    }
    let before = store.storage_usage().unwrap();
    let mut guard = store.payload_maintenance(pair).unwrap();
    let writer = Connection::open(store.path()).unwrap();
    writer.busy_timeout(std::time::Duration::ZERO).unwrap();
    for id in ["a", "m", "z"] {
        assert_eq!(
            guard.next_session().unwrap().unwrap().session.session_id,
            id
        );
        assert!(writer.execute_batch("BEGIN IMMEDIATE").is_err());
    }
    assert!(guard.next_session().unwrap().is_none());
    assert!(guard.next_session().unwrap().is_none());
    assert!(writer.execute_batch("BEGIN IMMEDIATE").is_err());
    drop(guard);
    writer.execute_batch("BEGIN IMMEDIATE; ROLLBACK").unwrap();
    assert_eq!(store.storage_usage().unwrap(), before);
    for id in ["a", "m", "z"] {
        assert_eq!(store.load(id).unwrap().unwrap().revision, 1);
    }
}

#[test]
fn maintenance_requires_existing_exact_pair_and_never_binds() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteSessionStore::open(dir.path().join("sessions.sqlite3")).unwrap();
    let original = store.payload_binding().unwrap();
    assert!(store.payload_maintenance(original).is_err());
    let expected = PayloadBinding::new(original.database(), StoreIdentity::generate().unwrap());
    assert!(store.payload_maintenance(expected).is_err());
    assert_eq!(store.payload_binding().unwrap(), original);
    store.bind_payload_store(expected).unwrap();
    let wrong = PayloadBinding::new(original.database(), StoreIdentity::generate().unwrap());
    assert!(store.payload_maintenance(wrong).is_err());
    assert_eq!(store.payload_binding().unwrap(), expected);
}

#[test]
fn maintenance_refuses_manual_admission_instead_of_interpreting_missing_job_as_orphan() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteSessionStore::open(dir.path().join("sessions.sqlite3")).unwrap();
    let pair = paired(&store);
    let (session, _, _) = fixture("manual", None);
    let original = store.create(&session).unwrap();
    assert!(
        matches!(store.payload_maintenance(pair).unwrap().next_session(),
        Err(StoreError::Protocol(error)) if error.code == crate::ERROR_ENTITY_INVALID)
    );
    assert_eq!(store.load("manual").unwrap().unwrap(), original);
}

#[test]
fn maintenance_validates_finished_input_not_only_dispatchable_jobs() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteSessionStore::open(dir.path().join("sessions.sqlite3")).unwrap();
    let pair = paired(&store);
    let (mut session, key, input) = fixture("finished", None);
    session.enqueue_job(key, input, 1).unwrap();
    let lease = session.acquire_job(None, key, 2, 100).unwrap().unwrap();
    session
        .publish_job(None, &lease, 3, |s| {
            s.complete_entity(key.entity, [2; 32])?;
            Ok(JobOutput::Processed(ProcessOutcome::Complete))
        })
        .unwrap();
    // A checksummed but semantically bad terminal descriptor is not ready work.
    // The maintenance audit must still reject it before files can be removed.
    let JobInput::Process { digest, .. } = &mut session.jobs.get_mut(&key).unwrap().input else {
        unreachable!()
    };
    *digest = [99; 32];
    store.create(&session).unwrap();
    assert_eq!(store.unfinished_job_count().unwrap(), 0);
    assert!(matches!(
        store.payload_maintenance(pair).unwrap().next_session(),
        Err(StoreError::Corrupt(_))
    ));
    assert_eq!(store.load("finished").unwrap().unwrap().revision, 1);
}

#[test]
fn maintenance_refuses_corrupt_accounting_before_exposing_a_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteSessionStore::open(dir.path().join("sessions.sqlite3")).unwrap();
    let pair = paired(&store);
    store
        .create(&Session::new("retained", 7, 100).unwrap())
        .unwrap();
    let connection = Connection::open(store.path()).unwrap();
    connection
        .execute_batch("UPDATE pipestream_storage_sessions SET image=zeroblob(56)")
        .unwrap();
    assert!(store.payload_maintenance(pair).is_err());
    let bytes: Vec<u8> = connection
        .query_row("SELECT image FROM pipestream_storage_sessions", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(bytes, vec![0; 56]);
    connection
        .execute_batch("BEGIN IMMEDIATE; ROLLBACK")
        .unwrap();
}
