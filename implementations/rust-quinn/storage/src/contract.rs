//! Backend-agnostic contract test suite.
//!
//! Every [`ObjectStore`] + [`Journal`] implementation must pass every
//! function in this module. A backend's test module constructs a fresh
//! instance and calls each entry point; [`run_all`] runs the whole suite in
//! one shot. New backends (Postgres, S3, Kafka) certify against the same
//! suite, which is what makes backend choice invisible to the protocol.
//!
//! The suite exercises every SPI semantic from
//! `docs/standards/storage-spi-design.md`: idempotent put, conflict and
//! digest-mismatch as Corrupt, AlreadySealed after sealing, verify_scope
//! match and mismatch, open_read surviving removal of an unsealed object,
//! journal replay ordering, commit-after-intent, and the
//! durability-of-return contract.

use crate::{
    Cursor, Digest32, Journal, JournalEntry, JournalKind, ObjectStore, ScopePath, SealRecord,
    StorageError, sha256,
};

/// A backend under test: both SPI roles on one instance.
pub trait Backend: ObjectStore + Journal {}

impl<T> Backend for T where T: ObjectStore + Journal {}

/// Runs every contract check against a fresh backend instance.
///
/// `fresh` must return a backend with no prior state. `reopen` must return
/// a handle to the *same* stored state through a new instance (for the
/// filesystem backend: drop and re-open the same root; for the in-memory
/// backend: clone the shared-state handle). Durability-focused backends
/// should additionally run [`durability_roundtrip`] with a `reopen` that
/// crosses a real process boundary.
pub fn run_all<B: Backend>(mut fresh: impl FnMut() -> B, mut reopen: impl FnMut() -> B) {
    put_is_idempotent(&mut fresh());
    put_conflicting_bytes_is_corrupt(&mut fresh());
    put_digest_mismatch_is_corrupt(&mut fresh());
    put_rejects_out_of_range_entity_id(&mut fresh());
    put_into_sealed_scope_is_already_sealed(&mut fresh());
    seal_is_atomic_and_idempotent(&mut fresh());
    seal_conflict_is_corrupt(&mut fresh());
    verify_scope_matches_seal_record(&mut fresh());
    verify_scope_detects_count_mismatch(&mut fresh());
    verify_unsealed_scope_is_not_found(&mut fresh());
    open_read_streams_payload(&mut fresh());
    open_read_missing_is_not_found(&mut fresh());
    open_read_survives_retire_of_unsealed_object(&mut fresh());
    retire_removes_only_past_cursor(&mut fresh());
    sealed_scope_retires_whole(&mut fresh());
    list_returns_sorted_entity_ids(&mut fresh());
    scopes_are_isolated(&mut fresh());
    journal_replay_is_ordered(&mut fresh());
    journal_replay_resumes_from_cursor(&mut fresh());
    commit_after_intent(&mut fresh());
    commit_unknown_intent_is_not_found(&mut fresh());
    commit_conflict_is_corrupt(&mut fresh());
    durability_roundtrip(&mut reopen);
}

fn nested_scope() -> ScopePath {
    match ScopePath::root().child(7).and_then(|p| p.child(9)) {
        Ok(path) => path,
        Err(_) => panic!("test scope construction failed"),
    }
}

fn payload(tag: u8) -> Vec<u8> {
    vec![tag; 64]
}

fn seal_digest(tag: &str) -> Digest32 {
    sha256(tag.as_bytes())
}

fn read_all<B: ObjectStore>(store: &B, scope: &ScopePath, entity: u32) -> Vec<u8> {
    use std::io::Read;
    let mut reader = match store.open_read(scope, entity) {
        Ok(reader) => reader,
        Err(error) => panic!("open_read({entity}) failed: {error}"),
    };
    let mut bytes = Vec::new();
    if let Err(error) = reader.read_to_end(&mut bytes) {
        panic!("read of entity {entity} failed: {error}");
    }
    bytes
}

fn expect_corrupt(result: Result<(), StorageError>, what: &str) {
    match result {
        Err(StorageError::Corrupt(_)) => {}
        other => panic!("{what}: expected Corrupt, got {other:?}"),
    }
}

/// `put` of identical bytes with the same digest succeeds as a no-op, and
/// the object remains readable.
pub fn put_is_idempotent<B: Backend>(store: &mut B) {
    let scope = ScopePath::root();
    let bytes = payload(1);
    let digest = sha256(&bytes);
    assert!(store.put(&scope, 1, &bytes, &digest).is_ok());
    assert!(store.put(&scope, 1, &bytes, &digest).is_ok());
    assert_eq!(read_all(store, &scope, 1), bytes);
    assert_eq!(
        store.list(&scope).unwrap_or_else(|e| panic!("list: {e}")),
        vec![1]
    );
}

/// Re-put of different bytes under the same entity id fails with Corrupt.
pub fn put_conflicting_bytes_is_corrupt<B: Backend>(store: &mut B) {
    let scope = ScopePath::root();
    let bytes = payload(2);
    assert!(store.put(&scope, 1, &bytes, &sha256(&bytes)).is_ok());
    let other = payload(3);
    expect_corrupt(
        store.put(&scope, 1, &other, &sha256(&other)),
        "conflicting re-put",
    );
}

/// A payload that does not match its expected digest fails with Corrupt and
/// stores nothing.
pub fn put_digest_mismatch_is_corrupt<B: Backend>(store: &mut B) {
    let scope = ScopePath::root();
    let bytes = payload(4);
    let wrong = sha256(b"not the payload");
    expect_corrupt(store.put(&scope, 1, &bytes, &wrong), "digest mismatch");
    assert!(matches!(
        store.open_read(&scope, 1),
        Err(StorageError::NotFound(_))
    ));
}

/// Entity ids above the protocol bound are rejected.
pub fn put_rejects_out_of_range_entity_id<B: Backend>(store: &mut B) {
    let scope = ScopePath::root();
    let bytes = payload(5);
    let result = store.put(&scope, crate::layout::MAX_ENTITY_ID + 1, &bytes, &sha256(&bytes));
    assert!(result.is_err(), "out-of-range entity id must be rejected");
}

/// After sealing, `put` into the scope fails with AlreadySealed.
pub fn put_into_sealed_scope_is_already_sealed<B: Backend>(store: &mut B) {
    let scope = nested_scope();
    let bytes = payload(6);
    assert!(store.put(&scope, 1, &bytes, &sha256(&bytes)).is_ok());
    assert!(store.seal_scope(&scope, &seal_digest("s"), 1).is_ok());
    let more = payload(7);
    match store.put(&scope, 2, &more, &sha256(&more)) {
        Err(StorageError::AlreadySealed(_)) => {}
        other => panic!("put into sealed scope: expected AlreadySealed, got {other:?}"),
    }
}

/// Sealing succeeds atomically and re-sealing with identical arguments is a
/// no-op.
pub fn seal_is_atomic_and_idempotent<B: Backend>(store: &mut B) {
    let scope = nested_scope();
    let bytes = payload(8);
    assert!(store.put(&scope, 1, &bytes, &sha256(&bytes)).is_ok());
    let digest = seal_digest("seal");
    assert!(store.seal_scope(&scope, &digest, 1).is_ok());
    assert!(store.seal_scope(&scope, &digest, 1).is_ok());
    let record = store
        .verify_scope(&scope)
        .unwrap_or_else(|e| panic!("verify_scope: {e}"));
    assert_eq!(
        record,
        SealRecord {
            digest,
            entity_count: 1
        }
    );
}

/// Re-sealing with a different digest or count fails with Corrupt.
pub fn seal_conflict_is_corrupt<B: Backend>(store: &mut B) {
    let scope = nested_scope();
    let bytes = payload(9);
    assert!(store.put(&scope, 1, &bytes, &sha256(&bytes)).is_ok());
    assert!(store.seal_scope(&scope, &seal_digest("a"), 1).is_ok());
    expect_corrupt(
        store.seal_scope(&scope, &seal_digest("b"), 1),
        "re-seal with different digest",
    );
    expect_corrupt(store.seal_scope(&scope, &seal_digest("a"), 2), "re-seal with different count");
}

/// `verify_scope` on a sealed, untampered scope returns the recorded digest
/// and entity count.
pub fn verify_scope_matches_seal_record<B: Backend>(store: &mut B) {
    let scope = nested_scope();
    for entity in 1..=4u32 {
        let bytes = payload(entity as u8);
        assert!(store.put(&scope, entity, &bytes, &sha256(&bytes)).is_ok());
    }
    let digest = seal_digest("verify");
    assert!(store.seal_scope(&scope, &digest, 4).is_ok());
    let record = store
        .verify_scope(&scope)
        .unwrap_or_else(|e| panic!("verify_scope: {e}"));
    assert_eq!(record.digest, digest);
    assert_eq!(record.entity_count, 4);
}

/// `verify_scope` fails with Corrupt when the live entity count disagrees
/// with the seal record.
pub fn verify_scope_detects_count_mismatch<B: Backend>(store: &mut B) {
    let scope = nested_scope();
    let bytes = payload(10);
    assert!(store.put(&scope, 1, &bytes, &sha256(&bytes)).is_ok());
    // Seal with a count that does not match the live set, then verify.
    assert!(store.seal_scope(&scope, &seal_digest("count"), 5).is_ok());
    match store.verify_scope(&scope) {
        Err(StorageError::Corrupt(_)) => {}
        other => panic!("verify_scope with wrong count: expected Corrupt, got {other:?}"),
    }
}

/// `verify_scope` on an unsealed or unknown scope fails with NotFound.
pub fn verify_unsealed_scope_is_not_found<B: Backend>(store: &mut B) {
    let scope = nested_scope();
    match store.verify_scope(&scope) {
        Err(StorageError::NotFound(_)) => {}
        other => panic!("verify_scope unknown scope: expected NotFound, got {other:?}"),
    }
    let bytes = payload(11);
    assert!(store.put(&scope, 1, &bytes, &sha256(&bytes)).is_ok());
    match store.verify_scope(&scope) {
        Err(StorageError::NotFound(_)) => {}
        other => panic!("verify_scope unsealed scope: expected NotFound, got {other:?}"),
    }
}

/// `open_read` streams exactly the stored payload.
pub fn open_read_streams_payload<B: Backend>(store: &mut B) {
    let scope = nested_scope();
    let bytes: Vec<u8> = (0..=255u8).cycle().take(200_000).collect();
    assert!(store.put(&scope, 42, &bytes, &sha256(&bytes)).is_ok());
    assert_eq!(read_all(store, &scope, 42), bytes);
}

/// `open_read` of a missing object fails with NotFound.
pub fn open_read_missing_is_not_found<B: Backend>(store: &mut B) {
    let scope = nested_scope();
    match store.open_read(&scope, 99) {
        Err(StorageError::NotFound(_)) => {}
        Err(error) => panic!("open_read of missing object: expected NotFound, got {error:?}"),
        Ok(_) => panic!("open_read of missing object must fail"),
    }
}

/// A live read holds the object: retiring the unsealed object under an open
/// read does not invalidate the read; the object is gone afterwards.
pub fn open_read_survives_retire_of_unsealed_object<B: Backend>(store: &mut B) {
    use std::io::Read;
    let scope = nested_scope();
    let bytes = payload(12);
    assert!(store.put(&scope, 1, &bytes, &sha256(&bytes)).is_ok());
    let mut reader = match store.open_read(&scope, 1) {
        Ok(reader) => reader,
        Err(error) => panic!("open_read: {error}"),
    };
    assert_eq!(
        store.retire(&scope, 1).unwrap_or_else(|e| panic!("retire: {e}")),
        1
    );
    let mut survivors = Vec::new();
    if let Err(error) = reader.read_to_end(&mut survivors) {
        panic!("open read invalidated by retire: {error}");
    }
    assert_eq!(survivors, bytes);
    assert!(matches!(
        store.open_read(&scope, 1),
        Err(StorageError::NotFound(_))
    ));
}

/// Retire removes entities at or below the cursor and keeps newer ones.
pub fn retire_removes_only_past_cursor<B: Backend>(store: &mut B) {
    let scope = nested_scope();
    for entity in 1..=5u32 {
        let bytes = payload(entity as u8);
        assert!(store.put(&scope, entity, &bytes, &sha256(&bytes)).is_ok());
    }
    assert_eq!(
        store.retire(&scope, 3).unwrap_or_else(|e| panic!("retire: {e}")),
        3
    );
    assert_eq!(
        store.list(&scope).unwrap_or_else(|e| panic!("list: {e}")),
        vec![4, 5]
    );
}

/// A sealed scope retires whole: every entity and the seal record go away.
pub fn sealed_scope_retires_whole<B: Backend>(store: &mut B) {
    let scope = nested_scope();
    for entity in 1..=3u32 {
        let bytes = payload(entity as u8);
        assert!(store.put(&scope, entity, &bytes, &sha256(&bytes)).is_ok());
    }
    assert!(store.seal_scope(&scope, &seal_digest("retire"), 3).is_ok());
    // The cursor is below every entity: a sealed scope retires whole anyway.
    assert_eq!(
        store.retire(&scope, 0).unwrap_or_else(|e| panic!("retire: {e}")),
        3
    );
    assert!(
        store
            .list(&scope)
            .unwrap_or_else(|e| panic!("list: {e}"))
            .is_empty()
    );
    assert!(matches!(
        store.verify_scope(&scope),
        Err(StorageError::NotFound(_))
    ));
    // The scope is writable again after retiring whole.
    let bytes = payload(13);
    assert!(store.put(&scope, 1, &bytes, &sha256(&bytes)).is_ok());
}

/// `list` returns entity ids sorted ascending; unknown scopes list empty.
pub fn list_returns_sorted_entity_ids<B: Backend>(store: &mut B) {
    let scope = nested_scope();
    assert!(
        store
            .list(&scope)
            .unwrap_or_else(|e| panic!("list: {e}"))
            .is_empty()
    );
    for entity in [7u32, 1, 300, 42] {
        let bytes = payload((entity % 256) as u8);
        assert!(store.put(&scope, entity, &bytes, &sha256(&bytes)).is_ok());
    }
    assert_eq!(
        store.list(&scope).unwrap_or_else(|e| panic!("list: {e}")),
        vec![1, 7, 42, 300]
    );
}

/// Entities in different scopes — including nested scopes — never mix.
pub fn scopes_are_isolated<B: Backend>(store: &mut B) {
    let parent = ScopePath::root().child(1).unwrap_or_else(|_| panic!("scope"));
    let child = parent.child(2).unwrap_or_else(|_| panic!("scope"));
    let bytes = payload(14);
    assert!(store.put(&parent, 1, &bytes, &sha256(&bytes)).is_ok());
    assert!(store.put(&child, 1, &bytes, &sha256(&bytes)).is_ok());
    assert_eq!(store.list(&parent).unwrap_or_else(|e| panic!("list: {e}")), vec![1]);
    assert_eq!(store.list(&child).unwrap_or_else(|e| panic!("list: {e}")), vec![1]);
    assert!(store.seal_scope(&parent, &seal_digest("parent"), 1).is_ok());
    // Sealing the parent leaves the child scope writable.
    assert!(store.put(&child, 2, &bytes, &sha256(&bytes)).is_ok());
}

/// Replay returns intents and commits in append order.
pub fn journal_replay_is_ordered<B: Backend>(store: &mut B) {
    let first = store
        .append_intent(b"intent-1")
        .unwrap_or_else(|e| panic!("append_intent: {e}"));
    let second = store
        .append_intent(b"intent-2")
        .unwrap_or_else(|e| panic!("append_intent: {e}"));
    assert!(second > first, "cursors must increase in append order");
    assert!(store.commit(first, b"commit-1").is_ok());
    let entries = store.replay(Cursor(0)).unwrap_or_else(|e| panic!("replay: {e}"));
    assert_eq!(
        entries,
        vec![
            JournalEntry {
                cursor: first,
                kind: JournalKind::Intent(b"intent-1".to_vec()),
            },
            JournalEntry {
                cursor: second,
                kind: JournalKind::Intent(b"intent-2".to_vec()),
            },
            JournalEntry {
                cursor: Cursor(second.0 + 1),
                kind: JournalKind::Commit {
                    intent: first,
                    record: b"commit-1".to_vec(),
                },
            },
        ]
    );
}

/// Replay from a cursor returns only later entries, in order.
pub fn journal_replay_resumes_from_cursor<B: Backend>(store: &mut B) {
    let first = store.append_intent(b"a").unwrap_or_else(|e| panic!("append: {e}"));
    store.append_intent(b"b").unwrap_or_else(|e| panic!("append: {e}"));
    let entries = store.replay(first).unwrap_or_else(|e| panic!("replay: {e}"));
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].kind, JournalKind::Intent(b"b".to_vec()));
    assert!(
        store
            .replay(entries[0].cursor)
            .unwrap_or_else(|e| panic!("replay: {e}"))
            .is_empty()
    );
}

/// Commit-after-intent: the recovery path sees the intent, then its commit,
/// and treats objects without a commit as uncommitted.
pub fn commit_after_intent<B: Backend>(store: &mut B) {
    let committed = store.append_intent(b"committed").unwrap_or_else(|e| panic!("append: {e}"));
    store.append_intent(b"uncommitted").unwrap_or_else(|e| panic!("append: {e}"));
    assert!(store.commit(committed, b"ok").is_ok());
    let entries = store.replay(Cursor(0)).unwrap_or_else(|e| panic!("replay: {e}"));
    let settled: Vec<Cursor> = entries
        .iter()
        .filter_map(|entry| match entry.kind {
            JournalKind::Commit { intent, .. } => Some(intent),
            JournalKind::Intent(_) => None,
        })
        .collect();
    assert_eq!(settled, vec![committed]);
    // Re-committing with the same record is a no-op.
    assert!(store.commit(committed, b"ok").is_ok());
}

/// Committing a cursor with no intent fails with NotFound.
pub fn commit_unknown_intent_is_not_found<B: Backend>(store: &mut B) {
    match store.commit(Cursor(999), b"nope") {
        Err(StorageError::NotFound(_)) => {}
        other => panic!("commit of unknown intent: expected NotFound, got {other:?}"),
    }
    match store.commit(Cursor(0), b"nope") {
        Err(StorageError::NotFound(_)) => {}
        other => panic!("commit of cursor 0: expected NotFound, got {other:?}"),
    }
}

/// A conflicting commit record for an already-committed intent is Corrupt.
pub fn commit_conflict_is_corrupt<B: Backend>(store: &mut B) {
    let intent = store.append_intent(b"work").unwrap_or_else(|e| panic!("append: {e}"));
    assert!(store.commit(intent, b"result-a").is_ok());
    expect_corrupt(store.commit(intent, b"result-b"), "conflicting commit");
}

/// Durability-of-return: state written through one handle is fully visible
/// through a reopened handle — objects, seal records, and journal entries.
pub fn durability_roundtrip<B: Backend>(reopen: &mut impl FnMut() -> B) {
    let scope = nested_scope();
    let digest;
    let intent;
    {
        let mut store = reopen();
        let bytes = payload(15);
        assert!(store.put(&scope, 1, &bytes, &sha256(&bytes)).is_ok());
        digest = seal_digest("durable");
        assert!(store.seal_scope(&scope, &digest, 1).is_ok());
        intent = store.append_intent(b"durable-intent").unwrap_or_else(|e| panic!("append: {e}"));
        assert!(store.commit(intent, b"durable-commit").is_ok());
    }
    let store = reopen();
    let bytes = payload(15);
    assert_eq!(read_all(&store, &scope, 1), bytes);
    let record = store
        .verify_scope(&scope)
        .unwrap_or_else(|e| panic!("verify_scope after reopen: {e}"));
    assert_eq!(record.digest, digest);
    let entries = store.replay(Cursor(0)).unwrap_or_else(|e| panic!("replay: {e}"));
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].kind, JournalKind::Intent(b"durable-intent".to_vec()));
    assert_eq!(
        entries[1].kind,
        JournalKind::Commit {
            intent,
            record: b"durable-commit".to_vec(),
        }
    );
}
