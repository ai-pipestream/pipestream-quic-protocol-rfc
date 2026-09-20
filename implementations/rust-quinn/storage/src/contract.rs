//! Backend-agnostic contract test suite.
//!
//! Every backend must pass the suite for each role it implements, and the
//! checks mirror the Java lane's `ObjectStoreContractTest` /
//! `JournalContractTest` one-for-one (minus Java's concurrency rows, which
//! Rust ownership rules make unreachable):
//!
//! - [`run_object_store`] for the [`ObjectStore`] role,
//! - [`run_journal`] for the [`Journal`] role,
//! - [`run_all`] for two-role backends.
//!
//! `fresh` must return a backend with no prior state. `reopen` must return
//! a handle to the *same* stored state through a new instance (filesystem:
//! drop and re-open the same root; in-memory: clone the shared-state
//! handle; Postgres: reconnect to the same database).

use crate::layout;
use crate::{
    Cursor, Digest32, Intent, Journal, ObjectStore, ScopePath, SealRecord, StorageError, sha256,
};

/// A backend under test: both SPI roles on one instance.
pub trait Backend: ObjectStore + Journal {}

impl<T> Backend for T where T: ObjectStore + Journal {}

/// Runs every contract check against a fresh backend instance; see the
/// [module documentation](self) for the `fresh`/`reopen` contract.
pub fn run_all<B: Backend>(mut fresh: impl FnMut() -> B, mut reopen: impl FnMut() -> B) {
    run_object_store(&mut fresh, &mut reopen);
    run_journal(&mut fresh, &mut reopen);
}

/// Runs the [`ObjectStore`] contract checks; single-role object backends
/// (S3) certify against this alone.
pub fn run_object_store<S: ObjectStore>(mut fresh: impl FnMut() -> S, mut reopen: impl FnMut() -> S) {
    put_then_open_read_returns_identical_bytes(&mut fresh());
    put_rejects_digest_mismatch_as_corrupt(&mut fresh());
    put_is_idempotent_for_identical_bytes_and_digest(&mut fresh());
    put_conflicting_bytes_is_corrupt_and_keeps_original(&mut fresh());
    put_rejects_out_of_range_entity_id(&mut fresh());
    put_into_sealed_scope_is_already_sealed_even_for_identical_bytes(&mut fresh());
    put_into_sealed_scope_with_wrong_digest_is_already_sealed(&mut fresh());
    open_read_of_missing_entity_is_not_found(&mut fresh());
    open_read_survives_retirement_of_unsealed_object(&mut fresh());
    seal_records_digest_and_count_and_verify_scope_matches(&mut fresh());
    seal_is_idempotent_and_rejects_conflicting_seal(&mut fresh());
    verify_scope_fails_as_corrupt_on_recorded_digest_mismatch(&mut fresh());
    verify_scope_fails_as_corrupt_on_entity_count_mismatch(&mut fresh());
    verify_scope_without_seal_is_not_found(&mut fresh());
    list_returns_ascending_ids_and_empty_for_unknown_scope(&mut fresh());
    retire_of_unsealed_scope_removes_only_entities_up_to_cursor(&mut fresh());
    retire_of_sealed_scope_removes_whole_scope(&mut fresh());
    retire_of_unknown_scope_is_not_found(&mut fresh());
    retire_of_empty_but_existing_unsealed_scope_is_not_found(&mut fresh());
    parent_and_child_scopes_hold_independent_entities(&mut fresh());
    object_store_durability_roundtrip(&mut reopen);
}

/// Runs the [`Journal`] contract checks; journal-only backends (Kafka)
/// certify against this alone.
pub fn run_journal<J: Journal>(mut fresh: impl FnMut() -> J, mut reopen: impl FnMut() -> J) {
    append_assigns_strictly_increasing_cursors(&mut fresh());
    replay_from_zero_returns_intents_in_append_order(&mut fresh());
    replay_since_cursor_returns_only_the_tail(&mut fresh());
    append_is_idempotent_for_identical_record(&mut fresh());
    append_conflicting_record_is_corrupt(&mut fresh());
    commit_marks_intent_committed_for_replay(&mut fresh());
    commit_of_unknown_intent_is_not_found(&mut fresh());
    commit_is_idempotent_and_rejects_conflicting_bytes(&mut fresh());
    replay_interleaves_commits_in_append_order(&mut fresh());
    commits_consume_cursors_in_the_global_order(&mut fresh());
    journal_durability_roundtrip(&mut reopen);
}

fn scope(a: u32, b: u32) -> ScopePath {
    match ScopePath::root(a).child(b) {
        Ok(path) => path,
        Err(_) => panic!("test scope construction failed"),
    }
}

fn bytes(text: &str) -> Vec<u8> {
    text.as_bytes().to_vec()
}

fn intent(id: &str, record: &str) -> Intent {
    match Intent::new(id, bytes(record)) {
        Ok(intent) => intent,
        Err(error) => panic!("intent construction failed: {error}"),
    }
}

/// The canonical seal digest for `[(id, payload)]` pairs, as callers are
/// expected to compute it with [`layout::scope_digest`].
fn canonical_digest(entities: &[(u64, &str)]) -> Digest32 {
    let digests: Vec<(u64, Digest32)> = entities
        .iter()
        .map(|(id, payload)| (*id, sha256(payload.as_bytes())))
        .collect();
    match layout::scope_digest(&digests) {
        Ok(digest) => digest,
        Err(error) => panic!("scope digest failed: {error}"),
    }
}

fn read_all<S: ObjectStore>(store: &S, scope: &ScopePath, entity: u64) -> Vec<u8> {
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

fn expect_not_found(result: Result<(), StorageError>, what: &str) {
    match result {
        Err(StorageError::NotFound(_)) => {}
        other => panic!("{what}: expected NotFound, got {other:?}"),
    }
}

/// put, then open_read, returns byte-identical content.
pub fn put_then_open_read_returns_identical_bytes<S: ObjectStore>(store: &mut S) {
    let scope = ScopePath::root(1);
    let payload = bytes("alpha");
    assert!(store.put(&scope, 1, &payload, &sha256(&payload)).is_ok());
    assert_eq!(read_all(store, &scope, 1), payload);
}

/// A payload that does not match its expected digest fails with Corrupt and
/// stores nothing.
pub fn put_rejects_digest_mismatch_as_corrupt<S: ObjectStore>(store: &mut S) {
    let scope = ScopePath::root(2);
    let wrong = sha256(b"not the payload");
    expect_corrupt(store.put(&scope, 1, &bytes("data"), &wrong), "digest mismatch");
    assert!(matches!(
        store.open_read(&scope, 1),
        Err(StorageError::NotFound(_))
    ));
}

/// Re-putting identical bytes with the same digest succeeds as a no-op.
pub fn put_is_idempotent_for_identical_bytes_and_digest<S: ObjectStore>(store: &mut S) {
    let scope = ScopePath::root(3);
    let payload = bytes("same");
    let digest = sha256(&payload);
    assert!(store.put(&scope, 1, &payload, &digest).is_ok());
    assert!(store.put(&scope, 1, &payload, &digest).is_ok());
    assert_eq!(
        store.list(&scope).unwrap_or_else(|e| panic!("list: {e}")),
        vec![1]
    );
}

/// Different bytes under an existing id fail with Corrupt and keep the
/// original content.
pub fn put_conflicting_bytes_is_corrupt_and_keeps_original<S: ObjectStore>(store: &mut S) {
    let scope = ScopePath::root(4);
    let original = bytes("original");
    assert!(store.put(&scope, 1, &original, &sha256(&original)).is_ok());
    let changed = bytes("changed");
    expect_corrupt(
        store.put(&scope, 1, &changed, &sha256(&changed)),
        "conflicting re-put",
    );
    assert_eq!(read_all(store, &scope, 1), original);
}

/// Entity ids above the unsigned 63-bit bound are rejected.
pub fn put_rejects_out_of_range_entity_id<S: ObjectStore>(store: &mut S) {
    let scope = ScopePath::root(5);
    let payload = bytes("bounded");
    let result = store.put(&scope, layout::MAX_ENTITY_ID + 1, &payload, &sha256(&payload));
    assert!(result.is_err(), "out-of-range entity id must be rejected");
}

/// After sealing, put into the scope fails with AlreadySealed — even for
/// bytes identical to what is stored.
pub fn put_into_sealed_scope_is_already_sealed_even_for_identical_bytes<S: ObjectStore>(
    store: &mut S,
) {
    let scope = ScopePath::root(6);
    let payload = bytes("sealed");
    assert!(store.put(&scope, 1, &payload, &sha256(&payload)).is_ok());
    assert!(store.seal_scope(&scope, &canonical_digest(&[(1, "sealed")]), 1).is_ok());
    match store.put(&scope, 1, &payload, &sha256(&payload)) {
        Err(StorageError::AlreadySealed(_)) => {}
        other => panic!("put into sealed scope: expected AlreadySealed, got {other:?}"),
    }
}

/// Seal is checked before the digest: even a digest-mismatched put into a
/// sealed scope fails with AlreadySealed, not Corrupt.
pub fn put_into_sealed_scope_with_wrong_digest_is_already_sealed<S: ObjectStore>(store: &mut S) {
    let scope = ScopePath::root(7);
    let payload = bytes("sealed-first");
    assert!(store.put(&scope, 1, &payload, &sha256(&payload)).is_ok());
    assert!(store.seal_scope(&scope, &canonical_digest(&[(1, "sealed-first")]), 1).is_ok());
    match store.put(&scope, 2, &payload, &[0u8; 32]) {
        Err(StorageError::AlreadySealed(_)) => {}
        other => panic!("sealed put with wrong digest: expected AlreadySealed, got {other:?}"),
    }
}

/// open_read of a missing entity fails with NotFound.
pub fn open_read_of_missing_entity_is_not_found<S: ObjectStore>(store: &mut S) {
    let scope = ScopePath::root(8);
    match store.open_read(&scope, 99) {
        Err(StorageError::NotFound(_)) => {}
        Err(error) => panic!("open_read of missing entity: expected NotFound, got {error:?}"),
        Ok(_) => panic!("open_read of missing entity must fail"),
    }
}

/// A live read holds the object: retiring the unsealed object under an open
/// read does not invalidate the read; the object is gone afterwards.
pub fn open_read_survives_retirement_of_unsealed_object<S: ObjectStore>(store: &mut S) {
    use std::io::Read;
    let scope = ScopePath::root(9);
    let payload = bytes("held open");
    assert!(store.put(&scope, 1, &payload, &sha256(&payload)).is_ok());
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
    assert_eq!(survivors, payload);
    assert!(matches!(
        store.open_read(&scope, 1),
        Err(StorageError::NotFound(_))
    ));
}

/// Sealing records digest and count; verify_scope recomputes and matches;
/// objects stay readable afterwards.
pub fn seal_records_digest_and_count_and_verify_scope_matches<S: ObjectStore>(store: &mut S) {
    let scope = scope(7, 8);
    let payloads = ["alpha", "beta", "gamma"];
    for (index, payload) in payloads.iter().enumerate() {
        let entity = index as u64 + 1;
        assert!(store.put(&scope, entity, &bytes(payload), &sha256(payload.as_bytes())).is_ok());
    }
    let digest = canonical_digest(&[(1, "alpha"), (2, "beta"), (3, "gamma")]);
    assert!(store.seal_scope(&scope, &digest, 3).is_ok());
    let record = store
        .verify_scope(&scope)
        .unwrap_or_else(|e| panic!("verify_scope: {e}"));
    assert_eq!(record, SealRecord { digest, entity_count: 3 });
    assert_eq!(read_all(store, &scope, 2), bytes("beta"));
    assert_eq!(
        store.list(&scope).unwrap_or_else(|e| panic!("list: {e}")),
        vec![1, 2, 3]
    );
}

/// Re-sealing identically is a no-op; a conflicting digest or count is
/// Corrupt.
pub fn seal_is_idempotent_and_rejects_conflicting_seal<S: ObjectStore>(store: &mut S) {
    let scope = ScopePath::root(10);
    let payload = bytes("sealed");
    assert!(store.put(&scope, 1, &payload, &sha256(&payload)).is_ok());
    let seal = sha256(&payload); // recorded verbatim; verify is not called here
    assert!(store.seal_scope(&scope, &seal, 1).is_ok());
    assert!(store.seal_scope(&scope, &seal, 1).is_ok());
    expect_corrupt(store.seal_scope(&scope, &[0u8; 32], 1), "conflicting seal digest");
    expect_corrupt(store.seal_scope(&scope, &seal, 2), "conflicting seal count");
}

/// verify_scope recomputes the canonical digest; a seal whose recorded
/// digest does not match the content is Corrupt.
pub fn verify_scope_fails_as_corrupt_on_recorded_digest_mismatch<S: ObjectStore>(store: &mut S) {
    let scope = ScopePath::root(11);
    let payload = bytes("mismatched");
    assert!(store.put(&scope, 1, &payload, &sha256(&payload)).is_ok());
    assert!(store.seal_scope(&scope, &[0u8; 32], 1).is_ok());
    match store.verify_scope(&scope) {
        Err(StorageError::Corrupt(_)) => {}
        other => panic!("verify_scope with wrong seal digest: expected Corrupt, got {other:?}"),
    }
}

/// verify_scope compares the entity count; a count that disagrees with the
/// live set is Corrupt.
pub fn verify_scope_fails_as_corrupt_on_entity_count_mismatch<S: ObjectStore>(store: &mut S) {
    let scope = ScopePath::root(12);
    let payload = bytes("counted");
    assert!(store.put(&scope, 1, &payload, &sha256(&payload)).is_ok());
    let empty_digest = canonical_digest(&[]);
    assert!(store.seal_scope(&scope, &empty_digest, 2).is_ok());
    match store.verify_scope(&scope) {
        Err(StorageError::Corrupt(_)) => {}
        other => panic!("verify_scope with wrong count: expected Corrupt, got {other:?}"),
    }
}

/// verify_scope on an unsealed scope fails with NotFound.
pub fn verify_scope_without_seal_is_not_found<S: ObjectStore>(store: &mut S) {
    let scope = ScopePath::root(13);
    let payload = bytes("unsealed");
    assert!(store.put(&scope, 1, &payload, &sha256(&payload)).is_ok());
    match store.verify_scope(&scope) {
        Err(StorageError::NotFound(_)) => {}
        other => panic!("verify_scope unsealed scope: expected NotFound, got {other:?}"),
    }
}

/// list returns ascending ids; an unknown scope lists empty.
pub fn list_returns_ascending_ids_and_empty_for_unknown_scope<S: ObjectStore>(store: &mut S) {
    let scope = ScopePath::root(14);
    assert!(
        store
            .list(&scope)
            .unwrap_or_else(|e| panic!("list: {e}"))
            .is_empty()
    );
    for entity in [9u64, 1, 5] {
        let payload = bytes(&format!("entity-{entity}"));
        assert!(store.put(&scope, entity, &payload, &sha256(&payload)).is_ok());
    }
    assert_eq!(
        store.list(&scope).unwrap_or_else(|e| panic!("list: {e}")),
        vec![1, 5, 9]
    );
}

/// An unsealed scope retires only the entities at or below the cursor.
pub fn retire_of_unsealed_scope_removes_only_entities_up_to_cursor<S: ObjectStore>(store: &mut S) {
    let scope = ScopePath::root(15);
    for entity in 1..=5u64 {
        let payload = bytes(&format!("payload-{entity}"));
        assert!(store.put(&scope, entity, &payload, &sha256(&payload)).is_ok());
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

/// A sealed scope retires whole: every entity and the seal record go away,
/// and the scope is writable again afterwards.
pub fn retire_of_sealed_scope_removes_whole_scope<S: ObjectStore>(store: &mut S) {
    let scope = ScopePath::root(16);
    for entity in 1..=3u64 {
        let payload = bytes(&format!("retire-{entity}"));
        assert!(store.put(&scope, entity, &payload, &sha256(&payload)).is_ok());
    }
    let digest = canonical_digest(&[(1, "retire-1"), (2, "retire-2"), (3, "retire-3")]);
    assert!(store.seal_scope(&scope, &digest, 3).is_ok());
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
    let payload = bytes("again");
    assert!(store.put(&scope, 1, &payload, &sha256(&payload)).is_ok());
}

/// Retiring a scope that was never written fails with NotFound.
pub fn retire_of_unknown_scope_is_not_found<S: ObjectStore>(store: &mut S) {
    expect_not_found(store.retire(&ScopePath::root(17), 10).map(|_| ()), "retire of unknown scope");
}

/// Retiring an existing but emptied unsealed scope fails with NotFound.
pub fn retire_of_empty_but_existing_unsealed_scope_is_not_found<S: ObjectStore>(store: &mut S) {
    let scope = ScopePath::root(18);
    let payload = bytes("brief");
    assert!(store.put(&scope, 1, &payload, &sha256(&payload)).is_ok());
    assert_eq!(
        store.retire(&scope, 1).unwrap_or_else(|e| panic!("retire: {e}")),
        1
    );
    expect_not_found(store.retire(&scope, 10).map(|_| ()), "retire of emptied scope");
}

/// Parent and child scopes hold independent entities; sealing the parent
/// leaves the child writable.
pub fn parent_and_child_scopes_hold_independent_entities<S: ObjectStore>(store: &mut S) {
    let parent = ScopePath::root(19);
    let child = match parent.child(2) {
        Ok(child) => child,
        Err(error) => panic!("scope: {error}"),
    };
    let payload = bytes("shared");
    assert!(store.put(&parent, 1, &payload, &sha256(&payload)).is_ok());
    assert!(store.put(&child, 1, &payload, &sha256(&payload)).is_ok());
    assert_eq!(store.list(&parent).unwrap_or_else(|e| panic!("list: {e}")), vec![1]);
    assert_eq!(store.list(&child).unwrap_or_else(|e| panic!("list: {e}")), vec![1]);
    assert!(store.seal_scope(&parent, &canonical_digest(&[(1, "shared")]), 1).is_ok());
    assert!(store.put(&child, 2, &payload, &sha256(&payload)).is_ok());
}

/// Object-store durability-of-return: objects and seal records written
/// through one handle are fully visible through a reopened handle.
pub fn object_store_durability_roundtrip<S: ObjectStore>(reopen: &mut impl FnMut() -> S) {
    let scope = scope(20, 21);
    let payload = bytes("durable");
    let digest = canonical_digest(&[(1, "durable")]);
    {
        let mut store = reopen();
        assert!(store.put(&scope, 1, &payload, &sha256(&payload)).is_ok());
        assert!(store.seal_scope(&scope, &digest, 1).is_ok());
    }
    let store = reopen();
    assert_eq!(read_all(&store, &scope, 1), payload);
    let record = store
        .verify_scope(&scope)
        .unwrap_or_else(|e| panic!("verify_scope after reopen: {e}"));
    assert_eq!(record.digest, digest);
}

/// Durability-of-return for backends implementing both roles.
pub fn durability_roundtrip<B: Backend>(reopen: &mut impl FnMut() -> B) {
    object_store_durability_roundtrip(reopen);
    journal_durability_roundtrip(reopen);
}

/// Appends get strictly increasing cursors starting at 1.
pub fn append_assigns_strictly_increasing_cursors<J: Journal>(store: &mut J) {
    assert_eq!(
        store.append_intent(&intent("a", "1")).unwrap_or_else(|e| panic!("append: {e}")),
        Cursor(1)
    );
    assert_eq!(
        store.append_intent(&intent("b", "2")).unwrap_or_else(|e| panic!("append: {e}")),
        Cursor(2)
    );
    assert_eq!(
        store.append_intent(&intent("c", "3")).unwrap_or_else(|e| panic!("append: {e}")),
        Cursor(3)
    );
}

/// replay(0) returns intents in append order with no commit state.
pub fn replay_from_zero_returns_intents_in_append_order<J: Journal>(store: &mut J) {
    let first = store.append_intent(&intent("first", "one")).unwrap_or_else(|e| panic!("append: {e}"));
    let second = store.append_intent(&intent("second", "two")).unwrap_or_else(|e| panic!("append: {e}"));
    let entries = store.replay(Cursor(0)).unwrap_or_else(|e| panic!("replay: {e}"));
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].cursor, first);
    assert_eq!(entries[0].intent.id(), "first");
    assert_eq!(entries[0].intent.record(), b"one");
    assert!(!entries[0].committed());
    assert_eq!(entries[1].cursor, second);
    assert_eq!(entries[1].intent.id(), "second");
}

/// replay(since) returns exactly the intents after the cursor.
pub fn replay_since_cursor_returns_only_the_tail<J: Journal>(store: &mut J) {
    store.append_intent(&intent("one", "1")).unwrap_or_else(|e| panic!("append: {e}"));
    let second = store.append_intent(&intent("two", "2")).unwrap_or_else(|e| panic!("append: {e}"));
    let third = store.append_intent(&intent("three", "3")).unwrap_or_else(|e| panic!("append: {e}"));
    let entries = store.replay(second).unwrap_or_else(|e| panic!("replay: {e}"));
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].cursor, third);
    assert_eq!(entries[0].intent.id(), "three");
    assert!(
        store
            .replay(third)
            .unwrap_or_else(|e| panic!("replay: {e}"))
            .is_empty()
    );
}

/// Re-appending an intent id with identical bytes returns the original
/// cursor and adds nothing.
pub fn append_is_idempotent_for_identical_record<J: Journal>(store: &mut J) {
    let cursor = store.append_intent(&intent("retry", "same")).unwrap_or_else(|e| panic!("append: {e}"));
    assert_eq!(
        store.append_intent(&intent("retry", "same")).unwrap_or_else(|e| panic!("append: {e}")),
        cursor
    );
    assert_eq!(
        store.replay(Cursor(0)).unwrap_or_else(|e| panic!("replay: {e}")).len(),
        1
    );
}

/// Re-appending an intent id with different bytes is Corrupt and keeps the
/// original record.
pub fn append_conflicting_record_is_corrupt<J: Journal>(store: &mut J) {
    store.append_intent(&intent("fixed", "original")).unwrap_or_else(|e| panic!("append: {e}"));
    expect_corrupt(
        store.append_intent(&intent("fixed", "changed")).map(|_| ()),
        "conflicting append",
    );
    let entries = store.replay(Cursor(0)).unwrap_or_else(|e| panic!("replay: {e}"));
    assert_eq!(entries[0].intent.record(), b"original");
}

/// commit folds into the intent's replay entry: the recovery path sees the
/// intent committed.
pub fn commit_marks_intent_committed_for_replay<J: Journal>(store: &mut J) {
    let cursor = store.append_intent(&intent("work", "admitted")).unwrap_or_else(|e| panic!("append: {e}"));
    assert!(store.commit("work", b"done").is_ok());
    let before = Cursor(cursor.0 - 1);
    let entries = store.replay(before).unwrap_or_else(|e| panic!("replay: {e}"));
    assert_eq!(entries.len(), 1);
    assert!(entries[0].committed());
    assert_eq!(entries[0].commit.as_deref(), Some(b"done".as_slice()));
}

/// Committing an unknown intent id fails with NotFound.
pub fn commit_of_unknown_intent_is_not_found<J: Journal>(store: &mut J) {
    expect_not_found(store.commit("absent", b"x"), "commit of unknown intent");
}

/// Re-committing identically is a no-op; conflicting commit bytes are
/// Corrupt.
pub fn commit_is_idempotent_and_rejects_conflicting_bytes<J: Journal>(store: &mut J) {
    store.append_intent(&intent("settled", "record")).unwrap_or_else(|e| panic!("append: {e}"));
    assert!(store.commit("settled", b"commit").is_ok());
    assert!(store.commit("settled", b"commit").is_ok());
    expect_corrupt(store.commit("settled", b"other"), "conflicting commit");
    let entries = store.replay(Cursor(0)).unwrap_or_else(|e| panic!("replay: {e}"));
    assert_eq!(entries[0].commit.as_deref(), Some(b"commit".as_slice()));
}

/// Replay returns intents in append order with commit state folded in,
/// however the commits were interleaved.
pub fn replay_interleaves_commits_in_append_order<J: Journal>(store: &mut J) {
    store.append_intent(&intent("a", "a")).unwrap_or_else(|e| panic!("append: {e}"));
    store.append_intent(&intent("b", "b")).unwrap_or_else(|e| panic!("append: {e}"));
    store.append_intent(&intent("c", "c")).unwrap_or_else(|e| panic!("append: {e}"));
    assert!(store.commit("a", b"a-done").is_ok());
    assert!(store.commit("c", b"c-done").is_ok());
    let entries = store.replay(Cursor(0)).unwrap_or_else(|e| panic!("replay: {e}"));
    let ids: Vec<&str> = entries.iter().map(|entry| entry.intent.id()).collect();
    assert_eq!(ids, vec!["a", "b", "c"]);
    assert!(entries[0].committed());
    assert!(!entries[1].committed());
    assert!(entries[2].committed());
}

/// Commits consume cursors in the global append order: intents need not
/// have contiguous cursors.
pub fn commits_consume_cursors_in_the_global_order<J: Journal>(store: &mut J) {
    let a = store.append_intent(&intent("a", "a")).unwrap_or_else(|e| panic!("append: {e}"));
    let b = store.append_intent(&intent("b", "b")).unwrap_or_else(|e| panic!("append: {e}"));
    assert!(store.commit("a", b"a-done").is_ok());
    let c = store.append_intent(&intent("c", "c")).unwrap_or_else(|e| panic!("append: {e}"));
    assert_eq!((a, b, c), (Cursor(1), Cursor(2), Cursor(4)));
    let entries = store.replay(Cursor(0)).unwrap_or_else(|e| panic!("replay: {e}"));
    let cursors: Vec<Cursor> = entries.iter().map(|entry| entry.cursor).collect();
    assert_eq!(cursors, vec![Cursor(1), Cursor(2), Cursor(4)]);
}

/// Journal durability-of-return: intents and their commit state survive a
/// reopen.
pub fn journal_durability_roundtrip<J: Journal>(reopen: &mut impl FnMut() -> J) {
    let cursor;
    {
        let mut store = reopen();
        cursor = store
            .append_intent(&intent("durable", "admitted"))
            .unwrap_or_else(|e| panic!("append: {e}"));
        assert!(store.commit("durable", b"committed").is_ok());
        store.append_intent(&intent("pending", "uncommitted")).unwrap_or_else(|e| panic!("append: {e}"));
    }
    let store = reopen();
    let entries = store.replay(Cursor(0)).unwrap_or_else(|e| panic!("replay: {e}"));
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].cursor, cursor);
    assert_eq!(entries[0].intent.id(), "durable");
    assert_eq!(entries[0].commit.as_deref(), Some(b"committed".as_slice()));
    assert_eq!(entries[1].intent.id(), "pending");
    assert!(!entries[1].committed());
}
