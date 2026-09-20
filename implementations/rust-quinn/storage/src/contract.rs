//! Backend-agnostic contract test suite.
//!
//! Every backend must pass the suite for each role it implements, and the
//! checks mirror the Java lane's `ObjectStoreContractTest` /
//! `JournalContractTest` one-for-one, including the concurrency rows
//! (exercised here behind a `Mutex`, the Rust-side expression of the
//! pinned thread-safety requirement):
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
    Cursor, Digest32, Intent, Journal, ObjectStore, PutWriter, ScopePath, SealRecord, StorageError,
    sha256,
};

/// A backend under test: both SPI roles on one instance.
pub trait Backend: ObjectStore + Journal {}

impl<T> Backend for T where T: ObjectStore + Journal {}

/// Runs every contract check against a fresh backend instance; see the
/// [module documentation](self) for the `fresh`/`reopen` contract.
pub fn run_all<B: Backend + Send>(mut fresh: impl FnMut() -> B, mut reopen: impl FnMut() -> B) {
    run_object_store(&mut fresh, &mut reopen);
    run_journal(&mut fresh, &mut reopen);
}

/// Runs the [`ObjectStore`] contract checks; single-role object backends
/// (S3) certify against this alone.
///
/// The `Send` bound is part of the contract: the SPI is thread-safe, and
/// the concurrency checks share one store across scoped threads.
pub fn run_object_store<S: ObjectStore + Send>(
    mut fresh: impl FnMut() -> S,
    mut reopen: impl FnMut() -> S,
) {
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
    verify_scope_returns_the_recorded_caller_digest_verbatim(&mut fresh());
    verify_scope_fails_as_corrupt_on_entity_count_mismatch(&mut fresh());
    verify_scope_without_seal_is_not_found(&mut fresh());
    list_returns_ascending_ids_and_empty_for_unknown_scope(&mut fresh());
    retire_of_unsealed_scope_removes_only_entities_up_to_cursor(&mut fresh());
    retire_of_sealed_scope_removes_whole_scope(&mut fresh());
    retire_of_unknown_scope_is_not_found(&mut fresh());
    retire_of_empty_but_existing_unsealed_scope_is_not_found(&mut fresh());
    parent_and_child_scopes_hold_independent_entities(&mut fresh());
    child_scope_dirs_coexist_with_entity_buckets(&mut fresh());
    retire_of_sealed_scope_preserves_child_scopes(&mut fresh());
    concurrent_conflicting_puts_yield_exactly_one_success(&mut fresh());
    put_racing_seal_scope_never_publishes_after_seal(&mut fresh());
    streaming_put_roundtrip_matches_one_shot(&mut fresh());
    streaming_put_digest_mismatch_stores_nothing(&mut fresh());
    streaming_put_into_sealed_scope_is_already_sealed(&mut fresh());
    streaming_put_conflict_with_existing_is_corrupt_at_begin(&mut fresh());
    streaming_put_idempotent_absorbing_reput(&mut fresh());
    streaming_put_invisible_until_finish(&mut fresh());
    streaming_put_medium_payload_boundaries(&mut fresh());
    list_scopes_empty_store(&mut fresh());
    list_scopes_nested_and_canonically_ordered(&mut fresh());
    list_scopes_visibility_across_seal_and_retire(&mut fresh());
    object_store_durability_roundtrip(&mut reopen);
}

/// Runs the [`Journal`] contract checks; journal-only backends (Kafka)
/// certify against this alone.
pub fn run_journal<J: Journal>(fresh: impl FnMut() -> J, reopen: impl FnMut() -> J) {
    run_journal_with(JournalProfile::DENSE, &mut RawJournalPayloads, fresh, reopen);
}

/// Behavior profile for [`run_journal_with`], letting adapters document
/// genuinely unbridgeable semantics instead of faking them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JournalProfile {
    /// True when commits consume dense cursors in the global append order
    /// (all six SPI backends). False for adapters over journals that assign
    /// commits no independent position (e.g. the v2 client journal, whose
    /// receipts update the intent's row in place): the cursor-density check
    /// is skipped, and append cursors are required only to strictly
    /// increase from 1.
    pub dense_cursors: bool,
}

impl JournalProfile {
    /// Dense global cursors (the pinned default).
    pub const DENSE: JournalProfile = JournalProfile { dense_cursors: true };
    /// Sparse/mapped cursors for adapters over journals without native
    /// commit positions.
    pub const SPARSE: JournalProfile = JournalProfile { dense_cursors: false };
}

/// Supplies the payloads journal contract checks write. The default
/// ([`RawJournalPayloads`]) uses opaque byte strings; adapters over typed
/// journals (whose records must be valid protocol encodings) supply their
/// own.
pub trait JournalPayloads {
    /// A fresh intent for `tag`; each call within a run must produce a
    /// unique id (a store may reject reused ids with different bytes).
    fn intent(&mut self, tag: &str) -> Intent;
    /// An intent with the same id as `base` but different record bytes —
    /// must be rejected with Corrupt by a conforming journal.
    fn variant_intent(&mut self, tag: &str, base: &Intent) -> Intent;
    /// A valid commit record for `intent`; `variant` distinguishes the
    /// conflicting-commit check (different variants must differ but both
    /// be valid).
    fn commit_record(&mut self, intent: &Intent, variant: u32) -> Vec<u8>;
}

/// Opaque-byte payloads (the default for real SPI backends).
#[derive(Debug, Default)]
pub struct RawJournalPayloads;

impl JournalPayloads for RawJournalPayloads {
    fn intent(&mut self, tag: &str) -> Intent {
        match Intent::new(tag, tag.as_bytes().to_vec()) {
            Ok(intent) => intent,
            Err(error) => panic!("intent construction failed: {error}"),
        }
    }

    fn variant_intent(&mut self, tag: &str, base: &Intent) -> Intent {
        let _ = tag;
        let mut record = base.record().to_vec();
        record.push(b'!');
        match Intent::new(base.id(), record) {
            Ok(intent) => intent,
            Err(error) => panic!("intent construction failed: {error}"),
        }
    }

    fn commit_record(&mut self, intent: &Intent, variant: u32) -> Vec<u8> {
        format!("{}-commit-{variant}", intent.id()).into_bytes()
    }
}

/// Runs the [`Journal`] contract checks with explicit payload and cursor
/// profiles — the entry point for adapters over existing typed journals.
/// See [`run_journal`] for the `fresh`/`reopen` contract.
pub fn run_journal_with<J: Journal>(
    profile: JournalProfile,
    payloads: &mut impl JournalPayloads,
    mut fresh: impl FnMut() -> J,
    mut reopen: impl FnMut() -> J,
) {
    append_assigns_with(&mut fresh(), payloads, profile.dense_cursors);
    replay_from_zero_with(&mut fresh(), payloads);
    replay_since_with(&mut fresh(), payloads);
    append_idempotent_with(&mut fresh(), payloads);
    append_conflicting_with(&mut fresh(), payloads);
    commit_marks_with(&mut fresh(), payloads);
    commit_unknown_with(&mut fresh(), payloads);
    commit_idempotent_with(&mut fresh(), payloads);
    replay_interleaves_with(&mut fresh(), payloads);
    if profile.dense_cursors {
        commits_consume_cursors_in_the_global_order(&mut fresh());
    }
    journal_durability_with(&mut reopen, payloads);
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

/// The canonical seal digest for `[(id, payload)]` pairs, as callers are
/// expected to compute it with [`layout::scope_digest`].
fn canonical_digest(entities: &[(u32, &str)]) -> Digest32 {
    let digests: Vec<(u32, Digest32)> = entities
        .iter()
        .map(|(id, payload)| (*id, sha256(payload.as_bytes())))
        .collect();
    match layout::scope_digest(&digests) {
        Ok(digest) => digest,
        Err(error) => panic!("scope digest failed: {error}"),
    }
}

fn read_all<S: ObjectStore>(store: &S, scope: &ScopePath, entity: u32) -> Vec<u8> {
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

/// Entity ids above the 32-bit protocol bound are rejected.
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
        let entity = index as u32 + 1;
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

/// verify_scope does not recompute the caller-supplied seal digest (the
/// protocol digest covers statuses the store does not hold): a seal with a
/// non-canonical digest verifies fine when every object's recorded digest
/// and the entity count hold, and the recorded digest is returned verbatim
/// for the protocol layer to compare.
pub fn verify_scope_returns_the_recorded_caller_digest_verbatim<S: ObjectStore>(store: &mut S) {
    let scope = ScopePath::root(11);
    let payload = bytes("mismatched");
    assert!(store.put(&scope, 1, &payload, &sha256(&payload)).is_ok());
    assert!(store.seal_scope(&scope, &[7u8; 32], 1).is_ok());
    let record = store
        .verify_scope(&scope)
        .unwrap_or_else(|e| panic!("verify_scope must not recompute the seal digest: {e}"));
    assert_eq!(record.digest, [7u8; 32]);
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
    for entity in [9u32, 1, 5] {
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
    for entity in 1..=5u32 {
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
    for entity in 1..=3u32 {
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

/// Child-scope containers coexist with entity buckets inside a parent scope
/// container: list and verify_scope iterate entity buckets only and never
/// error on child-scope entries.
pub fn child_scope_dirs_coexist_with_entity_buckets<S: ObjectStore>(store: &mut S) {
    let parent = ScopePath::root(30);
    let child = match parent.child(1) {
        Ok(child) => child,
        Err(error) => panic!("scope: {error}"),
    };
    let grandchild = match child.child(2) {
        Ok(grandchild) => grandchild,
        Err(error) => panic!("scope: {error}"),
    };
    // Write entities at every level, interleaved, so the parent's container
    // holds child-scope entries alongside its own entity buckets.
    let parent_payload = bytes("parent");
    assert!(store.put(&parent, 1, &parent_payload, &sha256(&parent_payload)).is_ok());
    let child_payload = bytes("child");
    assert!(store.put(&child, 1, &child_payload, &sha256(&child_payload)).is_ok());
    let grandchild_payload = bytes("grandchild");
    assert!(
        store
            .put(&grandchild, 1, &grandchild_payload, &sha256(&grandchild_payload))
            .is_ok()
    );
    let parent_payload_2 = bytes("parent-2");
    assert!(store.put(&parent, 2, &parent_payload_2, &sha256(&parent_payload_2)).is_ok());

    // The parent's list sees only its own direct entities.
    assert_eq!(
        store.list(&parent).unwrap_or_else(|e| panic!("list: {e}")),
        vec![1, 2]
    );
    // Sealing and verifying the parent ignores the child's container.
    let digest = canonical_digest(&[(1, "parent"), (2, "parent-2")]);
    assert!(store.seal_scope(&parent, &digest, 2).is_ok());
    assert!(store.verify_scope(&parent).is_ok());
    // The child remains writable and independent.
    let child_payload_2 = bytes("child-2");
    assert!(store.put(&child, 2, &child_payload_2, &sha256(&child_payload_2)).is_ok());
    assert_eq!(read_all(store, &child, 1), child_payload);
    assert_eq!(read_all(store, &grandchild, 1), grandchild_payload);
}

/// Sealed retire is scope-local: the scope's own entities and seal record
/// go away; child scopes survive untouched and stay verifiable.
pub fn retire_of_sealed_scope_preserves_child_scopes<S: ObjectStore>(store: &mut S) {
    let parent = ScopePath::root(33);
    let child = match parent.child(2) {
        Ok(child) => child,
        Err(error) => panic!("scope: {error}"),
    };
    let parent_payload = bytes("parent-object");
    let child_payload = bytes("child-object");
    assert!(store.put(&parent, 1, &parent_payload, &sha256(&parent_payload)).is_ok());
    assert!(store.put(&child, 1, &child_payload, &sha256(&child_payload)).is_ok());
    let parent_digest = canonical_digest(&[(1, "parent-object")]);
    let child_digest = canonical_digest(&[(1, "child-object")]);
    assert!(store.seal_scope(&parent, &parent_digest, 1).is_ok());
    assert!(store.seal_scope(&child, &child_digest, 1).is_ok());

    assert_eq!(
        store.retire(&parent, 0).unwrap_or_else(|e| panic!("retire: {e}")),
        1
    );
    // Parent is gone; child survives intact.
    assert!(
        store
            .list(&parent)
            .unwrap_or_else(|e| panic!("list: {e}"))
            .is_empty()
    );
    assert!(matches!(
        store.verify_scope(&parent),
        Err(StorageError::NotFound(_))
    ));
    assert_eq!(read_all(store, &child, 1), child_payload);
    let child_seal = store
        .verify_scope(&child)
        .unwrap_or_else(|e| panic!("child scope must survive the parent's retire: {e}"));
    assert_eq!(child_seal.digest, child_digest);
}

/// Thread safety: concurrent conflicting puts to one entity id yield
/// exactly one success; every loser fails with Corrupt; the stored content
/// is the winner's payload.
pub fn concurrent_conflicting_puts_yield_exactly_one_success<S: ObjectStore + Send>(
    store: &mut S,
) {
    let scope = ScopePath::root(31);
    let shared = std::sync::Mutex::new(store);
    let barrier = std::sync::Barrier::new(8);
    let outcomes: Vec<Result<Vec<u8>, StorageError>> = std::thread::scope(|threads| {
        let handles: Vec<_> = (0..8u8)
            .map(|variant| {
                let shared = &shared;
                let barrier = &barrier;
                let scope = &scope;
                threads.spawn(move || {
                    let payload = bytes(&format!("variant-{variant}"));
                    barrier.wait();
                    shared
                        .lock()
                        .unwrap_or_else(|e| panic!("lock: {e}"))
                        .put(scope, 1, &payload, &sha256(&payload))
                        .map(|_| payload)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap_or_else(|e| panic!("thread: {e:?}")))
            .collect()
    });
    let winners: Vec<&Vec<u8>> = outcomes.iter().filter_map(|r| r.as_ref().ok()).collect();
    assert_eq!(winners.len(), 1, "exactly one conflicting put wins: {outcomes:?}");
    for outcome in &outcomes {
        match outcome {
            Ok(_) => {}
            Err(StorageError::Corrupt(_)) => {}
            Err(error) => panic!("loser must fail Corrupt, got {error:?}"),
        }
    }
    let guard = shared.lock().unwrap_or_else(|e| panic!("lock: {e}"));
    assert_eq!(read_all(&**guard, &scope, 1), **winners[0]);
}

/// Thread safety: a put racing a seal never publishes after the seal —
/// every put either landed before the seal (readable, counted) or failed
/// with AlreadySealed, and a put that starts strictly after the seal
/// returns fails with AlreadySealed.
pub fn put_racing_seal_scope_never_publishes_after_seal<S: ObjectStore + Send>(store: &mut S) {
    let scope = ScopePath::root(32);
    let shared = std::sync::Mutex::new(store);
    let barrier = std::sync::Barrier::new(8);
    std::thread::scope(|threads| {
        let handles: Vec<_> = (0..8u32)
            .map(|variant| {
                let shared = &shared;
                let barrier = &barrier;
                let scope = &scope;
                threads.spawn(move || {
                    let entity = 100 + variant;
                    let payload = bytes(&format!("racing-{variant}"));
                    barrier.wait();
                    shared
                        .lock()
                        .unwrap_or_else(|e| panic!("lock: {e}"))
                        .put(scope, entity, &payload, &sha256(&payload))
                        .map(|_| entity)
                })
            })
            .collect();
        let published: Vec<u32> = handles
            .into_iter()
            .filter_map(|handle| {
                handle
                    .join()
                    .unwrap_or_else(|e| panic!("thread: {e:?}"))
                    .ok()
            })
            .collect();
        // Every racing put that succeeded is durably readable.
        let guard = shared.lock().unwrap_or_else(|e| panic!("lock: {e}"));
        for entity in &published {
            let payload = bytes(&format!("racing-{}", entity - 100));
            assert_eq!(read_all(&**guard, &scope, *entity), payload);
        }
        // Seal after the race, recording exactly the winners.
        let digests: Vec<(u32, Digest32)> = published
            .iter()
            .map(|id| (*id, sha256(format!("racing-{}", id - 100).as_bytes())))
            .collect();
        let digest = layout::scope_digest(&digests).unwrap_or_else(|e| panic!("digest: {e}"));
        let mut guard = guard;
        guard
            .seal_scope(&scope, &digest, published.len() as u64)
            .unwrap_or_else(|e| panic!("seal: {e}"));
        guard.verify_scope(&scope).unwrap_or_else(|e| panic!("verify: {e}"));
        // A put that starts strictly after the seal returned must never
        // publish.
        let late = bytes("late");
        match guard.put(&scope, 999, &late, &sha256(&late)) {
            Err(StorageError::AlreadySealed(_)) => {}
            other => panic!("put after seal: expected AlreadySealed, got {other:?}"),
        }
        assert!(matches!(
            guard.open_read(&scope, 999),
            Err(StorageError::NotFound(_))
        ));
        assert_eq!(
            guard.list(&scope).unwrap_or_else(|e| panic!("list: {e}")).len(),
            published.len(),
            "late put never published"
        );
    });
}

/// Durability-of-return for backends implementing both roles.
pub fn durability_roundtrip<B: Backend>(reopen: &mut impl FnMut() -> B) {
    object_store_durability_roundtrip(reopen);
    journal_durability_roundtrip(reopen);
}

/// Appends get strictly increasing cursors starting at 1.
pub fn append_assigns_strictly_increasing_cursors<J: Journal>(store: &mut J) {
    let mut payloads = RawJournalPayloads;
    append_assigns_with(store, &mut payloads, true);
}

fn append_assigns_with<J: Journal, P: JournalPayloads>(
    store: &mut J,
    payloads: &mut P,
    dense: bool,
) {
    let a = payloads.intent("a");
    let b = payloads.intent("b");
    let c = payloads.intent("c");
    let c1 = store.append_intent(&a).unwrap_or_else(|e| panic!("append: {e}"));
    let c2 = store.append_intent(&b).unwrap_or_else(|e| panic!("append: {e}"));
    let c3 = store.append_intent(&c).unwrap_or_else(|e| panic!("append: {e}"));
    assert_eq!(c1, Cursor(1), "the first cursor is 1");
    assert!(c1 < c2 && c2 < c3, "cursors strictly increase: {c1:?} {c2:?} {c3:?}");
    if dense {
        assert_eq!((c1, c2, c3), (Cursor(1), Cursor(2), Cursor(3)));
    }
}

/// replay(0) returns intents in append order with no commit state.
pub fn replay_from_zero_returns_intents_in_append_order<J: Journal>(store: &mut J) {
    let mut payloads = RawJournalPayloads;
    replay_from_zero_with(store, &mut payloads);
}

fn replay_from_zero_with<J: Journal, P: JournalPayloads>(store: &mut J, payloads: &mut P) {
    let first = payloads.intent("first");
    let second = payloads.intent("second");
    let c1 = store.append_intent(&first).unwrap_or_else(|e| panic!("append: {e}"));
    let c2 = store.append_intent(&second).unwrap_or_else(|e| panic!("append: {e}"));
    let entries = store.replay(Cursor(0)).unwrap_or_else(|e| panic!("replay: {e}"));
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].cursor, c1);
    assert_eq!(entries[0].intent, first);
    assert!(!entries[0].committed());
    assert_eq!(entries[1].cursor, c2);
    assert_eq!(entries[1].intent, second);
}

/// replay(since) returns exactly the intents after the cursor.
pub fn replay_since_cursor_returns_only_the_tail<J: Journal>(store: &mut J) {
    let mut payloads = RawJournalPayloads;
    replay_since_with(store, &mut payloads);
}

fn replay_since_with<J: Journal, P: JournalPayloads>(store: &mut J, payloads: &mut P) {
    let one = payloads.intent("one");
    let two = payloads.intent("two");
    let three = payloads.intent("three");
    store.append_intent(&one).unwrap_or_else(|e| panic!("append: {e}"));
    let second = store.append_intent(&two).unwrap_or_else(|e| panic!("append: {e}"));
    let third = store.append_intent(&three).unwrap_or_else(|e| panic!("append: {e}"));
    let entries = store.replay(second).unwrap_or_else(|e| panic!("replay: {e}"));
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].cursor, third);
    assert_eq!(entries[0].intent, three);
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
    let mut payloads = RawJournalPayloads;
    append_idempotent_with(store, &mut payloads);
}

fn append_idempotent_with<J: Journal, P: JournalPayloads>(store: &mut J, payloads: &mut P) {
    let intent = payloads.intent("retry");
    let cursor = store.append_intent(&intent).unwrap_or_else(|e| panic!("append: {e}"));
    assert_eq!(
        store.append_intent(&intent).unwrap_or_else(|e| panic!("append: {e}")),
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
    let mut payloads = RawJournalPayloads;
    append_conflicting_with(store, &mut payloads);
}

fn append_conflicting_with<J: Journal, P: JournalPayloads>(store: &mut J, payloads: &mut P) {
    let original = payloads.intent("fixed");
    let changed = payloads.variant_intent("fixed", &original);
    store.append_intent(&original).unwrap_or_else(|e| panic!("append: {e}"));
    expect_corrupt(
        store.append_intent(&changed).map(|_| ()),
        "conflicting append",
    );
    let entries = store.replay(Cursor(0)).unwrap_or_else(|e| panic!("replay: {e}"));
    assert_eq!(entries[0].intent, original);
}

/// commit folds into the intent's replay entry: the recovery path sees the
/// intent committed.
pub fn commit_marks_intent_committed_for_replay<J: Journal>(store: &mut J) {
    let mut payloads = RawJournalPayloads;
    commit_marks_with(store, &mut payloads);
}

fn commit_marks_with<J: Journal, P: JournalPayloads>(store: &mut J, payloads: &mut P) {
    let intent = payloads.intent("work");
    let commit = payloads.commit_record(&intent, 0);
    let cursor = store.append_intent(&intent).unwrap_or_else(|e| panic!("append: {e}"));
    assert!(store.commit(intent.id(), &commit).is_ok());
    let before = Cursor(cursor.0 - 1);
    let entries = store.replay(before).unwrap_or_else(|e| panic!("replay: {e}"));
    assert_eq!(entries.len(), 1);
    assert!(entries[0].committed());
    assert_eq!(entries[0].commit.as_deref(), Some(commit.as_slice()));
    assert!(entries[0].commit_cursor.is_some());
}

/// Committing an unknown intent id fails with NotFound.
pub fn commit_of_unknown_intent_is_not_found<J: Journal>(store: &mut J) {
    let mut payloads = RawJournalPayloads;
    commit_unknown_with(store, &mut payloads);
}

fn commit_unknown_with<J: Journal, P: JournalPayloads>(store: &mut J, payloads: &mut P) {
    let phantom = payloads.intent("absent"); // never appended
    let record = payloads.commit_record(&phantom, 0);
    expect_not_found(store.commit(phantom.id(), &record), "commit of unknown intent");
}

/// Re-committing identically is a no-op; conflicting commit bytes are
/// Corrupt.
pub fn commit_is_idempotent_and_rejects_conflicting_bytes<J: Journal>(store: &mut J) {
    let mut payloads = RawJournalPayloads;
    commit_idempotent_with(store, &mut payloads);
}

fn commit_idempotent_with<J: Journal, P: JournalPayloads>(store: &mut J, payloads: &mut P) {
    let intent = payloads.intent("settled");
    let commit = payloads.commit_record(&intent, 0);
    let conflicting = payloads.commit_record(&intent, 1);
    store.append_intent(&intent).unwrap_or_else(|e| panic!("append: {e}"));
    assert!(store.commit(intent.id(), &commit).is_ok());
    assert!(store.commit(intent.id(), &commit).is_ok());
    expect_corrupt(store.commit(intent.id(), &conflicting), "conflicting commit");
    let entries = store.replay(Cursor(0)).unwrap_or_else(|e| panic!("replay: {e}"));
    assert_eq!(entries[0].commit.as_deref(), Some(commit.as_slice()));
}

/// Replay returns intents in append order with commit state folded in,
/// however the commits were interleaved.
pub fn replay_interleaves_commits_in_append_order<J: Journal>(store: &mut J) {
    let mut payloads = RawJournalPayloads;
    replay_interleaves_with(store, &mut payloads);
}

fn replay_interleaves_with<J: Journal, P: JournalPayloads>(store: &mut J, payloads: &mut P) {
    let a = payloads.intent("a");
    let b = payloads.intent("b");
    let c = payloads.intent("c");
    store.append_intent(&a).unwrap_or_else(|e| panic!("append: {e}"));
    store.append_intent(&b).unwrap_or_else(|e| panic!("append: {e}"));
    store.append_intent(&c).unwrap_or_else(|e| panic!("append: {e}"));
    assert!(store.commit(a.id(), &payloads.commit_record(&a, 0)).is_ok());
    assert!(store.commit(c.id(), &payloads.commit_record(&c, 0)).is_ok());
    let entries = store.replay(Cursor(0)).unwrap_or_else(|e| panic!("replay: {e}"));
    let ids: Vec<&str> = entries.iter().map(|entry| entry.intent.id()).collect();
    assert_eq!(ids, vec![a.id(), b.id(), c.id()]);
    assert!(entries[0].committed());
    assert!(!entries[1].committed());
    assert!(entries[2].committed());
}

/// Commits consume cursors in the global append order: intents need not
/// have contiguous cursors. Dense backends only — journals whose commits
/// carry no independent position (the v2 client journal) run under
/// [`JournalProfile::SPARSE`], which skips this check.
pub fn commits_consume_cursors_in_the_global_order<J: Journal>(store: &mut J) {
    let mut payloads = RawJournalPayloads;
    let a = payloads.intent("a");
    let b = payloads.intent("b");
    let c = payloads.intent("c");
    let ca = store.append_intent(&a).unwrap_or_else(|e| panic!("append: {e}"));
    let cb = store.append_intent(&b).unwrap_or_else(|e| panic!("append: {e}"));
    assert!(store.commit(a.id(), &payloads.commit_record(&a, 0)).is_ok());
    let cc = store.append_intent(&c).unwrap_or_else(|e| panic!("append: {e}"));
    assert_eq!((ca, cb, cc), (Cursor(1), Cursor(2), Cursor(4)));
    let entries = store.replay(Cursor(0)).unwrap_or_else(|e| panic!("replay: {e}"));
    let cursors: Vec<Cursor> = entries.iter().map(|entry| entry.cursor).collect();
    assert_eq!(cursors, vec![Cursor(1), Cursor(2), Cursor(4)]);
    // The commit's own cursor is 3, carried on the intent's replay entry.
    assert_eq!(entries[0].commit_cursor, Some(Cursor(3)));
    assert_eq!(entries[1].commit_cursor, None);
    assert_eq!(entries[2].commit_cursor, None);
}

/// Journal durability-of-return: intents and their commit state survive a
/// reopen.
pub fn journal_durability_roundtrip<J: Journal>(reopen: &mut impl FnMut() -> J) {
    let mut payloads = RawJournalPayloads;
    journal_durability_with(reopen, &mut payloads);
}

fn journal_durability_with<J: Journal>(
    reopen: &mut impl FnMut() -> J,
    payloads: &mut impl JournalPayloads,
) {
    let durable = payloads.intent("durable");
    let commit = payloads.commit_record(&durable, 0);
    let pending = payloads.intent("pending");
    let cursor;
    {
        let mut store = reopen();
        cursor = store
            .append_intent(&durable)
            .unwrap_or_else(|e| panic!("append: {e}"));
        assert!(store.commit(durable.id(), &commit).is_ok());
        store.append_intent(&pending).unwrap_or_else(|e| panic!("append: {e}"));
    }
    let store = reopen();
    let entries = store.replay(Cursor(0)).unwrap_or_else(|e| panic!("replay: {e}"));
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].cursor, cursor);
    assert_eq!(entries[0].intent, durable);
    assert_eq!(entries[0].commit.as_deref(), Some(commit.as_slice()));
    assert!(entries[0].commit_cursor.is_some());
    assert_eq!(entries[1].intent, pending);
    assert!(!entries[1].committed());
}

/// Streaming put of the same bytes in many small chunks equals one-shot put.
pub fn streaming_put_roundtrip_matches_one_shot<S: ObjectStore>(store: &mut S) {
    use std::io::Write as _;
    let scope = ScopePath::root(50);
    let payload: Vec<u8> = (0..=255u8).cycle().take(100_000).collect();
    let digest = sha256(&payload);
    {
        let mut writer = store
            .begin_put(&scope, 1, &digest)
            .unwrap_or_else(|e| panic!("begin_put: {e}"));
        for chunk in payload.chunks(997) {
            writer.write_all(chunk).unwrap_or_else(|e| panic!("write: {e}"));
        }
        writer.finish().unwrap_or_else(|e| panic!("finish: {e}"));
    }
    let oneshot = ScopePath::root(51);
    assert!(store.put(&oneshot, 1, &payload, &digest).is_ok());
    assert_eq!(read_all(store, &scope, 1), payload);
    assert_eq!(read_all(store, &scope, 1), read_all(store, &oneshot, 1));
}

/// A stream that does not match its expected digest stores nothing.
pub fn streaming_put_digest_mismatch_stores_nothing<S: ObjectStore>(store: &mut S) {
    use std::io::Write as _;
    let scope = ScopePath::root(52);
    let wrong = sha256(b"other");
    let mut writer = store
        .begin_put(&scope, 1, &wrong)
        .unwrap_or_else(|e| panic!("begin_put: {e}"));
    writer.write_all(b"actual").unwrap_or_else(|e| panic!("write: {e}"));
    match writer.finish() {
        Err(StorageError::Corrupt(_)) => {}
        other => panic!("digest mismatch at finish: expected Corrupt, got {other:?}"),
    }
    assert!(matches!(
        store.open_read(&scope, 1),
        Err(StorageError::NotFound(_))
    ));
}

/// begin_put into a sealed scope fails with AlreadySealed.
pub fn streaming_put_into_sealed_scope_is_already_sealed<S: ObjectStore>(store: &mut S) {
    let scope = ScopePath::root(53);
    let payload = bytes("sealed-stream");
    assert!(store.put(&scope, 1, &payload, &sha256(&payload)).is_ok());
    assert!(store.seal_scope(&scope, &canonical_digest(&[(1, "sealed-stream")]), 1).is_ok());
    match store.begin_put(&scope, 2, &sha256(b"anything")) {
        Err(StorageError::AlreadySealed(_)) => {}
        Ok(_) => panic!("begin_put into sealed scope must fail"),
        Err(error) => panic!("expected AlreadySealed, got {error:?}"),
    }
}

/// begin_put against an existing entity with a different expected digest
/// fails with Corrupt before any byte is written.
pub fn streaming_put_conflict_with_existing_is_corrupt_at_begin<S: ObjectStore>(store: &mut S) {
    let scope = ScopePath::root(54);
    let payload = bytes("existing");
    assert!(store.put(&scope, 1, &payload, &sha256(&payload)).is_ok());
    match store.begin_put(&scope, 1, &sha256(b"different")) {
        Err(StorageError::Corrupt(_)) => {}
        Ok(_) => panic!("begin_put with conflicting digest must fail"),
        Err(error) => panic!("expected Corrupt, got {error:?}"),
    }
    assert_eq!(read_all(store, &scope, 1), payload);
}

/// Re-putting the same digest through a stream is an absorbing no-op.
pub fn streaming_put_idempotent_absorbing_reput<S: ObjectStore>(store: &mut S) {
    use std::io::Write as _;
    let scope = ScopePath::root(55);
    let payload = bytes("first");
    assert!(store.put(&scope, 1, &payload, &sha256(&payload)).is_ok());
    let writer = store
        .begin_put(&scope, 1, &sha256(&payload))
        .unwrap_or_else(|e| panic!("begin_put: {e}"));
    // The absorbing writer accepts the bytes (they are verified by digest,
    // not stored twice).
    let mut writer = writer;
    writer.write_all(&payload).unwrap_or_else(|e| panic!("write: {e}"));
    writer.finish().unwrap_or_else(|e| panic!("finish: {e}"));
    assert_eq!(
        store.list(&scope).unwrap_or_else(|e| panic!("list: {e}")),
        vec![1]
    );
    assert_eq!(read_all(store, &scope, 1), payload);
}

/// An unfinished stream is invisible to reads and listings.
pub fn streaming_put_invisible_until_finish<S: ObjectStore>(store: &mut S) {
    use std::io::Write as _;
    let scope = ScopePath::root(56);
    let payload = bytes("pending");
    let mut writer = store
        .begin_put(&scope, 1, &sha256(&payload))
        .unwrap_or_else(|e| panic!("begin_put: {e}"));
    writer.write_all(b"pend").unwrap_or_else(|e| panic!("write: {e}"));
    assert!(matches!(
        store.open_read(&scope, 1),
        Err(StorageError::NotFound(_))
    ));
    assert!(
        store
            .list(&scope)
            .unwrap_or_else(|e| panic!("list: {e}"))
            .is_empty()
    );
    writer.write_all(b"ing").unwrap_or_else(|e| panic!("write: {e}"));
    writer.finish().unwrap_or_else(|e| panic!("finish: {e}"));
    assert_eq!(read_all(store, &scope, 1), payload);
}

/// An empty store enumerates no scopes.
pub fn list_scopes_empty_store<S: ObjectStore>(store: &mut S) {
    assert!(
        store
            .list_scopes()
            .unwrap_or_else(|e| panic!("list_scopes: {e}"))
            .is_empty()
    );
}

/// Every scope with entities appears once, in canonical ascending rendered
/// order; scopes that only contain child scopes do not appear.
pub fn list_scopes_nested_and_canonically_ordered<S: ObjectStore>(store: &mut S) {
    let rendered = |scopes: Vec<ScopePath>| -> Vec<String> {
        scopes.iter().map(|s| s.to_path_string()).collect()
    };
    let root = ScopePath::root(0);
    let child = scope(0, 8);
    let grandchild = child.child(9).unwrap_or_else(|e| panic!("scope: {e}"));
    let sibling = ScopePath::root(2);
    let payload = bytes("x");
    let digest = sha256(&payload);
    // Write out of order and with a gap (sibling > child).
    assert!(store.put(&grandchild, 1, &payload, &digest).is_ok());
    assert!(store.put(&sibling, 1, &payload, &digest).is_ok());
    assert!(store.put(&child, 1, &payload, &digest).is_ok());
    assert!(store.put(&root, 1, &payload, &digest).is_ok());
    assert_eq!(
        rendered(store.list_scopes().unwrap_or_else(|e| panic!("list_scopes: {e}"))),
        vec![
            "00000000",
            "00000000/00000008",
            "00000000/00000008/00000009",
            "00000002"
        ]
    );
}

/// A sealed-but-empty scope appears; an unsealed scope retired empty
/// disappears; a sealed scope retired whole disappears.
pub fn list_scopes_visibility_across_seal_and_retire<S: ObjectStore>(store: &mut S) {
    let rendered = |scopes: &[ScopePath]| -> Vec<String> {
        scopes.iter().map(|s| s.to_path_string()).collect()
    };
    let seal_only = ScopePath::root(60);
    assert!(store.seal_scope(&seal_only, &canonical_digest(&[]), 0).is_ok());
    let payload = bytes("y");
    let digest = sha256(&payload);
    let emptied = ScopePath::root(61);
    assert!(store.put(&emptied, 1, &payload, &digest).is_ok());
    let retired_sealed = ScopePath::root(62);
    assert!(store.put(&retired_sealed, 1, &payload, &digest).is_ok());
    assert!(store.seal_scope(&retired_sealed, &canonical_digest(&[(1, "y")]), 1).is_ok());
    let survivors = store.list_scopes().unwrap_or_else(|e| panic!("list_scopes: {e}"));
    assert_eq!(
        rendered(&survivors),
        vec!["0000003c", "0000003d", "0000003e"],
        "seal-only and populated scopes are visible"
    );
    // Retire the unsealed scope empty: it disappears.
    assert_eq!(store.retire(&emptied, 1).unwrap_or_else(|e| panic!("retire: {e}")), 1);
    // Retire the sealed scope whole: it disappears.
    assert_eq!(
        store.retire(&retired_sealed, 0).unwrap_or_else(|e| panic!("retire: {e}")),
        1
    );
    assert_eq!(
        rendered(&store.list_scopes().unwrap_or_else(|e| panic!("list_scopes: {e}"))),
        vec!["0000003c"]
    );
}

/// Medium payloads through the streaming writer: the 6 MiB and 8 MiB sizes
/// that straddle the S3 multipart part boundary (the stashed first part
/// without an upload started), plus exactly-5-MiB. Every backend must
/// round-trip them; the pre-fix S3 backend panicked on this window.
pub fn streaming_put_medium_payload_boundaries<S: ObjectStore>(store: &mut S) {
    use std::io::Write as _;
    let scope = ScopePath::root(57);
    for (entity, mib) in [(1u32, 5usize), (2, 6), (3, 8)] {
        let total = mib << 20;
        let payload: Vec<u8> = (0..total).map(|i| ((i / 4096) % 251) as u8).collect();
        let digest = sha256(&payload);
        {
            let mut writer = store
                .begin_put(&scope, entity, &digest)
                .unwrap_or_else(|e| panic!("begin_put {mib} MiB: {e}"));
            for chunk in payload.chunks(256 * 1024) {
                writer.write_all(chunk).unwrap_or_else(|e| panic!("write {mib} MiB: {e}"));
            }
            writer.finish().unwrap_or_else(|e| panic!("finish {mib} MiB: {e}"));
        }
        assert_eq!(read_all(store, &scope, entity), payload, "{mib} MiB round-trip");
    }
}
