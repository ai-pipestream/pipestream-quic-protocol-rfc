//! The SPI contract: the [`ObjectStore`] and [`Journal`] traits.
//!
//! Semantics here are language-neutral and shared with the Java reference
//! implementation; see `docs/standards/storage-spi-design.md`. Two roles are
//! modeled separately because backends map to them differently: a filesystem
//! or a database implements both, an object store implements only
//! [`ObjectStore`], a log implements only [`Journal`].
//!
//! Durability rule for every backend: an operation returns success only when
//! the record survives a process kill. Each backend documents its
//! flush/fsync/ack configuration on its type.

use crate::{Digest32, EntityId, ScopePath, StorageError};
use std::io::Read;

/// A position in a [`Journal`]. Cursors are monotonically increasing in
/// append order; the cursor returned by an append or carried by a
/// [`JournalEntry`] identifies all entries up to and including that entry.
///
/// `Cursor(0)` is the beginning of the journal: replaying from it returns
/// every entry.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Cursor(pub u64);

/// A scope's seal record, stored in the scope's container at seal time.
///
/// `digest` is the scope's SCOPE_DIGEST (Section 9.5), supplied by the
/// protocol layer; the store treats it as opaque and never recomputes it
/// from statuses it does not hold. `entity_count` is the number of entities
/// the scope held when sealed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealRecord {
    /// The scope's SCOPE_DIGEST as recorded at seal time.
    pub digest: Digest32,
    /// The entity count recorded at seal time.
    pub entity_count: u64,
}

/// One replayed journal entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalEntry {
    /// Position of this entry; pass it to [`Journal::replay`] to resume
    /// after it.
    pub cursor: Cursor,
    /// The entry payload.
    pub kind: JournalKind,
}

/// The payload of a replayed journal entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JournalKind {
    /// An intent record: admitted-but-uncommitted work.
    Intent(Vec<u8>),
    /// A commit record marking an earlier intent committed.
    Commit {
        /// Cursor of the intent this commit settles.
        intent: Cursor,
        /// The commit record payload.
        record: Vec<u8>,
    },
}

/// Sealed, immutable entities and scope metadata.
///
/// Objects are write-once and content-verified: [`put`](ObjectStore::put)
/// checks the payload against its expected digest before storing and treats
/// a re-put of identical content as a no-op. After a scope seals, its
/// objects never change and `put` into it fails with
/// [`StorageError::AlreadySealed`].
///
/// `&mut self` methods mutate; `&self` methods read. Implementations are not
/// required to be thread-safe; callers serialize access.
pub trait ObjectStore {
    /// A streaming reader over a stored object's bytes.
    ///
    /// A live reader holds the object: retiring the object while a reader is
    /// open does not invalidate the reader.
    type Reader: Read;

    /// Stores `bytes` for `entity` in `scope`, verifying
    /// `SHA-256(bytes) == expected_digest` first.
    ///
    /// Idempotent: re-putting identical bytes with the same digest succeeds
    /// as a no-op. Fails with:
    ///
    /// - [`StorageError::Corrupt`] when `expected_digest` does not match the
    ///   payload, or when the entity already exists with different content;
    /// - [`StorageError::AlreadySealed`] when `scope` is sealed;
    /// - [`StorageError::Backend`] when `entity` exceeds the protocol id
    ///   bound.
    fn put(
        &mut self,
        scope: &ScopePath,
        entity: EntityId,
        bytes: &[u8],
        expected_digest: &Digest32,
    ) -> Result<(), StorageError>;

    /// Opens a streaming read of `entity` in `scope`.
    ///
    /// The returned reader yields exactly the stored payload bytes. Removal
    /// of the object while the reader is open does not invalidate the
    /// reader. Fails with [`StorageError::NotFound`] when the object does
    /// not exist.
    fn open_read(
        &self,
        scope: &ScopePath,
        entity: EntityId,
    ) -> Result<Self::Reader, StorageError>;

    /// Atomically marks `scope` sealed and records its SCOPE_DIGEST and
    /// entity count.
    ///
    /// Idempotent under identical arguments: re-sealing with the same digest
    /// and count succeeds as a no-op. Re-sealing with a different digest or
    /// count fails with [`StorageError::Corrupt`]. After sealing,
    /// [`put`](ObjectStore::put) into the scope fails with
    /// [`StorageError::AlreadySealed`].
    fn seal_scope(
        &mut self,
        scope: &ScopePath,
        digest: &Digest32,
        entity_count: u64,
    ) -> Result<(), StorageError>;

    /// Recomputes the scope's stored state and compares it against the seal
    /// record.
    ///
    /// The store-level recompute covers what the store holds: every object's
    /// payload is rehashed against the digest recorded at
    /// [`put`](ObjectStore::put) time, and the live entity count is compared
    /// against the sealed count. Any disagreement fails with
    /// [`StorageError::Corrupt`]. On success the recorded [`SealRecord`] is
    /// returned so the protocol layer can compare the SCOPE_DIGEST against
    /// its own expectation — the store cannot recompute the SCOPE_DIGEST
    /// itself because it does not hold entity statuses.
    ///
    /// Fails with [`StorageError::NotFound`] when the scope has no seal
    /// record (unknown or unsealed scope).
    fn verify_scope(&self, scope: &ScopePath) -> Result<SealRecord, StorageError>;

    /// Lists the entity ids stored in `scope`, sorted ascending.
    ///
    /// Used for audit and orphan reconciliation. A scope with no entities —
    /// including a scope that has never been written — yields an empty
    /// vector.
    fn list(&self, scope: &ScopePath) -> Result<Vec<EntityId>, StorageError>;

    /// Removes entities past the retention point and returns how many were
    /// removed.
    ///
    /// The retention cursor is an Entity ID bound: entities with
    /// `id <= retention_cursor` are removed (mirroring the protocol's
    /// cursor-based id recycling, Section 9.1). A sealed scope retires
    /// whole: every entity and the seal record are removed regardless of the
    /// cursor. Retiring an unknown scope succeeds and removes nothing.
    fn retire(&mut self, scope: &ScopePath, retention_cursor: u64) -> Result<u64, StorageError>;
}

/// The intent/commit record used for crash recovery.
///
/// Append-only and ordered. [`replay`](Journal::replay) from a cursor is the
/// recovery path's only read: it returns every entry after the cursor in
/// append order, letting a restarted endpoint distinguish committed work
/// from admitted-but-uncommitted work.
///
/// In the two-system configuration (object store and journal on different
/// systems) the rule is: object write first, journal
/// [`commit`](Journal::commit) second, reconcile on recovery by replaying
/// the journal and treating objects without a commit record as uncommitted.
pub trait Journal {
    /// Durably records admitted-but-uncommitted work and returns the
    /// entry's cursor.
    ///
    /// Record payloads are opaque to the journal; interpretation belongs to
    /// the recovery layer.
    fn append_intent(&mut self, record: &[u8]) -> Result<Cursor, StorageError>;

    /// Marks the intent at `intent` committed, recording `record`.
    ///
    /// Idempotent under identical arguments: re-committing the same intent
    /// with the same record succeeds as a no-op; a conflicting commit record
    /// for an already-committed intent fails with
    /// [`StorageError::Corrupt`]. Fails with [`StorageError::NotFound`] when
    /// no intent exists at `intent`.
    fn commit(&mut self, intent: Cursor, record: &[u8]) -> Result<(), StorageError>;

    /// Replays every entry after `since`, in append order.
    ///
    /// Each returned entry's cursor identifies the position up to and
    /// including that entry, so resuming is `replay(last_seen_cursor)`.
    /// `replay(Cursor(0))` replays the whole journal.
    fn replay(&self, since: Cursor) -> Result<Vec<JournalEntry>, StorageError>;
}
