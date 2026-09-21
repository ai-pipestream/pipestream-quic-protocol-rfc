//! The SPI contract: the [`ObjectStore`] and [`Journal`] traits.
//!
//! Semantics are pinned by `docs/standards/storage-spi-design.md` and match
//! the Java reference implementation (`ai.pipestream.quic.storage`)
//! exactly. Two roles are modeled separately because backends map to them
//! differently: a filesystem or a database implements both, an object store
//! implements only [`ObjectStore`], a log implements only [`Journal`].
//!
//! Durability rule for every backend: an operation returns success only
//! when the record survives a process kill. Each backend documents its
//! flush/fsync/ack configuration on its type.

use crate::{Digest32, EntityId, ScopePath, StorageError};
use std::io::Read;

/// Largest retained intent identifier, in UTF-8 octets.
pub const MAX_INTENT_ID_BYTES: usize = 256;

/// Largest retained intent or commit record payload, in octets.
pub const MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;

/// A position in a [`Journal`]. Cursors are strictly increasing from 1 in
/// the journal's global append order; intents *and commits* each consume a
/// cursor, so an intent's cursor need not be contiguous with its
/// neighbors'. `Cursor(0)` is the beginning: replaying from it returns
/// every intent.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Cursor(pub u64);

/// A scope's seal record, stored in the scope's container at seal time.
///
/// `digest` is the scope's SCOPE_DIGEST as supplied by the caller; the
/// store records it verbatim and cannot recompute it (the protocol digest,
/// Section 9.5, covers entity statuses the store does not hold). A caller
/// without statuses may use the canonical content digest
/// ([`crate::layout::scope_digest`]) as the seal digest. `entity_count` is the
/// number of direct entities the seal records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealRecord {
    /// The scope digest as recorded at seal time.
    pub digest: Digest32,
    /// The entity count recorded at seal time.
    pub entity_count: u64,
}

/// One immutable admitted-but-uncommitted work record for [`Journal`].
///
/// The intent id is caller-assigned and unique per journal; it is what
/// makes post-crash retries idempotent. The record payload is opaque to the
/// journal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Intent {
    id: String,
    record: Vec<u8>,
}

impl Intent {
    /// Validates an intent: id of 1 to [`MAX_INTENT_ID_BYTES`] UTF-8
    /// octets, record of 1 to [`MAX_RECORD_BYTES`] octets.
    pub fn new(id: impl Into<String>, record: impl Into<Vec<u8>>) -> Result<Self, StorageError> {
        let id = id.into();
        let record = record.into();
        if id.is_empty() || id.len() > MAX_INTENT_ID_BYTES {
            return Err(StorageError::failed(format!(
                "intent id must be 1 to {MAX_INTENT_ID_BYTES} UTF-8 octets"
            )));
        }
        if record.is_empty() || record.len() > MAX_RECORD_BYTES {
            return Err(StorageError::failed(format!(
                "intent record must be 1 to {MAX_RECORD_BYTES} octets"
            )));
        }
        Ok(Intent { id, record })
    }

    /// The caller-assigned intent identifier.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The opaque record payload.
    pub fn record(&self) -> &[u8] {
        &self.record
    }
}

/// One replayed intent with its commit state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayEntry {
    /// The intent's append cursor.
    pub cursor: Cursor,
    /// The intent as appended.
    pub intent: Intent,
    /// The commit bytes when committed, `None` while uncommitted.
    pub commit: Option<Vec<u8>>,
    /// The cursor the commit consumed in the global append order; `None`
    /// while uncommitted. Lets replay consumers order commit records among
    /// intents without a second read.
    pub commit_cursor: Option<Cursor>,
}

impl ReplayEntry {
    /// Whether the intent has a durable commit record.
    pub fn committed(&self) -> bool {
        self.commit.is_some()
    }
}

/// A streaming write to a not-yet-visible object, returned by
/// [`ObjectStore::begin_put`].
///
/// The object is never observable before [`finish`](PutWriter::finish):
/// `open_read` and `list` do not see it, and a kill mid-stream leaves only
/// backend-local scratch (swept on the next open, like any scratch).
/// Dropping without finishing aborts the write.
pub trait PutWriter: std::io::Write {
    /// Verifies the accumulated content against the expected digest given
    /// to [`ObjectStore::begin_put`], makes the object durable (the same
    /// durability rule as [`ObjectStore::put`]: success means it survives a
    /// process kill), and publishes it.
    ///
    /// Fails with [`StorageError::Corrupt`] when the written bytes do not
    /// match the expected digest, and with [`StorageError::AlreadySealed`]
    /// when the scope sealed after the write began. On failure nothing is
    /// visible.
    fn finish(self) -> Result<(), StorageError>;
}

/// Sealed, immutable entities and scope metadata.
///
/// Objects are write-once and content-verified: [`put`](ObjectStore::put)
/// checks the payload against its expected digest before storing and treats
/// a re-put of identical content as a no-op. After a scope seals, its
/// objects never change and `put` into it fails with
/// [`StorageError::AlreadySealed`].
///
/// `&mut self` methods mutate; `&self` methods read. Thread safety is part
/// of the contract: implementations must be `Send` and safe to share behind
/// caller-side synchronization (e.g. `Mutex`), behaving as if calls were
/// serialized; the contract suite exercises concurrent puts and a
/// put-vs-seal race on every object backend.
pub trait ObjectStore {
    /// A streaming reader over a stored object's bytes.
    ///
    /// A live reader holds the object: removing the object afterwards, for
    /// example through [`retire`](ObjectStore::retire), never invalidates a
    /// read opened before the removal.
    type Reader: Read;

    /// A streaming writer for [`begin_put`](ObjectStore::begin_put).
    type PutWriter: PutWriter;

    /// Begins a streaming put of `entity` in `scope` with
    /// `expected_digest` — the bounded-memory counterpart of
    /// [`put`](ObjectStore::put) for writers that receive content in
    /// chunks.
    ///
    /// The same checks as `put` apply up front: a sealed scope fails with
    /// [`StorageError::AlreadySealed`]; an existing entity with a different
    /// recorded digest fails with [`StorageError::Corrupt`]; an existing
    /// entity with the same digest returns an absorbing writer whose
    /// finish is a no-op success (idempotent re-put). Content is verified
    /// against `expected_digest` at finish; a mismatched stream stores
    /// nothing.
    fn begin_put(
        &mut self,
        scope: &ScopePath,
        entity: EntityId,
        expected_digest: &Digest32,
    ) -> Result<Self::PutWriter, StorageError>;

    /// Stores `bytes` for `entity` in `scope`.
    ///
    /// Checked in this order (pinned): a sealed scope fails with
    /// [`StorageError::AlreadySealed`] regardless of bytes; then
    /// `SHA-256(bytes) != expected_digest` fails with
    /// [`StorageError::Corrupt`]; then an existing entity with different
    /// content fails with [`StorageError::Corrupt`]. Re-putting identical
    /// bytes with the same digest succeeds as a no-op.
    ///
    /// [`StorageError::Failed`] when `entity` exceeds the protocol entity-id bound.
    fn put(
        &mut self,
        scope: &ScopePath,
        entity: EntityId,
        bytes: &[u8],
        expected_digest: &Digest32,
    ) -> Result<(), StorageError>;

    /// Opens a streaming read of `entity` in `scope`, positioned at the
    /// first payload octet.
    ///
    /// The returned reader yields exactly the stored payload bytes.
    /// Removal of the object while the reader is open does not invalidate
    /// the reader. Fails with [`StorageError::NotFound`] when the object
    /// does not exist.
    fn open_read(&self, scope: &ScopePath, entity: EntityId)
    -> Result<Self::Reader, StorageError>;

    /// Atomically marks `scope` sealed, recording `digest` and
    /// `entity_count` verbatim in the scope's own container.
    ///
    /// Idempotent: an identical re-seal succeeds as a no-op; a conflicting
    /// digest or count fails with [`StorageError::Corrupt`]. After sealing,
    /// [`put`](ObjectStore::put) into the scope fails with
    /// [`StorageError::AlreadySealed`] and [`retire`](ObjectStore::retire)
    /// removes the whole scope.
    fn seal_scope(
        &mut self,
        scope: &ScopePath,
        digest: &Digest32,
        entity_count: u64,
    ) -> Result<(), StorageError>;

    /// Verifies one scope against what the store owns: every object's
    /// payload is rehashed against the digest recorded at
    /// [`put`](ObjectStore::put) time, and the live entity count is
    /// compared against the sealed count. Any mismatch fails with
    /// [`StorageError::Corrupt`].
    ///
    /// The store does not recompute the recorded SCOPE_DIGEST — it covers
    /// protocol statuses the store does not hold. On success the recorded
    /// [`SealRecord`] is returned so the protocol layer can compare the
    /// digest against its own expectation.
    ///
    /// Fails with [`StorageError::NotFound`] when the scope was never
    /// sealed.
    fn verify_scope(&self, scope: &ScopePath) -> Result<SealRecord, StorageError>;

    /// Lists the direct entity ids of `scope`, sorted ascending, for audit
    /// and orphan reconciliation. A scope holding no entities — including
    /// one never written — yields an empty vector.
    fn list(&self, scope: &ScopePath) -> Result<Vec<EntityId>, StorageError>;

    /// Lists every scope path holding any state — entities or a seal
    /// record — in canonical ascending order (ascending rendered path).
    ///
    /// Recovery sweeps and audit enumerate scopes through this, never a
    /// backend-specific structure. A scope that held entities but was
    /// retired empty (no entities, no seal) no longer appears; a sealed
    /// scope appears until it retires whole.
    fn list_scopes(&self) -> Result<Vec<ScopePath>, StorageError>;

    /// Removes retained content and returns how many entities were removed.
    ///
    /// A sealed scope retires whole: every entity and the seal record go
    /// away. An unsealed scope retires only the entities at or below the
    /// retention cursor (`id <= retention_cursor`). Fails with
    /// [`StorageError::NotFound`] when the scope holds no entities and no
    /// seal record. Entities removed under a previously opened read do not
    /// invalidate that read.
    fn retire(&mut self, scope: &ScopePath, retention_cursor: u64) -> Result<u64, StorageError>;
}

/// The durable intent/commit log used for crash recovery.
///
/// Appends are ordered and replayable from a cursor; replay is the recovery
/// path's only read and distinguishes committed work from
/// admitted-but-uncommitted work. In the two-system configuration (object
/// store and journal on different systems) the rule is: object write first,
/// journal [`commit`](Journal::commit) second, reconcile on recovery by
/// replaying the journal and treating objects without a commit record as
/// uncommitted.
pub trait Journal {
    /// Durably records admitted-but-uncommitted work and returns its
    /// assigned cursor.
    ///
    /// Idempotent per intent id: appending an intent whose id is already
    /// recorded with identical bytes succeeds as a no-op and returns the
    /// original cursor; the same id with different bytes fails with
    /// [`StorageError::Corrupt`]. This makes post-crash retries safe.
    fn append_intent(&mut self, intent: &Intent) -> Result<Cursor, StorageError>;

    /// Marks one intent committed, durably recording the commit bytes after
    /// the intent (and, in a two-system deployment, after the object
    /// write). The commit consumes a cursor in the global append order.
    ///
    /// Idempotent: committing an already-committed intent with identical
    /// bytes succeeds as a no-op; different bytes fail with
    /// [`StorageError::Corrupt`]. Fails with [`StorageError::NotFound`]
    /// when no intent carries the id.
    fn commit(&mut self, intent_id: &str, record: &[u8]) -> Result<(), StorageError>;

    /// Replays every intent whose append cursor is greater than `since`, in
    /// append order, each with its current commit state folded in. Passing
    /// the last seen intent cursor replays exactly the new intents; passing
    /// `Cursor(0)` replays the whole journal.
    fn replay(&self, since: Cursor) -> Result<Vec<ReplayEntry>, StorageError>;
}

/// A buffering [`PutWriter`] for backends whose native write API takes
/// whole payloads (Postgres `bytea`, MongoDB documents, a single S3 PUT).
/// Content is bounded by the backend's own record limits; the digest is
/// verified before the closure runs, so a mismatched stream never reaches
/// the backend. Backends with a true streaming primitive (the filesystem's
/// scratch file) do not use this.
///
/// The closure receives the verified bytes and performs the backend write.
#[derive(Debug)]
pub struct BufferedPutWriter<F: FnOnce(&[u8]) -> Result<(), StorageError>> {
    buffer: Vec<u8>,
    expected: Digest32,
    finish: Option<F>,
}

impl<F: FnOnce(&[u8]) -> Result<(), StorageError>> BufferedPutWriter<F> {
    /// Creates a buffering writer; `finish` runs at [`PutWriter::finish`]
    /// after the digest check passes.
    pub fn new(expected: Digest32, finish: F) -> Self {
        BufferedPutWriter {
            buffer: Vec::new(),
            expected,
            finish: Some(finish),
        }
    }
}

impl<F: FnOnce(&[u8]) -> Result<(), StorageError>> std::io::Write for BufferedPutWriter<F> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<F: FnOnce(&[u8]) -> Result<(), StorageError>> PutWriter for BufferedPutWriter<F> {
    fn finish(mut self) -> Result<(), StorageError> {
        if crate::layout::sha256(&self.buffer) != self.expected {
            return Err(StorageError::corrupt(
                "streamed content does not match the expected digest",
            ));
        }
        let finish = match self.finish.take() {
            Some(finish) => finish,
            None => return Err(StorageError::failed("put writer is closed")),
        };
        finish(&self.buffer)
    }
}
