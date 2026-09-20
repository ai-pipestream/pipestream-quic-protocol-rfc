//! PostgreSQL backend implementing both SPI roles.
//!
//! Physical mapping of the shared logical layout:
//!
//! - Scope path → `TEXT` column holding the `/`-joined hex components
//!   ([`ScopePath::to_path_string`]). The fanout bucket is derivable from
//!   the entity id and is not stored.
//! - Objects → `pipestream_objects(scope_path, entity_id, digest, payload)`:
//!   metadata and payload (`bytea`) in one row. The digest recorded at
//!   [`put`](ObjectStore::put) time is stored alongside so
//!   [`verify_scope`](ObjectStore::verify_scope) can rehash and compare.
//! - Seal records → `pipestream_seals(scope_path, digest, entity_count)`.
//! - Journal → `pipestream_journal(seq, kind, intent_id, payload)` with
//!   `seq` an identity column; a record's cursor is its `seq`. Commits are
//!   appended as tail records and consume cursors in the global order,
//!   exactly like the log-based backends; replay folds each commit into its
//!   intent's entry.
//!
//! Durability configuration: every mutating call runs as a single
//! transaction and returns only after the commit succeeds; with the stock
//! `synchronous_commit = on`, commit success means the WAL is flushed and
//! the record survives a process kill. Multi-statement operations (`put`,
//! `seal_scope`, `commit`) use one transaction so the check-and-write is
//! atomic.
//!
//! Open-read survival: [`open_read`](ObjectStore::open_read) materializes
//! the payload into memory at open time, so retiring the row afterwards
//! cannot invalidate the reader. (Lazy streaming via portals would not
//! survive transaction boundaries in the blocking client; the snapshot is
//! the correct semantic here.)
//!
//! The blocking `postgres` client requires `&mut` access; the client lives
//! behind a mutex so the SPI's `&self` read methods work. Callers serialize
//! access as with every backend.

use crate::layout;
use crate::{
    Cursor, Digest32, EntityId, Intent, Journal, MAX_RECORD_BYTES, ObjectStore, ReplayEntry,
    ScopePath, SealRecord, StorageError,
};
use postgres::{Client, GenericClient, NoTls};
use std::io::{self, Read};
use std::sync::{Mutex, MutexGuard};

const KIND_INTENT: i16 = 0;
const KIND_COMMIT: i16 = 1;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS pipestream_objects (
    scope_path TEXT NOT NULL,
    entity_id BIGINT NOT NULL,
    digest BYTEA NOT NULL CHECK (length(digest) = 32),
    payload BYTEA NOT NULL,
    PRIMARY KEY (scope_path, entity_id)
);
CREATE TABLE IF NOT EXISTS pipestream_seals (
    scope_path TEXT PRIMARY KEY,
    digest BYTEA NOT NULL CHECK (length(digest) = 32),
    entity_count BIGINT NOT NULL CHECK (entity_count >= 0)
);
CREATE TABLE IF NOT EXISTS pipestream_journal (
    seq BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind SMALLINT NOT NULL,
    intent_id TEXT NOT NULL,
    payload BYTEA NOT NULL
);
";

/// PostgreSQL backend for both the [`ObjectStore`] and [`Journal`] roles.
///
/// See the [module documentation](self) for the schema and the durability
/// configuration.
pub struct PostgresStore {
    client: Mutex<Client>,
}

impl std::fmt::Debug for PostgresStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresStore").finish_non_exhaustive()
    }
}

impl PostgresStore {
    /// Connects to `url` (e.g. `postgres://user:pass@host:port/db`) without
    /// TLS and creates the SPI tables when absent.
    pub fn open(url: &str) -> Result<Self, StorageError> {
        let mut client = Client::connect(url, NoTls).map_err(map_pg_error)?;
        client.batch_execute(SCHEMA).map_err(map_pg_error)?;
        Ok(PostgresStore {
            client: Mutex::new(client),
        })
    }

    fn lock(&self) -> Result<MutexGuard<'_, Client>, StorageError> {
        self.client
            .lock()
            .map_err(|_| StorageError::failed("postgres store lock poisoned"))
    }
}

/// PostgreSQL backend reader: a snapshot of the payload taken at open time.
#[derive(Debug)]
pub struct PostgresReader {
    bytes: io::Cursor<Vec<u8>>,
}

impl Read for PostgresReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.bytes.read(buf)
    }
}

/// Maps a postgres client error into the taxonomy: a closed connection is
/// retryable; everything else is a general failure.
fn map_pg_error(error: postgres::Error) -> StorageError {
    if error.is_closed() {
        return StorageError::transient(format!("postgres: {error}"));
    }
    StorageError::failed(format!("postgres: {error}"))
}

fn path_key(scope: &ScopePath) -> String {
    scope.to_path_string()
}

fn entity_to_row(entity: EntityId) -> i64 {
    // Entity ids are validated as unsigned 63-bit before any SQL runs.
    entity as i64
}

fn entity_from_row(value: i64, scope: &ScopePath) -> Result<EntityId, StorageError> {
    if value < 0 || value > u32::MAX as i64 {
        return Err(StorageError::corrupt(format!(
            "entity id {value} stored for {scope} is out of range"
        )));
    }
    Ok(value as u32)
}

/// Fails with [`StorageError::AlreadySealed`] when the scope has a seal row.
fn require_unsealed(client: &mut impl GenericClient, scope: &ScopePath) -> Result<(), StorageError> {
    let sealed: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pipestream_seals WHERE scope_path = $1)",
            &[&path_key(scope)],
        )
        .map_err(map_pg_error)?
        .get(0);
    if sealed {
        return Err(StorageError::already_sealed(format!(
            "scope {scope} is sealed"
        )));
    }
    Ok(())
}

/// Reads the seal row for `scope`, if present.
fn read_seal(
    client: &mut impl GenericClient,
    scope: &ScopePath,
) -> Result<Option<SealRecord>, StorageError> {
    let row = client
        .query_opt(
            "SELECT digest, entity_count FROM pipestream_seals WHERE scope_path = $1",
            &[&path_key(scope)],
        )
        .map_err(map_pg_error)?;
    match row {
        Some(row) => {
            let digest: Vec<u8> = row.get(0);
            let entity_count: i64 = row.get(1);
            if entity_count < 0 {
                return Err(StorageError::corrupt(format!(
                    "negative entity count stored for {scope}"
                )));
            }
            let digest: Digest32 = digest.as_slice().try_into().map_err(|_| {
                StorageError::corrupt(format!("malformed digest stored for {scope}"))
            })?;
            Ok(Some(SealRecord {
                digest,
                entity_count: entity_count as u64,
            }))
        }
        None => Ok(None),
    }
}

impl ObjectStore for PostgresStore {
    type Reader = PostgresReader;

    fn put(
        &mut self,
        scope: &ScopePath,
        entity: EntityId,
        bytes: &[u8],
        expected_digest: &Digest32,
    ) -> Result<(), StorageError> {
        layout::check_entity_id(entity)?;
        let mut client = self.lock()?;
        let mut tx = client.transaction().map_err(map_pg_error)?;
        // Seal is checked before the digest: a sealed scope rejects every
        // put regardless of bytes.
        require_unsealed(&mut tx, scope)?;
        let digest = layout::sha256(bytes);
        if &digest != expected_digest {
            return Err(StorageError::corrupt(format!(
                "digest mismatch for entity {entity} in {scope}"
            )));
        }
        let existing: Option<(Vec<u8>, Vec<u8>)> = tx
            .query_opt(
                "SELECT digest, payload FROM pipestream_objects \
                 WHERE scope_path = $1 AND entity_id = $2",
                &[&path_key(scope), &entity_to_row(entity)],
            )
            .map_err(map_pg_error)?
            .map(|row| (row.get(0), row.get(1)));
        match existing {
            Some((_, payload)) if payload == bytes => {}
            Some(_) => {
                return Err(StorageError::corrupt(format!(
                    "conflicting bytes for entity {entity} in {scope}"
                )));
            }
            None => {
                tx.execute(
                    "INSERT INTO pipestream_objects (scope_path, entity_id, digest, payload) \
                     VALUES ($1, $2, $3, $4)",
                    &[
                        &path_key(scope),
                        &entity_to_row(entity),
                        &&digest[..],
                        &bytes,
                    ],
                )
                .map_err(map_pg_error)?;
            }
        }
        tx.commit().map_err(map_pg_error)
    }

    fn open_read(
        &self,
        scope: &ScopePath,
        entity: EntityId,
    ) -> Result<PostgresReader, StorageError> {
        let mut client = self.lock()?;
        let payload: Option<Vec<u8>> = client
            .query_opt(
                "SELECT payload FROM pipestream_objects \
                 WHERE scope_path = $1 AND entity_id = $2",
                &[&path_key(scope), &entity_to_row(entity)],
            )
            .map_err(map_pg_error)?
            .map(|row| row.get(0));
        match payload {
            Some(bytes) => Ok(PostgresReader {
                bytes: io::Cursor::new(bytes),
            }),
            None => Err(StorageError::not_found(format!(
                "entity {entity} is absent from scope {scope}"
            ))),
        }
    }

    fn seal_scope(
        &mut self,
        scope: &ScopePath,
        digest: &Digest32,
        entity_count: u64,
    ) -> Result<(), StorageError> {
        let count = i64::try_from(entity_count)
            .map_err(|_| StorageError::failed("entity count exceeds i64"))?;
        let mut client = self.lock()?;
        let mut tx = client.transaction().map_err(map_pg_error)?;
        match read_seal(&mut tx, scope)? {
            Some(existing)
                if existing.digest == *digest && existing.entity_count == entity_count => {}
            Some(_) => {
                return Err(StorageError::corrupt(format!(
                    "scope {scope} already carries a different seal"
                )));
            }
            None => {
                tx.execute(
                    "INSERT INTO pipestream_seals (scope_path, digest, entity_count) \
                     VALUES ($1, $2, $3)",
                    &[&path_key(scope), &&digest[..], &count],
                )
                .map_err(map_pg_error)?;
            }
        }
        tx.commit().map_err(map_pg_error)
    }

    fn verify_scope(&self, scope: &ScopePath) -> Result<SealRecord, StorageError> {
        let mut client = self.lock()?;
        let seal = read_seal(&mut *client, scope)?
            .ok_or_else(|| StorageError::not_found(format!("scope {scope} has no seal record")))?;
        let rows = client
            .query(
                "SELECT entity_id, digest, payload FROM pipestream_objects \
                 WHERE scope_path = $1 ORDER BY entity_id",
                &[&path_key(scope)],
            )
            .map_err(map_pg_error)?;
        for row in &rows {
            let entity = entity_from_row(row.get(0), scope)?;
            let recorded: Vec<u8> = row.get(1);
            let payload: &[u8] = row.get(2);
            if layout::sha256(payload) != recorded.as_slice() {
                return Err(StorageError::corrupt(format!(
                    "entity {entity} in {scope} fails its recorded digest"
                )));
            }
        }
        if rows.len() as u64 != seal.entity_count {
            return Err(StorageError::corrupt(format!(
                "scope {scope} entity count differs from its seal"
            )));
        }
        Ok(seal)
    }

    fn list(&self, scope: &ScopePath) -> Result<Vec<EntityId>, StorageError> {
        let mut client = self.lock()?;
        let rows = client
            .query(
                "SELECT entity_id FROM pipestream_objects WHERE scope_path = $1 \
                 ORDER BY entity_id",
                &[&path_key(scope)],
            )
            .map_err(map_pg_error)?;
        rows.iter()
            .map(|row| entity_from_row(row.get(0), scope))
            .collect()
    }

    fn retire(&mut self, scope: &ScopePath, retention_cursor: u64) -> Result<u64, StorageError> {
        let mut client = self.lock()?;
        if read_seal(&mut *client, scope)?.is_some() {
            // A sealed scope retires whole: every entity and the seal record.
            let mut tx = client.transaction().map_err(map_pg_error)?;
            let removed = tx
                .execute(
                    "DELETE FROM pipestream_objects WHERE scope_path = $1",
                    &[&path_key(scope)],
                )
                .map_err(map_pg_error)?;
            tx.execute(
                "DELETE FROM pipestream_seals WHERE scope_path = $1",
                &[&path_key(scope)],
            )
            .map_err(map_pg_error)?;
            tx.commit().map_err(map_pg_error)?;
            return Ok(removed);
        }
        let remaining: bool = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pipestream_objects WHERE scope_path = $1)",
                &[&path_key(scope)],
            )
            .map_err(map_pg_error)?
            .get(0);
        if !remaining {
            return Err(StorageError::not_found(format!(
                "scope {scope} holds nothing to retire"
            )));
        }
        let cursor = i64::try_from(retention_cursor)
            .map_err(|_| StorageError::failed("retention cursor exceeds i64"))?;
        let removed = client
            .execute(
                "DELETE FROM pipestream_objects WHERE scope_path = $1 AND entity_id <= $2",
                &[&path_key(scope), &cursor],
            )
            .map_err(map_pg_error)?;
        Ok(removed)
    }
}

impl Journal for PostgresStore {
    fn append_intent(&mut self, intent: &Intent) -> Result<Cursor, StorageError> {
        let mut client = self.lock()?;
        let mut tx = client.transaction().map_err(map_pg_error)?;
        let existing: Option<(i64, Vec<u8>)> = tx
            .query_opt(
                "SELECT seq, payload FROM pipestream_journal \
                 WHERE kind = $1 AND intent_id = $2",
                &[&KIND_INTENT, &intent.id()],
            )
            .map_err(map_pg_error)?
            .map(|row| (row.get(0), row.get(1)));
        if let Some((seq, payload)) = existing {
            if payload == intent.record() {
                return Ok(Cursor(seq as u64));
            }
            return Err(StorageError::corrupt(format!(
                "intent {} already exists with different bytes",
                intent.id()
            )));
        }
        let seq: i64 = tx
            .query_one(
                "INSERT INTO pipestream_journal (kind, intent_id, payload) \
                 VALUES ($1, $2, $3) RETURNING seq",
                &[&KIND_INTENT, &intent.id(), &intent.record()],
            )
            .map_err(map_pg_error)?
            .get(0);
        tx.commit().map_err(map_pg_error)?;
        Ok(Cursor(seq as u64))
    }

    fn commit(&mut self, intent_id: &str, record: &[u8]) -> Result<(), StorageError> {
        if record.is_empty() || record.len() > MAX_RECORD_BYTES {
            return Err(StorageError::failed(format!(
                "commit record must be 1 to {MAX_RECORD_BYTES} octets"
            )));
        }
        let mut client = self.lock()?;
        let mut tx = client.transaction().map_err(map_pg_error)?;
        let intent_present: bool = tx
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pipestream_journal \
                 WHERE kind = $1 AND intent_id = $2)",
                &[&KIND_INTENT, &intent_id],
            )
            .map_err(map_pg_error)?
            .get(0);
        if !intent_present {
            return Err(StorageError::not_found(format!(
                "intent {intent_id} has no durable append"
            )));
        }
        let prior: Option<Vec<u8>> = tx
            .query_opt(
                "SELECT payload FROM pipestream_journal \
                 WHERE kind = $1 AND intent_id = $2",
                &[&KIND_COMMIT, &intent_id],
            )
            .map_err(map_pg_error)?
            .map(|row| row.get(0));
        match prior {
            Some(existing) if existing == record => {}
            Some(_) => {
                return Err(StorageError::corrupt(format!(
                    "intent {intent_id} already carries a different commit"
                )));
            }
            None => {
                tx.execute(
                    "INSERT INTO pipestream_journal (kind, intent_id, payload) \
                     VALUES ($1, $2, $3)",
                    &[&KIND_COMMIT, &intent_id, &record],
                )
                .map_err(map_pg_error)?;
            }
        }
        tx.commit().map_err(map_pg_error)
    }

    fn replay(&self, since: Cursor) -> Result<Vec<ReplayEntry>, StorageError> {
        let since_seq = i64::try_from(since.0)
            .map_err(|_| StorageError::failed("cursor exceeds i64"))?;
        let mut client = self.lock()?;
        let rows = client
            .query(
                "SELECT i.seq, i.intent_id, i.payload, c.payload, c.seq \
                 FROM pipestream_journal i \
                 LEFT JOIN pipestream_journal c \
                   ON c.kind = $2 AND c.intent_id = i.intent_id \
                 WHERE i.kind = $3 AND i.seq > $1 \
                 ORDER BY i.seq",
                &[&since_seq, &KIND_COMMIT, &KIND_INTENT],
            )
            .map_err(map_pg_error)?;
        let mut entries = Vec::with_capacity(rows.len());
        for row in rows {
            let seq: i64 = row.get(0);
            if seq < 0 {
                return Err(StorageError::corrupt("negative journal sequence".to_string()));
            }
            let id: String = row.get(1);
            let record: Vec<u8> = row.get(2);
            let commit: Option<Vec<u8>> = row.get(3);
            let commit_seq: Option<i64> = row.get(4);
            let commit_cursor = match commit_seq {
                Some(seq) if seq > 0 => Some(Cursor(seq as u64)),
                Some(_) => {
                    return Err(StorageError::corrupt("negative journal sequence".to_string()));
                }
                None => None,
            };
            let intent = Intent::new(id, record).map_err(|_| {
                StorageError::corrupt("journal intent fails validation".to_string())
            })?;
            entries.push(ReplayEntry {
                cursor: Cursor(seq as u64),
                intent,
                commit,
                commit_cursor,
            });
        }
        Ok(entries)
    }
}
