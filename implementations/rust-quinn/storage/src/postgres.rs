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
    Cursor, Digest32, EntityId, Intent, Journal, MAX_RECORD_BYTES, ObjectStore, PutWriter,
    ReplayEntry, ScopePath, SealRecord, StorageError,
};
use postgres::{Client, GenericClient, NoTls};
use sha2::Digest as _;
use std::io::{self, Read, Write};
use std::sync::{Mutex, MutexGuard};

const KIND_INTENT: i16 = 0;
const KIND_COMMIT: i16 = 1;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS pipestream_objects (
    scope_path TEXT NOT NULL,
    entity_id BIGINT NOT NULL,
    digest BYTEA NOT NULL CHECK (length(digest) = 32),
    chunked BOOLEAN NOT NULL DEFAULT FALSE,
    payload BYTEA NOT NULL,
    PRIMARY KEY (scope_path, entity_id)
);
CREATE TABLE IF NOT EXISTS pipestream_object_chunks (
    scope_path TEXT NOT NULL,
    entity_id BIGINT NOT NULL,
    chunk_no BIGINT NOT NULL,
    payload BYTEA NOT NULL,
    PRIMARY KEY (scope_path, entity_id, chunk_no)
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
    // Option so Drop can take it: the sync postgres client blocks on its
    // internal runtime in Drop, which panics on a tokio driver thread, so the
    // final drop is handed to a plain thread.
    client: Option<std::sync::Arc<Mutex<Client>>>,
}

impl Drop for PostgresStore {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            let _ = std::thread::Builder::new()
                .name("postgres-client-drop".to_string())
                .spawn(move || drop(client));
        }
    }
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
            client: Some(std::sync::Arc::new(Mutex::new(client))),
        })
    }

    fn client_arc(&self) -> Result<std::sync::Arc<Mutex<Client>>, StorageError> {
        Ok(std::sync::Arc::clone(
            self.client
                .as_ref()
                .ok_or_else(|| StorageError::failed("postgres store is closed"))?,
        ))
    }

    fn lock(&self) -> Result<MutexGuard<'_, Client>, StorageError> {
        self.client
            .as_ref()
            .ok_or_else(|| StorageError::failed("postgres store is closed"))?
            .lock()
            .map_err(|_| StorageError::failed("postgres store lock poisoned"))
    }
}

/// PostgreSQL backend reader. Inline payloads snapshot at open; chunked
/// payloads stream chunk-by-chunk, so reads stay bounded in memory.
pub struct PostgresReader {
    inner: PostgresReaderInner,
}

impl std::fmt::Debug for PostgresReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresReader").finish_non_exhaustive()
    }
}

impl PostgresReader {
    fn inline(bytes: Vec<u8>) -> Self {
        PostgresReader {
            inner: PostgresReaderInner::Inline(io::Cursor::new(bytes)),
        }
    }

    fn chunked(
        client: std::sync::Arc<Mutex<Client>>,
        scope_key: String,
        entity: EntityId,
    ) -> Self {
        PostgresReader {
            inner: PostgresReaderInner::Chunked {
                client,
                scope_key,
                entity_row: entity_to_row(entity),
                next_chunk: 0,
                current: io::Cursor::new(Vec::new()),
            },
        }
    }
}

enum PostgresReaderInner {
    Inline(io::Cursor<Vec<u8>>),
    Chunked {
        client: std::sync::Arc<Mutex<Client>>,
        scope_key: String,
        entity_row: i64,
        next_chunk: i64,
        current: io::Cursor<Vec<u8>>,
    },
}

impl Read for PostgresReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match &mut self.inner {
            PostgresReaderInner::Inline(cursor) => cursor.read(buf),
            PostgresReaderInner::Chunked {
                client,
                scope_key,
                entity_row,
                next_chunk,
                current,
            } => {
                if current.position() == current.get_ref().len() as u64 {
                    let payload: Option<Vec<u8>> = client
                        .lock()
                        .map_err(|_| io::Error::other("postgres store lock poisoned"))?
                        .query_opt(
                            "SELECT payload FROM pipestream_object_chunks \
                             WHERE scope_path = $1 AND entity_id = $2 AND chunk_no = $3",
                            &[scope_key, entity_row, next_chunk],
                        )
                        .map_err(|e| io::Error::other(format!("postgres chunk read: {e}")))?
                        .map(|row| row.get(0));
                    match payload {
                        Some(bytes) => {
                            *next_chunk += 1;
                            *current = io::Cursor::new(bytes);
                        }
                        None => return Ok(0),
                    }
                }
                current.read(buf)
            }
        }
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
    // Entity ids are validated against the 32-bit protocol bound before any SQL runs.
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

/// Payloads at or above this size are stored as `pipestream_object_chunks`
/// rows (1 MiB each) instead of one inline `bytea`, keeping writes bounded
/// in memory. The row's inline payload is empty when chunked.
pub const CHUNK_BYTES: usize = 1 << 20;

/// Streams an entity's chunks and compares the recomputed digest against
/// the recorded one, bounded in memory.
fn verify_chunked_digest(
    client: &mut impl GenericClient,
    scope: &ScopePath,
    entity: EntityId,
    recorded: &[u8],
) -> Result<bool, StorageError> {
    use sha2::Digest as _;
    let key = path_key(scope);
    let entity_row = entity_to_row(entity);
    let mut hasher = sha2::Sha256::new();
    let mut chunk_no = 0i64;
    loop {
        let payload: Option<Vec<u8>> = client
            .query_opt(
                "SELECT payload FROM pipestream_object_chunks \
                 WHERE scope_path = $1 AND entity_id = $2 AND chunk_no = $3",
                &[&key, &entity_row, &chunk_no],
            )
            .map_err(map_pg_error)?
            .map(|row| row.get(0));
        match payload {
            Some(bytes) => {
                hasher.update(&bytes);
                chunk_no += 1;
            }
            None => break,
        }
    }
    Ok(hasher.finalize().as_slice() == recorded)
}

/// Inserts one object row, chunking payloads at or above [`CHUNK_BYTES`].
/// Stale chunk rows for the entity are removed first so a shorter rewrite
/// leaves nothing behind.
fn insert_object(
    tx: &mut impl GenericClient,
    scope: &ScopePath,
    entity: EntityId,
    digest: &Digest32,
    bytes: &[u8],
) -> Result<(), StorageError> {
    let key = path_key(scope);
    let entity_row = entity_to_row(entity);
    tx.execute(
        "DELETE FROM pipestream_object_chunks WHERE scope_path = $1 AND entity_id = $2",
        &[&key, &entity_row],
    )
    .map_err(map_pg_error)?;
    if bytes.len() < CHUNK_BYTES {
        tx.execute(
            "INSERT INTO pipestream_objects (scope_path, entity_id, digest, chunked, payload) \
             VALUES ($1, $2, $3, FALSE, $4)",
            &[&key, &entity_row, &&digest[..], &bytes],
        )
        .map_err(map_pg_error)?;
        return Ok(());
    }
    tx.execute(
        "INSERT INTO pipestream_objects (scope_path, entity_id, digest, chunked, payload) \
         VALUES ($1, $2, $3, TRUE, $4)",
        &[&key, &entity_row, &&digest[..], &&b""[..]],
    )
    .map_err(map_pg_error)?;
    for (index, chunk) in bytes.chunks(CHUNK_BYTES).enumerate() {
        tx.execute(
            "INSERT INTO pipestream_object_chunks (scope_path, entity_id, chunk_no, payload) \
             VALUES ($1, $2, $3, $4)",
            &[&key, &entity_row, &(index as i64), &chunk],
        )
        .map_err(map_pg_error)?;
    }
    Ok(())
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

/// Streaming put writer for the PostgreSQL backend: content is hashed and
/// flushed as [`CHUNK_BYTES`] rows as it arrives, never buffered whole.
/// Small streams (under the threshold) land as one inline row at finish.
pub struct PgChunkWriter {
    client: std::sync::Arc<Mutex<Client>>,
    scope_key: String,
    entity: EntityId,
    expected: Digest32,
    buffer: Vec<u8>,
    chunk_no: i64,
    hasher: sha2::Sha256,
    absorbing: bool,
    done: bool,
}

impl PgChunkWriter {
    fn flush_chunk(&mut self) -> Result<(), StorageError> {
        if self.buffer.len() < CHUNK_BYTES {
            return Ok(());
        }
        let chunk: Vec<u8> = self.buffer.drain(..CHUNK_BYTES).collect();
        let mut client = self
            .client
            .lock()
            .map_err(|_| StorageError::failed("postgres store lock poisoned"))?;
        client
            .execute(
                "INSERT INTO pipestream_object_chunks (scope_path, entity_id, chunk_no, payload) \
                 VALUES ($1, $2, $3, $4)",
                &[&self.scope_key, &entity_to_row(self.entity), &self.chunk_no, &chunk],
            )
            .map_err(map_pg_error)?;
        self.chunk_no += 1;
        Ok(())
    }

    fn delete_chunks(&self) {
        if self.chunk_no == 0 {
            return;
        }
        if let Ok(mut client) = self.client.lock() {
            let _ = client.execute(
                "DELETE FROM pipestream_object_chunks WHERE scope_path = $1 AND entity_id = $2",
                &[&self.scope_key, &entity_to_row(self.entity)],
            );
        }
    }
}

impl Write for PgChunkWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.absorbing {
            return Ok(buf.len());
        }
        self.hasher.update(buf);
        self.buffer.extend_from_slice(buf);
        self.flush_chunk()
            .map_err(|e| io::Error::other(format!("postgres chunk write: {e}")))?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl PutWriter for PgChunkWriter {
    fn finish(mut self) -> Result<(), StorageError> {
        if self.absorbing {
            return Ok(());
        }
        self.done = true;
        use sha2::Digest as _;
        let digest = self.hasher.finalize_reset();
        if digest[..] != self.expected[..] {
            self.delete_chunks();
            return Err(StorageError::corrupt(format!(
                "digest mismatch for entity {} in {}",
                self.entity, self.scope_key
            )));
        }
        let mut client = self
            .client
            .lock()
            .map_err(|_| StorageError::failed("postgres store lock poisoned"))?;
        let mut tx = client.transaction().map_err(map_pg_error)?;
        // Seal and conflict are re-checked: either may have changed mid-stream.
        require_unsealed(&mut tx, &ScopePath::parse(&self.scope_key)?)?;
        let existing: Option<Vec<u8>> = tx
            .query_opt(
                "SELECT digest FROM pipestream_objects \
                 WHERE scope_path = $1 AND entity_id = $2",
                &[&self.scope_key, &entity_to_row(self.entity)],
            )
            .map_err(map_pg_error)?
            .map(|row| row.get(0));
        match existing {
            Some(recorded) if recorded.as_slice() == self.expected => {
                // Identical rewrite: the streamed chunks are redundant.
                tx.execute(
                    "DELETE FROM pipestream_object_chunks WHERE scope_path = $1 AND entity_id = $2",
                    &[&self.scope_key, &entity_to_row(self.entity)],
                )
                .map_err(map_pg_error)?;
                self.chunk_no = 0;
            }
            Some(_) => {
                return Err(StorageError::corrupt(format!(
                    "conflicting bytes for entity {} in {}",
                    self.entity, self.scope_key
                )));
            }
            None => {
                let chunked = self.chunk_no > 0;
                if chunked {
                    // The remainder is the final chunk.
                    if !self.buffer.is_empty() {
                        tx.execute(
                            "INSERT INTO pipestream_object_chunks \
                             (scope_path, entity_id, chunk_no, payload) VALUES ($1, $2, $3, $4)",
                            &[
                                &self.scope_key,
                                &entity_to_row(self.entity),
                                &self.chunk_no,
                                &self.buffer,
                            ],
                        )
                        .map_err(map_pg_error)?;
                    }
                }
                tx.execute(
                    "INSERT INTO pipestream_objects (scope_path, entity_id, digest, chunked, payload) \
                     VALUES ($1, $2, $3, $4, $5)",
                    &[
                        &self.scope_key,
                        &entity_to_row(self.entity),
                        &&self.expected[..],
                        &chunked,
                        &&if chunked { &b""[..] } else { &self.buffer[..] },
                    ],
                )
                .map_err(map_pg_error)?;
            }
        }
        self.buffer.clear();
        self.chunk_no = 0;
        tx.commit().map_err(map_pg_error)
    }
}

impl Drop for PgChunkWriter {
    /// A kill or abort mid-stream deletes the chunks it wrote; the object is
    /// never visible before finish.
    fn drop(&mut self) {
        if !self.done {
            self.delete_chunks();
        }
    }
}

impl ObjectStore for PostgresStore {
    type Reader = PostgresReader;
    type PutWriter = PgChunkWriter;

    fn begin_put(
        &mut self,
        scope: &ScopePath,
        entity: EntityId,
        expected_digest: &Digest32,
    ) -> Result<Self::PutWriter, StorageError> {
        layout::check_entity_id(entity)?;
        let scope_key = path_key(scope);
        let mut client = self.lock()?;
        require_unsealed(&mut *client, scope)?;
        let existing: Option<Vec<u8>> = client
            .query_opt(
                "SELECT digest FROM pipestream_objects \
                 WHERE scope_path = $1 AND entity_id = $2",
                &[&scope_key, &entity_to_row(entity)],
            )
            .map_err(map_pg_error)?
            .map(|row| row.get(0));
        if let Some(recorded) = existing {
            if recorded.as_slice() == expected_digest {
                return Ok(PgChunkWriter {
                    client: self.client_arc()?,
                    scope_key,
                    entity,
                    expected: *expected_digest,
                    buffer: Vec::new(),
                    chunk_no: 0,
                    hasher: sha2::Sha256::new(),
                    absorbing: true,
                    done: false,
                });
            }
            return Err(StorageError::corrupt(format!(
                "conflicting bytes for entity {entity} in {scope}"
            )));
        }
        // Clean slate for the streamed chunks (and any earlier aborted attempt).
        client
            .execute(
                "DELETE FROM pipestream_object_chunks WHERE scope_path = $1 AND entity_id = $2",
                &[&scope_key, &entity_to_row(entity)],
            )
            .map_err(map_pg_error)?;
        Ok(PgChunkWriter {
            client: self.client_arc()?,
            scope_key,
            entity,
            expected: *expected_digest,
            buffer: Vec::new(),
            chunk_no: 0,
            hasher: sha2::Sha256::new(),
            absorbing: false,
            done: false,
        })
    }

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
                insert_object(&mut tx, scope, entity, &digest, bytes)?;
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
        let row = client
            .query_opt(
                "SELECT chunked, payload FROM pipestream_objects \
                 WHERE scope_path = $1 AND entity_id = $2",
                &[&path_key(scope), &entity_to_row(entity)],
            )
            .map_err(map_pg_error)?;
        match row {
            Some(row) => {
                let chunked: bool = row.get(0);
                if chunked {
                    Ok(PostgresReader::chunked(
                        self.client_arc()?,
                        path_key(scope),
                        entity,
                    ))
                } else {
                    let bytes: Vec<u8> = row.get(1);
                    Ok(PostgresReader::inline(bytes))
                }
            }
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
                "SELECT entity_id, digest, chunked, payload FROM pipestream_objects \
                 WHERE scope_path = $1 ORDER BY entity_id",
                &[&path_key(scope)],
            )
            .map_err(map_pg_error)?;
        for row in &rows {
            let entity = entity_from_row(row.get(0), scope)?;
            let recorded: Vec<u8> = row.get(1);
            let chunked: bool = row.get(2);
            let matches = if chunked {
                verify_chunked_digest(&mut *client, scope, entity, &recorded)?
            } else {
                let payload: &[u8] = row.get(3);
                layout::sha256(payload) == recorded.as_slice()
            };
            if !matches {
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

    fn list_scopes(&self) -> Result<Vec<ScopePath>, StorageError> {
        let mut client = self.lock()?;
        let rows = client
            .query(
                "SELECT scope_path FROM ( \
                   SELECT scope_path FROM pipestream_objects \
                   UNION SELECT scope_path FROM pipestream_seals \
                 ) scopes ORDER BY scope_path",
                &[],
            )
            .map_err(map_pg_error)?;
        let mut paths = Vec::new();
        for row in rows {
            paths.push(ScopePath::parse(&row.get::<usize, String>(0))?);
        }
        Ok(paths)
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
                "DELETE FROM pipestream_object_chunks WHERE scope_path = $1",
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
        client
            .execute(
                "DELETE FROM pipestream_object_chunks WHERE scope_path = $1 AND entity_id <= $2",
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
