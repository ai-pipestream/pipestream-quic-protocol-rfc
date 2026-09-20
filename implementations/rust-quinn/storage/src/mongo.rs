//! MongoDB backend implementing both SPI roles.
//!
//! Document mapping of the shared logical layout (one database, four
//! collections):
//!
//! - `objects`: `{ _id: { s: scope_path, e: entity_id }, d: digest, p: payload }`
//!   — digest and payload are binary; the digest recorded at
//!   [`put`](ObjectStore::put) time is stored alongside so
//!   [`verify_scope`](ObjectStore::verify_scope) can rehash and compare.
//! - `seals`: `{ _id: scope_path, d: digest, n: entity_count }`.
//! - `journal`: `{ _id: seq, k: 0|1, i: intent_id, p: payload }` — commits
//!   are tail records (`k = 1`) keyed by intent id; replay folds each commit
//!   into its intent's entry.
//! - `counters`: allocates journal cursors via an atomic `$inc`, so
//!   `seq` is strictly increasing from 1 and commits consume cursors in the
//!   global order.
//!
//! Durability configuration: every write uses `w: "majority", j: true`, so
//! success returns only after the change is journaled. Check-then-write
//! sequences are not transactional (a standalone deployment has no
//! multi-document transactions); the reference deployment is single-writer,
//! and the idempotency rules make crash-retries safe.
//!
//! The driver is async; the store owns a private current-thread tokio
//! runtime and blocks on each call, keeping the SPI synchronous.

use crate::layout;
use crate::{
    Cursor, Digest32, EntityId, Intent, Journal, MAX_RECORD_BYTES, ObjectStore, ReplayEntry,
    ScopePath, SealRecord, StorageError,
};
use mongodb::bson::{Binary, Document, doc, spec::BinarySubtype};
use mongodb::options::{
    Acknowledgment, FindOneAndUpdateOptions, FindOptions, InsertOneOptions, ReturnDocument,
    WriteConcern,
};
use std::io::{self, Read};

const KIND_INTENT: i32 = 0;
const KIND_COMMIT: i32 = 1;

/// Configuration for [`MongoStore`].
#[derive(Debug, Clone)]
pub struct MongoConfig {
    /// Connection URI, e.g. `mongodb://127.0.0.1:27017`.
    pub uri: String,
    /// Database holding the SPI collections.
    pub database: String,
}

/// MongoDB backend for both the [`ObjectStore`] and [`Journal`] roles.
///
/// See the [module documentation](self) for the document mapping and the
/// durability configuration.
pub struct MongoStore {
    client: mongodb::Client,
    runtime: tokio::runtime::Runtime,
    database: String,
}

impl std::fmt::Debug for MongoStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MongoStore")
            .field("database", &self.database)
            .finish_non_exhaustive()
    }
}

/// MongoDB backend reader: a snapshot of the payload taken at open time.
#[derive(Debug)]
pub struct MongoReader {
    bytes: io::Cursor<Vec<u8>>,
}

impl Read for MongoReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.bytes.read(buf)
    }
}

fn binary(bytes: &[u8]) -> Binary {
    Binary {
        subtype: BinarySubtype::Generic,
        bytes: bytes.to_vec(),
    }
}

/// Maps a driver error into the taxonomy: I/O and server-selection failures
/// are retryable; everything else is a general failure.
fn map_mongo_error(error: mongodb::error::Error) -> StorageError {
    use mongodb::error::ErrorKind;
    match &*error.kind {
        ErrorKind::Io(_) | ErrorKind::ServerSelection { .. } => {
            StorageError::transient(format!("mongodb: {error}"))
        }
        _ => StorageError::failed(format!("mongodb: {error}")),
    }
}

/// Write concern for every mutation: journaled before acknowledge.
fn journaled() -> WriteConcern {
    WriteConcern::builder()
        .w(Acknowledgment::Majority)
        .journal(true)
        .build()
}

impl MongoStore {
    /// Connects and prepares the SPI collections (indexes are created when
    /// absent).
    pub fn open(config: &MongoConfig) -> Result<Self, StorageError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| StorageError::failed(format!("tokio runtime: {e}")))?;
        let client = runtime
            .block_on(mongodb::Client::with_uri_str(&config.uri))
            .map_err(map_mongo_error)?;
        let store = MongoStore {
            client,
            runtime,
            database: config.database.clone(),
        };
        store.ensure_indexes()?;
        Ok(store)
    }

    fn block_on<F: std::future::IntoFuture>(&self, action: F) -> F::Output {
        self.runtime.block_on(action.into_future())
    }

    fn db(&self) -> mongodb::Database {
        self.client.database(&self.database)
    }

    fn objects(&self) -> mongodb::Collection<Document> {
        self.db()
            .collection_with_options::<Document>(
                "objects",
                mongodb::options::CollectionOptions::builder()
                    .write_concern(journaled())
                    .build(),
            )
    }

    fn seals(&self) -> mongodb::Collection<Document> {
        self.db()
            .collection_with_options::<Document>(
                "seals",
                mongodb::options::CollectionOptions::builder()
                    .write_concern(journaled())
                    .build(),
            )
    }

    fn journal(&self) -> mongodb::Collection<Document> {
        self.db()
            .collection_with_options::<Document>(
                "journal",
                mongodb::options::CollectionOptions::builder()
                    .write_concern(journaled())
                    .build(),
            )
    }

    fn ensure_indexes(&self) -> Result<(), StorageError> {
        use mongodb::IndexModel;
        let intent_index = IndexModel::builder()
            .keys(doc! { "k": 1, "i": 1 })
            .options(
                mongodb::options::IndexOptions::builder()
                    .unique(true)
                    .build(),
            )
            .build();
        self.block_on(self.journal().create_index(intent_index))
            .map_err(map_mongo_error)?;
        Ok(())
    }

    /// Allocates the next journal cursor atomically (durable `$inc` on a
    /// counters document).
    fn next_cursor(&self) -> Result<Cursor, StorageError> {
        let counters = self.db().collection::<Document>("counters");
        let options = FindOneAndUpdateOptions::builder()
            .upsert(true)
            .return_document(ReturnDocument::After)
            .build();
        let updated = self
            .block_on(
                counters
                    .find_one_and_update(doc! { "_id": "journal" }, doc! { "$inc": { "seq": 1 } })
                    .with_options(options),
            )
            .map_err(map_mongo_error)?
            .ok_or_else(|| StorageError::failed("journal counter upsert returned nothing"))?;
        let seq = updated
            .get("seq")
            .and_then(bson_i64)
            .ok_or_else(|| StorageError::corrupt("journal counter is not an integer"))?;
        u64::try_from(seq)
            .map(Cursor)
            .map_err(|_| StorageError::corrupt("negative journal cursor".to_string()))
    }

    fn find_seal(&self, scope: &ScopePath) -> Result<Option<SealRecord>, StorageError> {
        let found = self
            .block_on(self.seals().find_one(doc! { "_id": scope.to_path_string() }))
            .map_err(map_mongo_error)?;
        match found {
            Some(doc) => {
                let digest = binary_field(&doc, "d", scope)?;
                let count = doc
                    .get("n")
                    .and_then(bson_i64)
                    .and_then(|n| u64::try_from(n).ok())
                    .ok_or_else(|| {
                        StorageError::corrupt(format!("malformed seal record for {scope}"))
                    })?;
                Ok(Some(SealRecord {
                    digest,
                    entity_count: count,
                }))
            }
            None => Ok(None),
        }
    }

    /// The entities of one scope, ascending by id: `(id, recorded digest,
    /// payload)`.
    fn scan_entities(
        &self,
        scope: &ScopePath,
    ) -> Result<Vec<(EntityId, Digest32, Vec<u8>)>, StorageError> {
        let options = FindOptions::builder().sort(doc! { "_id.e": 1 }).build();
        let mut cursor = self
            .block_on(self.objects().find(doc! { "_id.s": scope.to_path_string() }).with_options(options))
            .map_err(map_mongo_error)?;
        let mut entities = Vec::new();
        while let Some(doc) = self
            .block_on(futures_next(&mut cursor))
            .transpose()
            .map_err(map_mongo_error)?
        {
            let id = doc
                .get_document("_id")
                .ok()
                .and_then(|id| id.get("e").cloned())
                .and_then(|e| bson_i64(&e))
                .and_then(|e| u32::try_from(e).ok())
                .ok_or_else(|| {
                    StorageError::corrupt(format!("malformed entity id stored for {scope}"))
                })?;
            let digest = binary_field(&doc, "d", scope)?;
            let payload = doc
                .get("p")
                .and_then(bson_bytes)
                .ok_or_else(|| {
                    StorageError::corrupt(format!("malformed payload stored for {scope}"))
                })?;
            entities.push((id, digest, payload));
        }
        Ok(entities)
    }
}

/// Awaits one cursor advance (the driver's `Cursor::advance`).
async fn futures_next(
    cursor: &mut mongodb::Cursor<Document>,
) -> Option<mongodb::error::Result<Document>> {
    match cursor.advance().await {
        Ok(true) => Some(cursor.deserialize_current()),
        Ok(false) => None,
        Err(error) => Some(Err(error)),
    }
}

/// Reads an integer BSON value as i64, accepting Int32 and Int64.
fn bson_i64(value: &mongodb::bson::Bson) -> Option<i64> {
    match value {
        mongodb::bson::Bson::Int32(v) => Some(i64::from(*v)),
        mongodb::bson::Bson::Int64(v) => Some(*v),
        _ => None,
    }
}
/// Extracts the bytes of a binary BSON value.
fn bson_bytes(value: &mongodb::bson::Bson) -> Option<Vec<u8>> {
    match value {
        mongodb::bson::Bson::Binary(binary) => Some(binary.bytes.clone()),
        _ => None,
    }
}

/// Extracts a 32-byte binary field.
fn binary_field(doc: &Document, field: &str, scope: &ScopePath) -> Result<Digest32, StorageError> {
    let bytes = doc
        .get(field)
        .and_then(bson_bytes)
        .ok_or_else(|| StorageError::corrupt(format!("malformed digest stored for {scope}")))?;
    let digest: Digest32 = bytes.as_slice().try_into().map_err(|_| {
        StorageError::corrupt(format!("malformed digest stored for {scope}"))
    })?;
    Ok(digest)
}

impl ObjectStore for MongoStore {
    type Reader = MongoReader;

    fn put(
        &mut self,
        scope: &ScopePath,
        entity: EntityId,
        bytes: &[u8],
        expected_digest: &Digest32,
    ) -> Result<(), StorageError> {
        layout::check_entity_id(entity)?;
        // Seal is checked before the digest: a sealed scope rejects every
        // put regardless of bytes.
        if self.find_seal(scope)?.is_some() {
            return Err(StorageError::already_sealed(format!(
                "scope {scope} is sealed"
            )));
        }
        let digest = layout::sha256(bytes);
        if &digest != expected_digest {
            return Err(StorageError::corrupt(format!(
                "digest mismatch for entity {entity} in {scope}"
            )));
        }
        let key = doc! { "s": scope.to_path_string(), "e": entity as i64 };
        let existing = self
            .block_on(self.objects().find_one(doc! { "_id": &key }))
            .map_err(map_mongo_error)?;
        if let Some(doc) = existing {
            let recorded: Vec<u8> = doc
                .get("p")
                .and_then(bson_bytes)
                .ok_or_else(|| {
                    StorageError::corrupt(format!("malformed payload stored for {scope}"))
                })?;
            if recorded == bytes {
                return Ok(());
            }
            return Err(StorageError::corrupt(format!(
                "conflicting bytes for entity {entity} in {scope}"
            )));
        }
        let document = doc! {
            "_id": key,
            "d": binary(&digest),
            "p": binary(bytes),
        };
        self.block_on(
            self.objects()
                .insert_one(document)
                .with_options(InsertOneOptions::default()),
        )
        .map_err(map_mongo_error)?;
        Ok(())
    }

    fn open_read(&self, scope: &ScopePath, entity: EntityId) -> Result<MongoReader, StorageError> {
        let key = doc! { "s": scope.to_path_string(), "e": entity as i64 };
        let found = self
            .block_on(self.objects().find_one(doc! { "_id": key }))
            .map_err(map_mongo_error)?;
        match found {
            Some(doc) => {
                let payload = doc
                    .get("p")
                    .and_then(bson_bytes)
                    .ok_or_else(|| {
                        StorageError::corrupt(format!("malformed payload stored for {scope}"))
                    })?;
                Ok(MongoReader {
                    bytes: io::Cursor::new(payload),
                })
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
        match self.find_seal(scope)? {
            Some(existing)
                if existing.digest == *digest && existing.entity_count == entity_count =>
            {
                Ok(())
            }
            Some(_) => Err(StorageError::corrupt(format!(
                "scope {scope} already carries a different seal"
            ))),
            None => {
                let count = i64::try_from(entity_count)
                    .map_err(|_| StorageError::failed("entity count exceeds i64"))?;
                let document = doc! {
                    "_id": scope.to_path_string(),
                    "d": binary(digest),
                    "n": count,
                };
                self.block_on(self.seals().insert_one(document))
                    .map_err(map_mongo_error)?;
                Ok(())
            }
        }
    }

    fn verify_scope(&self, scope: &ScopePath) -> Result<SealRecord, StorageError> {
        let seal = self
            .find_seal(scope)?
            .ok_or_else(|| StorageError::not_found(format!("scope {scope} has no seal record")))?;
        let entities = self.scan_entities(scope)?;
        for (entity, recorded, payload) in &entities {
            if layout::sha256(payload) != *recorded {
                return Err(StorageError::corrupt(format!(
                    "entity {entity} in {scope} fails its recorded digest"
                )));
            }
        }
        if entities.len() as u64 != seal.entity_count {
            return Err(StorageError::corrupt(format!(
                "scope {scope} entity count differs from its seal"
            )));
        }
        Ok(seal)
    }

    fn list(&self, scope: &ScopePath) -> Result<Vec<EntityId>, StorageError> {
        Ok(self
            .scan_entities(scope)?
            .into_iter()
            .map(|(id, _, _)| id)
            .collect())
    }

    fn retire(&mut self, scope: &ScopePath, retention_cursor: u64) -> Result<u64, StorageError> {
        let key = scope.to_path_string();
        if self.find_seal(scope)?.is_some() {
            // A sealed scope retires whole: every entity and the seal record.
            let removed = self
                .block_on(self.objects().delete_many(doc! { "_id.s": &key }))
                .map_err(map_mongo_error)?
                .deleted_count;
            self.block_on(self.seals().delete_one(doc! { "_id": &key }))
                .map_err(map_mongo_error)?;
            return Ok(removed);
        }
        let present = self
            .block_on(self.objects().count_documents(doc! { "_id.s": &key }))
            .map_err(map_mongo_error)?;
        if present == 0 {
            return Err(StorageError::not_found(format!(
                "scope {scope} holds nothing to retire"
            )));
        }
        let cursor = i64::try_from(retention_cursor)
            .map_err(|_| StorageError::failed("retention cursor exceeds i64"))?;
        let removed = self
            .block_on(
                self.objects()
                    .delete_many(doc! { "_id.s": &key, "_id.e": { "$lte": cursor } }),
            )
            .map_err(map_mongo_error)?
            .deleted_count;
        Ok(removed)
    }
}

impl Journal for MongoStore {
    fn append_intent(&mut self, intent: &Intent) -> Result<Cursor, StorageError> {
        let existing = self
            .block_on(
                self.journal()
                    .find_one(doc! { "k": KIND_INTENT, "i": intent.id() }),
            )
            .map_err(map_mongo_error)?;
        if let Some(doc) = existing {
            let payload = doc
                .get("p")
                .and_then(bson_bytes)
                .ok_or_else(|| StorageError::corrupt("malformed journal payload".to_string()))?;
            if payload == intent.record() {
                let seq = doc
                    .get("_id")
                    .and_then(bson_i64)
                    .and_then(|s| u64::try_from(s).ok())
                    .ok_or_else(|| StorageError::corrupt("malformed journal cursor".to_string()))?;
                return Ok(Cursor(seq));
            }
            return Err(StorageError::corrupt(format!(
                "intent {} already exists with different bytes",
                intent.id()
            )));
        }
        let cursor = self.next_cursor()?;
        let document = doc! {
            "_id": cursor.0 as i64,
            "k": KIND_INTENT,
            "i": intent.id(),
            "p": binary(intent.record()),
        };
        self.block_on(self.journal().insert_one(document))
            .map_err(map_mongo_error)?;
        Ok(cursor)
    }

    fn commit(&mut self, intent_id: &str, record: &[u8]) -> Result<(), StorageError> {
        if record.is_empty() || record.len() > MAX_RECORD_BYTES {
            return Err(StorageError::failed(format!(
                "commit record must be 1 to {MAX_RECORD_BYTES} octets"
            )));
        }
        let intent_present = self
            .block_on(
                self.journal()
                    .find_one(doc! { "k": KIND_INTENT, "i": intent_id }),
            )
            .map_err(map_mongo_error)?;
        if intent_present.is_none() {
            return Err(StorageError::not_found(format!(
                "intent {intent_id} has no durable append"
            )));
        }
        let prior = self
            .block_on(
                self.journal()
                    .find_one(doc! { "k": KIND_COMMIT, "i": intent_id }),
            )
            .map_err(map_mongo_error)?;
        if let Some(doc) = prior {
            let existing = doc
                .get("p")
                .and_then(bson_bytes)
                .ok_or_else(|| StorageError::corrupt("malformed journal payload".to_string()))?;
            if existing == record {
                return Ok(());
            }
            return Err(StorageError::corrupt(format!(
                "intent {intent_id} already carries a different commit"
            )));
        }
        let cursor = self.next_cursor()?;
        let document = doc! {
            "_id": cursor.0 as i64,
            "k": KIND_COMMIT,
            "i": intent_id,
            "p": binary(record),
        };
        self.block_on(self.journal().insert_one(document))
            .map_err(map_mongo_error)?;
        Ok(())
    }

    fn replay(&self, since: Cursor) -> Result<Vec<ReplayEntry>, StorageError> {
        let since_seq = i64::try_from(since.0)
            .map_err(|_| StorageError::failed("cursor exceeds i64"))?;
        let options = FindOptions::builder().sort(doc! { "_id": 1 }).build();
        let mut intents_cursor = self
            .block_on(
                self.journal()
                    .find(doc! { "k": KIND_INTENT, "_id": { "$gt": since_seq } })
                    .with_options(options),
            )
            .map_err(map_mongo_error)?;
        let mut entries = Vec::new();
        while let Some(doc) = self
            .block_on(futures_next(&mut intents_cursor))
            .transpose()
            .map_err(map_mongo_error)?
        {
            let seq = doc
                .get("_id")
                .and_then(bson_i64)
                .and_then(|s| u64::try_from(s).ok())
                .ok_or_else(|| StorageError::corrupt("malformed journal cursor".to_string()))?;
            let id = doc
                .get("i")
                .and_then(mongodb::bson::Bson::as_str)
                .map(str::to_string)
                .ok_or_else(|| StorageError::corrupt("malformed journal intent id".to_string()))?;
            let record = doc
                .get("p")
                .and_then(bson_bytes)
                .ok_or_else(|| StorageError::corrupt("malformed journal payload".to_string()))?;
            let commit_doc = self
                .block_on(self.journal().find_one(doc! { "k": KIND_COMMIT, "i": &id }))
                .map_err(map_mongo_error)?;
            let (commit, commit_cursor) = match commit_doc {
                Some(doc) => {
                    let payload = doc
                        .get("p")
                        .and_then(bson_bytes)
                        .ok_or_else(|| {
                            StorageError::corrupt("malformed journal payload".to_string())
                        })?;
                    let cursor = doc
                        .get("_id")
                        .and_then(bson_i64)
                        .and_then(|s| u64::try_from(s).ok())
                        .map(Cursor)
                        .ok_or_else(|| {
                            StorageError::corrupt("malformed journal cursor".to_string())
                        })?;
                    (Some(payload), Some(cursor))
                }
                None => (None, None),
            };
            let intent = Intent::new(id, record).map_err(|_| {
                StorageError::corrupt("journal intent fails validation".to_string())
            })?;
            entries.push(ReplayEntry {
                cursor: Cursor(seq),
                intent,
                commit,
                commit_cursor,
            });
        }
        Ok(entries)
    }
}
