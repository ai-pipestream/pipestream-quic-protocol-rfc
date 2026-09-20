//! S3 / object-store backend implementing the [`ObjectStore`] role.
//!
//! Object stores are a natural fit for sealed immutable objects; they do
//! **not** implement [`Journal`] (per the design note's backend matrix) —
//! pair an `S3Store` with another backend's journal for the two-system
//! configuration (object write first, journal commit second, reconcile by
//! replay).
//!
//! Physical mapping of the shared logical layout, under a configurable key
//! prefix inside one bucket:
//!
//! ```text
//! <prefix>objects/<scope-id-hex>/.../seal            40 bytes, same format as the fs backend
//! <prefix>objects/<scope-id-hex>/.../objects/<bucket-hex>/<entity-hex>
//!                                                   32-byte digest header + payload, same as fs
//! ```
//!
//! Object bytes are identical to the filesystem backend's entity files, so
//! the two backends can be mirrored byte-for-byte. The seal record is a
//! single PUT, which S3 applies atomically.
//!
//! Durability configuration: a PUT returns success only after the service
//! has durably stored the object (S3's strong read-after-write consistency);
//! against MinIO that means the object's erasure set is committed. The
//! kill/resume conformance rows verify this empirically per deployment.
//!
//! Open-read survival: [`open_read`](ObjectStore::open_read) materializes
//! the object bytes at open time, so deleting the object afterwards cannot
//! invalidate the reader (S3 has no open-file semantic to lean on).
//!
//! The SDK is async; the store owns a private current-thread tokio runtime
//! and blocks on each call, keeping the SPI synchronous.

use crate::layout;
use crate::{Digest32, EntityId, ObjectStore, ScopePath, SealRecord, StorageError};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use std::io::{self, Read};

/// Configuration for [`S3Store`].
#[derive(Debug, Clone)]
pub struct S3Config {
    /// Bucket holding all keys.
    pub bucket: String,
    /// Key prefix isolating this store inside the bucket ("" or ending in
    /// `/`). The shared layout begins below it.
    pub prefix: String,
    /// Service endpoint (`None` for AWS, `Some("http://host:9000")` for
    /// MinIO and other S3-compatible services). Path-style addressing is
    /// always used, as MinIO requires.
    pub endpoint_url: Option<String>,
    /// Signing region.
    pub region: String,
    /// Static credentials.
    pub access_key: String,
    /// Static credentials.
    pub secret_key: String,
}

/// S3/object-store backend for the [`ObjectStore`] role.
///
/// See the [module documentation](self) for the key layout and the
/// durability configuration.
pub struct S3Store {
    client: Client,
    runtime: tokio::runtime::Runtime,
    bucket: String,
    prefix: String,
}

impl std::fmt::Debug for S3Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Store")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl S3Store {
    /// Connects using `config` and creates the bucket when absent.
    pub fn open(config: &S3Config) -> Result<Self, StorageError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| StorageError::backend(format!("tokio runtime: {e}")))?;
        let credentials = Credentials::new(
            config.access_key.clone(),
            config.secret_key.clone(),
            None,
            None,
            "pipestream-storage",
        );
        let mut builder = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(config.region.clone()))
            .credentials_provider(credentials)
            .force_path_style(true);
        if let Some(endpoint) = &config.endpoint_url {
            builder = builder.endpoint_url(endpoint.clone());
        }
        let client = Client::from_conf(builder.build());
        let store = S3Store {
            client,
            runtime,
            bucket: config.bucket.clone(),
            prefix: config.prefix.clone(),
        };
        store.ensure_bucket()?;
        Ok(store)
    }

    fn ensure_bucket(&self) -> Result<(), StorageError> {
        let exists = self
            .block_on(self.client.head_bucket().bucket(&self.bucket).send())
            .is_ok();
        if exists {
            return Ok(());
        }
        match self.block_on(
            self.client
                .create_bucket()
                .bucket(&self.bucket)
                .send(),
        ) {
            Ok(_) => Ok(()),
            Err(error) => {
                // Another store may have created it concurrently.
                let message = error.to_string();
                if message.contains("BucketAlreadyOwnedByYou") || message.contains("BucketAlreadyExists") {
                    Ok(())
                } else {
                    Err(map_sdk_error("create bucket", error))
                }
            }
        }
    }

    fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(future)
    }

    fn object_key(&self, scope: &ScopePath, entity: EntityId) -> String {
        format!(
            "{}objects/{}/{}/{}/{}",
            self.prefix,
            scope.to_path_string(),
            OBJECTS_DIR,
            layout::bucket_hex(entity),
            layout::entity_component(entity)
        )
    }

    fn seal_key(&self, scope: &ScopePath) -> String {
        format!("{}objects/{}/{}", self.prefix, scope.to_path_string(), SEAL_FILE)
    }

    /// Everything under the scope's directory, objects and seal alike.
    fn scope_prefix(&self, scope: &ScopePath) -> String {
        format!("{}objects/{}/", self.prefix, scope.to_path_string())
    }

    /// Only the scope's entity objects.
    fn entities_prefix(&self, scope: &ScopePath) -> String {
        format!("{}objects/{}/{}/", self.prefix, scope.to_path_string(), OBJECTS_DIR)
    }

    /// GETs an object's full bytes; None when the key does not exist.
    fn get_object(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        let result = self.block_on(
            self.client
                .get_object()
                .bucket(&self.bucket)
                .key(key)
                .send(),
        );
        match result {
            Ok(output) => {
                let bytes = self
                    .block_on(output.body.collect())
                    .map_err(|e| StorageError::transient(format!("s3 body read: {e}")))?
                    .into_bytes()
                    .to_vec();
                Ok(Some(bytes))
            }
            Err(error) => {
                if is_not_found(&error) {
                    Ok(None)
                } else {
                    Err(map_sdk_error("get object", error))
                }
            }
        }
    }

    fn put_object(&self, key: &str, bytes: Vec<u8>) -> Result<(), StorageError> {
        self.block_on(
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .body(ByteStream::from(bytes))
                .send(),
        )
        .map(|_| ())
        .map_err(|error| map_sdk_error("put object", error))
    }

    /// Lists all keys under `prefix`, following pagination.
    fn list_keys(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);
            if let Some(token) = token.take() {
                request = request.continuation_token(token);
            }
            let output = self
                .block_on(request.send())
                .map_err(|error| map_sdk_error("list objects", error))?;
            for object in output.contents() {
                keys.push(object.key().unwrap_or_default().to_string());
            }
            match output.next_continuation_token() {
                Some(next) => token = Some(next.to_string()),
                None => return Ok(keys),
            }
        }
    }

    fn delete_keys(&self, keys: &[String]) -> Result<(), StorageError> {
        use aws_sdk_s3::types::{Delete, ObjectIdentifier};
        for chunk in keys.chunks(1000) {
            let mut identifiers = Vec::with_capacity(chunk.len());
            for key in chunk {
                let identifier = ObjectIdentifier::builder().key(key).build().map_err(|e| {
                    StorageError::backend(format!("invalid object key {key}: {e}"))
                })?;
                identifiers.push(identifier);
            }
            let delete = Delete::builder()
                .set_objects(Some(identifiers))
                .quiet(true)
                .build()
                .map_err(|e| StorageError::backend(format!("delete request: {e}")))?;
            self.block_on(
                self.client
                    .delete_objects()
                    .bucket(&self.bucket)
                    .delete(delete)
                    .send(),
            )
            .map_err(|error| map_sdk_error("delete objects", error))?;
        }
        Ok(())
    }
}

const OBJECTS_DIR: &str = "objects";
const SEAL_FILE: &str = "seal";
const SEAL_RECORD_LEN: usize = 32 + 8;

/// True when the SDK error is a 404/NoSuchKey/NotFound service error.
fn is_not_found<E>(error: &aws_sdk_s3::error::SdkError<E>) -> bool
where
    E: std::fmt::Debug + aws_sdk_s3::error::ProvideErrorMetadata,
{
    match error {
        aws_sdk_s3::error::SdkError::ServiceError(service) => {
            matches!(service.err().meta().code(), Some("NoSuchKey" | "NotFound"))
                || service.raw().status().as_u16() == 404
        }
        _ => false,
    }
}

/// Maps an SDK error into the taxonomy: transport failures, timeouts,
/// throttling, and 5xx service responses are retryable; everything else is
/// a general backend error.
fn map_sdk_error<E>(operation: &str, error: aws_sdk_s3::error::SdkError<E>) -> StorageError
where
    E: std::fmt::Debug + aws_sdk_s3::error::ProvideErrorMetadata,
{
    use aws_sdk_s3::error::SdkError;
    match &error {
        SdkError::TimeoutError(_) | SdkError::DispatchFailure(_) | SdkError::ResponseError(_) => {
            return StorageError::transient(format!("s3 {operation}: {error}"));
        }
        SdkError::ServiceError(service) => {
            let status = service.raw().status().as_u16();
            let throttling = matches!(
                service.err().meta().code(),
                Some("SlowDown" | "ServiceUnavailable" | "InternalError")
            );
            if status >= 500 || throttling {
                return StorageError::transient(format!("s3 {operation}: {error}"));
            }
        }
        _ => {}
    }
    StorageError::backend(format!("s3 {operation}: {error}"))
}

fn parse_seal_record(bytes: &[u8], scope: &ScopePath) -> Result<SealRecord, StorageError> {
    if bytes.len() != SEAL_RECORD_LEN {
        return Err(StorageError::corrupt(format!(
            "seal record for {scope} is {} bytes, expected {SEAL_RECORD_LEN}",
            bytes.len()
        )));
    }
    let mut digest: Digest32 = [0; 32];
    digest.copy_from_slice(&bytes[..32]);
    let mut count = [0u8; 8];
    count.copy_from_slice(&bytes[32..]);
    Ok(SealRecord {
        digest,
        entity_count: u64::from_be_bytes(count),
    })
}

/// Parses the entity id out of an entity key's final path segment.
fn entity_from_key(key: &str, scope: &ScopePath) -> Result<EntityId, StorageError> {
    let name = key.rsplit('/').next().unwrap_or_default();
    u32::from_str_radix(name, 16).map_err(|_| {
        StorageError::corrupt(format!("unparseable entity key {key} in {scope}"))
    })
}

/// Splits a stored object into its recorded digest header and payload.
fn split_object<'a>(bytes: &'a [u8], scope: &ScopePath) -> Result<(Digest32, &'a [u8]), StorageError> {
    if bytes.len() < 32 {
        return Err(StorageError::corrupt(format!(
            "object in {scope} is {} bytes, shorter than the digest header",
            bytes.len()
        )));
    }
    let mut digest: Digest32 = [0; 32];
    digest.copy_from_slice(&bytes[..32]);
    Ok((digest, &bytes[32..]))
}

/// S3 backend reader: a snapshot of the object bytes taken at open time.
#[derive(Debug)]
pub struct S3Reader {
    bytes: io::Cursor<Vec<u8>>,
}

impl Read for S3Reader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.bytes.read(buf)
    }
}

impl ObjectStore for S3Store {
    type Reader = S3Reader;

    fn put(
        &mut self,
        scope: &ScopePath,
        entity: EntityId,
        bytes: &[u8],
        expected_digest: &Digest32,
    ) -> Result<(), StorageError> {
        layout::check_entity_id(entity)?;
        let digest = layout::sha256(bytes);
        if &digest != expected_digest {
            return Err(StorageError::corrupt(format!(
                "digest mismatch for entity {entity} in {scope}"
            )));
        }
        if self.get_object(&self.seal_key(scope))?.is_some() {
            return Err(StorageError::already_sealed(format!(
                "scope {scope} is sealed"
            )));
        }
        let key = self.object_key(scope, entity);
        if let Some(existing) = self.get_object(&key)? {
            let (recorded, _) = split_object(&existing, scope)?;
            if recorded == digest {
                return Ok(());
            }
            return Err(StorageError::corrupt(format!(
                "conflicting bytes for entity {entity} in {scope}"
            )));
        }
        let mut contents = Vec::with_capacity(32 + bytes.len());
        contents.extend_from_slice(&digest);
        contents.extend_from_slice(bytes);
        self.put_object(&key, contents)
    }

    fn open_read(&self, scope: &ScopePath, entity: EntityId) -> Result<S3Reader, StorageError> {
        let key = self.object_key(scope, entity);
        let existing = self.get_object(&key)?;
        match existing {
            Some(bytes) => {
                let (_, payload) = split_object(&bytes, scope)?;
                Ok(S3Reader {
                    bytes: io::Cursor::new(payload.to_vec()),
                })
            }
            None => Err(StorageError::not_found(format!(
                "entity {entity} in {scope}"
            ))),
        }
    }

    fn seal_scope(
        &mut self,
        scope: &ScopePath,
        digest: &Digest32,
        entity_count: u64,
    ) -> Result<(), StorageError> {
        let key = self.seal_key(scope);
        if let Some(existing) = self.get_object(&key)? {
            let record = parse_seal_record(&existing, scope)?;
            if record.digest == *digest && record.entity_count == entity_count {
                return Ok(());
            }
            return Err(StorageError::corrupt(format!(
                "scope {scope} already sealed with a different digest or entity count"
            )));
        }
        let mut contents = Vec::with_capacity(SEAL_RECORD_LEN);
        contents.extend_from_slice(digest);
        contents.extend_from_slice(&entity_count.to_be_bytes());
        // A single PUT is the atomic seal; S3 never exposes a partial object.
        self.put_object(&key, contents)
    }

    fn verify_scope(&self, scope: &ScopePath) -> Result<SealRecord, StorageError> {
        let seal_bytes = self
            .get_object(&self.seal_key(scope))?
            .ok_or_else(|| StorageError::not_found(format!("seal record for {scope}")))?;
        let seal = parse_seal_record(&seal_bytes, scope)?;
        let keys = self.list_keys(&self.entities_prefix(scope))?;
        if keys.len() as u64 != seal.entity_count {
            return Err(StorageError::corrupt(format!(
                "scope {scope} holds {} entities but the seal record says {}",
                keys.len(),
                seal.entity_count
            )));
        }
        for key in &keys {
            let entity = entity_from_key(key, scope)?;
            let bytes = self
                .get_object(key)?
                .ok_or_else(|| StorageError::corrupt(format!("entity {entity} vanished under verify")))?;
            let (recorded, payload) = split_object(&bytes, scope)?;
            if layout::sha256(payload) != recorded {
                return Err(StorageError::corrupt(format!(
                    "entity {entity} in {scope} no longer matches its recorded digest"
                )));
            }
        }
        Ok(seal)
    }

    fn list(&self, scope: &ScopePath) -> Result<Vec<EntityId>, StorageError> {
        let keys = self.list_keys(&self.entities_prefix(scope))?;
        let mut entities = Vec::with_capacity(keys.len());
        for key in &keys {
            entities.push(entity_from_key(key, scope)?);
        }
        entities.sort_unstable();
        Ok(entities)
    }

    fn retire(&mut self, scope: &ScopePath, retention_cursor: u64) -> Result<u64, StorageError> {
        let sealed = self.get_object(&self.seal_key(scope))?.is_some();
        if sealed {
            // A sealed scope retires whole: every entity and the seal record.
            let keys = self.list_keys(&self.scope_prefix(scope))?;
            let removed = keys
                .iter()
                .filter(|key| key.rsplit('/').nth(2) == Some(OBJECTS_DIR))
                .count() as u64;
            self.delete_keys(&keys)?;
            return Ok(removed);
        }
        let keys = self.list_keys(&self.entities_prefix(scope))?;
        let mut doomed = Vec::new();
        for key in keys {
            let entity = entity_from_key(&key, scope)?;
            if u64::from(entity) <= retention_cursor {
                doomed.push(key);
            }
        }
        let removed = doomed.len() as u64;
        self.delete_keys(&doomed)?;
        Ok(removed)
    }
}
