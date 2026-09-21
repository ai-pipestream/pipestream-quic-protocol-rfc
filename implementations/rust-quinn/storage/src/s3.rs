//! S3 / object-store backend implementing the [`ObjectStore`] role.
//!
//! Object stores are a natural fit for sealed immutable objects; they do
//! **not** implement [`Journal`] (per the design note's backend matrix) —
//! pair an `S3Store` with another backend's journal for the two-system
//! configuration (object write first, journal commit second, reconcile by
//! replay).
//!
//! Physical mapping of the shared logical layout, under a configurable key
//! prefix inside one bucket. Object bytes are identical to the filesystem
//! backend's files, so the two backends can be mirrored byte-for-byte:
//!
//! ```text
//! <prefix>objects/<scope-hex>/.../scope.seal                  [32B digest][8B BE count]
//! <prefix>objects/<scope-hex>/.../<bucket-2hex>/<entity-16hex>
//!                                       [8B BE payload length][32B sha256][payload]
//! ```
//!
//! The seal record is a single PUT, which S3 applies atomically.
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
use crate::{
    Digest32, EntityId, ObjectStore, PutWriter, ScopePath, SealRecord, StorageError,
};
use aws_sdk_s3::Client;
use sha2::Digest as _;
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
    runtime: Option<std::sync::Arc<tokio::runtime::Runtime>>,
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

const OBJECTS_DIR: &str = "objects";
const SEAL_FILE_NAME: &str = "scope.seal";
const ENTITY_HEADER_BYTES: usize = 8 + 32;
const SEAL_RECORD_LEN: usize = 32 + 8;

impl S3Store {
    /// Connects using `config` and creates the bucket when absent.
    pub fn open(config: &S3Config) -> Result<Self, StorageError> {
        let runtime = std::sync::Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| StorageError::failed(format!("tokio runtime: {e}")))?,
        );
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
            .force_path_style(true)
            // Ring-based TLS: the workspace's only provider. The SDK's
            // default HTTPS client is aws-lc-only; pulling it in makes
            // rustls refuse to auto-select a provider for the whole
            // workspace, so we build our own ring client instead.
            .http_client(
                aws_smithy_http_client::hyper_014::HyperClientBuilder::new().build_https(),
            );
        if let Some(endpoint) = &config.endpoint_url {
            builder = builder.endpoint_url(endpoint.clone());
        }
        let client = Client::from_conf(builder.build());
        let store = S3Store {
            client,
            runtime: Some(runtime),
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
        match self.block_on(self.client.create_bucket().bucket(&self.bucket).send()) {
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
        self.runtime
            .as_ref()
            .expect("s3 store runtime")
            .block_on(future)
    }

    fn object_key(&self, scope: &ScopePath, entity: EntityId) -> String {
        format!(
            "{}{}/{}",
            self.scope_prefix(scope),
            layout::bucket_hex(entity),
            layout::entity_component(entity)
        )
    }

    fn seal_key(&self, scope: &ScopePath) -> String {
        format!("{}{}", self.scope_prefix(scope), SEAL_FILE_NAME)
    }

    /// Everything under the scope's prefix, objects and seal alike.
    fn scope_prefix(&self, scope: &ScopePath) -> String {
        format!(
            "{}{}/{}/",
            self.prefix,
            OBJECTS_DIR,
            scope.to_path_string()
        )
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

    /// GETs a byte range of an object; None when the key does not exist.
    fn get_object_range(
        &self,
        key: &str,
        start: u64,
        end_inclusive: u64,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let result = self.block_on(
            self.client
                .get_object()
                .bucket(&self.bucket)
                .key(key)
                .range(format!("bytes={start}-{end_inclusive}"))
                .send(),
        );
        match result {
            Ok(output) => Ok(Some(
                self.block_on(output.body.collect())
                    .map_err(|e| StorageError::transient(format!("s3 body read: {e}")))?
                    .into_bytes()
                    .to_vec(),
            )),
            Err(error) => {
                if is_not_found(&error) {
                    Ok(None)
                } else {
                    Err(map_sdk_error("get object range", error))
                }
            }
        }
    }

    /// Opens a streaming GET from `start` to the end of the object.
    fn get_object_stream(
        &self,
        key: &str,
        start: u64,
    ) -> Result<Option<aws_sdk_s3::primitives::ByteStream>, StorageError> {
        let result = self.block_on(
            self.client
                .get_object()
                .bucket(&self.bucket)
                .key(key)
                .range(format!("bytes={start}-"))
                .send(),
        );
        match result {
            Ok(output) => Ok(Some(output.body)),
            Err(error) => {
                if is_not_found(&error) {
                    Ok(None)
                } else {
                    Err(map_sdk_error("get object stream", error))
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

    /// Entity ids of a scope: keys directly below the scope prefix whose
    /// first segment is a 2-hex bucket and whose second is an 8-hex entity
    /// name. Child scopes (deeper keys) are skipped.
    fn list_entities(&self, scope: &ScopePath) -> Result<Vec<EntityId>, StorageError> {
        let prefix = self.scope_prefix(scope);
        let mut entities = Vec::new();
        for key in self.list_keys(&prefix)? {
            let relative = &key[prefix.len()..];
            let mut segments = relative.split('/');
            let (Some(bucket), Some(name), None) =
                (segments.next(), segments.next(), segments.next())
            else {
                continue; // deeper keys belong to child scopes
            };
            if bucket.len() == 2
                && bucket.bytes().all(|b| b.is_ascii_hexdigit())
                && name.len() == 8
                && name.bytes().all(|b| b.is_ascii_hexdigit())
            {
                let id = u32::from_str_radix(name, 16).map_err(|_| {
                    StorageError::corrupt(format!("unparseable entity key {key} in {scope}"))
                })?;
                entities.push(id);
            }
        }
        entities.sort_unstable();
        Ok(entities)
    }

    fn delete_keys(&self, keys: &[String]) -> Result<(), StorageError> {
        use aws_sdk_s3::types::{Delete, ObjectIdentifier};
        for chunk in keys.chunks(1000) {
            let mut identifiers = Vec::with_capacity(chunk.len());
            for key in chunk {
                let identifier = ObjectIdentifier::builder().key(key).build().map_err(|e| {
                    StorageError::failed(format!("invalid object key {key}: {e}"))
                })?;
                identifiers.push(identifier);
            }
            let delete = Delete::builder()
                .set_objects(Some(identifiers))
                .quiet(true)
                .build()
                .map_err(|e| StorageError::failed(format!("delete request: {e}")))?;
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
/// a general failure.
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
    StorageError::failed(format!("s3 {operation}: {error}"))
}

fn parse_seal_record(bytes: &[u8], scope: &ScopePath) -> Result<SealRecord, StorageError> {
    if bytes.len() != SEAL_RECORD_LEN {
        return Err(StorageError::corrupt(format!(
            "seal record of scope {scope} has wrong length"
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

/// Encodes a stored object: `[8B BE payload length][32B digest][payload]`.
fn encode_object(bytes: &[u8], digest: &Digest32) -> Vec<u8> {
    let mut contents = Vec::with_capacity(ENTITY_HEADER_BYTES + bytes.len());
    contents.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
    contents.extend_from_slice(digest);
    contents.extend_from_slice(bytes);
    contents
}

/// Splits a stored object into its recorded digest header and payload,
/// validating the length header against the actual size.
fn split_object<'a>(bytes: &'a [u8], scope: &ScopePath) -> Result<(Digest32, &'a [u8]), StorageError> {
    if bytes.len() < ENTITY_HEADER_BYTES {
        return Err(StorageError::corrupt(format!(
            "object in {scope} is {} bytes, shorter than the entity header",
            bytes.len()
        )));
    }
    let mut len = [0u8; 8];
    len.copy_from_slice(&bytes[..8]);
    let len = u64::from_be_bytes(len);
    if len != (bytes.len() - ENTITY_HEADER_BYTES) as u64 {
        return Err(StorageError::corrupt(format!(
            "object in {scope} length differs from its header"
        )));
    }
    let mut digest: Digest32 = [0; 32];
    digest.copy_from_slice(&bytes[8..ENTITY_HEADER_BYTES]);
    Ok((digest, &bytes[ENTITY_HEADER_BYTES..]))
}

/// Minimum multipart part size (S3 requires ≥ 5 MiB for all but the last).
const PART_BYTES: usize = 5 << 20;

/// Streaming put writer for the S3 backend: payloads at or above
/// [`PART_BYTES`] go through multipart upload (parts flushed as they fill);
/// smaller streams land as one PUT at finish. Nothing is visible before
/// finish; aborts remove the upload.
pub struct S3PutWriter {
    client: Client,
    runtime: std::sync::Arc<tokio::runtime::Runtime>,
    bucket: String,
    key: String,
    seal_key: String,
    scope: ScopePath,
    expected: Digest32,
    /// Part 1 is deferred: it carries the entity header whose length field
    /// is only known at finish, so it uploads last (parts assemble by
    /// number, not by upload order).
    part1: Vec<u8>,
    pending: Vec<u8>,
    hasher: sha2::Sha256,
    upload_id: Option<String>,
    parts: Vec<aws_sdk_s3::types::CompletedPart>,
    part_no: i32,
    total: u64,
    absorbing: bool,
    done: bool,
}

impl S3PutWriter {
    /// Stages the entity header (8-byte length, patched at finish, plus the
    /// expected digest) ahead of the payload in part 1. Must run even for a
    /// zero-byte payload, whose writer never sees a write() call.
    fn ensure_header(&mut self) {
        if self.part_no == 0 && self.pending.is_empty() && self.total == 0 {
            self.pending.extend_from_slice(&0u64.to_be_bytes());
            self.pending.extend_from_slice(&self.expected);
        }
    }

    fn upload_part(
        &self,
        upload_id: &str,
        part_no: i32,
        body: Vec<u8>,
    ) -> Result<aws_sdk_s3::types::CompletedPart, StorageError> {
        let output = self
            .runtime
            .block_on(
                self.client
                    .upload_part()
                    .bucket(&self.bucket)
                    .key(&self.key)
                    .upload_id(upload_id)
                    .part_number(part_no)
                    .body(aws_sdk_s3::primitives::ByteStream::from(body))
                    .send(),
            )
            .map_err(|e| map_sdk_error("upload part", e))?;
        Ok(aws_sdk_s3::types::CompletedPart::builder()
            .part_number(part_no)
            .e_tag(output.e_tag().unwrap_or_default())
            .build())
    }

    fn flush_pending(&mut self) -> Result<(), StorageError> {
        if self.pending.len() < PART_BYTES {
            return Ok(());
        }
        let chunk: Vec<u8> = self.pending.drain(..PART_BYTES).collect();
        if self.part_no == 0 {
            // The first part is stashed in memory, not uploaded yet.
            self.part1 = chunk;
            self.part_no = 1;
            return Ok(());
        }
        let upload_id = match &self.upload_id {
            Some(id) => id.clone(),
            None => {
                let output = self
                    .runtime
                    .block_on(
                        self.client
                            .create_multipart_upload()
                            .bucket(&self.bucket)
                            .key(&self.key)
                            .send(),
                    )
                    .map_err(|e| map_sdk_error("create multipart upload", e))?;
                let id = output.upload_id().unwrap_or_default().to_string();
                self.upload_id = Some(id.clone());
                id
            }
        };
        self.part_no += 1;
        let part_no = self.part_no;
        let part = self.upload_part(&upload_id, part_no, chunk)?;
        self.parts.push(part);
        Ok(())
    }

    fn abort(&mut self) {
        if let Some(upload_id) = self.upload_id.take() {
            let _ = self.runtime.block_on(
                self.client
                    .abort_multipart_upload()
                    .bucket(&self.bucket)
                    .key(&self.key)
                    .upload_id(upload_id)
                    .send(),
            );
        }
    }
}

impl std::io::Write for S3PutWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.absorbing {
            return Ok(buf.len());
        }
        self.ensure_header();
        self.hasher.update(buf);
        self.total += buf.len() as u64;
        self.pending.extend_from_slice(buf);
        self.flush_pending()
            .map_err(|e| io::Error::other(format!("s3 multipart write: {e}")))?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl PutWriter for S3PutWriter {
    fn finish(mut self) -> Result<(), StorageError> {
        if self.absorbing {
            return Ok(());
        }
        self.ensure_header();
        let digest = self.hasher.finalize_reset();
        if digest[..] != self.expected[..] {
            self.abort();
            return Err(StorageError::corrupt(format!(
                "digest mismatch for entity in {}",
                self.scope
            )));
        }
        self.done = true;
        // Seal is re-checked: a seal may have landed mid-stream.
        let sealed = self
            .runtime
            .block_on(
                self.client
                    .get_object()
                    .bucket(&self.bucket)
                    .key(&self.seal_key)
                    .send(),
            )
            .map(|_| ())
            .is_ok();
        if sealed {
            self.abort();
            return Err(StorageError::already_sealed(format!(
                "scope {} is sealed",
                self.scope
            )));
        }
        if self.part_no == 0 {
            // Small stream: the pending buffer already carries the entity
            // header; patch its length and PUT as-is.
            self.pending[..8].copy_from_slice(&self.total.to_be_bytes());
            let bytes = std::mem::take(&mut self.pending);
            return self
                .runtime
                .block_on(
                    self.client
                        .put_object()
                        .bucket(&self.bucket)
                        .key(&self.key)
                        .body(aws_sdk_s3::primitives::ByteStream::from(bytes))
                        .send(),
                )
                .map(|_| ())
                .map_err(|e| map_sdk_error("put object", e));
        }
        // Patch part 1's length field now that the total is known, then
        // upload deferred part 1 and the remainder as the final part.
        self.part1[..8].copy_from_slice(&self.total.to_be_bytes());
        // The upload is created lazily on the second part's flush, so a
        // payload between one and two parts arrives here with none yet.
        let upload_id = match self.upload_id.take() {
            Some(id) => id,
            None => {
                let output = self
                    .runtime
                    .block_on(
                        self.client
                            .create_multipart_upload()
                            .bucket(&self.bucket)
                            .key(&self.key)
                            .send(),
                    )
                    .map_err(|e| map_sdk_error("create multipart upload", e))?;
                output.upload_id().unwrap_or_default().to_string()
            }
        };
        let part1 = std::mem::take(&mut self.part1);
        let first = self.upload_part(&upload_id, 1, part1)?;
        let mut parts = vec![first];
        if !self.pending.is_empty() {
            let part_no = self.part_no + 1;
            let remainder = std::mem::take(&mut self.pending);
            parts.push(self.upload_part(&upload_id, part_no, remainder)?);
        }
        parts.append(&mut self.parts);
        parts.sort_by_key(|part| part.part_number());
        let completed = aws_sdk_s3::types::CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();
        self.runtime
            .block_on(
                self.client
                    .complete_multipart_upload()
                    .bucket(&self.bucket)
                    .key(&self.key)
                    .upload_id(upload_id)
                    .multipart_upload(completed)
                    .send(),
            )
            .map(|_| ())
            .map_err(|e| map_sdk_error("complete multipart upload", e))
    }
}

impl Drop for S3PutWriter {
    fn drop(&mut self) {
        if !self.done {
            self.abort();
        }
    }
}

/// S3 backend reader: small objects snapshot at open; large objects stream
/// the body, so reads stay bounded in memory.
pub struct S3Reader {
    current: io::Cursor<Vec<u8>>,
    body: Option<aws_sdk_s3::primitives::ByteStream>,
    runtime: std::sync::Arc<tokio::runtime::Runtime>,
}

impl std::fmt::Debug for S3Reader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Reader").finish_non_exhaustive()
    }
}

impl Read for S3Reader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.current.position() == self.current.get_ref().len() as u64 {
            match &mut self.body {
                Some(body) => {
                    let next = self.runtime.block_on(body.next());
                    match next {
                        Some(Ok(bytes)) => {
                            self.current = io::Cursor::new(bytes.to_vec());
                        }
                        Some(Err(error)) => {
                            return Err(io::Error::other(format!("s3 body read: {error}")));
                        }
                        None => return Ok(0),
                    }
                }
                None => return Ok(0),
            }
        }
        self.current.read(buf)
    }
}

impl ObjectStore for S3Store {
    type Reader = S3Reader;
    type PutWriter = S3PutWriter;

    /// Buffers content (MongoDB document inserts are whole-value anyway)
    /// and inserts at finish, re-checking the seal and conflicts.
    fn begin_put(
        &mut self,
        scope: &ScopePath,
        entity: EntityId,
        expected_digest: &Digest32,
    ) -> Result<Self::PutWriter, StorageError> {
        layout::check_entity_id(entity)?;
        if self.get_object(&self.seal_key(scope))?.is_some() {
            return Err(StorageError::already_sealed(format!(
                "scope {scope} is sealed"
            )));
        }
        let key = self.object_key(scope, entity);
        if let Some(existing) = self.get_object(&key)? {
            let (recorded, _) = split_object(&existing, scope)?;
            if recorded == *expected_digest {
                return Ok(S3PutWriter {
                    client: self.client.clone(),
                    runtime: std::sync::Arc::clone(
                        self.runtime.as_ref().expect("s3 store runtime"),
                    ),
                    bucket: self.bucket.clone(),
                    key,
                    seal_key: self.seal_key(scope),
                    scope: scope.clone(),
                    expected: *expected_digest,
                    part1: Vec::new(),
                    pending: Vec::new(),
                    hasher: sha2::Sha256::new(),
                    upload_id: None,
                    parts: Vec::new(),
                    part_no: 0,
                    total: 0,
                    absorbing: true,
                    done: false,
                });
            }
            return Err(StorageError::corrupt(format!(
                "conflicting bytes for entity {entity} in {scope}"
            )));
        }
        Ok(S3PutWriter {
            client: self.client.clone(),
            runtime: std::sync::Arc::clone(self.runtime.as_ref().expect("s3 store runtime")),
            bucket: self.bucket.clone(),
            key,
            seal_key: self.seal_key(scope),
            scope: scope.clone(),
            expected: *expected_digest,
            part1: Vec::new(),
            pending: Vec::new(),
            hasher: sha2::Sha256::new(),
            upload_id: None,
            parts: Vec::new(),
            part_no: 0,
            total: 0,
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
        // Seal is checked before the digest: a sealed scope rejects every
        // put regardless of bytes.
        if self.get_object(&self.seal_key(scope))?.is_some() {
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
        let key = self.object_key(scope, entity);
        if let Some(existing) = self.get_object(&key)? {
            let (_, payload) = split_object(&existing, scope)?;
            if payload == bytes {
                return Ok(());
            }
            return Err(StorageError::corrupt(format!(
                "conflicting bytes for entity {entity} in {scope}"
            )));
        }
        self.put_object(&key, encode_object(bytes, &digest))
    }

    fn open_read(&self, scope: &ScopePath, entity: EntityId) -> Result<S3Reader, StorageError> {
        let key = self.object_key(scope, entity);
        // Header validation needs the digest prefix only; the payload body
        // streams from the byte after it, bounded in memory.
        let header = self
            .get_object_range(&key, 0, (ENTITY_HEADER_BYTES - 1) as u64)?
            .ok_or_else(|| StorageError::not_found(format!("entity {entity} is absent from scope {scope}")))?;
        if header.len() != ENTITY_HEADER_BYTES {
            return Err(StorageError::corrupt(format!(
                "entity header in {scope} is truncated"
            )));
        }
        // A zero-length payload ends at the header; a ranged GET from the
        // end of the object is an S3 InvalidRange error, not an empty body.
        let declared = u64::from_be_bytes(header[..8].try_into().expect("header length field"));
        if declared == 0 {
            return Ok(S3Reader {
                current: io::Cursor::new(Vec::new()),
                body: None,
                runtime: std::sync::Arc::clone(self.runtime.as_ref().expect("s3 store runtime")),
            });
        }
        let body = self.get_object_stream(&key, ENTITY_HEADER_BYTES as u64)?;
        match body {
            Some(body) => Ok(S3Reader {
                current: io::Cursor::new(Vec::new()),
                body: Some(body),
                runtime: std::sync::Arc::clone(self.runtime.as_ref().expect("s3 store runtime")),
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
        let key = self.seal_key(scope);
        if let Some(existing) = self.get_object(&key)? {
            let record = parse_seal_record(&existing, scope)?;
            if record.digest == *digest && record.entity_count == entity_count {
                return Ok(());
            }
            return Err(StorageError::corrupt(format!(
                "scope {scope} already carries a different seal"
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
            .ok_or_else(|| StorageError::not_found(format!("scope {scope} has no seal record")))?;
        let seal = parse_seal_record(&seal_bytes, scope)?;
        let entities = self.list_entities(scope)?;
        if entities.len() as u64 != seal.entity_count {
            return Err(StorageError::corrupt(format!(
                "scope {scope} entity count differs from its seal"
            )));
        }
        for entity in entities {
            let bytes = self
                .get_object(&self.object_key(scope, entity))?
                .ok_or_else(|| {
                    StorageError::corrupt(format!("entity {entity} vanished under verify"))
                })?;
            let (recorded, payload) = split_object(&bytes, scope)?;
            if layout::sha256(payload) != recorded {
                return Err(StorageError::corrupt(format!(
                    "entity {entity} in {scope} fails its recorded digest"
                )));
            }
        }
        Ok(seal)
    }

    fn list(&self, scope: &ScopePath) -> Result<Vec<EntityId>, StorageError> {
        self.list_entities(scope)
    }

    fn list_scopes(&self) -> Result<Vec<ScopePath>, StorageError> {
        // Entity keys:  <prefix>objects/<path>/<bucket>/<entity>
        // Seal keys:    <prefix>objects/<path>/scope.seal
        let base = format!("{}{}/", self.prefix, OBJECTS_DIR);
        let mut paths = std::collections::BTreeSet::new();
        for key in self.list_keys(&base)? {
            let relative = &key[base.len()..];
            let segments: Vec<&str> = relative.split('/').collect();
            // Scope path = all segments above the bucket/entity pair or the
            // seal file; empty means the root-adjacent prefix (no scopes).
            let scope_path: Option<String> = match segments.as_slice() {
                [rest @ .., bucket, _entity]
                    if bucket.len() == 2
                        && bucket.bytes().all(|b| b.is_ascii_hexdigit()) =>
                {
                    Some(rest.join("/"))
                }
                [rest @ .., seal] if *seal == SEAL_FILE_NAME => Some(rest.join("/")),
                _ => None,
            };
            if let Some(rendered) = scope_path
                && !rendered.is_empty()
            {
                paths.insert(rendered);
            }
        }
        paths
            .iter()
            .map(|path| ScopePath::parse(path))
            .collect()
    }

    fn retire(&mut self, scope: &ScopePath, retention_cursor: u64) -> Result<u64, StorageError> {
        let seal_key = self.seal_key(scope);
        let sealed = self.get_object(&seal_key)?.is_some();
        let entities = self.list_entities(scope)?;
        if entities.is_empty() && !sealed {
            return Err(StorageError::not_found(format!(
                "scope {scope} holds nothing to retire"
            )));
        }
        // A sealed scope retires whole — scope-locally: only the scope's own
        // entity keys and the seal key are deleted; child scopes live deeper
        // under the same prefix and are untouched.
        let cursor = if sealed { u64::MAX } else { retention_cursor };
        let mut doomed: Vec<String> = Vec::new();
        let mut removed = 0u64;
        for entity in entities {
            if u64::from(entity) <= cursor {
                doomed.push(self.object_key(scope, entity));
                removed += 1;
            }
        }
        if sealed {
            doomed.push(seal_key);
        }
        self.delete_keys(&doomed)?;
        Ok(removed)
    }
}

impl Drop for S3Store {
    /// tokio runtimes must not drop on a runtime driver thread; the final
    /// drop is handed to a plain thread.
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            let _ = std::thread::Builder::new()
                .name("s3-runtime-drop".to_string())
                .spawn(move || drop(runtime));
        }
    }
}
