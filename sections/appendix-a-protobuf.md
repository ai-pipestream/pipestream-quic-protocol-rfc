# Appendix A: Protobuf Schema Reference

This appendix defines the Protocol Buffers (proto3) message schemas used by the PipeStream protocol. All messages use proto3 syntax and follow the wire encoding rules defined in Section A.3.

PipeStream organizes its protobuf messages into two categories: protocol-level messages (Section A.1) used for entity transport, framing, and coordination; and entity data messages (Section A.2) that define the payload types carried within entities. The protocol defines three layers -- Layer 0 (Core), Layer 1 (Recursive), and Layer 2 (Resilience) -- and messages are annotated with the layer that introduces them.

## A.1 Protocol-Level Messages

This section defines the core protocol messages used for capability negotiation, entity transport, framing, ledger tracking, and coordination in PipeStream.

### A.1.1 Capabilities

The Capabilities message is exchanged during CONNECT to negotiate supported protocol layers and operational limits. Peers negotiate down to common capabilities. If Layer 2 is requested but Layer 1 is not supported, Layer 2 MUST be disabled.

```protobuf
syntax = "proto3";

package pipestream.protocol.v1;

import "google/protobuf/any.proto";

// Capabilities exchanged during connection establishment.
// Peers negotiate down to common supported features.
message Capabilities {
  // Layer 0 (Core) support. Always true.
  bool layer0_core = 1;

  // Layer 1 (Recursive) support: scoped IDs, digest propagation.
  bool layer1_recursive = 2;

  // Layer 2 (Resilience) support: yield, claim checks.
  // Requires layer1_recursive = true.
  bool layer2_resilience = 3;

  // Maximum scope nesting depth. Default: 8.
  uint32 max_scope_depth = 4;

  // Maximum entities per scope. Default: 1,048,576 (2^20).
  uint32 max_entities_per_scope = 5;

  // Maximum entity ID window size before backpressure.
  // Default: 524,288 (2^19).
  uint32 max_window_size = 6;
}
```

**Field Semantics:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| layer0_core | bool | REQUIRED | true | Layer 0 support (always true) |
| layer1_recursive | bool | OPTIONAL | false | Layer 1 support |
| layer2_resilience | bool | OPTIONAL | false | Layer 2 support (requires Layer 1) |
| max_scope_depth | uint32 | OPTIONAL | 8 | Maximum nesting depth for scopes |
| max_entities_per_scope | uint32 | OPTIONAL | 1,048,576 | Memory bounds per scope |
| max_window_size | uint32 | OPTIONAL | 524,288 | Backpressure threshold |

### A.1.2 EntityHeader

The EntityHeader message provides metadata for each entity transmitted over a PipeStream connection. It MUST be present at the beginning of each entity transmission. Compared to draft-00, the header now uses uint32 entity_id and parent_id (20-bit local IDs within a scope), adds a scope_id field (Layer 1), uses a bytes checksum (fixed 32-byte SHA-256), includes ChunkInfo as an embedded message, and carries an optional CompletionPolicy (Layer 2).

```protobuf
// EntityHeader provides metadata for entity transmission.
// This header MUST precede the entity payload on the wire.
message EntityHeader {
  // Scope-local entity identifier (20-bit unsigned integer).
  // MUST be unique within its scope.
  uint32 entity_id = 1;

  // Parent entity identifier. 0 for root entities.
  uint32 parent_id = 2;

  // Scope identifier (Layer 1). Identifies the hierarchical
  // namespace for this entity's ID space.
  uint32 scope_id = 3;

  // Data layer index (0-3):
  //   0 = BlobBag, 1 = SemanticLayer,
  //   2 = ParsedData, 3 = CustomEntity
  uint32 layer = 4;

  // MIME type of the entity payload.
  // MUST conform to RFC 6838 media type format.
  string content_type = 5;

  // Length of the entity payload in bytes.
  uint64 payload_length = 6;

  // SHA-256 checksum of the payload (exactly 32 bytes).
  bytes checksum = 7;

  // Extensible key-value metadata associated with this entity.
  // Keys MUST be ASCII strings. Values MUST be UTF-8 strings.
  map<string, string> metadata = 8;

  // Chunking information when payload exceeds MTU.
  ChunkInfo chunk_info = 9;

  // Completion policy for vaporization (Layer 2).
  CompletionPolicy completion_policy = 10;
}
```

**Field Semantics:**

| Field | Type | Required | Layer | Description |
|-------|------|----------|-------|-------------|
| entity_id | uint32 | REQUIRED | 0 | 20-bit scope-local entity identifier |
| parent_id | uint32 | REQUIRED | 0 | Parent entity ID (0 = root) |
| scope_id | uint32 | OPTIONAL | 1 | Scope identifier for hierarchical namespacing |
| layer | uint32 | REQUIRED | 0 | Data layer index (0-3) |
| content_type | string | REQUIRED | 0 | MIME type per RFC 6838 |
| payload_length | uint64 | REQUIRED | 0 | Payload size in bytes |
| checksum | bytes | REQUIRED | 0 | SHA-256 integrity checksum (32 bytes) |
| metadata | map | OPTIONAL | 0 | Extensible key-value pairs |
| chunk_info | ChunkInfo | OPTIONAL | 0 | Chunking metadata for large payloads |
| completion_policy | CompletionPolicy | OPTIONAL | 2 | Failure handling policy |

### A.1.3 ChunkInfo

The ChunkInfo message provides chunking metadata when an entity payload exceeds the maximum transmission unit (MTU) and must be split across multiple QUIC frames.

```protobuf
// ChunkInfo describes the position of a chunk within
// a larger entity payload.
message ChunkInfo {
  // Total number of chunks comprising the complete entity.
  // MUST be greater than 0.
  uint32 total_chunks = 1;

  // Zero-based index of this chunk within the sequence.
  // MUST be in range [0, total_chunks - 1].
  uint32 chunk_index = 2;

  // Byte offset of this chunk's data within the complete
  // entity payload. The first chunk has offset 0.
  uint64 chunk_offset = 3;
}
```

**Chunking Requirements:**

1. When `total_chunks` equals 1, the ChunkInfo MAY be omitted.
2. Receivers MUST buffer chunks until all are received.
3. Chunks MAY arrive out of order; receivers MUST reassemble using `chunk_index`.
4. If any chunk is missing after a timeout period, the receiver MUST request retransmission or abort the entity.

### A.1.4 CompletionPolicy (Layer 2)

The CompletionPolicy message specifies how to handle partial failures during vaporization. It is carried in the EntityHeader of a parent entity being decomposed and governs the behavior when child entities fail, time out, or are deferred.

```protobuf
// CompletionPolicy configures failure handling for vaporization.
message CompletionPolicy {
  // Completion mode governing success criteria.
  CompletionMode mode = 1;

  // Maximum retry attempts per child. Default: 3.
  uint32 max_retries = 2;

  // Delay between retries in milliseconds. Default: 1000.
  uint32 retry_delay_ms = 3;

  // Overall timeout in milliseconds. Default: 300000 (5 min).
  uint32 timeout_ms = 4;

  // Minimum success ratio for QUORUM mode (0.0-1.0).
  float min_success_ratio = 5;

  // Action to take when timeout_ms is exceeded.
  FailureAction on_timeout = 6;

  // Action to take when a child fails.
  FailureAction on_failure = 7;
}
```

### A.1.5 CompletionMode Enum

The CompletionMode enum defines the success criteria for a vaporization operation.

```protobuf
// Completion modes for vaporization.
enum CompletionMode {
  // All children MUST complete successfully.
  COMPLETION_MODE_STRICT = 0;

  // Continue processing with partial results.
  COMPLETION_MODE_LENIENT = 1;

  // Complete with whatever succeeds; no minimum threshold.
  COMPLETION_MODE_BEST_EFFORT = 2;

  // Require at least min_success_ratio of children to succeed.
  COMPLETION_MODE_QUORUM = 3;
}
```

| Value | Name | Description |
|-------|------|-------------|
| 0 | STRICT | All children MUST complete |
| 1 | LENIENT | Continue with partial results |
| 2 | BEST_EFFORT | Complete with whatever succeeds |
| 3 | QUORUM | Need min_success_ratio |

### A.1.6 FailureAction Enum

The FailureAction enum defines the action to take when a child entity fails or times out.

```protobuf
// Actions to take on child failure or timeout.
enum FailureAction {
  // Propagate failure to parent.
  FAILURE_ACTION_FAIL = 0;

  // Skip the failed child, continue with siblings.
  FAILURE_ACTION_SKIP = 1;

  // Retry the child up to max_retries.
  FAILURE_ACTION_RETRY = 2;

  // Create a claim check and continue processing.
  FAILURE_ACTION_DEFER = 3;
}
```

| Value | Name | Description |
|-------|------|-------------|
| 0 | FAIL | Propagate failure up |
| 1 | SKIP | Skip, continue with siblings |
| 2 | RETRY | Retry up to max_retries |
| 3 | DEFER | Create claim check, continue |

### A.1.7 EntityStatus Enum

The EntityStatus enum defines all possible processing states for an entity. Compared to draft-00, this enum has been expanded from 7 values to 12 values, covering the full lifecycle across all three protocol layers.

```protobuf
// Status values for entity processing.
enum EntityStatus {
  // Entity announced, not yet transmitting. (Layer 0)
  ENTITY_STATUS_PENDING = 0;

  // Entity transmission in progress. (Layer 0)
  ENTITY_STATUS_PROCESSING = 1;

  // Entity successfully processed. (Layer 0)
  ENTITY_STATUS_COMPLETE = 2;

  // Entity processing failed. (Layer 0)
  ENTITY_STATUS_FAILED = 3;

  // Synchronization barrier. (Layer 0)
  ENTITY_STATUS_CHECKPOINT = 4;

  // Decomposing into children. (Layer 0)
  ENTITY_STATUS_VAPORIZING = 5;

  // Rejoining children. (Layer 0)
  ENTITY_STATUS_AGGREGATING = 6;

  // Paused with continuation token. (Layer 2)
  ENTITY_STATUS_YIELDED = 7;

  // Detached with claim check. (Layer 2)
  ENTITY_STATUS_DEFERRED = 8;

  // Retry in progress. (Layer 2)
  ENTITY_STATUS_RETRYING = 9;

  // Intentionally skipped (lenient mode). (Layer 2)
  ENTITY_STATUS_SKIPPED = 10;

  // Timed out, cursor advanced past. (Layer 2)
  ENTITY_STATUS_ABANDONED = 11;
}
```

**Status Code Summary:**

| Value | Name | Layer | Description |
|-------|------|-------|-------------|
| 0 | PENDING | 0 | Entity announced, not yet transmitting |
| 1 | PROCESSING | 0 | Entity transmission in progress |
| 2 | COMPLETE | 0 | Entity successfully processed |
| 3 | FAILED | 0 | Entity processing failed |
| 4 | CHECKPOINT | 0 | Synchronization barrier |
| 5 | VAPORIZING | 0 | Decomposing into children |
| 6 | AGGREGATING | 0 | Rejoining children |
| 7 | YIELDED | 2 | Paused with continuation token |
| 8 | DEFERRED | 2 | Detached with claim check |
| 9 | RETRYING | 2 | Retry in progress |
| 10 | SKIPPED | 2 | Intentionally skipped (lenient mode) |
| 11 | ABANDONED | 2 | Timed out, cursor advanced past |

### A.1.8 LedgerFrame

The LedgerFrame message reports the processing status of an entity on the Ledger Stream. In draft-01, a scope_id field has been added for hierarchical scoping (Layer 1).

```protobuf
// LedgerFrame reports the processing status of an entity.
message LedgerFrame {
  // Entity identifier this frame references (20-bit local ID).
  uint32 entity_id = 1;

  // Scope identifier (Layer 1). 0 for root scope.
  uint32 scope_id = 2;

  // Current processing status of the entity.
  EntityStatus status = 3;

  // Extended status data. Content depends on status:
  // - FAILED: Contains error details
  // - YIELDED: Contains YieldToken
  // - DEFERRED: Contains ClaimCheck
  google.protobuf.Any extended_data = 4;
}
```

### A.1.9 CheckpointFrame

The CheckpointFrame message establishes synchronization points for processing consistency and crash recovery.

```protobuf
// CheckpointFrame establishes a synchronization point.
message CheckpointFrame {
  // Unique identifier for this checkpoint.
  // MUST be monotonically increasing within a stream.
  string checkpoint_id = 1;

  // Sequence number establishing ordering relationship
  // with entity transmissions. All entities with sequence
  // numbers less than this value are included in the checkpoint.
  uint64 sequence_number = 2;

  // Bitfield of checkpoint behavior flags.
  uint32 flags = 3;

  // Maximum time in milliseconds to wait for checkpoint
  // confirmation before considering it failed.
  // Value of 0 indicates no timeout (wait indefinitely).
  uint32 timeout_ms = 4;
}
```

### A.1.10 PartsLedgerEntry

The PartsLedgerEntry message tracks the decomposition of a parent entity into child entities, supporting recursive processing patterns. In draft-01, this message uses uint32 IDs (matching the 20-bit entity ID space), adds scope_id (Layer 1), children_status tracking, CompletionPolicy (Layer 2), and a creation timestamp.

```protobuf
// PartsLedgerEntry tracks parent-child entity relationships.
message PartsLedgerEntry {
  // Identifier of the parent entity that was decomposed (20-bit).
  uint32 parent_id = 1;

  // Scope identifier (Layer 1).
  uint32 scope_id = 2;

  // List of child entity identifiers (20-bit each).
  // Order corresponds to derivation sequence.
  repeated uint32 children_ids = 3;

  // Per-child status tracking. Indices correspond to children_ids.
  repeated EntityStatus children_status = 4;

  // Completion policy governing failure handling (Layer 2).
  CompletionPolicy policy = 5;

  // Timestamp when this entry was created (Unix epoch millis).
  uint64 created_at = 6;
}
```

**Recursive Processing Model:**

When an entity is decomposed (vaporized) into child entities, the PartsLedgerEntry maintains the relationship:

```
Parent Entity (parent_id: 42, scope_id: 1)
    |
    +-- PartsLedgerEntry
            parent_id: 42
            scope_id: 1
            children_ids: [43, 44, 45]
            children_status: [COMPLETE, COMPLETE, PROCESSING]
            policy: { mode: STRICT }
            created_at: 1708300800000
```

### A.1.11 YieldToken (Layer 2)

The YieldToken message carries the continuation state for a yielded entity, enabling processing to be paused and resumed without reprocessing.

```protobuf
// YieldToken carries continuation state for a paused entity.
message YieldToken {
  // Reason the entity was yielded.
  YieldReason reason = 1;

  // Opaque continuation state for resumption.
  bytes continuation_state = 2;

  // Validation data for the stopping point.
  StoppingPointValidation validation = 3;
}
```

### A.1.12 YieldReason Enum (Layer 2)

```protobuf
// Reasons for yielding entity processing.
enum YieldReason {
  // Waiting on an external service call.
  YIELD_REASON_EXTERNAL_CALL = 0;

  // Voluntary throttle due to rate limiting.
  YIELD_REASON_RATE_LIMITED = 1;

  // Waiting for a specific sibling entity to complete.
  YIELD_REASON_AWAITING_SIBLING = 2;

  // Waiting for human or workflow approval.
  YIELD_REASON_AWAITING_APPROVAL = 3;

  // Blocked on a semaphore or lock.
  YIELD_REASON_RESOURCE_BUSY = 4;
}
```

| Value | Name | Description |
|-------|------|-------------|
| 0 | EXTERNAL_CALL | Waiting on external service |
| 1 | RATE_LIMITED | Voluntary throttle |
| 2 | AWAITING_SIBLING | Waiting for specific sibling |
| 3 | AWAITING_APPROVAL | Human/workflow gate |
| 4 | RESOURCE_BUSY | Semaphore/lock contention |

### A.1.13 ClaimCheck (Layer 2)

The ClaimCheck message provides a detached reference to a deferred entity that can be queried or resumed independently, potentially in a different session.

```protobuf
// ClaimCheck provides a detached reference to a deferred entity.
message ClaimCheck {
  // Globally unique claim check identifier.
  uint64 claim_id = 1;

  // Entity ID of the deferred entity (20-bit).
  uint32 entity_id = 2;

  // Scope ID of the deferred entity (Layer 1).
  uint32 scope_id = 3;

  // Expiry timestamp (Unix epoch seconds).
  // After expiry, the claim check MAY be garbage collected.
  uint64 expiry_timestamp = 4;

  // Validation data for the stopping point.
  StoppingPointValidation validation = 5;
}
```

### A.1.14 StoppingPointValidation (Layer 2)

The StoppingPointValidation message provides integrity and progress data when yielding or deferring an entity, enabling safe resumption.

```protobuf
// StoppingPointValidation captures processing progress.
message StoppingPointValidation {
  // Hash of the processing state at the stopping point.
  bytes state_checksum = 1;

  // Number of bytes processed so far.
  uint64 bytes_processed = 2;

  // Number of children that have completed.
  uint32 children_complete = 3;

  // Total number of children expected.
  uint32 children_total = 4;

  // Whether processing can be resumed from this point.
  bool is_resumable = 5;

  // Reference to the last satisfied checkpoint.
  string checkpoint_ref = 6;
}
```

### A.1.15 ScopeDigest (Layer 1)

The ScopeDigest message provides a cryptographic summary of all entity statuses within a completed scope. It is propagated to parent scopes for efficient subtree verification. The Merkle root is computed as SHA-256 over all child ledger entries in Entity ID order.

```protobuf
// ScopeDigest summarizes a completed scope's processing results.
message ScopeDigest {
  // Scope identifier.
  uint32 scope_id = 1;

  // Total entities processed within this scope.
  uint64 entities_processed = 2;

  // Number of entities that succeeded.
  uint64 entities_succeeded = 3;

  // Number of entities that failed.
  uint64 entities_failed = 4;

  // Number of entities that were deferred (Layer 2).
  uint64 entities_deferred = 5;

  // SHA-256 Merkle root over all child ledger entries.
  bytes merkle_root = 6;
}
```

### A.1.16 ResolutionState Enum

The ResolutionState enum tracks the overall resolution status of a PartsLedgerEntry.

```protobuf
// Resolution state for Parts Ledger entries.
enum ResolutionState {
  // Entry is still being processed.
  RESOLUTION_STATE_ACTIVE = 0;

  // All children resolved successfully.
  RESOLUTION_STATE_RESOLVED = 1;

  // Some children failed or were skipped.
  RESOLUTION_STATE_PARTIAL = 2;

  // Entry failed.
  RESOLUTION_STATE_FAILED = 3;
}
```

## A.2 Entity Data Messages

This section defines the payload message types transmitted as entity content within PipeStream. Messages are organized by data layer.

### A.2.1 PipeDoc

The PipeDoc message is the primary document container for pipeline processing. It encapsulates document content, metadata, and processing artifacts across all four data layers. In draft-01, a semantic_result field has been added (field 6) and ownership/doc_id_derivation have been renumbered to fields 7 and 8.

```protobuf
syntax = "proto3";

package pipestream.data.v1;

import "google/protobuf/any.proto";
import "google/protobuf/struct.proto";
import "google/protobuf/timestamp.proto";

// PipeDoc is the primary document container for pipeline processing.
message PipeDoc {
  // Unique identifier for this document across the entire system.
  string doc_id = 1;

  // Standardized search metadata for indexing and querying.
  SearchMetadata search_metadata = 2;

  // Binary content container for document attachments (Layer 0).
  BlobBag blob_bag = 3;

  // Customer-provided structured data in any protobuf format (Layer 3).
  google.protobuf.Any structured_data = 4;

  // Parser output metadata keyed by parser identifier (Layer 2).
  map<string, ParsedMetadata> parsed_metadata = 5;

  // Semantic processing result with chunks and embeddings (Layer 1).
  SemanticProcessingResult semantic_result = 6;

  // Ownership tracking for multi-tenant environments.
  optional OwnershipContext ownership = 7;

  // Derivation method for the doc_id (for auditability).
  optional DocIdDerivation doc_id_derivation = 8;
}
```

**Field Semantics:**

| Field | Type | Number | Required | Description |
|-------|------|--------|----------|-------------|
| doc_id | string | 1 | REQUIRED | Unique document identifier |
| search_metadata | SearchMetadata | 2 | OPTIONAL | Search engine metadata |
| blob_bag | BlobBag | 3 | OPTIONAL | Binary content (Layer 0) |
| structured_data | Any | 4 | OPTIONAL | Custom structured data (Layer 3) |
| parsed_metadata | map | 5 | OPTIONAL | Parser outputs (Layer 2) |
| semantic_result | SemanticProcessingResult | 6 | OPTIONAL | Semantic chunks/embeddings (Layer 1) |
| ownership | OwnershipContext | 7 | OPTIONAL | Multi-tenancy tracking |
| doc_id_derivation | DocIdDerivation | 8 | OPTIONAL | ID derivation audit trail |

### A.2.2 BlobBag, Blobs, and Blob (Layer 0)

The BlobBag and Blob messages provide flexible binary content storage with support for both inline data and external storage references. The Blobs wrapper message holds a repeated collection of Blob instances.

```protobuf
// Container for one or more binary blobs.
message BlobBag {
  oneof blob_data {
    // Single blob content.
    Blob blob = 1;

    // Multiple blob contents.
    Blobs blobs = 2;
  }
}

// Collection of multiple blobs.
message Blobs {
  repeated Blob blobs = 1;
}

// Binary blob with flexible storage options.
message Blob {
  // Unique identifier for this blob.
  string blob_id = 1;

  // Drive/bucket identifier.
  string drive_id = 2;

  // Blob content: inline data or storage reference.
  oneof content {
    // Inline binary data (for small files).
    bytes data = 3;

    // Reference to external storage (for large files).
    FileStorageReference storage_ref = 4;
  }

  // MIME type of the blob content.
  optional string mime_type = 5;

  // Original filename if available.
  optional string filename = 6;

  // Size of the blob content in bytes.
  int64 size_bytes = 8;

  // Checksum value for integrity verification.
  optional string checksum = 9;

  // Type of checksum algorithm used.
  ChecksumType checksum_type = 10;
}

// Checksum algorithm types.
enum ChecksumType {
  CHECKSUM_TYPE_UNSPECIFIED = 0;
  CHECKSUM_TYPE_MD5 = 1;
  CHECKSUM_TYPE_SHA1 = 2;
  CHECKSUM_TYPE_SHA256 = 3;
  CHECKSUM_TYPE_SHA512 = 4;
}
```

### A.2.3 FileStorageReference and EncryptionMetadata

The FileStorageReference message provides cloud-agnostic references to externally stored blob data. It supports multiple storage providers (S3, Azure Blob, GCS, MinIO) and includes optional encryption metadata for at-rest encryption.

```protobuf
// Cloud-agnostic reference to external file storage.
message FileStorageReference {
  // Storage provider identifier: "s3", "azure", "gcs", "minio".
  string provider = 1;

  // Bucket or container name.
  string bucket = 2;

  // Object key or path within the bucket.
  string key = 3;

  // Optional region hint for the storage location.
  string region = 4;

  // Provider-specific attributes (e.g., storage class, ACL).
  map<string, string> attrs = 5;

  // Encryption metadata for at-rest encryption.
  EncryptionMetadata encryption = 6;
}

// Encryption metadata for data at rest.
message EncryptionMetadata {
  // Encryption algorithm: "AES-256-GCM", "AES-256-CBC".
  string algorithm = 1;

  // Key management provider: "aws-kms", "azure-keyvault",
  // "gcp-kms", "vault".
  string key_provider = 2;

  // Key ARN, URI, or identifier.
  string key_id = 3;

  // Optional client-side encrypted data encryption key (DEK).
  bytes wrapped_key = 4;

  // Initialization vector.
  bytes iv = 5;

  // Encryption context for key derivation.
  map<string, string> context = 6;
}
```

**Security Requirements for FileStorageReference:**

1. Key IDs MUST reference keys in approved providers.
2. Wrapped keys MUST use approved envelope encryption.
3. Key rotation MUST be supported via key_id versioning.
4. Implementations MUST NOT log key material.

### A.2.4 SemanticProcessingResult and SemanticChunk (Layer 1)

The SemanticProcessingResult message encapsulates the results of semantic processing (chunking and embedding). In draft-01, this is a simplified top-level result message containing chunks, the chunking strategy used, and processing metadata.

```protobuf
// Complete result of semantic processing.
message SemanticProcessingResult {
  // List of semantic chunks with embeddings.
  repeated SemanticChunk chunks = 1;

  // Name of the chunking strategy used
  // (e.g., "sliding_window", "sentence").
  string chunking_strategy = 2;

  // Processing metadata (timing, model info, etc.).
  map<string, string> processing_metadata = 3;
}

// Single semantic chunk with text, embedding, and annotations.
message SemanticChunk {
  // Unique identifier for this chunk.
  string chunk_id = 1;

  // Sequential number within parent result.
  int64 chunk_number = 2;

  // Text content and embedding vector.
  ChunkEmbedding embedding_info = 3;

  // Chunk-specific metadata.
  map<string, google.protobuf.Value> metadata = 4;

  // NLP annotations (NER, POS, sentiment, etc.).
  repeated NLPAnnotation annotations = 5;
}
```

**SemanticChunk Changes from draft-00:**

The SemanticChunk message adds a repeated `annotations` field (field 5) carrying NLPAnnotation instances for named entity recognition, part-of-speech tagging, sentiment analysis, and other NLP results.

### A.2.5 ChunkEmbedding

The ChunkEmbedding message carries the text content and vector embedding for a semantic chunk. In draft-01, a model_id field has been added (field 3) and the offset fields have been renumbered to fields 4 and 5.

```protobuf
// Text content and vector embedding for a chunk.
message ChunkEmbedding {
  // Actual text content of the chunk.
  string text_content = 1;

  // Vector embedding (floating-point values).
  repeated float vector = 2;

  // Identifier for the model that generated this embedding.
  optional string model_id = 3;

  // Character offset where chunk starts in original document.
  optional int32 original_char_start_offset = 4;

  // Character offset where chunk ends in original document.
  optional int32 original_char_end_offset = 5;
}
```

### A.2.6 NLPAnnotation (Layer 1)

The NLPAnnotation message represents a single NLP annotation on a semantic chunk, supporting named entity recognition, part-of-speech tagging, sentiment analysis, and other annotation types.

```protobuf
// NLP annotation on a semantic chunk.
message NLPAnnotation {
  // Annotation type: "NER", "POS", "SENTIMENT", etc.
  string type = 1;

  // Label value: "PERSON", "ORG", "POSITIVE", etc.
  string label = 2;

  // Character start offset within the chunk text.
  int32 start_offset = 3;

  // Character end offset within the chunk text.
  int32 end_offset = 4;

  // Confidence score (0.0-1.0).
  float confidence = 5;

  // Additional annotation-specific attributes.
  map<string, string> attributes = 6;
}
```

### A.2.7 ParsedMetadata, TableData, and TableRow (Layer 2)

The ParsedMetadata message wraps parser output with structured field extraction. In draft-01, this message has been redesigned with a parser_id, structured fields map, table extraction support, and raw output. The TableData and TableRow messages support tabular data extracted from documents.

```protobuf
// Parser output with structured field extraction.
message ParsedMetadata {
  // Identifier for the parser that produced this output.
  string parser_id = 1;

  // Extracted structured fields as key-value pairs.
  map<string, google.protobuf.Value> fields = 2;

  // Tables extracted from the document.
  repeated TableData tables = 3;

  // Raw parser output (e.g., full text extraction).
  string raw_output = 4;
}

// Tabular data extracted from a document.
message TableData {
  // Unique identifier for this table.
  string table_id = 1;

  // Column headers.
  repeated string headers = 2;

  // Table rows.
  repeated TableRow rows = 3;
}

// Single row within a table.
message TableRow {
  // Cell values in column order.
  repeated string cells = 1;
}
```

### A.2.8 SearchMetadata

The SearchMetadata message provides standardized fields for search engine indexing and document retrieval. In draft-01, this is a simplified message focused on the most common search fields.

```protobuf
// Standardized search engine metadata.
message SearchMetadata {
  // Document title or heading.
  string title = 1;

  // Extracted keywords for search and categorization.
  repeated string keywords = 2;

  // Document description or summary.
  string description = 3;

  // Custom fields for domain-specific search metadata.
  map<string, string> custom_fields = 4;
}
```

### A.2.9 OwnershipContext

The OwnershipContext message provides multi-tenant ownership tracking for documents processed through shared pipelines.

```protobuf
// Ownership context for multi-tenant document tracking.
message OwnershipContext {
  // Tenant identifier.
  string tenant_id = 1;

  // Owner identifier within the tenant.
  string owner_id = 2;

  // Access control list entries.
  repeated string acl = 3;
}
```

### A.2.10 DocIdDerivation

The DocIdDerivation message records how a document's doc_id was derived, providing an audit trail for identifier generation.

```protobuf
// Records how a doc_id was derived for auditability.
message DocIdDerivation {
  // Derivation strategy: "hash", "uuid", "composite", etc.
  string strategy = 1;

  // Source field used for derivation (e.g., "source_uri").
  string source_field = 2;

  // Hash algorithm used (e.g., "sha256", "murmur3").
  string hash_algorithm = 3;
}
```

## A.3 Wire Encoding Notes

This section specifies the wire encoding requirements for PipeStream protocol messages.

### A.3.1 Protocol Buffer Wire Format

All messages MUST use Protocol Buffers version 3 (proto3) wire format as specified in the Protocol Buffers Language Guide. The key encoding rules are:

**Wire Types:**

| Wire Type | Encoding | Used For |
|-----------|----------|----------|
| 0 | Varint | int32, int64, uint32, uint64, sint32, sint64, bool, enum |
| 1 | 64-bit | fixed64, sfixed64, double |
| 2 | Length-delimited | string, bytes, embedded messages, packed repeated fields |
| 5 | 32-bit | fixed32, sfixed32, float |

### A.3.2 Length-Delimited Message Framing

Each PipeStream message transmitted over QUIC MUST be framed as follows:

```
+----------------+------------------+
| Message Length | Message Payload  |
| (varint)       | (proto3 encoded) |
+----------------+------------------+
```

**Framing Rules:**

1. The message length is encoded as a varint indicating the number of bytes in the payload.
2. The message payload immediately follows the length prefix.
3. Implementations MUST support message payloads up to 16 MiB (16,777,216 bytes).
4. Messages exceeding the MTU MUST be chunked using ChunkInfo.

### A.3.3 Varint Encoding

Variable-length integers (varints) are encoded using the standard Protocol Buffers format:

1. Each byte uses 7 bits for data and 1 bit (MSB) as continuation flag.
2. Bytes are ordered little-endian (least significant group first).
3. The MSB of each byte indicates if more bytes follow (1) or not (0).

**Example:** The value 300 is encoded as `0xAC 0x02`:

```
300 = 0b100101100
    = 0b10 0101100

Byte 1: 1 0101100 = 0xAC (MSB=1, more bytes follow)
Byte 2: 0 0000010 = 0x02 (MSB=0, final byte)
```

### A.3.4 String Encoding

All string fields MUST be encoded as UTF-8 per RFC 3629. String fields are transmitted as length-delimited wire type 2:

1. String length in bytes is encoded as a varint.
2. UTF-8 encoded string content follows immediately.
3. Implementations MUST validate UTF-8 encoding on receipt.
4. Invalid UTF-8 sequences MUST cause message rejection.

### A.3.5 Map Field Encoding

Map fields are encoded as repeated key-value pair messages:

```protobuf
// map<string, string> metadata = 8;
// is equivalent to:
message MetadataEntry {
  string key = 1;
  string value = 2;
}
repeated MetadataEntry metadata = 8;
```

**Map Encoding Rules:**

1. Each map entry is a length-delimited message containing key (field 1) and value (field 2).
2. Map entries with duplicate keys: the last value wins.
3. Empty maps are represented by absence of the repeated field.

### A.3.6 Default Values and Field Presence

Proto3 default value semantics apply:

| Type | Default Value |
|------|---------------|
| string | Empty string ("") |
| bytes | Empty bytes |
| bool | false |
| numeric | 0 |
| enum | First defined value (must be 0) |
| message | null/not set |

**Field Presence:**

1. Scalar fields with default values are not serialized.
2. Fields marked `optional` maintain presence information.
3. Repeated fields with zero elements are not serialized.
4. Receivers MUST treat missing fields as having default values.

### A.3.7 Timestamp Encoding

Timestamps use `google.protobuf.Timestamp`:

```protobuf
message Timestamp {
  int64 seconds = 1;  // Seconds since Unix epoch (1970-01-01T00:00:00Z)
  int32 nanos = 2;    // Non-negative fractions of a second [0, 999999999]
}
```

**Timestamp Requirements:**

1. `seconds` MUST represent UTC time.
2. `nanos` MUST be in range [0, 999999999].
3. Implementations SHOULD support timestamps from year 0001 to 9999.

### A.3.8 Any Type Encoding

The `google.protobuf.Any` type enables polymorphic message embedding:

```protobuf
message Any {
  string type_url = 1;  // URL identifying the message type
  bytes value = 2;      // Serialized message payload
}
```

**Type URL Format:**

1. Type URLs MUST use format: `type.googleapis.com/full.type.name`
2. Example: `type.googleapis.com/pipestream.data.v1.PipeDoc`
3. Receivers MUST verify type compatibility before deserialization.

---

## Security Considerations

Implementations MUST validate all message fields before processing:

1. Verify SHA-256 checksums (32 bytes) before accepting entity payloads.
2. Validate UTF-8 encoding of all string fields.
3. Enforce maximum message size limits (16 MiB default).
4. Verify entity_id uniqueness within its scope.
5. Validate scope depth to prevent recursion attacks (max_scope_depth default: 8).
6. Enforce entity window size limits to prevent resource exhaustion.
7. Validate claim check expiry timestamps and garbage-collect expired claims.
8. Never log encryption key material from EncryptionMetadata.

## IANA Considerations

This document defines no new IANA registrations beyond those in the main specification. The MIME type `application/protobuf` is used for protocol message payloads.

---

# Appendix B: Protocol Layer Capability Matrix

This appendix provides a quick reference showing which features are available at each protocol layer.

| Feature | Layer 0 (Core) | Layer 1 (Recursive) | Layer 2 (Resilience) |
|---------|:--------------:|:-------------------:|:--------------------:|
| Basic ledger frame (32-bit) | Yes | Yes | Yes |
| Entity streaming | Yes | Yes | Yes |
| PENDING / PROCESSING / COMPLETE / FAILED | Yes | Yes | Yes |
| CHECKPOINT status | Yes | Yes | Yes |
| VAPORIZING / AGGREGATING statuses | Yes | Yes | Yes |
| Parts Ledger | Yes | Yes | Yes |
| Cursor-based ID recycling | Yes | Yes | Yes |
| Scoped ledger frame | | Yes | Yes |
| Hierarchical scopes (scope_id) | | Yes | Yes |
| Scope digest (Merkle root) | | Yes | Yes |
| Barrier (subtree sync) | | Yes | Yes |
| YIELDED status | | | Yes |
| DEFERRED status | | | Yes |
| RETRYING status | | | Yes |
| SKIPPED status | | | Yes |
| ABANDONED status | | | Yes |
| Claim checks | | | Yes |
| Completion policies | | | Yes |
| Stopping point validation | | | Yes |

**Layer Dependencies:**

- Layer 0 is REQUIRED for all implementations.
- Layer 1 is OPTIONAL and adds hierarchical scoping, digest propagation, and barriers.
- Layer 2 is OPTIONAL, requires Layer 1, and adds yield/resume, claim checks, and completion policies.

---

*This appendix is part of the PipeStream Protocol Specification (draft-krickert-pipestream-01).*
