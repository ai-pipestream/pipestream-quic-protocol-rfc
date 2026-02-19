# Appendix A: Protobuf Schema Reference

This appendix defines the Protocol Buffers (proto3) message schemas used by the PipeStream protocol. All messages use proto3 syntax and follow the wire encoding rules defined in Section A.3.

PipeStream organizes its protobuf messages into two categories: protocol-level messages (Section A.1) used for entity transport, framing, and coordination; and entity data messages (Section A.2) that define the payload types carried within entities. The protocol defines three layers -- Layer 0 (Core), Layer 1 (Recursive), and Layer 2 (Resilience) -- and messages are annotated with the layer that introduces them.

## A.1 Protocol-Level Messages

This section defines the core protocol messages used for capability negotiation, entity transport, framing, ledger tracking, and coordination in PipeStream.

### A.1.1 Capabilities

The Capabilities message is exchanged during CONNECT to negotiate supported protocol layers and operational limits. Peers negotiate down to common capabilities. If Layer 2 is requested but Layer 1 is not supported, Layer 2 MUST be disabled.

```protobuf
// Copyright 2026 PipeStream AI
//
// PipeStream Protocol - IETF draft protocol for recursive entity streaming
// over QUIC. Defines the wire-format messages for Layers 0-2 of the
// PipeStream architecture: core streaming, recursive scoping, and resilience.
//
// Edition 2023 is used for closed enums (critical for wire-protocol safety)
// and implicit field presence (distinguishing "not set" from zero values).

edition = "2023";

package pipestream.protocol.v1;

import "google/protobuf/any.proto";

// All enums in this file are CLOSED. Unknown enum values received on the wire
// MUST be rejected.
option features.enum_type = CLOSED;

// Capabilities describes the feature set supported by a PipeStream endpoint.
message Capabilities {
  // Whether the endpoint supports Layer 0 (core entity streaming).
  bool layer0_core = 1;

  // Whether the endpoint supports Layer 1 (recursive scoping and vaporization).
  bool layer1_recursive = 2;

  // Whether the endpoint supports Layer 2 (resilience, yield, and claim-check).
  bool layer2_resilience = 3;

  // Maximum nesting depth allowed for recursive scopes.
  uint32 max_scope_depth = 4;

  // Maximum number of entities permitted within a single scope.
  uint32 max_entities_per_scope = 5;

  // Maximum flow-control window size, in number of entities.
  uint32 max_window_size = 6;
}
```

### A.1.2 EntityHeader

```protobuf
// EntityHeader is sent at the beginning of each entity stream.
message EntityHeader {
  // Unique identifier for this entity within the session.
  uint32 entity_id = 1;

  // Identifier of the parent entity that spawned this entity.
  uint32 parent_id = 2;

  // Identifier of the scope to which this entity belongs.
  uint32 scope_id = 3;

  // Protocol layer at which this entity was created (0, 1, or 2).
  uint32 layer = 4;

  // MIME content type of the entity payload.
  string content_type = 5;

  // Length in bytes of the complete entity payload.
  uint64 payload_length = 6;

  // Integrity checksum of the complete entity payload.
  bytes checksum = 7;

  // Arbitrary key-value metadata.
  map<string, string> metadata = 8;

  // Chunking information for this entity.
  ChunkInfo chunk_info = 9;

  // Completion policy that governs retry, timeout, and failure behavior.
  CompletionPolicy completion_policy = 10;
}
```

### A.1.3 ChunkInfo

```protobuf
// ChunkInfo describes how a single entity payload is divided into ordered chunks.
message ChunkInfo {
  uint32 total_chunks = 1;
  uint32 chunk_index = 2;
  uint64 chunk_offset = 3;
}
```

### A.1.4 CompletionPolicy

```protobuf
// CompletionPolicy controls Layer 2 resilience behavior.
message CompletionPolicy {
  CompletionMode mode = 1;
  uint32 max_retries = 2;
  uint32 retry_delay_ms = 3;
  uint32 timeout_ms = 4;
  float min_success_ratio = 5;
  FailureAction on_timeout = 6;
  FailureAction on_failure = 7;
}

enum CompletionMode {
  COMPLETION_MODE_UNSPECIFIED = 0;
  COMPLETION_MODE_STRICT = 1;
  COMPLETION_MODE_LENIENT = 2;
  COMPLETION_MODE_BEST_EFFORT = 3;
  COMPLETION_MODE_QUORUM = 4;
}

enum FailureAction {
  FAILURE_ACTION_UNSPECIFIED = 0;
  FAILURE_ACTION_FAIL = 1;
  FAILURE_ACTION_SKIP = 2;
  FAILURE_ACTION_RETRY = 3;
  FAILURE_ACTION_DEFER = 4;
}
```

### A.1.5 EntityStatus

```protobuf
// EntityStatus represents the lifecycle state of an entity.
enum EntityStatus {
  ENTITY_STATUS_UNSPECIFIED = 0;
  ENTITY_STATUS_PENDING = 1;
  ENTITY_STATUS_PROCESSING = 2;
  ENTITY_STATUS_COMPLETE = 3;
  ENTITY_STATUS_FAILED = 4;
  ENTITY_STATUS_CHECKPOINT = 5;
  ENTITY_STATUS_VAPORIZING = 6;
  ENTITY_STATUS_AGGREGATING = 7;
  ENTITY_STATUS_YIELDED = 8;
  ENTITY_STATUS_DEFERRED = 9;
  ENTITY_STATUS_RETRYING = 10;
  ENTITY_STATUS_SKIPPED = 11;
  ENTITY_STATUS_ABANDONED = 12;
}
```

### A.1.6 ResolutionState

```protobuf
enum ResolutionState {
  RESOLUTION_STATE_UNSPECIFIED = 0;
  RESOLUTION_STATE_ACTIVE = 1;
  RESOLUTION_STATE_RESOLVED = 2;
  RESOLUTION_STATE_PARTIAL = 3;
  RESOLUTION_STATE_FAILED = 4;
}
```

### A.1.7 LedgerFrame

```protobuf
// LedgerFrame is sent on the ledger stream.
message LedgerFrame {
  uint32 entity_id = 1;
  uint32 scope_id = 2;
  EntityStatus status = 3;
  google.protobuf.Any extended_data = 4;
}
```

### A.1.8 CheckpointFrame

```protobuf
// CheckpointFrame defines a synchronization barrier.
message CheckpointFrame {
  string checkpoint_id = 1;
  uint64 sequence_number = 2;
  uint32 flags = 3;
  uint32 timeout_ms = 4;
}
```

### A.1.9 PartsLedgerEntry

```protobuf
// PartsLedgerEntry tracks parent-child relationships.
message PartsLedgerEntry {
  uint32 parent_id = 1;
  uint32 scope_id = 2;
  repeated uint32 children_ids = 3;
  repeated EntityStatus children_status = 4;
  CompletionPolicy policy = 5;
  uint64 created_at = 6;
  ResolutionState state = 7;
}
```

### A.1.10 YieldToken

```protobuf
// YieldToken allows a Layer 2 processor to pause processing.
message YieldToken {
  YieldReason reason = 1;
  bytes continuation_state = 2;
  StoppingPointValidation validation = 3;
}

enum YieldReason {
  YIELD_REASON_UNSPECIFIED = 0;
  YIELD_REASON_EXTERNAL_CALL = 1;
  YIELD_REASON_RATE_LIMITED = 2;
  YIELD_REASON_AWAITING_SIBLING = 3;
  YIELD_REASON_AWAITING_APPROVAL = 4;
  YIELD_REASON_RESOURCE_BUSY = 5;
}
```

### A.1.11 ClaimCheck

```protobuf
// ClaimCheck is a Layer 2 deferred-processing reference.
message ClaimCheck {
  uint64 claim_id = 1;
  uint32 entity_id = 2;
  uint32 scope_id = 3;
  uint64 expiry_timestamp = 4;
  StoppingPointValidation validation = 5;
}
```

### A.1.12 StoppingPointValidation

```protobuf
// StoppingPointValidation captures a snapshot of processing progress.
message StoppingPointValidation {
  bytes state_checksum = 1;
  uint64 bytes_processed = 2;
  uint32 children_complete = 3;
  uint32 children_total = 4;
  bool is_resumable = 5;
  string checkpoint_ref = 6;
}
```

### A.1.13 ScopeDigest

```protobuf
// ScopeDigest is a Layer 1 summary of a completed scope.
message ScopeDigest {
  uint32 scope_id = 1;
  uint64 entities_processed = 2;
  uint64 entities_succeeded = 3;
  uint64 entities_failed = 4;
  uint64 entities_deferred = 5;
  bytes merkle_root = 6;
}
```

## A.2 Entity Data Messages

```protobuf
// PipeStream Data Model
//
// Defines the core document representation for the PipeStream ingestion and
// processing pipeline. A PipeDoc carries raw binary payloads (Layer 0),
// semantic chunks with embeddings (Layer 1), and structured parsed output
// (Layer 2) through every stage of the pipeline.

edition = "2023";

package pipestream.data.v1;

import "google/protobuf/any.proto";
import "google/protobuf/struct.proto";

// All enums in this file are CLOSED.
option features.enum_type = CLOSED;

// PipeDoc is the root document entity that flows through the PipeStream
// pipeline. It aggregates every data layer under a single deterministic
// document identifier.
message PipeDoc {
  string doc_id = 1;
  SearchMetadata search_metadata = 2;
  BlobBag blob_bag = 3;
  google.protobuf.Any structured_data = 4;
  map<string, ParsedMetadata> parsed_metadata = 5;
  SemanticProcessingResult semantic_result = 6;
  OwnershipContext ownership = 7;
  DocIdDerivation doc_id_derivation = 8;
}

// Layer 0: BlobBag
message BlobBag {
  oneof blob_data {
    Blob blob = 1;
    Blobs blobs = 2;
  }
}

message Blobs {
  repeated Blob blobs = 1;
}

message Blob {
  string blob_id = 1;
  string drive_id = 2;
  oneof content {
    bytes data = 3;
    FileStorageReference storage_ref = 4;
  }
  string mime_type = 5;
  string filename = 6;
  int64 size_bytes = 8;
  string checksum = 9;
  ChecksumType checksum_type = 10;
}

enum ChecksumType {
  CHECKSUM_TYPE_UNSPECIFIED = 0;
  CHECKSUM_TYPE_MD5 = 1;
  CHECKSUM_TYPE_SHA1 = 2;
  CHECKSUM_TYPE_SHA256 = 3;
  CHECKSUM_TYPE_SHA512 = 4;
}

message FileStorageReference {
  string provider = 1;
  string bucket = 2;
  string key = 3;
  string region = 4;
  map<string, string> attrs = 5;
  EncryptionMetadata encryption = 6;
}

message EncryptionMetadata {
  string algorithm = 1;
  string key_provider = 2;
  string key_id = 3;
  bytes wrapped_key = 4;
  bytes iv = 5;
  map<string, string> context = 6;
}

// Layer 1: SemanticLayer
message SemanticProcessingResult {
  repeated SemanticChunk chunks = 1;
  string chunking_strategy = 2;
  map<string, string> processing_metadata = 3;
}

message SemanticChunk {
  string chunk_id = 1;
  int64 chunk_number = 2;
  ChunkEmbedding embedding_info = 3;
  map<string, google.protobuf.Value> metadata = 4;
  repeated NLPAnnotation annotations = 5;
}

message ChunkEmbedding {
  string text_content = 1;
  repeated float vector = 2;
  string model_id = 3;
  int32 original_char_start_offset = 4;
  int32 original_char_end_offset = 5;
}

message NLPAnnotation {
  string type = 1;
  string label = 2;
  int32 start_offset = 3;
  int32 end_offset = 4;
  float confidence = 5;
  map<string, string> attributes = 6;
}

// Layer 2: ParsedData
message ParsedMetadata {
  string parser_id = 1;
  map<string, google.protobuf.Value> fields = 2;
  repeated TableData tables = 3;
  string raw_output = 4;
}

message TableData {
  string table_id = 1;
  repeated string headers = 2;
  repeated TableRow rows = 3;
}

message TableRow {
  repeated string cells = 1;
}

// Supporting Types
message SearchMetadata {
  string title = 1;
  repeated string keywords = 2;
  string description = 3;
  map<string, string> custom_fields = 4;
}

message OwnershipContext {
  string tenant_id = 1;
  string owner_id = 2;
  repeated string acl = 3;
}

message DocIdDerivation {
  string strategy = 1;
  string source_field = 2;
  string hash_algorithm = 3;
}
```

## A.3 Wire Encoding Notes

This section specifies the wire encoding requirements for PipeStream protocol messages.

### A.3.1 Protocol Buffer Wire Format

All messages MUST use Protocol Buffers wire format.

### A.3.2 Length-Delimited Message Framing

Each PipeStream message transmitted over QUIC MUST be framed as follows:

```
+----------------+------------------+
| Message Length | Message Payload  |
| (varint)       | (proto3 encoded) |
+----------------+------------------+
```
