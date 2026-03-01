# Protobuf Schema Reference

## Protocol-Level Messages

~~~~ protobuf
// Copyright 2026 PipeStream AI
//
// PipeStream Protocol - IETF draft protocol for recursive entity
// streaming over QUIC. Defines the wire-format messages for Layers 0-2
// of the PipeStream architecture: core streaming, recursive scoping,
// and resilience.
//
// Edition 2023 is used for closed enums (critical for wire-protocol
// safety) and implicit field presence (distinguishing "not set" from
// zero values). In this edition, all fields have explicit presence
// by default, making the 'optional' keyword unnecessary.

edition = "2023";

package pipestream.protocol.v1;

import "google/protobuf/any.proto";

// All enums in this file are CLOSED. Unknown enum values received on
// the wire MUST be rejected. This is essential because status codes
// are encoded as 4-bit values in the status frame wire format;
// accepting unknown values could cause undefined behavior in state
// machines and cursor advancement.
option features.enum_type = CLOSED;

// Capabilities describes the feature set supported by a PipeStream
// endpoint. Exchanged during the CONNECT handshake so that both
// sides can negotiate which protocol layers and resource limits
// apply to the session.
message Capabilities {
  // Whether the endpoint supports Layer 0 (core entity streaming).
  bool layer0_core = 1;

  // Whether the endpoint supports Layer 1 (recursive scoping and
  // dehydration).
  bool layer1_recursive = 2;

  // Whether the endpoint supports Layer 2 (resilience, yield, and
  // claim-check). Requires Layer 1 support; if layer1_recursive is
  // false, this MUST be false.
  bool layer2_resilience = 3;

  // Maximum nesting depth allowed for recursive scopes.
  // Default is 7 (8 levels: 0-7).
  uint32 max_scope_depth = 4;

  // Maximum number of entities permitted within a single scope.
  uint32 max_entities_per_scope = 5;

  // Maximum flow-control window size, in number of entities.
  uint32 max_window_size = 6;
}

// EntityHeader is sent at the beginning of each entity stream to
// describe the payload that follows. It carries identity, lineage,
// content metadata, chunking information, and the completion policy
// that governs how partial failures of this entity's children are
// handled.
message EntityHeader {
  // Scope-local entity identifier.
  uint32 entity_id = 1;

  // Identifier of the parent entity.
  uint32 parent_id = 2;

  // Identifier of the scope.
  uint32 scope_id = 3;

  // Data layer (0-3).
  uint32 layer = 4;

  // MIME content type.
  string content_type = 5;

  // Length in bytes of the complete entity payload, before any
  // chunking.
  uint64 payload_length = 6;

  // SHA-256 integrity checksum.
  bytes checksum = 7;

  // Arbitrary metadata.
  map<string, string> metadata = 8;

  // Chunking information.
  ChunkInfo chunk_info = 9;

  // Resilience completion policy.
  CompletionPolicy completion_policy = 10;
}

// ChunkInfo describes how a payload is divided into chunks.
message ChunkInfo {
  // Total number of chunks.
  uint32 total_chunks = 1;

  // Zero-based chunk index.
  uint32 chunk_index = 2;

  // Byte offset within the complete payload.
  uint64 chunk_offset = 3;
}

// CompletionPolicy controls Layer 2 resilience behavior.
message CompletionPolicy {
  // Mode for evaluating completion.
  CompletionMode mode = 1;

  // Maximum retry attempts.
  uint32 max_retries = 2;

  // Delay between retries in milliseconds.
  uint32 retry_delay_ms = 3;

  // Maximum wait time in milliseconds.
  uint32 timeout_ms = 4;

  // Minimum success ratio for QUORUM mode.
  float min_success_ratio = 5;

  // Action on timeout.
  FailureAction on_timeout = 6;

  // Action on failure.
  FailureAction on_failure = 7;
}

// CompletionMode specifies completion evaluation strategies.
enum CompletionMode {
  COMPLETION_MODE_UNSPECIFIED = 0;
  COMPLETION_MODE_STRICT = 1;
  COMPLETION_MODE_LENIENT = 2;
  COMPLETION_MODE_BEST_EFFORT = 3;
  COMPLETION_MODE_QUORUM = 4;
}

// FailureAction specifies error handling behaviors.
enum FailureAction {
  FAILURE_ACTION_UNSPECIFIED = 0;
  FAILURE_ACTION_FAIL = 1;
  FAILURE_ACTION_SKIP = 2;
  FAILURE_ACTION_RETRY = 3;
  FAILURE_ACTION_DEFER = 4;
}

// EntityStatus represents the lifecycle state of an entity.
enum EntityStatus {
  ENTITY_STATUS_UNSPECIFIED = 0;
  ENTITY_STATUS_PENDING = 1;
  ENTITY_STATUS_PROCESSING = 2;
  ENTITY_STATUS_COMPLETE = 3;
  ENTITY_STATUS_FAILED = 4;
  ENTITY_STATUS_CHECKPOINT = 5;
  ENTITY_STATUS_DEHYDRATING = 6;
  ENTITY_STATUS_REHYDRATING = 7;
  ENTITY_STATUS_YIELDED = 8;
  ENTITY_STATUS_DEFERRED = 9;
  ENTITY_STATUS_RETRYING = 10;
  ENTITY_STATUS_SKIPPED = 11;
  ENTITY_STATUS_ABANDONED = 12;
}

// StatusFrame is the Protobuf representation of a status transition.
message StatusFrame {
  // Identifier of the entity.
  uint32 entity_id = 1;

  // Identifier of the scope.
  uint32 scope_id = 2;

  // Current status.
  EntityStatus status = 3;

  // Optional extension data.
  google.protobuf.Any extended_data = 4;
}

// CheckpointFrame (Protobuf, Type 0x81)
message CheckpointFrame {
  // Unique checkpoint identifier.
  string checkpoint_id = 1;

  // Monotonic sequence number.
  uint64 sequence_number = 2;

  // Numeric ordering key for barrier evaluation.
  uint32 checkpoint_entity_id = 3;

  // Scope to which this checkpoint applies.
  uint32 scope_id = 4;

  // Checkpoint flags.
  uint32 flags = 5;

  // Maximum wait time in milliseconds.
  uint32 timeout_ms = 6;
}

// AssemblyManifestEntry tracks parent-child relationships.
message AssemblyManifestEntry {
  // Identifier of the parent entity.
  uint32 parent_id = 1;

  // Scope identifier.
  uint32 scope_id = 2;

  // Ordered child identifiers.
  repeated uint32 children_ids = 3;

  // Current status of each child.
  repeated EntityStatus children_status = 4;

  // Governing completion policy.
  CompletionPolicy policy = 5;

  // Creation timestamp (Unix epoch microseconds).
  uint64 created_at = 6;

  // Current resolution state.
  ResolutionState state = 7;
}

// ResolutionState tracks Assembly Manifest completion.
enum ResolutionState {
  RESOLUTION_STATE_UNSPECIFIED = 0;
  RESOLUTION_STATE_ACTIVE = 1;
  RESOLUTION_STATE_RESOLVED = 2;
  RESOLUTION_STATE_PARTIAL = 3;
  RESOLUTION_STATE_FAILED = 4;
}

// YieldToken captures continuation state for paused entities.
message YieldToken {
  // Reason for yielding.
  YieldReason reason = 1;

  // Opaque continuation state.
  bytes continuation_state = 2;

  // Validation data for resumption.
  StoppingPointValidation validation = 3;
}

// YieldReason describes why processing was yielded.
enum YieldReason {
  YIELD_REASON_UNSPECIFIED = 0;
  YIELD_REASON_EXTERNAL_CALL = 1;
  YIELD_REASON_RATE_LIMITED = 2;
  YIELD_REASON_AWAITING_SIBLING = 3;
  YIELD_REASON_AWAITING_APPROVAL = 4;
  YIELD_REASON_RESOURCE_BUSY = 5;
}

// ClaimCheck is a Layer 2 deferred-processing reference.
message ClaimCheck {
  // Unique claim identifier.
  uint64 claim_id = 1;

  // Identifier of the deferred entity.
  uint32 entity_id = 2;

  // Scope identifier.
  uint32 scope_id = 3;

  // Expiry timestamp (Unix epoch microseconds).
  uint64 expiry_timestamp = 4;

  // Validation data.
  StoppingPointValidation validation = 5;
}

// StoppingPointValidation captures a snapshot of progress.
message StoppingPointValidation {
  // Hash of internal state.
  bytes state_checksum = 1;

  // Payload bytes consumed.
  uint64 bytes_processed = 2;

  // Completed child count.
  uint32 children_complete = 3;

  // Total expected child count.
  uint32 children_total = 4;

  // Resumption capability flag.
  bool is_resumable = 5;

  // Last passed checkpoint reference.
  string checkpoint_ref = 6;
}

// ScopeDigest is a Layer 1 summary of a completed scope.
message ScopeDigest {
  // Identifier of the scope.
  uint32 scope_id = 1;

  // Total processed count.
  uint64 entities_processed = 2;

  // Total succeeded count.
  uint64 entities_succeeded = 3;

  // Total failed count.
  uint64 entities_failed = 4;

  // Total deferred count.
  uint64 entities_deferred = 5;

  // Merkle root hash.
  bytes merkle_root = 6;
}

// PipeDoc represents the top-level document envelope for an entity.
message PipeDoc {
  // Unique document identifier.
  string doc_id = 1;

  // Identifier of the entity.
  uint32 entity_id = 2;

  // Ownership and access context.
  OwnershipContext ownership = 3;
}

// OwnershipContext defines multi-tenancy and access control for
// entities.
message OwnershipContext {
  // Entity owner identifier.
  string owner_id = 1;

  // Group identifier.
  string group_id = 2;

  // List of access scopes.
  repeated string scopes = 3;
}

// FileStorageReference provides a location for external data.
message FileStorageReference {
  // Storage provider identifier.
  string provider = 1;

  // Bucket or container name.
  string bucket = 2;

  // Object key or path.
  string key = 3;

  // Optional region hint.
  string region = 4;

  // Provider-specific attributes.
  map<string, string> attrs = 5;

  // Encryption metadata.
  EncryptionMetadata encryption = 6;
}

// EncryptionMetadata defines encryption parameters.
message EncryptionMetadata {
  // Encryption algorithm.
  string algorithm = 1;

  // Key provider identifier.
  string key_provider = 2;

  // Encryption key identifier.
  string key_id = 3;

  // Optional wrapped DEK.
  bytes wrapped_key = 4;

  // Initialization vector.
  bytes iv = 5;

  // Additional encryption context.
  map<string, string> context = 6;
}
~~~~
