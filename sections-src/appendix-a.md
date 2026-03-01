## Appendix A: Protobuf Schema Reference

### A.1. Protocol-Level Messages

```protobuf
// Copyright 2026 PipeStream Authors
//
// PipeStream Protocol - IETF draft protocol for recursive entity streaming
// over QUIC. Defines the wire-format messages for Layers 0-2 of the
// PipeStream architecture.
//
// Edition 2023 is used for closed enums and implicit field presence.

edition = "2023";

package pipestream.protocol.v1;

import "google/protobuf/any.proto";

// All enums in this file are CLOSED.
option features.enum_type = CLOSED;

// Capabilities describes the feature set supported by a PipeStream endpoint.
message Capabilities {
  bool layer0_core = 1;
  bool layer1_recursive = 2;
  bool layer2_resilience = 3;

  // Maximum nesting depth allowed for recursive scopes.
  // Default is 7 (8 levels: 0-7).
  uint32 max_scope_depth = 4;

  // Maximum number of entities permitted within a single scope.
  uint32 max_entities_per_scope = 5;

  // Maximum flow-control window size, in number of entities.
  uint32 max_window_size = 6;
}

// EntityHeader is sent at the beginning of each entity stream.
message EntityHeader {
  uint32 entity_id = 1;
  uint32 parent_id = 2;
  uint32 scope_id = 3;
  uint32 layer = 4;
  string content_type = 5;
  uint64 payload_length = 6;
  bytes checksum = 7;
  map<string, string> metadata = 8;
  ChunkInfo chunk_info = 9;
  CompletionPolicy completion_policy = 10;
}

message ChunkInfo {
  uint32 total_chunks = 1;
  uint32 chunk_index = 2;
  uint64 chunk_offset = 3;
}

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

// CheckpointFrame (Protobuf, Type 0x81)
message CheckpointFrame {
  string checkpoint_id = 1;
  uint64 sequence_number = 2;
  uint32 checkpoint_entity_id = 3;  // Numeric ordering key for barrier evaluation
  uint32 scope_id = 4;              // Scope to which this checkpoint applies
  uint32 flags = 5;
  uint32 timeout_ms = 6;
}

// AssemblyManifestEntry tracks parent-child relationships.
message AssemblyManifestEntry {
  uint32 parent_id = 1;
  uint32 scope_id = 2;
  repeated uint32 children_ids = 3;
  repeated EntityStatus children_status = 4;
  CompletionPolicy policy = 5;
  uint64 created_at = 6;
  ResolutionState state = 7;
}

enum ResolutionState {
  RESOLUTION_STATE_UNSPECIFIED = 0;
  RESOLUTION_STATE_ACTIVE = 1;
  RESOLUTION_STATE_RESOLVED = 2;
  RESOLUTION_STATE_PARTIAL = 3;
  RESOLUTION_STATE_FAILED = 4;
}

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

message ClaimCheck {
  uint64 claim_id = 1;
  uint32 entity_id = 2;
  uint32 scope_id = 3;
  uint64 expiry_timestamp = 4;
  StoppingPointValidation validation = 5;
}

message StoppingPointValidation {
  bytes state_checksum = 1;
  uint64 bytes_processed = 2;
  uint32 children_complete = 3;
  uint32 children_total = 4;
  bool is_resumable = 5;
  string checkpoint_ref = 6;
}

// ScopeDigest (Fixed Frame 0x54 carries this info, but logic uses this structure)
message ScopeDigestData {
  uint32 scope_id = 1;
  uint64 entities_processed = 2;
  uint64 entities_succeeded = 3;
  uint64 entities_failed = 4;
  uint64 entities_deferred = 5;
  bytes merkle_root = 6;
}
```
