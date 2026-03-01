## 3. Protocol Layers

PipeStream defines three protocol layers that build upon each other. This layered approach allows simple deployments to use only the core protocol while complex deployments can leverage advanced features.

### 3.1. Layer 0: Core Protocol

Layer 0 provides the fundamental streaming capabilities:

- Status frame (32-bit, word-aligned)
- Entity frame (header + payload)
- Status codes: PENDING, PROCESSING, COMPLETE, FAILED, CHECKPOINT
- Assembly Manifest for parent-child tracking
- Cursor-based Entity ID recycling
- Single-level dehydrate/rehydrate
- Checkpoint blocking

All implementations MUST support Layer 0.

### 3.2. Layer 1: Recursive Extension

Layer 1 adds hierarchical processing capabilities:

- Scoped Entity ID namespaces (collection → document → part → job)
- SCOPE_OPEN and SCOPE_CLOSE frames
- SCOPE_DIGEST for Merkle-based subtree completion
- BARRIER for subtree-scoped synchronization
- Nested dehydration with depth tracking

Layer 1 is OPTIONAL. Implementations advertise Layer 1 support during capability negotiation.

### 3.3. Layer 2: Resilience Extension

Layer 2 adds fault tolerance and async processing:

- YIELDED status with continuation tokens
- DEFERRED status with claim checks
- RETRYING, SKIPPED, ABANDONED statuses
- Completion policies (STRICT, LENIENT, BEST_EFFORT, QUORUM)
- Claim check query/response frames
- Stopping point validation

Layer 2 is OPTIONAL and requires Layer 1. Implementations advertise Layer 2 support during capability negotiation.

### 3.4. Capability Negotiation

During CONNECT, endpoints exchange supported capabilities:

```protobuf
message Capabilities {
  bool layer0_core = 1;           // Always true
  bool layer1_recursive = 2;      // Scoped IDs, digests
  bool layer2_resilience = 3;     // Yield, claim checks
  uint32 max_scope_depth = 4;     // Default: 7 (8 levels, 0-7)
  uint32 max_entities_per_scope = 5;  // Default: 4,294,967,294 (2^32-2)
  uint32 max_window_size = 6;     // Default: 2,147,483,648 (2^31)
}
```

Peers negotiate down to common capabilities. If Layer 2 is requested but Layer 1 is not supported, Layer 2 MUST be disabled.
