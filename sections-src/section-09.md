## 9. Rehydration Semantics

### 9.1. Entity ID Lifecycle and Cursor

Entity IDs are managed using a cursor-based recycling scheme:

```
   Entity ID Space (32-bit circular buffer):

                       cursor (lowest unresolved)
                           │
      recyclable           │         in-flight
     <---------------      ▼      --------------->
     [...completed...]│[PENDING][PROCESSING][PENDING][...]│...free...
                      ^                                    ^
                   cursor                             last_assigned

   Window Size = (last_assigned - cursor) mod MAX_ID
   If window_size >= max_window → backpressure
```

**Rules:**
1. `new_id = (last_assigned + 1) % MAX_ENTITY_ID`
2. If `(new_id - cursor) % MAX_ID >= max_window` → STOP, apply backpressure
3. On COMPLETE/FAILED: mark resolved; if `entity_id == cursor`, advance cursor
4. IDs behind cursor are implicitly recyclable

### 9.2. Assembly Manifest

Each Assembly Manifest entry tracks:

```protobuf
message AssemblyManifestEntry {
  uint32 parent_id = 1;
  uint32 scope_id = 2;           // Layer 1
  repeated uint32 children_ids = 3;
  repeated EntityStatus children_status = 4;
  CompletionPolicy policy = 5;   // Layer 2
  uint64 created_at = 6;
  ResolutionState state = 7;
}

enum ResolutionState {
  RESOLUTION_STATE_UNSPECIFIED = 0;
  RESOLUTION_STATE_ACTIVE = 1;
  RESOLUTION_STATE_RESOLVED = 2;
  RESOLUTION_STATE_PARTIAL = 3;      // Some children failed/skipped
  RESOLUTION_STATE_FAILED = 4;
}
```

### 9.3. Checkpoint Blocking

A checkpoint is satisfied when:

1. All entities with IDs less than checkpoint ID have reached terminal state
2. All Assembly Manifest entries within scope have been resolved
3. All nested checkpoints have been satisfied

### 9.4. Scope Digest Propagation (Layer 1)

When a scope completes, the endpoint MUST compute a Scope Digest and propagate it to the parent scope via a SCOPE_DIGEST frame (Section 6.3).

The Merkle root in the Scope Digest is computed as follows:

1. For each entity in the scope, ordered by Entity ID (ascending), construct a leaf value by concatenating the 4-byte big-endian Entity ID with the 1-byte status code.
2. Compute SHA-256 over each leaf to produce leaf hashes.
3. Build a binary Merkle tree by repeatedly hashing pairs of sibling nodes: `SHA-256(left || right)`. If the number of nodes at any level is odd, the last node is promoted without hashing.
4. The root of this tree is the `merkle_root` value in the SCOPE_DIGEST frame.

This construction is deterministic: any two implementations processing the same set of entity statuses MUST produce the same Merkle root. The parent scope MAY use the Merkle root to verify subtree integrity with a single hash comparison. Full status history remains available on request for audit.

### 9.5. Rehydration Readiness Tracking

Implementations MUST track Assembly Manifest resolution order using a mechanism that provides O(1) insertion and amortized O(log n) minimum extraction. The tracking mechanism MUST support efficient decrease-key operations to handle out-of-order status updates.

Implementations MAY choose any data structure that satisfies these complexity requirements. See the companion document `REFERENCE_IMPLEMENTATION.md` for a recommended approach using a Fibonacci heap with pseudocode and amortized complexity analysis.

### 9.6. Stopping Point Validation (Layer 2)

When yielding or deferring, include validation:

```protobuf
message StoppingPointValidation {
  bytes state_checksum = 1;      // Hash of processing state
  uint64 bytes_processed = 2;    // Progress marker
  uint32 children_complete = 3;
  uint32 children_total = 4;
  bool is_resumable = 5;
  string checkpoint_ref = 6;
}
```
