# Rehydration Semantics

## Entity ID Lifecycle and Cursor

Entity IDs are managed using a cursor-based circular recycling scheme within the 32-bit ID space. The ID space is divided into three logical regions relative to the current `cursor` and `last_assigned` pointers:

| Region | ID Range | Description |
|--------|----------|-------------|
| Recyclable | IDs behind `cursor` | Resolved entities; IDs may be reused |
| In-flight | `cursor` to `last_assigned` | Active entities (PENDING, PROCESSING, etc.) |
| Free | Beyond `last_assigned` | Available for new entity assignment |

The window size is computed as `(last_assigned - cursor) mod 0xFFFFFFFD`. If `window_size >= max_window`, the sender MUST apply backpressure and stop assigning new IDs until the cursor advances.

**Rules:**
1. `new_id = (last_assigned + 1) % 0xFFFFFFFD`
2. If `new_id == 0`, `new_id = 1` (skip reserved NULL_ENTITY)
3. If `(new_id - cursor) % 0xFFFFFFFD >= max_window` → STOP, apply backpressure
4. On COMPLETE/FAILED: mark resolved; if `entity_id == cursor`, advance cursor
5. IDs behind cursor are implicitly recyclable

## Assembly Manifest

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

## Checkpoint Blocking

A checkpoint is satisfied when:

1. All entities in the checkpoint scope with IDs less than `checkpoint_entity_id` (considering circular wrap) have reached terminal state.
2. All Assembly Manifest entries within the checkpoint scope have been resolved.
3. All nested checkpoints within the checkpoint scope have been satisfied.

CheckpointFrame (Section 6.6 / Appendix A) carries both:

```protobuf
message CheckpointFrame {
  string checkpoint_id = 1;
  uint64 sequence_number = 2;
  uint32 checkpoint_entity_id = 3;
  uint32 scope_id = 4;
  uint32 flags = 5;
  uint32 timeout_ms = 6;
}
```

- `checkpoint_id`: an opaque identifier for logging and correlation.
- `checkpoint_entity_id`: the numeric ordering key used for barrier evaluation.

Implementations MUST use `checkpoint_entity_id` (not `checkpoint_id`) when evaluating Condition 1.

For circular comparison in Condition 1, implementations MUST use the same modulo ordering as cursor management. Define `MAX = 0xFFFFFFFD` and:

`is_before(a, b) = ((b - a + MAX) % MAX) < (MAX / 2)`

An entity ID `a` is considered "less than checkpoint_entity_id `b`" iff `is_before(a, b)` is true.

## Scope Digest Propagation (Layer 1)

When a scope completes, the endpoint MUST compute a Scope Digest and propagate it to the parent scope via a SCOPE_DIGEST frame (Section 6.3).

The Merkle root in the Scope Digest is computed as follows:

1. For each entity in the scope, ordered by Entity ID (ascending), construct a 5-octet leaf value by concatenating:
   - The 4-octet big-endian Entity ID.
   - A 1-octet status field where the lower 4 bits contain the `Stat` code (Section 6.2.2) and the upper 4 bits are zero.
2. Compute SHA-256 over each 5-octet leaf to produce leaf hashes.
3. Build a binary Merkle tree by repeatedly hashing pairs of sibling nodes: `SHA-256(left || right)`. If the number of nodes at any level is odd, the last node is promoted to the next level without hashing.
4. The root of this tree is the `merkle_root` value in the SCOPE_DIGEST frame.

This construction is deterministic: any two implementations processing the same set of entity statuses MUST produce the same Merkle root.

## Rehydration Readiness Tracking

Implementations MUST track Assembly Manifest resolution order using a mechanism that provides O(1) insertion and amortized O(log n) minimum extraction. The tracking mechanism MUST support efficient decrease-key operations to handle out-of-order status updates.

Implementations MAY choose any data structure that satisfies these complexity requirements. See the companion document `REFERENCE_IMPLEMENTATION.md` for a recommended approach using a Fibonacci heap.

## Stopping Point Validation (Layer 2)

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
