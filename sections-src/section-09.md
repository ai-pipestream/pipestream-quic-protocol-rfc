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
3. If `(new_id - cursor) % 0xFFFFFFFD >= max_window` -> STOP, apply backpressure
4. On reaching a terminal state (COMPLETE, SKIPPED, ABANDONED, or FAILED with no remaining retries): mark resolved; if `entity_id == cursor`, advance cursor past all contiguous resolved IDs
5. IDs behind cursor are implicitly recyclable

An entity in the FAILED state that may still transition to RETRYING (Section 6.2.2a) MUST NOT be marked resolved. Only when retries are exhausted or no completion policy permits retries does FAILED become terminal for cursor purposes.

## Assembly Manifest

The Assembly Manifest is a local data structure maintained by each endpoint to track the parent-child relationships created during dehydration. It is not transmitted on the wire; rather, each endpoint constructs its own manifest from the `parent-id` fields in received EntityHeaders and from status updates observed on the Control Stream. The CDDL below defines the logical structure of each entry; implementations MAY use any internal representation that preserves the required semantics.

Each Assembly Manifest entry tracks:

~~~~ cddl
assembly-manifest-entry = {
  parent-id: uint,
  ? scope-id: uint,              ; Layer 1
  children-ids: [* uint],
  ? children-status: [* entity-status],
  ? policy: completion-policy,   ; Layer 2
  ? created-at: uint,
  ? state: resolution-state,
}

resolution-state = &(
  unspecified: 0,
  active: 1,
  resolved: 2,
  partial: 3,                   ; Some children failed/skipped
  failed: 4,
)
~~~~

## Checkpoint Blocking

A checkpoint is satisfied when:

1. All entities in the checkpoint scope with IDs less than `checkpoint_entity_id` (considering circular wrap) have reached terminal state.
2. All Assembly Manifest entries within the checkpoint scope have been resolved.
3. All nested checkpoints within the checkpoint scope have been satisfied.

CheckpointFrame (Section 6.6 / Appendix C) carries both:

~~~~ cddl
checkpoint-frame = {
  checkpoint-id: tstr,
  sequence-number: uint,
  checkpoint-entity-id: uint,
  ? scope-id: uint,
  ? flags: uint,
  ? timeout-ms: uint,
}
~~~~

- `checkpoint_id`: an opaque identifier for logging and correlation.
- `checkpoint_entity_id`: the numeric ordering key used for barrier evaluation.

Implementations MUST use `checkpoint_entity_id` (not `checkpoint_id`) when evaluating Condition 1.

For circular comparison in Condition 1, implementations MUST use the same modulo ordering as cursor management. Define `MAX = 0xFFFFFFFD` and:

`is_before(a, b) = ((b - a + MAX) % MAX) < (MAX / 2)`

An entity ID `a` is considered "less than checkpoint_entity_id `b`" iff `is_before(a, b)` is true.

## Scope ID Allocation (Layer 1)

When Layer 1 is negotiated, Scope IDs are 32-bit unsigned integers assigned by the endpoint that initiates the dehydration. The allocation scheme is as follows:

1. Scope ID 0 is the root scope and MUST NOT be used for child scopes.
2. The dehydrating endpoint assigns a unique Scope ID to each new child scope created during dehydration. The Scope ID MUST be unique within the connection for the lifetime of that scope (i.e., until the scope's SCOPE_DIGEST frame has been emitted and acknowledged).
3. Scope IDs MAY be allocated sequentially or randomly; the protocol does not require any particular ordering. Sequential allocation is RECOMMENDED for simplicity and debuggability.
4. Once a scope has been closed (its SCOPE_DIGEST has been sent), the Scope ID MAY be reused for a new scope. Implementations MUST ensure that no in-flight status frames reference a recycled Scope ID; this is guaranteed if the implementation waits until all entities within the scope have reached terminal state before recycling.

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

Implementations MAY use a Fibonacci heap or similar priority queue to satisfy these complexity requirements.

## Stopping Point Validation (Layer 2)

When yielding or deferring, include validation:

~~~~ cddl
stopping-point-validation = {
  ? state-checksum: bstr,        ; Hash of processing state
  ? bytes-processed: uint,       ; Progress marker
  ? children-complete: uint,
  ? children-total: uint,
  ? is-resumable: bool,
  ? checkpoint-ref: tstr,
}
~~~~
