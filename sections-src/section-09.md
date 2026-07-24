# Rehydration Semantics

## Entity ID Lifecycle and Cursor

Entity IDs are managed using a cursor-based circular recycling scheme within the 32-bit ID space. All circular arithmetic in this document is performed modulo 0xFFFFFFFD; this modulus excludes the reserved values at the top of the 32-bit space from circulation. The following Entity ID values are reserved and MUST NOT be assigned to entities:

| Entity ID | Name | Purpose |
|-----------|------|---------|
| 0x00000000 | NULL_ENTITY | Reserved; skipped during assignment |
| 0xFFFFFFFD-0xFFFFFFFE | Reserved | Outside the circular ID space |
| 0xFFFFFFFF | CONNECTION_LEVEL | Connection-scoped signals such as heartbeats (Section 5.1.4) |

Assignable Entity IDs therefore range from 0x00000001 to 0xFFFFFFFC, yielding 4,294,967,292 usable identifiers per scope.

The ID space is divided into three logical regions relative to the current `cursor` and `last_assigned` pointers:

| Region | ID Range | Description |
|--------|----------|-------------|
| Recyclable | IDs behind `cursor` | Resolved entities; IDs may be reused |
| In-flight | `cursor` to `last_assigned` | Active entities (PENDING, PROCESSING, etc.) |
| Free | Beyond `last_assigned` | Available for new entity assignment |

The window size is computed as `(last_assigned - cursor) mod 0xFFFFFFFD`. If `window_size >= max_window`, the sender MUST apply backpressure and stop assigning new IDs until the cursor advances.

The `max_window` value MUST NOT exceed 2,147,483,646 (the largest value strictly less than 0xFFFFFFFD / 2). This bound guarantees that the circular comparison function `is_before` (Section 9.3) is unambiguous for any pair of in-flight Entity IDs. The default `max-window-size` advertised in the capabilities exchange (Section 3.4) is 2,147,483,646.

**Rules:**

1. `new_id = (last_assigned + 1) % 0xFFFFFFFD`
2. If `new_id == 0`, `new_id = 1` (skip reserved NULL_ENTITY)
3. If `(new_id - cursor) % 0xFFFFFFFD >= max_window` -> STOP, apply backpressure
4. On reaching a terminal state (COMPLETE, SKIPPED, ABANDONED, or FAILED with no remaining retries): mark resolved; if `entity_id == cursor`, advance cursor past all contiguous resolved IDs
5. IDs behind cursor are implicitly recyclable

An entity in the FAILED state that may still transition to RETRYING (see the state transition table in Section 6.2) MUST NOT be marked resolved. Only when retries are exhausted or no completion policy permits retries does FAILED become terminal for cursor purposes.

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

CheckpointFrame (Section 6.7 / Appendix C) carries both:

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
2. The dehydrating endpoint assigns a unique Scope ID to each new child scope created during dehydration. The Scope ID MUST be unique within the connection for the lifetime of that scope (i.e., until the scope's SCOPE_DIGEST frame has been sent and all entities in the scope have reached terminal state).
3. Scope IDs MAY be allocated sequentially or randomly; the protocol does not require any particular ordering. Sequential allocation is RECOMMENDED for simplicity and debuggability.
4. Once a scope has been closed (its SCOPE_DIGEST has been sent and all entities within the scope have reached terminal state), the Scope ID MAY be reused for a new scope. Implementations MUST ensure that no in-flight status frames reference a recycled Scope ID before reuse.

## Scope Digest Propagation (Layer 1)

When a scope completes, the endpoint MUST compute a Scope Digest and propagate it to the parent scope via a SCOPE_DIGEST frame (Section 6.3).

The Merkle root in the Scope Digest is computed as follows:

1. For each entity in the scope, ordered by ascending numeric Entity ID value, construct a 5-octet leaf value by concatenating:
   - The 4-octet big-endian Entity ID.
   - A 1-octet status field where the lower 4 bits contain the entity's terminal `Stat` code (Section 6.2.2) and the upper 4 bits are zero.
2. Compute each leaf hash as `SHA-256(0x00 || leaf)`, where `leaf` is the 5-octet value from step 1.
3. Build a binary Merkle tree by repeatedly hashing pairs of sibling nodes: `SHA-256(0x01 || left || right)`. If the number of nodes at any level is odd, the last node is promoted to the next level without hashing.
4. The root of this tree is the `merkle_root` value in the SCOPE_DIGEST frame.

The 0x00 and 0x01 domain-separation prefixes distinguish leaf hashes from interior-node hashes, preventing second-preimage attacks in which an interior node is presented as a leaf (or vice versa). This construction follows the approach used for Merkle Tree Hashes in Certificate Transparency.

This construction is deterministic: any two implementations processing the same set of entity statuses MUST produce the same Merkle root.

Because each Entity ID contributes exactly one leaf, an implementation MUST NOT recycle an Entity ID within a scope whose SCOPE_DIGEST has not yet been computed. Cursor-based recycling (Section 9.1) already guarantees this when scopes complete before the ID space wraps; implementations whose scopes approach the ID-space capacity MUST close and digest the scope before reusing any of its Entity IDs.

## Rehydration Readiness Tracking

Implementations MUST track Assembly Manifest resolution order using a local data structure that can efficiently identify the next parent entity eligible for rehydration as child statuses arrive out of order.

The specific algorithm and internal representation are implementation choices and are outside the scope of this specification.

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
