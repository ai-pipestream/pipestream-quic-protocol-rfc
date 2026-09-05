# Rehydration Semantics

Section 9.8 defines an opt-in sealed-work profile that replaces the
recycling, checkpoint-cut, and GOAWAY rules for its sessions. Other
connections continue to use Sections 9.1 through 9.7.

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

The Assembly Manifest is a local data structure maintained by each endpoint to track the parent-child relationships created during dehydration. It is not transmitted on the wire; rather, each endpoint constructs its own manifest from the (`parent-scope-id`, `parent-id`) pairs in received Layer 1 EntityHeaders and from status updates observed on the Control Stream. Layer 0 relationships in the root scope continue to use `parent-id` alone. The CDDL below defines the logical structure of each entry; implementations MAY use any internal representation that preserves the required semantics.

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
  checkpoint-entity-id: entity-id,
  ? scope-id: uint32,
  ? flags: checkpoint-flags,
  ? timeout-ms: uint,
}

checkpoint-flags = uint .le 1
                   ; Bit 0: ACK. All other bits are invalid.
~~~~

- `checkpoint_id`: an opaque identifier for logging and correlation.
- `checkpoint_entity_id`: the numeric ordering key used for barrier evaluation.

Implementations MUST use `checkpoint_entity_id` (not `checkpoint_id`) when evaluating Condition 1.

The endpoint requesting a barrier sends a CHECKPOINT frame with Flags set to 0. The processing endpoint MUST NOT acknowledge it until all three conditions above are satisfied. It then sends a CHECKPOINT frame with the same `checkpoint-id`, `sequence-number`, `checkpoint-entity-id`, and `scope-id`, and with the ACK flag (bit 0) set. All other flag bits are invalid and MUST cause PIPESTREAM_FRAME_ERROR (0x0D). A requester that receives an acknowledgement whose identifying fields do not match the outstanding checkpoint MUST close the connection with PIPESTREAM_ENTITY_INVALID (0x05).

The originating endpoint MUST NOT advance its cursor beyond `checkpoint-entity-id` until it has received the matching acknowledgement. A checkpoint is optional for ordinary Layer 0 transfer, but when one is used this request/acknowledgement exchange is the wire evidence that the barrier was crossed.

An unsatisfied checkpoint is pending, not a malformed request. The receiver
MUST continue accepting control traffic and eligible descendant work while
it waits. Its deadline starts when the complete request is received and
uses a local monotonic clock. `timeout-ms` defaults to 30000. Repetition
of the same request MUST NOT extend its deadline. Reuse of its scope and
sequence with different fields is PIPESTREAM_ENTITY_INVALID (0x05).
Expiry closes the connection with PIPESTREAM_CHECKPOINT_TIMEOUT (0x0E),
without an ACK or any claim that outstanding work completed.

QUIC provides no ordering between Stream 0 and Entity Streams. Before
requesting a checkpoint, its originator MUST have received PROCESSING or
a subsequent lifecycle status for each entity it includes in the cut.
Writing an Entity Stream or sending PENDING is not evidence of admission.
The receiver MUST NOT acknowledge a cut while an announced entity in
that cut still lacks a validated EntityHeader and completed payload.
This admission rule does not declare the complete set of descendants;
the work-set closure issue in Appendix E remains relevant to decomposition.

For circular comparison in Condition 1, implementations MUST use the same modulo ordering as cursor management. Define `MAX = 0xFFFFFFFD` and:

`is_before(a, b) = (a != b) && (((b - a + MAX) % MAX) < (MAX / 2))`

An entity ID `a` is considered "less than checkpoint_entity_id `b`" iff `is_before(a, b)` is true.

## Scope ID Allocation (Layer 1)

When Layer 1 is negotiated, Scope IDs are 32-bit unsigned integers assigned by the endpoint that initiates the dehydration. The allocation scheme is as follows:

1. Scope ID 0 is the root scope and MUST NOT be used for child scopes.
2. The dehydrating endpoint assigns a unique Scope ID to each new child scope created during dehydration. The Scope ID MUST be unique within the connection for the lifetime of that scope (i.e., until the scope's SCOPE_DIGEST frame has been sent and all entities in the scope have reached terminal state).
3. Scope IDs MAY be allocated sequentially or randomly; the protocol does not require any particular ordering. Sequential allocation is RECOMMENDED for simplicity and debuggability.
4. Once a scope has been closed (its SCOPE_DIGEST has been sent and all entities within the scope have reached terminal state), the Scope ID MAY be reused for a new scope. Implementations MUST ensure that no in-flight status frames reference a recycled Scope ID before reuse.

## Scope Digest Propagation (Layer 1)

When a scope completes, the endpoint MUST compute a Scope Digest and propagate it to the parent scope via a SCOPE_DIGEST frame (Section 6.3).

A child scope MUST contain at least one Entity before it is closed. An endpoint
MUST reject a SCOPE_DIGEST for an empty or unknown scope with
PIPESTREAM_SCOPE_INVALID (0x09). The digest covers direct Entity statuses in
the named scope. Nested scope integrity is verified independently by requiring
each nested scope's SCOPE_DIGEST before its parent scope can close.

The Merkle root in the Scope Digest is computed as follows:

1. For each entity in the scope, ordered by ascending numeric Entity ID value, construct a 5-octet leaf value by concatenating:
   - The 4-octet big-endian Entity ID.
   - A 1-octet status field where the lower 4 bits contain the entity's terminal `Stat` code (Section 6.2.2) and the upper 4 bits are zero.
2. Compute each leaf hash as `SHA-256(0x00 || leaf)`, where `leaf` is the 5-octet value from step 1.
3. Build a binary Merkle tree by repeatedly hashing pairs of sibling nodes: `SHA-256(0x01 || left || right)`. If the number of nodes at any level is odd, the last node is promoted to the next level without hashing.
4. The root of this tree is the `merkle_root` value in the SCOPE_DIGEST frame.

The 0x00 and 0x01 domain-separation prefixes distinguish leaf hashes from interior-node hashes, preventing second-preimage attacks in which an interior node is presented as a leaf (or vice versa). This construction follows the approach used for Merkle Tree Hashes in Certificate Transparency.

This construction is deterministic: any two implementations processing the same set of entity statuses MUST produce the same Merkle root.

This value commits to direct entity identifiers and terminal statuses only.
It does not commit to payload bytes, result bytes, parent links, completion
policy, or nested digest values. It MUST NOT be represented as proof of
content lineage or correct computation. Payload checksums are separate.
Applications that require authenticated content receipts need an
application profile that defines those commitments and their verification.

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

## Sealed Work Sets (Private-Use Profile)

The `sealed-work-sets-v1` profile uses private-use extension identifier
65281 (0xFF01) by explicit agreement between its peers. This is a draft
profile, not an IANA assignment or a complete bidirectional work protocol.
It requires Layer 1 and excludes Layer 2. A server selecting the profile
with any other layer combination MUST refuse CONNECT with
PIPESTREAM_EXTENSION_UNSUPPORTED (0x0F). A client MUST reject an invalid
selected combination with PIPESTREAM_FRAME_ERROR (0x0D).

All entities and declarations in a profile session originate at the QUIC
client. The server processes work and reports statuses. The client MUST
require this extension on every connection attaching to the session;
automatic fallback to an unsealed session is prohibited. This profile
does not activate yield, claim redemption, retry policies, or bidirectional
work origination. Those require separately specified contracts.

### Identity and Scope Ownership

The first WORK_SET request declares root scope 0, sequence 0, and a
nonzero 16-octet `producer-id`. The `session-id` uses the bounded ASCII
syntax in Section 11.6.1. The receiver MUST persist this producer binding
with the session. Every subsequent declaration on the connection MUST
have the same session and producer. EntityHeaders inherit the binding;
any explicit session metadata MUST agree with it.

Within the issuing authority, durable entity identity is the tuple of
session ID, producer ID, scope ID, and entity ID. Neither entity IDs within
a scope nor scope IDs within a session may be recycled, including after
completion or reconnection. IDs remain in 1..4294967292; exhaustion requires
a new session identity. Session IDs MUST NOT be reused for unrelated work.
The producer label distinguishes ownership but is not an authentication
credential or proof of authority. Section 10's authorization requirements
remain necessary before exposing durable sessions to untrusted callers.

Each child scope has exactly one parent in an existing scope. Its parent
MUST already be admitted and DEHYDRATING. The declared parent, producer,
and scope binding are immutable. A parent has at most one child scope.
This profile does not buffer declarations whose parent is not yet admitted.

The circular-window arithmetic of Section 9.1 does not apply. In this
profile, `max-window-size` limits outstanding PENDING announcements, not
the numeric distance between identifiers. Admission of the corresponding
entity releases its announcement slot. Declaration ACKs do not reserve
payload buffer space or execution capacity. QUIC flow control and local
bounded receive limits govern Entity Streams, including those sent without
PENDING; resource exhaustion MUST be reported, not treated as completion.

### Declaration, Seal, and Acknowledgment

WORK_SET uses UCF type 0x83 with the serialized `work-set-frame` in
Appendix C. It contains session and producer identity, scope, sequence,
an `entity-ids` array, flags, and optional parent and seal fields.
`parent-id` and `parent-scope-id` MUST occur together on child scopes and
MUST both be absent for scope 0. The flags are:

| Bit | Meaning |
|-----|---------|
| 0 | ACK: response to this exact declaration |
| 1 | SEAL: this is the final declaration batch for this scope |
| 2-7 | Reserved; MUST be zero |

The client sends requests with ACK clear. Sequences start at 0 independently
in each scope and increment by one for each new batch. IDs MUST be strictly
increasing, both within a batch and across batches. A batch contains at most
256 IDs. An empty batch is permitted only with SEAL and only if previous
batches declared at least one ID. A scope cannot seal an empty work set.
The receiver enforces the negotiated per-scope entity limit and MAY enforce
a lower aggregate local resource budget with PIPESTREAM_LIMIT_EXCEEDED.

The producer MAY send payloads and further batches while a scope remains
unsealed, but MUST wait for the exact declaration ACK covering an entity
before sending its PENDING or Entity Stream. QUIC stream order is not
admission evidence. An undeclared entity is PIPESTREAM_ENTITY_INVALID.
Payload validation, execution, and completion remain separate from
declaration acknowledgment. A WORK_SET ACK MUST NOT be presented as proof
that any payload was received or any computation finished.

SEAL MUST occur if and only if `seal-digest` is present. The receiver verifies
the digest against all declared IDs, including this final batch, before
committing the batch. A mismatch is PIPESTREAM_INTEGRITY_ERROR (0x04) and
MUST leave the prior declaration state unchanged. After sealing, the set
cannot grow or change. Payloads for already-declared, not-yet-admitted IDs
may still arrive. Cancellation, stream reset, payload rejection, or client
disconnect MUST NOT remove a declared ID or imply completion. A missing
entity keeps completion pending; timeout or connection failure reports no
successful barrier. This profile has no implicit cancellation tombstone.

After durable commit, the server echoes the same fields with ACK set.
The client MUST compare all fields with its outstanding request; a mismatch
is PIPESTREAM_ENTITY_INVALID (0x05). The receiver retains the identity of
each accepted request. Repetition of an identical request returns the same
ACK, including after sealing or reconnecting. Reusing a sequence with changed
fields, skipping a sequence, changing identity or parent, or extending a
sealed set is PIPESTREAM_ENTITY_INVALID. Malformed field types, flags,
ordering within a batch, or missing fields are PIPESTREAM_FRAME_ERROR.

A new connection attaches by repeating the root sequence-0 request. The
receiver MUST require the original producer, declaration, and profile,
and MUST refuse connection limits that cannot accommodate the retained
session. It MUST NOT convert an existing unsealed-mode session into this
profile or interpret this session through the unsealed lifecycle.

### Seal Hash

The seal is SHA-256 over the concatenation below. Integers are unsigned
big-endian; the domain string is ASCII without a terminator. This encoding
is independent of CBOR optional-member omission and batch boundaries.

1. `pipestream-work-set-v1`.
2. Two-octet session ID length, followed by the session ID's ASCII octets.
3. Sixteen-octet producer ID, then four-octet scope ID.
4. One octet: 0 for no parent, 1 for a parent. If 1, append the parent's
   four-octet scope ID and four-octet entity ID.
5. Eight-octet final entity count.
6. Each declared entity ID, four octets, in ascending order.

The seal commits to ownership labels, parent identity, scope, final count,
and declared identifiers. It is not a content receipt, an authorization
token, or proof of correct processing. Payload and result commitments are
separate, as described in Section 9.5.

### Completion, Checkpoints, and Shutdown

A scope's SCOPE_DIGEST MUST NOT be accepted until its set is sealed, every
declared entity has been admitted and resolved, and all child scopes have
closed under Section 9.5. Observing only the children received so far is
insufficient. The existing status Merkle construction remains unchanged.

In this profile, CHECKPOINT always covers the entire sealed scope.
`checkpoint-entity-id` is the inclusive largest declared ID, not a circular
exclusive bound. A request may arrive before the seal or final payload;
it remains pending under Section 9.3's connection-local deadline. Once the
scope is ready, a wrong bound is PIPESTREAM_ENTITY_INVALID. ACK additionally
requires the existing manifest and nested-checkpoint conditions. Each ACK
is therefore a scope-qualified completion cursor over a fixed set. STATUS
cursor updates and ID recycling MUST NOT be used in this profile.

After reconnecting, a retransmitted checkpoint starts a new connection-local
wait against the same immutable set. This does not retroactively satisfy
a timed-out request or assert that processing completed during disconnect.
Retained declarations and entity states continue to determine readiness.

GOAWAY's Last Entity ID MUST equal the inclusive largest root ID and MUST
match an acknowledged root checkpoint. Pending checkpoints, announcements,
or partial chunk assemblies prevent shutdown acknowledgment. Root readiness
includes all descendant scopes through the manifest rules, so GOAWAY does
not silently omit work in another scope.
