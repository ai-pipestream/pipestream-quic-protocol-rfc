# Terminology

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "NOT RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in BCP 14 {{RFC2119}} {{RFC8174}} when, and only when, they appear in all capitals, as shown here.

## Protocol Entities

**Entity**
:   The fundamental unit of data flowing through a PipeStream pipeline. An Entity represents either a complete input or a constituent part of a decomposed input. Each Entity possesses a unique identifier within its processing scope and carries payload data tagged with a transport-level layer value. Entities are immutable once created; transformations produce new Entities rather than modifying existing ones. An input enters the pipeline as a single root Entity and MAY be decomposed into multiple Entities during processing; it is considered complete when its root Entity (or the rehydrated result of its decomposition) exits the pipeline.

**Scope**
:   A hierarchical namespace for Entity IDs. Each scope maintains its own Entity ID space, cursor, and Assembly Manifest. Scopes enable root entities to contain parts, and parts to contain sub-tasks, each with independent ID management. (Protocol Layer 1)

## Dehydration and Rehydration

**Scatter-Gather**
:   The distributed processing pattern implemented by PipeStream. A single input is "scattered" (dehydrated) into multiple parts for parallel processing, and the results are "gathered" (rehydrated) back into a single output. PipeStream extends classical scatter-gather with recursive nesting: any scattered part may itself be scattered further.

**Dehydrate (Scatter)**
:   The operation of decomposing an Entity into multiple constituent Entities for parallel or distributed processing. When an Entity is dehydrated, the originating node MUST create an Assembly Manifest entry recording the identifiers of all resulting sub-entities. The dehydration operation is recursive; a sub-entity produced by dehydration MAY itself be dehydrated, creating a tree of decomposition. Dehydration transitions data from a solid state (a single complete unit) to a fluid state (multiple in-flight entities).

**Rehydrate (Gather)**
:   The operation of reassembling multiple Entities back into a single composite Entity. A rehydrate operation MUST NOT proceed until all constituent Entities listed in the corresponding Assembly Manifest entry have been received and processed (or handled according to the Completion Policy). Rehydration transitions data from a fluid state back to a solid state.

**Solid State**
:   An Entity that exists as a complete, stored unit or as a single root Entity entering or exiting a pipeline. Contrast with "fluid state".

**Fluid State**
:   An input that has been decomposed into multiple in-flight Entities being processed in parallel across distributed nodes. An input is in the fluid state between dehydration and rehydration. Contrast with "solid state".

## Consistency Mechanisms

**Checkpoint**
:   A scope-qualified synchronization request whose covered work must resolve before acknowledgment. Section 9.3 defines the unsealed cut; Section 9.8 replaces it with a fixed sealed work set. A checkpoint does not stop unrelated scopes or itself prove correct computation.

**Barrier**
:   A synchronization point scoped to a specific subtree, blocking entities dependent on a specific parent's descendants. Checkpoints also identify a scope, but use a different request and completion rule. (Protocol Layer 1)

**Control Stream**
:   The control stream that tracks Entity completion status throughout the processing pipeline. The Control Stream is transmitted on a dedicated QUIC stream parallel to the data streams.

**Assembly Manifest**
:   An endpoint-local data structure tracking a composite Entity and its constituent sub-entities. It is not a transmitted Control Stream frame; endpoints maintain it from the applicable lifecycle messages (Section 9.2).

**Cursor**
:   A pointer to the lowest unresolved Entity ID within a scope. Entity IDs behind the cursor are considered resolved and MAY be recycled. The cursor enables efficient ID space management without global coordination.

## Resilience Mechanisms (Protocol Layer 2)

**Yield**
:   A temporary pause in Entity processing, typically due to external dependencies (API calls, rate limiting, human approval). A yielded Entity carries a continuation token enabling resumption without reprocessing.

**Claim Check**
:   A detached reference to a deferred Entity that can be queried or resumed independently, potentially in a different session. Claim checks enable asynchronous processing patterns and retry queues.

**Completion Policy**
:   A configuration specifying how to handle partial failures during dehydration. Policies include STRICT (all must succeed), LENIENT (continue with partial results), BEST_EFFORT (complete with whatever succeeds), and QUORUM (require minimum success ratio).

## Data Representation

**Data Layer**
:   A transport-level 2-bit field carried in the Entity Header that identifies the payload representation. The concrete meaning of each layer value is defined by an application profile rather than by the core protocol.

## Additional Terms

**Pipeline**
:   A configured sequence of processing stages through which Entities flow.

**Processor**
:   A node in the mesh that performs operations on entities (e.g., transformation, dehydration, or rehydration).

**Sink**
:   A terminal stage in a pipeline where rehydrated entities or terminal entities are persisted or delivered to an external system.

**Stage**
:   A single processing step within a Pipeline.

**Scope Digest**
:   A cryptographic summary (Merkle root) of all Entity statuses within a completed scope, propagated to parent scopes for efficient verification. (Protocol Layer 1)
