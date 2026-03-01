# Section 8: Reassembly Semantics

## 8.1 Assembly Manifest

The Assembly Manifest is a distributed data structure that maintains the hierarchical relationships between dehydrated entities and their constituent parts. Each processing node MUST maintain a local Assembly Manifest for entities within its processing scope.

### 8.1.1 Manifest Entry Structure

Each Assembly Manifest entry SHALL contain the following fields (as defined in `AssemblyManifestEntry` protobuf):

```
Assembly Manifest Entry {
    Parent ID (20),
    Scope ID (32),
    Child Count (16),
    Children IDs (20) ...,
    Children Status (4) ...,
    Completion Policy (Layer 2),
    Creation Timestamp (64),
    Resolution State (8),
}
```

Field definitions:

Parent ID (20 bits):
: The identifier of the parent entity that was dehydrated.

Scope ID (32 bits):
: The identifier of the scope in which the dehydration occurred.

Child Count (16 bits):
: The number of child entities produced by dehydration.

Children IDs (variable):
: An array of 20-bit entity identifiers, one for each child.

Children Status (variable):
: An array of 4-bit status codes (EntityStatus), one for each child.

Completion Policy (Layer 2):
: The policy governing failure handling and success criteria for this decomposition.

Creation Timestamp (64 bits):
: Microseconds since the UNIX epoch when this manifest entry was created.

Resolution State (8 bits):
: The current state of the manifest entry (ResolutionState).

### 8.1.2 Completion Status Codes

Each child entity SHALL have one of the following completion status values:

| Value | Name | Layer | Description |
|-------|------|-------|-------------|
| 0x0 | PENDING | 0 | Entity announced, not yet transmitting |
| 0x1 | PROCESSING | 0 | Entity transmission in progress |
| 0x2 | COMPLETE | 0 | Entity successfully processed |
| 0x3 | FAILED | 0 | Entity processing failed |
| 0x4 | CHECKPOINT | 0 | Synchronization barrier |
| 0x5 | DEHYDRATING | 0 | Decomposing into children |
| 0x6 | REHYDRATING | 0 | Rehydrating children |
| 0x7 | Reserved | - | Reserved |
| 0x8 | YIELDED | 2 | Paused with continuation token |
| 0x9 | DEFERRED | 2 | Detached with claim check |
| 0xA | RETRYING | 2 | Retry in progress |
| 0xB | SKIPPED | 2 | Intentionally skipped (lenient mode) |
| 0xC | ABANDONED | 2 | Timed out, cursor advanced past |

### 8.1.3 Resolution States

| Value | Name | Description |
|-------|------|-------------|
| 0x0 | ACTIVE | Entry is active, awaiting child completion |
| 0x1 | RESOLVED | All children reached terminal state |
| 0x2 | PARTIAL | Some children failed/skipped (policy met) |
| 0x3 | FAILED | Entry resolution failed |

### 8.1.4 Status Frame Format

Assembly Manifest updates are transmitted using extended 3-byte frames with the following structure:

```
Status Frame {
    Frame Type (8) = 0x50,
    Frame Flags (8),
    Extended Length (16),
    Operation (8),
    Entry Data (..),
}
```

Frame Flags:
: - Bit 0: ACK_REQUIRED - Sender requires acknowledgment
  - Bit 1: ATOMIC - Frame is part of atomic operation sequence
  - Bit 2: REPLICATED - Frame is a replication from another node
  - Bits 3-7: Reserved

Operation codes:

| Value | Name | Semantics |
|-------|------|-----------|
| 0x01 | CREATE | Create new manifest entry |
| 0x02 | UPDATE_STATUS | Update child completion status |
| 0x03 | RESOLVE | Mark entry as resolved |
| 0x04 | DELETE | Remove manifest entry |
| 0x05 | QUERY | Request manifest entry state |
| 0x06 | SYNC | Synchronize manifest state |

### 8.1.5 Atomicity Requirements

Implementations MUST satisfy the following atomicity requirements:

1. An Assembly Manifest entry MUST be created and acknowledged before any child entity is emitted. Failure to observe this requirement MAY result in orphaned children.

2. The creation of a manifest entry and the emission of the first child entity SHOULD be performed as an atomic operation where the underlying transport supports such semantics.

3. If atomicity cannot be guaranteed, implementations MUST use the following two-phase protocol:

```
Phase 1: Create manifest entry with PENDING state
Phase 2: Await CREATE acknowledgment
Phase 3: Emit child entities
Phase 4: Update manifest entry to ACTIVE state
```

4. If a failure occurs between Phase 1 and Phase 3, the manifest entry MUST be garbage collected after the timeout specified in Section 8.2.5.

5. Multiple concurrent updates to the same manifest entry MUST be serialized. Implementations MAY use optimistic concurrency control with version vectors or pessimistic locking.

### 8.1.6 Resolution Conditions

An Assembly Manifest entry SHALL be considered resolved when one of the following conditions is met:

1. ALL children have Completion Status of COMPLETE (successful resolution)

2. ALL children have a terminal Completion Status (COMPLETE, FAILED, TIMEOUT, CANCELLED, or ORPHANED) AND the PARTIAL_FAILURE_ALLOWED flag is set (partial resolution)

3. ANY child has a non-COMPLETE terminal status AND the PARTIAL_FAILURE_ALLOWED flag is NOT set (failed resolution)

4. The entry timeout has been exceeded (timeout resolution)

Upon resolution, implementations MUST:

1. Update the Resolution State to RESOLVED
2. Enqueue the entry for rehydrate processing (Section 8.3)
3. Notify any blocked checkpoints (Section 8.2)

## 8.2 Checkpoint Blocking

Checkpoints provide synchronization barriers that ensure all preceding entities have completed processing before subsequent entities may proceed.

### 8.2.1 Checkpoint Frame Format

```
Checkpoint Frame {
    Frame Type (8) = 0x51,
    Checkpoint Flags (8),
    Checkpoint ID (32),
    Scope ID (32),
    Sequence Number (64),
    Timeout (32),
    Dependent Count (16),
    Dependent IDs (20) ...,
}
```

Checkpoint Flags:
: - Bit 0: HARD_CHECKPOINT - All entities MUST complete; no partial progress
  - Bit 1: SOFT_CHECKPOINT - Allows timeout with partial completion
  - Bit 2: NESTED - This checkpoint is nested within another scope
  - Bit 3: TERMINAL - Stream terminates after this checkpoint
  - Bits 4-7: Reserved

Checkpoint ID (32 bits):
: Unique identifier for this checkpoint within the stream.

Scope ID (32 bits):
: Identifier of the checkpoint scope. Nested checkpoints share the parent's scope or define a new inner scope.

Sequence Number (64 bits):
: Monotonically increasing sequence number. All entities with sequence numbers less than this value MUST complete before the checkpoint is satisfied.

Timeout (32 bits):
: Maximum time in milliseconds to wait for checkpoint satisfaction. A value of 0x00000000 indicates no timeout (wait indefinitely). A value of 0xFFFFFFFF indicates use of the default timeout.

Dependent Count (16 bits):
: Number of specific entity IDs that must complete (0 for sequence-based checkpoints).

Dependent IDs (variable):
: Array of 64-bit entity IDs that must complete for explicit dependency checkpoints.

### 8.2.2 Blocking Semantics

When a checkpoint frame is received, the following blocking semantics SHALL apply:

1. The receiving node MUST NOT process any entity with a sequence number greater than or equal to the checkpoint's Sequence Number until the checkpoint is satisfied.

2. Entities currently in-flight (being processed) when the checkpoint arrives MAY continue processing.

3. New entities received after the checkpoint MUST be buffered until checkpoint satisfaction.

4. The checkpoint establishes a happens-before relationship: all effects of entities preceding the checkpoint MUST be visible to entities following the checkpoint.

Pseudocode for checkpoint blocking:

```
procedure HANDLE_CHECKPOINT(checkpoint):
    checkpoint_registry[checkpoint.id] := checkpoint
    checkpoint.pending_entities := GET_PENDING_BEFORE(checkpoint.sequence_number)
    checkpoint.satisfied := FALSE

    if checkpoint.pending_entities IS EMPTY:
        SATISFY_CHECKPOINT(checkpoint)
    else:
        for each entity_id in checkpoint.pending_entities:
            entity_completion_callbacks[entity_id].add(
                lambda: ON_ENTITY_COMPLETE(checkpoint, entity_id)
            )

        if checkpoint.timeout > 0:
            SCHEDULE_TIMEOUT(checkpoint.timeout,
                lambda: ON_CHECKPOINT_TIMEOUT(checkpoint))

procedure ON_ENTITY_COMPLETE(checkpoint, entity_id):
    checkpoint.pending_entities.remove(entity_id)

    if checkpoint.pending_entities IS EMPTY:
        SATISFY_CHECKPOINT(checkpoint)

procedure SATISFY_CHECKPOINT(checkpoint):
    checkpoint.satisfied := TRUE
    CANCEL_TIMEOUT(checkpoint)
    RELEASE_BLOCKED_ENTITIES(checkpoint)
    NOTIFY_CHECKPOINT_SATISFIED(checkpoint.id)
```

### 8.2.3 Checkpoint Satisfaction Conditions

A checkpoint SHALL be considered satisfied when ALL of the following conditions are met:

1. All entities with sequence numbers less than the checkpoint's Sequence Number have reached a terminal completion state.

2. If Dependent IDs are specified, all listed entities have reached a terminal completion state.

3. All Assembly Manifest entries within the checkpoint's scope have been resolved.

4. All nested checkpoints within this checkpoint's scope have been satisfied.

For SOFT_CHECKPOINT:

- The checkpoint MAY be satisfied after the timeout expires, even if some entities have not completed.
- Incomplete entities SHALL be marked with TIMEOUT status.
- The checkpoint satisfaction notification MUST indicate partial completion.

For HARD_CHECKPOINT:

- The checkpoint MUST NOT be satisfied until all conditions are met or a fatal error occurs.
- Timeout expiration for a HARD_CHECKPOINT SHALL trigger checkpoint failure, not partial satisfaction.

### 8.2.4 Nested Checkpoint Scopes

Checkpoints MAY be nested to create hierarchical synchronization boundaries. Nested checkpoint semantics are as follows:

1. An inner checkpoint MUST be satisfied before its enclosing outer checkpoint can be satisfied.

2. Inner checkpoints establish independent blocking domains; entities blocked on an inner checkpoint SHALL NOT affect the outer checkpoint's pending count until the inner checkpoint is satisfied.

3. Checkpoint scope nesting MUST form a proper hierarchy (no partial overlaps). If checkpoint A's scope contains checkpoint B's scope, then B MUST be fully contained within A.

4. The maximum nesting depth is implementation-defined but MUST be at least 16 levels.

```
Nested Checkpoint Scope Structure:

    Outer Checkpoint (Scope A)
    +------------------------------------------+
    |  Entity 1                                |
    |  Entity 2                                |
    |  Inner Checkpoint (Scope B)              |
    |  +------------------------------------+  |
    |  |  Entity 3                          |  |
    |  |  Entity 4                          |  |
    |  |  [Inner must satisfy before outer] |  |
    |  +------------------------------------+  |
    |  Entity 5                                |
    |  [Outer waits for 1,2,5 + Inner scope]   |
    +------------------------------------------+
```

### 8.2.5 Timeout Handling for Stuck Checkpoints

Implementations MUST implement timeout handling to prevent indefinite blocking:

1. Default Timeout: Implementations SHOULD use a default timeout of 300,000 milliseconds (5 minutes) when no explicit timeout is specified and the timeout field is 0xFFFFFFFF.

2. Timeout Detection: A checkpoint is considered stuck when:
   - The timeout period has elapsed, AND
   - At least one pending entity has not reached a terminal state

3. Timeout Actions for SOFT_CHECKPOINT:
   - Mark all PENDING entities as TIMEOUT
   - Resolve all affected Assembly Manifest entries
   - Satisfy the checkpoint with partial completion status
   - Emit CHECKPOINT_PARTIAL_SATISFACTION notification

4. Timeout Actions for HARD_CHECKPOINT:
   - Emit CHECKPOINT_TIMEOUT error frame
   - Mark checkpoint as FAILED
   - Propagate failure to enclosing checkpoints
   - Implementation-defined recovery (see Section 8.2.6)

```
procedure ON_CHECKPOINT_TIMEOUT(checkpoint):
    if checkpoint.satisfied:
        return  // Already satisfied, ignore timeout

    if checkpoint.flags.SOFT_CHECKPOINT:
        for each entity_id in checkpoint.pending_entities:
            SET_ENTITY_STATUS(entity_id, TIMEOUT)

        for each manifest_entry in GET_MANIFEST_ENTRIES(checkpoint.scope_id):
            if manifest_entry.resolution_state = ACTIVE:
                FORCE_RESOLVE_MANIFEST_ENTRY(manifest_entry, TIMEOUT)

        SATISFY_CHECKPOINT_PARTIAL(checkpoint)

    else:  // HARD_CHECKPOINT
        FAIL_CHECKPOINT(checkpoint, TIMEOUT_EXCEEDED)
        PROPAGATE_FAILURE_TO_ENCLOSING(checkpoint)
```

### 8.2.6 Recovery from Checkpoint Failures

When a checkpoint fails, implementations MUST follow this recovery procedure:

1. Immediate Actions:
   - Halt emission of new entities in the affected scope
   - Cancel all pending operations in the affected scope
   - Preserve state for potential retry

2. Failure Notification:
   - Send CHECKPOINT_FAILED frame to all participants
   - Include failure reason and affected entity list
   - Notify monitoring/alerting systems

3. Recovery Options (implementation-defined):
   - ABORT: Terminate the stream with an error
   - RETRY: Re-execute failed entities and retry checkpoint
   - SKIP: Skip the checkpoint and continue (data loss risk)
   - ROLLBACK: Restore to last successful checkpoint

4. State Cleanup:
   - Release blocked entities (with appropriate error status)
   - Clean up Assembly Manifest entries for failed scope
   - Free resources held for checkpoint

```
Checkpoint Failure Frame {
    Frame Type (8) = 0x52,
    Failure Reason (8),
    Checkpoint ID (32),
    Failed Entity Count (16),
    Failed Entity IDs (20) ...,
    Recovery Hint (8),
    Diagnostic Data Length (16),
    Diagnostic Data (..),
}
```

Failure Reason codes:

| Value | Name |
|-------|------|
| 0x01 | TIMEOUT_EXCEEDED |
| 0x02 | ENTITY_PROCESSING_FAILED |
| 0x03 | RESOURCE_EXHAUSTED |
| 0x04 | NETWORK_PARTITION |
| 0x05 | INTERNAL_ERROR |

## 8.3 Eventual Consistency (Fibonacci Heap)

Due to the distributed nature of PipeStream processing, child entities MAY complete out of order. This section specifies the mechanism for efficiently tracking completion status and triggering rehydrations when all children of a dehydrated entity have completed.

### 8.3.1 Out-of-Order Entity Arrival Handling

Implementations MUST handle out-of-order completion notifications:

1. Each completion notification MUST be idempotent; duplicate notifications for the same entity MUST be safely ignored.

2. Completion notifications MUST include sufficient information to locate the relevant Assembly Manifest entry (parent_id or manifest entry reference).

3. Implementations MUST NOT assume any ordering of completion notifications, even for children emitted in sequence.

4. Completion notifications received before the corresponding manifest entry exists MUST be buffered for a grace period (minimum 30 seconds) before being discarded as orphans.

### 8.3.2 Priority Queue Structure

Implementations SHALL use a priority queue to efficiently track which Assembly Manifest entries are ready for rehydration. This specification RECOMMENDS a Fibonacci heap due to its O(1) amortized decrease-key operation.

Priority Queue Properties:

- Key: Number of completed children (completion_count)
- Value: Reference to Assembly Manifest entry
- Ordering: Entries with completion_count equal to child_count have highest priority

The Fibonacci heap provides the following complexity guarantees:

| Operation | Amortized Complexity |
|-----------|---------------------|
| Insert | O(1) |
| Find-min | O(1) |
| Extract-min | O(log n) |
| Decrease-key | O(1) |
| Merge | O(1) |

For PipeStream, the "decrease-key" operation is repurposed as an "increase-completion-count" operation, which maintains heap ordering by moving entries toward the root as they approach full completion.

### 8.3.3 Completion Count Tracking

```
Fibonacci Heap Node Structure:

    FibHeapNode {
        manifest_entry_ref: Reference to Assembly Manifest Entry,
        completion_count: Integer,
        target_count: Integer (equal to manifest_entry.child_count),
        priority: Float (computed as target_count - completion_count),
        parent: FibHeapNode reference,
        child: FibHeapNode reference,
        left: FibHeapNode reference,
        right: FibHeapNode reference,
        degree: Integer,
        marked: Boolean,
    }
```

Priority Calculation:

The priority SHALL be calculated such that entries closer to completion have LOWER priority values (min-heap behavior triggers rehydrate on extract-min):

```
priority = target_count - completion_count
```

When priority reaches 0, the entry is ready for rehydrate and will be at the top of the heap.

### 8.3.4 Bubble-Up on Completion

When a child entity completes, the following procedure updates the heap:

```
procedure ON_CHILD_COMPLETE(parent_id, child_id, status):
    manifest_entry := assembly_manifest[parent_id]
    if manifest_entry IS NULL:
        BUFFER_ORPHAN_COMPLETION(parent_id, child_id, status)
        return

    child_index := FIND_CHILD_INDEX(manifest_entry, child_id)
    if child_index = -1:
        ERROR("Unknown child entity")
        return

    if manifest_entry.completion_status[child_index] != PENDING:
        return  // Already completed, idempotent handling

    manifest_entry.completion_status[child_index] := status

    heap_node := heap_node_index[parent_id]
    heap_node.completion_count := heap_node.completion_count + 1
    new_priority := heap_node.target_count - heap_node.completion_count

    DECREASE_KEY(rehydrate_heap, heap_node, new_priority)

    if new_priority = 0:
        // Entry is now ready for rehydrate - will be at heap root
        SIGNAL_REHYDRATE_READY()

procedure DECREASE_KEY(heap, node, new_priority):
    if new_priority > node.priority:
        ERROR("New priority must be less than current priority")
        return

    node.priority := new_priority
    parent := node.parent

    if parent IS NOT NULL AND node.priority < parent.priority:
        CUT(heap, node, parent)
        CASCADING_CUT(heap, parent)

    if node.priority < heap.min.priority:
        heap.min := node

procedure CUT(heap, node, parent):
    REMOVE_FROM_CHILD_LIST(parent, node)
    parent.degree := parent.degree - 1
    ADD_TO_ROOT_LIST(heap, node)
    node.parent := NULL
    node.marked := FALSE

procedure CASCADING_CUT(heap, node):
    parent := node.parent
    if parent IS NOT NULL:
        if node.marked = FALSE:
            node.marked := TRUE
        else:
            CUT(heap, node, parent)
            CASCADING_CUT(heap, parent)
```

### 8.3.5 Rehydrate Triggering on Extract-Min

The rehydrate processor continuously monitors the heap and triggers rehydrations:

```
procedure REHYDRATE_PROCESSOR():
    loop:
        WAIT_FOR(rehydrate_heap.min.priority = 0 OR shutdown_signal)

        if shutdown_signal:
            break

        while rehydrate_heap IS NOT EMPTY AND rehydrate_heap.min.priority = 0:
            node := EXTRACT_MIN(rehydrate_heap)
            manifest_entry := node.manifest_entry_ref

            if VALIDATE_REHYDRATE_PRECONDITIONS(manifest_entry):
                EXECUTE_REHYDRATE(manifest_entry)
            else:
                HANDLE_REHYDRATE_FAILURE(manifest_entry)

procedure EXTRACT_MIN(heap):
    min_node := heap.min

    if min_node IS NOT NULL:
        // Add children to root list
        for each child in min_node.children:
            ADD_TO_ROOT_LIST(heap, child)
            child.parent := NULL

        REMOVE_FROM_ROOT_LIST(heap, min_node)

        if min_node = min_node.right:
            heap.min := NULL
        else:
            heap.min := min_node.right
            CONSOLIDATE(heap)

    return min_node

procedure VALIDATE_REHYDRATE_PRECONDITIONS(manifest_entry):
    // All children must have terminal status
    for each status in manifest_entry.completion_status:
        if status = PENDING:
            return FALSE

    // Check PARTIAL_FAILURE_ALLOWED flag
    if NOT manifest_entry.flags.PARTIAL_FAILURE_ALLOWED:
        for each status in manifest_entry.completion_status:
            if status != COMPLETE:
                return FALSE

    // Verify checkpoint scope allows rehydrate
    if NOT CHECKPOINT_SCOPE_ALLOWS_REHYDRATE(manifest_entry.checkpoint_scope):
        return FALSE

    return TRUE
```

### 8.3.6 Memory Bounds for Pending Entries

To prevent unbounded memory growth, implementations MUST enforce limits on pending entries:

1. Maximum Pending Entries: Implementations MUST support a configurable maximum number of pending heap entries. The default SHOULD be 1,000,000 entries.

2. Maximum Entry Age: Entries that have been pending for longer than the maximum age MUST be eligible for eviction. The default maximum age SHOULD be 3,600 seconds (1 hour).

3. Memory Pressure Response: When approaching memory limits, implementations SHOULD:
   - Apply backpressure to upstream producers
   - Evict oldest entries (LRU)
   - Log warnings for monitoring

4. Eviction Procedure:
```
procedure ENFORCE_MEMORY_BOUNDS():
    while heap.size > MAX_PENDING_ENTRIES OR MEMORY_PRESSURE_HIGH():
        // Find oldest entry that is not close to completion
        candidate := FIND_EVICTION_CANDIDATE()

        if candidate IS NOT NULL:
            EVICT_ENTRY(candidate)
            LOG_WARNING("Evicted pending entry due to memory pressure",
                        candidate.manifest_entry_ref.parent_id)
        else:
            // All entries are close to completion, apply backpressure
            APPLY_UPSTREAM_BACKPRESSURE()
            break

procedure FIND_EVICTION_CANDIDATE():
    // Prefer entries that are far from completion and old
    candidates := FILTER(heap.entries,
        entry -> entry.completion_count < entry.target_count / 2)

    if candidates IS EMPTY:
        return NULL

    return MIN_BY(candidates,
        entry -> entry.manifest_entry_ref.creation_timestamp)
```

5. Memory Accounting: Each heap node requires approximately:
   - Fixed overhead: 96 bytes (pointers, counters, flags)
   - Manifest reference: 8 bytes
   - Total per entry: ~104 bytes minimum

   Implementations SHOULD reserve additional memory for heap restructuring operations.

## 8.4 Parent Reference Resolution

Parent references establish the hierarchical structure of dehydrated entities and enable proper reassembly.

### 8.4.1 Parent ID Field Semantics

The parent_id field in entity metadata SHALL be interpreted as follows:

1. The parent_id references the entity that was dehydrated to produce this entity.

2. The parent_id MUST correspond to a valid Assembly Manifest entry, except for root entities.

3. The parent_id is immutable once set; it MUST NOT be modified during entity lifecycle.

4. The parent_id namespace is global within a PipeStream session; implementations MUST ensure uniqueness across all participating nodes.

### 8.4.2 Root Entity Identification

Root entities are top-level entities that were not produced by dehydration. Root entities SHALL be identified by one of the following:

1. Null Parent: parent_id = 0x0000000000000000

2. Self-Referential: parent_id = entity_id (the entity references itself)

Implementations MUST treat both representations as equivalent for root entity detection:

```
procedure IS_ROOT_ENTITY(entity):
    return entity.parent_id = 0 OR entity.parent_id = entity.id
```

Root entities:
- Do NOT require an Assembly Manifest entry (they have no parent to track them)
- MAY be dehydrated to produce children (becoming internal nodes)
- Represent the entry points of document processing pipelines

### 8.4.3 Recursive Dehydration Chains

Entities MAY be dehydrated recursively, creating chains of parent-child relationships:

```
Document (Root)
    |
    +--dehydrate--> Section 1
    |                  |
    |                  +--dehydrate--> Paragraph 1.1
    |                  |                  |
    |                  |                  +--dehydrate--> Sentence 1.1.1
    |                  |                  +--dehydrate--> Sentence 1.1.2
    |                  |
    |                  +--dehydrate--> Paragraph 1.2
    |
    +--dehydrate--> Section 2
```

Chain Properties:

1. Each non-root entity has exactly one parent.

2. Chains MAY be arbitrarily deep, limited only by implementation resources.

3. Rehydrating MUST proceed bottom-up: leaf entities rehydrate first, then their parents, recursively up to the root.

4. A parent entity MUST NOT rehydrate until ALL of its children have rehydrated.

Resolution Order:

```
procedure DETERMINE_REHYDRATE_ORDER(root_id):
    order := []
    visited := {}

    POST_ORDER_TRAVERSE(root_id, order, visited)

    return order

procedure POST_ORDER_TRAVERSE(entity_id, order, visited):
    if entity_id IN visited:
        return

    visited.add(entity_id)

    manifest_entry := assembly_manifest[entity_id]
    if manifest_entry IS NOT NULL:
        for each child_id in manifest_entry.children_ids:
            POST_ORDER_TRAVERSE(child_id, order, visited)

    order.append(entity_id)
```

### 8.4.4 Orphan Detection and Handling

Orphaned entities are children whose parent cannot be located or has been terminated. Implementations MUST detect and handle orphans:

Detection Conditions:

1. A completion notification references a parent_id with no corresponding Assembly Manifest entry, AND the grace period (30 seconds) has elapsed.

2. A parent entity is explicitly terminated (CANCELLED, FAILED) before all children complete.

3. An Assembly Manifest entry is evicted due to memory pressure while children are still processing.

Handling Procedure:

```
procedure DETECT_ORPHANS():
    for each buffered_completion in orphan_buffer:
        if CURRENT_TIME() - buffered_completion.timestamp > GRACE_PERIOD:
            HANDLE_ORPHAN(buffered_completion)

procedure HANDLE_ORPHAN(completion):
    orphan_id := completion.child_id
    claimed_parent := completion.parent_id

    // Attempt to locate parent in distributed manifest
    remote_entry := QUERY_REMOTE_MANIFESTS(claimed_parent)

    if remote_entry IS NOT NULL:
        // Parent found on remote node, forward completion
        FORWARD_COMPLETION(remote_entry.node, completion)
        return

    // Orphan confirmed
    LOG_WARNING("Orphaned entity detected", orphan_id, claimed_parent)

    // Options for handling:
    if ORPHAN_POLICY = DISCARD:
        DISCARD_ENTITY_RESULTS(orphan_id)

    else if ORPHAN_POLICY = ADOPT:
        // Create synthetic root and process independently
        ADOPT_AS_ROOT(orphan_id)

    else if ORPHAN_POLICY = QUARANTINE:
        // Move to quarantine for manual review
        QUARANTINE_ENTITY(orphan_id)

    EMIT_ORPHAN_NOTIFICATION(orphan_id, claimed_parent)
```

### 8.4.5 Cycle Prevention (DAG Enforcement)

The parent-child relationship graph MUST form a Directed Acyclic Graph (DAG). Cycles would cause infinite rehydrate loops and MUST be prevented.

Prevention Mechanisms:

1. Monotonic ID Assignment: If entity IDs are assigned monotonically, children MUST have IDs greater than their parent. This trivially prevents cycles.

2. Depth Tracking: Each entity carries a depth counter; children have depth = parent.depth + 1. Implementations MUST reject dehydration that would create children with depth exceeding a maximum (default: 1024).

3. Ancestry Verification: Before creating an Assembly Manifest entry, verify the parent is not a descendant of any proposed child.

```
procedure VERIFY_DAG_PROPERTY(parent_id, proposed_children):
    parent_ancestors := GET_ANCESTORS(parent_id)

    for each child_id in proposed_children:
        if child_id IN parent_ancestors:
            ERROR("Cycle detected: child is ancestor of parent")
            return FALSE

        if child_id = parent_id:
            ERROR("Self-reference in children")
            return FALSE

    return TRUE

procedure GET_ANCESTORS(entity_id):
    ancestors := {}
    current := entity_id

    while current != 0 AND current NOT IN ancestors:
        ancestors.add(current)
        manifest_entry := FIND_MANIFEST_ENTRY_FOR_CHILD(current)

        if manifest_entry IS NULL:
            break

        current := manifest_entry.parent_id

    return ancestors

procedure DEHYDRATE_WITH_DAG_CHECK(parent_id, children):
    if NOT VERIFY_DAG_PROPERTY(parent_id, children):
        ABORT_DEHYDRATION("DAG violation")
        return

    // Proceed with dehydration
    CREATE_MANIFEST_ENTRY(parent_id, children)
    EMIT_CHILDREN(children)
```

DAG Violation Frame:

When a DAG violation is detected, implementations MUST emit a violation frame:

```
DAG Violation Frame {
    Frame Type (8) = 0x53,
    Violation Type (8),
    Entity A (20),
    Entity B (20),
    Diagnostic Info Length (16),
    Diagnostic Info (..),
}
```

Violation Types:

| Value | Name |
|-------|------|
| 0x01 | CYCLE_DETECTED |
| 0x02 | SELF_REFERENCE |
| 0x03 | DEPTH_EXCEEDED |

## 8.5 Complete Reassembly Algorithm

The following pseudocode presents the complete reassembly algorithm integrating all components described in this section:

```
// Global state
assembly_manifest := HashMap<ParentID, ManifestEntry>
rehydrate_heap := FibonacciHeap<HeapNode>
heap_node_index := HashMap<ParentID, HeapNode>
checkpoint_registry := HashMap<CheckpointID, Checkpoint>
orphan_buffer := List<BufferedCompletion>

procedure INITIALIZE_REASSEMBLY_SUBSYSTEM():
    START_REHYDRATE_PROCESSOR_THREAD()
    START_ORPHAN_DETECTOR_THREAD()
    START_MEMORY_MONITOR_THREAD()

procedure ON_DEHYDRATE(parent_entity, child_entities):
    // Validate DAG property
    child_ids := [child.id for child in child_entities]
    if NOT VERIFY_DAG_PROPERTY(parent_entity.id, child_ids):
        ABORT_DEHYDRATION("DAG violation")
        return ERROR

    // Create manifest entry atomically before emitting children
    manifest_entry := ManifestEntry {
        parent_id: parent_entity.id,
        child_count: LENGTH(child_entities),
        children_ids: child_ids,
        completion_status: [PENDING] * LENGTH(child_entities),
        checkpoint_scope: CURRENT_CHECKPOINT_SCOPE(),
        creation_timestamp: CURRENT_TIME_MICROS(),
        resolution_state: ACTIVE,
        flags: parent_entity.dehydration_flags,
    }

    // Persist manifest entry
    SEND_STATUS_FRAME(CREATE, manifest_entry)
    AWAIT_STATUS_ACK()

    assembly_manifest[parent_entity.id] := manifest_entry

    // Create heap node for tracking
    heap_node := HeapNode {
        manifest_entry_ref: manifest_entry,
        completion_count: 0,
        target_count: manifest_entry.child_count,
        priority: manifest_entry.child_count,  // Far from completion
    }

    INSERT(rehydrate_heap, heap_node)
    heap_node_index[parent_entity.id] := heap_node

    // Now safe to emit children
    for each child in child_entities:
        EMIT_ENTITY(child)

    return SUCCESS

procedure ON_ENTITY_COMPLETION(entity_id, parent_id, status):
    // Check for orphan condition
    if parent_id NOT IN assembly_manifest:
        BUFFER_ORPHAN_COMPLETION(parent_id, entity_id, status)
        return

    manifest_entry := assembly_manifest[parent_id]

    // Find and update child status (idempotent)
    child_index := FIND_INDEX(manifest_entry.children_ids, entity_id)
    if child_index = -1:
        LOG_ERROR("Unknown child", entity_id, parent_id)
        return

    if manifest_entry.completion_status[child_index] != PENDING:
        return  // Already completed

    manifest_entry.completion_status[child_index] := status
    SEND_STATUS_FRAME(UPDATE_STATUS, parent_id, child_index, status)

    // Update heap
    heap_node := heap_node_index[parent_id]
    heap_node.completion_count := heap_node.completion_count + 1
    new_priority := heap_node.target_count - heap_node.completion_count
    DECREASE_KEY(rehydrate_heap, heap_node, new_priority)

    // Check checkpoint notifications
    NOTIFY_CHECKPOINT_PROGRESS(entity_id)

procedure REHYDRATE_PROCESSOR():
    loop:
        // Wait for entry ready for rehydrate
        WAIT_FOR(rehydrate_heap.min.priority = 0 OR shutdown)

        if shutdown:
            break

        // Process all ready entries
        while rehydrate_heap.min IS NOT NULL AND rehydrate_heap.min.priority = 0:
            node := EXTRACT_MIN(rehydrate_heap)
            manifest_entry := node.manifest_entry_ref
            parent_id := manifest_entry.parent_id

            // Remove from index
            DELETE heap_node_index[parent_id]

            // Validate preconditions
            if NOT VALIDATE_REHYDRATE_PRECONDITIONS(manifest_entry):
                HANDLE_REHYDRATE_FAILURE(manifest_entry)
                continue

            // Check for recursive dependencies
            for each child_id in manifest_entry.children_ids:
                if child_id IN assembly_manifest:
                    // Child is also a parent, must rehydrate first
                    child_manifest := assembly_manifest[child_id]
                    if child_manifest.resolution_state != REHYDRATED:
                        // Re-queue parent, child not ready
                        REQUEUE_AFTER_CHILD(node, child_id)
                        continue

            // Execute rehydrate
            manifest_entry.resolution_state := REHYDRATING
            SEND_STATUS_FRAME(RESOLVE, parent_id, REHYDRATING)

            rehydrated_entity := EXECUTE_REHYDRATE_OPERATION(manifest_entry)

            if rehydrated_entity IS NOT NULL:
                manifest_entry.resolution_state := REHYDRATED
                SEND_STATUS_FRAME(RESOLVE, parent_id, REHYDRATED)

                // Notify parent's parent (if any)
                grandparent_id := FIND_GRANDPARENT(parent_id)
                if grandparent_id != 0:
                    ON_ENTITY_COMPLETION(parent_id, grandparent_id, COMPLETE)

                // Clean up manifest entry after grace period
                SCHEDULE_CLEANUP(parent_id, CLEANUP_GRACE_PERIOD)
            else:
                manifest_entry.resolution_state := FAILED
                SEND_STATUS_FRAME(RESOLVE, parent_id, FAILED)
                HANDLE_REHYDRATE_FAILURE(manifest_entry)

procedure EXECUTE_REHYDRATE_OPERATION(manifest_entry):
    // Gather child results
    child_results := []

    for i := 0 to manifest_entry.child_count - 1:
        child_id := manifest_entry.children_ids[i]
        status := manifest_entry.completion_status[i]

        if status = COMPLETE:
            result := FETCH_ENTITY_RESULT(child_id)
            child_results.append(result)
        else if manifest_entry.flags.PARTIAL_FAILURE_ALLOWED:
            child_results.append(NULL)  // Placeholder for failed child
        else:
            return NULL  // Cannot rehydrate with failures

    // Reconstruct parent entity
    if manifest_entry.flags.ORDERED_REHYDRATE:
        // Children must be combined in emission order
        rehydrated := ORDERED_COMBINE(child_results)
    else:
        // Children can be combined in any order
        rehydrated := UNORDERED_COMBINE(child_results)

    return rehydrated
```

## 8.6 Security Considerations

Implementations MUST consider the following security aspects:

1. Assembly Manifest entries SHOULD be protected from unauthorized modification. Access control mechanisms are implementation-defined.

2. Parent ID spoofing could allow an attacker to inject results into unrelated reassembly operations. Implementations SHOULD validate that completion notifications originate from authorized processors.

3. Resource exhaustion attacks via excessive dehydration depth or breadth MUST be mitigated through configurable limits.

4. Checkpoint timeout manipulation could be used for denial-of-service. Implementations SHOULD enforce minimum timeout values.

## 8.7 IANA Considerations

This section defines frame types that should be registered with IANA:

| Frame Type | Value | Specification |
|------------|-------|---------------|
| STATUS | 0x50 | Section 8.1.4 |
| CHECKPOINT | 0x51 | Section 8.2.1 |
| CHECKPOINT_FAILED | 0x52 | Section 8.2.6 |
| DAG_VIOLATION | 0x53 | Section 8.4.5 |
