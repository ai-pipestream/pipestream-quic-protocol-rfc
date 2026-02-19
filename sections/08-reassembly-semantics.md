# Section 8: Reassembly Semantics

## 8.1 Parts Ledger

The Parts Ledger is a distributed data structure that maintains the hierarchical relationships between vaporized entities and their constituent parts. Each processing node MUST maintain a local Parts Ledger for entities within its processing scope.

### 8.1.1 Ledger Entry Structure

Each Parts Ledger entry SHALL contain the following fields (as defined in `PartsLedgerEntry` protobuf):

```
Parts Ledger Entry {
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
: The identifier of the parent entity that was vaporized.

Scope ID (32 bits):
: The identifier of the scope in which the vaporization occurred.

Child Count (16 bits):
: The number of child entities produced by vaporization.

Children IDs (variable):
: An array of 20-bit entity identifiers, one for each child.

Children Status (variable):
: An array of 4-bit status codes (EntityStatus), one for each child.

Completion Policy (Layer 2):
: The policy governing failure handling and success criteria for this decomposition.

Creation Timestamp (64 bits):
: Microseconds since the UNIX epoch when this ledger entry was created.

Resolution State (8 bits):
: The current state of the ledger entry (ResolutionState).

### 8.1.2 Completion Status Codes

Each child entity SHALL have one of the following completion status values:

| Value | Name | Layer | Description |
|-------|------|-------|-------------|
| 0x0 | PENDING | 0 | Entity announced, not yet transmitting |
| 0x1 | PROCESSING | 0 | Entity transmission in progress |
| 0x2 | COMPLETE | 0 | Entity successfully processed |
| 0x3 | FAILED | 0 | Entity processing failed |
| 0x4 | CHECKPOINT | 0 | Synchronization barrier |
| 0x5 | VAPORIZING | 0 | Decomposing into children |
| 0x6 | AGGREGATING | 0 | Rejoining children |
| 0x7 | YIELDED | 2 | Paused with continuation token |
| 0x8 | DEFERRED | 2 | Detached with claim check |
| 0x9 | RETRYING | 2 | Retry in progress |
| 0xA | SKIPPED | 2 | Intentionally skipped (lenient mode) |
| 0xB | ABANDONED | 2 | Timed out, cursor advanced past |

### 8.1.3 Resolution States

| Value | Name | Description |
|-------|------|-------------|
| 0x0 | ACTIVE | Entry is active, awaiting child completion |
| 0x1 | RESOLVED | All children reached terminal state |
| 0x2 | PARTIAL | Some children failed/skipped (policy met) |
| 0x3 | FAILED | Entry resolution failed |

### 8.1.4 Ledger Frame Format

Parts Ledger updates are transmitted using extended 3-byte frames with the following structure:

```
Ledger Frame {
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
| 0x01 | CREATE | Create new ledger entry |
| 0x02 | UPDATE_STATUS | Update child completion status |
| 0x03 | RESOLVE | Mark entry as resolved |
| 0x04 | DELETE | Remove ledger entry |
| 0x05 | QUERY | Request ledger entry state |
| 0x06 | SYNC | Synchronize ledger state |

### 8.1.5 Atomicity Requirements

Implementations MUST satisfy the following atomicity requirements:

1. A Parts Ledger entry MUST be created and acknowledged before any child entity is emitted. Failure to observe this requirement MAY result in orphaned children.

2. The creation of a ledger entry and the emission of the first child entity SHOULD be performed as an atomic operation where the underlying transport supports such semantics.

3. If atomicity cannot be guaranteed, implementations MUST use the following two-phase protocol:

```
Phase 1: Create ledger entry with PENDING state
Phase 2: Await CREATE acknowledgment
Phase 3: Emit child entities
Phase 4: Update ledger entry to ACTIVE state
```

4. If a failure occurs between Phase 1 and Phase 3, the ledger entry MUST be garbage collected after the timeout specified in Section 8.2.5.

5. Multiple concurrent updates to the same ledger entry MUST be serialized. Implementations MAY use optimistic concurrency control with version vectors or pessimistic locking.

### 8.1.6 Resolution Conditions

A Parts Ledger entry SHALL be considered resolved when one of the following conditions is met:

1. ALL children have Completion Status of COMPLETE (successful resolution)

2. ALL children have a terminal Completion Status (COMPLETE, FAILED, TIMEOUT, CANCELLED, or ORPHANED) AND the PARTIAL_FAILURE_ALLOWED flag is set (partial resolution)

3. ANY child has a non-COMPLETE terminal status AND the PARTIAL_FAILURE_ALLOWED flag is NOT set (failed resolution)

4. The entry timeout has been exceeded (timeout resolution)

Upon resolution, implementations MUST:

1. Update the Resolution State to RESOLVED
2. Enqueue the entry for rejoin processing (Section 8.3)
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

3. All Parts Ledger entries within the checkpoint's scope have been resolved.

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
   - Resolve all affected Parts Ledger entries
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

        for each ledger_entry in GET_LEDGER_ENTRIES(checkpoint.scope_id):
            if ledger_entry.resolution_state = ACTIVE:
                FORCE_RESOLVE_LEDGER_ENTRY(ledger_entry, TIMEOUT)

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
   - Clean up Parts Ledger entries for failed scope
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

Due to the distributed nature of PipeStream processing, child entities MAY complete out of order. This section specifies the mechanism for efficiently tracking completion status and triggering rejoins when all children of a vaporized entity have completed.

### 8.3.1 Out-of-Order Entity Arrival Handling

Implementations MUST handle out-of-order completion notifications:

1. Each completion notification MUST be idempotent; duplicate notifications for the same entity MUST be safely ignored.

2. Completion notifications MUST include sufficient information to locate the relevant Parts Ledger entry (parent_id or ledger entry reference).

3. Implementations MUST NOT assume any ordering of completion notifications, even for children emitted in sequence.

4. Completion notifications received before the corresponding ledger entry exists MUST be buffered for a grace period (minimum 30 seconds) before being discarded as orphans.

### 8.3.2 Priority Queue Structure

Implementations SHALL use a priority queue to efficiently track which Parts Ledger entries are ready for rejoin. This specification RECOMMENDS a Fibonacci heap due to its O(1) amortized decrease-key operation.

Priority Queue Properties:

- Key: Number of completed children (completion_count)
- Value: Reference to Parts Ledger entry
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
        ledger_entry_ref: Reference to Parts Ledger Entry,
        completion_count: Integer,
        target_count: Integer (equal to ledger_entry.child_count),
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

The priority SHALL be calculated such that entries closer to completion have LOWER priority values (min-heap behavior triggers rejoin on extract-min):

```
priority = target_count - completion_count
```

When priority reaches 0, the entry is ready for rejoin and will be at the top of the heap.

### 8.3.4 Bubble-Up on Completion

When a child entity completes, the following procedure updates the heap:

```
procedure ON_CHILD_COMPLETE(parent_id, child_id, status):
    ledger_entry := parts_ledger[parent_id]
    if ledger_entry IS NULL:
        BUFFER_ORPHAN_COMPLETION(parent_id, child_id, status)
        return

    child_index := FIND_CHILD_INDEX(ledger_entry, child_id)
    if child_index = -1:
        ERROR("Unknown child entity")
        return

    if ledger_entry.completion_status[child_index] != PENDING:
        return  // Already completed, idempotent handling

    ledger_entry.completion_status[child_index] := status

    heap_node := heap_node_index[parent_id]
    heap_node.completion_count := heap_node.completion_count + 1
    new_priority := heap_node.target_count - heap_node.completion_count

    DECREASE_KEY(rejoin_heap, heap_node, new_priority)

    if new_priority = 0:
        // Entry is now ready for rejoin - will be at heap root
        SIGNAL_REJOIN_READY()

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

### 8.3.5 Rejoin Triggering on Extract-Min

The rejoin processor continuously monitors the heap and triggers rejoins:

```
procedure REJOIN_PROCESSOR():
    loop:
        WAIT_FOR(rejoin_heap.min.priority = 0 OR shutdown_signal)

        if shutdown_signal:
            break

        while rejoin_heap IS NOT EMPTY AND rejoin_heap.min.priority = 0:
            node := EXTRACT_MIN(rejoin_heap)
            ledger_entry := node.ledger_entry_ref

            if VALIDATE_REJOIN_PRECONDITIONS(ledger_entry):
                EXECUTE_REJOIN(ledger_entry)
            else:
                HANDLE_REJOIN_FAILURE(ledger_entry)

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

procedure VALIDATE_REJOIN_PRECONDITIONS(ledger_entry):
    // All children must have terminal status
    for each status in ledger_entry.completion_status:
        if status = PENDING:
            return FALSE

    // Check PARTIAL_FAILURE_ALLOWED flag
    if NOT ledger_entry.flags.PARTIAL_FAILURE_ALLOWED:
        for each status in ledger_entry.completion_status:
            if status != COMPLETE:
                return FALSE

    // Verify checkpoint scope allows rejoin
    if NOT CHECKPOINT_SCOPE_ALLOWS_REJOIN(ledger_entry.checkpoint_scope):
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
                        candidate.ledger_entry_ref.parent_id)
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
        entry -> entry.ledger_entry_ref.creation_timestamp)
```

5. Memory Accounting: Each heap node requires approximately:
   - Fixed overhead: 96 bytes (pointers, counters, flags)
   - Ledger reference: 8 bytes
   - Total per entry: ~104 bytes minimum

   Implementations SHOULD reserve additional memory for heap restructuring operations.

## 8.4 Parent Reference Resolution

Parent references establish the hierarchical structure of vaporized entities and enable proper reassembly.

### 8.4.1 Parent ID Field Semantics

The parent_id field in entity metadata SHALL be interpreted as follows:

1. The parent_id references the entity that was vaporized to produce this entity.

2. The parent_id MUST correspond to a valid Parts Ledger entry, except for root entities.

3. The parent_id is immutable once set; it MUST NOT be modified during entity lifecycle.

4. The parent_id namespace is global within a PipeStream session; implementations MUST ensure uniqueness across all participating nodes.

### 8.4.2 Root Entity Identification

Root entities are top-level entities that were not produced by vaporization. Root entities SHALL be identified by one of the following:

1. Null Parent: parent_id = 0x0000000000000000

2. Self-Referential: parent_id = entity_id (the entity references itself)

Implementations MUST treat both representations as equivalent for root entity detection:

```
procedure IS_ROOT_ENTITY(entity):
    return entity.parent_id = 0 OR entity.parent_id = entity.id
```

Root entities:
- Do NOT require a Parts Ledger entry (they have no parent to track them)
- MAY be vaporized to produce children (becoming internal nodes)
- Represent the entry points of document processing pipelines

### 8.4.3 Recursive Vaporization Chains

Entities MAY be vaporized recursively, creating chains of parent-child relationships:

```
Document (Root)
    |
    +--vaporize--> Section 1
    |                  |
    |                  +--vaporize--> Paragraph 1.1
    |                  |                  |
    |                  |                  +--vaporize--> Sentence 1.1.1
    |                  |                  +--vaporize--> Sentence 1.1.2
    |                  |
    |                  +--vaporize--> Paragraph 1.2
    |
    +--vaporize--> Section 2
```

Chain Properties:

1. Each non-root entity has exactly one parent.

2. Chains MAY be arbitrarily deep, limited only by implementation resources.

3. Rejoining MUST proceed bottom-up: leaf entities rejoin first, then their parents, recursively up to the root.

4. A parent entity MUST NOT rejoin until ALL of its children have rejoined.

Resolution Order:

```
procedure DETERMINE_REJOIN_ORDER(root_id):
    order := []
    visited := {}

    POST_ORDER_TRAVERSE(root_id, order, visited)

    return order

procedure POST_ORDER_TRAVERSE(entity_id, order, visited):
    if entity_id IN visited:
        return

    visited.add(entity_id)

    ledger_entry := parts_ledger[entity_id]
    if ledger_entry IS NOT NULL:
        for each child_id in ledger_entry.children_ids:
            POST_ORDER_TRAVERSE(child_id, order, visited)

    order.append(entity_id)
```

### 8.4.4 Orphan Detection and Handling

Orphaned entities are children whose parent cannot be located or has been terminated. Implementations MUST detect and handle orphans:

Detection Conditions:

1. A completion notification references a parent_id with no corresponding Parts Ledger entry, AND the grace period (30 seconds) has elapsed.

2. A parent entity is explicitly terminated (CANCELLED, FAILED) before all children complete.

3. A Parts Ledger entry is evicted due to memory pressure while children are still processing.

Handling Procedure:

```
procedure DETECT_ORPHANS():
    for each buffered_completion in orphan_buffer:
        if CURRENT_TIME() - buffered_completion.timestamp > GRACE_PERIOD:
            HANDLE_ORPHAN(buffered_completion)

procedure HANDLE_ORPHAN(completion):
    orphan_id := completion.child_id
    claimed_parent := completion.parent_id

    // Attempt to locate parent in distributed ledger
    remote_entry := QUERY_REMOTE_LEDGERS(claimed_parent)

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

The parent-child relationship graph MUST form a Directed Acyclic Graph (DAG). Cycles would cause infinite rejoin loops and MUST be prevented.

Prevention Mechanisms:

1. Monotonic ID Assignment: If entity IDs are assigned monotonically, children MUST have IDs greater than their parent. This trivially prevents cycles.

2. Depth Tracking: Each entity carries a depth counter; children have depth = parent.depth + 1. Implementations MUST reject vaporization that would create children with depth exceeding a maximum (default: 1024).

3. Ancestry Verification: Before creating a Parts Ledger entry, verify the parent is not a descendant of any proposed child.

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
        ledger_entry := FIND_LEDGER_ENTRY_FOR_CHILD(current)

        if ledger_entry IS NULL:
            break

        current := ledger_entry.parent_id

    return ancestors

procedure VAPORIZE_WITH_DAG_CHECK(parent_id, children):
    if NOT VERIFY_DAG_PROPERTY(parent_id, children):
        ABORT_VAPORIZATION("DAG violation")
        return

    // Proceed with vaporization
    CREATE_LEDGER_ENTRY(parent_id, children)
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
parts_ledger := HashMap<ParentID, LedgerEntry>
rejoin_heap := FibonacciHeap<HeapNode>
heap_node_index := HashMap<ParentID, HeapNode>
checkpoint_registry := HashMap<CheckpointID, Checkpoint>
orphan_buffer := List<BufferedCompletion>

procedure INITIALIZE_REASSEMBLY_SUBSYSTEM():
    START_REJOIN_PROCESSOR_THREAD()
    START_ORPHAN_DETECTOR_THREAD()
    START_MEMORY_MONITOR_THREAD()

procedure ON_VAPORIZE(parent_entity, child_entities):
    // Validate DAG property
    child_ids := [child.id for child in child_entities]
    if NOT VERIFY_DAG_PROPERTY(parent_entity.id, child_ids):
        ABORT_VAPORIZATION("DAG violation")
        return ERROR

    // Create ledger entry atomically before emitting children
    ledger_entry := LedgerEntry {
        parent_id: parent_entity.id,
        child_count: LENGTH(child_entities),
        children_ids: child_ids,
        completion_status: [PENDING] * LENGTH(child_entities),
        checkpoint_scope: CURRENT_CHECKPOINT_SCOPE(),
        creation_timestamp: CURRENT_TIME_MICROS(),
        resolution_state: ACTIVE,
        flags: parent_entity.vaporization_flags,
    }

    // Persist ledger entry
    SEND_LEDGER_FRAME(CREATE, ledger_entry)
    AWAIT_LEDGER_ACK()

    parts_ledger[parent_entity.id] := ledger_entry

    // Create heap node for tracking
    heap_node := HeapNode {
        ledger_entry_ref: ledger_entry,
        completion_count: 0,
        target_count: ledger_entry.child_count,
        priority: ledger_entry.child_count,  // Far from completion
    }

    INSERT(rejoin_heap, heap_node)
    heap_node_index[parent_entity.id] := heap_node

    // Now safe to emit children
    for each child in child_entities:
        EMIT_ENTITY(child)

    return SUCCESS

procedure ON_ENTITY_COMPLETION(entity_id, parent_id, status):
    // Check for orphan condition
    if parent_id NOT IN parts_ledger:
        BUFFER_ORPHAN_COMPLETION(parent_id, entity_id, status)
        return

    ledger_entry := parts_ledger[parent_id]

    // Find and update child status (idempotent)
    child_index := FIND_INDEX(ledger_entry.children_ids, entity_id)
    if child_index = -1:
        LOG_ERROR("Unknown child", entity_id, parent_id)
        return

    if ledger_entry.completion_status[child_index] != PENDING:
        return  // Already completed

    ledger_entry.completion_status[child_index] := status
    SEND_LEDGER_FRAME(UPDATE_STATUS, parent_id, child_index, status)

    // Update heap
    heap_node := heap_node_index[parent_id]
    heap_node.completion_count := heap_node.completion_count + 1
    new_priority := heap_node.target_count - heap_node.completion_count
    DECREASE_KEY(rejoin_heap, heap_node, new_priority)

    // Check checkpoint notifications
    NOTIFY_CHECKPOINT_PROGRESS(entity_id)

procedure REJOIN_PROCESSOR():
    loop:
        // Wait for entry ready for rejoin
        WAIT_FOR(rejoin_heap.min.priority = 0 OR shutdown)

        if shutdown:
            break

        // Process all ready entries
        while rejoin_heap.min IS NOT NULL AND rejoin_heap.min.priority = 0:
            node := EXTRACT_MIN(rejoin_heap)
            ledger_entry := node.ledger_entry_ref
            parent_id := ledger_entry.parent_id

            // Remove from index
            DELETE heap_node_index[parent_id]

            // Validate preconditions
            if NOT VALIDATE_REJOIN_PRECONDITIONS(ledger_entry):
                HANDLE_REJOIN_FAILURE(ledger_entry)
                continue

            // Check for recursive dependencies
            for each child_id in ledger_entry.children_ids:
                if child_id IN parts_ledger:
                    // Child is also a parent, must rejoin first
                    child_ledger := parts_ledger[child_id]
                    if child_ledger.resolution_state != REJOINED:
                        // Re-queue parent, child not ready
                        REQUEUE_AFTER_CHILD(node, child_id)
                        continue

            // Execute rejoin
            ledger_entry.resolution_state := REJOINING
            SEND_LEDGER_FRAME(RESOLVE, parent_id, REJOINING)

            rejoined_entity := EXECUTE_REJOIN_OPERATION(ledger_entry)

            if rejoined_entity IS NOT NULL:
                ledger_entry.resolution_state := REJOINED
                SEND_LEDGER_FRAME(RESOLVE, parent_id, REJOINED)

                // Notify parent's parent (if any)
                grandparent_id := FIND_GRANDPARENT(parent_id)
                if grandparent_id != 0:
                    ON_ENTITY_COMPLETION(parent_id, grandparent_id, COMPLETE)

                // Clean up ledger entry after grace period
                SCHEDULE_CLEANUP(parent_id, CLEANUP_GRACE_PERIOD)
            else:
                ledger_entry.resolution_state := FAILED
                SEND_LEDGER_FRAME(RESOLVE, parent_id, FAILED)
                HANDLE_REJOIN_FAILURE(ledger_entry)

procedure EXECUTE_REJOIN_OPERATION(ledger_entry):
    // Gather child results
    child_results := []

    for i := 0 to ledger_entry.child_count - 1:
        child_id := ledger_entry.children_ids[i]
        status := ledger_entry.completion_status[i]

        if status = COMPLETE:
            result := FETCH_ENTITY_RESULT(child_id)
            child_results.append(result)
        else if ledger_entry.flags.PARTIAL_FAILURE_ALLOWED:
            child_results.append(NULL)  // Placeholder for failed child
        else:
            return NULL  // Cannot rejoin with failures

    // Reconstruct parent entity
    if ledger_entry.flags.ORDERED_REJOIN:
        // Children must be combined in emission order
        rejoined := ORDERED_COMBINE(child_results)
    else:
        // Children can be combined in any order
        rejoined := UNORDERED_COMBINE(child_results)

    return rejoined
```

## 8.6 Security Considerations

Implementations MUST consider the following security aspects:

1. Parts Ledger entries SHOULD be protected from unauthorized modification. Access control mechanisms are implementation-defined.

2. Parent ID spoofing could allow an attacker to inject results into unrelated reassembly operations. Implementations SHOULD validate that completion notifications originate from authorized processors.

3. Resource exhaustion attacks via excessive vaporization depth or breadth MUST be mitigated through configurable limits.

4. Checkpoint timeout manipulation could be used for denial-of-service. Implementations SHOULD enforce minimum timeout values.

## 8.7 IANA Considerations

This section defines frame types that should be registered with IANA:

| Frame Type | Value | Specification |
|------------|-------|---------------|
| LEDGER | 0x50 | Section 8.1.4 |
| CHECKPOINT | 0x51 | Section 8.2.1 |
| CHECKPOINT_FAILED | 0x52 | Section 8.2.6 |
| DAG_VIOLATION | 0x53 | Section 8.4.5 |
