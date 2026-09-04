# PipeStream Reference Implementation Guide

This document provides implementation guidance and recommended data structures for PipeStream protocol implementations. The content in this document is INFORMATIVE and not part of the normative protocol specification.

## Current Layer 0 Suite

Executable Layer 0 implementations now live under [`implementations/`](implementations/): Java/Netty, Rust/Quinn, and C++/MsQuic. Each directory builds a reusable library plus a standalone client/server. Their codecs and protocol state machines are separate implementations.

The checked-in [`test-vectors/`](test-vectors/) corpus supplies golden valid and invalid bytes, while [`conformance/run_interop.py`](conformance/run_interop.py) runs every client against every server as separate processes. The language-native applications in [`examples/`](examples/) exercise cross-language transfer, application-profile recovery, and three-node scatter/reassembly. The Python scenario runner is kept under `conformance/` and contains no application or protocol behavior. These implementations currently cover the documented Layer 0 subset; the algorithms below remain guidance for the recursive layers and are not implied by a passing Layer 0 run.

## 1. Rehydration Readiness Tracking (Fibonacci Heap)

### 1.1. Overview

Due to the distributed nature of PipeStream processing, child entities MAY complete out of order. The protocol requires implementations to efficiently track Assembly Manifest resolution order with O(1) insertion and amortized O(log n) minimum extraction (see Section 9.5 of the protocol specification).

A **Fibonacci heap** is the recommended data structure for this purpose, due to its O(1) amortized decrease-key operation, which maps naturally to the "child completed" event that moves a manifest entry closer to rehydration readiness.

### 1.2. Priority Queue Properties

- **Key**: Number of remaining incomplete children (`target_count - completion_count`)
- **Value**: Reference to Assembly Manifest entry
- **Ordering**: Min-heap — entries with key = 0 (all children complete) have highest priority

### 1.3. Complexity Guarantees

| Operation | Amortized Complexity |
|-----------|---------------------|
| Insert | O(1) |
| Find-min | O(1) |
| Extract-min | O(log n) |
| Decrease-key | O(1) |
| Merge | O(1) |

For PipeStream, the "decrease-key" operation is repurposed as an "increase-completion-count" operation, which maintains heap ordering by moving entries toward the root as they approach full completion.

### 1.4. Node Structure

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

When priority reaches 0, the entry is ready for rehydration and will be at the top of the heap.

### 1.5. Bubble-Up on Completion

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

### 1.6. Rehydration Triggering on Extract-Min

The rehydration processor continuously monitors the heap and triggers rehydrations:

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

    // Check completion policy
    if manifest_entry.policy.mode = COMPLETION_MODE_STRICT:
        for each status in manifest_entry.completion_status:
            if status != COMPLETE:
                return FALSE

    // Verify checkpoint scope allows rehydration
    if NOT CHECKPOINT_SCOPE_ALLOWS_REHYDRATE(manifest_entry.checkpoint_scope):
        return FALSE

    return TRUE
```

### 1.7. Memory Bounds

To prevent unbounded memory growth, implementations SHOULD enforce limits on pending entries:

1. **Maximum Pending Entries**: Implementations SHOULD support a configurable maximum number of pending heap entries. The recommended default is 1,000,000 entries.

2. **Maximum Entry Age**: Entries that have been pending for longer than the maximum age SHOULD be eligible for eviction. The recommended default maximum age is 3,600 seconds (1 hour).

## 2. Out-of-Order Entity Arrival Handling

Implementations MUST handle out-of-order completion notifications:

1. Each completion notification MUST be idempotent; duplicate notifications for the same entity MUST be safely ignored.

2. Completion notifications MUST include sufficient information to locate the relevant Assembly Manifest entry (parent_id or manifest entry reference).

3. Implementations MUST NOT assume any ordering of completion notifications, even for children emitted in sequence.

4. Completion notifications received before the corresponding manifest entry exists MUST be buffered for a grace period (minimum 30 seconds) before being discarded as orphans.
