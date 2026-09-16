# PipeStream Reference Implementation Guide

This document provides implementation guidance and recommended data structures for PipeStream protocol implementations. The content in this document is INFORMATIVE and not part of the normative protocol specification.

## Current Reference Suite (2026-09-16)

Three independent implementations live under [`implementations/`](implementations/):
Java/Netty, Rust/Quinn and C++/MsQuic. Each builds a reusable library plus a
standalone client and server. Their codecs and protocol state machines are
separate implementations that share no protocol code.

Java and Rust are the two reference implementations of the durable-work
profile (Section 12 and Appendix F). Each is a complete authority (listener,
durable stores, execution, retention and result delivery), a durable client
with an exclusive journal of intents, receipts and observations, and a
command-line launcher; the two launchers take the same arguments so one
driver runs either. Both authenticate callers with mutual TLS and map
certificate fingerprints to owner principals; the caller-authentication gap
noted at draft-04 is closed. C++/MsQuic remains a Layer 0 codec and endpoint
and is not a durable-work implementation.

Evidence, all checked in:

- **Neutral acceptance matrix.** The `durable` command of
  [`pipestream-conformance`](implementations/rust-quinn/conformance/) is a
  process driver with no PipeStream dependency. It runs every client against
  every server as separate processes and injects faults (dropped replies,
  kills at recorded boundaries, disconnects) through a fixture schedule both
  subjects honour. The final run on the subjects `main` ships is archive
  [`durable-18d5e94dc4306b7a`](conformance/results/async-neutral-v2/runs/durable-18d5e94dc4306b7a/):
  66 rows in both client/server directions, 64 PASS and 2 WAIVED with named
  reasons (no fixture clock on either subject; no cleanup boundary in the
  fixture interface), no FAIL. The driver's handoff is
  [`conformance/results/async-neutral-v2/handoff.md`](conformance/results/async-neutral-v2/handoff.md).
- **Java.** Handoff
  [`conformance/results/async-java-v2/handoff.md`](conformance/results/async-java-v2/handoff.md);
  clause-level traceability of Section 12 in
  [`docs/standards/section-12-java-traceability.md`](docs/standards/section-12-java-traceability.md)
  (every clause mapped to code and a test, with the partial and
  not-applicable rows named with reasons). The gate is the full test suite
  plus the source-built transport extension's own tests; see the Java
  [README](implementations/java-netty/README.md).
- **Rust.** `cargo test --locked --workspace` over the core, transport,
  server and driver crates; see the Rust
  [README](implementations/rust-quinn/README.md).
- **Applications on the profile.**
  [`benchmarks/durable-transform`](benchmarks/durable-transform/) measures a
  transform workload on PipeStream against a gRPC arm on loopback, with fault
  and stopped-consumer suites, and
  [`examples/durable-index-build`](examples/durable-index-build/) builds an
  inverted index across two authorities using descendant scopes, parent reads
  of child outputs, cross-authority result references, scope cancellation and
  kill/resume; its gate runs from a fresh clone. The Layer 0 examples
  (`java-to-rust`, `rust-to-cpp-recovery`, `three-node-scatter`) remain.
- **Specification changes that came out of building.**
  [`docs/standards/durable-work-v2-decisions.md`](docs/standards/durable-work-v2-decisions.md)
  records each decision the implementations forced, most recently the wire
  code for over-limit control bodies, a client's local checkpoint refusal, the
  removal of the inputless CANCELLING state, the answer to a foreign authority
  on attach, and the per-principal connection ceiling refusal.
- **Frozen bytes.** [`test-vectors/`](test-vectors/) supplies valid and
  invalid Layer 0 bytes; `pipestream-conformance verify` and `modelcheck` run
  them from [`conformance/run_all.sh`](conformance/run_all.sh), which also
  runs the acceptance matrix when `PIPESTREAM_DURABLE_ACCEPTANCE=1`.

Known limits: the two waived matrix rows above; the C++ implementation stops
at Layer 0; [draft-04 readiness](docs/standards/draft04-readiness.md) is the
historical record of the earlier state. The algorithms below are informative
and are not implied by a successful interoperability run.

## 1. Rehydration Readiness Tracking (Fibonacci Heap)

### 1.1. Overview

Child entities can complete out of order. Section 9.6 requires tracking
rehydration readiness but leaves the algorithm and representation to the
implementation; it does not mandate the complexity bounds below. Under the
sealed-work profile, a zero count of outstanding received children is not
sufficient: membership must also be sealed, every declared child admitted and
resolved, and descendant scopes closed. STRICT additionally requires every
child to succeed.

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
