# Restart-Safety Patterns (Informative)

This appendix gives application-design examples for Section 12.6. It adds no
wire fields, execution mode, conformance label or exactly-once external-effect
guarantee. A successful replay test establishes only the tested application,
effect store, fault model and identity rules. Multiple physical callback
invocations remain possible.

## Pure Recalculation and Immutable Publication

A callback can read immutable admitted input, compute without external side
effects, and place output only through the authority's staged result interface.
An interrupted invocation may repeat the calculation. Only a current attempt
with the required worker and ancestor fences can publish its manifest. An old
invocation's private output is not a committed result.

Test termination before publication, after publication but before its receipt
is observed, and during output retrieval. Verify the retained manifest and
output bytes, count physical invocations separately from committed outcomes,
and confirm that repeated result reads never call the application. Bit-identical
recalculation additionally requires deterministic inputs, dependencies and
transform behavior; it does not follow from restart safety alone.

## Transactional Effect Deduplication

An external effect store can retain an application effect key, the complete
effect parameters or their verified commitment, and a saved outcome. In one
transaction, it checks the key and either applies the effect and records the
outcome, returns the existing matching outcome, or refuses conflicting content.
For a multi-effect callback, each independently committed effect needs an
explicit identity and replay rule.

The effect key covers the authenticated resource and logical business action.
It stays stable across every physical invocation intended to represent that
same effect, including an authorized new work attempt when that attempt is
still a retry of the same business action. A connection request number, worker
lease or attempt number alone is therefore not a general-purpose deduplication
key. Choosing a new business action is an explicit application decision.

The deduplication record and effect share the same atomic commit. Writing the
effect first and a marker later leaves a duplicate-effect window. Retain the
record for the entire supported replay horizon; deleting it early can permit
the effect to execute again. A transactional outbox solves a local handoff,
but does not by itself deduplicate a downstream system that cannot enforce the
effect key.

Test process death immediately before and after the effect transaction, loss
of its reply, concurrent matching invocations and conflicting payloads under
one key. Count effects at the actual sink and verify saved outcomes after
restart. A mock callback count is not external-effect evidence.

## Sink-Enforced Fencing

For a replaceable writer, an effect sink can maintain a monotonic fencing token
per resource and reject stale writers. The sink checks the token atomically
with each protected effect; checking it only in a coordinator before a later
write leaves a race. Token allocation and its meaning are part of the
application/sink contract. A PipeStream attempt or local worker lease is not
automatically an externally enforced fence.

Fence validation excludes superseded writers, but does not deduplicate two
invocations with the same valid token. Combine it with effect deduplication
when both stale-writer exclusion and duplicate suppression are required.
Lease-renewal loops help a worker retain authority; renewal alone does not
prevent a paused worker's external write after another worker takes over.

Test a worker paused before an external effect, advance the sink's fence with
a replacement worker, and resume the old worker. The sink must reject the stale
effect under the chosen application contract. Test duplicate effects within
one valid generation separately, and preserve evidence across sink restart.

## Documenting a Restart-Safety Claim

An application claim should name the effect boundary, identity namespace,
deduplication retention, token issuer and validator where used, and the actual
crash/replay cases tested. State whether the evidence covers pure computation,
authority publication, a particular external sink, or several independently
committed effects. An API enum such as `RestartSafety` is an application
declaration, not an independent certification. These examples do not create
protocol-wide labels such as "exactly once" or "fenced".
