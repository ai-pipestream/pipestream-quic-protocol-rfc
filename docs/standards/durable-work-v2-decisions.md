# Durable work version 2: lifecycle decisions

This is the design input to the successor specification, not a wire profile
that any endpoint may advertise yet. The existing version-1 specification and
frozen vectors retain their meanings. The complete acceptance scope is in
[the execution record](durable-work-results-goal.md).

## Version and layering

Use a new major ALPN mapping for the reduced Core. A required capability is a
promise of its entire mandatory behavior. Core handles negotiation, bounded
framing, transport/error rules and connection draining; it does not mandate
recursive scheduling, provider-specific storage metadata, or legacy data-layer
values. Durable work adds authenticated identity, declarations, seals, admission,
attempts, cancellation, retained lookup and scoped closure. The result profile
adds output commitments, streams and authenticated references. It depends on
durable work. The final wire contract must allocate distinct versioned messages
and profile identifiers and specify downgrade refusal.

## Ownership and identity

Logical identity contains authority, authenticated owner, session generation,
producer namespace, scope and entity. The authority is verified deployment
identity, not selected by untrusted request metadata. Client and server producers
have disjoint allocation namespaces; neither a numeric entity ID nor a producer
label authenticates its issuer. A session has an explicit authenticated owner
and authorized producer bindings. Reconnection verifies those retained bindings.

Attempts have a monotonically increasing generation within logical work. A new
attempt is not a new scope member. Neither logical identifiers nor attempt
generations are recycled. Compact anti-reuse history may outlive payload and
receipt retention; quota exhaustion refuses new work rather than forgetting
identity. The wire contract must define session creation/replay and exhaustion
without relying on a global random-number uniqueness assertion.

## Declaration, admission, execution and results

Declaration durably reserves membership, not input or processing capacity.
Sealing fixes that membership. A declared input may still be missing or invalid.
Admission commits validated immutable input, a restartable job, resource
reservations, and its first attempt together before sending its receipt.

A duplicate admission with identical identity and commitment returns the
existing admission. Changed input is a conflict, never a silent update. A lookup
cannot create work. The caller retains request identity before transmission and
recovers through correlated lookup after an ambiguous disconnect.

An explicitly authorized retry names the expected current attempt and an
immutable operation identity. Acceptance atomically supersedes that attempt,
advances its publication fence, and records the replacement attempt and replay
receipt. Replaying the retry operation cannot advance the fence again. Retrying
terminal logical work is refused; an application wanting new work uses a new
identity. A server restart does not silently create a new attempt. A new retry
must be authorized before the original execution deadline; it does not extend
that deadline. A repeated accepted retry remains a lookup, not a new grant.

Output bytes are validated and durably installed before successful publication.
Publication atomically associates the output manifest with the exact admitted
input and current attempt and commits the logical terminal outcome. An old
worker may finish computing but cannot publish after retry, cancellation,
deadline settlement or revocation. Staged output alone is not a result.

A result stream transfers an already-published object; resetting it does not
change work state or authorize computation. Reference delivery has the same
identity, integrity, authorization and retention requirements. A manifest hash
does not prove correct computation or make a reference a bearer credential.
The result wire contract must cover object identity, byte ranges, integrity,
backpressure, replay and unavailable/expired refusals for both delivery modes.

## Cancellation, outcomes and closure

Cancellation is an authenticated operation serialized with publication. If
publication wins, cancellation returns the existing terminal outcome. If
cancellation wins, publication is fenced and the authoritative outcome records
cancellation. Cancellation can settle an unadmitted declared entity; transport
reset and invalid input cannot do so. A declaration remains in the sealed count.

Accepted cancellation does not claim to undo an external side effect. Attempt
fences protect protocol publication; application effects require transactional
integration or independent idempotency/fencing. A digest is not exactly-once
execution evidence.

The final-state summary partitions every declared member into success, failure,
cancellation or skip. Pending/running/deferred are not final count buckets.
Empty sealed scopes are valid and close with four zero counts. Completion
requires an immutable seal and all members settled, including each
member's descendant obligations. STRICT additionally requires all members to
succeed. A parent is not ready merely because the children received so far
finished. Parent cancellation must define descendant settlement explicitly;
it cannot drop a subtree. Authoritative subtree cancellation freezes descendant
membership and settles every existing nonterminal descendant as cancelled,
without changing successful descendants. A failed parent still owes descendant
settlement; failure is not implicit subtree cancellation. GOAWAY for connection
shutdown names the root scope and its acknowledged immutable cut, not an
unqualified highest entity ID or an unrelated child-scope checkpoint.

## Lifetimes and authority

Execution deadline, input retention, output availability, receipt replay,
authorization validity and anti-reuse history are independent. Active accepted
work remains recorded until an authoritative terminal settlement, even if its
admission is more than 24 hours old. Deadline expiry triggers a fenced settlement;
it is not silent eviction. Lookup remains authorized by current policy.

Promise terminal receipt retention relative to terminal commit, with a separate
output-availability promise, and reserve the necessary bounded capacity at
admission. Retrying or reconnecting does not extend promises implicitly. Terminal
expiry leaves an anti-reuse tombstone and returns an explicit expired result,
never a fresh admission. Revocation denies access and further execution/publication;
it does not remove accounting or allow name reuse. The final contract must define
how the authority settles revoked accepted work and handles its clock across
restart, including rollback and arithmetic overflow.

## Executable-model scope

The Rust conformance driver contains no production PipeStream dependency.
Its new model explores abstract durable mutations, commit-before-ACK loss,
disconnect, crash, reconnect, owner/revocation checks, two attempt generations,
staged output, publication and expiry. It models atomic metadata transactions;
real database/file crash consistency still requires implementation fault tests.

The model uses symbolic immutable input/output commitments, not cryptographic
proofs or encoded messages. Enumerated bounds, reached states, checked edges,
negative-control counterexamples and omitted behaviors must be reported.
Exceeding a state budget is inconclusive and fails the command, not a passing
proof. A separate two-scope model checks declared-but-missing children,
descendant closure, subtree cancellation, skipped counts and shutdown cuts.
Composition of those scope rules with the attempt/result model is still open.
Wire identity allocation, long-running clocks,
admission byte reservations and full result-transfer behavior still need their
own models/tests before task 1 can be accepted.
