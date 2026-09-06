# Open Design and Interoperability Issues

**RFC Editor Note:** This appendix is an issue inventory for draft review.
The issues require resolution before standards-track advancement; remove
the inventory when its decisions are incorporated into the specification.

The version-1 limitations below remain relevant to the shipped implementations.
Section 12 defines a distinct version-2 contract for identity, result delivery,
retention and profile composition; it does not change version-1 wire meanings.
Bounded composed failure-model checks now exercise a branch and leaf; remaining
successor acceptance work includes independent implementations, authenticated
failure interoperability and the
equivalent-workload comparison. Normative text alone does not establish those
properties in a running implementation.

## Work-Set Closure

Section 9.8 defines an opt-in, private-use lifecycle for client-produced
declaration batches, immutable seals, durable acknowledgment replay, and
full-scope checkpoints. Missing or rejected declared entities stay outstanding;
the profile has no implicit cancellation tombstones. This closes the
received-so-far cut only for sessions using that profile. The unsealed
lifecycle, including Layer 0 dehydration, still lacks complete-set evidence.

Independent implementations must validate the profile's lost-ACK, reconnect,
late-child, missing-payload, and descendant-closure sequences. A general
bidirectional protocol and explicit cancellation outcomes remain design work.

## Identity and Recycling

Outside Section 9.8, both endpoints can originate work, but the scope-local integer
namespaces lack an allocation partition or an owner field. Section 12 uses
authority-issued generations and disjoint producer namespaces in version 2.
Version-1 cursor recycling
also needs an epoch or a prohibition on reuse while durable references
remain valid. Scope-local cursors and the unscoped Last Entity ID in
GOAWAY must be reconciled with the same identity model.

Section 9.8 binds one client producer to a durable session, prohibits reuse,
uses scope-qualified completion cursors, and defines a root GOAWAY cut.
It does not allocate namespaces for two independent producers. Profiles
must not infer that independently allocated numeric IDs are globally unique
or describe a producer label as authentication.

## Extension Negotiation

Section 3.4.3 now defines bounded supported/required identifier lists,
activation rules, and refusal of unsupported requirements. The broad
Layer 2 boolean still needs either complete implementation or
separately negotiated, precisely named resilience profiles. A private
README profile is not an interoperable wire capability.

## Recovery Outcomes and Authenticated Receipts

Single-use CLAIM_REDEMPTION does not tell a requester whether its first request
committed when the acknowledgment was lost. Section 10.6.5 now specifies an
opt-in authenticated recovery profile with immutable request identity, retained
admission and terminal outcomes, expiry, and revocation. This resolves that
ambiguity only for negotiated recovery requests; it does not change legacy
redemption or add outcome lookup to sealed-work sessions. Independent recovery
implementations and broader failure-boundary evidence remain necessary.

A versioned content receipt could commit to a sealed manifest, payload and
result digests, parent identity, policy, and nested receipts. It would also
need authentication and a threat model. Neither a bare hash nor an
authenticated worker's assertion proves that computation was correct.

## Evidence Required

Independent implementations should test the clarified sequences against
frozen examples, including reordered streams, partial policies, timeouts,
lost acknowledgments, slow consumers, authorization failures, and crashes
at each persistence boundary. A successful one-entity transfer matrix is
not evidence for those behaviors. Document-format validation does not
validate distributed semantics.

## Result Delivery and Profile Composition

Core lifecycle reports and status Merkle roots do not specify where an
application retrieves transformed output or how that output is bound to
the input, processor, and attempt. Sealed work currently permits only
client-originated entities; returning a digest from a local callback does
not define a server-to-client result channel. Before claiming interoperable
video processing, map/reduce, or general work orchestration, choose an
explicit result contract: application-managed output with authenticated
references, or a negotiated result-stream profile. Define output identity,
integrity, authorization, retention, backpressure, and failure semantics.

Authenticated recovery currently excludes sealed work. Combining them
requires a negotiated lifecycle defining whether a retry replaces an
attempt or creates new work, how refusal or authorized cancellation resolves
a declared obligation, and how reconnect retrieves retained outcomes.
Removing the exclusion without those rules would change completion meaning.
The existing authentication requirement applies to durable sealed work;
the base sealed extension alone does not negotiate an authentication method.
Decide whether a successor profile requires a named binding on the wire
rather than relying on an explicit application profile.

## Retention and Long-Running Work

The recovery profile fixes receipt retention at 24 hours from admission,
not from completion. A job can still be running when replay becomes expired.
The specification must distinguish accepted-job lifetime, input and output
retention, receipt replay, authorization expiry, and anti-reuse history.
A successor profile could negotiate bounded retention or promise an interval
after completion, but would need admission quotas and crash-safe cleanup.
The current profile's fixed interval must not be silently extended by retry
or interpreted as cancellation of accepted work.

## Minimal Core and Completion Summary Semantics

Resolve the mandatory Core lifecycle before extending every reference
implementation. Evaluate whether recursive work management, storage-provider
metadata, and the four-value data-layer vocabulary belong in the mandatory
base or separately negotiated profiles. Any reduction of existing mandatory
behavior needs an explicit version and compatibility decision.

SCOPE_DIGEST also needs an unambiguous count partition: SKIPPED is terminal
but is neither COMPLETE nor a failure in the current Rust counters, and a
fully resolved scope cannot contain a still-DEFERRED entity. Specify whether
the counters represent final states or historical events and how skipped
work is counted before treating their sum as a completeness proof. The
current digest's status root is not a payload or output commitment.

Review should include a small executable state model and fault traces for
commit-before-ACK loss, reset during admission, descendant closure, and
stale publication. Utility evidence should compare equivalent processing
and durability semantics over PipeStream and an RPC-based coordinator,
measuring coordination code, latency, bytes, and resources rather than
assuming a transport win. Independent implementer review matters more
than adding another language that repeats the same unexamined assumptions.
