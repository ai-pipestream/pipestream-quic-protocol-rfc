# Open Design and Interoperability Issues

**RFC Editor Note:** This appendix is an issue inventory for draft review.
The issues require resolution before standards-track advancement; remove
the inventory when its decisions are incorporated into the specification.

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
namespaces lack an allocation partition or an owner field. The next
revision must choose one rule that prevents collisions. Cursor recycling
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
