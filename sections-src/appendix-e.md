# Open Design and Interoperability Issues

**RFC Editor Note:** This appendix is an issue inventory for draft review.
The issues require resolution before standards-track advancement; remove
the inventory when its decisions are incorporated into the specification.

## Work-Set Closure

The current manifest is reconstructed from received headers and statuses.
That is not sufficient to establish that every intended child has arrived.
Layer 0 lacks a wire declaration of the complete child set. Layer 1's
status digest carries a count and hash, but the producer, declaration,
seal, acknowledgment, and late-child rules need one explicit lifecycle.

The next design milestone is a streamed declaration with an immutable seal
binding the scope, owner, parent, admitted identifiers, and final count.
A checkpoint must refer to a declared cut, not to whatever streams happened
to arrive first. Cancellation and rejected children must remain accounted
for. This requires wire-sequence interoperability tests, not just a local
manifest data structure. The admission ordering rule in Section 9.3 is a
necessary restriction for current checkpoints, not a substitute for a seal.

## Identity and Recycling

Both endpoints can originate work, but the current scope-local integer
namespaces lack an allocation partition or an owner field. The next
revision must choose one rule that prevents collisions. Cursor recycling
also needs an epoch or a prohibition on reuse while durable references
remain valid. Scope-local cursors and the unscoped Last Entity ID in
GOAWAY must be reconciled with the same identity model.

Until those decisions are made, application profiles must not infer that
independently allocated numeric IDs are globally unique. A prototype that
does not recycle IDs must report that limitation rather than claiming the
entire Layer 0 lifecycle.

## Extension Negotiation

Closed capability maps cannot bootstrap arbitrary new members. A base
supported/required extension identifier mechanism is needed, with bounded
lists, activation rules, and refusal of unknown required extensions.
Unknown-frame skipping alone does not mean an extension was negotiated.
The broad Layer 2 boolean also needs either complete implementation or
separately negotiated, precisely named resilience profiles. A private
README profile is not an interoperable wire capability.

## Recovery Outcomes and Authenticated Receipts

Single-use redemption does not tell a requester whether its first request
committed when the acknowledgment was lost. A future revision needs an
idempotent redemption request identity and an authorized retained-outcome
lookup, with retention, expiry, and revocation rules. Existing duplicate
refusal prevents an ordinary replay but does not solve ambiguous outcomes.

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
