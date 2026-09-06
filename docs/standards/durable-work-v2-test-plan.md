# Version-2 implementation acceptance ledger

Normative source: Section 12 and Appendix F of local draft -05. This ledger
organizes the mandatory selected-profile requirements into test families. It
does not replace the normative text, waive any clause, or assert conformance.
The full goal remains in [the execution record](durable-work-results-goal.md).

Status: every implementation family below remains open as a cross-language
acceptance gate. Rust now has the library coverage recorded below; no V2
endpoint advertises a partially implemented profile. The existing version-1
tests and the three abstract models are regression/design evidence only.
Rust and Java must independently implement the same durable-work plus
result-delivery combination. Neither may advertise a partially implemented
profile. C++ remains at its existing version-1 subset for this goal.

For every family, record exact Rust tests, exact Java tests and the independent
Rust process-driver scenario where applicable. A happy path alone is not
sufficient. Storage families require actual restart/crash evidence and measured
resource gates, not only mocks or an in-memory state model. Keep test names and
logs traceable to these stable family IDs as implementation proceeds.

## Rust library evidence, 2026-09-06

Source: `implementations/rust-quinn/src/v2/`; tests in its `tests.rs`.
Run `cargo test --locked -p pipestream-core v2::` from that Rust workspace.
These are 17 library tests, not process-driver or Java evidence:

- V2-WIRE: `every_frozen_wire_case_has_exact_typed_roundtrip_or_named_refusal`
  executes all 70 frozen rows. Accepted typed records encode back to identical
  bytes; all 24 refusals retain their named code. The truncation test rejects
  every prefix and trailing byte for every accepted example. Separate tests
  cover forbidden CBOR types, hostile lengths and unknown frame classes.
- V2-NEG: `negotiation_checks_authentication_required_sets_dependencies_and_every_limit`
  checks the selection algorithm with a supplied authentication decision; it
  does not authenticate a certificate. Correlation tests cover increasing IDs,
  out-of-order replies, wrong kinds, duplicates, bounded pending/stream counts,
  abort, input stream identity and cross-connection result-proof rejection.
- V2-OP/SET/CLOSE: `all_frozen_commitments_are_computed_from_typed_fields`
  matches all 12 commitments. Incremental seal tests include 1,000 declarations
  and missing/extra/unsorted members. Incremental status roots match a separate
  level-by-level fold for sizes 0 through 1,000, including odd duplication.
- V2-VIEW/RESULT: tests reject contradictory terminal fields, profile-dependent
  success shapes, wrong receipt variants, invalid retry increments, inconsistent
  fence dispositions, count partitions and locator/manifest identities.
- V2-RESULT: payload/correlation tests require exact SHA-256/length/FIN and
  bounded monotonic idle/lifetime progress. A result header is not completion;
  a second response is refused, and a blocked library transfer does not borrow
  the control book. This is not a QUIC flow-control or process-memory gate.

The implementation pass exposed an error-code ambiguity: Section 12.1's generic
invalid-selection rule could be read as FRAME_ERROR for a missing result-profile
dependency, while the frozen response example specifies EXTENSION_UNSUPPORTED.
The text now explicitly preserves the dependency-specific refusal in either
direction. No frozen bytes or refusal expectations were changed.
The stream deadline text also now specifies the equality boundary explicitly:
progress or FIN at the idle/lifetime deadline is too late, not a renewal.

Still required: independent Java codecs; authenticated V2 Quinn/Netty endpoints;
session and operation journals; real durable authority/storage/execution;
restart/cleanup/resource measurements; neutral Rust cross-language scenarios.
The library helpers do not satisfy these outstanding acceptance gates.

## V2-WIRE: framing, decoding and representation (12.1, 12.2, Appendix F)

- Own the `pipestream/2` mapping without accepting version-1 messages or silently
  converting version-1 storage. Keep Core independent of recursive scheduling.
- Exact control type/u32-length framing and array cardinality; minimal integers
  and lengths; reject maps, tags, floats, indefinite items, undefined, invalid
  UTF-8, trailing items and extra/missing positions. Bound allocation before
  consuming an advertised length; reject malformed nested collections.
- Decode every defined request, response, receipt, work view, manifest, summary
  and object header into typed fields, independently in both libraries.
- Consume all frozen `test-vectors/v2/wire.tsv` expectations, including the
  semantic/canonical refusals beyond CDDL. Accepted bytes round-trip unchanged.
  Do not regenerate golden bytes from either implementation.
- Required/ignorable/private unknown type classes, message direction and
  profile-dependent types have their specified connection/request error scopes.

## V2-NEG: negotiation and connection accounting (12.1, 12.2)

- One capability exchange, client/server directions, exact supported/required
  intersection/union, bounded sorted unique lists, dependency selection and
  forbidden legacy IDs. Required unavailable profiles fail before activation.
- Reject unsolicited selections, required-set omissions, increased limits and
  invalid deadline relationships. Resume requires the retained profile set and
  representable retained responses, not an implicit downgrade.
- Strictly increasing connection-wide control request IDs, independent actual
  input-stream tags, response kind/identity validation, duplicate and unsolicited
  response refusal, bounded pending maps and exhaustion handling.
- Named REFUSAL and QUIC application error mapping, unknown errors, Stream 0
  failure, partial framing and errors after a result stream starts. Transport
  loss or a refusal is never an invented authoritative work outcome.

## V2-AUTH: authenticated owner and current authorization (12.3, 12.7)

- Server DNS/IP verification, TLS 1.3/QUIC v1, no 0-RTT, real client-certificate
  possession/trust/validity/usage checks and stable principal mapping in Java
  as well as Rust. Preserve RFC 9001 handshake errors, not application fallback.
- Missing/unmapped principals and optional/required durable activation;
  certificate rotation, expiration on a live connection and resumption policy.
- Owner/authority mismatch, foreign owner, revoked sessions and cross-producer
  authorization. Denials reveal no retained work or output, including through
  refusal selection or output-reference resolution.
- Recheck authorization in committing mutations, callback publication and every
  result read. Revocation stops further scheduling; previously transmitted
  bytes and committed external effects cannot be retracted.

## V2-SESSION: issuance, attachment and anti-reuse (12.3)

- Atomic authority generation/owner creation high-water allocation, exact policy
  and retained profile binding, root creation and creation receipt.
- Lost-ACK identical creation replay, changed-policy conflict, out-of-order
  sequences, concurrent callers with one principal, two principals, retirement
  returning EXPIRED and counter exhaustion without wraparound.
- One attached session per connection, immutable admission ceilings, explicit
  attach identity, current authorization and safe handling of stale backups.
  No random-ID uniqueness assumption or history eviction to recover quota.

## V2-OP: immutable operations and uncertainty (12.4)

- Nonzero 16-byte IDs; producer-and-session namespaces distinct from target-work
  producer; complete immutable parameters persisted before transmission.
- Independent operation-digest encoding against frozen commitments, omitting
  only connection correlation and raw payload bytes as specified.
- Atomic mutation/typed receipt, same-ID concurrent replay, changed digest/type
  conflict, pre-commit refusal and commit-before-lost-ACK recovery.
- NOT_FOUND while an old request is still in flight does not authorize new work.
  Keep identity/digest after full receipt expiry and never reapply a retired ID.

## V2-SET: immutable declarations and child identity (12.5, 12.8)

- Root and child producer ownership, increasing IDs within/across bounded
  batches, empty-seal rules, declaration capacity and covering receipt before
  input. Declaration is not processing admission.
- Immutable membership/seal, incremental whole-scope seal hashing, changed
  replay, late declaration, undeclared input and wrong-seal named refusals.
- Parent admission before descendants; leaf/caller-branch/authority-branch
  modes; atomic one-time child allocation; retry cannot replace a child scope.
- Ordered bounded scope pages, unsealed growth between snapshots, parent and
  producer binding, `more`, and empty pages not proving completeness.

## V2-ADMIT: payload validation and funded acceptance (12.5)

- Strict header length, actual stream identity, attached generation, external
  producer restrictions, configured versioned application contracts and budgets.
- Incremental bytes/hash/FIN validation, empty input, truncation, trailing bytes,
  wrong digest and interrupted reception without losing the declaration.
- Durable immutable input installation before atomic input/job/attempt/deadline/
  child/receipt/accounting commit. No irreversible callback effect beforehand.
- Matching header replay without re-execution; STOP_SENDING alone is not an
  admission receipt. Reserve all promised output/manifest/receipt/closure/control
  and journal capacity, including across restart. Refuse unfunded admission.

## V2-ATTEMPT: execution, retry and publication fences (12.6)

- Explicit allowed states, distinct attempt generations and restartable jobs.
  Callbacks run outside metadata transactions and never block control reading.
- Retry current expected attempt before the original deadline, exactly once
  under operation replay; preserve input/child/membership/policy/deadline.
- Current authority/owner, ancestor fence, revocation, attempt, deadline and
  durable worker lease all checked at publication commit. Race each with a
  staged callback result; restart does not increment the wire attempt.
- Retryable attempt failure versus terminal work failure, named refusal of
  terminal/cancelling/expired/exhausted retry, immutable final outcomes and
  application idempotency/fencing without exactly-once external-effect claims.

## V2-CANCEL: cancellation, skip and descendant settlement (12.6)

- Cancel/publication race in both orders; existing terminal outcome is returned
  unchanged. First accepted cancel/skip fence fixes its eventual outcome.
- Skip policy authorization, disposition/state validation, inputless settlement,
  target SKIPPED with unresolved descendants CANCELLED and no success counting.
- Ancestor fence excludes late declaration/admission/retry/publication before
  bounded batched settlement. Parent stays effectively CANCELLING until its
  descendant scopes close; committed descendant outcomes survive.
- Owner-authorized whole-scope cancellation including producer-1 and empty
  scopes, root cancellation, revocation of unadmitted declarations, restart
  reconciliation and deadline failure without implicit subtree disappearance.

## V2-VIEW: retained observation and revision (12.6, 12.9)

- Immediate snapshot, revision monotonicity, bounded change wait, invalid future
  revision, unchanged view at timeout and consistent immutable identity fields.
- All state-dependent null/required fields, positive admitted attempts, inputless
  cancellation/skip, diagnostics, manifests and independent availability times.
- Retained receipts after output expiry and readable retained outputs after full
  receipt expiry. No guessed state from transport loss or missing payload bytes.

## V2-RESULT: actual outputs, manifests and references (12.7)

- Bounded validated immutable output bytes installed before atomic manifest plus
  success; orphan is not visible; over-budget output cannot become truncated
  success. Zero outputs are allowed and non-success has no success manifest.
- Contiguous indexes, count/byte/object budgets, complete authority/owner/session/
  work/attempt/input/time binding and independent frozen manifest commitments.
- Manifest lookup and full object request/stream correlation, header/length/hash/
  FIN validation; named not-ready/not-found/expired/integrity/unavailable errors.
- Reset/read retry never runs computation. Read pins, pending creation, handles,
  buffers and deadlines are bounded; existing readers survive permitted expiry
  but not indefinitely. A corrupt retained object never triggers an automatic
  rerun or a replacement success manifest.
- URI grammar and numeric boundaries, manifest plus selected-index reference,
  explicit attachment/authentication, expected commitments, trusted authority
  mapping and no implicit redirects, bearer credentials or cross-authority access.

## V2-CLOSE: exact completion and distinct detach (12.8)

- Seal plus all declared members and all descendants terminal/closed; STRICT
  requires successful children for parent rehydration. Missing children, invalid
  input, skip, failure and cancellation must not become successful coverage.
- Four disjoint final counters and declared-count equality, empty scopes,
  immutable status roots, domain-separated leaves/nodes/empty root, odd-node
  duplication and parent/child commitments against frozen examples.
- Checkpoint seal/state/timeout refusals and immutable repeated summary; root
  completed-session DRAIN rejects child cuts, altered summaries and live transfers.
- Core detach drains only the connection, bounds its wait and never claims
  work completion, expiry or cancellation. Abrupt disconnect has the same
  non-effect on accepted work.
- An already accepted parent cancellation/skip fence takes precedence over
  automatic STRICT failure from a cancelled or failed child; descendant closure
  must not overwrite that promised settlement.

## V2-TIME: independent lifetimes and trusted clocks (12.9)

- Exact integer UTC milliseconds, checked arithmetic, session maxima and
  original execution deadlines; retry/replay/reconnect/read never extends them.
- Active input/identity cannot be evicted at a receipt age. Deadline/revocation
  drives authoritative fenced settlement, including after restart.
- Post-terminal output and receipt deadlines are independent; parent-dependency
  and active-read pins can outlive external output availability and stay charged.
- Persisted greatest UTC, backward/untrusted clock refusals, no destructive
  expiry under unsafe time, read-only evidence without fresh leases, documented
  trusted-clock and forward-jump assumptions, and safe restart behavior.

## V2-STORE: crash boundaries and measured resource limits (12.1, 12.5, 12.9)

- Crash both sides of input installation, admission, retry/lease fencing,
  publication, closure and cleanup commits, including lost replies. Retained
  results and anti-reuse identity must survive without phantom work.
- Atomically couple jobs, receipts, fences, summaries and reservations. Preserve
  pins and orphan charges on restart; reconcile before admitting new capacity.
- Safe matched-store ownership, references checked before deletion, replayable
  interrupted cleanup and retirement only after root closure plus every longer
  receipt/output/dependency/read promise. Preserve issuer/owner high-water marks.
- Last-owner lock release must not depend on unrelated inherited descriptors;
  copied nonowner guards must not unlock the original live process's store.
- Actual payloads larger than QUIC windows, slow/stopped consumers, independent
  control progress, saturated workers/storage, per-principal/global counts and
  bytes, file handles, staging, journals and retained promises.
- Measure Rust heap, Java heap, total/native process memory, disk-file lengths,
  actual disk/network I/O and failure-time cleanup separately. A heap gate is
  not an RSS claim and file length is not allocated filesystem blocks.

## Cross-language and workload gates

The independent Rust process driver must run Rust-to-Java and Java-to-Rust
happy-path and failure sequences from these families against real servers,
durable roots and certificates. It must not import either protocol codec as
its acceptance oracle. Exact bytes, authenticated views, named refusals and
persisted/recovered outputs are evidence; process launch or a digest-shaped
placeholder is not.

Task 3 remains separate: an external streaming chunk/transform/distribute/
reassemble application and equivalent streaming-gRPC baseline with matching
authentication, persistence, retry, processing and output guarantees. Retain
commands, pinned builds, raw performance/resource measurements and failure
traces, and feed measured tradeoffs back into the draft.
