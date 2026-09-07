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

### Rust authority storage evidence, 2026-09-06

`src/v2/authority/` now implements persistent session creation/attachment,
declaration, operation replay/lookup, membership pages, revision snapshots and
empty sealed closure. Its 18 tests (including the subprocess entry point) run
under the same `cargo test --locked -p pipestream-core v2::` command; 35 tests
pass together with the 17 wire-library tests.

- V2-SESSION/V2-OP: `creation_replays_after_reopen_without_reissuing_identity`,
  `simultaneous_creation_and_declaration_commit_once`,
  `different_concurrent_parameters_cannot_share_an_operation`, and
  `counter_exhaustion_and_retired_creation_never_reuse_identity` exercise actual
  transactions and retained identity. The retired-history case seeds a synthetic
  high-water row; it is not a retirement/cleanup implementation test.
- V2-AUTH/V2-TIME: `authorization_precedes_lookup_and_revocation_denies_replay`,
  `policy_withdrawal_before_commit_rolls_back_mutation_and_receipt`, and
  `clock_rollback_refuses_mutation_but_preserves_retained_evidence` use explicit
  local policy/clock implementations. They do not validate TLS or prove revoked
  work settlement. The revocation case sets the retained denial flag directly.
- V2-SET/V2-CLOSE: declaration/replay/page, batch-independent seal, missing input,
  and empty scope tests distinguish membership from admission and completion.
  Nonempty descendant closure is still unimplemented in this store.
- V2-STORE: `process_crash_on_each_side_of_creation_and_declaration_commit`
  runs four child-process exits without SQLite/Rust destructors. Reopen verifies
  pre-commit absence and post-commit replay. This covers metadata commits, not
  payload installation, worker execution, transport ACK loss or cleanup.
- V2-STORE: `physical_exhaustion_rolls_back_whole_batch_and_preserves_replay`
  exercises 128 KiB DB/journal, 4 MiB WAL and 64 KiB SHM caps; committed evidence
  remains readable/replayable after refusal and reopen. This measures file
  lengths, not allocated blocks. The WAL cap now funds record rewrite credits;
  the database cap still refuses a later declaration after a committed batch.
- Reopen/initialization tests prevent accidental empty-store creation during
  recovery. Exact large-integer tests include values above 2^53 and the maximum
  signed-63-bit entity ID. No JSON or floating-point persistence is used.
- V2-TIME: `empty_root_closure_refuses_unrepresentable_receipt_retention`
  failed before the fix and now checks overflow refusal without a committed seal
  or operation, plus acceptance at the exact maximum representable deadline.

The full cross-language gates below remain open. Funded payload admission,
workers/leases/cancellation, nonempty closure, results/read pins, retirement and
cleanup are the next authority implementation work, not implied by these tests.

### Rust payload/ingress evidence

`src/v2/authority/payload/tests.rs` adds 12 storage tests, including a subprocess
entry point; `ingress.rs` adds three header/reception tests. Their scope is:

- V2-ADMIT: reject invalid owner, producer, generation, undeclared work,
  application/mode, duration and response/byte budgets before payload allocation.
  Hash/FIN failure preserves declaration and leaves no operation receipt.
  Verified installation is expressly not admission or execution.
- V2-STORE: six process exits bracket staging-file creation/header, object fsync,
  rename, directory fsync and orphan unlink. Restart distinguishes abandoned
  staging from installed unreferenced objects. Physical power loss is not tested.
- V2-STORE: durable root/database pairing, exclusive ownership, live object pins,
  changed configuration/identity refusal and unknown/aliased entry refusal.
  Collection uses bounded cursor batches under the authority's writer transaction
  and preserves committed references. The reference test does not admit a job.
- V2-RESULT (storage only): exact retained descriptors, full length/hash checking
  at EOF, permanently failed corrupt reads and global/per-owner handle bounds.
  This does not implement authenticated RESULT RPCs, manifests or read leases.
- `cargo test --locked -p pipestream-core --test v2_payload_resources -- --nocapture`
  streams and verifies 32 MiB using 16 KiB buffers in its own allocator-instrumented
  executable. It gates added Rust heap below 256 KiB and individual allocations
  below 64 KiB, and separately reports file lengths, allocated blocks and process
  RSS/HWM. This is neither an end-to-end benchmark nor proof of all process-memory
  bounds or populated-inventory scaling.

Funded admission, durable jobs, complete metadata-transition reservations and
retirement remain unimplemented. Temporary reception quotas and object-reference
cleanup do not substitute for those gates or for either cross-language direction.
Durable output-capacity evidence is recorded separately below.

### Rust fixed-record funding evidence

`src/v2/authority/records/tests.rs` has 11 tests, including its subprocess entry.
This is persistent storage implementation used by declarations and empty scope
closure, not evidence that the entire admission/job transaction is funded.

- V2-STORE: checksummed preallocated work/summary records, exact revisions,
  ordinary writes preserving credits, transactional spending/rollback and named
  refusal for stale revisions, oversize records and exhausted credits.
- V2-VIEW/V2-STORE: a negative-first counter test prevents ordinary updates from
  using the last revision increments reserved for promised record rewrites.
- V2-STORE: a retained SQLite reader prevents WAL reclamation. Ordinary writes
  fill the protected ceiling; four reserved overwrites across two work records
  still commit under a 1 MiB WAL cap with database growth disabled. This measures
  record updates only, not an entire job settlement or cancellation RPC.
- V2-STORE: 18 page/capacity combinations cover 512/4096/65536-byte pages and
  512-byte through 1 MiB records with cache spilling, row replacement prohibited,
  and database page growth disabled. Every measured WAL length fits the
  pinned-layout record bound. File lengths are not filesystem allocated blocks.
- V2-STORE: two child-process exits bracket a credit-spending commit. Restart
  preserves either its entire prior revision/credit or its entire committed
  successor. Header, body, padding and cross-row corruption fail closed; startup
  rejects corrupt bodies even when their charge headers remain intact.

Global/per-owner job budgets, every other mutable record in a complete transition,
dependency/read pins, metadata retirement and cross-language V2 failure tests
remain required. These record-level credits do not close those gates. Output-file
budgets are covered by the reservation implementation below.

### Rust output-reservation evidence

`src/v2/authority/payload/reservations/tests.rs` adds 15 tests, including its
subprocess entry point. Output file capacity is now durable; the full
admission/job/receipt transaction and protocol-level result publication remain open.

- V2-STORE/V2-ADMIT: global/per-owner byte and object forecasts survive reopen;
  ordinary uploads cannot consume reserved output capacity. Partial outputs hold
  their entire maximum; unused bytes remain inside the reservation. Zero-length
  objects and a zero-count reservation have distinct behavior.
- V2-RESULT (storage only): outputs with initially unknown length/hash install
  their actual descriptors and verify through file-backed reads. All 256 slots
  and maximum owner/content-type labels work with only two live handles. Slot
  uniqueness, owner/budget mismatch, over-budget output, chunk bounds, monotonic
  regression, idle/lifetime equality, and non-renewing empty writes are checked.
- V2-STORE: 11 child-process exits cover reservation creation/fsync/rename/
  directory sync/unlink and output creation/header/payload/fsync/rename/directory
  sync. Restart retains installed output promises and reclaims only abandoned
  stages. These tests do not simulate physical power loss or network ACK loss.
- V2-STORE: actual SQLite reference transactions preserve reservation and object
  liveness through cleanup. The test leaves work DECLARED; it is not a fabricated
  admission, manifest, terminal result or retirement implementation.
- V2-STORE: missing/corrupt/aliased funding fails closed. A negative-first test
  forces a real rename failure and now verifies root quarantine until exclusive
  reopen. Filesystem space/quota errors map to LIMIT_EXCEEDED; this mapping test
  does not claim a whole-disk exhaustion experiment.
- `v2_output_resources` streams, installs and reads 32 MiB through 16 KiB buffers
  in an isolated allocator-instrumented executable. It uses the same 256 KiB
  added-heap and 64 KiB allocation gates as the input test. Both report file
  lengths, allocated blocks and RSS/HWM separately. This does not establish QUIC
  flow-control, complete endpoint memory bounds, or a gRPC performance advantage.

Executor capacity, complete metadata/journal funding, worker leases, authenticated
manifest publication/read leases, dependency retention, session retirement,
independent Java V2 and both cross-language failure directions remain required.

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
