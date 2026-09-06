# Draft-04 readiness and implementation evidence

Updated 2026-09-06. The original draft-04 landing starts from `00531de` and addresses the
[protocol review](../ai-slop/ietf-protocol-review-2026-09.md).
The specification remains an individual Internet-Draft, not an approved
standard. No implementation in this repository demonstrates full conformance.
The [recovery, execution and Java acceptance audit](recovery-execution-java-acceptance.md)
records completion evidence for those three implementation deliverables on
`5df4ec3`, separately from full protocol conformance and future extensions.
The increment sections retain their original validation counts and limitations;
dated follow-ups identify the guarantees that supersede them. Current Java storage
uses database schema 6 and payload policy 3. Current Rust storage
uses the physical completion-reservation layout described below, not the older
policies recorded in the incremental-index prerequisite.

## Changes and regression coverage

### Protocol-model and safety corrections (2026-09-06)

The [protocol review corrections](protocol-review-corrections-2026-09.md) separate
transport observations from authoritative outcomes, remove the blanket
whole-entity QUIC credit prerequisite, and align admission and checksum rules.
They clarify durable sealed authentication, privacy and encrypted-reference
boundaries, and correct stale implementation/outreach claims. No executable,
wire/CDDL, or storage-format change is included. Appendix E records the remaining
identity, result-delivery, retention, summary-count, and profile-composition
decisions without silently defining new wire behavior. The linked record holds
the current validation results and their limits.

### Rust scope-completion correlation (2026-09-06)

The Rust public client now requires the expected parent identity and depth for
scope closure. It correlates each returned parent status, preserves FAILED as
failure and closes on incorrect identity or lifecycle. The single-argument
`close_scope` API is replaced; repository callers supply their actual manifest
context. Five real-QUIC tests cover 35 exchanges, including both client profiles,
field/lifecycle mismatches, intact success/failure and partial-frame cancellation.
Local invalid contexts refuse before sending; cancellation requires reconnect
and does not cancel server work. Section 9.8 now states the parent-correlation
requirement explicitly. No wire/CDDL or storage format changed. See the
[goal evidence](recovery-execution-java-plan.md) for validation status and limits.

### Rust client status-history limits (2026-09-06)

The public recursive client's whole/chunked entity methods now retain at most
128 STATUS frames and 4 MiB of encoded STATUS data per operation, including
extensions and UCF headers. Exact boundaries return the unchanged history;
exhaustion closes with `PIPESTREAM_LIMIT_EXCEEDED`, not partial success. A peer
that stops after filling the count budget without a terminal status also refuses.
Four real-QUIC tests cover 19 exchanges across ordinary/sealed and whole/chunked
paths, byte/count boundaries and exact preservation of a large yield token and
claim check. The incoming frame and transient decoding allocations are separate
from the retained-history bound, so this is not a whole-process memory gate.
Wire/CDDL, storage formats and server execution semantics are unchanged.
See the [goal evidence](recovery-execution-java-plan.md) for validation status.

### Rust explicit orphan reconciliation (2026-09-06)

The Rust file backend now reconciles a closed, previously paired root against a
writer-locked session snapshot. It audits all managed admitted inputs, including
terminal/refused work and revoked sessions, before deleting any files. Foreign or
unbound pairs, caller-managed admission without an original PROCESS descriptor,
corrupt/missing inputs, live handles/readers/loans and unknown or aliased files
refuse. No cleanup operation changes session state or supplies a completion ACK.

Before removing an orphan body, its immutable `.meta` becomes a synced `.commit`.
Identity, owner, original length and digest remain. Matching retransmission
reserves normal installation capacity and renames the existing record back;
changed input refuses. Interrupted cleanup keeps remaining file lengths charged
and resumes explicitly. Admitted bodies, commitment object slots, partial metadata,
directories and final-lineage allowances are not garbage-collected. `PSRET004`
refuses prior file policies without conversion; database policy, wire/CDDL and
frozen vectors are unchanged.

Focused storage and actual-QUIC tests cover quota, replay, named refusals, writer
and live-handle exclusion, process exit and I/O failure. The sealed wire scenario
keeps missing declared input pending through timeout and reconnect, rejects changed chunks and
completes only after matching out-of-order chunks restore the original input.
The isolated 32 MiB resource case measures 13,048 bytes additional Rust heap and
a 2,216-byte largest allocation for installation/reclamation/restoration, under
separate 1 MiB gates. This is not native-allocation/RSS or multi-tenant evidence.
All 21 new tests pass: five core maintenance, fifteen Quinn storage and one actual
QUIC test. The final repository conformance command passed with 297 Rust workspace
tests, 158 Java tests with no failures/errors/skips, native SQLite/C++ checks,
nine executable pairings, 32 capability probes, recursive/recovery CLI checks and
all three external examples. Formatting, strict clippy and strict Rustdoc pass.
Draft -04 has zero idnits errors/flaws/warnings and one FIPS reference comment.

An earlier full run exposed a wire-test timing assumption: a connection deadline
may expire before checkpoint-request persistence. The corrected test checks that
neither an absent request nor a retained pending request acknowledges missing
work. A subsequent full run passed; final review then added the rejected-stage
regression and the complete suite passed again. Standalone spool handles now hold
the retained-root process lock too, verified with a subprocess. Rejected staging
bytes are reclaimable, but corrupt published bodies still refuse before deletion.
Persistent producer observations and broader resource/interoperability acceptance
remain unfinished. These are local gates, not hosted CI, an operational cleanup,
migration, deployment, release or draft submission.

### Rust payload-store pairing (2026-09-06)

Rust service construction now requires its `EntityStore` to durably bind the
session database before admission or dispatch. The file backend pairs a random
retained-root identity with the database's separately generated identity. Its
immutable file claim is synced before the SQLite claim; an interrupted complete
claim can replay, while foreign pairs, corrupt/partial images and missing claims
for bound stores refuse without repair. Binding does not admit payloads or prove
that all retained inputs exist.

The database stores a checked 72-byte `PSRBND01` image in a strict singleton table.
The file root has a 56-byte `PSRID001` identity and optional 72-byte pairing claim.
Every database connection validates its root schema and cached database identity.
Binding writes preserve unchanged admitted-job completion reservations; they are
ordinary metadata writes, not permission to consume protected WAL capacity.
`PSDBL003`/`PSRET003` refuse previous policies without conversion. Session format 7
and normative wire/CDDL are unchanged. Custom entity backends must implement the
new binding method, with no silent default.

Seventeen focused tests pass, covering quotas, rollback, corruption, concurrent
claims, held writers and abrupt process exit. The authenticated recovery QUIC
restart test pins the storage pair during retained receipt replay. Rust orphan
reclamation, persistent producer observations and
the broader resource/interoperability goal remain unfinished.

The final repository conformance command passed locally with 276 Rust workspace
tests, 158 Java tests without failures/errors/skips, native SQLite/C++ checks,
nine executable pairings, 32 capability probes, recursive/recovery CLI checks and
three external examples. Formatting, strict clippy and strict Rustdoc passed.
The 20 authenticated-session/recovery QUIC tests also passed as a focused gate.
Draft -04 has zero idnits errors/flaws/warnings and one FIPS reference comment.
Earlier full runs exposed test fixtures that assumed the old schema-validation
timing and two-file root inventory; those expectations now reflect the new
ownership checks without weakening no-admission or no-repair assertions.
These are local results, not hosted CI, an operational migration or a release.

### Java explicit orphan reconciliation (2026-09-06)

Java now offers a blocking offline maintenance API for a closed, paired managed
payload root. Exclusive ownership and SQLite's writer lock cover the audit and
filesystem sequence. Every managed input and immutable object must verify before
any abandoned receive/install name is removed. Caller-managed admission and
unbound/wrong pairs refuse without guessed ownership. All admitted bodies remain
retained, including completed and refused work.

Unadmitted bodies become `.commit` records: the original encoded metadata and
SHA-256 remain after a synced rename followed by truncation. They do not constitute
input admission or completion. Matching retransmission restores the body; changed
bytes or headers refuse. Interrupted cleanup retains the remaining actual file
lengths and resumes only through another explicit call. A restored admitted object
allows removal of its redundant commitment name, never its body.

Payload policy 3 introduces commitment records and refuses prior policies without
conversion. Database schema 6, wire/CDDL and frozen vectors are unchanged. Focused
tests cover quota, concurrent/chunked restoration, corruption, ownership, I/O
failure, four subprocess exit phases and a 32 MiB body under a 24 MiB Java heap.
Actual QUIC covers pending checkpoint timeout, changed retransmission refusal and
matching completion after reopening the reconciled store. This is not a full-goal
or whole-process resource claim. Rust reclamation,
persistent producer observations and broader resource/interoperability work remain.

The 12 focused tests and final `./conformance/run_all.sh` passed locally. The full
run includes 158 Java tests with no failures/errors/skips, 259 Rust workspace tests,
native SQLite/C++ checks, nine transfer pairings, 32 capability probes,
recursive/recovery CLI checks and three external examples. Formatting and strict
clippy passed. Draft -04 has zero idnits errors/flaws/warnings and one FIPS reference
comment. The changed payload API passed strict Javadoc and whole-module structural
Javadoc passed; strict whole-module Javadoc retains 100
missing-comment warnings in four unchanged Layer 0 types. No hosted CI, operational
cleanup, migration, deployment, release or draft submission is claimed.

### Java payload-store pairing (2026-09-06)

The Java managed executor now requires a persistent database/payload-store pair.
Schema 6 stores checksummed database and payload identities; payload policy 2
stores its own identity and a synced database claim. Earlier layouts are refused
without conversion. A complete file-side claim can replay after its database
transaction fails, while different database/root pairs and corrupt or missing
bound claims are refused. Managed admission revalidates the retained object and
pins its owning store through the transaction. Closed handles and foreign live
handles cannot admit cached metadata.

Fourteen focused tests cover both mismatch directions, reopen, an abrupt process
exit between claims, a real SQLite binding-write refusal, competing roots, pinned
admission while a writer is held, stale/foreign handles, missing/corrupt input,
corrupt binding images and prior-format refusal. Full-suite validation exposed
an intermittent C++ capability-refusal failure with no stderr diagnostic. A traced
rerun passed; the original exit cause was not retained. Inspection found a caller
reusing a connection handle after callback close and completion state destroyed
before callback teardown. C++ now shuts down through its owned registration and
keeps completion state alive through teardown. The capability oracle requires an
ordinary nonzero exit, refuses signal termination even after a named error, and
reports the case, status and both output streams.

All 32 probes passed three consecutive focused runs. Final repository conformance
passed with 146 Java tests without failures/errors/skips, 259 Rust workspace tests,
native SQLite/C++ checks, nine transfer pairings, 32 capability probes,
recursive/recovery CLI checks and all three external examples. Draft -04 passed
idnits with zero errors/flaws/warnings and one FIPS reference comment. Structural
Javadoc passed; strict Javadoc still reports missing comments in four unchanged
Layer 0 types. These are local results. This increment is a storage prerequisite
for orphan reclamation, not a new wire profile or a complete resource guarantee.

### Original review findings

| Review item | Implemented change | Evidence |
|---|---|---|
| R1 | Validate received maps before applying optional defaults; Java/C++ accept core recursive capability limits when downgrading | `test-vectors/optional-fields.tsv`, consumed by all three codecs |
| R2 | Omitted checkpoint flags decode as zero without false deterministic-encoding refusal | Same shared vectors |
| R3 | Recursive Rust skips unknown control frames after negotiation | `r3_r4_unknown_frame_and_entity_without_pending` |
| R4 | Entity reception does not require PENDING | Same raw-QUIC test |
| R5 | Announcements are matched by entity identity, not stream acceptance order | `r5_announcements_do_not_order_entity_streams` |
| R6 | Pending checkpoints allow descendant progress, use monotonic deadlines, and emit a named timeout | Two checkpoint wire tests |
| R7 | Session construction uses negotiated depth and entity limits | `r7_negotiated_depth_is_enforced_before_payload_storage` |
| R8 | A processor yield cannot emit Layer 2 statuses on a Layer 0 connection | `r8_layer0_never_receives_layer2_statuses` |
| R9 | Checkpoint ACK identity is compared exactly; claim, digest, and barrier correlation is also checked | `r9_mismatched_checkpoint_ack_is_refused`; happy-path recursive and recovery scenarios |
| R10 | Quorum uses shortest exact CBOR floats and an integer-computed success threshold | Deterministic encoding tests, corrected recursive vector, `quorum_threshold_uses_exact_integer_rounding` |
| R11 | Durable acquisition and publication fences coordinate recovery across store handles | `concurrent_recovery_fences_resume_across_store_handles` checks one live attempt; stale-attempt tests check publication refusal |
| Admission | Invalid parent/depth/identity and digest failures occur before application callbacks or final payload installation | Callback counters and absent-child-storage assertions in wire tests; receive spools are temporary and quota-charged |
| Resource handling | Independent bounded stream readers; aggregate receive/chunk budgets; incremental control-body allocation | Stalled-stream and aggregate-chunk-limit wire tests |
| URI | Typed session/entity/claim locators, explicit port and numeric bounds, no userinfo or bearer secrets | Core URI acceptance and refusal tests |
| Evidence integrity | Actual Appendix C/CDDL definition comparison; independent expected local receipt calculation | Conformance schema drift test and recursive CLI receipt equality checks |
| Extension negotiation | Bounded supported/required sets, exact intersection and requirement union, named refusal, client response validation | 35 shared codec cases and raw QUIC probes, detailed below |
| Sealed work sets | Opt-in client producer binding, durable declaration/seal ACKs, immutable full-scope cuts, and declaration replay | 20 frozen wire inputs, core storage tests, and raw/public-client QUIC tests, detailed below |

The wire tests are in `implementations/rust-quinn/quinn/tests/draft04_wire.rs`.
The coverage above is a review-finding matrix, not a requirement-to-test matrix
for every MUST in the specification. Unlisted behaviors are not implicitly
verified. All implementations still need that complete matrix.

## Standard changes

- Normative QUIC TLS mapping reference, corrected WebTransport comparison,
  and explicit UDP connection-failure behavior.
- Explicit RFC 9525 service-identity checks before application frames: DNS/IP
  SAN matching and no Common-Name fallback. Both Java clients now enforce this;
  a full certificate-edge-case matrix across all implementations remains due.
- Precise omitted-field and binary16/binary32 representation rules.
- Exact quorum threshold and partial-success semantics.
- Pending-checkpoint timeout, duplicate-request deadline behavior, and an
  admission requirement preventing checkpoints from overtaking their own cut.
- Scope status hashes explicitly distinguished from content receipts and
  proof of correct computation.
- Authentication, authorization, principal/session/authority binding,
  retention, revocation, and external-effect idempotency requirements.
- Tagged `pipestream://` resource paths, a required port, bounded identifiers,
  and explicit separation of locators from access credentials.
- Specification Required registry policies, registration templates, yield
  reasons, and checkpoint-timeout error 0x0E. Error 0x06 is named
  PIPESTREAM_LIMIT_EXCEEDED to cover payload and aggregate resource limits.
- Factual implementation status and an explicit open-issues appendix.
- Supported/required extension negotiation, one exchange per connection,
  explicit activation and downgrade rules, and a proposed 16-bit registry.
  No public extension identifiers are assigned. The Rust recursive service
  additionally offers private-use profiles 65281 and, when mutual TLS is
  configured, 65282 by explicit peer agreement.
- Section 9.8's client-owned work-set lifecycle, identity non-reuse, declaration
  and seal hashing, scope-qualified checkpoints, and root GOAWAY cut.

## Extension requirement coverage

The negotiation landing (`d10d9e2`) builds on the first draft-04 landing
(`89711e1`). It supplied the base mechanism for the sealed-work profile below
and future authenticated recovery profiles.

All three independent codecs consume `test-vectors/extension-negotiation.tsv`.
The positive selection cases use synthetic identifiers solely as test data;
they do not assert implementation or registration of an extension.

| Section 3.4.3 requirement | Evidence |
|---|---|
| Bounded, sorted, unique identifier lists and required subset | `too-many`, `unsorted`, `duplicate`, reserved/type cases, `required-not-supported` |
| Received CBOR determinism before defaults | Empty-list, non-minimal array/integer, indefinite array, float and trailing-item cases |
| Intersection of supported sets and union of requirements | `intersection-required-union`, `maximum-required-union` |
| Both parties' requirements must be supported | `client-required-unknown`, `server-required-unknown` |
| Client rejects omitted requirements or unsolicited selections | Missing-required, missing-echo and unsolicited-response cases |
| Response cannot escalate capabilities | Layer, window, timeout and serialization cases |
| Unsupported requirements fail before admission with error 0x0F | Raw QUIC probes verify the application close code and no stored entity |
| Optional unknown IDs are not activated; no repeated negotiation | Raw QUIC probes compare exact response bytes, then require duplicate-CAPABILITIES refusal |
| Application work waits for a valid response | Malformed-server probes against Java, C++, Rust one-entity and Rust recursive clients |
| Negotiation refusal is terminal | `rejected-then-valid` pipelines a second offer, PENDING and an Entity Stream; no stored entity after server exit |

The raw probes use Quinn as a transport and frozen message bodies, without
importing any PipeStream codec or state machine. Sealed-profile dependency
tests additionally require Layer 1 and exclude Layer 2. The subsequent Rust
authentication and recovery profiles are covered separately below; these
polyglot negotiation probes do not establish their interoperability.

## Sealed-work requirement coverage

The failed-descendant resolution increment adds Rust parity with Java's STRICT
failure behavior. The receiver commits verified child closure and failed parent
resolution atomically, echoes the digest and FAILED parent status, and does not
invoke rehydration. Tests cover a two-level Java producer across Rust and producer
restart, forged digests, lost response replay, zero rehydration callbacks,
completion-policy alternatives, and logical/physical completion capacity.
The 2026-09-06 full suite passed: 314 Rust and 193 Java tests, native SQLite/C++
checks, nine language pairs, 32 raw capability probes, recursive/recovery CLI
scenarios and three runnable examples. Strict clippy/rustdoc and the draft-04
build passed; idnits reports zero errors, flaws or warnings and the existing
FIPS reference comment. No wire field, CDDL or storage-format change is
introduced; Section 8 clarifies the existing failure semantics.

Section 9.8 uses private-use identifier 65281, `sealed-work-sets-v1`.
Rust and the separate Java sealed APIs implement tested parts of it. The Rust
public `connect_sealed` and `declare_work`
APIs require negotiation and exact ACK correlation. The producer label is
durable identity data, not a principal or credential.

| Profile requirement | Evidence |
|---|---|
| Deterministic bounded WORK_SET fields | 20 independent frozen inputs in `test-vectors/work-sets.tsv`; separate CDDL fixtures |
| Seal binds the complete set independently of batching | Fixed independently calculated SHA-256 and 1,024-ID, four-batch test |
| Missing declarations/payloads cannot disappear from completion | Core maximum-ID cut test and pending-checkpoint QUIC test |
| Child sets remain accountable through root completion | Out-of-order child payloads, scope closure, parent rehydration, root ACK test |
| Identity, sequence, and seal failures do not mutate declarations | Core state-equality and SQLite transaction rollback checks |
| Unobserved ACK can be replayed after restart | Public sealed client attaches to retained SQLite state and completes the original set |
| ACK must exactly match the request | Changed-owner and malformed-ACK tests check client error and actual QUIC close code |
| No unnegotiated declarations, undeclared admission, or early GOAWAY | Named-refusal wire tests and absent payload storage checks |
| Announcement budget and no STATUS cursor recycling | Bounded-window and cursor refusal wire tests |
| No mode conversion or unsafe old-format load | Legacy-session declaration refusal and version-1 row refusal without writes |

Payloads can arrive before a final seal, but only after their declaration
ACK. A missing or rejected declared payload stays outstanding; cancellation
tombstones and automatic retries are not implemented. The profile has a
single client producer and excludes Layer 2. It is not an authenticated
multi-tenant session or a claim-redemption protocol.

Stored session format changes from version 1 to version 2. Old records are
refused before deserialization, not converted. No running service or existing
application database was migrated. Keep old databases with their matching
binary. Tests cover declaration commit/reopen and lost-ACK replay, not every
payload/application-effect crash boundary.

The local aggregate budget is 1,000,000 declared IDs per session; each batch
is limited to 256 and per-scope negotiated limits still apply. The SQLite
adapter rewrites a serialized session per transaction, and sealing hashes
the complete ID set. No large-session throughput or resource-efficiency claim
is made. The local lineage digest now commits to the profile's producer and
scope seals under a distinct domain tag; it is still not an authenticated
content receipt or proof of correct computation.

## Authenticated-session binding prerequisite

The Rust service implements private-use `authenticated-session-v1` (65282),
defined in Section 10.6.4. It requires mutual TLS and explicit certificate-to-
principal mapping. The initial admission or declaration atomically binds the
session to a stable principal and issuing authority. Mutation transactions
and background recovery check that binding and durable session revocation.
An anonymous listener sharing the database cannot operate on protected work.
Clients configured with an identity require the extension and cannot silently
fall back to anonymous processing.

`quinn/tests/draft04/authenticated_sessions.rs` tests missing, untrusted,
expired, and unmapped credentials; downgrade refusal; cross-principal and
cross-authority access to a retained session; explicit certificate rotation;
live/reconnected revocation; and recovery authorization. Core tests reopen
owner/revocation records. These are transport/session-binding tests, not
evidence for retained redemption outcomes or an asynchronous executor.

This increment introduced format version 3. Version-1 and version-2 records are refused
without writes; no operational database was converted. Principal maps are
startup configuration, not a live authorization directory. At that landing,
claim revocation and retained recovery requests were still missing. The later
profile below adds them; portable recovery between unrelated authorities is
not claimed. Complete asynchronous resource guarantees remain goal requirements.
See [the full implementation plan](recovery-execution-java-plan.md).

## Durable execution publication fences

The Rust process, rehydrate, and resume paths acquire an execution lease in
a short transaction, invoke the application outside it, then atomically
publish the result and completion marker under the same fence. Publication
checks owner, revocation, session, operation, epoch, executor identity, and
expiry. Reacquiring expired work advances the epoch. Separate store handles
cannot acquire the same unexpired attempt. Callbacks may overlap after lease
expiry, so external effects still require application-level idempotency or
transactional fencing. Section 10.6.1 now distinguishes result publication
from stopping a stale callback.

Core tests cover simultaneous acquisitions, durable reopen and reacquisition,
stale/expired publication, clock and counter bounds, wrong owner/authority,
session substitution, revocation, and rollback of result plus completion.
QUIC tests exercise all three callbacks re-entering SQLite as writers,
publication refusal after a slow callback, and session revocation during a
callback. These are not yet complete asynchronous-executor crash tests.
An injected resume-callback panic also leaves an unfinished attempt across
SQLite reopen; recovery reacquires it after expiry under the next epoch.

This increment introduced format version 4. Earlier versions were refused
without conversion; no operational database was changed. At that point the
service lacked asynchronous dispatch, restartable job descriptors, periodic
recovery, and retained requests. Later increments below add those mechanisms.
An execution lease is neither a client credential nor a callback resource limit.
The fencing increment did not advertise a new wire capability.

## Retained authenticated recovery

Section 10.6.5 defines private-use `authenticated-recovery-v1` (65283), requiring
Layer 2 and authenticated-session negotiation. Rust's public `connect_recovery`,
`accept_recovery`, and `wait_recovery` APIs enforce request and full-receipt
correlation. This profile is separate from sealed work and cannot activate
legacy claim redemption or anonymous fallback on its connection.

One transaction commits claim redemption, a durable resume job, and the receipt.
Identical requests replay the same acceptance for 24 hours, even after initial
claim expiry; accepted jobs have their own fenced lifecycle. Terminal frames
distinguish successful completion from retained application refusal. Refusal
codes are diagnostic, never a substitute for the explicit outcome discriminator.
Claim revocation denies acceptance, replay, acquisition, and publication without
erasing unfinished work or undoing committed external effects.

Eleven core tests cover concurrent acceptance, abrupt process exit, reopen,
identity/owner/authority/expiry refusals, revocation fences, immutable outcomes,
the 1,024-receipt limit, queue rollback, and frozen wire bytes. Five real-QUIC
tests cover lost receipts and server restart, retained callback refusal without
retry, cross-owner/authority and missing-session denial, incompatible frames,
and malformed or mismatched receipts and terminal outcomes. Twenty independently
specified wire inputs and separate CDDL fixtures pin encoding and refusal rules.
The prior-format test now also refuses version 5 without modifying its state.

That increment introduced session format 6 for receipts and irreversible claim
revocation. The checkpoint correction below now requires format 7. Older formats
are refused, not converted; no operational database was migrated.
Expired receipt history is not evicted. Permanent storage accounting, explicit
and orphan reconciliation remain unfinished. These
Rust tests do not establish independent Java or C++ recovery interoperability.

## Incremental receive spools and allocation gate

The recursive service no longer buffers full entities or concatenates their
chunks in memory. A bounded header decoder precedes incremental file writes,
and FIN triggers length and SHA-256 validation before admission. Processors
receive a file-backed reader and return errors explicitly. Chunk assembly
uses ordered file segments, validating their original digests before computing
the combined digest. Final payload installation copies through an 8 KiB buffer
and syncs the file and directory chain.

Temporary byte/file limits apply to the connection, authenticated principal,
and store directory. Same-process handles share quotas, active principal
entries are bounded, empty files consume credit, and cancelled I/O retains
credit until its file operation finishes. Reopen counts abandoned files without
deleting them. The tests exercise these limits plus malformed early headers,
FIN length/checksum errors, spool corruption, and zero-byte chunk exhaustion
over actual QUIC.

The separate `spool_resources` test binary measures allocations during a 32 MiB
QUIC transfer with fixed-size sender blocks. It requires heap growth below
12 MiB, maximum individual allocation below 4 MiB, exact persisted SHA-256,
and zero temporary credit after completion. One focused local run measured
132,968 bytes of heap growth and a largest allocation of 15,972 bytes.
This does not measure native allocations, page cache, process RSS, or loaded
multi-principal behavior.

No stored-session format or wire schema changes in this increment. Temporary
spool quotas do not yet cover permanent payloads, SQLite state, or independent
writer processes. At that landing, dispatch and callbacks remained synchronous;
the queue and worker integration below add separate evidence. Durable storage
quotas and explicit orphan reconciliation remain unfinished.

## Durable job records and queue limits

Session format 5 adds typed input descriptors and retained execution outcomes.
Core job APIs acquire attempts under the existing authorization and expiry
checks and publish the outcome with the protocol result. Saves cannot discard
jobs or replace their inputs or terminal outcomes. A refusal retains unresolved
work instead of fabricating entity completion. Formats 1 through 4 are refused;
there is no stored-session conversion.

SQLite maintains an unfinished-job index in the same transaction as session
state and revision. Default limits are 128 jobs globally and 32 per authority
and principal, with one shared anonymous bucket. Limits are persisted and
cannot be changed by reopening another handle. Running jobs remain charged;
expired attempts become discoverable but still need fenced acquisition.
Revocation suppresses discovery without erasing work or releasing its charge.
An explicit integrity audit compares the index to checksummed session records.

Fifteen new core tests cover input identity, process/rehydrate/resume outcomes,
refusal without completion, ownership, revocation, concurrent admission,
rollback on overload and injected index failure, abrupt process exit and
reacquisition, immutable retained outcomes, corruption, and missing queue schema.
Those core storage tests alone were not evidence of responsive asynchronous
processing over QUIC. The integration below supplies separate execution evidence.

## Asynchronous worker integration

The transport service now installs immutable payloads before atomically
admitting their typed jobs. Chunk hashing and installation run in a bounded
admission pool. A periodic executor audits and reads the durable queue, acquires
fenced attempts, and reopens retained input with length/SHA-256 validation.
Connections observe committed outcomes instead of invoking application code.
Default physical limits are four execution workers and two per principal, plus
a separate admission pool with the same bounds. Handles for the same canonical
database share these limits within one process. Job observers are independently
bounded at 1,024 per connection.

The listener owns bounded connection tasks instead of detaching them. Cancellation
stops incomplete ingress while started blocking work retains its physical credit.
Pipelined roots wait for first admission within the receive/observation budgets.
Received but unadmitted entities block covered checkpoints; moving installation
to a worker cannot hide them from completion accounting.

Raw QUIC tests cover a fast job completing beside a held callback, checkpoint
deadline progress during process/rehydrate/resume callbacks, malformed control
refusal, queue overload with admitted jobs intact, and retained application
refusals. A child process exits abruptly after file/job admission; the parent
reopens the store and executes that retained input. Additional tests exercise
missing/corrupt input, detached rehydration/resume, shared physical permits, and
shutdown/replacement while an expired callback still occupies the sole slot.
The older panic recovery test now uses an interrupted durable dispatch; callback
panics are separately tested as retained refusals, not unbounded replay.

Shutdown stops dispatch and reports callbacks still active after its grace
period. It does not forcibly kill callbacks or remove admitted work. Expired
attempts may be reacquired, so external effects still need idempotency/fencing.
Permanent storage quotas, orphan reconciliation, and
broader multi-principal resource measurements remain required. The connection
storage isolation increment below moves metadata and lineage calls to workers;
physical worker/spool accounting does not cover independent writer processes.

## Retained-state quotas

Rust's `StorageLimits` bounds serialized bytes and retained-session counts
globally and per authority/principal. Completion and revocation do not free
these charges. State changes, their accounting entry, and the unfinished-job
index commit in one transaction. Reads validate accounting in the same snapshot
as the session, avoiding false corruption from concurrent commits. Serialization
refuses growth at the record cap; oversized stored blobs are not copied into
Rust before refusal. Existing unaccounted stores are refused, not converted.

Thirteen core tests cover byte/count exhaustion, anonymous and authority-qualified
budgets, concurrent admission and reads, exact rollback, abrupt exit, corrupted
or missing accounting/policy, invalid limits, and bounded serialization. Two
real-QUIC tests exercise both owner/global session limits and oversized work-set
updates; previously acknowledged declarations remain replayable after refusal
and restart. The wire format and CDDL are unchanged. Section 10.3 clarifies that
retained obligations do not become free capacity merely because work finishes.

This increment passed `./conformance/run_all.sh` with 153 Rust workspace tests
(79 core, 27 Quinn unit, 44 wire, one allocation gate, two runner tests), five
Java tests, the C++ vector test, nine transfer pairings, 32 capability probes,
and all external examples. Physical database/WAL and payload quotas, completion
headroom reservations and orphan reconciliation still
need implementation and measurement. A logical byte quota is not their proof.

## Rust SQLite file-length bounds

The bundled Unix SQLite backend now has separate immutable caps for database,
WAL, rollback-journal and shared-memory file lengths. Every store connection
uses the guard and a main-page limit. Writes, enlarging truncates and WAL-index
mappings check their caps before growth; size-hint preallocation, chunk rounding
and database mmap cannot bypass them. A checksummed 72-byte policy is synced
before database creation. Reopen cannot change it or convert a nonempty store
that lacks it. Unsupported backends and file aliases refuse explicitly.

Eleven core tests cover each actual file cap, size-hint/truncate bypass attempts,
policy corruption and changes, alias/legacy refusal, held-reader exhaustion,
job/session rollback, checkpoint busy reporting, abrupt-exit WAL recovery,
and concurrent sidecar creation/unlink during connection churn.
One real-QUIC test fills WAL under a read snapshot, requires the named capacity
refusal, and verifies retained declaration replay after reclamation. These are
cooperative-writer file-length bounds, not allocated filesystem blocks or
process memory. Completion-space reservations, Rust retained-payload accounting,
Java JDBC bounds, and orphan reconciliation remain due. Session format 7 and
the normative wire/CDDL are unchanged; no operational database was converted.

The final full suite passed with 176 Rust workspace tests, 89 Java tests
(no errors, failures or skips), C++, all nine Layer 0 pairings, 32 capability
probes, recursive/recovery CLI checks and all external examples. Draft -04
passed idnits with zero errors/flaws/warnings and one FIPS reference comment.

## Rust retained-payload reservations

The file store now enforces immutable global and authority/principal byte and
object policies for retained payloads and lineage, separate from temporary
spools and SQLite. Staging is reserved before copying and remains charged until
physical cleanup. Fixed checksummed metadata and receipts bind immutable input,
owner and publication. Same-process handles share accounting; an exclusive Unix
root lock prevents a second cooperating writer and survives store-handle drop
while readers, spool loans or object operations still hold it.

Eighteen focused tests cover retained and staging quotas, lineage, immutable replay,
cross-owner/authority refusal, incremental oversized-input rejection, process
exit and copy resumption, partial metadata and receipt publication, accounting
rollback, empty-directory bounds, alias refusal and unrelated work during a
held reader. Prefix-only metadata stays globally charged without inventing
an owner or blocking unrelated admitted work. Full corrupt metadata is refused,
not rewritten; these process-exit/image checks do not establish every power-loss
boundary. Reopen preserves unused canonical directories and counts them against
a fixed metadata budget. Orphan reconciliation is still explicit unfinished work.

A real-QUIC test reaches a principal's payload object limit, observes
`PIPESTREAM_LIMIT_EXCEEDED`, checks that the refused entity remains declared but
unadmitted, and completes another principal's work. Declaration replay remains
possible at capacity. The credential-refusal test now pins the two startup
policy/lock files and zero retained/spool usage rather than expecting an empty
root. Capability probes compare startup output byte-for-byte with no exempt
filenames; a regression test detects additions and mutations. No authentication
or negotiation refusal is weakened to allow payload installation.

The new local policy refuses nonempty unaccounted payload stores without
conversion. Session format 7, normative wire messages and CDDL are unchanged.
File-length reservations are not allocated-disk or total-memory bounds and do
not reserve every future completion publication. Java JDBC bounds, completion
reservations, orphan reconciliation, persistent producer observations and the
broader cross-language crash/resource matrix remain required.

The final `./conformance/run_all.sh` passed with 196 Rust workspace tests
(91 core, 51 Quinn unit, 50 wire, one allocation gate, three runner tests),
89 Java tests without errors/failures/skips, C++, all nine Layer 0 pairings,
32 capability probes, recursive/recovery CLI scenarios and every external
example. Draft -04 passed idnits with zero errors/flaws/warnings and one
informational FIPS reference comment. These are local validation results.

## Connection storage isolation

Connection metadata and lineage operations use eight physical slots per canonical
database and four per authority/principal, shared by handles in one process.
Cancellation keeps a started operation charged until it returns. A separate
control reader and deadline watchdog prevent storage or output waits from
postponing checkpoint clocks. The ingress backlog is bounded to 32 events;
overflow is a named refusal. Queued duplicate checkpoints remain charged and
cannot reset their initial deadline or lose their clocks to an earlier ACK.

Four raw QUIC tests hold SQLite and lineage writes while checking deadline and
protocol refusals, backlog/oversized-frame limits, and independent connection
completion. Five unit tests cover shared physical bounds, cancellation, and
checkpoint clock lifecycle. Existing recursive wire tests pin result-before-cut
ordering even when workers commit between separate storage snapshots. Sections
9.3 and 10.3 clarify storage-independent deadlines and cancellation-safe charges.
No wire encoding or stored-session format changes. Ordered state-dependent work
on one connection still waits for its storage operation; this is not a physical
disk quota or a concurrent-workload performance claim.

The full `./conformance/run_all.sh` passed with 162 Rust workspace tests
(79 core, 32 Quinn unit, 48 wire, one allocation gate, two runner tests), five
Java tests, the C++ test, nine transfer pairings, 32 capability probes, and
all external examples. Draft -04 passed idnits with zero errors, flaws, and
warnings, and one informational FIPS reference comment.

## Java SQLite file-length bounds

Every Java session-store connection now selects a bounded VFS inside Xerial's
bundled SQLite engine. The small C extension is packaged with the Java library;
the codec, state machine, listener, producer and executor remain independent
Java code. No Rust protocol/state code or second runtime SQLite is linked.
The build pins and verifies SQLite's public-header archive against SHA-256 and
refuses an incompatible runtime version. The current backend is 64-bit Linux.

An immutable checksummed 72-byte `PSJDB001` policy is synced before database
creation. Default caps are 256 MiB database, 64 MiB WAL, 64 MiB rollback journal
and 512 KiB shared memory. A main-page cap prevents WAL commits from outgrowing
the eventual main-file limit. Guarded writes, truncates and shared-memory maps
check growth; database mmap, size hints and chunk preallocation cannot bypass
the policy. A fixed native registry allows 64 concurrent database identities and
releases capacity after their final open file closes. The private registration
connection does not expose its SQL functions to normal store connections.

JDBC tests exhaust actual main/WAL/journal storage, preserve committed rows and
declarations after rollback, hold readers through busy checkpoints, reject policy
changes and oversized/aliased files, exhaust/reclaim the registry, use concurrent
handles and abruptly exit with uncheckpointed WAL. Direct native tests exercise
main/WAL/journal/shared-memory file methods, negative and overflowing offsets,
preallocation controls, and callback lifetime after the loading connection closes.
They also pass under address and undefined-behavior sanitizers. A real-QUIC test
receives `PIPESTREAM_LIMIT_EXCEEDED` at WAL capacity, retains exact acknowledged
membership with no accidental payload/job admission, and replays declarations
after checkpointing and reopen.

Nonempty unaccounted stores are refused before SQLite can change them; no
operational database was converted. Schema version 3, normative wire messages
and CDDL are unchanged. These are file-length caps for cooperating writers in
private local directories, not allocated filesystem bounds, Java principal
quotas or future completion-space reservations. Full producer resumption,
orphan reconciliation and broader crash/resource evidence remain required.

The final `./conformance/run_all.sh` passed with 196 Rust workspace tests
(91 core, 51 Quinn unit, 50 wire, one allocation gate, three runner tests),
99 Java tests without errors/failures/skips, the native SQLite test, C++, all
nine Layer 0 pairings, 32 capability probes, recursive/recovery CLI checks and
every external example. The native address/undefined-behavior sanitizer gate
and strict public Javadoc passed. Draft -04 passed idnits with zero errors,
flaws and warnings and one informational FIPS reference comment. These are
local validation results, not independent review or a full conformance claim.

## Java protected rehydration reservations

The version-4 Java store charges the exact future rehydration descriptor and its
bounded outcome allowance when admitting processing. A waiting parent's credit
survives disconnect, reopen and unrelated admissions. Child closure atomically
converts it to a queued rehydration job; STRICT failure instead releases unused
future credit. Terminal/refused retained records stay charged. `jobUsage()` audits
and reports both retained and reserved metadata and execution-slot counts.

Ordinary processing retains its 128-global/32-session queued/running limit.
Reserved and queued/running rehydration slots have a separate 65,536-global/
16,384-session bound, one per admitted entity, within the same combined 64 MiB/
16 MiB metadata budget. Waiting parents therefore do not prevent their children
from using ordinary slots. Physical worker limits stay four global/two session.
Discovery interleaves sessions in bounded pages and prioritizes rehydration within
each session; a large reserved queue cannot fill a page exclusively with its jobs.

Tests saturate both ordinary processing and retained metadata while preserving
the parent's conversion credit, inject a closure-write failure, pin the exact
descriptor delta for large metadata and maximum identifiers, reopen after abrupt
process exit, and refuse version-3 stores without writes. Thirty-two waiting
parents still admit children, and bounded discovery includes another session.
A real-QUIC test fills ordinary processing, closes the child scope into reserved
rehydration, releases blocked callbacks and receives the full root checkpoint ACK.

Schema version 4 changes local admission semantics, not wire/CDDL. No operational
store was converted. Section 10.3 clarifies that reserved completion credit cannot
be borrowed by unrelated admissions. These are logical metadata/queue guarantees;
physical publication headroom, orphan reconciliation,
persistent producer observations and the full crash/resource matrix remain due.

The final `./conformance/run_all.sh` passed with 196 Rust workspace tests,
104 Java tests without errors/failures/skips, native SQLite and C++ tests, all
nine Layer 0 pairings, 32 capability probes, recursive/recovery CLI checks and
every external example. Changed public APIs passed strict Javadoc. Draft -04
passed idnits with zero errors/flaws/warnings and one FIPS reference comment.
These are local results, not hosted CI or independent implementer review.

## Validation

### Rust admitted-job publication reservations

Storage policy version 2 charges the possible serialized result and execution
growth of admitted jobs before dispatch. The calculation covers processing,
rehydration, resume, full bounded refusals, output digests and attempt fields.
Layer 2 processing reserves an explicitly configured token budget (64 KiB by
default) and bounded claim validation; the callback sees the smaller of this policy
and the usable STATUS frame limit in advance, or zero without Layer 2.
Oversized results are retained named refusals, not truncated tokens or completion.
All store writes protect actual plus reserved bytes under the existing global,
principal and record caps. Reopen checks the derived reservation against its
checksummed accounting. Existing version-1 storage policies refuse without
conversion; the session payload remains version 7 and the wire/CDDL is unchanged.

Ten core tests pin serializer-derived growth, exact-quota publication for every
current job stage/outcome, claim and map-prefix boundaries, rollback, corrupt
accounting, concurrent principal admission, revocation and abrupt exit. Two
authenticated QUIC tests cover a held yield publishing after unrelated work
fills the store, exact callback budgets, an oversized-result refusal with no
claim, and successful authenticated recovery for an in-budget token, including
the exact frame boundary and one byte over. Section
10.3 requires application-result budgets to be exposed and enforced before commit.
Physical publication headroom, final lineage
reservations, orphan reconciliation and broader resource/crash evidence remain due.

The final `./conformance/run_all.sh` passed with 208 Rust workspace tests,
104 Java tests without errors/failures/skips, native SQLite and C++ tests, all
nine Layer 0 pairings, 32 capability probes, recursive/recovery CLI checks and
every external example. Strict workspace clippy and touched-file rustfmt passed.
Draft -04 passed idnits with zero errors/flaws/warnings and one informational
FIPS reference comment. These are local results, not independent implementer
review, hosted CI or proof that the full goal is complete.

### Rust future-rehydration reservations

Queue policy version 2 separates ordinary processing/resume capacity from
reserved/active rehydration. Defaults are 128 global/32 authority-principal
ordinary jobs and 65,536 global/16,384 authority-principal rehydration slots.
PROCESS admission protects its possible rehydration; waiting parents keep
that slot without blocking admission of their own children. Worker limits
remain unchanged. `job_queue_usage()` audits and distinguishes ordinary,
future and active counts; future reservations are never executable jobs.

Storage policy version 3 protects the future descriptor, maximum outcome and
attempt, parent output and child scope-close digest. Postcard size accounting
covers job/attempt map prefixes collectively. Closure converts these charges
and its queue slot atomically. New descendants, payloads and checkpoint requests
still need ordinary admission capacity. Mutations audit queue rows against
checksummed session state before using free capacity; changed/missing reservation
rows cannot create capacity for another session. This bounded full scan and
principal-interleaved discovery are not constant-time or global fairness claims.
Old queue/storage policies are refused without conversion; session payload
version 7 and the wire/CDDL are unchanged.

Ten core tests exercise exact-quota closure/publication, large identifiers and
map-prefix boundaries, independent/concurrent principals, revocation, admission
rollback, corrupted reservations, policy compatibility and abrupt process exit
before and after conversion. A real sealed-work QUIC test keeps ordinary
processing full with a held callback while a parent rehydrates, then verifies
the complete root checkpoint and GOAWAY. Physical DB/WAL publication space,
orphan reclamation, persistent producer observations,
and the remaining crash/resource/conformance matrix are still open.

The final `./conformance/run_all.sh` passed with 219 Rust workspace tests
(111 core, 51 Quinn unit, 53 wire, one allocation gate, three runner tests),
104 Java tests without errors/failures/skips, native SQLite and C++ tests,
all nine Layer 0 pairings, 32 capability probes, recursive/recovery CLI checks
and every external example. Workspace formatting/clippy and strict Rustdoc
passed. Draft -04 passed idnits with zero errors/flaws/warnings and one FIPS
reference comment. These are local results, not hosted CI or proof of full
goal completion.

## Rust final-lineage file reservations

`FileEntityStore` now durably protects 1,120 bytes and one retained object per
session before payload installation can succeed. The charge covers a 512-byte
session/authority/principal marker plus final metadata, digest, receipt and
publication staging. A marker reserves capacity only: it contains no guessed
digest and is not protocol admission or completion evidence. Final publication
uses this allowance instead of ordinary staging credit, but still needs one
bounded active storage operation. Its complete charge survives publication.

Partial markers stay globally charged until matching replay binds a durable
owner. A refused owner-quota promotion restores the original unattributed
charge. Partial final metadata and receipts use the same prepaid allowance.
Full markers are checked on payload load and lineage publication; missing or
changed markers cannot free capacity. The `PSRET002` policy refuses prior
policies and unreserved retained payloads without migration. Session payload
format 7, SQLite storage/queue policies and normative wire/CDDL are unchanged.

Tests cover exact retained/staging limits, concurrent owner admission, immutable
replay, metadata/receipt prefixes, failed marker creation, abrupt process exit,
invalid descriptors and policy refusal. Authenticated QUIC checks saturate the
retained budget while two callbacks are held, then compare published lineage
bytes with actual session digests and complete checkpoint/GOAWAY. A separate
missing declared payload remains unadmitted and times out without a successful
checkpoint or final lineage.

The first full-suite run exposed a Java reset-test race: Netty's stream `close()`
sends FIN, so shutdown could race valid payload installation/admission. The
test now explicitly sends RESET_STREAM through the error-code shutdown overload,
waits for the receiver's refusal, and checks zero admission before restart.
A separate FIN test verifies legal completion; no server refusal is weakened.

This closes configured final-lineage file-quota headroom, not physical allocation
or DB/WAL completion-space reservation. Failed admissions can leave conservative
reservations requiring explicit orphan reconciliation. Persistent producer
observations, broader tenant/resource stress and the full independent Java
conformance matrix remain required.

The final `./conformance/run_all.sh` passed with 231 Rust workspace tests
(111 core, 62 Quinn unit, 54 wire, one allocation gate, three runner tests),
105 Java tests without errors/failures/skips, native SQLite and C++ tests,
all nine Layer 0 pairings, 32 capability probes, recursive/recovery CLI checks
and all external examples. The corrected Java FIN/reset cases also passed five
consecutive focused runs. Workspace formatting/clippy and strict Rustdoc passed.
Draft -04 passed idnits with zero errors/flaws/warnings and one FIPS reference
comment. These are local results, not independent review or full-goal completion.

## Rust incremental queue and accounting indexes

This records the prerequisite landing at PR #27. The subsequent fixed-image
layout below replaces selective deletion with in-place retirement.

Session mutations no longer delete and rebuild every queue row and the accounting
row. Reconciliation leaves unchanged values untouched, preserves row IDs on
updates, and deletes obsolete entries before admitting replacements. Global and
principal quotas are evaluated against other sessions plus the proposed state.
Full pre-write integrity audits remain mandatory; missing or corrupt entries
cannot be silently repaired into apparent free capacity. The same transaction
still commits session revision, state, queue and checksummed charges together.
Session payload 7, storage policy 3, queue policy 2 and normative wire/CDDL remain
unchanged. No operational database was converted.

Six tests in `persistence::index_delta_tests` check no-op saves, one-row acquisition,
selective completion, revocation, reopen, replacement at full quota, new-row
insertion failure and rollback after selective deletion/accounting failure.
The earlier acquisition, rehydration-conversion and accounting fault tests now
inject failures on the actual UPDATE path; new INSERT failure coverage remains.
The focused persistence run passed all 63 tests.

An isolated index reconciliation experiment pins a WAL reader and compares the
same retained state with forced full index replacement. At 1, 128 and 512 jobs,
unchanged reconciliation adds zero WAL bytes; full replacement adds 28,872,
61,832 and 144,232 bytes in the local bundled-SQLite run. This is index-only
evidence. A public save still updates its revision and rewrites the whole session
blob, and audits still scan bounded retained state. These changes remove
unrelated index writes as a prerequisite to a useful publication-space bound;
they do not implement physical DB/WAL reservations or prove service throughput.

The full `./conformance/run_all.sh` passed with 237 Rust workspace tests
(117 core, 62 Quinn unit, 54 wire, one allocation gate, three runner tests),
105 Java tests without errors/failures/skips, native SQLite and C++ checks,
all nine Layer 0 pairings, 32 capability probes, recursive/recovery CLI checks
and every external example. Workspace formatting/clippy and strict Rustdoc
passed. Draft -04 passed idnits with zero errors/flaws/warnings and one FIPS
reference comment. These are local results, not hosted CI or full-goal completion.

## Rust physical completion reservations

The Rust store now allocates actual state plus protected logical growth in a
`PSIMG001` session image with a 104-byte checksummed header and verified zero
padding. Mutable dispatch and accounting occupy fixed 32- and 56-byte images
with immutable SQL keys. Admission preallocates future rehydration rows; conversion
and retirement update them in place. Allocated capacity remains charged even when
unused logical credit is released. Queue policy 3, storage policy 4 and physical
policy `PSDBL002` refuse old layouts without conversion. Session payload 7 and
normative wire/CDDL are unchanged; no operational database was migrated.

Each queued job funds acquisition and publication, each running job retains
publication, and a possible rehydration funds conversion/acquisition/publication.
Under SQLite's actual writer transaction, every public mutation derives all
remaining credit before writing. Its main and WAL handles share a per-connection
VFS ceiling through commit or rollback. Unrelated writes cannot consume that
credit, expired lease renewal does not release it, and enlarging a session must
fund its existing jobs' higher future cost. The usable WAL limit also accounts
for the configured WAL-index shared-memory capacity.

The production bound is tied to bundled SQLite 3.53.2: zero reserved bytes per
page, supported power-of-two page sizes, at most 64 KiB sectors, whole-image and
changed-index pages, frame headers, commit-frame repetition and sector padding.
It is not derived just from serialized byte growth. See the implementation in
`persistence/physical/reservation.rs` and SQLite's
[incremental BLOB contract](https://www.sqlite.org/c3ref/blob_open.html) and
[WAL checkpoint behavior](https://www.sqlite.org/wal.html).

The original 256 KiB WAL fixture now explicitly refuses admission because it
cannot fund five future maximum-image stages. The 1 MiB acceptance case instead
admits work, pins a reader, exhausts unrelated writes, and requires publication
without releasing that reader. Other tests cover queued work for two principals,
full 64 KiB token publication at 512-/4,096-/65,536-byte pages, concurrent admission,
expired reacquisition, future rehydration, authenticated resume/receipt replay,
and abrupt exit after admission. The real-QUIC test
`storage_quotas::authenticated_callback_publishes_while_unrelated_writes_saturate_reserved_wal`
checks the held callback's full-budget publication at saturation.

`complete_stage_bound_covers_spilling_acquisition_refusal_and_token_publication`
measures 144 whole transactions across 72 cases: a two-page cache with spilling,
three page sizes, eight token budgets from 127 bytes through 8 MiB, and complete,
full-diagnostic refused and maximum-field deferred outcomes. It pins readers,
sets the main-page cap to the existing page count, forbids SQL row replacement,
and requires unchanged row identity/capacity plus exact persisted state. Every
acquisition/publication must fit the actual production stage bound.

Five additional regression tests refuse malformed dispatch flags/padding/timestamps,
oversized metadata and invalid session IDs before unbounded materialization.
Missing or changed owned tables/indexes refuse on reopen; schema, database and
WAL bytes remain unchanged in those tests. Corrupt rows cannot look like available
job or storage credit. Existing rollback tests inject SQLite's real refusal of
writable indexed BLOBs; UPDATE/DELETE triggers alone cannot test that path.

These reservations protect configured file-length headroom for cooperating
writers, not filesystem-block allocation, arbitrary external writers, callback
effects or unknown future descendants. Whole-session serialization and integrity
audits remain. Discovery includes retained retired rows; the scan grows with
history and is not constant-time. Large sessions with many pending stages can
reach the WAL reservation limit before logical queue limits. Java physical
completion reservations, orphan reconciliation, persistent producer observations
and the remaining full-goal evidence are still due.

The final `./conformance/run_all.sh` passed with 258 Rust workspace tests
(137 core, 62 Quinn unit, 55 wire, one allocation gate and three runner tests),
105 Java tests without errors/failures/skips, native SQLite/C++ checks, all nine
Layer 0 pairings, 32 capability probes, recursive/recovery CLI checks and all
external examples. Workspace formatting/clippy and strict Rustdoc passed.
Draft -04 passed idnits with zero errors/flaws/warnings and one informational
FIPS reference comment. These are local results, not hosted CI or full-goal
completion.

### Independent Java sealed-work foundation

Java now has separate deterministic CBOR, WORK_SET, and SCOPE_DIGEST codecs
plus SQLite declaration, admission, result, and child-closure storage. It
consumes the frozen work-set and scope-digest inputs without importing Rust
protocol or state code. Strict unsigned decoding and exact ACK correlation
avoid numeric coercion or changed-request replay. Declaration checksums and
membership checks precede replay or completion decisions.

The state machine waits for sealed membership, missing declared payloads,
terminal statuses, and all descendant closures. STRICT rehydration requires
every child to succeed; a failed child instead fails its parent. Scope summaries
commit only to direct identifiers and statuses. Tests exercise nested scopes
with out-of-order arrival, WAL reopen, abrupt exit after declaration/closure,
concurrent handles, quota exhaustion, corruption, and transactional rollback.

At that landing, the Java public network client and listener remained Layer 0.
The foundation did not itself add network behavior, payload storage or executor
fencing, or establish Java/Rust interoperability. The producer increment below
adds one-direction transport evidence. Logical declaration quotas are not
physical storage bounds. Remaining work is explicit in the
[Java README](../../implementations/java-netty/README.md).

The complete `./conformance/run_all.sh` passed with 162 Rust workspace tests,
30 Java tests (25 new), the C++ test, all nine existing transfer pairings,
32 capability probes, and all external examples. New Java APIs also passed
Javadoc with `-Xdoclint:all -Werror`. Draft -04 passed idnits with zero errors,
flaws, and warnings, and one informational FIPS reference comment.

### Independent Java sealed producer

The public Netty `SealedClient` requires the sealed profile and validates the
selected limits. Its independent codecs preserve uint64 counters and optional
checkpoint fields. File and chunk sends use 8 KiB buffers; local membership,
checkpoint history, and the response backlog have explicit limits. Operations
are blocking and serialized. They do not persist producer observations, retry
payloads, or provide a measured whole-process memory bound.

Five real-QUIC tests exercise Java-to-Rust nested and out-of-order chunked work,
scoped cuts, replay after server restarts, and a declaration ACK discarded at
the transport boundary. Named refusals cover changed producer labels, retained
limits, missing seals, and incorrect cuts. Scripted fault-injection peers test
changed ACK fields, downgrade, oversized frames, and forbidden Layer 2 responses;
they are not a replacement for an independent Java reference server.

This testing exposed Rust's rejection of explicit root checkpoint scope zero
and its loss of that optional field during ACK construction. Section 9.3 now
clarifies the existing CDDL, and `optional-fields.tsv` supplies a shared valid
input. Stored-session format 7 preserves scope presence across SQLite reopen
and rejects changed replays without mutating the retained checkpoint. Formats
1 through 6 are refused without conversion. This changes no wire schema and
migrates no operational database.

The Java sealed listener, durable payload/chunk storage integration, responsive
pending checkpoints, and Rust-to-Java tests remain required. The existing nine
Layer 0 pairings do not supply that missing evidence.

The full-suite recovery CLI exposed a separate Rust shutdown race. A focused
isolated-runtime test failed when the yield client stopped its runtime before
QUIC sent connection close. `begin-yield` now uses the public asynchronous
`disconnect_gracefully` API to drain its endpoint. The test verifies a clean
server exit while the retained entity stays DEFERRED and its claim unredeemed;
transport shutdown is not treated as successful work completion.

The final `./conformance/run_all.sh` passed with 164 Rust workspace tests,
40 Java tests with no skips (including five real-QUIC sealed tests), C++, the
nine existing Layer 0 pairings, 32 capability probes, recursive/recovery CLI
checks, and all external examples. New Java public APIs passed Javadoc with
`-Xdoclint:all -Werror`. Draft -04 passed idnits with zero errors, flaws, and
warnings and one informational FIPS reference comment.

### Independent Java payload store

`SealedPayloadStore` receives into quota-charged files, checks FIN length and
checksum, validates complete chunk geometry, and installs immutable input
before session admission. Publication reserves both staging and final-name
capacity and syncs file and directory state. Retained metadata is bounded and
checksummed; readers verify the complete payload without whole-entity buffering.
Identical replay remains possible without fresh publication headroom. Changed
payloads, layouts, and persistent policy are refused without conversion.

Tests cover concurrent installation, cancellation while an installer holds a
receipt, concurrent reader close, corrupt input, quota refusal, and abandoned
file accounting. A subprocess exits after installing a payload but before
admission; reopening never turns that orphan into admitted or completed work.
Cross-process tests also cover a rejected same-process duplicate open: a
process-local guard prevents a second lock-channel close from releasing the
first writer's OS lock. The supported boundary is one cooperative local
filesystem writer and one loaded library copy per store in that process.

The 32 MiB receive/install/read test passes under a 24 MiB Java maximum heap.
This is a storage-library allocation guard, not a QUIC, RSS, native-memory, or
concurrency measurement. Quotas count logical file lengths and file names,
including staging and object headers; filesystem blocks, SQLite/WAL,
per-principal accounting, orphan reconciliation, and server integration remain
outside this increment. No wire schema, Rust session format, or Java SQLite
format changed. The Java listener still advertises only Layer 0.

The full `./conformance/run_all.sh` passed with 164 Rust workspace tests,
55 Java tests with no skips (15 new payload tests), C++, all nine existing
Layer 0 pairings, 32 capability probes, recursive/recovery CLI checks, and
every external example. The new Java API passed Javadoc with
`-Xdoclint:all -Werror`. Draft -04 passed idnits with zero errors, flaws, and
warnings and one informational FIPS reference comment.

### Java durable execution for server integration

Java database format 2 adds managed entities and a typed, checksummed durable
job queue. Admission and processing dispatch commit together; child closure,
parent resolution, and rehydration dispatch share another transaction. Queue
overflow rolls back both the work transition and dispatch. Version-1 Java
stores are refused without conversion. Existing manual lifecycle calls cannot
bypass fences on managed work.

`SealedExecutor` invokes callbacks outside storage transactions using reopened,
verified file-backed inputs. Global/per-session worker limits and per-job
physical exclusion survive lease expiry and shutdown until callbacks actually
return. Durable epochs and expiry prevent stale publication after restart.
Refusals are retained separately from entity success/failure; even a zero
diagnostic refusal does not make an entity complete. Missing or corrupt job
records prevent completion checks.

Tests cover transaction rollback, global/per-session capacity, retained metadata
charges after completion, recursive and failed-child closure, cross-handle lease
acquisition, abrupt exit, stale publication, callback re-entry, independent work
during a stall, corrupt input, and executor shutdown/restart. A 32 MiB input also
executes through the worker under a 24 MiB Java heap cap. This does not establish
QUIC control responsiveness, native-memory/RSS bounds, or tenant isolation.
The Java sealed listener and Rust-to-Java interoperability remain unfinished.
Logical descriptor/outcome reservations are not physical SQLite/WAL quotas or
reservations for every future child or rehydration job.

The final `./conformance/run_all.sh` passed with 164 Rust workspace tests,
74 Java tests with no skips (19 new execution/storage tests), C++, all nine
existing Layer 0 pairings, 32 capability probes, recursive/recovery CLI checks,
and all external examples. Changed/new public Java APIs passed Javadoc with
`-Xdoclint:all -Werror`. Draft -04 passed idnits with zero errors, flaws, and
warnings and one informational FIPS reference comment.

### Java sealed listener and reverse interoperability

The independent public `SealedServer` connects the Java codec, SQLite store,
payload library, and executor without importing Rust protocol/state code.
`SealedServerTest` covers actual-QUIC recursive/chunked completion, held-SQLite
deadline enforcement and immediate refusal, independent completion during a
stalled callback, duplicate clocks, storage backlog overflow, STRICT child
failure, forged-digest rollback, reset streams, and unobserved ACK replay after
server restart. The `sealed-interop` profile runs the Rust public producer's
`sealed-scenario` against this server, including reconnect replay and named
changed-owner/checkpoint refusals. This complements the earlier Java-to-Rust
tests rather than replacing them with shared protocol code or scripted peers.

Java database format 3 retains checkpoint request identity and ACK state, with
ownership-bound checksums and aggregate history accounting. Missing rows or
changed ACK bits cannot remove pending obligations. `SealedCheckpointStoreTest`
covers unsigned/optional-field correlation, nested readiness across reopen,
corruption, atomic history-write rollback, retained capacity, and refusal of
the old format without conversion. The Rust state format and wire schema are
unchanged. The existing Java standalone commands remain Layer 0.

The listener uses bounded file, metadata, application, and cleanup workers.
No application callback or file/SQLite operation runs on its event loop.
Outstanding payloads, results, and nested checkpoints prevent a completion
ACK; timeout remains authoritative even if a storage operation later commits.
A 32 MiB real-QUIC receive/install/execute gate passes with a 24 MiB Java heap.
These tests do not establish native-memory/RSS, physical disk/WAL, concurrent
tenant bounds, completion-space reservations, or full producer recovery.
The complete profile requirement matrix remains due.

The final `./conformance/run_all.sh` passed with 164 Rust workspace tests,
89 Java tests with no skips (nine server and six checkpoint-store tests), C++,
all nine Layer 0 pairings, 32 capability probes, recursive/recovery CLI checks,
and every external example. Changed public Java APIs passed Javadoc with
`-Xdoclint:all -Werror`. Draft -04 passed idnits with zero errors, flaws, and
warnings and one informational FIPS reference comment. These are local gates,
not hosted CI, independent implementer review, or an IETF conformance claim.

### Java durable producer observations and service identity

The opt-in `SealedClient.connectDurable` now persists request intents before
network writes and validated observations before reporting them. Its independent
SQLite journal restores declaration membership, immutable file/chunk commitments,
recursive parent observations and checkpoint identities. It retains original
unacknowledged requests and uncertain input identities instead of assuming a lost
response means no admission. It never blindly resends payloads. Reconnect replays
the original root declaration and requires a fresh root-checkpoint ACK before
GOAWAY; acknowledged shutdown prevents further use of that journal.

The local binding fixes the exact CA PEM bytes, configured service name, ALPN and
sealed profile, not the server IP/port or a producer credential. Both Java clients
now independently check the actual server certificate's DNS/IP SAN after chain
validation and before opening the application control stream. A real-QUIC wrong
DNS-name test failed against the earlier Java client and passes with the fix.
Additional tests cover DNS wildcard depth, IPv4/IPv6, an IP written in a DNS SAN,
Common-Name-only certificates and ASCII validation before Unicode case folding.

Focused validation passed 35 tests with no failures, errors or skips: journal and
input-descriptor tests, name-matching tests, public Java client/server sequences,
and Java-to-Rust/fault-injection sequences. Coverage includes three abrupt producer
exits, recursive restart, dropped/changed declaration ACKs, interrupted rehydration,
checkpoint timeout before a later seal, and quota refusal before payload admission.
A 32 MiB actual-QUIC transfer, journaling and observation restore passes in a JVM
with a 24 MiB heap and no duplicate execution. Strict Javadoc passes for all five
production types in this increment. These are component/heap tests, not RSS, native-memory
or concurrent-load measurements. The full integrated suite passed with 297 Rust
workspace tests and 192 Java tests, zero Java failures/errors/skips from JUnit XML,
native SQLite/C++ checks, all nine implementation pairings, 32 capability probes,
recursive/recovery CLI checks and all three external examples. Draft -04 passed
idnits with zero errors, flaws or warnings and one FIPS reference comment. These
are local gates, not hosted CI or a complete-goal conformance claim.

There is still no retained input-outcome query in this sealed profile. Remembered
observations are not a substitute for that exchange, authenticated recovery or
exactly-once effects. The broader resource and bidirectional adversarial test
matrix remains part of the goal. Wire/CDDL and frozen frame bytes are unchanged;
Section 10 now references RFC 9525 for service identity.

### Rust recovery exchange ownership and cancellation

The public Rust client now keeps one accepted receipt until its correlated terminal
outcome is consumed. Unrelated operations refuse locally before network writes,
and a wrong caller-supplied receipt cannot consume the pending response. Legacy
redemption is also refused locally on the authenticated-recovery profile. A real
QUIC regression failed before this change because a second recovery receipt was
accepted while the first outcome remained pending.

Cancelling `accept_recovery` or `wait_recovery` during exchange I/O now closes the
connection through an owned guard. An incompletely read frame cannot become the
next operation's response. This does not cancel an admitted server job or report
successful shutdown. Actual malformed or mismatched responses retain their named
protocol refusal codes. Request persistence remains the embedding application's
responsibility; this is not a Rust durable-client journal or general cancellation
safety for all recursive-client methods.

Eight focused retained-recovery tests pass, including six injected partial-response
boundaries and an authenticated real-service test that holds a resume callback,
cancels the client wait, verifies retained unfinished work, then replays completion
after server restart with a rotated certificate for the same principal. Callback
and attempt counts remain one. Strict workspace Rustdoc passes with warnings
denied. The full suite passed with 300 Rust workspace tests and 192 Java tests,
zero Java failures/errors/skips from JUnit XML, native SQLite/C++ checks, all nine
implementation pairings, 32 capability probes, recursive/recovery CLI checks and
all three external examples. Draft -04 passed idnits with zero errors, flaws or
warnings and one FIPS reference comment. These are local gates, not hosted CI or
full-goal completion. Wire/CDDL and storage formats are unchanged. Appendix E now distinguishes the implemented
retained recovery profile from legacy redemption and the still-separate sealed
input-outcome lookup gap.

### Reproducible suite and earlier landings

`./conformance/run_all.sh` runs Rust formatting, Clippy with warnings denied,
the workspace tests, Java tests, C++ CTest, both Rust example suites, the
frozen-vector and CDDL checks, all nine client/server pairings, both recursive
and recovery CLI scenarios, and all three cross-language applications.
The first landing passed locally on 2026-09-05 with 45 Rust workspace tests.
The follow-up passed the complete command locally on 2026-09-05 with
46 Rust workspace tests, 35 shared extension cases, and 32 raw QUIC
capability probes. All three language suites, nine transfer pairings,
recursive/recovery scenarios, and external examples passed. The conformance
runner also builds separately from the rest of the Cargo workspace.

The sealed-work landing passed the complete command locally on 2026-09-05
with 63 Rust workspace tests (including nine sealed-work core tests and
eight sealed-work QUIC tests), 20 new frozen work-set inputs, the Java and
C++ suites, all nine transfer pairings, 32 capability probes, and every
recursive/recovery scenario and external example.

The authenticated-session increment passed the full command locally on
2026-09-05 with 71 Rust workspace tests, including six mutual-TLS wire tests,
the owner/revocation store test, and principal-map validation. Java/C++ tests,
all nine transfer pairings, 32 capability probes, and every example passed.
The authentication path also disables TLS resumption to require a fresh
credential check on each connection. This does not establish the remaining
goal's recovery, asynchronous execution, or Java sealed-work requirements.

The execution-fencing increment passed the complete command locally on
2026-09-05 with 81 Rust workspace tests, Java/C++ tests, all nine transfer
pairings, 32 capability probes, and every external example. The new tests
cover acquisition/publication, callback re-entry and expiry, revocation while
processing, and an interrupted resume callback recovered after lease expiry.

The spooling increment passed the full command locally on 2026-09-05 with
92 Rust workspace tests, including seven spool-state tests, three new QUIC
refusal tests, and the isolated 32 MiB allocation gate. Java/C++ tests, all
nine transfer pairings, 32 capability probes, and the external examples passed.
The first full run caught stale example lockfiles after `tempfile` became a
runtime dependency; both were regenerated without changing any package version,
then the entire locked suite passed. Draft -04 again passed idnits with zero
errors/flaws/warnings and one informational FIPS 180-4 comment.

The adversarial pass also fixed terminal failure handling: Java completes
both public waiters and ignores callbacks after the first failure; C++
will not process buffered frames or entities after failure; Rust drains
its endpoint before a one-shot server exits following a refusal. The raw
probes require the actual close code and inspect storage after server exit.

The durable-queue increment passed the final full command locally on 2026-09-05
with 107 Rust workspace tests (55 core, 20 Quinn unit, 29 wire, one allocation
gate, and two conformance-runner tests), Java/C++ suites, nine transfer pairings,
32 capability probes, and all external examples. Draft -04 again passed idnits
with zero errors/flaws/warnings and one informational FIPS 180-4 comment.

The asynchronous-worker increment passed the full command locally on 2026-09-05
with 122 Rust workspace tests (55 core, 27 Quinn unit, 37 wire, one allocation
gate, two runner tests), five Java tests, the C++ vector test, all nine transfer
pairings, 32 capability probes, and all external examples. New tests cover
listener cancellation, retained execution, pipelined first admission, and
checkpoint progress during payload installation as well as application callbacks.
Draft -04 passed idnits with zero errors/flaws/warnings and one FIPS comment.

The retained-recovery increment passed the complete command locally on
2026-09-05 with 138 Rust workspace tests (66 core, 27 Quinn unit, 42 wire,
one allocation gate, two runner tests), five Java tests, the C++ vector test,
all nine transfer pairings, 32 capability probes, and every external example.
The new recovery frames have 20 frozen wire cases and separate CDDL fixtures.
The build script now checks the idnits summary as well as its exit status:
idnits can exit zero while reporting a document error.

`./build.sh core 04` produced XML, text, and HTML. idnits reported zero errors,
zero flaws, zero warnings, and one informational possible-downref comment
for the NIST FIPS 180-4 normative reference. This is document validation,
not distributed-protocol validation. The build now refuses a filename
revision that differs from the source XML docName.

These are local results, not hosted CI or independent implementer review.
No Python implementation, protocol oracle, or example was added. The external
xml2rfc renderer remains confined to document authoring.

## Still required before a conformance or deployment claim

1. Complete the mandatory behavior matrix and independent interoperability
   evidence. Rust's explicit retained-recovery profile is implemented, but the
   current Layer 2 boolean still advertises more than the prototype implements.
2. Complete the asynchronous executor's storage/resource evidence, including
   further crash-boundary tests and concurrent-workload resource gates. Rust and
   Java now provide durable quotas and explicit offline orphan reconciliation;
   that does not establish whole-process or multi-tenant bounds.
3. Complete the Java profile's requirement matrix, persistent producer
   observations, and expanded cross-language crash/reconnect/resource coverage.
   Its separate Netty server now has recursive and replay interoperability.
   C++ follows as the third implementation; the full requirement matrix
   remains necessary for every implementation.

Additional gaps include Layer 0 dehydration, arbitrary bidirectional work
origination, child-before-parent admission buffering, automatic retry/timer
policies, authorization tests, crash-boundary coverage, and measured resource
and performance gates. The standalone durable service supports optional mutual
TLS but still lacks the resource and execution guarantees required for an
untrusted multi-tenant service.

SQLite transactions serialize lease acquisition and result publication through
one database, including separate store handles. Application callbacks no longer
hold the write transaction. Expired callbacks are fenced from protocol
publication, not forcibly stopped. These records do not guarantee exactly-once
external effects or fence unrelated databases.

## Submission and adoption

See [the draft-04 checklist](../../advocacy/SUBMISSION-CHECKLIST-04.md).
A published draft, a merged repository branch, working-group adoption, IESG
approval, and IANA registration are separate milestones. Document checks
and language count do not establish adoption, approval, or registration.
