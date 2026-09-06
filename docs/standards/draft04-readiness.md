# Draft-04 readiness and implementation evidence

Updated 2026-09-05. This landing starts from `00531de` and addresses the
[protocol review](../ai-slop/ietf-protocol-review-2026-09.md).
The specification remains an individual Internet-Draft, not an approved
standard. No implementation in this repository demonstrates full conformance.

## Changes and regression coverage

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
final-lineage headroom, orphan reclamation, persistent producer observations,
and the remaining crash/resource/conformance matrix are still open.

The final `./conformance/run_all.sh` passed with 219 Rust workspace tests
(111 core, 51 Quinn unit, 53 wire, one allocation gate, three runner tests),
104 Java tests without errors/failures/skips, native SQLite and C++ tests,
all nine Layer 0 pairings, 32 capability probes, recursive/recovery CLI checks
and every external example. Workspace formatting/clippy and strict Rustdoc
passed. Draft -04 passed idnits with zero errors/flaws/warnings and one FIPS
reference comment. These are local results, not hosted CI or proof of full
goal completion.

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
2. Complete the asynchronous executor's storage/resource guarantees: durable
   quotas, orphan reconciliation, further crash-boundary
   tests, and concurrent-workload resource gates.
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
