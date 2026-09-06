# Recovery, execution, and independent Java implementation

Goal: authenticated recovery, bounded asynchronous execution, and an
independent Java implementation of the sealed-work profile. All three must
be implemented and verified before this goal is complete. This file tracks
the full goal, not a substitute for the normative draft.

## Acceptance requirements

1. Authenticated recovery
   - Verify client certificates and map them to explicit stable principals.
   - Bind durable sessions and their claims to principal and issuing authority.
     Check ownership and revocation before admission, lookup, and execution.
     No anonymous fallback, ownership conversion, or metadata impersonation.
   - Negotiate a precisely specified recovery capability; authenticate and
     correlate recovery requests, including retained outcomes after lost ACKs.
   - Enforce expiry, revocation, retention, request identity, and immutable
     outcomes across reconnects and server restarts.
   - Use durable executor fencing. A stale executor cannot publish completion;
     external effects require application idempotency or transactional fencing.
   - Test missing/untrusted credentials, cross-principal and cross-authority
     attacks, revocation, expiry, concurrent recovery, lost ACKs, and crashes.
2. Bounded asynchronous execution
   - Spool payloads incrementally with bounded memory, disk, metadata, streams,
     chunk assemblies, queued jobs, and per-principal/global concurrency.
   - Keep control parsing, deadlines, and other work responsive during slow
     payload reception and application processing. No callbacks under database
     transactions or unbounded task spawning.
   - Persist dispatch and completion state around fenced asynchronous workers.
     Define cancellation, overload, restart, and shutdown behavior without
     silently omitting declared work or claiming exactly-once external effects.
   - Measure resource bounds and test slow/stalled consumers, queue exhaustion,
     independent job completion, deadline progress, and persistence boundaries.
3. Independent Java sealed work
   - Implement the Section 9.8 codec, durable state machine, Netty server, and
     public client in Java, without importing Rust protocol/state code.
   - Cover declarations, seals, identity, descendant closure, scoped cuts,
     replay, named refusals, and persistence. No one-entity substitute.
   - Run Java-to-Rust and Rust-to-Java positive and adversarial sequences over
     real QUIC, including lost ACK/reconnect and recursive completion.

## Verification and publication

Every implementation increment needs focused tests and the full applicable
suite. Update normative text, CDDL, frozen vectors, and implementation-status
claims together. Keep Python out of implementations, examples, and conformance.
Build draft -04 and inspect idnits separately from protocol validation.
Publish reviewed commits through Forgejo first; verify the GitHub mirror.
Do not submit a draft, deploy, migrate an operational database, or claim IETF
approval as part of a repository landing.

## Evidence and progress

### Validated increment: Rust recovery exchange ownership and cancellation

Branch `fix/recovery-client-exchanges` starts at PR #34's merge `55100c6`.
An actual-QUIC regression demonstrated that the Rust public client could accept
another recovery receipt while the previous outcome remained unconsumed. The
client now retains the exact pending receipt and refuses unrelated operations
before any network write. A wrong caller-supplied receipt cannot consume the
pending response. A verified terminal frame releases the obligation; ordinary
disconnect never does so on behalf of server work.

A recovery exchange guard closes the connection when its future is cancelled or
fails partway through I/O. Partial receipt/outcome framing cannot carry into another
operation. Cancellation requests normal transport disconnection, not a successful
GOAWAY or a fabricated protocol violation. Actual peer framing/correlation failures
retain their named refusal codes. This is scoped to the two recovery methods, not
a claim that every recursive-client method is cancellation-safe or that the Rust
client persists requests for its caller.

Focused tests cover six interrupted response boundaries, pending-operation refusals,
caller-receipt mismatch, repeated successful recovery and the existing authorization,
revocation, malformed-response and retained-refusal scenarios. An authenticated
real-service test persists the request before transmission, holds the resume
callback, cancels the client's outcome wait and verifies unfinished work remains.
After publication and server restart, a rotated credential for the same principal
retrieves the identical receipt and completion without a second callback or attempt.
All eight focused retained-recovery tests passed. The full
`./conformance/run_all.sh` passed with 300 Rust workspace tests and 192 Java tests,
zero Java failures/errors/skips counted from JUnit XML, native SQLite/C++ checks,
all nine implementation pairings, 32 capability probes, recursive/recovery CLI
checks and all three external examples. Strict workspace Rustdoc passed with
warnings denied. Draft -04 passed idnits with zero errors, flaws or warnings and
one FIPS reference comment. These are local gates, not hosted CI or completion of
the full goal. Wire/CDDL and storage formats are unchanged. Appendix E is corrected
to acknowledge the existing retained recovery
profile while preserving the separate sealed-work outcome-lookup limitation.

A follow-up resource audit identified a concrete remaining client bound:
`RecursiveClient::read_entity_statuses` appends every received status to a vector
until a terminal state, without a count limit. The frame-size cap does not bound
that accumulated history. Add a named refusal and adversarial real-QUIC test for
this path before treating the client side of the resource requirement as verified.

### Validated increment: Java durable producer observations

Branch `feat/java-producer-observations` starts at PR #33's merge `80f38d6`.
The package-private `SealedProducerJournal` now stores immutable request intents
separately from verified observations. Each intent allocates its complete bounded
observation image before it can be sent; observation updates replace that image
without increasing the logical reservation. A protected append frontier detects
missing records, including a deleted last row, rather than reusing their identities.
Identical request replay retains the same slot; changed requests and changed final
observations refuse. An intent without verified evidence remains unresolved.

The journal has its own strict SQLite schema, immutable peer-context digest and
storage limits, and checksummed row/observation bindings. It refuses server databases
and foreign schemas without conversion. A single cooperating writer owns the
journal through a process lock and one exclusive SQLite connection. The existing
guarded VFS bounds database and sidecar lengths; this journal uses DELETE rollback
journals, not a WAL shared with server execution. Startup audits one bounded request
at a time. These are local persistence primitives, not authentication credentials,
server execution state, or a whole-process memory bound.

The focused tests exercise restart, abrupt process exit, hot-journal rollback,
logical and physical exhaustion, immutable replay, stale observations, corrupted
records/frontiers, cross-record and cross-journal substitution, foreign schemas,
and same-JVM/separate-JVM writer exclusion. The writer tests exposed a POSIX lock
hazard: opening then closing a second descriptor could drop the owner's process
lock. A canonical-path ownership registry now refuses same-JVM opens before they
open another lock descriptor; the subprocess test verifies the lock remains held.

Foundation validation: all 12 focused tests pass. Final `./conformance/run_all.sh`
passed with 297 Rust workspace tests and 170 Java tests, with no Java failures,
errors or skips counted from JUnit XML, native SQLite/C++ checks, all nine
implementation pairings, 32 capability probes, recursive/recovery CLI checks and
all three external examples. Strict package-level Javadoc for the new type passed
with `-Xdoclint:all -Werror`. Draft -04 passed idnits with zero errors, flaws or
warnings and one FIPS reference comment. Those foundation wire tests covered
existing transport behavior, before the durable-client integration below.

The public `SealedClient.connectDurable` now uses the journal. Typed declaration,
file commitment, scope, checkpoint and shutdown records precede their network
effects. Validated observations are persisted before returning them. Restore checks
request/response identity, membership, recursive outcomes and checkpoint cuts. A
checkpoint ACK retained in an earlier row is checked after rebuilding membership,
because its request can precede a later seal. Scope ACK replay cannot erase an
earlier REHYDRATING observation. Inventory APIs expose uncertain inputs, original
unacknowledged declarations/checkpoints and unfinished closures, including after
disconnect. They do not infer remote outcomes or permit blind payload resends.

Durable inputs gain mandatory measured length/SHA-256 commitments before their
descriptor is journaled; caller headers and source files are not modified. Fixed
buffers hash and stream the files in separate passes. The journal stores neither
payload bytes nor paths. Reopen audits descriptors without reading source bodies.
Its peer-context binding covers the exact CA PEM bytes, configured service name,
ALPN and sealed profile. Address/port changes are allowed, trust/name changes are
refused. This is not a producer credential or a retained server-store identity.

The actual-QUIC negative test discovered that the existing Java clients trusted a
CA without checking the certificate's target name. Both Layer 0 and sealed clients
now wait for successful chain validation and verify DNS/IP SAN identity before
creating their application stream. Common Names are never a fallback. ASCII-only
DNS validation precedes case folding, so Unicode fold characters cannot manufacture
a match. Section 10 now explicitly requires RFC 9525 service identity validation;
wire frames, CDDL and frozen wire bytes are unchanged. Java tests cover valid DNS,
single-label wildcards, IPv4 and IPv6, wrong names/IPs, a misleading DNS SAN for an
IP target, and a CA-valid Common-Name-only certificate. These are Java transport
checks, not proof of a complete three-language certificate matrix.

Integration evidence: 35 focused tests passed, including the 12 journal tests,
four input-descriptor tests, three DNS-matching tests, Java public-client/server
scenarios and two Java-to-Rust/fault-injection scenarios. They cover three abrupt
producer exits, recursive observations across server restarts, lost/changed ACKs,
an interrupted rehydration wait, checkpoint timeout before a later seal, pre-send
quota refusal and uncertain input without retry. A 32 MiB transfer plus producer
journaling and reconnect runs under a 24 MiB Java heap without a second execution;
this does not establish RSS/native-memory or concurrent-load bounds. Strict package
Javadoc passed for all five production Java types in this increment. The final integrated
`./conformance/run_all.sh` passed with 297 Rust workspace tests and 192 Java tests,
zero Java failures/errors/skips counted from JUnit XML, native SQLite/C++ checks,
all nine implementation pairings, 32 capability probes, recursive/recovery CLI
checks and all three external examples. Draft -04 passed idnits with zero errors,
flaws or warnings and one FIPS reference comment. These are local validation
results, not hosted CI or proof that the complete goal has been achieved.

Two current transport constraints must survive integration: Java's sealed server
refuses PENDING/payload replay for previously admitted IDs, and declaration replay
does not subscribe to their retained processing outcomes. A missing observation
therefore cannot authorize a blind resend. Also, GOAWAY needs an acknowledged root
checkpoint on its current connection; remembering an older ACK does not replace
the exact checkpoint exchange after reconnecting.

### Validated increment: Rust offline orphan reconciliation

Branch `feat/rust-orphan-reconciliation` starts from PR #32's merge `7906d3c`.
The file backend now requires exclusive root ownership and the matched database's
writer lock throughout audit and reclamation. The core maintenance cursor audits
one bounded session at a time, including finished/refused input descriptors;
caller-managed admission refuses rather than treating a missing job as an orphan.
All admitted payloads remain protected in every state.

Unadmitted `.meta` records are durably renamed to `.commit` before removing bodies,
receipts or stages. Immutable identity, owner, length and digest survive; matching
retransmission restores the original installation using the same metadata record.
No temporary metadata headroom is needed at a full retained quota. Remaining
files stay charged through interrupted cleanup. Partial metadata, object identities
and final-lineage reservations are retained, not silently reclaimed. No lifecycle
state, admission, checkpoint or completion is fabricated. `PSRET004` refuses prior
file policies without conversion; database policy and normative wire are unchanged.

Focused tests cover retained quota, live-handle and writer exclusion, corrupt and
missing input, owner/pair mismatch, concurrent/interrupted restoration, I/O failure
and four process-exit phases. The real-QUIC sealed scenario verifies pending timeout,
changed chunk refusal and matching out-of-order restoration through checkpoint and
GOAWAY. The isolated 32 MiB resource gate measured 13,048 bytes additional Rust heap
and a largest allocation of 2,216 bytes across installation/reclamation/restoration;
both gates are below 1 MiB. This does not measure native SQLite memory or RSS.
Standalone spool handles also hold the retained root's process lock; a subprocess
test verifies exclusion. Rejected full-length staging bytes can be reclaimed
without weakening their retained expected digest or published-body corruption checks.
All 21 new focused tests pass (five core maintenance, fifteen Quinn storage and
one actual-QUIC test). Final `./conformance/run_all.sh` passed locally with 297
Rust workspace tests (150 core, 86 Quinn unit, 56 wire, one allocation gate and
four runner tests), 158 Java tests without failures/errors/skips counted from
JUnit XML, native SQLite/C++ checks, nine executable pairings, 32 capability
probes, recursive/recovery CLI checks and all three external examples.
Formatting, strict clippy and strict workspace Rustdoc passed. Draft -04 passed
idnits with zero errors/flaws/warnings and one FIPS reference comment.

The first full run exposed a new wire fixture assuming a checkpoint request must
persist before its connection deadline. Under load, the existing control deadline
can fire first. The test now checks both valid outcomes: no acknowledged cut,
unchanged declared work and matching input required after reconnect. A subsequent
complete run passed before final review added the rejected-full-stage regression;
the full suite was rerun successfully after that change. Persistent producer
observations and the remaining resource/interoperability matrix remain required.
No hosted CI, operational cleanup/migration, deployment, release or draft
submission is claimed.

### Validated increment: Rust database/payload pairing before orphan reconciliation

Branch `feat/rust-payload-store-binding` starts from PR #31's merge `b7509f5`.
Rust lacked a durable database/root association. Reclamation cannot safely use an
arbitrary database's absence of jobs as evidence that another root's bodies are
unadmitted. Service startup now requires explicit durable pairing through the
`EntityStore` contract, with no default no-op for custom backends.

A checked singleton database image retains its identity and once-assigned payload
identity. The file root records its own identity and syncs the complete pair before
the database claim. Complete interrupted claims replay; wrong pairs, missing bound
claims, partial/corrupt images and stale database identities refuse. The database
write preserves every unchanged job's physical completion allowance under the
writer transaction. New `PSDBL003`/`PSRET003` policies refuse older layouts without
conversion; session format 7 and wire/CDDL are unchanged. This is a pairing
prerequisite, not implementation of Rust reclamation or proof of payload admission.

Seventeen focused tests pass, including concurrent claims, actual BLOB-write
failure, held-writer ordering, process exit and completion after WAL saturation.
The real authenticated-recovery restart test additionally pins the retained pair.
The first full-conformance run found an existing corruption-test
setup calling the public checkpoint after deliberately damaging the root schema;
the new early identity/schema check now refuses that call. The fixture uses its
already-held diagnostic connection for snapshot preparation while retaining its
no-repair assertions. A second run reached the wire tests and found their old
two-file unauthorized-client inventory; it now explicitly includes the startup
identity/claim files and asserts that rejected clients cannot change the pair.
All 20 authenticated-session/recovery wire tests pass in a focused run, and
strict workspace Rustdoc passes. Rust reclamation, persistent producer observations and the
remaining resource/interoperability acceptance criteria remain required.

Final `./conformance/run_all.sh` passed locally: 276 Rust workspace tests
(145 core, 71 Quinn unit, 55 wire, one allocation gate and four runner tests),
158 Java tests with zero failures/errors/skips counted from JUnit XML, native
SQLite/C++ checks, nine executable pairings, 32 capability probes,
recursive/recovery CLI checks and all three external examples. Formatting and
strict clippy passed, as did strict workspace Rustdoc. Draft -04 passed idnits
with zero errors/flaws/warnings and one FIPS reference comment. No hosted CI,
operational migration, cleanup, deployment, release or draft submission is claimed.

### Validated increment: Java offline orphan reconciliation

Branch `feat/java-orphan-reconciliation` starts from PR #30's merge `c18dcda`.
The public offline `SealedPayloadStore.reconcile` requires a closed, previously
paired payload root and holds SQLite's writer lock while auditing and reclaiming.
Every managed input and immutable object is checked before deleting any file.
Caller-managed admitted input, corrupt records, wrong pairs, missing admitted
payloads and unknown filesystem entries refuse; no inferred ownership is allowed.

Abandoned spool/install names are removed. Unadmitted bodies are atomically renamed
to commitment records before truncation, preserving encoded headers, identity,
length and digest. Identical retransmission can restore input without changing the
original publication quota; changed replay refuses. Admitted bodies remain retained
in every state, including completion and refusal. No lifecycle row changes and no
missing entity completes. Partial filesystem reclamation survives an I/O failure
or process exit and can resume explicitly without relying on SQLite rollback to
undo file operations. Payload policy 3 refuses older policies without conversion;
database schema 6, normative wire/CDDL and frozen vectors are unchanged.

The 12 focused tests pass and cover full quota, immutable and chunked
replay, concurrent restoration, older-policy refusal, corrupt input/job records,
writer exclusion, I/O failure and process exit at four actual filesystem phases.
A 32 MiB body is reclaimed/restored under a 24 MiB Java heap. The real-QUIC test
checks missing-work timeout, changed replay refusal and matching completion after
offline reclamation. These do not prove native-memory/RSS or multi-tenant bounds.
Rust orphan reconciliation, persistent producer observations and the remaining
resource/interoperability acceptance matrix are still required by the full goal.

Final `./conformance/run_all.sh` passed locally: 158 Java tests with no
failures/errors/skips (counted from JUnit XML), all 259 Rust workspace tests,
native SQLite/C++ checks, nine executable transfer pairings, 32 capability probes,
recursive/recovery CLI checks and all three external examples. Formatting and
strict clippy passed. Draft -04 builds with zero idnits errors/flaws/warnings and
one FIPS reference comment. The changed payload API passes strict Javadoc, and
whole-module structural Javadoc passes; strict whole-module Javadoc
still reports 100 missing-comment warnings in the same four unchanged Layer 0
types. This is not hosted CI, a deployment, migration, release or draft submission.

### Validated increment: Java payload/database ownership before orphan reconciliation

Branch `feat/java-payload-store-binding` starts from PR #29's merge `52d2717`.
Managed admission previously trusted the metadata in a `Stored` handle after its
store closed, and an executor accepted handles from another payload root. Admission
now revalidates the retained file and holds the store open through its transaction;
executors reject foreign handles before admission. Schema 6 and payload policy 2
retain separate persistent identities and require one matching database/root pair.
A synced file claim precedes the database claim. Both are ownership records, not
admission evidence, and complete interrupted claims can replay without changing the
pair. Corrupt or partial markers refuse without automatic deletion or conversion.

The 14 focused tests pass: pair/reopen, both wrong-store directions, failed
startup ownership release, foreign/stale input handles, missing/corrupt payloads,
actual BLOB-write rollback with retained file claim, competing roots, a held SQLite
writer with a pinned admission, corrupt-marker reopen, older-format refusal,
abrupt process exit between claims, missing bound claims and corrupt images.
The first full Java/interoperability run passed 143 tests before the final three
defensive tests and retry-sync correction. The next repository run passed all 146
Java tests, the Rust workspace and nine transfer pairings, then failed the C++
invalid-capability-response probe with empty stderr. A traced diagnostic rerun
passed all 32 probes, so the original failure was intermittent and did not retain
enough exit information to identify its exact cause.

Inspection found two concrete C++ shutdown lifetime errors: the caller's failure
path could reuse a connection already closed by its callback, and stack completion
state could die before registration teardown drained callbacks. Failure shutdown
now uses the still-owned registration, and completion state outlives runtime
teardown in both client and server. The capability oracle now rejects signal
termination even with a named stderr line, and reports the case, exit status,
stdout and stderr on failure. A runner unit test covers those refusal diagnostics.
All 32 capability probes then passed three consecutive focused runs without
increasing deadlines or weakening the required refusal. Structural Javadoc
(`all,-missing`) passes; strict Javadoc still reports 100 missing-comment warnings
in four unchanged Layer 0 types. Draft -04 builds with zero idnits errors, flaws
or warnings and one FIPS reference comment.
Final `./conformance/run_all.sh` passed: 146 Java tests with zero failures/errors/skips
(independently counted from JUnit XML), 259 Rust workspace tests, native SQLite/C++
checks, all nine transfer pairings, 32 capability probes, recursive/recovery CLI
checks and all three external examples. Workspace formatting and strict clippy
passed in that run. These are local results, not hosted CI, a deployment or a
complete-goal claim. Explicit orphan reclamation, persistent producer observations
and the full resource/interoperability goal remain unfinished.

### Validated increment: Java physical completion reservations

Branch `feat/java-completion-reservations` starts from the Rust reservation
merge `991b779`. The public-store regression
`admittedPublicationSurvivesUnrelatedWalExhaustionWithReaderPinned` admits and
acquires a real job, pins a WAL reader, and submits unrelated declarations until
the configured 512 KiB WAL refuses another write. The original run committed
21 declarations and reached 523,296 bytes before publication failed. The test
still requires successful publication with that reader pinned; it is neither
disabled nor changed to accept a refusal.

The current Java edit adds a storage-only fixed-BLOB helper and per-connection
WAL ceiling to the existing JDBC extension. The image helper requires an
explicit writer transaction, exact bounded capacity and direct invocation;
bootstrap management remains private. A main handle shares its atomic ceiling
only with its own WAL, including a WAL opened later. Native tests now obtain
actual SQLite journal handles instead of constructing filenames without the
pager association required by `sqlite3_database_file_object`.

Real JDBC evidence ruled out generated columns: SQLite 3.53.4 refuses writable
BLOB handles on such tables. The Java job table instead stores a fixed 256-byte
image and immutable SQL keys/input. Java encodes and checks the image, including
padding, identity-bound checksum and lifecycle fields. Ready queries project
state and expiry without storing a second copy or indexing mutable image bytes.
The schema policy is version 5 and refuses earlier policies without conversion.
Tests verify changed image bytes with unchanged row IDs, rollback after an
actual BLOB-write refusal, malformed images, and expiry ordering across byte
boundaries and the maximum signed Java counter.

Focused storage tests passed 15 cases, and the native address/undefined-behavior
sanitizer gate passed. The subsequent job regression run passed 20 tests and
failed only the original missing-publication-space case. The full Java run
`mvn -B test -Psealed-interop` then ran 117 tests, with 116 passing, the same
publication-space error and no skipped tests. This includes real Rust
interoperability; the full repository conformance command has not been rerun
for this unfinished increment. These are local WIP results, not a completed
reservation guarantee or a full-suite pass.

The subsequent edit allocates 112-byte entity and 128-byte scope-closure images
at declaration, with Java codecs and identity-bound checksums. A presence flag
distinguishes an absent closure from the 77-byte committed summary. Fixed-page
tests execute recursive state changes at 512-, 4,096- and 65,536-byte page sizes
without allocating main pages. Corruption and invalid lifecycle tests include
independently recomputed checksums to exercise semantic validation separately
from checksum rejection.

Every managed PROCESS admission now allocates a second, explicit RESERVED
rehydration row. Its input has the original descriptor and 85 checked zero bytes;
it is not a runnable job or a fabricated child result. Closure converts that
input, hash and state in place. Unneeded futures become RETIRED without deleting
their rows or releasing their allocated byte charges. Pair validation binds
future input to processing input and, after conversion, to the actual child
closure. Missing or corrupt reserved rows refuse lookup, acquisition and
completion. Tests pin row identity, image length, absence of SQL row mutation,
full-queue conversion and rollback at the actual BLOB-write failure. The large
metadata test now audits the allocated bytes of both rows, including retired
ones, and proves quota exhaustion within one smallest tested pair instead of
assuming that an unused future disappeared from storage.

The next full Java/interoperability run executed 123 tests, with 122 passing,
the original publication-space error and no skips. This is still an unfinished
increment and not a full reservation guarantee.

Admission funding is now implemented. Every public write transaction audits its
remaining stages under `BEGIN IMMEDIATE` and installs a connection-local ceiling
before mutation; validated stages release their allowances and a final audit
checks the resulting credit before commit. The Java-specific model covers
each fixed-image write set, spill/commit repetition, sector padding and the
WAL-index capacity. Renewal cannot consume publication credit. `PSJDB002`
refuses previous file policies without conversion.

The original 512 KiB publication test passed without raising its limit. The
first funded full Java/interoperability run passed all 123 tests without skips.
Seven subsequent reservation tests passed, including 54 cost scenarios covering
378 complete transactions across three page sizes, two cache sizes, three
metadata lengths and three rehydration outcomes. Recursive pinned-reader tests
cover successful conversion, STRICT retirement, reopen and shared-memory-first
exhaustion. A failed-conversion test observes an actual uncommitted WAL tail,
then verifies unchanged reservations and successful retry with the same reader
pinned. Native address/undefined-behavior sanitizer tests also passed.
The first repository-wide conformance attempt exposed a QUIC queue-test timeout.
The listener had acquired a separate writer transaction for each observation,
repeating the new reservation audits. Observer lookups now share one bounded
batch, and read-only operations use enforced query-only snapshots instead of
competing for SQLite's writer lock. Tests additionally pin batch order, ownership,
absence/refusal, and observation progress while another writer holds its lock.
The test peer now reports connection/receive failures instead of only a generic
timeout. No timeout or reply-queue limit was increased.
The corrected QUIC queue test passed five consecutive focused runs with unchanged
timeouts and reply limits. Final `./conformance/run_all.sh` then passed: 258 Rust
workspace tests (137 core, 62 Quinn, 55 wire, one allocation gate, three runner),
132 Java tests with zero failures/errors/skips, native SQLite and C++ checks,
all nine transport pairings, 32 capability probes, recursive/recovery CLI checks
and all three external examples. JUnit XML independently confirms the Java
counts. These are local validation results, not hosted CI or full-goal completion.
See [the derivation](java-completion-reservations.md).
Strict Javadoc reported pre-existing missing-comment
warnings in the unchanged Layer 0 APIs; `all,-missing` structural doclint passed.
The draft -04 build passed with zero idnits errors/flaws/warnings and one FIPS
reference comment. Keep the larger goal's orphan reconciliation,
persistent producer observations and resource/interoperability requirements intact.

### Validated increment: Rust physical publication reservations

Branch `feat/sqlite-completion-reservations` starts from the PR #27 merge.
The new `unrelated_writes_cannot_spend_an_admitted_jobs_publication_space`
test reproduces the missing guarantee: with a WAL reader pinned, unrelated saves
exhaust the file cap and an already-acquired processing job cannot publish.
Admission enforcement now makes it pass; the final cross-language validation
and cost audit are recorded below.

The current edit changes the session table to a fixed-capacity SQLite BLOB image.
Admission allocates actual serialized bytes plus protected logical result growth;
updates within that capacity use incremental BLOB I/O, not SQL row replacement.
The checksummed header binds session identity, payload version, revision, timestamp,
logical length, capacity and state checksum. Padding is zero and verified, never a
placeholder outcome. The session payload remains version 7; the outer image uses
`PSIMG001`, and `PSDBL002` refuses previous SQLite file policies without conversion.
Four image tests cover in-place acquisition/publication at a fixed database page
cap, corruption, growth rollback, and WAL extent with cache spilling at 512-,
4,096- and 65,536-byte page sizes. Whole-transaction coverage was added separately
as recorded below; these image-only tests do not establish that bound themselves.
The initial local core run passed 121 tests and failed only the new competing-write
reservation test. That negative result preceded admission enforcement; it is not
the current state of the branch.

Mutable dispatch and accounting now use 32- and 56-byte images with immutable
SQL keys. Future rehydration slots are allocated with processing and become
active or retired in place. Retired entries are not runnable and do not consume
unfinished-job quota. Allocated session capacity remains charged after unused
logical growth is released. Queue policy 3 and storage policy 4 refuse older
layouts. SQL triggers do not intercept incremental BLOB writes; rollback tests
now inject real SQLite refusal with an expression index that prevents opening
the target column for writing. Byte comparisons verify changed images and retained
row identities, separately from SQL insert/update/delete auditing.

The new reservation model counts queued acquisition/publication, running
publication, and future rehydration conversion/acquisition/publication. Renewal
of an expired running attempt cannot spend its publication credit. Each remaining
stage funds the allocated session image, two dispatch images, an accounting image,
the possible final commit-frame repeat and 64 KiB sector padding. Enlarging a
session also funds the higher cost of its old jobs. Under SQLite's actual writer
transaction, each public mutation computes the next state's aggregate reserve
before any image write. A per-connection VFS ceiling protects that reserve through
commit or rollback. Main and WAL handles share that connection's ceiling, not a
process-global value that could race another writer's commit. The usable WAL
extent is also constrained by its WAL-index shared-memory policy. Reopening an
existing queue/accounting policy no longer recreates indexes or writes policy rows.

The original 256 KiB Layer 2 fixture cannot fund five future maximum-image writes
under this model and is now an explicit admission-refusal case. The saturation
acceptance case uses a 1 MiB WAL, then fills ordinary writes to refusal as before.
Large index-only tests explicitly configure enough WAL/index capacity for their
hundreds of retained jobs; production defaults and admission checks are unchanged.
These are real capacity costs of serializing the entire session, not a throughput
claim or an assumption that the default WAL can fund arbitrarily large work sets.

Current tests cover saturated publication, two owners' queued acquisition and
full 64 KiB token publication at 512-, 4,096- and 65,536-byte pages, concurrent
admission, expired reacquisition, future rehydration, authenticated resume with
retained receipt replay, and abrupt process exit after admission into a pinned WAL.
A new authenticated real-QUIC test releases a held callback after unrelated writes
exhaust their ceiling and verifies full-token publication while the reader stays
pinned. The corrected full Rust workspace run passes 131 core, 62 Quinn unit,
55 wire, one allocation and three runner tests. The full `conformance/run_all.sh`
run passed with those 252 Rust tests, 105 Java tests without errors/failures/skips,
native SQLite/C++ checks, all nine language pairings, 32 raw capability probes,
recursive/recovery CLI checks, and all examples. Workspace formatting and strict
clippy passed in that run. After the subsequent defensive changes, the final
`./conformance/run_all.sh` passed with 258 Rust workspace tests (137 core,
62 Quinn unit, 55 wire, one allocation gate and three runner tests), 105 Java
tests without errors/failures/skips, native SQLite/C++ checks, all nine pairings,
32 capability probes, recursive/recovery CLI checks and every external example.
Workspace formatting/clippy and strict Rustdoc passed. Draft -04 passed idnits
with zero errors/flaws/warnings and one informational FIPS reference comment.
These are local results, not hosted CI or proof of full-goal completion.

The completed whole-transaction cost audit measures 144 acquisition/publication
transactions across 72 cases with a two-page cache and spilling enabled. Three
page sizes, eight token budgets through 8 MiB, and complete/full-diagnostic
refused/maximum-field deferred outcomes all fit the production stage bound.
The test fixes the database page cap at its current count, forbids SQL image
replacement, pins row identity and capacity, and verifies the exact retained
state after each transaction. This includes mutable dispatch and accounting,
not just the session image. The default physical policy is unchanged.

Five defensive regression tests now cover corrupt fixed-field discriminants,
timestamps and padding; oversized dispatch/accounting metadata; invalid session
identities; and missing/changed owned schema. Discovery checks shapes before
decoding identities; exact audit comparisons remain inside SQLite. Retained
session IDs are length-gated before materialization. Schema refusal verifies
unchanged schema, main database and WAL bytes instead of silently rebuilding
indexes. README, Rust README, readiness and Appendix D now describe allocated
capacity, retained-row scan costs and the pinned SQLite boundary.

Publication follows exact-diff and live-remote review, then a Forgejo commit/PR
and merge with automatic GitHub-mirror verification. Git history and live remote
refs, not this validation entry, are the authority for publication status.

Filesystem block allocation, arbitrary external writers and unbounded callback
effects remain outside the existing cooperating-writer file-length contract.
No operational database has been migrated, and this increment is not a release.

### Landed increments

- Starting point: `ed1a468`, clean main on 2026-09-05. Section 9.8 is implemented
  only in Rust. Durable sessions are not caller-authenticated, callbacks are
  synchronous, and payloads are whole-entity buffered.
- Landed through Forgejo PR #7 at `53e2a7c`, also verified on GitHub main:
  required mutual-TLS session negotiation and durable
  principal/authority binding, including denial through unprotected listeners.
  Ownership and session revocation are checked inside mutation transactions
  and on background recovery. Focused wire tests exercise credentials,
  downgrade refusal, rotation, cross-owner/authority access, and revocation.
  Private-use extension 65282 (`authenticated-session-v1`) prevents anonymous
  downgrade. TLS resumption is disabled on this path. Session format 3 retains
  ownership; formats 1 and 2 are refused without conversion.
  `./conformance/run_all.sh` passed with 71 Rust workspace tests, all language
  suites, nine pairings, 32 capability probes, and the examples. Draft -04
  builds with zero idnits errors/flaws/warnings and one FIPS 180-4 comment.
- Execution increment landed through Forgejo PR #8 at `7ca85f5`, also verified
  on GitHub main: process, rehydrate, and resume callbacks now run outside
  database transactions. Session format 4 retains per-operation execution
  epochs, executor identities, expiry, and completion records. Acquisition and
  publication are separate transactions with owner/revocation checks. Expired
  and superseded attempts cannot publish. Focused tests cover separate store
  handles, reopen/reacquisition, transactional rollback, stale clocks/fences,
  callback re-entry, overlong callbacks, and revocation during a QUIC callback.
  An interrupted resume callback is reacquired after expiry and SQLite reopen.
  The full suite passed with 81 Rust workspace tests, Java/C++ tests, all nine
  transfer pairings, 32 capability probes, and the external examples. Draft -04
  again passed idnits with zero errors/flaws/warnings and one FIPS 180-4 comment.
- Spooling increment: headers are decoded before payload reception, and each
  body is incrementally written to quota-charged temporary files with an 8 KiB
  I/O bound. Chunk assembly retains original segments and checks their stored
  digests. Processing consumes a file-backed reader. Temporary bytes, files,
  and active principal budgets are bounded across connections and same-process
  store handles. Cancellation retains credit until outstanding I/O finishes;
  abandoned files are counted on reopen, not silently deleted.
  The real-QUIC 32 MiB transfer allocation gate passed: 132,968 bytes of heap
  growth and largest allocation 15,972 bytes in one focused local run.
  `./conformance/run_all.sh` then passed with 92 Rust workspace tests, Java/C++
  tests, nine transfer pairings, 32 capability probes, and all external examples.
  The Rust examples' lockfiles now include the existing pinned `tempfile`
  dependency on the library's runtime path; no package versions changed.
  Draft -04 passed idnits with zero errors/flaws/warnings and one FIPS comment.
- Durable queue increment: session format 5 adds typed process/rehydrate/resume
  descriptors, queued/running/finished/refused states, immutable inputs and
  terminal outcomes, and fenced result publication. SQLite indexes unfinished
  jobs in the same transaction as the session revision. Persistent global and
  authority/principal limits are enforced across store handles; reopen cannot
  reset policy. Queue overflow and injected index-write failures roll back both
  records. Tests cover an abrupt process exit after committed acquisition,
  lease-expiry rediscovery, identity and outcome refusals, and an explicit
  integrity audit for missing/extra/changed index rows. At that landing, the
  transport service did not enqueue descriptors or run a dispatcher. That
  storage API alone did not complete the asynchronous execution requirement.
  The final `./conformance/run_all.sh` passed with 107 Rust workspace tests,
  Java/C++ suites, all nine transfer pairings, 32 raw QUIC capability probes,
  recursive/recovery CLI scenarios, and the external examples. Draft -04 built
  with zero idnits errors/flaws/warnings and one informational FIPS comment.
- Worker integration: the service now installs payloads before atomically
  committing admission and typed job descriptors. Bounded periodic workers
  process, rehydrate, and resume independently of connection dispatch, reopening
  and checking retained input. Callback panics and invalid decisions are
  retained as refusals. Physical permits remain charged until callbacks return,
  including after shutdown and lease expiry. Queue and worker limits are
  separate from the bounded per-connection outcome observers.
  Raw QUIC tests cover independent completion, control refusals and checkpoint
  deadlines during stalled callbacks, and queue overflow without lost admission.
  Listener-owned connection tasks stop ingress on cancellation without erasing
  admitted jobs. Held-installation tests cover pipelined root admission and
  checkpoint deadlines without prematurely acknowledging received work.
  An abrupt-exit test exercises durable file/job admission and execution after
  reopening. Detached rehydration/resume, missing/corrupt input, shared worker
  limits, and replacement executors with a still-running expired callback are
  also covered. This does not complete retained-storage accounting or make
  synchronous SQLite metadata and lineage operations safe under storage stalls.
  The final `./conformance/run_all.sh` passed with 122 Rust workspace tests
  (55 core, 27 Quinn unit, 37 wire, one allocation gate, two runner tests),
  Java/C++ suites, nine transfer pairings, 32 capability probes, and all external
  examples. Draft -04 passed idnits with zero errors/flaws/warnings and one
  informational FIPS 180-4 comment. Session format 5 is unchanged.
- Retained authenticated recovery: private-use extension 65283 requires Layer 2
  and authenticated-session binding. Authority-qualified immutable request IDs
  correlate atomic claim redemption, resume admission, and 24-hour receipts.
  Complete/refused terminal frames echo the full receipt; disconnect never
  substitutes for an application refusal. Receipt replay cannot extend retention
  or enqueue another job. Durable irreversible claim revocation fences acceptance,
  replay, acquisition, and publication. Session format 6 retains the new state;
  formats 1 through 5 are refused without operational database conversion.
  Eleven core tests cover concurrency, abrupt exit, expiry, rollback, bounded
  receipt history, immutable refusal, and 20 frozen wire cases. Five real-QUIC
  tests cover lost receipt/restart, callback refusal/restart without retry,
  credential ownership and authority, incompatible frames, and full response
  correlation. Separate CDDL fixtures cover all 20 wire shapes.
  The full `./conformance/run_all.sh` passed with 138 Rust workspace tests
  (66 core, 27 Quinn unit, 42 wire, one allocation gate, two runner tests),
  five Java tests, the C++ vector test, all nine transfer pairings, 32 capability
  probes, recursive/recovery CLI scenarios, and every external example.
- Retained-state quota increment: persistent global and authority/principal
  byte/count limits now cover serialized sessions, including completed and
  revoked records. Bounded serialization and length-gated reads prevent
  materializing over-budget serialized blobs. Session reads validate accounting
  in one snapshot; create/save/transaction paths commit state, usage, and the
  job index together. Writes verify checksummed accounting metadata before
  using its capacity; this scans the bounded session set. Thirteen core tests
  and two real-QUIC tests cover capacity,
  concurrency, restart, corruption, rollback, and retained-declaration replay.
  The complete suite passed with 153 Rust workspace tests (79 core, 27 Quinn
  unit, 44 wire, one allocation gate, two runner tests), Java/C++ suites, nine
  transfer pairings, 32 capability probes, and all external examples.
  Payload format 6 is unchanged, but nonempty unaccounted stores are refused
  without conversion. No operational database was migrated.
- Connection storage isolation: metadata operations and lineage writes now use
  a shared canonical-database pool with eight global and four principal slots.
  Physical credit survives waiter cancellation. Control parsing starts and
  enforces checkpoint clocks independently of database admission and output I/O;
  bounded backlog overflow and malformed frames are named refusals. Tests hold
  a SQLite writer through timeout, hold lineage writes while another connection
  completes work, and verify queued-duplicate clocks and cancellation accounting.
  Covered result observers prevent a checkpoint overtaking a concurrently
  committed result. State-dependent operations on one connection remain ordered;
  no disk-latency or concurrent-workload performance guarantee is claimed.
  The full `./conformance/run_all.sh` passed with 162 Rust workspace tests
  (79 core, 32 Quinn unit, 48 wire, one allocation gate, two runner tests),
  five Java tests, the C++ test, nine transfer pairings, 32 capability probes,
  and every external example. Draft -04 passed idnits with zero errors, flaws,
  and warnings, and one informational FIPS reference comment.
- Java state-machine increment: independent deterministic CBOR and WORK_SET
  codecs preserve uint64 values and consume the frozen declaration corpus.
  A separate SQLite store durably retains declaration history, membership,
  payload admission, outcomes, child closure, and STRICT parent resolution.
  Replay and completion checks compare acknowledged history with retained scope
  membership; missing payloads cannot become completion. Status Merkle summaries
  match frozen frames and literal odd-node-promotion hashes. Scoped readiness
  uses the entire sealed inclusive cut, not a received-so-far cursor.
  Tests cover concurrent handles, failed-transaction rollback, retained global
  and session limits, missing/corrupt membership, and abrupt process exits after
  declaration and child closure. The full `./conformance/run_all.sh` passed
  with 162 Rust workspace tests, 30 Java tests, the C++ test, nine existing
  transfer pairings, 32 capability probes, and all external examples. New Java
  public APIs passed Javadoc with doclint and warnings denied. Draft -04 passed
  idnits with zero errors/flaws/warnings and one informational FIPS comment.
  This is a library foundation, not the full Java profile: Netty integration,
  connection state, chunk/payload storage, public sealed client, and real
  Java/Rust recursive and reconnect interoperability remain unimplemented.
  The Java listener still advertises only Layer 0. No Rust state format or
  protocol wire format changed, and no operational database was converted.
- Java producer increment: the independent public Netty `SealedClient` and
  transport codecs now send file-backed nested and out-of-order chunked work
  to the Rust service. Exact declaration, scope digest, checkpoint, and GOAWAY
  correlation precede success. Five real-QUIC tests cover recursive completion,
  declaration and checkpoint replay across restarts, a discarded declaration
  ACK, ownership-label and retained-limit refusals, and checkpoint timeout/bound
  failures. Scripted transports inject altered ACKs, downgrade, oversized
  replies, and Layer 2 frames; they are not independent reference servers.
  Rust now accepts explicit root checkpoint scope zero and preserves its
  presence through durable replay. Stored-session format 7 is required;
  versions 1 through 6 are refused without operational database conversion.
  Java operations are serialized and blocking, with bounded encoded reply
  queues and fixed-size file reads. The producer's observed-state ledger is
  not persistent; declaration replay does not reconstruct previous admission
  observations. A pending checkpoint blocks the client's next operation.
  This is one-direction interoperability, not the full Java profile.
  The full-suite recovery CLI also exposed a runtime-exit race: requesting
  disconnect without draining Quinn could leave the peer waiting for idle
  timeout. An isolated client-runtime regression reproduced it before the fix.
  `begin-yield` now awaits graceful transport shutdown; the regression also
  checks that the durable entity remains DEFERRED and its claim unredeemed.
  After both corrections, `./conformance/run_all.sh` passed with 164 Rust
  workspace tests (80 core, 33 Quinn unit, 48 wire, one allocation gate, two
  runner tests), 40 Java tests with no skips, C++, all nine existing Layer 0
  pairings, 32 capability probes, recursive/recovery CLI checks, and every
  external example. New Java APIs passed Javadoc with doclint and warnings
  denied. Draft -04 passed idnits with zero errors/flaws/warnings and one
  informational FIPS reference comment.
- Java payload-store increment: independent incremental receivers validate FIN
  length/checksum and retain quota-charged file receipts. Complete chunk sets
  are streamed into immutable checksummed objects before any session admission.
  Installation reserves staging/final-name capacity and uses synced no-replace
  publication. Replays verify the retained object without new publication
  capacity; changed input is refused. Reopen counts abandoned files and refuses
  changed policy or foreign layouts. Tests cover abrupt exit before admission,
  corruption, chunk geometry, capacity refusal, concurrent installation and
  cancellation, and cross-process locking after a rejected local open.
  A 32 MiB input is received, installed, and read under a 24 MiB Java heap cap.
  These are blocking library calls and logical file-length/count quotas, not
  Netty integration, per-principal quotas, physical filesystem bounds, or a
  whole-process memory measurement. The Java SQLite and Rust session formats
  are unchanged; the Java payload directory has a separate version-1 policy.
  The full `./conformance/run_all.sh` passed with 164 Rust workspace tests,
  55 Java tests with no skips (15 new payload tests), C++, all nine existing
  Layer 0 pairings, 32 capability probes, recursive/recovery CLI checks, and
  all external examples. The new public API passed Javadoc with doclint and
  warnings denied. Draft -04 passed idnits with zero errors/flaws/warnings and
  one informational FIPS reference comment. This does not complete the Java
  server, reverse-direction interoperability, or the full goal.
- Java durable execution integration: Java database version 2 atomically stores
  admission/processing jobs and child closure/rehydration jobs. Version-1 Java
  stores are refused without conversion. Descriptors, attempt fences, and
  outcomes are checksummed and retained under fixed global/per-session logical
  byte and count policies. Manual lifecycle methods refuse managed work, and
  completion checks reject corrupt or missing jobs. Queue overflow rolls back
  the associated admission or closure transition.
  `SealedExecutor` runs file-backed processing and rehydration outside database
  transactions, with physical global/per-session worker limits and no duplicate
  callback for a still-running local attempt. Shutdown keeps canonical-database
  ownership until actual callbacks and started storage calls return. Input
  corruption and callback exceptions become retained refusals, not successful
  work or automatic retries. Epoch and expiry checks reject stale publication;
  an interrupted expired attempt can be reacquired after restart.
  This is execution used by the forthcoming Java server, not a Netty sealed
  listener. Per-session limits do not substitute for authenticated principal
  quotas, and logical outcome reservations do not establish physical DB/WAL
  bounds or reserve every future rehydration job. The full objective remains
  incomplete, including reverse-direction QUIC and connection-control evidence.
  The final `./conformance/run_all.sh` passed with 164 Rust workspace tests,
  74 Java tests with no skips (19 new execution/storage tests), C++, all nine
  existing Layer 0 pairings, 32 capability probes, recursive/recovery CLI
  checks, and all external examples. Changed/new public Java APIs passed
  Javadoc with doclint and warnings denied. Draft -04 passed idnits with zero
  errors/flaws/warnings and one informational FIPS reference comment.
- Java sealed listener integration: the separate public `SealedServer` now
  connects the independent codec, SQLite state machine, payload store, and
  executor over actual Netty QUIC. Fixed-size file reads and bounded metadata
  workers do not hold the network event loop. Pending checkpoint clocks begin
  before storage queueing; duplicates cannot extend them, and late storage
  completion cannot produce a late ACK. Covered ingress, unsent job outcomes,
  and nested checkpoints gate completion. Scope-digest comparison and STRICT
  resolution commit together; a forged summary rolls back closure.
  Java database version 3 retains bounded checkpoint identity and ACK history,
  checksummed with ownership and protected against missing records. Versions 1
  and 2 are refused without conversion. No operational store was migrated.
  The Rust public-client `sealed-scenario` exercises the Java server's recursive
  and out-of-order chunked completion, reconnect replay, and changed-owner or
  checkpoint-identity refusals. Java-server tests additionally hold SQLite,
  stall application callbacks, reset streams, discard ACK observations before
  restarting, verify STRICT failure, and exhaust the storage backlog. A 32 MiB
  real-QUIC transfer and callback run under a 24 MiB Java heap cap; this is not
  a native-memory/RSS or concurrent multi-tenant measurement.
  Persistent producer observations and broader resource/crash evidence remain
  due. The full goal is not complete merely because these scenarios pass.
  The final `./conformance/run_all.sh` passed with 164 Rust workspace tests,
  89 Java tests with no skips (nine server and six checkpoint-store tests),
  C++, all nine Layer 0 pairings, 32 capability probes, recursive/recovery CLI
  checks, and all external examples. Changed public Java APIs passed Javadoc
  with doclint and warnings denied. Draft -04 passed idnits with zero errors,
  flaws, and warnings and one informational FIPS reference comment.
- Rust SQLite file-length increment: every store connection uses a non-default
  guard over the bundled Unix VFS and an explicit main-page cap. Immutable,
  checksummed policy bounds database, WAL, rollback-journal and shared-memory
  lengths. Write/truncate/map checks precede growth; preallocation and database
  mmap are disabled. Nonempty stores without policy, policy changes, aliases,
  and unsupported backends refuse without operational conversion. Quota failures
  roll back state and job admission and propagate as named capacity refusals.
  Eleven focused core tests exhaust each file type, audit growth controls, hold a
  WAL reader, corrupt policy, and exit abruptly before reopening retained state.
  Concurrent connection churn covers sidecars unlinked during metadata sampling;
  a zero-link observation is not misclassified as a hardlink alias.
  A real-QUIC test preserves declared membership at WAL exhaustion and resumes
  declaration replay after checkpointing. This does not establish allocated
  filesystem bounds, Java JDBC bounds, retained-payload quotas, or completion
  reservations. The final `./conformance/run_all.sh` passed with 176 Rust
  workspace tests (91 core, 33 Quinn unit, 49 wire, one allocation gate, two
  runner tests), 89 Java tests with no skips, C++, all nine Layer 0 pairings,
  32 capability probes, recursive/recovery CLI checks, and all external
  examples. Draft -04 passed idnits with zero errors, flaws, and warnings
  and one informational FIPS reference comment.
- Rust retained-payload increment: immutable policy now bounds global and
  authority/principal object lengths/counts, staging and directory metadata,
  including final lineage. Reservation precedes disk creation; checksummed
  identity metadata, verified incremental copies and synced receipts precede
  installation success. Matching replay does not double-charge an object.
  Reopen accounts for interrupted copies, partial metadata/receipts and empty
  canonical directories without deleting them. Only matching prefixes resume;
  corrupt complete metadata and unexpected aliases refuse. An exclusive Unix
  root lock survives handle drop while payload readers, spool loans or I/O
  retain ownership. Eighteen unit tests cover individual quota boundaries,
  owner/authority refusal, process exit, prefix images, conservative rollback,
  stalled readers, aliases and cross-process locking. A real-QUIC test preserves
  declared-but-unadmitted work at one principal's payload limit while another
  principal completes. Authentication refusal checks pin startup policy/lock
  files and zero payload usage. Capability probes now compare startup output
  byte-for-byte, with a separate regression test and no exempt filenames.
  Nonempty unaccounted payload roots are refused without conversion; session
  format 7 and normative wire/CDDL are unchanged. These are cooperating-writer
  file-length reservations, not allocated filesystem or all-power-loss proof.
  The final `./conformance/run_all.sh` passed with 196 Rust workspace tests
  (91 core, 51 Quinn unit, 50 wire, one allocation gate, three runner tests),
  89 Java tests with no skips, C++, all nine Layer 0 pairings, 32 capability
  probes, recursive/recovery CLI checks and every external example. Draft -04
  passed idnits with zero errors/flaws/warnings and one FIPS reference comment.
- Java SQLite file-length increment: a packaged native extension registers a
  non-default bounded VFS inside the pinned JDBC engine. It carries no PipeStream
  protocol/state code or Rust dependency. Main/WAL/journal/shared-memory limits
  are immutable, checksummed and synced before database creation. Writes,
  truncates and shared-memory maps check bounds before growth, and store
  connections set a main-page cap. Mmap and preallocation cannot bypass the caps.
  Nonempty unaccounted stores, policy drift, aliases and unsupported backends
  refuse without conversion; the Java schema remains version 3.
  Nine JDBC tests cover database/WAL/journal exhaustion, rollback, held readers,
  immutable policy, oversized/corrupt layouts, concurrent handles, bounded native
  registrations and abrupt exit with uncheckpointed WAL. Direct native tests
  exercise all four file families and growth controls. A real-QUIC test checks
  named refusal, retained membership, absent accidental payload/job admission,
  and replay after checkpointing and reopen. Completion reservations,
  authenticated Java principal quotas and the full crash/resource matrix remain
  due. No operational database was converted.
  The final `./conformance/run_all.sh` passed with 196 Rust workspace tests,
  99 Java tests with no errors/failures/skips, the native SQLite test, C++, all
  nine Layer 0 pairings, 32 capability probes, recursive/recovery CLI checks and
  every external example. Native address/undefined-behavior sanitizer checks
  and strict public Javadoc passed. Draft -04 passed idnits with zero errors,
  flaws and warnings and one informational FIPS reference comment.
- Java rehydration reservation increment: version-4 stores charge possible
  rehydration descriptors/outcomes at PROCESS admission. Waiting parents retain
  completion slots independently of the ordinary processing queue, so parents
  do not block their own children and closure does not compete with new work.
  Child closure, parent transition and reservation conversion commit together.
  STRICT failure and ordinary terminal/refused processing release only unused
  future credit; retained records remain charged. Reopen derives reservations
  from checksummed job/entity state. Ordinary processing remains bounded at
  128 global/32 session jobs; reserved/active rehydration slots are separately
  bounded by 65,536 global/16,384 session entities and the combined 64/16 MiB
  metadata policy. Physical worker limits are unchanged. Bounded discovery pages
  interleave sessions so a large reserved queue does not hide other sessions.
  Tests cover exact conversion with maximum identifiers/large metadata, saturated
  ordinary and metadata budgets, transactional rollback, waiting parents admitting
  children, version-3 refusal without writes, and abrupt process exit. A real-QUIC
  test fills ordinary processing, commits rehydration, releases held callbacks,
  and completes the whole-scope checkpoint. These are logical reservations,
  not physical DB/WAL space or admission of unlimited future descendants.
  The final `./conformance/run_all.sh` passed with 196 Rust workspace tests,
  104 Java tests without errors/failures/skips, native SQLite and C++ tests, all
  nine Layer 0 pairings, 32 capability probes, recursive/recovery CLI checks and
  every external example. Strict public Javadoc passed. Draft -04 passed idnits
  with zero errors/flaws/warnings and one informational FIPS reference comment.
- Rust publication reservation increment: storage policy version 2 protects
  the serialized growth of every admitted job's outcome, entity output digest
  and execution attempt. Layer 2 processing reserves its explicit continuation
  budget and bounded claim validation fields. The default token budget is 64 KiB,
  configurable independently of the unchanged wire ceiling; callbacks receive
  the smaller of that budget and the usable STATUS frame limit before execution
  (zero without Layer 2). Exact-frame and one-byte-over QUIC cases pin the limit.
  Oversized results become retained named refusals rather
  than claims, truncated tokens or successful completion. Direct store mutations
  enforce the same policy. Charges are bound to session checksums, recomputed on
  reopen, and protected from unrelated admissions under record/principal/global
  limits. Actual publication converts reserved bytes; retained outcomes stay charged.
  Ten core tests cover exact-quota publication for all current stages/outcomes,
  serialization and map-prefix boundaries, rollback, reservation corruption,
  concurrent principal admission, revocation and abrupt process exit. Two real
  authenticated QUIC tests publish a held yield after other work fills the store,
  verify the callback's budget, retain an oversized-result refusal and complete
  authenticated recovery for an in-budget claim. Old storage policies are refused
  without conversion; session format 7 and wire/CDDL are unchanged.
  These are admitted-job logical reservations, not future rehydration admission,
  physical DB/WAL publication space, final-lineage headroom or orphan cleanup.
  The final `./conformance/run_all.sh` passed with 208 Rust workspace tests
  (101 core, 51 Quinn unit, 52 wire, one allocation gate, three runner tests),
  104 Java tests without errors/failures/skips, native SQLite and C++ tests, all
  nine Layer 0 pairings, 32 capability probes, recursive/recovery CLI checks and
  every external example. Strict workspace clippy and touched-file rustfmt checks
  passed. Draft -04 passed idnits with zero errors/flaws/warnings and one
  informational FIPS reference comment. These are local results, not a complete
  goal, independent implementer review or hosted CI result.
- Rust future-rehydration increment: queue policy version 2 reserves a possible
  rehydration slot at PROCESS admission, independently of ordinary PROCESS/RESUME
  capacity. Defaults remain 128 global/32 principal ordinary jobs; future/active
  rehydration has separate 65,536 global/16,384 principal limits, with no added
  physical workers. Waiting DEHYDRATING parents retain slots across revocation
  and restart without occupying the processing slots needed by their children.
  Storage policy version 3 funds the maximum serialized rehydration descriptor,
  outcome, attempt, output and scope-close digest, including collective job/attempt
  map-prefix growth. Closure and job admission convert bytes and slots atomically.
  Retained refusals do not count as successful work. Every store mutation audits
  queue rows against checksummed sessions before using free capacity; discovery
  interleaves principal buckets within bounded pages. These are bounded scans,
  not constant-time or global fairness claims. Old queue/storage policies refuse
  without conversion; the session payload remains version 7 and wire/CDDL unchanged.
  Ten core tests cover exact byte/slot capacity, map and identifier boundaries,
  concurrent owner admission, revocation, rollback, corruption, abrupt process
  exit before/after conversion, policy refusal, and independent-owner discovery.
  One real-QUIC sealed test completes a parent's rehydration while a held callback
  fills ordinary processing, then verifies full root checkpoint and GOAWAY.
  The final `./conformance/run_all.sh` passed with 219 Rust workspace tests
  (111 core, 51 Quinn unit, 53 wire, one allocation gate, three runner tests),
  104 Java tests without errors/failures/skips, native SQLite and C++ tests,
  all nine Layer 0 pairings, 32 capability probes, recursive/recovery CLI checks
  and all external examples. Workspace formatting/clippy and strict Rustdoc
  passed. Draft -04 passed idnits with zero errors/flaws/warnings and one FIPS
  reference comment. These are local results, not hosted CI or full-goal completion.
- Rust final-lineage file quota: payload installation now durably reserves
  1,120 bytes and one object per session for its ownership marker, future final
  metadata, digest, receipt and publication stage. Publication never creates a
  placeholder digest or borrows ordinary staging byte/object credit; the whole
  allowance stays charged afterward. Partial markers remain globally charged,
  partial final metadata/receipts use prepaid credit, and matching replay cannot
  double-charge. Failed owner-quota promotion preserves unattributed credit.
  The new `PSRET002` policy refuses old/unreserved roots without conversion;
  session payload 7 and wire/CDDL are unchanged. Focused tests exercise exact
  quota, prefix images, process exit, concurrent ownership and immutable replay.
  Authenticated QUIC tests hold callbacks while independent principals fill
  storage, then pin actual lineage bytes, checkpoint ACKs and GOAWAY. A missing
  declared payload instead times out without successful completion. These are
  configured file-length reservations, not filesystem-block preallocation or
  physical DB/WAL publication capacity.
  Full-suite validation exposed a Java reset-test race caused by using Netty's
  FIN-sending `close()` as a reset injector. The test now sends explicit
  RESET_STREAM and waits for refusal before asserting zero admission; a separate
  normal-FIN test verifies completion. Java server behavior is unchanged.
  The corrected FIN/reset cases passed five consecutive focused runs, then the
  final `./conformance/run_all.sh` passed with 231 Rust workspace tests
  (111 core, 62 Quinn unit, 54 wire, one allocation gate, three runner tests),
  105 Java tests without errors/failures/skips, native SQLite/C++ tests, all nine
  Layer 0 pairings, 32 capability probes, recursive/recovery CLI checks and every
  external example. Workspace formatting/clippy and strict Rustdoc passed;
  draft -04 had zero idnits errors/flaws/warnings and one FIPS reference comment.
  These are local results, not hosted CI or proof of full-goal completion.
- Rust incremental-index prerequisite: queue reconciliation now preserves
  unchanged entries and updates changed rows in place. Obsolete entries are
  deleted before replacement admission; accounting rows are updated only when
  their charge or checksum changes. Quota evaluation excludes the current session
  without deleting its index first, and full pre-write integrity audits remain.
  Six tests cover no-op/acquisition/completion/revocation row identity, reopen,
  full-quota replacement and transactional rollback on deletion-followed-by-insert
  or accounting failures. The focused persistence run passed all 63 tests.
  An index-only comparison at 1, 128 and 512 jobs emitted zero WAL bytes for
  unchanged reconciliation versus 28,872, 61,832 and 144,232 for full replacement.
  Public saves still rewrite the serialized session and advance its revision;
  these measurements are not a transaction-space or service-throughput bound.
  No session format, queue/storage policy or wire/CDDL changed. Physical DB/WAL
  reservations are still required; this removes unrelated index writes before
  deriving and enforcing that capacity guarantee.
  The full `./conformance/run_all.sh` passed with 237 Rust workspace tests
  (117 core, 62 Quinn unit, 54 wire, one allocation gate, three runner tests),
  105 Java tests without errors/failures/skips, native SQLite/C++ checks, all
  nine Layer 0 pairings, 32 capability probes, recursive/recovery CLI checks
  and every external example. Workspace formatting/clippy and strict Rustdoc
  passed. Draft -04 passed idnits with zero errors/flaws/warnings and one FIPS
  reference comment. These are local results, not proof of full-goal completion.
- Java physical DB/WAL completion reservations passed their conformance gate;
  see the current increment above.
  At that increment, Rust orphan reclamation was not implemented; PR #33's
  evidence above now covers it. Persistent producer observations and the remaining
  independent Java profile requirement matrix are still unfinished. Physical
  execution limits and temporary spools are coordinated only within one writer
  process; broader tenant/resource stress evidence remains due.

Implementation evidence must replace these status entries as it lands. A
transport-authentication increment alone does not satisfy authenticated recovery.

Next implementation boundaries: complete storage/resource guarantees around
the asynchronous workers and independent Java sealed-work interoperability.
Legacy CLAIM_REDEMPTION still refuses a duplicate after a lost ACK. Clients
requiring retained admission and completion use the negotiated recovery profile.
The service now populates job descriptors and reopens retained input. Lease
expiry is not callback cancellation. Periodic dispatch can reacquire unfinished
expired attempts, but does not retry application-refused jobs automatically.
Temporary spool accounting is process-local and excludes permanent entity files.
Rust and Java SQLite file-length caps are now independent of that accounting.
With independent Java physical completion headroom, Rust explicit orphan
reconciliation and Java producer observations implemented, complete validation and
the remaining crash/resource matrix without treating missing input as completed work.
Do not treat recovery receipts or publication fencing as completion of bounded
asynchronous execution. Java now has a separate sealed listener and public
producer; its original listener and CLI remain Layer 0. Extend the existing
cross-language tests through full retained-work resumption,
including lost ACKs, reconnect, scoped checkpoints, and recursive completion.
