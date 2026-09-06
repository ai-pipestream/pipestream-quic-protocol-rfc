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
- Not yet implemented: physical DB/WAL completion-space reservations,
  orphan reclamation,
  persistent producer observations, and the remaining independent Java
  profile requirement matrix. Physical execution limits and temporary spools are coordinated only
  within one writer process; broader tenant/resource stress evidence remains due.

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
Add physical DB/WAL publication headroom and
explicit orphan reconciliation without
deleting a live generation or treating missing input as completed work.
Do not treat recovery receipts or publication fencing as completion of bounded
asynchronous execution. Java now has a separate sealed listener and public
producer; its original listener and CLI remain Layer 0. Extend the existing
cross-language tests through full retained-work resumption,
including lost ACKs, reconnect, scoped checkpoints, and recursive completion.
