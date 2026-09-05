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
- Not yet implemented: physical database/WAL and retained-payload quotas,
  completion-space reservations, orphan reclamation,
  storage-stall handling, and the independent Java
  profile. Physical execution limits and temporary spools are coordinated only
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
Temporary spool accounting is process-local and excludes permanent entity files
and physical SQLite storage. Add physical quotas, completion reservations, and
explicit orphan reconciliation without
deleting a live generation or treating missing input as completed work.
Do not treat recovery receipts or publication fencing as completion of bounded
asynchronous execution. Java still implements only the earlier Layer 0 subset.
