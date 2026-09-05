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
- Not yet implemented: retained recovery outcomes, asynchronous job dispatch,
  durable storage quotas and restartable inputs, periodic recovery for every operation, and
  the independent Java profile. Callback execution itself is still synchronous.

Implementation evidence must replace these status entries as it lands. A
transport-authentication increment alone does not satisfy authenticated recovery.

Next implementation boundaries: authority-qualified recovery requests with
stable request identity, retained outcomes, expiry/revocation/retention rules;
and durable dispatch of the file-backed inputs with bounded per-principal/global workers.
The current CLAIM_REDEMPTION path still refuses a duplicate after a lost ACK.
Processing attempts lack restartable header/spool descriptors; lease expiry
is not callback cancellation. No automatic execution retry is advertised.
Temporary spool accounting is process-local and excludes permanent entity files
and SQLite state. Add durable quotas and explicit orphan reconciliation without
deleting a live generation or treating missing input as completed work.
Do not treat authentication or publication fencing as completion of recovery
or asynchronous execution. Java still implements only the earlier Layer 0 subset.
