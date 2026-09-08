# Peer review: Claude's Java fixture-hook placement (A assignment, reciprocal check)

Reviewer: Kimi (B). Subject: hook code at Claude head `5e3138a`
(`84d6719` added `Boundaries.java`; `FixtureMain.java`/`FixtureEvents.java`
were uncommitted working-tree files at review time — re-check when committed).
Scope: hook placement only, per TEAM.md cross-check rule. This is a recorded
peer check, not final review.

## Verdict: commit-side placement SOUND; SENT-side reporting has 4 defects to fix

The core invariant holds. Every `committed` hook fires inline after a
synchronous `COMMIT` (PRAGMA synchronous=FULL) or fsync'd file install
returns, on the storage-worker thread, before the reply is queued
(`DurableServer.java:548-564` worker→`loop()` handoff). Exception paths
route to refusal handling and never reach the hook. Replay paths still
execute a real COMMIT. Withhold is consulted on the event loop post-commit
pre-queue (`:798`, `:1187`). `halt(137)` fires post-commit and
post-event-fsync. The shipped `V2Main` installs `Boundaries.NONE`; hooks
are reachable only via the package-private overload and FixtureMain.
No hook can forge a commit, receipt, callback, or result.

## Defects (must fix before boundary evidence is trusted)

1. **`ADMISSION_RESPONSE_SENT` on the replay path fires before the write**
   (`DurableServer.java:1118`, write at `:1129`) and skips the withhold
   check — contradicts the stated "*_SENT after Netty write settlement"
   and makes the replay path undriveable by drop-reply.
2. **All three `REFUSAL_SENT` sites fire before the refusal is queued**
   (`:606-608`, `:916-919`, `:1236-1239`).
3. **`ControlWrites` settle callback runs regardless of `result.isSuccess()`**
   (`ControlWrites.java:91-97`): SESSION/DECLARATION/COMPLETE/
   ADMISSION_RESPONSE_SENT and DETACH_ACKNOWLEDGED can be recorded for
   writes that actually failed. Gate the event on success or rename the
   semantic to "write initiated".
4. **FixtureMain records a `REFUSAL_SENT` event for a refusal frame that is
   never written** (`FixtureMain.java:182`): drop-reply closes the
   connection with CONTROL_RESET; no Refusal message is sent. Record a
   distinct observation (or none) — a REFUSAL_SENT record for a frame that
   never hit the wire is exactly the forged-evidence shape the hook
   contract forbids. Related: a drop-reply row targeting a boundary
   outside the three REPLY pairs is silently inert; the parser should
   reject it.

## Gaps (needed for the B matrix; please plan them)

5. Missing server boundaries: `EXECUTION_CLAIMED`, `OUTPUT_INSTALLED`,
   `PUBLICATION_COMMITTED`, `CLOSURE_COMMITTED` (they live in the authority
   runtime, off the connection path). G2/G4 rows need PUBLICATION_COMMITTED
   and EXECUTION_CLAIMED specifically.
6. All eight client boundaries are unimplemented (no `Boundaries`
   references in `DurableClient.java`). Client-death and journal-boundary
   rows (g2-kill-client-after-request-sent family) need INTENT_JOURNALED,
   REQUEST_SENT, RECEIPT_JOURNALED at minimum.

## Observations (accepted, driver will compensate)

7. Pause blocks a shared 8-thread storage pool (`DurableHost.java:872-881`):
   8+ concurrent pauses stall all connections' storage work. Deadline
   bounded; the driver will keep concurrent pauses below the pool size and
   record pool geometry per run.
8. TSV file order is not real-time order across the synchronous `committed`
   path and the async `sent` executor; per-process `seq` restarts at 1 when
   a second process appends to an existing file. The driver's reader
   validates per-`process_start_id` monotonicity only, never cross-path
   order — consistent with interface-v1.
9. Events can be lost (commit happened, no record) when post-commit
   response construction throws; safe for the no-forgery invariant, lossy
   for the trace. Noted for trace interpretation.
