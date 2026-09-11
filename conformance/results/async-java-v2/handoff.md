# Handoff: Java V2 durable endpoints (A-SERVER + A-CLIENT)

Owner: Claude (A). Branch `agent/rfc-claude-java-v2`, worktree
`/work/worktrees/pipestream-rfc-claude`, base `8eb5a17` on
`feat/durable-work-results-v2`.

## Status

State on the board: REVIEW_READY. Every assigned gate passed on the
`pipestream.4` transport at `e1533d3`: focused and full Java install (716
tests, 0 failures, 123 fresh XML reports), strict changed-type doclint (21
types, exit 0), fresh source-pinned native build (297 native tests), real
opposite-Rust integration in both directions, combined Java/Java process run,
source-pinned artifact hashes, raw logs and checksums preserved under
`conformance/results/async-java-v2/raw/`. SERVER_READY and CLIENT_READY pins
are final at this head (section 2).

Commits (all plain author identity, no generated attribution):

| commit | content |
|---|---|
| `2db1344` | `api-plan.md`: public API, configuration, fixture contract |
| `84d6719` | `DurableHost` (authority composition) + `DurableServer` (listener) + loopback tests |
| `5e3138a` | `DurableClient`, `ClientJournal`, `ClientValidation`, `ResultFiles`, `InputSource`, `V2Main`/`ClientCommands`, cross-language tests |
| `b13995b` | failure matrix tests, `FixtureMain`/`FixtureEvents`, hook-placement corrections, runtime + client boundaries, transform/v2, Kimi peer check |
| `9dfcfc6` | client-death fixture scenario |
| `723629e` | live revocation, control FIN before detach, expiry under a pinned read |
| `18f3728` | package-private javadoc for the strict doclint gate |
| `53aaadb` | Java README V2 section, storage-worker exhaustion row |
| `8fee938` | `RawDurableAuthority` harness and `DurableClientResultNegativeTest` (client-side malformed result streams) |
| `e1533d3` | transport bundle `pipestream.4` (quiche drained-stream collection, Netty parent-map release, native credit test), POM pin, manifest hashes, evidence file and raw logs |
| `63d03a0`, `afd15dd`, `417ba83` | final handoff (REVIEW_READY), closed client result-stream gap |
| `b5a3a91` | review of the spec branch `docs/client-recovery-guidance-2026-09` (7315713) against the Java endpoints: `spec-review-client-recovery-2026-09-09.md` |
| `68821f3`, `d7ba2e0` | client recovery after the spec hardening: `--retry-budget`/`--retry-backoff-ms`, `REFUSED`/`UNRESOLVED`/`CAPABILITIES` launcher output, REFUSAL details naming the local bound, stream-credit observation on both stacks (`RawPeerRustAuthorityTest`, `ClientRecoveryTest`); CR04/CR06/CR10 scenarios on the Java authority; evidence `conformance/results/durable-work-v2-java-client-recovery-2026-09-09.txt` |
| `0176855` | response to Kimi's milestone 16 resource rows: stream bounds judged before the connection, idle-control clock gated on outstanding work, named deadline reasons on REFUSAL and APPLICATION_CLOSE on both listeners, host-held query-only SQLite anchor stopping the per-call WAL-index rebuild (`DurableHostIdleWritesTest`), documented listener ceilings |
| `7585a9dc` | durable-profile connections are never closed for control silence (core-only connections keep the control deadline); `DurableWireNegativeTest.stalledPrincipalIsRefusedPerStreamOnASurvivingConnection` replays the neutral driver's stalled-principal shape; full offline run 717 tests, 0 failures |
| `ab59dafb` | client control deadline corrected (traceability defect D1: send renews the activity clock, no silence failure with nothing pending, pending requests due within wait + control deadline; `DurableClientControlDeadlineTest`); `--db-mib`/`--wal-mib` storage funding on `init-authority` and `serve` for Meta (`V2MainStorageFundingTest`); refusal timing and before/after write probes in the stalled-principal test; replayed-input stop and release assertions; clause-level Section 12 traceability (`docs/standards/section-12-java-traceability.md`, 373 statements); Kimi M17b question (2) answered (section 5) |
| `0a088050` | client cross-checks scope pages before journaling them and views against retained child scopes (defect 8, `DurableClientContradictionTest`); STOP_SENDING alone is not admission evidence (`DurableClientControlDeadlineTest`, two cases, raw authority input and control script hooks); replayed-stream and D2/D3/D7 notes; handoff section 4 items 7-9 |

Working tree at `0a088050`: clean. Nothing pushed (no push authorization was
given); no CI exists for this branch; no draft/deploy action taken; the
shared feature branch and main were not merged.

## 1. Contract to source to tests to evidence

Section 12 / Appendix F contract items, where they live, which tests prove
them, and the raw evidence (scratchpad logs are quoted on the board; the
evidence files under `conformance/results/` are committed).

| contract | source | tests | evidence |
|---|---|---|---|
| Core + durable-work (65284) + result-delivery (65285) negotiation, ALPN `pipestream/2`, Core-only path for anonymous peers | `DurableServer` (Control handler), `DurableOptions.offer`, `ClientOptions.offer` | `DurableServerTest`, `RustClientJavaServerTest`, `JavaClientRustServerTest` | `server-test*.log`, `interop-rc*.log`, `interop-jc1.log` |
| SESSION create/attach/next-sequence, generation fencing, replay of a retained creation | `DurableHost.access`, `SessionStore`, `DurableServer.bindAndRespond`, `DurableClient.binding` | `DurableServerTest`, `DurableClientTest`, `DurableAuthorizationTest` | `auth-wire-life-1.log`, `wire-more-1.log` |
| SCOPE declare/seal, page, checkpoint with status roots and seals, manifest, complete exclusion | `SessionStore`, `Commitments`, `DurableServer.dispatch`, `DurableClient.declare/page/checkpoint/manifest/complete`, `ClientJournal.observePage/observeSummary` | `DurableBranchTest`, `DurableWireNegativeTest.resultReadRefusalsAndCompleteExclusionAreExact`, `V2MainProcessTest` | `branch-test*.log`, `wire-more-1.log` |
| WORK admission over uni streams with `[1, streamId]` tags, replay → STOP_SENDING 0 + admission response, header/idle/lifetime deadlines, interrupted input → INTEGRITY_ERROR | `DurableServer.InputTransfer`, `StreamTransport.Data.stopReplayed`, `InputStore` | `DurableServerTest`, `DurableWireNegativeTest.stalledInputs…`, `DurableLifecycleTest.disconnectDuringInput…` | `java-hooks-1.log`, `wire-8.log` |
| Execution modes 0/1/2 (leaf, caller-expanded, authority-expanded chunking), STRICT closure, empty and zero-output cases, undeclared/wrong-scope refusals | `ExecutionRuntime`, `ReferenceApplications` (copy, consume, retry-copy, reassemble, chunk-copy, transform) | `DurableBranchTest` (4) | `branch-test3.log` |
| Retry/cancel/skip/scope-cancel semantics, skip authorization, deadline expiry, watch CONFLICT, transform/v2 oracle | `SessionStore` fences, `DurableHost.fenceAuthorization`, `DurableClient.retry/cancel/skip/cancelScope` | `DurableMutationTest` (4) | `mut-auth-1.log` |
| Owner authorization: rotation, remap, removal, cross-owner refusal without disclosure, offline and live revocation, untrusted credentials fail the handshake | `TlsAuthentication.Guard`, `DurableHost.OwnerPolicy`, `SessionStore.Access` | `DurableAuthorizationTest` (4) | `wire-more-1.log` |
| Lost-ACK recovery for create/declare/admit (real commit, withheld reply, replay), disconnect releases only connection state, shutdown waits for a paused callback, storage-worker exhaustion answers LIMIT_EXCEEDED on the loop instead of stalling | `Boundaries.withhold`, `DurableServer.close` drain, `DurableRequests.refuseNew` | `DurableLifecycleTest` (4) | `java-hooks-1.log` |
| Framing/correlation violations fatal, refused requests consume ids, stream credit replenishment after sequential inputs, stalled inputs expire without blocking others, control FIN before DETACH is a framing failure, output expiry while a read pins it | `DurableServer`, `ControlWrites`, `ClientCorrelation`, `ResultService`, `RetentionService` | `DurableWireNegativeTest` (6) | `wire-more-1.log`, `wire-more-2.log` |
| Result delivery: header, chunks, FIN, client verification and hard-link install, same-output re-read, local verification without network | `DurableServer.ResultTransfer`, `DurableClient.read`, `ResultFiles` | `DurableServerTest`, `DurableClientTest`, `V2MainProcessTest` | `client-test*.log`, `combined-1.log` |
| Detach and FIN rules: server FIN only after all preceding responses and the detach acknowledgement; client owns graceful close | `DurableServer.Control.finishOutput`, `DurableClient.detach` | `DurableServerTest`, `DurableClientTest`, `V2MainProcessTest` | `combined-1.log` |
| Separate V2 launchers, public application registration, SIGTERM drain with `DRAINED`, `READY host:port`, Rust-compatible principal map, journal args identical to the Rust CLI | `V2Main`, `ClientCommands`, `PrincipalMap`, `DurableHost.Application` | `V2MainProcessTest` (1), `RustClientJavaServerTest` (2), `JavaClientRustServerTest` (1) | `combined-1.log`, `interop-rc3.log`, `interop-jc1.log` |
| Client result-stream negatives from a raw authority: headers contradicting the selection (length, digest, attempt), truncated/over-long/corrupted/reset payloads fail only that delivery; unsolicited or duplicate deliveries and oversized or undecodable headers fail the connection; nothing installed, no staging leftovers | `DurableClient.incomingStream`/`ResultTransfer`, `ClientCorrelation.beginResult/resultBytes/finishResult`, `ObjectStream.HeaderReader/Payload`, `ResultFiles.Staging` | `DurableClientResultNegativeTest` (6) with the test-only `RawDurableAuthority` | `raw/client-result-negative-2026-09-09.log` |
| Test-only fixture adapter: interface-v1 events for both roles, pause/drop-reply/kill, runtime and client boundaries, parser rejections | `FixtureMain`, `FixtureEvents`, `Boundaries`, hooks in `DurableServer`/`ExecutionRuntime`/`ExecutionScheduler`/`DurableClient` | `FixtureMainTest` (5) | `fixture-test-2.log`, `fixture-test-3.log` |

Full-suite counts at `e1533d3` on transport `.4` (`mvn -Psealed-interop
install`, 716 tests, 0 failures, raw log and XML checksums in `raw/`):
`DurableServerTest` 4, `DurableClientTest` 2, `DurableBranchTest` 4,
`DurableMutationTest` 4, `DurableAuthorizationTest` 4, `DurableLifecycleTest`
4, `DurableWireNegativeTest` 6, `V2MainProcessTest` 1, `FixtureMainTest` 5,
`TransportDependencyTest` 1, `RustClientJavaServerTest` 2 and
`JavaClientRustServerTest` 1 (real Rust peer), plus the 682 pre-existing
tests of the reference. `DurableClientResultNegativeTest` 6 was added after
that run at `8fee938` (log in `raw/`). Artifact hashes at this head: library jar
`52ef1077cc6b5f3727a489e1861cc02cf2f27b757468f38e6544892c65221ea7`, shaded
all-jar `6da5e9d08c6ccb39455557defaaf8ba043d7fc42cb547bc5c3be81ef17ad3de4`
(not timestamp-reproducible), transport classes jar
`87a3d581978b63085e5f52fc8dafe132eb81ddf86c180a87cf5461b126547105`, transport
native jar `e49d88b724cc79c936899542c1565a00454a93d512816de6e8cfefa637c51c50`.

## 2. Pins accepted by peers

- Kimi consumed SERVER_READY + CLIENT_READY at `5e3138a` (merged into
  `agent/rfc-kimi-neutral-v2` at `bc791fb`); their g1-leaf-copy runs all three
  directions with byte-identical output. Kimi's all-jar hash differs from mine
  (shaded-jar timestamps); the lib jar hash `c513823117309d9ff373e41e21b858ba185807bdab97fe53c9447c3cb0bf207a`
  is the reproducible one. Reproducible builds are not configured in the POM;
  that is a follow-up, not a contract gap.
- Kimi's interface-v1 (`1452f60`, schema hash
  `c566751af3866984370eb3945ac93818a7122aab16401efb6619aeded54b16dd`) is
  implemented exactly by `FixtureMain`/`FixtureEvents` (api-plan section 6.3).
- Kimi's peer check of my hook placement (`08f13c6`): commit side sound; the
  four SENT-side defects are fixed in `b13995b` and covered by
  `FixtureMainTest`. My peer check of Kimi's Rust hook proposal (`0beeaa8`) is
  `peer-review-kimi-rust-hooks.md`.
- Meta: transform/v2 (`rotl8(b,1) XOR (i mod 251)`, IDEMPOTENT) is in
  `ReferenceApplications` at `b13995b`; custom mode-0 applications register
  through `DurableHost.initialize/open`; Rust `RestartSafety::Pure` maps to
  Java `IDEMPOTENT`.
- Current subject pins at `7585a9dc` (transport `.4` unchanged): lib jar
  `a21ae9c38262d38218fa0cdad2fb3acf9dcf1a7206755eb377dacce5d6a915e9`, shaded
  all-jar `61ab64a312908dad40f458bf32ce8d8dadcb8a13a0aea489e44dd87e918a458a`.
  Superseded at `ab59dafb` (listener wire behaviour unchanged; client
  deadline and launcher flags changed): lib jar `950b8fb8a2f2ad4bc13571775b1de66640ec26f3a760bb6716142687bedc8e18`, shaded all-jar
  `e1763b4a460b524ba54c41287dc469a369c778eefce72e3cc2d4d3bc90c18779`. Meta: the 48 MiB mixed cell needs
  `--db-mib 1024 --wal-mib 256` on both `init-authority` and `serve`.
  `0a088050` changes only the Java client and tests, so the `ab59dafb` jar stays
  the peer pin and is the build in `target/`; no new jar is issued until the
  listener or launcher changes again.
  Kimi's driver (run by follow-on agents while Kimi is away) merged `0176855`
  at milestone 17 (`add98fd6`, archive `durable-18d3ea398f09f12e`, JVM heap
  frozen at `-Xms256m -Xmx2g`) and `7585a9dc` at milestone 17b.

## 3. Gates and remaining gaps

All assigned gates passed on `.4` (see Status and
`conformance/results/durable-work-v2-java-drained-streams-2026-09-09.txt`).
The gap listed in the first handoff is now closed at `8fee938`:

1. Client-side negative result streams (oversize, undecodable, contradicting
   or duplicate result headers and truncated, over-long, corrupted or reset
   payloads from a misbehaving authority) are now driven by the test-only
   `RawDurableAuthority` harness in `DurableClientResultNegativeTest`; no gap
   remains open.

Client recovery after the spec hardening (`docs/client-recovery-guidance-2026-09`,
reviewed at `b5a3a91`, implemented at the head of this branch; evidence in
`conformance/results/durable-work-v2-java-client-recovery-2026-09-09.txt`):

- Covered now against the new CR rows: CR01 in both documented modes (driver
  re-invocation of the one-shot launcher, and `--retry-budget`), against real
  pre-commit capacity refusals and real reply loss; CR09 for CONTROL_RESET and
  budget exhaustion; CR14 with the explicit stream-credit observation on both
  stacks (Rust authority measured by `RawPeerRustAuthorityTest`, Java by
  `DurableWireNegativeTest`); Section 12.1 diagnostics (`REFUSED code=…
  detail=…`, `client capabilities`, REFUSAL details naming the local bound).
- Covered after that, on the Java authority: CR04 (lookup NOT_FOUND while
  the original admission is genuinely pending before commit, then one effect
  and identical receipts; `ClientRecoveryTest`), CR10 (real journal I/O
  failures before transmission, while saving a receipt and while saving a
  verified selection; `ClientJournalFaultTest`), CR06 (temporary owner-policy
  withdrawal and restoration, untrusted and regressed clock, durable
  revocation kept distinct; `AuthorizationClockRecoveryTest`).
- Still open, not claimed: CR11/CR12 (pacing measurements, Meta's workload
  territory), CR13 over the wire (keepalives during a stalled transfer), and
  the Rust-authority side of CR04/CR06/CR10 (the Rust CLI has no commit-time
  hooks or injectable clock). None of these changes a wire behaviour.
- The three spec-text resolutions (12.1 credit replenishment sentence, 12.1
  local-diagnostic sentence, 12.2.1 one-shot paragraph, test-plan preamble and
  CR01/CR14 rows, disposition note outcome) are committed on the spec branch
  `docs/client-recovery-guidance-2026-09` at `7315713b`, and the follow-up
  correction (MAX_STREAMS cumulative per connection, refused-stream retirement
  as PipeStream's own rule, batching permitted, "returns to four" labelled as
  fixture evidence) at `ff901451`.

Listener behaviour corrected after Kimi's milestone 16/17 resource rows
(`0176855`, `7585a9dc`; see section 5, items 5 and 6): stream bounds are
judged before the connection; a durable-profile connection is never closed for
control silence; every deadline names itself in the REFUSAL detail or the
close reason; the host holds one query-only SQLite anchor so an idle authority
no longer rewrites the WAL index on every store call. Full offline run at
`7585a9dc`: 717 tests, 0 failures.

## 4. Proposed normative corrections and clarifications

1. **Declaring into a cancelled scope.** Section 12.5 says a late declaration
   answers CONFLICT; Section 12.6 leaves the cancelled-scope case unspecified.
   Both Java and the store answer `CANCELLED` (the scope's terminal fence),
   which is more informative than CONFLICT. Propose: "a declaration into a
   cancelled scope answers CANCELLED".
2. **Reading declared-only work.** A `READ` for work that is declared but has
   no attempt answers `NOT_FOUND` (no manifest exists yet). Propose making
   that explicit alongside the NOT_READY case for a running attempt.
3. **Interrupted input.** A reset or disconnected input stream answers
   `INTEGRITY_ERROR` "input stream interrupted" on the correlated `[1,
   streamId]` tag, matching the Rust authority; the declaration survives.
   Propose naming that code for "invalid FIN geometry" explicitly.
4. **Transport-enforced limits.** A second bidirectional stream and more than
   `dataStreams` concurrent unidirectional streams are refused by QUIC
   transport parameters (the client sees STREAM_LIMIT_ERROR locally), not by
   an application refusal. Propose stating that stream-count ceilings are
   transport parameters, so drivers do not wait for a LIMIT_EXCEEDED frame.
5. **Control FIN before DETACH** is a framing failure (`FRAME_ERROR`, QUIC
   application error 0x201) in both Java and Rust; pending responses are not
   delivered. Propose stating it in Section 12 next to the detach rule.
6. **Cancelled-scope declaration and replay codes** aside, no other refusal
   code disagreement was found between the Java and Rust reference endpoints
   in the exercised matrix.
7. **Oversized private-type frames (traceability D2, needs a spec call).**
   Section 12.1 says both "Validate lengths before allocating buffers" and
   "Private types 0xC0..0xFF require an activated defining profile; otherwise
   refuse EXTENSION_UNSUPPORTED". The Java decoder (`Wire.Decoder`) judges the
   declared length against the negotiated control limit before it classifies
   the type, so a private type whose body exceeds the limit is LIMIT_EXCEEDED,
   and only an in-limit private type is EXTENSION_UNSUPPORTED. The Rust
   reference was not compared on this input. Propose stating the precedence:
   "length validation precedes type classification; an over-limit body is
   LIMIT_EXCEEDED whatever its type". If the intent is the opposite (a peer
   probing profile support with a large private frame should learn
   EXTENSION_UNSUPPORTED), the Java decoder changes one branch. No code change
   until decided; `V2WireTest` will pin whichever code is chosen.
8. **Client-side seal mismatch on checkpoint (traceability D3, needs a spec
   call).** Section 12.8 gives the authority's answer to a checkpoint over a
   different seal (INTEGRITY_ERROR) and requires the client to verify identity,
   seal, count partition and commitments "before acknowledging coverage". The
   Java client also refuses *before sending* when its journal holds verified
   membership under another seal, with NOT_READY "sealed membership not
   verified for this seal", so the authority's INTEGRITY_ERROR is unreachable
   through this client for a scope it has verified. Two questions: may a client
   refuse locally (saving a round trip that can only fail), and if so which
   code names the local refusal (NOT_READY as today, or INTEGRITY_ERROR to
   mirror the authority)? Until decided the Java behaviour stays; the
   authority side is covered by `SessionStoreTest`, and
   `DurableBranchTest`'s comment at the checkpoint call is corrected to say
   the refusal is local.
9. **Overflow codes (traceability D7, resolved without a spec change).**
   Section 12.9's "Checked arithmetic overflow is LIMIT_EXCEEDED before
   commitment" is met: every store-side deadline and capacity sum
   (`AdmissionStore.add`, `InputStore.add`, `RetirementStore` cutoff) refuses
   LIMIT_EXCEEDED. `Checks.sum` raising FRAME_ERROR is reached only from
   decoding peer-supplied aggregates (`Records.Counts`, manifest output
   totals), where a value outside the schema range is a framing violation under
   Section 12.2. `V2WireTest` should pin FRAME_ERROR there explicitly; the
   traceability document records the mapping.

## 5. Defects found

1. **Transport (native): refused inputs leak MAX_STREAMS credit.** Upstream
   quiche drains a stream after a local `stream_shutdown(Read)` and never
   collects a peer-initiated unidirectional stream once the peer's
   RESET_STREAM (its mandatory reply to STOP_SENDING) or a late FIN completes
   it, so every refused or abandoned object stream permanently consumes one of
   the peer's stream slots until the connection closes. Reproduced red in
   three new quiche unit tests, fixed in the `pipestream.4` bundle (collect
   drained streams on completion; Netty releases the closed channel from the
   parent's stream map; a Netty credit test drives the whole path). Also
   affects Rust client → Java server after `dataStreams` refusals.
   `DurableWireNegativeTest.stalledInputsExpireWithoutBlockingAHealthyConnection`
   was red on `.3` and is green on `.4`; it is the Java-level regression for it.
2. **Java: finished input and result stream channels were never closed** after
   release, which kept Netty channel state alive per transfer; fixed in
   `b13995b` (`stream.close()` after release in server input and client result
   paths).
3. **Java hooks (from Kimi's review):** `*_SENT` recorded before settlement,
   settle callback ungated on write success, replay admission reply bypassing
   withhold, and a fabricated REFUSAL_SENT on drop-reply. All fixed in
   `b13995b`.
4. **Kimi/Meta defects investigated:** none reproduced against the Java
   endpoints in this window; Kimi's g1 runs are byte-identical across all
   three directions.
5. **Java listener: control silence closed live connections.** The durable
   listener advanced its idle-control clock only on inbound control frames
   and judged it before any stream bound, so under the neutral
   stalled-principal row it closed the whole connection at the idle bound
   while it was itself holding a granted watch, the per-stream refusals
   Section 12.1 requires were never readable, and a long upload with nothing
   to say on control would have been cut at the control deadline. Fixed in
   `0176855` (stream bounds first, clock gated on outstanding work, named
   reasons) and `7585a9dc` (durable-profile connections are never closed for
   control silence; the Rust reference keeps them, Section 12 requires no
   silence bound on a live connection). Regressions:
   `controlSilenceIsIdleOnlyWithNothingOutstanding` and
   `stalledPrincipalIsRefusedPerStreamOnASurvivingConnection`, both red on
   `d7ba2e0`.
6. **Java store: WAL index rebuilt on every store call.** Every store
   operation opened its own SQLite connection, so as sole opener it tore the
   WAL index down and rebuilt it (32 KiB) four times per 50 ms scheduler tick
   with no client connected: about 2.5 MiB/s of write accounting, almost all
   cancelled page-cache writeback, plus the matching allocation churn. Fixed in
   `0176855` by one query-only anchor connection held by the host (host-scoped
   because the native guard has 64 process-wide slots). Regression:
   `DurableHostIdleWritesTest` measures `/proc/self/io` over an idle window
   (7,602,176 bytes in 3 s without the anchor). The Rust authority shows the
   same signature under the same row (about 11.5 MB/s, 98 to 99 percent
   cancelled); raised with Meta on the board as a question.

7. **Java client: control silence judged against the bare control deadline.**
   `DurableClient.check()` failed its own connection with LIMIT_EXCEEDED
   whenever no control frame had *arrived* for `controlTimeoutMs` (30 s),
   regardless of whether anything was outstanding and regardless of the wait a
   pending WATCH or CHECKPOINT legitimately carries (up to 30000 ms, Section
   12.6). A client issuing the maximum legal wait therefore killed its own
   connection at almost exactly the moment the conformant response was due,
   and a durable connection held quietly with nothing outstanding died after
   30 s. Found by the Section 12 clause-level traceability pass
   (`docs/standards/section-12-java-traceability.md`, defect D1). Fixed in
   `ab59dafb`: the activity clock is renewed on send as well as receive; a
   durable connection with nothing pending is never failed for silence (the
   listener already keeps such connections since `7585a9dc`, and the transport
   idle timeout bounds a dead peer); a pending request is due within the
   control deadline after its own wait, and the failure is named
   `control response deadline`. Regression:
   `DurableClientControlDeadlineTest` (three cases: idle silence, a delayed
   answer inside a long wait, a withheld answer bounded and named), red on
   `1e7d25a7`.
8. **Java client: contradicting scope page journaled before it was
   cross-checked; parent view never checked against retained child scopes.**
   `DurableClient.page` called `journal.observePage` and only then compared the
   page's parent against the retained parent view, so a page contradicting a
   validated parent admission was refused INTEGRITY_ERROR but had already become
   a durable observation (Section 12.8: the client MUST NOT replace prior
   validated commitments with the contradictory observation). In the other
   order, `DurableClient.relationships` checked a view only against the scope
   it names as its child, never against retained scopes that name the view as
   their parent, so a parent view allocating child scope 6 was accepted after
   scope 5 had been paged with parent = that work. Found while writing the
   tests for S12-292 to S12-295 (traceability gap list). Fixed in `0a088050`:
   the page is cross-checked against retained evidence before it is journaled,
   and a view is checked against every retained scope naming it as parent
   (`ClientJournal.childScopes`). Regression: `DurableClientContradictionTest`
   (two tests, both orders, membership and producer contradictions), red on
   `ab59dafb`.

Kimi's milestone 17b question (2), the per-stream abort of stalled inputs
landing between idle+10 s and lifetime+10 s instead of at the 30 s idle bound,
is answered and is not a Java defect. A timestamped reproduction (Java
listener built with per-stream `System.err` diagnostics, the driver's raw
client run under a `quinn` trace subscriber, `r-stalled-principal-progress`
against both subjects) shows:

- Java refuses each stalled input at idle + 0.1 s after its last payload byte
  (`sinceProgressMs=30094`; REFUSAL frames on control at +30.03 s from the
  headers) and its STOP_SENDING with code 0x204 (516) leaves in the next send.
  The Java-side regression `stalledPrincipalIsRefusedPerStreamOnASurvivingConnection`
  now times each refusal from the last stalled byte (all three within idle + 1 s
  on a 3 s idle bound) and shows that a one-byte write on each stalled stream
  completes before the bound and no longer makes progress after it.
- The driver's raw client runs a tokio `new_current_thread` runtime that is
  driven only inside `block_on` calls, and its enforcement loop sleeps between
  probes outside the runtime; the `quinn` endpoint driver, which receives
  packets and marks the send stream stopped, therefore runs only in bursts
  (inbound activity during seven isolated seconds of a five-minute run; the
  same STOP_SENDING for streams 6/10/14 retransmitted about twenty times and
  all processed within one millisecond). A probe whose `write_all` completes
  from local credit never yields to the driver, so the abort is observed only
  at a later `block_on` that actually waits. The probes also write payload
  bytes, which are progress and renew the idle clock when they land before
  the bound.
- Consequence for the row: drive the runtime continuously (multi-thread
  runtime, or the current-thread runtime's `block_on` on a dedicated thread)
  and make the probes non-writing (`SendStream::stopped()` with a bounded
  wait) at idle+2 s, +5 s and +10 s. Sent to the milestone 18 driver run in
  Kimi's worktree; the conformance crate is the only thing that changes.

## 6. Build and verification commands

```
# Java (focused), against the isolated transport repository
mvn -o -Dmaven.repo.local=<transport repo> -Dtest='Durable*Test,FixtureMainTest,V2MainProcessTest' test
# Rust-peer suites (RawPeerRustAuthorityTest writes target/rust-stream-credit-observations.tsv)
mvn -o -Dmaven.repo.local=<transport repo> -Psealed-interop -Dtest='RustClientJavaServerTest,JavaClientRustServerTest,RawPeerRustAuthorityTest' test
# Client recovery modes, journal faults, authorization/clock recovery through the launcher
mvn -o -Dmaven.repo.local=<transport repo> -Dtest='ClientRecoveryTest,ClientJournalFaultTest,AuthorizationClockRecoveryTest' test
# Strict changed-type doclint (exit 0 at 18f3728, 21 types)
javadoc -quiet -package -Xdoclint:all -Werror -sourcepath implementations/java-netty/src/main/java \
  -classpath "$(mvn -o -Dmaven.repo.local=<transport repo> dependency:build-classpath -Dmdep.outputFile=/dev/stdout -q)" \
  $(git diff --name-only 8eb5a17 HEAD -- implementations/java-netty/src/main/java)
# Native transport rebuild (needs BENCHMARK.lock then NATIVE-BUILD.lock)
bash implementations/java-netty/transport/build.sh
```

## 7. Merge readiness

Three branches carry this window's work, all based on `8eb5a17` on
`feat/durable-work-results-v2`, none pushed, none rebased:

| branch | tip | content |
|---|---|---|
| `docs/client-recovery-guidance-2026-09` | `ff901451` | spec text: client recovery guidance, MAX_STREAMS correction |
| `agent/rfc-claude-java-v2` | `0a088050` (code; this document follows on the same branch) | Java V2 durable authority, listener and client |
| `agent/rfc-kimi-neutral-v2` | `73766f6a` | neutral conformance driver; contains `7585a9dc` by merge |

`git merge-tree --write-tree feat/durable-work-results-v2 <branch>` reports
no conflicts for any of the three, and the spec branch shares no changed file
with either agent branch. Suggested order: spec branch, then this branch, then
Kimi's (its merges of `0176855` and `7585a9dc` make this branch an ancestor,
so the Java files arrive once). The merge itself is the coordinating owner's
action; nothing here performs it.

