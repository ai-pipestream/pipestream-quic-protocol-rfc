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
| `a700f5f4` | store-level Section 12.6 tests: a replacement lease already expired at the final time observation is CONFLICT and commits nothing (`ExecutionStoreTest`); a cancellation fence on a STRICT parent keeps precedence over a later child failure, excludes new descendants and their outcome commits, and settles them through the bounded cascade (`ClosureReconciliationTest`) |
| `0d8ec7bf` | wire-level scope paging (`ScopePagingWireTest`): a 300-member sealed scope walked with `after-entity` and `more` over real QUIC, a mid-range page, a page past the end, an unsealed scope that grows after an empty page and seals only on the sealing declaration (S12-288 to S12-290); the 18 refusal codes pinned to the Section 11.10 registry with reserved and out-of-range values mapped to FRAME_ERROR (`RefusalCodeRegistryTest`, S12-072). The paging fixture reproduces the C15 storage bound: the default file policy refuses the first 256-member declaration with LIMIT_EXCEEDED `SQLite file capacity exhausted`, so the test funds the authority through `V2Main.configuration` (`db-mib` 1024, `wal-mib` 256) |
| `c9edaed8` | wire and client gap tests: OUTPUT_UNAVAILABLE over real QUIC after the installed object is deleted, with the SUCCEEDED outcome and manifest unchanged, and a revoked session refused UNAUTHORIZED on a fresh `Read` with no pending read (`ResultDeliveryWireGapTest`, S12-265, S12-277); capability profile lists bounded at 32, strictly increasing, in range, on both lists (`CapabilityListBoundsTest`, S12-028); a manifest locator naming another endpoint is never dereferenced or sent credentials, the read stays on the configured connection (`DurableClientLocatorTest`, S12-280, S12-283); credential expiry closes the bound connection UNAUTHORIZED, the expired certificate cannot return, and a longer-lived certificate for the same principal attaches to the same retained generation (`CredentialExpiryReconnectTest`, S12-098) |
| `4cb4b444` | listener fix for defect 9: an input's stream and pending slots are released when its admission response or refusal is sent, not when the retained storage-cleanup owner closes (`DurableRequests.Ticket`), so a peer that read the refusal and was granted transport credit is not refused LIMIT_EXCEEDED on its retransmission; found when slow fsync made `DurableWireNegativeTest.stalledInputsExpireWithoutBlockingAHealthyConnection` fail deterministically; regression `DurableRequestsTest.anInputSlotIsReleasedWithItsResponseNotWithItsCleanupOwner`; the wire test now names the drained control messages on failure; new jar pin (section 2) |
| `ce1bfd77` | listener fix for defect 10: an installed input is pinned against orphan reclamation until its admission transaction ends (`InputStore.Receiver.finish(now, true)`, `Stored.release`, `inputInUse`), so a retention sweep running during the admission reports PINNED instead of deleting the object and refusing NOT_READY `complete validated input is unavailable`; root cause of the `admit-notready` rows in Meta's C16 cells; reproduced with the INPUT_INSTALLED boundary park (`InputInstallReclaimRaceTest`, red before the fix) and pinned at the store (`InputStorePinnedInstallTest`); gate: the targeted 54-test run is green, the full 744-test run under host load average 32 had four timing failures in unrelated classes that fail identically on 4fe9fff2 under the same load (A/B in a scratch worktree); clean gate: full offline `mvn test` 744 tests, 0 failures at `2ce4d318` (same Java tree; `raw/full-offline-2026-09-12.summary.log`) with the JVM temp directory on the host's root drive, after two runs on `/work` (`raw/full-offline-2026-09-11f.summary.log`, 21 failures) failed only in the lease-interval and bounded-deadline classes because the RAID drives cost 31 ms per fsync after a small write (root cause, measurements and the mitigation in `raw/host-fsync-2026-09-12.md`); new jar pin (section 2) |

Working tree at `a700f5f4`: clean. Nothing pushed (no push authorization was
given); no CI exists for this branch; no draft/deploy action taken; the
shared feature branch and main were not merged.
| `4fb8f747` | authority fix for defect 11: the session write-ahead log is restarted with a truncating checkpoint by the retention service once it exceeds one sixty-fourth of its bound (`SessionStore.restartLog`, `RetentionService`, `DurableHost.Status.logRestarts`), because SQLite restarts it only when no reader holds it and a continuously polled authority never reaches that moment; root cause of Meta's C16a xlarge64 mixed refusal LIMIT_EXCEEDED `SQLite file capacity exhausted` after 4 declare batches and 333 admissions at 1024/256 MiB funding (Meta request 3); reproduced with six unpaused readers over 100 work units (`SessionLogGrowthTest`: 12.7 MiB before, under 8 MiB after, restarts counted); gate: 108 tests in 30 store, retention, host and wire classes green, full run queued behind the benchmark lock; jar pin unchanged until the rebuild (section 2) |
| `ada67cec` | reference-application fix for defect 12: `chunk-copy/v2` admitted every expansion child with a fixed 1,000 ms execution duration, so a child admitted just before an authority restart expired while the process came back (Kimi driver `g3-restart-same-roots` rust-client/java-server on `28c3369b`: child 2:1:3 FAILED `execution deadline reached`, STRICT parent FAILED); children now carry the parent's execution duration through `DurableHost.Production.executionMs`, as the Rust reference does; `DurableBranchTest` asserts it on every child (red before at 1,000 ms); full offline suite 745/745 (`raw/full-offline-2026-09-12b.summary.log`); new jar pin (section 2) |
| `52d4776d` | `HookPlacementTest`: boundary hooks pin four ordering clauses deterministically (S12-221 fence-first and publication-first, S12-208 control progress with a parked callback, S12-158 input-store usage unchanged at the refusal, S12-311 checkpoint wait counted from acceptance behind a parked storage worker); traceability 296/62/8/7 at `c7c059a4` |
| `62412c14` | `LossyTransportCreditTest`: a seeded UDP relay drops 8% and reorders every seventh datagram in both directions while 2n+1 transfers (refused, reset, admitted) run on an allowance of n; no stream-limit failure, correlated refusals, credit never below n/2, injection counted (about 190 drops and 90 reorders per run); observation O-1 recorded (section 5); S12-046 covered, S12-011 left PARTIAL by decision; traceability 297/62/7/7 |
| `dce9d384` | `ResultAbortWireTest` (a RESULT_HEADER_SENT hook truncates the published object: the result stream aborts without FIN, no second control response, work still SUCCEEDED; S12-079) and the no-wrong-direction-refusal assertions over every frame the raw authority recorded (S12-078); S12-056 recognised as covered by the ceiling test's transport-close assertion; traceability 300/60/6/7 |
| `6174d92d` | `DurableClientContradictionTest`: a child scope naming an unadmitted parent stays pending evidence (no synthesized parent commitment) and is concluded only by the parent's own view, for and against (S12-294); traceability 301/60/5/7 |
| `c115e292` | `DatagramRelay` test helper (loss, reordering, mid-session rebind) shared by `LossyTransportCreditTest` and the new `MigrationWireTest`: the same session continues under the same owner after the peer's packets arrive from a new address, nothing re-authenticated (S12-010); `V2TlsTest` asserts no follow-up attempt after an ALPN refusal (S12-005); S12-008 (0-RTT) refined as proposal P-TLS-1 |
| `3f8846e2` | thirteen store-level traceability partials closed by unit tests written in a separate worktree and reviewed here (P-STORE-3, 5 to 16; each confirmed red by inverting its load-bearing assertion): S12-100, 112, 169, 170, 171, 177, 206, 218, 219, 226, 247, 249, 251, 345, 346, 347, 355, 361; `ResultFixture` takes a policy; S12-248 not producible (D13, spec-owner question); traceability 321/43/2/7 |
| `7583b00f` | `SchemaBoundsAndRegistryTest`: work-view nullable sweep with null and boolean refusals (S12-017), the 256-id declaration bound (S12-145), the nine-state integer registry (S12-193), the WORK operation numbers 8 to 11 read off encoded frames (S12-215); traceability 327/37/2/7 |
| `aa05d201` | `PeerRuleWireTest` (raw peer and raw authority): ignorable frame activates no profile (S12-024), a hundred never-reused stream ids (S12-068), no second refusal for a replayed admitted input (S12-077), unrecognised close code releases slots without implying success (S12-082), client drain close is application error 0 observed at the authority (S12-321); traceability 332/32/2/7 |
| `d26c5a92` | receiver-side abort of a stalled result read releases the listener's read with no control response (S12-076); 30 000 ms wait bound at the schema (S12-239); client makes no discovery call from a bare locator (S12-284) and keeps delivered bytes across a later revocation (S12-285); traceability 336/28/2/7 |
| `ea55c57d` | CLIENT FIX defect 14: the client sent an input on the caller's word that a declaration covered it (the journal checked only the session generation); a first send is now refused NOT_READY unless the named declaration's receipt is held for the input's scope and producer, resends unaffected (S12-150, red before at a raw authority); the raw test authority answers declarations with genuine receipts; reconnect offers require every journaled profile (S12-038) |
| `54267a72` | second store-level round from the agent worktree, reviewed and cherry-picked (P2-STORE-1 to 3, 5 to 11, 13 to 15, P2-WIRE-5; each confirmed red): S12-114, 139, 140, 141, 199, 204, 227, 237, 240, 263, 276, 278, 300, 316, 348, 353; S12-164 not producible (child scope row is inserted in the parent's admission transaction); D4 and D6 closed; traceability 354/10/2/7 |
| `77ee4830` | `PeerRuleWireTest`: a 1 MiB object over a 64 KiB stream window both ways with bounded credit (S12-041); control answered within a second with every data slot held open, then every slot returned (S12-043, S12-044, S12-045); traceability 358/6/2/7 |
| `1cbb389f` | LAUNCHER FIX defect 15: `--wal-mib` above about 257 MiB was capped by the fixed 512 KiB shared-memory sidecar that indexes the log, so Meta's xlarge64 cell refused at 332 admissions on every jar; the sidecar now scales with the funded log (`BoundedSqlite.Limits.sharedMemoryFor`, 16 MiB ceiling); `FundingScaleTest` (arithmetic plus 2 000 declarations over the wire) |

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
  listener or launcher changes again. Superseded at `4cb4b444` (listener request
  tracker: a refused or answered input releases its stream slot with its
  response, defect 9; wire behaviour otherwise unchanged): lib jar `5a658ac1ca61f13b43dce42b46f513fa4b508110c44e178dbb0d8336b8ea0f59`,
  shaded all-jar `02a410fc711a038e729305db5fdaae4cd80b809a220b8e2be6cdbb6e0c1bc014`.
  Superseded at `ce1bfd77` (listener: an installed input stays pinned through its
  admission transaction so a retention sweep cannot reclaim it, defect 10; wire
  behaviour otherwise unchanged): lib jar `a2d98870649a479347d437511df55fa7a63fc656232e0e21732ba72af628a722`, shaded all-jar `28c3369bd95210ab50e3e3fc9026c5150a9fc91bac1810b796a23adfe19b002c`.
  Superseded at `ada67cec` (authority: the session log is restarted under
  continuous readers, defect 11; reference application: chunk-copy children
  carry the parent's execution duration, defect 12; wire behaviour otherwise
  unchanged; the tree passed 745/745 before the build): lib jar `03f8c85b29469563075f96fa1d52e582aa9ebd4d0a9f44a00fdf7a811a032b1f`,
  shaded all-jar `32360ec3dbff58a1581c9b64f8afca32dfe7b6c42c49bf5c43d6fad64d19aa7c`.
  This pin still cannot run Meta's xlarge64 mixed cell: its launcher caps the
  usable log at about 257 MiB (defect 15); the next rebuild, at `1cbb389f` or
  later, supersedes it and needs `--wal-mib 512` or more for that cell.
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

9. **Java listener: a refused input kept its connection stream slot until
   storage cleanup finished, after the peer had already been granted transport
   credit.** `DurableServer.InputTransfer.refuse` closes the QUIC stream (so
   quiche collects it and the peer receives MAX_STREAMS credit) and sends the
   correlated REFUSAL, but the `DurableRequests` input ticket was released only
   when the retained cleanup owner closed, after `InputStore.Receiver.close`
   (unlink and fsync of the partial input) had run on a storage worker. A peer
   that read the refusal and retransmitted on the fresh credit was refused
   LIMIT_EXCEEDED `connection input or request capacity exhausted` although
   the negotiated stream bound (Section 12.1, `stream-limit`) had a free slot.
   The window is the storage cleanup latency; it was invisible while fsync on
   the RAID-0 NVMe took about a millisecond and became deterministic on
   2026-09-11 when fsync rose to 35-75 ms (`DurableWireNegativeTest.
   stalledInputsExpireWithoutBlockingAHealthyConnection` failed five runs in
   a row on the committed tree; a diagnostic build reported
   `pending=2/64 inputs=2/2` with both refused inputs still counted). Fixed in
   `4cb4b444`: an input's primary ticket (the response sender) releases the
   stream and pending slots when the admission response or refusal has been
   sent; retained owners keep the binding for cleanup only. Control requests
   keep their capacity until the last owner releases it. Regression:
   `DurableRequestsTest.anInputSlotIsReleasedWithItsResponseNotWithItsCleanupOwner`
   (deterministic, no storage involved), plus the wire test under slow
   storage. Listener wire behaviour otherwise unchanged; new jar pin below.
10. **Java listener: an installed input under admission could be reclaimed
   as an orphan by the retention sweep.** `InputStore.Receiver.finish` links
   the complete validated object into place and then closes the receiver,
   which drops the object's physical ownership; the admission transaction
   (`SessionStore.admit` -> `AdmissionStore.admit` -> `InputStore.find`) runs
   afterwards on the storage worker and fsyncs several times. In that window
   the object has no admission record and no reader or receiver, which is
   exactly the retention sweep's orphan criterion (`OrphanStore.reference`
   not LIVE, `InputStore.inputInUse` false), so `reclaimOrphan` deleted it and
   the admission refused NOT_READY `complete validated input is unavailable`
   for an input the peer had fully transmitted. Meta's C16 coordinator
   observed it 24 times across its cells as `admit-notready` rows (all mixed
   arms, all on the first admission of a chunk, each retried successfully)
   and classified it as backpressure; it is a listener defect. Reproduced
   deterministically with the Boundaries hook parking the transfer at
   INPUT_INSTALLED while the host's retention timer sweeps every millisecond
   (`InputInstallReclaimRaceTest`, red before the fix with the identical
   refusal). Fixed in `ce1bfd77`: `finish(now, true)` pins the installed object
   (`installedPins`, consulted by `inputInUse`) and the transfer releases the
   pin in a `finally` after the admission transaction; the sweep now reports
   PINNED for such an object. Pins are in-memory, so a crash between
   installation and commit still leaves a reclaimable orphan (S12-365).
   Store-level regression `InputStorePinnedInstallTest`. Listener wire
   behaviour otherwise unchanged; new jar pin below.

11. **Java authority: the session write-ahead log grew until its bound under
   continuous readers.** SQLite restarts the WAL only when a writer finds no
   reader holding it. Meta's pipelined coordinator polls without pause, so
   the Java authority never saw that moment and every commit appended to the
   log; the fixed-record model reserves the retained promises' share of the
   WAL (`FixedRecords.install` -> `walCeiling(usable - retained)`), so as
   declarations and admissions accumulated the permitted length shrank while
   the actual length grew, and the next write was refused LIMIT_EXCEEDED
   `SQLite file capacity exhausted` with the database itself nearly empty.
   That is Meta's C16a xlarge64 mixed refusal after four declare batches and
   333 admissions at 1024/256 MiB (its request 3, "raise or expose Java
   storage funding for >=341-entity scopes"): the funding was sufficient, the
   log was never restarted. Reproduced with six readers paging without pause
   over 100 copy/v2 units (`SessionLogGrowthTest`, red before the fix at
   12.7 MiB of log). Fixed in `4fb8f747`: `SessionStore.restartLog` runs
   `PRAGMA wal_checkpoint(TRUNCATE)` from the retention sweep once the log
   exceeds one sixty-fourth of its bound (at least 1 MiB); it waits, within
   the busy timeout, only for readers already on the log, later readers use
   the database file, and a busy checkpoint is not counted. `DurableHost.Status`
   gains `logRestarts`. Funding knobs are unchanged; 1024/256 MiB funds the
   xlarge64 cell. Jar pin at `ada67cec` (section 2). Correction at `1cbb389f`:
   the log restart is real and stays, but it was not what refused Meta's
   xlarge64 cell. That cell refuses at the same count (332 admissions) with
   and without this fix, with the store nearly empty and no log file present:
   the bound is the shared-memory index cap, defect 15 below, and 1024/256 MiB
   never funded more than about 257 MiB of usable log.

12. **Java reference application: chunk-copy children could not survive an
   authority restart.** `ReferenceApplications.chunkCopy` admitted every
   expansion child with a fixed 1,000 ms execution duration. In Kimi's driver
   row `g3-restart-same-roots` (rust-client/java-server, `28c3369b` jar, run
   `durable-18d489eaaffc93e8`) the seeded kill landed while the expansion was
   admitting children; child 2:1:3 was admitted 1,000 ms before its deadline
   and the restarted process could not claim it in time (deadline 13 ms before
   its terminal write), so the recovered authority settled it FAILED
   `execution deadline reached` (11) and the STRICT parent 0:0:3 FAILED
   `STRICT child scope contains non-successful work` (7): exactly the
   "restart fabricated a failure outcome" the row forbids. Verified by decoding
   the archived authority store (all four children, the parent and their job
   records). Earlier runs of the same seed passed because the kill landed
   before or after the vulnerable window. The Rust reference gives children
   the parent's execution duration (`ExpansionContext::execution_duration`).
   Fixed in `ada67cec`: `DurableHost.Production.executionMs` exposes the
   parent's fixed duration and chunk-copy admits children with it;
   `DurableBranchTest.authorityExpandedChunksProduceChildrenAndParentReassembly`
   asserts the inherited duration on every expanded child (red before at
   1,000 ms, green after). Full offline suite 745/745 at `ada67cec`; jar pin
   in section 2. Driver evidence: `raw/kimi-driver-acceptance-2026-09-12.log`
   and `.run.tsv` (the driver does not archive a run with a FAIL row).
   Rerun of `g3-restart-same-roots` on the `32360ec3` jar over seeds 24301,
   7, 99, 1234 and 4242: PASS on all three directions every time
   (`raw/g3-restart-rerun-32360ec3-2026-09-12.log`); the merged landing tree
   `b5af2b1d` (main + this branch + Meta C16) passed the 116-test targeted
   confirmation run.

14. **Java client: an input could be sent before its covering declaration
   receipt was held.** `ClientJournal.journalInput` checked only that the
   header named the journaled session; the declaration operation the caller
   named was journaled as a reference without checking that its receipt had
   arrived. An admission whose declaration had never been receipted therefore
   went to the authority and was refused there (CONFLICT, undeclared), which
   is the authority's rule but not the client's: Section 12.5 (S12-150) says
   the producer MUST receive its covering receipt before sending an input.
   Found by `AuthorityRuleClientTest.anInputIsNeverSentBeforeItsCoveringDeclarationReceiptIsHeld`
   against a raw authority that records input streams (red: the input
   arrived and the admission hung). Fixed in `ea55c57d`: a first send is
   refused NOT_READY `covering declaration receipt not held` unless the named
   declaration's receipt is held for the input's scope and producer; a resend
   of an already journaled operation is unaffected. Three tests that relied
   on sending undeclared inputs to reach authority-side behaviour now declare
   first or expect the local refusal; the raw test authority answers
   declarations with genuine receipts (the client's own digest, the seal when
   sealed). Wire behaviour of the authority is unchanged; the jar pin stays
   `32360ec3` for the server subject, and the client CLI in the same jar gains
   the check at the next rebuild.

15. **Java launcher: `--wal-mib` above about 257 MiB was silently ineffective.**
   SQLite indexes the write-ahead log through the shared-memory sidecar, in
   32 KiB regions of 4096 frames (4062 in the first); `V2Main` scaled the
   log bound with `--wal-mib` but kept the sidecar at the reference 512 KiB,
   which indexes about 257 MiB of log at the 4096-byte page. `FixedRecords`
   computes the usable log as the smaller of the funded bound and what the
   sidecar indexes, and reserves the retained promises' share of it (about
   86 KiB per rewrite credit, roughly 0.8 MiB per admitted unit with its job,
   view and fence records), so one authority topped out near 330 admitted
   units whatever the flag said. That is Meta's xlarge64 mixed refusal
   (`c4-xlarge64-seed6-r2/REPRO-DEFECT11.txt`: 332 admissions on jar
   `e1763b4a` and again on `32360ec3`, deterministic, store 4.3 MiB, no log
   file). Fixed in `1cbb389f`: `BoundedSqlite.Limits.sharedMemoryFor` sizes
   the sidecar for the funded log (never below 512 KiB, a 64 KiB multiple,
   never above the 16 MiB ceiling that indexes about 8 GiB) and the launcher
   applies it. `FundingScaleTest` pins the arithmetic and shows twenty
   hundred-member declarations refused under a 256 MiB log and all accepted
   under 2048 MiB. Sizing rule for Meta: fund about 1 MiB of `--wal-mib` per
   unit a session will hold at once, so xlarge64 (341 units per worker) needs
   `--wal-mib 512` at least on the next jar, and the retained file policy
   means a fresh root per funding. New jar pin below once the rebuild lands.

The 53-row driver run on `28c3369b` (2026-09-12, stores on the root drive)
otherwise matched the milestone 17b baseline: 52 rows PASS on every
implemented direction, the INCOMPLETE directions identical to the baseline
(named Java CLI gaps and the driver's Rust-only view parser for the Java
client direction), and one new INCOMPLETE that is not a Java defect:
`g4-stale-attempt-retry` rust-client/java-server answered ALREADY_TERMINAL
instead of CONFLICT because the driver waits until attempt 2 is live and
then spawns a client subprocess for the stale retry; on the fast store the
copy under attempt 2 finished first. Both authorities check terminal state
before the attempt mismatch (Rust `retry_work`: `eligible` precedes
"retry attempt changed"), so the row needs a way to hold attempt 2 live
(noted for Kimi's branch on the board).

Observation O-1 (transport pin, not a Java defect; recorded at `62412c14`): the
pinned quiche retransmits a lost RESET_STREAM only while the local stream still
exists (`quiche/src/lib.rs`, lost-frame handling: `if self.streams.get(stream_id)
.is_some() { insert_reset }`). A peer that resets an input after a partial
payload and lets its stream go at once can therefore lose the reset for good
under packet loss; the listener then refuses the stalled input at the
negotiated idle bound (LIMIT_EXCEEDED `input receive deadline`, 1 s at the
offered minimum, 5 s at the raw peer default) instead of at the reset, and the
slot returns with that refusal. `LossyTransportCreditTest` sees this on about
one reset in ten under 8% loss with reordering, and about once in a hundred
for a FIN-terminated (truncated) input whose tail is lost, which the same
guard does not explain and is left unattributed; it records the count per
shape and the latency per run in `target/lossy-credit-observations.tsv`. The Java client resets through the
same transport, so its resets have the same exposure; the credit reservation
(S12-046) is unaffected. A fix belongs in the transport pin (keep a reset
stream until its RESET_STREAM is acknowledged, or retransmit regardless), not
in the endpoints. Separately, one run in roughly ten of the first test shape
timed out waiting for a control response before the observations were logged
incrementally; the test now records the transfer and credit at that moment so
the next occurrence can be attributed.

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
| `agent/rfc-claude-java-v2` | `ce1bfd77` (tests and this document on the same branch) | Java V2 durable authority, listener and client |
| `agent/rfc-kimi-neutral-v2` | `73766f6a` | neutral conformance driver; contains `7585a9dc` by merge |

`git merge-tree --write-tree feat/durable-work-results-v2 <branch>` reports
no conflicts for any of the three, and the spec branch shares no changed file
with either agent branch. Suggested order: spec branch, then this branch, then
Kimi's (its merges of `0176855` and `7585a9dc` make this branch an ancestor,
so the Java files arrive once). The merge itself is the coordinating owner's
action; nothing here performs it.

