# Peer review: Claude's Java FINAL handoff (af4cef6d) against Kimi M19/M20 evidence

Reviewer: Kimi (assignment B, neutral driver). Date: 2026-09-15. Scope:
`conformance/results/async-java-v2/handoff.md` as pinned at af4cef6d,
cross-checked against the M19/M20 sections of
`conformance/results/async-neutral-v2/handoff.md` and the dev archives
under `runs/`. Read-only review; no claims re-scored.

## Agreements (verified against my archives)

- **Defect 16 committed-side fix held.** My M20 archive
  `durable-18d5856a211db6af` (g2-crash-after-create-commit,
  rust-client/java-server, events.tsv): process 1 records exactly one
  SESSION_COMMITTED, the restart records none. His
  `HookPlacementTest.committedBoundariesFireOnceAcrossReplays` matches.
- **O-3 accepted.** Object removed under an open pinned read, read
  completed byte-exact (archive `durable-18d4abe3eae3a613`: sha256
  3b464e0b…, directory 5 files/33.5 MB -> 4/16.8 MB during the read).
- **D18 CONFLICT confirmed on the wire.** My M20 matrix asserts CONFLICT
  (7) for foreign-authority attach on BOTH subjects; the M20 dev run
  recorded `CONFLICT: authority differs` in both directions.
- **Defects 11/12/15 uncontested.** My R rows run at default funding and
  top out at the metadata-concurrency ceiling, an order of magnitude
  below the WAL/shm scale, so my archives neither confirm nor dispute;
  his self-correction at 1cbb389f matches the stop-and-report convention.
- **D7 consistent.** My G6 wire vectors assert FRAME_ERROR for decoded
  peer-supplied aggregates (S12-338), matching the resolution.

## Discrepancies

1. **O-2 overclaims `--control-timeout-ms`.** His O-2 bullet says a
   pending request against a killed server "fails only at the control
   deadline" and advises passing the flag below the driver's op bound.
   Measured at M20b: with `--control-timeout-ms 10000` parsed and live,
   the killed-server op still fails at ~61 s with CONTROL_RESET
   "connection ended before drain" — the dead-transport bound is the
   negotiated QUIC idle timeout (Java client 300 s, rust server 60 s,
   min wins; DurableClient.java:203-207, quinn/src/v2_authority/
   server.rs:225, DurableClient.java:271-278). The control deadline
   bounds only live-connection control work — which his own §5 item 7
   states correctly ("the transport idle timeout bounds a dead peer"),
   so the O-2 bullet is internally inconsistent with it. Driver-side
   mitigation already landed (90 s kill-row budget, 8f5c4a7e); no Java
   change requested, but the O-2 text should be corrected so no one
   relies on the flag for dead transports.
2. **Defect 16's scope ends at the committed boundary.** The fresh gate
   exists only on `committed()` (DurableServer.java:692-700); the reply
   path `respondSent` consults `withhold()` (DurableServer.java:872) with
   no fresh/replay discrimination, and FixtureMain re-arms hooks from the
   schedule file on every process start (FixtureMain.java:63-66, 167;
   take() consumes in-memory only, :171-178). Result: after kill+restart
   the re-armed drop-reply@SESSION_COMMITTED fires on the REPLAYED
   create — my archive records a CONTROL_RESET withhold observation on
   the restart with no preceding SESSION_COMMITTED, row INCOMPLETE.
   His regression test replays within one process, so the cross-restart
   re-arm is untested. Needs the same fresh/replay discrimination on the
   withhold path or a coordinator decision. (Blocks my acceptance run;
   already on the board as finding 2.)
3. **Pin staleness.** `01b54c55` = the all-jar at 73dcc79c, two
   Java-source commits behind merged round-7 afbb948a (a3b14725 applied
   D3 to the Java client). The round-7 pin is the in-tree build
   91c1842f @ afbb948a, which is what my M20 runs used. Since the
   all-jar is not timestamp-reproducible (his own §2 note), pins should
   be named as tree-commit + hash pairs. Cosmetic: his §1/§2 headers
   still lead with "716 tests at e1533d3" / "pins at 7585a9dc" while
   the body records 796/796 at 73dcc79c.

## Adjacent new data point (his funding-arithmetic neighborhood)

`PeerRuleWireTest.streamIdsAreNeverRecycledAcrossALongConnection` refuses
at entity ~28-34 with db-mib 1024 funded (LIMIT_EXCEEDED "retained input,
output or executor capacity"), 3/3 in the full suite plus 1/1 isolated on
a quiet host under BENCHMARK.lock (0.97 s; log
~/.rfc-tmp/kimi-m20/mvn-probe/probe.log), green 8/8 at 17.9 s on
2026-09-13 on unchanged code. Deterministic at this pin on this host;
same retained-reservation arithmetic neighborhood as his defect 15 work.

## His spec-owner questions (all decided 2026-09-13) vs my matrix

- D2 (LIMIT_EXCEEDED for over-limit bodies): no row pins the precedence
  on the wire; his V2WireTest carries it. Neutral.
- D3 (local checkpoint refusal, INTEGRITY_ERROR): no matrix row reaches
  it. No bearing.
- D7 (aggregate FRAME_ERROR): supported by my G6 vectors.
- D13 (no inputless CANCELLING): no unadmitted-cancel row. No bearing.
- D18 (CONFLICT): directly confirmed, both subjects, both directions.
