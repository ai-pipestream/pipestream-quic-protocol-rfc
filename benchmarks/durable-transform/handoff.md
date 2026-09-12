# Handoff: external durable workload + equivalent gRPC baseline (C)

Owner: Meta. Branch: `agent/rfc-meta-workload-v2`.
Base: `8eb5a17` (feat/durable-work-results-v2, contains `82a1b11`).
Worktree: `/work/worktrees/pipestream-muse` (standalone clone; origin fetches
the source path — see board for why it is not a linked worktree).
State at writing: code-complete, execution evidence pending. NOT review-ready.

## 1. Commits (this branch, oldest first)

- `67ecd0b` C1 fairness/application contract (`benchmarks/durable-transform/contract.md`)
- `85e7c5f` workload-core: generator, frozen transform, oracle, pinned digests
- `fe7d368` workload-authority: `transform/v2` on public library APIs
- `506caed` workload-coordinator: 3 sessions, journaled resume, verified assembly
- `92112a7` C3 gRPC baseline: proto + worker + coordinator
- `a22b799` C4 runner: quick/full/faults/PKI/measurement scaffolding
- `1f79818` review of Claude `api-plan.md` 2db1344 (§§1.1/1.5)
- `b91e916` reply to Kimi review; v1 schedule adoption; COMMIT markers

Dirty state: none (verify with `git status --short` at review time).
No pushes made (no push authorization); peer exchange is by local fetch.

## 2. Requirement → source → test → evidence map

| # | Requirement | Source | Test / gate | Evidence (actual) |
|---|---|---|---|---|
| C1 | frozen contract | `benchmarks/durable-transform/contract.md` | Kimi scoped review | `peer-review-meta-contract-c1.md` (Kimi worktree): no blocking mismatch |
| C1 | deterministic transform + oracle | `workload-core/src/lib.rs` | 9 unit tests (vectors, boundaries, determinism, alphabet, swap/wrong rejection, pinned digests) | typecheck clean (1.97.1 `--emit=metadata`); oracle digests cross-checked by independent hashlib mirror (empty `e3b0c4…`, tiny `9d2771…`, boundary `fe6f0a…`, b+1 `45768e…`); TEST EXECUTION PENDING |
| C2 | external authority + app | `workload-authority/src/main.rs` | typecheck vs host Sep-8 closure | clean, zero warnings; BUILD/RUN PENDING |
| C2 | external coordinator + recovery | `workload-coordinator/src/main.rs` | typecheck vs host Sep-8 closure | clean, zero warnings; BUILD/RUN PENDING |
| C2 | mixed Java/Rust run | needs Claude `transform/v2` on Java | — | BLOCKED (needs recorded in `reviews/claude-api-plan.md`) |
| C3 | equivalent gRPC baseline | `durable-transform-grpc/` (proto, worker, coordinator) | protoc codegen; API cross-check vs tonic 0.14.6 source; lib typecheck | codegen clean; generated names verified; lib logic typechecks clean; FULL BUILD PENDING (no prebuilt tonic-0.14 rlibs in sandbox) |
| C3 | equivalence reviewed | `reviews/kimi-interface-v1.md`, `schedules/fault-schedule-v1.tsv` | Kimi §8 review: sound, 2 observations | manifest field correspondence pinned; fsync accounting via COMMIT markers |
| C4 | quick/full runner + faults | `benchmarks/durable-transform/run-*.sh`, `libexec-*.sh`, `sample.sh`, `mk-test-pki.sh` | `bash -n` all clean; PKI executed + chain-verified | scripts parse; PKI fingerprints match TSV, openssl verify OK; RUNS PENDING |
| C4 | measurements + report | events/sample/net artifact formats frozen | — | NO MEASURED DATA YET |

## 3. Exact build/run commands (for the unblocked host)

```bash
cd examples/durable-transform-workload/workload-authority && cargo build --release --locked
cd ../workload-coordinator && cargo build --release --locked
cd ../../../examples/durable-transform-grpc && cargo build --release --locked
cargo test --locked -p workload-core
./benchmarks/durable-transform/run-quick.sh /tmp/dt-quick
REPO_BIN=/path/to/bin-dir ./benchmarks/durable-transform/run-full.sh /tmp/dt-full 5
```

Lockfiles are committed per crate. Toolchain note: repo `rust-version` is
1.88 but this host's Sep-8 reference build used stable 1.97.1; my typechecks
used 1.97.1 against that closure. `cargo build` on the host will confirm.

## 4. Dependency pins consumed

- Claude `api-plan.md` at `2db1344` (reviewed; 2 needs open).
- Kimi `interface-v1.md` at `1452f60`, schema `c566751a…` (acknowledged,
  schedules adopted).
- Normative: Section 12 + Appendix F at base; Rust `v2-cli.md` behaviors
  mirrored (journal reopen, replay-not-reinvent, select-before-read,
  checkpoint-before-complete).

## 5. Known gaps (explicitly incomplete, never passing skips)

1. No `cargo build` / `cargo test` / run output yet (sandbox denies linker
   and child-process exec; escalation unavailable). Everything above the
   typecheck/protoc/shell/PKI line is unverified execution.
2. Mixed Java/Rust functional run: needs Claude's Java `transform/v2` (or
   custom-registration confirmation) + RestartSafety mapping.
3. Final comparative labels need Kimi's driver; F3/F4 need boundary-armed
   precision; negative-case scripts (revoked reader, expired output,
   wrong-output, missing chunk) are capabilities, not yet runs.
4. No performance numbers exist; no comparison claim is made. Performance
   deltas, when measured, are findings, not grounds to weaken guarantees.

## 6. Execution evidence (2026-09-08, unblocked host)

- `cargo test --locked -p workload-core`: 10/10 pass (vectors, boundaries,
  determinism, alphabet, swap/wrong rejection, pinned digests, oracle match).
- All four release binaries compile warning-free (rustc 1.97.1).
- `run-quick.sh` (seed 7, 200,000-byte binary corpus, 4 chunks over 3
  workers per arm): **QUICK PASS**. Both finals SHA-256
  `e74beab6d449bef00d75890d49988673cf7a53b66ec53ddd94c5ab0c25bd362e`
  (200,000 bytes, byte-identical across arms). First-usable milestones on
  all workers both arms; per-worker COMMIT markers; final-verified both.
  Binary pins: authority `ea7cf584…`, ps-coord `fd1bbe9e…`, grpc-worker
  `e48b226d…`, grpc-coord `03321977…` (full hashes in run artifacts).
- Bugs found and fixed by real runs (not review): `receipt()` unknown-op
  semantics, manifest column location, shared-DB WAL contention, ceiling
  (not deadline) op identity, connect-retry, EXIT-trap cleanup.
- Host was shared (another agent building concurrently); these are
  correctness results, no performance claim.

## 8. Full execution evidence (2026-09-09, seed 6, 8 MiB corpus, 128 chunks)

- `run-full.sh /tmp/dt-full 5`: 10/10 arms green (5 ps + 5 grpc),
  every final SHA-256
  `b3dd5e34376dba8b6042c99fea2ab6e821045305e7ed00ff9a0cecfe92b0160b`
  (8,388,608 bytes, byte-identical across all arms and both fault
  recoveries). Milestones (`first-usable-output`, `final-verified`)
  present in all 12 event streams.
- Fault 1 (PipeStream, SIGKILL worker b mid-run + restart): coordinator
  rode through on transport reconnect with same-identity resends (no
  resume needed, no refusal rows); final byte-identical.
- Fault 2 (gRPC, SIGKILL coordinator + `--resume`): journal replay
  recovery (2nd `final-verified` row); final byte-identical, cross-arm
  `cmp` clean. **FAULTS PASS.**
- Bugs found by the pilot (fixed at C5-C7, proven by rerun, never
  committed broken): `date +%s%3N` emits nanoseconds here (wall_ms
  mislabeled; fixed with 13-digit truncation); `/^lo:/` never matches
  indented `/proc/net/dev` (loopback counters empty; fixed); authority
  capacity refusal killed the coordinator mid-burst (new
  `send_admission` same-identity backpressure retry, 240 attempts
  capped backoff, named codes in events; proven with throwaway
  ceiling=1 authority: 119 refusals, byte-identical final);
  `run-full.sh` never exported runner env to the faults phase (fixed)
  and now truly alternates A/B/B/A.
- Coordinator binary with retry: `ac94b2f1…` (full hash in run
  artifacts `bin.sha256`). Timing still shared-host; no performance
  claim.
- CR mapping (Kimi `cr-mapping.md` @ `50c1871`): CR11 (pacing) STRONG —
  `579defe` is exactly "admission receipts don't replenish executor
  credit": the coordinator rides finite capacity refusals with the same
  identity (ceiling=1 proof: 119 refusals, byte-identical final;
  rep-4-ps took 1 natural refusal the same way). Terminal-status vs
  retained-byte release PARTIAL: `watch_terminal` + `fetch_verified`
  read outputs only after terminal states and verify bytes, but nothing
  here measures release timing. CR12 (healthy progress under shared
  contention) WEAK: all repeats progressed to byte-identical finals on
  a loaded host, which is consistent with but not a measurement of
  per-principal progress guarantees.
- Contract §6 dead-collector control enforced: all three runners now
  fail the run when the metric sampler dies mid-run or any worker has
  no samples (proven: killed-sampler arm exits 1
  `metric sampler died mid-run`; clean arm passes).
- CR13 arms (`run-cr13.sh`, new `probe` subcommand), reopened with
  timing-window fingerprints + validated wire traffic (the earlier
  `observed_work` stall traffic was local journal reads, corrected to
  scope pages + watch polls with asserted responses) + session-survival
  gate + injected-cancellation negative arm: idle (3 s survives; 7 s
  stall dies ~7.1 s), lifetime (28 s progress dies ~30.0 s), complete
  (commits), cancel-neg (injected shutdown correctly rejected — the old
  code+window predicate would have passed it). PASS requires in-window
  failure with LimitExceeded/Cancelled, all other codes rejected, and
  a surviving session afterwards. Timestamps are first-observation
  brackets (alive, dead], not termination instants.
- CR13 status: CLOSED (C15, 2026-09-10, seed 6, base 85887911). Vs
  Rust all four arms PASS (unchanged, §8 above). Vs Java all four arms
  PASS against Claude `7585a9dc` (jar `61ab64a3…` hash-verified, copied
  into the run dir, no rebuild): idle (stall dies in-window,
  alive_at=3120ms dead_at=7068ms, session survives), lifetime (30 s
  kill lands in-window, `probe-stream-dead` Cancelled alive_at=187ms
  dead_at=30015ms, then `probe-session-survived`), complete (commits),
  cancel-neg (injected shutdown correctly rejected). Artifacts:
  `benchmarks/durable-transform/results/cr13-c15-java-seed6/`
  (`probe-events.tsv`, four worker logs, `MANIFEST.sha256` verified).
  The earlier divergence was measured at `63d03a0` (connection unusable
  after the 30 s kill) and is closed by `7585a9dc` (a durable-profile
  connection is no longer closed for control silence). Remaining open
  requests to the client-library owner (not gaps in this evidence):
  (1) a disable/extend knob for the idle/lifetime timers (no true
  disable-timer negative control exists without it); (2) causal
  deadline evidence (post-deadline writes surface Cancelled, exact
  death instants unobservable); (3) server-side enforcement and the
  download direction are untested by these arms.

## 9. C16 (2026-09-11): milestones, commits, run ids, gates, deviations

Branch `agent/rfc-meta-workload-v2`, base `8eb5a17`. Brief
`/work/worktrees/pipestream-rfc-coordination/MUSE-BRIEF-2026-09-11.md`.
No pushes (no authorization); no history rewritten.

- C16a (funded large cells): `69372f27` ladder v3 + jar pins + Java
  funding flags; `ac2d64f1` scope-page pagination; `cacdd1a0` large48 r2
  PASS 12/12 (digest matches C15); `7ddcd3be` xlarge64 PARTIAL (batched
  declares proven via b28b0b7b pin; mixed blocked on Java storage,
  2nd request to Claude open). Archives: `results/c4-large48-seed6/`,
  `results/c4-large48-seed6-r2/`, `results/c4-large64-attempts/`.
- C16b (negative controls): `ec8229e4` TEST-ONLY hooks (swap/drop/kill/
  nofetch/delay) + runner plumbing; `64ec8d96` green 31/31 + archive
  `results/neg-seed6/` (CELL.txt + MANIFEST.sha256 verified, state/PKI
  pruned). Every control: positive twin (exit 0) + injected run that
  must FAIL with a named reason (INVALID markers).
- C16c (boundary faults F3/F4): `f5a46589` green 15/15 + archive
  `results/boundary-seed6/`. Armed run dies at the boundary; restart on
  the same state dirs + coordinator resume completes byte-exact.
- C16d (stopped/slow): `3e6b0e10` green + archives `results/stopped-seed6/`
  (per arm 5 positive twins + 5 delay runs + 5 stall-read probes + no-fetch
  gate demo), `results/slow-seed6/` (worker-c at 1/10 pace, exactly-once
  chunks, exposed-vs-hidden slowdown per arm).
- C16e (pipelining): `5062e65e` green 72/72 + archive
  `results/pipeline-seed6/` (602 files, MANIFEST verified, 9.2G -> 2.1G
  pruned). Both coordinators pipeline to `--pending-limit` 16 by default,
  `--serial` reproduces old order, durability rules unchanged, grpc gets
  the same concurrency (contract.md §12 change log). Headline: grpc wall
  ~2x better + tail collapse (large48 serial grpc max 72770 ms -> pipe
  max 9081 ms); ps wall ~unchanged (+8-11%) — authority admission
  capacity is the bottleneck (61% of pipelined large48 admits refused
  vs 0% serial; all refusal details named/counted, no uncategorized
  LIMIT_EXCEEDED, so Claude's defect-9 window did not pollute).
- C16f (metrics + idle): `2211f5c1`. sample.sh extended (threads, VmHWM,
  utime/stime, per-tick jstat -gc, t=0 burst, pidfile late-PIDs, JSTAT_GAP
  fails the run); all runners sample coordinators too (bg + wait +
  wall-conditional coord gate); `results/c16f-standard-seed6/` 3/3 PASS
  (threads/RSS/CPU/JVM heap in report.md; Rust heap + per-record funding
  stay named gaps). Idle write_bytes ANSWERED for Kimi: unanchored
  authority ~12.0 MB/s, ~99.9% cancelled (per-call rusqlite opens churn
  -wal/-shm on every 20 ms maintenance pass); read-only anchor in
  workload-authority serve() -> 0/s (subject untouched; before/after TSVs
  archived). Pinned binaries for C16g: anchored authority (hash in
  c16f-standard-seed6/c16f.bin.sha256) + C16e trio (hashes in
  pipeline-seed6.bin.sha256); run-full.sh verifies both at startup.
- C16g (final run/report/handoff/board): `run-full.sh` rewritten as the
  real full-matrix driver (smoke -> ladder 6 cells -> negative ->
  boundary -> stopped -> slow -> cr13 rust+java -> pipeline; per-rep and
  per-suite DONE resume; driver never holds BENCHMARK.lock, self-locking
  children would deadlock; non-locking children flock-wrapped). Archive
  `results/full-seed6/` (2241 files, MANIFEST verified, 14G -> 2.9G
  pruned): FULL MATRIX PASS 2026-09-11T18:54:41Z, re-verified SKIP-clean
  post-reboot. The matrix parked 6 attempts at pipeline large48/pipe/ps
  rep2-ps (INTERNAL_ERROR storage flakes, admit- and execution-commit
  paths; diagnostics "authority storage operation failed" /
  "application storage failure" swallow the StoreError; only the
  maintenance path treats contention as retryable) under sustained
  quantized ~31 ms fsync (healthy ~1 ms; NVMe raid0, no resync, no
  cgroup throttle on us, device-level contention, survived a reboot):
  synchronous=FULL stretches write-txn lock holds until 16-in-flight
  convoys exceed the 5 s busy_timeout; serial/small cells pass
  throughout. Failed reps preserved (FAILED-attemptN snapshots +
  OPERATOR-NOTEs, none discarded). A fsync-gated waiter found a 2-3 ms
  window and everything went green (rep2-ps 74 s, rep3-5 all PASS).
  Pipe/large48 timings are regime-qualified in report.md §2.
  xlarge64 stays UNAVAILABLE (Java storage, 2nd request open).
- Answers delivered: Kimi cancelled_write_bytes (yes, per-call opens;
  anchor fix; numbers above — closes Kimi M18 request 1); Kimi pending-
  ceiling variability independently corroborated (631 ps + 233 mixed
  "metadata concurrency exhausted" vs "aggregate admission capacity
  exhausted" rows in C16e data). Still open: Claude Java >=341-entity
  storage (2nd request); Kimi labels PENDING (~2026-09-17); new Java jar
  02a410fc (defect 9) noted but NOT adopted mid-matrix (C16a-e all ran
  e1763b4a; both valid per Claude, differ only in the retransmission
  window; our refusal details show no uncategorized LIMIT_EXCEEDED).
  New pin verified available 2026-09-11, adoption at next jar-sensitive
  cell: all-jar 02a410fc711a at
  /work/worktrees/pipestream-rfc-claude/implementations/java-netty/target/
  pipestream-quic-netty-0.1.0-SNAPSHOT-all.jar, source commit 4cb4b444
  ("Release an input's connection slot with its response, not after
  storage cleanup"), with storage-funding flags. Running pipeline suite
  on e1763b4a stays a valid subject.
  Superseded 2026-09-11 for next-cell adoption: all-jar 28c3369bd952
  (source ce1bfd77 "Keep an installed input pinned until its admission
  transaction ends", descends from 4cb4b444, adds listener defect-10 fix;
  artifact hash verified). Listener defect 10 explains the 37
  admit-notready rows in C16 mixed-arm data (all worker-c/Java:
  "authority refusal NOT_READY: complete validated input is unavailable";
  serial and pipe, standard and large48): a listener bug, not client or
  storage pressure. Coordinator retry on NOT_READY stays correct client
  behavior (all affected runs completed byte-exact); report must say so
  and expect zero such rows on the 28c3369b jar.
- Deviations fixed en route (all in-tree, committed or pending with the
  C16g commit): sampler `set --` clobbered the PID list (died tick 2);
  sampler missed sub-second processes (t=0 burst + pidfile + wall-
  conditional coord gate); empty corpus first-usable gate vacuous
  (ALLOW_VACUOUS=1, final-verified + empty digest govern; C4 empty was
  PARTIAL on this); revoked-reader-grpc injection raced startup and run
  end (now triggers on all-three-ready, deterministic); run-negative.sh
  stays mode 644, driver invokes suites via bash.
- Unit tests on final tree: workload-coordinator 9/9, workload-authority
  5/5, grpc-coordinator 1/1 (`--locked --offline`).

## 10. Safe next actions

1. DONE: repeats + fault demonstrations (see §8).
2. DONE 2026-09-09: mixed run against Claude `63d03a0` (.4 transport,
   jar `6da5e9d0…` hash-verified, no rebuild): `run-mixed.sh` with Java
   worker c + Rust a/b, seed 6, 8 MiB — Java admitted and executed 42
   chunks, final `sha256sum -c` OK against the all-Rust pin `b3dd5e34…`,
   all milestones present, no refusals, no mismatch to report.
3. When Kimi publishes driver checkpoints: run full comparative labels.
4. Propose spec improvements (if any) in a separate note, not in this file.

## 11. C15 report pointer (2026-09-11)

C4 comparison report: `benchmarks/durable-transform/report.md`
(correctness / measured performance / unmeasured assumptions).
Clause-level proposals (none proposed):
`benchmarks/durable-transform/spec-proposals.md`.
Ladder: `benchmarks/durable-transform/ladder.md` (v2).
Cell archives: `benchmarks/durable-transform/results/c4-*-seed6/`
plus `results/cr13-c15-java-seed6/` and
`results/c4-large64-attempts/`.
