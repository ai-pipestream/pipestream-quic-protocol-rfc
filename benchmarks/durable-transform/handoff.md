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
  timing-window fingerprints: idle arm (3 s stall survives as boundary
  control; 7 s stall under varied unrelated wire traffic — missing-work
  reads, scope pages, watch polls — dies at ~7.1 s), lifetime arm
  (28 s continuous progress dies at ~30.0 s), complete arm (in-cap
  transfer commits cleanly, proving non-vacuity). PASS requires the
  failure inside the window with a LimitExceeded/Cancelled code;
  integrity/conflict/auth codes are explicitly rejected. PASS vs Rust
  AND Java workers with identical fingerprints. Scoped claim: Rust
  client upload path only; server-side enforcement and the download
  direction are NOT covered. Fidelity note for transport owners:
  post-deadline writes surface Cancelled, not the deadline that killed
  the stream; a true disable-deadline negative control needs a client
  idle/lifetime knob that does not exist in the current API.

## 9. Safe next actions

1. DONE: repeats + fault demonstrations (see §8).
2. DONE 2026-09-09: mixed run against Claude `63d03a0` (.4 transport,
   jar `6da5e9d0…` hash-verified, no rebuild): `run-mixed.sh` with Java
   worker c + Rust a/b, seed 6, 8 MiB — Java admitted and executed 42
   chunks, final `sha256sum -c` OK against the all-Rust pin `b3dd5e34…`,
   all milestones present, no refusals, no mismatch to report.
3. When Kimi publishes driver checkpoints: run full comparative labels.
4. Propose spec improvements (if any) in a separate note, not in this file.
