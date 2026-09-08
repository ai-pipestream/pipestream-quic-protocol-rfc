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

## 6. Safe next actions

1. On an unblocked host: run §3 commands in order; paste results here.
2. When Claude publishes Java transform availability: add one mixed worker
   to the quick gate, labelled separately.
3. When Kimi publishes driver checkpoints: run full comparative labels.
4. Propose spec improvements (if any) in a separate note, not in this file.
