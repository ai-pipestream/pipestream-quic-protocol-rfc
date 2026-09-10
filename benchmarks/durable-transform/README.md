# Durable-transform benchmark runner

Comparative PipeStream-vs-gRPC measurement for contract
[`contract.md`](contract.md). Loopback only; no WAN extrapolation; no
production deployment.

## Build (pinned, offline-capable)

Each example crate keeps its own lockfile and target dir (never shared
between concurrent assignments):

```bash
cd ../../examples/durable-transform-workload/workload-authority
cargo build --release --locked
cd ../workload-coordinator && cargo build --release --locked
cd ../../../examples/durable-transform-grpc && cargo build --release --locked
```

Binaries:

- `workload-authority/target/release/workload-authority`
- `workload-coordinator/target/release/workload-coordinator`
- `durable-transform-grpc/target/release/grpc-worker`
- `durable-transform-grpc/target/release/grpc-coordinator`

Record `sha256sum` of all four into the run artifacts (scripts do this).

## Quick correctness gate

```bash
./run-quick.sh /tmp/dt-quick
```

Tiny binary corpus on both arms, byte-identity gate plus
first-usable/final-verified milestone gates. No performance claim.

## Full comparative run

```bash
REPO_BIN=/path/to/bin-dir ./run-full.sh /tmp/dt-full 5
```

`REPO_BIN` must contain `ps-auth ps-coord grpc-worker grpc-coord`
(symlink the four release binaries there). Takes `BENCHMARK.lock`,
alternates arms per repeat, runs crash demonstrations
(`libexec-faults.sh`: worker SIGKILL + restart, coordinator SIGKILL +
`--resume`), and requires byte-exact outputs throughout.

## What is measured

- Events TSV per arm: monotonic ms, wall ms, worker, ordinal, event
  (`session-open`, `admitted`, `succeeded`, `first-usable-output`,
  `chunk-verified`, `checkpoint`, `complete`, `final-verified`).
- `*-sample.tsv`: 5 Hz per-process RSS/VSZ/FDs/IO for workers.
- `*-net.txt`: wall time plus loopback RX/TX byte counters around the run.
- `bin.sha256` + `provenance.txt`: exact binaries, toolchains, kernel.
- Coordinator journals, worker SQLite DBs, staging dirs: retained per run.

Analysis (p50/p95/p99 with sample counts, RSS/HWM totals, IO bytes,
state sizes) is computed from these raw artifacts, never from console
summaries. Missing collectors invalidate the run; no invented zeros.

## Known gaps (honest)

- Mixed Java/Rust run needs Claude's A checkpoint (tracked on the board).
- Full `cargo build`/`cargo test` execution evidence is pending on an
  unblocked host; this session verified via `rustc --emit=metadata`
  typechecks, protoc codegen, an independent hashlib oracle mirror, and
  source-level API review (see handoff notes).
- Fault schedules 5–6 (explicit retryable vs restart, revoked reader,
  expired output, wrong-output) are coordinator/worker capabilities, not
  yet scripted end-to-end here.
- `schedules/fault-schedule-v1.tsv` rows F3/F4 (post-commit worker kill,
  mid-download coordinator kill) are declared intent: current fault scripts
  demonstrate F1/F2 timing-approximated; boundary-armed precision (arming on
  `PUBLICATION_COMMITTED` / partial `RESULT_VERIFIED` markers) is pending.
  See `reviews/kimi-interface-v1.md` for the proxy mapping.
