# C4 measurement ladder (frozen 2026-09-10, base 85887911 + C15)

No decisive C4 run may start before this file is committed. Any change to
this ladder after the freeze is a new commit that states what changed and
why; cells run under different ladder versions are not pooled.

## Fixed parameters (same for every arm and cell)

- Transform/chunking: `workload-core` `CHUNK_SIZE = 65_536`, transform
  `rotl8(b,1) XOR (i mod 251)`. No chunk-size or concurrency knob exists
  in the runners, so the ladder varies payload SIZE; chunk count follows
  as `ceil(SIZE / 65536)`. Concurrency is fixed at 3 worker authorities
  (a/b/c) + 1 coordinator, all loopback isolated processes on one host.
- Workers: PipeStream arm ports 17443-17445, gRPC arm 18443-18445.
- Seeds: smoke seed 7 (matches `run-quick.sh`); all ladder cells seed 6.
- JVM freeze (Kimi `async-neutral-v2` handoff §freeze, same values):
  `JAVA_TOOL_OPTIONS="-Xms256m -Xmx2g"` exported for every Java launch.
  Pinned jar `61ab64a3…` (Claude `7585a9dc`), copied per run dir,
  hash-verified pre-run, never rebuilt.
- Rust binaries (rebuilt 2026-09-10 from synced tree, `--locked --offline`):
  authority `e7175dd7…`, coordinator `54330cbf…`, grpc-worker `e48b226d…`,
  grpc-coordinator `03321977…` (full hashes in run artifacts).
- Fault schedule: `schedules/fault-schedule-v1.tsv` (F1-F4, Kimi's format).
- Order/alternation: A/B/B/A per repeat pair (`run-full.sh` pattern);
  per-cell driver alternates ps/grpc/ps... mirrored the same way.
- Warmup: 1 unrecorded run per arm per cell, labelled `warmup`, discarded.
- Repeats: 5 measured repeats per arm per cell, except the 64 MiB cell
  which uses 3 (time ceiling, see below). p99 only from >= 100 samples
  with the count stated; per-chunk samples pool across repeats of a cell.
- Locks: every measured run under `flock -w 3600 BENCHMARK.lock`, one run
  at a time. Native builds only under BENCHMARK.lock + NATIVE-BUILD.lock.
- Archive: per-cell dir under `benchmarks/durable-transform/results/`
  with raw artifacts + `MANIFEST.sha256` (verified at archive time).

## Host and toolchain (recorded in every run manifest)

- Host: 32 CPUs, 121 GB RAM, xfs on /work (/dev/md0, 2.3T free at freeze).
- Kernel `7.0.0-31-generic`, rustc `1.97.1`, OpenSSL `3.5.5`,
  GraalVM CE 25.1.3 (openjdk 25.0.3). No cgroup/CPU limits imposed;
  shared-host contention is recorded as load, not controlled.
- TLS: per-run PKI from `mk-test-pki.sh` (loopback-only, ~2d validity);
  TLS config + versions in each run's manifest.

## Payload ladder (SIZE bytes; all three arms: ps-all-Rust, ps-mixed, grpc)

| cell | SIZE | chunks | purpose |
|------|------|--------|---------|
| empty | 0 | 0 | zero input |
| tiny | 1_000 | 1 | sub-chunk tiny input |
| uneven | 100_000 | 2 (partial tail) | uneven chunk count, exact boundary + partial |
| quick | 200_000 | 4 (partial tail) | parity with `run-quick.sh` gate |
| standard | 8_388_608 | 128 | primary comparison cell (matches §8 evidence) |
| large | 50_331_648 | 768 (256/worker) | object >> window, streamed from disk |

## Explicit ceilings (chosen before measuring, with reasons)

- Largest corpus 48 MiB (ladder v2; was 64 MiB): the 64 MiB cell
  (1024 chunks, ~341/worker) is infeasible on default authority funding —
  measured 2026-09-11: single declares >256 violate the V2 list bound
  (fixed: batched declares, seal-last), 100-entity batches fit a single
  transaction (254 fails, 115 succeeds per conformance), but cumulative
  record-completion funding caps declared entities at ~256/worker
  (3rd 100-batch refused). 48 MiB x 3 arms x (1 warmup + 3 reps) is the
  largest block that fits both funding and realistic lock windows.
  Anything larger is UNAVAILABLE, not extrapolated. The failed 64 MiB
  attempts stay archived (c4-large64-attempts), not pooled.
- Corpus exceeding the 2g Java heap (the "larger than allowed heap" case
  at full scale) is UNAVAILABLE on this host: a >2 GiB loopback run would
  hold BENCHMARK.lock for hours and starve the shared host. The large
  cell streams from disk (no full-corpus heap copy) as the feasible proxy;
  the full >heap case stays explicitly unmeasured.
- Cell timeout: any single arm run exceeding 60 min wall is a failed run
  in the table (never discarded, never silently retried).
- Repeats cut 5 -> 3 for the large cell only (time ceiling above).

## Correctness smoke (before the ladder, not part of it)

`run-quick.sh /tmp/c4-smoke` (200 kB, seed 7, both arms byte-identical,
milestones present). Must PASS before any ladder cell starts.

## Fault boundaries

- F1 (worker-b kill after admission) and F2 (coordinator kill mid-run):
  existing `libexec-faults.sh` demonstrations are time-armed (sleep-based),
  NOT boundary-armed. They stay as demonstrations; against the C4
  boundary-arming requirement they are PARTIAL with this reason.
- F3 (worker-c kill at PUBLICATION_COMMITTED) and F4 (coordinator kill at
  RESULT_VERIFIED) boundary-armed: runner support does not exist yet
  (kill must fire on the observed boundary event row, recovery measured
  from the reached boundary). Status: UNIMPLEMENTED. No F3/F4 claim until
  the runner lands and the rows prove arming. If a boundary cannot be
  armed precisely, the row is PARTIAL with the reason.

## Negative controls (each a run with a named invalidation reason, or else)

| control | status at freeze |
|---------|------------------|
| swapped chunk order | covered by `workload-core` unit tests only, not as a run: PARTIAL |
| correct-hash/wrong-transform | unit tests only: PARTIAL |
| missing chunk | planned run: UNIMPLEMENTED |
| truncated artifact | planned run: UNIMPLEMENTED |
| killed collector | gate proven (run exits 1, named reason): DONE |
| stale binary hash | planned run: UNIMPLEMENTED |
| revoked reader | planned run: UNIMPLEMENTED |
| expired output | planned run: UNIMPLEMENTED |
| cancel-neg (injected shutdown rejected) | DONE via CR13 (`run-cr13.sh` cancel-neg) |

## Collector capability checks

Before each run: `/proc/<pid>/io` readable, `/proc/net/dev` loopback
accounting non-empty, jstat present for the Java worker, RSS/HWM +
threads + FDs sampled per process and process-group totals. Dead
collector or missing worker sample fails the run (already gated in
`libexec-*` via `check_samples`; keep it).

## Measures recorded per cell (C4 list, no substitutes)

First verified usable output, total verified completion, per-chunk
p50/p95/p99 with counts, recovery time per reached boundary, CPU,
heap/native/RSS/HWM per process + group, threads, FDs, retained bytes,
file/block sizes, actual I/O, network bytes with loopback + TLS/retry
accounting, coordination source and state size (generated code separate).
Uncollectable measures are UNAVAILABLE, never zero-filled or inferred.
