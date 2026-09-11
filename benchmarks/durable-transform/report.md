# C4 comparative report (C15)

Base 85887911 + C15 commits, seed 6 (smoke seed 7), ladder v1/v2 frozen
before measuring. Raw artifacts with verified MANIFEST.sha256 under
`benchmarks/durable-transform/results/`. No WAN claim, no marketing,
no statement about IETF acceptance.

## 1. Proven correctness

| cell | result | final digest |
|------|--------|--------------|
| empty (0 B) | PARTIAL, gate vacuous | 0-byte finals exact |
| tiny (1 kB) | PASS + 1 kept infra fail | 55d2e6e9… |
| uneven (100 kB) | PASS + 1 kept infra fail | 65283733… |
| quick (200 kB) | PASS 18/18 | 3cde15aa… |
| standard (8 MiB) | PASS 18/18, repros 09-09 pin | b3dd5e34… |
| large (48 MiB) | PARTIAL, mixed blocked | acc15911… (ps+grpc) |

- Every passing event stream carries `first-usable-output` and
  `final-verified`. Empty has `final-verified 0 bytes` on all arms;
  `first-usable-output` cannot exist for zero chunks (PARTIAL reason).
- The two kept infra fails are sampler races on sub-second runs
  (0.2 s sampler, no rows for one worker PID); milestones present,
  finals byte-identical, replacements pass. Failed runs stay in the
  table, never discarded.
- CR13 closed vs Java at 7585a9dc (4/4 arms, session survives the
  30.015 s kill). Earlier divergence at 63d03a0.
- Mixed Java/Rust passes at all cells through 8 MiB (42+ chunks
  through Java transform/v2 at standard). At 48 MiB the Java
  authority refuses 100-entity declares
  (`SQLite file capacity exhausted`, bound in (43,100]); request
  to Claude is open. Kimi review labels PENDING (unavailable
  until ~2026-09-17), recorded as pending, never passed.
- Faults: F1/F2 time-armed demonstrations from §8 stand (not
  re-run in C15). F3/F4 boundary-armed runs are UNIMPLEMENTED
  (no runner support); against the C4 requirement they are
  missing, not passed.
- Negative controls: killed-collector gate proven, cancel-neg
  proven via CR13, swapped-order and wrong-transform covered by
  unit tests only (PARTIAL), missing-chunk / truncated-artifact /
  stale-hash / revoked-reader / expired-output runs UNIMPLEMENTED.
- Stopped-consumer and slow-worker arms UNIMPLEMENTED.
- 64 MiB attempts (1024 chunks) archived separately, not pooled:
  they measured the real bounds (V2 256-entry list, single-TX
  fit, cumulative record funding) that produced the coordinator
  batching fix and ladder v2.

## 2. Measured performance (with dispersion)

Loopback, shared host without cgroup limits. Raw observations only;
deltas are findings, never grounds to weaken a guarantee.

Coordinator-arm wall_ms by rep (warmup excluded):

- tiny: ps ~1 s, mixed ~1-2 s, grpc ~0-1 s (sub-second arms).
- uneven: ps ~0-1 s, mixed ~1-2 s, grpc ~0-1 s.
- quick: ps ~0-1 s, mixed ~1-2 s, grpc ~0-1 s.
- standard: ps 1869,1807,1971,2086,2062; mixed
  2290,2546,2039,2178,2220; grpc 199,200,200,200,223.
- large48: ps 18185,18311,18171; grpc 1099,1101,1087.
  (mixed has no passing large run.)

Per-chunk admit-to-verified latency, pooled across reps:

- tiny (n=5/5/6): ps p50 31 ms, mixed p50 34 ms, grpc p50 1 ms.
- uneven (n=10/10/12): ps p50 39 ms, mixed p50 44 ms,
  grpc p50 2 ms.
- quick (n=20): ps p50 47 ms p95 57 ms, mixed p50 53 ms
  p95 131 ms, grpc p50 2 ms p95 3 ms.
- standard (n=640): ps p50 875 ms p95 1061 ms p99 1114 ms;
  mixed p50 915 ms p95 1139 ms p99 1299 ms;
  grpc p50 2 ms p95 3 ms p99 3 ms.
- large48 (n=2304): ps p50 10207 ms p95 11251 ms p99 11453 ms;
  grpc p50 2 ms p95 3 ms p99 3 ms.
- p99 from n<100 (tiny, uneven) is reported with its count
  and is not meaningful.

Structure behind the gap: the workload coordinator admits
and verifies serially per shard, so per-chunk latency grows
with shard length (standard p50 875 ms at 43/shard, large
p50 10.2 s at 256/shard). The gRPC baseline retrieves
concurrently (flat ~2 ms at all sizes). This is the
application's coordination shape, not a transport verdict;
confounders include per-rep JVM startup in mixed arms and
shared-host contention.

Loopback bytes per run (rep-0 standard): ps 17.88 MB,
mixed 18.03 MB, grpc 17.15 MB for 8.39 MB payload
(~2.0-2.2x in each direction, all arms; includes
handshakes, retries, coordination).

Worker RSS high-water (sampler, 0.2 s): ps authority
~19 MB at 8 MiB, ~28 MB at 48 MiB; grpc worker ~9 MB
at 8 MiB, ~10 MB at 48 MiB.

UNAVAILABLE (not zero-filled): thread counts (sampler
records FDs/IO but not threads), coordinator RSS/heap,
CPU-time split, JVM heap detail (no jstat capture),
per-record funding breakdown.

## 3. Unmeasured deployment assumptions

Loopback only (no WAN/production claim); shared host with
no CPU/cgroup isolation; corpus above the 2 GiB Java heap
never run; F3/F4 boundary recovery time never measured;
six of eight negative controls never run; stopped-consumer
and slow-worker behavior never run; 60-minute per-run
timeout never approached (max observed arm ~19 s).

Where PipeStream reduces application coordination:
durable sessions with same-identity backpressure retry
(transient aggregate-capacity refusals absorbed and
retried to byte-exact finals in the funded 48 MiB runs),
journaled replay with frozen operation identities,
scope seals with checkpoint proof. The unfunded 48 MiB
grind (418 refusals, zero admissions) was terminated as
unsalvageable and is archived in the terminated
/tmp/c4-large48 partials, not in results/.

Where it adds bytes, state, or latency: Declare framing
in ≤100-entity batches with last-only seal; seal plus
checkpoint round trips per run; authority state funding
that must be sized to the corpus (256/64 MiB defaults
hold through 8 MiB; 48 MiB needed 1024/256 MiB on the
Rust authorities); per-chunk latency that scales with
shard length under the serial admit/verify loop.
Loopback bytes are ~2.1x payload on both sides; no byte
advantage is claimed for either side.
