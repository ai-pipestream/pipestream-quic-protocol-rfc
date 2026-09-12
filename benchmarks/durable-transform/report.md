# C16 comparative report (2026-09-11)

Binaries pinned in `results/full-seed6/full.bin.sha256` (anchored
workload-authority + C16e coordinator/grpc trio; run-full.sh verifies
both pins at startup). Seed 6 throughout. Raw artifacts with verified
MANIFEST.sha256 under `benchmarks/durable-transform/results/full-seed6/`
(2241 files). Java jars: standard `61ab64a3`, large48 `e1763b4a` (both
valid subjects; defect-9/10 jars noted for next-cell adoption). No WAN
claim, no marketing, no statement about IETF acceptance.

## 1. Proven correctness

| cell | result | final digest |
|------|--------|--------------|
| empty (0 B) | PASS (first-usable vacuous, ALLOW_VACUOUS=1) | e3b0c442… (0-byte finals exact, all arms) |
| tiny (1 kB) | PASS 15/15 + warmups | 55d2e6e9… |
| uneven (100 kB) | PASS 15/15 + warmups | 65283733… |
| quick (200 kB) | PASS 15/15 + warmups | 3cde15aa… |
| standard (8 MiB) | PASS 15/15 + warmups, repros C15 pin | b3dd5e34… |
| large48 (48 MiB) | PASS 9/9 + warmups, all 3 arms incl. mixed | acc15911… |

- Every measured rep carries `first-usable-output` (except SIZE=0,
  where zero chunks admit nothing) and `final-verified`, plus a
  digest match against the cell pin. The C15 empty PARTIAL is closed:
  the gate was vacuous, not the behavior.
- C15 large48 mixed PARTIAL is closed: with 1024/256 MiB funding on
  the Java worker, mixed passes 48 MiB on all reps (digest acc15911…).
  xlarge64 stays UNAVAILABLE with the reason (Java >=341-entity
  storage; 2nd Claude request open), not passed.
- Negative controls 31/31 (`results/full-seed6/negative/`): every
  control is a positive twin (exit 0) plus an injected run that must
  FAIL with a named reason (INVALID markers). The revoked-reader-grpc
  injection was fixed en route: triggering on principals-file
  appearance raced worker startup (wrong gate), triggering on event
  rows raced run end (110 ms run); it now triggers on all-three-ready
  and refuses deterministically mid-run.
- Boundary faults F3/F4 15/15: armed run dies at the boundary, restart
  on the same state dirs + coordinator resume completes byte-exact.
- Stopped-consumer and slow-worker arms green (5 positive twins +
  delay runs + stall-read probes per arm; worker-c at 1/10 pace,
  exactly-once chunks, exposed-vs-hidden slowdown).
- CR13 closed vs Rust and vs Java (cancel-neg/complete/idle/lifetime
  probes in `cr13-rust/`, `cr13-java/`; session survives the kill,
  post-kill session refused).
- Kimi review labels PENDING (unavailable until ~2026-09-17),
  recorded as pending, never passed.
- Failed runs stay in the table, never discarded: pipeline
  large48/pipe/ps rep2-ps FAILED-attempt1..5 plus ladder-large48
  mixed rep1-mixed FAILED-attempt1, with OPERATOR-NOTEs (slow-fsync
  storage flakes, mechanism in §2).

## 2. Measured performance (with dispersion)

Loopback, shared host without cgroup limits. Raw observations only;
deltas are findings, never grounds to weaken a guarantee. Ladder
cells run pipelined (pending-limit 16) on the anchored binaries.

Coordinator-arm wall_ms, sorted reps (warmups excluded):

- empty: ps 278-316, mixed 427-937, grpc 35-38.
- tiny: ps 405-439, mixed 497-562, grpc 58-83.
- uneven: ps 477-532, mixed 515-586, grpc 72-77.
- quick: ps 605-623, mixed 646-702, grpc 96-104.
- standard: ps 10540-11216, mixed 13982-16004, grpc 1406-1523.
- large48: ps 70151-76274, mixed 75416-82149, grpc 8290-8724.

Before/after pipelining (pipeline suite, wall medians, n=5):

- standard ps: serial 9696 -> pipe 11283. grpc: 2077 -> 1488.
  mixed: 11128 -> 14560.
- large48 ps: serial 59092 -> pipe 72409. grpc: 12443 -> 8854
  (max 42007 in one rep). mixed: 66912 -> 76710.
- Pipe collapses grpc tails but not ps walls: the authority's
  admission capacity, not coordinator serialism, is the ps
  bottleneck. Serial ps sees ZERO backpressure in 4480 admissions;
  pipelined large48 ps has 60% of admissions hit >= 1 refusal
  (5485 "aggregate admission capacity exhausted", 338 "metadata
  concurrency exhausted", standard pipe 52%). Admit p50 119 ->
  658 ms, watch p50 21 -> 267 ms, fetch p50 47 -> 565 ms; 16-way
  concurrency absorbs most of it, net wall +9-23%.
- Fetch-path backpressure shares the admit-* labels (a "stalled
  read ordinal N" row logged as admit-refused is a fetch retry,
  not an admission refusal; 5 per storm in the large cells).
- The 37 admit-notready rows (all mixed-arm, Java worker-c:
  "NOT_READY: complete validated input is unavailable") are
  listener defect 10 (fixed at ce1bfd77), not client or storage
  pressure. Retry on NOT_READY stays correct client behavior (all
  affected runs byte-exact); expect zero on the 28c3369b jar.
- No uncategorized LIMIT_EXCEEDED anywhere: every refusal detail
  is named and counted, so the defect-9 retransmission window did
  not pollute the e1763b4a measurements.
- Storage regime caveat (measured, read-only probes): the host
  sat in sustained quantized ~31 ms fsync (healthy ~1 ms; NVMe
  raid0, no resync, no cgroup throttle on us) across a reboot.
  synchronous=FULL stretches write-txn lock holds until
  16-in-flight convoys exceed the 5 s busy_timeout: pipe/large48
  ps then fails always (6 consecutive INTERNAL_ERROR flakes at
  admit and execution commit, WALs ~3.7 MB so not funding),
  while serial and small cells pass throughout. A fsync-gated
  waiter completed the matrix in the next 2-3 ms window
  (FULL MATRIX PASS 2026-09-11T18:54:41Z). Pipe/large48 timings
  above come from that window; do not compare them against
  numbers taken in a different fsync regime.

Worker/coordinator resources (extended sampler, C16f standard
rerun, one rep per arm; CPU seconds from utime+stime at 100 Hz):

- ps rep (wall 18.5 s): authorities 54-55 threads, RSS max ~21 MB
  (HWM ~23 MB), ~4.2 CPU-s each; coordinator 40 threads,
  RSS/HWM ~18.8 MB, 2.26 CPU-s.
- grpc rep (wall 1.5 s): workers 33 threads, RSS ~14 MB, ~0.06
  CPU-s each; coordinator 33 threads, RSS ~27 MB, 0.14 CPU-s.
- mixed rep (wall 17.1 s): Rust authorities ~21 MB / 4.0 CPU-s;
  Java worker 53 threads, RSS/HWM 434 MB, 9.87 CPU-s; coordinator
  40 threads, ~17 MB, 1.77 CPU-s. jstat -gc every tick, 34 rows,
  zero gaps: Eden 112->149 MB, Old ~107 MB, Metaspace ~10->20 MB,
  8 young GCs totaling 0.11 s, 0 full GCs.
- Rust heap stays a named gap (no allocator counter crate).
  Per-record funding breakdown stays a named gap.

Idle write_bytes (Claude's question; /proc/pid/io, serving but
uncontacted, 60 s): before ~12.0 MB/s with ~99.9% cancelled
(per-call rusqlite opens churn -wal/-shm on every 20 ms
maintenance pass); after a read-only anchor connection held by
workload-authority serve(): 0/s writes, 0/s cancelled (subject
untouched; before/after TSVs in c16f-standard-seed6/).

Loopback bytes are ~2.1x payload on both sides; no byte advantage
is claimed for either side.

## 3. Unmeasured deployment assumptions

Loopback only (no WAN/production claim); shared host with no
CPU/cgroup isolation and an fsync regime that moves pipe/large48
results by regime (see §2 caveat); corpus above 48 MiB never run;
per-record funding breakdown never measured; 60-minute per-run
timeout never approached (max observed arm ~85 s).

Where PipeStream reduces application coordination: durable
sessions with same-identity backpressure retry (thousands of
aggregate-capacity refusals absorbed and retried to byte-exact
finals in the funded 48 MiB runs), journaled replay with frozen
operation identities, scope seals with checkpoint proof,
coordinator resume after boundary death. The unfunded 48 MiB
grind (418 refusals, zero admissions) was terminated as
unsalvageable and stays archived in the terminated /tmp
partials, not in results/.

Where it adds bytes, state, or latency: Declare framing in
≤100-entity batches with last-only seal; seal plus checkpoint
round trips per run; authority state funding that must be sized
to the corpus (256/64 MiB defaults hold through 8 MiB; 48 MiB
needed 1024/256 MiB on the Rust authorities and the Java
worker); per-chunk latency that scales with shard length, plus
admission-capacity backoff under pipelining (61% refused
admissions at 16-in-flight on large48).
