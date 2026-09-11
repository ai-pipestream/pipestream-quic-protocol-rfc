# Durable transform workload: application and fairness contract (C1, frozen)

Status: FROZEN for implementation, PENDING Kimi scoped peer review.
Review request: Kimi — scoped review of semantic-equivalence matrix (§8) and
failure-schedule compatibility (§7) before large comparative runs.
Owner: Meta. Branch: `agent/rfc-meta-workload-v2`.

This contract freezes the workload, the fairness rules for the PipeStream vs
streaming-gRPC comparison, and what each metric includes. It does not weaken
any Section 12 / Appendix F guarantee to fit either backend.

## 1. Workload shape

A deterministic byte transform over sharded inputs, with durable per-chunk
outcomes and ordered reconstruction of the exact final object.

- Chunk size: 65,536 bytes, except a final partial chunk (1..65,535 bytes).
  Empty input yields zero chunks and a zero-length final object.
- Each chunk is one leaf work item (mode 0), application `transform/v2`,
  in the root scope (scope 0, producer 0). Chunk ordinal `k` (0-based) maps
  to entity ID `k+1`. Ordinals, never hashes, define reconstruction order.
- One processing authority owns each session (§12: composition across
  authorities is not a distributed transaction). The coordinator opens one
  session per worker authority and shards chunks across sessions.

## 2. Deterministic transform (frozen)

For each chunk, for input byte `b` at chunk-relative offset `i` (0-based):

```
out[i] = rotate_left_8(b, 1) XOR (i mod 251)
rotate_left_8(b, 1) = ((b << 1) | (b >> 7)) & 255
```

Output length equals input length. The transform processes actual bytes; it
never returns an input hash or copies input unchanged. Hash verification
(SHA-256 per chunk and of the final object) supplements byte comparison.

The independent streaming oracle (§6) derives exact expected bytes from the
recorded seed alone, without touching either backend's state.

## 3. Input generator and datasets

Inputs are generated incrementally from recorded 64-bit seeds (splitmix64
keystream), never stored as full corpora except pinned samples:

| dataset | size | purpose |
|---|---|---|
| `empty` | 0 bytes | zero-chunk path, zero-length final object |
| `tiny` | 1 byte | single partial chunk |
| `boundary` | exactly 65,536 bytes | exact chunk boundary |
| `boundary-plus-one` | 65,537 bytes | full chunk + 1-byte partial |
| `binary` | 200,000 bytes, all 256 byte values incl. `0x00` and invalid UTF-8 | binary safety |
| `standard` | 8 MiB | pinned measurement corpus |
| `large` | exceeds worker heap allowance, streamed from disk | bounded-memory proof |

Seeds, chunk size, and sharding are recorded per run. Uneven chunk counts
(non-multiple of 3 workers) are exercised deliberately.

## 4. Identity, authentication, sessions

- PipeStream arm: mutual TLS, ALPN `pipestream/2`, profiles
  `durable-work-v2` + `result-delivery-v2`. Coordinator principal `metacoord`
  (client certificate mapped via principal TSV). Authorities `workload-a/b/c`,
  one session each, creation sequences persisted in the coordinator journal
  before transmission. Owner label `workload`.
- gRPC arm: same principals and authorization policy. TLS server identity
  verification (RFC 9525 equivalent) plus mutual TLS client authentication
  with the same certificates/keys. Same session/work/attempt identity model
  at the application level (UUIDv4 operation IDs, immutable parameters).
- Session policy (both arms): execution ceiling 60,000 ms per chunk,
  output retention 3,600,000 ms, receipt retention 86,400,000 ms.
- No anonymous fallback on either side. Revoked-reader and expired-output
  cases must refuse with a named error, never a false completed file.

## 5. Durable promises and ACK boundaries (both arms)

- The coordinator journals input/chunk identity, selected
  authority/session/work, immutable original operation IDs, and completed
  output selections before acting on them.
- An ACK (PipeStream operation receipt / gRPC commit response) is sent only
  after the metadata commit that precedes it: admission receipt after the
  atomic admission commit; outcome receipt after the fenced terminal commit.
- Committed-but-unobserved receipts are resolved by retrying the SAME
  immutable operation, never by inventing a new operation/work identity.
- Coordinator recovery replays journal intent: same session binding
  (authority/owner/generation/policy/profile set), same operation IDs, same
  expected attempts. No server-directory reads to gather results; all bytes
  arrive through authenticated result streams.
- First usable output = the first fully verified committed chunk (bytes +
  length/hash + identity + committed outcome + replay metadata retained so
  the coordinator survives immediate process death). First byte/header
  arrivals may be logged separately, never compared to that milestone.
- Final install only after every required chunk verifies: byte-exact final
  object plus SHA-256 match against the oracle. Verified partial chunks are
  exposed as progress with ordinal/identity; unverified headers, out-of-order
  prefixes, or provisional manifests are never usable output.

## 6. Independent oracle and conformance gates

- `oracle`: byte-level transform/reconstruction reference fed only with
  (seed, sizes). Covers empty, partial, binary, large inputs.
- Negative controls (each must fail the run, never a passing skip):
  swapped chunk order, correct-hash/wrong-transform payload, truncated
  artifact, missing worker metric samples, killed collector, stale
  binary/config hash vs the run's pinned hashes.
- Resource/correctness gates freeze before final measurement; a failure or
  timeout is a failed run, never discarded noise.

## 7. Failure schedule (shared schema, independently built runners)

No arbitrary shell in schedules: named target, reached boundary, action,
seed, deadline.

1. Kill coordinator after submission, before saving an ACK → recover same
   identities/bytes via journal replay.
2. Kill worker after admission, before terminal commit → retry/fence to the
   same work identity; no duplicate execution effects beyond the
   application's idempotent re-execution.
3. Kill worker after result commit, before delivery → re-read same manifest,
   no re-execution, no new attempt.
4. Kill coordinator during result download/reassembly → resume from retained
   selections, byte-exact final object, no re-admission.
5. Explicit retryable failure (attempt reports retryable) vs restart vs
   repeated reads are separate cases with distinct expected receipts.
6. Missing chunk, wrong-output (INTEGRITY-class), revoked reader, expired
   remote output: named failure, no false completed file. Local retained
   copies stay distinguishable from fresh remote authorization.

## 8. gRPC equivalence matrix (what "equivalent" means)

The baseline implements the application guarantees gRPC transport alone
does not supply. Shared code (allowed): deterministic transform, dataset
generator, measurement code, application-level identity types. Independently
owned per arm: protocol state machine, journals, retry/fencing, manifest
commitments, result streaming, cleanup. The baseline must not call
PipeStream or wrap PipeStream wire frames.

| obligation | PipeStream mechanism | gRPC baseline mechanism |
|---|---|---|
| durable session/work identity, immutable operation IDs | §12 operation IDs + digests | UUID operation IDs + request digest log |
| commit-before-ACK replay, changed-parameter refusal | receipt replay / CONFLICT | committed-outcome table / conflicting-digest refusal |
| non-reuse after history expiry | high-water marks | tombstoned operation log |
| verified incremental input before admitted work | admission validation | staged-then-admitted input with length/hash check |
| durable jobs/outcomes, explicit authorized retry | attempts + fences | attempt generations + lease fencing |
| original deadlines, cancellation/skip semantics | §12 fences | deadline table + fence records |
| current-attempt/worker/ancestor publication fencing | worker leases | worker lease table checked in commit transaction |
| same transform/chunking/placement/concurrency | transform/v2, 64 KiB | same code, same sizes |
| output promises, manifest commitments | v2-result-manifest | signed-by-journal manifest record (same fields) |
| bounded streaming result retrieval + hash/length validation | result streams | chunked server-streaming RPC + client validation |
| independent execution/receipt/output/auth lifetimes | §12 lifetimes | separate TTL columns, independently enforced |
| read/dependency pins, retained outcomes, replayable cleanup | §12 pins | pin-counted blob store, idempotent GC receipts |
| restart accounting | journal replay | journal replay |
| persistence ACK point + sync settings | §9 | same SQLite journal_mode/synchronous + fsync policy |
| stable journals, CPU allowances, auth work, failure model | §9–10 | identical caps and schedule |

Persistence (§9): same SQLite `journal_mode`, `synchronous` policy, file +
directory sync discipline, metadata-commit-before-ACK, committed-but-
unobserved receipt handling on both arms. Document any residual difference
and measure it.

## 9. Topology and resource freeze (paired runs)

- All-Rust coordinator + three worker processes on both arms, loopback,
  identical placement, CPU/cgroup limits, warm/cold state, chunk placement,
  principal identities, concurrency (one in-flight chunk per worker;
  coordinator reassembly bounded to two chunks in memory + disk staging).
- Mixed Java/Rust run (after Claude's A checkpoint) is separate functional
  evidence with its own label, unless a matching gRPC topology is added.
  Never attribute JVM/topology costs to transport.
- Bounded state everywhere: buffers, in-flight work, journals, staging,
  reassembly. No full-corpus duplication in memory; no unbounded
  application queue outside measured budgets.
- Runs alternate order (A/B/B/A), warmup separate, ≥5 repeats per measured
  case where feasible. Whole-machine runs take `BENCHMARK.lock`; expensive
  native rebuilds take `NATIVE-BUILD.lock` (BENCHMARK first). Record
  unrelated host load; reject materially contended measurements.

## 10. Metrics (what each includes)

- Correctness: byte-exact final + partial outputs, identity checks, no
  false completion, capacity retained/recovered under failures.
- Latency: first verified usable chunk, total verified completion,
  per-chunk p50/p95/p99 with sample counts, recovery time per fault
  boundary. Queue time and unsuccessful runs stay visible.
- Cost: per-process CPU time/utilization, RSS/HWM + process-group total,
  threads, FDs; heap/native where collectable; logical retained bytes,
  file lengths, allocated blocks, actual read/write I/O and network bytes
  (loopback-accounted, TLS/retry bytes included) with stated method and
  calibrated collector overhead.
- Code/state: application coordination sources + persistent schema/state
  size, with generated code, shared transform, reference implementation,
  and baseline glue counted separately.
- Provenance: hardware, OS/kernel, filesystem/storage, toolchains,
  transport/native deps, TLS config, binary hashes, exact commands/configs.
- Dispersion and raw observations reported; no p99 from tiny samples; no
  invented zeros for uncollectable metrics; no WAN extrapolation.

## 11. Non-goals

No production deployment, no draft submission, no IETF-acceptance claim,
no network-wide speed claim from loopback data. Performance deltas are
findings; they never justify weakening either side's guarantees.

## 12. Change log

- C16e (2026-09-11): both coordinators pipeline admissions and
  verification by default (up to `--pending-limit`, default 16, a local
  concurrency cap, not a negotiated protocol limit). Durability rules are
  unchanged: every admission journals its intent before sending, results
  validate before journaling, one receipt per operation, frozen operation
  identities. `--serial` reproduces the old one-at-a-time order and
  numbers. Event rows now carry phase timings (`admit=`, `watch=`,
  `fetch=` ms details) for the before/after breakdown; labels and gates
  are unchanged. gRPC gets the same concurrency (fairness).
