# C: External durable workload and equivalent streaming-gRPC comparison

Read [the shared handoff](README.md) first. This implements original task 3.
The result must show a useful complete application and an honest comparison,
including where PipeStream costs more. No predetermined throughput win is an
acceptance criterion.

## Ownership and source boundaries

Own new external projects under `examples/durable-transform-workload/` and
`examples/durable-transform-grpc/`, and the runner/configuration/evidence under
`benchmarks/durable-transform/`. Keep application behavior outside the protocol
reference implementation. Rust is the implementation/harness language; no Python.

Use public Java/Rust client/authority APIs or their documented process adapters.
Applications may register real callbacks through public extension points. Do not
read authority directories/databases to gather results. Existing
`examples/three-node-scatter/` is a Layer-0 local-output-directory demo, not the
foundation for claiming durable network gather. Read the current Rust V2 CLI
guide and A's public adapter contract before integration.

The gRPC baseline may own a small new `.proto` contract in its external project;
it is a comparison application protocol, not a new PipeStream wire mapping or
change to platform-wide protobuf ownership. Do not import unrelated platform
services or deploy a live cluster. Pin dependencies/toolchains and check current
official documentation when choosing or using gRPC libraries.

## Deliverables, in order

### C1. Freeze the application and fairness contract first

Write `benchmarks/durable-transform/contract.md` before benchmarking. Define
the exact workload inputs, deterministic transformations, identity and retry
rules, authenticated principals, persistent promises, outputs, bounded resources,
failure points, expected reconstructed bytes and what each metric includes.
Prepare this contract for coordinating review while implementation proceeds;
do not spend a large benchmark run on unreviewed semantic equivalence.

Use binary data generated incrementally from recorded seeds, including zero
bytes, invalid text encodings, exact chunk boundaries and a final partial chunk.
Use at least three worker authorities, initially loopback isolated processes;
record which language each worker uses. One processing authority owns each
session. Application composition across authorities is not a protocol-level
distributed transaction and must not be advertised as one.

Suggested fixed versioned transform: for each chunk, byte `b` at chunk-relative
offset `i` becomes `rotate_left_8(b, 1) XOR (i mod 251)`. Define the rotation
explicitly as `((b << 1) | (b >> 7)) & 255`; output length equals input length.
The coordinator reconstructs transformed chunks in original ordinal order.
Freeze this or another equally precise non-identity transform before comparison.
It must process actual bytes, not return the input hash or copy input unchanged.
An independent streaming oracle derives exact expected output. Hash verification
supplements byte comparison and is not itself the transformation.

The coordinator journals input/chunk identity, selected authority/session/work,
immutable original operation IDs and completed output selections. Children can
finish out of order. Fetch real outputs through authenticated result streams and
install a complete final object only after every required chunk verifies. Expose
verified partial chunks as useful progress with ordinal/identity; do not call an
unverified header, out-of-order byte prefix or provisional manifest usable output.

Bound memory, in-flight chunks, disk staging, durable state and reassembly. Keep
resource reservations for admitted work through the documented failure cases.
Do not duplicate the entire corpus in memory or hide an unbounded application
queue outside the server's measured budgets.

### C2. Implement the PipeStream application and recovery

Build external authority executables against public library composition APIs;
the shipped Rust CLI has a fixed application registry, not a plugin-loading
interface. Java needs A-SERVER's public registration/composition surface. Do
not assume callbacks can be dynamically installed into either existing CLI.

Build an external Rust coordinator and actual registered worker callbacks using
the public reference APIs. Begin with Rust workers; add Java workers through A's
reviewed API. Demonstrate at least one mixed Java/Rust run and both reference
client directions as applicable to the scenario, without shelling through a
different language to pretend that language implements the protocol.

Use explicit initialization and reopen, mutual TLS/current authorization,
durable-work plus result-delivery, original immutable operations, scoped
checkpoint/coverage and distinct detach. State clearly how application chunk
sessions compose; use mode-1/mode-2 branch behavior where required by the chosen
application rather than relabeling a flat copy as recursive processing.

Kill/restart the coordinator after submission but before saving an ACK, a worker
after admission, a worker after result commit but before delivery, and a
coordinator during result download/reassembly. Recover the same identities and
bytes without reading server files or inventing a new attempt. Test explicit
retryable failure separately from restart and repeated reads. Include missing
chunk, wrong output, revoked reader and expired remote output cases with named
failure and no false completed file. Local retained copies remain distinguishable
from fresh remote authorization and availability.

### C3. Implement an equivalent durable streaming-gRPC baseline

Use an all-Rust, identically placed coordinator plus three worker processes for
the primary performance comparison on both protocols. Keep the required mixed
Java/Rust run as separate functional/integration evidence unless a matching gRPC
language/process topology is also implemented. Do not attribute JVM/process-count
differences to transport. Freeze identical chunk placement, principal identities,
CPU/cgroup limits and warm/cold state for each paired measurement.

The fairness contract must name the actual persistence boundaries: SQLite journal
mode/synchronous policy (or documented alternative), file and directory sync,
metadata commit preceding ACK, and handling of committed-but-unobserved receipts.
State the precise guarantee each acknowledgment makes on each backend.

Use actual HTTP/2 streaming gRPC with verified server identity and client
authentication, the same owners/authorization policy and equivalent processing
resources. The baseline must implement the application guarantees that gRPC
transport alone does not supply. In the equivalence matrix include:

- Durable session/work and immutable mutation identity; commit-before-ACK
  replay, changed-parameter refusal and non-reuse after history expiry.
- Verified incremental input before admitted work, durable jobs and outcomes,
  explicit authorized retry, original deadlines, current-attempt/worker/ancestor
  publication fencing and cancellation semantics for exercised operations.
- Same actual worker transform, chunk sizes and placement, concurrency limits,
  output promises, manifest commitments, bounded streaming result retrieval,
  hash/length validation and exact reconstruction.
- Independent execution/receipt/output/auth lifetimes, read/dependency pins,
  retained original outcomes, replayable cleanup and restart accounting.
- Same persistence acknowledgment point, synchronization/durability setting,
  stable journals, CPU/worker allowances, authentication work and failure model.

The baseline must not call PipeStream to obtain durability, share its production
state machine, or wrap its complete wire frames inside gRPC. It may share the
external deterministic transform, dataset generator, measurement code and
application-level identity types; enumerate shared versus independently owned
code. Compare semantic obligations, not forced identical network packaging:
gRPC may use its idiomatic service/stream shape, not an intentionally bad unary
RPC for every byte. Document any unsupported or non-equivalent obligation and
exclude comparative claims until resolved.

Use the same durable storage technology/settings where practical to isolate
transport and coordination effects; otherwise explain and measure the difference.
Do not use an in-memory baseline against a durable PipeStream run, or impose
extra persistence only on gRPC. Count the application protocol, state machine,
background maintenance and client journal required by each approach.

### C4. Measure correctness, cost and practical utility

On both sides, first usable output means a complete chunk with verified bytes,
length/hash, identity, committed outcome and replay metadata retained so the
coordinator can recover it after immediate process death. Earlier streaming
bytes/header arrivals may be reported separately, not compared to that milestone.

Record collector capability/permission checks before runs. Use scoped network
namespace/interface counters or packet capture with explicit loopback accounting;
include TLS/retry bytes and calibrate collection overhead. Freeze the shared
schedule schema with B but keep the runner independently buildable. Negative
controls must invalidate a run for swapped chunk order, correct-hash/wrong
transform, missing worker metrics, truncated artifacts, a killed collector or
stale binary/configuration hashes.

Use the same inputs, worker placement, machine limits, concurrency and failure
schedule. Separate runs so workloads do not contend with each other; record
hardware, OS/kernel, filesystem/storage, JVM/toolchains, transport/native
dependencies, TLS configuration, binary hashes and exact command/config files.
Do not extrapolate loopback results to a WAN or production deployment.

Measure at least:

- First independently verified usable output, total verified completion,
  per-chunk p50/p95/p99 with sample counts, and recovery time from each reached
  fault boundary. Queue time and unsuccessful runs remain visible.
- CPU time/utilization, Java/Rust heap, native/direct memory where applicable,
  each process's RSS/HWM and the full process-group total, threads and FDs.
- Logical retained bytes, file lengths, allocated blocks, actual read/write I/O
  and network bytes with a stated collection method. Retries, TLS/handshake and
  cleanup traffic are included or separately and explicitly reported.
- Application coordination source and persistent state/schema size, with
  generated code, common transform, reference implementation and baseline glue
  counted separately. Lines of code are a scoped proxy, not a quality score.
- Byte-exact final and partial output correctness, operation/attempt identity,
  no false completion, and capacity retained/recovered under failures.

Include zero/small inputs, uneven chunk counts, objects larger than windows and
a larger corpus that exceeds the process's allowed heap, streamed from disk.
Exercise a stopped consumer and a slow worker. Start with correctness smoke
runs, then a pinned concurrency/payload ladder; select feasible sizes with
explicit ceilings before the final run. Keep warmup separate, alternate run
order and repeat measured cases at least five times where feasible. Report
dispersion and raw observations; do not present p99 as meaningful with too few
samples. A failure or timeout is a failed run, never silently discarded noise.

Use B's compatible fault schedule format when available, but do not import its
oracle or require its full driver to build the application. Any measurement
that cannot be collected remains explicitly unavailable; no fabricated zeros
or inferred network bytes from payload size. Establish resource/correctness
gates before final measurement. Performance differences alone are findings,
not reasons to weaken a protocol or baseline guarantee.

## Acceptance and review handoff

Deliver independently buildable external projects, both authenticated durable
backends, failure/recovery demonstrations, a reviewed semantic-equivalence matrix,
the pinned runner and configurations, stable raw metrics/output/fault artifacts,
and an honest report. Include reproducible quick and full commands without
production credentials or implicit global dependency installs.

The final report separates proven correctness from measured performance and
unmeasured deployment assumptions. Identify where PipeStream reduces application
coordination, where it adds bytes/state/latency, and which normative requirements
need clarification or simplification. Put concrete clause-level correction
proposals in C's handoff for coordinated review; no marketing claim that a custom
URI scheme or successful benchmark establishes IETF acceptance.

Run application/baseline focused and full tests, resource/failure gates and
affected public-library integration tests through the assigned test runner.
Final mixed-language acceptance depends on A; full original-goal acceptance
also requires B. A standalone gRPC success or Rust-only demo is useful progress,
not completion of this assignment or the goal.
