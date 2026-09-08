# Meta assignment: real workload and equivalent durable gRPC baseline

You own the external application and comparative evidence. Claude owns Java
integration; Kimi owns independent protocol certification. Work outside their
production trees. The goal is a useful, honestly measured system, not a benchmark
constructed to make PipeStream win.

## Read and obey

Read [TEAM.md](TEAM.md), [the shared handoff](README.md), and
[C-workload-grpc-comparison.md](C-workload-grpc-comparison.md) in full. Follow all
technical and evidence requirements. TEAM.md updates only the three-agent
allocation and peer integration sequence; it does not reduce the original goal.

Read applicable AGENTS.md, Section 12, Appendix F, the current public Rust V2
API/CLI and relevant examples. You are building actual Rust applications and
streaming gRPC, not a Python orchestrator or a prototype that reads authority
directories instead of fetching authenticated results.

## Checkout and ownership

- Source repository: `/work/main/pipestream-ai/dev-tools/pipestream-quic-protocol-rfc`.
- Assigned worktree: `/work/worktrees/pipestream-rfc-meta`.
- Assigned branch: `agent/rfc-meta-workload-v2`.
- Start from the published feature-branch commit containing this file, with
  `82a1b1133974734553b5fc120fc8954ed3c5fdaf` in its ancestry, never main.
- Inspect existing path/branch/dirty state before creation or reuse.
- Own `examples/durable-transform-workload/`,
  `examples/durable-transform-grpc/` and `benchmarks/durable-transform/`.
  A comparison-local `.proto` belongs in the baseline project, not unrelated
  platform contracts. Do not modify Java, the neutral driver or the normative
  source to fit the application.

## Work immediately, without waiting for Java

1. Record STARTED/base on the live board. Write the exact workload/fairness
   contract first: deterministic real transform, input seeds/chunking, operation
   identity, authentication, durable ACK boundaries, recovery, output guarantees,
   resources and metrics. Request Kimi's scoped peer review. Keep implementing
   the standalone pieces while that review is pending.
2. Build the streaming input generator and an independent byte-level transform/
   reconstruction oracle. Cover empty, partial, binary and large inputs. Keep
   buffers, in-flight work, journals, staging and reassembly bounded.
3. Build the external PipeStream coordinator and Rust authority application
   executables against actual public APIs. Existing CLIs have fixed callback
   registries; do not invent plugin loading. Start with all-Rust workers and
   real authenticated result fetches, durable outcomes and restart recovery.
4. Implement the independent authenticated durable streaming-gRPC baseline.
   Its application-level identity, journals, admitted work, retry/fencing,
   outcome/result promises, pins and cleanup must meet C's equivalence matrix.
   Never implement its durability by calling PipeStream or tunneling its frames.
5. Publish your application-registration/API needs to Claude early. Once a
   tested public Java server checkpoint is available, add the required mixed
   Java/Rust functional run. Do not wait for Java to build the baseline/oracles.
6. After peer review of equivalence, run correctness/failure gates, then pinned
   comparative measurements. Reuse Kimi's frozen schedule schema where useful
   without importing the neutral oracle or making the application depend on its
   crate. Report discovered Java defects to Claude with reproducible evidence.

## Fairness and measurement requirements

The primary measured arms have identical all-Rust coordinator/worker topology,
three worker processes, placement, CPU limits and persistence promises. Mixed
Java/Rust is separately labeled functional evidence unless a matching gRPC
topology is also implemented. Do not attribute JVM/topology costs to transport.

Use the same actual transform, data, chunk sizes, auth identities, synchronization
settings, retry/deadline/output semantics and failure schedules on both sides.
First usable output means a fully verified committed chunk with recoverable
coordinator metadata, not first byte/header. Verify final bytes and identity.

Record collector capability/overhead, raw CPU/heap/native/RSS/FD data, disk sizes
and actual I/O/network traffic, completion/tail/recovery samples and scoped
coordination-code/state costs. Freeze gates before final runs. Detect stale
binaries, corrupt/swapped outputs, missing worker samples and dead collectors.
An unavailable mandatory measurement remains incomplete; no invented zeros.
Report failures, dispersion and all scope limitations. Performance regression
is a finding, not grounds to weaken one side's guarantees.

## Completion and return

Deliver buildable external applications, an independent durable gRPC baseline,
the reviewed equivalence matrix, actual reconnect/worker/coordinator crash
demonstrations, mixed-language integration, and pinned reproducible full/quick
runner commands. Preserve raw artifacts, hashes, measurements and fault traces.

Final `benchmarks/durable-transform/handoff.md` must contain exact commit/artifact
pins, gate results, known gaps and an honest comparison report. Propose concrete
spec improvements separately; do not claim IETF acceptance or a network-wide
speed advantage from a loopback test. No production deployment or draft submission.
Mark REVIEW_READY only after the complete C assignment passes, not after a
Rust-only smoke test or a successful standalone gRPC server.
