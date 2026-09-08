# Kimi assignment: independent failure and resource certification

You own the neutral driver and complete both-direction acceptance evidence.
Claude implements the Java server/client; Meta implements the external workload
and equivalent gRPC baseline. Your role is to expose errors in real behavior,
not adapt expected results to make either implementation look correct.

## Read and obey

Read [TEAM.md](TEAM.md), [the shared handoff](README.md), and
[B-neutral-failure-driver.md](B-neutral-failure-driver.md) in full. Follow all
their technical acceptance/evidence rules. TEAM.md supersedes only the older
four-assignee allocation and mid-flight integration-review sequence.

Read applicable AGENTS.md, Section 12, Appendix F, frozen vectors, the acceptance
ledger and actual driver code. The current driver lacks complete V2 durable
certification. Legacy examples and Rust-only integration are not its replacement.

## Checkout and ownership

- Source repository: `/work/main/pipestream-ai/dev-tools/pipestream-quic-protocol-rfc`.
- Assigned worktree: `/work/worktrees/pipestream-rfc-kimi`.
- Assigned branch: `agent/rfc-kimi-neutral-v2`.
- Start from the published feature-branch commit containing this file, with
  `82a1b1133974734553b5fc120fc8954ed3c5fdaf` in its ancestry, never main.
- Inspect existing path/branch/dirty state before creation or reuse.
- Own `implementations/rust-quinn/conformance/`, `conformance/run_all.sh`,
  narrowly scoped Rust subject test adapters in separately reviewable commits,
  and `conformance/results/async-neutral-v2/`. Claude owns Java; Meta owns
  workload/baseline code. Do not edit their trees or rewrite their results.

## Work immediately, without waiting for Java

1. Record STARTED/base on the live board. Define the concrete bounded fixture
   event and fault-schedule schema required by the shared plan. Own its initial
   proposal under `conformance/results/async-neutral-v2/interface-v1.md`.
   Publish the commit and schema hash; request Claude/Meta acknowledgement of
   interfaces they consume. They can proceed with independent work meanwhile.
2. Build the neutral command, process ownership/readiness/timeout machinery,
   certificate/root provisioning, independently derived expected outcomes,
   negative controls and capability/measurement collectors. Start real scenarios
   against the existing Rust authority/client. Development partial runs must
   report incomplete, never full conformance PASS.
3. Make process-death tests reach actual commit boundaries. Keep subject hooks
   separate from the independent oracle and out of production launchers. Ask
   Claude to inspect Rust hook placement; inspect Claude's Java hook placement.
   Record these as peer checks, not final review. A guessed sleep is not a
   committed-before-lost-ACK boundary.
4. Take Claude's SERVER_READY checkpoint and exercise Rust-client/Java-server.
   Take CLIENT_READY and exercise Java-client/Rust-server. Consume immutable
   commits/artifacts with hashes, not a live sibling checkout or uncommitted JAR.
5. Complete the full B scenario matrix in both directions, including actual
   client and authority deaths, named refusal scope, missing descendants,
   stale publication, authorization changes, retention/cleanup, paused readers,
   resource ceilings and control progress. Keep private storage probes clearly
   supplementary to black-box network recovery evidence.
6. Review Meta's initial fairness contract against the original goal before
   Meta spends time on large measurements. Record concrete mismatches or a
   scoped peer acknowledgement. You are not responsible for writing the gRPC
   baseline; protect your driver's independence and prioritize its own gates.

## Independence and remediation

Mechanically prohibit production codec/client/authority/helper dependencies in
the neutral oracle. Check actual output bytes/identities/refusals independently.
Fake evidence, stale binary pins, skipped missing prerequisites and dead metric
collectors must be rejected by negative controls.

Report defects with scenario/seed, reached boundary, binary hashes, expected
versus actual result and smallest reproducible command. Java fixes go to Claude;
workload/baseline fixes go to Meta. For a Rust production defect, propose a
separate subject-fix commit with red evidence and obtain a recorded peer review
before using it as a certification target. Do not silently fix and certify a
subject with shared test logic. Final coordinator review remains required.

## Completion and return

Every externally meaningful B family runs both language directions; justified
private-storage exceptions are explicit. Whole-process/native memory, heap,
disk lengths/blocks/I/O and measured network bytes retain their separate scopes.
Missing mandatory metrics or missing Java scenarios are incomplete, not zeros
or passing skips. Add full acceptance to the actual invoked conformance gate.

Commit coherent increments and publish usable dependency pins on the board.
Follow TEAM.md for push authority and provisional peer integration. No shared
feature/main merge, release, deployment, fleet changes or IETF submission.

Final `conformance/results/async-neutral-v2/handoff.md`: complete requirement/
direction/scenario/source/evidence map; raw logs, direct exits and resource
samples; expected and observed values; exact subject/hook/driver pins; all
negative-control evidence; failures/fixes and remaining exclusions. Mark
REVIEW_READY only when the whole B contract passes; Rust-only preparation is
useful progress, not completion.
