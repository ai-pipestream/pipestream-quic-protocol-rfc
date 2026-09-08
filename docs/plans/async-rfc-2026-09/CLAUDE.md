# Claude assignment: complete Java V2 integration

You own the complete Java track: both A-SERVER and A-CLIENT. The user is running
Kimi on independent certification and Meta on the external workload/baseline.
They need your public interfaces and immutable tested checkpoints early; they
do not need to wait for your entire assignment to begin their work.

## Read and obey

Read [TEAM.md](TEAM.md), [the shared handoff](README.md), and
[A-java-durable-endpoints.md](A-java-durable-endpoints.md) in full. Follow their
normative-source, test, storage, security and evidence requirements. TEAM.md is
the current three-agent ownership/integration arrangement; it supersedes the
older four-assignee allocation, not the technical acceptance criteria.

Read the applicable AGENTS.md, Section 12, Appendix F and actual relevant
code/tests before editing. You must implement the full current contract, not
another subset. No backward compatibility is required; no implicit conversion,
profile downgrade, mocked durable behavior or Python implementation is allowed.

## Checkout and ownership

- Source repository: `/work/main/pipestream-ai/dev-tools/pipestream-quic-protocol-rfc`.
- Assigned worktree: `/work/worktrees/pipestream-rfc-claude`.
- Assigned branch: `agent/rfc-claude-java-v2`.
- Start from the published feature-branch commit containing this file, with
  `82a1b1133974734553b5fc120fc8954ed3c5fdaf` in its ancestry, never main.
- Inspect existing branch/path/dirty state before creating or reusing anything.
  Work only in your own worktree; do not edit the source checkout.
- Own `implementations/java-netty/`, Java public/test adapters and
  `conformance/results/async-java-v2/`. Kimi owns the neutral Rust driver;
  Meta owns external examples/benchmarks. No shared production-file edits by
  another agent without your explicit recorded ownership agreement.

## Ordered work and publishable checkpoints

1. Record STARTED and the exact base in the live team board. Inspect the actual
   package-private storage/runtime and public Core-only endpoint boundary.
   Agree early with Kimi on the versioned test-adapter events/barriers and with
   Meta on public authority application-registration APIs. Publish a concrete
   signature/configuration document before consumers write against invented APIs.
2. Compose the real Java durable server: matched storage, current owner gate,
   bounded worker/retention/result services, native credit, request ownership,
   streamed admission/results and control waits. First exercise it with the
   existing Rust client; do not wait for a new Java client. Then finish every
   selected-profile operation, branch mode, refusal and lifecycle gate in A.
3. Build the independent Java durable client, journal and owned file adapters.
   Exercise it against the existing Rust authority. Preserve original immutable
   intent and uncertainty through actual process death, not just reconnect.
   Complete all mutation, observation, result and scope/parent coverage rules.
4. Supply separate V2 launchers, public callback registration and test-only
   boundary hooks outside shipped launchers. Keep legacy commands explicitly
   separate. Real Java/Java composition must also pass after both sides exist.
5. Publish small tested dependency checkpoints throughout, not one final giant
   patch. Mark SERVER_READY and CLIENT_READY independently with commit/artifact
   hashes, entry points, exact commands, supported contract and remaining gaps.
   These are provisional integration milestones, never whole-profile acceptance.
6. Investigate Kimi's failing independent scenarios and Meta's integration defects.
   Fix production bugs in your owned tree with red-test/fix/green evidence;
   publish replacement pins. Do not change their oracle or weaken their baseline.

## Specific risks you must close

Read A in full for the complete requirements. In particular: DB-future completion
is not physical ownership release; read pins survive busy I/O; FIN is not admission;
native write completion is not peer persistence. Keep COMPLETE exclusion through
the actual response path and honor DETACH/half-close ordering. No SQLite/file
hashing/callbacks on a Netty event loop. Shared authorization must remain correct
across worker threads, live revocation and owner-map changes. Full Java client
coverage must validate parent/child observations in both arrival orders.

## Completion and return

Run focused and full Java gates, changed-type strict doclint, required native
checks when changed, real opposite-Rust integration and the combined Java run.
Use the source-pinned native artifact and report its hash. Preserve fresh XML,
raw logs/direct exits and checksums. A historical 679-test pass is your baseline,
not evidence that these new endpoints work.

Commit coherent increments; follow TEAM.md for peer dependency exchange and
push authorization. Never merge the shared feature branch/main, deploy or submit
a draft. Keep working on your independent backlog while another agent is busy;
do not repeatedly poll an unchanged board.

Your final `conformance/results/async-java-v2/handoff.md` must map the entire
A server/client contract to source/tests/raw evidence, list exact pins accepted
by Kimi/Meta, disclose every missing gate and proposed normative correction,
and state clean/dirty, committed/pushed/CI status separately. Mark REVIEW_READY
only after your assigned gates pass. Final acceptance belongs to the returning
coordinating review, not a peer's status message.
