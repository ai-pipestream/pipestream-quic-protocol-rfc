# Asynchronous RFC implementation handoff

## Current launch plan: Claude, Kimi and Meta

The user selected three concurrent agents. Give each its named file:
[CLAUDE.md](CLAUDE.md) owns both Java tickets,
[KIMI.md](KIMI.md) owns the neutral failure/resource driver, and
[META.md](META.md) owns the workload/gRPC comparison.
[TEAM.md](TEAM.md) defines shared-board locking, ownership and provisional peer
integration before the final coordinating review. It supersedes the older
four-assignee scheduling below; all A/B/C technical requirements remain intact.
The live board is `/work/worktrees/pipestream-rfc-coordination/TEAM-STATUS.md`,
outside their separate worktrees. Preparing these files has not launched agents.

Each launch prompt can be: "Read and obey your named assignment file in full,
including TEAM.md and the linked technical specification. Work in your assigned
worktree/branch, update your section of the shared board, and return the complete
tested handoff for final review. Do not merge the shared feature branch or main."
Add push authorization explicitly if desired; local committed checkpoints can
be exchanged between these same-host worktrees without pushing.

## Original scope and technical ticket breakdown

Prepared 2026-09-08. These assignments preserve the original three-part goal;
they do not replace it with a smaller acceptance target. Plan day-scale working
increments; an assignment may need more than one day or reviewed increment.
These are not runtime promises. Stop on completion evidence,
not elapsed time, token consumption, test count, or a convenient partial API.

## Starting checkpoint and honest status

Implementation base: `24975c241e152a893d9827b5c76493c1e1d5d0d5` on
`feat/durable-work-results-v2`. It is committed and published on Forgejo and its
GitHub mirror. It is **not merged into main**. Start from the subsequent commit
containing this handoff pack, with the implementation base in its ancestry.
Do not start from main and accidentally omit the durable implementation.

1. Original task 1 reached contract/design acceptance: Section 12, Appendix F,
   frozen examples and bounded executable models. Implementation findings still
   require corrections; this is not IETF approval or an unbounded proof.
2. Original task 2 is substantially implemented but not accepted. Rust has the
   durable authority, client and commands. Java has independent codecs, real
   authentication/Core transport, authority/input/output storage, execution,
   branch processing, fences, cleanup, retirement, control waits and connection
   ownership. The durable Java listener/client composition and the neutral
   two-direction failure/resource gates remain unfinished.
3. Original task 3 is open. Existing version-1 examples are not the required
   durable transform/reassembly workload or equivalent streaming-gRPC baseline.

Verified local checkpoint evidence:

- Java focused: 88 tests. Full install: 679 tests, zero failures/errors/skips,
  112 fresh XML reports; native storage guard, strict changed-type doclint,
  three existing-profile examples and draft checks pass.
- Latest full Rust verification is at ancestor `9372f08`: 816 passing tests
  and Clippy. No Rust production source changed in `24975c2`.
- Exact commands, artifact hashes and boundaries:
  [connection ownership](../../../conformance/results/durable-work-v2-connection-ownership-2026-09-08.txt)
  and [control waits](../../../conformance/results/durable-work-v2-control-waits-2026-09-08.txt).
- The [raw checkpoint archive](../../../conformance/results/durable-work-v2-connection-ownership-2026-09-08.raw.tar.gz)
  preserves all seven logs/exits and 112 final Java XML reports; its SHA-256 and
  byte-for-byte provenance are in the connection-ownership record. Reviewers
  need not rely on this host retaining `/tmp` for that checkpoint.
- These are local results, not a claim of new hosted CI, V2 interoperability,
  main merge, release, deployment or draft submission.

## Assignments and integration order

| Assignment | Owns | Can start now | Final dependency |
| --- | --- | --- | --- |
| [A-SERVER: Java durable authority](A-java-durable-endpoints.md) | Server composition, host API and server adapter | Yes, against existing Rust client | Real Rust-client/Java-server acceptance |
| [A-CLIENT: Java durable client](A-java-durable-endpoints.md) | Client journal, file delivery, public API and adapter | Yes, against existing Rust server | Real Java-client/Rust-server acceptance |
| [B: Independent failure and resource certification](B-neutral-failure-driver.md) | Neutral Rust driver and requirement evidence | Yes, including Rust-only preparation | Both A artifacts; real both-direction matrix |
| [C: External workload and equivalent gRPC baseline](C-workload-grpc-comparison.md) | External applications, baseline, comparative runner | Yes, including standalone gRPC and Rust workload | A for mixed-language run; compatible B failure schedules |

There are three workstreams but four assignable tickets. A-SERVER and A-CLIENT
have separate scopes in A and can develop against the opposite Rust peer without
sharing a worktree or waiting for each other. Both are on the critical path.
B and C must not wait idly for A: their independent
process machinery, workload semantics, reference outputs and baseline are useful
work now. But an unavailable Java artifact is a named incomplete integration
gate, never a passing skip. Review both A tickets, then B, then C's final comparison.
Review C's semantic-equivalence contract before spending time on large benchmarks.

Use a separate git worktree and branch for each assignment. The primary checkout
is `/work/main/pipestream-ai/dev-tools/pipestream-quic-protocol-rfc`.
Suggested worktrees under `/work/worktrees`: `pipestream-rfc-java-server-v2`,
`pipestream-rfc-java-client-v2`,
`pipestream-rfc-neutral-v2`, `pipestream-rfc-workload-v2`; first check that the
chosen path and branch do not already belong to someone. Do not create or delete
these merely by reading this plan. Never share target/build directories between
concurrently running assignments. Reference dependencies belong under
`/work/reference-code`, not documentation directories.

After A is reviewed, integrate it by a normal merge into B/C's branches and
rerun their affected gates. Do not rewrite published branches or silently test
against an uncommitted sibling checkout. Record every dependency commit and
binary hash. The coordinating review owns final aggregation into the feature
branch and any decision to merge main. An agent does not self-certify that merge.

## Required reading and source precedence

Read the applicable workspace/repository `AGENTS.md` before edits. Read this
file and the assigned specification in full. Then read:

- [Section 12](../../../sections-src/section-12.md) and
  [Appendix F](../../../sections-src/appendix-f.md), in full: the complete V2
  normative source, not the version-1 layer terminology.
- [Decisions](../../standards/durable-work-v2-decisions.md),
  [frozen examples](../../../test-vectors/v2/README.md), and the current
  [execution record](../../standards/durable-work-results-goal.md): required
  order, acceptance and execution decisions at its start, then relevant latest
  checkpoints. The older chronological entries are historical evidence.
- The V2-WIRE through V2-STORE requirement families in the
  [acceptance ledger](../../standards/durable-work-v2-test-plan.md), plus the
  implementation evidence relevant to the assigned files.
- The implementation README and actual code/tests for the surfaces changed.

The normative source defines intended behavior; code and tests establish what
exists. If they disagree, reproduce the defect and report the exact clause.
Do not treat a historical README claim, a passing mock, or either production
implementation as the final semantic oracle. No backward compatibility is
required. Preserve existing regression evidence unless a reviewed normative
correction actually invalidates it; do not remove tests merely to turn green.

## Parallel ownership and specification corrections

A-SERVER owns Java authority composition/shared transport and existing build
configuration. A-CLIENT owns new client/journal/file-adapter sources and tests;
A defines the overlap procedure. B owns `implementations/rust-quinn/conformance/`, its necessary small
Rust process fixtures, and `conformance/run_all.sh`. C owns the new external
workload/baseline and benchmark directories listed in its assignment.

Each assignment owns its own new evidence record and `handoff.md` under a
task-specific directory. Do not concurrently rewrite the shared execution
record, acceptance ledger, root README, Section 12, Appendix F or frozen vectors.
For a normative correction, put a clause-specific proposal with a red test and
expected semantics in the assignment's handoff directory. The coordinator
resolves it and synchronizes source, CDDL, examples, models and both languages
when affected. Never weaken an invariant to fit one implementation.

If a production Rust defect or shared native-transport change blocks progress,
report a minimal reproducer and exact file ownership request. Isolate the fix
on a reviewable commit once ownership is agreed. Do not let B's independent
driver silently repair its subject and then certify that subject using the
same code. Interface changes must be documented before downstream use.

## Work and verification rules

### Shared test-adapter interface to freeze before integration

This is fixture orchestration, not another PipeStream protocol or a production
admin API. Keep its versioned schema in each assignment's evidence and compare
the schema hash before combining artifacts. The first interface document must
define bounded UTF-8 TSV records with exact columns for version, run/scenario ID,
subject language/role, process start identity, event sequence, reached boundary,
operation/work/attempt identity, observed refusal code, and relative artifact
path/length/SHA-256. Use exact decimal integers and hexadecimal binary fields;
escape labels unambiguously. No credentials or private key contents in evidence.
Publish complete records atomically inside fixture-owned directories, with
bounded file/event counts; truncated, stale, unknown-version or mismatched-run
records fail the run. Readiness is not acceptance and events are not outcomes.

Freeze a corresponding schedule schema naming target, reached boundary, action
(disconnect, stop/kill, restart, pause/release, or fixture-clock change), seed
and deadline. No arbitrary shell command embedded in a schedule. B owns its
driver implementation; C can implement the same frozen schedule independently
without depending on B's crate. Interface revisions are explicit proposals,
not silent changes to a sibling checkout.

A-SERVER/A-CLIENT supply their Java test-only hooks. B may supply minimal Rust
subject hooks in a separate test-only adapter and separately reviewable commit.
Hooks may pause/report real boundaries; they cannot forge commits, receipts,
callbacks or protocol results. Keep them out of shipped release launchers.
The coordinator reviews hook placement against the actual transaction/write
path before accepting boundary coverage. B's oracle has no dependency on subject
helpers; it independently validates actual wire/refusal/output evidence.
Lost-ACK claims require a reached committed boundary and withheld/dropped reply,
not just a guessed sleep before killing a process. Neither existing CLI provides
this complete stable fixture contract today; building it is explicit task work.

### Implementation and evidence discipline

- No Python implementation, test oracle, workload or benchmark harness; no
  JSON conversion for protocol parsing, commitments or durable exact integers.
  Use typed records and the actual frozen binary contract. Small orchestration
  shell scripts are acceptable; they cannot replace reference behavior.
- No stubs, manufactured outcomes, passing missing-artifact skips, silent
  profile downgrade, new work identity on uncertain retry, or digest-only
  success in place of actual output bytes. No premature profile advertisement.
- No force-push, history reset/cleanup, main merge, release, IETF submission,
  live fleet changes, deployment, or unrelated repository work. Preserve other
  edits. Do not delete existing stores/generations to make recovery tests pass.
- Commit coherent tested increments to the assignment branch. Push only if
  the user's assignment authorizes it, Forgejo first. GitHub is a mirror;
  verify the exact remote hash rather than assuming replication. Pull the
  assigned branch normally before publishing and stop if remote divergence
  requires review. Never overwrite other PR merges.
- When these models are available, Sol writes tests/simple tasks; Terra runs
  builds/tests and mechanical fixes; Astra coordinates, handles design and
  substantive production changes, and reviews source plus raw evidence.
  Freeze all new test sources before a build. Other LLMs assigned by the user
  can implement the task without these specific tools, but must preserve the
  same implementation/test/evidence separation and report who did what.
- Serialize expensive native builds; reuse a verified source-pinned transport
  artifact where appropriate. Use distinct build outputs and isolated Maven
  repositories. Do not replace the patched transport with an upstream binary
  simply because it compiles. Missing tooling is a reproducibility gap to solve
  or report, not permission to remove the gate.
- Focused red-test/fix/green evidence first, then the affected full suites.
  Preserve terminal exit status, exact commands/toolchain/commit/artifact hashes,
  fresh report counts, failures and exclusions. A started process or log tail
  is not completion. Do not call a deterministic failure flaky.

## Definition of a review-ready handoff

Check in a concise handoff with:

1. Branch, base, final commit, dirty state, upstream dependency commits, pushed
   refs and mirror/CI status separately. No claim that commits are merged.
2. Requirement-to-source-to-test-to-raw-evidence map, with each missing gate
   explicitly incomplete. Test count alone is not coverage.
3. Exact build/run commands and environment, checksums of tested binaries,
   direct exits, fresh XML/structured counts, failures and their fixes. Preserve
   sanitized raw artifacts in the repo or a stable retrievable archive with
   hashes; `/tmp` paths alone are not an asynchronous handoff.
4. A narrow diff/risk summary: authority/authentication, persistent format,
   resource ownership, native credit and deadlines; any spec changes proposed.
5. Known limitations, external effects not covered, measurement scopes, and
   safe next action. If interrupted, preserve resumable work without calling
   the assignment done. Do not burn a day polling unchanged dependencies.

The returning review checks the exact diff and dependency versions, independently
reads raw evidence, reproduces high-risk failures through the test runner, and
reviews normative traceability before integration. Shared green integration
gates still run after merging the reviewed assignment branches.

## Copy/paste prompts

For each agent, give it the repository and its own worktree, then use:

> Read and obey docs/plans/async-rfc-2026-09/README.md and
> docs/plans/async-rfc-2026-09/A-java-durable-endpoints.md in full. Your ticket is
> A-SERVER, as scoped there. Implement its entire assigned behavior, not a cheaper
> substitute. Start from the published
> feat/durable-work-results-v2 handoff checkpoint, not main. Work only in your
> assigned worktree/branch. Commit coherent tested increments; do not merge main
> or push without explicit authorization. Produce the required durable handoff
> and raw evidence for this chat to review. Incomplete dependencies remain
> explicitly incomplete, not passing skips.

For the independent client, change the ticket to A-CLIENT. For B or C,
substitute `B-neutral-failure-driver.md` or `C-workload-grpc-comparison.md` and
name that ticket instead. The user controls when to launch these tasks and
when to pause/resume the coordinating goal; this document launches no agents.
