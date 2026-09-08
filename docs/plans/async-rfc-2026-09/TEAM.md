# Claude / Kimi / Meta: concurrent execution agreement

This is the user's current three-agent assignment arrangement. It supersedes
the four-assignee allocation and requirement for the coordinating chat to review
each dependency before peers can test it. It does not supersede the protocol,
the full A/B/C specifications, safety constraints or final acceptance review.
The coordinating chat reviews the completed branches afterward.

## Ownership

| Agent | Complete assignment | Branch | Worktree |
| --- | --- | --- | --- |
| Claude | A-SERVER and A-CLIENT together | `agent/rfc-claude-java-v2` | `/work/worktrees/pipestream-rfc-claude` |
| Kimi | B neutral failure/resource certification | `agent/rfc-kimi-neutral-v2` | `/work/worktrees/pipestream-rfc-kimi` |
| Meta | C external workload and gRPC comparison | `agent/rfc-meta-workload-v2` | `/work/worktrees/pipestream-rfc-meta` |

These are assignments, not assertions about vendor rankings. Java stays under
one owner because server/client, shared wire types and transport lifetimes are
coupled. The neutral oracle is separately owned. The external application has
a disjoint code tree and a concrete independent correctness/fairness contract.
All three may require multiple day-scale increments; elapsed time is not done.

The primary source is
`/work/main/pipestream-ai/dev-tools/pipestream-quic-protocol-rfc`.
Start from the published `feat/durable-work-results-v2` commit containing these
named files, with `82a1b1133974734553b5fc120fc8954ed3c5fdaf` in its ancestry.
Read `/work/main/AGENTS.md` explicitly as well as instructions applicable to
the new worktree. Inspect path/branch/dirty state; never reset another checkout.
Create your named worktree/branch only if it does not already belong to anyone.
Record the exact base before edits. Never start from main or move the shared
feature branch while another agent is using it as a checkpoint.

## One actual shared board

Live board for agents on this same host/filesystem:

`/work/worktrees/pipestream-rfc-coordination/TEAM-STATUS.md`

This intentionally lives outside all three Git worktrees. A tracked file in each
branch is a separate copy and is not a live collaboration board. The repo's
[TEAM-STATUS.template.md](TEAM-STATUS.template.md) is a recoverable starting
template, not the live copy. Do not overwrite the live board from the template
on startup. If running on different machines, use a genuinely shared mount or
agree a hosted coordination location with the user; local copies do not sync.

Each agent owns only its own section. Include UTC update time, branch/worktree,
tested commit, current phase, terminal gate evidence, exported interface/artifact
pins, directed requests and next independent action. Update at meaningful
milestones or roughly every 30 minutes during long active work, not every command.
Read before changing a shared interface, consuming a dependency or declaring a
blocker. Do not spend turns polling unchanged status or treating a stale timestamp
as evidence that a process died.

All modifications to this shared file require an exclusive short `flock` on
`/work/worktrees/pipestream-rfc-coordination/TEAM-STATUS.lock` around the fresh
read/patch. Never lock it during a build, network wait or model deliberation.
Use narrow edits to your own section and preserve all other sections. For agents
with the `apply_patch` CLI available, pass the patch on stdin in one invocation:

```bash
flock -w 30 /work/worktrees/pipestream-rfc-coordination/TEAM-STATUS.lock apply_patch <<'PATCH'
*** Begin Patch
*** Update File: /work/worktrees/pipestream-rfc-coordination/TEAM-STATUS.md
@@
-State: NOT_STARTED (claude)
+State: RUNNING (claude)
*** End Patch
PATCH
```

Reread and reconstruct the patch if its context no longer matches; never replace
the whole board from a stale snapshot. Other editing tools must hold the same
lock through their edit. If that cannot be done, leave a uniquely named message
file in the shared directory and request incorporation; do not perform an
unlocked concurrent overwrite. Do not delete the lock file to bypass a holder.
The file's existence is not a held lock; the OS lock determines ownership.

At handoff, commit a snapshot of your own section and directed messages in your
own evidence directory. The live board is coordination, not a test oracle or
substitute for durable commit/log artifacts. No keys/tokens or credential dumps.

## Interface exchange without waiting for the final review

1. Kimi proposes the versioned fixture event/barrier/schedule contract first;
   Claude acknowledges the Java adapter portion, Meta the workload schedule
   portion. Claude publishes actual public host/client and callback-registration
   signatures early. Meta publishes its fairness contract for Kimi's scoped
   review before large comparative runs. Record explicit proposal/acknowledgment
   commit hashes in each agent's own board section; unchanged silence is not assent.
2. Each producer commits a coherent tested checkpoint and advertises its exact
   hash, artifact hashes, gate commands/results and limitations. Consumers may
   inspect, build and normally merge that immutable checkpoint into their own
   branch for provisional integration, without waiting for this chat to return.
   All worktrees of this local repo share Git objects, so local commits can be
   exchanged without a push. Never consume uncommitted sibling files/build output.
3. This peer integration is not final review or authority to merge the shared
   feature/main branches. Track subject, adapter, oracle and application pins
   separately. A final failure requires a new producer commit and consumer rerun;
   never edit someone else's worktree or silently change the acceptance oracle.
4. Before integrating a peer commit, check for changes outside its ownership and
   new transitive dependencies. Prefer narrow published checkpoint commits to
   avoid importing another consumer's unfinished changes. If a normal merge
   conflicts in owned code, ask its owner; do not choose 'ours'/'theirs' blindly.
5. Cross-checks can proceed independently: Claude uses existing Rust peers;
   Kimi builds the driver and Rust scenarios; Meta builds all-Rust workload and
   gRPC baseline. Later Kimi consumes Java server/client, Meta consumes Java host.
   Missing Java gates remain incomplete, not waived to make everyone finish.

Routine peer API/hook changes can be agreed this way. Normative contradictions
must be documented with an exact clause and reproducer; do not silently relax
the standard. If a genuine protocol decision cannot be made consistently with
the current text, report it for the returning review and complete unaffected
work. Nobody waits for permission for ordinary in-scope implementation.

## Builds, publication and final status

Use your own target/build directories, fixture roots, ports and isolated Maven
cache. Serialize expensive native transport rebuilds with a common OS lock at
`/work/worktrees/pipestream-rfc-coordination/NATIVE-BUILD.lock`; release on process
exit, preserve direct exit status. Normal independent tests may run concurrently
within host resources. Coordinate whole-machine measurements on the board and
use a common `BENCHMARK.lock` for heavy builds, full native/Maven/Cargo suites
and final performance runs so those do not overlap. If both locks are needed,
always acquire BENCHMARK.lock before NATIVE-BUILD.lock and release in reverse
order. The lock cannot exclude unrelated host workloads: record them
and reject materially contended measurements. Never stop another live service.

Claude/Kimi/Meta may implement and run their own tests using their available
tools; the earlier Sol/Terra/Astra allocation describes this coordinating chat,
not a requirement to call unavailable models. Preserve independent regression
tests, raw evidence and final review. No Python oracle/implementation/harness.

Commit tested increments to your own branch. Push only when the user's launch
instructions authorize it; Forgejo first, GitHub mirror checked separately.
No force push, main/shared-feature merge, release, deployment or IETF submission.
Do not block same-host dependency exchange merely because a push is unauthorized.

Allowed board states: NOT_STARTED, RUNNING, DEPENDENCY_READY, NEEDS_INPUT,
REVIEW_READY. DEPENDENCY_READY does not mean whole assignment complete.
NEEDS_INPUT identifies the exact missing choice/artifact and unaffected work
remaining. REVIEW_READY requires the entire assigned A, B or C acceptance plus
durable handoff; missing final cross-language/measurement gates prohibit it.

Once all three are REVIEW_READY, stop implementation. Each returns branch/head,
base, dependency pins, handoff path, exact verification and known risks to the
user. The coordinating chat then reviews code and raw evidence, integrates
accepted changes and runs final combined gates. No peer can mark the original
three-part goal accepted on behalf of that review.
