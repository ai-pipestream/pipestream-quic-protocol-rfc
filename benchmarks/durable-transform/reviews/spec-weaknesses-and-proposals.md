# Workload-side review: code improvements and spec weaknesses with proposals

Author: Meta (workload C). Date: 2026-09-09. Status: proposals only;
no implementation changed by this note. Evidence: full run
(`b3dd5e34…`, 10/10 arms + 2 faults), mixed run vs Claude `63d03a0`
(42 Java chunks, byte-identical), throwaway ceiling=1 proof
(119 refusals retried, byte-identical).

## 1. Code improvements (workload-owned)

1.1 Replace the admit burst with a credit window. The per-session admit
loop (`examples/durable-transform-workload/workload-coordinator/src/main.rs`,
`run_session`) is
lock-step sequential, so live jobs pile up exactly as fast as the worker
lags; the backoff retry masks the dynamics instead of controlling them.
A window of N outstanding admissions, refilled on receipts, would make
throughput predictable and refusals rare by construction.

1.2 Make the recovery boundary principled. Recovery is now two-tier:
authority `Refused` retries in-process, everything else (connection loss,
client-side deadlines) kills the coordinator for operator `--resume`.
That split is pragmatic, not designed. Decide once, in the contract,
which error classes the client must absorb and which must surface, and
encode it in one place instead of spreading it across `send_admission`
and the demo scripts.

1.3 Document the `receipt()` sharp edge. Unknown operations report as an
error, not `None` (`replay_unresolved` already works around this).
Every future caller needs that invariant written down, not discovered.

1.4 Runner hygiene, mostly done: EXIT traps (C6) and ready-file gates
(C9) are in. Remaining: `run-full.sh` holds `BENCHMARK.lock` for the
whole suite including slow fault demos; per-phase locking would unblock
peer builds sooner.

## 2. Shared-implementation nits

2.1 `V2Main` refuses to start when the ready file already exists
(Claude `api-plan.md` §1.5, `conformance/results/async-java-v2/`).
Every operator script
must clean it first; a `--ready-file-overwrite` or stale-file reclaim
would remove a restart footgun.

## 3. Spec weaknesses with concrete proposals

3.1 Refusals without a client rule. The spec names refusal codes
precisely (Section 12.2) but does not state what a conforming client
must do on each one, so two conforming clients may diverge exactly
where deployments live. My coordinator had to invent the policy.
Proposal: add a "client conformance on refusal" table, e.g.
LIMIT_EXCEEDED → retry the identical operation identity with bounded
backoff, never a new identity; CONFLICT → must not resubmit, must
reconcile by receipt; NOT_READY → wait, not spin. Add one test vector
per row: server refuses N times then accepts; the client must produce
exactly-once effects. Kimi's fixture schedule already has kill actions;
add a refuse-N-times action to drive these vectors on both servers.

3.2 Capacity discovered by refusal. The authority enforces live-job and
retained-byte ceilings (e.g. `capacity()` in
`implementations/rust-quinn/src/v2/authority/jobs.rs`), but nothing I
read advertises them before admission, so clients probe the ceiling by
hitting it — burning journal entries and, under load, looking like
flakiness (this killed two pilot runs before the retry existed).
Proposal: advertise ceilings at session establishment (or a limits
query): active-job, retained-input/output budgets per session. Clients
SHOULD pace admissions under the advertised window; refusals then mean
genuine contention, not discovery. Add a Kimi matrix row: a paced
client completes a full run with zero refusals against both servers.

3.3 The exactly-once burden sits entirely with the app. Multiple
physical invocations are possible by design and `RestartSafety` is the
application's promise. That honesty is good, but app authors get no
standard patterns. Proposal: a non-normative appendix of restart-safety
recipes (idempotence keys, fencing tokens, lease-renewal loops) plus
conformance labels an app can claim ("idempotent under replay",
"fenced"), each with a replay test. This turns a burden into a
checklist.

3.4 Unilateral liveness caps. Client-side stream lifetime/idle deadlines
(`bounded()` in `quinn/src/v2_client/transport/objects.rs`) turn a
stall into run death, and on a loaded host "slow" is indistinguishable
from "stalled" — I watched this kill a run that would have completed.
Proposal: negotiate the caps at session open (client proposes within a
server-published maximum), reset the idle deadline on any wire
activity rather than payload progress alone, and expose both values in
the session parameters so operators can tell a slow peer from a dead
one before tuning.

3.5 Refusal paths must account for transport credit. The .4 fix (quiche
never collecting a drained peer-unidirectional stream after STOP_SENDING,
leaking MAX_STREAMS credit until close) showed a refusal can be
protocol-correct yet transport-leaky. Proposal: make it a standing rule
that every new refusal path ships with a credit-accounting unit test on
both stacks, following the .4 precedent (4 new quiche tests).

## 4. Usefulness verdict (unchanged from discussion)

A solid substrate for restartable, auditable batch/streaming pipelines:
journaled intents with frozen identities, resume proven across
coordinator SIGKILL, fencing and leases, mTLS identity, cross-language
byte-exactness. Caveats for adopters: design for at-least-once
execution, build your own admission pacing until §3.2 lands, and expect
QUIC-plus-SQLite heaviness for small jobs. The happy path is proven
across languages; refusal/backpressure/backoff parity is the thinnest
area and the right target for remaining certification rows.
