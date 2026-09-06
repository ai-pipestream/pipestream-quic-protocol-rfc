# Durable work, results, and usefulness: execution record

Status: active, not accepted. Baseline: `68b02ea55169187da123b7efa0f6045718fbb645`.
The user approved all three tasks in the goal objective attached on 2026-09-06.
This record preserves the complete scope; an intermediate commit is not completion.
Task 1's contract/design acceptance is recorded below. Tasks 2 and 3 are open;
no version-2 endpoint, interoperability claim or workload comparison exists yet.

## Required order and acceptance evidence

### 1. Complete the contract

Make "I submitted work, disconnected, and returned: what happened, and where is
my output?" interoperably answerable. Required deliverables:

- A small mandatory Core and explicitly negotiated durable-work/result profiles.
- Stable owner-qualified work identity, distinct attempts, and safe anti-reuse rules.
- First-class result streams and authenticated output references, bound to the
  input, logical work, producing attempt, and authority.
- An explicit successor composing sealed work, caller authentication, retained
  recovery, authorized cancellation/retry, and stale-publication fencing.
- Separate execution deadlines, input/output retention, receipt replay,
  authorization expiry, and anti-reuse history. Active work cannot expire out
  of observability merely because 24 hours elapsed.
- Exact completion-count partitions and scope-qualified shutdown cuts.
- Revised normative source, explicit wire/version compatibility, frozen valid
  and invalid wire examples, and an executable bounded failure-state model.

Acceptance: no unresolved admission, outcome authority, publication, or closure
semantics within the selected profiles. Build/render/inspect the draft, validate
its CDDL, check the vectors independently, run the model with reported bounds
and counterexample traces, and review security/resource invariants. A model is
not implementation conformance or an unbounded proof.

### 2. Independent Rust and Java implementations and failure driver

Implement the same complete profile combination in both existing libraries,
including Java caller authentication, result delivery, and sealed recovery.
Do not substitute legacy subset tests or shared protocol code. Keep C++ at its
existing subset until this contract survives the required work.

The protocol-neutral Rust driver must cover both language directions and:

- Crashes on both sides of durable commits and lost acknowledgments.
- Duplicates, stream reorder, missing descendants, and stale attempts.
- Cancellation versus completion/result publication.
- Unauthorized replay, changed principals, malformed frames, resource exhaustion.
- Slow consumers and payloads larger than receiver flow-control windows.
- Retention expiry and crash-safe cleanup.

Acceptance: trace every mandatory selected-profile requirement to tests;
cross-language outcomes and named refusals agree, restart works, and measured
resource bounds hold. Preserve full existing regression gates. Report heap,
native/process memory, disk-file lengths and actual I/O with their own scopes;
do not substitute one for another.

### 3. External workload and equivalent streaming-gRPC baseline

An application outside the reference implementation must stream chunks,
distribute real transformations, return real output, and reconstruct the
result. Exercise reconnect and worker failure during execution.

Implement the same workload over streaming gRPC with equivalent authentication,
persistence, retry rules, processing and output guarantees. Measure time to first
usable output, total completion, tail/recovery latency, CPU, heap, total process
memory, disk I/O, network bytes, coordination code/state, and failure correctness.

Acceptance: reproducible commands, pinned builds, raw measurements, failure
traces, and an honest report of improvements, regressions and simplifications.
Feed results back into normative source and implementation status. Lower
coordination complexity is a possible benefit; faster transport is not assumed.

## Execution decisions

- Reducing mandatory version-1 behavior requires a new major protocol mapping;
  do not weaken what `pipestream/1` or an existing extension ID promises.
- Separate work identity from attempt identity and result delivery observations.
  Neither disconnect nor result-stream reset authorizes execution.
- Use Rust for the independent model/driver. No Python implementation,
  conformance oracle, example or benchmark harness.
- Forgejo is the source of truth; use normal PRs and preserve other merges.
  GitHub is a downstream mirror. No force push, deployment, live-cluster change,
  or IETF submission is authorized by completion of this goal.

## Progress and outstanding work

The first increment adds the lifecycle decision record and two independent
bounded state models to the Rust conformance driver. The full suite now runs
`modelcheck --depth 32 --max-states 1000000`.

The actual finite graphs contain 311,539 work-model states and 5,776 scope-model
states, with 8,723,092 and 173,280 checked edges respectively. Their longest
shortest paths are 19 and 17 transitions; both searches finish with no depth
frontier. All 14 deliberately incorrect rule variants produce counterexamples.
The model tests also exercise insufficient state budgets and named successful
recovery/cancellation/cleanup traces. These are separate models, not a proof
of their composition or of real storage, cryptography, networking or liveness.

Decisions captured include a new major mapping for reduced Core, stable logical
work versus attempt identity, explicit retry without deadline extension,
post-terminal replay retention, result publication fencing, immutable scope
membership, four final count buckets, empty scopes and root-qualified shutdown.

These are inputs to the normative wire contract, not a replacement for it.
The second increment defines that contract and freezes its schemas and examples.
The third checks bounded composition and records task-1 design acceptance.
Next: implement the complete version-2 combination independently in Rust and
Java, following [the mandatory test ledger](durable-work-v2-test-plan.md).
No new profile is advertised yet. Tasks 2 and 3 remain open until their complete
evidence is recorded and audited against the actual repository.

### Initial model increment validation (2026-09-06)

- `./conformance/run_all.sh`: exit 0. Formatting, strict workspace clippy,
  328 Rust workspace tests, frozen version-1 vectors and CDDL, the two models,
  193 Java reference tests, native SQLite/C++, nine client/server pairings,
  32 raw QUIC capability probes, recursive/recovery scenarios and all three
  external examples passed. Java counts were read from 20 Surefire XML reports:
  zero failures, errors or skips. This is local evidence, not hosted CI.
- After selecting 32 as the CLI's default exploration depth, focused formatting,
  all 18 conformance-crate tests, strict clippy and the default release
  `modelcheck` command passed again. The full suite uses an explicit depth of 32.
- `git diff --check`: passed. No production protocol codec, wire schema,
  immutable vector, database format, reference server or dependency changed.
- Logs: `/tmp/pipestream-successor-model-conformance.log` and
  `/tmp/pipestream-durable-work-model.log` on the validation host. Java still
  emits its existing Netty `sun.misc.Unsafe` deprecation warning.

This increment does not complete task 1, task 2 or task 3. The next increment
must implement the normative contract rather than treating model success as
wire interoperability or replacing the remaining implementation/benchmark work.

### Normative version-2 increment (2026-09-06)

Local draft -05 now contains the self-contained normative Section 12 and
Appendix F. The version-1 mapping, schemas and frozen bytes retain their
meanings. Version 2 selects `pipestream/2` and the explicit private-use
`durable-work-v2` / `result-delivery-v2` combination. No endpoint advertises it.

The contract defines bounded array-only deterministic CBOR; separate connection
requests and immutable producer-qualified operations; verified mTLS identity;
authority-issued, non-reusable session generations; declarations, sealed
membership and branch admission; explicit attempt and worker-lease fences;
retained typed outcomes; cancellation/skip/revocation with descendant settlement;
atomic output manifests, actual result streams and authenticated locators;
four disjoint terminal counters; root-qualified completion and separate detach;
independent execution/output/receipt lifetimes, dependency/read pins, clock
assumptions and crash-safe accounting. Reconnection cannot change a session's
selected profile combination. TLS handshake failures retain the RFC 9001
CRYPTO_ERROR mapping instead of pretending application negotiation succeeded.

`test-vectors/v2` adds 70 frozen framing/schema examples and 12 independent
domain-separated commitments. The verifier checks hashes, exact frame/schema
roots and Appendix F synchronization. It uses the pinned CDDL library with
CBOR-only input, not its CLI's JSON fallback. Ten schema refusals are checked;
14 additional semantic/canonical refusal expectations are frozen but still
require independent codecs and state/transport tests. The examples are not
one sequential session transcript.

The validator exposed a pre-existing process-driver deadlock: it waited for
child exit before draining pipe output. A 2 MiB-per-pipe regression failed
with the old code's timeout and passes with concurrent, bounded capture.
Each pipe retains at most 1 MiB while draining excess output; exit status and
deadlines still determine command success. This changes the process driver,
not a production protocol codec, server, storage format or dependency.

Validation:

- `./build.sh core 05`: exit 0; generated XML, text and HTML; document validation
  reports zero errors, flaws or warnings and the existing FIPS reference comment.
  Inspected the rendered version-2 text, URI rules and Appendix F schema layout.
- `./conformance/run_all.sh`: exit 0. Formatting, strict clippy, 332 Rust workspace
  tests, 193 Java reference tests (20 Surefire XML reports; zero failures, errors
  or skips), C++/native checks, all nine black-box pairings, 32 raw QUIC capability
  probes, recursive/recovery scenarios and the three external examples pass.
  The external Rust examples also pass their four and two unit tests.
- Both existing bounded models still exhaust their finite graphs and catch
  all 14 negative controls. They are not yet a composed lifecycle model.
- `git diff --check`: passed. Logs on the validation host:
  `/tmp/pipestream-v2-contract-conformance.log`,
  `/tmp/pipestream-v2-vector-check.log`,
  `/tmp/pipestream-v2-draft-final.log`, and the deliberately failing old-driver
  regression `/tmp/pipestream-output-capture-negative.log`.

This is local validation, not hosted CI, version-2 interoperability, a submitted
Internet-Draft, or task-1 acceptance. The composed model/contract audit remains;
tasks 2 and 3 still require their full implementation and workload evidence.

### Composed failure model and contract acceptance (2026-09-06)

The composed model explores one branch with zero or one leaf, two attempts per
item, two durable worker epochs across one restart, staged publication,
ancestor cancellation/skip/revocation, immutable child identity, output-read and
parent-dependency pins, symbolic expiry, batched settlement and root closure.
It reaches 620,796 states and checks 29,177,412 edges; the longest shortest path
is 27 transitions and depth 32 leaves no frontier. All 11 negative controls
produce counterexamples. Eight unit tests cover successful and refusing traces,
negative controls and insufficient exploration budgets. The two earlier models
also retain their verified bounds and 14 negative controls.

Raw output is checked in at
[`conformance/results/durable-work-v2-models-2026-09-06.txt`](../../conformance/results/durable-work-v2-models-2026-09-06.txt).
This is finite symbolic safety evidence. It does not establish liveness, arbitrary
depth/cardinality, a wire implementation, physical storage crash consistency,
clock correctness or measured resource bounds. Those remain task-2 obligations.

The review clarified that a saved output reference includes its authenticated
manifest and selected index, not a bare URI with guessed identity/credentials.
It also made sender RESET_STREAM versus receiver STOP_SENDING explicit under
RFC 9000, disallowed wrong-direction client REFUSAL responses, and preserved
an accepted parent cancel/skip fence over subsequent STRICT child-failure
settlement. No frozen version-1 or version-2 bytes were changed.

Task-1 acceptance audit against the original seven contract deliverables:

- Core and negotiated profiles: Section 12.1, Appendix F capabilities and
  Section 11's distinct major-version registrations. Legacy semantics are
  preserved, required dependencies are explicit, and downgrade is refused.
- Stable identity and distinct attempts: Sections 12.3, 12.4 and 12.6 define
  authenticated authority/owner/generation/producer/scope/entity binding,
  high-water anti-reuse, immutable operation replay and separate worker leases.
- First-class results and references: Section 12.7 and the frozen manifests,
  headers and hash commitments bind installed output to admitted input, work,
  attempt and authority, with authenticated full-object retrieval.
- Composed sealed recovery: Sections 12.4 through 12.8 define admission versus
  declaration, authoritative typed receipts/views, explicit retry, ancestor
  cancellation and terminal publication. The work and composed models exercise
  lost-ACK/restart and stale-attempt/worker/cancellation sequences.
- Independent lifetimes: Section 12.9 defines execution settlement, post-terminal
  output/receipt promises, credential/grant separation, active/dependency/read
  pins, trusted clocks, retirement and permanent non-reuse history. The models
  detect active eviction, deadline extension and premature output deletion.
- Exact completion and shutdown: Section 12.8 plus scope/composed models cover
  seals, missing descendants, four final count buckets, STRICT rehydration,
  immutable root summaries and completed-session versus connection-only drain.
- Normative text, compatibility, examples and executable models: draft -05
  builds and renders; synchronized CDDL and all 70 frozen examples/12 commitments
  validate; three bounded graphs exhaust their stated domains with all 25
  negative controls detected. Semantic/canonical codec expectations are frozen,
  not falsely counted as implemented transport refusals.

Within the selected profiles, the identified admission, outcome-authority,
publication and closure decisions now have explicit normative rules and bounded
design checks. This closes task 1 as a contract/design milestone, not as an
IETF approval or implementation milestone. Implementation findings can and must
reopen a rule if actual interoperability or failure testing contradicts it.

Validation and regression fixes:

- `./conformance/run_all.sh`: exit 0 after the fixes below. Formatting, strict
  workspace clippy, 342 Rust workspace tests, 193 Java tests (20 Surefire XML
  reports, zero failures/errors/skips), C++/native checks, nine black-box pairs,
  32 raw QUIC capability probes, recursive/recovery tests and all three external
  examples passed. External Rust examples also pass their four and two tests.
- `./build.sh core 05`: exit 0, zero errors/flaws/warnings and the existing FIPS
  reference comment; inspected rendered Section 12 and the unchanged schemas.
- The first rerun exposed a shared 30-second schema-group deadline. Each frozen
  case now runs in an isolated validator process with its own 30-second bound.
  This preserves strict CBOR/CDDL checks and prevents cross-case diagnostic
  accumulation; no JSON fallback or regenerated corpus was introduced.
- The next rerun exposed a retained-root lock lifetime defect. A duplicated
  descriptor deterministically reproduced the same EWOULDBLOCK after the last
  logical owner dropped. Commit `fb9c012` adds a PID-qualified RAII lock guard
  that explicitly unlocks for its owning process, including failed-open paths.
  A copied nonowner guard cannot unlock the parent's store. See the open-file
  description semantics in the [Linux flock manual](https://man7.org/linux/man-pages/man2/flock.2.html).
- Both new lock tests, all 56 retained-storage tests and five additional complete
  runs of the 88-test Rust transport library passed. Genuine live-handle and
  cross-process maintenance exclusions remain covered. No storage format,
  dependency or public wire API changed for this fix.
- `git diff --check`: passed. Local logs:
  `/tmp/pipestream-composed-contract-complete-suite.log`,
  `/tmp/pipestream-composed-contract-draft-final.log`,
  `/tmp/pipestream-composed-vectors-isolated.log`,
  `/tmp/pipestream-retained-lock-tests.log`,
  `/tmp/pipestream-retained-lock-parallel-regressions.log`, and the deliberately
  failing pre-fix `/tmp/pipestream-retained-lock-negative.log`.

Next is task 2, not a claim that these models replace either language's codec,
authenticated server, durable executor, result delivery, failure driver or
resource measurements. The entire three-task goal remains active.
