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

- The resumed goal explicitly requires the entire current contract in Rust and
  Java, with no backward-compatibility requirement. Historical version-1 tests
  remain regression evidence, not a reason to preserve a conflicting design
  or defer current behavior. The selected target is Section 12/Appendix F's
  durable-work plus result-delivery combination. All failure-driver and
  equivalent-workload deliverables remain required; milestones do not finish
  the goal.
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

### Task 2 implementation progress: Rust V2 wire foundation, 2026-09-06

Implemented `pipestream_core::v2`, without enabling V2 on an endpoint or changing
V1 storage/ALPN. This is partial task-2 delivery, not completion of task 2 or
the overall goal. The [acceptance ledger](durable-work-v2-test-plan.md) records
the precise library coverage and outstanding independent implementation gates.

- Typed Rust decoding/encoding for every Section 12/Appendix F message, receipt,
  view, manifest, summary and object header. All 70 frozen examples have exact
  round trips or their named refusal, including semantic/canonical negatives.
- All 12 frozen commitments match typed operations, manifests, scope seals and
  status roots. Full-scope hashing is incremental rather than capped to a wire
  batch, and the status tree uses bounded logarithmic memory.
- Negotiation and sender/profile checks, bounded request/input/result correlation,
  actual input-stream tags, known reply-field comparisons and duplicate refusal.
  A response stream remains pending through verified FIN. Connection-bound
  completion evidence cannot be applied to a different connection or aborted ID.
- Incremental borrowed-chunk payload validation with exact length, SHA-256, FIN,
  idle and lifetime checks; no object-sized validation buffer or callback on
  the control book. This is not measured QUIC flow-control independence.
- Clarified two normative boundaries exposed by implementation: unmet result
  dependencies retain EXTENSION_UNSUPPORTED in both negotiation directions;
  reaching an idle/lifetime deadline is too late for progress or FIN renewal.
  No frozen byte, commitment or refusal expectation changed.

Verification: 17 new library tests; 359 Rust workspace tests; strict workspace
clippy and formatting; `./conformance/run_all.sh` exit 0 with 193 Java tests
(20 Surefire reports, zero failures/errors/skips), C++/native tests, all nine
black-box pairs, 32 raw capability probes, models, recursive/recovery scenarios
and three external examples. The final Rust library edits were rechecked with
focused tests and strict workspace clippy. `./build.sh core 05` returned 0;
rendered text includes both corrections. Idnits reports zero errors/flaws/warnings
and the existing FIPS reference comment. Local logs:

- `/tmp/pipestream-v2-rust-wire-suite-20260906.log`
- `/tmp/pipestream-v2-rust-wire-final-rust.log`
- `/tmp/pipestream-v2-rust-wire-draft-20260906.log`

Still missing: independent Java V2 codec, Quinn/Netty V2 authentication and
transport, persistent client/authority state and resource transactions, durable
executors, real crash/cleanup tests, cross-language V2 driver scenarios, and
task 3's equivalent streaming-gRPC workload measurements. No V2 conformance,
IETF acceptance, submission, merge, deployment or performance win is claimed.

### Task 2 implementation progress: Rust authority transactions, 2026-09-06

The resumed implementation adds `v2/authority/`, a normalized STRICT SQLite
store using WAL, FULL synchronization, the guarded physical VFS and typed
canonical records. It is actual persistent library behavior, not an activated
endpoint or a replacement for the complete execution contract.

- Session creation atomically allocates the authority generation, owner creation
  sequence and root scope. Matching requests replay the same binding; changed
  policy/profile, future sequence, retired creation history and exhausted
  counters have named refusals. Limits and exact retention policies are retained.
- Reopening never initializes a missing/empty database. New issuing history
  requires explicit `initialize` against a nonexistent database path. Startup
  checks stored authority and policy and syncs the directory. This prevents
  accidental reset on reopen; it cannot prove an operator's stale backup is safe.
- Declaration commits membership, counters, whole-scope seal and immutable
  operation receipt together. Sealing uses a fallible SQLite cursor and a
  constant-memory incremental hasher. Replay precedes new-capacity checks.
  Bounded pages and revision snapshots are read-only. An empty sealed scope
  closes; a sealed scope with missing declared inputs remains unresolved.
- Authorization is checked before retained lookup and rechecked before mutation
  commit. UTC high-water is durable; rollback/untrusted time refuses new
  mutations without denying authorized retained evidence. Integer persistence
  uses checked signed-63-bit conversions, not floating point or JSON.
- Tests exercise concurrent identical and conflicting operations, policy
  withdrawal before commit, restart replay, independent principals, changed
  profiles, batch-independent seals, exact IDs above 2^53, quotas and physical
  exhaustion with whole-batch rollback. Four subprocess exits bracket creation
  and declaration commits, proving both uncommitted absence and committed
  replay after the acknowledgment was never delivered to a caller.

The authority tests are in `v2/authority/tests.rs`; frozen wire tests remain in
`v2/tests.rs`. Focused command: `cargo test --locked -p pipestream-core v2::`.
There are 35 passing tests (17 wire/library tests and 18 authority tests,
including the subprocess entry point). The physical test limits database, WAL
and rollback journal to 128 KiB each and SHM to 64 KiB. These are file-length
ceilings, not filesystem-block, heap/RSS, or future-completion funding evidence.

A negative-first regression caught an empty-root closure accepting a timestamp
whose creation-receipt retention would overflow the wire integer range. Closure
now checks that entire interval before committing its seal, summary or receipt;
the maximum exactly representable deadline is accepted. The refusal rolls back
both the operation and UTC high-water update.

Still required in this store: staged immutable payloads, admission funding and
jobs, attempts/worker leases, cancellation/revocation settlement, nonempty
closure, result publication/read leases, independent expiry and retirement,
crash-safe cleanup and accounting reconciliation. No incomplete profile is
advertised. Next implementation work is payload staging plus atomic funded
admission, followed by execution/result/recovery integration. Independent Java,
the neutral cross-language failure driver and the equivalent streaming-gRPC
workload remain part of the active goal.

Verification: `./conformance/run_all.sh` exited 0, including 193 Java tests
(20 Surefire reports, zero failures/errors/skips), native/C++ checks, frozen
vectors, all nine black-box pairs, 32 raw capability probes, models and all
three external examples. After the final retention-overflow correction, strict
workspace clippy and all 377 Rust workspace tests passed. No draft wire bytes,
CDDL or normative Section 12 text changed in this increment. Local logs:

- `/tmp/pipestream-v2-authority-suite-20260906.log`
- `/tmp/pipestream-v2-authority-final-clippy.log`
- `/tmp/pipestream-v2-authority-final-workspace.log`
- `/tmp/pipestream-v2-authority-retention-red.log` (deliberate pre-fix failure)

### Task 2 implementation progress: immutable payload storage and ingress

The Rust authority now has a Unix file-backed payload store and header/reception
API. This is partial task-2 implementation; it does not admit jobs or activate an
incomplete profile. The complete goal, independent Java implementation and all
original failure/workload deliverables remain active.

- Private, exclusively locked payload roots are durably paired with the SQLite
  authority's local store identity and canonical path. A live stage, installed
  token or reader retains root ownership; inherited-process handles cannot use
  the parent's ownership. Unknown entries, aliases, changed policy and mismatched
  roots fail closed without adopting or clearing unrelated files.
- Reception reserves full declared length plus bounded metadata overhead before
  the first payload byte, with global/per-owner byte, object and handle ceilings.
  I/O uses bounded borrowed buffers. Length, SHA-256, FIN and monotonic deadlines
  are checked before fsynced installation and directory synchronization.
- Opaque installed tokens pin exact objects through a future metadata commit.
  Read handles verify retained length/hash at EOF; earlier bytes are provisional.
  A corrupt read fails permanently, never turning missing/corrupt storage into a
  successful empty result or rerunning an application.
- Startup under exclusive root ownership safely reclaims abandoned stages,
  including torn headers that could never have been admitted. Installed objects
  remain charged. Orphan collection holds the SQLite writer transaction, checks
  retained references, skips live pins and uses bounded cursor batches. Interrupted
  unlink is replayable. This is not retention expiry or session retirement.
- Header preflight checks current owner authorization, profile, generation,
  producer, declared membership, cancellation fences, configured application/mode,
  duration, input/output byte ceilings and a conservative response-encoding bound
  before creating a stage. Unknown applications have no fallback. Installed input
  remains DECLARED with no receipt/job; reception cannot fabricate admission.

Storage format 2 adds local root pairing and payload references. Format-1
prototype authority files are refused rather than converted; no user database
was migrated or deleted. No draft wire/CDDL/vector change is involved. The Unix
lock dependency reuses the already pinned `rustix` 1.1.4; the three Rust lockfiles
only add that existing dependency edge to `pipestream-core`.

Evidence: 12 payload tests (including a subprocess entry point), three ingress
tests and the existing 18 authority tests pass together. Six real child-process
exits cover file creation, complete staging header, object fsync, rename,
directory fsync and cleanup unlink. These are process-death tests, not a physical
power-loss experiment. The authority-reference test commits a storage reference,
not a fabricated work admission or a transport ACK.

The separate `v2_payload_resources` test streams, installs and verifies 32 MiB
through 16 KiB buffers. A measured run used 1,864 bytes of additional Rust heap,
with a largest allocation of 1,512 bytes; its gates are 256 KiB and 64 KiB
respectively. File lengths were 33,554,602 bytes, allocated blocks 33,562,624 bytes,
and observed process RSS/HWM 3,564 KiB. This isolates buffering for one object;
it is not a QUIC flow-control, metadata admission, populated-index scaling,
end-to-end workload, or streaming-gRPC comparison result.

Next: the atomic funded admission/job/receipt transaction, persistent output and
metadata reservations, completion WAL headroom, worker leases and result
publication. Neither the physical file caps nor temporary staging reservations
prove those accepted-work promises. Java V2, transport, the neutral failure
driver, independent retention/cleanup and task 3 remain outstanding.

Local evidence logs:

- `/tmp/pipestream-v2-payload-authority.log`
- `/tmp/pipestream-v2-payload-workspace.log`
- `/tmp/pipestream-v2-payload-resources.log`
- `/tmp/pipestream-v2-payload-suite.log`

Final verification: `./conformance/run_all.sh` exited 0 against this increment,
including strict Rust formatting/clippy, 393 Rust workspace tests, 193 Java tests
(20 Surefire reports, zero failures/errors/skips), native/C++ checks, frozen
vectors and models, all nine black-box language pairs, all 32 raw capability
probes, and the three external examples. The existing network tests exercise
their historical profiles; they do not establish V2 network interoperability.

### Task 2 implementation progress: preallocated records and rewrite funding

Work views and scope summaries now use individually checksummed fixed-capacity
records inside their normalized SQLite tables. Declaration reserves a 2048-byte
view and two rewrite credits per member; scope creation reserves a 512-byte
summary and one credit. Empty sealed closure writes that preallocated summary.
This is storage-format 3, explicitly refusing older prototype formats without
conversion or deletion. Wire/CDDL/frozen examples do not change.

The guarded VFS protects the retained sum of credits against unrelated creation,
declaration and payload-root binding writes. Credit accounting is reconstructed
from bounded headers under the SQLite writer lock, without a whole-session image
or an in-memory inventory of every record. Each credit funds one incremental
BLOB overwrite of that fixed record under pinned SQLite 3.53.2 geometry; its
revision, content and remaining credit commit atomically. Ordinary rewrites
preserve credits. Oversize or stale updates refuse before spending a credit.
Reopen and integrity checks validate complete record bodies, zero padding and
their relational identities, not only the accounting header.

A negative-first regression exposed another exhaustion boundary: an ordinary
revision increment could consume the last integer needed for a promised update.
The record now reserves revision increments with its credits. The two promised
updates still succeed at the exact top of the signed-63-bit domain; the preceding
ordinary update is refused. No floating point, saturation or wraparound is used.

Current focused evidence: 44 authority tests, including 11 new record tests,
pass; strict workspace clippy passes. Two subprocess exits bracket credit
spending before/after commit. The existing creation/declaration subprocess cases
also exercise initialized fixed records. These are process-death tests, not
physical power-loss tests or transport-level acknowledgment loss.

Measured record gates:

- With a reader pinning the WAL, 53 ordinary commits filled its protected
  ceiling at 655136 bytes. Four reserved rewrites across two work records still
  committed, ending at 671592 bytes under the 1048576-byte cap. Database growth
  was disabled. This is record escrow, not full job-completion evidence.
- All 18 page/capacity combinations passed: 512/4096/65536-byte SQLite pages,
  capacities 512/2048/4096/8192/65536/1048576 bytes, cache spilling enabled,
  SQL row replacement prohibited, and database page count fixed. For the 1 MiB
  record, measured WAL lengths were 1106872, 1058872 and 1114552 bytes respectively,
  within their bounds of 1173872, 1133032 and 1311232 bytes. These are file-length
  bounds, not allocated-block, RAM or throughput measurements.

The 600-member seal test explicitly funds its credits with a 128 MiB WAL policy.
The physical-exhaustion test keeps its 128 KiB database/journal and 64 KiB SHM
ceilings but uses a 4 MiB WAL allowance so it still checks refusal after an
accepted whole declaration batch. Logical ceilings are not unconditional grants
against smaller physical capacity.

Still required before job admission: funding and atomically committing the job,
input binding, admission receipt, attempt/deadline, child scope, global/per-owner
output capacity, and every other mutable record in its completion write set.
The present credits fund only their fixed-record writes; they cannot justify an
unfunded job, arbitrary SQL, output promises or complete subtree settlement.
The header audit currently scans retained records per protected transaction;
populated-store scaling is not claimed. No V2 profile is activated. Independent
Java, authenticated endpoints, workers/results/retention, the neutral failure
driver and the equivalent streaming-gRPC workload all remain in the active goal.

Local evidence logs:

- `/tmp/pipestream-v2-records-authority.log`
- `/tmp/pipestream-v2-records-costs.log`
- `/tmp/pipestream-v2-records-clippy.log`
- `/tmp/pipestream-v2-records-revision-red.log` (deliberate pre-fix failure)
- `/tmp/pipestream-v2-records-suite.log`

Final validation: `./conformance/run_all.sh` exited 0, including 404 Rust
workspace tests, strict formatting/clippy, 193 Java tests (20 Surefire reports,
zero failures/errors/skips), native/C++ checks, frozen vectors and models, all
nine black-box pairs, all 32 raw capability probes and the external examples.
Those network interoperability tests remain historical-profile evidence, not
V2 endpoint conformance. No draft submission, main merge or deployment occurred.

### Task 2 implementation progress: durable output capacity and materialization

The payload backend now installs immutable output-reservation files before a
future metadata admission. Their full maximum count/bytes, including per-file
overhead, remain charged across restart and cannot be consumed by unrelated
uploads. Materialized outputs spend capacity inside that promise; they do not
double-charge it or release unused quota globally. SQLite payload references can
retain reservation files independently from their output files. No job admission
or manifest publication is implied by an installed reservation.

Output staging supports unknown final lengths and digests with bounded borrowed
buffers. It writes into a preallocated header slot, computes the descriptor from
actual bytes, syncs the file and installs it without copying/shifting the body.
Partial output reserves its maximum; finish returns unused capacity only to that
same reservation. Owner/budget bindings and unique output slots are checked, and
over-budget/error/late staging cannot install a successful prefix. Storage errors
are not themselves authoritative job outcomes; the executor still must fence and
commit the appropriate outcome.

The inventory reconstructs per-owner charges and per-reservation occupancy in a
single pass with ordered-map lookups and 256-bit slot sets. Reservation and object
maps share one lock. Live reservation pins protect uncommitted outputs without
requiring 256 open producer handles. Reference-safe collection keeps a funding
record until all its objects are gone. Startup syncs the reconstructed directory
before granting new capacity, completing interrupted namespace durability.
Ordinary quota/usage operations still scan the inventory; this is not a claim
about populated-store throughput.

A negative-first test caught an uncertain-installation gap. Errors creating or
renaming staging files now quarantine the live root instead of allowing cleanup
or new quota decisions to guess the namespace outcome. Waiting inventory callers
recheck quarantine after taking the lock. Exclusive reopen audits what exists and
syncs the directory before resuming. Existing work receipts are not rewritten as
success/failure by this filesystem decision. Reopening a reservation as new
storage evidence also completes its file/directory synchronization first.

Authority schema and payload-root formats are now 4. Prior prototype formats
are refused explicitly; no user data was converted, deleted or replaced. Local
object files now have fixed padded headers and optional typed funding identity.
No normative wire, CDDL or frozen-vector byte changes are involved.

Focused evidence: 15 output-reservation tests, including 11 child-process exit
boundaries and the subprocess entry point. The SQLite-reference test commits
storage references while leaving the work DECLARED, not a fake admitted job.
Maximum labels and 256 zero-length output slots are exercised with two handles.
The tests also cover capacity/handle exhaustion, duplicate slots, changed owners
and budgets, missing/corrupt/aliased funding, exact zero-output behavior, poisoned
errors and monotonic idle/lifetime deadlines. Process-death testing is not a
physical power-loss experiment. Space/quota errno normalization is tested as a
mapping, not represented as an actual full-filesystem experiment.

The isolated 32 MiB output resource run measured 3519 bytes of additional Rust
heap and a largest allocation of 1952 bytes, under the 256 KiB/64 KiB gates.
It reported 33555480 charged bytes, 33555099 file-length bytes, 33566720 allocated
block bytes and 3912 KiB RSS/HWM. The 32 MiB input test also remains below its
gates (2305 bytes added heap, largest allocation 1952 bytes). The tests now share
allocator instrumentation source but remain separate one-test executables.
These are single-object storage buffering measurements, not full endpoint,
QUIC flow-control, populated-inventory or streaming-gRPC workload evidence.
Reservations limit internal file-length use; they are not physical block
preallocation or protection against unrelated filesystem/hardware failure.

The complete admission transaction remains next: revalidate the installed input
and reservation against the bound authority root, fund executor slots and every
metadata write in the promised lifecycle, then atomically commit the input/job,
attempt/deadline, child scope, resource references and immutable receipt. Do not
activate V2 profiles before workers, cancellation/closure, results/read leases,
retention/reconciliation, independent Java and neutral cross-language failure
tests are complete. The original workload and equivalent gRPC baseline remain
part of the goal.

Local evidence logs:

- `/tmp/pipestream-v2-output-reservations-authority.log`
- `/tmp/pipestream-v2-output-reservations-focused.log`
- `/tmp/pipestream-v2-output-reservations-resources.log`
- `/tmp/pipestream-v2-output-reservations-clippy.log`
- `/tmp/pipestream-v2-output-reservations-namespace-red.log` (deliberate pre-fix failure)
- `/tmp/pipestream-v2-output-reservations-suite.log`

Final validation: `./conformance/run_all.sh` exited 0. The Rust workspace passed
420 tests, and the 20 Java Surefire reports contain 193 tests with zero failures,
errors or skips. Strict Rust formatting/clippy, frozen-vector verification,
bounded models, C++ tests, all nine black-box pairs, all 32 raw QUIC capability
probes and the external examples passed. Network interoperability still covers
historical profiles, not the unfinished V2 endpoints. This checkpoint does not
complete the goal, submit a draft, merge main or deploy a server.

### Task 2 implementation progress: input preparation and funded record growth

The admission path needed to expand a declaration's 2048-byte work-view slot to
represent its future manifest. Record growth now preserves the exact validated
body and observable revision, retains existing credits, and protects credits at
the expanded WAL write cost before allocating pages. Capacity/credits cannot be
shrunk by this API. A savepoint restores the old initialized record if the SQL
resize succeeds but the subsequent BLOB initialization fails. Other records'
promises remain protected. No storage or wire format change was required.

`AuthorityStore::prepare_input` now combines an opaque validated input with its
durable output reservation and expanded work-view capacity. Filesystem I/O runs
outside the SQLite writer transaction; the final writer repeats input preflight,
checks the exact database-bound root, and checks current authorization again
before commit. Preparation issues no operation receipt, leaves work DECLARED at
the same revision, and confers no execution permission. Late refusal rolls back
metadata; unreferenced payloads stay charged until reference-safe collection.
Expanded metadata belongs to the declared record until later retirement, rather
than being silently released when a preparatory handle is dropped.

New evidence covers unchanged work/revision under repeated preparation, live
input/output pins, exact-root substitution refusal despite a matching store ID,
changed application/limits/scope fences, final authorization denial and unsafe
time. Record tests cover preservation of other credits, additional-credit
funding, stale revisions, overflow, corrupt source, refusal after a real SQL
resize, SQLite page exhaustion and four process-death points. The pinned-WAL
test now exercises an expanded record and refused further growth before spending
both records' existing credits. A separate representation test encodes 0/1/256
manifest outputs with large fields; it does not claim an admitted or completed
job. The focused authority run passes 70 tests, and strict workspace clippy passes.
In that focused run, 50 ordinary commits filled the protected WAL ceiling at
622176 bytes; refused growth preserved both promises, and their four reserved
rewrites finished at 667472 bytes under the 1048576-byte cap with database growth
disabled. These are record-level measurements, not complete job-transition costs.

The next required work remains the atomic admission/job/receipt transaction with
executor and complete metadata/clock/closure funding. Then durable worker leases,
attempt/cancellation/deadline settlement, manifest/read/dependency retention,
independent Java V2 and real cross-language failure testing are still required.
The external workload and equivalent streaming-gRPC baseline remain in scope.
Neither these preparations nor the historical interop suite complete the goal.

Local evidence logs:

- `/tmp/pipestream-admission-preparation-records.log`
- `/tmp/pipestream-admission-preparation-authority.log`
- `/tmp/pipestream-admission-preparation-clippy.log`
- `/tmp/pipestream-admission-preparation-suite.log`

Final verification: `./conformance/run_all.sh` exited 0. The Rust workspace
passed 431 tests; 20 Java Surefire reports contain 193 tests with zero failures,
errors or skips. Formatting, strict clippy, frozen vectors, bounded models, C++
tests, nine black-box pairs, 32 raw capability probes and the external examples
passed. The network tests remain historical-profile evidence, not V2 conformance.
No main merge, server deployment or Internet-Draft submission occurred.

### Task 2 implementation progress: funded scope state and shared clock

Admission also depends on funding the state that changes after a job is accepted.
Mutable scope membership counters, seal, cancellation/revocation flags and summary
now occupy one preallocated 1024-byte record with two credits. Root revocation
is read from that record. Greatest observed UTC occupies a separate fixed
64-byte record. The authority format is now 5; payload format 4 is unchanged.
Older authority stores are refused explicitly, without conversion or deletion.
No normative wire, CDDL or frozen-vector changes were needed.

Each non-clock record credit now covers that record and one shared-clock
overwrite in the same transaction. The combined bound accounts for both BLOBs,
SQLite's final frame and sector padding. Remaining state credits also reserve
the corresponding shared-clock revision increments. Ordinary time observations,
new slots and credit expansion cannot consume those increments. A funded
transition checks trusted time first, spends its state credit and records that
observation in the same commit; repeated equal UTC requires no rewrite. Actual
empty-scope closure uses this ordering at clock-counter exhaustion.

The 18 page/capacity cost cases now execute both record and clock writes, with
maximum authority-label length and a long payload-path tail on the clock row.
The focused 1 MiB pinned-WAL run admitted 40 ordinary writes, refused further
growth at 494456 bytes, then committed four reserved work/clock pairs at 560352
bytes without database growth. Separate scope-fence/clock storage tests prohibit
SQL row replacement as well. These are measured paired-record costs, not a
complete job transaction or a production throughput comparison.

Two additional process-death points bracket a scope/clock commit; the existing
work-credit crash test now includes its clock update too. Reopen validates the
clock record and refuses retained scope/work timestamps ahead of greatest UTC.
Counter tests reach the exact 63-bit revision ceiling without losing an owed
observation. Private fence fixtures do not constitute cancellation/revocation
RPCs or descendant settlement.

A negative-first ownership test also found that binding or scope corruption
could be decoded before rejecting a different owner. Session authorization now
checks ownership with a scalar comparison before decoding either record. Both
corruption cases produce UNAUTHORIZED for the other owner. The focused authority
run passes 77 tests, and strict workspace clippy passes.

The next implementation remains the complete admission/job/receipt transaction,
not profile activation. Its mutable job state must have fixed, funded storage
for leases, attempt progress and resource liveness, and every autonomous write
must be covered before acknowledgment. Input/output reference liveness and
global/per-owner/session executor budgets must commit with it. Those jobs and
their execution/settlement APIs are not implemented by this checkpoint. The
independent Java implementation, cross-language failures and equivalent gRPC
workload remain required by the unchanged goal.

Local evidence logs:

- `/tmp/pipestream-scope-clock-owner-red.log` (deliberate pre-fix failure)
- `/tmp/pipestream-scope-clock-authority.log`
- `/tmp/pipestream-scope-clock-clippy.log`
- `/tmp/pipestream-scope-clock-suite.log`

Full `./conformance/run_all.sh` completed with exit 0. The Rust workspace passed
438 tests; the two Rust examples passed another six. Java's 20 Surefire reports
contain 193 tests with zero failures, errors or skips. Formatting, strict clippy,
frozen vectors, bounded models, C++ tests, all nine black-box pairs, 32 raw QUIC
capability probes and the external examples passed. Those network tests still
exercise historical profiles, not the unfinished V2 authority. No main merge,
server deployment or Internet-Draft submission occurred.

### Task 2 implementation progress: atomic external admission

`AuthorityStore::admit_input` now consumes a real prepared input and commits
its input/output-reservation references, immutable admission receipt, attempt 1,
timestamps, child scope and fixed job record under one SQLite writer transaction.
All three modes retain the correct child ownership; only caller-expanded branches
start WAITING_CHILDREN. The API never invokes a callback or advertises a profile.
Preparation alone remains unadmitted and produces no receipt.

Authority format 6 adds a checksummed 2048-byte job slot and global/per-owner
accepted-job limits. Payload format 4 and the normative wire are unchanged.
Admission allocates four rewrite credits in each job and admitted work record
and checks the whole current
insertion transaction against the guarded journal; future lifecycle transitions
must still prove their complete write sets fit the reserved allowances. The job
retains immutable application/input/output parameters, safe-restart classification,
attempt and lease fields, and input/output/executor liveness. Capacity is derived
from retained records, including waiting branches and separate sessions, rather
than a volatile thread count. Session input/output/job, operation, scope and
largest-response/object requirements are checked in the committing transaction.

The 13 admission tests include concurrent identical preparations, changed replay,
all three child modes, exact timestamps above 2^53, replay with an unsafe clock,
cross-session owner/global budgets, every session budget, and late authorization
rollback. Actual child-process death on both sides of the admission commit
distinguishes no accepted operation from a replayable lost-ACK receipt. Restart
opens and verifies the real input bytes and reopens the actual output reservation;
it does not fabricate a completed callback or result manifest.

A negative-first test found that an installed output could be collected after
its process handles disappeared despite its committed reservation. Collection now
honors the reservation's durable liveness too. Its former test expectation was
changed explicitly: orphan collection cannot recycle a retained worker's output
slots. Replacement-worker cleanup needs an attempt/lease-fenced operation that
preserves live handles and charged bytes; it is not implemented here.

The pinned-journal admission test committed 284 unrelated bounded test writes,
then refused admission at WAL 3670976 bytes under a 4194304-byte limit, with DB
73728 under 131072. Existing declaration receipts remain readable, work remains
DECLARED, and no partial job, child, reference or receipt survives. Reopen also
rejects missing/changed job, input-reference, admission-receipt, parameter and
stage evidence. The maximum typed job record fits its reserved slot. These are
storage/API gates, not complete lifecycle, physical-power-loss, V2 QUIC or gRPC
performance evidence.

Focused authority verification passes 90 tests and strict workspace clippy.
Local logs:

- `/tmp/pipestream-admission-initial.log` (deliberate pre-fix collection failure)
- `/tmp/pipestream-admission-authority.log`
- `/tmp/pipestream-admission-clippy.log`
- `/tmp/pipestream-admission-suite.log`

Next is the real worker lifecycle: register executable application callbacks,
claim/recover durable leases without changing the wire attempt, fence old workers,
and implement publication, explicit retry and terminal/deadline/cancellation
settlement with complete transaction cost tests. Local producer-1 admission and
descendant generation must use the same validation/funding rules. Nonempty closure,
result/read/dependency retention and retirement remain open. Independent Java V2,
neutral cross-language failures and the equivalent streaming-gRPC workload remain
required by the unchanged goal. This checkpoint does not complete Task 2.

Final verification: `./conformance/run_all.sh` exited 0. The Rust workspace
passed 451 tests and the two Rust examples passed another six. Java's 20 Surefire
reports contain 193 tests with zero failures, errors or skips. Formatting, strict
clippy, frozen vectors, bounded models, C++ tests, all nine black-box language
pairs, 32 raw QUIC capability probes and the external examples passed. Network
interop still exercises historical profiles, not V2 execution. No main merge,
deployment or Internet-Draft submission occurred.

### Task 2 implementation progress: fenced workers and publication

Registered applications now carry real executable callbacks. The Rust authority
claims a known durable job under a private lease, runs the callback outside its
SQLite writer and publishes actual output manifests or failure outcomes. The
included copy application streams input into a real output without buffering the
whole payload. Every context I/O call and the committing publication transaction
check current authorization, ancestor fences, attempt, lease and original deadline.
Callback panic, invalid diagnostics, unfinished output and ignored output errors
cannot publish success. Result locators come from validated server configuration.

Lease renewal preserves the lease identity and original deadline and cannot revive
an expired lease. Recovery increments only the private lease; explicit `retry_work`
increments the wire attempt, retains input/child/deadline, replenishes credits and
atomically commits its replayable operation receipt. Replacement workers wait for
all old payload handles to drain before recovering unpublished output slots under
the original reservation. Committed results are never recycled through that path.
Publication verifies the immutable inventory under its reservation pin without
needing an additional reader handle. Reopen verifies manifest ownership, session,
selected result profile and admitted output budgets. Authority format 6, payload
format 4 and normative wire/frozen examples are unchanged.

Sixteen execution tests include real subprocess deaths on both sides of claim,
publication and retry commits, plus death during unpublished-output recovery.
The recovery paths reopen and check actual output bytes. Lease equality, stale
attempts, retry replay/conflict/deadline/terminal refusals, callback-time fencing,
resultless work, output-limit errors and corrupt manifest ownership are covered.
Caller-branch execution requires retained successful child closure (the current
test exercises empty closure); authority expansion is explicitly refused.

The complete publication write set, including work, job and shared clock, also
commits with 0, 1 and 256 actual outputs under a pinned WAL reader after ordinary
writes exhaust their allowance. SQL triggers forbid row replacement, and database
page count stays fixed. The focused run's WAL lengths were 3308416 to 3320752 bytes
for 0/1 outputs and 2006496 to 2348432 bytes for 256 outputs, all below 4194304.
These are local file-length gates, not allocated-block, entire lifecycle or gRPC
baseline measurements. All 106 focused authority tests and strict clippy pass.

Local logs:

- `/tmp/pipestream-execution-authority.log`
- `/tmp/pipestream-execution-clippy.log`
- `/tmp/pipestream-execution-suite.log`

Next: reserve worker callback I/O permits before claim and build bounded persistent
job discovery/scheduling. Current callbacks can still lose staging-handle capacity
to concurrent reads; publication's fixed write set does not solve that. Implement
deadline/cancellation/skip/revocation settlement and the full branch lifecycle,
including producer-1 admission and nonempty closure. Input/output liveness remains
charged pending authenticated result reads, read/dependency retention, expiry and
safe retirement. Independent Java V2, actual Quinn/Netty V2 endpoints, the neutral
cross-language failure driver and equivalent streaming-gRPC workload remain
required. No V2 profile is activated and the full goal remains incomplete.

Final verification: `./conformance/run_all.sh` exited 0. The Rust workspace
passed 467 tests and the two Rust examples passed another six. Java's 20 Surefire
reports contain 193 tests with zero failures, errors or skips. Formatting, strict
clippy, frozen vectors, bounded models, C++ tests, all nine black-box language
pairs, 32 raw QUIC capability probes and the external examples passed. The network
evidence remains historical-profile coverage, not V2 interoperability. No main
merge, deployment or Internet-Draft submission occurred.

### Task 2 implementation progress: bounded pull workers and reserved I/O

The previous worker implementation could fail a promised callback output when
unrelated readers exhausted the handle pool after claim. A new negative-first
regression reproduced FAILED instead of SUCCEEDED. A claim now charges one
reusable output-I/O slot before committing its lease, in addition to its input
reader and output reservation. Each output staging/installed token borrows that
slot without double charging. Finishing or dropping an output returns the slot
to that worker, not unrelated readers. Dropping the reservation while its output
remains live transfers the charge under the inventory lock. Jobs declaring no
outputs need no output slot. Pressure can defer a claim without mutating its job or
invoking its application; the existing accepted job remains the backlog.

`Executor::start_workers(PoolConfig)` now supplies a real fixed pull pool. It
discovers retained jobs in bounded row batches with a shared wraparound cursor,
runs callbacks outside metadata/control handling, and bounds concurrent dispatches
globally and per owner. There is no volatile payload queue or required work-key
resubmission. Polling finds admissions and retries from any connection; optional
wake notifications only reduce latency. One pool owns a payload authority across
executor clones, retaining that ownership until all worker threads return.
Shutdown is cooperative: stop requests halt further discovery, while dispatched
callbacks may finish. No cancellation receipt is fabricated by stopping a pool.

The process-death test now injects failures through the pool and recovers through
a new pool without known-key submission. Claim/publication/retry/cleanup boundaries
continue to reopen and verify actual bytes. Ten new execution tests cover later
admission discovery, explicit retry, clock/handle-pressure recovery, real callback
concurrency with independent work-view reads, progress past unready branches,
invalid/duplicate pools, drop/shutdown with retained backlog, and visible fail-stop
on corrupt job storage. Two payload tests cover global/per-owner slot accounting,
sequential use, concurrent-use refusal, and stage/installed-token drop order.
Final review added a second negative-first regression: an immutable two-handle
ceiling previously admitted output-producing work that could never run. Admission
now checks permanent worker-I/O feasibility without confusing temporary reader
occupancy with an impossible policy. Refusal leaves no job/receipt; work declaring
no outputs still admits with two handles. All 119 focused authority tests and strict
workspace clippy pass.

This is bounded-memory discovery, not an indexed ready queue or a measured
constant-cost populated-store sweep. Arbitrary application code cannot be forcibly
preempted; it must cooperate with context deadlines and lease renewal. Transient
I/O slots are reacquired after restart; immutable byte budgets remain persisted.
Authority format 6, payload format 4, normative text and frozen bytes are unchanged.

Local logs:

- `/tmp/pipestream-worker-io-red.log` (the reproduced pre-fix output failure)
- `/tmp/pipestream-worker-admission-io-red.log` (the impossible admission)
- `/tmp/pipestream-worker-pool-authority.log`
- `/tmp/pipestream-worker-pool-clippy.log`
- `/tmp/pipestream-worker-pool-suite.log`
- `/tmp/pipestream-worker-pool-final-suite.log`

Next: authoritative deadline/cancellation/skip/revocation settlement and complete
branch execution, including producer-1 admission and nonempty closure. The pool
currently refuses those unfinished transitions; it does not settle expired work.
Authenticated RESULT read leases, dependency retention, expiry and retirement
remain required. Independent Java V2, actual V2 Quinn/Netty endpoints, neutral
cross-language failure tests and the equivalent streaming-gRPC workload remain
part of the unchanged full goal. No V2 profile is activated by this checkpoint.

Final verification after the admission correction:
`./conformance/run_all.sh` exited 0. The Rust workspace passed 480 tests and the
two Rust examples passed another six. Java's 20 Surefire reports contain 193 tests
with zero failures, errors or skips. Formatting, strict clippy, frozen vectors,
bounded models, C++ tests, all nine black-box language pairs, 32 raw QUIC capability
probes and the external examples passed. Network evidence still concerns the
historical profiles, not V2 interoperability. This is local verification, not hosted
CI, a main merge, a deployment or an Internet-Draft submission.

### Task 2 implementation progress: authoritative fences and bounded settlement

The Rust authority now commits cancel/skip receipts together with typed first
fences. Declared inputless work can settle without inventing admission. Branches
remain CANCELLING until descendant obligations and scope summaries are durable;
earlier terminal results never change. A later ancestor cancellation, deadline
or STRICT child failure cannot replace an accepted SKIPPED/CANCELLED promise.
Scope cancellation freezes membership immediately, including authority-producer
scopes. A separately authorized local revocation API installs the root fence and
denies caller access even when ordinary caller authorization has been withdrawn.

`reconcile` processes bounded work batches and one incremental scope fold.
Deadline expiry settles ACTIVE/AWAITING_RETRY work as FAILED. STRICT nonsuccess
settles unfenced parents, but parent failure does not erase descendant obligations.
Frozen membership yields a full seal, and terminal members yield disjoint counters
and an immutable status root including child roots and successful manifests.
An unfinished volatile hash may be recomputed after restart. Work and scope
transactions are separate, so already committed work survives a later scope-pass
refusal. The worker pool has one additional maintenance thread so occupied
application workers do not prevent authoritative settlement.

Authority format 7 adds a fixed 256-byte first-fence body plus its 104-byte record
header and one rewrite credit. Scope credits increase from two to four to fund
freeze, deferred seal, root revocation upgrade and closure. Prior authority
formats are explicitly refused; payload format 4 and frozen wire bytes are
unchanged. Existing physical-exhaustion tests retain their 4 MiB WAL cap and now
use eight-member initial declaration batches to fit the additional promises.
The unrelated 600-member seal-equivalence fixture receives more funding instead
of weakening its record reservations.

Seventeen new tests cover operation replay/conflicts/dispositions, four exact
outcome buckets and manifest/child status commitments, nested fence precedence,
late prepared-admission/retry/publication exclusion, explicit skip/revoke policy,
last-moment authorization rollback and unsafe clocks. Real copy callbacks are
held after installing outputs; all four fence paths beat their late publication
without refunding live handles or byte promises. Dedicated maintenance settles
expiry and closes the root while the only callback worker is still held.
A 600-member closure uses batches of 73 then 127 across reopen and verifies full
seal/status commitments. Twelve actual child-process exits bracket work fence,
scope fence, revocation, deadline settlement, seal and summary commits; recovery
reopens real retained input and preserves charged output reservations.

Whole-write-set gates fill ordinary journal capacity behind a pinned WAL reader,
forbid SQL row replacement and verify no database page growth. With the unchanged
4194304-byte WAL cap, the focused run measured 3048856 to 3065312 bytes for expiry
and 3135376 to 3160072 for cancellation plus deferred seal and closure. These are
file-length gates, not filesystem-block preallocation, whole-lifecycle or gRPC
baseline measurements. Reopen rejects prior formats and fences rebound to a
changed receipt or impossible terminal timestamp, and validates scope ancestry.

Implementation experience clarified Section 12: membership freeze is atomic but
large seal computation may be deferred, with a null page seal until the full
digest commits. Cancellation and deadline failure serialize by accepted fence or
terminal commit, not merely by clock observation. The composed model now also
rejects failure overriding an accepted fence. The draft rebuild passed with zero
idnits errors, flaws or warnings; its one informational comment is the FIPS
normative-reference downref check.

Current focused verification: 136 authority tests and eight composed-model tests
pass. The final authority rerun also checks public membership pages before and
after the deferred seal commits. Strict workspace clippy passes.
Logs: `/tmp/pipestream-settlement-final-authority.log`,
`/tmp/pipestream-settlement-model.log`, `/tmp/pipestream-settlement-final-draft.log`,
`/tmp/pipestream-settlement-final-clippy.log`,
and `/tmp/pipestream-settlement-verified-suite.log`.

Next: complete authority-produced children and branch/child-output execution,
then authenticated RESULT lookup/read leases, dependency retention, expiry and
safe retirement. Input/output liveness remains charged until that cleanup exists.
Independent Java V2, real V2 Quinn/Netty endpoints, the neutral cross-language
failure driver and external workload plus equivalent streaming-gRPC evidence
remain explicit deliverables. No V2 profile is activated. The full goal remains
active and incomplete; this is a storage/application checkpoint, not conformance.

Final verification: `./conformance/run_all.sh` exited 0. The Rust workspace passed
497 tests and the two Rust examples passed another six. Java's 20 Surefire reports
contain 193 tests with zero failures, errors or skips. Formatting, strict clippy,
frozen vectors, bounded models, C++ tests, all nine black-box language pairs,
32 raw QUIC capability probes and the external examples passed. The composed
model explored 620796 states and 29177412 edges with no frontier at depth 32;
all 12 negative controls were detected. The checked-in model evidence matches
this run. This is bounded safety evidence, not a liveness or storage proof.
The final draft rebuild again passed with zero idnits errors, flaws or warnings.
Network tests still concern historical profiles, not V2 interoperability. No
main merge, deployment or Internet-Draft submission occurred.
