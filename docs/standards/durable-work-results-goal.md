# Durable work, results, and usefulness: execution record

Status: active, not accepted. Baseline: `68b02ea55169187da123b7efa0f6045718fbb645`.
The user approved all three tasks in the goal objective attached on 2026-09-06.
This record preserves the complete scope; an intermediate commit is not completion.
Task 1's contract/design acceptance is recorded below. Tasks 2 and 3 are open;
Rust now has a version-2 durable endpoint and client. Complete independent Java
parity, cross-language failure evidence and the workload comparison remain open.

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

## Authority expansion and real child-output reassembly checkpoint, 2026-09-07

Continued from `f5c56b3` without changing the full goal or touching the search
fleet. Rust authority-mode applications now provide a real expansion callback.
Its opaque producer grant binds local declarations and admissions to the parent,
child scope, current attempt/worker lease, authorization and deadline. The shared
admission pipeline preserves producer-1 immutable operations and ordinary quota,
input-integrity and reservation checks; an external flag cannot grant this role.

Expansion yields release the worker while preserving accepted children and
terminal-transition credits. The pool backs off after a yield. Complete expansion
commits a separate durable phase and WAITING_CHILDREN state. A regression first
failed because the implementation mistook a sealed child scope for completed
expansion, omitting still-missing child admissions. Authority format 8 now stores
that phase explicitly; prior format 7 is refused without conversion. Payload
format 4 and wire examples do not change. A second negative-first regression
strengthened reopening checks: a valid record checksum cannot legitimize
successful authority work whose expansion is still marked unfinished.

Both branch modes page closed direct children and stream their retained output
objects into reassembly. Verified child EOF is required before successful parent
publication. A parent can use a child dependency after external output expiry,
but its reads remain subject to its own authorization, deadline and fences.
An extra reader slot is reserved before reassembly claim; it cannot be stolen
by concurrent opens, and a live reader keeps its charge after the worker drops.
Minimum permanent handle policies are checked before admission. Child staging
and admission still acquire their own quotas: applications may yield on pressure,
and parent admission does not promise unlimited descendant capacity.

Nineteen branch tests (including the subprocess entry point) verify two-byte
streaming of `abc` into two child transformations and actual `ABC` reassembly,
both producers, single-worker progress, stale local preparation across retry,
lease/deadline/cancel/revoke/auth changes, reader fencing/integrity and capacity.
Six real process deaths bracket local declaration, admission and phase commits.
Recovered discovery completes the original attempt with exactly three jobs and
three local operations, preserving lost acknowledgments. Explicit retry keeps
the same children and never repeats completed expansion; unpublished parent
output is reclaimed before reassembly retry. This is not the external distributed
workload or the equivalent streaming-gRPC benchmark.

Pinned-WAL gates fill ordinary capacity, forbid work/job/clock SQL row replacement
and verify unchanged database page counts for expansion completion and final
publication. With a 4194304-byte cap, the focused run measured WAL lengths
1112456 to 1120672 and 1821096 to 1829312 respectively. These are configured
file-length bounds, not filesystem-block preallocation or RSS measurements.
The full focused authority run passes 155 tests and strict workspace clippy
passes. Section 12 now explicitly distinguishes sealing, admission and expansion
completion and requires replayable local progress with parent commit fences.
The draft rebuild passed with zero idnits errors, flaws or warnings and the
existing informational FIPS-180-4 normative-downref comment.

Logs: `/tmp/pipestream-expansion-final-authority.log`,
`/tmp/pipestream-expansion-final-clippy.log`,
`/tmp/pipestream-expansion-final-draft.log`,
`/tmp/pipestream-expansion-verified-suite.log`.

Next: authenticated RESULT lookup/read leases, dependency retention and crash-safe
cleanup/retirement. Reservations still remain charged until that cleanup is built.
Independent Java V2, real authenticated V2 endpoints, neutral cross-language
failure scenarios and external workload/gRPC evidence remain required. The full
goal is active and incomplete; no V2 profile is advertised by these library APIs.

Final verification: `./conformance/run_all.sh` exited 0. Rust workspace tests
passed 516/0, with another six passing Rust example tests. All 20 fresh Java
Surefire reports total 193 tests, zero failures/errors/skips. C++ tests, frozen
vectors, bounded lifecycle models, all nine black-box language pairs, all 32
raw QUIC capability probes and recursive/external example scenarios passed.
The network evidence still covers historical profiles, not V2 interoperability.
`./build.sh core 05` exited 0; the rendered text includes the new expansion
requirements. No main merge, deployment or Internet-Draft submission occurred.

## Retained result lookup and bounded delivery checkpoint, 2026-09-07

Continued from `85977e9`. Rust now supplies `ResultService` independently of the
executor. It checks the host-verified identity under a separate `ReadResult`
permission, retrieves immutable manifests, and grants object reads only for the
exact published work/attempt/index/digest while trusted UTC is before availability
expiry. Object pinning and the clock commit precede the returned pending transfer.
Manifest lookup can expose retained evidence under unsafe time or after output
expiry without granting a fresh lease. No read path invokes application code,
advances an attempt or changes the successful work view.

Result tokens expose the exact response header and one bounded outstanding chunk.
The host checks before scheduling writes/FIN and reports actual bounded transport
acceptance; disk reads and empty progress do not renew idle time. Source EOF is
hash/length verified, and the receiver still validates its own object and FIN.
Drop/reset affects only delivery. Current authorization and monotonic deadlines
remain enforceable after output expiry; no later UTC regression can turn a
previously granted stream into a new or extended availability promise.

Pending and active reads use the shared root's global/per-owner handle ceiling,
including across separate service instances. Bounded maintenance closes idle,
expired or revoked transfers while callers retain their tokens. Registry locks
do not span per-read I/O or authorization, and busy I/O remains charged until it
can be closed safely. A negative-first regression caught a timer race: a delayed
maintenance timestamp could incorrectly label newer send progress a clock
regression. The fix preserves foreground observations without extending deadlines.
Another negative-first regression showed that continuous arrivals could keep the
cursor past an older expired lease. Each pass now captures a finite upper bound,
so it revisits older leases even while new reads arrive.
The endpoint must still drive maintenance, enforce connection limits, bound
transport buffers and reset stopped streams; no V2 endpoint exists yet.

Nineteen result tests include three actual process exits around grant/clock commit
and partial delivery, exact replay without re-execution, distinct/late-withdrawn
permission, corrupt or missing output, named identity/commitment/state refusals,
pending/idle/lifetime/FIN equality, zero outputs versus empty output, cancellation
versus revocation, global/per-owner pressure and shutdown with retained tokens.
A pinned-WAL test exhausts ordinary table writes and then the smaller clock-write
shape: new grants refuse while an admitted delivery completes with unchanged
work, 299 DB pages and a 3226016-byte WAL under the 4194304-byte cap.

The standalone 32 MiB result test publishes actual bytes through the application
API, opens eight reads, transfers in 16 KiB chunks, independently verifies the
receiver digest and expires seven stalled tokens without caller Drop. The focused
run measured 6035 bytes of added Rust heap, a largest allocation of 464 bytes,
unchanged 69632-byte database and zero WAL growth; process RSS/HWM was 7228 KiB.
Its 2600 ms delivery phase is a local measurement, not a network throughput or
gRPC comparison. This does not bound SQLite/native allocations by the Rust heap
gate. Source: `tests/v2_result_resources.rs`.

Focused verification passes 174 authority tests, the standalone resource gate,
and strict workspace clippy. Logs: `/tmp/pipestream-result-final-authority.log`,
`/tmp/pipestream-result-resources-final.log`,
`/tmp/pipestream-result-final-clippy.log`,
`/tmp/pipestream-result-final-draft.log`,
`/tmp/pipestream-result-final-suite.log`.

Section 12 now explicitly separates manifest evidence from new read permission,
bounds pending-read pins and excludes disk/application buffering from sender
idle progress. Authority format 8, payload format 4 and frozen wire bytes remain
unchanged. Next is reference-safe retention and dependency cleanup/retirement;
accepted input/output reservations remain charged until that lifecycle is built.
Independent Java V2, authenticated V2 endpoints, the neutral cross-language
failure driver and external workload plus equivalent streaming-gRPC evidence
remain required. The full goal is active and incomplete.

Final verification after the fairness fix: `./conformance/run_all.sh` exited 0
(`/tmp/pipestream-result-final-suite.log`). Rust workspace tests passed 536/0,
plus six Rust example tests. Java's 20 fresh Surefire reports total 193 tests
with zero failures/errors/skips. C++ tests, frozen vectors, bounded models, all
nine basic language pairs, all 32 raw QUIC capability probes and the recursive
and external example scenarios passed. These remain historical-profile network
tests, not V2 interoperability. The current draft build exited 0 with zero idnits
errors/flaws/warnings and the existing informational FIPS downref comment.
No main merge, deployment or Internet-Draft submission occurred.

### Dependency-aware retention and cross-store accounting (2026-09-07)

This checkpoint implements Rust payload reclamation, not session retirement.
`AuthorityStore::reclaim` first commits a typed, timestamped release intent while
the job remains logically charged. A separate writer-locked collector removes
only eligible unpinned files and syncs the directory. A later fixed-record commit
releases logical input/output capacity only after those files and their funded
reservation are absent. Crashes can retain conservative charges but cannot
refund still-occupied resources. Manifests, receipts, work views, scope summaries
and authority/owner high-water marks remain untouched.

Terminal input additionally waits for child closure. Outputs wait for their
external interval to expire and for their direct dependent parent to settle.
Actual result-read and callback handles still block physical deletion even
after intent commits. Local maintenance is independent of current caller
permission, but destructive expiry refuses unsafe or regressed UTC. Reclaim
uses separate bounded metadata and file cursors; descending job passes revisit
old jobs even while new work is admitted.

Matched-root rebind, executor startup and fresh admission now audit every
retained job against input, output reservation and published manifest files.
Missing required bytes fail closed unless a verified committed release intent
authorizes their absence. These are bounded-header/file-length audits, not
hashes of every body; actual readers still verify length and SHA-256. They
stream all jobs under the writer/inventory locks and are not constant-time
admission checks. Record-credit audits also still stream retained metadata;
batch limits bound materialization and mutations, not total database work.

Thirteen new retention tests include six actual process deaths around intent,
unlink and quota commits, held result/callback pins, independent input/output
expiry, unchanged retained evidence, unsafe UTC, authorization withdrawal,
missing live files before new capacity, cursor limits/fairness and checksummed
invalid intents. The real `ABC` branch test now runs cleanup after child output
expiry, proves the active parent still retains those outputs, reassembles them,
and then releases child and parent outputs at their separate eligibility cuts.

A deliberately removed prior-intent validation guard caused the corruption
regression to fail: cleanup could replace a forged future intent with a new
otherwise valid timestamp. The restored guard validates the old evidence before
any mutation. Reopen also refuses early/pre-terminal/future intent and authority
format 8. Authority format 9 adds the release field and six funded job rewrites;
payload format 4 and frozen wire bytes do not change.

The focused pinned-WAL test exhausts ordinary inserts and smaller clock writes,
then commits four separately timed input/output intent/finish writes. SQL
triggers forbid row replacement; the database stays at 284 pages and WAL grows
from 3052976 to 3085912 bytes under the 4194304-byte cap (236 fill writes).
The 32 MiB library resource test now holds a result pin through batch-one cleanup,
checks that only input can disappear, drops the pin and reclaims the output and
reservation. Its focused cleanup phase measured 1374 bytes of additional Rust
heap, largest allocation 338 bytes, unchanged 69632-byte DB and zero WAL over
102 ms. These are scoped local measurements, not native heap, allocated disk
blocks, network flow control or an equivalent gRPC benchmark.

Section 12 now explicitly requires recoverable deletion eligibility before
cross-store deletion and prohibits early quota refunds or treating missing live
storage as proof of expiry. Focused logs are
`/tmp/pipestream-retention-authority.log`, `/tmp/pipestream-retention-wal.log`,
`/tmp/pipestream-retention-resources.log`, and
`/tmp/pipestream-retention-corrupt-intent-red.log` (deliberate negative control).
Focused captured measurements and commands are checked in at
`conformance/results/durable-work-v2-retention-2026-09-07.txt`.

Next: implement session retirement after closed-root creation-receipt retention
and every longer work/output/read/dependency promise. Retirement must preserve
authority generation and owner creation high-water marks and remain recoverable
through bounded metadata deletion; `sessions::load` currently assumes its root
exists, so partial retirement needs an explicit retained state before removing
that root. Existing per-record rewrite credits do not by themselves fund
arbitrary SQL deletion. This is still implementation work, not an external
permission blocker. Independent Java V2, authenticated endpoints, the neutral
cross-language driver and the full external workload/gRPC comparison remain
required. The full goal is active and incomplete.

Final retention checkpoint verification: `./conformance/run_all.sh` exited 0
(`/tmp/pipestream-retention-full-suite.log`). The 549 Rust workspace tests and
six Rust example tests pass. All 20 fresh Java Surefire XML reports total 193
tests, zero failures/errors/skips. C++ checks, frozen vectors, bounded models,
nine black-box language pairs, all 32 raw QUIC capability probes and recursive
and external examples pass. Formatting and strict workspace clippy pass in that
same run. These network tests still exercise historical profiles, not V2
interoperability. `./build.sh core 05` exited 0
(`/tmp/pipestream-retention-draft.log`); the rendered cleanup paragraph was
inspected and idnits reports zero errors/flaws/warnings with the existing FIPS
downref comment. `git diff --check` passes. No main merge, deployment or IETF
submission occurred.

### Session retirement and restart ownership checkpoint, 2026-09-07

Rust now implements the retirement step identified above. A fixed immutable
proof records eligibility before any bounded metadata deletion; the closed root
and session slot survive until the final atomic deletion. Every work receipt,
output/read/dependency promise must have drained first. Requests by a currently
authorized owner receive EXPIRED during partial retirement, with authorization
denials taking precedence. Owner creation and authority generation high-water
marks are preserved and old identities cannot be reused. Authority format 10
refuses its predecessor without conversion; payload and frozen wire formats do
not change. Section 12 now explicitly specifies the durable retirement cut.

Twelve focused tests cover corrupt proof/flag/history, full creation retention
from root closure, longer output/read promises, empty and SKIPPED-only sessions,
other-owner isolation, quota timing, clock safety and bounded cursor refusals.
Ten actual process exits straddle all five retirement commit phases using real
authority-expanded child output and `ABC` reassembly. Intermediate reopen,
payload audit and settlement/reclamation remain valid through partial deletion.
Disabling the job-liveness guard made the held-read test fail; it is restored.

Retirement uses protected ordinary SQL capacity, not unfunded claims about
fixed-record rewrite credits. Pinned-WAL tests refuse safely both before and
after intent without refunding capacity or losing history. Releasing the reader
alone did not reliably reclaim journal space with another connection open;
`checkpoint_storage` now provides explicit nonblocking local WAL maintenance.
It refuses while the reader is pinned and permits retirement after release and
checkpoint. Eligibility/integrity audits stream metadata: batch size is not a
constant-time scan guarantee.

The extended 32 MiB gate measures delivery, payload reclamation and session
retirement separately. Retirement used 1105 bytes of additional Rust heap,
largest allocation 338 bytes, unchanged 73728-byte DB and zero WAL over 26 ms
in the focused run. Raw scoped measurements and commands are checked in at
`conformance/results/durable-work-v2-retirement-2026-09-07.txt`.

The first full run failed a historical application-refusal restart test with
OS error 11. A hundred isolated repeats passed without locating the call. A
deterministic destructor barrier then reproduced a real retained-root race:
zero Arc strong count was mistaken for completed OS-lock release. The registry
now coordinates this local ownership handoff with a five-second condition-variable
wait budget and still refuses live or external owners. This does not bound
filesystem operations or mutex acquisition. Two simultaneous reopeners share one
root; an unrelated root progresses while the old finalizer is paused. All 56
focused retained-root tests pass. This is regression-driven repair, not proof
that the original untraced failure could have no other source.

Next: integrate the complete Rust V2 authority/result lifecycle with
authenticated Quinn endpoints and durable client uncertainty journals; implement
the same current contract independently in Java and the neutral Rust failure
driver. Then complete the external chunk/distribute/transform/reassemble workload
and equivalent streaming-gRPC baseline, with all requested process/resource and
failure evidence. These are implementation deliverables, not permission blockers.
The full goal remains active and incomplete; no V2 endpoint is advertised yet.

Final full-suite validation: `./conformance/run_all.sh` exited 0, captured in
`/tmp/pipestream-retirement-full-suite-final.log`. The 562 Rust workspace tests,
six Rust example tests, and 193 Java tests in 20 fresh XML reports pass without
failures/errors/skips. Formatting, strict clippy, frozen vectors, bounded models,
C++ checks, nine black-box language pairs, 32 raw QUIC capability probes and
recursive/external examples pass. Historical network coverage is not V2
interoperability evidence. No main merge, deployment or IETF submission occurred.
`./build.sh core 05` exited 0 (`/tmp/pipestream-retirement-draft-final.log`),
the rendered retirement paragraph was inspected, and idnits reports zero
errors/flaws/warnings plus the existing informational FIPS downref comment.

### V2 TLS and live credential checkpoint, 2026-09-07

The separate Rust `v2_tls` module now provides V2 TLS 1.3 configuration and
full-handshake peers, with mandatory validation of presented certificates and
an explicit stable fingerprint-to-owner mapping. Missing/valid-unmapped peers
remain Core-only; they cannot activate a required durable profile. Live request
checks revalidate credential validity, current trust and mapping under one
serialized guard. Invalidating a peer cannot change its identity or make it
anonymous, and it cannot resurrect if the old policy/time later returns.

Server session storage/tickets and client resumption are disabled independently.
Sixteen tests, including real QUIC handshakes, cover certificate rotation, invalid chains/usage/time,
DNS/IP server identity, Core-only peers, mapping/trust replacement, exact PKIX
validity boundaries, pre-handshake and in-handshake clock failure, chain bounds,
legacy ALPN refusal and both sides of resumption policy. Three removed-guard
negative controls failed as intended and were restored; raw captures are in
`conformance/results/durable-work-v2-tls-2026-09-07.txt`.

The clock test exposed a local-stack diagnostic limitation: absent time inside
rustls reaches Quinn as PROTOCOL_VIOLATION rather than a certificate TLS alert.
Known absence is now refused before handshake as local CLOCK_UNSAFE/wire
CONNECTION_REFUSED. Mid-handshake clock loss still fails closed; correct wire
categorization remains an integration follow-up before declaring that gate done.
The injected clock is a trusted deployment input, not an independent UTC proof.

This checkpoint does not activate a partial durable profile or complete Task 2.
Next is bounded Core framing/dispatch and connection accounting, followed by
the authenticated authority/input/result paths and durable client uncertainty
journals. Independent Java, the neutral cross-language failure driver and every
external workload/equivalent-gRPC measurement remain required. Appendix D now
reflects real Rust authority/TLS progress without claiming those missing pieces.
The full goal remains active and incomplete.

The TLS checkpoint passed `./conformance/run_all.sh`: 578 Rust workspace tests,
six Rust-example tests, 193 Java tests in 20 fresh XML reports with no failures,
errors or skips, frozen vectors/CDDL, all three bounded models, C++/CTest,
nine existing interop pairs, 32 raw capability probes and recursive/external
examples. The original suite process exited 0; these existing end-to-end
checks remain V1 regression evidence. `./build.sh core 05` also exited 0;
the rendered implementation-status paragraph was inspected, with zero idnits
errors/flaws/warnings and the existing FIPS downref comment.

### V2 local TLS error correction, 2026-09-07

The clock-loss follow-up from the TLS checkpoint is now corrected in both
client and server configurations. The stricter real-handshake test first
failed with PROTOCOL_VIOLATION. A private adapter at Quinn's pinned rustls
handshake-read boundary now uses fatal TLS `handshake_failure` (QUIC 0x128)
when rustls fails without an alert. Existing TLS alerts and genuine transport
errors are preserved. It does not substitute a clock value or enable fallback.

Nineteen TLS/security tests pass, including both peers' observed clock-failure
closes, all 256 existing TLS alert codes, and a real incorrect-connection-ID
transport-parameter failure that retains its own error. No normative wire
format or storage changed. Complete V2 dispatch/quotas, client journals,
independent Java, neutral failures and the equivalent-workload measurements
remain unfinished; the original goal remains active.

Before exposing the endpoint, bind `ServerSecurity::accept` to its owned
configuration with Quinn's `Incoming::accept_with`, and test an incoming
connection created under a different endpoint default. Currently it awaits
the incoming connection's default configuration; the embedding application
must keep that default identical to `ServerSecurity::configuration`. The
existing ticket-offering peer fixture must remain an independent test after
this tightening. Then wire bounded Core dispatch and connection accounting.

Final correction verification: `./conformance/run_all.sh` exited 0. It passed
581 Rust workspace tests, six Rust-example tests, 193 Java tests in 20 fresh
XML reports (no failures/errors/skips), frozen vectors/CDDL, all three bounded
models, C++/CTest, nine existing interop pairs, 32 raw capability probes and
all recursive/external examples. `./build.sh core 05` exited 0 with zero
idnits errors/flaws/warnings and the existing FIPS downref comment. Raw results
are in `conformance/results/durable-work-v2-tls-alerts-2026-09-07.txt`.
No main merge, deployment or IETF submission occurred.

### Owned V2 TLS configuration and Core endpoint, 2026-09-07

The previous configuration integration follow-up is implemented: acceptance
selects the owned TLS configuration explicitly. Its regression first reproduced
the stale-default anonymous-peer error. The ticket-offering test peer remains
independent, and removing client resumption disablement still fails that test.

The Rust `v2_core::Server` library now provides an actual QUIC-v1 Core-only
listener, bounded concurrent tasks and retained transport connections,
stable-owner and anonymous quotas, canonical incremental control framing,
capability selection, named/correlated refusals and connection detach. Fourteen
Core tests, including 13 real-QUIC cases, pass alongside 20 TLS tests. A deliberately relaxed quota
boundary failed its refusal test. All negative-control edits were restored.
The post-detach control-request refusal is now explicitly NOT_READY in Section
12.8; it consumes correlation IDs but never asserts durable work completion.

These are bounded local network tests, not a completed V2 durable endpoint or
independent-language/process-resource proof. The raw control-buffer product is
bounded separately from process heap/RSS. The Core server cannot advertise the
unimplemented durable profiles. Existing standalone commands still run V1.

Next integrate the authenticated authority, input staging and result streams
through this transport, including independent progress and aggregate quotas;
provide the V2 client and durable uncertainty journals and expose the runnable
reference commands. Java parity, the neutral failure driver, external workload,
equivalent streaming-gRPC baseline and every original measurement remain
required. The goal remains active and incomplete.

The Core checkpoint passed `./conformance/run_all.sh` with exit 0: 596 Rust
workspace tests, six Rust-example tests, 193 Java tests in 20 fresh XML reports
(no failures/errors/skips), frozen vectors/CDDL, all three bounded models,
C++/CTest, nine existing interop pairs, 32 raw capability probes and all
recursive/external examples. The existing end-to-end pairs remain V1 evidence.
`./build.sh core 05` exited 0; the rendered Section 12.8 clarification and
Appendix D status were inspected, with zero idnits errors/flaws/warnings and
the existing FIPS downref comment. Results are captured in
`conformance/results/durable-work-v2-core-2026-09-07.txt`.
No main merge, deployment or IETF submission occurred.

### Authenticated durable control adapter, 2026-09-07

`pipestream_quic::v2_authority` now joins actual TLS peer identity to the existing
on-disk authority. It handles session creation/attachment/sequence, declaration,
scope pages/checkpoints/cancellation, operation lookup, work views/waits/retry/
cancel/skip, manifest/result-read requests, completed-session drain and detach.
The Core listener is not yet connected to this adapter, so neither durable
profile is advertised. These are local dispatcher tests, not durable QUIC RPCs.

Connection-local tickets cover unresolved controls, unsent responses, result
reads and input jobs. A blocking metadata task retains its ticket and global
metadata permit even when its async waiter is cancelled. The lost-response test
demonstrates a still-running real commit, exclusion of an early detach, and
replay of the same generation after commitment. A deliberately removed ticket
makes that test fail. Another negative control removes exact root comparison
and demonstrates the invalid completed acknowledgment for a changed summary.
Both deliberate faults were restored.

The first full suite found a real contention error: an already accepted watch
could return capacity refusal when cancellation held the only metadata slot.
A held-transaction regression reproduced it. Subsequent polls now join the fair
metadata queue within their existing wait budget, without holding SQLite or a
thread while waiting. They preserve timeout snapshot/checkpoint semantics.
The final focused set has 12 passing tests, including actual admission, retry,
copy execution and 12 KiB stored-result reading without advancing committed work.

Section 12.8 now explicitly includes other pending control requests in the
completion cut and keeps that cut stable through response sending. Frozen
wire/CDDL bytes, storage formats and dependencies are unchanged. The final
`./conformance/run_all.sh` exited 0: 608 Rust workspace tests, six Rust-example
tests, 193 Java tests in 20 fresh XML reports (zero failures/errors/skips), all
models/vectors/native checks, nine existing interop pairs, 32 capability probes,
recursive cases and all examples. Those complete interop pairs remain V1 only.
`./build.sh core 05` exited 0, with rendered cut/status inspection and zero
idnits errors/flaws/warnings plus the existing FIPS comment. Evidence is in
`conformance/results/durable-work-v2-dispatch-2026-09-07.txt`.

Next connect this adapter to bounded QUIC input/result I/O and the lifecycle
runtime. Input tasks must retain/clone `InputSlot` through blocking commits;
result tasks must retain `Response` through stream creation, chunk writes and
FIN/abort. Metadata and data worker capacity, per-connection pending responses,
and QUIC flow-control windows must reserve independent control progress, with
aggregate byte/count limits. The standalone V2 client, durable uncertainty
journals, Java parity, neutral failure driver, full resource measurements and
the original external/equivalent-gRPC workload remain required. This checkpoint
does not complete the goal or authorize a partial-profile advertisement.
No main merge, deployment or IETF submission occurred.

### Authenticated QUIC input transport, 2026-09-07

The input adapter now receives actual QUIC objects through the existing durable
admission path. Eight tests cover bounded reception across smaller windows,
receipt replay without body/FIN, malformed and empty inputs, owner identity and
configuration limits, stalled headers, idle/lifetime expiry, and cancellation.
An independent worker test checks that file destruction precedes quota release
and runs outside the async executor. Its deliberate direct-drop negative control
failed, and the deferred-cleanup guard was restored. All 55 focused V2 tests and
strict clippy pass. Storage formats, dependencies and normative wire bytes did
not change; Appendix D records only the implemented input subset.

Cancelling an input task cannot cancel an already-running storage operation or
refund its quota early. File jobs and returned staged values retain connection
and input leases until cleanup ends. Fixed file workers and a bounded queue keep
that work separate from metadata dispatch. Empty declared length does not replace
FIN, and ongoing payload progress does not extend lifetime. The actual 64 KiB
QUIC input commits once and later runs the real copy application.

These tests still call durable controls locally. Public durable-listener and
result I/O integration, runtime maintenance, V2 client/journals, independent Java,
neutral failure testing, full resource measurement and the original external
workload/equivalent streaming-gRPC baseline remain required. This input checkpoint
does not complete the goal. Validation evidence is in
`conformance/results/durable-work-v2-input-2026-09-07.txt`.

Final regression and draft builds exited 0. The suite passed 617 Rust workspace
tests, six Rust-example tests and 193 Java tests from 20 fresh XML reports
(zero failures/errors/skips), frozen vectors/CDDL, all three bounded models,
native checks, nine existing interop pairs, 32 raw capability probes and all
examples. The complete language pairs remain V1 evidence. Rendered Appendix D
was inspected; idnits reports zero errors/flaws/warnings and the existing FIPS
comment. No main merge, deployment or IETF submission occurred.

### Authenticated retained-result streams, 2026-09-07

The Rust output adapter now sends actual retained objects over QUIC with fixed
file workers, bounded global/owner/connection counts, fresh authorization before
nonblocking writes, and exclusive idle/lifetime deadlines. Ten tests cover empty
and larger-than-window outputs, replay without execution, stopped/slow receivers,
pending stream creation, credential expiry, corruption, quota and cancellation.
The output's header starts its one response; later failures reset the stream
without a second control response. A read does not repair or rerun computation.

Review also found and reproduced an input idle timeout that waited for blocked
file preflight. Preflight/chunk waits now abort on time without releasing still-
running I/O quota. The possible admission commit preserves its uncertainty rule.
A watch test now accounts for legitimate pre-start metadata capacity refusal
instead of assuming a fixed sleep proves the slot is free. The deliberately
disabled output credential guard fails its regression and was restored.

Focused checks pass 66 V2 transport/security tests and the exact library result
deadline case, with strict clippy. Normative wire/CDDL, storage formats and
dependencies are unchanged; Appendix D records the actual adapter coverage.
Both adapters still use local control calls in these tests. Public durable V2
runtime integration, reserved control credit, maintenance/shutdown, client and
uncertainty journals, independent Java V2, neutral cross-language failures, full
resource measurements and the original external/equivalent streaming-gRPC
workload remain required. This checkpoint does not complete the goal. Evidence:
`conformance/results/durable-work-v2-output-2026-09-07.txt`.

Final regression and draft builds exited 0. The suite passed 628 Rust workspace
tests, six Rust-example tests and 193 Java tests from 20 fresh XML reports
(zero failures/errors/skips), frozen vectors/CDDL, all three bounded models,
native checks, nine existing interop pairs, 32 raw capability probes and all
examples. These complete language pairs remain V1 evidence. Rendered Appendix D
was inspected; idnits reports zero errors/flaws/warnings and the existing FIPS
comment. No main merge, deployment or IETF submission occurred.

### Connection-owned control reservation, 2026-09-07

The result adapter now shares one send-admission owner with control traffic;
cross-connection owners are refused. Nine real QUIC tests and two result tests
exercise send/receive reservations, actual stored-result/control progress,
role limits, stream replacement and named configuration refusals. Review found
and reproduced two missing liveness conditions: batched connection-credit
updates need headroom beyond `(N+1)*W`, and blocked control polls need an
independent retry wake because Quinn's normal wake condition uses the lower
data window. Both regressions fail before their fixes and pass afterward.
A deliberate removal of the send-window restoration guard also fails its test.

Section 12.1 now specifies reservation through replenishment/replacement and
its limits when a peer or network cannot make progress. The implementation uses
the pinned transport's actual credit policy, not stream priority as a substitute.
The focused run passes 77 V2 tests and strict clippy. No wire/CDDL, storage or
dependency version changed. This is not the public durable runtime: integration
of bounded readers/writers, lifecycle maintenance/shutdown, V2 client/journals,
independent Java, neutral failure driver and the original external workload plus
equivalent streaming-gRPC/resource comparison remain required. The goal remains
active. Evidence: `conformance/results/durable-work-v2-flow-2026-09-07.txt`.

Final full regression and draft builds exited 0: 639 Rust workspace tests, six
Rust-example tests, 193 Java tests in 20 fresh XML reports without failures,
errors or skips, all three bounded models, frozen vectors/CDDL, native checks,
nine existing V1 interop pairs, 32 raw capability probes and all examples.
Rendered Section 12.1 and Appendix D were inspected; idnits has zero errors,
flaws or warnings and its existing FIPS comment. No main merge, deployment or
IETF submission occurred.

### Authority execution and maintenance runtime, 2026-09-07

The Rust authority now has an owned execution pool and independent native read,
retention and retirement loops. Tests exercise prior admission discovery, real
copy execution, background read expiry while a callback is blocked, unsafe-clock
pause/recovery, read-pinned retirement and identity non-reuse. The runtime reports
typed failure/health state; only explicit clock, capacity and storage contention
are retried. Existing full accounting/retirement eligibility scans remain visible
limitations, not hidden behind the cursor batch parameter.

Integration also reproduced and fixed a stop request blocked on the discovery
mutex, an executor rejecting retained durable-only sessions, and callback dispatch
before successful creation of every worker thread. The startup guard's negative
control fails; final focused checks pass 81 V2 transport/runtime tests, 90
execution tests and strict workspace clippy. Storage formats and wire/CDDL did
not change. Evidence: `conformance/results/durable-work-v2-runtime-2026-09-07.txt`.

This is the execution/maintenance component, not a finished public durable
listener. The full goal stays active. Next integration must own bounded control
reader/writer and operation tasks, preserve partially read frames across other
stream events, apply control-frame deadlines after its first byte rather than
timing out healthy long result transfers between frames, and supervise runtime
faults. Shutdown must account separately for connection metadata/file jobs; the
runtime's thread-completion flag alone is insufficient. The public listener,
client/journals, independent Java, neutral failures and original external workload
plus equivalent streaming-gRPC/resource comparison remain required.

Final verification: full repository conformance exited zero with 646 Rust tests,
6 external Rust example tests and 193 Java tests in 20 fresh XML reports; all
three bounded models, frozen vectors/CDDL, native/C++ checks, nine V1 interop
pairs and 32 capability probes passed. After refining the explicit read-pin test
and its nonblocking snapshot observations, the entire Rust workspace and strict
clippy passed again. The draft rebuild exited zero; the rendered runtime status
was inspected, with zero idnits errors/flaws/warnings and the existing FIPS comment.

### Public Rust durable listener, 2026-09-07

The preceding runtime checkpoint was progress (`0cffeb1`, published to Forgejo).
The next implementation now connects negotiation, authenticated authority control,
input/result streams, shared flow reservation and runtime supervision through
`v2_authority::server::Server`. Standalone commands remain V1. The listener uses
bounded persistent frame reading/writing, ordered submissions and fixed operation
ceilings. Shutdown distinguishes owned child-future destruction, native runtime
threads, metadata commits, file cleanup and transport; expiry never releases a
running callback's pins or reports unfinished local work as drained.

Actual wire tests found and reproduced lost responses on client half-close in
both the new durable listener and the existing Core listener. Immediate QUIC
close discarded queued bytes. Both now finish their control send direction and
wait for its acknowledgment under a deadline; Section 12.8 explicitly separates
that transport observation from application validation or durable recovery.
Tests also cover real output, exact root cuts, stalled-data independence, partial
control frames, the full 30-second work wait, invalid input/control, optional Core
fallback, rotated credentials, exclusive root reopen, and a metadata commit still
running beyond shutdown grace. No storage format or wire/CDDL change was required.

The full goal stays active. Required next work remains the usable V2 client/CLI
and uncertainty journals, independent Java V2, the neutral cross-language
process-failure driver, and the original external workload with equivalent
streaming-gRPC authentication/durability plus raw resource/cost evidence. These
Rust listener tests share the Rust wire codec; they do not satisfy the neutral
oracle, independent implementation or external-usefulness gates.

Final verification: the complete Rust workspace passed 664 tests, formatting
and strict clippy on the final source. The preceding repository-wide regression
passed 662 Rust tests before the final two listener regressions, six external
Rust example tests, 193 Java tests in 20 fresh XML reports, all bounded models,
frozen vectors/CDDL, native/C++ checks, nine V1 interop pairs, 32 raw capability
probes and all examples. No non-Rust implementation or model/vector source
changed afterward. The rebuilt draft was inspected and reports zero idnits
errors/flaws/warnings with its existing FIPS comment. Evidence:
`conformance/results/durable-work-v2-server-2026-09-07.txt`.

### Client creation and immutable-operation journal, 2026-09-07

The preceding goal turn was progress: authenticated durable listener `a0929e7`
was tested, committed and published to Forgejo. The next client component stores
creation policy/profile choice before transmission and immutable operation IDs/
parameters before sending each mutation. Real SQLite commits, checksummed records,
bounded inventory and physical file caps preserve replayable uncertainty through
reopen, local write failure and forced process termination. Receipt acceptance
checks its digest and typed outcome against retained intent; it cannot substitute
for TLS authentication, full scope coverage or validated output references.

Thirteen substantive storage tests and one subprocess entry point pass. Removing
the intent commit deliberately makes crash recovery fail; the commit is restored.
Two real-QUIC tests reopen after replies were not locally recorded, replay the
same creation/declaration and recover admission without a replacement attempt.
Focused checks and strict clippy pass. The wire, CDDL, authority storage format
and dependency versions do not change; the client journal has its own format.
Review also reproduced and fixed an incompatible reopen changing SQLite journal
mode before refusing the file.

The full goal remains active. Next: the production V2 client event loop/CLI with
bounded asynchronous journal ownership, durable work/coverage observations and
retained authenticated result references; independent complete Java V2; neutral
cross-language failure/resource gates; and the original external workload with
equivalent streaming-gRPC semantics and raw cost/failure evidence. The storage
component and same-language tests are not a substitute for those deliverables.

Final verification: 680 Rust workspace tests, strict clippy and formatting pass
on the final source. Full repository conformance exited zero with 679 Rust tests
before the final reopen regression, six external Rust-example tests, 193 Java
tests in 20 fresh XML reports, all bounded models, frozen vectors/CDDL, native/
C++ checks, nine V1 pairs, 32 capability probes and all examples. No non-Rust
source changed afterward. The draft rebuild and rendered status inspection pass;
idnits reports zero errors/flaws/warnings and its existing FIPS comment. Evidence:
`conformance/results/durable-work-v2-client-journal-2026-09-07.txt`.

### Client work observations and retained result selections, 2026-09-07

The Rust journal now persists revisioned work observations, full immutable
manifests and explicit output-index selections. Known admission, retry and
cancel/skip receipts constrain those records in either arrival order. An older
compatible reply cannot replace a newer view; a terminal/fence contradiction
refuses instead of inventing an execution transition. Manifest retention does
not grant fresh output availability or rerun processing.

Fifteen added storage scenarios include a reproduced contradictory-receipt bug,
forced process termination after observation/reference commit, empty-output and
inputless-cancellation cases, corrupt indexes/images, independent quotas, atomic
rollback and actual large-manifest physical exhaustion. The existing actual-QUIC
recovery test now reopens the saved manifest/selection, authenticates with rotated
owner credentials and reads original attempt-1 bytes with unchanged terminal
revision. These tests still share the Rust codec, not an independent oracle.

Client local format 2 explicitly refuses older history without conversion or
deletion. Authority storage, normative wire/CDDL and dependencies are unchanged.
The draft implementation-status section and overview now distinguish the public
Rust listener from the unfinished client/CLI integration and Java V2 work.
Evidence: `conformance/results/durable-work-v2-client-observations-2026-09-07.txt`.

Final full-suite handle 63558 exited zero: 695 Rust workspace tests, strict
clippy/formatting, 193 Java tests in 20 fresh reports, six external Rust example
tests, frozen vectors/CDDL, bounded models, native checks, nine V1 interop pairs,
32 capability probes and all examples. Draft build handle 36750 exited zero;
the rendered Appendix D was inspected. idnits reports zero errors/flaws/warnings
and the existing FIPS comment. No submission or deployment occurred.

The full objective is unchanged. Next: durable scope membership/status coverage
and bounded asynchronous client transport/journal ownership, followed by the
independent Java implementation and neutral failure/resource driver. The external
workload and equivalent streaming-gRPC comparison remain mandatory, not deferred
out of the goal. This checkpoint does not establish complete V2 conformance.

### Client sealed membership, parent consistency and coverage, 2026-09-07

The Rust journal now merges bounded scope pages, verifies the full membership
seal and commits bottom-up closure only after terminal work, child coverage,
counts, status roots and closure times agree. It preserves the exact root
summary across restart and constructs DRAIN from that saved cut, while leaving
connection draining and authenticated response validation to the transport.

The new tests exposed parent/child contradictions accepted in two paths: a later
parent page and a later sealing receipt verifying cached membership. Both now
revalidate previously retained child relationships before committing new evidence.
The normative Section 12 clarification explicitly allows child-first observations,
requires both-direction consistency checks and rejects contradictions with
INTEGRITY_ERROR. Missing parent evidence stays pending; no parent admission or
membership is fabricated. This changes neither the wire layout nor response
delivery ordering.

Eighteen new substantive core scenarios cover empty/300-member scopes, overlapping
pages across reopen, valid child-first metadata, wrong parent/child allocations,
late sealing receipts, missing descendants, STRICT failure, count/hash/time
contradictions, quotas, atomic rollback, corruption and forced process termination
after coverage commit. The actual-QUIC recovery test now saves SCOPE page/checkpoint
evidence, exclusively reopens, reconnects with rotated configured credentials and
completes DRAIN with the original root summary. It still shares the Rust codec.

Client local format 3 explicitly refuses older history without conversion or
deletion. The extra tables raise the measured empty database floor on the pinned
build to 73,728 bytes (WAL 0). The actual physical-exhaustion fixtures now use
128 KiB database/WAL/journal and 64 KiB SHM limits and still prove refusal and
preservation. The full-local-evidence coverage policy adds reads and storage;
it is not a universal wire requirement to fetch all WORK views. Blocking scans
and file/count caps do not establish measured heap/RSS/latency guarantees.
Evidence: `conformance/results/durable-work-v2-client-scopes-2026-09-07.txt`.

The full objective remains unchanged. Next is bounded asynchronous client
transport/journal ownership and complete client/CLI integration, followed by
independent Java V2 and the neutral cross-language failure/resource driver.
The original external chunk/distribute/transform/reassemble workload and equivalent
streaming-gRPC comparison, including pinned raw cost/failure evidence, remain
mandatory. This is an implementation checkpoint, not goal completion.

Verification: final Rust handle 34274 exited zero with 713 workspace tests,
47 focused client-journal entries, strict clippy and formatting. Full repository
suite handle 45780 exited zero with 712 Rust tests before the last test-only
addition, 193 Java tests in 20 fresh XML reports, six external Rust example tests,
frozen vectors/CDDL, bounded models and negative controls, native checks, nine V1
interop pairs, 32 capability probes and all examples. No non-Rust implementation
changed afterward. Draft build handle 64110 exited zero; rendered normative text
and Appendix D were inspected. idnits has zero errors/flaws/warnings and its
existing FIPS comment. No submission, merge to main or deployment occurred.

### Bounded asynchronous client journal ownership, 2026-09-07

`pipestream_quic::v2_client::journal::Journal` now supplies async access to the
complete core journal API. One worker owns opening/auditing, every storage call
and final store destruction. Its 1..=32 operation ceiling (default 16) includes
queued/running calls and replies not yet consumed or discarded. Cancelled waiters
do not cancel accepted commits or release their capacity early. Clones share
close; last-handle drop drains accepted operations. Shutdown confirms actual
operation/store-owner cleanup, not remote work completion or network DRAIN.

A stable empty `.client-lock` sidecar prevents another cooperating async owner
from bypassing the first worker's limits, including across processes. The directory
must remain private; do not unlink the lock or open the raw core journal alongside
the worker. Worker failure does not erase committed intent. Construction failure
waits for ownership cleanup before reporting its result. The core's existing
typed mutation checks now also run before cloning request/header parameters.
Wire/CDDL, client format 3, authority storage and dependency versions are unchanged.

Nine substantive worker tests and one subprocess entry point cover cancellation,
queue/unread-reply capacity, single-thread async progress, shared/last-handle close,
panic, invalid history, sidecar safety and forced process death after a real
commit. Both real-QUIC recovery tests now use the async owner for original
identity/receipt, work/reference and root-coverage persistence across reconnect.
An isolated process opens 64 real owners, refuses the 65th without creating its
files, then admits a replacement after shutdown. The empty main databases total
4,718,592 bytes. This is ownership/file-length evidence, not measured heap/RSS
or comparative workload throughput.

Final Rust handle 58271 exited zero: 724 workspace tests, focused worker/resource
gates, strict clippy and formatting. Full suite handle 72643 exited zero with
723 Rust tests before the final startup cleanup refinement and resource test,
193 Java tests in 20 fresh reports, six external Rust tests, vectors/CDDL/models,
native checks, nine V1 pairs, 32 capability probes and all examples. No non-Rust
implementation changed afterward. Final draft build handle 90630 exited zero;
rendered Appendix D was inspected, with zero idnits errors/flaws/warnings and the
existing FIPS comment. Evidence:
`conformance/results/durable-work-v2-async-client-journal-2026-09-07.txt`.

The original full objective remains active. Next: the production V2 connection
multiplexer, integrated incremental input/result file transport and client CLI;
independent complete Java V2; the neutral cross-language failure/resource driver;
and the original external workload/equivalent streaming-gRPC comparison with
pinned raw cost/failure evidence. The tests still manually drive the network;
the async journal is not a substitute for those remaining deliverables.

### Bounded V2 client wire transport, 2026-09-07

`v2_client::transport::Transport` now owns authenticated QUIC negotiation,
monotonic request allocation, independent control reading/writing, incremental
input sending and incremental result receiving. Accepted/cancelled request
waiters retain correlation until their actual reply or bounded connection failure.
Input stream allocation is serialized without blocking ordinary controls; local
admission limits are checked before allocating an uncorrelated stream. A dropped
input does not make a later legitimate receipt unsolicited. Output chunks stay
explicitly unverified until full length, SHA-256 and FIN are validated. Independent
stream tasks enforce deadlines even when an application stops polling.

Connection/task/queue ownership is bounded. A process has at most 64 client
connection owners, including cancelled negotiations and QUIC draining. Request
tickets include queued/unresolved and unconsumed internal replies; object queues
have one 8 KiB chunk and one in-flight chunk. Finished task records are reaped
before replacement admission. No object streams receive credit before selection,
and Core-only selection does not grant result-stream credit. These structural
gates do not establish measured whole-process heap/RSS or workload performance.

Twelve wire tests exercise the public client against the real durable listener
or a deliberately adversarial authenticated peer. The real-server case persists
intent and receipts, transfers 256 KiB, replays an admission header without the
body, reopens the journal with rotated credentials, retrieves retained output,
verifies sealed coverage and obtains an exact root-completion response. Negative
cases cover reordering/cancellation, pending/admission ceilings, wrong-direction
control, invalid selection, wrong commitments, corrupt/truncated/extra bytes and
unpolled consumers. A separate process opens 64 real Core connections, refuses
the 65th and admits a replacement after actual draining with old handles held.

The result-header negative test found an error-scope gap. Section 12 now explicitly
distinguishes a recognizable wrong object commitment (delivery-local
INTEGRITY_ERROR) from invalid correlation (fatal FRAME_ERROR). Core correlation
and the new client enforce that distinction; no wire/CDDL/storage version changes.

Verification: final Rust handle 16346 exited zero with 737 workspace tests,
12 focused wire tests, strict clippy and formatting. Full suite handle 53177
exited zero with 737 Rust tests before the final initial-object-credit adjustment,
193 Java tests in 20 fresh XML reports, six external Rust tests, vectors/models,
native tests, nine V1 pairs, 32 capability probes and examples. Final Rust tests
cover the adjustment; non-Rust implementation sources did not change. Draft
handle 62349 exited zero; rendered Section 12.2/Appendix D inspected; idnits has
zero errors/flaws/warnings and the existing FIPS comment. Evidence:
`conformance/results/durable-work-v2-client-transport-2026-09-07.txt`.

The full original objective remains active. Next is the durable session facade
that automatically composes the journal and transport, including persistence of
receipts after caller cancellation and independently owned input-response waits,
then CLI/file adapters. The current transport is intentionally the low-level
network half, not a facade that automatically persists/authenticates every caller
observation. Full independent Java V2, the neutral cross-language failure/resource
driver, and the original external workload/equivalent streaming-gRPC comparison
with pinned raw cost/failure evidence remain required. No submission, deployment
or merge to main occurred.

### Durable V2 session client integration, 2026-09-07

The public Rust `v2_client::session::Client` now composes the asynchronous journal
and authenticated transport. Original creation/operation intent is persisted
before transmission; binding, receipts, work views, manifests, explicit output
selections and scope coverage are validated and saved before successful return.
Owned collectors outlive cancelled connect/mutation/upload waiters. The input
writer and admission response have separate owners, so dropping an upload handle
does not discard a legitimate late receipt. Refusals and transport/storage errors
leave the original intent available for explicit recovery, never automatic new
operation/attempt allocation.

Only one facade may claim a journal owner. Client operation/reply tickets bound
queued and running work, including abandoned waiters; disk calls serialize without
holding the network reader. Completion and detach first drain accepted facade
operations, then obtain the authority's actual connection cut. A failed cut
reopens acceptance; a successful cut closes it. Shutdown waits for owned collectors,
then transport and journal cleanup. These are structural ownership/count bounds,
not whole-process resource measurements.

Nine actual authenticated-server scenarios cover a 256 KiB persisted round trip,
credential rotation/reopen, exact root completion, cancellation at creation and
mutation/admission commits, missing/changed commitments, identity mismatch,
authorization refusal/retry and exclusive ownership. A development assertion
confused WORK wait expiry with checkpoint WAIT_TIMEOUT; checking Section 12
confirmed WORK returns the unchanged view. The test was corrected without changing
the contract. No normative wire, CDDL, storage format or dependency change was
needed for this integration.

Verification: final Rust handle 24983 exited zero with 9 focused tests, strict
clippy and 746 workspace tests. Full suite handle 36763 exited zero with the same
Rust tests, 193 Java tests in 20 fresh XML reports, six external Rust tests,
vectors/models, native checks, nine V1 pairs, 32 capability probes and examples.
Draft handle 63449 exited zero; rendered Appendix D TXT/HTML inspected; idnits
has zero errors/flaws/warnings and the existing FIPS comment. Evidence:
`conformance/results/durable-work-v2-session-client-2026-09-07.txt`.

The full objective remains active. Standalone V2 CLI/file adapters, complete
independent Java V2, the neutral cross-language crash/failure/resource driver,
and the original external workload/equivalent authenticated, durable streaming-
gRPC baseline with pinned raw measurements remain required. The existing
cross-language suite is V1 evidence, not proof of Java V2 interoperability.

### Owned client file adapters and refusal preservation, 2026-09-07

Rust `session::files::FileInput` now opens and prehashes a bounded regular file,
keeps its descriptor and streams an exact matching original admission intent
through the durable client. It refuses non-regular inputs and final-component
symlinks; a nonblocking open prevents a FIFO from waiting for a writer. Length
and digest are checked again before FIN. `Output::save_to` stages bounded chunks
until verified FIN, synchronizes the file, installs without overwriting a local
destination and synchronizes its directory. The low-level output also exposes
this adapter without claiming automatic journal persistence.

File operations use four shared workers and 64 process-wide owner slots. The
existing deferred-destructor/file-worker implementation is reused; cancelled
send/save waiters retain their owned transfer and cleanup. `FileInput::close`
waits for descriptor cleanup. Caller-supplied per-file byte ceilings do not claim
a shared retained-directory quota or measured total process memory.

Thirteen new tests include empty/256 KiB real-server transfers and replay, changed
source bytes, pre-submission intent mismatch, cancellation, no-overwrite, byte
limits, non-regular files, independent descriptor lifetime gates and adversarial
corrupt/truncated/overlong results. A withheld-FIN test proves cancellation of
the file-save waiter does not publish the prefix. Ordinary abort removes only
its own temporary file, not unrelated staging.

The early-refusal regression failed first: an actual UNAUTHORIZED admission
refusal was hidden by the secondary local "transport writer stopped" error.
The adapter now preserves the correlated authority outcome when the writer is
stopped. A valid original replay receipt still takes precedence over a redundant
upload abort; locally detected commitment errors remain visible. No normative
wire, CDDL, storage-format or dependency change was needed.

Verification: final Rust handle 26733 exited zero with the refusal regression,
strict clippy and 759 workspace tests. Full suite handle 87425 exited zero; its
Rust phase preceded the final refusal correction (758 tests), which the final
Rust run covers. Non-Rust source was unchanged. The suite also passed 193 Java
tests in 20 fresh XML reports, six external Rust tests, vectors/models, native
checks, nine V1 pairs, 32 capability probes and examples. Draft handle 3251 exited
zero; rendered Appendix D TXT/HTML inspected; idnits has zero errors/flaws/warnings
and the existing FIPS comment. Evidence:
`conformance/results/durable-work-v2-client-files-2026-09-07.txt`.

The full original objective remains active. Next is standalone V2 CLI integration
with explicit file-staging ownership, shared disk budgeting and bounded restart
reconciliation. These file adapters require trusted, stable local directories;
process death may leave identifiable staging files and does not authorize blind
directory cleanup. Complete independent Java V2, neutral cross-language crash/
failure/resource testing and the original external workload/equivalent
authenticated, durable streaming-gRPC comparison with pinned raw measurements
remain required. No main merge, deployment or draft submission occurred.

### Runnable V2 authority and durable client commands, 2026-09-07

The preceding implementation turn made progress with the uncommitted V2 command
surface. This checkpoint verifies and documents it and adds application-boundary
coverage. It does not shrink or complete the original goal.

The Unix Rust executable now supports explicit authority initialization and reopen,
mutual-TLS serving, owner creation-sequence lookup, separate client initialization,
original-operation replay/lookup, observations, manifest selection and verified file
retrieval. Retry, cancellation, skip, scope cancellation, exact root completion,
detach and offline operator revocation use the existing durable APIs. Startup
requires bounded regular credential/configuration files, mapped principals and an
explicit system-UTC trust choice. No missing history is silently initialized.
SIGTERM/SIGINT require an actual drained shutdown report; they do not claim durable
work completion.

The registry provides explicit pure consume/copy, caller-produced reassembly,
authority-produced chunking and a retryable exercise application. Unknown contracts
or modes do not fall back. Reassembly verifies actual retained child outputs
against the parent input. Chunking uses fixed 64 KiB chunks, at most 256, and
replays original child identities after capacity yields. The admitted execution
duration is exposed to expansion without changing the parent's fixed deadline.
These are reference applications, not the original external comparative workload.

Three configuration unit tests and six actual subprocess tests cover both profile
combinations, original replay after an admission-committed server crash, rotated/
changed principals, missing input with successful receipt lookup, both branch
modes, authorized retry/skip, cancellation, revocation and exact completion.
The additional boundary case verifies empty, exact/partial 64 KiB and 33-child
inputs with byte-exact reassembly under the 16-active-job session ceiling. The
crash is not a precisely instrumented publication-boundary fault; these tests
share the Rust implementation rather than an independent acceptance oracle.

Final Rust verification handle 73856 exited 0 with strict clippy and 768 tests.
Full conformance handle 30945 exited 0: its earlier Rust phase had 767 tests,
superseded by that final run. All 193 Java tests in 20 fresh XML reports, native
checks, vectors/models, six external Rust tests, nine V1 pairings, 32 capability
probes and examples passed. Draft handle 41346 exited 0; final Appendix D TXT/HTML
inspected, idnits zero errors/flaws/warnings and the existing FIPS comment.
Evidence: `conformance/results/durable-work-v2-cli-2026-09-07.txt`.
Usage and limits: `implementations/rust-quinn/docs/v2-cli.md`.

Tokio's signal feature adds signal-hook-registry 1.4.8 to the lockfile; other
dependencies reuse already-pinned versions. No wire/CDDL/database-format change.
Forgejo was pulled ff-only before publication, with no incoming branch changes.

The full goal remains active. Next is exclusive client result-root ownership,
shared disk budgeting and bounded restart reconciliation, followed by complete
independent Java V2 and the neutral cross-language failure/resource driver. The
external chunk/distribute/transform/reassemble workload and equivalent authenticated,
durable streaming-gRPC baseline with pinned raw measurements remain mandatory.
The current file adapter requires trusted, stable local directories; process death
can leave staging, and a prefix is not permission to delete another transfer's
files. No main merge, deployment or draft submission occurred.

### Managed local result storage and recovery, 2026-09-07

The preceding turn was progress: `5b4bd36` was verified and published to Forgejo
and GitHub. This checkpoint adds a real managed local-copy library; it does not
complete the original goal or change its independent Java/failure/workload gates.

Core `v2::client::results::ResultStore` reuses the immutable payload store's
exclusive process lock, complete bounded inventory audit, reservation-before-write,
verified installation and crash-left staging cleanup. A purpose-qualified trusted
authority/owner binding permits multiple sessions of that owner, without serving as
a credential. Full selected content commitments come from the durable journal;
local copies cannot replace work/attempt evidence or renew remote retention.
Shared object/byte/handle quotas cover all downloads in the root. Explicit local
removal refuses live pins and releases capacity only after directory sync.

Async `session::files::managed::ManagedResults` runs opens, I/O, deletion and
destruction on the existing four-worker/64-owner file pool. Its download owns the
durable client's exact saved selection and negotiated limits through verified FIN
and installation. Explicit local reads verify bytes again at EOF; a stopped server
does not turn those already possessed bytes into a fresh authorization grant.
Repeated downloads are separately charged copies, with no automatic eviction.
No dependency, wire encoding or journal/database-format change was needed.

Six substantive core tests and a subprocess entry point cover empty/nonempty
reopen, byte/object/handle ceilings, pinning, identity/policy mismatch, corruption,
unknown-file preservation and process death before finish, after durable finish
and after unlink. Two async tests cover clone/root ownership and a real authenticated
256 KiB download, quota rejection, unchanged terminal revision, local retrieval
after server shutdown, exact bytes and removal. These are implementation tests,
not independent cross-language acceptance or actual machine power-loss evidence.

Final review added a negative-first injected directory-sync regression. Before the
fix, a failed installation could still be looked up through the live root without
auditing its uncertain durability. The shared object store now quarantines reads
and capacity decisions after any directory-sync failure, retaining the bytes until
exclusive reopen audits and synchronizes the namespace. No wire change was needed.

Section 12.7 now explicitly requires clients to distinguish local copies from
newly authorized transfers; expiry/revocation cannot recall delivered bytes.
The acceptance ledger tracks this rule. Appendix D and both READMEs distinguish
the managed library from unfinished CLI/export integration and the full goal.

Verification: focused core handle 82989 and async/clippy handle 3060 exited zero.
Full suite handle 39379 exited zero with 776 Rust workspace tests, 193 Java tests
in 20 fresh XML reports, native checks, vectors/models, six external Rust tests,
nine V1 pairings, 32 capability probes and examples. It preceded the final sync
correction: final Rust handle 69846 exited zero with its formerly failing regression,
strict clippy and all 777 workspace tests. Non-Rust/V1 source was unchanged.
Final draft handle 29897 exited zero; Section 12.7/Appendix D TXT and Appendix D HTML inspected; idnits zero
errors/flaws/warnings with the existing FIPS comment. Evidence:
`conformance/results/durable-work-v2-managed-results-2026-09-07.txt`.

Next is wiring managed copies into CLI recovery/export with explicit initialization,
without adopting or deleting old arbitrary-path staging. Full independent Java V2,
the neutral cross-language failure/resource driver and the original external
chunk/distribute/transform/reassemble workload plus equivalent authenticated,
durable streaming-gRPC baseline with pinned raw measurements remain mandatory.
Arbitrary-path file exports still require trusted stable directories and can leave
temporary files after process death. This is not a measured total heap/RSS claim.
Forgejo was pulled ff-only with no incoming changes; no main merge, deployment or
draft submission occurred. The goal remains active.

### Managed CLI downloads and raw export recovery, 2026-09-07

The preceding implementation turn was progress: `3b62f78` was published, with
unfinished export code retained in the worktree. This checkpoint finishes that
code and connects the managed libraries to actual CLI commands. It is not a
replacement for the original complete Rust/Java/failure/workload objective.

Explicit initialization binds each private local root to trusted authority/owner
and immutable quotas. Authenticated `client download` uses the saved selection;
offline `local verify`/`local export` opens original journal evidence without an
endpoint or remote fallback. Raw exports commit stable local IDs and full
manifest/index identity before copying. Pending reservations survive process death,
reopen audits before staging cleanup, exact replay verifies existing bytes, and
changed identity cannot overwrite them. Explicit local cleanup does not change
source copies, journal evidence or authority outcomes. The old arbitrary-path
adapter keeps its documented limits; its temporary files are not adopted/deleted.

Six substantive core tests plus a crash entry exercise five exit points, quotas,
corruption, partially consumed readers and sync failures. A deterministic held-worker
case verifies completion after waiter cancellation. Two new actual CLI tests cover
offline retrieval/export, missing roots, unchanged remote state, changed owner/policy
and same-byte/different-work conflict. They exposed a real argument-group collision,
now fixed and protected by a full command-tree check. The isolated 32 MiB local gate
measured 4266 bytes additional Rust heap, largest allocation 1952 bytes, observed
RSS/HWM 7160 KiB, and exact named export bytes. Extra copy/hash I/O and the limits of
named-file quotas versus external readers/allocated blocks are documented.

Full suite handle 14007 exited zero (787 Rust tests in that phase, 193 Java tests
in 20 fresh reports, native guards, frozen vectors/models, six external Rust tests,
nine V1 pairings, 32 probes and examples). Final Rust handle 47787 exited zero with
strict clippy and 788 tests, including the command-tree check; final fmt/clippy and
focused check handle 11247 also exited zero. Draft handle 74049 exited zero;
Appendix D TXT/HTML inspected, idnits zero errors/flaws/warnings and the existing
FIPS comment. Evidence: `conformance/results/durable-work-v2-managed-exports-2026-09-07.txt`.
No wire, dependency or journal/database-format change. Forgejo pull was ff-only
and already current; no main merge, deployment or draft submission.

Next is the complete independent Java V2 implementation, starting with its own
typed framing/records against the frozen vectors, then authenticated durable
execution, recovery and actual outputs. This does not reduce that requirement to
a codec or a profile subset. The protocol-neutral Rust failure driver, both-language
outcome/refusal/resource evidence, and the external chunk/distribute/transform/
reassemble workload with reconnect/worker failure plus equivalent authenticated,
durable streaming-gRPC baseline and pinned raw measurements remain mandatory.
The full goal remains active.

### Independent Java V2 wire and commitments, 2026-09-07

The pending Java codec work is now a typed library in `ai.pipestream.quic.v2`.
It implements every Appendix F message/record schema, deterministic CBOR,
incremental control framing, profile-selection checks and all domain-separated
commitments independently of Rust. The 70 frozen wire cases and 12 typed-input
hashes pass. Structural checks now also reject WAITING_CHILDREN without a child.
The exact-count scope seal streams IDs, and the status fold validates identity,
ordering, terminal views and child-root presence with at most 63 subtree hashes.
An independent full-level tree reduction agrees for every size 0 through 1025.

A fresh JVM folded 4,000,003 members under a 24 MiB heap cap with at most 672
retained hash bytes. Primitive membership IDs alone would exceed its heap. Total
RSS/HWM was 385152 KiB in that first run, separately recorded: this is not a
low-RSS claim or a network/endpoint gate. Standard Java verification passed 278
tests in 22 fresh reports, excluding the separate V1 interop profile. New V2
Javadoc passes strict doclint; the whole project's older V1 missing-doc warnings
remain. The draft rebuilt cleanly and its implementation-status paragraph was
inspected in generated TXT/HTML. Evidence and source hashes are in
`conformance/results/durable-work-v2-java-wire-2026-09-07.txt`.

Next remains the complete independent Java implementation: bounded correlation
and timed incremental objects, authenticated Netty V2 endpoints, transactional
admission/execution/fencing/results/retention and durable client recovery with
parent/child validation in both observation orders. No V2 profile is advertised
from this codec. Both-language failure/resource evidence, the protocol-neutral
Rust driver, and the original external workload plus equivalent authenticated,
durable streaming-gRPC baseline remain mandatory. This is progress, not Task 2
or full-goal completion.

Full reference suite handle 84214 then exited zero: 788 Rust workspace tests,
287 Java tests in 23 fresh XML reports including V1 interop, native guards,
frozen vectors and bounded models, six external Rust tests, nine V1 client/server
pairs, 32 capability probes and three examples. These network gates remain V1;
they do not establish Java V2 interoperability. No implementation changes followed
that full run. Forgejo was already current on ff-only pull; no main merge,
deployment or draft submission is part of this checkpoint.

### Java V2 correlation and incremental objects, 2026-09-07

The prior goal turn was verified progress: `8a0ae61` is on Forgejo and GitHub.
The Java V2 library now adds bounded client correlation and incremental object
validation. It checks negotiation, all request/response families, actual input
stream tags, out-of-order replies, pending capacity and separate active-result
permits. A mismatched retained object is a delivery-local integrity failure;
unknown, wrong-kind or duplicate response correlation invalidates the connection.
Unresolved requests remain available for uncertainty recovery on close.

Header parsing leaves payload bytes for authorization/reservation. Payload hashing
requires exact length, digest and actual FIN, with independent monotonic idle and
lifetime checks. Reaching either deadline cannot be repaired by late progress.
The test suite covers every response-family mismatch, all header split points,
wrong object fields, permit ownership after rejection, reset, malformed FIN,
deadline equality and clock wrap. A 64 MiB object verified through an 8 KiB buffer
under a 24 MiB heap cap; first measured RSS/HWM was 135844 KiB, not a low-total-memory
claim. These are 18 new local tests, not Java endpoint interoperability.

Next is actual Java V2 Netty integration with server identity, mutual TLS and
stable principal mapping, bounded connection/control/stream ownership and live
deadline scheduling; then transactional admission/execution/fences/results,
retention/retirement and durable client recovery including parent/child evidence
in both arrival orders. Control correlation alone does not verify or persist
an admission receipt. No profile is advertised by these helpers. All original
both-language failure/resource testing and external workload/equivalent
authenticated durable streaming-gRPC deliverables remain mandatory. The full
goal is active and incomplete. Evidence is in
`conformance/results/durable-work-v2-java-streams-2026-09-07.txt`.

Full reference suite handle 28864 exited zero: 788 Rust workspace tests,
305 Java tests in 26 fresh XML reports, native checks, frozen vectors/bounded
models, six external Rust tests, nine V1 black-box pairs, 32 capability probes
and all three examples. Final V2 doclint and formatting passed. Draft handle
41637 exited zero; the new Appendix D paragraph was inspected in rendered
TXT/HTML, with zero idnits errors/flaws/warnings and the existing FIPS comment.
No implementation changes followed the full suite. Forgejo ff-only pull was
already current; this checkpoint does not merge main, deploy or submit a draft.

### Java V2 actual QUIC/TLS authentication, 2026-09-07

The preceding implementation checkpoint was verified progress: `17e5a58` is
published. This turn adds the independent Java TLS boundary, not a replacement
for the full Rust/Java/failure/workload objective. Netty now has a V2 guard with
explicit CA trust, handshake service-name verification, optional validated caller
certificates, full-DER stable mapping, current owner checks and no application
0-RTT. The built-in client uses full handshakes. External resumed clients revalidate
their actual retained certificate chain and current mapping before application
activation. Missing/unmapped callers cannot activate required durable/results.

The implementation accounts for Netty's channel-active notification preceding
handshake-complete. Tests exposed reentrant closure suppressing peer TLS errors
and never-active connections leaving readiness unresolved; both paths are fixed.
Nine new actual-QUIC scenarios cover rotation, same-key certificate reissuance,
bad trust/usage/time, inconsistent local keys, DNS/IP/wildcard/CN-only identity,
ALPN mismatch, required/optional profile policy, live expiry/remapping, actual
resumption and configuration limits. Negative readiness tests require an actual
exceptional completion, not a timeout. A test-only retained engine observation
keeps resumption evidence available after native connection cleanup.

Full reference handle 1782 exited zero: 788 Rust workspace tests, 314 Java tests
in 27 fresh reports, native guards/C++ checks, frozen vectors and bounded models,
six external Rust tests, all nine V1 language pairings, 32 probes and the existing
recursive/recovery/examples. After the test-only observer correction, final full
Java handle 87216 exited zero with 314 tests in 27 fresh reports and no failures,
errors or skips; source hashes remain unchanged. Strict doclint passed for the
entire V2 package plus shared SAN helper; unrelated older V1 private-field warnings
remain outside that explicit source scope. Formatting and whitespace checks pass.

Section 12 now explicitly forbids protocol processing before resumed credential
revalidation; this changes no wire fields. Final draft handle 83060 exited zero,
with Section 12.3 and Appendix D inspected in generated TXT/HTML and idnits zero
errors/flaws/warnings plus the existing FIPS comment. Evidence:
`conformance/results/durable-work-v2-java-tls-2026-09-07.txt`.

Next is the actual bounded Java V2 connection/control/object owner: stream-zero
rules, complete Core client/server behavior, control-credit reservation and
independent live deadline scheduling. Then complete transactional admission,
execution/fences, results/retention/retirement and durable client recovery,
including parent/child evidence in both arrival orders. The guard is not a
durable authorization store and the test dispatcher is not a shipped endpoint.
Native handshake memory, total RSS and full endpoint resource bounds are not
proven by configuration ceilings. Neither V2 durable profile is advertised by
the shipped Java CLI yet. The independent Rust failure driver, both-language
outcome/refusal/resource evidence and the original external workload with
reconnect/worker failure plus equivalent authenticated durable streaming-gRPC
baseline remain mandatory. The full goal remains active and incomplete.
Forgejo was current on ff-only pull; no main merge, deployment or draft submission.

### Java V2 Core listener and live resource deadlines, 2026-09-07

The preceding implementation turn was progress: it left the actual Core listener
and a failing global-admission network test. This checkpoint finishes that listener
without replacing the full independent Java contract with a Core-only target.
`CoreServer` now integrates verified TLS, Stream 0, capability minima, all valid
profile-dependent refusals, strict request IDs, detach/half-close and independent
handshake/control/queued-write/detach deadlines. It does not advertise either
durable profile or fabricate work state. Native write completion is not a peer
ACK; the server leaves graceful parent close to the client after sending FIN.

The original overload test found that closing inside Netty's initializer preceded
Initial-packet processing and suppressed the peer's refusal. A synchronous packet
boundary now emits transport CONNECTION_REFUSED using at most one additional
packet-local transport, with no rejected-handshake queue. Admission capacity is
released after native close; global limits include incomplete handshakes and
per-owner limits include a shared anonymous bucket. Application write counts/bytes
and aggregate configured buffer allowances are bounded separately from native
memory. The exact bundled quiche commit and its distinct send-capacity accounting
are recorded for the forthcoming data/control flow-control work.

Full-suite testing also corrected two invalid observation assumptions: TLS can
finish before the overload close arrives, and repeated Initial packets can create
more refusal attempts than distinct clients. Tests now require each peer's actual
transport refusal and counter increase, while checking exact active/owner counts,
bounded transport high water, preserved existing connections and replacement
admission. Timeouts never count as successful refusals. The 13 scenarios also
cover every profile-dependent request family, malformed controls, gapped/decreasing
IDs, reset/stop, actual FIN with a paused reader, tiny windows, separate byte/count
exhaustion, stalled TLS and deadlines that later requests cannot renew.

Final full reference handle 78781 exited zero: 788 Rust workspace tests, 327 Java
tests in 28 fresh reports with no failures/errors/skips, native guards/C++ checks,
frozen vectors/bounded models, six external Rust tests, nine V1 pairs, 32 probes
and all examples. Final strict V2/shared-helper doclint, formatting and whitespace
checks passed. Draft handle 59469 exited zero; Section 12.1 and Appendix D were
inspected in generated TXT/HTML, with idnits zero errors/flaws/warnings and the
existing FIPS comment. The draft now distinguishes pre-authentication transport
refusal and requires documented handshake/refusal-state accounting. No wire
layout, profile identifier, dependency or persistent-format change. Evidence:
`conformance/results/durable-work-v2-java-core-2026-09-07.txt`.

Next remains complete independent Java V2 client/object transport and durable
admission, execution/fences, publication/results, retention/retirement and recovery
with parent/child validation in both observation orders. Shared data/control credit
and whole-process resource evidence are not proven by Core's no-object tests.
The protocol-neutral Rust failure driver, both-language outcome/refusal/restart/
resource evidence, and the original external chunk/distribute/transform/reassemble
workload with reconnect/worker failure plus equivalent authenticated durable
streaming-gRPC baseline and pinned raw measurements remain mandatory. The full goal
is active and incomplete. Forgejo was current on ff-only pull; no main merge,
deployment or draft submission occurred.

### Java V2 Core client and Rust authority check, 2026-09-07

The previous implementation checkpoint was verified progress, not completion of
the full goal. The Java Core client now owns authenticated selection, bounded
detach, actual peer FIN validation and cleanup. Repeated/cancelled application
waiters cannot invent another request or a successful drain. Process-local client
count and configured buffer admission are held through owned termination.

Fourteen new network tests cover malformed/miscorrelated replies, reset/stop,
gated caller cancellation, incomplete-frame and absolute detach deadlines,
tiny windows and admission exhaustion. The tagged Java-client/Rust-authority
case negotiates Core and drains with anonymous and mapped callers. It does not
claim the reverse direction, Java durable behavior, native memory bounds or the
complete failure driver. Detailed commands, hashes, initial compiler failure,
corrected test oracles and scope limits are recorded in
`conformance/results/durable-work-v2-java-core-client-2026-09-07.txt`.

Full reference handle 43271 exited zero: 788 Rust workspace tests, six external
Rust tests, 341 Java tests in 29 fresh reports with no failures/errors/skips,
native checks, frozen vectors, bounded models, nine existing V1 pairs, 32 probes,
recursive/recovery and all examples. Strict Javadoc, formatting and draft build
passed; rendered Appendix D was inspected. Forgejo was current on ff-only pull.

Next remains complete independent Java object/control credit and durable
admission/execution/fences, publication/results, retention/retirement and client
recovery, including parent/child evidence in both orders. The neutral Rust failure
driver, both-language outcome/refusal/restart/resource evidence and original
external workload/equivalent durable streaming-gRPC comparison remain mandatory.
No goal requirement is waived. The full goal remains active and incomplete.

### Maintained Java QUIC dependency and credit review, 2026-09-07

The previous implementation continuation made progress through the mechanical
Netty migration. This continuation revalidated the dirty checkout and completed
the migration's verification and fixes. Both Java Maven projects now use the
Netty 4.2.17.Final BOM and maintained QUIC artifacts, with one-thread NIO owners.
The native JAR manifest still identifies the same bundled quiche/BoringSSL
revisions as the prior incubator package. No wire, profile or storage format
changed and neither Java V2 durable profile is advertised.

The first full run failed three old certificate-name exception assertions.
Pinned upstream source confirms Netty 4.2 checks hostnames during TLS by default.
The clients keep that earlier check explicitly enabled, as well as PipeStream's
stricter SAN-only guard. Corrected tests require precise certificate failure,
zero decoded controls for invalid names, and valid-name negotiation using the
same certificates. Review also corrected positive test capabilities and
removed an asynchronous STATUS-count assumption before verification. No timeout
or arbitrary exception is accepted as a successful refusal.

Final full suite handle 60811 exited zero: 788 Rust workspace tests plus six
external Rust tests, 341 Java tests in 29 fresh XML reports with zero failures,
errors or skips, native/C++ checks, frozen vectors and bounded models, all nine
existing V1 interop pairs, 32 probes and the recursive/recovery/examples.
Strict V2/shared-helper Javadoc and seven-file V2 formatting checks passed.
Both resolved Java dependency trees use only Netty 4.2.17.Final. The draft built
with zero idnits errors/flaws/warnings and the existing FIPS comment; Section
12.1 and Appendix D were inspected in generated TXT/HTML. Failed-run provenance,
tool-output limitations, scoped metrics and source/artifact hashes are recorded
in `conformance/results/durable-work-v2-java-netty42-2026-09-07.txt`.

The important remaining transport constraint is now explicit in
`docs/standards/java-v2-transport-credit.md`: quiche's initial connection credit
is not its later replenishment window, receive windows autotune, and native
write acceptance is not release of unacknowledged transport data. Section 12.1
now includes those facts in the control-reservation invariant. This is source
analysis and a stronger acceptance boundary, not proof that the Java object
transport exists or satisfies it. The legacy listener README now correctly
labels its values as initial credit instead of fixed windows.

An unmodified, clean Netty release checkout is available at
`/work/reference-code/netty-quic-pipestream`, detached at the verified peeled
tag commit `e0789d32c72f46fd2e7c99b6fdbbf7e2f4409e44`. It is outside the RFC
notes and is not a local Maven override. Its native build deletes its generated
`quicheSourceDir`; that property must not point at a retained reference checkout.

Next close the actual Java transport admission/credit API and object ownership
boundary with repeat/replacement and stalled-data tests, then complete Java
durable admission/execution/fences, publication/results, retention/retirement
and client recovery. The neutral Rust process driver, full both-language failure
and resource evidence, and original external workload/equivalent authenticated
durable streaming-gRPC comparison remain mandatory. No requirement was removed
or replaced by this migration checkpoint. Forgejo was current on ff-only pull;
no main merge, deployment or draft submission occurred. The full goal is active
and incomplete.

### Java native transport-credit foundation, 2026-09-07

The source-pinned quiche/Netty extension now exposes explicit replenishment-window
configuration and saturating retained-send-span accounting. A connection-wide
send limit protects allowance for locally classified control streams, including
automatic queued-write retries. Native transport tests also caught and fixed FIN
overtaking queued payload and local reset leaving write promises pending.

The final exported patches and exact Cargo lock passed a fresh isolated build
from the upstream commit pins, not a retained checkout: handle 41099 exited zero,
with 294 native Java tests in 31 fresh XML reports and no failures, errors or skips.
Artifact manifests and hashes identify both patches and the separately named
native library. Standalone quiche tests passed (934 with native feature flags;
930 plus 43 doc tests in the normal package), with eight added cases rerun after
the final formatting-only changes. Existing upstream clippy/whole-file formatter
failures are explicitly recorded, not suppressed or reported green. Evidence:
`conformance/results/durable-work-v2-java-native-credit-2026-09-07.txt`.

The RFC contains only source patches, lock, notices and a repeatable build command;
all upstream repositories and generated build trees remain under reference-code.
The extension uses its own artifact coordinates and native-library name. The
Java reference POM still uses the official dependency and advertises Core only.
This checkpoint is not a published Maven package, Java durable implementation,
end-to-end control reservation, or measured whole-process bound.

Next integrate the pinned dependency and implement the Java object/control owner
against the five transport acceptance gates, then complete independent Java
durable admission/execution/fences, publication/results, retention/retirement and
client recovery. The neutral Rust failure driver, both-language exact outcome/
refusal/restart/resource evidence, original external workload and equivalent
authenticated durable streaming-gRPC baseline remain mandatory. The full goal
remains active and incomplete; no main merge, deployment or draft submission.

### Java source-pinned transport integration, 2026-09-07

The previous checkpoint was verified progress and is published as `eef343c`.
The Java reference now selects the extension's exact Maven coordinates; the
external Java example resolves the same dependency. The source bootstrap installs
only into a fresh isolated repository, emits that path on stdout only after
verification, and keeps progress on stderr. The full conformance command uses the
same isolated repository for both Java builds. Official/incubator QUIC duplicates
are banned. A runtime test checks actual class/native uniqueness and exact
revision/patch manifests; both packaged executables contain the tested native
library byte for byte, not merely a matching dependency declaration.

Focused handle 81020 exited zero with 40 tests. Full integrated handle 38418
exited zero: 342 Java reference tests in 30 fresh XML reports, 294 native transport
tests in 31 reports, 788 Rust workspace tests plus six external Rust tests,
native/C++ checks, frozen vectors/bounded models, nine V1 pairings, 32 probes and
all three examples. Java/native reports have no failures, errors or skips.
Draft handle 24989 exited zero; Appendix D was inspected in TXT/HTML and idnits
reported zero errors/flaws/warnings with the existing FIPS comment. Strict scoped
Javadoc and whitespace checks pass. Full commands, output contracts, source and
packaged-artifact hashes are in
`conformance/results/durable-work-v2-java-transport-integration-2026-09-07.txt`.

This integrates the dependency, not the still-missing Java object/control owner.
Its next tests must include receive-window replenishment, MAX_STREAM_DATA arriving
before MAX_DATA, stream replacement, blocked send admission, exact incremental
payload/FIN and independent control deadlines. Native admission and Core-only
tests do not prove those end-to-end bounds. Complete independent Java durable
behavior, the neutral Rust failure driver, full both-language failure/resource
evidence and the original workload/equivalent authenticated durable streaming-gRPC
comparison remain required. The full goal remains active and incomplete.

### Java caller-expanded reassembly checkpoint, 2026-09-08

Building on the admitted-job, publication and closure checkpoints, Java now runs
mode 1 parent callbacks against real committed child outputs. It reserves a
sequential child-reader handle before dispatch, checks current parent execution
authority on incremental I/O, and preserves internal dependencies after external
output expiry. Waiting parents occupy no worker slot; a one-worker scheduler
can execute children, close their scope, reassemble the parent and close the root.
Storage faults remain storage faults even when swallowed by application code.

Reviewed assertions and raw execution evidence establish that 38 focused tests
and the unfiltered 506-test Java suite pass with no failures/errors/skips,
as do the three existing external examples, strict scoped Javadoc and draft build.
The [branch verification record](../../conformance/results/durable-work-v2-java-branches-2026-09-08.txt)
retains initial fixture failures, exact corrections, process-interruption and
handle-accounting evidence, command exits and artifact hashes.

Next is the independently fenced local producer-1 expansion interface, including
durable progress distinct from sealing and phase-specific resource reservation.
Explicit retry/cancellation, external result delivery, dependency-safe cleanup,
retirement and Java durable transport/client integration still remain. Neither
this checkpoint nor existing Core examples complete task 2. The neutral
cross-language failure driver and all original workload/gRPC comparison
deliverables remain required; the full goal stays active.

### Local producer transactions and final-time fencing checkpoint, 2026-09-08

Java now has real parent-fenced producer-1 declaration, preflight and admission,
with separate operation namespaces audited across recovery. It retains normal
input/output/job funding and never equates a membership seal with completed
expansion. A crash before admission resumes from installed input; a crash after
commit replays the original child receipt without another job or deadline.

Review found a shorter child's proposed deadline could pass during final parent
authorization. Java now checks both intervals against the same final sample.
The corresponding Rust declaration/admission paths now recheck local parent
ownership after final authorization and retained local receipt validation.
Section 12.5 explicitly states the final new-admission deadline requirement.

Reviewed raw evidence: 45 focused and 516 full Java tests, 74 focused and 801
full Rust workspace tests, no failures or skips/ignored tests. Strict Java
documentation checks, core Rust warnings-denied Clippy, three existing external
examples and the updated draft build pass. Commands, hashes, crash boundaries
and scope limitations are in the
[producer verification record](../../conformance/results/durable-work-v2-local-producer-2026-09-08.txt).

Next complete Java's phase-specific receiver credit, producer callback scheduling
and durable expansion transition, including declared-but-unadmitted obligations.
Also audit remaining Rust execution/publication final-time boundaries: for example,
`execution/branch.rs::finish` currently loads the execution fence before mutation
and performs final authorization without rechecking time against that pre-transition
fence. The declaration/admission fixes do not establish safety for every transition.
Full Java retry/cancellation/results/retirement/transport/client behavior, the
neutral cross-language driver and every original workload/gRPC deliverable remain
required. This checkpoint is verified progress, not completion of the full goal.

### Rust execution commit-time checkpoint, 2026-09-08

The remaining worker transitions identified above now check time after final
authorization. Claim validates its proposed lease; renewal validates the old
lease as well as its replacement; publication and expansion yield/completion
validate pre-transition ownership. Explicit retry rechecks the original deadline.
New receipt/output intervals cannot already have elapsed before publication
commits. Section 12.6 states these requirements without changing the wire format.

Reviewed raw evidence establishes 100 focused execution tests and 807 full Rust
workspace tests passing, with no failures or ignored tests. Warnings-denied core
Clippy and the updated draft build pass. Tests pin named refusals, rollback of
record revisions/credits/clock, valid forward-time transitions and expansion
state on reopen. The
[execution commit-time record](../../conformance/results/durable-work-v2-execution-commit-time-2026-09-08.txt)
retains exact commands, result counts, source hashes and scope limitations.

Next complete Java's phase-specific receiver credit, producer callback scheduling
and durable expansion transition, including declared-but-unadmitted obligations.
Full Java retry/cancellation/results/cleanup/retirement/transport/client behavior,
the neutral cross-language failure driver and all original workload/equivalent
authenticated durable streaming-gRPC deliverables remain required. This is
verified progress; the complete goal remains active and incomplete.

### Java authority-expansion checkpoint, 2026-09-08

Java now invokes a real mode-2 expander with parent-fenced producer operations,
stable declaration/admission replay and a phase-specific receiver credit. Complete
requires sealed membership with admitted or already-terminal obligations; Yield
releases local ownership while preserving the wire attempt and settlement funding.
After actual child closure, a separate invocation reassembles committed child
outputs. One-worker, three-handle tests produce and publish actual reassembled bytes.

Review also corrected sticky refusal precedence and dispatch fairness. An earlier
capacity refusal cannot hide later wrong-thread misuse behind a Yield. Capacity-
blocked dispatch retains the ready child's place; independent finite deadline
maintenance continues even with all workers occupied. A dispatch sweep can extend
its tail only once before wrapping. Section 12.5 states the completion coverage rule.

Reviewed raw evidence: 73 focused tests and the full unfiltered 537-test Java suite
pass with no failures/errors/skips. Strict scoped Javadoc, the native guard, all
three existing external examples and the updated draft build pass. The
[expansion verification record](../../conformance/results/durable-work-v2-java-expansion-2026-09-08.txt)
retains the corrected red/green evidence, final source/artifact hashes and costs.
Those examples do not establish authenticated Java V2 wire behavior.

Next align the Rust completion path and recovery audit with the same explicit
sealed/admitted-or-terminal rule; current fixture declarations without input must
not receive a test-only exemption. Complete Java retry/cancellation/results/
cleanup/retirement/transport/client behavior, the neutral cross-language failure
driver and every original workload/equivalent authenticated durable streaming-gRPC
deliverable remain required. The full goal remains active and incomplete.

### Rust expansion-completion checkpoint, 2026-09-08

Rust now applies Section 12.5's sealed/admitted-or-terminal rule both at live
expansion completion and in the recovery audit. Invalid completion returns
NOT_READY without settling the parent, releasing its lease or spending its
remaining WORK/JOB settlement credits. Real admitted-but-unexecuted children
and real terminal inputless children permit completion. Contradictory stored
completion is refused on integrity checking and reopen. Existing default test
expanders now admit actual child input; deliberately unfinished production yields.

Reviewed raw evidence establishes 812 full Rust workspace tests passing, followed
by 226 authority tests and warnings-denied Clippy after an import-only correction.
The updated draft builds with zero idnits errors/flaws/warnings and its existing
FIPS comment. The
[completion verification record](../../conformance/results/durable-work-v2-expansion-completion-2026-09-08.txt)
retains exact source hashes, raw logs, the initial regression failures, the
corrected DECLARED-view test assertion and both intermediate import diagnostics.
This is not a new wire interoperability, process-death or resource benchmark.

Next complete Java explicit retry and cancellation, followed by the remaining
results/cleanup/retirement/transport/client behavior. The neutral cross-language
failure driver, both-language failure/resource evidence and every original
external workload/equivalent authenticated durable streaming-gRPC deliverable
remain mandatory. The full goal stays active and incomplete; this checkpoint
does not shrink the original objective.

### Java explicit retry checkpoint, 2026-09-08

Java now accepts an owner-authorized replacement attempt as one atomic metadata
transaction. It replenishes settlement credits, fences the previous local worker
and retains the immutable receipt without changing input, child scope, completed
expansion or original deadline. Exact replay remains authenticated evidence after
deadline or terminal settlement. Current authorization and time are rechecked
before a new mutation commits. Storage format 6 indexes typed retry intent and
audits a complete receipt sequence from admission to the current attempt.

Reviewed raw evidence: 13 focused tests and the full unfiltered 550-test Java suite
pass with no failures/errors/skips. The full run has a retained exit code of zero
and 82 fresh XML reports; the focused launch lacked a returned process code, so
its BUILD SUCCESS/report evidence is recorded without inventing one. Strict
six-type Javadoc, the native guard, three existing external examples and draft
build pass. The
[retry verification record](../../conformance/results/durable-work-v2-java-retry-2026-09-08.txt)
contains exact hashes, commands and limitations. These are local authority tests,
not new authenticated Java V2 wire or process-death evidence.

Next implement explicit cancellation/skip acceptance and bounded subtree
reconciliation. The current declaration reader still requires an empty work-fence
image; replace that with checked typed fence state and recovery invariants, not
an exemption. Freeze membership at acceptance, preserve the first promised target
outcome, fence all descendant mutations immediately, and materialize seals and
settlement in bounded resumable batches. Results/read pins, cleanup/retirement,
durable Java endpoint/client integration, the neutral cross-language failure
driver and all original workload/equivalent authenticated durable streaming-gRPC
deliverables remain required. The full goal remains active and incomplete.

### Java cancellation and bounded settlement checkpoint, 2026-09-08

Java now accepts explicit work cancellation/skip and scope cancellation as atomic
owner-authorized mutations. The first own fence fixes the promised outcome;
ancestor checks immediately exclude new descendant declaration, admission, retry
and publication. Pending branches retain CANCELLING until actual child closure.
Local administrative revocation denies caller access while maintenance still
settles unadmitted as well as admitted obligations. Prior terminal outcomes and
earlier own skip outcomes are preserved.

Bounded keyset sweeps compute real full frozen seals and terminal settlements.
Partial digests remain volatile and are never advertised. A reopen during a fold
restarts from durable records, and a monotonic scope fence restarts a partial
closure fold without relaxing its membership checks. Private schema 7 records
typed fences, cancellation journal/index provenance and pending job state.
The admission/retry job reservation is eight writes, preserving a separate
four-write cleanup allowance beyond a conservative lifecycle envelope.

The corrected focused 27-test gate and final unfiltered 563-test Java suite pass,
with no failures/errors/skips. The final exit code is zero and all 84 XML reports
were independently checked for counts and freshness. This includes the existing
actual publication process-death test. The first full run exposed its old hardcoded
six-credit expectation; exact before/after equality now uses the configured
reservation and reservation minus one. Production was not weakened to satisfy
that obsolete constant. Strict ten-type doclint and the updated draft build also
pass; exact raw evidence and limitations are recorded in the
[cancellation evidence](../../conformance/results/durable-work-v2-java-cancellation-2026-09-08.txt).

Next implement external result-read lifetime pins and dependency-safe cleanup,
then session retirement and the durable Java endpoint/client integration. Keep
the neutral cross-language failure driver and all original external workload
and equivalent authenticated durable streaming-gRPC comparison deliverables.
These local authority APIs do not activate the Java V2 listener. The full goal
remains active and incomplete.

### Java result leases and Rust read commit-time checkpoint, 2026-09-08

Java now independently returns authenticated immutable result manifests and pins
exact published objects. Its current result policy is separate from execution
permission. Fresh reads check safe UTC and external availability after file
verification and final authorization; a refused precommit acquisition closes its
pin and rolls back the watermark. Manifest lookup grants no lease and does not
depend on current UTC or payload availability.

One local service per exclusive input installation accounts for acquiring,
pending and active reads globally and per owner. It shares physical handle
limits with callback and input/output storage. One bounded chunk can be pending;
only reported transport acceptance renews idle time. Pending stream-slot time
counts against the original lifetime, independent timer sweeps expire unused
reads, and current permission/revocation remain scheduling gates. Busy physical
I/O stays charged. This is local delivery lifecycle behavior, not Netty V2
result-stream integration or a measured native/process-memory bound.

The comparison found a real Rust acquisition gap: its availability sample
preceded storage work and final authorization. Three independent regressions
failed against that exact prior code, demonstrating late expiry, intra-call
clock regression and retention of the wrong UTC sample. The corrected code
passed all 22 result tests and the full 815-test Rust workspace, with strict
Clippy. Java's focused 26-test gate and full 578-test suite passed, including
the existing physical publication crash test and native storage guard. All
three existing-profile external examples passed; they do not establish Java
durable V2 parity. Raw exits, XML counts, source hashes and scope limitations
are in the [result-lease evidence](../../conformance/results/durable-work-v2-result-leases-2026-09-08.txt).

Next implement dependency-safe Java retention and retirement. Cleanup needs
durable eligibility evidence before deletion, per-object physical liveness,
synchronized removal before refunds, and recovery checks that distinguish an
authorized interrupted deletion from missing promised bytes. Parent dependencies
remain valid beyond external output expiry. Then complete Java's durable
endpoint/client and the two-direction neutral failure/resource driver. The
external workload and equivalent authenticated durable streaming-gRPC comparison
remain required. This checkpoint does not complete task 2 or the overall goal.
