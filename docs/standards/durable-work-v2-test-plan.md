# Version-2 implementation acceptance ledger

Normative source: Section 12 and Appendix F of local draft -05. This ledger
organizes the mandatory selected-profile requirements into test families. It
does not replace the normative text, waive any clause, or assert conformance.
The full goal remains in [the execution record](durable-work-results-goal.md).

Status: every implementation family below remains open as a cross-language
acceptance gate. Rust now has the library coverage recorded below; no V2
endpoint advertises a partially implemented profile. The existing version-1
tests and the three abstract models are regression/design evidence only.
Rust and Java must independently implement the same durable-work plus
result-delivery combination. Neither may advertise a partially implemented
profile. C++ remains at its existing version-1 subset for this goal.

For every family, record exact Rust tests, exact Java tests and the independent
Rust process-driver scenario where applicable. A happy path alone is not
sufficient. Storage families require actual restart/crash evidence and measured
resource gates, not only mocks or an in-memory state model. Keep test names and
logs traceable to these stable family IDs as implementation proceeds.

## Rust library evidence, 2026-09-06

Source: `implementations/rust-quinn/src/v2/`; tests in its `tests.rs`.
Run `cargo test --locked -p pipestream-core v2::` from that Rust workspace.
These are 17 library tests, not process-driver or Java evidence:

- V2-WIRE: `every_frozen_wire_case_has_exact_typed_roundtrip_or_named_refusal`
  executes all 70 frozen rows. Accepted typed records encode back to identical
  bytes; all 24 refusals retain their named code. The truncation test rejects
  every prefix and trailing byte for every accepted example. Separate tests
  cover forbidden CBOR types, hostile lengths and unknown frame classes.
- V2-NEG: `negotiation_checks_authentication_required_sets_dependencies_and_every_limit`
  checks the selection algorithm with a supplied authentication decision; it
  does not authenticate a certificate. Correlation tests cover increasing IDs,
  out-of-order replies, wrong kinds, duplicates, bounded pending/stream counts,
  abort, input stream identity and cross-connection result-proof rejection.
- V2-OP/SET/CLOSE: `all_frozen_commitments_are_computed_from_typed_fields`
  matches all 12 commitments. Incremental seal tests include 1,000 declarations
  and missing/extra/unsorted members. Incremental status roots match a separate
  level-by-level fold for sizes 0 through 1,000, including odd duplication.
- V2-VIEW/RESULT: tests reject contradictory terminal fields, profile-dependent
  success shapes, wrong receipt variants, invalid retry increments, inconsistent
  fence dispositions, count partitions and locator/manifest identities.
- V2-RESULT: payload/correlation tests require exact SHA-256/length/FIN and
  bounded monotonic idle/lifetime progress. A result header is not completion;
  a second response is refused, and a blocked library transfer does not borrow
  the control book. This is not a QUIC flow-control or process-memory gate.

The implementation pass exposed an error-code ambiguity: Section 12.1's generic
invalid-selection rule could be read as FRAME_ERROR for a missing result-profile
dependency, while the frozen response example specifies EXTENSION_UNSUPPORTED.
The text now explicitly preserves the dependency-specific refusal in either
direction. No frozen bytes or refusal expectations were changed.
The stream deadline text also now specifies the equality boundary explicitly:
progress or FIN at the idle/lifetime deadline is too late, not a renewal.

Still required: independent Java codecs; authenticated V2 Quinn/Netty endpoints;
session and operation journals; real durable authority/storage/execution;
restart/cleanup/resource measurements; neutral Rust cross-language scenarios.
The library helpers do not satisfy these outstanding acceptance gates.

### Rust authority storage evidence, 2026-09-06

`src/v2/authority/` now implements persistent session creation/attachment,
declaration, operation replay/lookup, membership pages, revision snapshots and
empty sealed closure. Its 18 tests (including the subprocess entry point) run
under the same `cargo test --locked -p pipestream-core v2::` command; 35 tests
pass together with the 17 wire-library tests.

- V2-SESSION/V2-OP: `creation_replays_after_reopen_without_reissuing_identity`,
  `simultaneous_creation_and_declaration_commit_once`,
  `different_concurrent_parameters_cannot_share_an_operation`, and
  `counter_exhaustion_and_retired_creation_never_reuse_identity` exercise actual
  transactions and retained identity. The retired-history case seeds a synthetic
  high-water row; it is not a retirement/cleanup implementation test.
- V2-AUTH/V2-TIME: `authorization_precedes_lookup_and_revocation_denies_replay`,
  `policy_withdrawal_before_commit_rolls_back_mutation_and_receipt`, and
  `clock_rollback_refuses_mutation_but_preserves_retained_evidence` use explicit
  local policy/clock implementations. They do not validate TLS or prove revoked
  work settlement. The revocation case sets the retained denial flag directly.
- V2-SET/V2-CLOSE: declaration/replay/page, batch-independent seal, missing input,
  and empty scope tests distinguish membership from admission and completion.
  These initial tests covered empty closure only; later nonempty closure
  evidence is recorded in the settlement checkpoint below.
- V2-STORE: `process_crash_on_each_side_of_creation_and_declaration_commit`
  runs four child-process exits without SQLite/Rust destructors. Reopen verifies
  pre-commit absence and post-commit replay. This covers metadata commits, not
  payload installation, worker execution, transport ACK loss or cleanup.
- V2-STORE: `physical_exhaustion_rolls_back_whole_batch_and_preserves_replay`
  exercises 128 KiB DB/journal, 4 MiB WAL and 64 KiB SHM caps; committed evidence
  remains readable/replayable after refusal and reopen. This measures file
  lengths, not allocated blocks. The WAL cap now funds record rewrite credits;
  bounded storage refuses a later declaration after a committed batch.
- Reopen/initialization tests prevent accidental empty-store creation during
  recovery. Exact large-integer tests include values above 2^53 and the maximum
  signed-63-bit entity ID. No JSON or floating-point persistence is used.
- V2-TIME: `empty_root_closure_refuses_unrepresentable_receipt_retention`
  failed before the fix and now checks overflow refusal without a committed seal
  or operation, plus acceptance at the exact maximum representable deadline.

The full cross-language gates below remain open. Admission evidence added after
this initial checkpoint is recorded below. Workers/leases/cancellation, nonempty
closure, results/read pins, retirement and cleanup are not implied by these tests.

### Rust payload/ingress evidence

`src/v2/authority/payload/tests.rs` adds 12 storage tests, including a subprocess
entry point; `ingress.rs` adds three header/reception tests. Their scope is:

- V2-ADMIT: reject invalid owner, producer, generation, undeclared work,
  application/mode, duration and response/byte budgets before payload allocation.
  Hash/FIN failure preserves declaration and leaves no operation receipt.
  Verified installation is expressly not admission or execution.
- V2-STORE: six process exits bracket staging-file creation/header, object fsync,
  rename, directory fsync and orphan unlink. Restart distinguishes abandoned
  staging from installed unreferenced objects. Physical power loss is not tested.
- V2-STORE: durable root/database pairing, exclusive ownership, live object pins,
  changed configuration/identity refusal and unknown/aliased entry refusal.
  Collection uses bounded cursor batches under the authority's writer transaction
  and preserves committed references. The reference test does not admit a job.
- V2-RESULT (storage only): exact retained descriptors, full length/hash checking
  at EOF, permanently failed corrupt reads and global/per-owner handle bounds.
  This does not implement authenticated RESULT RPCs, manifests or read leases.
- `cargo test --locked -p pipestream-core --test v2_payload_resources -- --nocapture`
  streams and verifies 32 MiB using 16 KiB buffers in its own allocator-instrumented
  executable. It gates added Rust heap below 256 KiB and individual allocations
  below 64 KiB, and separately reports file lengths, allocated blocks and process
  RSS/HWM. This is neither an end-to-end benchmark nor proof of all process-memory
  bounds or populated-inventory scaling.

Temporary reception quotas and object-reference cleanup do not substitute for
admission or either cross-language direction. Subsequent admission evidence is
recorded below; complete lifecycle transitions and retirement remain open.
Durable output-capacity evidence is recorded separately below.

### Rust fixed-record funding evidence

`src/v2/authority/records/tests.rs` has 22 tests, including its subprocess entry.
This is persistent storage implementation used by declarations and empty scope
closure, not evidence that the entire admission/job transaction is funded.

- V2-STORE: checksummed preallocated work/summary records, exact revisions,
  ordinary writes preserving credits, transactional spending/rollback and named
  refusal for stale revisions, oversize records and exhausted credits.
- V2-VIEW/V2-STORE: a negative-first counter test prevents ordinary updates from
  using the last revision increments reserved for promised record rewrites.
- V2-STORE: a retained SQLite reader prevents WAL reclamation. Ordinary writes
  fill the protected ceiling; four reserved work/clock pairs across two work records
  still commit under a 1 MiB WAL cap with database growth disabled. This measures
  paired record updates only, not an entire job settlement or cancellation RPC.
- V2-STORE: 18 page/capacity combinations cover 512/4096/65536-byte pages and
  512-byte through 1 MiB records with cache spilling, row replacement prohibited,
  and database page growth disabled. Every measured WAL length fits the
  pinned-layout record-plus-clock bound, including a maximum authority label
  and a long payload-path tail on the clock's row. File lengths are not
  filesystem allocated blocks.
- V2-STORE: two child-process exits bracket a credit-spending commit. Restart
  preserves either its entire prior revision/credit or its entire committed
  successor. Header, body, padding and cross-row corruption fail closed; startup
  rejects corrupt bodies even when their charge headers remain intact.

Complete lifecycle write sets, dependency/read pins, metadata retirement and
cross-language V2 failure tests remain required. These record-level credits do
not close those gates. Global/per-owner job-budget evidence is below. Output-file
budgets are covered by the reservation implementation below.

### Shared-clock and complete scope-record evidence

Authority format 5 retains greatest UTC in a fixed clock record and moves all
mutable scope fields, including root revocation, into the funded scope record.
The payload-root format and normative wire representation are unchanged.

- V2-CLOCK/V2-STORE: ordinary clock writes and new reservations cannot consume
  clock revision increments owed to existing record credits. Already funded
  work/scope updates can still persist time through exact counter exhaustion.
  An actual empty-scope declaration/closure exercises the funded ordering.
- V2-STORE: scope seal/fence fields and clock are updated with SQL row replacement
  prohibited and database page growth disabled. This private storage test is
  not a public cancellation/revocation API or descendant-settlement claim.
- V2-STORE: paired work/clock and scope/clock process-death tests preserve either
  the complete prior state or the complete successor. Reopen rejects clock
  corruption and scope/work timestamps beyond retained greatest UTC.
- V2-AUTH: a negative-first regression verifies ownership denial before decoding
  another owner's corrupt binding or scope. Both cases now return UNAUTHORIZED,
  not a storage-corruption diagnostic from the other owner's retained state.

Paired-record funding does not claim arbitrary SQL writes or whole-job costs
are reserved. Admission evidence is below; durable lease/execution/publication
transitions, cancellation settlement, retention and Java V2 remain open.

### Rust output-reservation evidence

`src/v2/authority/payload/reservations/tests.rs` adds 15 tests, including its
subprocess entry point. Output file capacity is durable; these filesystem tests
are not admission/job/receipt or protocol-level result-publication evidence.

- V2-STORE/V2-ADMIT: global/per-owner byte and object forecasts survive reopen;
  ordinary uploads cannot consume reserved output capacity. Partial outputs hold
  their entire maximum; unused bytes remain inside the reservation. Zero-length
  objects and a zero-count reservation have distinct behavior.
- V2-RESULT (storage only): outputs with initially unknown length/hash install
  their actual descriptors and verify through file-backed reads. All 256 slots
  and maximum owner/content-type labels work with only two live handles. Slot
  uniqueness, owner/budget mismatch, over-budget output, chunk bounds, monotonic
  regression, idle/lifetime equality, and non-renewing empty writes are checked.
- V2-STORE: 11 child-process exits cover reservation creation/fsync/rename/
  directory sync/unlink and output creation/header/payload/fsync/rename/directory
  sync. Restart retains installed output promises and reclaims only abandoned
  stages. These tests do not simulate physical power loss or network ACK loss.
- V2-STORE: actual SQLite reference transactions preserve reservation and object
  liveness through cleanup. The test leaves work DECLARED; it is not a fabricated
  admission, manifest, terminal result or retirement implementation.
- V2-STORE: missing/corrupt/aliased funding fails closed. A negative-first test
  forces a real rename failure and now verifies root quarantine until exclusive
  reopen. Filesystem space/quota errors map to LIMIT_EXCEEDED; this mapping test
  does not claim a whole-disk exhaustion experiment.
- `v2_output_resources` streams, installs and reads 32 MiB through 16 KiB buffers
  in an isolated allocator-instrumented executable. It uses the same 256 KiB
  added-heap and 64 KiB allocation gates as the input test. Both report file
  lengths, allocated blocks and RSS/HWM separately. This does not establish QUIC
  flow-control, complete endpoint memory bounds, or a gRPC performance advantage.

Complete lifecycle metadata/journal funding, worker leases, authenticated
manifest publication/read leases, dependency retention, session retirement,
independent Java V2 and both cross-language failure directions remain required.

### Rust admission-preparation evidence

`AuthorityStore::prepare_input` is executable storage preparation, not admission.
It retains the validated input/output pins and funds a larger work-view record
without changing DECLARED, its revision, or operation history. This API must not
trigger an admission ACK; the separate admission transaction is described below.

- V2-ADMIT/V2-STORE: preparation checks the exact database-bound payload root,
  not merely its store identity. Application, response limits, scope fences,
  current authorization and trusted clock are rechecked. A denial at the final
  authorization check rolls back record expansion; installed orphans remain
  charged until safe collection. Repeated preparation preserves the work view.
- V2-STORE: expansion preserves exact typed contents, revision and prior credits;
  larger credit costs are protected before page allocation. A real trigger-induced
  failure after resizing rolls back even if the caller commits its outer
  transaction. Corrupt source, shrinking promises, counter exhaustion and stale
  revisions are refused. Four process exits bracket resize, initialization and
  the outer commit; restart never sees a partially initialized record.
- V2-STORE: real SQLite page exhaustion refuses growth without changing the prior
  committed record. The pinned-reader WAL test now grows one work record before
  filling ordinary capacity, refuses another enlargement, then successfully spends
  both records' promised rewrites without database growth. The 18 page/capacity
  rewrite-bound cases continue to run.
- V2-ADMIT/V2-RESULT (representation only): sizing checks encode 0, 1 and 256
  outputs with maximum labels/numeric fields and a maximum-length DNS host. They
  fit the conservative response reservation. No result is published by this test.

Preparation does not fund executor slots or the complete metadata transition,
install jobs, grant worker leases, publish manifests or implement retention.
Private expanded work-view capacity remains charged to declared work. Neither
V2 endpoint nor cross-language conformance follows from this checkpoint.

### Rust atomic admission evidence

`src/v2/authority/admission/tests.rs` adds 13 tests, including its subprocess
entry. `AuthorityStore::admit_input` now commits external admission in the actual
SQLite authority. Storage format 6 adds fixed job records and global/per-owner
job ceilings; payload format 4 and normative wire/commitments are unchanged.

- V2-ADMIT/V2-SET: all three modes commit input, attempt 1, timestamps, reservation,
  fixed job and immutable receipt together; branch modes allocate exactly one
  correctly owned child. Stored input bytes are read and verified, not inferred
  from a descriptor. No callback runs in admission.
- V2-OP: concurrent duplicate prepared inputs serialize to one job/child/receipt;
  changed parameters conflict. Replay under an unsafe clock issues no new promise.
- V2-ADMIT/V2-STORE: global/per-owner executor ceilings span separate sessions;
  session job/input/output, operation and scope ceilings refuse without accepting
  a second job. Waiting branches remain charged. Limits on reconnection preserve
  the promised response/object representation; timestamps above 2^53 stay exact.
- V2-AUTH/V2-CLOCK: preparation is not authority to bypass current application,
  scope, revocation, clock, counter or limit checks. A late authorization denial
  rolls back the entire job, child, references, receipt and extra record credits.
- V2-STORE: actual subprocess death immediately before/after admission commit
  leaves respectively DECLARED/no receipt or admitted/replayable work. Both paths
  reopen real input bytes and the output reservation. This exercises lost-ACK
  state at the storage API, not a V2 QUIC transport.
- V2-STORE: a pinned reader fills guarded journal capacity. The focused run
  committed 284 ordinary fill writes, then refused admission at WAL 3670976 bytes
  under a 4194304-byte cap (DB 73728 under 131072). Prior receipts remain readable;
  no partial job/child/reference or new operation survives. These are file lengths,
  not allocated-block, whole-lifecycle or end-to-end cost measurements.
- V2-STORE: a negative-first regression found installed outputs could be collected
  after process pins disappeared despite a committed reservation. Collection now
  honors that reservation's durable liveness. This does not publish a result;
  replacement-worker cleanup requires an explicit lease-fenced implementation.
- V2-STORE: reopen rejects missing/changed job, input reference, operation receipt,
  parameters and stage. The maximum typed job representation fits its fixed slot.

This checkpoint covers external admission and durable job storage. Subsequent
execution evidence is below; it does not activate a V2 endpoint or complete the
remaining lifecycle, Java, cross-language or workload gates.

### Rust worker execution and publication evidence

`src/v2/authority/execution/tests.rs` adds 16 tests, including its subprocess
entry. Applications register real callbacks. The synchronous executor claims a
known retained job, runs outside the metadata writer and commits authoritative
outcomes. Subsequent worker-pool evidence is below; no V2 listener is activated.

- V2-ATTEMPT/V2-RESULT: a real streaming copy callback publishes a manifest and
  the test opens and verifies the output bytes. Admission and receipt replay do
  not invoke the callback. Resultless work has no fabricated manifest; trying to
  emit an output without a budget fails instead of silently discarding it.
- V2-ATTEMPT/V2-AUTH: claim and publication check owner, ancestor fence, original
  deadline, wire attempt and private worker lease. Equality at lease expiry is
  stale. Renewal retains the lease and cannot resurrect it. A callback commits a
  scope fence through a separate connection, proving it runs outside the writer;
  that accepted fence prevents its later publication.
- V2-OP/V2-ATTEMPT: retryable outcomes wait for explicit retry. The retry operation
  commits one replayable receipt and a new wire attempt, fences a live old worker,
  retains input/child/deadline and replenishes record credits. Changed operations,
  stale expected attempts, expired deadlines and terminal work refuse by name.
- V2-RESULT: ignored output-limit errors, panic, invalid diagnostics and unfinished
  outputs cannot produce SUCCEEDED. Publication verifies installed inventory under
  the live reservation pin without acquiring an extra result read handle. Reopen
  refuses a checksummed manifest rebound to another owner.
- V2-CLOSE (partial): caller-branch execution waits for actual retained successful
  child closure; the test uses the implemented empty closure. Authority-expanded
  execution is explicitly refused, not silently treated as a leaf.
- V2-STORE: actual process death before/after claim, publication and explicit retry
  preserves the relevant committed outcome. Recovery after an unpublished output
  unlink preserves the original funded budget; live old handles prevent recycling.
  Reopened output bytes are verified and committed success is never executed again.
- V2-STORE: the complete publication work/job/clock write set commits with 0, 1 and
  256 real zero-byte outputs after ordinary WAL capacity is exhausted by a pinned
  reader. SQL triggers forbid row replacement; database page count does not grow.
  With a 4194304-byte WAL cap, the focused run grew from 3308416 to 3320752 bytes
  for 0/1 outputs and from 2006496 to 2348432 for 256 outputs. These are actual
  file lengths, not allocated blocks, complete lifecycle funding or baseline costs.

### Rust bounded workers and reserved I/O evidence

Ten additional execution tests and two payload-reservation tests cover the fixed
pull pool and callback I/O capacity. The earlier process-death test now uses that
pool to discover jobs in both the crashed process and its replacement; neither
startup requires a caller-supplied work key. An additional admission test checks
permanent worker-I/O feasibility. The focused authority suite has 119
passing tests, with strict workspace clippy clean.

- V2-STORE: a negative-first test filled the handle pool with unrelated readers
  after claim and reproduced FAILED instead of SUCCEEDED. Claim now reserves a
  reusable output slot before committing the lease; the same test completes real
  output under saturated reader capacity. Insufficient claim capacity leaves the
  job unchanged and invokes no callback. Completion returns all transient slots.
- V2-ADMIT: another negative-first regression reproduced admission under an
  immutable two-handle ceiling that could never run an output-producing callback.
  Admission now refuses that global/per-owner configuration without a job or
  receipt; work declaring no outputs can still admit with two handles. Temporary occupancy
  is not confused with permanent impossibility.
- V2-STORE: real global/per-owner handle ceilings cover sequential output reuse,
  refusing concurrent reuse of one slot, dropping a stage, and a stage/installed
  token outliving its reservation. Charge transfer never exposes occupied capacity
  or double charges the reserved slot. The existing 256-output tests still pass.
- V2-ATTEMPT/V2-STORE: polling discovers later committed admission without a wake
  or submit call, retains known terminal work without re-executing it, and recovers
  after actual process death. Retryable work remains dormant through further
  scans until explicit retry commits a replacement attempt.
- V2-AUTH/V2-STORE: held real callbacks demonstrate the configured global/per-owner
  concurrency ceilings while authoritative work-view reads still complete.
  Clock regression and reader saturation refuse before callback execution; removal
  of the condition lets the same durable job execute without resubmission.
- V2-CLOSE (partial): one-record scans progress past unimplemented authority
  expansion and unclosed caller branches to execute a later leaf, preserving
  named NOT_READY refusals. This does not implement those missing branch paths.
- V2-STORE: invalid pool limits and a second pool refuse explicitly. Dropping a
  pool stops further discovery but preserves admitted backlog and retains its
  ownership until dispatched callbacks return. A new pool then executes remaining
  jobs. Corrupt job storage stops discovery visibly without invoking callbacks or
  replacing the damaged job.

The pool scans retained jobs with a bounded batch and one shared cursor; it is not
an indexed ready queue. These tests bound live dispatches and handle accounting,
not whole-workload CPU/RSS/disk/network cost or forced preemption of callback code.
Subsequent settlement evidence is below. Still required: producer-1
admission and complete branch execution, authenticated RESULT
read leases, dependency retention, expiry and retirement. Input/output liveness
stays charged until safe cleanup exists. Independent Java V2, both cross-language
failure directions and the equivalent streaming-gRPC workload remain required.

### Rust authoritative settlement and nonempty closure evidence

`src/v2/authority/settlement/tests.rs` adds 17 tests including its subprocess
entry. The library now exposes real cancellation/skip/scope-cancel transactions,
explicit operator revocation and bounded autonomous reconciliation. A dedicated
maintenance thread runs independently of the callback-worker limit. This is
local storage/application evidence, not authenticated V2 network conformance.

- V2-CANCEL/V2-OP: `declared_cancel_skip_and_terminal_receipts_replay_without_changing_outcomes`
  checks inputless outcomes, operation conflicts, explicit skip permission,
  dispositions and preserved successful manifests. The late-authorization test
  rolls back the fence, outcome, operation and clock together.
- V2-CANCEL/V2-CLOSE: `nested_first_skip_fence_survives_parent_cancel_deadline_and_restart`
  keeps a branch CANCELLING until its descendants close and preserves its earlier
  SKIPPED promise under ancestor cancellation and expiry. A prepared-admission
  test excludes admission/retry/publication before descendant materialization.
  An authority-producer empty scope can be cancelled by its authorized owner;
  this does not implement producer-1 declaration/admission or expansion.
- V2-TIME/V2-CLOSE: active and awaiting-retry deadlines settle FAILED with the
  original input, attempt and deadline. STRICT child failure settles its parent;
  an expired parent does not cancel missing descendants. Unsafe/regressed clocks
  refuse new promises but preserve immutable receipt replay.
- V2-AUTH: `revocation_uses_operator_permission_and_settles_without_caller_authorization`
  freezes the root under separate Revoke authority, denies replay/view/admission,
  and settles even unadmitted descendants without restored caller credentials.
- V2-RESULT/V2-CANCEL: `accepted_fences_beat_inflight_publication_without_refunding_live_payloads`
  holds a real copy callback after output installation, commits each of work
  cancel, skip, scope cancel and revocation, then rejects publication. Durable
  byte charges and live handles remain intact. Success-first cancellation is
  separately tested. Maintenance closes expired work with every callback worker
  still occupied; callback release is not required for authoritative settlement.
- V2-SET/V2-CLOSE: 600 declarations are cancelled, sealed and folded in batches
  of 73 and 127 across reopen. Per-call work/member counts are bounded, computed
  seals/status roots match complete typed folds, and repeated summaries are
  immutable. Empty closure and foreign-cursor refusal are also tested.
- V2-STORE: `actual_process_death_preserves_atomic_fences_settlement_seals_and_closure`
  exits child processes before/after six commit cases: work fence, scope fence,
  revocation, deadline settlement, computed seal and closure summary. Reopen
  verifies the accepted state, actual retained input bytes and charged outputs.
- V2-STORE: `autonomous_settlement_fits_reserved_wal_without_row_replacement_or_page_growth`
  completes work/job/scope/clock writes after ordinary journal capacity is
  exhausted with a pinned reader. Triggers forbid SQL row replacement; page count
  stays fixed. Under the unchanged 4194304-byte WAL cap, the focused run measured
  3048856 to 3065312 bytes for expiry and 3135376 to 3160072 for scope cancellation
  plus deferred seal and closure. This is file-length evidence, not filesystem
  block preallocation, full lifecycle costs or a scalability benchmark.
- V2-STORE: format 7 preallocates a 256-byte typed first-fence body plus 104-byte
  header and one rewrite credit per work item; scope records now carry four
  credits. Prior format 6 is refused without conversion. Reopen binds fences to
  immutable receipts and terminal timestamps, and validates scope-parent links.
  Payload format 4 and all frozen wire bytes remain unchanged.

Section 12 now distinguishes atomic membership freeze from deferred full seal
computation: pages carry null until the digest is committed, without permitting
new membership. It also explicitly serializes deadline failure with cancellation
acceptance. An accepted fence takes precedence even when materialization follows
expiry; an already terminal outcome stays immutable. The independent composed
model adds a negative control for failure overriding an accepted fence.

These transitions leave payload liveness charged. Producer-1 admission, complete
branch/child-output execution, result read leases, dependency retention, expiry,
safe retirement, Java V2, real V2 endpoints, the neutral cross-language driver
and equivalent streaming-gRPC workload are still required. Work and scope passes
commit separately; interrupted volatile hashes may be recomputed. Credit audits
still scan retained records, so bounded batches do not imply constant transaction
cost or fairness/latency guarantees for a large store.

### Rust authority expansion and actual branch-output evidence, 2026-09-07

Source: `src/v2/authority/execution/branch.rs`, `origin.rs`, and
`payload/read_credit.rs`; 19 tests, including the subprocess entry point, in
`execution/tests/branch_tests.rs`. Run
`cargo test --locked -p pipestream-core branch_tests -- --nocapture`.
The complete focused authority run now passes 155 tests; strict workspace
clippy passes. These are local library tests, not V2 endpoint or Java evidence.

- V2-SET/ADMIT/RESULT/CLOSE:
  `authority_expansion_commits_real_children_and_reassembles_their_transformed_outputs`
  streams `abc` into two actual admitted children and reconstructs `ABC` from
  their transformed outputs using two-byte buffers. The caller-expanded variant
  covers producer 0 and internal child reads after external output expiry.
  The one-worker test discovers and executes the entire tree without a thread
  parked per waiting branch. This is not the external workload/gRPC comparison.
- V2-STORE/SET/OP:
  `process_death_recovers_partial_expansion_and_lost_local_acknowledgments`
  exits an actual process before and after local declaration, local admission
  and expansion-complete commits. Reopened discovery finishes the original
  attempt, preserves exactly three jobs and three producer-1 operations, and
  verifies `ABC`. The seal-only regression failed before adding an independent
  durable expansion-complete flag. Retry tests preserve child views/operations
  and do not rerun completed expansion; an unpublished partial parent output
  is discarded before retrying reassembly.
- V2-AUTH/ATTEMPT/CANCEL/TIME: escaped prepared child admissions recheck current
  parent attempt, lease, exact deadline, authorization, cancellation and
  revocation. External producer-1 admission/declaration/operation lookup is
  refused. An already open child reader cannot bypass these parent fences.
  Immediate parent cancellation after child closure is ALREADY_TERMINAL to
  stale workers; an unresolved scope fence is CANCELLED.
- V2-RESULT/STORE: unverified child EOF prevents successful parent publication.
  Reader capacity is reserved before reassembly claim and cannot be consumed by
  other opens. A reader outliving its worker retains its charge under both
  global and per-owner limits. Impossible handle policies refuse admission;
  authority execution also passes at its five-handle minimum with outputs.
  Child admission pressure can yield after sealing without creating a child job
  or consuming terminal credits; replay after pressure clears completes `ABC`.
- V2-STORE: `branch_completion_fits_reserved_wal_without_row_replacement_or_page_growth`
  fills ordinary capacity behind a pinned WAL reader, forbids work/job/clock SQL
  row replacement and checks unchanged DB page count. Under the 4194304-byte
  WAL cap, the focused run measured expansion completion from 1112456 to
  1120672 bytes and reassembly publication from 1821096 to 1829312 bytes.
  These are configured file-length gates, not allocated disk blocks, RSS or
  whole-workload throughput. Format 8 adds the durable phase and stronger
  minimum handle promises; format 7 is refused with no conversion. Reopen also
  rejects a checksummed successful branch whose expansion was never completed.

Section 12 now explicitly distinguishes membership sealing, child admission and
expansion completion, and requires recoverable local operations and parent
commit fences. Frozen wire examples do not change. Child admission acquires its
own quotas and staging capacity; unlimited expansion is not promised. Result
read leases, reference-safe dependency/retention cleanup, Java V2, authenticated
V2 endpoints, neutral cross-language failure scenarios and the equivalent
streaming-gRPC workload remain open.

Verification log: `/tmp/pipestream-expansion-verified-suite.log` (exit 0).
The complete suite passed 516 Rust workspace tests, six Rust example tests,
193 Java tests from 20 fresh reports, C++ tests, frozen vectors/models, nine
basic language pairs, 32 raw QUIC capability probes and the recursive/external
examples. The draft build also exited 0 with zero idnits errors/flaws/warnings
and the existing informational FIPS downref comment. Historical network tests
do not satisfy the open V2 endpoint/Java/neutral-driver gates.

## V2-WIRE: framing, decoding and representation (12.1, 12.2, Appendix F)

- Own the `pipestream/2` mapping without accepting version-1 messages or silently
  converting version-1 storage. Keep Core independent of recursive scheduling.
- Exact control type/u32-length framing and array cardinality; minimal integers
  and lengths; reject maps, tags, floats, indefinite items, undefined, invalid
  UTF-8, trailing items and extra/missing positions. Bound allocation before
  consuming an advertised length; reject malformed nested collections.
- Decode every defined request, response, receipt, work view, manifest, summary
  and object header into typed fields, independently in both libraries.
- Consume all frozen `test-vectors/v2/wire.tsv` expectations, including the
  semantic/canonical refusals beyond CDDL. Accepted bytes round-trip unchanged.
  Do not regenerate golden bytes from either implementation.
- Required/ignorable/private unknown type classes, message direction and
  profile-dependent types have their specified connection/request error scopes.

## V2-NEG: negotiation and connection accounting (12.1, 12.2)

- One capability exchange, client/server directions, exact supported/required
  intersection/union, bounded sorted unique lists, dependency selection and
  forbidden legacy IDs. Required unavailable profiles fail before activation.
- Reject unsolicited selections, required-set omissions, increased limits and
  invalid deadline relationships. Resume requires the retained profile set and
  representable retained responses, not an implicit downgrade.
- Strictly increasing connection-wide control request IDs, independent actual
  input-stream tags, response kind/identity validation, duplicate and unsolicited
  response refusal, bounded pending maps and exhaustion handling.
- Named REFUSAL and QUIC application error mapping, unknown errors, Stream 0
  failure, partial framing and errors after a result stream starts. Transport
  loss or a refusal is never an invented authoritative work outcome.

## V2-AUTH: authenticated owner and current authorization (12.3, 12.7)

- Server DNS/IP verification, TLS 1.3/QUIC v1, no 0-RTT, real client-certificate
  possession/trust/validity/usage checks and stable principal mapping in Java
  as well as Rust. Preserve RFC 9001 handshake errors, not application fallback.
- Missing/unmapped principals and optional/required durable activation;
  certificate rotation, expiration on a live connection and resumption policy.
- Owner/authority mismatch, foreign owner, revoked sessions and cross-producer
  authorization. Denials reveal no retained work or output, including through
  refusal selection or output-reference resolution.
- Recheck authorization in committing mutations, callback publication and every
  result read. Revocation stops further scheduling; previously transmitted
  bytes and committed external effects cannot be retracted.

## V2-SESSION: issuance, attachment and anti-reuse (12.3)

- Atomic authority generation/owner creation high-water allocation, exact policy
  and retained profile binding, root creation and creation receipt.
- Lost-ACK identical creation replay, changed-policy conflict, out-of-order
  sequences, concurrent callers with one principal, two principals, retirement
  returning EXPIRED and counter exhaustion without wraparound.
- One attached session per connection, immutable admission ceilings, explicit
  attach identity, current authorization and safe handling of stale backups.
  No random-ID uniqueness assumption or history eviction to recover quota.

## V2-OP: immutable operations and uncertainty (12.4)

- Nonzero 16-byte IDs; producer-and-session namespaces distinct from target-work
  producer; complete immutable parameters persisted before transmission.
- Independent operation-digest encoding against frozen commitments, omitting
  only connection correlation and raw payload bytes as specified.
- Atomic mutation/typed receipt, same-ID concurrent replay, changed digest/type
  conflict, pre-commit refusal and commit-before-lost-ACK recovery.
- NOT_FOUND while an old request is still in flight does not authorize new work.
  Keep identity/digest after full receipt expiry and never reapply a retired ID.

## V2-SET: immutable declarations and child identity (12.5, 12.8)

- Root and child producer ownership, increasing IDs within/across bounded
  batches, empty-seal rules, declaration capacity and covering receipt before
  input. Declaration is not processing admission.
- Immutable membership/seal, incremental whole-scope seal hashing, changed
  replay, late declaration, undeclared input and wrong-seal named refusals.
- Parent admission before descendants; leaf/caller-branch/authority-branch
  modes; atomic one-time child allocation; retry cannot replace a child scope.
- Ordered bounded scope pages, unsealed growth between snapshots, parent and
  producer binding, `more`, and empty pages not proving completeness.

## V2-ADMIT: payload validation and funded acceptance (12.5)

- Strict header length, actual stream identity, attached generation, external
  producer restrictions, configured versioned application contracts and budgets.
- Incremental bytes/hash/FIN validation, empty input, truncation, trailing bytes,
  wrong digest and interrupted reception without losing the declaration.
- Durable immutable input installation before atomic input/job/attempt/deadline/
  child/receipt/accounting commit. No irreversible callback effect beforehand.
- Matching header replay without re-execution; STOP_SENDING alone is not an
  admission receipt. Reserve all promised output/manifest/receipt/closure/control
  and journal capacity, including across restart. Refuse unfunded admission.

## V2-ATTEMPT: execution, retry and publication fences (12.6)

- Explicit allowed states, distinct attempt generations and restartable jobs.
  Callbacks run outside metadata transactions and never block control reading.
- Retry current expected attempt before the original deadline, exactly once
  under operation replay; preserve input/child/membership/policy/deadline.
- Current authority/owner, ancestor fence, revocation, attempt, deadline and
  durable worker lease all checked at publication commit. Race each with a
  staged callback result; restart does not increment the wire attempt.
- Retryable attempt failure versus terminal work failure, named refusal of
  terminal/cancelling/expired/exhausted retry, immutable final outcomes and
  application idempotency/fencing without exactly-once external-effect claims.

## V2-CANCEL: cancellation, skip and descendant settlement (12.6)

- Cancel/publication race in both orders; existing terminal outcome is returned
  unchanged. First accepted cancel/skip fence fixes its eventual outcome.
- Skip policy authorization, disposition/state validation, inputless settlement,
  target SKIPPED with unresolved descendants CANCELLED and no success counting.
- Ancestor fence excludes late declaration/admission/retry/publication before
  bounded batched settlement. Parent stays effectively CANCELLING until its
  descendant scopes close; committed descendant outcomes survive.
- Owner-authorized whole-scope cancellation including producer-1 and empty
  scopes, root cancellation, revocation of unadmitted declarations, restart
  reconciliation and deadline failure without implicit subtree disappearance.

## V2-VIEW: retained observation and revision (12.6, 12.9)

- Immediate snapshot, revision monotonicity, bounded change wait, invalid future
  revision, unchanged view at timeout and consistent immutable identity fields.
- All state-dependent null/required fields, positive admitted attempts, inputless
  cancellation/skip, diagnostics, manifests and independent availability times.
- Retained receipts after output expiry and readable retained outputs after full
  receipt expiry. No guessed state from transport loss or missing payload bytes.

## V2-RESULT: actual outputs, manifests and references (12.7)

- Bounded validated immutable output bytes installed before atomic manifest plus
  success; orphan is not visible; over-budget output cannot become truncated
  success. Zero outputs are allowed and non-success has no success manifest.
- Contiguous indexes, count/byte/object budgets, complete authority/owner/session/
  work/attempt/input/time binding and independent frozen manifest commitments.
- Manifest lookup and full object request/stream correlation, header/length/hash/
  FIN validation; named not-ready/not-found/expired/integrity/unavailable errors.
- Reset/read retry never runs computation. Read pins, pending creation, handles,
  buffers and deadlines are bounded; existing readers survive permitted expiry
  but not indefinitely. A corrupt retained object never triggers an automatic
  rerun or a replacement success manifest.
- URI grammar and numeric boundaries, manifest plus selected-index reference,
  explicit attachment/authentication, expected commitments, trusted authority
  mapping and no implicit redirects, bearer credentials or cross-authority access.

## V2-CLOSE: exact completion and distinct detach (12.8)

- Seal plus all declared members and all descendants terminal/closed; STRICT
  requires successful children for parent rehydration. Missing children, invalid
  input, skip, failure and cancellation must not become successful coverage.
- Four disjoint final counters and declared-count equality, empty scopes,
  immutable status roots, domain-separated leaves/nodes/empty root, odd-node
  duplication and parent/child commitments against frozen examples.
- Checkpoint seal/state/timeout refusals and immutable repeated summary; root
  completed-session DRAIN rejects child cuts, altered summaries and live transfers.
- Core detach drains only the connection, bounds its wait and never claims
  work completion, expiry or cancellation. Abrupt disconnect has the same
  non-effect on accepted work.
- An already accepted parent cancellation/skip fence takes precedence over
  automatic STRICT failure from a cancelled or failed child; descendant closure
  must not overwrite that promised settlement.

## V2-TIME: independent lifetimes and trusted clocks (12.9)

- Exact integer UTC milliseconds, checked arithmetic, session maxima and
  original execution deadlines; retry/replay/reconnect/read never extends them.
- Active input/identity cannot be evicted at a receipt age. Deadline/revocation
  drives authoritative fenced settlement, including after restart.
- Post-terminal output and receipt deadlines are independent; parent-dependency
  and active-read pins can outlive external output availability and stay charged.
- Persisted greatest UTC, backward/untrusted clock refusals, no destructive
  expiry under unsafe time, read-only evidence without fresh leases, documented
  trusted-clock and forward-jump assumptions, and safe restart behavior.

## V2-STORE: crash boundaries and measured resource limits (12.1, 12.5, 12.9)

- Crash both sides of input installation, admission, retry/lease fencing,
  publication, closure and cleanup commits, including lost replies. Retained
  results and anti-reuse identity must survive without phantom work.
- Atomically couple jobs, receipts, fences, summaries and reservations. Preserve
  pins and orphan charges on restart; reconcile before admitting new capacity.
- Safe matched-store ownership, references checked before deletion, replayable
  interrupted cleanup and retirement only after root closure plus every longer
  receipt/output/dependency/read promise. Preserve issuer/owner high-water marks.
- Last-owner lock release must not depend on unrelated inherited descriptors;
  copied nonowner guards must not unlock the original live process's store.
- Actual payloads larger than QUIC windows, slow/stopped consumers, independent
  control progress, saturated workers/storage, per-principal/global counts and
  bytes, file handles, staging, journals and retained promises.
- Measure Rust heap, Java heap, total/native process memory, disk-file lengths,
  actual disk/network I/O and failure-time cleanup separately. A heap gate is
  not an RSS claim and file length is not allocated filesystem blocks.

## Cross-language and workload gates

The independent Rust process driver must run Rust-to-Java and Java-to-Rust
happy-path and failure sequences from these families against real servers,
durable roots and certificates. It must not import either protocol codec as
its acceptance oracle. Exact bytes, authenticated views, named refusals and
persisted/recovered outputs are evidence; process launch or a digest-shaped
placeholder is not.

Task 3 remains separate: an external streaming chunk/transform/distribute/
reassemble application and equivalent streaming-gRPC baseline with matching
authentication, persistence, retry, processing and output guarantees. Retain
commands, pinned builds, raw performance/resource measurements and failure
traces, and feed measured tradeoffs back into the draft.
