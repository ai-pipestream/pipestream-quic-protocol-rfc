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

### Rust retained-result delivery evidence, 2026-09-07

Source: `src/v2/authority/results.rs`; 19 tests including the child-process entry
point in `execution/tests/result_tests.rs`, plus the standalone allocator test
`tests/v2_result_resources.rs`. The focused authority suite now passes 174 tests
and strict workspace clippy passes. The service consumes a host-verified identity;
it does not authenticate a certificate or activate a V2 endpoint.

- V2-RESULT/ATTEMPT: repeated reads and aborted delivery reproduce the exact
  committed bytes without invoking execution, changing the attempt/work revision,
  or replacing the manifest. Tests distinguish zero outputs, a real zero-byte
  output and unsuccessful work; wrong attempt/index, unpublished output, changed
  digest and profile/limit errors retain their named refusals.
- V2-AUTH/TIME: a separate `ReadResult` permission is checked before retained
  lookup and at read commitment. Last-moment withdrawal rolls back the clock and
  releases the pending pin. Tests cover pending/active revocation, cancellation
  preserving earlier successful results, exact output-expiry boundaries and
  continued admitted delivery under later unsafe UTC. A manifest is evidence,
  not a fresh lease; current UTC is required only for new availability grants.
- V2-RESULT/TIME/STORE: pending time, partial/empty progress, idle/lifetime equality
  and incomplete FIN are checked. Only one chunk can be outstanding. A delayed
  maintenance timestamp originally misdiagnosed newer send progress as clock
  regression; the failing regression now passes without extending any deadline.
  A second negative-first regression prevents continual arrivals from starving
  expiry of an older held lease: each maintenance pass fixes its upper bound.
  Bounded maintenance releases stalled tokens' handles; global/per-owner limits
  span service instances, and dropping the last service aborts held transfers.
- V2-STORE: `result_process_death_drops_only_delivery_and_reopens_exact_published_output`
  exits a process before/after the grant's clock commit and after reading a chunk.
  Reopen has no pending delivery but preserves the same work and output bytes.
  Missing/header/body/truncation/trailing-byte corruption refuses
  OUTPUT_UNAVAILABLE without re-execution or a fabricated replacement result.
- V2-STORE: `admitted_result_delivery_needs_no_further_database_writes_or_publication_credit`
  exhausts ordinary capacity behind a pinned WAL reader, including the smaller
  clock-rewrite shape after table inserts refuse. New grants refuse, while the
  existing delivery completes with unchanged 3226016-byte WAL, 299 DB pages,
  work view and reservations under the 4194304-byte WAL cap.
- V2-STORE/RESULT: `thirty_two_mib_result_delivery_has_bounded_heap_handles_and_no_database_growth`
  publishes and reads 33554432 real bytes, checks their SHA-256 with the receiver,
  bounds eight pending reads and a 16384-byte buffer, then expires seven held
  tokens without caller Drop. The focused run measured 6035 bytes of additional
  Rust heap and largest allocation 464 bytes; DB length stayed 69632 bytes with
  no WAL growth. Process RSS/HWM was 7228 KiB, reported separately from Rust heap.
  The delivery phase took 2600 ms on this host. This is not a throughput claim,
  a native-allocation bound or a network/gRPC workload comparison.

Section 12 now explicitly permits retained-manifest evidence without a fresh
availability grant and forbids indefinite pending-read pins or disk/application
buffering as sender idle progress. Frozen wire bytes and authority format 8 are
unchanged. The endpoint must drive maintenance independently, check before writes,
enforce connection limits and bound its transport buffers. Reference-safe
retention/dependency cleanup, real authenticated V2 endpoints, independent Java
V2, the neutral cross-language driver and workload/gRPC evidence remain open.

Logs: `/tmp/pipestream-result-final-authority.log`,
`/tmp/pipestream-result-resources-final.log`,
`/tmp/pipestream-result-final-clippy.log`.

The final full suite (`/tmp/pipestream-result-final-suite.log`, exit 0) passes
536 Rust workspace tests, six Rust example tests, 193 Java tests from 20 fresh
reports, C++ tests, frozen vectors/models, nine basic language pairs, 32 raw QUIC
capability probes and recursive/external examples. The draft build passes with
zero idnits errors/flaws/warnings and the existing informational FIPS downref.
This preserves historical-profile coverage; it does not close the V2 transport
or independent-Java acceptance gates.

### Rust reference-safe retention and accounting evidence

The retention checkpoint adds `authority::reclaim` and startup/admission payload
audits. Thirteen `execution/tests/retention_tests.rs` tests include a subprocess
entry; the existing real branch-reassembly and 32 MiB resource tests also now
run cleanup. These are local Rust storage/library tests, not V2 network tests.

- V2-TIME/V2-STORE: a durable typed intent precedes deletion; directory sync and
  disappearance of input/funded outputs/reservation precede logical quota refund.
  Work views, manifests, operation receipts and status roots remain retained.
  Input can be reclaimed independently of the longer output promise. Equality
  at output expiry is eligible, not before it. Cleanup is repeatable and bounded
  by separate job/file batches; invalid batches and foreign cursors refuse.
- V2-RESULT/V2-ATTEMPT: a read granted before expiry keeps bytes and logical quota
  charged after expiry until it finishes. An old cancelled callback similarly
  pins its input and output reservation; its late write refuses. Active jobs
  and unclosed missing descendants are not collected just because time passed.
  New admitted jobs cannot keep the metadata cursor above an older due job.
- V2-CLOSE/V2-TIME: the actual caller-expanded `ABC` reassembly test now runs
  cleanup after child outputs expire, checks that their inputs are released but
  their outputs remain pinned by the active parent, and then executes the parent.
  Child outputs are released only after that settlement; the parent's output
  obeys its own later expiry.
- V2-AUTH/V2-TIME: local maintenance still runs after owner authorization is
  withdrawn. Unsafe/regressed UTC refuses destructive collection, while an
  accounting audit issues no new time promise and remains permitted.
- V2-STORE: actual process exits occur before/after `retention-intent` and
  `retention-finish` commits, after object unlink and after reservation unlink.
  Exclusive reopen accepts legitimate partially completed deletion, reconciles
  accounting, finishes cleanup and preserves the exact published view/manifest.
- V2-STORE: rebind, executor startup and fresh input refuse missing required
  input, reservation or published output. Reopen with a missing live input does
  not silently gain capacity. The audit streams all jobs and reads bounded
  headers/file lengths, not payload bodies; it is not a constant-time admission
  path or a full body-integrity scan. Storage reads retain their hash checks.
- V2-STORE: format 9 adds the release record and six job rewrite credits. Prior
  format 8 and checksummed early/future/pre-terminal intents refuse. A deliberate
  negative control removing validation of the prior intent failed: it let a
  future intent acquire a new valid timestamp instead of rejecting corruption.
  The restored guard prevents that laundering before any cleanup mutation.
- V2-STORE: a pinned reader exhausts ordinary table inserts and the smaller clock
  rewrite shape. Four distinct input/output intent/finish transactions still
  commit with clock advances, SQL row-update triggers and unchanged 284 DB pages.
  The focused run filled 236 writes; WAL grew from 3052976 to 3085912 bytes under
  the 4194304-byte cap. This is file-length/paired-rewrite evidence, not native
  heap, allocated disk blocks or complete session-retirement funding.
- V2-STORE/RESULT: the 32 MiB test holds a live read during batch-one cleanup,
  verifies only input deletion, then drops the read and deletes the output and
  reservation. The focused cleanup phase measured 1374 bytes of additional Rust
  heap, a largest allocation of 338 bytes, unchanged 69632-byte DB and zero WAL,
  over 102 ms. This covers local library cleanup, not network performance or
  a SQLite/native-memory bound.

Section 12 now explicitly requires recoverable deletion eligibility before
cross-store deletion and prohibits early quota refunds or treating missing live
storage as expired. Payload format 4 and all frozen wire bytes are unchanged.
Session retirement, authenticated V2 endpoints, independent Java V2, the neutral
cross-language failure driver and equivalent workload/gRPC evidence remain open.

Focused logs: `/tmp/pipestream-retention-authority.log`,
`/tmp/pipestream-retention-wal.log`, `/tmp/pipestream-retention-resources.log`,
`/tmp/pipestream-retention-corrupt-intent-red.log` (deliberate negative control).
Captured measurements and reproduction commands are retained in
[`conformance/results/durable-work-v2-retention-2026-09-07.txt`](../../conformance/results/durable-work-v2-retention-2026-09-07.txt).

Final validation: `/tmp/pipestream-retention-full-suite.log`, exit 0, passes
549 Rust workspace tests, six Rust example tests, 193 Java tests from 20 fresh
XML reports (zero failures/errors/skips), C++ checks, frozen vectors/models,
nine language pairs, 32 raw QUIC probes and recursive/external examples.
Formatting and strict clippy pass. The draft build exits 0 with zero idnits
errors/flaws/warnings and the existing informational FIPS downref. These remain
historical-profile network gates, not independent Java or V2 interoperability.

### Rust crash-safe session retirement evidence, 2026-09-07

`AuthorityStore::retire` now commits an immutable eligibility record before
incremental metadata deletion. The closed root, creation receipt and occupied
session slot remain until the final atomic root/proof/session deletion. Global
generation and per-owner creation history are not changed by retirement.
Authority format 10 adds the paired retirement flag and checksummed proof;
format 9 is refused without conversion. Payload format 4 and frozen wire bytes
are unchanged. These are Rust library tests, not authenticated V2 endpoints.

- V2-SESSION/V2-AUTH: `retirement_fences_access_before_bounded_deletion_and_preserves_nonreuse_history`
  checks EXPIRED during partial cleanup, authorization-denial precedence, final
  absence, and non-reusable creation/generation history. Separate tests preserve
  another owner's active session and retain the session quota until final commit.
- V2-TIME/V2-RESULT: `retirement_requires_closed_root_and_the_full_creation_receipt_interval`
  and `longer_output_and_live_read_promises_prevent_retirement_after_receipt_expiry`
  require closed-root receipt retention, every longer output promise and drained
  read pins/accounting. Empty roots and never-admitted SKIPPED work retire without
  inventing job records. Unsafe/regressed UTC, invalid batches and foreign cursors
  refuse before starting or resuming deletion.
- V2-STORE: `actual_process_death_recovers_every_retirement_phase_without_reusing_identity`
  kills a real authority-expanded `ABC` workload before and after each of five
  commit phases: intent, work bundle, nonroot scope, operation, and final session.
  All ten boundaries reopen the paired stores, audit every batch and interleave
  reclamation/settlement before completing retirement and issuing a new identity.
- V2-STORE: `corrupted_retirement_proof_never_authorizes_deletion_or_successful_reopen`
  rejects changed identities, creation/root commitments, premature/future cuts,
  forged credits, rolled-back history, missing flags/proofs and the prior format.
  Rewrites of the immutable proof are refused. Maximum legal identity/summary
  fields fit the fixed 1024-byte body plus 104-byte record header.
- V2-STORE: `pinned_wal_refusal_preserves_retirement_state_then_resumes_after_reader_release`
  exhausts ordinary writes before and after intent. Retirement refuses without
  deleting work, freeing the session slot or altering high-water marks. Both
  focused cases filled 262 writes and kept WAL at 3399056 bytes under a 4194304-byte
  cap. `checkpoint_storage` refuses a pinned reader without waiting; after that
  reader releases, an explicit local checkpoint and retirement complete. Ordinary
  SQL deletion is not falsely charged to fixed-record rewrite credits.
- V2-STORE/RESULT: the 32 MiB resource gate now also runs batch-one retirement,
  verifies each intermediate store, checks old creation replay and reuses the
  released one-session quota with a new generation. Focused retirement measured
  1105 bytes of additional Rust heap, largest allocation 338 bytes, unchanged
  73728-byte DB and zero WAL over 26 ms. This is local Rust allocator/file-length
  evidence, not a native-memory bound or a network/gRPC comparison.

Removing the job-liveness eligibility guard deliberately caused the held-read
regression to fail; the guard was restored before verification. Section 12 now
defines the authoritative retirement cut, partial-cleanup access refusal and
restart distinction from unexplained missing live metadata. Eligibility and
integrity checks stream retained records; a deletion batch bounds mutations and
materialization, not total metadata-scan cost. WAL reclamation is host-driven.

Full validation initially failed the historical application-refusal restart
test with OS error 11. One hundred isolated repeats passed, so repeats were not
accepted as a fix. A deterministic last-Arc destructor test reproduced the same
file-lock failure: the registry discarded a zero-strong-count entry before its
owner released the OS lock. The corrected registry retains that entry through
unlock, coordinates local reopeners with a bounded condition-variable wait, and
preserves external/live-owner exclusion. The regression checks two reopeners
share one root and another root progresses while this finalizer is paused.
All 56 focused retained-root tests pass. This fixes an independently reproduced
race consistent with the initial error; the original failure had no backtrace
identifying its exact failing call.

Focused captures and commands:
[`durable-work-v2-retirement-2026-09-07.txt`](../../conformance/results/durable-work-v2-retirement-2026-09-07.txt).
Authenticated V2 endpoints, independent Java V2, neutral cross-language failures
and the complete external workload/gRPC comparison remain open.

Final full-suite validation: `./conformance/run_all.sh`, exit 0, captured in
`/tmp/pipestream-retirement-full-suite-final.log`. All 562 Rust workspace tests,
six Rust example tests and 193 Java tests from 20 freshly written XML reports
pass, with no failures/errors/skips. Strict formatting/clippy, frozen vectors,
bounded models, C++ checks, nine black-box language pairs, 32 raw QUIC probes and
the recursive/external examples pass. This preserves existing-profile network
coverage; it does not close the still-unimplemented V2 network/Java gates.
`./build.sh core 05` also exited 0 (`/tmp/pipestream-retirement-draft-final.log`);
the rendered retirement paragraph was inspected. Idnits reports zero errors,
flaws and warnings, with the existing informational FIPS downref comment.

### Rust V2 TLS and current-credential boundary, 2026-09-07

`quinn/src/v2_tls.rs` now supplies V2-only TLS configuration, completed
server-handshake peers and per-request credential revalidation. Sixteen
`v2_tls::tests` cases cover the constructors and real Quinn/rustls connections.
Credential decisions do not use a supplied mock authentication Boolean.
Capability selection is then checked using the verified
peer result. These are TLS/library tests, not the complete V2 request dispatcher
or independent Java authentication evidence.

- V2-AUTH/NEG: rotated certificates map to the same stable owner. Anonymous
  and valid-unmapped peers can only authorize Core; a required durable profile
  refuses UNAUTHORIZED. Mapped expired/future/wrong-usage/untrusted certificates
  fail actual TLS validation with CRYPTO_ERROR, before application negotiation.
- V2-AUTH: every guard call rechecks certificate validity, trust and current
  mapping. Owner/authority changes, one/all removed mappings and changed roots
  refuse without live identity replacement or anonymous fallback. A serialized
  invalidation latch prevents a later check resurrecting that connection.
- V2-AUTH/TIME: tests cover exact certificate validity endpoints, unavailable
  and regressed time, recovery of safe time before credential expiry, and
  permanent refusal after expiry. Known missing time before TLS produces local
  CLOCK_UNSAFE and wire CONNECTION_REFUSED. Time lost during TLS never produces
  an application peer. At this checkpoint the pinned stack reported
  PROTOCOL_VIOLATION for that local failure; the following alert-mapping
  checkpoint fixes its categorization. The first clock test exposed this
  distinction directly rather than treating failing closed as sufficient.
- V2-AUTH/NEG: real clients reject incorrect DNS names, IP identities, trust roots
  and legacy ALPN. A Core-only server never fabricates a client principal when
  a configured client credential was not requested by TLS.
- V2-AUTH: a resumption-enabled client receives no tickets from the public server;
  another connection after credential expiry requires a full TLS failure.
  Independently, the public client requires fresh TLS even when the other server
  offers tickets and early data. It cannot rely solely on the server factory's
  no-ticket behavior. No resumption/early-data fallback is performed.
- V2-STORE/NEG: local certificate/mapping bounds and an oversized peer certificate
  inventory refuse before application dispatch. Peer capture allows at most 16
  certificates/65535 DER bytes; this is not a process-memory or connection-flood
  resource gate. The enclosing server still must reserve bounded connection slots.

Three deliberate negative controls failed: removing live certificate verification
accepted an expired owner; enabling server ticket storage/issuance delivered two
tickets instead of zero; removing client resumption disablement resumed TLS
instead of reaching the required fresh-certificate handshake failure. All guards
were restored. Focused test/clippy logs and captured failures are listed in
[`durable-work-v2-tls-2026-09-07.txt`](../../conformance/results/durable-work-v2-tls-2026-09-07.txt).

The validity endpoint check follows
[RFC 5280 Section 4.1.2.5](https://www.rfc-editor.org/rfc/rfc5280.html#section-4.1.2.5),
independently of the V2 stream deadline rule. TLS alert transport mapping follows
[RFC 9001 Section 4.8](https://www.rfc-editor.org/rfc/rfc9001.html#section-4.8);
pre-handshake connection refusal uses
[RFC 9000 Section 20.1](https://www.rfc-editor.org/rfc/rfc9000.html#section-20.1).
No normative wire bytes, authority storage or existing profile behavior changed.
Appendix D now records the actual Rust library/TLS progress and keeps complete
V2 dispatch, Java parity and workload evidence explicitly open.

Final regression validation: `./conformance/run_all.sh` exited 0 with 578 Rust
workspace tests, six Rust-example tests, 193 Java tests across 20 fresh XML
reports (zero failures/errors/skips), frozen vectors/CDDL, all three bounded
models, C++/CTest, nine existing interop pairs, 32 raw capability probes and the
recursive/external examples. `./build.sh core 05` exited 0; the rendered
implementation-status paragraph was inspected, and idnits reports zero
errors/flaws/warnings plus the existing FIPS downref comment. The existing
end-to-end pairs remain V1 regression evidence, not V2 interoperability proof.

### Rust V2 local TLS error mapping, 2026-09-07

The strengthened mid-handshake clock test first failed on the actual
PROTOCOL_VIOLATION close. A private client/server adapter now maps only the
pinned Quinn/rustls `read_handshake` alertless TLS error to fatal
`handshake_failure` (QUIC 0x128), as allowed by
[RFC 9001 Section 4.8](https://www.rfc-editor.org/rfc/rfc9001.html#section-4.8).
It preserves existing TLS alerts and all other error codes, never parses error
strings and never replaces unavailable time with stale time. There is no
application negotiation after this failure. The same Quinn dependency version
already in every lockfile is now explicitly pinned for this integration boundary.

Nineteen TLS/security tests pass. Both client and server clock-failure tests
check the local error and the peer-observed CONNECTION_CLOSE. A hostile real
QUIC client sends a syntactically valid but incorrect initial source connection
ID; its transport-parameter failure remains TRANSPORT_PARAMETER_ERROR, not a
TLS error. A local exhaustive check preserves all 256 existing TLS alert codes.
This closes the specific categorization follow-up above, not the complete V2
authentication/dispatch, Java or resource acceptance families.

Final validation: `./conformance/run_all.sh` exited 0 with 581 Rust workspace
tests, six Rust-example tests, 193 Java tests in 20 fresh XML reports with no
failures/errors/skips, frozen vectors/CDDL, all three bounded models,
C++/CTest, nine existing interop pairs, 32 raw capability probes and all
recursive/external examples. `./build.sh core 05` exited 0 with zero idnits
errors/flaws/warnings and the existing FIPS comment. Captured red/green and
regression results are in
[`durable-work-v2-tls-alerts-2026-09-07.txt`](../../conformance/results/durable-work-v2-tls-alerts-2026-09-07.txt).
Existing end-to-end pairs remain V1 evidence, not complete V2 interop.

### Rust V2 owned TLS configuration and Core server, 2026-09-07

`v2_core::Server` is now a real, bounded Core-only Quinn listener, not an
advertisement of either incomplete durable profile. Its 14 Core tests include
configuration refusals and 13 real-QUIC cases with a raw client control reader
distinct from the server's framing code. The TLS
boundary has 20 tests. These are local Rust tests, not Java V2 interoperability
or the neutral multi-process failure driver's completed acceptance gates.

- V2-AUTH: `ServerSecurity::accept` selects its own configuration explicitly.
  The regression first failed when a no-client-auth endpoint default silently
  produced an anonymous peer instead of the mapped owner. Now valid certificates
  map correctly and invalid presented certificates fail TLS under that same
  stale listener default. The independent ticket-offering peer was retained;
  removing client resumption disablement still makes its test fail.
- V2-WIRE/NEG: real negotiation checks minima and required profiles. Missing
  identity with required durable work closes UNAUTHORIZED; an authenticated
  caller requiring the unimplemented profile closes EXTENSION_UNSUPPORTED.
  No capabilities acknowledgment is sent in either case. Unknown type classes,
  duplicate capabilities, wrong direction, noncanonical values, oversize
  prefixes, truncation and invalid request IDs have their named fatal scope.
- V2-WIRE/CORE: ignorable frames are incrementally discarded using 4096 bytes.
  A 128 KiB frame traverses the 64 KiB QUIC receive window and preserves the
  following detach frame's alignment. Core alone grants no object/second-control
  stream credit. Reset and STOP_SENDING on either control direction close with
  CONTROL_RESET; incomplete headers and blocked response writes are bounded.
- V2-AUTH/STORE: the global connection check includes retained QUIC connections
  as well as owned tasks; stable-principal and anonymous quotas are independent.
  Rotated credentials share the owner's quota, without blocking another owner.
  A deliberate quota off-by-one caused the named-refusal test to time out.
  Known raw parser-buffer budgets and QUIC queues/windows are finite; this does
  not close the whole-process heap/RSS/CPU resource-measurement requirement.
- V2-CORE/TIME: a non-reading client advertises only 4096 bytes of receive credit
  before TLS, then floods requests deliberately beyond its pending allowance.
  Its blocked output is bounded while a second peer can detach. Live credential
  expiry is checked per request and cannot resurrect after a clock reset.
- V2-CLOSE: detach does not claim root closure. Later otherwise valid control
  requests receive NOT_READY, while wrong correlation remains fatal. Section
  12.8 now names that previously unspecified post-detach refusal. Shutdown
  closes active connections without manufacturing any completion response.

Remaining integration: the public V2 client and standalone commands, authenticated
durable/input/result dispatch, durable uncertainty journals, independent Java,
neutral cross-language restart/failure cases, and the external equivalent-gRPC
workload. No frozen wire bytes or authority storage format changed here.

Final validation: `./conformance/run_all.sh` exited 0, with 596 Rust workspace
tests, six Rust-example tests, 193 Java tests in 20 fresh XML reports (no
failures/errors/skips), frozen vectors/CDDL, all three bounded models,
C++/CTest, nine existing interop pairs, 32 raw capability probes and all
recursive/external examples. The existing end-to-end pairs remain V1 evidence.
`./build.sh core 05` exited 0; the rendered detach and implementation-status
paragraphs were inspected. Idnits reports zero errors/flaws/warnings and the
existing FIPS comment. Red/green and final results are captured in
[`durable-work-v2-core-2026-09-07.txt`](../../conformance/results/durable-work-v2-core-2026-09-07.txt).

### Rust authenticated durable control adapter, 2026-09-07

`quinn/src/v2_authority.rs` and `v2_authority/requests.rs` implement durable
control dispatch to the existing real authority, not another database or a
simulated execution path. The public Core listener still does not use this
adapter or advertise either durable profile. Run:

```sh
cd implementations/rust-quinn
cargo test --locked -p pipestream-quinn v2_tls::tests::authority -- --nocapture
```

Twelve tests in `v2_tls/tests/authority.rs` and `authority/results.rs` use real
authenticated TLS peers and guarded SQLite/payload roots. The durable control
calls and result reads are local API calls, not wire or independent-driver tests.

- V2-SESSION/AUTH/OP: `dispatcher_creation_replay_attachment_and_single_binding`
  covers lost response recovery with a rotated certificate, one in-flight or
  installed binding per connection, ownership denial and retry after failed
  attachment. Configuration requires mapped identity and a matching issuing
  authority. Separate tests preserve the profile combination on attach, refuse
  unselected result access, and recheck credential/policy/revocation before reads.
- V2-NEG: fatal wrong direction, duplicate/decreasing/incorrect-first request
  IDs and repeated capabilities; valid refusals consume IDs. Pending accounting
  includes unsent responses, active result reads, and cloned input-job handles.
- V2-VIEW/CLOSE: revision waiting releases metadata capacity, permits cancellation
  to proceed, returns an unchanged view at timeout, and rejects an ahead-of-state
  revision. Missing obligations yield checkpoint WAIT_TIMEOUT; reconciliation
  produces the actual cancelled scope summary. A 30000-ms snapshot with revision
  zero returns immediately. Complete drain compares the whole committed root,
  refuses a child cut, excludes pending input/result transfers, and holds its
  connection cut until the response is released after sending.
- V2-ATTEMPT/RESULT: the result test stages and admits 12 KiB, retries through the
  dispatcher and checks identical replay, then runs the real streaming copy
  callback. Manifest lookup and bounded actual result reading match the admitted
  bytes and attempt 2. Wrong digest/attempt have specific refusals; reading or
  abandoning another read does not change the committed work view. Cancellation
  after publication preserves the terminal result. Separate scope cancel/skip
  requests settle to one CANCELLED and one SKIPPED member, with replay receipts.
- V2-NEG/CLOSE: detach waits for existing work, refuses later controls, and closes
  with a fatal error when its lifetime expires. A deliberately paused real
  storage operation keeps both metadata capacity and the connection pending slot
  after its async waiter is cancelled. Another connection observes capacity
  refusal; detach cannot finish early. Once the operation commits, replay recovers
  the same generation. Removing its captured ticket makes this test fail because
  detach responds while the commit is still running.

Removing the exact-root comparison also makes its test fail: an altered closure
timestamp receives a completed response instead of CONFLICT. Both deliberate
faults were restored. Section 12.8 now explicitly includes other control requests
in the completion cut and requires that cut to remain stable through sending the
response; intervening requests may receive NOT_READY. No frozen wire bytes,
CDDL, storage format or dependency changed.

The first full-suite run exposed a real dispatcher contention error: a later
watch poll used new-request admission and returned LIMIT_EXCEEDED while a
cancellation occupied the only metadata slot. The regression now deliberately
holds that transaction; it fails with the previous poll logic. Accepted waiters
now fairly reacquire metadata slots within their wait budget, keeping their
existing pending-request charge. Expiry returns the last consistent work view
or checkpoint WAIT_TIMEOUT, not a new summary or a capacity refusal from a poll.
The cancellation/watcher case passes after this change. Initial admission of a
new request can still receive LIMIT_EXCEEDED when the metadata ceiling is full.

Still required: connect the adapter, input and result I/O to the public V2
listener with independently bounded control/data progress; maintain result-read
deadlines and lifecycle workers; implement the V2 client and uncertainty journal;
implement independent Java and both-language failure testing; finish the original
external workload and equivalent streaming-gRPC comparison. Metadata slot counts
are not measured heap/RSS, disk-I/O or complete endpoint resource evidence.

Final validation: `./conformance/run_all.sh` exited 0 after the contention fix:
608 Rust workspace tests, six Rust-example tests, 193 Java tests in 20 fresh
XML reports (no failures/errors/skips), frozen vectors/CDDL, three bounded
models, native checks, nine existing interop pairs, 32 capability probes and
all examples. `./build.sh core 05` exited 0; the rendered completion-cut and
adapter-status paragraphs were inspected, with zero idnits errors/flaws/warnings
and the existing FIPS comment. Results and failing-before-fix evidence are in
[`durable-work-v2-dispatch-2026-09-07.txt`](../../conformance/results/durable-work-v2-dispatch-2026-09-07.txt).

### Rust authenticated QUIC input adapter, 2026-09-07

`v2_authority::input` now carries actual client unidirectional streams into the
existing durable receive/prepare/admit pipeline. It accepts only a stream from
its `Connection` peer and checks the authority instance before accepting. It
remains separate from the public Core listener, with no additional advertised
profile. Its eight input tests use real TLS/QUIC and guarded storage, but their
control operations are local adapter calls, not durable wire interoperability.

- V2-WIRE/ADMIT: a 64 KiB object crosses 4 KiB stream and 16 KiB connection
  windows, commits one real receipt/attempt and later produces matching bytes
  through the actual copy executor. The reply tag uses the real stream ID.
  A repeated immutable header receives the same receipt without body or FIN,
  STOP_SENDING 0, and no revision or attempt change.
- V2-WIRE/AUTH/ADMIT: zero/oversized/truncated header prefixes and malformed
  CBOR refuse FRAME_ERROR. Missing/trailing/wrong-hash bytes refuse
  INTEGRITY_ERROR. Stale generation, external producer 1 and unknown application
  get their named errors without retaining the operation or losing declaration.
  An unbound connection refuses NOT_READY. Empty input remains DECLARED until
  actual FIN, then admits with the real empty-string digest. Configuration
  limits and independently owned authority quota domains are checked before I/O.
- V2-NEG/TIME: a stalled header is bounded by one whole-header deadline; a
  rotated certificate cannot evade its owner's active-input quota. Local control
  metadata still progresses while that input waits. A stopped payload and a
  continuously progressing payload both expire without admission; progress does
  not extend absolute lifetime. This is not the integrated endpoint's reserved
  QUIC control-credit test, which remains required.
- V2-STORE/CLOSE: a deliberately held file preflight survives cancellation of
  its async receiver, keeps owner quota occupied, and prevents detach. Releasing
  the file worker allows abandoned-stage cleanup, then detach and quota reuse;
  no input operation or attempt was committed. File I/O uses fixed native
  workers separate from control metadata capacity. File-owning returned values
  retain their transfer lease through deferred destruction on that pool.
- V2-STORE: a separate worker drop test checks that file cleanup happens on a
  file worker before releasing its pin. Replacing deferred cleanup with direct
  destruction makes this test fail on the executor thread assertion (exit 101).
  The guard was restored before final validation. This guards an actual eager
  staged-file removal and directory-sync path, not merely a mock job count.

The configured active-input ceiling funds two queue slots per live input: at
most one ordinary job and one deferred destructor, both retaining that lease.
The header body is at most 4096 bytes; the reused payload buffer is at most
16 KiB and bounded by the payload store's chunk limit. These construction
bounds and tests are not measured heap/RSS, native transport allocations,
disk/network I/O or complete endpoint resource evidence. Storage formats,
dependencies, normative wire text and frozen examples are unchanged.

Focused checks: 55 V2 transport/security tests pass, including eight input and
one worker cleanup test; strict clippy passes. Public durable listener/result
I/O, lifecycle/read maintenance, client uncertainty journals, independent Java,
neutral cross-language failures and the original external workload/gRPC
comparison all remain required. Full-suite and draft results are recorded in
[`durable-work-v2-input-2026-09-07.txt`](../../conformance/results/durable-work-v2-input-2026-09-07.txt).

Final `./conformance/run_all.sh` exited 0: 617 Rust workspace tests, six Rust
example tests, 193 Java tests in 20 fresh XML reports (zero failures/errors/skips),
frozen vectors/CDDL, all three bounded models, native checks, nine existing
interop pairs, 32 raw capability probes and all examples. The full language pairs
remain V1 evidence. `./build.sh core 05` exited 0, and the rendered Appendix D
status was inspected, with zero idnits errors/flaws/warnings and the existing
FIPS downref comment.

### Rust authenticated result streams and blocked-input deadlines, 2026-09-07

`v2_authority::output` sends actual retained objects on the authenticated peer's
server-initiated QUIC streams. Ten output tests use real TLS/QUIC, on-disk
admission and the actual copy executor. Control requests are still local adapter
calls; neither adapter activates a profile on the public Core listener.

- V2-RESULT/WIRE: empty and 64 KiB outputs carry the exact request/session/work/
  attempt/index/length/digest header and FIN across 4 KiB stream/16 KiB connection
  receive windows and an 8 KiB server send window. Repeated reads return the same
  bytes without changing the committed work view, revision or attempt.
- V2-RESULT/TIME: a stopped reader aborts delivery and can request the same object
  again. A non-reading peer is reset LIMIT_EXCEEDED after its header, without a
  second control response. Zero available stream slots produce a bounded control
  refusal before any header. Exact library deadlines include pending time, ignore
  empty progress and stop at the original lifetime. The new `next_deadline`
  accessor exposes that exclusive bound without granting I/O or renewing it.
- V2-AUTH: a blocked sender rechecks current TLS credentials before scheduling
  more bytes, including after Quinn wakes it. Removing its central live check
  makes the expiry test fail with the later LIMIT_EXCEEDED instead of UNAUTHORIZED;
  the guard was restored. Wrong result commitments refuse before stream creation.
- V2-RESULT/STORE: a real retained output's final byte is changed after publication.
  The sender emits the committed header and provisional bytes, then resets
  OUTPUT_UNAVAILABLE instead of successful FIN. Work view/manifest/attempt remain
  unchanged. Reading never invokes execution as a repair for missing or bad data.
- V2-NEG/STORE/CLOSE: configured global output capacity spans distinct authenticated
  owners; certificate rotation cannot evade owner capacity. Connection result
  stream limits count pending creation and an unsent refusal. Cancelled file
  preflight retains owner capacity and prevents detach until the job and cleanup
  finish. Capacity reuse is checked after cleanup, not assumed synchronous with
  dropping the async response. Invalid configuration/authority pairing refuses
  before any file acquisition.

The input review found that awaiting a blocked preflight could postpone its idle
timeout. A new test failed on that exact held-worker case. Preflight and chunk
waits now time out independently while their underlying jobs retain quota and
pending tickets; cleanup must still finish before detach. The possible admission
commit is not given a false pre-commit timeout response. There are now nine input
tests plus the worker destructor test.

An earlier dispatcher test also assumed its first watch snapshot had released
the metadata slot after a fixed sleep. Under contention the new cancellation
could correctly refuse LIMIT_EXCEEDED, so the test never reached its intended
held-transaction condition. The test now retries only that pre-start refusal
with a fresh request ID and the same immutable operation, and observes the actual
transaction entry before checking that the existing watch remains pending. No
production metadata-admission or watch rule was relaxed.

Focused validation passes 66 V2 transport/security tests and the exact local
result-deadline test; strict clippy passes. File pools, queues, application buffers
and request counts are bounded by construction, not claimed as measured whole-
process memory or network/disk I/O. The complete endpoint must still reserve
control credit, wire lifecycle/read maintenance and provide shutdown, client
uncertainty journals, independent Java, neutral failures and the original external
workload/equivalent streaming-gRPC measurements. Full validation evidence is in
[`durable-work-v2-output-2026-09-07.txt`](../../conformance/results/durable-work-v2-output-2026-09-07.txt).

Final `./conformance/run_all.sh` exited 0: 628 Rust workspace tests, six Rust
example tests, 193 Java tests in 20 fresh XML reports (zero failures/errors/skips),
frozen vectors/CDDL, all three bounded models, native checks, nine existing
interop pairs, 32 raw capability probes and all examples. The full language pairs
remain V1 evidence. `./build.sh core 05` exited 0; rendered Appendix D was inspected,
with zero idnits errors/flaws/warnings and the existing FIPS downref comment.

### Rust connection-level control reservation, 2026-09-07

`quinn/src/v2_flow.rs` now owns outgoing stream admission for authority result
writers and their control writer. Nine actual-QUIC flow tests plus two new
result-adapter tests cover:

- V2-STORE: data cannot consume the control-only local send allowance; a failed
  control poll restores the lower data window before releasing the mutex.
- V2-RESULT: a stored 64 KiB output blocks on the receiver while an actual
  control response crosses the same connection. The result then finishes with
  unchanged committed work state. Wrong-TLS-connection flow owners are refused.
- V2-STORE: control progresses with all data receive windows full. The explicit
  unsafe-peer counterexample cannot progress until data consumption returns
  connection credit. Local send admission cannot create remote credit.
- V2-STORE: `batched_connection_credit_updates_cannot_spend_the_control_reservation`
  failed with only `(N+1)*W` receive credit. Quinn's independently batched stream
  updates spent the control reserve before MAX_DATA was due. Accounting for its
  R/8 update threshold fixes the observed deadlock. Section 12.1 now requires
  preserving the reservation during replenishment and stream replacement.
- V2-STORE: `blocked_control_registers_retry_independent_of_data_credit` failed
  before adding a control-owned retry timer. Ordinary Quinn writable events use
  the restored data window. The bounded retry wake rechecks control's allowance
  without depending on data credit or spawning another task.
- V2-WIRE/NEG: role-specific bidi credit, actual Control Stream 0 checks,
  consumed/reset stream replacement, and checked byte/count ceilings.

The deliberately disabled send-window restoration also fails its regression;
the guard is restored. All 77 focused V2 tests and strict workspace clippy pass.
Wire bytes, CDDL, storage and dependency versions are unchanged. These remain
local-dispatch/real-object adapter tests, not full V2 endpoints, Java V2 or neutral
cross-language failure evidence. Process-resource measurements, client journals,
runtime maintenance and the original workload/baseline remain open. Evidence:
[`durable-work-v2-flow-2026-09-07.txt`](../../conformance/results/durable-work-v2-flow-2026-09-07.txt).

The full suite exited 0 with 639 Rust workspace tests, six Rust-example tests,
193 Java tests in 20 fresh XML reports (zero failures/errors/skips), all three
bounded models, frozen vectors/CDDL, native checks, nine existing interop pairs,
32 raw capability probes and all examples. The draft build also exited 0;
rendered Section 12.1 and Appendix D were inspected. Idnits reports zero
errors/flaws/warnings and the existing FIPS downref comment.

### Rust authority execution/maintenance runtime, 2026-09-07

`Authority::start_runtime` now owns execution and three independent native
maintenance loops for read leases, retention and retirement. It is not yet
connected to a public durable listener. Three local-dispatch/real-QUIC-input
runtime tests and one failure-classification unit test add evidence for:

- V2-ATTEMPT/STORE: discovery of previously admitted work without resubmission,
  actual copy execution and background sealed closure.
- V2-RESULT/TIME: background read expiry while the one callback worker is held;
  the closed lease is observed before another foreground check can expire it.
- V2-STORE/TIME: unsafe clock pauses destructive maintenance without treating
  it as a fatal runtime error; safe-time restoration and read-pin release permit
  retirement, with owner creation history still refusing reuse.
- V2-STORE: invalid batch/interval refusal and duplicate worker ownership.
  Only clock/capacity and typed SQLite busy/locked errors are retried; corruption
  and arbitrary I/O failures are not parsed as transient text.

Three core regressions exposed and cover runtime integration defects:
`worker_pool_stop_request_does_not_wait_for_a_discovery_state_lock` failed before
the atomic stop signal; `executor_accepts_retained_durable_only_work_without_adding_result_delivery`
failed when deployment capabilities replaced the retained session's profile
combination; bypassing the new startup barrier makes
`failed_worker_pool_startup_never_dispatches_an_application_callback` fail.
All guards are restored. Focused checks pass 81 V2 transport/runtime tests,
90 execution tests and strict workspace clippy.

Stop and health observation are nonblocking. Explicit joins remain blocking;
in-flight callbacks/I/O retain roots and resource pins. The completion flag
describes execution/maintenance threads, not connection metadata or input/output
file jobs. Existing full accounting and retirement eligibility scans are not
bounded by the cursor step count. Public supervision/shutdown, complete client
journals, independent Java V2, neutral failures, resource gates and the original
workload/baseline remain due. No partial durable profile is advertised. Evidence:
[`durable-work-v2-runtime-2026-09-07.txt`](../../conformance/results/durable-work-v2-runtime-2026-09-07.txt).

Full repository conformance passed 646 Rust tests, 6 external Rust example tests,
193 Java tests in 20 fresh XML reports, all bounded models/vectors/CDDL, native
checks, nine V1 interoperability pairs and 32 raw capability probes. A final
whole-workspace Rust/clippy rerun passed after test-only pin/snapshot refinements.
The rebuilt draft's runtime status was inspected; idnits reported zero
errors/flaws/warnings and its existing FIPS comment.

### Rust public durable listener, 2026-09-07

`v2_authority::server::Server` now connects authenticated negotiation to the actual
authority, input/result file pools and execution/maintenance runtime. It is an
embeddable Rust listener, not a V2 CLI/client pair or an independent-language
implementation. Fifteen tests send actual control/input/result streams rather
than calling the local dispatcher. Two unit tests check configuration budgets
and destruction accounting for aborted child futures. Coverage includes:

- V2-NEG/AUTH: optional anonymous/unmapped Core versus required durable refusal;
  identity/store validation before capability acknowledgment; rotated credentials
  share their stable owner's quota while anonymous Core remains available.
- V2-WIRE/NEG: actual partial control frames survive unrelated input completion;
  first-byte frame deadlines, malformed types/lengths/canonical forms, direction,
  repeated IDs and stopped control directions retain named fatal errors.
- V2-ADMIT/RESULT: empty/64 KiB inputs and copy outputs, actual stream tags,
  invalid input preserving its declaration, immutable repeated reads, manifest
  lookup, and control progress while a result exceeds an unread receive window.
- V2-VIEW/CLOSE: out-of-order revision waits and other responses; the full
  30000 ms wait returns unchanged DECLARED/revision 1 with transport keep-alives,
  not fabricated processing progress. Exact root completion refuses live result
  transfers; detach drains prior work and pipelined NOT_READY refusals.
- V2-SESSION/STORE: reconnect with rotated credentials and exclusive close/reopen
  of real authority/payload roots preserves creation, view, attempt and output.
  This is not a cross-process kill/recovery test.
- V2-STORE/CLOSE: metadata saturation produces named refusal on a live control
  stream. Shutdown grace expiry reports the still-running metadata commit and
  does not undo it. Owned child futures are counted until their held resources
  are destroyed, separately from blocking metadata/file jobs and runtime threads.

Two half-close tests failed before their respective fixes: the new durable
listener's Core-fallback case and
`core_half_close_preserves_detach_and_pipelined_refusal_bytes` in the existing
Core-only listener. Immediate QUIC close discarded queued responses. Both paths
now finish control and await its acknowledgment under a bounded deadline.
Section 12.8 clarifies that transport acknowledgment is not proof of application
validation or persisted recovery evidence. A non-reading-peer test also exposed
a writer timeout incorrectly relabeled CONTROL_RESET after its response channel
closed; the writer now preserves LIMIT_EXCEEDED before dropping that channel.

Input tasks have their own negotiated stream-count ceiling, including refused
inputs that hold no admission slot. Control response queues and connection-owned
send reservation remain separate from execution/file workers. Raw encoded-state
and transport-credit products have independent 128 MiB configuration ceilings;
neither is measured process heap/RSS/native memory. The bounded models and V1
interoperability checks remain regression evidence, not independent V2 endpoint
proof. Evidence: [`durable-work-v2-server-2026-09-07.txt`](../../conformance/results/durable-work-v2-server-2026-09-07.txt).

Still required: independent Java V2, complete client/CLI uncertainty journals,
the neutral cross-language process-failure oracle, full resource gates and the
original external workload/equivalent streaming-gRPC comparison. This listener
checkpoint does not complete the full goal.

Final source verification passed 664 Rust workspace tests, formatting and
strict clippy. The preceding full repository run passed 662 Rust tests before
the final two listener regressions, six external Rust example tests, 193 Java
tests from 20 fresh XML reports, bounded models, frozen vectors/CDDL, native/C++
checks, nine V1 interop pairs, 32 raw capability probes and all examples. The
remaining source changes were Rust-only. Draft rebuild and inspection passed;
idnits has zero errors/flaws/warnings and its existing FIPS comment.

### Rust client creation/intent journal, 2026-09-07

`v2::client::Journal` durably stores one configured creation/session and immutable
mutation intent before transmission. The same guarded SQLite backend bounds file
lengths; journal-specific operation and image ceilings bound retained inventory.
Reopen audits each retained operation without a whole-history buffer. Original
operation identity/parameters survive matching replay and reconnect with new
connection request numbers. Receipt storage checks the session-bound digest,
typed outcome and known request constraints; TLS authentication and full scope/
result validation remain separate mandatory client responsibilities.

Thirteen substantive storage tests plus a subprocess entry point cover V2-SESSION,
V2-OP and V2-STORE: exclusive reopen, wrong owner/profile/binding, every mutation
kind, changed intent/receipt, concurrent preparation, bounded unresolved pages,
cursor exhaustion, corrupted images, failed receipt commits and a real 64 KiB
database/WAL cap. A killed subprocess leaves committed intent without a receipt;
recovery retrieves the same unresolved operation. Deliberately removing the
intent commit makes that test fail with missing intent; the commit is restored.
SQL write failure does not falsely mark a receipt as durably observed.
An incompatible-format reopen regression reproduced an unintended journal-mode
change before refusal; format and identity preflight now precede WAL configuration.

Two additional actual QUIC tests use an exclusively reopened client journal:
creation and declaration replies received but not durably recorded replay the
same identities, and operation lookup recovers an unrecorded input admission
before reading the original attempt's real output. Certificate rotation keeps
the same mapped owner. These tests share Rust wire codecs; they are not the
neutral, cross-language authority/client kill oracle required by the goal.

This is creation/intent/receipt persistence, not complete client conformance.
The production event loop and CLI must still own bounded asynchronous storage,
correlation and stream I/O, enforce declaration coverage before input, validate
and persist work/coverage observations, and retain authenticated manifest/index
references. A journal does not infer terminal work from transport failure,
NOT_FOUND or absence of a local receipt. Independent Java, measured process
resource gates and the original external/equivalent gRPC workload remain open.
Evidence: [`durable-work-v2-client-journal-2026-09-07.txt`](../../conformance/results/durable-work-v2-client-journal-2026-09-07.txt).

Final checks pass: 680 Rust workspace tests and strict clippy. The full repository
run passed its 679-test Rust snapshot before the last reopen regression, six
external Rust-example tests, 193 Java tests in 20 fresh XML reports, all bounded
models, frozen vectors/CDDL, native/C++ checks, nine V1 interop pairs, 32 raw
capability probes and all examples. No non-Rust source changed afterward. Draft
rebuild and rendered inspection pass with zero idnits errors/flaws/warnings and
the existing FIPS comment.

### Rust client work observations and result references, 2026-09-07

`v2::client::Journal` now persists authenticated work observations and full
manifests with explicit selected output indices. Transport authentication and
correlation still precede these blocking APIs. Normalized SQLite target indexes
select matching immutable typed receipts; they are checked against checksummed
CBOR records, not an opaque server-state snapshot.

- V2-OP/ATTEMPT/VIEW: compare admission fields, retry replacement/commit time and
  cancellation/skip disposition with known evidence, independently of reply
  arrival order. Refuse conflicting terminal states, attempts and immutable
  manifests without inventing revisions. Stale compatible views return the newest
  durable observation. Awaiting-retry/cancellation cannot resume the fenced attempt.
- V2-VIEW/TIME/RESULT: validate issuing identity, root producer, exact retained
  policy intervals, original execution deadline and known output budget. Empty
  successful manifests and unadmitted cancellation remain valid. A retained
  manifest does not grant fresh availability or prove object-byte validation.
- V2-RESULT/STORE: atomically retain full manifest plus selected index; restored
  requests use retained issuer/owner/generation/attempt/digest, never credentials
  or endpoint configuration inferred from URI text. Manifest-only storage does
  not implicitly choose an output. The real-QUIC recovery test reopens this
  reference, reconnects with rotated credentials and verifies original bytes and
  unchanged terminal revision.
- V2-STORE: failing SQLite inserts roll back view/manifest/selection together.
  Independent count ceilings refuse without eviction. Changed indexes, corrupt
  images and missing backing manifests refuse reopen. A forced-kill child proves
  post-commit observation/selection recovery. A large manifest hits an actual
  64 KiB physical cap while preserving the earlier view/reference and file bounds.

The new fence/manifest arrival-order regression first failed because a contradictory
receipt was accepted. It now passes in both directions. An older typed-receipt
fixture also attempted both accepted cancel and accepted skip for one work item;
it now represents the valid skip disposition reporting pre-existing CANCELLED.
Local client format 2 refuses format 1, without conversion or loss of old history.
No normative wire, authority format or dependency changes were required.
Evidence: [`durable-work-v2-client-observations-2026-09-07.txt`](../../conformance/results/durable-work-v2-client-observations-2026-09-07.txt).

Still open: production client transport/CLI and asynchronous journal ownership,
complete scope membership/status coverage validation, independent Java V2, neutral
cross-language process failures and measured whole-process/workload comparison.
These tests do not claim that broader conformance or measured RSS/heap bounds.

### Rust client scope membership and closure, 2026-09-07

`v2::client::Journal` now stores bounded scope identities/member snapshots and
verified bottom-up coverage. All APIs still require authenticated, correlated
transport evidence and blocking storage ownership outside the control reader.

- V2-VIEW/CLOSE: merge out-of-order/overlapping pages, including 300 members and
  a reopen between pages. Empty pages and state hints are not completeness or
  full WORK evidence. Only complete sorted membership matching the recomputed
  immutable seal sets `membership_verified`.
- V2-OP/CLOSE: known declaration receipts, parent scope producer/membership,
  immutable child allocations, work/manifest evidence and ancestor fences must
  agree in either arrival order. Valid child-first metadata survives reopen and
  does not invent parent admission. Parent pages and later sealing receipts
  recheck retained child relationships, refusing contradictions atomically.
- V2-CLOSE: checkpoint requires the full seal, terminal WORK evidence and saved
  child coverage. Incrementally recompute counters/status root, enforce time
  ordering and STRICT successful-parent closure. Failed parents still wait for
  child closure. Missing evidence is NOT_READY, not an inferred success.
- V2-STORE: quota/write-failure refusal preserves prior evidence and unresolved
  operations. Corrupt normalized keys/images, missing backing membership/work/
  descendant coverage and older client formats refuse reopen. Actual forced
  process exit preserves committed membership and root coverage.
- V2-WIRE/CLOSE: the actual-QUIC journal recovery test now receives SCOPE pages
  and checkpoint, saves coverage, exclusively reopens, reconnects with rotated
  configured owner credentials and completes DRAIN with the exact saved root cut.
  The test still shares the Rust codec, not the independent failure driver.

The new parent-page and sealing-receipt regressions each first accepted a
contradiction with `Ok(())`. Both paths now revalidate relationships in the same
transaction as the new evidence. Section 12 explicitly states the both-arrival-
orders requirement without imposing parent-first response delivery.

This journal's complete-local-evidence coverage policy has extra member reads
and storage cost; it is not a new universal requirement to fetch every WORK view
before using any summary. Inventory scans and receipt comparisons are blocking.
File/count limits do not establish measured latency, heap, native-memory or RSS
bounds. Client format 3 refuses older history without conversion or deletion.
Its measured empty database is 73,728 bytes, WAL 0 on the pinned build. The two
physical-exhaustion fixtures now use 128 KiB DB/WAL/journal and 64 KiB SHM caps;
they still prove real refusal, rollback and preservation on reopen. Earlier
dated format-1/2 evidence above retains its original measured 64 KiB fixtures.

Evidence: [`durable-work-v2-client-scopes-2026-09-07.txt`](../../conformance/results/durable-work-v2-client-scopes-2026-09-07.txt).
Production asynchronous client/CLI integration, independent Java V2, neutral
cross-language process failures, measured whole-process resource gates and the
original external workload/equivalent streaming-gRPC comparison remain open.

### Rust asynchronous client journal ownership, 2026-09-07

The public `pipestream_quic::v2_client::journal::Journal` now runs the complete
core journal API on one bounded worker. It is not a network connection and does
not supply TLS authentication or response correlation on behalf of its caller.

- V2-OP/STORE: initialization, audit, commits, reads and final file-owner destruction
  run off the async runtime. Await intent persistence before transmission. A
  cancelled accepted call still executes and may durably record the original
  intent; it does not authorize a replacement identity.
- V2-RESOURCE: count queued, running and completed-but-unconsumed replies under
  one ceiling (default 16, range 1..=32). Refuse overload without an unbounded
  waiter queue. Actual gated worker jobs on a single-thread Tokio runtime verify
  that cancelling waiters does not refund queued/running capacity and that
  unread completed replies remain charged. Typed arguments/pages remain bounded;
  a separate process test opens 64 actual journal owners, refuses the 65th without
  creating its database/lock files and admits a replacement after all owners
  finish shutdown. The 64 empty databases total 4,718,592 bytes on this build;
  this is file-length evidence, not a process-memory measurement.
- V2-STORE: clones share shutdown; last-handle drop drains already accepted jobs.
  `closed` confirms operation/store-owner cleanup, not network completion.
  Worker panic after a real commit reports failure but preserves its intent on
  reopen. Construction failure waits for owner cleanup before returning.
- V2-STORE: a stable empty advisory-lock sidecar refuses a second owner, including
  another process, while the first worker is live. A subprocess commits intent,
  publishes a flushed marker, is confirmed live, excludes another opener, is
  killed and is observed to exit unsuccessfully; reopen retains the original
  intent and uncertainty. Symlink/nonempty lock files refuse without overwrite.
- V2-WIRE: both actual-QUIC journal recovery tests now use the async owner for
  creation/binding, original mutation replay, work/reference persistence and exact
  root coverage across orderly local shutdown/reopen and credential rotation.

Evidence: [`durable-work-v2-async-client-journal-2026-09-07.txt`](../../conformance/results/durable-work-v2-async-client-journal-2026-09-07.txt).
These are worker/count/storage and Rust wire tests, not measured whole-process
memory/latency guarantees or independent V2 conformance. The production client
multiplexer/CLI and complete transport/file integration remain open, together
with independent Java V2, the neutral driver and original workload/gRPC comparison.
No wire, CDDL, client database format, authority format or dependency changed.

### Rust client wire transport, 2026-09-07

`v2_client::transport` supplies bounded QUIC connection ownership and all typed
control/input/result wire paths. It is not the automatic durable journal facade
or independent acceptance oracle. The tests use the existing Rust codec.

- Actual durable server: persisted intent/receipt; 256 KiB incremental input;
  header-only original admission replay; certificate rotation and journal reopen;
  retained-manifest result stream; independent control while output is unread;
  exact bytes and verified FIN; sealed membership/checkpoint and root DRAIN.
- Correlation: reordered replies, cancelled checkpoint waiter with retained late
  response, reverse-order replies at the pending ceiling, no ID spent on local
  refusal, abandoned input with later correlated refusal, and unresolved input
  capacity checked before stream allocation.
- Adversarial authenticated wire peer: invalid increased capability selection,
  cancelled negotiation, wrong-direction/unsolicited control, recognizable wrong
  result commitment, bad digest/truncation/extra bytes, response deadline, and a
  consumer not polled until its negotiated idle deadline sends STOP_SENDING.
  The last test uses the legal 1,000 ms minimum, not an invalid shorter offer.
- Isolated resource process: 64 actual Core connections; the 65th refused locally;
  all owners drained with handles still held; replacement succeeds. This proves
  count/lifetime enforcement, not heap/RSS or comparative workload performance.

Section 12 distinguishes wrong result commitments (delivery-local INTEGRITY_ERROR)
from wrong/duplicate/unsolicited correlation (fatal FRAME_ERROR). No wire/CDDL,
database format, authority storage or dependency versions changed. The automatic
durable facade/CLI/file integration, independent Java V2, neutral cross-language
failure driver and original workload/equivalent streaming-gRPC comparison remain
required. Evidence: `conformance/results/durable-work-v2-client-transport-2026-09-07.txt`.

### Rust durable session client, 2026-09-07

`v2_client::session::Client` now owns journal/transport composition. Its typed
operations persist intent before transmission and validated evidence before
returning success. Nine actual-server tests (not an independent codec oracle)
cover:

- V2-OP/WIRE/RESULT/SCOPE: 256 KiB input/output, stored original receipts, explicit
  manifest selection, reopen with a rotated same-owner certificate, original
  operation lookup, terminal evidence, sealed coverage and exact root DRAIN.
- V2-OP/RESOURCE: cancelled mutation waiter after request dispatch, original intent
  visible before the authority commit, retained single-slot capacity until the
  collector finishes, and receipt persistence despite the cancelled waiter.
- V2-OP/STORE: dropped upload waiter while actual admission is blocked; client
  shutdown remains pending until the collector saves the late receipt. Reopen
  retains it. A cancelled creation waiter likewise saves the original binding
  before shutdown; neither path allocates replacement identity.
- V2-IDENTITY/SCOPE: absent covering declaration receipt refuses before admission
  intent/transmission; changed admission intent refuses CONFLICT; mismatched
  binding leaves original creation intact without saving the wrong binding.
- V2-OP/AUTH: UNAUTHORIZED preserves the original intent and missing local receipt;
  an explicit same-operation retry after authorization succeeds, with no automatic
  replacement or attempt allocation.
- V2-RESOURCE: a second facade cannot claim the same journal or shut down its
  existing client. A one-slot journal serializes storage under concurrent facade
  activity. Result and metadata waits remain independent of the storage reader.
- V2-SCOPE/CORE: completion waits for already accepted facade operations, refuses
  new calls during its barrier and reopens acceptance on a failed cut. WORK wait
  timeout returns the unchanged view. Later exact coverage completes successfully;
  detach alone records no completed-work coverage.

No wire/CDDL, storage format or dependency changes. The new facade is not a
standalone V2 CLI/file adapter, measured whole-process resource bound, independent
Java V2 implementation or the original workload/equivalent gRPC comparison.
Evidence: `conformance/results/durable-work-v2-session-client-2026-09-07.txt`.

### Rust owned client file transfers, 2026-09-07

`session::files::FileInput` and `Output::save_to` supply file-backed adapters, not
another protocol/codec or standalone CLI. Thirteen new test functions cover:

- V2-OP/RESULT: real authenticated-server empty and 256 KiB round trips, saved
  admission receipts, explicit manifest selection and verified result files;
  identical header replay returns its original receipt when the body is stopped.
- V2-OP/INTEGRITY: mismatched prehash/intent refuses before admission preparation;
  a modified source after prehash fails verification and remains DECLARED with
  the original local intent but no admission receipt.
- V2-OP/STORE: cancelled file-send waiter still saves the late actual admission
  across journal reopen. Cancelled result-save waiter retains transfer ownership
  and does not install a destination before verified FIN.
- V2-AUTH/OP: an early UNAUTHORIZED refusal is preserved instead of hidden by
  the secondary local writer-stop error. This test failed before the correction;
  the original intent remains saved without an admission receipt.
- V2-RESULT/INTEGRITY: an authenticated adversarial peer sends corrupt, truncated
  and overlong objects; none installs a destination, each removes its own staging
  file, and the same connection still answers control requests.
- V2-RESOURCE: empty/binary prehash, byte ceilings, non-regular files and symlinks,
  FIFO refusal without a writer, and an isolated 64-descriptor ceiling/replacement
  gate after actual cleanup. These are structural limits, not measured RSS.
- V2-STORE: staging does not publish a prefix; a destination created before
  installation is preserved on CONFLICT; successful installation is byte-exact;
  ordinary abort removes its own temporary file, not an unrelated staging file.

Caller-owned directories must remain trusted and stable. Process-death staging
reconciliation and shared disk budgeting are not supplied by these adapters.
Standalone V2 CLI, complete independent Java V2, neutral cross-language failures
and the original workload/equivalent streaming-gRPC measurement remain required.
Evidence: `conformance/results/durable-work-v2-client-files-2026-09-07.txt`.

### Rust runnable V2 endpoints and commands, 2026-09-07

`server/tests/v2_cli.rs` now runs six actual executable tests with mutual TLS,
independent client/authority histories and real input/result streams. Commands
have owned subprocess handles and bounded observation deadlines; timeout cleanup
targets only the test's own process. The tests do not call a local authority
dispatcher in place of a wire request.

- V2-SESSION/STORE: explicit initialization versus reopening, missing/existing
  history checks, original creation replay/attach and next-sequence lookup.
  A forced server process exit after the admission receipt is followed by reopen
  and byte-identical original-operation replay. This is not an instrumented
  crash on both sides of every commit or publication boundary.
- V2-AUTH/NEG: both durable-only and durable/results combinations; same-owner
  certificate rotation, changed-principal UNAUTHORIZED, skip permission disabled
  by default, and offline generation revocation followed by live rejection.
- V2-OP/RESULT: lookup still returns the original receipt after local input loss;
  replay without that file fails NOT_FOUND without replacement work. Original
  retry/cancellation replay, saved manifest selection and verified file bytes.
- V2-SET/ADMIT/CLOSE: caller-generated and authority-generated children, actual
  reassembly, bottom-up checkpoint and exact root completion. Unknown applications
  return APPLICATION_UNSUPPORTED without fallback. Authority chunking covers empty,
  exact/partial 64 KiB inputs and a 2,097,153-byte input producing 33 children under
  the default 16-active-job session ceiling; output bytes and membership agree.
- V2-ATTEMPT/CANCEL: explicit retry from AWAITING_RETRY advances to attempt 2,
  exact retry replay, successful terminal retry ALREADY_TERMINAL, authorized skip,
  cancellation preserving a terminal skip and scope cancellation settlement.
- V2-STORE/TIME: three startup unit tests check bounded regular-file reads,
  symlink/directory/oversize rejection, malformed/ambiguous principal maps and
  explicit skip authorization. CLI startup requires explicit system-UTC trust;
  it does not derive cross-restart UTC from a monotonic timer. Signal shutdown
  asserts local drain, not durable-work completion.

The [CLI guide](../../implementations/rust-quinn/docs/v2-cli.md) documents immutable
configuration, application contracts and remaining limits. Production input is
incremental; the test oracle holds fixture bytes for exact comparison. The CLI
applications are not the external comparative workload and these tests share the
Rust implementation, not an independent protocol oracle. Client staging-root
ownership, shared disk budgeting and bounded crash reconciliation remain open,
as do independent Java V2, neutral cross-language failures, whole-process resource
gates and the original equivalent authenticated/durable streaming-gRPC workload.

### Rust managed local result copies, 2026-09-07

`v2::client::results::ResultStore` reuses bounded immutable payload storage with
a purpose-qualified trusted authority/owner binding, independent of authority
history. `session::files::managed::ManagedResults` uses the fixed file-worker pool
and existing shared file-owner slots. The durable client's output now retains its
exact journal selection and negotiated limits for this adapter. No wire or journal
format changes, new dependencies or automatic adoption of arbitrary directories.

- V2-RESULT/STORE: full-length/header reservation before reception, empty and
  nonempty install only after verified transport FIN, exact selection matching,
  corrupt-body rejection at local EOF, shared byte/object/handle ceilings and no
  silent eviction. Repeated downloads are separate charged local copies.
- V2-AUTH: owner/purpose/policy mismatch and unknown paths fail rather than adopt
  or delete local data. Explicit local-only lookup requires a saved manifest
  selection; it neither creates work nor grants/renews remote authorization.
- V2-STORE: six substantive core tests plus a subprocess entry point cover
  exclusive ownership, pinned-copy removal rejection, quotas, unknown-file
  preservation and process death before installation, after durable installation
  and after unlink. Incomplete stages are reclaimed on audited exclusive reopen;
  completed copies survive and are verified again on read.
- V2-STORE: an injected directory-sync failure after installation failed the
  negative-first quarantine test: a later lookup could use the uncertain copy.
  The shared object store now quarantines all operations until exclusive reopen
  audits/synchronizes the namespace. The regression preserves the installed bytes
  and verifies them after reopen rather than silently deleting or replacing them.
- V2-RESULT/VIEW: two async tests include an actual authenticated 262144-byte
  download, quota refusal for another copy, unchanged authoritative terminal
  observation, local read after the server stops, exact bytes and explicit removal.
  A separate owner test checks clone release, root exclusion and changed binding.

Section 12 now explicitly distinguishes already delivered local bytes from current
authority access. These tests are not proof of remote erasure, a new authorization
lease, measured total heap/RSS, power-loss behavior, or independent cross-language
V2 conformance. Managed-store CLI/export integration is recorded below. The neutral
failure driver, Java V2 and original external/equivalent streaming-gRPC workload
with pinned raw measurements remain required.

### Managed CLI and raw export recovery, 2026-09-07

Rust locations: `src/v2/client/results/exports.rs`, its `tests.rs`,
`quinn/src/v2_client/session/files/managed/exports.rs`,
`quinn/src/v2_tls/tests/authority/server/session/files/managed.rs`,
`server/src/v2/results.rs`, `server/tests/v2_cli/managed.rs`, and
`tests/v2_local_export_resources.rs` (all below `implementations/rust-quinn`).

- V2-RESULT: real authenticated CLI downloads require the existing managed root
  and saved selection. Offline verification/export receives no endpoint or TLS
  options. The test stops the server, uses the original journal, verifies raw
  bytes, and rejects missing local copies without remote fallback. Restarted
  authority receipt/manifest and unresolved operations remain unchanged.
- V2-RESULT/STORE: a stable nonzero local export ID commits the full manifest/index
  digest before copying; both pending and complete exports reserve full length plus
  112 bytes. Identical payload bytes under another work identity still conflict.
  Verified source EOF, copied-file verification, file sync, rename and directory
  sync precede successful installation. Partially consumed sources cannot publish
  a prefix; exact replay rehashes existing raw bytes without overwriting them.
- V2-STORE: six substantive core tests plus a subprocess entry cover empty/raw
  bytes, owner/policy/lock checks, quotas, unknown-file preservation, body corruption,
  and process exit at intent commit, body copy, rename, directory sync and removal.
  Complete inventory/byte audit precedes staging cleanup. Intent stays charged
  until explicit local removal. Failed sync at either export or removal boundary
  quarantines the live root until exclusive audited reopen.
- V2-STORE: a held single worker and an explicit poll enqueue the actual export
  before dropping its waiter; a FIFO completion barrier and verified replay prove
  that the accepted operation and source pin survived cancellation. Clone close
  cannot release another root owner. This does not simulate machine power loss.
- V2-STORE: the isolated 32 MiB local gate includes copy staging, export, full-file
  replay verification and cleanup. Additional Rust heap must remain below 256 KiB,
  largest allocation below 64 KiB; exact named export lengths and quota release are
  asserted. RSS is reported separately, not bounded by the Rust allocator counter.
  The first run measured 4266/1952 bytes peak/largest, 7160 KiB observed RSS/HWM and
  33554616 named export file bytes (body, 112-byte intent, 72-byte binding).

The initial real CLI tests caught a duplicate argument-group name; that is fixed
with an explicit group identity and covered by the complete command-tree check.
No protocol encoding, dependency or journal/database-format change was needed.
Extra copy/hash I/O is documented, not hidden as a free cache hit. Named-file
quotas do not account for filesystem allocation or external raw-file readers
holding unlinked files. Arbitrary-path direct downloads retain their documented
crash-left staging limits. These are Rust implementation gates, not independent
Java V2, neutral cross-language failures or the original comparative workload.

### Independent Java typed wire and commitments, 2026-09-07

Sources: `implementations/java-netty/src/main/java/ai/pipestream/quic/v2/`.
Tests: the matching `src/test/java/ai/pipestream/quic/v2/` package.
Evidence: `conformance/results/durable-work-v2-java-wire-2026-09-07.txt`.

- V2-WIRE: `V2WireTest` consumes every one of the 70 frozen expectations, checks
  the fixture byte digest, and requires accepted frames/records to round-trip
  byte-for-byte. It exercises every control split and one-byte delivery, strict
  CBOR/UTF-8, nonnegative 63-bit integer boundaries, checked aggregate overflow, immutable
  collections/digests, partial FIN and failure poisoning. Oversized u32 declarations
  fail before body allocation; 1 MiB ignored bodies retain zero body-buffer bytes.
- V2-NEG: the same test validates profile intersection/required union, exact
  minimum ceilings/deadlines, unsupported dependencies, increased limits and
  unavailable required profiles. It does not yet test a real Java negotiation
  exchange, connection correlation or authenticated profile activation.
- V2-OP/RESULT/CLOSE: `V2CommitmentsTest` constructs typed inputs for all 12
  frozen hashes. Context, originator namespace, work identity, operation ID/type,
  retry attempt and declaration changes alter the digest; connection request
  numbers do not. Locator numeric identity must agree with its manifest. A status
  leaf requires terminal work and exactly the child-root presence in its view.
- V2-CLOSE: streaming seals reject incomplete, extra, duplicate and reordered
  members. The status frontier agrees with independent full-level reduction at
  every size 0..1025, including repeated odd-last duplication. Counts, exact scope
  identity and ascending member IDs are checked; any error invalidates the builder.
  These functions do not prove parent membership or descendant coverage by
  themselves; retained relationship validation remains part of Java's next work.
- V2-STORE/resource foundation only: `V2CommitmentResourceTest` starts an isolated
  `-Xmx24m` JVM and streams 4,000,003 members, more primitive IDs than fit in that
  heap. It checks the fixed frontier/count bound and reports elapsed time, heap,
  hash bytes and total RSS/HWM separately. First run: 1653 ms, 24379392-byte maximum
  heap, 672 retained hash bytes and 385152 KiB observed RSS/HWM. This is not a storage,
  endpoint, flow-control, network-memory or full-process bound.

The separate V2 package does not activate profiles or reinterpret historical
storage. Full independent Java durable state, authenticated endpoints, timed
incremental objects, recovery/refusals, both-language failure/resource driver and
the original external workload/equivalent streaming-gRPC comparison remain open.

### Independent Java correlation and object verification, 2026-09-07

Sources: `ClientCorrelation.java` and `ObjectStream.java` under the Java V2
package. Tests: `V2ClientCorrelationTest`, `V2ObjectStreamTest` and
`V2ObjectResourceTest`. Evidence:
`conformance/results/durable-work-v2-java-streams-2026-09-07.txt`.

- V2-WIRE/NEG: one capability exchange, ignored frames only after negotiation,
  explicit profile use, ID 1 first and strictly increasing shared request IDs,
  maximum-ID exhaustion, negotiated pending ceilings and exact response-family
  matching. Every control response kind is tested against every different kind.
  Reordered replies succeed; unsolicited, duplicate or wrong-direction responses
  invalidate correlation while preserving unresolved requests for recovery.
- V2-ADMIT/OP: actual input stream tags are disjoint from control IDs and may be
  registered out of order. Stream/pending ceilings are separate; duplicates and
  wrong stream type fail. There is no API that promotes STOP_SENDING to admission;
  only a correlated receipt/refusal or connection uncertainty resolves the slot.
  Typed receipt content still requires independent durable-journal verification.
- V2-RESULT: retained selected descriptors bind every result-header field. Wrong
  generation/work/attempt/index/length/digest fails only that delivery; another
  control request still completes. Unknown/non-result/duplicate header correlation,
  or a second control response after stream start, is fatal. Transfer exhaustion
  does not mask commitment mismatch. Aborting a rejected stream cannot free another
  transfer's permit. Requests remain pending until verified FIN, failure FIN,
  abort or connection loss; neither reset nor another read changes work state.
- V2-ADMIT/RESULT/TIME: all header split positions leave coalesced payload untouched;
  lengths fail before allocation and header progress cannot renew its absolute
  deadline. Actual FIN must match length/digest, including empty bodies. Extra,
  truncated or corrupt bytes fail; equal-to-idle/lifetime deadlines fail before
  progress or FIN. Explicit timer calls enforce bounds without callbacks. Monotonic
  signed-long wrap succeeds; backward time cannot create extra lifetime.
- V2-STORE/resource foundation only: a fresh `-Xmx24m` JVM verifies a 64 MiB zero
  object through a reusable 8 KiB direct buffer against a separately checked
  SHA-256 fixture. First run: 49 ms, 24379392-byte maximum heap, 135844 KiB RSS/HWM.
  This is local hashing, not a network, file-persistence or low-RSS guarantee.

These helpers do not activate profiles or authorize work. Real Netty stream
ownership, connection credit reservation, timer scheduling, bounded pending
result creation/header queues, authenticated durable execution and recovery still
need implementation and actual endpoint tests. The complete cross-language
failure/resource driver and original external/equivalent streaming-gRPC workload
remain required by the unchanged goal.

## Java V2 QUIC/TLS boundary evidence, 2026-09-07

Sources: `TlsAuthentication.java`, the shared Java `TlsPeerIdentity.java` SAN matcher,
and `V2TlsTest.java`. No Rust implementation is linked and no protocol JSON
translation is involved. The dispatcher in these tests only exercises authentication
and capability policy; it is not a durable endpoint and does not establish V2
cross-language interoperability.

- V2-AUTH: actual mutual-TLS handshakes bind two different certificates to one
  configured owner. A reissued certificate with the same public key but different
  full DER remains unmapped. Missing/unmapped callers can negotiate Core but cannot
  activate required durable work or results, including result-only required sets.
  Rejected negotiation sends no capabilities response.
- V2-AUTH/WIRE: untrusted, wrong-EKU, expired/future certificates, inconsistent
  local cert/key material and ALPN mismatch fail TLS. Service-name cases cover
  exact DNS, literal IP, whole-label wildcard, multiple-label mismatch, CN-only,
  malformed reference, wrong server EKU and wrong server trust root. Peer-observed
  close codes are transport CRYPTO_ERROR values, not application refusals.
- V2-AUTH/TIME: existing connections reject expiry at the exact certificate end
  time and removal/remapping of the original owner. Three reconnects with the
  built-in client use full handshakes. An external caching client demonstrably
  resumes; subsequent connections revalidate current mapping and credential time.
  An expired resumed peer never activates downstream application handlers.
- V2-NEG/lifecycle: application activation follows credential verification, not
  Netty's earlier channel-active notification. Reentrant close on TLS exception
  originally suppressed peer alert delivery; negative network tests failed before
  that fix. A never-active connection's unregistration resolves readiness as failure,
  not an indefinitely pending promise. Negative readiness assertions require an
  actual exceptional completion; a test observation timeout is not a passing refusal.
- V2-STORE/configuration only: empty/over-1-MiB trust files, over-256 anchor files,
  malformed owners and over-16,384 mappings refuse. Peer chain checks allow at most
  16 certificates/64 KiB after native decoding. This is not proof of bounded native
  TLS allocations, listener admission quotas or total process memory.

Java still needs complete bounded connection/control/object ownership, scheduling,
transactional work/results/retention and client recovery, including the same
parent/child evidence checks in both arrival orders. The neutral Rust failure
driver, measured both-language resource/outcome evidence and original external
chunk/distribute/transform/reassemble workload plus equivalent authenticated durable
streaming-gRPC comparison remain required.

## Java V2 Core listener evidence, 2026-09-07

Sources: `CoreServer.java`, `CoreOptions.java`, `ControlWrites.java`; tests:
`V2CoreServerTest.java`. Actual Java QUIC, not a mocked stream. The reusable
listener advertises no durable profile and supplies no fake work state.

- V2-WIRE/NEG: exact Core minima; optional unknown profiles excluded; required
  unsupported or unauthenticated durable profiles rejected before response.
  Wrong first frames, direction, repeated negotiation, oversized prefix, private
  types, repeated/decreasing IDs and truncated/early FIN cause named fatal errors.
  Every one of the 15 profile-dependent control request families receives a
  correlated `EXTENSION_UNSUPPORTED` without closing unrelated requests.
- V2-CLOSE: paused client reader with a 128-byte receive window, batched requests
  and real client FIN; every earlier refusal, detach response and later `NOT_READY`
  arrives in order before server FIN. Parent remains open for client-owned close.
  Duplicate detach IDs remain fatal. RESET and STOP yield `CONTROL_RESET`.
- V2-TIME: independently scheduled missing-stream, partial-prefix and trickled-frame
  expiry; actual stalled TLS verification releases its global slot. Increasing
  requests cannot extend the oldest blocked response or absolute detach deadline.
- V2-NEG/resources: global two/per-owner one permits; duplicate owner rejected
  after TLS, then 16 global excess connections each receive actual transport
  `CONNECTION_REFUSED`, not an accepted observation timeout. TLS may finish before
  that close; the actual transport refusal and unchanged admission counts are the
  oracle, not the local connect-future outcome. Admitted high water
  stays two; total observed transport high water is three, including the explicitly
  bounded packet-local refusal slot. Existing and replacement peers still work.
  The shared anonymous bucket rejects a second peer independently of mapped owners.
  Refusal telemetry counts admission attempts, including repeated Initial packets;
  it is not asserted equal to the number of distinct clients. Each peer observes
  its own exact transport refusal and an increase in the refusal counter.
- V2-WIRE/resources: count and byte queue exhaustion tested separately with a
  nonreading peer. High-water counters stay within configured bounds; another
  peer makes progress. A 12000-byte ignored body crosses a 128-byte receive window
  without taking a request ID. Invalid and excessive aggregate buffer policies fail.

The 128 MiB configuration gate covers queue/frame/read-buffer allowances only,
not native TLS/retransmission allocation or measured total heap/RSS. There are no
Core object streams. Complete independent Java client/durable execution/results/
recovery, shared data/control credit proof, neutral both-language failure/resource
driver and the original external/equivalent streaming-gRPC workload remain required.

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

- Distinguish explicitly local retained copies from freshly authorized result
  transfers. A local hit cannot renew remote output availability, grant access,
  or claim that revocation/expiry erases previously delivered bytes.

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
