# Java V2 retention and retirement implementation plan

This records implementation and remaining work for Section 12.9. Java now has
single-job input and terminal output reclamation operations. Bounded orphan
reconciliation and automatic cleanup scheduling are implemented and locally verified.
Checked session retirement and its automatic scheduling are implemented and
locally verified, separately from those earlier gates.
The normative contract remains
in `sections-src/section-12.md`. The local result-read service and per-input
physical reader pins supply part of the liveness machinery. Java's durable
endpoint/client and the independent failure driver remain separate obligations.

## Durable eligibility must outlive deletion

The former format-7 `JobRecord.releaseIntent` discriminator was not sufficient
for reclamation. It recorded neither when retention conditions were satisfied
nor proof that a completed release was authorized. Clearing an intent after
refunding capacity would lose the distinction between intentional deletion and
unexplained missing bytes on the next restart.

Private format 8 replaces that unused discriminator with separate nullable
input-release and output-release timestamps. It keeps each
timestamp after its corresponding `inputLive` or `outputsLive` charge becomes
false. These are authoritative eligibility records, not inferred file-deletion
times, user-supplied clocks or new wire fields. No old private format conversion
is provided. Each resource requires at most one intent write and one completion
write, within the four cleanup writes already reserved by `JOB_CREDITS`.

Before committing or accepting a retained release record, verify:

- The exact admitted job and work are terminal and agree on attempt/identity.
- Terminal time is no later than the release timestamp, and that timestamp is
  no later than the persisted greatest trusted UTC value.
- Any owned child scope has a verified closure no later than the release time.
- For outputs, the external availability interval has ended and the dependent
  parent, if any, settled no later than the release time. Resolve the checked
  containing scope and its matching parent admission, not aggregate counts or
  an unverified parent identifier.
- Every released logical charge has retained eligibility evidence. An unsettled
  job cannot carry a release timestamp or lose its funded resources.

The input and output timestamps are independent. A successful child may release
its own input after child closure while its outputs remain required by its
parent. Neither a result read nor retry may extend an external promise. Ordinary
output expiry does not remove the manifest or terminal receipt.

## Three separately recoverable phases

1. In a bounded writer transaction, commit checked eligibility and the final
   safe UTC sample. Refuse unsafe or regressing time before commitment. Local
   maintenance does not require a disconnected or revoked caller's permission.
2. While holding exclusive paired-store ownership and the physical-liveness
   monitor, recheck the durable record and remove only its eligible names.
   Synchronize each affected directory. An existing intent permits resuming an
   interrupted removal; absent files without that intent are corruption, not
   expiry. Retain conservative charges across unlink or synchronization errors.
3. After synchronized removal and absence of relevant physical pins, clear the
   corresponding logical charge in a new writer transaction. Preserve the
   eligibility timestamp and all still-required identity/receipt/manifest state.

Input readers are counted per immutable object, in addition to the shared
handle pool. EOF does not release a descriptor. A stale callback's descriptor
continues to block deletion after the callback's logical execution is fenced.
Unrelated input readers or unborrowed reception credits must not block an
eligible object. Output readers, writers and borrowed callback credits already
pin the exact output-funding identity; fresh result reads share those pins.

Active input reception now has a separate identity gate used by input cleanup.
`Receiver.finish` can install the immutable name after an earlier authorization
check, including a duplicate reception started before the work settled. A reader
pin alone does not prevent that late installation. The store tracks each receiving
input's exact identity through synchronized staging cleanup and refuses physical
removal while it remains active. An unborrowed receiver credit has no object identity
and is not such a dependency. Test a delayed duplicate FIN racing with retention,
not just a reader held after EOF.

Output names and any staging aliases must be removed and synchronized before
their funding record is removed. Never refund the full output reservation while
one of its physical names or descriptors remains. A same-process failed sync
needs retryable accounting state; a restart reconstructs physical usage before
admission can reuse capacity.

Use finite keyset job sweeps with an upper bound captured at sweep start, rather
than a scan that new admissions can extend indefinitely. File cleanup needs its
own bound. Output slot names are deterministically derived from funding and
index, so use bounded slot visits instead of repeatedly scanning every other
job's output directory entries. Preserve progress only after its corresponding
commit; a new cursor may safely restart from durable records.

Include bounded orphan reconciliation for installation-before-admission crashes.
Prove absence of an authoritative reference under the same paired-store ownership
and metadata transaction used by admission; file age is not that proof. Exclude
active reception/installations and distinguish partial retirement from live
metadata loss. Recheck immutable input and funding identity before deleting an
orphan. Otherwise interrupted admissions can permanently consume the bounded
store even when all accepted jobs have settled.

## Recovery audits are part of the implementation

`ExecutionStore.audit` checks retained release times against terminal dependencies
and the trusted clock watermark, with the adjusted remaining rewrite-credit floor.
It does not exempt all settled jobs from validation. Job decoding rejects refunded
charges without eligibility evidence and release evidence on unsettled jobs.

`AdmissionStore.verifyStorage` distinguishes a live input promise, an authorized
interrupted input release and a completed release. For outputs without release
evidence, funding and published objects remain mandatory. The new terminal
output audit permits interrupted deletion only after relational release evidence
has passed validation. It continues checking every
remaining object's exact identity and contents. Allowing missing bytes merely
because a work is terminal would conceal data loss before output expiry or
parent settlement. Retained publication metadata remains audited after bytes
are gone.

`SessionStore.reclaimInput` performs at most two writer transactions for one
explicitly named admitted job, holding paired-store ownership throughout. It
checks an existing input before creating the first intent, so missing live bytes
cannot manufacture deletion authority. A repeat completes an earlier intent or
reports already-released state. Revocation does not prevent local maintenance.
Child-closure validation currently uses the existing session-wide streaming audit;
the single target bound is not a constant-time or hard wall-clock bound. The
automatic cleanup service below schedules these operations in finite pages.

### Terminal output collector

`SessionStore.reclaimOutput` uses the same paired-store writer/clock discipline
as input cleanup. It checks terminal and child-closure evidence, external output
expiry and dependent parent settlement before beginning cleanup. Exact output
reader/writer/credit pins delay even the first intent: a fenced callback can
still own a mutable staging header, which must not be inspected concurrently
as if it were immutable retained storage. Input and output release timestamps
and logical charges remain independent.

Before the first output intent, all promised published bytes must exist and
match the retained manifest. After intent commit, missing output names are
allowed, but every remaining name must still have funding and match the admitted
identity, output budget and producing fence. Successful outputs must match the
exact published producer and descriptor; failed work can retain unpublished
outputs from an older execution fence, never a future one. The collector visits
the admitted slots directly, at most 256, rather than scanning unrelated output
names. Existing child-closure auditing remains session-wide.

Cleanup removes staging aliases first and synchronizes their directory, then
installed names and their directory. Only then does it unlink and synchronize
funding. Output byte/file usage is a prepaid reservation, not the physical size
of each installed output: deleting an output name does not refund any of that
allowance. Interrupted funding unlink/sync retains the exact same-process charge.
After restart, an absent funding record contributes no physical reservation;
the logical charge remains until checked reconciliation commits its completion.
No output name is permitted to remain after its funding disappears.

Startup also synchronizes the input-object and funding directories, including
empty ones, before exposing reconstructed capacity. Seeing a name absent after
process death does not prove that the previous process synchronized its unlink.
The recovery barrier applies even when no retained file remains to trigger an
individual lookup/synchronization. Failure at that barrier refuses opening the
installation; it does not return an apparently usable empty store.

The focused 85-test gate and full 616-test Java suite pass, including six real
JVM output-release crash boundaries, a two-output interrupted deletion, actual
parent settlement with a live output reader, exact prepaid quota/refund checks,
and recovery-barrier refusal. Strict doclint, the native guard, existing examples
and draft checks pass. See the
[output reclamation evidence](../../conformance/results/durable-work-v2-output-reclamation-2026-09-08.txt).
That checkpoint did not supply orphan reclamation or fair scheduling. The next
increment below supplies those locally verified operations; session
retirement, durable Java wire integration and two-language failure evidence remain.

### Bounded orphan reconciliation and automatic cleanup

`OrphanStore` checks a discovered input or funding identity against its known
session, declared member, retained operation receipt and any admitted job in an
exclusive metadata writer snapshot. A missing job with an admitted member or
receipt is corruption, not deletion authority. Unknown and retiring sessions
are refused. A referenced resource is not an orphan merely because its job has
settled. Input reception/read pins and output callback/descriptor pins protect
the exact identity throughout the check and removal.

Discovery hints can outlive both admission and legitimate terminal cleanup. A
retry of such a hint must distinguish a checked completed release from a live
reference or an unreferenced orphan. Completed release evidence plus physical
absence yields `ABSENT`; resurrected names or uncertain charges still refuse.
Treating every refunded job reference as corruption would permanently block the
orphan pass after a temporarily refused upload is later accepted and cleaned up.

Unlike terminal resource deletion, this operation establishes that no admission
ever promised the candidate resource. It does not create an expiry intent or
erase metadata. Physical names and headers are independently checked, input
contents are hashed before removal, and funding with execution output names is
refused. Removal and directory synchronization precede physical refunds. A
same-process failure retains the exact identity and charge in the storage
instance, blocks same-name reinstallation, and is rediscoverable by a replacement
cleanup service even after the file name disappeared. Restart reconstructs
physical usage behind the existing directory-synchronization barrier.

The produced-input runtime holds the payload-store monitor across both FIN and
admission, including the already-installed shortcut. Otherwise an in-flight
installation could become unpinned before its metadata transaction starts and
be mistaken for abandoned staging. Future durable transport integration must
preserve that handoff as well; this local runtime change is not wire integration.

`RetentionService` has one authority-owned daemon and one serialized maintenance
operation. Each call visits at most its configured number of job candidates and
physical candidates, from 1 through 64. Jobs use finite keyset sweeps; pinned or
refused jobs do not prevent later entries from being visited. A separate physical
scan holds at most one directory descriptor charged to the shared handle pool.
Each pass visits at most the immutable file-policy ceiling. Directory discovery
is weakly consistent, not a snapshot or a fairness guarantee under arbitrary
continuous directory mutation; later passes revisit surviving names.

An uncertain orphan removal is retried before advancing that physical pass.
Repeated orphan failure does not stop job cleanup, but can delay other orphans;
the service reports a bounded named diagnostic rather than inferring safe
deletion from damaged metadata. Closing the service stops scheduling without
interrupting physical I/O. Active maintenance completes before releasing its
storage attachment and scanner charge. This is bounded scheduling and handle
accounting, not a measured whole-process memory or hard I/O latency bound.

Regression coverage includes real process deaths at orphan unlink/sync,
same-process synchronization failures, replacement-service retry, stale discovery
after legitimate admission and cleanup, resurrection refusal, admission
handoff, exact refunds, live pins, job-page fairness and service shutdown. The
103-test focused gate and full 638-test Java suite pass with zero failures,
errors or skips. Strict six-type doclint, native guard, existing examples and
draft checks also pass. The stale-hint regression failed against the prior
implementation and passed after the classification/absence correction. See the
[orphan and scheduling evidence](../../conformance/results/durable-work-v2-orphan-retention-2026-09-08.txt).

## Retirement is a different durable transition

Retirement requires verified root closure, expiry of the creation receipt
interval measured from root closure, every longer work/output/receipt promise,
and completion of dependency and physical-pin obligations. Preserve owner and
authority non-reuse high-water marks.

Private format 9 adds an immutable, checksummed `RetirementRecord` and a unique
session ownership link to it. A flag without that record is corruption. The
record binds authority, owner, generation, creation sequence, verified closed
root, latest retention cutoff and trusted authorization time. Recovery checks
the exact retained root, clock watermark and both non-reuse allocators before
allowing the intentional absence of previously deleted members or jobs. A
missing mandatory root is storage corruption, not an ordinary scope lookup miss.
The fixed-capacity proof is allocated under the ordinary protected SQLite
budget; capacity refusal rolls back the entire intent and cannot consume writes
already promised to accepted work. Earlier private formats are refused, not
silently converted or reset.

`retireSession(generation, inputs, limit, clock)` first audits complete live
declarations, admissions, fences, closure, terminal intervals and payload release
evidence. It then checks physical quiescence under the paired input-store monitor
and refreshes safe UTC before committing proof and flag together. That initial
call returns `STARTED` without deleting any metadata. Remaining physical names,
active reception or uncertain orphan charges prevent intent. Logical input,
output or executor ownership also prevents retirement.

Later calls remove at most 1 through 256 metadata bundles, each in its own
protected writer transaction. Foreign keys remain enabled. Deletion order is:

1. Refunded jobs and their private state slots.
2. Non-declaration operations, including retry and scope-cancellation links.
3. Terminal entities with their work and fence slots.
4. Declaration operations, after all declared members are gone.
5. Non-root closed scopes and their slots.
6. Root, retirement proof and session together in the final atomic commit.

The root and immutable proof survive every intermediate commit. Recovery audits
surviving terminal work, released jobs and closed scopes without demanding that
already deleted operations reappear. Owner and authority high-water marks are
never reduced or deleted. Currently authorized owner requests receive `EXPIRED`
during partial cleanup; wrong-owner and revoked-owner denials precede proof
decoding. Final absence permits `NOT_FOUND` for lookup while old creation replay
still returns `EXPIRED`.

Binding physical storage installs a current database-backed generation gate on
new input reception, output funding and input FIN installation. The check shares
the storage monitor with retirement; it does not cache authorization in a retired
generation set. It refuses retiring generations and holes at or below the retained
generation high-water mark. Per-session receiver counts include pending uploads
before a final immutable filename exists, so quiescence cannot overlook delayed
FIN. Standalone physical-store fixtures without an authority do not install this
gate; protocol integration must bind the store before accepting inputs.

`RetentionService` now also visits finite keyset session pages with a captured
generation ceiling. Open sessions consume the page budget but do not prevent
later closed sessions from being examined. Each candidate gets one metadata
cleanup unit per visit. A session created beyond a captured ceiling appears in
the next sweep. Service shutdown retries uncertain retirement-directory closes
before releasing storage ownership.

These bounds cover examined candidates and committed bundles, not constant-time
validation: eligibility and recovery stream retained session relationships,
fixed-record protection scans retained accounting, and physical quiescence can
inspect up to the configured file ceiling using a charged descriptor. They do
not establish whole-process memory, physical disk I/O or hard latency bounds.
The independent resource driver and durable transport integration remain required.

The retirement gate passes 32 focused tests and all 659 Java tests, with zero
failures/errors/skips in 108 fresh full-run reports. It includes five real JVM
post-commit deaths, actual branch/child cleanup and partial reopen, corrupt
proof/flag/root refusals, current authorization precedence, active receiver pins,
exact cutoff and unsafe-clock tests, and automatic finite-sweep scheduling.
Strict doclint, the native storage guard, existing-profile examples and draft
checks also pass. See the
[retirement verification record](../../conformance/results/durable-work-v2-session-retirement-2026-09-08.txt).

## Required verification

Tests must exercise actual SQLite and filesystem stores, including real process
death at intent commit, each unlink/sync boundary, quota completion and partial
session retirement. Inject synchronization failure separately from process
death. Check exact physical and logical charges before and after reopen.

Cover live input readers, output readers and callback credits; independent
parent/child retention intervals; cancellation and stale workers; unsafe UTC,
intra-operation regression and expiry equality; missing bytes with and without
valid intent; altered eligibility timestamps; repeated cleanup; zero-byte
objects; finite sweeps under new admissions; revocation and authorization
priority; and generation/creation non-reuse after retirement. The later neutral
driver must repeat applicable scenarios across both authenticated language
directions. Local tests alone do not establish that interoperability evidence.
