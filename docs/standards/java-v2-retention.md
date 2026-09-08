# Java V2 retention and retirement implementation plan

This records implementation and remaining work for Section 12.9. Java now has a
single-job input and terminal output reclamation operations. Orphan sweeping
and session retirement remain
unimplemented. The normative contract remains
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
the single target bound is not a constant-time or hard wall-clock bound. A fair
bounded sweep/scheduler integration remains required.

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
This local collector does not supply orphan reclamation, fair scheduling, session
retirement, durable Java wire integration or two-language failure-driver evidence.

## Retirement is a different durable transition

Retirement requires verified root closure, expiry of the creation receipt
interval measured from root closure, every longer work/output/receipt promise,
and completion of dependency and physical-pin obligations. Preserve owner and
authority non-reuse high-water marks.

The current session `retiring` flag is not an implemented retirement operation
or sufficient eligibility evidence by itself. Before adding a transition that
sets it, define a retained, checked retirement record and fund its writes and
incremental metadata cleanup. Recovery must validate that record before it
skips live-session audits. Keep enough immutable binding/identity for current
authorization to take precedence over EXPIRED throughout partial retirement.
No request may replay a partially deleted session as live.
The receiver-installation fence must also prevent a delayed pre-retirement
upload from resurrecting names after that session's physical cleanup pass.

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
