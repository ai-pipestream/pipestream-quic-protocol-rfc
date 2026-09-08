# Java V2 retention and retirement implementation plan

This is the remaining implementation work for Section 12.9, not a claim that
Java reclamation or retirement is implemented. The normative contract remains
in `sections-src/section-12.md`. The local result-read service and per-input
physical reader pins supply part of the liveness machinery. Java's durable
endpoint/client and the independent failure driver remain separate obligations.

## Durable eligibility must outlive deletion

The current private `JobRecord.releaseIntent` discriminator is not sufficient
for reclamation. It records neither when retention conditions were satisfied
nor proof that a completed release was authorized. Clearing an intent after
refunding capacity would lose the distinction between intentional deletion and
unexplained missing bytes on the next restart.

Replace that unused discriminator in the next private storage version with
separate nullable input-release and output-release timestamps. Keep each
timestamp after its corresponding `inputLive` or `outputsLive` charge becomes
false. These are authoritative eligibility records, not inferred file-deletion
times, user-supplied clocks or new wire fields. No old private format conversion
is required. Each resource requires at most one intent write and one completion
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

Active input reception needs a separate identity gate before cleanup is enabled.
`Receiver.finish` can install the immutable name after an earlier authorization
check, including a duplicate reception started before the work settled. A reader
pin alone does not prevent that late installation. Track each receiving input's
exact identity through synchronized staging cleanup, and exclude or fence it
before authorizing deletion. An unborrowed receiver credit has no object identity
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

`ExecutionStore.audit` currently requires every job's input/output charges to
remain live and requires a zero release discriminator. Replace those conditions
with checked release evidence and the correct remaining rewrite-credit floor.
Do not simply exempt all settled jobs from validation.

`AdmissionStore.verifyStorage` and `PublicationStore.verifyStorage` currently
expect charged objects to exist. They must distinguish a live promise, an
authorized interrupted release and a completed release. Continue checking every
remaining object's exact identity and contents. Allowing missing bytes merely
because a work is terminal would conceal data loss before output expiry or
parent settlement. Retained publication metadata remains audited after bytes
are gone.

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
