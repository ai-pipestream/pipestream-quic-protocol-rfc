# Java V2 authority storage

`v2.SessionStore`, `v2.DeclarationStore`, `v2.AdmissionStore`, `v2.ExecutionStore`,
`v2.PublicationStore`, `v2.ClosureStore` and `v2.BranchStore` are the independent Java
session, declaration, admission, local execution, result-publication, closure and
direct-child dependency layer for Sections 12.3 through 12.9. They are
package-private and are not wired into a durable-profile listener. The shipped
Java endpoint still advertises Core only. Authority-produced expansion, result delivery
and retirement remain to be implemented; storage behavior is not endpoint interoperability.

## Identity and transaction boundary

First installation uses `initialize(path, configuration)` and requires a new
database filename with no existing database or policy sidecars. Recovery uses
`open(path, configuration)` and requires a nonempty, initialized V2 database.
Missing files, empty storage, foreign/V1 schemas, changed configuration and
inconsistent live roots are refused. Neither path converts another schema.
Interrupted initialization can leave an incomplete installation that recovery
refuses; it is never silently reset. Operators remain responsible for not
initializing another directory under a previously used authority identity or
restoring a stale backup without external durable anti-reuse evidence.

A creation transaction couples the authority generation, owner's creation
high-water mark, immutable receipt/profile combination and empty producer-0
root scope. SQLite's writer transaction serializes independent handles and
processes. Policy is accepted exactly or refused, never clamped. Matching replay
returns the same binding with the new connection request number; the normalized
stored receipt uses request 1, not the original connection's request history.
Policy/profile changes, future sequences and exhausted counters have distinct
named refusals. No creation refusal returns an accepted receipt.

The local `Access` gate must represent an already verified owner, recheck current
credentials and owner policy, and reject mapping changes. It runs before database
access, after transaction acquisition and immediately before commit. The eventual
TLS dispatcher must supply that gate and enforce one session per connection;
the storage API does not authenticate certificates or replace the dispatcher.
Stored owner/revocation/retirement state is checked before decoding a requested
receipt. The database contains reserved lifecycle flags, but this layer exposes
no revocation or retirement transition: those need the full fenced settlement
and cleanup implementation, not a flag-setting shortcut.

The retained control ceiling is conservative: reconnection must select at least
the original control limit. Stream counts, pending counts, timers and current
object limits are connection policy, not reasons to rewrite a creation receipt.
Admission additionally retains the largest promised control representation and
object requirement. A later attachment that cannot represent those promises
refuses without rewriting the original creation receipt.

## Membership and operation receipts

A caller declaration commits its ordered members, initial DECLARED views,
scope high-water/count/seal, session charges and immutable operation receipt in
one writer transaction. The receipt is inserted last; a deferred foreign key
requires every member to reference its committed declaration operation. A failed
insert or authorization withdrawal immediately before commit rolls back all of
those writes. No input is admitted and no callback is scheduled by declaration.

Replay validates the normalized original request, normative operation digest,
typed receipt, referenced scope and every covered member's operation link before
returning the original receipt with a fresh connection correlation. Changed
intent conflicts; missing operation lookup is NOT_FOUND, not proof that an
in-flight request cannot commit. Receipt and entity charges have separate limits;
existing replay does not consume another charge. A sealed scope cannot grow.

Sealing hashes the ordered SQLite cursor incrementally across all batches.
Paging reads at most one bounded page plus a continuation row, with an exclusive
lower entity bound. Empty pages do not assert completeness. `snapshot` reads one
current work view and rejects a future revision; it deliberately does not perform
the WORK request's optional wait. The dispatcher must implement bounded waiting
outside the database transaction. No checkpoint or closure is inferred here.

Recovery checks actual row counts, scope counts/high-water marks, streamed seals,
checksummed typed views, exact declaration links and complete receipt coverage.
Adjusting counters to hide a missing member cannot make the retained receipt
valid. These checks use bounded records and point lookups, not a whole-session
membership map. Caller and authority operations have separate producer namespaces,
including in the normative digest, local record checksum and recovery lookup.
Only the fenced local producer interface below can declare producer-1 children;
the caller declaration and operation-lookup APIs remain producer-0 only.

The local database format is version 5. Earlier experimental V2 storage formats,
like V1 and foreign databases, are refused without conversion. This is an internal
format revision, not a wire-profile change or an authorized reset of an existing
authority identity.

## Paired input-store ownership

The database now retains a fresh installation UUID separate from its protocol
authority name and generation allocator. A bounded checksummed metadata record
binds that UUID, the exact database configuration and either no input-store UUID
or the one permanently selected input installation. Every transaction checks
the retained record against the handle's immutable database identity.

Setup proceeds in durable order:

1. Initialize the database and retain its installation UUID.
2. Initialize an input root with `InputStore.initializeForAuthority` for that
   exact UUID. This claim is part of its immutable policy, not a mutable label.
3. Call `SessionStore.bindInputs` to commit the reverse input-root UUID in the
   database. Ordinary-write funding protects existing promised image writes.

An interruption after step 2 can resume the same setup after reopen. Replaying
step 3 is idempotent; an empty replacement root does not qualify as the same
installation. A root created for another database, standalone unbound storage,
or a closed input handle refuses before database inspection. The database also
refuses a different root created for its own UUID after the original binding
commits. There is no rebind, conversion or identity-reset API.

These local APIs use `IOException` for input ownership, policy and handle
refusals, and `SQLException` for absent/conflicting database bindings or corrupt
database metadata. Guarded SQLite capacity exhaustion remains LIMIT_EXCEEDED.
They do not manufacture a peer-facing operation receipt for setup.

`verifyInputs` requires an already committed exact pair. It never adopts an
unbound database. Both setup and verification keep the input owner's monitor
through the SQLite transaction, verify the retained input policy and complete
its file/directory synchronization before commit. This excludes a concurrent
cooperative input close; it does not defend against privileged filesystem or
database replacement outside the owning process.

Pairing is local storage setup, not caller authentication or work admission. It
does not create sessions, declarations, operations, jobs or input objects. The
admission transaction checks the exact pair again inside its own writer
transaction and retains a live input owner until that commit. A prior
successful `verifyInputs` call is not a transferable permission or durable job
promise. Cloned or stale backups still require the operator's external
anti-rollback and non-reuse discipline; UUIDs cannot prove backup freshness.

## Fixed records and promised writes

Scope state, work views, first-fence storage and the shared clock now occupy
fixed-capacity images in `ps_v2_slots`. Owning rows retain immutable slot links
through foreign keys. Each 128-byte header contains the local revision, remaining
rewrite credits, used/capacity lengths, immutable identity key, body checksum
and header checksum. The latter also binds the physical row ID and record role.
Reads bound the image before copying it from SQLite, verify its identity and
checksums, and reject nonzero padding. These are local integrity checks, not
authentication against an operator able to rewrite both data and checksums.

Session creation reserves a 1,024-byte root-scope body and four future image
writes. Each declared member reserves a 2,048-byte work body with two writes
and a 256-byte first-fence body with one write. Every reserved write also funds
one 64-byte shared-clock image in the same transaction. Those credits are not
an allowance for arbitrary SQL, payload files, jobs or the whole future lifecycle.
Admission enlarges its work body to at least 4,096 bytes and to the conservative
`2048 + 1280 * maximum_output_count` representation bound, with at least four
future work writes. A 2,048-byte job image reserves six writes for expansion/
settlement and input/output reclamation intent/completion. Branches atomically
allocate their child scope and its existing four-credit image. Ordinary
admission writes do not spend these future credits. The eventual lifecycle
writers must implement and verify those funded transitions before activation.

Ordinary declaration counter/seal updates preserve the scope's credits; replay
does not rewrite any image or consume another credit. A funded replacement
spends one credit while atomically changing the body, revision and retained
funding. Revision checks preserve enough counter increments for remaining
promises, including the shared clock. Growth may add capacity/credits but cannot
reduce them or change the observable body/revision. All helpers require an
owned writer transaction and rollback on failure.

Before ordinary mutations, the store streams checksummed headers and installs
a connection-local WAL ceiling below retained completion reservations. The
guarded native incremental-BLOB primitive overwrites existing bytes, without
SQL row replacement, image indexes or UPDATE triggers. Promised bytes survive
restart as image credits; recovery verifies full records, exact owning links,
clock geometry and the reservation total. No whole-session map is built, but
these audits do scan retained state and are not constant-time admission.

For page size `p`, frame size `f = p + 24`, and body capacity `c`, a credit uses
the pinned SQLite 3.53.4 model:

```text
d = ceil((128 + c) / (p - 4)) + 1
    + ceil((128 + 64) / (p - 4)) + 1
reserved_WAL = 32 + (d + 1 + ceil(65536 / f)) * f
```

This covers each full image, its shared-clock write, a possible repeated commit
frame and sector padding. The usable WAL is the smaller of the configured WAL
ceiling and the WAL-index capacity funded by the shared-memory limit. The
[Java native cost derivation](java-completion-reservations.md#cost-derivation)
describes the same pinned SQLite geometry; V2 does not use V1 job states or
its reservation ledger. These are guarded file-length bounds, not measured
allocated disk blocks, process RSS, throughput or a power-loss proof.

## Funded input admission

The immutable deployment configuration includes explicit application labels,
supported modes, a safe-restart mechanism and global/per-owner executor limits.
The constructor without an execution policy enables no applications. There is
no unknown-application fallback. Application policy is rechecked through a local
authorization gate, separate from connection credentials and free of processing
effects.

`checkInput` validates an owned attached session, input producer, declared
membership, application/mode, duration, output profile, representation and resource
bounds, ancestry and trusted UTC. It reserves nothing. Actual reception remains
outside the database transaction. A later `admit` repeats those checks and requires
the exact installed FIN/digest-verified input. Missing complete input is NOT_READY.
An input header whose operation already committed can replay its exact receipt
without reading its bytes or extending its execution interval. Declarations and
admissions share one operation namespace per producer with explicitly tagged stored
requests; cross-kind identity reuse and changed admission headers are CONFLICT.

The committing transaction holds the paired input-store monitor and SQLite writer.
It installs durable output funding before creating authoritative metadata links,
then commits the work view, attempt 1, one child scope for modes 1/2, restartable
job, original operation receipt, representation requirements and UTC watermark
together. Mode 1 waits for caller children; mode 2 starts active with a separately
retained unfinished-expansion obligation. A membership seal cannot complete that
obligation. No callback runs inside admission. A receipt promises accepted work,
not that processing has succeeded.

Input/output charges and global, per-owner and per-session job counts derive
from bounded typed job records. They are not reconstructed from a client stream
or a success counter. Output allowances remain charged in the same file store
that funds ordinary inputs. Failed metadata admission can leave an installed,
charged file or funding orphan, but no receipt, admitted view, job or child scope.
Recovery verifies operation/job/member coverage, parent/child agreement, funded
image geometry, profile bounds and the UTC watermark. Paired-store verification
also checks every retained input and funding reference before treating that pair
as ready. These admission APIs do not reclaim orphans.

Caller descendants inherit cancellation/skip fences, not their parent's deadline
failure. They remain independent obligations after that deadline. The local
producer-1 interface additionally enforces its parent attempt, lease and deadline;
the caller API does not expose that interface.

The deployment supplies trusted UTC explicitly. Missing trust, negative time,
regression within an operation or regression behind the retained watermark is
CLOCK_UNSAFE for a new admission. Final policy checks precede the last UTC sample,
which must still precede the proposed execution deadline. All deadline and
retention additions are checked before commitment. Caller replays remain observable
under unsafe time without issuing a new promise. Forward jumps are accepted
only when the deployment marks that sample trusted; a jump across the proposed
deadline refuses the admission, never clamps or extends it. This API does not
establish clock trust, backup freshness or elapsed time across power loss.

## Fenced local child declarations and admission

`declareProduced`, `checkProducedInput` and `admitProduced` operate on the exact
producer-1 child scope allocated by a mode-2 parent's admission. They require a
current execution grant and lease for that parent, its unchanged database/input
installation pair, compatible retained capabilities, pending expansion, and
current parent application permission. Child admission additionally checks the
child's application permission. Requests for another parent's scope are CONFLICT;
external producer-1 input remains UNAUTHORIZED.

Each transaction checks the parent before its action and again after storage
work and final authorization, using fresh trusted UTC. Parent attempt, local
lease, original deadline and ancestor cancellation fences apply even when the
local operation merely replays its original receipt. Unlike caller receipt
observation, local replay is not permitted under unsafe time or stale parent
ownership. A replacement lease can reuse the original operation identity and
immutable parameters without repeating admission or changing accepted children.
For a new admission, that final clock sample must satisfy both the parent's
lease/deadline and the child's newly promised deadline. A still-live parent
cannot make an already-expired child admission valid.

Declaration uses the existing bounded membership, seal, receipt and fixed-record
accounting. Input preflight reserves nothing. Reception remains outside the
database transaction and installs FIN/digest-verified immutable bytes before
admission. Local admission uses the same configured application/mode, capacity,
input/output funding, job, child-scope and clock promises as caller admission.
It returns the operation receipt directly, without inventing a QUIC stream ID.
A refused commit rolls back authoritative records; already installed input or
output funding stays charged until safe orphan reclamation. Accepted sibling
work and its original declaration are not erased by resource pressure.

The operation journal and recovery audit now validate both producer namespaces.
The same 16-byte operation ID can independently name a caller operation and an
authority operation; within one producer namespace its original parameters and
operation kind remain immutable. Scope seals and input commitments bind the
actual producer, not an inferred caller identity. Existing record capacities,
session-wide streaming audit costs and guarded file-length limits still apply.

These are local storage APIs, not an expansion callback runtime or a durable
endpoint. A child membership seal still leaves `expansionComplete=false` in its
parent job. Durable expansion completion, phase-specific receiver credits and
resumable producer callback scheduling remain required before mode 2 can run.
No wire message, profile advertisement or storage-format revision changes here.

## Durable worker ownership and failure settlement

`claimExecution` commits a strictly increasing internal lease number without
changing the wire attempt, admitted input, child allocation or original deadline.
A live lease prevents another claim. An expired lease can be replaced only after
rechecking the same configured restart contract, current execution authorization,
immutable input and output funding. Reopening a database alone does not invalidate
a live lease or authorize duplicate concurrent execution. Renewal requires the
old lease to remain live through the transaction's final checks; it cannot revive
expired ownership or extend the work's original deadline. Its requested timestamp
addition is checked before the deadline ceiling is applied.

Execution authorization is a local retained-grant gate, separate from presenting
TLS credentials. A certificate expiring after admission does not by itself erase
that grant. Owner and application policy are rechecked before commit, followed by
a fresh trusted UTC sample checked against the pre-transition lease. Claims and
renewals use ordinary write capacity, preserving funded settlement credits.
Callbacks must run outside these transactions and must recheck their fence before
further effects. These storage methods are not themselves a callback runtime.

`failExecution` atomically changes the work and job images under the current lease.
A retryable outcome is AWAITING_RETRY, retains executor capacity and requires a
future explicit retry operation. Terminal failure releases the logical executor
charge but retains input/output charges. Neither operation grants permission to
delete bytes held by a callback, reader or dependency. Each consumes one funded
work-image and job-image write; deadline failure after AWAITING_RETRY consumes a
second pair, leaving the job's four cleanup writes funded.

`expireExecution` is owner-independent authority maintenance, not a caller RPC.
It can settle an admitted work item at its deadline even when the former caller
is disconnected or no longer has an execution grant. It requires safe UTC for a
new failure; an already terminal observation needs no fresh time promise. An
accepted cancellation or revocation fence takes precedence and must be reconciled
through cancellation, not overwritten by FAILED. Parent deadline failure still
does not cancel independent children.

Terminal timestamps are sampled within the committing transaction after current
policy checks. A final clock sample reaching the proposed terminal receipt's
expiry makes that new promise unsafe and refuses CLOCK_UNSAFE with rollback;
maintenance can try again with stable trusted time. It does not publish a receipt
whose entire promised interval elapsed during its own transaction. This check
does not establish the deployment clock's trust or undo external callback effects.

A branch cannot be claimed for rehydration from a membership seal alone: it needs
a committed successful child closure, verified against actual terminal members,
descendant commitments and status counts. Closure verification streams retained
state; it is not constant-time scheduling. The closure writer described below
now produces that evidence; caller-expanded callbacks can consume the committed
child outputs through the dependency interface below. The scheduler does not
substitute a seal for a successful closure.

## Fenced result publication

`succeedExecution` requires the current durable worker lease and the exact paired
input/output store. It verifies the complete installed output set, including
contiguous indexes, lengths, digests, content types, admitted count/byte budget and
individual object ceiling. A caller cannot publish only a prefix of installed
outputs or treat an unfinished writer as a completed object. These checks stream
the payloads with bounded buffers; a descriptor array is bounded by the admitted
maximum of 256 outputs, not their byte lengths.

The authority constructs locators from a validated deployment endpoint, never a
callback-supplied redirect or credential. It samples publication time after file
verification and atomically writes SUCCEEDED, the immutable manifest and the
settled job using one prepaid work/job image pair. Result-enabled success includes
a manifest even for zero objects. Durable-work-only success has no manifest and
permits no output budget. Original input, attempt, child identity and execution
deadline remain unchanged. Receipt and output deadlines use their separate
retained durations.

Current owner/application authorization, ancestor exclusion fences, the old lease
and the original execution deadline are checked again after metadata writes.
If the final trusted UTC sample overtakes either newly proposed retention interval,
the transaction rolls back with CLOCK_UNSAFE. A branch still needs verified prior
STRICT child success and completed authority expansion. A stale worker cannot
publish files simply because it managed to install them before losing ownership.

Recovery validates successful manifests against profiles, identity, budgets and
time promises, and paired-store recovery checks every published descriptor against
the actual immutable files. It does not re-execute work to repair missing storage.
Failure before publication leaves charged orphan files, not a visible result.
The output store refuses to recycle an installed slot without authoritative
reclamation. The callback runner now reclaims strictly older unpublished
outputs under a newly committed current claim, without releasing their funding.
Result-read authorization/pins, broader reconciliation and the QUIC delivery
adapter remain separate required implementations.

## Bounded callback execution

`ExecutionRuntime` invokes real registered application code after a durable claim
and outside metadata transactions. It is a synchronous runner for host-owned
worker threads, not a durable-profile listener. `ExecutionScheduler` supplies its
background discovery and physical worker pool.
It accepts exact leaf (mode 0) and caller-expanded branch (mode 1) registrations
matching the admission registry's label, mode and restart-safety contract. Missing
callbacks, mismatched contracts and authority-expansion (mode 2) registrations
are refused, not executed through a fallback.

One runner bounds simultaneous physical invocations globally and per retained
owner across sessions, with no waiting queue. The host must use that runner as its
shared dispatch boundary; constructing several independent runners is not a
deployment-wide pool. Invocation slots remain occupied until application code
returns and its physical input/output handles close. Cleanup makes a bounded
idempotent close retry: a namespace-sync error does not leak a worker slot once
physical closure is proven, but persistent uncertainty conservatively retains it.
Arbitrary callback code is not forcibly preempted. Applications
must cooperate with current fences and satisfy their explicitly declared safe
restart contract; multiple invocations do not imply exactly-once external effects.

The callback context is thread-confined, invalid after invocation, and exposes no
raw file handle. Input reads and output begin/write/finish are bounded incremental
operations with fresh owner, application, ancestor, attempt, deadline and lease
checks. The ownership check and physical output action share the input monitor
with replacement claims, so an old callback cannot recreate a slot after its
replacement reclaims it. Callback computation itself never holds that monitor.
Renewal is explicit and cooperative, retains the same wire attempt, and cannot
extend the original deadline. A monotonic interval also fences each invocation;
its check participates in the final committing authorization gates, not just the
check before payload verification.

Before invoking code, the runner acquires its input handle, a sequential child-reader
credit for a branch and, for a nonzero output budget, a dedicated output-writer
credit. Sequential readers/writers borrow their respective credit; ordinary readers
cannot consume it. Fresh admission refuses an intrinsically insufficient handle
policy before output funding or a receipt: one handle for own input, one for branch
dependencies, and one for a nonzero output count. This is a policy check, not an
admission-time reservation of all future simultaneous callbacks. If these resources
are unavailable, no callback runs and the committed job remains recoverable,
not FAILED because of transient dispatch pressure. Byte/name funding remains
unchanged. Repeated renewal under a frozen trusted UTC sample cannot restart the
monotonic allowance: only an actual positive durable-expiry delta adds process time.

Success publishes the exact completed set through `succeedExecution`. An unfinished
output writer or child reader, or a swallowed interface refusal, prevents success. Application
exceptions become a bounded generic INTERNAL_ERROR failure without disclosing
exception text; an explicit retryable outcome remains AWAITING_RETRY. Persistence
failures and lost authority cannot be turned into fabricated successful or failed
computations. Final publication/failure still uses the current authority checks.

Replacement claims also provide the durable eligibility evidence for recycling
strictly older `(attempt, local lease)` orphan output slots. Claim and reclamation
hold the same input monitor. Every candidate identity, allowance and installed
digest is checked before any unlink. Live output readers or writers for that
funding refuse reclamation; unrelated funding is not blocked by those pins.
Pending and installed namespaces are synchronized before reuse, including after
interrupted cleanup. No funding is refunded and no same/current/future or foreign
output is inferred to be reclaimable. A terminal success cannot receive a new
claim, so this path cannot authorize deleting a published result. Corrupt headers
that cannot establish an old identity remain a refusal, not deletion evidence.
Reclamation scans the two shared output namespaces and retains at most 512 target
descriptors, with installed-body verification bounded by that job's funded bytes.
It is not a constant-time lookup or a demonstrated many-job throughput result.

## Caller-expanded child reassembly

Mode 1 callbacks begin only after their exact child scope has committed successful
STRICT closure. `children(after, limit)` returns at most 256 ordered direct-child
identities and an exact continuation flag. It cannot enumerate another scope.
`beginChildOutput(entity, index)` resolves a published descriptor from that same
scope; `readChildOutput` streams bounded chunks and `finishChildOutput` requires
observed EOF before returning the sequential reader credit. Reading zero bytes
does not establish EOF. Only one child output can be open per callback, alongside
the parent's own input and optional output writer. Applications need not select
every child output, but every opened reader must finish before successful return.

Each metadata observation checks the current parent's retained owner/application
grant, attempt, local lease, deadline and exclusion fences. A read-only SQLite
snapshot validates the exact child allocation and complete successful closure,
then rechecks current authorization and time before returning. The callback's
paired-store monitor covers this observation and the physical output open, and
each later chunk checks current parent authority. Descriptors do not grant bearer
access; no callback-supplied locator is dereferenced. Historical child attempt/lease
identity locates the immutable file without granting new execution ownership.

The output's externally promised interval may have expired while its internal
parent dependency remains live. The reader therefore uses retained successful
child evidence rather than an external result lease. Exact length, digest and
content type must match the committed manifest. Missing or corrupt physical
storage and contradictory metadata preserve the recoverable job and report a
storage error, not a fabricated computation outcome. Unknown child/output
selection and invalid callback sequencing are sticky named interface refusals.

The child-reader credit is bound to its physical store and remains charged while
idle or borrowed. A borrowed reader also pins that child's funding against local
reclamation; returning it removes the per-file pin, not the reserved handle.
Closing the credit with a live physical reader refuses. This increment does not
implement external result-read leases or dependency-aware expiry/refunds: retained
output allowances still remain charged after parent settlement.

Pages and payload chunks have bounded memory, not constant cost. Child metadata
checks perform the existing session-wide streaming closure audit. Object lookup
and opening each verify the payload using fixed buffers. These repeated scans and
hashes are real costs, not a zero-copy or many-job throughput claim.

## Background discovery and deadline maintenance

`ExecutionScheduler` discovers committed jobs without a connection or an in-memory
admission notification. `scanExecutions` is an authority-internal read-only API,
not an authenticated caller endpoint. It reads at most 64 checked job/work pairs
and one continuation key in a SQLite snapshot, ordered by the existing
`(generation, scope, entity)` primary key. Scopes have one immutable producer.
The first page fixes an inclusive upper job key; later admissions cannot keep
extending that sweep. New admissions behind the cursor and transiently refused
jobs are revisited in the next sweep. A cursor is neither a lease nor a receipt,
and restart begins discovery again from retained state.

One dispatcher and at most 128 physical workers use a zero-capacity handoff
queue. The scheduler retains at most one page, one sweep cursor, an in-flight
map bounded by the worker ceiling and per-owner counts bounded by that same
ceiling. Global and per-owner limits apply before submission; reaching a per-owner
cap leaves any remaining global slots eligible for other owners. There is no unbounded
waiting job queue or retained exception history. One bounded last-failure record
and saturating process counters describe refusals, not authoritative work or
closure counts. The host supplies one shared runtime and scheduler for its
paired authority.

Workers resolve a current retained-owner grant independently of connection
credentials, then invoke the real callback runner. Discovery observations cannot
authorize processing: claims and publication recheck current state, policy,
the trusted clock watermark, deadline and durable ownership. A live lease is
left alone across restart. Reacquiring an expired lease preserves wire attempt
identity. AWAITING_RETRY is never automatically rerun, and terminal jobs are
never re-executed. Discovery observes whether an exact successful child summary
is present and leaves waiting parents out of the worker pool, so a parent at the
front of a one-worker sweep cannot starve its own children. This bounded readiness
hint does not verify all descendants or authorize execution: the actual claim
still audits STRICT closure. Authority-produced expansion remains unsupported.

Deadline settlement runs on the discovery thread, separately from the callback
pool, including when every physical worker is busy or the owner grant is denied.
It uses the existing funded owner-independent transaction and rechecks safe UTC
and cancellation precedence. It does not need to wait for a callback to cooperate
before recording a reached deadline, nor does it release that callback's physical
slot or payload handles. Unsafe clock or storage failures retain the durable job
and produce a bounded local diagnostic; later sweeps can retry.

`close()` stops new submissions and requests worker-pool shutdown without
interrupting callbacks, cancelling logical work, closing storage or reporting
completion. Already started maintenance can finish. `awaitStopped` uses one
bounded monotonic wait for both discovery and workers; false means physical
activity may remain and the host must keep its stores open. Daemon threads are
not a promise to keep a JVM alive after its host exits. Arbitrary application or
grant code must cooperate; it is not forcibly terminated.

Discovery has bounded retained memory, not constant sweep cost or guaranteed
deadline latency. Every retained job, including terminal history, is examined
once per sweep; database work and the configured inter-page pause determine
revisit latency. Each action still performs its normal metadata/file checks.
This is not an indexed deadline queue or a many-job throughput claim. Future
indexing must preserve funded writes and restart consistency rather than moving
accepted jobs into a volatile queue.

## Incremental closure and STRICT settlement

`reconcileClosures` discovers scopes in a descending `(generation, scope)` sweep,
so a child's larger identity is normally visited before its parent. Each step
selects at most one scope and reads at most 256 direct members plus a continuation
key. Unsealed scopes and members that are not yet terminal leave no summary;
other scopes remain eligible on that sweep. Newly added scopes are revisited on
the next sweep. The scheduler performs one closure step between discovery pages,
independently of callback grants or busy physical workers.

For a sealed scope, a volatile cursor folds the actual ordered member IDs and
terminal views into the separate membership and status commitments. Its status
frontier retains at most 63 SHA-256 hashes (2,016 payload bytes), plus fixed cursor
and hash-engine state. No partial hash checkpoint or provisional summary is stored.
Restart discards partial progress and recomputes it from immutable retained
evidence. A cursor is bound to the database installation, not transferable to a
different authority. Scope state changes invalidate its partial fold.

A terminal branch also requires its exact child's committed summary and verified
descendant evidence. Existing summary verification performs the session-wide
streaming audit described above, at most once per direct-member batch that uses
child evidence. The direct-page bound does not bound that audit's total work or
transaction duration. Funded image writes also retain the existing
installation-wide reservation-header scans. This is bounded retained memory,
not constant-time closure or a demonstrated many-scope latency result.

Only a complete exact seal, terminal count partition and status root permit a
summary write. Closure time cannot precede member outcomes or descendant closure.
The writer spends one prepaid scope-image credit. A child's non-success summary
also settles an unresolved STRICT parent FAILED in the same transaction, spending
one prepaid parent work/job image pair. A previous terminal outcome is unchanged;
revocation and accepted cancellation/skip fences keep precedence. Failure of a
parent does not remove its independent child obligations. Successful parent
evidence cannot be repaired retroactively by manufacturing a missing child summary.

The transaction advances the durable UTC watermark once and checks root receipt
retention for overflow and a final clock jump that would exhaust the promised
interval before commit. Unsafe time refuses a new summary without changing its
scope or parent. No input/output funding is refunded or bytes reclaimed by closure.
The scope summary and any parent failure are one SQLite commit, including across
lost observations; re-observing an existing summary spends no additional credit.

`scopeSummary` checks current caller authorization before retained state, then
requires the expected complete membership seal and audits the committed summary.
Unsealed or unfinished scopes return NOT_READY; a changed seal is INTEGRITY_ERROR.
It returns immutable evidence without making a new clock promise. It is a snapshot
API, not the wire checkpoint's bounded wait implementation or a DRAIN handler.
Cancellation reconciliation must still supply its frozen seals and terminal
settlements before this writer can close those scopes.

## Persistence and resource scope

`BoundedSqlite` shares the Java implementation's existing bounded Linux native
VFS and immutable checksummed file policy, not the V1 session schema. Each
connection enables full synchronization and foreign keys, disables mmap, sets a
2 MiB SQLite cache target and bounds busy waiting to five seconds. Bootstrap
refuses incompatible metadata before switching to WAL. Receipts and local
configuration are bounded deterministic CBOR, with no JSON or floating-point
conversion. Receipt integrity also covers the retained profile combination and
control ceiling.

There are at most 16 simultaneous database operations across all V2 store
handles in this process, with immediate capacity refusal instead of an unbounded
waiter queue. Retained sessions, per-owner sessions and permanent owner history
have separate immutable limits. Owner-sequence queries do not allocate history.
Database, WAL, rollback journal and shared-memory lengths have the native VFS's
separate hard bounds. These are not allocated filesystem-block or total RSS
measurements, and a cache target is not a whole-process memory guarantee.

The storage contract relies on SQLite's documented
[transaction semantics](https://www.sqlite.org/lang_transaction.html) and
[full synchronization](https://www.sqlite.org/pragma.html#pragma_synchronous),
plus the native VFS. Abrupt-process-exit tests exercise actual storage recovery;
they cannot simulate every power-loss or storage-device failure.

## Remaining full-goal gates

The independent [input store](java-v2-input-store.md) now receives, verifies and
durably installs bounded immutable bytes, and exact input/database installations
can be paired explicitly. Storage admission now commits its receipt and funded
job atomically; the endpoint and executor are not activated by that fact.

Authority-produced branch expansion, explicit wire-attempt retry,
local producer-1 ingress, subtree settlement,
results/read pins, retirement/reconciliation,
durable client observations and authenticated endpoint integration remain
mandatory. Fixed image credits now protect their specifically bounded writes;
cancellation, expansion and cleanup write sets still need their real funded transitions and
cost gates before activation. This increment does not prove full
Java V2 behavior, live TLS-policy
revocation settlement, cross-language V2 equivalence or the protocol-neutral
failure driver. The external chunk/distribute/transform/reassemble workload and
equivalent authenticated durable streaming-gRPC comparison are still required.

Enabling mode 2 is not just widening the registration check. Expansion needs
its own fenced declaration/admission interface and a durable completion transition
distinct from sealing. Replacement workers must reuse original producer operation
identities. Resource admission must cover each execution phase: receiving a child
must not compete with idle parent reassembly credits for the last available handle.
The expansion phase must release its physical worker while awaiting children;
rehydration then reacquires and verifies the completed expansion and STRICT closure.
