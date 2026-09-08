# Java V2 authority storage

`v2.SessionStore`, `v2.DeclarationStore`, `v2.AdmissionStore`, `v2.ExecutionStore`,
`v2.PublicationStore`, `v2.ClosureStore`, `v2.FenceStore`, `v2.BranchStore`,
`v2.ResultStore` and `v2.ResultService` are the independent Java session,
declaration, admission, local execution, publication, closure, direct-child
dependency and local result-read layer for Sections 12.3 through 12.9. They are
package-private and are not wired into a durable-profile listener. The shipped
Java endpoint still advertises Core only. Local authority-produced expansion is
implemented behind the host-owned runtime; result-stream transport, dependency-safe
cleanup, retirement and endpoint integration remain to be implemented. Storage and local execution behavior are not
endpoint interoperability.

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
receipt. Local administrative revocation now installs a real root cancellation
fence, as described below. Retirement still requires the cleanup implementation;
its reserved flag is not a supported flag-setting shortcut.

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

The local database format is version 7. Earlier experimental V2 storage formats,
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
future work writes. A 2,048-byte job image reserves eight writes: a conservative
four-write expansion/settlement envelope plus four input/output reclamation
intent/completion writes. Branches atomically
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

These storage operations are consumed by the local mode-2 expansion runtime, not
exposed as a durable endpoint. A child membership seal alone still leaves
`expansionComplete=false` in its parent job. The expander must return Complete only
after its stable producer operations have sealed the child scope and every declared
member has been admitted or has already reached a permitted terminal outcome;
the committing transition verifies those obligations.
A Yield keeps the same wire attempt and durable expansion obligation for a replacement
lease. No wire message, profile advertisement or storage-format revision changes here.

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
second pair. The separate four-write cleanup allowance remains funded, including
when expansion or cancellation has also consumed lifecycle writes.

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
It accepts exact leaf (mode 0), caller-expanded branch (mode 1) and
authority-expanded branch (mode 2) registrations matching the admission registry's
label, mode and restart-safety contract. Mode 2 has a separate `Expander` phase and
reassembly callback: expansion declares and admits producer-1 children, while the
ordinary callback runs only after completed expansion and successful STRICT child
closure. Missing phase callbacks and mismatched contracts are refused, not executed
through a fallback.

The host configures explicit local-producer capability ceilings. Expansion narrows
those ceilings to the exact durable profile retained by the session; a retained
durable-only session cannot silently acquire results. Replacement leases replay the
original producer-1 operation identities and immutable receipts, inputs, deadlines
and admitted jobs rather than duplicating them. Complete is refused while a produced
receiver is unfinished or the sealed/admitted membership obligations are incomplete.
Resource LIMIT_EXCEEDED may Yield without changing the wire attempt. Either Complete
or Yield ends the expansion invocation and releases its physical worker slot and
phase-specific receiver credit after safe cleanup; reassembly later acquires its own
input, child-reader and optional output-writer resources.

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
still audits STRICT closure. Authority-expanded parents are discoverable while their
expansion obligation is pending; after Complete they wait for the same authoritative
STRICT closure before reassembly.

Dispatch preserves the first ready position blocked by global worker capacity,
rather than repeatedly restarting at a yielding parent. At the end of a finite
sweep it may inspect one fresh suffix for newly admitted children, then must wrap;
continuous tail admissions cannot extend that sweep indefinitely. Deadline
maintenance has a separate finite cursor, so it keeps progressing while dispatch
is capacity-blocked. Each poll examines at most one configured page for dispatch
and one for deadline maintenance, in addition to the existing closure batch. The
extra bounded metadata scan is a real cost, not a zero-overhead fairness claim.

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
different authority. Monotonic cancellation/revocation flags restart a partial
fold; changes to its pinned membership or session are refused as inconsistent.

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
job atomically; admission itself neither invokes application code nor activates the
network endpoint.

Results/read pins, retirement/reconciliation,
durable client observations and authenticated endpoint integration remain
mandatory. Fixed image credits now protect their specifically bounded writes;
remaining cleanup write sets still need their real funded transitions
and cost gates before activation. This increment does not prove full
Java V2 behavior, live TLS-policy
revocation settlement, cross-language V2 equivalence or the protocol-neutral
failure driver. The external chunk/distribute/transform/reassemble workload and
equivalent authenticated durable streaming-gRPC comparison are still required.

Local mode 2 uses its own fenced declaration/admission interface and a durable
completion transition distinct from sealing. Replacement workers reuse original
producer operation identities. Resource admission covers each execution phase:
receiving a child does not compete with idle parent reassembly credits for the last
available handle. The expansion phase releases its physical worker while awaiting
children; rehydration then reacquires and verifies completed expansion and STRICT
closure. These local guarantees do not activate the V2 network endpoint or establish
full conformance or interoperability.

## Explicit attempt replacement

`SessionStore.retry` accepts WORK operation 6 through the local owner-authorized
API. Its access and application gates must authorize retry, not merely read
access. A fresh operation checks the expected attempt, all applicable fences,
original deadline, configured restart contract and remaining operation capacity.
WORK/JOB image credits are replenished under the same bounded WAL policy used at
admission. Their rewrites and the immutable caller-namespace receipt commit together.
The local lease counter is not reused; its expiry is cleared and the next claim
advances the counter. The wire attempt advances by exactly one.

Input and output funding stay charged, and the original input, admission time,
deadline, child scope and completed-expansion flag are retained. Caller-expanded
work and already-completed authority expansion return to WAITING_CHILDREN.
Incomplete authority expansion remains resumable with its prior producer receipts.
The runtime's existing replacement-claim rules govern stale unpublished output;
retry itself neither deletes bytes nor runs a callback.

Before commitment the authority rechecks current owner/application permission,
the pre-transition work fences and original deadline, and the operation-local
clock bound. Any refusal rolls back the replacement, receipt, accounting, fixed
record credits/revisions and clock watermark. Exact receipt replay still checks
current authorization but does not acquire a lease or sample time. It remains
valid after the original deadline or a later terminal outcome; a new retry does not.

Storage format 6 introduced typed retry intent and indexed work/expected-attempt keys
to the existing operation journal. Recovery checks each index against its typed
request and receipt, the admitted work and retained clock. Unique expected-attempt
keys plus count/high-water checks prove a gap-free sequence from admission attempt
one to the current attempt. Receipt retention remains charged and bounded by the
session operation limit. These are local transaction/recovery guarantees; the
full durable Java wire/client path remains unfinished.

## Cancellation, skip and bounded reconciliation

`SessionStore.cancel`, `skip` and `cancelScope` accept typed caller intents under
current owner and operation-specific policy. Skip requires explicit permission.
The first accepted own fence fixes CANCELLED or SKIPPED; a conflicting later
fence is CANCELLED, while an exact operation replays authenticated evidence
without a clock sample. Already terminal work yields disposition 1 and retains
its original outcome. New cancellation can win after the execution deadline if
terminal failure has not committed first. Deadline maintenance cannot overwrite
an accepted own or ancestor fence.

Work acceptance writes its prepaid typed fence, work/job state and operation
receipt in one SQLite transaction. A leaf or branch with an already closed child
can settle immediately; an unresolved branch remains CANCELLING. Its old lease
is excluded immediately, and ancestor checks prevent new declaration, admission,
retry and publication anywhere below it. An earlier own skip remains SKIPPED
when a later ancestor cancellation covers the subtree. Descendants otherwise
settle CANCELLED. Payloads and output funding remain charged; cancellation does
not reclaim files or claim to undo external effects.

Scope cancellation freezes the accepted membership at commit, without needing a
live producer. `revoke` is a local, owner-independent administrative operation:
its gate must authorize revocation of the target generation. It atomically denies
caller access and installs the root cancellation fence, including for unadmitted
declarations. It is not an authenticated peer RPC or a live TLS-policy integration.

`reconcileCancellation` uses installation-bound volatile keyset cursors to inspect
at most the requested number of direct work records and membership IDs per call
(one through 256 of each). It incrementally computes one scope's actual full seal;
until that commit, pages still report `sealed=false` and a null seal. Partial
hashes are never stored as evidence. Restart uses fresh cursors and reconstructs
progress from frozen records. Ancestor walks and existing funded-image reservation
scans add work beyond the direct-record counts; those counts are not process-memory
or latency measurements. The scheduler invokes this maintenance independently of
occupied callbacks and invokes the separate closure fold afterward. Only actual
child closure allows a branch's terminal cancellation.

Each mutation rechecks final authorization and safe UTC. A forward jump exhausting
a newly promised receipt interval rolls back all tentative writes and credits.
Reconciliation discards a hash advanced inside a failed transaction. A later
monotonic scope fence restarts a partial closure fold, but cannot change its pinned
membership. This adds no placeholder seal or relaxed closure proof.

Private format 7 adds typed cancellation journal entries, a scope-cancellation
index checked against the immutable request, and the pending job state. Recovery
requires accepting receipts for own fences, direct or inherited provenance for
scope flags, matching root/session revocation, valid terminal intervals, and child
closure before branch settlement. Earlier private schemas are refused without
conversion. Wire values and Section 12's cancellation semantics are unchanged.

## Result evidence and bounded delivery leases

`SessionStore.manifest` returns an exact authenticated publication without
consulting UTC or payload files. It checks the producing work/job relationship,
manifest, retained profile and response ceiling. Availability expiry does not
erase that evidence, renew a promise or assert that bytes are still present.
Result permission is a separate current gate, not inferred from application
execution permission, a historical lease or a locator.

Fresh reads check the exact work, attempt, output index and expected commitment
inside the same writer transaction that pins the payload and remembers safe UTC.
The final permission, elapsed-lifetime and UTC checks follow file verification.
Deadline equality refuses the new lease. Refusal before commit closes the pin
and rolls back the watermark. Storage opens one descriptor and fully verifies
the committed bytes on it, rather than hashing once to find a file and again to
open it. Missing/corrupt retained bytes are OUTPUT_UNAVAILABLE, not a rerun.

`ResultService` owns the exclusive input installation's single read registry.
Configured global and per-owner counts include acquisition and stream-slot
waiting across connections, not just started streams. Every opened descriptor
also consumes the existing shared input/output handle pool. A read allows one
bounded outstanding payload chunk; reported accepted transport progress may
consume it incrementally. Neither a disk read, a deadline check nor zero-byte
progress renews the idle deadline. Stream start does not reset either deadline.

The service's independent timer checks a finite snapshot of at most 128 reads.
Busy entries stay pinned; foreground operations check again after blocking
storage work. Elapsed deadlines use a local monotonic nanosecond source, including
signed values and normal nanoTime wrap, not wire UTC arithmetic. A lease acquired
before external expiry can finish without sampling UTC again, but current owner
permission and revocation remain gates before further scheduling. The transport
must call `check` before each write or FIN, including after flow-control waits,
and call `sent` only for payload accepted by its bounded transport writer.

FIN after complete payload or abort closes the physical reader without refunding
durable object bytes. Failed physical closes remain charged for cleanup retry;
service shutdown does not detach its storage claim while any read is outstanding.
The service does not retract already buffered bytes or establish client receipt.
It is not yet wired to Java's V2 Netty endpoint. Per-connection request/stream
credits, native send-buffer ownership, full process bounds, dependency-safe
reclamation and retirement require their own integration and evidence.
