# Java V2 authority storage

`v2.SessionStore`, `v2.DeclarationStore` and `v2.AdmissionStore` are the independent
Java session, declaration and admission transaction layer for Sections 12.3
through 12.5. They are
package-private and are not wired into a durable-profile listener. The shipped
Java endpoint still advertises Core only. Worker execution, results and retirement
remain to be implemented; storage admission is not endpoint interoperability.

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
membership map. Producer-1 declarations are not exposed through an unfenced local
shortcut; they require the future parent-attempt/lease/deadline checks.

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
admissions share one operation namespace with explicitly tagged stored requests;
cross-kind identity reuse and changed admission headers are CONFLICT.

The committing transaction holds the paired input-store monitor and SQLite writer.
It installs durable output funding before creating authoritative metadata links,
then commits the work view, attempt 1, one child scope for modes 1/2, restartable
job, original operation receipt, representation requirements and UTC watermark
together. Mode 1 waits for caller children; mode 2 starts active with a separately
retained unfinished-expansion obligation. A membership seal cannot complete that
obligation. No callback runs inside admission. A receipt promises accepted work,
not that processing has succeeded or that Java's worker loop is implemented.

Input/output charges and global, per-owner and per-session job counts derive
from bounded typed job records. They are not reconstructed from a client stream
or a success counter. Output allowances remain charged in the same file store
that funds ordinary inputs. Failed metadata admission can leave an installed,
charged file or funding orphan, but no receipt, admitted view, job or child scope.
Recovery verifies operation/job/member coverage, parent/child agreement, funded
image geometry, profile bounds and the UTC watermark. Paired-store verification
also checks every retained input and funding reference before treating that pair
as ready. No orphan is reclaimed by this increment.

Caller descendants inherit cancellation/skip fences, not their parent's deadline
failure. They remain independent obligations after that deadline. The future
local producer-1 worker interface must additionally enforce its parent attempt,
lease and deadline; the caller API does not expose that interface.

The deployment supplies trusted UTC explicitly. Missing trust, negative time,
regression within an operation or regression behind the retained watermark is
CLOCK_UNSAFE for a new admission. Final policy checks precede the last UTC sample,
which must still precede the proposed execution deadline. All deadline and
retention additions are checked before commitment. Replays remain observable
under unsafe time without issuing a new promise. Forward jumps are accepted
only when the deployment marks that sample trusted; a jump across the proposed
deadline refuses the admission, never clamps or extends it. This API does not
establish clock trust, backup freshness or elapsed time across power loss.

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

Worker leases and attempt fences, local producer-1 ingress, subtree settlement,
results/read pins, retirement/reconciliation,
durable client observations and authenticated endpoint integration remain
mandatory. Fixed image credits now protect their specifically bounded writes;
terminal and cleanup write sets still need their real funded transitions and
cost gates before activation. This increment does not prove full
Java V2 behavior, live TLS-policy
revocation settlement, cross-language V2 equivalence or the protocol-neutral
failure driver. The external chunk/distribute/transform/reassemble workload and
equivalent authenticated durable streaming-gRPC comparison are still required.
