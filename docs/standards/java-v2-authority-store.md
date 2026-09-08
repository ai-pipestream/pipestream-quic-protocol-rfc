# Java V2 authority storage

`v2.SessionStore` and `v2.DeclarationStore` are the independent Java session and
declaration transaction layer for Sections 12.3 through 12.5. They are
package-private and are not wired into a durable-profile listener. The shipped
Java endpoint still advertises Core only. Payload admission, execution, results
and retirement remain to be implemented.

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
Future admission must fund its representations and obey both current negotiated
limits and retained session admission ceilings.

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

The local database format is version 3. Earlier experimental V2 storage formats,
like V1 and foreign databases, are refused without conversion. This is an internal
format revision, not a wire-profile change or an authorized reset of an existing
authority identity.

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
Admission still has to fund its complete write set and any larger representations.

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
durably installs bounded immutable bytes. It is not yet bound to this authority
database and does not issue admission receipts or create jobs.

Funded payload admission, worker leases and attempt fences, subtree settlement,
results/read pins, retirement/reconciliation,
durable client observations and authenticated endpoint integration remain
mandatory. Fixed image credits now protect their specifically bounded writes;
the complete admission, terminal and cleanup write sets still need their own
funded transitions before activation. This increment does not prove full
Java V2 behavior, live TLS-policy
revocation settlement, cross-language V2 equivalence or the protocol-neutral
failure driver. The external chunk/distribute/transform/reassemble workload and
equivalent authenticated durable streaming-gRPC comparison are still required.
