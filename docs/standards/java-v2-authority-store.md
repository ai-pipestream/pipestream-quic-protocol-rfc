# Java V2 authority storage

`v2.SessionStore` is the independent Java creation/attachment transaction layer
for Section 12.3. It is package-private and is not wired into a durable-profile
listener. The shipped Java endpoint still advertises Core only. It does not yet
implement declaration, payload admission, execution, results or retirement.

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

Declaration/operation journals, funded payload admission, worker leases and
attempt fences, subtree settlement, results/read pins, retirement/reconciliation,
durable client observations and authenticated endpoint integration remain
mandatory. This increment does not prove full Java V2 behavior, live TLS-policy
revocation settlement, cross-language V2 equivalence or the protocol-neutral
failure driver. The external chunk/distribute/transform/reassemble workload and
equivalent authenticated durable streaming-gRPC comparison are still required.
