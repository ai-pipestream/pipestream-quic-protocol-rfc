# Java completion reservations

This is the independent Java implementation's local storage contract, not a
new wire extension or a full sealed-work conformance claim. `PSJDB002` file
policy and Java schema 5 refuse older layouts without conversion.

## What admission funds

Managed PROCESS admission allocates both job rows and protects the remaining
acquisition, publication, possible child-scope conversion, and rehydration
writes. Entity and closure images were already allocated by declaration.
The job input and state remain checksummed Java records, not Rust session
images or a second copy of protocol state.

The exact fixed-size write sets are:

- Acquisition: the 256-byte job state.
- PROCESS publication: a 112-byte entity state, a 256-byte job state, and
  possibly the 256-byte retirement state of the unused future.
- Child-scope conversion: the 128-byte closure, 112-byte parent state,
  preallocated future descriptor, 32-byte descriptor hash and 256-byte state.
- REHYDRATE publication: the 112-byte entity and 256-byte job state.

QUEUED jobs hold acquisition plus publication credit. RUNNING jobs hold
publication credit. RESERVED futures hold conversion, acquisition and
publication credit. Finished, refused and retired jobs hold no execution
credit; their allocated record capacity remains logically charged.
An expired RUNNING lease does not receive a fresh acquisition reservation.
Renewal must fit ordinary write headroom and cannot spend publication credit.

Ordinary terminal PROCESS results retire the unused future. DEHYDRATING results
retain it. Successful child closure consumes conversion credit; STRICT failure
retires the future and releases its unneeded execution credit. Replay does not
consume another stage or mutate an already recorded result.

## Cost derivation

The model is tied to the native guard's pinned Xerial SQLite 3.53.4 engine,
WAL with FULL synchronization, no auto-vacuum, zero reserved bytes per page,
and Unix sectors no larger than 65,536 bytes. Unsupported geometry is refused.
Dependency changes require rechecking the implementation and cost tests.

For page size `p`, an exact-size BLOB write of `n` bytes dirties at most
`ceil(n / (p - 4)) + 1` pages. A BLOB starting in a leaf enters its first
overflow page at offset zero. A BLOB starting in overflow can straddle an
additional page. This bound covers both cases without assuming row alignment.
Only existing payload bytes change, so no B-tree allocation, mutable index
maintenance or auto-vacuum pointer-map write is needed. See SQLite's
[overflow-page format](https://www.sqlite.org/fileformat2.html#overflow_pages).

Sum that page bound independently for every image in the stage. Counting a
shared page more than once is conservative. Let this sum be `d` and a WAL
frame's size be `f = p + 24`. The per-stage reservation is:

```text
32 + (d + 1 + ceil(65536 / f)) * f
```

The bundled amalgamation's `walFrames` overwrites same-transaction spill
frames, but can repeat its final commit frame and pad to a sector boundary.
The extra frame, sector allowance and WAL header cover those paths. The
incremental BLOB path does not rewrite SQL rows or activate UPDATE triggers.

The usable WAL ceiling also protects shared memory. The pinned WAL-index
maps 4,062 frames in its first 32 KiB region and 4,096 in each later region.
The VFS rounds mappings to 64 KiB. Therefore `r` funded regions permit at most
`32 + (4062 + (r - 1) * 4096) * f` WAL bytes. Use the smaller of this value
and the immutable WAL file cap. See SQLite's
[WAL-index layout](https://www.sqlite.org/walformat.html).

## Transaction boundary and rollback

Every public store write transaction obtains `BEGIN IMMEDIATE`, audits retained
records, and installs a connection-local ceiling of usable WAL bytes minus
all remaining credit before its first mutation. Admission adds future credit
before changing the entity or inserting jobs. A validated stage releases its
allowance before its first fixed-image write. A final audit must match the
predicted remaining credit before COMMIT; otherwise the transaction rolls back.

The ceiling remains in force through COMMIT and ROLLBACK, and is inherited by
WAL handles opened later on the same connection. Other connections cannot
relax it. Ordinary declarations, manual lifecycle calls and checkpoint writes
must fit below existing execution reservations.

Read-only observations use `query_only=ON` snapshots, not writer locks. The
listener reads its at-most-128 observed jobs in one audited snapshot. Each
requested key still receives ownership, lifecycle and processing/future-pair
validation; missing observations are refused, not silently dropped. Read-only
APIs cannot invoke the native writer helpers or mutate SQL state.

Physical WAL length is not the committed append cursor. Failed transactions
can leave uncommitted tails that SQLite overwrites on retry. The implementation
does not reject a read or retry merely because file length plus remaining
credit would exceed the cap. Instead, the VFS bounds SQLite's actual write
offsets. Rollback leaves the authoritative job states and their credit intact.

## Evidence and limits

`SealedCompletionReservationsTest` measures 378 whole transactions across 54
scenarios: 512-, 4,096- and 65,536-byte pages, two- and 2,000-page caches,
127-, 4,096- and 65,000-byte metadata values, and successful, failed or refused
rehydration. Every measured stage must remain within its own reservation and
leave the main page count unchanged. Additional tests cover admission refusal,
renewal, misaccounted credit rollback, old-policy refusal, pinned-reader
recursive completion, STRICT retirement, shared-memory-first exhaustion,
reopen, and abort after actual large-descriptor conversion spills followed by
retry with the same reader pinned. The original 512 KiB publication regression
remains in `SealedJobsTest`.

These are configured file-length and admitted-job guarantees for private local
directories with cooperating library writers. They are not filesystem-block
preallocation, protection against arbitrary raw database writers, a disk-latency
bound, or a power-loss proof. Unknown future children, new payloads and new
checkpoint records still need separate admission capacity. Full retained-record
audits remain on the storage path; this is not a constant-time admission or
large-session throughput claim. Orphan reconciliation, persistent producer
observations and the broader goal's resource/conformance matrix remain due.
