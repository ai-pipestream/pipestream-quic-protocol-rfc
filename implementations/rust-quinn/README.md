# Rust protocol prototype

The Rust implementation exercises selected PipeStream Layer 0, Layer 1,
and Layer 2 behaviors. It is not feature-complete or fully conformant.
The specification is authoritative; codecs and vectors must be corrected
when they contradict it. See [draft-04 readiness](../../docs/standards/draft04-readiness.md)
for tested requirements and remaining design and implementation gaps.

The workspace separates protocol behavior from transport and deployment:

- `pipestream-core` contains the independent wire codec, entity and scope state
  machines, manifests, completion policies, checkpoints, claim state, lineage
  digests, and persistence interfaces. It has no Quinn dependency.
- `pipestream-quinn` contains TLS 1.3 and ALPN setup, QUIC stream handling,
  out-of-order chunk reassembly, the reusable client, and the embeddable server.
- `pipestream-server` builds the `pipestream-quinn` command-line server and
  scenario client.
- `pipestream-conformance` is a protocol-neutral process driver. It does not
  depend on any PipeStream crate and is not a fourth implementation.

```bash
cargo test --locked
cargo build --release --locked
```

The original `serve` and `send` commands retain the common Layer 0 black-box
contract documented under `conformance/`. The durable server uses:

```bash
target/release/pipestream-quinn serve-recursive \
  --bind 127.0.0.1:9443 \
  --cert server.crt --key server.key \
  --state-db state/sessions.sqlite3 \
  --entity-dir state/entities
```

`serve-recursive` accepts concurrent connections with a configurable bound,
enforces scope depth, entity-count, frame, chunk-count, and payload limits, and
persists state transitions before reporting them. SQLite runs in WAL mode with
full synchronous durability and checksummed, versioned session records. Payload
objects and final lineage digests are immutable, fsynced files. A periodic
dispatcher executes admitted jobs independently of attached connections,
including durably redeemed claims interrupted before completion.

The public `RecursiveService`, `RecursiveServer`, `RecursiveClient`,
`EntityProcessor`, `EntityStore`, and `SessionStore` APIs support embedding the
same behavior without the command-line process. Applications provide processing
and storage behavior while the service owns protocol transitions and durable
state.

## Sealed work sets

`RecursiveClient::connect_sealed` requires private-use extension 65281
(`sealed-work-sets-v1`), Layer 1, and no Layer 2. `serve-recursive` supports
it without requiring it from legacy clients. The profile is defined in
Section 9.8, not just by these APIs.

The client calls `declare_work` with a `work_set::WorkSetFrame` before
sending any declared entity. Start with root scope 0, sequence 0, a stable
nonzero 16-octet producer label, and a unique session ID. Batches contain
up to 256 strictly increasing IDs. The final batch sets `SEAL` and includes
`work_set::seal_digest` over the entire set. `declare_work` waits for and
compares the durable ACK. IDs cannot be removed or reused after declaration.
The producer label is not an authentication credential.

Child declarations require an admitted DEHYDRATING parent. Scope digests
and checkpoints wait for the immutable set, including missing declared
entities. Checkpoints name the inclusive largest declared ID in their scope;
GOAWAY names the largest root ID after an acknowledged root checkpoint.
Payloads can arrive out of ID order after their declaration ACK.

After a connection loss, connect with the same profile and replay the original
root sequence-0 request to attach to the retained session. Identical batches
replay the same ACK; changed identities, sequences, or seals are refused.
This is declaration replay, not automatic retry or recovery of application
effects. A rejected or missing payload remains outstanding. No cancellation
tombstone, authenticated claim redemption, or server-originated work is
implemented in this profile.

`sealed-scenario --connect HOST:PORT --ca ca.crt --session-id UNIQUE_ID` runs
the public sealed producer against a server implementing the exemplar actions.
It declares two roots, three children, and two grandchildren, sends out-of-order
chunks, verifies recursive closure and scoped checkpoints, reconnects to replay
declarations and the root ACK, and checks changed-owner/request refusals. The
Java interoperability suite runs this command against `SealedServer`. This is
an application scenario, not a complete conformance oracle or persistent
producer recovery ledger.

Stored session format is now version 7, including durable owner, claim/session
revocation, execution attempts, typed jobs, retained recovery receipts, and the
original optional checkpoint scope. An omitted root scope and explicit zero
are both valid, but remain distinct for ACK correlation and replay identity.
Versions 1 through 6 are refused without conversion or
modification; preserve old databases with their matching binary.
The implementation caps each session at 1,000,000 declared IDs, in addition
to negotiated per-scope limits. SQLite still serializes the whole session
on each transaction, and final sealing walks the full identifier set.
These bounds are not a throughput or large-session performance claim.

## Mutual TLS and session ownership

Configure all three authentication settings together:

```bash
target/release/pipestream-quinn serve-recursive \
  --bind 127.0.0.1:9443 --cert server.crt --key server.key \
  --state-db state/sessions.sqlite3 --entity-dir state/entities \
  --client-ca client-ca.crt --authority example-authority \
  --principal-map principals.tsv
```

`principals.tsv` starts with `sha256<TAB>principal`, followed by one hexadecimal
SHA-256 fingerprint of a DER client leaf certificate and its stable principal
per row. Trust-chain validation and certificate proof of possession still
apply; a fingerprint is not a credential. The map permits 1..4096 certificates;
multiple certificates may map to one principal for rotation. Authority and
principal identifiers contain 1..128 ASCII alphanumeric or `-._~` characters.

Recursive client commands accept `--client-cert` and `--client-key` together.
The library uses `RecursiveClientOptions::identity` and `ClientIdentity`;
embedded servers call `RecursiveService::with_authentication` before binding.
Both sides require private-use extension 65282, `authenticated-session-v1`.
A configured client refuses an anonymous server instead of dropping its
authentication requirement. Retained recovery additionally requires the
separate extension described below.
TLS session resumption is disabled on this path so reconnects recheck client
credentials; 0-RTT remains disabled on every path.

The first admission atomically records the principal and issuing authority.
All subsequent mutations check ownership and revocation inside the session
transaction. `Session::revoke_access` is an operator API to invoke through the
store's transaction mechanism; it disables live/reconnected access and
background recovery. An unprotected listener sharing the database cannot
access a bound session. An authenticated listener does not adopt anonymous
sessions. Neither producer labels nor metadata identify the caller.

Principal maps are loaded at startup. Reconfigure them to withdraw certificates
from future connections; revoke a session to deny its existing connections.
This is not online certificate-status checking or portable recovery between
unrelated authorities. Claim revocation is a separate durable operator action.

## Retained authenticated recovery

`RecursiveClient::connect_recovery` requires client identity, Layer 2, and both
private-use extensions 65282 and 65283 (`authenticated-recovery-v1`). The
authenticated server supports this profile without requiring it from legacy
clients. It cannot be combined with sealed work. Section 10.6.5 defines the
request, receipt, and terminal-outcome wire contract.

Persist a `recovery::RecoveryRequest` before sending it: configured authority,
session ID, a unique nonzero 16-byte request ID, claim ID, and stopping-point
checksum. `accept_recovery` returns a `RecoveryReceipt` after claim redemption,
the resume job, and the receipt commit in one transaction. It acknowledges
admission, not successful execution. `wait_recovery(&receipt)` returns an
explicit `RecoveryOutcome::Complete` or `RecoveryOutcome::Refused(JobFailure)`.
A refusal is not success even if its diagnostic code is zero. Consume the
outcome or reconnect before sending another recovery request on that client.

After a lost response, resend the identical persisted request. During the
receipt's 24-hour interval, it returns the same receipt and retained terminal
outcome when available, without enqueueing again. Claim expiry gates first
acceptance; it does not cancel an already-accepted job. Receipt expiry, changed
request identity, wrong authority/owner, and revocation are named refusals.
Receipts and completed/refused outcomes remain immutable across store reopen.
Legacy `CLAIM_REDEMPTION` stays single-use and is refused on this profile.

Call `Session::revoke_claim` through a store transaction for irreversible claim
revocation. It denies initial acceptance, receipt replay, attempt acquisition,
and result publication; already-committed external effects are not undone.
Revoked unfinished jobs remain charged to the durable queue. Each session
retains at most 1,024 recovery receipts; expired entries are not evicted to
admit new requests. Separate retained-state and payload quotas apply;
completion-space reservations and reclamation remain unfinished.

## Other prototype paths and limitations

The Layer 1 end-to-end scenario is runnable with `recursive-scenario`. It sends
one root, three children, and two grandchildren; completes descendants out of
order; verifies nested scope digests; crosses scope barriers and checkpoints;
rehydrates both parents; and persists a deterministic final lineage digest.
`begin-yield` and `redeem` exercise disconnect, cross-server claim redemption,
single-use replay refusal, and recovery.

All transport paths require TLS 1.3 with ALPN `pipestream/1`; 0-RTT is never
enabled. The implemented Layer 2 scope is intentionally narrow. Automatic retry
scheduling, claim federation between unrelated persistence domains, and the
other optional resilience behaviors are not claimed.

## File-backed receive payloads

The recursive receiver reads and validates the bounded CBOR header first,
then writes payload octets in 8 KiB pieces to temporary files. Measured length
and SHA-256 are checked at FIN before admission. It retains up to eight stream
readers per connection. QUIC receive windows are explicitly 1 MiB per connection
and 64 KiB per stream; these flow-control windows are not a total memory limit.

`ProcessContext::payload` is now a file-backed `spool::Payload`, not a byte
slice. `reader()` implements `std::io::Read`; `len()` and `digest()` describe the
validated input. Processing returns `Result<ProcessingDisposition, ProtocolError>`
so read failures cannot become successful processing. The exemplar hashes input
through an 8 KiB buffer. Chunk assemblies retain ordered file segments and
verify each segment before calculating the combined digest. They do not build
a contiguous payload buffer or a second assembled temporary file.

`FileEntityStore::open_with_spool_limits` configures temporary receive quotas:

| Scope | Bytes | Files |
|---|---:|---:|
| Store directory, shared by handles in one process | 256 MiB | 4,096 |
| Authenticated authority/principal, across connections | 128 MiB | 1,024 |
| Connection | `max_entity_bytes` | 512 |

At most 1,024 principal budget entries may be active; anonymous connections
share one identity bucket. A payload reader or clone retains its disk credit.
Empty files consume file credit. Exhaustion is `PIPESTREAM_LIMIT_EXCEEDED`,
not an unbounded wait while incomplete items hold all the capacity. File I/O
owns its credit until it finishes even if the receiver is cancelled. Failed
cleanup retains credit instead of claiming disk space was reclaimed.

Restart counts abandoned files against the store quota without deleting them.
Live handles for the same directory share accounting and cannot reset it with
different limits. Accounting is not coordinated between separate operating
system processes. Temporary receive budgets do not limit retained entity files,
SQLite state, or the filesystem cache. Completion-space reservations and explicit
orphan reclamation remain required before production
multi-tenant use. Do not run multiple writer processes against this spool root.

`cargo test -p pipestream-quinn --test spool_resources -- --nocapture` sends
32 MiB over real QUIC without allocating an input-sized client buffer. It gates
instrumented Rust heap growth below 12 MiB and individual allocations below
4 MiB, verifies the persisted digest, and requires temporary disk credit to
return to zero. One local run on 2026-09-05 measured 132,968 bytes of heap growth
and a largest allocation of 15,972 bytes. This is a single-transfer allocation
measurement, not a process-RSS, concurrency, or throughput claim.

## Bounded asynchronous execution

### Retained session-state quotas

`SqliteSessionStore::open_with_limits(path, JobQueueLimits, StorageLimits)` sets
both durable policies when creating a database. Default state limits are
128 MiB and 4,096 sessions globally, 32 MiB and 1,024 sessions per authority and
principal, and 8 MiB per serialized record. Anonymous sessions share one bucket.
`storage_usage` and `principal_storage_usage` report retained bytes, protected
publication growth, and counts. Their `charged_bytes()` sum is the byte charge
against all three limits: global, principal and individual record.

All create/save/transaction paths commit the state charge, session revision,
and job index atomically. Completing, refusing, or revoking work does not erase
its retained-state charge. A full store refuses new work with
`PIPESTREAM_LIMIT_EXCEEDED`, without changing previously acknowledged state or
evicting receipts. Identical declaration replay remains possible at the session
count limit. Serialization stops at the record cap rather than allocating an
oversized output and checking afterward. Reads cap blob materialization and
validate the accounting entry in the same SQLite snapshot as the session.

The storage policy persists across reopen and cannot be replaced by another
handle. Missing policy or accounting is corruption, not empty capacity.
`integrity_check` verifies per-session identity/length and aggregate limits.
Every write verifies checksummed accounting metadata before using its capacity;
missing or altered entries cannot create free space for another session. This
scan is bounded by the session-count policy, not constant-time. No large-store
throughput claim is made.
The session payload format is version 7. Storage policy version 3 adds future
rehydration charges to the protected publication and continuation-token policy.
Queue policy version 2 distinguishes ordinary jobs from reserved/active rehydration.
Version-1 and version-2 storage policies, the old unversioned queue policy,
and unaccounted stores are refused without conversion. No operational database is migrated or
silently assigned new quotas. Preserve old stores with their matching binary.

### Protected publication growth

Before persisting a new job, the store charges its current serialized bytes and
the possible growth of its outcome, entity output digest and execution record.
Queued attempts reserve their future map entry; running attempts retain space for
larger epochs/timestamps and the completion timestamp. Refusals reserve the full
512-byte diagnostic allowance. Layer 2 processing additionally reserves a claim
with the configured maximum token and all supported validation fields, including
the 256-byte checkpoint reference. Map-length prefix growth is charged separately.
The calculation uses the pinned Postcard serializer's size counter for the actual
types, without cloning the full session or allocating a maximum-sized token.

`StorageLimits::yield_token_bytes` is a configurable 64 KiB default, not a change
to the protocol's 24-bit token ceiling. `ProcessContext::max_yield_token_bytes`
exposes the smaller of the retained policy and the usable STATUS frame budget
before calling application code, or zero if Layer 2 was not negotiated. The
current frame budget is 1 MiB minus its 24-byte STATUS/YIELD overhead. Configure enough
record, principal and global credit for the requested budget; otherwise admission
refuses. The service checks a returned token before installing the claim. A token
outside the admission budget produces a retained `PIPESTREAM_LIMIT_EXCEEDED`
refusal, not a truncated token or a successful entity. Direct store callers face
the same bound, with transaction rollback before any claim/outcome commit.

All store mutation paths protect these reservations from unrelated work. The
serialized record cap is reduced by outstanding reservations before serialization,
and the checksummed accounting entry binds actual and reserved bytes to the session.
Reopen recomputes the reservation from retained jobs. Acquisition, lease expiry,
revocation and disconnect do not discard it. A terminal job converts needed credit
to actual bytes and releases only unused publication credit; its retained outcome
stays charged. Callback-created memory and external effects are not bounded by
this policy, and application idempotency/fencing is still required.

Tests exercise every current processing outcome, rehydration and resume at the
exact logical quota, full refusal diagnostics, token/prefix boundaries, concurrent
principal admission, revocation, corruption, rollback and abrupt process exit.
Real authenticated QUIC tests hold a callback while other admissions fill the
store, then publish its reserved yield. Another test checks the exact token limit,
retained oversized-result refusal and authenticated resume of an in-budget claim,
including the exact frame boundary and one byte over it.

Processing admission additionally reserves its possible rehydration descriptor,
maximum outcome/attempt, parent output digest and child scope-close digest. The
future scope identifier and counters use their maximum serialized widths; map
prefix growth covers all outstanding job and attempt insertions together. Waiting
DEHYDRATING parents keep that credit, including across revocation and restart.
Creating the child scope and admitting descendants still require their own capacity.
Scope closure, rehydration admission and conversion of the reservation commit
together. Ordinary terminal/refused processing releases only unused future credit;
actual outcomes remain charged. A retained rehydration refusal does not complete
its parent or erase its unresolved obligations.

These are logical serialized-state reservations, not reserved DB/WAL pages,
filesystem blocks, payload storage or total process memory. Physical
publication headroom, final-lineage headroom, and orphan reconciliation remain
unfinished. Physical exhaustion may still refuse publication, leaving work
unfinished and charged rather than inventing successful completion.

### Retained payload and lineage reservations

`FileEntityStore::open_with_limits(root, SpoolLimits, RetainedLimits)` configures
both receive and retained stores. `open` and `open_with_spool_limits` read an
existing retained policy or create the default for a new directory. Defaults
reserve at most 512 MiB and 8,192 objects globally, 128 MiB and 2,048 objects per
authority/principal, and 1,024 retained principal buckets. Anonymous work shares
one bucket. Staging is separately capped at 128 MiB and 32 objects; at most 32
object operations may run concurrently under the default policy.

Every object reserves its entire length plus 512 bytes of checksummed identity
metadata and a 32-byte publication receipt before any file is created. The
reservation includes final lineage files. Pending copies additionally reserve
their full payload length and one staging object until staging cleanup finishes.
These conservative sums bound file lengths, not filesystem allocation or RSS;
hardlinked staging/publication names can refer to the same bytes. Temporary
receive spools and SQLite files retain their independent budgets.

`retained_usage` and `principal_retained_usage` report retained and staging
reservations, including incomplete writes. Matching replay does not charge an
object twice. Crossing an object, byte, principal, staging or directory limit
returns `PIPESTREAM_LIMIT_EXCEEDED`, including through the service's QUIC path.
Previously declared work remains outstanding; no rejected payload becomes an
admitted job or a successful scope. Authority/principal labels come from the
authenticated service or durable job, never untrusted entity metadata.

The 96-byte `.retained-policy` uses `PSRET001`, seven big-endian limits and a
SHA-256 checksum. Each `.meta` uses `PSOBJ001` and commits to session, owner,
entity/scope or lineage identity, length and digest. The `.done` file contains
the metadata checksum. Raw entity and lineage bodies keep their original paths
and bytes. Metadata fsync precedes copying through 8 KiB buffers; verified data
is fsynced before no-replace hardlink publication, and the receipt and directory
are synced before installation succeeds. Session format 7 and the wire/CDDL
are unchanged. Existing nonempty payload roots without policy are refused,
not converted. Preserve them with their matching binary.

Reopen performs a bounded inventory. An interrupted copy resumes only from a
matching stored prefix; an incomplete receipt is not loadable until verified
publication finishes. Metadata prefixes shorter than 512 bytes, with no payload
files yet, reserve 512 global bytes and one global object until matching replay
establishes their owner and full charge. `incomplete_metadata` reports this
unattributed count. Empty canonical directories left before metadata creation
stay present and consume `directories` credit, bounded by twice the object cap.
Directory counts are global only. The root, spool directory, policy and empty
lock file are separate fixed overhead. Neither reopen nor refusal deletes
admitted objects or silently discards surviving artifacts.

The root has an exclusive Unix advisory lock using the existing pinned
[rustix file-lock API](https://docs.rs/rustix/latest/rustix/fs/fn.flock.html).
Same-process handles share accounting, and payload readers, spool loans and
in-flight operations retain root ownership. A second cooperating writer process
is refused. Use a private directory; external filesystem mutation is outside
this boundary. Symlinks, foreign files, noncanonical directories, unexpected
hardlinks, policy changes and corrupt complete metadata are refused rather than
repaired. The only permitted two-name alias is the matching payload/stage pair.
Process-exit and prefix-image tests are not proof against every torn-sector or
power-loss failure. Unsupported platforms have no unbounded fallback.

Eighteen focused tests cover quotas, lineage accounting, replay, owner checks, interrupted copies,
metadata/receipt prefixes, empty directories, alias refusal, blocked readers
and cross-process lock retention. A real-QUIC test exhausts one principal's
payload allowance, verifies unchanged declared membership, and lets a different
principal complete independent work. Future completion-space reservations,
explicit orphan reconciliation and broader tenant stress remain unfinished.

### SQLite file-length caps

Every `SqliteSessionStore` connection now uses a non-default VFS guard over
bundled SQLite's `unix` backend. Defaults are 256 MiB for the main database,
64 MiB each for WAL and rollback journal, and 512 KiB for shared memory.
`open_with_all_limits` additionally accepts `PhysicalLimits`; `physical_limits`
returns the retained policy and `physical_usage` samples the current lengths.
The fixed 72-byte `.pslimits` sidecar stores the version, four limits, and SHA-256.
It is synced before database writes. All limits are positive multiples of
64 KiB, capped at 16 GiB per file and 16 MiB for shared memory.

The guard rejects growth before writes, enlarging truncates, and WAL-index
mappings. Size hints cannot preallocate space, chunk-size rounding is disabled,
and the database mmap path is disabled. Every connection also sets a main-page
limit so WAL cannot commit a database too large to checkpoint. Temporary SQL
storage stays in memory; unnamed/unregistered disk files cannot be opened
through the guard. `SQLITE_FULL` becomes `PIPESTREAM_LIMIT_EXCEEDED`, including
over QUIC; a failed transaction does not publish its session or job state.

Held readers can prevent WAL reset. At capacity, writes refuse until enough
space is reclaimed; there is no automatic eviction of admitted work.
`checkpoint()` now returns busy when TRUNCATE could not reclaim the WAL, rather
than reporting success from an unread PRAGMA result. See
[SQLite's WAL checkpoint rules](https://sqlite.org/wal.html) and
[VFS file methods](https://sqlite.org/c3ref/io_methods.html).

Reopen reads the immutable policy. Changed/missing/corrupt policies, nonempty
unaccounted databases, oversized sidecars, symlinks, hardlink aliases, and
reserved database suffixes are refused without conversion. Preserve older
databases with their matching binary. Session payload version 7 is unchanged;
the file policy has its own version. No operational database is migrated.
At most 64 guarded database identities may be live in one process.

This guard currently requires the bundled Unix VFS and OS pages no larger than
64 KiB; unsupported backends refuse, with no unbounded fallback. Use a private
directory and cooperating writers. External unguarded SQLite connections or
filesystem writers are outside the enforcement boundary. The limits cover
file lengths, not filesystem allocation, snapshots, native memory, payloads,
or completion-space reservations. They do not lift the service's single-writer-
process restriction for other resource accounting. Java JDBC has its own independent
file-length guard, described in the Java implementation README.

Eleven core tests exercise main-page, WAL, rollback-journal and actual shared-memory
exhaustion, growth-control bypass attempts, immutable/corrupt policies, aliases,
transaction rollback, held-reader checkpoint refusal, abrupt-exit recovery,
and concurrent connection/sidecar churn.
A real-QUIC test verifies the named refusal, preserved declaration and replay
after checkpointing. These checks are not a throughput benchmark.

### Durable queue APIs

The core provides `Session::enqueue_job`, `acquire_job`, `publish_job`, and
`refuse_job` for processing, rehydration, and resume operations. Invoke these
through a store transaction. Inputs retain the validated header, measured
length and digest, negotiated layers, or the specific closed scope/claim.
Publication retains the outcome together with the execution fence and computed
protocol state. Input replay cannot replace the original descriptor, and saves
cannot remove a retained job or rewrite a terminal outcome. An application
refusal is retained separately from entity completion.

`SqliteSessionStore::open_with_job_limits` sets database-wide unfinished-job
limits. Defaults are 128 queued/running PROCESS/RESUME jobs globally and 32 per
authority/principal. REHYDRATE has separate limits of 65,536 future/active slots
globally and 16,384 per authority/principal; anonymous work shares one bucket.
`JobQueueLimits::rehydration_total` and `rehydration_per_principal` configure
these on creation. The larger completion queue does not add physical workers.
Waiting parents do not consume ordinary slots needed by their children, and
an existing rehydration reservation cannot be consumed by a new admission.
`job_queue_usage()` audits and reports ordinary jobs, future rehydration slots
and active rehydration jobs. `unfinished_job_count()` counts actual queued/running
jobs only. Limits persist across reopen, and
a handle cannot silently replace them. Queue admission and the session revision
commit together. Exhaustion returns `PIPESTREAM_LIMIT_EXCEEDED` and rolls both
back, including through `create` and `save`. Revoked work remains charged but
is not returned for execution.

`ready_jobs(now, limit)` returns a bounded page from the SQLite index, interleaving
authority/principal buckets and preferring rehydration within each bucket. A
large ready queue from one principal therefore does not fill a multi-owner page
before the others' first eligible job. This is not a global fairness guarantee.
The discovery page is bounded by the ordinary queue limit even though the
completion queue can be larger. A future reservation is never runnable.
An unexpired attempt is
not returned; lease expiry makes it discoverable again but does not grant
execution. `acquire_job` still checks authorization and the durable fence.
`integrity_check` audits queue rows against checksummed session records in one
read snapshot, including missing and extra entries. This full audit scans one
session at a time, not an in-memory list of all sessions. Store mutations also
audit these rows against retained session state before counting free capacity,
so missing or altered reservations cannot admit unrelated work. These bounded
scans and discovery ordering are not constant-time or large-store throughput claims.

Ten focused storage tests cover exact-quota scope closure and publication,
identifier/map-prefix boundaries, concurrent owner admission, revocation,
transaction rollback, missing/altered future slots, abrupt exit before or after
conversion, immutable policies, and independent-principal discovery. A real-QUIC
sealed-work test fills the ordinary queue with a held callback while another
parent rehydrates and later crosses the full root checkpoint.

The transport service now uses these APIs. Bounded admission workers perform
chunk hashing and immutable payload installation before committing admission
and its job descriptor together. Failure before that commit produces no
runnable job. A crash between file installation and commit can leave an orphan;
it is not treated as admitted or completed work.

### Workers and connection handling

Application processing, rehydration, and resume callbacks run in blocking
workers outside database transactions and may re-enter the store. Each runs
with a durably acquired `ExecutionLease`. Publication
atomically checks the session owner, revocation, operation, epoch, executor
identity, and expiry before applying its result and marking the attempt done.
Expired or superseded attempts cannot publish, including after reopening the
database. An active attempt prevents another store handle from acquiring it.

`RecursiveServer::run` starts the periodic executor automatically. Embedded
applications can call `RecursiveService::start_executor` and retain its handle.
The dispatcher audits queue integrity before execution and scans a bounded
ready-job index every 10 ms. `EntityStore::load_payload` reconstructs processing
input from retained files; length and SHA-256 are checked with an 8 KiB buffer
before invoking the processor. Rehydration and resume use their retained scope
and claim descriptors. Missing/corrupt input and callback panics produce named,
retained refusals, not successful completion or automatic application retries.

`with_execution_limits` configures physical worker limits: defaults are four
workers per canonical session database and two per authority/principal. Handles
and listeners in one process share these permits. Admission has a separate pool
with the same bounds, allowing jobs to be queued while execution is occupied.
Anonymous work shares one principal bucket. The same job cannot occupy two
physical worker slots in one process, even after lease expiry. These limits do
not coordinate physical threads in different processes; the single-writer-
process restriction for the spool directory still applies.

Connections retain at most 1,024 job observers and emit replies from committed
outcomes. Callback execution, chunk hashing, and payload installation do not
hold their dispatch loop. Raw QUIC tests pin independent job completion,
checkpoint deadline progress during a stalled callback, immediate control
refusals, and queue overflow without losing admitted jobs. Pipelined roots
received during the first admission wait within the same observation and spool
budgets. Known entities still being assembled or installed block covered
checkpoints even without a PENDING announcement. Replies for covered entities
and descendants are delivered before their checkpoint ACK, including when a
worker commits between the reply and checkpoint snapshots.

Connection metadata operations and lineage writes run in a separate storage
pool: eight physical operations per canonical database, at most four per
authority/principal. Anonymous connections share one bucket. Handles in one
process share these bounds; excess operations receive a named capacity refusal.
Started operations keep their permits after their connection waiter is cancelled,
until the actual operation returns. This does not cancel SQLite transactions or
filesystem calls, bound their latency, or coordinate separate writer processes.

The control reader starts checkpoint clocks on complete-frame reception, before
database admission. A watchdog runs independently of ordered dispatch and output
writes. Heartbeats, malformed controls, duplicate capabilities, and oversized
frames do not wait for storage. The ingress backlog holds at most 32 complete
events, with each control body capped at 1 MiB and each payload quota-charged.
A full control backlog closes with `PIPESTREAM_LIMIT_EXCEEDED`; it cannot suspend
deadline enforcement. Up to 1,024 parsed checkpoint requests are tracked, counting
duplicates. Repeated pending requests do not extend their original deadline, and
an ACK does not remove clocks belonging to copies still queued for dispatch.

Raw QUIC tests hold a SQLite writer through a checkpoint timeout, send invalid
controls during that stall, exhaust the control backlog, and hold lineage I/O
while another connection completes work. No checkpoint ACK is sent after its
deadline even if persistence later finishes. Durable state may have committed
without an observed ACK; reconnect/replay remains necessary. State-dependent
operations on one connection remain ordered and may wait behind its storage
operation. These tests do not establish concurrent-workload performance or a
filesystem-wide resource bound.

The default lease is 300 seconds; embedded services can use
`with_execution_lease` to choose 1 microsecond through 300 seconds. Lease
expiry rejects publication; it does not cancel a callback, bound its memory,
or renew automatically. The issuer uses Unix microseconds, so clock changes
can delay recovery or expire work early. Epoch checks remain necessary even
when the clock moves backward. Applications must use idempotency or enforce
their own transactional fence for external effects. A lease is not a wire
credential and does not prove exactly-once execution.

Unfinished expired attempts can be reacquired by periodic dispatch. Refused
application jobs are not automatically retried. The blocking operator API
`recover_interrupted_resumptions` uses the same bounded queue and physical
permits; it no longer scans all sessions. It only executes queued resume jobs.
This is execution recovery. Use the separate authenticated-recovery profile
to retrieve admission and terminal outcomes after a lost acknowledgment;
legacy claim redemption alone still refuses duplicates.

Dropping an executor handle stops new dispatch. `shutdown(grace)` additionally
waits up to the grace period and returns the store-wide count of callbacks
still active. It cannot kill a synchronous callback. Started callbacks retain
their physical permits until they return, and their publication remains fenced.
The listener owns a bounded set of connection tasks. Dropping its run future
aborts connection handling and incomplete receive streams in both one-shot and
long-lived modes. Already-started blocking admission or execution may finish;
their resource permits remain charged until they return.
A connection loss does not cancel admitted work or remove declared IDs. A
shutdown or expired callback cannot make an unfinished job count as complete.
Tests cover abrupt process exit after durable admission, input corruption,
detached rehydration/resume, and replacement executors while an expired callback
still occupies the sole worker slot.

`RecursiveClient::disconnect` requests transport close without waiting. Before
stopping the client's async runtime, use `disconnect_gracefully().await`; it
keeps the endpoint alive through QUIC shutdown. The `begin-yield` CLI now uses
this path. An isolated-runtime regression test verifies that the server exits
without waiting for its idle timeout and that the claim remains unredeemed and
the entity DEFERRED. Transport shutdown is not a work-completion barrier.

Without the explicit mutual-TLS settings, the standalone prototype authenticates
only the server and remains suitable solely for trusted local demonstrations.
Even with mutual TLS, retained recovery, bounded workers and retained-storage
quotas, completion reservations, orphan reclamation, and a complete resilience capability remain
unfinished. It MUST NOT yet be described as a production multi-tenant durable
work service. Its Layer 2 boolean still advertises more than the tested subset.

The core `uri` module parses typed `pipestream://` session, entity, and claim
locators with explicit ports. Parsing grants no access and does not perform
network I/O. The scheme is proposed, not registered by this repository.
