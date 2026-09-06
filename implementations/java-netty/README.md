# Java/Netty implementation

This independent PipeStream Layer 0 implementation uses Netty QUIC and Jackson
CBOR. It publishes a reusable Java library and a shaded standalone client/server
JAR.

```bash
mvn verify
java -jar target/pipestream-quic-netty-0.1.0-SNAPSHOT-all.jar --help
```

The current build uses Netty's `linux-x86_64` native classifier. Building also
requires CMake 3.24 or newer and a C11 compiler for the small SQLite file-limit
extension. Maven builds and packages it in both JARs and runs its native tests
during `test`. See [native storage guard](native/README.md) for the pinned source,
supported backend, and sanitizer command. No Rust protocol or storage code is
linked into the Java library.
The client
requires a CA certificate and the server requires an end-entity certificate and
private key. QUIC 0-RTT is never enabled.

The public `PipeStreamClient` and `PipeStreamServer` classes implement the same
transfer used by the CLI: deterministic CBOR capability negotiation, one
SHA-256-protected entity with optional `parent-id`, checkpoint acknowledgement,
cursor advancement, and GOAWAY. They currently implement Layer 0 only.

## Sealed-work library foundation

The independent Java `SealedWork`, `SealedScope`, and `SealedSessionStore`
APIs implement declaration encoding, durable membership, admission, and
recursive closure for Section 9.8. They do not import Rust protocol or state
code. `SealedCbor` handles the sealed Layer 1 schema's deterministic CBOR
types, including the full unsigned 64-bit range; it is not a generic Layer 2
codec. `SealedWorkTest` consumes all 20 frozen declaration inputs, and
`SealedScopeTest` consumes the frozen scope-digest inputs and pins the Merkle
construction, including odd-node promotion.

`SealedSessionStore.open(path)` creates a separate SQLite database with strict
typed tables, WAL, and synchronous FULL commits. The Java database format is
now version 5, with fixed-capacity job, entity and closure images, preallocated future jobs,
protected rehydration reservations, and checksummed checkpoint request/ACK history. Version-1 through version-4
stores are refused without conversion; keep them with their
matching binary. Do not point this API at a Rust
store. Unknown table sets or policy versions are refused without conversion.
The JDBC driver requires native access; run an embedding application with
`--enable-native-access=ALL-UNNAMED`. Tests enable this explicitly.

- `declare` commits before returning the exact ACK. Replays check retained
  history against the scope's complete membership, identity, sequence, and seal.
- `admit` requires the caller to validate and durably retain the payload first.
  Missing payloads remain declared; no API deletes or recycles identifiers.
- `processed` records COMPLETE, FAILED, or DEHYDRATING after application work.
- `closeScope` returns a durable status summary only after the child scope is
  sealed, every declared entity resolves, and all descendant scopes close.
- `resolveChildren` enforces STRICT: successful children allow REHYDRATING;
  a closed set containing failure instead makes its parent FAILED.
  `rehydrated` records the subsequent application outcome.
- `checkpointReady` checks the durable, whole-scope inclusive cut. It is not
  a checkpoint ACK; the connection must additionally enforce deadlines, exact
  request correlation, outstanding ingress, and nested checkpoints.

All storage calls are blocking and must run outside Netty event loops.
Application callbacks are not executed under storage transactions. These
manual lifecycle APIs refuse managed work so they cannot bypass its executor
fence. Producer labels are not credentials; the Java APIs do not provide
authenticated recovery or exactly-once external effects. The separate payload
library below supplies retained input, not state-machine admission or execution.

Persistent local policy allows 512 sessions, 65,536 declared entities globally,
and 16,384 per session, in addition to negotiated per-scope and depth limits.
Declaration history and scope trees are checked with bounded scans, not a
constant-time readiness index. These logical record limits are separate from
the file-length policy below; neither is a throughput claim. Reopen cannot
reset either policy.

The original `PipeStreamServer` and standalone commands remain Layer 0. The
separate `SealedServer` and public client require `sealed-work-sets-v1`; neither
silently converts an unsealed session or falls back to weaker capabilities.

## SQLite file-length bounds

Every session-store connection uses a non-default bounded VFS over Xerial's
bundled Unix SQLite backend. The default `FileLimits` are 256 MiB for the main
database, 64 MiB each for WAL and rollback journal, and 512 KiB for shared memory.
`open(path, limits)` accepts explicit immutable caps; `open(path)` uses the
retained policy on reopen or defaults for a new store. Caps are positive multiples
of 64 KiB, at most 16 GiB for main/WAL/journal and 16 MiB for shared memory.
`fileUsage()` validates the policy and samples actual lengths; it is not an
atomic snapshot or a count of allocated filesystem blocks.

The guard checks writes, truncates and shared-memory mappings before growth.
Database mmap and size-hint/chunk preallocation are disabled. Every store
connection also sets a main-page cap so a WAL transaction cannot commit a
database that would exceed its main-file bound when checkpointed. `SQLITE_FULL`
from store transactions becomes `PIPESTREAM_LIMIT_EXCEEDED`, including through
the public QUIC listener. It does not erase declarations, admit missing input,
or report completion. A reader retaining an old WAL snapshot can exhaust the
WAL cap before the logical record cap. Releasing that reader and successfully
checkpointing can permit new writes; a busy checkpoint is not success.

A synced `.psjlimits` sidecar retains the 72-byte `PSJDB002` version, four
big-endian limits, and a SHA-256 checksum. The empty `.psjlock` file coordinates
policy creation. Nonempty databases or sidecars without policy are refused
before SQLite opens them. Policy changes, corrupt or oversized files, symlinks,
and hardlink aliases are refused. File policy `PSJDB002` is separate from the
version-5 Java schema. Previous file-policy versions are refused: keep each with its
matching binary. No automatic conversion or operational migration is supplied.

This backend requires 64-bit Linux, the pinned JDBC SQLite version, private
local directories, and cooperating writers using this library. Unrelated raw
JDBC connections and filesystem writers are not controlled by the guard. Use
one loaded copy of the library per process. The native registry permits 64
concurrently open database identities, with capacity released on connection
close; sequentially used databases do not accumulate registrations. Bootstrap
registration is private, extension loading is disabled afterward, and missing
or incompatible native support is a hard failure without an unbounded fallback.

Direct native tests exercise all four file families and growth bypass controls.
JDBC tests exhaust actual database/WAL/journal files, hold WAL readers, check
rollback and integrity, corrupt retained policy, exhaust registry capacity,
and abruptly exit with an uncheckpointed WAL. A real-QUIC test checks the named
capacity refusal, unchanged acknowledged membership, zero accidental payload/job
admission, and declaration replay after checkpointing and reopen. The transaction
layer additionally reserves WAL and shared-memory capacity for admitted jobs as
described below. File caps do not provide authenticated principal quotas.

## Sealed-work payload storage

`SealedPayloadStore.open(directory, limits)` owns a dedicated local directory,
separate from the SQLite state store. `begin(identity, header)` returns an
incremental receiver: `write` charges temporary capacity before each at-most-8-KiB
file write, and `finish` checks measured length and SHA-256 before transferring
spool ownership to a `Received` receipt. Close unfinished receivers or receipts
to release their spools. Installation pins receipts against concurrent close;
failed cleanup retains conservative capacity charges.

`install(receipts)` validates the complete entity or chunk set, including
identity, headers, indexes, and contiguous nonoverlapping offsets. It streams
the inputs into a checksummed `PSJPAY01` object, syncs the file, publishes it
with a no-replace hard link, and syncs the directory. The installed filename
hashes the scoped identity; request text never becomes a path component.
Identical replay reuses and verifies the existing object without requiring
new retained-file headroom. Changed bytes or commitments are refused.
`find(identity)` and `Stored.openStream()` verify retained metadata and the
complete payload before exposing input to an application. The latter then
reads from that same opened file; this adds a verification pass, not a
whole-payload allocation.

Persistent defaults allow 256 MiB/4,096 temporary files, 512 MiB/8,192 retained
files, 64 MiB per assembled entity, and 1,024 chunks per entity. Publication
reserves the complete object length and file credit for both staging and final
names. These are conservative logical file-length charges, including metadata,
not filesystem-block, SQLite/WAL, page-cache, or whole-process memory bounds.
There is also a fixed aggregate limit of 128 active receivers, receipts,
readers, and operations. That limit can refuse a chunk set below the configured
chunk count; exhaustion is `PIPESTREAM_LIMIT_EXCEEDED`, never completion.
It is not a per-principal quota.

Reopen requires the exact recorded policy and counts abandoned files without
deleting them or inferring admission. Unknown layouts, symlinks, and incompatible
policies are refused without conversion. One cooperative writer owns the
directory lock; duplicate same-process opens are refused before opening a
second lock channel. The latter matters because closing another channel can
release a JVM's locks on that file ([JDK FileLock documentation](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/nio/channels/FileLock.html)).
Use one loaded copy of this library for a store in a process. Locks are advisory;
unrelated filesystem writers, separate class loaders sharing a store, and network
filesystem semantics are outside this implementation's supported boundary.
The filesystem must support hard links and directory synchronization.

These calls are blocking and must run outside Netty event loops. Installing a
payload does not admit or complete an entity: the session store must still
commit admission and execution state. An abrupt-exit test checks that a file
installed before admission remains unadmitted after restart. Other tests cover
chunk geometry, integrity, concurrent installation/close, immutable replay at
capacity, and cross-process writer exclusion. A 32 MiB receive/install/read test
runs in a JVM with a 24 MiB maximum heap; it is not a QUIC, native-memory, RSS,
or concurrent-throughput measurement. The sealed listener below uses these
APIs; explicit orphan reconciliation remains unfinished.

## Durable sealed-work execution

`SealedExecutor.start(sessions, payloads, processor, limits)` starts a periodic
dispatcher and bounded application workers, independently of client connections.
One executor owns a canonical Java database within this process. Reuse it across
listeners rather than starting one per connection. `admit(stored)` commits
payload admission and its processing descriptor together. `closeScope` commits
child closure, parent resolution, and conversion of its reserved rehydration
capacity together. Storage failure rolls all those changes back. STRICT child failure propagates
FAILED without a rehydration callback.

The internal typed queue retains immutable, checksummed input descriptors and
outcomes. Its fixed persistent policy permits 128 queued/running PROCESS jobs
globally and 32 per session. REHYDRATE jobs use separately reserved completion
slots: at most 65,536 reserved or queued/running slots globally and 16,384 per
session, one per admitted entity. This is a larger, separately bounded durable
queue, not additional workers. Waiting parents do not occupy processing slots
needed by their children, and a full processing queue cannot refuse conversion
of an already-held rehydration slot.

Admission charges the processing descriptor and its 256-byte state image,
plus an allocated future rehydration descriptor and another 256-byte state image.
A rehydration descriptor adds a fixed 85-byte CBOR
member to the existing processing descriptor. The combined retained and reserved
charges must fit 64 MiB globally and 16 MiB per session. Each PROCESS admission
allocates both rows atomically. The future row has an explicit RESERVED state;
its input holds the original descriptor plus 85 verified zero bytes, not a
fabricated child result. It has no executor or outcome and is neither returned
as a job nor eligible for discovery or acquisition. Child closure overwrites
the reserved input, hash and state in place, without inserting another row.
Ordinary terminal or refused processing marks an unused future RETIRED.
This releases its execution slot, but the allocated record remains charged;
it is not deleted or treated as free storage. DEHYDRATING parents hold their future bytes and slot until child
closure converts them to a rehydration job or STRICT failure makes it unnecessary.
Reopen and disconnect cannot release a reservation. The checksummed job/entity
state determines these charges; no independently mutable counter can create free
capacity. `SealedSessionStore.jobUsage()` audits and reports them in one snapshot.
Earlier schema policies are refused without conversion.

The version-5 job image contains a format marker, state, executor epoch and
identity, expiry, outcome length, at most 128 outcome bytes, verified zero
padding, and the existing identity-bound state checksum. Acquisition and
publication use SQLite's fixed-size BLOB API through the guarded connection;
they do not replace the job row or update a mutable job index. Ready-job queries
use read-only projections of the big-endian state and expiry, with a bounded
full audit before selection. These projections are not generated columns or a
second stored copy. SQLite rejects writable BLOB handles on generated-column
tables and indexed images; the Java tests retain both refusals.

Declaration allocates a 112-byte entity image and a 128-byte closure image per
scope. Their checksums bind the session, producer and scoped identity. Entity
state explicitly distinguishes missing admission, managed work, input digest
and optional result digest. Scope closure has a separate presence flag and
verified unused bytes; allocated capacity is not a completion marker. Admission,
processing, child resolution and rehydration overwrite these images in place.
Tests exercise recursive state transitions under a fixed main-page cap at 512-,
4,096- and 65,536-byte page sizes, plus real write failures and corruption.

The storage guard also provides a per-connection WAL ceiling that remains in
force through commit or rollback and is inherited by that connection's WAL
handle. Every public store write transaction audits the remaining execution stages
under its actual writer lock and installs a ceiling before any mutation.
Admission adds its acquisition/publication and possible rehydration credit;
validated stages consume their own allowances. A final audit must match the
predicted credit before commit. Lease renewal uses ordinary headroom and cannot
spend publication credit. Rolled-back tails do not erase job reservations or
prevent retries merely because the physical file retains uncommitted bytes.
The bound includes spill/commit repetition, sector padding and WAL-index
capacity under the pinned SQLite 3.53.4 geometry.

The original 512 KiB pinned-reader regression now passes. Whole-transaction
cost tests cover 54 scenarios and 378 stages; additional tests cover recursive
completion, STRICT failure, WAL-index-first exhaustion, reopen and retry after
actual conversion spills. See the
[derivation and evidence](../../docs/standards/java-completion-reservations.md).
These reservations do not promise to admit unknown future children, payloads or checkpoint requests.
Those remain subject to their own admission limits. Session and producer labels are not authenticated principals;
these limits do not provide tenant isolation. Reads bound blob materialization,
and integrity checks reject missing jobs, changed descriptors, and outcomes
that disagree with entity state before executing or acknowledging completion.
Job lookup also validates its processing/future pair; a missing reserved row is
corruption, not an optional job that silently disappeared.
The dispatcher audits retained records, not just the ready-job query. Discovery
interleaves sessions within each bounded page and prefers their rehydration work,
so one large completion queue cannot fill the page with only its own jobs. That
bounded full scan and SQL ordering are not a large-session throughput or global
fairness claim.

Read-only discovery, status and readiness calls use enforced `query_only`
snapshots instead of taking SQLite's writer lock. The listener reads its bounded
observer batch in one snapshot rather than reopening and auditing the store for
each observed job. A held-writer test pins read progress and write refusal from
those snapshots; this does not bound arbitrary storage latency.

Defaults are four workers, at most two per session, with five-minute leases.
Acquisition and publication check a durable increasing epoch, executor identity,
and issuer wall-clock expiry. Expired or superseded attempts cannot publish.
An expired callback retains its physical permit and excludes a duplicate local
callback for the same job until it actually returns. Epoch-millisecond clock
changes may delay reacquisition or expire an attempt early; fencing remains
necessary. Applications must deduplicate or transactionally fence external
effects themselves.

Before a callback, retained input is reopened and checked against its original
header, measured length, and digest. `Processor.execute(context, input)` receives
a file-backed reader and returns COMPLETE with an output digest, FAILED, or
DEHYDRATING. Rehydration cannot dehydrate again. It executes outside database
transactions and may re-enter storage. Callback exceptions or corrupt input
produce retained refusals, not entity completion or automatic retries. A fatal
dispatch/storage failure is exposed by `failure()` and stops new dispatch.
An interrupted unfinished attempt may run again after expiry and restart.

`close()` stops new dispatch without interrupting application callbacks.
`usage()` continues to count them, and database ownership remains reserved
until callbacks and started storage calls return. At most eight admission or
scope-closure storage calls may be outstanding; excess calls receive a named
capacity refusal, and closing does not release those slots early.
Keep the payload store alive
until `isTerminated()` is true. Closing a connection does not cancel or complete
its durable jobs. The executor does not bound application-created threads,
callback memory, or external effects.

Tests exercise queue rollback, recursive closure, exact conversion at processing
and metadata capacity, waiting parents admitting children, independent session progress
during a stalled callback, callback storage re-entry, stale publication after
abrupt exit and reopen, cancellation-safe ownership, corrupt input, retained
refusals, and metadata capacity retained by completed jobs. A 32 MiB retained
input executes through the worker under a 24 MiB Java heap cap. This is not a
QUIC, RSS, native-memory, or multi-tenant stress measurement. The listener below
adds separate network and connection-control evidence.

## Sealed-work listener

`SealedServer.start(bind, certificate, privateKey, sessions, payloads, processor,
limits, executionLimits)` starts a sealed-only Netty listener and its durable
executor. It is an embeddable Java API, independent of the existing Layer 0 CLI.
TLS authenticates the server, not the producer label: this listener is for
application-authorized peers, not untrusted multi-tenant exposure or the
authenticated-recovery profile.

Declarations are durably acknowledged before payload reception. Entity headers
are bounded, bodies are read in at-most-8-KiB pieces, and FIN validation and
immutable chunk installation precede atomic admission/dispatch. Streams do not
need PENDING announcements. A reset or disconnect does not remove declared
work, admit partial input, or cancel an already-admitted job.

Fixed listener limits are 32 connections, eight entity readers per connection,
four file/receive workers with 32 queued tasks, and four metadata workers with
64 queued tasks. A reader's Java byte backlog is at most eight 8-KiB pieces;
QUIC windows are 1 MiB per connection and 64 KiB per Entity Stream. Readers may
occupy a file worker while waiting for network input. Per-connection limits
include 32 partial assemblies, 128 result observers, 32 queued storage actions
with 4 MiB of encoded controls, and 1 MiB of pending control output. These are
component bounds, not whole-process/native-memory, filesystem-block, or tenant
quotas. Capacity exhaustion is a named refusal.

Complete checkpoint receipt starts a monotonic deadline on the network event
loop before SQLite queueing. Pending requests do not block eligible payloads
or control parsing; duplicates retain the original deadline. Covered ingress,
unsent outcomes, and nested checkpoints prevent ACK. An operation completing
after timeout cannot emit a late ACK on that connection. The current store
retains exact request identity, optional root-scope presence, unsigned counters,
and ACK state under checksums and history accounting. Missing records cannot
erase outstanding obligations. Limits are 4,096 retained checkpoints globally,
1,024 per session, and 4 KiB per encoded request; ACKs remain charged.

SCOPE_DIGEST comparison, closure, STRICT parent resolution, and any rehydration
dispatch occur in one transaction. A forged digest rolls closure back. Failure
propagates without running a rehydration callback. GOAWAY requires the matching
acknowledged root cut and no outstanding connection work.

`close()` stops ingress without interrupting callbacks or claiming cancellation
of started storage calls. Keep both stores alive until `isTerminated()` reports
physical shutdown. A separate bounded cleanup worker closes abandoned receipts;
connection credit is retained until cleanup returns.

`SealedServerTest` uses actual QUIC to cover nested and out-of-order chunked work,
independent completion during a stalled callback, held-SQLite deadlines and
protocol refusals, duplicate clocks, storage backlog overflow, STRICT failure,
forged digest rollback, reset streams, and unobserved ACK replay after restart.
The reset test uses the error-code `shutdownOutput` overload and waits for the
receiver's refusal before asserting zero admission. A separate test pins normal
FIN completion: Netty's `stream.close()` sends FIN and is not a reset injector.
The `sealed-interop` profile also runs the Rust public producer against this
Java server. A separate 32 MiB QUIC transfer/install/execute test runs with
a 24 MiB Java heap limit; it does not measure native memory or RSS.
Persistent producer-side observations, broader crash-boundary and
resource stress coverage and orphan reconciliation remain unfinished;
this is not a full conformance claim.

## Sealed-work network producer

`SealedClient.connect(remote, caCertificate, serverName, limits, timeout)`
opens real QUIC with server-certificate validation and requires extension
65281, Layer 1, and no Layer 2. It does not retry with weaker capabilities,
accept server-originated work, or enable 0-RTT. This is a separate reusable
API, not a change to the Layer 0 `PipeStreamClient` or CLI.

1. Call `declare` for the root batch, then any subsequent batches. It checks
   every ACK field before remembering membership. Keep the original requests
   for explicit replay after reconnecting.
2. `send(header, path)` streams a regular file through 8 KiB buffers and waits
   for its processing outcome. `sendChunks` streams a complete file-backed
   chunk set in caller order, checking identity and contiguous nonoverlapping
   geometry. It reports one entity lifecycle, not one per chunk.
3. Declare child scopes for dehydrated parents, send descendants, then call
   `closeScope` from the leaves upward. The client verifies the status Merkle
   digest and the parent's returned lifecycle. `barrier` correlates both scope
   and parent identity.
4. Seal membership and request a whole-scope `checkpoint`. Its ACK must preserve
   scope/timeout presence and the full uint64 sequence; known unfinished work
   cannot be accepted as completion. `goaway` requires the acknowledged sealed
   root cut and closed descendants.

Operations are blocking and serialized per client. Run them outside Netty event
loops. The monotonic operation budget bounds network waits, not blocking local
file I/O. The receive backlog is limited to 128 frames and 4 MiB of encoded
frames, with a 1 MiB individual control limit. Local bookkeeping allows 65,536
entities, 65,536 chunks per send, and 1,024 checkpoint identities. These limits
and fixed-size file reads are not a measured whole-process memory bound.

This client does not persist its producer ledger or observed statuses, export
resume tokens, retry payloads, or provide authenticated-session/recovery APIs.
A new client can replay declaration history and send work not previously
admitted. Replaying declarations alone does not reconstruct prior admission or
completion observations for checkpointing already-processed work. A pending
checkpoint blocks this client's next operation; concurrent submission needs
additional client API work. Closing never claims successful completion.

The `sealed-interop` Maven profile runs five actual-QUIC tests, including
Java-to-Rust nested/chunked completion, declaration replay and checkpoint ACK
replay after Rust restarts, a deliberately discarded declaration ACK, and named
refusals for changed ownership labels, lower retained limits, missing seals,
wrong checkpoint bounds, changed ACKs, downgrade, oversized frames, and Layer 2
responses. Scripted test peers inject faults; they are not reference servers.
The separate `SealedServerTest` now supplies Java-server and reverse-direction
evidence; the scripted peers are still only fault injectors.

```bash
cargo build --release --locked --manifest-path ../rust-quinn/Cargo.toml
mvn test -Psealed-interop
```

Default `mvn test` runs the independent Java codec/store tests without requiring
a Rust executable. The repository's `conformance/run_all.sh` explicitly enables
the interoperability profile after building Rust; a missing executable is a
failure, not a skipped integration test.
