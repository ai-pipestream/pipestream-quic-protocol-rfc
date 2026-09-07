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

## Version-2 Core server

`pipestream_quic::v2_core::Server::bind(address, security, options)` creates
an actual QUIC-v1, `pipestream/2` Core listener. `server.run(shutdown_future)`
serves bounded concurrent connections until that future completes. The
embedding application supplies `v2_tls::ServerSecurity` and can obtain the
bound address with `local_addr()`. This is the Core library integration point.
The separate [V2 command group](docs/v2-cli.md) uses the durable listener below;
original version-1 commands remain available.

Core advertises no durable profiles; supplying such an enabled inventory is
refused at configuration time. It implements capability minima/required-profile
refusals, Stream 0 framing, canonical decoding, increasing request IDs,
correlated refusals and connection-only detach. Unknown ignorable frames use
a 4096-byte discard buffer, including frames larger than the 64 KiB receive
window. Known lengths are checked before body allocation. No input/result
streams or second control stream receive QUIC stream credit in Core-only mode.
Each fully received control request rechecks credential validity. Stopped/reset
control directions terminate the connection, and headers/frames have bounded
receive and send deadlines. Empty ignorable-frame floods yield the scheduler
at least every 32 frames.

`Options` separates negotiated limits from deployment ceilings. Defaults are
64 global connections, eight active connections per stable authority/owner,
eight anonymous/unmapped connections, a five-second handshake timeout and a
ten-second whole-control-frame timeout. The global admission check includes
Quinn's retained closed/draining connections and bounds the owned task set.
Quinn's pending-incoming queue, handshake buffers, receive windows and send
window are bounded separately. The configurable raw-control-buffer product
cannot exceed 64 MiB. That is not a measurement or bound for the whole process
heap/RSS; TLS, decoded values and QUIC state also consume memory.

The policy is immutable for a Core server instance; replace the listener to
change it. Rotated certificates share their mapped owner's quota. Credential
expiry cannot be reversed on a live peer. `DRAIN` detach acknowledges only the
connection cut; subsequent valid control requests receive correlated NOT_READY.
No detach, transport close or operator shutdown asserts durable completion.
Fifteen Core tests cover configuration and real QUIC paths, including non-reading peers,
oversized/truncated frames, resets, quota boundaries and shutdown. Twenty TLS
tests cover the underlying security boundary. The separate durable listener
below integrates storage/execution; client staging recovery, Java V2 and whole-process
resource measurements remain unfinished.

## Version-2 durable server

`pipestream_quic::v2_authority::server::Server::bind(address, security, authority,
applications, result_endpoint, options)` supplies the authenticated durable-work
and result-delivery listener. Binding audits paired storage and starts the real
executor/maintenance runtime; it is a blocking setup operation, not a connection
callback. The caller supplies the application registry and trusted result endpoint.
The existing Core-only listener and standalone V1 commands remain separate.

The durable listener negotiates both profiles, or only durable work when results
are not selected. Optional profiles are excluded for anonymous/unmapped peers;
requiring durable work without an identity closes before a capability response.
Identity/store matching also precedes the response. Core fallback supports detach
and correlated profile refusals without creating a session.

Each connection has one persistent bounded control reader and writer, bounded
operation/input tasks and a shared control/result flow owner. Other streams cannot
cancel a partially consumed control frame. After negotiation, the frame deadline
starts at its first byte; a long WORK wait is not an incomplete frame. Stream 0
STOP_SENDING is monitored even with an empty response queue. Responses retain their
pending/input/output pins through writes. A half-closed client can still receive
queued replies: the server finishes control and waits for its FIN acknowledgment
before graceful close, within the drain/deadline bound. Neither enqueue nor a
transport acknowledgment asserts application validation or persisted receipt.

Defaults: 16 connections, four per mapped principal or anonymous group, a two-item
response queue, five-second handshake and shutdown grace, ten-second frame bound,
four incoming data streams, and the runtime/input/output defaults below. A
60-second local QUIC idle bound and five-second PING interval accommodate the
30-second WORK wait; transport PING is not input/result progress. Encoded-state
accounting includes request/response pairs and queued refusals. Configurations
exceeding either 128 MiB raw-control or 128 MiB transport-credit budget are refused.
These ceilings are not measured heap, native memory, RSS or allocator bounds.

`run(shutdown_future)` supervises runtime faults and returns a `Shutdown` report.
It stops admissions, cancels owned connection tasks and waits up to the grace
for their actual destruction, metadata permits, file leases and runtime threads.
`drained()` means those local owners and transport are idle, not that all durable
work succeeded or became terminal. A timed-out join reports what remains live;
accepted callbacks/commits keep their roots and pins. Dropping the server requests
stop without blocking on arbitrary application code or claiming completion.

Fifteen black-box listener tests cover actual empty/64 KiB copy results, repeated
reads, exact root completion, stalled output/control progress, partial frames,
full-duration waits, detach/refusal ordering, invalid input/control, credential
rotation and owner quotas, a non-reading control peer, exclusive storage reopen
and shutdown with a commit still in flight.
Two unit tests cover configuration bounds and child-future destruction accounting.
The neutral V2 process-kill driver, independent Java V2, client staging recovery
and equivalent external streaming-gRPC workload/resource comparison
remain required. This is Rust endpoint evidence, not completion of those gates.

## Version-2 runnable commands

The Unix `pipestream-quinn v2` commands explicitly initialize or reopen authority
and client history, require configured mutual TLS, and expose durable mutations,
observations, file retrieval and exact root completion. The reference server
registers explicit consume/copy, caller-reassembly and authority-chunking applications.
See the [CLI guide](docs/v2-cli.md) for commands, immutable configuration, application
limits, original-operation replay and remaining staging-recovery requirements.
Six real subprocess tests cover restart, rotated/changed principals, missing
input, both branch modes, retry, skip, cancellation, revocation and verified bytes;
the chunk cases include empty, exact/partial 64 KiB and 33-child inputs. Three
startup unit tests cover configuration bounds and permission handling.

## Version-2 TLS boundary

`pipestream_quic::v2_tls` supplies separate TLS 1.3 configuration for
`pipestream/2`, client DNS/IP identity verification, explicit verified-leaf
mapping, and a completed server-side `Peer`. It does not implement a complete
Core/durable dispatcher or advertise the new profiles on its own. The separate
Core server wires the Core path, and the separate durable server above connects
the authority, object streams and execution/maintenance runtime.

`ServerSecurity::accept` awaits a full handshake within a positive, at-most-30-
second timeout after the host reserves a connection slot. It explicitly selects
its owned configuration with `Incoming::accept_with`; a stale listener default
cannot change its authentication policy. `set_transport_config` changes
transport limits on future accepts and listener configurations without exposing
the owned TLS verifier. Configured client
authentication requests a certificate but allows its absence for Core. A
presented expired, future, wrong-usage or untrusted certificate fails TLS even
if its fingerprint is mapped. A valid unmapped or absent certificate supplies
no durable identity. An empty mapping withdraws all durable identities while
retaining TLS verification. Authority/owner labels use the core wire validator.

Call `Peer::authorize` with current trust/mapping before every new request.
It checks credential validity and stable identity under a serialized guard;
current owner authorization/revocation must also be checked by the authority.
Changed owner/authority, removed mapping, changed trust or invalid credentials
cannot rebind a live connection or downgrade it to anonymous access. Once
invalidated it requires a fresh handshake, even if the policy or clock later
returns to its old value. A certificate's validity endpoints use TLS/PKIX rules,
not the protocol's exclusive stream deadlines. Accepted job authorization is
separate from the presenting certificate's lifetime.

The supplied TLS time provider must provide trusted UTC. Within a peer, unknown
or regressed time refuses a new check without extending validity. Known missing
time before a handshake refuses the incoming QUIC connection and reports local
CLOCK_UNSAFE. A private adapter at the pinned Quinn/rustls `read_handshake`
boundary maps a TLS failure without an alert to fatal `handshake_failure`
(QUIC 0x128). Existing TLS alerts and QUIC transport-parameter errors retain
their codes; no error text is parsed, and no stale time is substituted.
The adapter is applied independently to client and server TLS configuration.
This is not a persisted cross-connection clock proof or
an online CRL/OCSP service; current trust/mapping is operator-supplied.

Server tickets, server session storage, early data and client resumption are
disabled. Client and server policies are tested independently against peers that
offer/request resumable sessions. Certificate inventories are capped at 16
entries/65535 DER bytes, mappings at 4096, and post-handshake peer capture also
checks its input bound. These limits are not a measured whole-process TLS memory
bound or a replacement for global/per-owner connection quotas.

Twenty security tests, including real QUIC handshakes, and removed-guard
negative controls cover these APIs. No durable request,
result transfer, client journal or Java V2
endpoint is implied by those tests. See the
[acceptance ledger](../../docs/standards/durable-work-v2-test-plan.md).

## Version-2 library foundation

`pipestream_quic::v2_authority` is the authenticated control adapter for the
existing transactional authority. `v2_authority::server` connects it to the
durable listener; `v2_core::Server` intentionally remains Core-only.

Construct one `Authority` during setup with the paired authority/payload roots
and a ceiling of 1..64 concurrent metadata jobs, then clone it across listeners
and connections sharing that ceiling. Construct a `Connection` only from its
actual TLS `Peer`, owned `ServerSecurity` and validated selected capabilities.
Pass its single `v2_flow::Connection` owner as well; another TLS connection's
owner is refused before binding. Result writers cannot bypass this owner.
The TLS issuing identity must match the store. `submit` runs synchronously in
control decode order and returns either a correlated immediate refusal or a
`Pending` request that can run concurrently. Malformed/direction/correlation
errors remain fatal; valid refusals still consume request IDs.

The adapter implements all durable control operations through the store:
creation/attachment/sequence, declarations/pages/checkpoints, operation lookup,
work snapshots/waits/retry/cancel/skip, manifest/read requests and both drain
forms. At most one binding may be in flight or installed. Snapshot/checkpoint
waits poll bounded snapshots without holding SQLite transactions, metadata
permits or threads while sleeping. Subsequent polls join the fair metadata
queue within the remaining wait budget; contention does not re-admit the watch.
At wait expiry the last consistent work snapshot remains the timeout response,
while an unresolved checkpoint returns WAIT_TIMEOUT, never a made-up summary.
Exact completed-session drain checks the
committed root and keeps its connection cut stable until the response is sent.
Detach waits for existing requests/transfers and has a fatal lifetime deadline.

Keep each returned `Response` until its control write or result transfer ends.
Read a result through its borrowed `ResultRead`; after scheduling successful
transport FIN, consume `Response::finish_result`. Dropping a result response
aborts delivery only. Clone an `InputSlot` into every outstanding input I/O or
commit task and retain the last copy through the admission response write.
These slots account for unresolved requests, including completed responses not
yet written and result reads waiting for a stream. Cancelled async database
waiters do not release the slots of still-running blocking jobs.

The durable server supplies bounded control readers/writers, input/result
integration, shared control capacity, runtime supervision and connection-level
shutdown. The execution/maintenance runtime below supplies independent workers.
The blocking journal, asynchronous owner and durable client are described below.
Twelve adapter tests use actual TLS peers
and on-disk stores but call the dispatcher locally. They are not V2 wire,
cross-language or whole-process resource evidence. See the acceptance ledger.

### Version-2 authority runtime

`Authority::start_runtime` starts the real durable executor plus three separate
native threads for read leases, retention and retirement. Each retains its own
bounded cursor; no client must resubmit admitted keys after startup. The existing
execution pool has its own independent cancellation/deadline/closure reconciler.
Read expiry therefore does not wait behind a blocked application callback or a
retention pass. Unsafe clocks, capacity refusal and SQLite lock contention retry
at the configured interval. Other maintenance failures stop maintenance and appear
in its bounded health snapshot; the enclosing listener must supervise that fault
and stop new admissions. No storage error text is parsed or sent as a wire label.

Defaults are four callback workers, two per owner, 32-item cursor steps, 20 ms idle
and maintenance intervals, and a 30-second worker lease. Maintenance batches are
1..256 and intervals 1 ms..60 seconds. Existing accounting and retirement
eligibility audits still scan retained records; the batch is not a latency or
whole-inventory scan bound. Setup and explicit `shutdown` are blocking.
`request_stop`, `snapshot` and `is_finished` do not wait for callback/storage I/O;
a busy execution snapshot is None, never an idle assertion. Thread completion
covers this runtime only, not separate connection metadata or input/output jobs.
Dropping requests stop without joining, and live threads keep roots and pins.
Already dispatched accepted work can finish after stop or a later setup failure.

The executor preserves each retained session's exact result-profile selection
even when the deployment supports more profiles. A startup barrier prevents a
partially created execution pool from dispatching callbacks. Three runtime
integration tests cover pre-existing admission, real copy execution, independent
read expiry during blocked execution, safe-clock/pinned-read retirement and
duplicate/configuration refusal; a fourth checks failure classification. Three
core regressions cover profile selection, stop behind a discovery lock and failed
thread creation. These tests do not activate a public durable listener or prove
Java V2, cross-language recovery, whole-process bounds or workload usefulness.

### Version-2 control reservation

`v2_flow::Limits::configure` sets transport limits before the handshake, with
the actual client/server role. Only the server grants incoming bidirectional
stream credit; the client opens Control Stream 0. Both sides grant the bounded
data-stream count and uniform per-stream receive window W. Create exactly one
`v2_flow::Connection` for each actual QUIC connection, then share it between
control and result writers. Do not write through raw handles or change the
underlying windows while that owner is active.

Short nonblocking write polls serialize under the owner's mutex. Data uses
send-window B; a control poll temporarily uses B+C, then restores B before
unlocking, including on error. No await, file I/O or callback runs under this
lock. Thus data cannot borrow control's extra local send space. Priorities are
also set, but packet ordering alone is not the reservation mechanism.
Pending control polls also register a 20 ms retry wake: Quinn's ordinary
writable-event condition observes the restored B window, not B+C. One timer
belongs to the control writer; no retry task or unbounded queue is spawned.

The receive budget is `ceil(8*(N+1)*W/7)`, not merely `(N+1)*W`. Pinned
quinn-proto 0.11.17 batches MAX_DATA at R/8 consumed bytes, independently of
stream-credit updates. The larger budget preserves a full control window even
while those connection credits are withheld. Recheck this policy when upgrading
Quinn. Tests reproduce a deadlock without this headroom, then verify the fixed
geometry, stream retirement/replacement and a deliberately unsafe peer. Local
reservation cannot supply credit withheld by a peer or guarantee network delivery.

Defaults are B=C=W=65536 and N=4, giving 374492 connection receive bytes. B and
C each range from 1 byte to 8 MiB; W from 1024 bytes to 1 MiB; N from 0 to 128.
These are transport-credit/admission limits, not measured process-memory limits.
Nine flow tests and two additional result-adapter tests cover this layer,
including an actual stored result stalled while a control response crosses the
same connection. Control dispatch in those earlier adapter tests remains local;
the durable listener has additional black-box tests described above.

### Version-2 input transport adapter

`v2_authority::input::Inputs` receives actual authenticated QUIC input streams
through the authority's existing receive/prepare/admit path. Construct it once
with the same `Authority` instance and registered applications, and clone it
across connections. It refuses a mismatched authority instance before accepting
a stream. `accept` derives the request tag from the actual peer stream ID and
reserves connection, global and owner transfer capacity before reading a header.
Retain the returned `Reply` through its control response write. The adapter does
not advertise profiles or connect itself to the public Core listener.

Options bound active transfers (1..128), per-owner transfers (1..active), file
workers (1..32 and no more than active transfers) and whole-header receipt time
(positive, at most 30 seconds). Defaults are 8, 4, 4 and 5 seconds. Header bodies
are bounded to 4096 bytes before allocation; the reused body buffer is at most
16 KiB and no larger than the payload store's chunk ceiling. Progress renews
only idle time, never lifetime; queued/slow file writes cannot renew either.
Matching committed operations replay their receipt without payload/FIN and stop
the redundant stream with application error 0. New empty inputs still need FIN.

A fixed native file pool keeps file work separate from the control metadata
pool. At most one ordinary job and one deferred file destructor per live input
can occupy the queue, with two queue slots reserved per configured active input.
Jobs and returned file-owning values retain the same transfer lease. Cancelling
an async waiter cannot refund quota or complete detach while its file work or
stage cleanup remains. Abandoned staging files are removed and directory-synced
on those file workers, not a Tokio executor thread. A possible admission commit
is not wrapped in an async timeout that could falsely report pre-commit refusal.

Nine input tests use actual QUIC streams and guarded storage, including a
64 KiB input across 4 KiB stream/16 KiB connection windows, malformed/truncated
headers, identity and application checks, integrity failures, unbound/empty
input, owner quota across rotated certificates, stalled headers and payloads,
continuous-progress lifetime expiry, cancelled file work and idle expiry while
file preflight is deliberately blocked. Preflight/chunk timeouts abort reception
without refunding still-running file jobs; the possible admission commit keeps
its separate uncertainty rules. A worker
test checks off-executor cleanup before pin release and fails when that cleanup
is deliberately made synchronous. Control operations in these tests remain local
adapter calls, not durable control traffic over QUIC. No Java interoperability,
whole-process memory bound, public durable endpoint or workload result is implied.

### Version-2 retained-result transport adapter

`v2_authority::output::Outputs` sends actual retained objects over the requesting
TLS peer's server-initiated unidirectional streams. For an ordered, validated
`RESULT Read`, pass its `Pending` submission to `Outputs::request` instead of
calling `Pending::run`. Construct one shared output service with the same
`Authority` instance. It checks global/owner transfer ceilings before acquiring
a result lease or file handle. Connection pending/result-stream counters include
stream creation, blocked writes, unsent refusals and outstanding file cleanup.

Options bound active transfers (1..128), per-owner transfers (1..active), file
workers (1..32 and at most active transfers) and pending stream-creation time
(positive, at most 30 seconds). Defaults are 8, 4, 4 and 5 seconds. The selected
idle/lifetime bounds still apply, including time before a stream can open. File
workers have the same two-slot-per-live-transfer queue reservation as inputs.
Result chunks reuse a buffer no larger than 16 KiB or the payload-store ceiling;
object headers remain bounded to 4096 bytes plus their four-byte length prefix.

File reads and current retained-session checks run outside the async executor.
Before each nonblocking Quinn write poll, the adapter checks authorization and
the earliest exclusive deadline. A blocked write registers a separate notifier;
its next poll follows fresh checks, not a previously authorized write future.
While blocked, a 20 ms local check interval also detects revoked/expired owners.
Only bytes accepted by the transport renew idle time. Read-ahead, headers, empty
progress and file-worker delays do not. A verified EOF precedes FIN; successful
local scheduling is not proof of client receipt.

Retain `Delivery` through its control refusal write or sent/aborted handling.
`Status::Refused` supplies the one control response when no header byte started.
After the header starts, errors produce only a reset and `Status::Aborted`, not
a second control response. Cancelling the async task resets its stream; queued
file work retains its read and quota until cleanup. A new identical read serves
the same output without retrying execution or changing its manifest/attempt.

Twelve result transport tests cover empty/64 KiB output, small receive/send windows,
repeated reads, stopped/slow readers, pending stream creation, current credential
expiry, wrong commitments, actual retained-byte corruption, rotated and distinct
owners, configuration and connection/global quotas, and cancelled file preflight.
They also cover shared control/data send ownership and wrong-connection refusal.
Those adapter tests still call control locally; the separate durable listener
now supplies public integration. Independent Java V2, neutral cross-language
failure tests, full resource measurements and the equivalent workload remain open.

`pipestream_core::v2` implements Section 12/Appendix F typed wire records,
control and object-header framing, canonical and cross-field validation,
profile negotiation, domain-separated commitments, incremental scope seals
and status trees, bounded client correlation, and constant-memory payload
length/hash/FIN/deadline validation. All 70 frozen wire expectations and 12
frozen commitments are tested without regenerating their bytes.

This module does not advertise profiles or run a V2 listener. Its public
constructed records are revalidated before encoding. `Control::decode` checks
one complete bounded frame; a transport must use `control_body_length` before
allocating/receiving it. `object_header_length` supplies the equivalent check
for object headers. `Control::validate_context` checks direction and selected profiles.
`Correlation` tracks connection requests and stream responses, including
known request fields. The client journal below adds durable creation/intent and
receipt-digest checks; transport authentication and complete client integration
remain required. `PayloadReceiver` accepts borrowed
chunks but neither stores them nor runs callbacks. An endpoint must drive
its deadline checks even when no bytes arrive, and invoke `finish` only for
an actual successful FIN, never a stream reset.

### Version-2 client recovery journal

`pipestream_core::v2::client::Journal` stores one immutable creation/session in
a separate SQLite database. `initialize` is explicit new history; `open` refuses
missing, empty, incompatible or changed history. Both require trusted expected
authority, owner, creation sequence, exact policy, durable/result profile choice
and local limits. These values are not discovered from a locator or guessed
after losing a journal. No credentials or input/output payload bytes are stored.

Initialize before sending creation. After TLS and wire correlation validation,
`record_binding` checks and durably stores the exact binding, excluding the
connection request number. Before transmitting any mutation, `prepare` commits
its original operation ID and complete typed parameters. `intent` reconstructs
the same declaration, admission, retry, cancellation, skip or scope cancellation
under a fresh control request number or actual input stream. Changed parameters
under an existing ID are refused; matching preparation is idempotent.

`record_receipt` verifies the operation digest under the stored session, typed
outcome and all known request constraints, then persists the exact receipt.
This library does not authenticate a directly supplied record. The scope APIs
below validate complete membership and status coverage; a stored
declaration receipt alone is not proof of complete scope coverage. An input
producer must still receive the covering declaration receipt before transmission.

`unresolved(after, limit)` returns at most 256 records with monotonic local
cursors. A missing local receipt, NOT_FOUND, a refusal, stream reset or failed
receipt commit never proves that the authority did not commit and never authorizes
a replacement identity. Replaying the original intent or operation lookup resolves
that uncertainty. No automatic eviction, expiration or identity regeneration is
provided. A local disk-full error leaves earlier intent available for recovery;
callers must not report the unsaved observation as durably recorded.

`observe_work(revision, view)` checks known admission, attempt/fence, policy and
manifest commitments, then commits the view and its manifest atomically. An
out-of-order reply cannot overwrite a newer view. Accepted retry/cancel/skip
receipts and terminal observations are checked in either arrival order.
`observed_work` returns the newest durable observation, not an inferred current
authority state. `remember_manifest` retains immutable evidence independently of
output availability; its presence never renews a read lease or schedules work.

`remember_reference(manifest, index)` atomically retains the full manifest and
the application's explicit output selection. `retained_reference(work, attempt,
index)` restores it and constructs attachment/read requests with the retained
issuer, owner, session, attempt and digest. No endpoint or credentials are derived
from the locator. The caller supplies trusted endpoint mapping and authentication,
and must still validate the actual result bytes, length and FIN. A manifest alone
does not choose an output, and a failed download does not authorize a new attempt.

`observe_scope_page(request, response)` validates the actual correlated page pair,
merges bounded out-of-order or overlapping membership snapshots, and records
`membership_verified` only after the full count and recomputed seal agree. Empty
pages and terminal state hints do not supply missing membership or full WORK views.
`scope_observation` and bounded `scope_members` restore this evidence on reopen.
Known declarations, immutable parent allocations, terminal outcomes and ancestor
fences constrain new evidence in either arrival order. Valid child-first metadata
may remain pending without inventing parent admission; learning a parent through
a page or sealing receipt rechecks previously retained child relationships.
Contradictions refuse atomically, preserving the earlier evidence and uncertainty.

`record_checkpoint(summary)` uses a conservative full-local-evidence policy:
require verified complete membership, every terminal WORK view and already
verified child coverage, then recompute the exact count partition and status root.
It checks closure times and STRICT successful-parent semantics before committing.
This is stronger than merely comparing a summary with the client's currently
known commitments; it adds member reads and local storage that must be measured
in the workload comparison, not a new universal wire requirement. `covered_scope`
restores the committed proof. `root_completion(request)` constructs DRAIN with
the exact saved root summary; the transport must separately drain requests and
transfers and validate the authenticated echo. It is not a shutdown acknowledgment.

Default inventory is 4096 operations and independently 4096 entries in each of
the work-view, manifest, output-selection, scope, scope-member and coverage
inventories. Each configured ceiling
is from 1 through 1,000,000. Existing entries remain usable at capacity;
individual images are bounded before reads/allocations. The existing guarded
SQLite backend separately caps database, WAL, rollback-journal and shared-memory
file lengths. FULL synchronous commits and checksummed, identity-bound records
survive restart under that backend's durability assumptions. These are file-length
and record-count bounds, not measured heap/RSS/native-memory guarantees. Opening
audits retained records and validates child coverage before parent coverage without
a recursive whole-tree buffer. Membership and status hashing are incremental.
Relationship scans and receipt comparisons remain blocking and their latency is
not bounded by these tests; preparation also checks the retained inventory.
All journal calls are blocking and belong off the control reader/async executor.
The directory must remain private to cooperating journal users; checksums do not
authenticate hostile edits or repair rollback/loss of local history.

The original thirteen substantive journal tests plus one subprocess entry point cover exclusive
reopen, profile/identity conflicts, all six mutation kinds, concurrent preparation,
pagination/cursor exhaustion, corrupt images, failed receipt commits, actual
physical exhaustion and forced process termination after intent commit.
Removing that commit deliberately makes the crash-recovery test fail. A regression
also verifies that incompatible reopen refuses before changing SQLite journal mode.
Additional tests cover contradictory receipts in both arrival orders, observation
monotonicity, terminal immutability, empty output manifests, inputless cancellation,
explicit reference selection, independent quotas, corrupt normalized keys/images,
atomic rollback and a large manifest refused by a real disk cap. A second
forced-exit scenario reopens the committed terminal view, manifest and selected
index. Two real-QUIC tests reopen the journal after unrecorded creation/declaration/
admission replies, replay original identities and read the original attempt's
output. The result case reopens its saved reference, authenticates with a rotated
owner certificate and checks that retrieval leaves the terminal revision unchanged.
It now also records actual SCOPE pages/checkpoint, exclusively reopens, reconnects
and completes DRAIN with the original saved root summary. Additional scope tests
cover empty and 300-member paginated scopes, valid child-first metadata, both-order
contradictions, late sealing receipts, missing descendants, STRICT failure,
counts/hash/time corruption, quotas and atomic rollback. A third forced-exit
scenario preserves verified membership and root coverage across process death.
Those tests use the existing Rust codec, not an independent oracle.

The local client format is now 3, distinct from authority storage and wire version.
Older client journals refuse before WAL configuration; there is no automatic
migration, deletion or replacement of unresolved history. No wire format changed.
The additional tables raise the measured empty database floor on the pinned build
to 73,728 bytes (WAL 0). The physical-exhaustion fixtures now use 128 KiB limits
for the database, WAL and rollback journal and 64 KiB for shared memory; they still
exercise actual exhaustion/refusal and verify earlier evidence survives reopen.

The V2 transport below supplies the network event loop, and the durable session
client composes these journal/coverage APIs automatically. Owned file adapters
are available below and exposed by the V2 command group.
The async journal owner supplies bounded off-runtime storage ownership.
Independent Java V2, neutral cross-language failures and the original
external workload/equivalent streaming-gRPC resource comparison remain open.

### Version-2 client wire transport

`v2_client::transport::Transport` owns one authenticated QUIC connection. Its
`Security` constructor uses configured trust roots and an optional client
certificate, never an insecure verifier or credentials inferred from a locator.
The capability offer defaults to Core only. A durable application must explicitly
require the profiles its journal/work needs and implement their full obligations.

`exchange` replaces a caller's placeholder request ID with a connection-local
monotonic ID. It leaves operation identities and commitments unchanged. Controls
are independently read and written through the reserved-credit flow owner;
the bounded correlation book accepts reordered replies. A cancelled waiter
retains correlation until a response or the configured response deadline closes
the connection. It does not cancel work or authorize a new mutation identity.
Result reads require a previously authenticated, identity-checked manifest.

`input` registers actual stream IDs in allocation order. `Input::write` copies
at most one 8 KiB chunk per command, sends incrementally and aborts if an accepted
write future is cancelled. `finish` validates length/digest and schedules FIN,
not admission. `response` returns the authority's correlated receipt/refusal,
including header-only replay. A locally abandoned input keeps its correlation
until that response or bounded connection failure. Admission limits are checked
before another stream is allocated.

`Output::read_unverified` returns bounded chunks for reversible staging. Only
validated full length, digest and FIN make `verification()` available. Independent
tasks enforce idle/lifetime deadlines even if the consumer stops polling; an
object queue has one 8 KiB chunk plus one in-flight chunk. A corrupt/truncated
payload or recognizable wrong commitment fails only its delivery. Unsolicited,
duplicate and wrong-direction correlation remains connection-fatal.

There are 64 process-wide connection owners, including cancelled handshakes and
Quinn draining. Each connection has at most 128 pending calls, negotiated stream
ceilings, bounded headers, and conservative 2 MiB raw-control and 2 MiB transport
configuration gates. Pending tickets include completed internal replies until
consumption/discard; caller-owned returned values are not internal history.
Object tasks are owned and joined, and completed task records are reaped before
replacement admission. `close` closes transport only; `closed` confirms network
task/endpoint drainage. These structural/count gates are not measured heap/RSS.

This is the low-level network half, not an automatic durable-session facade.
Applications must await journal intent persistence and a covering declaration
receipt before transmitting input, then validate/persist authenticated observations
before using their commitments. A real-server test composes the public journal
and transport across a 256 KiB transfer, replay, certificate rotation/reopen and
exact root completion. Client staging recovery, complete independent
Java V2 and neutral cross-language/workload evidence remain unfinished.

### Version-2 durable session client

`v2_client::session::Client` owns one journal and one authenticated connection.
Supply a previously initialized or reopened async `Journal` and a trusted
`Endpoint` (socket, verified TLS server name, trust and caller certificate).
The client requires exactly the durable profile combination retained by its
journal; it neither follows a locator nor changes the retained identity/policy.
It replays the original creation if no binding was saved, otherwise attaches to
that same session. Binding identity/policy validation and persistence finish
before `connect` returns. A cancelled connect waiter still has an owned binding
collector, followed by journal/transport shutdown.

`mutate` persists the exact immutable intent before transmission and validates/
records the receipt before successful return. `recover_operation` looks up only
a locally retained original operation, then saves the returned receipt. Refusals,
NOT_FOUND, transport loss and journal errors never authorize another operation
identity. `intent`, `receipt` and `unresolved` expose bounded local recovery reads;
there is no automatic new operation or retry-attempt allocation.

`input` requires the operation ID of an already saved declaration receipt that
actually covers the entity. It then commits admission intent before opening the
stream. The upload writer and admission collector have separate owners. Dropping
an upload or receipt waiter cannot stop persistence of a legitimate late admission.
`Admission::finish` is local FIN only; `receipt` reports success only after the
receipt is validated and committed. The low-level transport also exposes
`Input::split` for separately owned writer/response halves.

`watch`, `scope_page`, `checkpoint`, `manifest` and `select_output` authenticate,
correlate and persist their evidence before returning it. Reordered work replies
return the newest consistent locally saved view. WORK wait expiry returns an
unchanged view, while an unfinished checkpoint wait refuses WAIT_TIMEOUT.
`checkpoint` still requires the journal's complete local closure evidence;
missing evidence is not invented. `manifest` permits legitimate zero-output
success without requiring an output selection. `read_output` uses an explicitly
saved manifest/index and performs a fresh authorized read of those same bytes.
Output stays unverified until length, digest and FIN pass.

`complete` and `detach` are barriers over accepted client operations before
requesting the authority's actual connection cut. New calls refuse NOT_READY
while a barrier is active. A failed cut reopens acceptance; a successful cut
closes it. Complete uses the exact saved root summary; detach never claims work
completion. `close` refuses new calls, drains owned collectors and closes the
transport/journal; `closed` waits for that cleanup, and `shutdown` does both.
A timeout around `closed` is not completed shutdown. Last-handle drop also drains
accepted collectors. Reconnect by reopening the original journal, not replacing it.

Only one durable client can claim a given async journal owner. Do not mutate
through retained raw journal clones while attached. Disk calls serialize off
the control reader, with bounded waiting under the client operation ceiling.
There are at most 64 client owners per process; `Options::in_flight` is 1..=32,
default 16, including queued/running work and unconsumed internal replies.
Cancelled waiters retain capacity until their collector and reply are finished
or discarded. Returned output handles retain a slot until dropped. Ordinary
returned records become caller-owned. These are structural/count bounds, not
measured heap/RSS or comparative workload costs.

Real-server tests cover a persisted 256 KiB round trip and root completion across
reopen/certificate rotation, cancelled creation/mutation/upload waiters, identity
mismatch, missing covering receipts, changed intent, refused-operation recovery,
exclusive ownership and failed/successful barriers. The journal is configured
with a single operation slot in these tests to exercise serialized storage.
Client staging recovery, independent Java V2, the neutral failure/resource
driver and original external workload/streaming-gRPC comparison remain required.

### Version-2 owned file transfers

`v2_client::session::files::FileInput::open(path, maximum_bytes)` opens a regular
file without following a final-component symlink, checks its size and prehashes
it with an 8 KiB buffer. Directories and FIFOs refuse; the nonblocking open avoids
waiting for a FIFO writer. The same open descriptor is rewound and retained.
`length` and `sha256` supply the admission commitment. `send(client, intent,
declaration_operation)` requires an exact match before preparing the admission,
then streams the file through the durable client and waits for its saved receipt.
Length/digest are checked again before FIN, so mutation of a prehashed file does
not create a new successful admission. A replay may legitimately return the
original receipt after the server stops the redundant upload. `Admission::abort`
also stops the writer while preserving its independently owned receipt collector.

`Output::save_to(path, maximum_bytes)` refuses an oversized result before creating
a file, stages bounded chunks, and installs the destination only after verified
length/SHA-256/FIN. It synchronizes the file, installs without replacing an existing
file or symlink, and synchronizes the containing directory. A failure after
installation is ambiguous local durability; the installed file is not deleted
to hide it. `SavedOutput` includes the verified transport commitment. The same
adapter is available on the low-level transport output, but that API does not
persist a manifest/selection in the journal.

These APIs require trusted, stable application-owned containing directories.
Inputs are not filesystem snapshots and must not be modified during transfer.
Accepted send/save tasks survive cancelled waiters. Four shared file workers and
64 process-wide file-owner slots bound active descriptors, queued work and deferred
cleanup; an unconsumed internal reply retains its slot. `FileInput::close` waits
for descriptor cleanup. Buffer sizes and owner counts are not measured total
heap/RSS guarantees. The caller supplies the per-file byte ceiling; this is not a
shared retained-directory disk quota.

Ordinary failed downloads remove only their own temporary file. Abrupt process
death may leave `.pipestream-result-*` staging files. This adapter deliberately
does not scan or delete other transfers' files; exclusive staging ownership,
bounded restart reconciliation and shared disk budgeting are provided by the
separate managed store below, not by this arbitrary-path export adapter.
It never turns client file loss into new remote operation identity or work.

Tests cover empty/256 KiB real-server round trips, replay, changed source bytes,
pre-submission commitment mismatch, cancellation, byte limits and no-overwrite;
adversarial authenticated peers send corrupt/truncated/extra bytes and withhold
FIN. A separate process opens 64 input descriptors, refuses the next and admits a
replacement only after explicit cleanup.

### Version-2 managed local result copies

`v2_client::session::files::managed::ManagedResults` wraps the existing bounded
immutable object store. `initialize(path, authority, owner, policy)` accepts only
a new private directory; `open` requires the same trusted authority/owner and
immutable `ResultPolicy`. The purpose-qualified local binding is separate from
the authority's random storage identity. It permits copies from multiple sessions
of the same owner, not cross-authority or cross-owner reuse. It is not a credential.

`save(output)` takes a durable client's authenticated result stream, its retained
manifest selection and negotiated limits. It reserves the complete length plus
524 bytes of header allowance before writing, stages incrementally, and installs
only after verified SHA-256/length/FIN and file/directory synchronization. Accepted
tasks survive waiter cancellation. Repeated downloads are separate charged copies;
there is no silent eviction. `usage()` reports shared object/byte occupancy.

`find(retained_reference)` is explicitly local-only: the full journal selection
supplies the content commitment. It returns a `LocalCopy`, with provisional chunks
from `read_unverified()` until `None` and `verified()==true`. A cache hit does not
prove current server authorization or renew remote output retention. The journal
still owns work/attempt identity; cached bytes are not a replacement outcome.
`remove(key)` discards only that local copy. Live downloads/readers refuse removal;
quota is returned after directory sync, including retry after interrupted unlink.

The process lock and complete bounded root audit precede reuse. Unknown paths,
symlinks, changed ownership/policy or corrupt object metadata fail reopen.
Unreferenceable interrupted staging is reclaimed under exclusive ownership;
committed objects survive and are hash-checked again when read. `recovered_stages()`
reports that cleanup. Initialization never adopts the old arbitrary-path temporary
files. An operator must not treat a filename prefix as ownership proof.

All opens, reads, writes, deletion and last-owner cleanup use the existing four
file workers and shared 64-owner limit. `close()` drains this wrapper's root owner,
refusing remaining clones; outstanding `LocalCopy`/download handles can retain
the root lock until their own cleanup. LocalCopy has an explicit `close()`.
`ResultPolicy` supplies object, charged-byte, chunk and handle ceilings; these
are not whole-process heap/RSS or filesystem-block reservations. No new dependency,
wire encoding or journal/database format is introduced.

Six substantive core tests plus a subprocess entry point cover exact/empty
reopen, shared quotas, pins, changed commitments, corrupt body, owner/policy checks,
unknown-file preservation and process death before/after installation and unlink.
An injected directory-sync failure reproduced an installed copy becoming readable
despite uncertain namespace durability. The shared object store now quarantines
all reads/capacity decisions on that failure until exclusive audited reopen.
Two async tests cover root ownership and a real authenticated 256 KiB transfer,
quota rejection, unchanged terminal revision, offline reopen/verification and
explicit removal. These are implementation tests, not the neutral cross-language
failure oracle or measured workload/resource comparison.

The low-level blocking API is `pipestream_core::v2::client::results::ResultStore`.
Its `PendingResult::finish` requires already authenticated, correlated transport
FIN evidence; the async wrapper obtains that evidence from the opaque transport
verification result. The core API itself does not authenticate arbitrary records.
Managed-store CLI integration and crash-safe arbitrary-path exports remain open,
alongside complete Java V2, the neutral failure driver and the original external
workload/equivalent authenticated, durable streaming-gRPC comparison.

### Version-2 asynchronous journal owner

`pipestream_quic::v2_client::journal::Journal` exposes async versions of the
complete core journal API. One dedicated worker owns opening/auditing, all SQLite
operations and final store/file-owner destruction. No application callbacks are
accepted. Typed variable-size arguments are checked before queueing; mutation
validation also precedes cloning parameters in the core request/header helpers.
This does not authenticate caller-supplied observations or replace correlation.

`Options::in_flight` defaults to 16 and accepts 1 through 32. The ceiling includes
queued calls, executing calls and completed replies not yet consumed or discarded
by their callers. Capacity exhaustion refuses with LIMIT_EXCEEDED instead of
adding an unbounded waiter queue. Cancelling an accepted future does not cancel
its queued/running journal operation or refund its slot early. The original
intent may commit after cancellation and remains its recovery identity. Results
returned to an application become caller-owned, not an internal retained history.
There are at most 64 journal workers per process, including opening/closing work.
These count and typed-record bounds are not measured heap/native-memory/RSS limits.

Clones share the same worker. `close()` atomically refuses further calls on all
clones and lets accepted operations finish. `closed()` waits for their completion
and release of the store/ownership lease; `shutdown()` does both. A timeout around
that wait is not completed shutdown. Dropping the last handle also drains accepted
operations. Worker panic reports INTERNAL_ERROR and releases its owners without
erasing committed history. Failed construction waits for owner cleanup before
returning, so immediate retry does not race the failed worker's lease release.
None of these local operations cancels remote work or acknowledges network DRAIN.

The Unix backend takes a nonblocking advisory lock on the stable, empty
`<journal>.client-lock` sidecar. A second cooperating async opener, even in another
process, refuses CONFLICT while the first worker owns it. Symlink or nonempty
sidecars refuse without overwrite. Keep the directory private, never unlink the
lock while clients may hold it, and do not bypass ownership by opening the
low-level core journal concurrently. The lock is not durable identity evidence
and does not repair lost storage. Client format 3 and authority storage are unchanged.

Nine substantive worker scenarios plus one subprocess entry point cover bounded
admission, unread-reply capacity, cancellation of queued/running calls, single-
threaded async progress, shared close, last-handle drop, panic, invalid history,
ownership conflicts, sidecar safety and forced process exit after a real intent
commit. The two real-QUIC recovery tests now use these async APIs for creation,
binding, intent/receipt, work, reference and root-coverage persistence, waiting for
actual local shutdown before reopen. They still share the Rust codec and manually
drive the network; this is not the production connection multiplexer, independent
Java V2, neutral process-failure driver or workload/resource comparison.
An isolated resource test opens 64 real journal owners, refuses the 65th before
creating its database/lock files and admits a replacement after owner shutdown.
The 64 empty databases total 4,718,592 bytes on the pinned build. This measures
owner admission and database lengths, not process memory or comparative throughput.

`v2::authority::AuthorityStore` adds normalized SQLite session/creation history,
declarations, immutable operation receipts, bounded pages/revision snapshots and
sealed closure. It checks current local authorization, trusted UTC,
logical quotas and guarded database/WAL file-length limits. `initialize` is an
explicit new-store operation; `open` fails on missing/empty history. The caller
must supply a verified principal, a bounded local authorization policy and a
trusted clock. An accepted forward jump is treated as real UTC; the store refuses
later regression. Operators must reject unjustified jumps in the clock provider
and must not restore stale issuing history without external anti-reuse proof.

On Unix, `authority::payload` now provides bounded streamed staging, durable
immutable installation, verified file-backed reading and exclusive root ownership.
Limits separately bound global/per-owner bytes, objects and live handles, plus
the size of each borrowed I/O buffer. Incomplete reception reserves its full
declared length and a header allowance. Reopen rebuilds file accounting and
reclaims abandoned stages only under exclusive ownership; installed orphans
remain charged until the authority's reference-safe collector removes them.
Live installed/read handles remain pinned against collection. The database
retains a local random store identity and a once-bound canonical payload path.
Internal authority storage is now format 10; payload roots remain format 4.
Prior authority formats are refused, not silently converted or replaced.
This changes no wire schema or frozen vector.

Work views and complete mutable scope state use fixed-capacity checksummed
records, not whole-session images. Declaration preallocates 2048 bytes per work
view and two record-rewrite credits, plus a separate 256-byte first-fence record
and one credit. Each record has a 104-byte header. Scope creation preallocates
1024 bytes and four credits: cancellation freeze, deferred seal computation,
root revocation upgrade and closure. Membership counters, seal,
cancellation/revocation flags and summary
share that scope record. Root revocation is read from it, not a separate mutable
session column. The typed first work fence binds to its immutable accepted
operation receipt and preserves CANCELLED versus SKIPPED across restart.
The shared greatest-UTC value has its own fixed 64-byte record.

Each credit funds its record overwrite plus one shared-clock overwrite in the
same transaction. Forecasts also preserve both records' required revision
increments; ordinary observations cannot consume the shared clock's last
increments reserved for existing work/scopes. A funded transition checks time,
spends its state credit, then remembers that time before committing. Equal UTC
observations need no clock rewrite.
Ordinary metadata writers protect those credits through the guarded SQLite VFS.
An incremental BLOB rewrite spends one credit without allocating database pages;
empty scope closure already uses its reserved state and clock allowance. Reopen
verifies record bodies, padding, relational identities and timestamps against the
shared clock, and reconstructs journal funding.
The cost bound is specific to bundled SQLite 3.53.2 and its checked page/sector
geometry. These credits fund their stated paired rewrites, not arbitrary
additional SQL. Admission now allocates work/job credits atomically with its
receipt. Publication and settlement write sets now have the tests described
below; the complete execution/retirement lifecycle still requires resource
tests. Record credits alone do not prove those APIs correct.

`PayloadStore::reserve_outputs` durably reserves the maximum output count/bytes
before metadata admission. The immutable reservation file remains charged across
restart, including unused slots and bytes. `PayloadUsage::objects` now reports
charged slots, not just present files. Global/per-owner forecasts include ordinary
uploads, reservation files and all promised outputs. Materializing those outputs
does not charge the same promise twice or donate unused space to another upload.
These are bounded file-length quotas, not an allocation of filesystem blocks or
a guarantee against external disk failure.

`OutputReservation::stage` accepts a per-output maximum and streams through
borrowed bounded buffers. The final descriptor is computed from actual writes;
no length/hash placeholder can become an installed output. Fixed-size header
slots avoid moving the body when its descriptor becomes known. Each output slot
is unique within its reservation. Live reservation pins preserve uncommitted
outputs while allowing sequential production of all 256 slots with two handles.
Garbage collection also respects SQLite references: a committed live reservation
pins its installed outputs even after all process handles are gone. It retains a
funding record while any of its output objects remains. Reclaiming unpublished
outputs for a replacement worker now uses the executor's lease-fenced recovery
operation, which refuses while any old reservation/output handle remains live.
`AuthorityStore::reclaim` supplies the separate retention eligibility and
logical-capacity transitions; the collector alone does not decide expiry.

Startup rebuilds owner totals and output occupancy, rejects missing/corrupt or
contradictory funding, and syncs the directory before using reconstructed capacity.
An error creating or renaming a staging file quarantines the root; callers must
release its handles and reopen exclusively to establish the namespace outcome.
An installed reservation reopened as evidence is synced before its pin is issued.
No admission or successful-work acknowledgment follows from these filesystem APIs.

`AuthorityStore::receive_input` validates ownership, membership, application/mode,
profile, immutable operation, duration and response/byte limits before staging.
Applications must register immutable versioned labels, explicit safe-restart
semantics and an actual `Application` callback; an unknown contract has no
fallback. `ValidatedInput` is installed
storage evidence only: it does not change DECLARED work, schedule a job or permit
an admission acknowledgment. The committing transaction must revalidate authority
and reserve every execution/publication/retention resource before admission.

`AuthorityStore::prepare_input` combines validated input with a durable output
reservation and expands its work-view record to the conservative promised
response size. Filesystem installation occurs outside the metadata writer;
membership, application, limits, exact bound payload root and authorization are
rechecked before private funding commits. Preparation preserves DECLARED and its
revision and creates no operation receipt or job. Dropped/failed preparations
leave charged, reference-safe payload orphans. Expanded metadata remains charged
to its declared work until later retirement; it is not an untracked staging file.

`AuthorityStore::admit_input` consumes that opaque preparation and repeats current
authorization, scope/operation/application/limit and payload-root checks under the
SQLite writer. It commits input and reservation references, attempt 1, exact
timestamps, one child for either branch mode, a fixed 2048-byte job record and
the immutable admission receipt together. It invokes no callback. Duplicate
prepared inputs serialize to the same receipt, not two jobs. A pre-commit refusal
leaves work DECLARED; post-commit process death leaves replayable admission and
real reopenable input/reservation files.

The retained policy bounds global and per-owner accepted jobs as well as session
jobs and retained input/output bytes. Waiting branches consume executor capacity;
this is an accepted-job limit, not a count of running threads. Job liveness flags
and six rewrite credits per job plus four per admitted work record provide
storage for subsequent branch, fence, settlement and release transitions. Quota reconciliation streams these
records instead of caching a whole-session image. Reopen validates job/work,
child, payload-reference and admission-receipt consistency, including manifest
owner, session, profile and admitted output limits.

`authority::execution::Executor` now claims a known job under a durable private
lease and invokes its registered callback outside the metadata transaction.
`WorkContext` streams the real input and stages bounded outputs, checking current
authorization, ancestor fences, attempt, lease and original deadline on each I/O
operation and again at publication. Lease renewal cannot revive an expired lease
or extend the execution deadline. The included `CopyApplication` streams actual
bytes through an 8192-byte stack buffer, clipped to the configured I/O limit.
Callbacks cannot supply result locators; a validated authority endpoint does.

Success publishes a manifest derived from fsynced immutable outputs and terminal
timestamps in the same work/job/clock transaction. Panic, incomplete output or an
ignored output error cannot publish success. Retryable outcomes remain nonterminal;
`retry_work` must explicitly advance the attempt, retain input/child/deadline,
replenish credits and commit its immutable receipt. Restart recovery advances only
the private lease, not the wire attempt. Replacement workers recycle unpublished
output slots only after old handles drain, preserving the original quota promise.
Publication uses the live reservation pin without acquiring another read handle.

The complete publication write set is tested with 0, 1 and 256 actual outputs,
a pinned WAL reader, ordinary writes exhausted and SQL row replacement forbidden.
Real subprocess exits bracket claim, publication, retry and output recovery.
These tests do not establish a complete resource-bounded lifecycle.

`Executor::start_workers(PoolConfig)` now runs a fixed pull pool over the durable
job table. It needs neither a resubmitted job key after restart nor a volatile
queue of admitted payloads. Configuration bounds callback threads (1..128), concurrent
dispatched jobs per owner, records inspected per discovery pass (1..256), and
idle polling (1..60000 ms). One additional maintenance thread drives settlement
even while every callback worker is occupied. A shared row cursor advances past refused/waiting work
and wraps through retained jobs; this is bounded-memory scanning, not an indexed
ready queue or a constant-cost sweep of a large populated store. Accepted-job
capacity remains separately enforced by the persisted admission policy.

Before committing a claim, the worker opens its input and reservation and charges
one reusable output-I/O slot when outputs are allowed. A staged/installed token
borrows that slot without double charging. Between outputs the slot remains
unavailable to unrelated reads. If an output outlives its reservation, its charge
transfers to the ordinary live pin under the same inventory lock. Capacity
pressure can defer a new claim without changing the job or invoking its callback;
it cannot steal staging capacity from an already claimed worker. These transient
slots are re-acquired on restart, not confused with durable byte reservations.
Admission also checks permanent feasibility: the immutable global/per-owner
handle ceiling must permit at least three slots for output-producing work, or
two for work declaring no outputs. It does not reject queued work merely because other
readers temporarily occupy otherwise sufficient capacity.

Only one pool can own the payload authority, including across executor clones.
Optional `wake()` lowers admission/retry latency; polling supplies correctness.
`snapshot()` reports active dispatches, scans, completions, retries, settlements,
sealed/closed scopes, refusals and
fault state using bounded counters and one bounded diagnostic, not an unbounded
history. Clock/capacity/auth/fence refusals back off; corruption and unknown storage
failures stop discovery. `request_stop()` and dropping the pool stop further
discovery without cancelling durable work; dispatched callbacks may finish.
`shutdown()` joins them. The pool retains ownership until all its threads return.
Callbacks must cooperate with context deadlines/renewal; arbitrary application
code is not forcibly preempted. Real subprocess tests now use the pool for both
crash injection and recovery without submitting known keys.

`cancel_work` and `skip_work` atomically retain their operation receipt and first
fence. A leaf can settle immediately; a branch remains CANCELLING until its
descendants close. The first fence preserves its promised outcome despite later
ancestor cancellation, deadline expiry or STRICT child failure. Existing terminal
outcomes and manifests never change. `cancel_scope` freezes current membership,
including an empty or authority-produced scope, before background hashing.
`revoke_session` is a local operator API with a distinct Revoke permission, not
an unauthenticated RPC. It denies caller access and freezes the root even when
the caller's authorization has already been withdrawn.

`reconcile(ReconcileCursor, limit)` accepts limits from 1 through 256 and visits
at most that many work records and members of one scope per call. It settles expired ACTIVE/AWAITING_RETRY work,
cancelled descendants and STRICT parents; frozen membership and terminal views
feed incremental seal and status folds. Inputless declarations do not disappear.
The cursor retains one bounded fold, not a session image. Losing it causes
unfinished hashes to be recomputed from immutable rows; it cannot lose outcomes.
Work and scope passes commit separately, so an error in the second pass does not
roll back already committed work settlement. Retrying is safe. Credit accounting
still streams retained records: bounded batches are not a constant-cost entire
transaction or a measured scalability guarantee.

The public fence, revocation and settlement commits are tested on both sides of
actual child-process death. Tests also cover real callback publication races,
600-member chunked closure across restart, independent maintenance while all
callback workers are blocked, and complete work/job/scope/clock transitions with
a pinned WAL reader after ordinary writes exhaust their allowance. These gates
forbid SQL row replacement and check unchanged database page count. Input/output
liveness and byte reservations stay charged until the separate reclamation pass;
settlement alone does not release them.

Authority-expanded applications now register an actual `Expansion` callback;
registering mode 2 without one is APPLICATION_UNSUPPORTED. `ExpansionContext`
supplies bounded input reads, stable local operation IDs, declaration and the
same receive/prepare/admit pipeline used by external inputs. An opaque grant
binds every committing mutation to the current parent identity, child scope,
attempt and worker lease. Replayed local receipts use producer namespace 1;
external callers still cannot supply producer-1 inputs or claim that namespace.

Expansion can complete, yield, request explicit retry or fail. Yield preserves
accepted children and the attempt, releases the worker and uses ordinary writes,
not terminal-transition credits. Discovery backs off after a yield. The fixed
job record now has 16 typed fields, including a durable expansion-complete flag
and the later resource-release intent:
sealing membership alone cannot suppress missing child admissions after restart.
An explicit retry preserves the child scope, operations and completed expansion;
once expansion finished it reruns only reassembly, not child generation.

Both branch modes require a retained successful child closure before reassembly.
`WorkContext::children` pages direct children; `begin_child_output`,
`read_child_output` and `finish_child_output` stream their exact retained outputs
under the parent worker's authorization and fences. Verified EOF is mandatory
before successful publication. These internal dependency reads may outlive
external output expiry; they are not caller result-read leases or URI fetching.
Before a reassembly claim the worker reserves a reusable child-reader slot.
Unrelated readers cannot steal it, and a live reader retains its charge even
after worker ownership drops. Policies that cannot support a branch's minimum
handles are refused before admission. With outputs, caller branches need four
handles and authority branches five; the zero-output minima are three and four.
Child staging/admission still acquires its own capacity and may refuse under
concurrent pressure. Applications can yield and replay; a parent admission does
not promise unlimited future descendants or their execution slots.

Nineteen branch tests include real `abc` -> two uppercase children -> `ABC`
reassembly with two-byte buffers, both producer modes, one-worker progress,
six subprocess commit crashes, explicit retries, stale grants, unverified reads,
handle saturation/lifetime, minimum-policy execution, and pinned-WAL completion
without SQL row replacement or database page growth. Reopen refuses format 7
and semantically impossible successful work with unfinished expansion.
`authority::results::ResultService` now implements retained manifest lookup and
object-read leases under the separate `ReadResult` permission. The supplied
identity must already be authenticated by the host; this is not a TLS endpoint.
Manifest lookup may return retained immutable evidence after output expiry or
under unsafe UTC, without granting availability. Fresh object reads require
trusted current UTC, the exact committed attempt/index/digest and unexpired
availability. They pin the object under the authority writer transaction and
persist the clock before returning a pending transfer. No read runs an executor.

`ResultRead::start` returns the exact response header; `read_chunk` borrows a
bounded buffer and permits only one outstanding chunk. The host must call
`check_deadline` before scheduling a transport write/FIN, then `sent` for bytes
actually accepted by its bounded transport writer. Disk reads and empty progress
do not renew idle time. `finish` requires verified source EOF and complete send
progress but does not claim client receipt; the receiver independently checks
its complete object and FIN. Drop aborts delivery only. The original manifest,
work revision and attempt remain unchanged, including after corrupt/missing
storage or process death. A new read never retries computation.

Pending and active reads share the payload root's global/per-owner handle limits,
even across service instances. No send buffer is allocated by the service.
The endpoint must separately enforce negotiated connection limits and drive
`maintain(ReadCursor, limit, now)` with batches 1..256 when peers stop progressing.
It closes expired/revoked leases even if callers keep their tokens. Each scan
fixes an upper bound so continuous arrivals cannot starve older leases. Pending time
is included in the lifetime; UTC output expiry does not terminate an already
admitted lease, and later unsafe UTC does not replace its monotonic timer.
Maintenance skips busy I/O without releasing its pin; no registry lock spans
per-read I/O or authorization. The host still owns timers, socket resets and
bounded transport buffers. These integration obligations are not implemented
by the historical network endpoints.

Nineteen result unit tests include negative-first delayed-timer and scan-fairness regressions,
three actual child-process deaths, corrupt/missing object refusals, distinct and
late-withdrawn read permission, expiry/FIN equality, zero objects versus an empty
object, and cancellation versus revocation. A pinned-WAL test fills both ordinary
table-insert and smaller clock-rewrite capacity: new grants refuse, while an
already admitted result completes with unchanged work and journal length.
`tests/v2_result_resources.rs` exercises actual 32 MiB publication and delivery,
eight pending reads and 16 KiB buffers. The focused run measured a 6035-byte Rust
heap increase, largest allocation 464 bytes, unchanged 69632-byte DB and zero WAL
growth, with 7228 KiB process RSS/HWM reported separately. This is a local library
resource gate, not QUIC flow-control coverage or a gRPC performance comparison.

`AuthorityStore::reclaim` drives three-phase retention under trusted UTC: commit
a typed release intent, delete reference/pin-safe files and sync their directory,
then clear logical liveness only after the files and funded output reservation
are gone. Input requires terminal work and closed children. Outputs additionally
require their external interval to end and the direct dependent parent to settle.
Published manifests, work views, receipts and status roots are not deleted by
this API. A held result read or cancelled callback can keep physical and logical
capacity charged after an otherwise eligible intent. Revocation does not prevent
local cleanup; an unsafe or regressed clock does.

`RetentionCursor` bounds inspected jobs and file keys independently to 1..256
per call. Each metadata pass walks descending retained job IDs; the file pass
has a saved upper key. Keep the cursor across calls. This bounds batch memory
and mutations, not total database work: record-credit audits still stream the
retained metadata. `audit_payloads`, rebind, executor startup and fresh input
also stream all jobs and check their input/reservation/manifest files under
the authority writer and root inventory lock. They read bounded headers and
file lengths, not complete payload bodies; actual readers verify body hashes.
Missing required bytes fail closed unless a verified, committed release intent
authorizes their absence. Auditing a missing live file does not repair it.

Crash tests cover both sides of intent and quota commits and interrupted object
and reservation unlink. A pinned-WAL gate exhausts ordinary inserts and smaller
clock writes, then completes four separately timed input/output release writes
without SQL row replacement or page growth. The 32 MiB resource test also runs
batch-one cleanup while a result pin remains held, then after it drops. This API
only reclaims payload resources; session metadata has a separate retirement cut.

`AuthorityStore::retire` inspects one session per call, using descending generation
passes in a `RetirementCursor`. Eligibility requires root closure plus the full
creation-receipt interval, every longer retained work/output promise, and no
live input/output/executor resources. It streams the records and audits the
matched payload root before committing a paired session flag and immutable
1024-byte eligibility record. Missing or inconsistent halves fail closed.
All valid authorized session operations then refuse EXPIRED; denied owners still
receive UNAUTHORIZED. No partially deleted session is a live binding.

Subsequent calls remove at most 1..256 work bundles, nonroot scopes or operations,
one transaction per unit. Jobs and payload references are deleted with their
work. The closed root, session and eligibility record survive until one final
atomic commit; the session quota slot is only then reusable. Authority generation
and per-owner creation high-water marks are never removed or reduced. Known
creation replay stays EXPIRED after retirement; a new creation advances both
its owner sequence and the authority generation. Audits accept missing retired
relationships only under the verified proof, never live jobs or payloads.

Retirement uses ordinary protected SQL, not the fixed-record credits promised
to active work. A pinned WAL can therefore refuse it with LIMIT_EXCEEDED before
or after eligibility commits. Previously committed batches survive; a refused
transaction releases no session capacity. The host must drive
`checkpoint_storage` to truncate committed WAL after bounded readers release
their snapshots. It does not require trusted protocol UTC and does not change
authoritative contents. It uses a nonblocking checkpoint and refuses while a
reader still pins the WAL; releasing a reader alone need not truncate it.
Actual tests cover both refusal states and completion after checkpoint, with
SQL triggers protecting owner and generation history. Root eligibility scans
and record-credit audits remain streaming all-record work, not constant-time
operations or bounded CPU per batch.

Ten actual process deaths cover both sides of eligibility, work-bundle, scope,
operation and final retirement commits. Tests also cover empty/skipped sessions,
longer output/read promises, owner isolation, safe clocks, quota reuse, malformed
proofs and storage format refusal. The 32 MiB resource gate now finishes with
batch-one retirement and verifies the next creation cannot reuse the old identity.
V2 client journals, authenticated endpoints and independent Java remain open.

Record expansion preserves exact typed contents and existing credits, reserves
the larger future WAL write cost before allocating pages, and uses a savepoint
so a failed resize cannot leave an uninitialized record. It can add credits but
cannot reduce existing capacity or credits. Process-crash, full-page/full-WAL,
stale-revision and rollback tests cover this path. These guarantees concern the
work-view record, not the complete job/receipt/clock/closure write set.

No V2 profile is activated until the remaining lifecycle and resource gates pass.
Physical file caps and staging reservations alone do not reserve future
completion space; fixed-record credits cover only their stated write sets.
Durable output reservations cover payload capacity; the worker's separately
charged transient slot covers output I/O. Input/output liveness remains charged
until reference-safe cleanup has actually released those resources;
manifest publication alone does not release those reservations.
Client journals, V2 mTLS/QUIC integration and independent Java V2 implementation
also remain outstanding.
The current contract has no backward-compatibility requirement; historical V1
tests are regression evidence only. See the
[V2 acceptance ledger](../../docs/standards/durable-work-v2-test-plan.md).

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

`close_scope(&digest, parent, parent_depth)` requires the parent's `EntityKey`
and depth from the caller's assembly manifest. This replaces the earlier
single-argument API: a digest identifies the child scope, not its parent.
Every returned parent status is checked against that context; wrong scope,
entity, depth or lifecycle closes with `PIPESTREAM_ENTITY_INVALID`. A reported
FAILED parent is returned intact, either directly or after REHYDRATING, not
converted into successful processing. Invalid local context refuses before
transmission. A cancelled or incomplete closure exchange closes the connection;
it never cancels declared work or supplies a completion barrier. As with recovery,
reconnect after an interrupted exchange. Other client methods do not yet share
this cancellation guard. The caller still owns manifest persistence and the
construction of the digest from actual descendant observations.

Closing a scope whose terminal children fail the parent's completion policy
now durably resolves that parent as FAILED. The server echoes the validated
digest followed by the failed parent's status, without starting a rehydration
callback or inventing an output. Child records remain intact. The closure and
failed resolution commit together; an identical closure replay returns the
same failure after reconnect or restart. Missing or retryable children still
prevent closure. Existing completion reservations cover this resolution at
logical queue/storage capacity and at a pinned-reader WAL ceiling; retired
logical credit does not reclaim the preallocated state image.

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

The client retains one accepted receipt on its connection and refuses unrelated
operations before writing any frames while its outcome remains unconsumed. Passing
a different receipt to `wait_recovery` refuses locally without consuming the queued
response. Consuming the matching terminal frame releases that local obligation;
both successful and refused outcomes allow a subsequent exchange. Legacy redemption
is refused locally on this profile even when no outcome is pending.

If `accept_recovery` or `wait_recovery` is cancelled after polling begins, or fails
during network I/O, its exchange guard closes the connection. A partial frame cannot
be consumed as the next response. Callers reconnect and replay their persisted
request; closing the connection neither cancels an accepted server job nor claims
completion. These APIs still require application-owned request persistence before
transmission. The connection's receipt tracking is not a durable producer journal,
and this guard does not claim cancellation safety for every other client operation.

Real-QUIC tests interrupt receipt and outcome reads before response bytes, inside
the frame header and inside the body. A separate authenticated-service test holds
a resume callback, cancels the client's wait, verifies the job remains unfinished,
then lets it publish and replays its exact receipt/outcome after server restart and
client certificate rotation. Callback and attempt counts remain one. The negative
response tests continue to pin exact wire refusal codes; local wait cancellation
closes normally without presenting it as a malformed request or successful GOAWAY.

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
admitted-job SQLite completion reservations apply as described below. Receipt
reclamation remains unfinished.

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
SQLite state, or the filesystem cache. Explicit orphan reclamation and broader
resource verification remain required before production
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
publication growth, allocated state capacity, and counts. `charged_bytes()` is
the larger of allocated capacity and actual bytes plus protected growth. Padding
remains charged after unused logical growth is released. Record capacity also
stays within the individual record limit; the image header and SQLite overhead
are covered by the physical file policy.

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
The session payload remains version 7 inside a new `PSIMG001` fixed-capacity
image. Storage policy 4 charges allocated capacity; queue policy 3 uses fixed
dispatch images, including preallocated future rehydration rows. The physical
policy is `PSDBL003`. Older images, policies and unaccounted stores are refused
without conversion. Missing or changed owned tables/indexes refuse on reopen
instead of being rebuilt over retained work. No operational database is migrated or
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

These logical reservations fund the fixed-capacity images and WAL budget
described below. They do not reserve filesystem blocks, payload storage or total
process memory. Final-lineage file quota is protected separately. Filesystem
exhaustion or I/O failure can still prevent publication, leaving work unfinished
and charged rather than inventing successful completion.

### Retained payload and lineage reservations

`FileEntityStore::open_with_limits(root, SpoolLimits, RetainedLimits)` configures
both receive and retained stores. `open` and `open_with_spool_limits` read an
existing retained policy or create the default for a new directory. Defaults
reserve at most 512 MiB and 8,192 objects globally, 128 MiB and 2,048 objects per
authority/principal, and 1,024 retained principal buckets. Anonymous work shares
one bucket. Staging is separately capped at 128 MiB and 32 objects; at most 32
object operations may run concurrently under the default policy.

Every payload reserves its entire length plus 512 bytes of checksummed identity
metadata and a 32-byte publication receipt before its files are created.
Pending payload copies additionally reserve their full payload length and one
staging object until staging cleanup finishes.
These conservative sums bound file lengths, not filesystem allocation or RSS;
hardlinked staging/publication names can refer to the same bytes. Temporary
receive spools and SQLite files retain their independent budgets.

Before payload installation can succeed, the session also durably reserves
1,120 bytes and one retained object for final lineage: a 512-byte ownership
marker, 512-byte final metadata, 32-byte digest, 32-byte receipt and 32-byte
publication stage. `FileEntityStore::reserve_lineage` exposes this step for
callers constructing retained work without the payload-ingest path. The marker
commits to session and authority/principal, not a fabricated completion digest.
Final publication uses this prepaid allowance rather than ordinary staging
byte/object credit; it still needs one bounded active storage operation.
The full allowance remains charged after publication. Repeated payloads and
lineage replay do not reserve it again. Admission of additional work cannot
spend it, but it does not preallocate filesystem blocks or prevent `ENOSPC`.

`retained_usage` and `principal_retained_usage` report retained and staging
reservations, including incomplete writes. Matching replay does not charge an
object twice. Crossing an object, byte, principal, staging or directory limit
returns `PIPESTREAM_LIMIT_EXCEEDED`, including through the service's QUIC path.
Previously declared work remains outstanding; no rejected payload becomes an
admitted job or a successful scope. Authority/principal labels come from the
authenticated service or durable job, never untrusted entity metadata.

The 96-byte `.retained-policy` uses `PSRET004`, seven big-endian limits and a
SHA-256 checksum. Each `.meta` uses `PSOBJ001` and commits to session, owner,
entity/scope or lineage identity, length and digest. The `.done` file contains
the metadata checksum. Raw entity and lineage bodies keep their original paths
and bytes. Metadata fsync precedes copying through 8 KiB buffers; verified data
is fsynced before no-replace hardlink publication, and the receipt and directory
are synced before installation succeeds. Session format 7 and the wire/CDDL
are unchanged. The `lineage.reserve` marker uses the separate `PSLIN001`
encoding and is synced before payload installation. Old `PSRET001` through `PSRET003` policies,
nonempty roots without a policy, and payloads missing a matching reservation
are refused, not converted. Preserve them with their matching binary.

Reopen performs a bounded inventory. An interrupted copy resumes only from a
matching stored prefix; an incomplete receipt is not loadable until verified
publication finishes. Payload metadata prefixes shorter than 512 bytes, with no
payload files yet, reserve 512 global bytes and one global object until matching replay
establishes their owner and full charge. `incomplete_metadata` reports this
unattributed count. Empty canonical directories left before metadata creation
stay present and consume `directories` credit, bounded by twice the object cap.
Directory counts are global only. The root, spool directory, policy, identity,
database claim and empty lock file are separate fixed overhead. Neither reopen nor refusal deletes
admitted objects or silently discards surviving artifacts.

Partial lineage markers retain their entire 1,120-byte global charge and object
slot until matching replay establishes a durable owner. `lineage_reservations`
includes these allowances; `incomplete_lineage_reservations` identifies the
unattributed subset. Failed owner-quota checks preserve that credit. Partial final
lineage metadata and publication receipts use the original allowance, including
when every byte/object slot is occupied. A missing or corrupt previously durable
marker fails loading or publication; it cannot become fresh admission capacity.
Final-lineage reservations left by failed payload installation stay charged,
including after orphan reconciliation. Reservation alone does not admit a protocol job.

The root has an exclusive Unix advisory lock using the existing pinned
[rustix file-lock API](https://docs.rs/rustix/latest/rustix/fs/fn.flock.html).
Same-process handles share accounting, and payload readers, spool loans and
in-flight operations retain root ownership. A second cooperating writer process
is refused. Reopening while the last local handle is being destroyed uses a
five-second condition-variable wait budget for that handle's lock release.
This is not a bound on filesystem operations or mutex acquisition.
The registry keeps its entry
until unlock, so a zero Arc strong count cannot falsely imply available ownership.
Simultaneous reopeners share the new accounting state; other roots can progress
while the old finalizer is paused. A live same-process maintenance owner or an
external owner is still refused, not waited out.
Use a private directory; external filesystem mutation is outside
this boundary. Symlinks, foreign files, noncanonical directories, unexpected
hardlinks, policy changes and corrupt complete metadata are refused rather than
repaired. The only permitted two-name alias is the matching payload/stage pair.
Process-exit and prefix-image tests are not proof against every torn-sector or
power-loss failure. Unsupported platforms have no unbounded fallback.

Retained-store tests cover quotas, lineage accounting, replay, owner checks,
interrupted copies, metadata/receipt prefixes, empty directories, alias refusal, blocked readers
and cross-process lock retention. A real-QUIC test exhausts one principal's
payload allowance, verifies unchanged declared membership, and lets a different
principal complete independent work. Further tests exercise partial reservation,
metadata and receipt writes, abrupt process exit after payload installation,
exact-quota publication and concurrent owner limits. Authenticated QUIC tests
hold admitted callbacks while two principals fill the complete retained budget,
then verify real lineage bytes, checkpoint ACKs and GOAWAY. A missing declared
payload instead times out without a final lineage or successful checkpoint.
Explicit orphan reconciliation is described below; broader tenant stress remains
unfinished.

### Database and retained-root pairing

`RecursiveService::with_limits` and `new` now invoke the required
`EntityStore::bind_session_store` method before returning a service. A custom
backend must implement durable ownership itself; there is no default no-op or
fallback based on a directory path or protocol session label. The file backend
permits one persistent database/root pair, including across restart. Multiple
service handles can share the same pair; a different database or retained root
is refused before admission or dispatch.

The database creates its identity atomically with its root schema. A strict
singleton table retains a fixed 72-byte `PSRBND01` image: database identity,
initially absent payload identity, and SHA-256. `SqliteSessionStore::payload_binding`
reads it; `bind_payload_store` performs the once-only database half under the
writer lock. Every connection checks the root schema and original database
identity before exposing the handle. Malformed, missing or oversized binding
images cannot become an unbound store or be regenerated on reopen.

The retained root has a separate 56-byte `.retained-identity` (`PSRID001`) and
an optional 72-byte `.session-store` claim containing the same complete pair.
The file backend syncs its immutable claim and directory before the database
claim. A complete file claim can replay after a failed transaction or process
exit, without assigning a new identity or admitting work. Partial/corrupt claims
refuse without repair. A bound database with a missing file claim cannot recreate
that claim. Same-process root handles serialize binding and the existing advisory
lock excludes a second cooperating writer process.

Binding is an ordinary metadata write. It derives unchanged completion
reservations under SQLite's writer lock and cannot spend admitted jobs' protected
WAL/shared-memory credit. Exact binding replay does not rewrite the row. These
local ownership records are not authentication, payload-admission evidence,
complete input-integrity audits or an orphan-cleanup API.

`PSDBL003` and the current `PSRET004` refuse earlier policies without conversion,
so an older binary cannot silently ignore pairing or reclaimed commitments.
Session payload format 7, queue/storage
policy, wire/CDDL and the physical completion-cost derivation are unchanged.
Back up and restore the database, payload root and their policy/identity/claim
files as one matched set. Cloned identities do not authorize independent writers
to the same live root. No operational migration or automatic cleanup is supplied.

Tests cover both mismatch directions, concurrent roots and connections, a held
SQLite writer, failed binding BLOB writes, process exit after the file claim,
old policies, corrupt/missing identities and claims, and completion after WAL
saturation. The authenticated recovery QUIC test pins the same pair while replaying
an unobserved receipt after restart. Pairing is a prerequisite to the maintenance
API below, not completion of the broader resource/interoperability goal.

### Explicit offline orphan reconciliation

`FileEntityStore::reconcile(root, spool_limits, &sessions)` opens a previously
paired root exclusively and acquires SQLite's writer lock. Close all file-store
handles, retained readers, receive loans and services first; a live owner refuses
maintenance instead of racing it. The public core `payload_maintenance` guard
provides the checked database snapshot and a one-session-at-a-time audit cursor.
It blocks admission and publication until dropped, even after iteration finishes.
This is an offline storage operation, not an application-callback context.
Standalone spool handles inside an initialized retained root also retain its
process lock, including when opened without a `FileEntityStore` handle.

Before deleting anything, reconciliation verifies the complete retained inventory,
body checksums and every managed PROCESS input, including finished/refused jobs,
revoked sessions and waiting parents. Caller-managed admission without an original
PROCESS descriptor refuses. Wrong/unbound pairs, missing admitted bodies, corrupt
records, unrecognized spool names, symlinks and foreign hardlinks also refuse.
Supply the receive-file bounds previously used with the root; spool limits are
not a separate persisted policy. Directory traversal and metadata inventory are
bounded, payload verification uses 8 KiB buffers, and database decoding remains
proportional to its configured maximum session record.
An unpublished stage can contain a rejected full-length copy with a bad checksum.
It is not admitted input: cleanup removes those bytes while preserving the expected
digest. A corrupt published body still refuses the entire audit before deletion.

An unadmitted payload's `.meta` is renamed to `.commit` and synced before its
stage, receipt or body is removed. The original 512-byte `PSOBJ001` identity,
owner, length and SHA-256 survive. The commitment is not an executable payload or
admission evidence. Matching retransmission reserves the original body/receipt
and staging allowance, then renames that same record back to `.meta` before
installation. It needs no duplicate metadata allocation at an otherwise full
quota. Changed bytes refuse with `PIPESTREAM_ENTITY_INVALID`; insufficient
restoration capacity refuses with `PIPESTREAM_LIMIT_EXCEEDED`.

Interrupted maintenance is replayed explicitly. Remaining body/receipt/stage file
lengths stay charged after reopen; restoration refuses until an interrupted
cleanup has removed them. A crash after restoration's rename instead leaves a
fully reserved pending installation. Neither SQLite rollback nor process exit
is assumed to undo file operations. Missing work stays pending and the database
state, checkpoint identity and completion markers are never changed by cleanup.

`Reconciliation` reports before/after reservations, counts and removed file-name
lengths, not allocated disk blocks. Admitted bodies in every state, all immutable
commitments and their object/owner slots, partial metadata, directories and
final-lineage allowances remain retained. This is not session expiry or general
garbage collection. `PSRET004` refuses prior policies without conversion; no
wire/CDDL, session database layout or completion-cost formula changes.

Tests cover full quota, concurrent and interrupted restoration, matching and
changed replay, audit refusals, live-handle/reader/spool exclusion, writer locking,
injected I/O failure and process exit at four filesystem phases. A real-QUIC
sealed scenario checks missing-input timeout, changed-input refusal, and matching
out-of-order chunks through checkpoint ACK and GOAWAY. The isolated resource test
also installs, reclaims and restores a 32 MiB body with less than 1 MiB additional
Rust-managed heap and no allocation reaching 1 MiB. That gate does not measure
SQLite native allocation, whole-process RSS or multi-tenant throughput.

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

Held readers can prevent WAL reset. Ordinary writes refuse when they would spend
protected completion credit; admitted execution stages can consume their own
reservation as described below. There is no automatic eviction of admitted work.
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
file lengths, not filesystem allocation, snapshots, native memory or payloads.
Completion-space enforcement is an additional layer below. These limits do not
lift the service's single-writer-
process restriction for other resource accounting. Java JDBC has its own independent
file-length guard, described in the Java implementation README.

Eleven core tests exercise main-page, WAL, rollback-journal and actual shared-memory
exhaustion, growth-control bypass attempts, immutable/corrupt policies, aliases,
transaction rollback, held-reader checkpoint refusal, abrupt-exit recovery,
and concurrent connection/sidecar churn.
A real-QUIC test verifies the named refusal, preserved declaration and replay
after checkpointing. These checks are not a throughput benchmark.

### Physical SQLite completion reservations

Admission allocates a session image containing a 104-byte checksummed header,
the serialized state, and zero padding for its protected logical growth. The
header binds identity, version, revision, timestamp, capacity and state checksum.
Reads validate the bounded header before allocating state and check padding in
8 KiB pieces. This padding is storage capacity, never a fabricated result.
Dispatch and accounting use fixed 32- and 56-byte images. Updates within capacity
use SQLite's [incremental BLOB API](https://www.sqlite.org/c3ref/blob_open.html),
without replacing SQL rows or allocating new B-tree pages. New admission or
capacity growth remains an ordinary, quota-checked write.

Each queued job reserves acquisition and publication; a running job retains
publication credit. Possible rehydration reserves conversion, acquisition and
publication. Lease renewal does not release publication credit, so repeated
expiry cannot spend another job's allowance. Enlarging a session must also fund
the increased write cost of its existing jobs. Terminal outcomes remain retained;
unused logical credit does not shrink the allocated image.

Before its first write, each mutation holds SQLite's actual writer transaction
and derives all remaining reservations from retained state. A per-connection VFS
ceiling subtracts that reserve from usable WAL capacity. The main and WAL handles
share the same ceiling through commit or rollback, without a process-global
ceiling race. Usable capacity also accounts for the configured WAL-index
shared-memory limit. Reopen re-derives credit; it does not depend on a surviving
in-memory counter or recreate indexes.

The bound is pinned to bundled SQLite 3.53.2, zero page-reserved bytes, page sizes
512 through 65,536, and sectors no larger than 64 KiB. Each stage funds the whole
allocated session image, up to two dispatch images and one accounting image,
plus WAL frame headers, a possible commit-frame repeat and sector padding.
Unsupported geometry or an unfunded reservation refuses explicitly. This is a
configured file-length guarantee for cooperating writers, not preallocation of
filesystem blocks, immunity to I/O failure, or capacity for unknown descendants
and unrelated checkpoint requests. Ordinary writes still need unreserved space.

Tests pin readers while unrelated writes saturate the WAL, then acquire/publish
queued jobs for two principals, convert and finish rehydration, and resume an
authenticated claim with retained receipt replay. They cover concurrent admission,
lease renewal, reopen and abrupt process exit. A real authenticated QUIC test
publishes a full-budget token after saturation while the reader remains pinned.
The whole-transaction cost matrix uses a two-page cache at 512-, 4,096- and
65,536-byte pages, token budgets across varint/page boundaries through 8 MiB,
complete/refused/deferred outcomes, and a fixed main-page cap. Every measured
acquisition/publication stays within its production stage bound without changing
row identities or image capacity. Corruption tests refuse malformed dispatch,
oversized metadata and altered schemas without converting them into free capacity.

The cost is deliberately conservative: large sessions multiplied by many pending
stages may exhaust the default WAL reservation budget before logical queue limits.
The engine still serializes whole sessions, audits retained state and scans
retired dispatch rows. These are not constant-time or throughput guarantees.

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
completion queue can be larger. Future and retired rows are never runnable.
Completed/refused rows are retained, so discovery scans can grow with retained
history even when few jobs are unfinished. Fixed-field shape checks precede
discovery/count queries; malformed flags cannot silently hide work.
An unexpired attempt is
not returned; lease expiry makes it discoverable again but does not grant
execution. `acquire_job` still checks authorization and the durable fence.
`integrity_check` audits queue rows against checksummed session records in one
read snapshot, including missing and extra entries. This full audit scans one
session at a time, not an in-memory list of all sessions. Store mutations also
audit these rows against retained session state before counting free capacity,
so missing or altered reservations cannot admit unrelated work. These bounded
scans and discovery ordering are not constant-time or large-store throughput claims.

Queue reconciliation preserves unchanged rows and updates changed images in
place. Future rehydration rows are allocated with processing admission and become
active or retired in place; finished/refused job rows are retired, not deleted.
Retired rows do not consume unfinished-job quota. Limits are checked against
other sessions plus the desired state, without temporarily deleting charges.
The pre-write integrity audit remains: corrupt or missing entries cannot become
admission credit. State, revision, index changes and accounting commit or roll
back together under the new queue/storage policies described above.

Six index-delta tests check row identity across no-op saves, acquisition,
publication, revocation and reopen; replacement at full quota; and failures during
new insertion or accounting after in-place retirement. Fault tests use actual
SQLite refusal of writes to an indexed BLOB column; SQL UPDATE/DELETE triggers
do not intercept incremental BLOB I/O. An isolated index-only WAL test with 1,
128 and 512 jobs still requires zero bytes for unchanged reconciliation. This
comparison excludes the session update; whole-transaction funding is tested
separately above. No service-throughput improvement is inferred.

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

`RecursiveClient::send_entity` and `send_chunked_entity` retain at most 128 STATUS
frames and 4 MiB of aggregate encoded STATUS data per operation. Both budgets
include the terminal frame, extension bytes, and five-byte UCF headers. Exact
boundaries succeed without clipping status history or yield tokens. Exhaustion
closes the connection with `PIPESTREAM_LIMIT_EXCEEDED` (0x06) and returns an error,
not partial success. A full count budget without a terminal state refuses without
waiting for another frame. These are fixed local client limits, not negotiated
wire limits or evidence of remote cancellation/completion. The incoming frame
buffer and temporary decoding allocations are separate from retained history;
this is not a 4 MiB bound on total process memory.

`RecursiveClient::disconnect` requests transport close without waiting. Before
stopping the client's async runtime, use `disconnect_gracefully().await`; it
keeps the endpoint alive through QUIC shutdown. The `begin-yield` CLI now uses
this path. An isolated-runtime regression test verifies that the server exits
without waiting for its idle timeout and that the claim remains unredeemed and
the entity DEFERRED. Transport shutdown is not a work-completion barrier.

Without the explicit mutual-TLS settings, the standalone prototype authenticates
only the server and remains suitable solely for trusted local demonstrations.
Even with mutual TLS, retained recovery, bounded workers and retained-storage
quotas, admitted-job completion reservations and offline orphan reconciliation,
the complete resilience capability and resource/interoperability matrix remain
unfinished. It MUST NOT yet be described as a production multi-tenant durable
work service. Its Layer 2 boolean still advertises more than the tested subset.

The core `uri` module parses typed `pipestream://` session, entity, and claim
locators with explicit ports. Parsing grants no access and does not perform
network I/O. The scheme is proposed, not registered by this repository.
