# Implementation Status

**RFC Editor Note:** Please remove this entire appendix, and the reference to
{{RFC7942}}, before publication.

This appendix records the status of known implementations of the
protocol defined by this specification at the time of posting of this
Internet-Draft, following the process described in {{RFC7942}}. The
description of implementations in this appendix is intended to assist
the IETF in its decision processes in progressing drafts to RFCs.
Please note that the listing of any individual implementation here does
not imply endorsement by the IETF. Furthermore, no effort has been
spent to verify the information presented here that was supplied by
IETF contributors. This is not intended as, and must not be construed
to be, a catalog of available implementations or their features.
Readers are advised to note that other implementations may exist.

The standalone implementations below implement documented version-1 subsets.
The Rust library additionally supplies version-2 Core and authenticated durable
listeners, described below; Java V2 and a complete V2 command-line pair remain
unfinished. Independent
Rust abstract models explore durable attempts/results/retention and sealed
scope closure. A third bounded model composes a branch and leaf with attempts,
worker epochs, ancestor cancellation, output read/dependency pins and closure.
These are design evidence, not wire interoperability, real storage
crash-consistency evidence or an unbounded composition proof.

As of 2026-09-06, a separate Rust transport-independent version-2 module
implements typed messages and records, canonical/cross-field validation,
negotiation checks, domain-separated commitments, bounded client correlation
and incremental payload verification. Library tests consume the frozen
70 wire examples and 12 commitments, including semantic refusals. Neither
the Rust nor Java endpoint advertised these profiles at that checkpoint. Those library tests
are not version-2 mutual-TLS, durable execution, crash recovery, cross-language
interoperability or measured resource-conformance evidence.

A separate independent Java version-2 library now supplies immutable typed
messages/records, schema-directed deterministic CBOR, incremental control framing,
profile-selection checks and domain-separated commitments. It consumes the same
70 frozen wire cases and constructs typed inputs for all 12 frozen commitments;
it does not import Rust implementation code or use a JSON conversion layer.
Tests check fragmented controls, noncanonical encoding, UTF-8/integer limits,
bounded ignored frames, immutable values and contradictory record fields.
Its constant-memory seal and bounded status fold are tested over 4,000,003 members
under a 24 MiB Java heap cap; process RSS/HWM is measured separately and is not
bounded by that heap setting. These are local library tests, not Java V2
authentication, durable recovery, result transport or cross-language conformance.
The Java endpoint does not advertise either V2 durable profile.

The Java library also now supplies bounded caller-side request correlation and
incremental object verification. Local tests exercise every response family,
out-of-order replies and input tags, duplicate/wrong-direction responses,
delivery-local result commitment failures and separate transfer permits. Object
tests enforce header bounds, exact length/digest/FIN, independent idle/lifetime
deadlines, reset behavior and deadline equality. An isolated 24 MiB-heap JVM
verifies a 64 MiB object through an 8 KiB buffer against an independently checked
digest. This is not network flow-control, persistence, authentication, total-process
memory conformance or cross-language V2 evidence. Timer scheduling, QUIC stream
ownership and authenticated durable endpoint integration remain unfinished.

Java now also has a real Netty QUIC/TLS authentication guard for `pipestream/2`.
Loopback tests exercise certificate trust/usage/validity, service DNS/IP names,
stable DER-leaf mapping, rotation, missing/unmapped callers, optional/required
profile policy, live credential expiry and actual TLS resumption. Every resumed
server connection rechecks current peer credentials and mapping before application
activation. The guard delays Netty's early application-activation event. Tests
exposed reentrant closure that could suppress the native TLS alert; it preserves
the native error path. It also resolves readiness when an unsuccessful connection
unregisters without becoming active. Configured trust/mapping and peer verification
limits do not establish native handshake or whole-process memory bounds. These
are Java TLS-boundary tests, not a complete Java V2 durable endpoint, independent
cross-language failure evidence or the original workload/gRPC comparison.
Section 12 explicitly withholds protocol processing until resumed credential
revalidation succeeds; this clarification changes no wire fields.

The independent Java library now also supplies a Core-only Netty V2 listener.
Thirteen actual-network scenarios cover capability selection, all profile-dependent
request refusals, strict shared request IDs, malformed framing/direction, detach,
half-close, reset/stop, tiny receive windows and paused readers. The server leaves
graceful connection close to the client after sending its control FIN; local
Netty write completion is not treated as acknowledgment. Live timers bound
handshake, control framing/idle, oldest queued response and absolute detach waits.
Tests check that later requests cannot renew older blocked writes or detach.
Connection admission counts incomplete handshakes and mapped-owner/anonymous
buckets. An excess connection is refused after its Initial packet is processed,
with transport CONNECTION_REFUSED and at most one extra packet-local transport;
repeated refusals preserve existing connections and release native state before
reusing admission capacity. Queued application counts/bytes and aggregate configured
buffer allowances are bounded; these are not native-memory or total-RSS measurements.
Core has no data streams, so its tests do not prove the shared control/data credit
reservation required by durable object transport. Java durable storage/execution,
results, client recovery, independent cross-language failure/resource evidence and
the external workload/equivalent streaming-gRPC comparison remain incomplete.
Neither durable profile is advertised by this listener.

The Java Core client now owns authenticated negotiation and bounded detach on
Stream 0. It checks capability selection, response correlation and actual peer
FIN before reporting successful drain; transport close alone cannot supply that
result. Repeated detach calls share one operation, and cancelling an application
waiter cannot cancel that operation. Independent timers bound stalled negotiation,
framing, writes and detach, while process-local count and configured application
buffer quotas remain charged through owned transport termination. These are not
measurements of native memory or evidence of durable-profile implementation.

The Java reference and external Java example use a source-pinned extension of
Netty 4.2.17.Final's maintained QUIC modules and the aligned dependency BOM,
replacing the archived incubator module. Exact source revisions, patches and the
native Rust dependency lock are checked in; source repositories remain separate.
The build verifies native transport tests and installs uniquely identified
artifacts into an isolated Maven repository. Runtime tests check the loaded
classes/native artifact identity and patch revisions, and reject duplicate
official QUIC resources. This dependency integration does not establish the shared
control/data reservation. Review of the bundled transport found that initial
receive credit differs from the replenishment window and that receive windows
autotune. Section 12.1 now explicitly includes those changing windows and
transport-owned send buffering in the reservation requirement. Java's Core-only
endpoints still accept no data streams; independent durable object transport
and its stalled-data/resource evidence remain required.

The Java connection-confined stream owner now binds native reservation to
Stream 0 and bounds object stream slots, copied in-flight chunks, stream creation
history and local open/write/FIN waits. Authenticated transport fixtures check
control progress beside stalled data in both directions, exact incremental bytes
and FIN, explicit slot settlement, reset and a named write deadline. They do not
advertise a durable profile or establish all receive-credit orderings, native
ACK-withholding behavior or whole-process memory bounds. Those tests also exposed
a native stream-retirement bug: payload acknowledgments could cause a later empty
FIN to be lost when the application consumed the peer's FIN first. The pinned
dependency now distinguishes FIN acknowledgment from payload acknowledgment;
a deterministic packet-level regression reproduces that order. This is an
underlying QUIC correction, not a PipeStream wire change. Independent Java durable
storage, execution, results, recovery and the original cross-language failure and
workload-comparison deliverables remain open.

A subsequent packet-level test exposed another gap: independently delivered
MAX_STREAM_DATA could exhaust connection credit while Stream 0 retained stream
credit. The Java transport extension now has an opt-in policy pairing each
stream-credit or stream-count update with sufficient MAX_DATA in the same packet,
deferring an update that cannot fit as a pair. The Java V2 owner enables it.
The regression checks actual protected packets with a constrained send buffer,
then requires progress with adequate packet space. Actual-loss tests also found
that MAX_STREAMS updates were not rescheduled after loss. The correction retains
per-direction pending credit; tests require repeated paired limits and usable
replacement streams for both stream directions. This does not establish the
corresponding packet-loss and reordering guarantees for the independent Rust
transport or complete either endpoint's durable-profile acceptance gates.

The independent Java V2 storage layer now implements atomic session creation,
owner-sequence queries and immutable attachment/replay, including allocation of
the empty root scope. It distinguishes first installation from recovery and
refuses missing recovery storage instead of resetting the authority's counters.
It uses the Java bounded SQLite file facility without converting V1 session
schemas. Caller declarations now atomically retain ordered members, operation
receipts and streamed seals; bounded pages and immediate work snapshots are
available. Real SQLite tests cover replay, concurrent mutations, crash boundaries,
capacity refusal and contradictory receipt/member recovery. Java V2 storage
preallocates fixed-capacity scope, work, fence and shared-clock images with
persistent rewrite credits and guarded WAL/shared-memory headroom. An image
credit covers that image and one clock write, not arbitrary SQL or a whole job.
Earlier local formats are refused without a wire change. Independent Java V2
input storage also implements bounded immutable reception, exact FIN/digest
verification and crash-safe file installation. Current database format 5 and
input policy format 4 retain immutable two-way installation binding and atomic
storage admission: verified input, attempt 1, branch child identity, restartable
job, replay receipt, funded output allowances and metadata, and the UTC watermark.
Ordinary input reception cannot consume the retained output allowances. Tests
cover real process death before admission commit and after receipt return,
funding-installation interruptions, header replay and policy/capacity refusal.
Recovery cross-checks job/member/receipt coverage and parent/child metadata.
A regression also preserves caller child obligations after a parent's deadline;
that failure is not implicit subtree cancellation. Local worker leases now
support durable claim/renewal and expired-lease replacement without changing the
wire attempt. Failure and retryable outcomes commit with their job charges;
owner-independent deadline maintenance does not require the caller to reconnect.
Claims preserve funded settlement writes, and failure retains payload allowances
for later dependency-safe reclamation. Parent rehydration cannot rely on unchecked
closure counters. Output bytes now stream into immutable storage under the funded
allowances. Fenced success publication verifies the exact object set and atomically
commits its manifest and terminal work/job state; paired recovery verifies actual
published bytes. Unpublished installed files remain charged orphans. This layer is
not yet a durable-profile listener. A bounded Java runner now invokes actual
registered callbacks outside metadata transactions and publishes their real outputs.
It rechecks current execution fences and can reclaim unpinned, strictly older
unpublished output slots under a durable replacement claim, without refunding
their funding. Background discovery now revisits committed jobs in finite bounded
pages and dispatches leaf and caller-expanded callbacks without a connection or
volatile job queue. Waiting parents occupy no physical worker; discovery uses an
advisory child-summary readiness hint and the claim fully verifies STRICT closure.
Global/per-owner physical limits and independent deadline maintenance apply;
shutdown does not claim logical cancellation or forced callback termination.
The Java scheduler also drives incremental closure folds over actual sealed
membership and terminal outcomes. Complete child/root summaries and any required
STRICT parent failure commit using reserved image writes; prior outcomes and
cancellation fences retain precedence. Partial folds are volatile, not durable
completion evidence. Direct-member pages are bounded, but verification of existing
descendant summaries still performs a session-wide streaming audit. The local
snapshot API checks authorization and the expected seal; it does not implement
wire checkpoint waits or completed-session shutdown.
Caller-expanded callbacks now page exact direct children and stream committed
outputs through a reserved sequential reader. Current parent execution authority
is checked on each I/O operation, including internal reads after external output
expiry. Unfinished readers and swallowed interface refusals prevent success.
Missing or corrupt retained bytes are storage failures, not computed outcomes.
Metadata verification still audits the session and output verification hashes
actual payloads; bounded buffers are not a constant-time or zero-copy claim.
Local producer-1 declaration and input-admission APIs now recheck current parent
ownership, deadline and authorization through commitment, even on replay. Their
operation journal and recovery audit distinguish the caller and authority
namespaces. Membership sealing still cannot complete authority expansion.
Java now runs a separate authority-expansion callback, admits actual child inputs,
and resumes original operations under replacement leases without changing the wire
attempt. Expansion completion requires sealed membership and admitted or already
terminal obligations; unfinished receivers and non-capacity interface failures
prevent completion. Phase-specific credits release producer resources before
waiting for child closure and reacquire reassembly resources under a new lease.
Bounded dispatch cursors and independent deadline scanning prevent an occupied
worker from holding up maintenance. This remains local authority behavior.
Explicit retry,
broader orphan/subtree reconciliation, result-read pins and transport,
retirement and full cross-language failure/resource gates remain required.

The Rust expansion-completion path still needs the same explicit check that sealed
but unadmitted, nonterminal child obligations cannot be left behind. Existing Rust
tests and implementation coverage do not establish that invariant yet.

As of 2026-09-07, the Rust authority library also implements transactional
admission and replay, fenced worker execution, both branch producers and real
child-output reassembly, cancellation/closure, retained result reads,
dependency-aware payload reclamation and crash-safe session retirement.
Local storage tests include process death, pinned journals, retained read
handles and a 32 MiB result-resource case. Rust authority storage format 11 adds
checksummed operation evidence, bounded original declaration intent and deferred
declaration-member links. Replay verifies exact membership; recovery streams
scope members to reconcile batch counts, seals and session charges. Prior local
formats are refused without changing the wire mapping. These local checksums do
not authenticate data against an operator able to rewrite both data and checksums.
A separate V2 TLS boundary now tests
real QUIC handshakes, certificate mapping/rotation, live credential validity,
server identity, clock failure and disabled resumption. It does not advertise
the durable profiles or supply the complete V2 application dispatcher.
Independent Java V2, cross-language V2 failures and the equivalent streaming-gRPC
workload comparison remain unfinished. Local library and TLS tests do not
establish those missing interoperability or usefulness claims.

The Rust library now also supplies a Core-only version-2 QUIC server, with
bounded concurrent connections, owner/anonymous quotas, capability selection,
control framing, correlated refusals and connection detach. Fifteen Core
tests cover configuration and real QUIC paths, including a 128 KiB ignored frame crossing a
64 KiB flow-control window and a non-reading peer. Twenty TLS tests include
owned authentication-configuration selection. Neither durable profile is
advertised by this Core server. The standalone commands remain version 1;
independent Java V2, complete V2 command-line endpoints, process-level resource evidence
and the workload comparison remain open.

A separate Rust durable control adapter now connects authenticated peer identity
to the persistent authority APIs. Twelve local dispatcher tests cover single
session binding, replay, revision/checkpoint waits, cancellation/retry, actual
stored result reads and connection drain accounting. They use real TLS peers
but do not send those durable controls or objects over QUIC. In-flight database
jobs retain their connection and metadata slots after an async waiter is
cancelled. The adapter is now used by the separate durable listener below, not
by the intentionally Core-only listener.

The Rust input adapter now receives actual QUIC object streams through bounded
file workers into the durable authority. Nine input tests cover incremental
reception across smaller flow-control windows, replay, empty and malformed
inputs, owner/connection checks, idle/lifetime expiry and cancelled or blocked
file work.
A worker test checks that asynchronous cancellation cannot release a file's
resource pin before off-executor cleanup. The accompanying control calls remain
local adapter calls. Client recovery journals, independent Java V2 and full
workload/resource evidence remain unfinished. The input adapter does not
advertise profiles by itself.

A separate Rust result adapter now streams actual retained outputs over QUIC.
Ten tests cover empty and larger-than-window objects, repeated reads without
execution, stopped/slow receivers, pending stream creation, live credentials,
corrupt bytes, global/owner/connection quotas and cancellation during file work.
Before every nonblocking write poll it rechecks authorization and deadlines;
post-header failures reset only that stream. The control calls in these tests
remain local. These adapters do not activate a durable profile or establish
complete V2 endpoints, Java interoperability or workload/resource conformance.

The Rust result adapter now shares connection-owned send admission with control
writers. Nine flow tests cover local send reservation, stalled receive windows,
batched credit updates, retry wakes, stream replacement and transport role limits. Two more
result tests check real stored-output/control progress and wrong-connection
ownership refusal. Testing exposed a deadlock when stream credit was replenished
before connection credit; the implementation now budgets update headroom and
Section 12.1 explicitly requires preserving the reservation across updates.
This is adapter evidence, not complete durable-endpoint interoperability.
These tests also do not establish the reservation under selectively lost or
reordered connection-credit packets; that transport-level evidence remains
required independently of the Java transport correction above.

The Rust authority runtime now independently drives execution, read expiry,
retention and retirement. Three integration tests cover discovery of prior
admissions, copy execution, read expiry during blocked execution, safe-clock
and read-pin retirement gates, and configuration/ownership refusal. A unit test
checks maintenance failure classification. Three execution regressions cover
retained profile selection, nonblocking stop and partial thread-pool startup.
This runtime is used by the durable listener; it alone advertises no profile.

The Rust library now includes an authenticated durable-work/result-delivery
QUIC listener with the actual authority, file workers and runtime. Fifteen
black-box tests exercise real control/input/result streams, exact root completion,
repeated reads, blocked data/control independence, full 30-second work waits,
credential rotation/owner quotas, a non-reading control peer, malformed/refused
requests, exclusive storage reopen and
shutdown with an unfinished metadata commit. Two unit tests cover configuration
limits and child-task destruction accounting. These tests use the existing Rust
wire codec, not the still-required independent failure driver's acceptance oracle.

Half-close testing exposed lost control replies in both Rust V2 listeners:
immediate QUIC close could discard queued responses. Both now finish control
and await its transport acknowledgment before graceful close. Section 12.8
clarifies that this does not prove peer application validation or persistence.
Independent Java V2, client file-staging recovery, neutral cross-language
process failures, whole-process resource gates and the original external
workload/equivalent streaming-gRPC comparison remain unfinished.

The Rust client library now has a bounded SQLite creation/intent journal for
the six mutation kinds. It validates immutable binding and receipt commitments,
retains uncertainty after local failures and refuses changed history on reopen.
Thirteen substantive storage tests and a subprocess entry point include forced
termination after commit and actual physical exhaustion. A negative control
removing the intent commit fails recovery. Incompatible reopen now refuses
before changing SQLite journal mode, with a reproduced regression. Two real-QUIC
tests replay replies
not recorded by the client, retaining original session/work/attempt identities.
The result test now also reopens a durably retained full manifest and explicit
output index, authenticates with rotated owner credentials and retrieves the
original attempt's bytes without changing its terminal revision. The journal
validates revisioned work observations and immutable manifests against retained
admission, retry, fence and policy commitments in either reply order. Additional
tests exercise conflicting records, atomic rollback, corruption, independent
inventory limits, large-manifest physical exhaustion and forced termination after
committing the observation/reference.

The journal also retains bounded scope pages, verifies complete membership seals
and recomputes status roots from terminal work and previously verified child
coverage. Valid child-first metadata is allowed; parent membership and immutable
child allocations are checked in either observation order. Regression tests
reproduced contradictions accepted through both a later parent page and a later
sealing receipt; both now refuse before committing contradictory evidence.
Additional tests cover pagination, missing descendants, STRICT failure, changed
counts/hash/times, quotas, corruption, rollback and forced process death after
coverage commit. The actual-QUIC recovery test now reopens saved root coverage
and completes DRAIN with its exact summary. This client policy requires complete
local terminal evidence, with additional storage/read cost not yet measured in
the workload comparison; it is not an extra universal wire requirement.

Client local storage format 3 refuses older history without automatic conversion
or deletion; authority storage is unchanged. The pinned empty database measures
73,728 bytes; physical-exhaustion tests now use 128 KiB database/WAL/journal caps
and a 64 KiB shared-memory cap, not a process-memory claim.
These are not independent cross-language failure evidence or measured
whole-process resource bounds. The durable facade described below now integrates
these APIs. Section 12 explicitly requires parent/child consistency
checks regardless of observation order; no wire-format change was needed.

The Rust async journal owner now runs initialization, audits, journal operations
and store destruction on one bounded worker per journal. Its configurable call
ceiling includes queued/running operations and replies not yet consumed. Cancelling
a waiter does not cancel a commit or refund its capacity early. A stable empty
advisory-lock sidecar excludes a second cooperating owner across processes; a
forced-process-exit test verifies ownership release and retention of committed
intent. Nine substantive worker tests also cover bounded queues/replies, shutdown,
last-handle drop, panic, invalid history and sidecar safety. An isolated resource
test opens 64 real journal owners, refuses another and restores capacity after
shutdown; their empty database files total 4,718,592 bytes on the pinned build.
The two QUIC recovery tests now use the async journal API. They do not establish a complete production
client, independent V2 interoperability or measured whole-process resource bounds.

The Rust public V2 client wire transport now owns authenticated connections,
correlated control I/O and incremental input/result streams. A real-server test
composes it with the async journal for a 256 KiB object, admission replay, rotated
credentials, retained output and exact root completion. Additional wire tests
exercise reordered/cancelled waits, abandoned inputs, malformed control/selection,
wrong commitments, corrupt/truncated/extra bytes, slow consumers and pending
deadlines. An isolated gate opens 64 actual Core connections, refuses a 65th and
admits a replacement after draining. These are count/behavior checks, not measured
whole-process memory or independent V2 interoperability. Client staging recovery,
Java V2 and workload comparison remain incomplete. Section
12 clarifies that an identifiable result's wrong commitment fails that delivery;
invalid correlation remains fatal. The wire encoding is unchanged.

The Rust durable session client now composes the journal and transport. Owned
collectors persist original intent before transmission and validate/save receipts,
work views, manifests, selections and scope coverage before returning success.
Cancelling a creation/mutation/upload waiter does not cancel that collector or
allocate another identity. Completion/detach barriers first drain accepted client
operations, then request the authority's actual cut. Real authenticated-server
tests exercise a 256 KiB round trip across reopen and credential rotation, late
receipt persistence, missing/changed commitments, binding identity mismatch,
refused-operation recovery and exclusive journal ownership. These remain Rust
implementation tests, not the independent V2 failure driver or workload comparison.
No wire or database-format change was needed for this client integration.

Rust's file adapters now prehash an open regular input, retain the same descriptor
and stream it through the durable client. Result files remain temporary until
verified length, digest and FIN, then synchronize and install without overwriting
an existing destination. Owned transfers survive waiter cancellation. Tests cover
empty/256 KiB transfers, replay, changed source bytes, malformed results, withheld
FIN, byte ceilings and descriptor capacity. These adapters require trusted local
directories; process-death staging reconciliation and shared client disk budgeting
remain open. Fixed buffers and owner counts do not establish measured total
process memory or the original comparative workload costs.

The Unix Rust executable now exposes explicit V2 authority initialization,
authenticated serving, durable client initialization/reopen, mutation replay,
observations, result selection/download and exact root completion. Its registered
pure applications exercise leaf consume/copy, caller-produced reassembly and
authority-produced chunking; they are application contracts, not new wire profiles.
Six real subprocess tests exercise both negotiated combinations, committed
admission followed by process death, same-owner certificate rotation, changed-owner
rejection, lost local input, both branch modes, explicit retry/skip, scope
cancellation and offline revocation. Chunk cases include empty input, exact and
partial 64 KiB boundaries, and 33 children under the 16-active-job session ceiling.
Three unit tests cover bounded startup reads,
principal-map validation and permission policy. These tests use the Rust
implementation, not the required independent V2 failure oracle; the crash is not
an instrumented publication-boundary fault. This does not establish Java V2 parity
or the original external workload and equivalent streaming-gRPC comparison.

The Rust client now also has a managed local-copy library using the existing
immutable object store. A private root binds trusted authority/owner and immutable
object/byte/handle quotas. Downloads reserve their full length, install after
verified FIN and file/directory synchronization, and survive cancelled waiters.
Reopen reclaims unreferenceable interrupted staging under exclusive ownership;
unknown files or changed configuration are not adopted or deleted. Local lookup
requires the journal's exact manifest selection and verifies bytes again at EOF.
It does not assert current server authorization or renew remote retention.
Explicit removal cannot delete pinned copies or alter remote work outcomes.
Six substantive storage tests and a subprocess entry point cover quota/ownership,
corruption and crashes before/after installation and unlink. Two async tests
include a real authenticated 256 KiB download, quota rejection, unchanged terminal
revision and local retrieval after the server stops. Independent V2 failure testing,
Java V2 and the original workload/resource comparison remain unfinished.
An injected directory-synchronization failure reproduced a live copy being
accessible despite uncertain installation durability. The shared Rust object
store now quarantines that root until exclusive audited reopen; prior bytes
are retained rather than deleted or treated as durably committed by the failed call.

The Rust CLI now integrates explicit managed-root initialization, authenticated
downloads and offline verification/export from the original saved selection,
without network fallback. A separate private raw-export directory commits a
stable local ID and full manifest/index commitment before copying. Reopen audits
the bounded inventory before reclaiming incomplete staging; committed intent
stays charged. Exact replay verifies installed bytes, and changed identity cannot
overwrite them. Explicit removal is local only. Six substantive storage tests and
a subprocess entry cover five crash points, quotas, corruption and directory-sync
failure. A deterministic held-worker test covers cancelled export waiters; two
added CLI subprocess tests cover offline use, identity, initialization and cleanup.
The isolated 32 MiB local-copy/export/replay gate uses 8 KiB chunks, limits additional
Rust heap to below 256 KiB and individual allocations below 64 KiB, checks named
file lengths and reports observed RSS separately. Exporting adds a raw copy and
integrity reads. These are not independent V2 interoperability, allocated-block
reservations, external-reader accounting or machine-power-loss evidence. The old
arbitrary-path file adapter does not gain automatic crash-left staging cleanup.

## Java/Netty Reference Implementation

Organization:
:   PipeStream AI

Description:
:   Java 21 implementation using Netty QUIC for transport and Jackson CBOR for an independently implemented Layer 0 codec. It is available as a reusable Java library and a standalone client/server executable.

Maturity:
:   Prototype, publicly available in the `implementations/java-netty` directory of this document's source repository.

Coverage:
:   TLS 1.3 with ALPN `pipestream/1`; no 0-RTT; deterministic CBOR Capabilities, EntityHeader, and Checkpoint messages; STATUS heartbeat and entity progression; cursor advancement; parent identity; SHA-256 payload validation; checkpoint request/acknowledgement; and GOAWAY. The standalone command handles one entity per connection and does not implement Layers 1 or 2.

    Separate Java libraries implement the Section 9.8 declaration codec,
    durable SQLite membership and closure state, and a public Netty producer.
    A file-backed payload library adds bounded incremental reception and
    immutable retained inputs. Payload installation does not itself admit or
    complete work. A separate executor commits processing and rehydration jobs
    with state transitions, then runs fenced callbacks in bounded workers.
    A separate public SealedServer integrates these libraries with bounded
    Netty ingress, asynchronous execution, pending checkpoint deadlines,
    durable request/ACK replay identity, and recursive completion.
    A small native SQLite extension supplies JDBC file-length enforcement;
    it contains no PipeStream protocol or state-machine code.

Licensing:
:   MIT.

Implementation:
:   `https://github.com/ai-pipestream/pipestream-quic-protocol-rfc/tree/main/implementations/java-netty`

Contact:
:   Kristian Rickert (kristian.rickert@pipestream.ai)

## Rust/Quinn Reference Implementation

Organization:
:   PipeStream AI

Description:
:   Rust prototype using Quinn and Minicbor. Transport-independent protocol
    logic, Quinn transport, and the runnable server are separate crates.
    It is not feature-complete or fully conformant. Implementations and
    test vectors are non-normative and may require correction against the text.

Maturity:
:   Prototype, publicly available in the `implementations/rust-quinn` directory of this document's source repository.

Coverage:
:   Layer 0 plus Layer 1 recursive scopes, cross-scope parent identity, nested out-of-order completion, SCOPE_DIGEST verification, BARRIER, scoped checkpoints, rehydration, and lineage digests. Its Layer 2 subset provides durable yield, claim checks, cross-connection CLAIM_REDEMPTION, replay refusal, SQLite WAL recovery, and immutable payload storage. TLS 1.3 with ALPN `pipestream/1` is mandatory and 0-RTT is disabled. The original one-entity Layer 0 command remains available for the polyglot interoperability matrix.

    The separate private-use profile in Section 9.8 provides client-owned
    work-set declarations and seals, durable declaration ACK replay,
    non-reused identities, and fixed full-scope completion cuts. It excludes
    Layer 2. The separately negotiated authenticated-session binding in
    Section 10.6.4 adds mutual TLS, certificate-to-principal mapping, durable
    principal/authority ownership, and session-access revocation.

Licensing:
:   MIT.

Implementation:
:   `https://github.com/ai-pipestream/pipestream-quic-protocol-rfc/tree/main/implementations/rust-quinn`

Contact:
:   Kristian Rickert (kristian.rickert@pipestream.ai)

## C++/MsQuic Reference Implementation

Organization:
:   PipeStream AI

Description:
:   C++20 implementation using Microsoft MsQuic and a manually implemented deterministic CBOR codec. It contains reusable wire and transport libraries and a standalone client/server executable. It does not share protocol implementation code with the Java or Rust implementations.

Maturity:
:   Prototype, publicly available in the `implementations/cpp-msquic` directory of this document's source repository.

Coverage:
:   TLS 1.3 with ALPN `pipestream/1`; no 0-RTT; deterministic CBOR Capabilities, EntityHeader, and Checkpoint messages; STATUS heartbeat and entity progression; cursor advancement; parent identity; SHA-256 payload validation; checkpoint request/acknowledgement; and GOAWAY. The standalone command handles one entity per connection and does not implement Layers 1 or 2.

Licensing:
:   MIT.

Implementation:
:   `https://github.com/ai-pipestream/pipestream-quic-protocol-rfc/tree/main/implementations/cpp-msquic`

Contact:
:   Kristian Rickert (kristian.rickert@pipestream.ai)

## Interoperability Evidence

As of 2026-09-06, none of these prototypes demonstrates complete Layer 0
conformance. The common command exercises one-entity transfers, not the
entire mandatory manifest and recycling lifecycle. Rust's recursive path
adds independent control/data reception, identity-based stream dispatch,
pending checkpoints with deadlines, negotiated depth enforcement, and
fenced recovery-result publication. Its recursive receiver incrementally
spools payloads to temporary files with byte and file quotas. Application
callbacks consume file-backed readers in bounded asynchronous workers.

The durable Rust prototype now has optional mutual-TLS principal/session
binding, retained authenticated recovery, retained-storage quotas and admitted-job
SQLite completion reservations and explicit offline orphan reconciliation,
but lacks automatic retry scheduling, bidirectional work-set origination, scoped
cursor recycling, and full resilience semantics. Its
Layer 2 advertisement does not identify the narrower implemented subset.
It is unsuitable for untrusted multi-tenant deployment without additional
implementation work. Java's independent sealed producer exercises recursive
work against Rust, and a Rust public-client scenario exercises the Java sealed
server. Persistent Java producer observations are implemented and tested
across restart. Broader crash/resource and profile-conformance evidence remains
incomplete. C++ does not yet provide recursive or resilience evidence.
These limitations remain open;
passing vectors or document checks does not resolve them.

Rust tests exercise Section 9.8 with frozen wire fixtures, reordered
descendants, missing declarations and payloads, immutable seal refusals,
and a public-client reconnect after an unobserved declaration ACK and
server restart. Java-to-Rust QUIC tests additionally cover nested/chunked
completion, scoped checkpoints, replay after restart and a discarded declaration
ACK, and named protocol refusals. Fault-injection peers check Java's rejection
of changed ACKs, downgrade, oversized replies, and Layer 2 frames. Reverse
Rust-to-Java tests cover recursive/chunked completion, reconnect replay, and
changed-owner/checkpoint refusals. These scenarios do not prove the entire
profile. The original Java listener/CLI and C++ endpoints remain Layer 0;
Java's separate SealedServer requires the sealed profile. It does not yet
authenticate a client principal or implement the authenticated-session and
authenticated-recovery profiles. Its server-authenticated TLS lifecycle
fixtures are not evidence of compliance with Section 10.6.1's requirement
to authenticate and authorize callers before durable work admission.

Java payload-library tests additionally cover chunk geometry, immutable replay,
file-length and file-count quotas, cancellation-safe accounting, writer
exclusion, corruption, and abrupt exit between installation and admission.
A 32 MiB receive/install/read test runs with a 24 MiB Java heap limit.
Separate executor and actual-QUIC 32 MiB tests now run under the same Java
heap limit. Neither set establishes native-memory, RSS, physical filesystem,
or concurrent-workload bounds; library storage tests alone are not network
interoperability evidence.

Java's managed execution path now binds its database and payload root using
persistent store identities. Admission revalidates retained input and keeps the
store open through the transaction. Closed or foreign-store input handles are
refused. A complete file-side ownership claim can survive a failed database claim
and be retried by the same pair; a corrupt marker is refused. This is local
storage ownership, not producer authentication, a wire extension or orphan cleanup.
Earlier Java storage layouts are refused without conversion.

Rust now also pairs its retained root and SQLite database before service admission
or dispatch. Independently generated store identities and a synced file-first
claim prevent another database/root pair from being adopted implicitly. Complete
claims replay after a failed database transaction or process exit; partial,
corrupt or missing bound claims refuse without repair. The database metadata write
preserves admitted completion reservations. Tests cover competing roots, corruption,
interruption and WAL saturation; authenticated recovery over QUIC retains the pair
across restart and receipt replay. Older Rust storage policies are refused without
conversion. This local ownership prerequisite is not an orphan-cleanup API or a
replacement for principal authentication.

Java also provides explicit offline orphan reconciliation under exclusive payload
ownership and a database writer transaction. It audits managed input and retained
objects before deleting abandoned staging names. Unadmitted payload bodies become
commitment-only records retaining their immutable metadata and digest. Missing
input remains pending, changed retransmission is refused, and all admitted payloads
remain retained. Interrupted cleanup resumes only through another explicit call;
ordinary reopen counts remaining bytes without deleting them. Tests cover full
quota, concurrent and chunked restoration, process exit at filesystem boundaries,
and a real-QUIC timeout/refusal followed by matching restoration and completion.
Payload policy 3 refuses earlier policies without conversion; schema 6 and the
wire profile are unchanged. This is not retention expiry or whole-process resource
evidence. Rust now has its own explicit reconciliation path described below.

Java's durable execution tests cover atomic admission and dispatch, bounded
queued jobs and retained descriptors, recursive rehydration, stale executor
refusal after restart, and independent progress during a stalled callback.
Callbacks run outside database transactions and consume verified file-backed
input. Shutdown retains physical ownership until active work returns. These
are local library tests. Separate real-QUIC listener tests hold SQLite writes,
stall a callback, reset input, discard ACK observations, and restart the server.
They check pending deadlines, independent completion, replay, capacity refusal,
STRICT child failure, and rollback of forged scope summaries.
Per-session labels are not
authenticated tenants, and logical record quotas do not bound SQLite/WAL
pages or every future completion allocation.

Authentication tests cover missing, untrusted, expired and unmapped client
certificates; refusal of anonymous downgrade; principal and authority checks;
certificate rotation; live and reconnected session revocation; and background
recovery authorization. These authentication tests do not themselves establish
execution durability or solve ambiguous outcomes after a lost redemption ACK.

The Rust service durably fences process, rehydrate, and resume result
publication and no longer invokes application callbacks under database
transactions. Tests cover simultaneous lease acquisition, expiry, stale
publication after reopen and reacquisition, callback database re-entry, and
revocation during a callback. Storage quotas do not reserve all future completion
space or establish a complete resource guarantee.

The separate Rust-only Section 10.6.5 profile uses authority-qualified request
identities and immutable acceptance receipts with 24-hour retention. Recovery
acceptance commits redemption and a resume job together. Terminal outcomes
explicitly distinguish completion from refusal and echo the complete receipt.
Tests cover owner and authority refusals, expiry, irreversible claim revocation,
concurrent acceptance, queue rollback, abrupt process exit, lost receipt replay
after restart, and retained application refusal without automatic retry. Public
clients reject malformed responses and mismatched receipts or outcomes.
Twenty frozen wire cases and separate CDDL fixtures cover the new frames.
This does not add recovery to the sealed-work profile or establish independent
cross-language recovery interoperability.

The Rust core has typed job descriptors and a transactionally bounded
unfinished-job index with retained outcomes. Storage tests exercise limits,
rollback, interrupted attempts, and index integrity. The transport service
uses this queue for processing, rehydration, and resume operations. Bounded
admission workers install payloads before committing their job descriptors;
execution workers reopen and verify retained input before callbacks. Tests
cover abrupt process exit after admission and detached execution after reopen.
Raw QUIC tests exercise independent completion and deadline progress during
stalled callbacks. Shutdown stops dispatch without falsely releasing physical
capacity still occupied by a callback. Listener cancellation aborts its owned
connection and ingress tasks. Tests also cover pipelined first admission and
checkpoint accounting for received payloads awaiting installation.
Physical permits are shared within one
process, not across independent writer processes. Temporary quotas and
worker counts are not a complete multi-tenant resource guarantee.

Connection metadata and lineage I/O use separately bounded blocking workers.
Checkpoint clocks start at control-frame reception and are enforced independently
of storage completion. Tests hold SQLite writes and lineage persistence while
checking timely refusal, bounded control backlogs, and another connection's
progress. Cancelled waiters do not release still-running storage slots. Ordered
state-dependent dispatch can still wait behind storage; these tests do not
establish disk latency or concurrent-workload performance bounds.

Rust now applies persistent global and authority/principal quotas to serialized
session bytes and retained-session counts, including completed and revoked work.
State, accounting, and job-index changes share a transaction; readers validate
state and accounting in one snapshot. Tests cover concurrent capacity admission,
restart, atomic refusal, missing accounting, bounded serialization, and real-QUIC
declaration replay and rollback at quota limits. This is not a physical database
or payload-file quota, nor a reservation for every future completion record.

A separate Rust guard now bounds main database, WAL, rollback-journal, and
shared-memory file lengths for the bundled SQLite Unix backend. An immutable,
checksummed policy precedes database creation; nonempty unaccounted stores and
policy changes are refused without conversion. Growth is checked at file writes,
truncates, and shared-memory mappings, with preallocation and database mmap
disabled. Tests exhaust each file budget, hold WAL readers, interrupt a process,
and verify transaction rollback and a named capacity refusal over real QUIC.
This is a file-length boundary for cooperating writers in a private directory,
not a filesystem-allocation quota. Admitted-job completion reservations are
described below; the file-length guard alone is not orphan reconciliation.

Java separately enforces main database, WAL, rollback-journal and shared-memory
file lengths through a non-default VFS over Xerial's bundled Unix SQLite engine.
The packaged native extension does not link a second SQLite runtime or share
Rust protocol code. Private bootstrap registration is bounded, and VFS callbacks
remain loaded after bootstrap closure. Ordinary connections cannot manage the
registry or load extensions.
An immutable checksummed policy is synced before database creation. Every store
connection sets a main-page cap, and writes, truncates and shared-memory maps
check growth before delegation. Preallocation and database mmap are disabled.
Existing nonempty stores without policy, changed policies, incompatible backends,
corruption and aliases refuse without conversion. Native file-method and JDBC
tests cover actual file exhaustion, rollback, held WAL readers, registry capacity
and abrupt exit with an uncheckpointed WAL. A real-QUIC test verifies named
capacity refusal, retained membership, and replay after checkpointing and reopen.
The current backend supports private local directories and cooperating writers
on 64-bit Linux. These are file-length limits, not filesystem-allocation quotas,
authenticated principal quotas or future completion-space reservations.

Java version-4 stores separately reserve logical rehydration descriptor bytes
and completion slots at processing admission. Waiting parents retain that credit
across reopen, without occupying ordinary processing slots needed by children.
Closure converts the reservation and queues rehydration in the same transaction;
unrelated admissions cannot consume its metadata allowance. Processing stays
bounded at 128 global and 32 per-session queued/running jobs. Reserved or active
rehydration slots are separately bounded by 65,536 global and 16,384 per-session
entities within the combined metadata quota; physical worker limits are unchanged.
Tests cover queue and metadata saturation, rollback, exact descriptor conversion,
abrupt exit and real-QUIC completion. Discovery interleaves sessions in bounded
pages. These are not physical DB/WAL publication reservations or guarantees of
admitting unknown future descendants. Older Java schemas are refused without
conversion. Rust's admitted-job publication reservations are described below;
Java physical publication headroom and the full resource matrix remain due.

Rust storage policy version 4 reserves logical outcome, entity-digest and executor
record growth for admitted processing, rehydration and resume jobs. Layer 2
processing additionally reserves its configured continuation-token budget and
bounded claim metadata. The default token budget is 64 KiB and is exposed to the
application before dispatch, capped by the usable STATUS frame limit; this is a
local policy, not a wire-format reduction.
An oversized application result becomes a retained named refusal, without a
claim or successful entity transition. Checksummed actual/reserved charges commit
with session and job state and survive reopen. Old storage policies are refused
without conversion; session payload format 7 is unchanged. Tests pin serialized
growth, exact-quota publication, process exit, concurrent principal admission and
authenticated QUIC yield/recovery. Processing also reserves a possible rehydration
descriptor, outcome, attempt, parent output and scope-close digest. Queue policy
version 3 separates future/active rehydration from ordinary processing/resume slots:
65,536 global and 16,384 per authority/principal, versus 128 and 32 ordinary jobs.
Waiting parents retain credit without blocking their children; closure converts
the reservation atomically. Job discovery interleaves principals in bounded pages.
Store writes audit the bounded queue against retained session state before using
capacity. Tests exercise byte/slot exhaustion, interrupted conversion, corruption
and a sealed QUIC parent completing while ordinary processing remains full.
These reservations do not fund new child membership or payload admission,
checkpoint requests or filesystem blocks. They fund the fixed-capacity state
images and WAL stages described next; final-lineage file quota is separate.

Rust now preallocates a fixed-capacity session image containing a checksummed
header, serialized state and zero padding for protected growth. Mutable dispatch
and accounting use fixed images with immutable SQL keys. Future rehydration rows
are allocated with processing admission and become active or retired in place.
Within capacity, incremental BLOB writes preserve rows and allocated database
pages; unused logical credit does not shrink the retained allocation. The new
image and physical policies refuse older layouts without conversion; the session
payload remains version 7 and normative wire/CDDL are unchanged.

Before writing, a SQLite writer transaction funds every remaining acquisition,
publication and future rehydration-conversion stage. A per-connection VFS ceiling
protects that reserve against unrelated writes, including across rollback and
reopen. The ceiling also accounts for WAL-index shared-memory capacity. Expired
lease renewal retains publication credit rather than spending another job's
allowance. The stage bound covers the whole image, changed dispatch/accounting
pages, frame overhead, commit repetition and sector padding under pinned bundled
SQLite 3.53.2. Unsupported page geometry refuses explicitly.

Tests saturate ordinary WAL capacity with a pinned reader, then finish admitted
processing, rehydration and authenticated resume, including full-budget tokens,
two principals, concurrent admission, lease renewal and abrupt process exit.
A real authenticated QUIC test verifies token publication while the reader
remains pinned. A two-page-cache matrix measures complete acquisition/publication
transactions across three page sizes and token boundaries through 8 MiB, under
a fixed database page cap. Corrupt images and changed owned schemas are refused
without silent repair. These are cooperating-writer file-length reservations,
not allocated filesystem blocks or guarantees against I/O failure. Whole-session
serialization, integrity audits and scans including retired dispatch rows remain;
large sessions require proportionally more reserved WAL. No throughput or full
multi-tenant resource guarantee is inferred.

The Rust retained-payload store separately reserves global and authority/principal
bytes and object counts before disk creation. An immutable
checksummed policy survives reopen. Interrupted copies retain staging credit;
incomplete metadata and empty canonical directories remain globally charged.
Matching prefix replay can finish publication without overwriting admitted
input. A verified, synced receipt precedes successful installation. Tests cover
process exit, prefix images, policy and alias refusal, shared handles, and an
exclusive writer lock retained by readers and outstanding I/O. A real-QUIC test
checks named exhaustion, unchanged declared membership and independent principal
progress. These are bounded file-length reservations for a private single-writer
root, not filesystem-allocation, full power-loss or concurrent-tenant performance
proof. No orphan is silently deleted.

Rust payload installation now protects a separate 1,120-byte final-lineage
allowance and object slot per session before work admission. A checksummed
ownership marker covers the future digest, final metadata, receipt and stage
without inventing an output value. Partial markers stay globally charged;
matching replay establishes their owner without double charging. Final
publication uses prepaid staging credit even at the full ordinary quota,
and the complete allowance remains charged after publication. Tests exercise
partial metadata/receipts, process exit, owner limits, exact-quota publication,
and authenticated QUIC callbacks held while independent principals fill storage.
Missing declared payloads still prevent a successful checkpoint. The version-2
retained policy refuses old stores without conversion. SQLite completion capacity
is protected separately above; filesystem allocation and orphan reconciliation
are not established by these tests.

Spool tests cover quota exhaustion, file-backed chunk assembly, corruption
before assembly, cancellation-safe disk credit, and abandoned-file accounting.
A real-QUIC 32 MiB transfer measures Rust heap allocations while streaming
input and verifies the persisted payload digest and released temporary credit.
This is not a total process-memory or concurrent-workload performance claim.
Temporary quotas remain separate from retained-storage quotas. Their accounting
does not coordinate independent writer processes; the retained root now refuses
a second cooperating writer process.

The repository's protocol-neutral Rust driver starts each executable as a separate process and tests all nine client/server pairings. The driver has no dependency on a PipeStream implementation and does not encode or decode PipeStream frames. The implementations share the normative specification, CDDL, and golden vector corpus, but no protocol implementation code. The current suite verifies binary and UTF-8 payload transfer, parent identity, status progression, checkpoint acknowledgement, cursor advancement, graceful GOAWAY, and byte-exact delivery. The result is reproducible evidence for the listed protocol subset, not a claim of complete support for every optional field or extension in this document.

The authors welcome reports of additional implementations for inclusion
in future revisions of this appendix.

Rust's explicit offline reconciliation now audits the paired, writer-locked
database and payload root before reclaiming unadmitted bodies. It retains
immutable commitments, rejects changed retransmission and preserves admitted
input, declarations, and completion reservations. Tests cover concurrent
ownership refusals, interruption boundaries, quota restoration, and an actual
QUIC sealed session that remains pending until missing input is restored.
An isolated 32 MiB install/reclaim/restore test measures Rust heap allocations;
it does not establish total RSS or safe automatic cleanup under arbitrary
external writers. Reconciliation remains an explicit local operation.
