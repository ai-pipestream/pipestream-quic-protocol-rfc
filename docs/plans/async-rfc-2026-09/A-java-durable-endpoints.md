# A: Finish the independent Java durable authority and client

Read [the shared handoff](README.md) first. This implements the remaining Java
portion of original task 2. Deliver a usable authenticated durable endpoint and
client, not another collection of disconnected helpers. The neutral driver in
B is independent additional acceptance, not a substitute for A's real tests.

## Two independently assignable tickets

**A-SERVER** owns A1/A2 and the server-side A4 cases, exercising the real Java
host with the existing Rust client. Its reviewable increments are host ownership
and the first actual wire path, then the complete host operation/failure matrix.
Completing only the first increment is not completing A-SERVER.

**A-CLIENT** owns A3 and client-side A4 cases against the existing Rust authority.
Its reviewable increments are journaled creation/admission/result recovery, then
the full mutation/branch/coverage/error matrix. Completing only the first path
is not completing A-CLIENT. Neither ticket waits for the other to start tests.

Use separate worktrees. A-SERVER owns existing shared Java wire/transport/build
files, authority composition, `DurableServer` and a separate server launcher.
A-CLIENT owns new client/journal/file sources, `DurableClient` and a separate
client launcher; these are proposed names, not existing APIs. Name tests and
handoff directories by ticket. Before altering a shared helper or Maven file,
send a minimal requested change to A-SERVER for a separate integration commit.
Do not rewrite `Main` concurrently or silently redirect legacy serve/send.
Both may add files in the existing package to consume current typed helpers.
Public shared types needed by either client or external application must be
made accessible through a small reviewed API change, not reflection or code copies.

After both tickets are reviewed, a real Java/Java composed run is a final
integration gate. B alone owns the neutral mixed-language failure matrix;
the A tickets' production-client tests are supporting evidence, not B's oracle.

## Ownership and existing implementation

Own `implementations/java-netty/` and new evidence/handoff files under
`conformance/results/async-java-v2/server/` or `client/`, according to the ticket
ownership above. Do not rewrite the Rust authority/client or
shared normative files without the coordination procedure in the shared plan.

Start with actual sources in
`implementations/java-netty/src/main/java/ai/pipestream/quic/v2/`:

- `CoreServer`, `CoreClient`: authenticated Core-only public endpoints, not
  durable endpoints. `Main` in the parent package launches legacy commands.
- `TlsAuthentication`, `StreamTransport`, `ControlWrites`: real identity and
  event-loop/native ownership. Read the source-pinned transport README and
  packet-level credit tests before changing transport behavior.
- `DurableRequests`: decode-order IDs, one immutable binding, physical ownership
  tickets, input/output caps, detach and exact-completion exclusion.
- `SessionStore`, `InputStore`, admission/execution/publication/fence/closure
  stores, `ExecutionScheduler`, `ResultService`, `RetentionService`: actual
  durable local behavior. They are not all public or composed by a listener.
- `ControlWaitService`: shared bounded asynchronous WORK/checkpoint waits.
  A wait future can end before its running storage observation releases the
  physical charge. Inspect cancellation and worker-finally paths carefully.
- `Messages`, `Records`, `Wire`, `ClientCorrelation`, `ObjectStream`: independent
  typed wire foundations. Existing Java legacy journals are not V2 journals.

Representative current tests: `V2CoreServerTest`, `V2CoreClientTest`,
`StreamTransportTest`, `ControlWaitServiceTest`, `DurableRequestsTest`,
`CompletedSessionObservationTest`, and the storage/runtime/retention recovery
tests. They must continue to pass, but do not establish a durable wire endpoint.

## Deliverables, in order

### A1. Publish the executable/API interface, then wire a real vertical slice

Document the planned public Java endpoint/client entry points and machine-usable
test adapter before B/C integrate them. Use explicit create versus reopen,
authority and object roots, client journal, server/client certificates and trust
roots, stable principal mapping, supported applications, bounded configuration,
and a fresh readiness marker with the actual bound address. No implicit empty
store after missing history, default anonymous durable owner, or success inferred
from process launch. Library entry points must support applications, not just
one opaque demo command. CLI argument names may differ from Rust; document the
adapter instead of adding a new protocol to make them match.

Compose an authority-owned durable listener with one matched store pair and
bounded execution, waits, result delivery and maintenance services. Validate
configuration and recover/reconcile before readiness or capacity admission.
Wire create/declaration/real streamed input/admission/background execution/
manifest/result/checkpoint into a real mTLS loopback test early. Keep the endpoint
unadvertised outside controlled tests until every mandatory selected-profile
operation is implemented. Do not stop at this first vertical slice.

Use one actual durable connection owner composing `TlsAuthentication.Guard`,
stable `SessionStore.Access`, `DurableRequests`, `ControlWaitService`,
`ControlWrites` and `StreamTransport`. Do not bypass the reviewed ownership
primitives with a parallel state machine. Include the test-only fixture hooks
described in the shared plan, separate from the shipped launcher. The existing
`Main` serve/send paths stay explicitly legacy; add a separate V2 entry point.

### A2. Complete all server operations and lifecycle ownership

Implement every current Core, durable-work and result-delivery operation and
named refusal. The shared acceptance ledger is the checklist: both branch modes,
local producer grants, explicit retry, cancellation/skip/subtree settlement,
nonempty closure, retained reads, expiry, cleanup and retirement must be reachable
through the composed host with configured application callbacks.

- Establish current authentication before retained-state disclosure. Capture one
  stable owner; certificate rotation may retain that owner, remapping cannot
  silently change it. Publish an actually thread-safe current-authorization
  gate for storage workers and background execution, not an event-loop-only
  object accessed unsafely across threads. Recheck at required commit/read edges.
  Inspect `TlsAuthentication.Guard.requireOwner()` and its current fingerprint
  mapping check; reuse that logic rather than inventing another owner mapping.
  Verify its access/thread-safety assumptions before invoking it off-loop.
- Keep parsing/correlation and all Netty operations on the proper event loop.
  Run SQLite, hashing, blocking file I/O and application callbacks in bounded
  off-loop workers. Queues have global/per-owner capacities and ownership even
  after the caller cancels. WORK/checkpoint waits do not occupy workers between
  polls; their elapsed interval includes accepted queue time.
- Accept controls in decode order. Bind only a real correlated committed
  SessionStore response; second or competing attachment cannot replace it.
  Tag admission by the actual input stream ID. No application-supplied stream
  identity, second response after result start, or sender reset on receive-only
  streams. Refused valid controls consume IDs; malformed controls remain fatal.
- Retain `DurableRequests.Ticket` across every real worker, admission, response
  write and result transfer. A cancelled/completed future is not physical
  quiescence. If helpers lack a physical-completion notification, add a tested
  explicit one. A callback completing a future before its finally block is a
  specific race to exercise, not a reason to release early.
  Add a paused-write regression: storage verification finishes, the write still
  owns its ticket, then connection loss occurs. Capacity and COMPLETE exclusion
  cannot disappear merely because the database task finished. An incoming
  stream's direction/ID/header must be validated before allocating payload state;
  retain its input ticket through FIN, admission commit and response ownership.
- Stream inputs/results larger than windows incrementally. Respect the patched
  transport's paired receive-credit and native send-budget behavior, including
  loss/reordering and replacement streams. Count buffers, pending stream opens,
  headers, output leases, handles, stalled writes and native retained bytes.
- Preserve read pins through actual I/O. Connection close stops new connection
  work without cancelling durable obligations or refunding busy resources.
  Closing the listener/services must leave accepted jobs recoverable and honor
  actual worker/stream/store lifetimes. Startup starts cleanup safely; shutdown
  does not destroy stores while a callback or collector still owns them.
  Document and test a shutdown dependency order: stop new acceptance, stop new
  scheduling/discovery, drain or explicitly fail connection-local waits/transfers,
  wait for all busy worker/read/write owners, then release services/store handles.
  If a deadline ends the process instead, leave active work recoverable; do not
  close stores under live callbacks and claim graceful shutdown. Include a real
  paused callback/collector in this gate rather than relying on close() names.
- Distinguish WORK unchanged snapshots from checkpoint WAIT_TIMEOUT. Distinguish
  verified stored root, an exclusive connection cut, sent response, FIN, transport
  ACK, and peer application persistence. Use the exact Section 12.8 rules for
  COMPLETE and DETACH; preserve responses preceding control FIN. Do not invent
  a requirement that both FINs arrive before sending the detach response.

### A3. Complete the independent durable Java client and recovery

Provide public typed operations for the entire selected combination. Independently
journal original session creation and immutable operation parameters before send;
persist verified receipts/observations before reporting durable success, even if
the waiter disappeared. Use exact integers and independently implemented typed
commitments; do not shell out to Rust or import its codec/journal.

Preserve uncertainty across actual client-process death, connection loss and lost
ACK. Reopen original intent, attach/replay/lookup under the same identities;
NOT_FOUND while an original request is in flight is not authorization for new
work. Never increment an attempt except through the explicit authorized retry.
Bound journal size/WAL and pending operations; preserve accepted observations
and credits through late callbacks and local persistence failures.

Retain and validate immutable work identity, revision, manifest/selected object,
complete sealed membership and bottom-up status coverage. Check parent/child
commitments in both arrival orders, including parent membership verified later.
Do not overwrite known evidence with a contradiction or infer coverage from
an empty page. COMPLETE uses the exact verified saved root.

Stream from stable input handles; validate result header, bytes, length, hash
and FIN before installing the destination. Use bounded owned staging, no overwrite,
crash-safe cleanup and explicit local-copy versus newly authorized remote-read
semantics. A locator is not trust configuration or a bearer capability. Provide
commands sufficient for B/C to exercise recovery using the actual Java client.
Matching every Rust convenience/export command is not required; matching every
mandatory protocol/client obligation is required.

### A4. Attack the composed endpoints and document the real boundary

Add real Java-client/Java-server tests, Rust-client/Java-server and
Java-client/Rust-server tests. Use actual callbacks that produce bytes. Exercise
leaf, empty scope, caller-expanded and authority-expanded branch workflows,
STRICT failure, inputless cancellation, skip policy, retry and retained outputs.

Required negative/race cases include:

- Wrong/missing/expired/unmapped credentials, live revocation/owner remap,
  cross-owner reads, profile downgrade and contradictory authenticated replies.
- Lost create/declaration/admission/retry ACKs; kill before/after durable
  boundaries with reopening the same roots; client-journal commit/ACK loss.
- Missing declared descendants, child-before-parent observations, stale producer
  grants/workers and cancellation before/after actual publication.
- Incomplete/oversize headers, wrong hash, truncated FIN, unsolicited/duplicate
  results, stopped consumers, stream-open exhaustion, blocked DB workers and
  full control queues. Another healthy connection must still progress.
- Disconnect/cancel during a busy wait and a queued write; exact physical
  ticket/lease lifetime; COMPLETE competing with transfers; repeated DETACH,
  following valid requests and control FIN with outstanding responses.
- Positive checkpoint wait expiring during storage, zero-wait ready/not-ready,
  stream deadline equality, safe-clock regression, retention while readers or
  parents pin bytes, and cleanup/reopen without invented free capacity.

## Acceptance and handoff

For a single A ticket, accept only its assigned host or client scope, with the
opposite real Rust peer and its relevant A4 cases. Report the Java/Java combined
gate as pending until both artifacts are integrated. This permits two reviewed
ticket completions without declaring the combined contract or goal complete.

All mandatory profile operations must work through the real Java host and client;
focused tests, full Java install, strict changed-type doclint, source-pinned
native checks when changed, affected Rust suites, current frozen-vector/model
checks, legacy examples and draft checks retain direct exits and raw evidence.
Use the commands in the published checkpoint/transport README; do not assume
this machine's `/tmp` Maven artifacts exist on another worker.

Check in A's public adapter contract, reproducible mTLS setup, complete local
and cross-language run commands, requirement/evidence mapping, tested artifact
hashes and final handoff. A's tests may use production clients. B's independent
failure oracle is still required; A does not declare the whole goal complete.
No main merge, deployment or draft submission follows automatically.
