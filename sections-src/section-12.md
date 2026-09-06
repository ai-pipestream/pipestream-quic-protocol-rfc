# Version 2: Durable Work and Results

This section defines the successor major mapping, identified by ALPN
`pipestream/2`. Sections 2 through 10 and Appendix C describe version 1;
their layer booleans, mandatory recursion, circular cursors, status words,
storage-provider metadata and private-use profiles do not implicitly apply
to version 2. This section and Appendix F define version 2 completely.
Section 11 distinguishes the two registrations. Neither identifier is an
IANA assignment merely because an implementation uses this draft.

Version 2 has a small mandatory Core: QUIC/TLS, bounded deterministic CBOR
framing, capability selection, correlation, refusal and connection draining.
Two explicitly agreed private-use profiles add durable work (65284, named
`durable-work-v2`) and result delivery (65285, `result-delivery-v2`). Result
delivery requires durable work. Their meanings are immutable; incompatible
changes require a different identifier or major mapping. Version-1 profile
identifiers 65281 through 65283 MUST NOT be selected on version 2. An endpoint
MUST NOT advertise either new profile until it implements all its mandatory
behavior, including authorization, restart, retention and refusals.

The durable profile has one processing authority per session. The authenticated
caller can originate work and the authority can generate descendant work under
the same owner's authorization. The profile does not transfer processing
authority between peers or standardize a scheduler. Applications can compose
sessions at different authorities; such composition is not a distributed
transaction. A peer requiring version 2 MUST NOT retry its work as version 1
after ALPN or capability refusal. Stored version-1 sessions MUST NOT be
converted implicitly or opened through the version-2 lifecycle.

## Core Mapping and Negotiation

Use QUIC version 1 {{RFC9000}} and its TLS mapping {{RFC9001}}, with TLS 1.3,
ALPN `pipestream/2`, no application 0-RTT, and server identity verification
under {{RFC9525}}. Connection migration does not change authenticated identity.
The client opens bidirectional Stream 0 for control. Other bidirectional
streams are forbidden. Unidirectional streams carry profile-defined input
or result objects; Core alone defines no application object format.

Every control frame is one type octet, a four-octet unsigned big-endian body
length, then exactly that many body octets. Known bodies are exactly one
deterministically encoded CBOR item under {{RFC8949}}, Section 4.2. Arrays
have the exact cardinality in Appendix F. Integers and lengths use the
shortest representation; indefinite lengths, trailing items, tags, floats,
undefined, and extra positions are invalid. Booleans and null occur only
where the schema permits them. Validate lengths before allocating buffers.
The initial CAPABILITIES body is limited to 4096 octets.

Control type values in this mapping are CAPABILITIES (0x01), SESSION (0x02),
SCOPE (0x03), WORK (0x04), RESULT (0x05), DRAIN (0x06), and REFUSAL (0x07).
All are CBOR, not the version-1 fixed/serialized type classes. An unknown
type in 0x00..0x7F is a fatal FRAME_ERROR. Types 0x80..0xBF are ignorable
extension frames: discard their bounded body incrementally without applying
state changes. Private types 0xC0..0xFF require an activated defining profile;
otherwise refuse EXTENSION_UNSUPPORTED. Unknown frames cannot activate a
profile. Using a known profile-dependent message without that profile is
EXTENSION_UNSUPPORTED. Only CAPABILITIES is allowed before negotiation.

The client sends `v2-capabilities` with response 0. The server selects the
intersection of enabled supported identifiers and sends response 1. Lists
are strictly increasing, contain at most 32 entries and have no duplicates.
Each required identifier MUST also be in the sender's supported list. The
server's supported list is the selected intersection, not its complete
implementation inventory; its required list is the union of both required
sets. Unknown optional identifiers are not selected. If a required identifier
is unavailable or a dependency cannot be activated, refuse CONNECT with
EXTENSION_UNSUPPORTED, without acknowledging capabilities or work.

The four size/count limits and two stream deadlines are selected by taking
the minimum of each offer. Stream idle time MUST NOT exceed stream lifetime.
The client checks list membership, required-set echo, dependencies and every
limit. An unsolicited selection, increased limit or invalid response is a
FRAME_ERROR. Both peers MUST require every profile needed by resumed work.
There is one exchange per connection; another CAPABILITIES is a FRAME_ERROR.

`stream-limit` bounds concurrent incoming unidirectional streams in each
direction; `pending-limit` bounds unresolved requests in each direction;
`object-limit` bounds the payload of each input or result object. An object
may be larger than a QUIC flow-control window. Receivers MUST consume
incrementally and replenish credit without waiting for the entire object.
They MUST keep control progress independent of blocked input, processing,
storage and result delivery, with bounded queues and reserved control
capacity. QUIC stream priority alone does not supply connection credit.
Flow-control and dependency considerations in {{RFC9308}} apply.

Stream idle time runs between payload-progress observations. Stream lifetime
runs from accepting its header, without extension by progress. Exceeding
either bound resets that stream with LIMIT_EXCEEDED. Implementations MUST
also bound the time and bytes spent receiving headers, pending result-stream
creation, per-principal connections, staging files, metadata, queued work,
retained bytes and storage journals. Global and per-principal exhaustion
refuses new work, not existing promises. Negotiated limits are ceilings, not
an unconditional reservation against aggregate deployment quotas.

## Correlation and Error Scope

The requests specified here are sent by the client and the responses by the
server. Authority-produced descendants use the server's validated local
interface, not unsolicited requests to the client. Ignorable extension frames
do not change that rule.

Client control requests use strictly increasing positive `request` integers,
starting at 1 per connection, shared across message types. A repeated or
decreasing identifier is FRAME_ERROR; reconnect before exhaustion. Responses
may arrive out of order but MUST identify an outstanding request of the
correct kind. A mismatched, duplicate or unsolicited response is FRAME_ERROR.
The request is complete when its specified response or REFUSAL arrives,
except a result read, which completes at validated result-stream FIN or
refusal/reset. No response may be interpreted as an unspecified later step.

Input admissions are requested by their actual QUIC unidirectional stream,
not by a control request number. `v2-request-tag` is `[0, control-request]`
for control and `[1, actual-stream-id]` for input admission. QUIC stream IDs
are never recycled. Admission headers do not claim their own stream ID.
This allows independently arriving inputs without a growing application
request-ID history. All outstanding-request maps remain bounded by the
negotiated stream and pending limits.

REFUSAL contains the request tag, a named code and at most 512 UTF-8 octets
of diagnostic text. Diagnostics are not state transitions. A refusal cannot
be a terminal computation outcome unless an authenticated WORK view reports
that outcome. Codes are: FRAME_ERROR (1), EXTENSION_UNSUPPORTED (2),
UNAUTHORIZED (3), LIMIT_EXCEEDED (4), NOT_FOUND (5), EXPIRED (6), CONFLICT (7),
INTEGRITY_ERROR (8), NOT_READY (9), WAIT_TIMEOUT (10), DEADLINE_EXCEEDED (11),
CANCELLED (12), APPLICATION_UNSUPPORTED (13), CONTROL_RESET (14),
INTERNAL_ERROR (15), OUTPUT_UNAVAILABLE (16), CLOCK_UNSAFE (17), and
ALREADY_TERMINAL (18). Values 19..31 are reserved; an unknown REFUSAL code
is FRAME_ERROR. The same named error used as a QUIC application error has
numeric value `0x200 + code`; graceful transport close uses 0.

Malformed control, wrong message direction, negotiation failure, lost control
framing, and an unusable Control Stream terminate the connection. Use
CONTROL_RESET when Stream 0 is reset. A valid but refused request returns
REFUSAL without closing unrelated work. Invalid input or result streams are
reset with the corresponding error; if their request can be correlated,
send REFUSAL unless a response was already sent. After a result header has
started its response, subsequent delivery errors use stream reset, not a
second control response. Neither stream reset, connection loss nor timeout
modifies a declared obligation, commits failure, or authorizes a new attempt.
An unrecognized QUIC error still ends that transport, without implying success.

Schema, canonical encoding and cross-field structural violations use FRAME_ERROR.
A structurally valid request naming inconsistent retained state uses CONFLICT
unless a more specific refusal is defined below. Identity or commitment
failures MUST NOT be disguised as successful empty results. Authentication
and authorization are checked before any refusal that would reveal retained
existence, state or commitments.

## Authenticated Sessions and Non-Reusable Identity

Durable work requires mutual TLS and a configured stable principal mapping.
The server requests and validates a client certificate during the handshake,
including possession, trust chain, validity and client-authentication usage.
Missing or unmapped identity prevents durable-profile activation. Mapping
may use SHA-256 of the complete verified DER leaf certificate; fingerprint
equality is not a replacement for TLS authentication. Rotated certificates
may map to the same principal. TLS resumption MUST reapply current credential
validity and mapping, or be refused in favor of a full handshake. No anonymous
fallback is permitted. Core-only operation does not require a client principal.

TLS certificate validation failures terminate the handshake using the QUIC
CRYPTO_ERROR mapping in {{RFC9001}}, Section 4.8; an endpoint MUST NOT suppress
that failure to reach application negotiation. After a successful handshake,
if required durable work cannot activate because caller identity is absent or
unmapped, close with UNAUTHORIZED before a capabilities response. When durable
work is merely optional, it and result delivery are
excluded from that connection's enabled set. Credential expiry prevents new
requests on that connection; a fresh credential may reconnect to the same
retained principal. An accepted job's retained authorization grant is separate
from the presenting certificate's lifetime and remains subject to current owner
policy, revocation and its execution deadline.

Authority and owner labels are 1..128 ASCII letters, digits or `-._~`, compared
byte-for-byte. Authority is verified deployment configuration, not selected by
the client. Owner is the mapped authenticated principal. The server checks
current authorization before exposing any session, operation, input, output
or scope state, and again in every committing transaction and publication.
Session revocation is durable, irreversible for that session identity, and
applies to existing and new connections. Denials disclose no retained contents.

SESSION operation 3 queries the authenticated owner's next creation sequence;
operation 4 returns it. Creation sequences start at 1. SESSION operation 0
requests creation with that sequence and the exact desired `v2-policy`.
The authority atomically allocates a monotonically increasing positive session
generation, increments that owner's creation high-water mark, binds the owner,
policy and profile set, creates root scope 0, and records the creation receipt.
Only then does it send SESSION operation 1. It MUST accept the policy exactly
or refuse it; lowering a retention promise silently is forbidden.

An identical creation request replays the same receipt and generation.
The selected durable-work/result-delivery profile combination is also part
of that immutable creation binding. The client persists it with the sequence
and policy; creation replay and attachment MUST select that same combination,
or receive EXTENSION_UNSUPPORTED. A session cannot acquire or shed result
delivery through reconnection. Changed policy under the same retained sequence
is CONFLICT. A sequence
greater than the next expected value is CONFLICT. A prior sequence whose
receipt has been retired is EXPIRED, never a fresh creation. The client
persists its creation sequence and policy before transmission. Concurrent
creators sharing a principal serialize through these rules, not a random
identifier uniqueness assumption.

SESSION operation 2 attaches using expected authority, owner and generation;
operation 1 returns the same immutable session binding and policy. Limits
are the session's retained admission ceilings. Reconnection limits must
accommodate its retained message representations; otherwise refuse
LIMIT_EXCEEDED, without changing the session. A connection binds to at most
one session and cannot create or attach a second one. Core-only connections
cannot use SESSION. Mutation requires the durable profile; output access also
requires the result profile retained by that session.

A logical work identity is `(authority, owner, generation, scope, producer,
entity)`. Wire `v2-work-key` carries its last three fields; the connection
supplies the authenticated session binding. Producer 0 is the caller and
producer 1 is the authority's authorized descendant generator. External
clients cannot declare or admit inputs as producer 1. All scopes have one producer,
and all their entities use that producer. Numeric IDs in different scopes
or producers are not the same work. Scope IDs are allocated monotonically
by the authority within the session; root is 0. Entity IDs strictly increase
within their producer's scope. Neither is recycled, even after completion.

Session generations and owner creation high-water marks are durable and
never decrease. Exhaustion refuses creation. Restoring a stale backup under
the same issuing identity is forbidden unless external durable anti-reuse
state proves no generation can be reissued; otherwise use a new explicitly
configured authority identity. This protocol does not repair lost durable
storage or authenticate an operator's incorrect backup restoration.

## Immutable Operations and Replay

Each mutation has a nonzero 16-octet operation ID, unique within its producer
and session. The originator persists the ID and complete immutable parameters
before transmission. A new connection uses a new connection request number
but the same operation ID and parameters. An operation receipt commits its
ID, request digest, and exactly one typed outcome from Appendix F.

The request digest is SHA-256 over ASCII `pipestream-operation-v2`, without
a terminator, followed by deterministic CBOR encoding of:

`[authority, owner, generation, producer, operation-id, control-type,
operation-code, parameters]`.

For input admission, control type is 4, operation code is 0 and parameters
are `v2-admit-parameters`. For SCOPE declaration, parameters are
`[scope, entity-ids, seal]`; for scope cancellation, `[scope]`. WORK retry
parameters are `[work-key, expected-attempt]`; cancellation and skip use
`[work-key]`. Connection request identifiers and payload bytes are excluded;
the input descriptor includes the payload's required length and digest.

The authority atomically commits a mutation and its replay receipt. Matching
replay returns the identical receipt without executing the mutation again.
Reusing an ID in the same producer namespace with a different digest, type or
operation is CONFLICT.
Concurrent matching requests serialize through the same durable uniqueness
constraint. They do not create two jobs. A pre-commit refusal leaves no accepted
operation. WORK operation 2 looks up an operation; operation 3 returns its
receipt. NOT_FOUND does not prove that an older in-flight request cannot still
commit. To resolve ambiguity, retry the same immutable operation, never invent
a new operation or work identity as an automatic recovery action.

Here the operation's producer identifies its originator, not necessarily the
producer of its target work. Client-originated operations and WORK operation
lookup use namespace 0; local authority operations use namespace 1. An owner
may authorize retry, cancellation or skip of authority-generated work through
namespace 0 without becoming its input producer. Such authorization does not
permit an external declaration or input in a producer-1 scope. A scope-cancel
request also uses its originator's namespace, including when its target scope
belongs to the other producer.

Retain operation identity and digest until session retirement. Full receipts
are retained through the affected work's receipt interval, or through session
retirement for declaration/scope operations. After that promise an operation
lookup may return EXPIRED, but never become a fresh mutation. Clients validate
the operation ID, recomputed digest and every typed outcome field before
accepting a receipt. Authenticated replay is not a transferable signed proof.

## Declaration, Admission and Descendant Scopes

Root scope 0 belongs to producer 0. SCOPE operation 0 declares up to 256 IDs
in an existing scope and optionally seals it. IDs are strictly increasing
within and across batches. Empty batches are permitted only with seal true;
an empty scope may be sealed. Only the scope's producer can declare or seal
normal work. SCOPE operation 1 returns the durable declaration receipt.
The producer MUST receive its covering receipt before sending an input.
Declaration consumes bounded membership/receipt capacity, not input, output
or execution capacity. It is not admission or evidence of processing.

After sealing, no producer can extend membership. The seal digest is SHA-256
over ASCII `pipestream-scope-seal-v2` followed by deterministic CBOR of
`[authority, owner, generation, scope, producer, parent-or-null, entity-ids]`.
The final array contains every declared ID in ascending order, independent
of batching. Hash it incrementally; no whole-scope buffer is required.
Changed membership under a replayed operation, a late new declaration,
or an undeclared input is CONFLICT. Seal mismatch is INTEGRITY_ERROR.

Each input is a client-initiated unidirectional stream containing a four-octet
unsigned big-endian header length, the exact CBOR `v2-input-header`, then
exactly the declared input bytes and FIN. Header length is 1..4096. Validate
session, producer, declared membership, application profile, mode, execution
duration, resource budget, descriptor and cancellation fences before accepting
its payload. The header's generation MUST equal the attached session and
the work key MUST belong to producer 0. An authority-generated input uses
the same admission rules through its local producer-1 interface; it is not
an unrequested input stream sent to the caller.

An external input for producer 1 is UNAUTHORIZED. A different generation or
a work key outside the attached session is CONFLICT after owner authorization.

Modes are leaf (0), caller-expanded branch (1) and authority-expanded branch
(2). A branch admission atomically allocates its one child scope, owned by
producer 0 or 1 respectively, and records that identity in the receipt.
No child can precede admission of its parent. A leaf cannot acquire children
later, and a branch cannot replace its child scope. This closes the ambiguity
between an empty subtree and a subtree that has not yet been declared.

Content type and application labels are bounded printable ASCII without
control characters. The application label identifies an explicitly configured
versioned processing contract. Unknown application contracts are refused
APPLICATION_UNSUPPORTED, not processed through an arbitrary fallback.
Input length can be zero; SHA-256 of the empty byte string is still required.
Invalid FIN geometry, trailing bytes or digest mismatch refuses the input
with INTEGRITY_ERROR and discards partial reception, not its declaration.

Receive and hash incrementally into bounded immutable storage. After complete
validation, atomically commit immutable input identity, an admission timestamp,
execution deadline, attempt 1, the child scope if any, a restartable job and
all reserved publication/retention capacity. Only then send WORK operation 1,
tagged with the actual input stream ID and the admission receipt. Admission
is separate from successful processing. No irreversible application effect
is permitted before validated admission and application authorization.

An already-admitted matching operation may replay its receipt from the header
without re-reading or re-admitting transmitted bytes. The server then stops
that redundant input stream with application error 0. The client still needs
the correlated receipt or operation lookup; STOP_SENDING alone is not admission
evidence. A changed header or commitment is CONFLICT. Input reception that
has not committed can be interrupted without losing its declared obligation.

Admission reserves the declared maximum output count/bytes, worst-case bounded
manifest and receipt encoding, attempt/closure metadata, executor capacity and
retention accounting. It MUST also reserve control-message space for its
largest promised response under the retained negotiated limit. No implementation
may admit an output budget that cannot later be represented or committed.
Over-budget callback output produces an authoritative FAILED outcome, not
truncated successful output. Input, output, metadata and journal reservations
remain charged across restart. Capacity refusal is LIMIT_EXCEEDED and leaves
the admission and job uncommitted.

## Attempts, Cancellation and Authoritative Outcomes

WORK states are DECLARED (0), ACTIVE (1), AWAITING_RETRY (2), WAITING_CHILDREN
(3), CANCELLING (4), SUCCEEDED (5), FAILED (6), CANCELLED (7) and SKIPPED (8).
The last four alone are terminal. A terminal outcome never changes. A declared
entity has attempt 0, no input/admission/deadline, and no result manifest.
Admission assigns attempt 1. Leaf execution becomes ACTIVE; a branch retains
its child scope and waits for closure before its rehydration can succeed.
An application may report a retryable attempt failure as AWAITING_RETRY;
this is not a terminal failure of logical work.

WORK operation 6 explicitly requests retry with the current expected attempt.
Before the original execution deadline, an authorized retry atomically fences
that attempt, increments its generation by one, installs the replacement job
and receipt, and returns operation 7. It preserves input, membership, child
scope, admission time, deadline and retention policy. Replay does not increment
again. A stale expected attempt is CONFLICT unless the same operation already
committed. Terminal or cancelling work is ALREADY_TERMINAL or CANCELLED;
expired deadline is DEADLINE_EXCEEDED. Exhausted counters or reservations
are LIMIT_EXCEEDED. Retry never extends the deadline or creates a new member.

Every callback publication checks the retained authority/owner, revocation,
ancestor cancellation fences, current attempt, execution deadline and a
current durable worker lease in the committing transaction. A server restart
may resume an already-admitted job with a new internal lease; it does not
create a new wire attempt. The processing contract MUST define safe restart
through application idempotency, external fencing or transactional effects.
If it cannot, refuse that application contract. Multiple physical callback
invocations are possible: this protocol does not promise exactly-once external
effects. Callbacks run outside metadata transactions and cannot occupy the
control reader while waiting for I/O or computation.

WORK operation 8 requests cancellation and operation 10 requests an explicit
skip. Their receipts are returned as operation 9 and 11. Disposition 0 means
an authoritative cancellation/skip fence was accepted, not that every worker
or descendant has already stopped. Disposition 1 returns the pre-existing
terminal state without changing it. Skip is permitted only when the
application's authorization policy explicitly permits it. It never counts
as success under STRICT closure.

A skip uses the same exclusion fence and bounded descendant settlement as
cancellation. Its target eventually settles as SKIPPED; unresolved descendants
settle as CANCELLED, not SKIPPED. The first accepted cancellation or skip fence
fixes the target's eventual outcome. A later conflicting fence is CANCELLED;
an identical operation replays its receipt. For disposition 0 the receipt's
state is CANCELLING or the requested terminal outcome when settlement completed
in that transaction; disposition 1 always contains an existing terminal state.

Cancellation and successful publication serialize at one logical commit
boundary. If publication wins, cancellation reports that terminal result.
If the cancellation fence wins, the old attempt cannot publish. Cancellation
may settle an unadmitted declared entity; missing/invalid input or disconnect
cannot. Branch cancellation freezes every existing descendant scope and
fences all its unresolved work. Already terminal descendants retain their
outcomes. CANCELLING remains nonterminal until all affected obligations have
durable terminal settlements. Large subtrees may be materialized in bounded
batches, but the accepted ancestor fence MUST already exclude new declaration,
admission, retry and publication throughout the subtree.

SCOPE operation 6 requests that same operation for a whole scope; operation 7
returns the accepted fence receipt. It seals current membership, including an
empty set, and settles existing nonterminal members as CANCELLED. Scope
cancellation is an explicit owner-authorized exception to the normal producer
seal rule. Root scope cancellation covers the entire session. A failed parent
does not silently cancel descendants; those obligations still require ordinary
completion or explicit scope cancellation before root closure.

Execution deadline expiry and session revocation must drive authoritative
fenced settlement of accepted work, not eviction. Expiry yields FAILED;
revocation cancels work and denies caller access. Reconciliation must complete
those settlements after restart. They cannot retroactively retract an external
effect or a result that committed before the fence.

Revocation applies an owner-independent root cancellation fence, seals existing
scope membership and settles all unresolved declarations as well as admitted
work. It does not leave unadmitted obligations waiting for a now-unauthorized
caller. A deadline failure of one work item does not implicitly cancel other
obligations; the enclosing scope still requires their closure.

WORK operation 4 reads a work view or waits up to 30000 ms for its revision
to change. Revision starts at 1 on declaration and strictly increases on
observable durable change. `after-revision` 0 requests an immediate snapshot;
a value greater than current revision is CONFLICT. Operation 5 returns a
consistent current revision/view; a wait timeout returns the unchanged view,
not failure. Every non-null admission, terminal, child and manifest field
MUST agree with the immutable records. The client checks identity and known
commitments; transport observations cannot fill missing authoritative fields.

Every admitted view retains its input descriptor, admitted-at, deadline and
positive attempt, even when payload bytes later become reclaimable. A branch
has its child-scope field; a leaf has null. Nonterminal views have null
terminal-at, receipt-until, output-until and manifest. CANCELLING may be
inputless when a declared entity was cancelled before admission. Terminal
views have terminal-at and receipt-until; FAILED requires a diagnostic, and
AWAITING_RETRY requires the current attempt's diagnostic. Inputless CANCELLED
and SKIPPED retain attempt 0 and null input/admission/deadline/child. SUCCEEDED
requires admitted input and, when result delivery is selected, the immutable
manifest and output-until. Other terminal outcomes have null manifest and
output-until. Without result delivery, output budget must be zero and success
has no manifest. An expired object's manifest may remain in an unexpired work
receipt; its availability timestamp, not its presence, determines read eligibility.

## Result Publication, Streams and References

The result profile requires both first-class object streams and authenticated
reference retrieval. No result operation schedules processing. A callback
produces bounded output objects through the implementation's application API.
Before success, the authority validates and durably installs their immutable
bytes, lengths and SHA-256 digests. In one fenced metadata commit it publishes
the exact `v2-result-manifest` and SUCCEEDED state. A staged file without that
commit is an orphan, not a visible result. Failure, cancellation and skip have
no success manifest. Success may legitimately publish zero objects.

Manifest output indexes are contiguous from 0 in array order. Their count and
aggregate lengths do not exceed the admitted output budget, and each object
obeys its object limit. The manifest binds authority, owner, session, logical
work, current attempt, admitted input digest, commit time and availability
deadline. Its commitment, where an application retains one, is SHA-256 over
ASCII `pipestream-result-manifest-v2` followed by its deterministic CBOR.
A commitment proves byte identity, not correctness of computation. Receipts
and manifests are authenticated when obtained from the authority over TLS;
neither is specified as a portable signature or bearer capability.

RESULT operation 1 reads a retained manifest for a work/attempt; operation 2
returns it. RESULT operation 0 requests a specific object with its expected
SHA-256. The authority checks current owner authorization, the exact committed
work/attempt/index/digest and unexpired output availability before pinning a
read lease. Wrong commitment is INTEGRITY_ERROR; unpublished output is
NOT_READY; a wrong attempt/index is NOT_FOUND; expired availability is EXPIRED;
unexpectedly missing/corrupt retained storage is OUTPUT_UNAVAILABLE, never
an automatic rerun or a new successful result.

A successful object request responds on one server-initiated unidirectional
stream: four-octet big-endian header length (1..4096), exact
`v2-result-header`, the complete object, then FIN. Its request field is the
outstanding control request number; generation, work, attempt, index, length
and digest MUST all match the requested committed object. This stream is the
response, not a second independently correlated RPC. The client validates
length, SHA-256 and FIN before presenting bytes as a verified result. It may
perform reversible incremental consumption earlier, with explicit unverified
status. Extra bytes, truncation or a mismatched header are refused. This
profile transfers full objects, not implicit byte ranges or digest-only output.

A reset aborts only delivery. Another identical object request reopens the
same immutable output while authorized and retained. It MUST NOT re-execute
the job, advance an attempt or alter terminal outcome. Pending result requests,
read leases, send buffers and file handles have bounded global/per-principal
accounting. A read admitted before output expiry may finish within its
negotiated stream lifetime; its bytes remain charged and pinned until FIN or
abort. Revocation stops new reads and further scheduling on live reads;
already transmitted or transport-buffered bytes cannot be retracted.

Each output's locator uses the version-2 URI form in Section 11.6. It names
the same issuing authority and session, not an arbitrary storage-provider
credential. A consumer resolves its authority with server identity verification,
negotiates both profiles, authenticates as an authorized owner, attaches to
the session, and performs RESULT lookup/read with the manifest commitments.
Locators grant no access. Do not follow redirects, send credentials to another
authority, or dereference a caller-supplied URL without separate application
authorization. Cross-authority delegation and other storage schemes require
a separately specified profile; they are not implicit reference conformance.

## Sealed Closure, Counts and Shutdown

SCOPE operation 2 pages through a scope's declarations; operation 3 returns
its producer, parent, seal, total count and up to the requested number of IDs
strictly greater than `after-entity`, in increasing order. `more` means more
declared IDs exist beyond the returned page in that snapshot. An unsealed
scope may grow between requests. An empty page is not completeness evidence;
only its immutable seal establishes final membership.

A scope closes only when sealed, every declared member is terminal, and every
descendant scope has closed. A success rehydration under this profile's STRICT
policy additionally requires all children to succeed. Partial completion
policies are not inherited from version 1. A child scope with failure,
cancellation or skip prevents successful parent rehydration; the authority
settles the parent FAILED unless it already has another terminal outcome.
Retrying terminal failed logical work requires a new work identity and scope
membership, not rewriting the failed outcome.

The four counters in `v2-counts` count final SUCCEEDED, FAILED, CANCELLED and
SKIPPED members respectively. They are disjoint and their sum equals the
scope's declared count. ACTIVE, AWAITING_RETRY, WAITING_CHILDREN and CANCELLING
are not historical count buckets. An empty sealed scope has four zero counts.
Cancellation does not subtract from declared membership.

The status root uses SHA-256 with distinct domains. For each member in ascending
ID order, its leaf is SHA-256 of ASCII `pipestream-status-leaf-v2` followed by
deterministic CBOR of `[work-key, terminal-state, attempt, manifest-digest-or-null,
child-status-root-or-null]`. Terminal inputless cancellation/skip uses attempt
0; a child status root is present exactly when that work has a child scope.
Hash adjacent pairs as SHA-256 of ASCII `pipestream-status-node-v2` followed by
the two 32-octet hashes; duplicate an odd final hash at each level. The empty
root is SHA-256 of ASCII `pipestream-status-empty-v2`. A one-member root is
its leaf. The separately verified seal binds owner, membership and parent.
Status roots do not authenticate computation or replace payload commitments.

SCOPE operation 4 requests a checkpoint over an expected seal, including while
declared inputs are still missing. Before sealing it is NOT_READY; a different
seal is INTEGRITY_ERROR. Once ready, operation 5 returns the immutable summary
with scope, producer, parent, seal, declared count, counters, status root and
closure time. If its connection-local wait expires first, return WAIT_TIMEOUT,
not a successful summary. Reconnection begins a new wait against the same set.
The client verifies identity, seal, count partition and known commitments
before acknowledging coverage in its own durable observations.

DRAIN operation 0 requests completed-session shutdown. It contains the attached
generation and exact previously obtained root summary for scope 0, producer 0,
with null parent. The server requires that same committed root summary and no
pending admissions, checkpoints or result transfers on that connection, then
echoes operation 1. A child-scope cut or altered summary is CONFLICT; pending
work is NOT_READY. The client may then close with QUIC application error 0.
This does not expire the session or outputs.

Core also supports connection-only detach: DRAIN operation 2 refuses further
new requests on that connection and, after existing connection requests and
transfers drain, is acknowledged by operation 3. Detach makes no assertion
about durable work completion and is not a root checkpoint. If it cannot drain
within the negotiated stream lifetime, close without a completed-work claim.
Applications can disconnect without detach; reconnect must negotiate and
authenticate again. Neither detach nor abrupt disconnect cancels durable work.

## Lifetimes, Clocks and Crash-Safe Accounting

All timestamps are unsigned Unix milliseconds in 0..9223372036854775807.
Durations are milliseconds, not calendar units. Session policy separately
defines maximum execution duration, output retention and terminal-receipt
retention. A work's execution deadline is admission time plus its requested
execution duration, which cannot exceed the session maximum. Terminal receipt
and output deadlines are terminal commit time plus their respective policy
durations. Checked arithmetic overflow is LIMIT_EXCEEDED before commitment.
No retry, read, reconnect or duplicate operation extends any deadline.

Fresh object-read leases require trusted current time to check availability
and otherwise refuse CLOCK_UNSAFE. Diagnostic or retained-receipt lookup under
an unsafe clock grants no new lease or execution. All clock comparisons and
deadline additions use the schema's exact integer range, without saturation,
wraparound, floating-point conversion or locale-dependent parsing.

Active accepted work and its input/identity cannot expire out of the authority's
records. Passing a deadline schedules fenced settlement. Input remains retained
while any accepted callback, retry or descendant rehydration depends on it;
referenced child outputs remain internally pinned through dependent parent
settlement even if their externally promised output interval has elapsed.
Admission must fund those dependency pins. External expiry is not permission
to delete bytes required by accepted parent work.

Output and receipt deadlines are independent. A caller with a retained output
reference can still read an available object after the full work receipt has
expired; the authority retains the binding/manifest needed for that read. A
retained terminal receipt can describe EXPIRED output without claiming the
bytes remain available. After a receipt's deadline, WORK view or operation
lookup may refuse EXPIRED, but enough terminal/identity state remains to
prevent reuse and support any longer output or dependency promise.

An authority uses a trusted UTC clock for external timestamps and monotonic
elapsed timers within a process. It persists the greatest observed UTC value
in the same transactions that issue time-based promises. If the clock regresses,
new admissions, retries, publication and destructive expiry pause or refuse
CLOCK_UNSAFE until safe time is restored. Read-only retrieval of already
retained evidence may continue under current authorization, without promising
new availability or shortening retention. A restart with untrusted or invalid
clock state cannot evict records as expired. Implementations must document
their trusted-clock assumption and forward-jump handling; a monotonic clock
alone does not establish elapsed time across power loss.

Root closure and all dependent work/output/receipt/read-lease deadlines determine
session retirement. Do not retire an unsealed or unresolved session. Preserve
the full creation receipt until at least root closure time plus session receipt
retention and every longer work/output/dependency promise. Then a session can
be compacted to non-reusable history: the authority generation high-water mark
and each owner's creation high-water mark remain. Absent generations at or below
that mark cannot be created again. A known-owner retired creation is EXPIRED;
an attachment whose owner cannot be authorized is UNAUTHORIZED, not a disclosure
of another owner's historical state. Quotas never justify evicting a live promise.

Storage must atomically couple admission/jobs/receipts, attempt/lease fences,
terminal manifests, scope summaries and their resource reservations. Durable
payload installation precedes metadata references. A crash before metadata
commit leaves only bounded orphan storage; after commit it leaves replayable
authoritative state even if the acknowledgment was lost. Cleanup requires
exclusive ownership or an equivalent transactional liveness proof, verifies
references before deletion, preserves non-reuse commitments and read/dependency
pins, and is itself replayable after interruption. A successful restart must
reconcile accounting before admitting new capacity. Independent implementations
must test these boundaries against their real storage, not infer them from
the abstract models or a metadata-only unit test.
