# Security Considerations

## Transport Security

PipeStream uses the QUIC transport {{RFC9000}} and QUIC TLS mapping
{{RFC9001}}, with TLS 1.3 {{RFC9846}}. Implementations MUST follow the
QUIC TLS mapping, including ALPN and peer authentication. Implementations
MUST NOT provide mechanisms to disable encryption. TLS is not a separate
record-layer wrapper around UDP datagrams.

A client MUST validate the server's certificate chain and verify its service
identity under {{RFC9525}} before sending any PipeStream application frame.
The reference identity MUST come from the client's configured target or an
authenticated discovery mechanism, not from a name asserted by the unverified
peer. DNS targets use dNSName subjectAltName entries; IP-literal targets use
iPAddress subjectAltName entries. Implementations MUST NOT fall back to the
certificate subject's Common Name. Wildcards in DNS identities are supported
only as a complete left-most label matching exactly one reference label.
Certificate trust alone does not establish the intended service identity.

PipeStream frames MUST NOT be sent or processed in 0-RTT early data (Section 5.3), which removes the replay exposure that early data would otherwise introduce for capability negotiation and status frames.

## Entity Payload Integrity

Each Entity SHOULD include a SHA-256 {{FIPS-180-4}} checksum in its EntityHeader (the `checksum` field defined in Section 6.8.2). The checksum is OPTIONAL in the wire format to accommodate zero-length entities, streamed entities whose final length is unknown at header-emission time, and scenarios where application-layer integrity mechanisms provide equivalent guarantees. When a checksum is present, it MUST be exactly 32 octets containing the SHA-256 digest computed over the raw payload bytes (the octet sequence following the EntityHeader on the Entity Stream). The checksum does not cover the EntityHeader itself.

For chunked entities (where `chunk-info` is present in the EntityHeader), each chunk MAY carry its own per-chunk checksum. The checksum in the first chunk's EntityHeader, if present, MUST cover only that chunk's payload bytes. An implementation that requires whole-entity integrity verification MUST either compute a rolling digest across all chunks or require the sender to transmit a final summary entity containing the whole-payload checksum.

To support true streaming of large entities, implementations MAY begin processing an entity payload before the complete payload has been received and verified. However, the final rehydration or terminal SINK operation MUST NOT be committed until the complete payload checksum has been verified.

If a checksum verification fails, the implementation MUST:

1. Reject the entity with PIPESTREAM_INTEGRITY_ERROR (0x04).
2. Discard any partial results or temporary state associated with the entity.
3. Propagate the failure according to the Completion Policy (Section 8.3).

Implementations that require immediate consistency SHOULD buffer the entire entity and verify the checksum before initiating processing.

### Algorithm Agility

This specification mandates SHA-256 {{FIPS-180-4}} as the sole checksum algorithm for both payload integrity (this section) and Merkle tree construction (Section 9.5). SHA-256 is well-studied and widely deployed; however, future developments may necessitate migration to a different algorithm.

PipeStream supports algorithm migration through the capability negotiation mechanism (Section 3.4). A future specification MAY define additional fields in the `capabilities` structure to advertise supported checksum algorithms, following the general principles outlined in {{RFC7696}}. Until such negotiation is defined, all implementations MUST use SHA-256 when producing or verifying checksums. An implementation that receives a checksum of a length other than 32 octets MUST reject the entity with PIPESTREAM_INTEGRITY_ERROR (0x04).

The `checksum` field in the EntityHeader is typed as `bstr .size 32` in the CDDL schema (Appendix C). A future algorithm negotiation extension would need to update this constraint, the SCOPE_DIGEST Merkle root size, and the corresponding IANA registry entries.

## Resource Exhaustion

| Limit | Default | Description |
|-------|---------|-------------|
| Max scope depth | 7 | Prevents recursive bombs (8 levels: 0-7) |
| Max entities per scope | 4,294,967,292 | Memory bounds (Section 9.1) |
| Max window size | 2,147,483,646 | Max in-flight entities (Section 9.1) |
| Checkpoint timeout | 30s | Prevents stuck state |
| Claim check expiry | 86400s | Garbage collection |

Implementations MUST enforce all resource limits listed above. Exceeding any limit MUST result in the corresponding error code (see Section 11.4). Implementations SHOULD allow operators to configure stricter limits than the defaults shown here.

Durable implementations MUST account for retained protocol state, not only
unfinished work. Completion, refusal, revocation, or connection loss MUST NOT
release a storage charge for state that remains retained. Quota exhaustion
MUST NOT produce an acknowledgment for a rejected state transition, remove
declared obligations, or evict an unexpired recovery receipt. Implementations
SHOULD reserve capacity for the completion records of admitted work. Logical
state-byte limits do not bound temporary files, retained payloads, database
journals, indexes, or other physical storage overhead; implementations need
separate resource policies for those objects.

When an implementation reserves completion capacity, unrelated admissions MUST
NOT consume that capacity. Waiting on child scopes or losing a connection does
not itself make the reservation unused. Converting a reservation into retained
completion state MUST preserve the applicable storage charge. Implementations
MUST distinguish reserved completion capacity from admission limits for new
descendants and payloads; reserving a parent's completion does not promise
unlimited future work or exempt it from those limits.

Where completion reservations depend on bounded application results, an
implementation MUST make those bounds available to the application before
dispatch and enforce them before committing the resulting state. A result
exceeding its admission budget MUST NOT be represented as successful completion.
Retaining an explicit refusal also requires capacity; such a refusal does not
discharge the entity's unresolved obligations.

To prevent memory-exhaustion attacks, implementations MUST NOT pre-allocate memory for variable-length payloads based solely on the 32-bit Length field in the UCF header (Section 6.1.1). Memory MUST be allocated incrementally as octets are received, or capped at a smaller initial buffer until the message type and context are verified.

The entity-count and window defaults are identifier-space maxima, not safe
memory allocations. Receivers MUST additionally bound aggregate buffered
octets, incomplete streams, metadata, chunks, and pending checkpoints per
connection and per authenticated principal. Per-chunk limits alone are
insufficient: every arriving chunk MUST be charged to the entity and
connection budgets before buffering. A budget failure MUST NOT silently
drop a child or convert an incomplete checkpoint into success.

Identity, parent linkage, negotiated depth and count limits, payload
integrity, and authorization MUST be checked before invoking application
callbacks with irreversible effects. Speculative incremental processing
MAY operate on admitted data only if effects can be discarded on failure.
Control readers MUST remain able to make progress when a data stream
stalls. Implementations SHOULD separate control parsing, payload reception,
worker completion, and deadline handling as described in {{RFC9308}}.
Queues between these activities MUST have bounded capacity. Offloading a
blocking operation MUST NOT allow cancelled waiters to release its resource
charge while the operation still runs. A full control backlog MUST produce a
named resource refusal, not discard a parsed request or suspend a checkpoint
clock indefinitely behind a storage operation.

## Amplification Attacks

A single dehydration operation can produce an arbitrary number of child entities from a small input, creating a potential amplification vector. To mitigate this:

1. Implementations MUST enforce the max_entities_per_scope limit negotiated during capability exchange (Section 3.4). Any dehydration that would exceed this limit MUST be rejected.

2. Implementations MUST enforce the max_scope_depth limit. A dehydration chain deeper than this limit MUST be rejected with PIPESTREAM_DEPTH_EXCEEDED (0x07).

3. Implementations SHOULD enforce a configurable ratio between input entity size and total child entity count. A recommended default is no more than 1,000 children per megabyte of parent payload.

4. The backpressure mechanism (Section 9.1) provides a natural throttle: when the in-flight window fills, no new Entity IDs can be assigned until existing entities complete and the cursor advances. Implementations MUST NOT bypass backpressure for dehydration-generated entities.

## Privacy Considerations

PipeStream entity headers and control stream frames carry metadata that may reveal information about the entities being processed, even when payloads are encrypted at the application layer:

1. **Entity structure leakage**: The number of child entities produced by dehydration, the scope depth, and the Entity ID assignment pattern may reveal the structure of the input being processed (e.g., an entity that dehydrates into 50 children is likely a multi-part input). Implementations that require structural privacy SHOULD pad dehydration counts or use fixed decomposition granularity. Deployments that do not handle privacy-sensitive data MAY omit this padding.

2. **Metadata in headers**: The `content_type`, `metadata` map, and `payload_length` fields in EntityHeader (Section 6.8.2) are transmitted in cleartext within the QUIC-encrypted stream. Implementations that require metadata confidentiality beyond transport encryption SHOULD encrypt EntityHeader fields at the application layer and use an opaque content_type such as `application/octet-stream`. This overhead is unnecessary when the deployment operates within a trusted network.

3. **Traffic analysis**: The timing and size of status frames on the Control Stream may correlate with processing patterns. Implementations operating in privacy-sensitive environments SHOULD send status frames at fixed intervals with padding to obscure processing timing. Deployments in trusted environments MAY omit traffic padding to reduce bandwidth overhead.

4. **Identifiers**: Application-level input identifiers and filenames carried inside profile-defined payload envelopes are not interpreted by PipeStream Core but may still be logged by intermediate processing nodes. Implementations SHOULD provide mechanisms to redact or pseudonymize such identifiers at pipeline boundaries. This recommendation may be relaxed when all nodes in the pipeline are operated by the same administrative entity.

## Replay and Token Reuse

### Authentication and Authorization

A claim ID is a public lookup identifier, not a bearer credential. Neither
its randomness nor a state checksum grants access. A deployment MUST
authenticate a principal before admitting durable work or redeeming a
claim, and MUST authorize that principal for the session, entity, and
issuing authority. Server authentication alone does not authenticate the
requesting principal. Mutual TLS is one possible profile mechanism;
another mechanism requires an explicit authenticated application profile.

The issuer MUST retain the principal binding and authorization policy with
the durable claim record and apply revocation and expiry before accepting
redemption. Execution of accepted work remains authorized and fenced as below;
Section 10.6.5 distinguishes receipt retention from a claim's acceptance expiry.
Session names supplied in entity metadata MUST NOT override that binding.
A receiving authority MUST NOT accept a claim issued by an unrelated
authority solely because it has the same numeric ID. Profiles using bearer
credentials MUST carry a separate secret with at least 128 bits of
unpredictability, transported confidentially and excluded from locators.

The issuer's clock determines expiry. A reconnect does not extend claim
lifetime. Retention and maximum lifetime are deployment policy, and the
advertised expiry MUST be bounded by the issuer's retention commitment.
The default maximum lifetime is 86400 seconds.

Implementations MUST serialize acceptance of recovery results for a claim
across concurrent executors, including after reconnection. A lock local to
one connection is insufficient. Each attempt MUST have a durable fence;
publication MUST atomically verify that the attempt is still authorized
and current before committing the result and completion state. A stale
attempt MUST NOT overwrite a successor's result or produce a successful
completion acknowledgment.

When a lease is used, expiry MUST invalidate publication by that attempt,
even if no successor has acquired the work. Reacquisition MUST advance the
durable fence without reusing a prior value. Clock rollback MUST NOT make
a superseded fence valid. A lease is not a mechanism for stopping an old
worker: callbacks can overlap after expiry or a network partition, and
revocation cannot undo an external effect already committed. Applications
MUST provide idempotency or transactional fencing at external-effect sinks.
This protocol does not guarantee exactly-once external execution.

### Yield Token Replay

Yield tokens (Section 6.6.2) contain opaque continuation state that enables resumption of paused entity processing. A replayed yield token could cause an entity to be processed multiple times or to resume from a stale state. To prevent this:

1. Implementations MUST associate each yield token with a stable application context identifier (for example, a session identifier) and Entity ID. In Layer 0-only operation, this context MAY be implicit in the active transport connection. For Layer 2 resumptions that can occur across reconnects or different nodes, the context identifier MUST remain stable across transport connections. A yield token MUST be rejected if presented in a different context than the one that issued it, unless the token was explicitly transferred via a claim check.

2. Implementations MUST invalidate a yield token after it has been consumed for resumption. A second resumption attempt with the same token MUST be rejected.

3. The StoppingPointValidation (Section 9.7) provides integrity checking at resume time. Implementations MUST verify the `state_checksum` field before accepting a resumed entity. If the checksum does not match the current state, the resumption MUST be rejected and the entity MUST be reprocessed from the beginning.

### Claim Check Replay

Claim checks (Section 6.6.3) are long-lived references that can be redeemed in different sessions. To prevent misuse:

1. Each claim check carries an `expiry_timestamp` (Unix epoch microseconds). Implementations MUST reject expired claim checks. Cross-connection redemption uses the CLAIM_REDEMPTION frame defined in Section 6.7.1.

2. Implementations MUST track redeemed claim check IDs and reject duplicate redemptions. The tracking state MUST persist for at least the claim check expiry duration.

3. Claim check IDs MUST be generated using a cryptographically secure random number generator to prevent guessing.

4. Because the claim check identifier space is 64 bits, an online attacker with sufficient query volume could attempt to enumerate valid identifiers. Implementations SHOULD rate-limit claim redemption attempts per peer and SHOULD treat repeated redemption failures as a signal of probing.

5. A claim issuer MUST durably bind the claim ID to its session identifier,
Entity identity, expiry, continuation state, and stopping-point checksum before
announcing DEFERRED. Deployments MUST deliver the session identifier and
stopping-point checksum to the redeeming application through an authenticated
application context. The fixed Claim Check status extension intentionally does
not disclose that additional context.

### Authenticated Session Binding (Private-Use Profile)

The `authenticated-session-v1` profile uses private-use extension identifier
65282 (0xFF02) by explicit agreement. It can accompany the sealed-work profile
in Section 9.8 or another negotiated lifecycle. It does not itself activate
Layer 2, change claim replay semantics, or provide retained recovery outcomes.
This is an authentication binding, not a complete resilience profile.

Both endpoints MUST require this extension on a connection using this profile.
The server MUST require and verify a client certificate during the QUIC TLS
handshake. Verification includes proof of possession, the configured trust
chain, certificate validity, and client authentication usage. Anonymous clients
MUST NOT be accepted. The client MUST authenticate the server as required by
the QUIC TLS mapping. Supplying a client certificate is not sufficient evidence
that the server enforces caller identity; successful extension negotiation is
also required. Refusal MUST NOT trigger an anonymous retry.
If TLS session resumption is enabled, the implementation MUST reapply the
current credential validity and authorization policy, or refuse resumption
and require a full handshake. Cached authentication MUST NOT bypass expiry
or removal of the identity mapping.

The deployment MUST explicitly map the verified certificate to a stable
principal. A trusted certificate without an authorized mapping is refused
with PIPESTREAM_UNAUTHORIZED (0x10). An implementation MAY use configured
SHA-256 fingerprints of complete DER leaf certificates for this mapping;
the fingerprint is an identifier, not a replacement for certificate and
signature verification. Operators may map rotated certificates to the same
principal. Removing an identity mapping affects subsequent authentication;
immediate denial of existing work requires revoking that session's access.

Before acknowledging the first durable admission or declaration, the server
MUST atomically bind the session to its authenticated principal and configured
issuing authority. Claims inherit this durable session binding. The server
MUST verify the binding before revealing retained state, admitting further
work, or executing a claim. Metadata, producer labels, and numeric claim IDs
cannot override it. The authority identifier is deployment configuration, not
a string chosen by a request. This binding alone does not define portable
claim transfer between independent authorities.

An existing anonymous session MUST NOT be converted implicitly. A bound
session MUST NOT be served by an anonymous connection or by another principal
or authority, even if an operator exposes the same database through another
listener. Such access is PIPESTREAM_UNAUTHORIZED. Denial responses MUST NOT
include private session contents or claim state.

Session-access revocation MUST be durable and apply to live and reconnected
clients. Authorization MUST be checked in the transaction that admits or
commits work, not only at connection establishment. Background recovery MUST
also check the retained binding, current authority policy, and revocation
before execution. These checks do not cancel an external effect already
committed before revocation; asynchronous executors additionally require
fencing and application idempotency under Section 10.6.1.

### Retained Authenticated Recovery (Private-Use Profile)

The `authenticated-recovery-v1` profile uses private-use extension identifier
65283 (0xFF03) by explicit agreement. Both endpoints MUST require this extension
and `authenticated-session-v1` (65282). It requires Layer 2 and excludes the
sealed-work profile. An incompatible selection MUST fail CONNECT with
PIPESTREAM_EXTENSION_UNSUPPORTED; a client MUST reject an invalid response.
No anonymous or legacy-redemption fallback is permitted. These values are
draft identifiers, not IANA assignments.

RECOVERY uses UCF type 0x84 and the `recovery-frame` schema in Appendix C.
A request has flags 0 and contains an authority, session ID, nonzero
16-octet request ID, nonzero 64-bit claim ID, and 32-octet stopping-point
checksum. The authority uses the principal-binding identifier syntax:
1..128 ASCII alphanumeric or `-._~` characters. Session syntax is defined in
Section 11.6.1. The client MUST persist a unique request ID and its complete
request before first transmission, MUST reuse that request after an ambiguous
disconnect, and MUST NOT reuse its ID for different work in the same session.
The request ID is a correlation label, not a credential.

The receiver MUST authenticate and authorize the current principal against
the durable session owner and configured authority before revealing a receipt
or claim state. A request's authority cannot select or override the listener's
authority. Session and claim revocation apply to initial acceptance, replay,
and fenced execution publication. Revocation MUST be durable and irreversible
for that identity. It does not retract effects already committed.

For a new request, the receiver MUST check the claim's expiry and stopping-point
checksum, then atomically commit claim redemption, a restartable resume job,
and an immutable acceptance receipt. A claim already redeemed by another
request, or through CLAIM_REDEMPTION, is PIPESTREAM_CLAIM_NOT_FOUND. Invalid
checksums are PIPESTREAM_INTEGRITY_ERROR. Queue or receipt capacity exhaustion
is PIPESTREAM_LIMIT_EXCEEDED and MUST leave all three records uncommitted.
A refusal without an acceptance receipt is not evidence of successful work.

After that commit, the receiver returns RECOVERY with flags 1, echoes every
request field, and includes the admitted entity and scope IDs, `accepted-at`,
and `retain-until`. Timestamps are unsigned Unix microseconds.
`retain-until` MUST equal `accepted-at` plus 86400000000 microseconds
(24 hours); arithmetic overflow is refused. All four receipt-only fields MUST
be absent in requests and present in receipts and terminal outcomes. Flags are
0 for a request, 1 for a receipt, or 2 for a terminal outcome; other values are
reserved. Unknown fields and malformed encodings are PIPESTREAM_FRAME_ERROR.

An identical request before `retain-until` MUST return the identical receipt,
including after reconnect or server restart, without redeeming again,
enqueueing another job, or changing the original retention deadline.
Changing any field under a retained request ID is PIPESTREAM_ENTITY_INVALID.
The client MUST compare all echoed request fields and validate the receipt
fields; a mismatched receipt MUST close the connection rather than report
acceptance. A connection remains bound to one session.

The receipt acknowledges durable admission, not successful processing.
Claim expiry prevents new acceptance; it does not invalidate a receipt already
committed before expiry or cancel its accepted job. Accepted jobs follow their
durable execution lifecycle, authorization checks, and publication fences.
Replaying a receipt is not a new execution grant. After receipt delivery, the
server returns RECOVERY with flags 2 only when a terminal job outcome is durably
committed. It echoes the complete receipt and adds an `outcome` discriminator:
0 means successful resume completion, and 1 means application refusal. Refusal
additionally requires `failure-code` (the unsigned 32-bit diagnostic code) and
`failure-detail` (at most 512 UTF-8 octets). Both fields MUST be absent for
success. The discriminator, not a diagnostic code, determines success. A
transport error or disconnect MUST NOT substitute for this correlated refusal.
Unknown outcome values are PIPESTREAM_FRAME_ERROR. The client MUST compare
every receipt field in the outcome against its accepted receipt; mismatches
are PIPESTREAM_ENTITY_INVALID and MUST close the connection.

Pending work produces no terminal outcome. After reconnect, an identical request
replays its receipt and then the same committed terminal outcome when available.
These outcome records MUST be retained for the receipt's promised interval and
MUST NOT change after publication. A refusal does not resolve the entity as
successful, remove declared obligations, or authorize an application retry.
This profile uses the correlated terminal frame, not uncorrelated STATUS
transitions, for recovery completion. A client MUST consume the terminal frame
or reconnect before issuing another recovery request on its connection.

A replay at or after `retain-until` is PIPESTREAM_CLAIM_EXPIRED, not a new
request. An issuer clock earlier than `accepted-at` MUST NOT produce a
receipt. Implementations MUST retain the complete receipt through its promised
interval and retain sufficient request-ID and redeemed-claim history afterward
to prevent reuse. Compaction cannot convert an expired receipt into fresh
work. Retention does not override revocation. Resource limits MAY refuse new
requests, but MUST NOT evict an unexpired receipt to admit another.

An unexpected RECOVERY on a connection without this extension is
PIPESTREAM_EXTENSION_UNSUPPORTED. A receiver selecting this profile MUST
refuse CLAIM_REDEMPTION on that connection; the legacy frame keeps its
single-use semantics on other connections. A successful reconnect only
restores the ability to retrieve retained outcomes. It does not imply
completion, automatic retry of refused work, or exactly-once external effects.

## Encryption Key Management

When using FileStorageReference with encryption:

1. Key IDs MUST reference keys in approved providers.
2. Wrapped keys MUST use approved envelope encryption.
3. Key rotation MUST be supported via key_id versioning.
4. Implementations MUST NOT log key material.
5. Implementations MUST NOT include unwrapped data encryption keys in EntityHeader metadata or Control Stream frames.
