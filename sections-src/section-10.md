# Security Considerations

## Transport Security

PipeStream inherits security from QUIC {{RFC9000}} and TLS 1.3 {{RFC8446}}. All connections MUST use TLS 1.3 or later. Implementations MUST NOT provide mechanisms to disable encryption.

## Entity Payload Integrity

Each Entity MUST include a SHA-256 checksum in its EntityHeader. 

To support true streaming of large entities, implementations MAY begin processing an entity payload before the complete payload has been received and verified. However, the final rehydration or terminal SINK operation MUST NOT be committed until the complete payload checksum has been verified. 

If a checksum verification fails, the implementation MUST:
1. Reject the entity with PIPESTREAM_INTEGRITY_ERROR (0x04).
2. Discard any partial results or temporary state associated with the entity.
3. Propagate the failure according to the Completion Policy (Section 8.3).

Implementations that require immediate consistency SHOULD buffer the entire entity and verify the checksum before initiating processing.

## Resource Exhaustion

| Limit | Default | Description |
|-------|---------|-------------|
| Max scope depth | 7 | Prevents recursive bombs (8 levels: 0-7) |
| Max entities per scope | 4,294,967,294 | Memory bounds |
| Max window size | 2,147,483,648 | Backpressure threshold |
| Checkpoint timeout | 30s | Prevents stuck state |
| Claim check expiry | 86400s | Garbage collection |

Implementations MUST enforce all resource limits listed above. Exceeding any limit MUST result in the corresponding error code (see Section 11.4). Implementations SHOULD allow operators to configure stricter limits than the defaults shown here.

## Amplification Attacks

A single dehydration operation can produce an arbitrary number of child entities from a small input, creating a potential amplification vector. To mitigate this:

1. Implementations MUST enforce the max_entities_per_scope limit negotiated during capability exchange (Section 3.4). Any dehydration that would exceed this limit MUST be rejected.

2. Implementations MUST enforce the max_scope_depth limit. A dehydration chain deeper than this limit MUST be rejected with PIPESTREAM_DEPTH_EXCEEDED (0x07).

3. Implementations SHOULD enforce a configurable ratio between input entity size and total child entity count. A recommended default is no more than 1,000 children per megabyte of parent payload.

4. The backpressure mechanism (Section 9.1) provides a natural throttle: when the in-flight window fills, no new Entity IDs can be assigned until existing entities complete and the cursor advances. Implementations MUST NOT bypass backpressure for dehydration-generated entities.

## Privacy Considerations

PipeStream entity headers and control stream frames carry metadata that may reveal information about the documents being processed, even when payloads are encrypted at the application layer:

1. **Document structure leakage**: The number of child entities produced by dehydration, the scope depth, and the Entity ID assignment pattern may reveal the structure of the document being processed (e.g., a document that dehydrates into 50 children is likely a multi-page document). Implementations that require structural privacy SHOULD pad dehydration counts or use fixed decomposition granularity.

2. **Metadata in headers**: The `content_type`, `metadata` map, and `payload_length` fields in EntityHeader (Section 6.7) are transmitted in cleartext within the QUIC-encrypted stream. Implementations that require metadata confidentiality beyond transport encryption SHOULD encrypt EntityHeader fields at the application layer and use an opaque content_type such as `application/octet-stream`.

3. **Traffic analysis**: The timing and size of status frames on the Control Stream may correlate with document processing patterns. Implementations operating in privacy-sensitive environments SHOULD send status frames at fixed intervals with padding to obscure processing timing.

4. **Identifiers**: The `doc_id` field in PipeDoc (Section 7.1) and filenames in BlobBag entries are application-layer data but may be logged by intermediate processing nodes. Implementations SHOULD provide mechanisms to redact or pseudonymize identifiers at pipeline boundaries.

## Replay and Token Reuse

### Yield Token Replay

Yield tokens (Section 6.5.1) contain opaque continuation state that enables resumption of paused entity processing. A replayed yield token could cause an entity to be processed multiple times or to resume from a stale state. To prevent this:

1. Implementations MUST associate each yield token with a stable application context identifier (for example, a session identifier) and Entity ID. In Layer 0-only operation, this context MAY be implicit in the active transport connection. For Layer 2 resumptions that can occur across reconnects or different nodes, the context identifier MUST remain stable across transport connections. A yield token MUST be rejected if presented in a different context than the one that issued it, unless the token was explicitly transferred via a claim check.

2. Implementations MUST invalidate a yield token after it has been consumed for resumption. A second resumption attempt with the same token MUST be rejected.

3. The StoppingPointValidation (Section 9.6) provides integrity checking at resume time. Implementations MUST verify the `state_checksum` field before accepting a resumed entity. If the checksum does not match the current state, the resumption MUST be rejected and the entity MUST be reprocessed from the beginning.

### Claim Check Replay

Claim checks (Section 6.5.2) are long-lived references that can be redeemed in different sessions. To prevent misuse:

1. Each claim check carries an `expiry_timestamp` (Unix epoch microseconds). Implementations MUST reject expired claim checks.

2. Implementations MUST track redeemed claim check IDs and reject duplicate redemptions. The tracking state MUST persist for at least the claim check expiry duration.

3. Claim check IDs MUST be generated using a cryptographically secure random number generator to prevent guessing.

## Encryption Key Management

When using FileStorageReference with encryption:

1. Key IDs MUST reference keys in approved providers.
2. Wrapped keys MUST use approved envelope encryption.
3. Key rotation MUST be supported via key_id versioning.
4. Implementations MUST NOT log key material.
5. Implementations MUST NOT include unwrapped data encryption keys in EntityHeader metadata or Control Stream frames.
