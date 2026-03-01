# PipeStream Protocol Specification: Sections 9 and 10

## 9. Security Considerations

This section describes security considerations for the PipeStream protocol. Implementers and deployers MUST carefully consider these aspects when implementing or deploying PipeStream-based systems.

### 9.1 Transport Security

#### 9.1.1 QUIC Transport Layer Security

PipeStream inherits its transport security properties from QUIC [RFC 9000] and TLS 1.3 [RFC 8446]. All PipeStream connections MUST use QUIC with TLS 1.3 or later. Implementations MUST NOT provide any mechanism to disable or downgrade transport encryption.

The security properties inherited from QUIC include:

- **Confidentiality**: All PipeStream frames, including Status frames, Checkpoint frames, and Entity frames, are encrypted using AEAD algorithms negotiated during the TLS handshake.

- **Integrity**: The QUIC packet protection mechanism provides integrity protection for all transmitted data.

- **Replay Protection**: QUIC's packet number space and anti-replay mechanisms prevent replay attacks on PipeStream sessions.

#### 9.1.2 Mandatory Encryption

Implementations MUST reject any attempt to establish unencrypted PipeStream connections. The 0-RTT feature of QUIC MAY be used for PipeStream connections, subject to the replay considerations defined in Section 8 of [RFC 9001]. Implementations SHOULD carefully evaluate whether the latency benefits of 0-RTT justify the replay risks for their specific use cases.

When 0-RTT is used, implementations MUST NOT send Entity frames containing sensitive payloads in 0-RTT data unless the application layer provides its own replay protection mechanism.

#### 9.1.3 Certificate-Based Authentication

PipeStream endpoints MUST authenticate using X.509 certificates as specified in [RFC 5280]. Server authentication is REQUIRED for all connections. Client authentication SHOULD be required in production deployments and MAY be enforced based on deployment policy.

Implementations MUST support certificate revocation checking via OCSP [RFC 6960] or CRLs [RFC 5280]. Implementations SHOULD support OCSP Stapling [RFC 6066] to reduce latency and privacy concerns associated with revocation checking.

Certificate validation MUST include:

1. Verification of the certificate chain to a trusted root certificate authority
2. Verification that the certificate has not expired
3. Verification that the certificate has not been revoked
4. Verification that the certificate is valid for the requested server name (for server certificates)

#### 9.1.4 Connection Migration Security

QUIC connection migration allows endpoints to change their network addresses while maintaining connection state. PipeStream implementations MUST implement the path validation mechanisms specified in Section 9 of [RFC 9000] to prevent connection hijacking attacks.

When a connection migrates, implementations MUST:

1. Validate the new path before sending Entity payloads on it
2. Maintain the integrity of any in-progress dehydration operations
3. Ensure that Checkpoint state remains consistent across migration events
4. Rate-limit migration events to prevent denial-of-service attacks

Implementations SHOULD log connection migration events for security monitoring purposes.

### 9.2 Application Security

#### 9.2.1 Entity Payload Integrity

Each Entity transmitted via PipeStream MUST include a SHA-256 checksum of its payload. The checksum MUST be computed over the raw payload bytes before any compression or encoding is applied.

The checksum is represented as a 32-octet value and MUST be included in the Entity frame header:

```
Entity Frame with Integrity Check {
  Frame Type (i) = 0x60,
  Entity ID (20),
  Parent ID (20),
  Layer (4),
  Payload Length (64),
  Checksum (256),
  Payload (..),
}
```

Receiving implementations MUST verify the checksum before processing the Entity payload. If checksum verification fails, the implementation MUST:

1. Discard the Entity payload
2. Send a PIPESTREAM_INTEGRITY_ERROR to the peer
3. Update the Assembly Manifest entry for this Entity to FAILED status
4. Log the integrity failure for security analysis

Implementations SHOULD consider repeated integrity failures from a peer as a potential attack indicator and MAY terminate the connection after a configurable threshold of failures.

#### 9.2.2 Entity Origin Authentication

In deployments where Entities may originate from multiple sources, implementations MUST support Entity-level origin authentication. This is accomplished through a cryptographic signature attached to each Entity frame.

The signature MUST be computed using one of the following algorithms:

- Ed25519 [RFC 8032]
- ECDSA with P-256 [RFC 6979]
- RSA-PSS with SHA-256 [RFC 8017] (minimum 2048-bit keys)

The signature covers the following fields, concatenated in order:

1. Entity ID (4 octets, big-endian)
2. Parent ID (4 octets, big-endian)
3. Layer Depth (4 octets, big-endian)
4. Payload Checksum (32 octets)

Implementations MUST maintain a registry of trusted signing keys and MUST reject Entities signed by untrusted keys. Key management and distribution is outside the scope of this specification but SHOULD follow established practices such as those defined in [RFC 7517] for JSON Web Keys.

#### 9.2.3 Assembly Manifest Tampering Prevention

The Assembly Manifest maintains authoritative state about Entity processing progress. Tampering with the Assembly Manifest could cause:

- Processing of already-processed Entities (waste of resources)
- Skipping of unprocessed Entities (data loss)
- Incorrect aggregation of results
- Violation of processing dependencies

To prevent tampering, implementations MUST:

1. **Authenticate Status Frame Updates**: All Status Frame updates MUST originate from authenticated peers. Implementations MUST verify the connection-level authentication before accepting status frame updates.

2. **Maintain Update Sequence Numbers**: Each status frame update MUST include a monotonically increasing sequence number. Implementations MUST reject updates with sequence numbers less than or equal to the last accepted update.

3. **Validate State Transitions**: Implementations MUST enforce valid state transitions as defined in Section 5 of this specification. Invalid state transitions MUST be rejected with PIPESTREAM_CONTROL_RESET.

4. **Compute Control Stream Digests**: Implementations SHOULD periodically compute and exchange cryptographic digests of control stream state to detect divergence. The digest MUST be computed as:

```
Control_Stream_Digest = SHA-256(Entry_1 || Entry_2 || ... || Entry_n)
```

Where entries are serialized in Entity ID order.

#### 9.2.4 Checkpoint Manipulation Attacks

Checkpoints provide recovery points for long-running dehydration operations. An attacker who can manipulate Checkpoint state could:

- Cause infinite processing loops by reverting to old Checkpoints
- Cause data loss by advancing past incomplete processing
- Exhaust storage resources by triggering excessive Checkpoint creation

Implementations MUST protect against these attacks by:

1. **Authenticating Checkpoint Operations**: Only the endpoint that initiated a dehydration operation SHOULD be able to create or restore Checkpoints for that operation. Checkpoint frames MUST include an authentication tag derived from the session keys.

2. **Limiting Checkpoint Frequency**: Implementations MUST enforce a minimum interval between Checkpoint creation operations. The default minimum interval SHOULD be 1 second. Implementations receiving Checkpoint requests more frequently than the configured limit MUST reject them with PIPESTREAM_CHECKPOINT_RATE_EXCEEDED.

3. **Validating Checkpoint Consistency**: Before restoring from a Checkpoint, implementations MUST verify that the Checkpoint state is consistent with any Entities that have been fully processed. Implementations MUST NOT restore to a Checkpoint that would re-process an Entity whose results have already been committed.

4. **Limiting Checkpoint History**: Implementations MUST limit the number of retained Checkpoints. The default limit SHOULD be 16 Checkpoints per dehydration session. Implementations MUST delete the oldest Checkpoint when this limit is exceeded.

### 9.3 Resource Exhaustion

Distributed document processing systems are susceptible to resource exhaustion attacks. This section defines requirements for preventing such attacks.

#### 9.3.1 Dehydration Depth Limits

Recursive Entity dehydration can create deeply nested processing hierarchies. Without limits, an attacker could craft documents that dehydrate into arbitrarily deep structures, exhausting stack space or other resources.

Implementations MUST enforce a maximum dehydration depth. The default maximum depth MUST be 32 layers. Implementations MAY allow configuration of a different limit but MUST NOT allow configuration above 256 layers.

When an Entity would exceed the maximum depth, implementations MUST:

1. Reject the dehydration operation
2. Set the Entity status to FAILED in the Assembly Manifest
3. Include the error code PIPESTREAM_DEPTH_EXCEEDED in the failure indication
4. Continue processing other Entities that do not exceed the depth limit

Implementations SHOULD emit a warning when dehydration depth exceeds 16 layers, as this may indicate a maliciously crafted document or a misconfigured processor.

#### 9.3.2 Entity Count Limits

An attacker could attempt to exhaust memory or processing resources by creating an excessive number of Entities within a single dehydration session.

Implementations MUST enforce a maximum Entity count per session. The default maximum SHOULD be 1,000,000 Entities. Implementations MAY configure higher limits based on available resources but MUST document the resource implications.

The Entity count limit applies to the total number of Entities created during a session, including:

- Top-level Entities received from peers
- Child Entities created through dehydration
- Entities in any processing state (PENDING, PROCESSING, COMPLETE, or FAILED)

When the Entity limit is reached, implementations MUST:

1. Reject creation of new Entities
2. Allow existing Entities to complete processing
3. Send PIPESTREAM_ENTITY_LIMIT_EXCEEDED to peers attempting to create new Entities
4. Resume accepting new Entities only after existing Entities complete and the count drops below the limit

#### 9.3.3 Checkpoint Timeout Requirements

Checkpoint state consumes storage resources. Without timeouts, abandoned Checkpoints could accumulate indefinitely.

Implementations MUST associate a timeout with each Checkpoint. The default timeout MUST be 3600 seconds (1 hour). Implementations MAY allow configuration of longer timeouts up to a maximum of 86400 seconds (24 hours).

When a Checkpoint timeout expires:

1. The Checkpoint MUST be deleted
2. Associated Entity state MAY be deleted if no other Checkpoint references it
3. If the associated dehydration session is still active, a warning SHOULD be logged

Implementations SHOULD provide mechanisms for extending Checkpoint timeouts for legitimate long-running operations. Such extensions MUST be authenticated and MUST NOT exceed the maximum timeout.

#### 9.3.4 Memory Bounds for Pending Entries

The Assembly Manifest may contain entries for Entities in PENDING state awaiting processing. Without bounds, an attacker could submit Entities faster than they can be processed, exhausting memory.

Implementations MUST enforce a maximum size for pending control stream entries. This limit SHOULD be expressed in terms of both:

- Maximum number of PENDING entries (default: 10,000)
- Maximum total bytes of PENDING entry metadata (default: 100 MB)

When either limit is reached, implementations MUST apply backpressure by:

1. Sending PIPESTREAM_FLOW_CONTROL to the peer
2. Delaying acknowledgment of new Entity frames
3. Optionally, reducing the QUIC flow control window for Entity streams

Implementations MUST NOT drop PENDING entries to accommodate new entries. Implementations MUST resume normal operation when pending entries drop below 80% of the configured limits.

### 9.4 Privacy Considerations

This section describes privacy-relevant aspects of PipeStream that implementations and deployers should consider.

#### 9.4.1 Entity Metadata Exposure

Entity frames contain metadata that may reveal information about the documents being processed:

- **Entity IDs**: Sequential Entity IDs may reveal the number of documents processed over time.
- **Layer Depth**: The depth value reveals structural information about documents.
- **Payload Length**: The size of Entity payloads may reveal document characteristics.
- **Parent-Child Relationships**: The hierarchy of Entities reveals document structure.

While payload contents are encrypted by QUIC, this metadata is visible to any endpoint handling the Entity. Implementations that require privacy protection for this metadata SHOULD consider:

1. Using randomized Entity IDs within a session
2. Padding payloads to fixed sizes or size classes
3. Introducing dummy Entities to obscure processing patterns
4. Using separate connections for sensitive and non-sensitive processing

Network observers can perform traffic analysis even on encrypted connections. Deployers concerned about traffic analysis SHOULD consider additional mitigations such as VPNs, Tor, or traffic shaping.

#### 9.4.2 Timing Attacks on Processing

The time required to process an Entity may reveal information about its contents. For example:

- Documents containing certain patterns may take longer to parse
- Encryption or decryption of sensitive sections may have measurable timing
- Dehydration into more child Entities takes longer than simple processing

Implementations SHOULD NOT make security decisions based on timing of remote processing operations. Implementations that process sensitive documents SHOULD consider:

1. Adding random delays to processing operations
2. Performing processing in constant time where feasible
3. Batching status updates to obscure individual processing times
4. Using dedicated resources to avoid timing variation from resource contention

#### 9.4.3 Control Stream Information Leakage

The control stream reveals detailed information about processing progress:

- The rate of status updates reveals processing speed
- The pattern of FAILED statuses may reveal document characteristics
- The Checkpoint pattern reveals processing structure

In multi-tenant deployments, implementations MUST ensure that control stream information for one tenant is not visible to other tenants. This requires:

1. Separate QUIC connections or streams for each tenant
2. Authentication and authorization of all control stream access
3. Encryption of control stream state at rest if stored persistently

Implementations SHOULD provide configuration options to reduce control stream verbosity for privacy-sensitive deployments, including:

- Aggregated status updates rather than per-Entity updates
- Delayed status updates to obscure timing
- Omission of optional metadata fields

---

## 10. IANA Considerations

This document requests several registrations from IANA. All registrations in this section follow the guidelines established in [RFC 8126].

### 10.1 ALPN Identifier Registration

This document requests the registration of an Application-Layer Protocol Negotiation (ALPN) protocol identifier for PipeStream, as specified in [RFC 7301].

#### 10.1.1 Registration Request

IANA is requested to add the following entry to the "TLS Application-Layer Protocol Negotiation (ALPN) Protocol IDs" registry:

| Protocol | Identification Sequence | Reference |
|----------|------------------------|-----------|
| PipeStream Version 1 | 0x70 0x69 0x70 0x65 0x73 0x74 0x72 0x65 0x61 0x6d 0x2f 0x31 ("pipestream/1") | [this document] |

#### 10.1.2 Usage

The "pipestream/1" ALPN identifier MUST be used for all PipeStream version 1 connections. Implementations MUST include this identifier in the TLS ClientHello and MUST verify that the server selects this identifier in the ServerHello.

Future versions of PipeStream will register separate ALPN identifiers (e.g., "pipestream/2") and will define procedures for version negotiation.

### 10.2 PipeStream Frame Type Registry

This document establishes a new IANA registry for PipeStream frame types.

#### 10.2.1 Registry Definition

**Registry Name**: PipeStream Frame Types

**Registration Procedure**:
- 0x00-0x3F: Standards Action
- 0x40-0x7F: Specification Required
- 0x80-0xFF: Expert Review

**Initial Contents**:

| Value | Frame Type Name | Reference |
|-------|-----------------|-----------|
| 0x50 | STATUS | [this document], Section 5.1 |
| 0x51 | CHECKPOINT | [this document] |
| 0x52 | STATUS_ACK | [this document] |
| 0x53 | CHECKPOINT_ACK | [this document] |
| 0x54 | SCOPE_DIGEST | [this document] |
| 0x55 | BARRIER | [this document] |
| 0x56 | SCOPE_OPEN | [this document] |
| 0x57 | SCOPE_CLOSE | [this document] |
| 0x60 | ENTITY | [this document] |
| 0x61 | ENTITY_START | [this document] |
| 0x62 | ENTITY_CONTINUATION | [this document] |
| 0x63 | ENTITY_END | [this document] |
| 0x70 | CLAIM_CHECK_QUERY | [this document] |
| 0x71 | CLAIM_CHECK_RESPONSE | [this document] |
| 0x72 | COMPLETION_POLICY | [this document] |

#### 10.2.2 Provisional Registrations

The following frame type values are reserved for experimental use and MUST NOT be used in production deployments:

| Value Range | Purpose |
|-------------|---------|
| 0xF0-0xFE | Experimental Use |
| 0xFF | Reserved |

#### 10.2.3 Registration Requirements

Requests to register frame types in the 0x40-0x7F range MUST include:

1. A specification document describing the frame format
2. A description of when the frame is sent and how it is processed
3. Security considerations specific to the frame type

Requests for Expert Review (0x80-0xFF) MUST include items 1 and 2 above. The designated expert(s) will evaluate whether the registration could negatively impact protocol security or interoperability.

### 10.3 PipeStream Status Code Registry

This document establishes a new IANA registry for PipeStream entity status codes.

#### 10.3.1 Registry Definition

**Registry Name**: PipeStream Status Codes

**Registration Procedure**:
- 0x00-0x0F: Standards Action
- 0x10-0x7F: Specification Required
- 0x80-0xFF: Private Use

**Initial Contents**:

| Value | Status Code Name | Description | Reference |
|-------|------------------|-------------|-----------|
| 0x0 | PENDING | Entity announced, not yet transmitting | [this document] |
| 0x1 | PROCESSING | Entity transmission in progress | [this document] |
| 0x2 | COMPLETE | Entity successfully processed | [this document] |
| 0x3 | FAILED | Entity processing failed | [this document] |
| 0x4 | CHECKPOINT | Synchronization barrier | [this document] |
| 0x5 | DEHYDRATING | Decomposing into children | [this document] |
| 0x6 | REHYDRATING | Rehydrating children | [this document] |
| 0x7 | Reserved | Reserved | [this document] |
| 0x8 | YIELDED | Paused with continuation token | [this document] |
| 0x9 | DEFERRED | Detached with claim check | [this document] |
| 0xA | RETRYING | Retry in progress | [this document] |
| 0xB | SKIPPED | Intentionally skipped (lenient mode) | [this document] |
| 0xC | ABANDONED | Timed out, cursor advanced past | [this document] |
| 0xD-0xF | Reserved | Reserved for future use | [this document] |

#### 10.3.2 Status Code Semantics

Status codes in this registry represent states in the Entity processing lifecycle. Implementations MUST support all status codes in the 0x00-0x07 range. Implementations SHOULD ignore unrecognized status codes in the 0x10-0x7F range. Implementations MUST NOT send status codes from the 0x80-0xFF range to peers unless prior agreement has been established through an extension mechanism.

### 10.4 PipeStream Error Code Registry

This document establishes a new IANA registry for PipeStream error codes.

#### 10.4.1 Registry Definition

**Registry Name**: PipeStream Error Codes

**Registration Procedure**:
- 0x00-0x3F: Standards Action
- 0x40-0xFF: Specification Required

**Initial Contents**:

| Value | Name | Description |
|-------|------|-------------|
| 0x00 | PIPESTREAM_NO_ERROR | Graceful shutdown |
| 0x01 | PIPESTREAM_INTERNAL_ERROR | Implementation error |
| 0x02 | PIPESTREAM_IDLE_TIMEOUT | Idle timeout |
| 0x03 | PIPESTREAM_CONTROL_RESET | Control stream must reset |
| 0x04 | PIPESTREAM_INTEGRITY_ERROR | Checksum failed |
| 0x05 | PIPESTREAM_ENTITY_INVALID | Invalid format |
| 0x06 | PIPESTREAM_ENTITY_TOO_LARGE | Size exceeded |
| 0x07 | PIPESTREAM_DEPTH_EXCEEDED | Scope depth exceeded |
| 0x08 | PIPESTREAM_WINDOW_EXCEEDED | Window full |
| 0x09 | PIPESTREAM_SCOPE_INVALID | Invalid scope |
| 0x0A | PIPESTREAM_CLAIM_EXPIRED | Claim check expired |
| 0x0B | PIPESTREAM_CLAIM_NOT_FOUND | Claim check not found |
| 0x0C | PIPESTREAM_LAYER_UNSUPPORTED | Protocol layer not supported |
| 0x12-0x3F | Reserved | Reserved for future standards use | [this document] |

#### 10.4.2 Error Code Usage

Error codes are used in CONNECTION_CLOSE and RESET_STREAM frames at the QUIC layer, as well as in PipeStream-specific error indication frames.

When reporting errors, implementations MUST:

1. Use the most specific applicable error code
2. Include additional diagnostic information where available
3. Log error occurrences for operational monitoring

Implementations MUST NOT include sensitive information in error messages that could be exploited by attackers.

### 10.5 PipeStream Extension Type Registry

This document establishes a new IANA registry for PipeStream extension types.

#### 10.5.1 Registry Definition

**Registry Name**: PipeStream Extension Types

**Registration Procedure**:
- 0x00-0x1F: Standards Action
- 0x20-0x7F: Specification Required
- 0x80-0xFE: Expert Review
- 0xFF: Reserved

**Initial Contents**:

| Value | Extension Type Name | Applicable Frames | Reference |
|-------|---------------------|-------------------|-----------|
| 0x00 | Reserved | N/A | [this document] |
| 0x01 | ASSEMBLY_MANIFEST | STATUS | [this document], Section 5 |
| 0x02 | CHECKPOINT | CHECKPOINT | [this document], Section 6 |
| 0x03 | ENTITY_SIGNATURE | ENTITY | [this document], Section 9.2.2 |
| 0x04 | ENTITY_COMPRESSION | ENTITY | [this document] |
| 0x05 | STATUS_ENCRYPTION | STATUS | [this document], Section 9.4.3 |
| 0x06 | PRIORITY | ENTITY, STATUS | [this document] |
| 0x07 | TRACE | ENTITY, STATUS, CHECKPOINT | [this document] |
| 0x08-0x1F | Reserved | N/A | [this document] |

#### 10.5.2 Extension Frame Format

Extensions are carried in an extension block appended to the base frame:

```
Extension Block {
  Extension Count (i),
  Extensions (..) ...,
}

Extension {
  Extension Type (i),
  Extension Length (i),
  Extension Data (..),
}
```

#### 10.5.3 Assembly Manifest Extension (0x01)

The Assembly Manifest Extension carries additional metadata for status frame entries:

```
Assembly Manifest Extension Data {
  Entry Count (i),
  Entries (..) ...,
}

Assembly Manifest Entry {
  Entity ID (i),
  Parent ID (i),
  Status Code (8),
  Timestamp (64),
  Processor ID (i),
  Metadata Length (i),
  Metadata (..),
}
```

This extension MUST only be attached to STATUS frames. Implementations MUST support this extension.

#### 10.5.4 Checkpoint Extension (0x02)

The Checkpoint Extension carries checkpoint-specific metadata:

```
Checkpoint Extension Data {
  Checkpoint ID (i),
  Creation Timestamp (64),
  Expiry Timestamp (64),
  Entity Count (i),
  Control Stream Digest (256),
  Application Data Length (i),
  Application Data (..),
}
```

This extension MUST only be attached to CHECKPOINT frames. Implementations MUST support this extension.

#### 10.5.5 Registration Requirements

Requests to register extension types MUST include:

1. A specification document describing the extension format
2. A list of frame types to which the extension may be attached
3. Processing requirements for implementations
4. Security considerations specific to the extension

### 10.6 URI Scheme Registration

This document registers a new URI scheme for PipeStream resource identification.

#### 10.6.1 Registration Request

IANA is requested to register the "pipestream" URI scheme in the "Uniform Resource Identifier (URI) Schemes" registry:

| Field | Value |
|-------|-------|
| Scheme Name | pipestream |
| Status | Permanent |
| Applications/Protocols | PipeStream Protocol |
| Contact | [Authors] |
| Change Controller | IETF |
| Reference | [this document] |

#### 10.6.2 URI Syntax

The PipeStream URI scheme follows this syntax:

```
pipestream-URI = "pipestream://" authority "/" session-id ["/" entity-id]

authority     = host [":" port]
session-id    = 1*HEXDIG
entity-id     = 1*HEXDIG
```

Examples:
- `pipestream://processor.example.com/a1b2c3d4`
- `pipestream://processor.example.com:8443/a1b2c3d4/e5f6`

---

## References

### Normative References

- [RFC 2119] Bradner, S., "Key words for use in RFCs to Indicate Requirement Levels", BCP 14, RFC 2119, DOI 10.17487/RFC2119, March 1997.
- [RFC 5280] Cooper, D., et al., "Internet X.509 Public Key Infrastructure Certificate and Certificate Revocation List (CRL) Profile", RFC 5280, DOI 10.17487/RFC5280, May 2008.
- [RFC 6066] Eastlake 3rd, D., "Transport Layer Security (TLS) Extensions: Extension Definitions", RFC 6066, DOI 10.17487/RFC6066, January 2011.
- [RFC 6960] Santesson, S., et al., "X.509 Internet Public Key Infrastructure Online Certificate Status Protocol - OCSP", RFC 6960, DOI 10.17487/RFC6960, June 2013.
- [RFC 7301] Friedl, S., et al., "Transport Layer Security (TLS) Application-Layer Protocol Negotiation Extension", RFC 7301, DOI 10.17487/RFC7301, July 2014.
- [RFC 8017] Moriarty, K., et al., "PKCS #1: RSA Cryptography Specifications Version 2.2", RFC 8017, DOI 10.17487/RFC8017, November 2016.
- [RFC 8032] Josefsson, S. and I. Liusvaara, "Edwards-Curve Digital Signature Algorithm (EdDSA)", RFC 8032, DOI 10.17487/RFC8032, January 2017.
- [RFC 8126] Cotton, M., et al., "Guidelines for Writing an IANA Considerations Section in RFCs", BCP 26, RFC 8126, DOI 10.17487/RFC8126, June 2017.
- [RFC 8446] Rescorla, E., "The Transport Layer Security (TLS) Protocol Version 1.3", RFC 8446, DOI 10.17487/RFC8446, August 2018.
- [RFC 9000] Iyengar, J., Ed. and M. Thomson, Ed., "QUIC: A UDP-Based Multiplexed and Secure Transport", RFC 9000, DOI 10.17487/RFC9000, May 2021.
- [RFC 9001] Thomson, M., Ed. and S. Turner, Ed., "Using TLS to Secure QUIC", RFC 9001, DOI 10.17487/RFC9001, May 2021.

### Informative References

- [RFC 6979] Pornin, T., "Deterministic Usage of the Digital Signature Algorithm (DSA) and Elliptic Curve Digital Signature Algorithm (ECDSA)", RFC 6979, DOI 10.17487/RFC6979, August 2013.
- [RFC 7517] Jones, M., "JSON Web Key (JWK)", RFC 7517, DOI 10.17487/RFC7517, May 2015.

---

## Authors' Addresses

Kristian Rickert
PipeStream AI
Email: kristian.rickert@pipestream.ai
