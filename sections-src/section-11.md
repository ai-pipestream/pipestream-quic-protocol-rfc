# IANA Considerations

This document requests the creation of several new registries and one ALPN identifier registration. All registries defined in this section use the "Expert Review" policy {{RFC8126}} for new assignments unless otherwise stated; guidance for the designated experts appears in Section 11.7.

## ALPN Identifier Registration

This document registers the following ALPN {{RFC7301}} protocol identifier:

Protocol:
:   PipeStream Version 1

Identification Sequence:
:   0x70 0x69 0x70 0x65 0x73 0x74 0x72 0x65 0x61 0x6D 0x2F 0x31 ("pipestream/1")

Specification:
:   This document

## PipeStream Frame Type Registry

IANA is requested to create the "PipeStream Frame Types" registry. All frames on Stream 0 MUST use a 4-octet length prefix following the 1-octet Type. The registry is partitioned into three ranges that determine both the registration policy and the framing class of the payload:

| Range | Framing Class | Registration Policy |
|-------|---------------|---------------------|
| 0x00-0x7F | Fixed bit-packed payload (Section 6) | Expert Review |
| 0x80-0xBF | Serialized payload in the negotiated format (Section 6.7) | Expert Review |
| 0xC0-0xFF | Private use | Not applicable |

Each registration consists of a value, a frame type name, the minimum protocol layer, a short description, and a reference to a specification. The initial contents are:

| Value | Frame Type Name | Layer | Description | Reference |
|-------|-----------------|-------|-------------|-----------|
| 0x50 | STATUS | 0 | Entity lifecycle status | Section 6.2 |
| 0x54 | SCOPE_DIGEST | 1 | Merkle completion summary | Section 6.3 |
| 0x55 | BARRIER | 1 | Subtree synchronization | Section 6.4 |
| 0x56 | GOAWAY | 0 | Graceful shutdown signal | Section 6.5 |
| 0x80 | CAPABILITIES | 0 | Negotiated limits/layers | Section 3.4 |
| 0x81 | CHECKPOINT | 0 | Global synchronization | Section 9.3 |
| 0x82 | CLAIM_REDEMPTION | 2 | Durable claim request or acknowledgement | Section 6.7.1 |

### Unknown Frame Handling

Receivers that encounter a frame type that they do not recognize MUST skip the frame by reading and discarding the number of octets indicated by the 4-octet length prefix. This mechanism ensures that future protocol extensions can be introduced without breaking backward compatibility for older implementations.

## PipeStream Status Code Registry

IANA is requested to create the "PipeStream Status Codes" registry. Status codes are 4-bit values (0x0-0xF). Values 0xD-0xE are reserved for Expert Review. Value 0xF is reserved for private use.

| Value | Name | Layer | Description |
|-------|------|-------|-------------|
| 0x0 | UNSPECIFIED | - | Default / heartbeat |
| 0x1 | PENDING | 0 | Entity announced |
| 0x2 | PROCESSING | 0 | In progress |
| 0x3 | COMPLETE | 0 | Success |
| 0x4 | FAILED | 0 | Failed |
| 0x5 | CHECKPOINT | 0 | Barrier |
| 0x6 | DEHYDRATING | 0 | Dehydrating into children |
| 0x7 | REHYDRATING | 0 | Rehydrating children |
| 0x8 | YIELDED | 2 | Paused |
| 0x9 | DEFERRED | 2 | Claim check issued |
| 0xA | RETRYING | 2 | Retry in progress |
| 0xB | SKIPPED | 2 | Intentionally skipped |
| 0xC | ABANDONED | 2 | Timed out |

## PipeStream Error Code Registry

IANA is requested to create the "PipeStream Error Codes" registry. Values in the range 0x00-0x3F are assigned by Expert Review. Values in the range 0x40-0xFF are reserved for private use.

PipeStream error codes are used as QUIC application error codes in CONNECTION_CLOSE and RESET_STREAM frames. When terminating a connection or aborting a stream due to a protocol-level error, the endpoint MUST use the corresponding PipeStream error code value as the QUIC Application Error Code.

| Value | Name | Description |
|-------|------|-------------|
| 0x00 | PIPESTREAM_NO_ERROR | Graceful shutdown |
| 0x01 | PIPESTREAM_INTERNAL_ERROR | Implementation error |
| 0x02 | PIPESTREAM_IDLE_TIMEOUT | Idle timeout |
| 0x03 | PIPESTREAM_CONTROL_RESET | Control stream must reset |
| 0x04 | PIPESTREAM_INTEGRITY_ERROR | Checksum failed |
| 0x05 | PIPESTREAM_ENTITY_INVALID | Invalid format or state |
| 0x06 | PIPESTREAM_ENTITY_TOO_LARGE | Size exceeded |
| 0x07 | PIPESTREAM_DEPTH_EXCEEDED | Scope depth exceeded |
| 0x08 | PIPESTREAM_WINDOW_EXCEEDED | Window full |
| 0x09 | PIPESTREAM_SCOPE_INVALID | Invalid scope |
| 0x0A | PIPESTREAM_CLAIM_EXPIRED | Claim check expired |
| 0x0B | PIPESTREAM_CLAIM_NOT_FOUND | Claim check not found |
| 0x0C | PIPESTREAM_LAYER_UNSUPPORTED | Protocol layer not supported |
| 0x0D | PIPESTREAM_FRAME_ERROR | Malformed frame or improper stream usage |

## PipeStream Serialization Format Registry

IANA is requested to create the "PipeStream Serialization Formats" registry. New entries require Expert Review {{RFC8126}}. A registration request MUST include a reference to a specification that normatively defines the encoding of every serialized PipeStream message (Section 3.4.2).

| Value | Name | Description | Reference |
|-------|------|-------------|-----------|
| 0 | CBOR | Concise Binary Object Representation | RFC 8949, this document |
| 1-255 | Unassigned | Available via Expert Review | |

## URI Scheme Registration

This section registers the "pipestream" URI scheme per {{RFC7595}}. The URI scheme identifies application context for detached or resumable resources (for example, Layer 2 yield/claim-check flows). PipeStream Layer 0 streaming semantics do not depend on this URI scheme.

Scheme name:
:   pipestream

Status:
:   Permanent

Applications/protocols that use this scheme:
:   PipeStream protocol (this document)

Scheme syntax:
:   See Section 11.6.1.

Scheme semantics:
:   A pipestream URI identifies an application-level session context within a PipeStream deployment and, optionally, a scope path and an entity reference within that context. It is used to locate detached or resumable resources, such as Layer 2 claim checks, potentially across transport connections. The authority component identifies the endpoint that issued the resource.

Encoding considerations:
:   All components other than the authority are restricted to a subset of unreserved ASCII characters by the ABNF in Section 11.6.1. Percent-encoding is neither required nor permitted in those components. The authority component follows the syntax and encoding rules of {{RFC3986}}, Section 3.2.

Interoperability considerations:
:   None beyond those described in this document.

Security considerations:
:   See Section 10 of this document. A pipestream URI may embed session and claim identifiers that function as bearer capabilities; such URIs SHOULD be treated as sensitive and MUST NOT be exposed in logs or referral contexts where they could enable claim redemption by unauthorized parties.

Contact:
:   Kristian Rickert (kristian.rickert@pipestream.ai)

Change controller:
:   IETF

References:
:   This document

### URI Syntax

The URI syntax is defined using ABNF {{RFC5234}}:

~~~~

pipestream-URI = "pipestream://" authority "/" session-id
                 [ "/" scope-path ] [ "/" entity-ref ]

session-id     = 1*( ALPHA / DIGIT / "-" )
scope-path     = scope-id *( "." scope-id )
scope-id       = 1*DIGIT
entity-ref     = 1*( ALPHA / DIGIT )
authority      = <authority, see RFC 3986, Section 3.2>
~~~~
{: type="ascii-art"}

Examples:

- `pipestream://processor.example.com/a1b2c3d4`
- `pipestream://processor.example.com:8443/a1b2c3d4/1.42/e5f6`

## Guidance for Designated Experts

For all registries defined in this section, the designated experts are advised to apply the following criteria when evaluating requests:

1. A permanent, readily available public specification of the proposed value is required and needs to define the value's semantics precisely enough for independent interoperable implementations.
2. The proposed value must not conflict with, duplicate, or create ambiguity with existing registrations, including the framing class of frame types (fixed bit-packed versus serialized payload; Section 11.2).
3. Requests should be evaluated for their effect on existing deployments; extensions that require behavior changes from unmodified endpoints are inappropriate for these registries and instead require a new protocol version (Section 3.4.1).
4. Assignments from scarce code spaces (in particular the 4-bit status code space) should be granted conservatively; the expert may require demonstrated deployment interest before assigning values from a scarce space.
