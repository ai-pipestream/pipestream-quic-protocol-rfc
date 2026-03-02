# IANA Considerations

This document requests the creation of several new registries and one ALPN identifier registration. All registries defined in this section use the "Expert Review" policy {{RFC8126}} for new assignments unless otherwise stated.

## ALPN Identifier Registration

This document registers the following ALPN {{RFC7301}} protocol identifier:

Protocol:
:   PipeStream Version 1

Identification Sequence:
:   0x70 0x69 0x70 0x65 0x73 0x74 0x72 0x65 0x61 0x6D 0x2F 0x31 ("pipestream/1")

Specification:
:   This document

## PipeStream Frame Type Registry

IANA is requested to create the "PipeStream Frame Types" registry. Values are categorized into Fixed (type-sized, no length prefix) frames in 0x50-0x7F and Variable (4-octet length prefix) frames in 0x80-0xFF. Values 0xC0-0xFF are reserved for private use.

| Value | Frame Type Name | Class | Size | Layer | Reference |
|-------|-----------------|-------|------|-------|-----------|
| 0x50 | STATUS | Fixed | 16 octets base | 0 | Section 6.2 |
| 0x54 | SCOPE_DIGEST | Fixed | 72 octets | 1 | Section 6.3 |
| 0x55 | BARRIER | Fixed | 12 octets | 1 | Section 6.4 |
| 0x56 | GOAWAY | Fixed | 8 octets | 0 | Section 6.4a |
| 0x57-0x7F | Reserved | Fixed | - | - | this document |
| 0x80 | CAPABILITIES | Var | Length-prefixed | 0 | Section 3.4 |
| 0x81 | CHECKPOINT | Var | Length-prefixed | 0 | Section 9.3 |
| 0x82-0xBF | Reserved | Var | - | - | this document |

### Unknown Frame Handling

Receivers that encounter a Variable-class frame type (0x80-0xFF) that they do not recognize MUST skip the frame by reading and discarding the number of octets indicated by the 4-octet length prefix. Receivers that encounter an unknown Fixed-class frame type (0x50-0x7F) for which no size is defined MUST close the connection with PIPESTREAM_ENTITY_INVALID (0x05), since the frame size cannot be determined. Future specifications that register new Fixed-class frame types MUST define the frame size in the registry entry.

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

PipeStream error codes are used as QUIC application error codes in CONNECTION_CLOSE and RESET_STREAM frames. When closing a connection due to a PipeStream error, the endpoint MUST use the corresponding PipeStream error code value as the QUIC Application Error Code.

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

## PipeStream Serialization Format Registry

IANA is requested to create the "PipeStream Serialization Formats" registry. New entries require Expert Review {{RFC8126}}.

| Value | Name | Description | Reference |
|-------|------|-------------|-----------|
| 0 | CBOR | Concise Binary Object Representation | RFC 8949, this document |
| 1 | PROTOBUF | Protocol Buffers (see Appendix D) | this document |
| 2-255 | Reserved | Reserved for future use | this document |

## URI Scheme Registration

This section registers the "pipestream" URI scheme per {{RFC7595}}. The URI scheme identifies application context for detached or resumable resources (for example, Layer 2 yield/claim-check flows). PipeStream Layer 0 streaming semantics do not depend on this URI scheme.

Scheme name:
:   pipestream

Status:
:   Permanent

Applications/protocols that use this scheme:
:   PipeStream protocol (this document)

Contact:
:   Kristian Rickert (kristian.rickert@pipestream.ai)

Change controller:
:   IETF

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
