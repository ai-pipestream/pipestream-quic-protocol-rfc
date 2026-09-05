# IANA Considerations

This document requests the creation of several new registries and one ALPN identifier registration. All registries defined in this section use the "Specification Required" policy {{RFC8126}} for new assignments unless otherwise stated; guidance for the designated experts appears in Section 11.7.

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
| 0x00-0x7F | Fixed bit-packed payload (Section 6) | Specification Required |
| 0x80-0xBF | Serialized payload in the negotiated format (Section 6.7) | Specification Required |
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

IANA is requested to create the "PipeStream Status Codes" registry. Status codes are 4-bit values (0x0-0xF). Values 0xD-0xE are reserved for Specification Required. Value 0xF is reserved for private use.

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

IANA is requested to create the "PipeStream Error Codes" registry. Values in the range 0x00-0x3F are assigned by Specification Required. Values in the range 0x40-0xFF are reserved for private use.

PipeStream error codes are used as QUIC application error codes in CONNECTION_CLOSE and RESET_STREAM frames. When terminating a connection or aborting a stream due to a protocol-level error, the endpoint MUST use the corresponding PipeStream error code value as the QUIC Application Error Code.

| Value | Name | Description |
|-------|------|-------------|
| 0x00 | PIPESTREAM_NO_ERROR | Graceful shutdown |
| 0x01 | PIPESTREAM_INTERNAL_ERROR | Implementation error |
| 0x02 | PIPESTREAM_IDLE_TIMEOUT | Idle timeout |
| 0x03 | PIPESTREAM_CONTROL_RESET | Control stream must reset |
| 0x04 | PIPESTREAM_INTEGRITY_ERROR | Checksum failed |
| 0x05 | PIPESTREAM_ENTITY_INVALID | Invalid format or state |
| 0x06 | PIPESTREAM_LIMIT_EXCEEDED | Payload or aggregate resource limit exceeded |
| 0x07 | PIPESTREAM_DEPTH_EXCEEDED | Scope depth exceeded |
| 0x08 | PIPESTREAM_WINDOW_EXCEEDED | Window full |
| 0x09 | PIPESTREAM_SCOPE_INVALID | Invalid scope |
| 0x0A | PIPESTREAM_CLAIM_EXPIRED | Claim check expired |
| 0x0B | PIPESTREAM_CLAIM_NOT_FOUND | Claim check not found |
| 0x0C | PIPESTREAM_LAYER_UNSUPPORTED | Protocol layer not supported |
| 0x0D | PIPESTREAM_FRAME_ERROR | Malformed frame or improper stream usage |
| 0x0E | PIPESTREAM_CHECKPOINT_TIMEOUT | Pending checkpoint deadline expired |
| 0x0F | PIPESTREAM_EXTENSION_UNSUPPORTED | Required extension or selected combination unavailable |
| 0x10-0x3F | Unassigned | Available for registration |
| 0x40-0xFF | Private Use | Requires explicit agreement |

The registry has an 8-bit value space. QUIC application error codes have
a larger space; values above 0xFF are reserved by this mapping and MUST
NOT be emitted without a future specification. An unrecognized error
code still terminates the affected connection or stream; it MUST NOT
be interpreted as success.

## PipeStream Serialization Format Registry

IANA is requested to create the "PipeStream Serialization Formats" registry. New entries require Specification Required {{RFC8126}}. A registration request MUST include a reference to a specification that normatively defines the encoding of every serialized PipeStream message (Section 3.4.2).

| Value | Name | Description | Reference |
|-------|------|-------------|-----------|
| 0 | CBOR | Concise Binary Object Representation | RFC 8949, this document |
| 1-255 | Unassigned | Available via Specification Required | |

## URI Scheme Registration

This section requests registration of the "pipestream" URI scheme per
{{RFC7595}}. Registration has not occurred merely because this draft
uses the scheme. It identifies session, entity, and durable claim
resources; Layer 0 streaming does not require a URI.

Scheme name:
:   pipestream

Status:
:   Permanent

Applications/protocols that use this scheme:
:   PipeStream protocol (this document)

Scheme syntax:
:   See Section 11.6.1.

Scheme semantics:
:   A locator names a session, a scope-qualified entity, or a durable claim
    at an issuing authority. Tagged paths distinguish these resource types.
    A locator is not a command and grants no permission. Its application
    profile selects the operation, including CLAIM_REDEMPTION for a claim.

Encoding considerations:
:   Components are ASCII. Internationalized DNS names use their ASCII
    A-label form. Percent-encoding, userinfo, query, and fragment components
    are prohibited. IPv6 literals use brackets without zone identifiers.

Interoperability considerations:
:   An explicit UDP port is required; this draft assigns no default port
    or discovery service. Resolution uses the authority's DNS name or IP
    address and QUIC with ALPN pipestream/1. The endpoint identity MUST be
    verified for that authority. Locators do not imply an HTTP GET operation.

Security considerations:
:   See Section 10. Locators MUST NOT contain bearer secrets or replace
    authentication and authorization. Session and claim identifiers can
    still expose sensitive correlation information and SHOULD be redacted
    from public logs. An endpoint MUST NOT follow an untrusted locator to
    another authority without application authorization for that destination.

Contact:
:   Kristian Rickert (kristian.rickert@pipestream.ai)

Change controller:
:   IETF

References:
:   This document

### URI Syntax

The URI syntax uses ABNF {{RFC5234}} and the IP address productions from
{{RFC3986}}, Section 3.2.2:

~~~~

pipestream-URI = "pipestream://" ps-authority "/sessions/" session-id
                 [ entity-path / claim-path ]
ps-authority  = ( dns-name / IPv4address / "[" IPv6address "]" )
                ":" decimal
session-id    = 1*128( ALPHA / DIGIT / "-" / "_" )
entity-path   = "/scopes/" decimal "/entities/" decimal
claim-path    = "/claims/" decimal
decimal       = "0" / ( %x31-39 *DIGIT )
dns-name      = dns-label *( "." dns-label )
dns-label     = alphanum [ *61( alphanum / "-" ) alphanum ]
alphanum      = ALPHA / DIGIT
; IPv4address and IPv6address are from RFC 3986, Section 3.2.2.
~~~~
{: type="ascii-art"}

The port MUST be in 1..65535; scope IDs in 0..4294967295; entity IDs in
1..4294967292; and claim IDs in 1..18446744073709551615. Decimal components
MUST NOT contain leading zeros except for the single value `0`. DNS names
are limited to 253 characters. Scheme and DNS name comparison is
case-insensitive; session identifiers and path tags are case-sensitive.
An entity locator is valid only for the lifetime of its session identity;
the recycling issue in Appendix E prohibits treating a recycled slot as
a durable application identifier. URI length is limited to 1024 octets.

Examples:

- `pipestream://processor.example:9443/sessions/job-1`
- `pipestream://processor.example:9443/sessions/job-1/scopes/7/entities/42`
- `pipestream://[2001:db8::1]:9443/sessions/job-1/claims/99`

## Guidance for Designated Experts

For all registries defined in this section, the designated experts are advised to apply the following criteria when evaluating requests:

1. A permanent, readily available public specification of the proposed value is required and needs to define the value's semantics precisely enough for independent interoperable implementations.
2. The proposed value must not conflict with, duplicate, or create ambiguity with existing registrations, including the framing class of frame types (fixed bit-packed versus serialized payload; Section 11.2).
3. Requests should be evaluated for their effect on existing deployments; extensions that require behavior changes from unmodified endpoints are inappropriate for these registries and instead require a new protocol version (Section 3.4.1).
4. Assignments from scarce code spaces (in particular the 4-bit status code space) should be granted conservatively; the expert may require demonstrated deployment interest before assigning values from a scarce space.

A request MUST identify the requested value or range, name, contact,
minimum layer and required capability, precise wire encoding, state
transitions, error scope, security considerations, and a permanent public
specification. It SHOULD include interoperability vectors. Private-use
values convey no agreement without an explicit application profile.

## Yield Reason Registry

IANA is requested to create the 8-bit "PipeStream Yield Reasons" registry.
Values 0x00-0xBF use Specification Required. Values 0xC0-0xFF are Private Use.

| Value | Name | Reference |
|-------|------|-----------|
| 0x00 | UNSPECIFIED | Section 6.6.2 |
| 0x01 | EXTERNAL_CALL | Section 6.6.2 |
| 0x02 | RATE_LIMITED | Section 6.6.2 |
| 0x03 | AWAITING_SIBLING | Section 6.6.2 |
| 0x04 | AWAITING_APPROVAL | Section 6.6.2 |
| 0x05 | RESOURCE_BUSY | Section 6.6.2 |
| 0x06-0xBF | Unassigned | |
| 0xC0-0xFF | Private Use | |

An unknown yield reason does not change the YIELDED lifecycle state or
authorize automatic retries. An unknown status code has no defined
state transition; absent a negotiated extension defining it, the receiver
MUST reject it with PIPESTREAM_ENTITY_INVALID (0x05).

## Extension Identifier Registry

IANA is requested to create the 16-bit "PipeStream Extension Identifiers"
registry, used by the supported and required lists in Section 3.4.3.
An identifier names one immutable extension contract. An incompatible
revision requires a new identifier; an identifier is not a version range.

| Value | Use | Registration Policy |
|-------|-----|---------------------|
| 0 | Reserved | Not assignable |
| 1-65279 | Unassigned | Specification Required |
| 65280-65534 | Private Use | Explicit agreement |
| 65535 | Reserved | Not assignable |

Registrations include an identifier, name, contact, permanent specification,
prerequisite layers and extensions, incompatible combinations, and
security considerations. The specification must define all mandatory
behavior, wire values, failure handling, and test vectors. Registration
of an extension does not allocate its frame types or status values;
those require their own registry entries. Designated experts apply
Section 11.7 and check that activation is unambiguous and dependencies
can be evaluated during CONNECT. Private-use values MUST NOT be used
without prior agreement on the same contract and are not IANA assignments.
