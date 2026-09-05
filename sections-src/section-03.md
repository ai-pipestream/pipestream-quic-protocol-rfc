# Protocol Layers

PipeStream defines three protocol layers that build upon each other. This layered approach allows simple deployments to use only the core protocol while complex deployments can leverage advanced features.

## Layer 0: Core Protocol

Layer 0 provides the fundamental streaming capabilities:

- Unified Control Frame (UCF) header (1-octet type + 4-octet length prefix)
- Status frame (16-octet base bit-packed frame)
- Entity frame (header + payload)
- Status codes: PENDING, PROCESSING, COMPLETE, FAILED, CHECKPOINT
- Assembly Manifest for parent-child tracking
- Cursor-based Entity ID recycling
- Single-level dehydrate/rehydrate
- Checkpoint blocking

All implementations MUST support Layer 0.

## Layer 1: Recursive Extension

Layer 1 adds hierarchical processing capabilities:

- Scoped Entity ID namespaces (root -> component -> sub-task -> leaf)
- Explicit Depth tracking in status frames
- SCOPE_DIGEST for Merkle-based subtree completion
- BARRIER for subtree-scoped synchronization
- Nested dehydration with depth tracking

Layer 1 is OPTIONAL. Implementations advertise Layer 1 support during capability negotiation.

## Layer 2: Resilience Extension

Layer 2 adds fault tolerance and async processing:

- YIELDED status with continuation tokens
- DEFERRED status with claim checks
- RETRYING, SKIPPED, ABANDONED statuses
- Completion policies (STRICT, LENIENT, BEST_EFFORT, QUORUM)
- Claim check extensions and deferred processing tokens
- Stopping point validation

Layer 2 is OPTIONAL and requires Layer 1. Implementations advertise Layer 2 support during capability negotiation.

## Capability Negotiation

PipeStream uses a two-tier negotiation model. The ALPN identifier (Section 11.1) identifies the base PipeStream transport mapping, while the `capabilities` structure handles dynamic resource limits and optional layer support that may vary based on endpoint configuration or real-time load.

During CONNECT, endpoints exchange supported capabilities using the `capabilities` structure. This message MUST be encoded using the default CBOR format for both the client's initiation and the server's response (Section 3.4.2).

~~~~ cddl
serialization-format = uint .le 255
                       ; Value from the PipeStream Serialization
                       ; Formats registry (Section 11.5).
                       ; This document defines only CBOR (0).

capabilities = {
  layer0-core: bool,               ; MUST be true
  layer1-recursive: bool,          ; Scoped IDs, digests
  layer2-resilience: bool,         ; Yield, claim checks
  ? max-scope-depth: uint .le 7,   ; Default: 7 (8 levels, 0-7)
  ? max-entities-per-scope: uint,  ; Default: 4,294,967,292
                                   ; (Section 9.1)
  ? max-window-size: uint,         ; Default: 2,147,483,646
                                   ; (Max in-flight entities;
                                   ; see Section 9.1)
  ? serialization-format: serialization-format, ; Default: CBOR
  ? keepalive-timeout-ms: uint,    ; Default: 30000 (30s)
  ? supported-extensions: extension-list, ; Default: []
  ? required-extensions: extension-list,  ; Default: []
}

extension-id = 1..65534
extension-list = [0*32 extension-id]
~~~~

Peers negotiate down to common capabilities. If Layer 2 is requested but Layer 1 is not supported, Layer 2 MUST be disabled.

The `layer0-core` field MUST be true; an endpoint that receives a
Capabilities message with `layer0-core` set to false MUST close the
connection with PIPESTREAM_LAYER_UNSUPPORTED (0x0C). The
`max-entities-per-scope` and `max-window-size` values are bounded by
the Entity ID space constraints defined in Section 9.1; an endpoint
MUST NOT advertise values exceeding those bounds.

### Version Negotiation

PipeStream protocol versioning is carried in two places: the ALPN identifier and the Ver field in the STATUS frame (Section 6.2.1). The ALPN identifier `pipestream/1` identifies the major protocol version and the QUIC transport mapping defined in this document. The 4-bit Ver field in STATUS frames carries the value 0x1 for this specification.

A future major version of PipeStream (e.g., `pipestream/2`) would register a new ALPN identifier. QUIC's native ALPN negotiation during the TLS handshake provides version selection: if a client offers both `pipestream/2` and `pipestream/1` and the server supports only `pipestream/1`, the TLS handshake selects `pipestream/1` without additional round trips. This mechanism is consistent with the versioning approach used by HTTP/3 {{RFC9114}} and DNS over QUIC {{RFC9250}}.

Minor, backward-compatible extensions do not require a new ALPN identifier. Such extensions use newly registered frame or status values and explicit capability negotiation as specified by the extension. The maps defined by this document are closed: an endpoint that receives an unrecognized map member before negotiating an extension that defines it MUST close the connection with PIPESTREAM_FRAME_ERROR (0x0D). An extension therefore cannot add an optional member to a core map without also defining how support for that member is negotiated.

The initial CAPABILITIES map remains closed even when extensions are
offered. Extension-specific members MUST NOT appear in this exchange.
The identifier lists in Section 3.4.3 bootstrap negotiation; any additional
parameters use messages defined by the activated extension after CONNECT.

### Serialization Format Negotiation

The serialization_format field determines the encoding used for all variable-length control messages (frame types 0x80-0xFF) and entity headers.

This document defines a single serialization format: CBOR {{RFC8949}}, value 0 in the PipeStream Serialization Formats registry (Section 11.5). All PipeStream implementations MUST support CBOR. Every CBOR message MUST use the deterministic encoding requirements of Section 4.2 of {{RFC8949}}: definite-length items, the shortest form for integers and lengths, and deterministic map-key ordering. A receiver MUST reject a non-deterministically encoded PipeStream CBOR message with PIPESTREAM_FRAME_ERROR (0x0D). This gives every message one golden byte representation and makes conformance results independent of a library's default encoder settings.

The `serialization-format` capability field and its registry exist to permit future specifications to define additional formats; any such specification MUST normatively define the encoding of every serialized message in this document.

Determinism applies to the map actually transmitted, before defaults are
applied. Omission of an optional member and explicit transmission of its
default value are both valid, distinct maps. Receivers MUST NOT reject an
omitted optional member because re-encoding a default-filled object would
insert that member. Duplicate map keys, non-minimal integers or lengths,
indefinite-length items, and trailing CBOR items are invalid. Floating-point
values follow the shortest exact representation rule in Section 8.4.

Negotiation proceeds as follows:

1. CBOR {{RFC8949}} is the default and MUST be supported by all endpoints.
2. Each peer advertises its preferred `serialization-format` in its Capabilities message.
3. If both peers advertise the same format and both support it, that format is used.
4. If preferences differ, if either peer omits the field, or if a peer advertises a value the other does not recognize, the peers MUST fall back to CBOR {{RFC8949}}.

The initial Capabilities exchange on a new connection MUST use the default CBOR format for both the client's initiation and the server's response. The negotiated serialization format and resource limits take effect immediately following the successful completion of this initial Capabilities exchange (one request and one response). If a peer cannot decode the initial Capabilities exchange, it MUST close the connection with PIPESTREAM_FRAME_ERROR (0x0D).

### Extension Negotiation

`supported-extensions` and `required-extensions` are arrays of identifiers
from Section 11.9. Omission means an empty array. Each array MUST contain
at most 32 identifiers in strictly increasing numeric order, with no
duplicates. Values 0 and 65535 are reserved and MUST NOT appear. Every
required identifier MUST also appear in the sender's supported array.
Invalid types, lengths, ordering, or membership cause
PIPESTREAM_FRAME_ERROR (0x0D). These semantic constraints supplement CDDL.

The client offers the extensions it implements and is willing to activate.
The server computes the intersection with its own enabled supported set.
If either endpoint's required set is not contained in this intersection,
the server MUST close with PIPESTREAM_EXTENSION_UNSUPPORTED (0x0F),
without acknowledging capabilities or admitting application work.
Unknown optional identifiers have no effect and MUST NOT be selected.

In the response, `supported-extensions` is the selected intersection,
not the server's entire supported set. `required-extensions` is the union
of both required sets. The client MUST verify that the response selects
only offered identifiers and includes every client-required identifier.
A missing required selection causes PIPESTREAM_EXTENSION_UNSUPPORTED;
an unsolicited selection or omitted required-set echo causes
PIPESTREAM_FRAME_ERROR. A response MUST NOT enable an unoffered layer,
enable Layer 2 without Layer 1, increase an offered resource limit or
keepalive timeout, or select a serialization format outside Section 3.4.2.
The client MUST reject such a response with PIPESTREAM_FRAME_ERROR.

Only the selected identifiers become active, after the server sends and
the client validates the response. Neither endpoint may send
extension-dependent work before this point. An endpoint MUST NOT
advertise an identifier merely because its codec can parse the identifier.
It MUST implement that extension's complete mandatory behavior. Layer
booleans remain independent promises and MUST NOT stand for partial
implementations of a layer. An extension profile that reuses a layer's
messages without implementing the entire layer must define its own
activation conditions, permitted messages, and refusal behavior.

An extension specification MUST identify its prerequisite layers and
extensions, resource bounds, state transitions, and incompatible
combinations. If a selected combination cannot be activated, the server
MUST refuse CONNECT with PIPESTREAM_EXTENSION_UNSUPPORTED. It MUST NOT
silently drop required behavior. No extensions are assigned by this draft.

CAPABILITIES is exchanged once per connection. A subsequent CAPABILITIES
frame causes PIPESTREAM_FRAME_ERROR; reconnect to renegotiate. Every
connection, including one resuming durable work, negotiates independently.
The application MUST require all capabilities needed by the resumed work.
Skipping an unknown frame does not constitute extension activation.

Earlier drafts have closed capability maps without these list members.
Sending either member to such a peer can therefore fail CONNECT. Empty
lists SHOULD be omitted for compatibility. A client MUST NOT silently
retry without required extensions following any negotiation failure.
TLS protects the exchange in transit, but cannot make a peer's advertised
implementation truthful. Applications still need authorization and
conformance evidence for security-sensitive extensions.
