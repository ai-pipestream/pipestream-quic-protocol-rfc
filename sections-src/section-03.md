# Protocol Layers

PipeStream defines three protocol layers that build upon each other. This layered approach allows simple deployments to use only the core protocol while complex deployments can leverage advanced features.

## Layer 0: Core Protocol

Layer 0 provides the fundamental streaming capabilities:

- Unified Control Frame (UCF) header (1-octet type)
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

- Scoped Entity ID namespaces (collection -> document -> part -> job)
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

During CONNECT, endpoints exchange supported capabilities using the `capabilities` structure. This message MUST be encoded using the default CBOR format for both the client's initiation and the server's response (Section 3.5).

~~~~ cddl
serialization-format = &(
  cbor: 0,                         ; Default (IETF native)
  protobuf: 1,
)

capabilities = {
  layer0-core: bool,               ; Always true
  layer1-recursive: bool,          ; Scoped IDs, digests
  layer2-resilience: bool,         ; Yield, claim checks
  ? max-scope-depth: uint .le 7,   ; Default: 7 (8 levels, 0-7)
  ? max-entities-per-scope: uint,  ; Default: 4,294,967,294
  ? max-window-size: uint,         ; Default: 2,147,483,648 (2^31)
                                   ; (Max in-flight entities)
  ? serialization-format: serialization-format, ; Default: CBOR
  ? keepalive-timeout-ms: uint,    ; Default: 30000 (30s)
}
~~~~

Peers negotiate down to common capabilities. If Layer 2 is requested but Layer 1 is not supported, Layer 2 MUST be disabled.

### Version Negotiation

PipeStream protocol versioning is carried in two places: the ALPN identifier and the Ver field in the STATUS frame (Section 6.2.1). The ALPN identifier `pipestream/1` identifies the major protocol version and the QUIC transport mapping defined in this document. The 4-bit Ver field in STATUS frames carries the value 0x1 for this specification.

A future major version of PipeStream (e.g., `pipestream/2`) would register a new ALPN identifier. QUIC's native ALPN negotiation during the TLS handshake provides version selection: if a client offers both `pipestream/2` and `pipestream/1` and the server supports only `pipestream/1`, the TLS handshake selects `pipestream/1` without additional round trips. This mechanism is consistent with the versioning approach used by HTTP/3 {{RFC9114}} and DNS over QUIC {{RFC9250}}.

Minor, backward-compatible extensions (such as new optional capability fields or new status codes within the reserved ranges) do not require a new ALPN identifier. Such extensions are negotiated through the capabilities structure or the IANA registries defined in Section 11.

### Serialization Format Negotiation

The serialization_format field determines the encoding used for all variable-length control messages (frame types 0x80-0xFF) and entity headers. Negotiation proceeds as follows:

1. Each peer advertises its preferred serialization_format in its Capabilities message.
2. If both peers advertise the same format, that format is used.
3. If a peer receives a Capabilities message without serialization_format, the sender is assumed to prefer CBOR {{RFC8949}}.
4. If the resulting preferences differ, the peers MUST use CBOR {{RFC8949}} as the fallback.

The initial Capabilities exchange on a new connection MUST use the default CBOR format for both the client's initiation and the server's response. The negotiated serialization format and resource limits take effect immediately following the successful completion of this initial Capabilities exchange (one request and one response). If a peer cannot decode the initial Capabilities exchange, it MUST close the connection with PIPESTREAM_INTERNAL_ERROR (0x01).
