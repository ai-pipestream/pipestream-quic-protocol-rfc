# Appendix B: Relationship to Existing Protocols

This appendix discusses the relationship between PipeStream and existing transport and application protocols. The intent is to clarify the design rationale for specifying a new application protocol directly over QUIC {{RFC9000}} rather than layering on HTTP/3 {{RFC9114}}, gRPC, or WebTransport {{RFC9297}}.

## QUIC as Application Transport

RFC 9308 {{RFC9308}} provides guidance for designers of application protocol mappings to QUIC. PipeStream follows this guidance: stream semantics are mapped to the protocol's data model (Section 4), flow control operates at both the stream and connection level, and an ALPN token is registered for protocol identification (Section 11).

The precedent for application protocols that bypass HTTP and map directly onto QUIC is well established. DNS over Dedicated QUIC Connections {{RFC9250}} adopts a direct mapping on the grounds that HTTP framing introduces unnecessary overhead when the application has its own message semantics. The Media over QUIC Transport protocol {{MOQT}} similarly defines its own framing and control messages over QUIC streams, with HTTP/3 as an optional encapsulation rather than a requirement.

PipeStream's requirements align with these precedents. The protocol's dual-stream architecture (Section 4), bit-packed control frames (Section 6), and recursive entity lifecycle (Section 9) have no counterpart in HTTP semantics, and mapping them onto request-response pairs would add complexity without benefit.

## Efficiency Considerations

The decision to implement PipeStream as a standalone protocol over QUIC is driven by the following efficiency requirements:

### Framing Overhead

HTTP/3 and gRPC introduce multiple layers of framing. A gRPC message over HTTP/3 incurs QUIC stream overhead, HTTP/3 DATA frame overhead (minimum 2 octets), and gRPC envelope overhead (5 octets). For small status updates like PipeStream's 16-octet STATUS frame, this encapsulation would double the bandwidth requirement. PipeStream's bit-packed frames achieve near-theoretical minimum overhead for high-frequency coordination.

### Stateless Control Plane

HTTP/3 requires maintaining state for QPACK header compression. In high-concurrency environments where a single node manages thousands of sessions, the memory overhead of these contexts is significant. PipeStream status frames are stateless and self-describing; a receiver can parse any frame without reference to previous frames, enabling low-latency processing and easier load balancing across redundant nodes.

### Stream Independence

gRPC bidirectional streaming is constrained by the lifecycle of a single RPC call. While multiple entities can be multiplexed into one RPC, this reintroduces head-of-line blocking at the application layer if one entity requires retransmission. Alternatively, opening a new RPC for every entity (which could number in the millions for large input sets) incurs substantial setup and teardown overhead. PipeStream treats each entity as an independent QUIC stream, ensuring maximum parallelism and optimal use of QUIC's loss recovery mechanisms.

## HTTP/3

HTTP/3 {{RFC9114}} provides multiplexed request-response exchanges over QUIC. Its stream model binds each client-initiated request to a server response on the same stream. PipeStream requires bidirectional, peer-initiated entity streams where either endpoint may open new streams to transmit sub-entities arising from recursive decomposition. The request-response constraint precludes this.

PipeStream also requires a persistent control stream carrying compact, fixed-size status frames at high frequency. HTTP/3 does define unidirectional control streams, but their framing is specific to HTTP semantics (SETTINGS, GOAWAY, MAX_PUSH_ID) and cannot be repurposed for application-level status coordination without introducing a parallel signaling mechanism that duplicates much of what QUIC already provides.

## gRPC

gRPC defines a remote procedure call framework over HTTP/2, with experimental support for HTTP/3. Bidirectional streaming in gRPC is scoped to a single RPC method: one request stream and one response stream per call. PipeStream requires an arbitrary number of concurrent entity streams with independent flow control, plus a dedicated control stream, all within a single connection. Achieving this over gRPC would require either multiplexing all entities onto a single bidirectional RPC (sacrificing per-stream flow control and head-of-line independence) or opening a separate RPC per entity (sacrificing session-level coordination and incurring per-call overhead).

Beyond the stream model mismatch, gRPC does not provide protocol-level primitives for recursive entity decomposition, checkpoint-based consistency across parallel processing paths, or hierarchical scope management with digest propagation. Building equivalent semantics on top of gRPC requires application-layer coordination logic that PipeStream provides natively as part of the protocol specification.

gRPC further mandates a 5-octet length-prefixed framing envelope for every message. PipeStream's fixed-size control frames are bit-packed at the wire level with zero serialization overhead, which is material at the status update frequencies the protocol is designed to sustain.

PipeStream complements rather than replaces gRPC: a PipeStream pipeline stage MAY internally use gRPC to communicate with processing services while relying on PipeStream for inter-stage coordination and consistency.

## WebTransport

WebTransport {{RFC9297}} provides bidirectional streams and unreliable datagrams over HTTP/3, and is the closest existing protocol to the transport abstraction PipeStream requires. However, several properties make it unsuitable as a substrate:

WebTransport sessions are established via an HTTP/3 CONNECT request, inheriting the client-server asymmetry of HTTP. In PipeStream, both endpoints participate symmetrically in capability negotiation and may initiate entity streams; the protocol does not distinguish a "client" role from a "server" role after the handshake.

WebTransport is designed for environments constrained by the web security model (origin-based isolation, CORS). PipeStream targets server-to-server processing pipelines where these constraints are inapplicable.

WebTransport provides raw byte streams with no built-in coordination semantics. PipeStream would need to implement its own framing, status state machine, checkpoint barriers, and scope hierarchy on top of WebTransport streams. At that point, the HTTP/3 session layer introduces an additional round trip during establishment and per-stream framing overhead, with no corresponding benefit.

## Summary

PipeStream occupies a design point not addressed by existing protocols: a QUIC-native application protocol combining multiplexed entity streaming, recursive decomposition with hierarchical scopes, Merkle-based integrity propagation, and barrier-synchronized reassembly. While existing protocols like SCTP {{RFC9260}} and PPSPP {{RFC7574}} address subsets of these requirements, none provide the integrated lifecycle and coordination semantics that PipeStream defines.
