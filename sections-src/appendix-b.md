# Relationship to Existing Protocols

PipeStream proposes shared processing semantics, not a claim that existing
transports cannot implement scatter-gather. Comparisons must separate
transport properties, application coordination, and measured implementation
cost. No comparative performance benchmark is presented in this draft.

## Direct QUIC Mapping

QUIC {{RFC9000}} provides independently flow-controlled streams and a
connection-level flow-control budget. Mapping payloads to separate streams
can isolate stream-level loss recovery, but it does not eliminate
connection-level blocking or application dependency deadlocks. {{RFC9308}}
describes these considerations for application protocol designers.

PipeStream chooses direct QUIC so its control and entity streams have one
application-specific mapping selected through ALPN. This requires native
QUIC access and UDP reachability. It does not by itself provide browser
access, HTTP authentication integrations, intermediary support, or a
connectivity advantage over an HTTP-based deployment.

## HTTP/3 and gRPC

HTTP/3 {{RFC9114}} provides an HTTP mapping over QUIC. An application can
implement work coordination over HTTP requests or streaming RPCs, including
multiple concurrent RPCs. Doing so requires a specification of their shared
session, entity, and completion semantics, which is the subject of
PipeStream. Additional framing is not evidence of a material performance
disadvantage without workload measurements.

PipeStream STATUS payloads do not depend on a header-compression history.
Its control plane is nevertheless stateful: validation depends on negotiated
capabilities, admitted entities, scopes, checkpoints, and durable claims.
Parsing a status frame independently does not make it safe to route that
frame to a stateless worker.

## WebTransport

WebTransport over HTTP/3 {{WEBTRANSPORT}} supports streams initiated by
either endpoint after session establishment. Its client-initiated CONNECT
handshake does not prevent symmetric use of streams within a session.
It is therefore a possible substrate for a future PipeStream mapping,
particularly where browser access or HTTP infrastructure is required.

Such a mapping would need to specify session establishment, stream
identification, errors, authentication, and extension negotiation. This
draft specifies only direct QUIC and does not assert that WebTransport is
unsuitable or intrinsically slower. RFC 9297 defines HTTP Datagrams and the
Capsule Protocol, not WebTransport itself.

## Media over QUIC and Broker-Based Systems

Media over QUIC Transport {{MOQT}} specifies media distribution semantics.
PipeStream instead focuses on work decomposition and completion reporting.
This distinction is an application-model choice, not evidence that one
transport is generally more capable.

Message brokers and durable logs provide persistence, decoupling, and replay
that a direct peer connection alone does not provide. A deployment can use
those systems behind a PipeStream endpoint. PipeStream does not remove the
need to persist state or coordinate failures across application effects.

## Connectivity Failure

An endpoint that cannot establish a QUIC connection MUST report connection
failure to its application. This version defines no TCP, HTTP, or plaintext
fallback. A deployment MAY offer another explicitly selected application
protocol, but MUST NOT silently change PipeStream's security or completion
semantics. Retry budgets and user-visible timeout policy are deployment
choices.
