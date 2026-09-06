# Protocol Overview

This section provides a high-level overview of the PipeStream protocol architecture, design principles, and operational model.

## Design Goals

This section is descriptive; the normative requirements that realize
these goals appear in Sections 5 through 9.

### True Streaming Processing

PipeStream framing permits incremental entity transmission and reversible processing. It does not inherently require a complete workload in memory. A checksum in the initial header, or an application profile's validation rules, can require a prior read or bounded spooling of an entity; Section 10.2 distinguishes these requirements from transport streaming.

### Recursive Decomposition

The protocol supports recursive decomposition of entities, wherein a single input entity may produce zero, one, or many output entities.

### Checkpoint Consistency

PipeStream provides checkpoint blocking semantics (Section 9.3) to maintain processing consistency across distributed workers.

### Control and Data Plane Separation

The protocol maintains strict separation between the control plane (the Control Stream) and the data plane (Entity Streams).

### QUIC Foundation

PipeStream is defined directly over QUIC {{RFC9000}} to leverage:

- Native stream multiplexing without head-of-line blocking
- Built-in flow control at both connection and stream levels
- TLS 1.3 security by default
- Connection migration capabilities

### Multi-Layer Data Representation

The protocol carries a 2-bit data layer field supporting four distinct
data representation layers whose concrete semantics are defined by
application profiles:

| Layer | Conventional Role | Description |
|-------|-------------------|-------------|
| 0     | Raw input | Binary or originating payload bytes |
| 1     | Enriched intermediate | Annotated or partially processed payload |
| 2     | Structured result | Normalized or extracted output |
| 3     | Extension | Application-specific payload semantics |

## Architecture Summary

PipeStream uses a dual-plane architecture within a single QUIC connection. The endpoint that initiates the QUIC connection is the client. Either endpoint MAY originate entities (Section 5.2); in many deployments the client acts as producer and the server as consumer, but the protocol does not require this asymmetry after connection establishment.

| Stream | Type | Plane | Content |
|--------|------|-------|---------|
| Stream 0 | Bidirectional (client-initiated) | Control | STATUS, SCOPE_DIGEST, BARRIER, GOAWAY, CAPABILITIES, CHECKPOINT |
| Entity Streams | Unidirectional (either endpoint) | Data | Entity frames (Header + Payload) |

## Connection Lifecycle

A PipeStream connection follows this lifecycle:

1. **Establishment:** Client initiates QUIC connection with ALPN identifier "pipestream/1"
2. **Control Stream Initialization:** Client opens Stream 0 as bidirectional Control Stream
3. **Capability Exchange:** Client and server exchange supported protocol layers and limits on Stream 0
4. **Entity Streaming:** Entities are transmitted per Sections 5 and 6
5. **Termination:** Connection closes via GOAWAY-initiated graceful shutdown (Section 6.5) or QUIC CONNECTION_CLOSE
