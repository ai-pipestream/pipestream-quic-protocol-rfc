# Protocol Overview

This section provides a high-level overview of the PipeStream protocol architecture, design principles, and operational model.

## Design Goals

### True Streaming Processing

PipeStream MUST enable true streaming processing where entities are transmitted and processed incrementally as they become available. Implementations MUST NOT buffer complete inputs before initiating transmission.

### Recursive Decomposition

The protocol MUST support recursive decomposition of entities, wherein a single input entity MAY produce zero, one, or many output entities.

### Checkpoint Consistency

PipeStream MUST provide checkpoint blocking semantics to maintain processing consistency across distributed workers.

### Control and Data Plane Separation

The protocol MUST maintain strict separation between the control plane (control stream) and the data plane (entities).

### QUIC Foundation

PipeStream MUST be implemented over QUIC {{RFC9000}} to leverage:

- Native stream multiplexing without head-of-line blocking
- Built-in flow control at both connection and stream levels
- TLS 1.3 security by default
- Connection migration capabilities

### Multi-Layer Data Representation

The protocol MUST support four distinct data representation layers:

| Layer | Name       | Description                                    |
|-------|------------|------------------------------------------------|
| 0     | BlobBag    | Raw binary data with metadata                  |
| 1     | SemanticLayer | Annotated content with embeddings           |
| 2     | ParsedData | Structured extracted information               |
| 3     | CustomEntity | Application-specific extension               |

## Architecture Summary

PipeStream uses a dual-stream architecture within a single QUIC connection between a Client (Producer) and Server (Consumer):

| Stream | Type | Plane | Content |
|--------|------|-------|---------|
| Stream 0 | Bidirectional | Control | STATUS, SCOPE_DIGEST, BARRIER, CAPABILITIES, CHECKPOINT |
| Streams 2+ | Unidirectional | Data | Entity frames (Header + Payload) |

## Connection Lifecycle

A PipeStream connection follows this lifecycle:

1. **Establishment:** Client initiates QUIC connection with ALPN identifier "pipestream/1"
2. **Capability Exchange:** Client and server exchange supported protocol layers and limits
3. **Control Stream Initialization:** Client opens Stream 0 as bidirectional Control Stream
4. **Entity Streaming:** Entities are transmitted per Sections 5 and 6
5. **Termination:** Connection closes via QUIC CONNECTION_CLOSE or application-level shutdown
