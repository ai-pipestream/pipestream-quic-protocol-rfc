## 4. Protocol Overview

This section provides a high-level overview of the PipeStream protocol architecture, design principles, and operational model.

### 4.1. Design Goals

#### 4.1.1. True Streaming Processing

PipeStream MUST enable true streaming document processing where entities are transmitted and processed incrementally as they become available. Implementations MUST NOT buffer complete documents before initiating transmission.

#### 4.1.2. Recursive Decomposition

The protocol MUST support recursive decomposition of entities, wherein a single input entity MAY produce zero, one, or many output entities.

#### 4.1.3. Checkpoint Consistency

PipeStream MUST provide checkpoint blocking semantics to maintain processing consistency across distributed workers.

#### 4.1.4. Control and Data Plane Separation

The protocol MUST maintain strict separation between the control plane (control stream) and the data plane (entities).

#### 4.1.5. QUIC Foundation

PipeStream MUST be implemented over QUIC {{RFC9000}} to leverage:

- Native stream multiplexing without head-of-line blocking
- Built-in flow control at both connection and stream levels
- TLS 1.3 security by default
- Connection migration capabilities

#### 4.1.6. Multi-Layer Data Representation

The protocol MUST support four distinct data representation layers:

| Layer | Name       | Description                                    |
|-------|------------|------------------------------------------------|
| 0     | BlobBag    | Raw binary data with metadata                  |
| 1     | SemanticLayer | Annotated content with embeddings           |
| 2     | ParsedData | Structured extracted information               |
| 3     | CustomEntity | Application-specific extension               |

### 4.2. Architecture Summary

```
+------------------+                              +------------------+
|                  |        QUIC Connection       |                  |
|     Client       |<---------------------------->|     Server       |
|   (Producer)     |                              |   (Consumer)     |
|                  |                              |                  |
+--------+---------+                              +--------+---------+
         |                                                 |
         |  +-------------------------------------------+  |
         |  |            QUIC Connection                |  |
         |  |  +-------------------------------------+  |  |
         |  |  |  Stream 0: Control (Control Plane)  |  |  |
         |  |  |  [STATUS][STATUS][STATUS]...        |  |  |
         |  |  +-------------------------------------+  |  |
         |  |                                           |  |
         |  |  +-------------------------------------+  |  |
         |  |  |  Stream 2+: Entity (Data Plane)    |  |  |
         |  |  |  [HEADER][PAYLOAD]                 |  |  |
         |  |  +-------------------------------------+  |  |
         |  +-------------------------------------------+  |
         |                                                 |
         +-------------------------------------------------+
```

Figure 1: PipeStream Dual-Stream Architecture

### 4.3. Connection Lifecycle

A PipeStream connection follows this lifecycle:

1. **Establishment:** Client initiates QUIC connection with ALPN identifier "pipestream/1"
2. **Capability Exchange:** Client and server exchange supported protocol layers and limits
3. **Control Stream Initialization:** Client opens Stream 0 as bidirectional Control Stream
4. **Entity Streaming:** Entities are transmitted per Sections 5 and 6
5. **Termination:** Connection closes via QUIC CONNECTION_CLOSE or application-level shutdown
