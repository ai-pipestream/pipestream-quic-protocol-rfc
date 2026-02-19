# 3. Protocol Overview

This section provides a high-level overview of the PipeStream protocol architecture, design principles, and operational model. PipeStream is a recursive entity streaming protocol designed for distributed document processing, built upon QUIC [RFC9000] to leverage its native multiplexing, flow control, and security capabilities.

## 3.1 Design Goals

The PipeStream protocol is designed to meet the following objectives:

### 3.1.1 True Streaming Processing

PipeStream MUST enable true streaming document processing where entities are transmitted and processed incrementally as they become available. Implementations MUST NOT buffer complete documents before initiating transmission. This requirement ensures minimal latency for large document processing and enables processing pipelines to begin work before source documents are fully received.

### 3.1.2 Recursive Decomposition

The protocol MUST support recursive decomposition of entities, wherein a single input entity MAY produce zero, one, or many output entities. This capability, termed "vaporization," enables document parsing operations where a single document entity becomes multiple component entities (e.g., a PDF document decomposing into page entities, which further decompose into paragraph and image entities).

### 3.1.3 Checkpoint Consistency

PipeStream MUST provide checkpoint blocking semantics to maintain processing consistency across distributed workers. When a checkpoint is declared, all entities preceding that checkpoint MUST reach a terminal state (COMPLETE or FAILED) before entities following the checkpoint are permitted to proceed. This mechanism ensures deterministic processing boundaries for operations requiring transactional semantics.

### 3.1.4 Control and Data Plane Separation

The protocol MUST maintain strict separation between the control plane (ledger) and the data plane (entities). This separation enables:

- Independent scaling of coordination and data transfer
- Lightweight status tracking without payload inspection
- Efficient reassembly coordination for recursive operations

### 3.1.5 QUIC Foundation

PipeStream MUST be implemented over QUIC [RFC9000] to leverage:

- Native stream multiplexing without head-of-line blocking
- Built-in flow control at both connection and stream levels
- TLS 1.3 security by default
- Connection migration capabilities

### 3.1.6. Multi-Layer Data Representation

The protocol MUST support four distinct data representation layers to accommodate varying processing requirements:

| Layer | Name       | Description                                    |
|-------|------------|------------------------------------------------|
| 0     | BlobBag    | Raw binary data with minimal metadata          |
| 1     | SemanticLayer | Annotated content with semantic metadata      |
| 2     | ParsedData | Structured extracted information               |
| 3     | CustomEntity | Application-specific extension Layer          |

### 3.1.7. Protocol Layering

PipeStream is organized into three protocol layers to accommodate varying deployment requirements:

| Protocol Layer | Name | Description |
|----------------|------|-------------|
| Layer 0 | Core | Basic streaming, vaporize/rejoin, checkpoint |
| Layer 1 | Recursive | Hierarchical scopes, digest propagation, barriers |
| Layer 2 | Resilience | Yield/resume, claim checks, completion policies |

Implementations MUST support Layer 0. Support for Layers 1 and 2 is OPTIONAL and negotiated during connection establishment.

## 3.2 Architecture Summary

PipeStream employs a dual-stream architecture that separates entity lifecycle management from payload transmission. All communication occurs over a single QUIC connection, with dedicated streams serving distinct purposes.

### 3.2.1 High-Level Architecture

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
         |  |  |  Stream 0: Ledger (Control Plane)  |  |  |
         |  |  |  [STATUS][STATUS][STATUS]...       |  |  |
         |  |  +-------------------------------------+  |  |
         |  |                                           |  |
         |  |  +-------------------------------------+  |  |
         |  |  |  Stream 1: Entity (Data Plane)     |  |  |
         |  |  |  [HEADER][PAYLOAD]                 |  |  |
         |  |  +-------------------------------------+  |  |
         |  |                                           |  |
         |  |  +-------------------------------------+  |  |
         |  |  |  Stream 2: Entity (Data Plane)     |  |  |
         |  |  |  [HEADER][PAYLOAD]                 |  |  |
         |  |  +-------------------------------------+  |  |
         |  |                                           |  |
         |  |  +-------------------------------------+  |  |
         |  |  |  Stream N: Entity (Data Plane)     |  |  |
         |  |  |  [HEADER][PAYLOAD]                 |  |  |
         |  |  +-------------------------------------+  |  |
         |  +-------------------------------------------+  |
         |                                                 |
         +-------------------------------------------------+
```

Figure 1: PipeStream Dual-Stream Architecture

### 3.2.2 Ledger Stream (Stream 0)

The Ledger Stream MUST be allocated as QUIC Stream ID 0 and serves as the control plane for entity lifecycle management. This stream carries lightweight status frames that track entity state transitions without transmitting payload data.

#### 3.2.2.1 Ledger Frame Format

Each basic ledger frame is exactly 4 octets (32 bits), word-aligned:

```
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |E|C|              Entity ID (20 bits)         |Stat |  Flags  |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

Figure 2: Ledger Frame Format

- **E (1 bit):** Extended frame flag.
- **C (1 bit):** Cursor update flag.
- **Entity ID (20 bits):** Unique identifier for the entity within the scope.
- **Stat (4 bits):** Current lifecycle state of the entity.
- **Flags (6 bits):** Reserved.

| Value | Status     | Layer | Description                                      |
|-------|------------|-------|--------------------------------------------------|
| 0x0   | PENDING    | 0     | Entity announced, payload not yet transmitted    |
| 0x1   | PROCESSING | 0     | Entity payload transmission in progress          |
| 0x2   | COMPLETE   | 0     | Entity successfully processed                    |
| 0x3   | FAILED     | 0     | Entity processing failed                         |
| 0x4   | CHECKPOINT | 0     | Synchronization barrier                          |
| 0x5   | VAPORIZING | 0     | Decomposing into children                        |
| 0x6   | AGGREGATING| 0     | Rejoining children                               |
| 0x7   | YIELDED    | 2     | Paused with continuation token                   |
| 0x8   | DEFERRED   | 2     | Detached with claim check                        |
| 0x9   | RETRYING   | 2     | Retry in progress                                |
| 0xA   | SKIPPED    | 2     | Intentionally skipped                            |
| 0xB   | ABANDONED  | 2     | Timed out, cursor advanced past                  |

Implementations MUST process ledger frames in order. A receiver MUST NOT process an entity payload until a corresponding PENDING or PROCESSING status has been received on the Ledger Stream.

#### 3.2.2.2 Ledger Stream Properties

The Ledger Stream has the following properties:

- The Ledger Stream MUST be bidirectional to support acknowledgment of status transitions.
- Implementations SHOULD prioritize Ledger Stream frames over Entity Stream frames to ensure timely coordination.
- The Ledger Stream MUST remain open for the duration of the QUIC connection.
- Flow control on the Ledger Stream SHOULD be configured to prevent backpressure from blocking status updates.

### 3.2.3 Entity Streams (Streams 1+)

Entity Streams carry the actual payload data and MUST use QUIC Stream IDs starting from 1. Each entity SHOULD be transmitted on its own dedicated QUIC stream to leverage QUIC's native multiplexing and eliminate head-of-line blocking between independent entities.

#### 3.2.3.1 Entity Frame Format

Entity frames consist of a header followed by layer-specific payload data. The header is encoded using Protocol Buffers [PROTOBUF] for extensibility:

```
+----------------------------------+
|     Frame Length (varint)        |
+----------------------------------+
|     Entity Header (protobuf)     |
|   +----------------------------+ |
|   | entity_id: uint32          | |
|   | parent_id: uint32          | |
|   | layer: uint8               | |
|   | content_type: string       | |
|   | payload_length: uint64     | |
|   | checksum: bytes            | |
|   +----------------------------+ |
+----------------------------------+
|                                  |
|     Payload (variable length)    |
|                                  |
+----------------------------------+
```

Figure 3: Entity Frame Format

#### 3.2.3.2 Entity Stream Lifecycle

Each Entity Stream follows this lifecycle:

1. Sender opens a new unidirectional QUIC stream
2. Sender transmits PENDING status on Ledger Stream
3. Sender transmits entity frame on Entity Stream
4. Sender updates status to PROCESSING on Ledger Stream
5. Sender closes the stream upon payload completion
6. Sender transmits terminal status (COMPLETE or FAILED) on Ledger Stream

Receivers MUST correlate Entity Stream data with Ledger Stream status using the Entity ID present in both.

#### 3.2.3.3 Stream Allocation

Implementations MUST use the following stream allocation scheme:

- Stream 0: Ledger Stream (bidirectional, client-initiated)
- Streams 1, 5, 9, ...: Client-initiated entity streams (unidirectional)
- Streams 3, 7, 11, ...: Server-initiated entity streams (unidirectional)

This allocation follows QUIC stream ID encoding where the two least significant bits indicate the stream type and initiator.

### 3.2.4 Checkpoint Semantics

Checkpoints provide synchronization barriers within the entity stream. When a CHECKPOINT status frame is transmitted on the Ledger Stream:

1. The checkpoint MUST include a unique Entity ID (checkpoint identifier)
2. All entities with Entity IDs less than the checkpoint identifier MUST reach a terminal state before the checkpoint is considered satisfied
3. Entities with Entity IDs greater than the checkpoint identifier MUST NOT be marked COMPLETE until the checkpoint is satisfied
4. Receivers MUST acknowledge checkpoint satisfaction on the Ledger Stream

```
Timeline:
=========

Entity 0: PENDING -> PROCESSING -> COMPLETE
                                      |
Entity 1: PENDING -> PROCESSING ------+---> COMPLETE
                                      |
Entity 2: PENDING -> PROCESSING -> FAILED
                                      |
                                      v
                            +-------------------+
Checkpoint 3: -----------> | CHECKPOINT        |
                            | (blocks until     |
                            |  0,1,2 terminal)  |
                            +-------------------+
                                      |
                                      v
Entity 4: PENDING ----------------> PROCESSING -> COMPLETE
```

Figure 4: Checkpoint Blocking Semantics

## 3.3 Processing Pipeline

PipeStream defines a processing pipeline model wherein entities flow through a directed acyclic graph (DAG) of processing workers. Each worker performs one of four fundamental actions on entities.

### 3.3.1 Pipeline Actions

| Action  | Description                                           |
|---------|-------------------------------------------------------|
| CONNECT | Establish connection and initiate entity stream       |
| PARSE   | Decompose entity structure (may vaporize)             |
| PROCESS | Transform entity content (1:1 transformation)         |
| SINK    | Terminal consumption of entity                        |

Implementations MUST support all four actions. Workers MAY implement any combination of actions.

### 3.3.2 Entity Cardinality Operations

Workers MAY perform cardinality-changing operations on entities:

#### 3.3.2.1 Vaporization (1:N)

A single input entity produces multiple output entities. The output entities MUST include a `parent_id` field referencing the input entity. Vaporization is commonly used in PARSE operations.

```
                        +-> Entity 1a (parent_id=1)
                        |
Entity 1 --[PARSE]------+-> Entity 1b (parent_id=1)
                        |
                        +-> Entity 1c (parent_id=1)
```

Figure 5: Vaporization Operation

#### 3.3.2.2 Rejoin (N:1)

Multiple input entities are combined into a single output entity. The output entity SHOULD include metadata referencing all input entities. Rejoin operations MUST respect checkpoint boundaries; entities from different checkpoint epochs MUST NOT be rejoined.

```
Entity 1a --+
            |
Entity 1b --+--[PROCESS]--> Entity 1' (sources=[1a,1b,1c])
            |
Entity 1c --+
```

Figure 6: Rejoin Operation

### 3.3.3 Pipeline Flow

The following diagram illustrates a complete pipeline flow:

```
+----------+     +---------+     +-----------+     +--------+
|          |     |         |     |           |     |        |
| CONNECT  |---->|  PARSE  |---->|  PROCESS  |---->|  SINK  |
|          |     |         |     |           |     |        |
+----------+     +---------+     +-----------+     +--------+
     |                |                |                |
     v                v                v                v
 Open QUIC       Vaporize         Transform         Consume
 Connection      Entities         Payloads          Output
     |                |                |                |
     v                v                v                v
+----------+     +---------+     +-----------+     +--------+
| Ledger:  |     | Ledger: |     | Ledger:   |     | Ledger:|
| PENDING  |     | PENDING |     | PROCESSING|     |COMPLETE|
|          |     | (N new) |     |           |     |        |
+----------+     +---------+     +-----------+     +--------+
```

Figure 7: Pipeline Action Flow with Ledger Updates

### 3.3.4 Worker DAG Topology

Workers are organized in a directed acyclic graph. The topology MUST satisfy:

1. Exactly one CONNECT worker as the root node
2. At least one SINK worker as a leaf node
3. No cycles in the worker graph
4. All workers reachable from CONNECT
5. All non-SINK workers having at least one downstream worker

```
                    +-------------+
                    |   CONNECT   |
                    +------+------+
                           |
              +------------+------------+
              |                         |
       +------v------+           +------v------+
       |    PARSE    |           |    PARSE    |
       |  (PDF Doc)  |           | (HTML Doc)  |
       +------+------+           +------+------+
              |                         |
    +---------+---------+               |
    |         |         |               |
+---v---+ +---v---+ +---v---+     +-----v-----+
|PROCESS| |PROCESS| |PROCESS|     |  PROCESS  |
| (OCR) | |(Image)| |(Text) |     |  (DOM)    |
+---+---+ +---+---+ +---+---+     +-----+-----+
    |         |         |               |
    +----+----+---------+---------------+
         |
   +-----v-----+
   |   SINK    |
   | (Index)   |
   +-----------+
```

Figure 8: Example Worker DAG Topology

## 3.4 Connection Lifecycle

A PipeStream connection follows this lifecycle:

1. **Establishment:** Client initiates QUIC connection with ALPN identifier "pipestream/1"
2. **Ledger Initialization:** Client opens Stream 0 as bidirectional Ledger Stream
3. **Capability Exchange:** Client and server exchange supported layers and actions
4. **Entity Streaming:** Entities are transmitted per Sections 3.2.2 and 3.2.3
5. **Termination:** Connection closes via QUIC CONNECTION_CLOSE or application-level shutdown

Implementations MUST use the ALPN identifier "pipestream/1" during TLS negotiation.

## 3.5 Error Handling

Errors are communicated through:

1. **Entity-level:** FAILED status on Ledger Stream with optional error code
2. **Stream-level:** QUIC RESET_STREAM or STOP_SENDING frames
3. **Connection-level:** QUIC CONNECTION_CLOSE with application error code

Error codes are defined in Section 7 (Error Codes).
