# PipeStream: A Recursive Entity Streaming Protocol for Distributed Document Processing

**Internet-Draft**

**draft-krickert-pipestream-00**

**Intended status:** Standards Track

**Expires:** August 23, 2026

---

## Abstract

This document specifies PipeStream, a recursive entity streaming protocol designed for distributed document processing over QUIC transport. PipeStream enables the decomposition ("vaporization") of documents into constituent entities, their transmission across distributed processing nodes, and subsequent reassembly ("rejoining") at destination endpoints.

The protocol employs a dual-stream architecture consisting of a data stream for entity payload transmission and a ledger stream for tracking entity completion status and maintaining consistency. PipeStream defines four hierarchical data layers for entity representation: BlobBag for raw binary data, SemanticLayer for annotated content with metadata, ParsedData for structured extracted information, and CustomEntity for application-specific extensions.

To ensure consistency across distributed processing pipelines, PipeStream implements checkpoint blocking, whereby processing nodes MUST synchronize at defined points before proceeding. This mechanism guarantees that all constituent parts of a vaporized document are successfully processed before reassembly operations commence.

---

## Status of This Memo

This Internet-Draft is submitted in full conformance with the provisions of BCP 78 and BCP 79.

Internet-Drafts are working documents of the Internet Engineering Task Force (IETF). Note that other groups may also distribute working documents as Internet-Drafts.

---

## Table of Contents

1. Introduction
2. Terminology
3. Protocol Overview
4. QUIC Stream Mapping
5. Frame Formats
6. Entity Model
7. Processing Actions
8. Reassembly Semantics
9. Security Considerations
10. IANA Considerations
Appendix A: Protobuf Schema Reference
References
Authors' Addresses

---

## 1. Introduction

### 1.1. Problem Statement

Distributed document processing pipelines face significant challenges when handling large, complex documents that require multiple stages of transformation, analysis, and enrichment. Traditional batch processing approaches require entire documents to be loaded into memory, processed sequentially, and transmitted in their entirety between processing stages. This methodology introduces substantial latency, excessive memory consumption, and poor utilization of distributed computing resources.

Modern document processing workflows increasingly demand the ability to:

- Process documents incrementally as data becomes available
- Distribute processing load across heterogeneous worker nodes
- Maintain consistency guarantees across parallel processing paths
- Handle documents of arbitrary size without memory constraints
- Support recursive decomposition where document parts may themselves be decomposed

Current approaches based on batch processing and store-and-forward architectures are inefficient for large documents and fail to exploit the inherent parallelism available in distributed processing environments. Furthermore, existing streaming protocols do not provide the consistency semantics required for document processing where the integrity of the reassembled output depends on the successful processing of all constituent parts.

### 1.2. PipeStream Overview

PipeStream addresses these challenges by defining a streaming protocol that enables incremental processing with strong consistency guarantees. The protocol is built upon QUIC [RFC9000] transport, leveraging its native support for multiplexed streams, low-latency connection establishment, and reliable delivery semantics.

The fundamental innovation of PipeStream is its treatment of documents as recursive compositions of entities. A document MAY be decomposed into multiple entities, each of which MAY itself be further decomposed, creating a tree structure of processing tasks. This recursive decomposition enables fine-grained parallelism while the protocol's ledger mechanism ensures that all branches of the decomposition tree are tracked and synchronized.

PipeStream employs a dual-stream design:

1. **Data Stream**: Carries entity payloads through the processing pipeline. Entities flow through this stream with minimal buffering, enabling low-latency incremental processing.

2. **Ledger Stream**: Carries control information tracking the status of entity decomposition and reassembly. The ledger ensures that all parts of a vaporized document are accounted for before reassembly proceeds.

### 1.3. Design Philosophy

The PipeStream design philosophy may be understood through analogy to the "Star Trek Transporter" concept: a document is "vaporized" at the source into its constituent entities, these entities are transmitted and processed through the distributed pipeline, and finally the entities are "reassembled" at the destination to reconstitute the complete processed document.

This approach provides several advantages:

- **Incremental Processing**: Processing nodes MAY begin work on early entities before the complete document has been transmitted.

- **Parallelism**: Independent entities MAY be processed concurrently across multiple worker nodes.

- **Memory Efficiency**: No single node is required to hold the complete document in memory.

- **Fault Isolation**: Failures in processing individual entities can be detected, reported, and potentially retried without affecting other entities.

- **Consistency**: The checkpoint blocking mechanism ensures that reassembly operations proceed only when all constituent parts have been successfully processed.

### 1.4. Scope

This document specifies the PipeStream protocol including message formats, state machines, error handling, and the interaction between data and ledger streams. The document defines the four standard data layers but does not mandate specific processing semantics, which are left to application-layer specifications.

---

## 2. Terminology

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "NOT RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in BCP 14 [RFC2119] [RFC8174] when, and only when, they appear in all capitals, as shown here.

### 2.1. Protocol Entities

**Entity**
:   The fundamental unit of data flowing through a PipeStream pipeline. An Entity represents either a complete document or a constituent part of a decomposed document. Each Entity possesses a unique identifier within its processing context and carries payload data in one of the four defined Layer formats. Entities are immutable once created; transformations produce new Entities rather than modifying existing ones. An Entity MAY be marked as "composite," indicating that it is itself composed of sub-entities that must be tracked via the Parts Ledger.

**Document**
:   A logical unit of content submitted to a PipeStream pipeline for processing. A Document enters the pipeline as a single root Entity and MAY be decomposed into multiple Entities during processing. The Document is considered complete when its root Entity (or the rejoined result of its decomposition) exits the pipeline.

### 2.2. Decomposition and Reassembly

**Vaporize**
:   The operation of decomposing a document or Entity into multiple constituent Entities for parallel or distributed processing. When an Entity is vaporized, the originating node MUST create a Parts Ledger entry recording the identifiers of all resulting sub-entities. The vaporization operation is recursive; a sub-entity produced by vaporization MAY itself be vaporized, creating a tree of decomposition. Vaporization SHOULD be performed according to semantic boundaries within the document (e.g., chapters, sections, paragraphs) when such boundaries are discernible.

**Rejoin**
:   The operation of reassembling multiple Entities back into a single composite Entity or Document. A rejoin operation MUST NOT proceed until all constituent Entities listed in the corresponding Parts Ledger entry have been received and processed. The rejoin operation is the inverse of vaporization; for any vaporization that produces N sub-entities, a corresponding rejoin MUST consume exactly those N sub-entities. The semantics of combining Entity payloads during rejoin are Layer-specific and defined in Section 6.

### 2.3. Consistency Mechanisms

**Checkpoint**
:   A synchronization point in the processing pipeline where all in-flight Entities MUST reach a consistent state before processing may continue. When a checkpoint is declared, all processing nodes MUST complete their current Entity operations and report completion via the Ledger Stream. No new Entities SHALL be accepted for processing until the checkpoint has been satisfied. Checkpoints provide consistency boundaries that enable:
    - Guaranteed completion of all pending vaporize/rejoin operations
    - Consistent state snapshots for fault recovery
    - Backpressure propagation through the pipeline

    A checkpoint is considered "satisfied" when all Parts Ledger entries created before the checkpoint have been resolved (all constituent Entities processed and rejoined).

**Ledger**
:   The control stream that tracks Entity completion status throughout the processing pipeline. The Ledger is transmitted on a dedicated QUIC stream parallel to the data stream, enabling control information to flow independently of Entity payloads. The Ledger carries:
    - Entity lifecycle events (created, processing, completed, failed)
    - Parts Ledger updates for vaporization tracking
    - Checkpoint declarations and acknowledgments
    - Error and retry notifications

    All nodes participating in a PipeStream pipeline MUST maintain a consistent view of the Ledger. The Ledger provides the consistency guarantees that enable safe vaporization and rejoin operations across distributed nodes.

**Parts Ledger**
:   A data structure within the Ledger that tracks the relationship between a composite Entity and its constituent sub-entities produced by vaporization. Each Parts Ledger entry contains:
    - The identifier of the parent Entity that was vaporized
    - An ordered list of identifiers for all sub-entities produced
    - The completion status of each sub-entity
    - The checkpoint scope within which the vaporization occurred

    A Parts Ledger entry is created atomically when an Entity is vaporized and MUST be transmitted on the Ledger Stream before any sub-entities are transmitted on the Data Stream. A Parts Ledger entry is "resolved" when all constituent sub-entities have reached "completed" status, at which point a rejoin operation MAY proceed.

### 2.4. Routing and Distribution

**WorkerMap**
:   A routing table that specifies how Entities should be distributed across processing nodes during vaporization. The WorkerMap defines:
    - Available worker nodes and their capabilities
    - Routing predicates based on Entity properties (type, size, Layer)
    - Load balancing policies for distributing sub-entities
    - Affinity rules for co-locating related Entities

    When vaporizing an Entity, the originating node SHOULD consult the WorkerMap to determine the destination for each sub-entity. The WorkerMap MAY be distributed via the Ledger Stream to ensure all nodes maintain a consistent routing view. Updates to the WorkerMap MUST be applied at checkpoint boundaries to prevent routing inconsistencies during active processing.

### 2.5. Data Representation

**Layer**
:   One of four defined representations for Entity payload data. Layers provide a progression from raw binary data to structured semantic information, enabling processing nodes to operate at the appropriate level of abstraction. The four Layers, in order of increasing semantic richness, are:

    1. **BlobBag**: Raw binary data with minimal metadata. A BlobBag Entity contains an uninterpreted byte sequence and MUST include a media type identifier. BlobBag is the entry point for documents ingested into the pipeline and the exit point for final output. Processing nodes that operate on BlobBag Entities perform format conversion, compression, or other byte-level transformations.

    2. **SemanticLayer**: Annotated content with structural and semantic metadata. A SemanticLayer Entity contains the document content plus annotations identifying semantic elements (headings, paragraphs, tables, figures, etc.). SemanticLayer preserves the original content while adding a metadata overlay that enables semantic-aware processing. SemanticLayer Entities MUST be convertible back to BlobBag without information loss in the primary content (annotations MAY be discarded).

    3. **ParsedData**: Structured information extracted from document content. A ParsedData Entity contains data elements extracted during analysis (named entities, relationships, classifications, summaries, etc.) represented in a structured format. ParsedData represents derived information and is not generally reversible to the original document content. ParsedData Entities MAY reference their source SemanticLayer or BlobBag Entities.

    4. **CustomEntity**: Application-specific extension Layer for specialized processing requirements. CustomEntity payloads MUST include a type identifier registered with the pipeline configuration. The semantics of CustomEntity Layers are defined by the registering application and are opaque to the core PipeStream protocol. Implementations MUST support forwarding CustomEntity Entities even when unable to interpret their contents.

    An Entity MUST be associated with exactly one Layer at any point in time. Transformation between Layers is a processing operation that produces a new Entity; the original Entity's Layer is immutable.

### 2.6. Additional Terms

**Pipeline**
:   A configured sequence of processing stages through which Entities flow. A Pipeline defines the processing topology, including available transformations, vaporization points, rejoin points, and checkpoint locations.

**Stage**
:   A single processing step within a Pipeline. Each Stage receives Entities, performs transformations, and emits Entities (possibly vaporized or at a different Layer) to subsequent Stages.

**Flow Control**
:   The mechanism by which PipeStream regulates the rate of Entity transmission to prevent overwhelming downstream processors. Flow control operates at both the QUIC transport level and the application level via checkpoint blocking and Ledger-based backpressure signals.

---

## 3. Protocol Overview

This section provides a high-level overview of the PipeStream protocol architecture, design principles, and operational model. PipeStream is a recursive entity streaming protocol designed for distributed document processing, built upon QUIC [RFC9000] to leverage its native multiplexing, flow control, and security capabilities.

### 3.1 Design Goals

The PipeStream protocol is designed to meet the following objectives:

#### 3.1.1 True Streaming Processing

PipeStream MUST enable true streaming document processing where entities are transmitted and processed incrementally as they become available. Implementations MUST NOT buffer complete documents before initiating transmission. This requirement ensures minimal latency for large document processing and enables processing pipelines to begin work before source documents are fully received.

#### 3.1.2 Recursive Decomposition

The protocol MUST support recursive decomposition of entities, wherein a single input entity MAY produce zero, one, or many output entities. This capability, termed "vaporization," enables document parsing operations where a single document entity becomes multiple component entities (e.g., a PDF document decomposing into page entities, which further decompose into paragraph and image entities).

#### 3.1.3 Checkpoint Consistency

PipeStream MUST provide checkpoint blocking semantics to maintain processing consistency across distributed workers. When a checkpoint is declared, all entities preceding that checkpoint MUST reach a terminal state (COMPLETE or FAILED) before entities following the checkpoint are permitted to proceed. This mechanism ensures deterministic processing boundaries for operations requiring transactional semantics.

#### 3.1.4 Control and Data Plane Separation

The protocol MUST maintain strict separation between the control plane (ledger) and the data plane (entities). This separation enables:

- Independent scaling of coordination and data transfer
- Lightweight status tracking without payload inspection
- Efficient reassembly coordination for recursive operations

#### 3.1.5 QUIC Foundation

PipeStream MUST be implemented over QUIC [RFC9000] to leverage:

- Native stream multiplexing without head-of-line blocking
- Built-in flow control at both connection and stream levels
- TLS 1.3 security by default
- Connection migration capabilities

#### 3.1.6 Multi-Layer Data Representation

The protocol MUST support four distinct data representation layers to accommodate varying processing requirements:

| Layer | Name       | Description                                    |
|-------|------------|------------------------------------------------|
| 0     | RAW        | Unprocessed binary octets                      |
| 1     | PARSED     | Structurally parsed representation             |
| 2     | ENRICHED   | Semantically annotated representation          |
| 3     | NORMALIZED | Canonicalized output representation            |

Implementations MAY support any subset of layers but MUST support at least Layer 0 (RAW).

### 3.2 Architecture Summary

PipeStream employs a dual-stream architecture that separates entity lifecycle management from payload transmission. All communication occurs over a single QUIC connection, with dedicated streams serving distinct purposes.

#### 3.2.1 High-Level Architecture

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

#### 3.2.2 Ledger Stream (Stream 0)

The Ledger Stream MUST be allocated as QUIC Stream ID 0 and serves as the control plane for entity lifecycle management. This stream carries lightweight status frames that track entity state transitions without transmitting payload data.

##### 3.2.2.1 Ledger Frame Format

Each ledger frame is exactly 3 octets:

```
 0                   1                   2
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|         Entity ID (16 bits)       |  Status   |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

Figure 2: Ledger Frame Format

- **Entity ID (16 bits):** Unique identifier for the entity within the connection scope. Implementations MUST assign Entity IDs sequentially starting from 0. The value 0xFFFF is reserved for connection-level control messages.

- **Status (8 bits):** Current lifecycle state of the entity, encoded as follows:

| Value | Status     | Description                                      |
|-------|------------|--------------------------------------------------|
| 0x00  | PENDING    | Entity announced, payload not yet transmitted    |
| 0x01  | PROCESSING | Entity payload transmission in progress          |
| 0x02  | COMPLETE   | Entity successfully processed                    |
| 0x03  | FAILED     | Entity processing failed                         |
| 0x04  | CHECKPOINT | Synchronization barrier (see Section 3.2.4)      |

Implementations MUST process ledger frames in order. A receiver MUST NOT process an entity payload until a corresponding PENDING or PROCESSING status has been received on the Ledger Stream.

#### 3.2.3 Entity Streams (Streams 1+)

Entity Streams carry the actual payload data and MUST use QUIC Stream IDs starting from 1. Each entity SHOULD be transmitted on its own dedicated QUIC stream to leverage QUIC's native multiplexing and eliminate head-of-line blocking between independent entities.

#### 3.2.4 Checkpoint Semantics

Checkpoints provide synchronization barriers within the entity stream. When a CHECKPOINT status frame is transmitted on the Ledger Stream:

1. The checkpoint MUST include a unique Entity ID (checkpoint identifier)
2. All entities with Entity IDs less than the checkpoint identifier MUST reach a terminal state before the checkpoint is considered satisfied
3. Entities with Entity IDs greater than the checkpoint identifier MUST NOT be marked COMPLETE until the checkpoint is satisfied
4. Receivers MUST acknowledge checkpoint satisfaction on the Ledger Stream

### 3.3 Processing Pipeline

PipeStream defines a processing pipeline model wherein entities flow through a directed acyclic graph (DAG) of processing workers. Each worker performs one of four fundamental actions on entities.

#### 3.3.1 Pipeline Actions

| Action  | Description                                           |
|---------|-------------------------------------------------------|
| CONNECT | Establish connection and initiate entity stream       |
| PARSE   | Decompose entity structure (may vaporize)             |
| PROCESS | Transform entity content (1:1 transformation)         |
| SINK    | Terminal consumption of entity                        |

Implementations MUST support all four actions. Workers MAY implement any combination of actions.

### 3.4 Connection Lifecycle

A PipeStream connection follows this lifecycle:

1. **Establishment:** Client initiates QUIC connection with ALPN identifier "pipestream/1"
2. **Ledger Initialization:** Client opens Stream 0 as bidirectional Ledger Stream
3. **Capability Exchange:** Client and server exchange supported layers and actions
4. **Entity Streaming:** Entities are transmitted per Sections 3.2.2 and 3.2.3
5. **Termination:** Connection closes via QUIC CONNECTION_CLOSE or application-level shutdown

Implementations MUST use the ALPN identifier "pipestream/1" during TLS negotiation.

### 3.5 Error Handling

Errors are communicated through:

1. **Entity-level:** FAILED status on Ledger Stream with optional error code
2. **Stream-level:** QUIC RESET_STREAM or STOP_SENDING frames
3. **Connection-level:** QUIC CONNECTION_CLOSE with application error code

---

## 4. QUIC Stream Mapping

### 4.1. Ledger Stream (Stream 0)

The Ledger Stream provides the control plane for PipeStream operations, carrying status updates and synchronization information between endpoints.

#### 4.1.1. Stream Identification

The Ledger Stream MUST use QUIC Stream ID 0, which per [RFC 9000] Section 2.1 is a client-initiated bidirectional stream. Both client and server transmit ledger frames on this single bidirectional stream.

#### 4.1.2. Stream Properties

Implementations MUST adhere to the following requirements for the Ledger Stream:

1. The client MUST open Stream 0 before any Entity Streams.
2. Stream 0 MUST remain open for the duration of the PipeStream session.
3. Stream 0 MUST NOT carry entity payload data; it is reserved exclusively for ledger frames.
4. Implementations SHOULD assign the Ledger Stream higher priority than Entity Streams using QUIC priority mechanisms [RFC 9218].

#### 4.1.3. Flow Control Considerations

The Ledger Stream carries small, fixed-size frames (3 octets each). Implementations MUST ensure adequate flow control credits are maintained:

- The initial MAX_STREAM_DATA for Stream 0 SHOULD be at least 4096 octets, allowing approximately 1365 ledger frames before requiring credit updates.
- Implementations SHOULD NOT block Entity Stream transmission due to Ledger Stream flow control exhaustion.
- Receivers MUST process Ledger Stream data promptly and issue MAX_STREAM_DATA frames to prevent sender blocking.

#### 4.1.4. Keep-Alive and Heartbeat Mechanisms

To maintain session liveness and detect connection failures, PipeStream defines a heartbeat mechanism on the Ledger Stream:

```
   Heartbeat Frame (3 octets):
   +----------------+----------+
   | Entity ID      | Status   |
   | 0xFFFF         | 0x00     |
   +----------------+----------+
```

The following requirements apply:

1. When no ledger updates have been transmitted for a duration exceeding the KEEPALIVE_TIMEOUT (default: 30 seconds), an endpoint SHOULD send a heartbeat frame.
2. Heartbeat frames use the reserved Entity ID 0xFFFF with status PENDING (0x00).
3. If no data (including heartbeats) is received on Stream 0 for a duration exceeding 3 * KEEPALIVE_TIMEOUT, an endpoint SHOULD consider the connection failed and close it with error code PIPESTREAM_IDLE_TIMEOUT (0x01).

### 4.2. Entity Streams (Streams 1+)

Entity Streams carry the actual document entity payloads. Each entity is transmitted on a dedicated unidirectional stream, leveraging QUIC's native multiplexing capabilities.

#### 4.2.1. Stream Type and Allocation

Entity Streams MUST be unidirectional streams as defined in [RFC 9000] Section 2.1.

```
   Client-Initiated Unidirectional Streams:
   Stream IDs: 2, 6, 10, 14, ... (4n + 2 where n >= 0)

   Server-Initiated Unidirectional Streams:
   Stream IDs: 3, 7, 11, 15, ... (4n + 3 where n >= 0)
```

#### 4.2.2. One Entity Per Stream

PipeStream employs a strict one-entity-per-stream model:

1. Each Entity Stream MUST carry exactly one entity.
2. The entity_id in the Entity Frame header MUST be unique within the session.
3. Once an entity has been completely transmitted, the sender MUST close the stream.
4. Implementations MUST NOT reuse a stream for multiple entities.

---

## 5. Frame Formats

This section defines the wire formats for PipeStream frames. All multi-octet integer fields are encoded in network byte order (big-endian) unless otherwise specified.

### 5.1. Ledger Frames

Ledger frames are fixed-size control messages transmitted on Stream 0. They provide lightweight status updates for entity processing coordination.

#### 5.1.1. Basic Ledger Frame Format

A basic ledger frame is exactly 3 octets:

```
    0                   1                   2
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |         Entity ID             |    Status     |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

   Entity ID (16 bits):
      Unsigned integer identifying the entity. Range 0x0000-0xFFFD
      for regular entities; 0xFFFE-0xFFFF reserved.

   Status (8 bits):
      Single octet indicating entity processing state.
```

#### 5.1.2. Status Byte Encoding

The status byte indicates the current processing state of an entity:

| Value | Name        | Description                            |
|-------|-------------|----------------------------------------|
| 0x00  | PENDING     | Entity announced, transmission not yet started |
| 0x01  | PROCESSING  | Entity data transmission in progress   |
| 0x02  | COMPLETE    | Entity successfully transmitted and stream closed |
| 0x03  | FAILED      | Entity transmission failed             |
| 0x04  | CHECKPOINT  | Synchronization point                  |
| 0x05-0xFF | Reserved | Reserved for future use               |

#### 5.1.3. Reserved Entity ID Values

| Value  | Name              | Purpose                            |
|--------|-------------------|------------------------------------|
| 0x0000 | NULL_ENTITY       | Reserved; MUST NOT be used         |
| 0xFFFE | CHECKPOINT_MARKER | Synchronization points             |
| 0xFFFF | CONNECTION_LEVEL  | Connection-wide control messages   |

### 5.2. Entity Frames

Entity frames carry the actual document entity data on Entity Streams. They consist of a length-prefixed header followed by the payload.

#### 5.2.1. Entity Frame Overview

```
   Entity Frame Structure:

   +---------------------------+
   |    Header Length (4)      |   4 octets, big-endian uint32
   +---------------------------+
   |                           |
   |    Header (Protobuf)      |   Variable length
   |                           |
   +---------------------------+
   |                           |
   |    Payload                |   Variable length (per header)
   |                           |
   +---------------------------+
```

#### 5.2.2. Entity Frame Header

The entity frame header is encoded using Protocol Buffers (proto3):

```protobuf
   syntax = "proto3";

   message EntityHeader {
       uint32 entity_id = 1;
       uint32 parent_id = 2;
       uint32 layer = 3;
       string content_type = 4;
       uint64 payload_length = 5;
       bytes checksum = 6;
       map<string, string> metadata = 7;
       ChunkInfo chunk_info = 8;
   }

   message ChunkInfo {
       uint32 total_chunks = 1;
       uint32 chunk_index = 2;
       uint64 chunk_offset = 3;
   }
```

#### 5.2.3. Checksum Algorithm

PipeStream uses SHA-256 [FIPS 180-4] for payload integrity verification. The checksum MUST be exactly 32 octets. Receivers MUST validate the checksum and report FAILED status if validation fails.

---

## 6. Entity Model

This section defines the entity model for PipeStream, specifying the structure and semantics of documents flowing through the distributed processing pipeline.

### 6.1. Core Fields

Every PipeStream entity is represented as a PipeDoc message. The following table defines the core fields:

| Field | Type | Requirement | Description |
|-------|------|-------------|-------------|
| doc_id | string | REQUIRED | Unique identifier for this document |
| ownership | OwnershipContext | OPTIONAL | Tracks ownership for multi-tenancy |
| doc_id_derivation | DocIdDerivation | OPTIONAL | Records how doc_id was determined |

### 6.2. Four Layers

The PipeStream entity model supports four distinct data layers:

```
+------------------------------------------------------------------+
|                          PipeDoc                                 |
|  +------------------------------------------------------------+  |
|  |  Layer 1: BlobBag (Raw Binary Data)                        |  |
|  |  - Original document bytes                                  |  |
|  |  - Images, attachments, embedded files                      |  |
|  +------------------------------------------------------------+  |
|  +------------------------------------------------------------+  |
|  |  Layer 2: SemanticLayer (Semantic Chunks)                  |  |
|  |  - SemanticProcessingResult with SemanticChunks            |  |
|  |  - Text segments with vector embeddings                     |  |
|  +------------------------------------------------------------+  |
|  +------------------------------------------------------------+  |
|  |  Layer 3: ParsedData (Structured Extraction)               |  |
|  |  - ParsedMetadata from various parsers                      |  |
|  |  - JSON/key-value representation                            |  |
|  +------------------------------------------------------------+  |
|  +------------------------------------------------------------+  |
|  |  Layer 4: CustomEntity (Extension Point)                   |  |
|  |  - structured_data as google.protobuf.Any                  |  |
|  |  - Domain-specific protobuf payloads                        |  |
|  +------------------------------------------------------------+  |
+------------------------------------------------------------------+
```

#### 6.2.1. Layer 1: BlobBag (Raw Binary Data)

The BlobBag layer provides storage for raw binary data. Designed for original document bytes, images, attachments, and binary content requiring parsing.

#### 6.2.2. Layer 2: SemanticLayer (Structured Semantic Chunks)

The SemanticLayer provides structured semantic chunking and vector embedding capabilities. Enables vector search, semantic similarity matching, and RAG workflows.

#### 6.2.3. Layer 3: ParsedData (Extracted Structured Data)

The ParsedData layer stores structured data extracted by document parsers. Supports multiple parsers operating on the same document.

#### 6.2.4. Layer 4: CustomEntity (Extension Point)

The CustomEntity layer provides an extension point for domain-specific data using google.protobuf.Any for extensibility.

### 6.3. Protobuf Encoding

All PipeStream entities MUST be encoded using Protocol Buffers version 3 (proto3) syntax. The complete schema is provided in Appendix A.

---

## 7. Processing Actions

This section defines the four fundamental processing actions in PipeStream: CONNECT, PARSE, PROCESS, and SINK.

### 7.1. Overview

```
                +─────────────────────────────────────────────+
                │           PipeStream Action Flow            │
                +─────────────────────────────────────────────+
                                     │
                                     ▼
                ┌─────────────────────────────────────────────┐
                │                  CONNECT                    │
                │         (Session Establishment)             │
                └─────────────────────────────────────────────┘
                                     │
                                     ▼
                ┌─────────────────────────────────────────────┐
                │                   PARSE                     │
                │        (Vaporization: 1:N possible)         │
                └─────────────────────────────────────────────┘
                                     │
                       ┌─────────────┼─────────────┐
                       ▼             ▼             ▼
                ┌───────────┐ ┌───────────┐ ┌───────────┐
                │  PROCESS  │ │  PROCESS  │ │  PROCESS  │
                │   (1:1)   │ │   (1:1)   │ │   (N:1)   │
                └───────────┘ └───────────┘ └───────────┘
                       │             │             │
                       └─────────────┼─────────────┘
                                     ▼
                ┌─────────────────────────────────────────────┐
                │                   SINK                      │
                │          (Terminal Consumption)             │
                └─────────────────────────────────────────────┘
```

### 7.2. CONNECT Action

The CONNECT action establishes the entry point to a PipeStream pipeline. It is responsible for session initialization, capability negotiation, authentication, and the submission of initial entities.

#### 7.2.1. Transport Requirements

PipeStream operates over QUIC [RFC9000]. The Application-Layer Protocol Negotiation (ALPN) [RFC7301] token for PipeStream version 1 is:

```
   ALPN Protocol ID: "pipestream/1"
```

### 7.3. PARSE Action

The PARSE action performs document structure analysis and serves as the primary vaporization point, enabling 1:N decomposition of complex documents into constituent entities.

#### 7.3.1. Vaporization Semantics

Vaporization is the controlled decomposition of a single input entity into multiple output entities while maintaining referential integrity:

- **1:1 Mapping**: Single input produces single output (simple documents)
- **1:N Mapping**: Single input produces multiple outputs (compound documents)
- **1:0 Mapping**: Single input produces no outputs (filtered/empty documents)

### 7.4. PROCESS Action

The PROCESS action performs content transformation on entities, supporting both 1:1 transformations (enrichment, conversion) and N:1 operations (rejoin, aggregation).

#### 7.4.1. Processing Modes

| Mode | Description |
|------|-------------|
| TRANSFORM | 1:1 entity transformation |
| REJOIN | N:1 merge of siblings from vaporization |
| AGGREGATE | N:1 with reduction function |
| PASSTHROUGH | Metadata-only modification |

### 7.5. SINK Action

The SINK action represents terminal consumption of entities, performing final operations such as indexing, storage, and notification.

#### 7.5.1. Sink Types

| Type | Description |
|------|-------------|
| INDEX | Search engine integration |
| STORAGE | Blob storage persistence |
| NOTIFICATION | Webhook/messaging triggers |

---

## 8. Reassembly Semantics

### 8.1 Parts Ledger

The Parts Ledger is a distributed data structure that maintains the hierarchical relationships between vaporized entities and their constituent parts.

#### 8.1.1 Ledger Entry Structure

Each Parts Ledger entry SHALL contain:

- Parent ID (64 bits): Identifier of the parent entity
- Child Count (16 bits): Number of child entities produced
- Children IDs: Array of 64-bit entity identifiers
- Completion Status: Array of 8-bit status codes per child
- Checkpoint Scope (32 bits): Innermost checkpoint scope identifier
- Creation Timestamp (64 bits): Microseconds since UNIX epoch
- Resolution State (8 bits): Current state of the entry

#### 8.1.2 Completion Status Codes

| Value | Name | Description |
|-------|------|-------------|
| 0x00 | PENDING | Child has not yet completed |
| 0x01 | COMPLETE | Child completed successfully |
| 0x02 | FAILED | Child processing failed |
| 0x03 | TIMEOUT | Child exceeded processing deadline |
| 0x04 | CANCELLED | Child was explicitly cancelled |
| 0x05 | ORPHANED | Child was orphaned |

### 8.2 Checkpoint Blocking

Checkpoints provide synchronization barriers that ensure all preceding entities have completed processing before subsequent entities may proceed.

#### 8.2.1 Checkpoint Satisfaction Conditions

A checkpoint SHALL be considered satisfied when:

1. All entities with sequence numbers less than the checkpoint's Sequence Number have reached a terminal state
2. If Dependent IDs are specified, all listed entities have reached a terminal state
3. All Parts Ledger entries within the checkpoint's scope have been resolved
4. All nested checkpoints within this checkpoint's scope have been satisfied

### 8.3 Eventual Consistency (Fibonacci Heap)

Due to the distributed nature of PipeStream processing, child entities MAY complete out of order. Implementations SHALL use a priority queue (Fibonacci heap recommended) to efficiently track which Parts Ledger entries are ready for rejoin.

The Fibonacci heap provides:

| Operation | Amortized Complexity |
|-----------|---------------------|
| Insert | O(1) |
| Find-min | O(1) |
| Extract-min | O(log n) |
| Decrease-key | O(1) |

### 8.4 Parent Reference Resolution

#### 8.4.1 Root Entity Identification

Root entities are identified by:
- Null Parent: parent_id = 0x0000000000000000
- Self-Referential: parent_id = entity_id

#### 8.4.2 Cycle Prevention (DAG Enforcement)

The parent-child relationship graph MUST form a Directed Acyclic Graph (DAG). Implementations MUST prevent cycles through:

1. Monotonic ID Assignment
2. Depth Tracking (default max: 1024)
3. Ancestry Verification before creating Parts Ledger entries

---

## 9. Security Considerations

This section describes security considerations for the PipeStream protocol.

### 9.1 Transport Security

#### 9.1.1 QUIC Transport Layer Security

PipeStream inherits its transport security properties from QUIC [RFC 9000] and TLS 1.3 [RFC 8446]. All PipeStream connections MUST use QUIC with TLS 1.3 or later. Implementations MUST NOT provide any mechanism to disable or downgrade transport encryption.

#### 9.1.2 Mandatory Encryption

Implementations MUST reject any attempt to establish unencrypted PipeStream connections.

#### 9.1.3 Certificate-Based Authentication

PipeStream endpoints MUST authenticate using X.509 certificates as specified in [RFC 5280]. Server authentication is REQUIRED for all connections. Client authentication SHOULD be required in production deployments.

### 9.2 Application Security

#### 9.2.1 Entity Payload Integrity

Each Entity transmitted via PipeStream MUST include a SHA-256 checksum of its payload. Receiving implementations MUST verify the checksum before processing.

#### 9.2.2 Parts Ledger Tampering Prevention

Implementations MUST:
1. Authenticate Ledger Updates
2. Maintain Update Sequence Numbers
3. Validate State Transitions
4. Compute Ledger Digests periodically

### 9.3 Resource Exhaustion

#### 9.3.1 Vaporization Depth Limits

Implementations MUST enforce a maximum vaporization depth. The default maximum depth MUST be 32 layers.

#### 9.3.2 Entity Count Limits

Implementations MUST enforce a maximum Entity count per session. The default maximum SHOULD be 1,000,000 Entities.

#### 9.3.3 Checkpoint Timeout Requirements

Implementations MUST associate a timeout with each Checkpoint. The default timeout MUST be 3600 seconds (1 hour).

### 9.4 Privacy Considerations

#### 9.4.1 Entity Metadata Exposure

Entity frames contain metadata that may reveal information about documents being processed. Implementations requiring privacy protection SHOULD consider:
1. Using randomized Entity IDs
2. Padding payloads to fixed sizes
3. Introducing dummy Entities

---

## 10. IANA Considerations

This document requests several registrations from IANA.

### 10.1 ALPN Identifier Registration

IANA is requested to add the following entry to the "TLS Application-Layer Protocol Negotiation (ALPN) Protocol IDs" registry:

| Protocol | Identification Sequence | Reference |
|----------|------------------------|-----------|
| PipeStream Version 1 | "pipestream/1" | [this document] |

### 10.2 PipeStream Frame Type Registry

| Value | Frame Type Name | Reference |
|-------|-----------------|-----------|
| 0x50 | LEDGER | Section 5 |
| 0x51 | CHECKPOINT | Section 8.2 |
| 0x52 | LEDGER_ACK | Section 5 |
| 0x53 | CHECKPOINT_ACK | Section 8.2 |
| 0x60 | ENTITY | Section 4 |
| 0x61 | ENTITY_START | Section 4 |
| 0x62 | ENTITY_CONTINUATION | Section 4 |
| 0x63 | ENTITY_END | Section 4 |

### 10.3 PipeStream Status Code Registry

| Value | Status Code Name | Description |
|-------|------------------|-------------|
| 0x00 | PENDING | Entity received, awaiting processing |
| 0x01 | PROCESSING | Entity processing in progress |
| 0x02 | COMPLETE | Entity processing completed |
| 0x03 | FAILED | Entity processing failed |
| 0x04 | CHECKPOINT | Entity state saved to checkpoint |
| 0x05 | VAPORIZING | Entity being decomposed |
| 0x06 | AGGREGATING | Child entities being combined |
| 0x07 | SUSPENDED | Processing temporarily suspended |

### 10.4 PipeStream Error Code Registry

| Value | Error Code Name | Description |
|-------|-----------------|-------------|
| 0x00 | PIPESTREAM_NO_ERROR | Graceful shutdown |
| 0x01 | PIPESTREAM_INTERNAL_ERROR | Implementation error |
| 0x02 | PIPESTREAM_IDLE_TIMEOUT | Connection idle timeout |
| 0x03 | PIPESTREAM_LEDGER_RESET | Ledger state must be reset |
| 0x04 | PIPESTREAM_INTEGRITY_ERROR | Checksum verification failed |
| 0x05 | PIPESTREAM_ENTITY_INVALID | Invalid entity frame format |
| 0x06 | PIPESTREAM_ENTITY_TOO_LARGE | Entity exceeds size limit |
| 0x07 | PIPESTREAM_DEPTH_EXCEEDED | Vaporization depth exceeded |

### 10.5 URI Scheme Registration

IANA is requested to register the "pipestream" URI scheme:

```
pipestream-URI = "pipestream://" authority "/" session-id ["/" entity-id]
```

Examples:
- `pipestream://processor.example.com/a1b2c3d4`
- `pipestream://processor.example.com:8443/a1b2c3d4/e5f6`

---

## Appendix A: Protobuf Schema Reference

This appendix defines the Protocol Buffers (proto3) message schemas used by the PipeStream protocol.

### A.1 Protocol-Level Messages

#### A.1.1 EntityHeader

```protobuf
syntax = "proto3";

package pipestream.protocol.v1;

message EntityHeader {
  string entity_id = 1;
  optional string parent_id = 2;
  uint32 layer = 3;
  string content_type = 4;
  uint64 payload_length = 5;
  string checksum = 6;
  map<string, string> metadata = 7;
}
```

#### A.1.2 ChunkInfo

```protobuf
message ChunkInfo {
  uint32 total_chunks = 1;
  uint32 chunk_index = 2;
  uint64 chunk_offset = 3;
}
```

#### A.1.3 LedgerFrame

```protobuf
enum EntityStatus {
  ENTITY_STATUS_UNSPECIFIED = 0;
  ENTITY_STATUS_RECEIVED = 1;
  ENTITY_STATUS_PROCESSING = 2;
  ENTITY_STATUS_COMPLETED = 3;
  ENTITY_STATUS_FAILED = 4;
  ENTITY_STATUS_SKIPPED = 5;
  ENTITY_STATUS_RETRYING = 6;
}

message LedgerFrame {
  string entity_id = 1;
  EntityStatus status = 2;
  google.protobuf.Any extended_data = 3;
}
```

#### A.1.4 CheckpointFrame

```protobuf
message CheckpointFrame {
  string checkpoint_id = 1;
  uint64 sequence_number = 2;
  uint32 flags = 3;
  uint32 timeout_ms = 4;
}
```

#### A.1.5 PartsLedgerEntry

```protobuf
enum CompletionStatus {
  COMPLETION_STATUS_UNSPECIFIED = 0;
  COMPLETION_STATUS_IN_PROGRESS = 1;
  COMPLETION_STATUS_CHILDREN_ENUMERATED = 2;
  COMPLETION_STATUS_ALL_COMPLETED = 3;
  COMPLETION_STATUS_PARTIAL_FAILURE = 4;
}

message PartsLedgerEntry {
  string parent_id = 1;
  uint32 child_count = 2;
  repeated string children_ids = 3;
  CompletionStatus completion_status = 4;
}
```

### A.2 Entity Data Messages

#### A.2.1 PipeDoc

```protobuf
message PipeDoc {
  string doc_id = 1;
  SearchMetadata search_metadata = 2;
  BlobBag blob_bag = 3;
  google.protobuf.Any structured_data = 4;
  map<string, ParsedMetadata> parsed_metadata = 5;
  optional OwnershipContext ownership = 6;
  optional DocIdDerivation doc_id_derivation = 7;
}
```

#### A.2.2 BlobBag and Blob

```protobuf
message BlobBag {
  oneof blob_data {
    Blob blob = 1;
    Blobs blobs = 2;
  }
}

message Blob {
  string blob_id = 1;
  string drive_id = 2;
  oneof content {
    bytes data = 3;
    FileStorageReference storage_ref = 4;
  }
  optional string mime_type = 5;
  optional string filename = 6;
  int64 size_bytes = 8;
  optional string checksum = 9;
  ChecksumType checksum_type = 10;
}

enum ChecksumType {
  CHECKSUM_TYPE_UNSPECIFIED = 0;
  CHECKSUM_TYPE_MD5 = 1;
  CHECKSUM_TYPE_SHA1 = 2;
  CHECKSUM_TYPE_SHA256 = 3;
  CHECKSUM_TYPE_SHA512 = 4;
}
```

#### A.2.3 SemanticChunk

```protobuf
message SemanticChunk {
  string chunk_id = 1;
  int64 chunk_number = 2;
  ChunkEmbedding embedding_info = 3;
  map<string, google.protobuf.Value> metadata = 4;
}

message ChunkEmbedding {
  string text_content = 1;
  repeated float vector = 2;
  optional string chunk_id = 3;
  optional int32 original_char_start_offset = 4;
  optional int32 original_char_end_offset = 5;
}
```

### A.3 Wire Encoding Notes

All messages MUST use Protocol Buffers version 3 (proto3) wire format:

| Wire Type | Encoding | Used For |
|-----------|----------|----------|
| 0 | Varint | int32, int64, uint32, uint64, bool, enum |
| 1 | 64-bit | fixed64, sfixed64, double |
| 2 | Length-delimited | string, bytes, embedded messages |
| 5 | 32-bit | fixed32, sfixed32, float |

---

## References

### Normative References

- [RFC 2119] Bradner, S., "Key words for use in RFCs to Indicate Requirement Levels", BCP 14, RFC 2119, March 1997.
- [RFC 8174] Leiba, B., "Ambiguity of Uppercase vs Lowercase in RFC 2119 Key Words", BCP 14, RFC 8174, May 2017.
- [RFC 9000] Iyengar, J., Ed. and M. Thomson, Ed., "QUIC: A UDP-Based Multiplexed and Secure Transport", RFC 9000, May 2021.
- [RFC 9001] Thomson, M., Ed. and S. Turner, Ed., "Using TLS to Secure QUIC", RFC 9001, May 2021.
- [RFC 8446] Rescorla, E., "The Transport Layer Security (TLS) Protocol Version 1.3", RFC 8446, August 2018.
- [RFC 5280] Cooper, D., et al., "Internet X.509 Public Key Infrastructure Certificate and CRL Profile", RFC 5280, May 2008.
- [RFC 7301] Friedl, S., et al., "Transport Layer Security (TLS) Application-Layer Protocol Negotiation Extension", RFC 7301, July 2014.
- [RFC 8126] Cotton, M., et al., "Guidelines for Writing an IANA Considerations Section in RFCs", BCP 26, RFC 8126, June 2017.

### Informative References

- [FIPS 180-4] National Institute of Standards and Technology, "Secure Hash Standard (SHS)", FIPS PUB 180-4, August 2015.

---

## Authors' Addresses

Kevin Rickert
Email: [To be completed]

---

*This document was generated using the PipeStream spec's own vaporize/reassemble pattern: the specification was decomposed into parallel section agents, each wrote their section, and the results were reassembled into this final document.*
