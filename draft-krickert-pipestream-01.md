# PipeStream: A Recursive Entity Streaming Protocol for Distributed Document Processing

**Internet-Draft**

**draft-krickert-pipestream-01**

**Intended status:** Standards Track

**Expires:** August 23, 2026

---

## Abstract

This document specifies PipeStream, a recursive entity streaming protocol designed for distributed document processing over QUIC transport. PipeStream enables the decomposition ("vaporization") of documents into constituent entities, their transmission across distributed processing nodes, and subsequent reassembly ("rejoining") at destination endpoints.

The protocol employs a dual-stream architecture consisting of a data stream for entity payload transmission and a ledger stream for tracking entity completion status and maintaining consistency. PipeStream defines four hierarchical data layers for entity representation: BlobBag for raw binary data, SemanticLayer for annotated content with metadata, ParsedData for structured extracted information, and CustomEntity for application-specific extensions.

PipeStream is organized into three protocol layers: Layer 0 (Core) provides basic streaming with vaporize/rejoin semantics; Layer 1 (Recursive) adds hierarchical scoping and digest propagation; Layer 2 (Resilience) adds yield/resume, claim checks, and completion policies. Implementations MUST support Layer 0 and MAY support Layers 1 and 2.

To ensure consistency across distributed processing pipelines, PipeStream implements checkpoint blocking, whereby processing nodes MUST synchronize at defined points before proceeding. This mechanism guarantees that all constituent parts of a vaporized document are successfully processed before reassembly operations commence.

---

## Status of This Memo

This Internet-Draft is submitted in full conformance with the provisions of BCP 78 and BCP 79.

Internet-Drafts are working documents of the Internet Engineering Task Force (IETF). Note that other groups may also distribute working documents as Internet-Drafts.

---

## Table of Contents

1. Introduction
2. Terminology
3. Protocol Layers
4. Protocol Overview
5. QUIC Stream Mapping
6. Frame Formats
7. Entity Model
8. Processing Actions
9. Reassembly Semantics
10. Security Considerations
11. IANA Considerations
Appendix A: Protobuf Schema Reference
Appendix B: Protocol Layer Capability Matrix
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
- Scale from single documents to collections of millions of documents

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

### 1.4. Protocol Layering

PipeStream is organized into three protocol layers to accommodate varying deployment requirements:

| Protocol Layer | Name | Description |
|----------------|------|-------------|
| Layer 0 | Core | Basic streaming, vaporize/rejoin, checkpoint |
| Layer 1 | Recursive | Hierarchical scopes, digest propagation, barriers |
| Layer 2 | Resilience | Yield/resume, claim checks, completion policies |

Implementations MUST support Layer 0. Support for Layers 1 and 2 is OPTIONAL and negotiated during connection establishment.

### 1.5. Scope

This document specifies the PipeStream protocol including message formats, state machines, error handling, and the interaction between data and ledger streams. The document defines the four standard data layers but does not mandate specific processing semantics, which are left to application-layer specifications.

---

## 2. Terminology

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "NOT RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in BCP 14 [RFC2119] [RFC8174] when, and only when, they appear in all capitals, as shown here.

### 2.1. Protocol Entities

**Entity**
:   The fundamental unit of data flowing through a PipeStream pipeline. An Entity represents either a complete document or a constituent part of a decomposed document. Each Entity possesses a unique identifier within its processing scope and carries payload data in one of the four defined Layer formats. Entities are immutable once created; transformations produce new Entities rather than modifying existing ones.

**Document**
:   A logical unit of content submitted to a PipeStream pipeline for processing. A Document enters the pipeline as a single root Entity and MAY be decomposed into multiple Entities during processing. The Document is considered complete when its root Entity (or the rejoined result of its decomposition) exits the pipeline.

**Scope**
:   A hierarchical namespace for Entity IDs. Each scope maintains its own Entity ID space, cursor, and Parts Ledger. Scopes enable collections to contain documents, documents to contain parts, and parts to contain jobs, each with independent ID management. (Protocol Layer 1)

### 2.2. Decomposition and Reassembly

**Vaporize**
:   The operation of decomposing a document or Entity into multiple constituent Entities for parallel or distributed processing. When an Entity is vaporized, the originating node MUST create a Parts Ledger entry recording the identifiers of all resulting sub-entities. The vaporization operation is recursive; a sub-entity produced by vaporization MAY itself be vaporized, creating a tree of decomposition.

**Rejoin**
:   The operation of reassembling multiple Entities back into a single composite Entity or Document. A rejoin operation MUST NOT proceed until all constituent Entities listed in the corresponding Parts Ledger entry have been received and processed (or handled according to the Completion Policy).

### 2.3. Consistency Mechanisms

**Checkpoint**
:   A synchronization point in the processing pipeline where all in-flight Entities MUST reach a consistent state before processing may continue. A checkpoint is considered "satisfied" when all Parts Ledger entries created before the checkpoint have been resolved.

**Barrier**
:   A synchronization point scoped to a specific subtree. Unlike checkpoints which are global, barriers block only entities dependent on a specific parent's descendants. (Protocol Layer 1)

**Ledger**
:   The control stream that tracks Entity completion status throughout the processing pipeline. The Ledger is transmitted on a dedicated QUIC stream parallel to the data stream.

**Parts Ledger**
:   A data structure within the Ledger that tracks the relationship between a composite Entity and its constituent sub-entities produced by vaporization.

**Cursor**
:   A pointer to the lowest unresolved Entity ID within a scope. Entity IDs behind the cursor are considered resolved and MAY be recycled. The cursor enables efficient ID space management without global coordination.

### 2.4. Resilience Mechanisms (Protocol Layer 2)

**Yield**
:   A temporary pause in Entity processing, typically due to external dependencies (API calls, rate limiting, human approval). A yielded Entity carries a continuation token enabling resumption without reprocessing.

**Claim Check**
:   A detached reference to a deferred Entity that can be queried or resumed independently, potentially in a different session. Claim checks enable asynchronous processing patterns and retry queues.

**Completion Policy**
:   A configuration specifying how to handle partial failures during vaporization. Policies include STRICT (all must succeed), LENIENT (continue with partial results), BEST_EFFORT (complete with whatever succeeds), and QUORUM (require minimum success ratio).

### 2.5. Data Representation

**Data Layer**
:   One of four defined representations for Entity payload data:

    1. **BlobBag**: Raw binary data with minimal metadata
    2. **SemanticLayer**: Annotated content with structural and semantic metadata
    3. **ParsedData**: Structured information extracted from document content
    4. **CustomEntity**: Application-specific extension Layer

### 2.6. Additional Terms

**Pipeline**
:   A configured sequence of processing stages through which Entities flow.

**Stage**
:   A single processing step within a Pipeline.

**Scope Digest**
:   A cryptographic summary (Merkle root) of all Entity statuses within a completed scope, propagated to parent scopes for efficient verification. (Protocol Layer 1)

---

## 3. Protocol Layers

PipeStream defines three protocol layers that build upon each other. This layered approach allows simple deployments to use only the core protocol while complex deployments can leverage advanced features.

### 3.1. Layer 0: Core Protocol

Layer 0 provides the fundamental streaming capabilities:

- Ledger frame (32-bit, word-aligned)
- Entity frame (header + payload)
- Status codes: PENDING, PROCESSING, COMPLETE, FAILED, CHECKPOINT
- Parts Ledger for parent-child tracking
- Cursor-based Entity ID recycling
- Single-level vaporize/rejoin
- Checkpoint blocking

All implementations MUST support Layer 0.

### 3.2. Layer 1: Recursive Extension

Layer 1 adds hierarchical processing capabilities:

- Scoped Entity ID namespaces (collection → document → part → job)
- SCOPE_OPEN and SCOPE_CLOSE frames
- SCOPE_DIGEST for Merkle-based subtree completion
- BARRIER for subtree-scoped synchronization
- Nested vaporization with depth tracking

Layer 1 is OPTIONAL. Implementations advertise Layer 1 support during capability negotiation.

### 3.3. Layer 2: Resilience Extension

Layer 2 adds fault tolerance and async processing:

- YIELDED status with continuation tokens
- DEFERRED status with claim checks
- RETRYING, SKIPPED, ABANDONED statuses
- Completion policies (STRICT, LENIENT, BEST_EFFORT, QUORUM)
- Claim check query/response frames
- Stopping point validation

Layer 2 is OPTIONAL and requires Layer 1. Implementations advertise Layer 2 support during capability negotiation.

### 3.4. Capability Negotiation

During CONNECT, endpoints exchange supported capabilities:

```protobuf
message Capabilities {
  bool layer0_core = 1;           // Always true
  bool layer1_recursive = 2;      // Scoped IDs, digests
  bool layer2_resilience = 3;     // Yield, claim checks
  uint32 max_scope_depth = 4;     // Default: 8
  uint32 max_entities_per_scope = 5;  // Default: 1,048,576 (2^20)
  uint32 max_window_size = 6;     // Default: 524,288 (2^19)
}
```

Peers negotiate down to common capabilities. If Layer 2 is requested but Layer 1 is not supported, Layer 2 MUST be disabled.

---

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

The protocol MUST maintain strict separation between the control plane (ledger) and the data plane (entities).

#### 4.1.5. QUIC Foundation

PipeStream MUST be implemented over QUIC [RFC9000] to leverage:

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
         |  |  |  Stream 0: Ledger (Control Plane)  |  |  |
         |  |  |  [LEDGER][LEDGER][LEDGER]...       |  |  |
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
3. **Ledger Initialization:** Client opens Stream 0 as bidirectional Ledger Stream
4. **Entity Streaming:** Entities are transmitted per Sections 5 and 6
5. **Termination:** Connection closes via QUIC CONNECTION_CLOSE or application-level shutdown

---

## 5. QUIC Stream Mapping

### 5.1. Ledger Stream (Stream 0)

The Ledger Stream provides the control plane for PipeStream operations.

#### 5.1.1. Stream Identification

The Ledger Stream MUST use QUIC Stream ID 0, which per [RFC 9000] Section 2.1 is a client-initiated bidirectional stream.

#### 5.1.2. Stream Properties

1. The client MUST open Stream 0 before any Entity Streams.
2. Stream 0 MUST remain open for the duration of the PipeStream session.
3. Stream 0 MUST NOT carry entity payload data.
4. Implementations SHOULD assign the Ledger Stream higher priority than Entity Streams.

#### 5.1.3. Flow Control Considerations

The Ledger Stream carries small, fixed-size frames (4 octets each for basic frames). Implementations MUST ensure adequate flow control credits:

- The initial MAX_STREAM_DATA for Stream 0 SHOULD be at least 8192 octets.
- Implementations SHOULD NOT block Entity Stream transmission due to Ledger Stream flow control exhaustion.

#### 5.1.4. Heartbeat Mechanism

To maintain session liveness:

```
   Heartbeat Frame (4 octets):
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |0|0|1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1|0 0 0 0|0 0 0 0 0 0|
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |E|C|<-- Entity ID = 0xFFFFF (20 bits) -->|Stat=0| Flags=0    |

   E=0 (no extended data), C=0 (no cursor update)
   Entity ID = 0xFFFFF (CONNECTION_LEVEL)
   Status = 0x0 (UNSPECIFIED, used as heartbeat signal)
```

When no ledger updates have been transmitted for KEEPALIVE_TIMEOUT (default: 30 seconds), an endpoint SHOULD send a heartbeat frame. If no data is received on Stream 0 for 3 * KEEPALIVE_TIMEOUT, the connection SHOULD be closed with PIPESTREAM_IDLE_TIMEOUT (0x02).

### 5.2. Entity Streams (Streams 2+)

Entity Streams carry the actual document entity payloads.

#### 5.2.1. Stream Type and Allocation

Entity Streams MUST be unidirectional streams:

```
   Client-Initiated Unidirectional Streams:
   Stream IDs: 2, 6, 10, 14, ... (4n + 2 where n >= 0)

   Server-Initiated Unidirectional Streams:
   Stream IDs: 3, 7, 11, 15, ... (4n + 3 where n >= 0)
```

#### 5.2.2. One Entity Per Stream

1. Each Entity Stream MUST carry exactly one entity.
2. The entity_id in the Entity Frame header MUST be unique within its scope.
3. Once an entity has been completely transmitted, the sender MUST close the stream.

---

## 6. Frame Formats

This section defines the wire formats for PipeStream frames. All multi-octet integer fields are encoded in network byte order (big-endian).

### 6.1. Ledger Frames (Layer 0)

#### 6.1.1. Basic Ledger Frame Format (32 bits)

```
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |E|C|              Entity ID (20 bits)         |Stat |  Flags  |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

   E (1 bit):
      Extended frame flag. When set, additional data follows the
      basic 4-octet frame.

   C (1 bit):
      Cursor update flag. When set, a 3-octet cursor value follows.

   Entity ID (20 bits):
      Unsigned integer identifying the entity within the current scope.
      Range 0x00000-0xFFFFD for regular entities.
      0xFFFFE: SCOPE_MARKER (Layer 1)
      0xFFFFF: CONNECTION_LEVEL (heartbeat, shutdown)

   Stat (4 bits):
      Status code (see Section 6.1.2).

   Flags (6 bits):
      Reserved for future use. MUST be zero when sent.
      Receivers MUST ignore non-zero flags.
```

#### 6.1.2. Status Codes

| Value | Name        | Layer | Description                            |
|-------|-------------|-------|----------------------------------------|
| 0x0   | UNSPECIFIED | -     | Proto3 default / heartbeat signal      |
| 0x1   | PENDING     | 0     | Entity announced, not yet transmitting |
| 0x2   | PROCESSING  | 0     | Entity transmission in progress        |
| 0x3   | COMPLETE    | 0     | Entity successfully processed          |
| 0x4   | FAILED      | 0     | Entity processing failed               |
| 0x5   | CHECKPOINT  | 0     | Synchronization barrier                |
| 0x6   | VAPORIZING  | 0     | Decomposing into children              |
| 0x7   | AGGREGATING | 0     | Rejoining children                     |
| 0x8   | YIELDED     | 2     | Paused with continuation token         |
| 0x9   | DEFERRED    | 2     | Detached with claim check              |
| 0xA   | RETRYING    | 2     | Retry in progress                      |
| 0xB   | SKIPPED     | 2     | Intentionally skipped (lenient mode)   |
| 0xC   | ABANDONED   | 2     | Timed out, cursor advanced past        |
| 0xD-0xF | Reserved  | -     | Reserved for future use                |

#### 6.1.3. Cursor Update Extension

When C=1, a 3-octet cursor update follows:

```
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |        New Cursor Value (20 bits)    |Reserv |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

The cursor indicates the lowest unresolved Entity ID. IDs below the cursor are considered resolved and MAY be recycled.

#### 6.1.4. Reserved Entity ID Values

| Value   | Name              | Purpose                            |
|---------|-------------------|------------------------------------|
| 0x00000 | NULL_ENTITY       | Reserved; MUST NOT be used         |
| 0xFFFFE | SCOPE_MARKER      | Scope operations (Layer 1)         |
| 0xFFFFF | CONNECTION_LEVEL  | Connection-wide control messages   |

### 6.2. Scoped Ledger Frame (Layer 1)

When Protocol Layer 1 is negotiated, ledger frames support hierarchical scoping:

```
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |E|Dep|    Scope ID (12 bits)   | Local ID (10) |Stat | Flags  |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

   E (1 bit):
      Extended frame flag (same as Layer 0).

   Dep (3 bits):
      Scope depth. 0=root/collection, 1=document, 2=part, etc.
      Maximum depth of 7 (negotiated, default: 7).

   Scope ID (12 bits):
      Identifier for the current scope. Derived from parent path hash.
      Allows 4,096 concurrent scopes per depth level.

   Local ID (10 bits):
      Entity ID within this scope. Allows 1,024 entities per scope
      before requiring cursor advancement.

   Stat (4 bits):
      Status code (same as Layer 0, see Section 6.1.2).

   Flags (2 bits):
      Bit 0: Scope is root of a new document
      Bit 1: Reserved
```

Total: 1 + 3 + 12 + 10 + 4 + 2 = 32 bits (word-aligned).

### 6.3. Scope Digest Frame (Layer 1)

When a scope completes, a digest summarizes its processing:

```
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   | Frame Type = 0x54 (SCOPE_DIGEST)      |    Scope ID (14)     |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                    Entities Processed (32)                    |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                    Entities Succeeded (32)                    |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                    Entities Failed (32)                       |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                    Entities Deferred (32)                     |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                                                               |
   |                    Merkle Root (256 bits)                     |
   |                                                               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

The Merkle root is computed as SHA-256 over all child ledger entries in Entity ID order.

### 6.4. Yield Frame (Layer 2)

When Status = YIELDED (0x7) and E=1:

```
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |1|C|              Entity ID (20)              |0111 |  Flags  |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   | Yield Reason  |         Token Length (12 bits)               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                                                               |
   |                  Yield Token (variable)                       |
   |                  (up to 4095 bytes)                           |
   |                                                               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

   Yield Reason (4 bits):
     0x0 = EXTERNAL_CALL     (waiting on external service)
     0x1 = RATE_LIMITED      (voluntary throttle)
     0x2 = AWAITING_SIBLING  (waiting for specific sibling)
     0x3 = AWAITING_APPROVAL (human/workflow gate)
     0x4 = RESOURCE_BUSY     (semaphore/lock)
     0x5-0xF = Reserved
```

### 6.5. Claim Check Frame (Layer 2)

When Status = DEFERRED (0x8) and E=1:

```
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |1|C|              Entity ID (20)              |1000 |  Flags  |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                                                               |
   |                    Claim Check ID (64 bits)                   |
   |                                                               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                    Expiry Timestamp (32 bits)                 |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

The Claim Check ID can be used to query status or trigger retry in any session.

### 6.6. Claim Check Query/Response Frames (Layer 2)

```
   CLAIM_CHECK_QUERY (Frame Type = 0x70):
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   | Frame Type = 0x70             |           Flags              |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                                                               |
   |                    Claim Check ID (64 bits)                   |
   |                                                               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

   CLAIM_CHECK_RESPONSE (Frame Type = 0x71):
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   | Frame Type = 0x71             | Status        |    Flags     |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                                                               |
   |                    Claim Check ID (64 bits)                   |
   |                                                               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |              Result Entity ID (20 bits)              |Reserv |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### 6.7. Barrier Frame (Layer 1)

```
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   | Frame Type = 0x55 (BARRIER)   |B|      Barrier ID (15 bits)  |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                Parent Entity ID (20 bits)              |Flags|
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

   B (1 bit):
      Barrier satisfied (0 = waiting, 1 = released)

   Flags (6 bits):
      Bit 0: Include parent itself
      Bit 1: Fail-fast on first child failure
      Bit 2-5: Reserved
```

### 6.8. Entity Frames

Entity frames carry the actual document entity data on Entity Streams.

#### 6.8.1. Entity Frame Structure

```
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

#### 6.8.2. Entity Header (Protobuf)

```protobuf
message EntityHeader {
  uint32 entity_id = 1;         // 20-bit local ID
  uint32 parent_id = 2;         // 0 for root entities
  uint32 scope_id = 3;          // Layer 1: scope identifier
  uint32 layer = 4;             // Data layer (0-3)
  string content_type = 5;      // MIME type
  uint64 payload_length = 6;
  bytes checksum = 7;           // SHA-256 (32 bytes)
  map<string, string> metadata = 8;
  ChunkInfo chunk_info = 9;
  CompletionPolicy policy = 10; // Layer 2: failure handling
}
```

#### 6.8.3. Checksum Algorithm

PipeStream uses SHA-256 [FIPS 180-4] for payload integrity verification. The checksum MUST be exactly 32 octets.

---

## 7. Entity Model

### 7.1. Core Fields

Every PipeStream entity is represented as a PipeDoc message:

| Field | Type | Requirement | Description |
|-------|------|-------------|-------------|
| doc_id | string | REQUIRED | Unique document identifier (UUID recommended) |
| entity_id | uint32 | REQUIRED | Scope-local identifier (20-bit) |
| ownership | OwnershipContext | OPTIONAL | Multi-tenancy tracking |

### 7.2. Four Data Layers

```
+------------------------------------------------------------------+
|                          PipeDoc                                 |
|  +------------------------------------------------------------+  |
|  |  Layer 0: BlobBag (Raw Binary Data)                        |  |
|  |  - Original document bytes, images, attachments            |  |
|  +------------------------------------------------------------+  |
|  +------------------------------------------------------------+  |
|  |  Layer 1: SemanticLayer (Semantic Chunks)                  |  |
|  |  - Text segments with vector embeddings                     |  |
|  |  - NLP annotations, NER, classifications                    |  |
|  +------------------------------------------------------------+  |
|  +------------------------------------------------------------+  |
|  |  Layer 2: ParsedData (Structured Extraction)               |  |
|  |  - Key-value pairs, tables, structured fields               |  |
|  +------------------------------------------------------------+  |
|  +------------------------------------------------------------+  |
|  |  Layer 3: CustomEntity (Extension Point)                   |  |
|  |  - Domain-specific protobuf via google.protobuf.Any        |  |
|  +------------------------------------------------------------+  |
+------------------------------------------------------------------+
```

### 7.3. Cloud-Agnostic Storage Reference

```protobuf
message FileStorageReference {
  string provider = 1;           // "s3", "azure", "gcs", "minio"
  string bucket = 2;             // Bucket/container name
  string key = 3;                // Object key/path
  string region = 4;             // Optional region hint
  map<string, string> attrs = 5; // Provider-specific attributes
  EncryptionMetadata encryption = 6;
}

message EncryptionMetadata {
  string algorithm = 1;          // "AES-256-GCM", "AES-256-CBC"
  string key_provider = 2;       // "aws-kms", "azure-keyvault", "gcp-kms", "vault"
  string key_id = 3;             // Key ARN/URI/ID
  bytes wrapped_key = 4;         // Optional: client-side encrypted DEK
  bytes iv = 5;                  // Initialization vector
  map<string, string> context = 6; // Encryption context
}
```

---

## 8. Processing Actions

### 8.1. Overview

```
                +─────────────────────────────────────────────+
                │           PipeStream Action Flow            │
                +─────────────────────────────────────────────+
                                     │
                                     ▼
                ┌─────────────────────────────────────────────┐
                │                  CONNECT                    │
                │    (Session + Capability Negotiation)       │
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

### 8.2. CONNECT Action

The CONNECT action establishes the session with capability negotiation.

#### 8.2.1. ALPN Identifier

```
   ALPN Protocol ID: "pipestream/1"
```

#### 8.2.2. Capability Exchange

Immediately after QUIC handshake, peers exchange Capabilities messages on Stream 0.

### 8.3. PARSE Action

The PARSE action performs vaporization with optional completion policy:

```protobuf
message CompletionPolicy {
  CompletionMode mode = 1;
  uint32 max_retries = 2;        // Default: 3
  uint32 retry_delay_ms = 3;     // Default: 1000
  uint32 timeout_ms = 4;         // Default: 300000 (5 min)
  float min_success_ratio = 5;   // For QUORUM mode
  FailureAction on_timeout = 6;
  FailureAction on_failure = 7;
}

enum CompletionMode {
  STRICT = 0;       // All children MUST complete
  LENIENT = 1;      // Continue with partial results
  BEST_EFFORT = 2;  // Complete with whatever succeeds
  QUORUM = 3;       // Need min_success_ratio
}

enum FailureAction {
  FAIL = 0;         // Propagate failure up
  SKIP = 1;         // Skip, continue with siblings
  RETRY = 2;        // Retry up to max_retries
  DEFER = 3;        // Create claim check, continue
}
```

### 8.4. PROCESS Action

| Mode | Description |
|------|-------------|
| TRANSFORM | 1:1 entity transformation |
| REJOIN | N:1 merge of siblings from vaporization |
| AGGREGATE | N:1 with reduction function |
| PASSTHROUGH | Metadata-only modification |

### 8.5. SINK Action

| Type | Description |
|------|-------------|
| INDEX | Search engine integration (Elasticsearch, Solr, etc.) |
| STORAGE | Blob storage persistence (S3, Azure, GCS) |
| NOTIFICATION | Webhook/messaging triggers |

---

## 9. Reassembly Semantics

### 9.1. Entity ID Lifecycle and Cursor

Entity IDs are managed using a cursor-based recycling scheme:

```
   Entity ID Space (20-bit circular buffer):

                       cursor (lowest unresolved)
                           │
      recyclable           │         in-flight
     <---------------      ▼      --------------->
     [...completed...]│[PENDING][PROCESSING][PENDING][...]│...free...
                      ^                                    ^
                   cursor                             last_assigned

   Window Size = (last_assigned - cursor) mod MAX_ID
   If window_size >= max_window → backpressure
```

**Rules:**
1. `new_id = (last_assigned + 1) % MAX_ENTITY_ID`
2. If `(new_id - cursor) % MAX_ID >= max_window` → STOP, apply backpressure
3. On COMPLETE/FAILED: mark resolved; if `entity_id == cursor`, advance cursor
4. IDs behind cursor are implicitly recyclable

### 9.2. Parts Ledger

Each Parts Ledger entry tracks:

```protobuf
message PartsLedgerEntry {
  uint32 parent_id = 1;
  uint32 scope_id = 2;           // Layer 1
  repeated uint32 children_ids = 3;
  repeated EntityStatus children_status = 4;
  CompletionPolicy policy = 5;   // Layer 2
  uint64 created_at = 6;
  ResolutionState state = 7;
}

enum ResolutionState {
  RESOLUTION_ACTIVE = 0;
  RESOLUTION_RESOLVED = 1;
  RESOLUTION_PARTIAL = 2;      // Some children failed/skipped
  RESOLUTION_FAILED = 3;
}
```

### 9.3. Checkpoint Blocking

A checkpoint is satisfied when:

1. All entities with IDs less than checkpoint ID have reached terminal state
2. All Parts Ledger entries within scope have been resolved
3. All nested checkpoints have been satisfied

### 9.4. Scope Digest Propagation (Layer 1)

When a scope completes:

1. Compute Merkle root of all child Entity statuses
2. Send SCOPE_DIGEST frame to parent scope
3. Parent can verify subtree integrity with single hash
4. Full ledger available on request for audit

### 9.5. Eventual Consistency (Fibonacci Heap)

Implementations SHALL use a priority queue (Fibonacci heap recommended) to track which Parts Ledger entries are ready for rejoin:

| Operation | Amortized Complexity |
|-----------|---------------------|
| Insert | O(1) |
| Find-min | O(1) |
| Extract-min | O(log n) |
| Decrease-key | O(1) |

### 9.6. Stopping Point Validation (Layer 2)

When yielding or deferring, include validation:

```protobuf
message StoppingPointValidation {
  bytes state_checksum = 1;      // Hash of processing state
  uint64 bytes_processed = 2;    // Progress marker
  uint32 children_complete = 3;
  uint32 children_total = 4;
  bool is_resumable = 5;
  string checkpoint_ref = 6;
}
```

---

## 10. Security Considerations

### 10.1. Transport Security

PipeStream inherits security from QUIC [RFC 9000] and TLS 1.3 [RFC 8446]. All connections MUST use TLS 1.3 or later. Implementations MUST NOT provide mechanisms to disable encryption.

### 10.2. Entity Payload Integrity

Each Entity MUST include a SHA-256 checksum. Receivers MUST verify before processing.

### 10.3. Resource Exhaustion

| Limit | Default | Description |
|-------|---------|-------------|
| Max scope depth | 8 | Prevents recursive bombs |
| Max entities per scope | 1,048,576 | Memory bounds |
| Max window size | 524,288 | Backpressure threshold |
| Checkpoint timeout | 3600s | Prevents stuck state |
| Claim check expiry | 86400s | Garbage collection |

### 10.4. Encryption Key Management

When using FileStorageReference with encryption:

1. Key IDs MUST reference keys in approved providers
2. Wrapped keys MUST use approved envelope encryption
3. Key rotation MUST be supported via key_id versioning
4. Implementations MUST NOT log key material

---

## 11. IANA Considerations

### 11.1. ALPN Identifier Registration

| Protocol | Identification Sequence | Reference |
|----------|------------------------|-----------|
| PipeStream Version 1 | "pipestream/1" | [this document] |

### 11.2. PipeStream Frame Type Registry

| Value | Frame Type Name | Layer | Reference |
|-------|-----------------|-------|-----------|
| 0x50 | LEDGER | 0 | Section 6.1 |
| 0x51 | CHECKPOINT | 0 | Section 9.3 |
| 0x52 | LEDGER_ACK | 0 | Section 6.1 |
| 0x53 | CHECKPOINT_ACK | 0 | Section 9.3 |
| 0x54 | SCOPE_DIGEST | 1 | Section 6.3 |
| 0x55 | BARRIER | 1 | Section 6.7 |
| 0x56 | SCOPE_OPEN | 1 | Section 6.2 |
| 0x57 | SCOPE_CLOSE | 1 | Section 6.2 |
| 0x60 | ENTITY | 0 | Section 6.8 |
| 0x61 | ENTITY_START | 0 | Section 6.8 |
| 0x62 | ENTITY_CONTINUATION | 0 | Section 6.8 |
| 0x63 | ENTITY_END | 0 | Section 6.8 |
| 0x70 | CLAIM_CHECK_QUERY | 2 | Section 6.6 |
| 0x71 | CLAIM_CHECK_RESPONSE | 2 | Section 6.6 |
| 0x72 | COMPLETION_POLICY | 2 | Section 8.3 |

### 11.3. PipeStream Status Code Registry

| Value | Name | Layer | Description |
|-------|------|-------|-------------|
| 0x0 | UNSPECIFIED | - | Proto3 default / heartbeat |
| 0x1 | PENDING | 0 | Entity announced |
| 0x2 | PROCESSING | 0 | In progress |
| 0x3 | COMPLETE | 0 | Success |
| 0x4 | FAILED | 0 | Failed |
| 0x5 | CHECKPOINT | 0 | Barrier |
| 0x6 | VAPORIZING | 0 | Decomposing |
| 0x7 | AGGREGATING | 0 | Rejoining |
| 0x8 | YIELDED | 2 | Paused |
| 0x9 | DEFERRED | 2 | Claim check issued |
| 0xA | RETRYING | 2 | Retry in progress |
| 0xB | SKIPPED | 2 | Skipped (lenient) |
| 0xC | ABANDONED | 2 | Timed out |
| 0xD-0xF | Reserved | - | Reserved for future use |

### 11.4. PipeStream Error Code Registry

| Value | Name | Description |
|-------|------|-------------|
| 0x00 | PIPESTREAM_NO_ERROR | Graceful shutdown |
| 0x01 | PIPESTREAM_INTERNAL_ERROR | Implementation error |
| 0x02 | PIPESTREAM_IDLE_TIMEOUT | Idle timeout |
| 0x03 | PIPESTREAM_LEDGER_RESET | Ledger must reset |
| 0x04 | PIPESTREAM_INTEGRITY_ERROR | Checksum failed |
| 0x05 | PIPESTREAM_ENTITY_INVALID | Invalid format |
| 0x06 | PIPESTREAM_ENTITY_TOO_LARGE | Size exceeded |
| 0x07 | PIPESTREAM_DEPTH_EXCEEDED | Scope depth exceeded |
| 0x08 | PIPESTREAM_WINDOW_EXCEEDED | Window full |
| 0x09 | PIPESTREAM_SCOPE_INVALID | Invalid scope |
| 0x0A | PIPESTREAM_CLAIM_EXPIRED | Claim check expired |
| 0x0B | PIPESTREAM_CLAIM_NOT_FOUND | Claim check not found |
| 0x0C | PIPESTREAM_LAYER_UNSUPPORTED | Protocol layer not supported |

### 11.5. URI Scheme Registration

```
pipestream-URI = "pipestream://" authority "/" session-id ["/" scope-path] ["/" entity-id]

scope-path = scope-id *("." scope-id)
```

Examples:
- `pipestream://processor.example.com/a1b2c3d4`
- `pipestream://processor.example.com:8443/a1b2c3d4/1.42/e5f6`

---

## Appendix A: Protobuf Schema Reference

### A.1. Protocol-Level Messages

```protobuf
syntax = "proto3";
package pipestream.protocol.v1;

import "google/protobuf/any.proto";

// Capabilities describes the feature set supported by a PipeStream endpoint.
// Exchanged during the CONNECT handshake to negotiate protocol layers and limits.
message Capabilities {
  bool layer0_core = 1;            // Layer 0 (Core) support. MUST be true.
  bool layer1_recursive = 2;       // Layer 1 (Recursive) scoping support.
  bool layer2_resilience = 3;      // Layer 2 (Resilience) yield/claim support.
  uint32 max_scope_depth = 4;      // Max nested scope depth. Default 8.
  uint32 max_entities_per_scope = 5; // Max entities per scope. Default 1,048,576.
  uint32 max_window_size = 6;      // Max active entity IDs in cursor window.
}

// EntityHeader is sent at the beginning of each entity stream.
message EntityHeader {
  uint32 entity_id = 1;            // Scope-local ID (20-bit, 1 to 1,048,573).
  uint32 parent_id = 2;            // Parent entity ID, or 0 for root entities.
  uint32 scope_id = 3;             // Scope ID for Layer 1. 0 if Layer 1 not used.
  uint32 layer = 4;                // Data layer (0=Blob, 1=Semantic, 2=Parsed, 3=Custom).
  string content_type = 5;         // MIME type of the entity payload.
  uint64 payload_length = 6;       // Total payload length in bytes.
  bytes checksum = 7;              // SHA-256 checksum of the complete payload.
  map<string, string> metadata = 8; // Application-defined key-value metadata.
  ChunkInfo chunk_info = 9;        // Chunking info (omitted for single-frame entities).
  CompletionPolicy completion_policy = 10; // Layer 2 failure handling policy.
}

// ChunkInfo describes chunking when a payload spans multiple frames.
message ChunkInfo {
  uint32 total_chunks = 1;         // Total chunks in the entity payload.
  uint32 chunk_index = 2;          // Zero-based index of this chunk.
  uint64 chunk_offset = 3;         // Byte offset within the complete payload.
}

// CompletionPolicy governs Layer 2 partial failure handling during vaporize/rejoin.
message CompletionPolicy {
  CompletionMode mode = 1;         // Success criteria for child completion.
  uint32 max_retries = 2;          // Max retry attempts. Default 3.
  uint32 retry_delay_ms = 3;       // Delay between retries in ms. Default 1000.
  uint32 timeout_ms = 4;           // Max wait time in ms. Default 300000.
  float min_success_ratio = 5;     // Min success ratio for QUORUM mode (0.0-1.0).
  FailureAction on_timeout = 6;    // Action when timeout expires.
  FailureAction on_failure = 7;    // Action when a child fails.
}

// CompletionMode defines success criteria for vaporize/rejoin operations.
enum CompletionMode {
  COMPLETION_MODE_UNSPECIFIED = 0;  // Unspecified; treat as STRICT.
  COMPLETION_MODE_STRICT = 1;       // ALL children must succeed.
  COMPLETION_MODE_LENIENT = 2;      // Continue despite failures.
  COMPLETION_MODE_BEST_EFFORT = 3;  // No completeness guarantees.
  COMPLETION_MODE_QUORUM = 4;       // min_success_ratio of children must succeed.
}

// FailureAction defines what to do when a child entity fails or times out.
enum FailureAction {
  FAILURE_ACTION_UNSPECIFIED = 0;   // Unspecified; treat as FAIL.
  FAILURE_ACTION_FAIL = 1;          // Propagate failure to parent immediately.
  FAILURE_ACTION_SKIP = 2;          // Skip failed child, continue siblings.
  FAILURE_ACTION_RETRY = 3;         // Retry up to max_retries times.
  FAILURE_ACTION_DEFER = 4;         // Defer via claim check for later processing.
}

// EntityStatus represents entity lifecycle state on the ledger stream.
// Encoded as 4-bit values (0x0-0xC) in the 32-bit ledger frame wire format.
enum EntityStatus {
  ENTITY_STATUS_UNSPECIFIED = 0;    // Proto3 default. Must not appear in ledger frames.
  ENTITY_STATUS_PENDING = 1;        // Registered, processing not started.
  ENTITY_STATUS_PROCESSING = 2;     // Currently being processed.
  ENTITY_STATUS_COMPLETE = 3;       // Processing completed successfully.
  ENTITY_STATUS_FAILED = 4;         // Processing failed with error.
  ENTITY_STATUS_CHECKPOINT = 5;     // Reached a synchronization barrier.
  ENTITY_STATUS_VAPORIZING = 6;     // Being decomposed into children.
  ENTITY_STATUS_AGGREGATING = 7;    // Children being reassembled into parent.
  ENTITY_STATUS_YIELDED = 8;        // Paused via yield token (Layer 2).
  ENTITY_STATUS_DEFERRED = 9;       // Deferred via claim check (Layer 2).
  ENTITY_STATUS_RETRYING = 10;      // Being retried after failure (Layer 2).
  ENTITY_STATUS_SKIPPED = 11;       // Skipped via SKIP failure action (Layer 2).
  ENTITY_STATUS_ABANDONED = 12;     // Abandoned after exhausting retries (Layer 2).
}

// LedgerFrame is sent on the ledger stream (QUIC Stream 0) to report
// entity status transitions.
message LedgerFrame {
  uint32 entity_id = 1;            // Entity whose status is being reported.
  uint32 scope_id = 2;             // Scope of the entity. 0 if Layer 1 not used.
  EntityStatus status = 3;         // Current lifecycle status.
  google.protobuf.Any extended_data = 4; // Extension data when E flag is set.
}

// CheckpointFrame creates a synchronization barrier on the ledger stream.
message CheckpointFrame {
  string checkpoint_id = 1;        // Unique checkpoint ID within the session.
  uint64 sequence_number = 2;      // Monotonically increasing sequence number.
  uint32 flags = 3;                // Bitfield: bit 0=MANDATORY, bit 1=SCOPE_LOCAL.
  uint32 timeout_ms = 4;           // Max wait time. Default 30000.
}

// PartsLedgerEntry tracks parent-child relationships from vaporization.
message PartsLedgerEntry {
  uint32 parent_id = 1;            // Entity ID of vaporized parent.
  uint32 scope_id = 2;             // Scope of the vaporization.
  repeated uint32 children_ids = 3; // Child entity IDs, in order.
  repeated EntityStatus children_status = 4; // Status of each child (parallel array).
  CompletionPolicy policy = 5;     // Policy governing child completion.
  uint64 created_at = 6;           // Unix timestamp (microseconds) of creation.
}

// YieldToken allows a Layer 2 processor to pause and resume entity processing.
message YieldToken {
  YieldReason reason = 1;          // Why processing is being paused.
  bytes continuation_state = 2;    // Opaque state for resumption.
  StoppingPointValidation validation = 3; // Consistency validation data.
}

// YieldReason describes why processing is being paused.
enum YieldReason {
  YIELD_REASON_UNSPECIFIED = 0;         // Unspecified yield reason.
  YIELD_REASON_EXTERNAL_CALL = 1;       // Waiting for external service call.
  YIELD_REASON_RATE_LIMITED = 2;        // Rate limited by downstream.
  YIELD_REASON_AWAITING_SIBLING = 3;    // Waiting for a sibling entity.
  YIELD_REASON_AWAITING_APPROVAL = 4;   // Waiting for human/system approval.
  YIELD_REASON_RESOURCE_BUSY = 5;       // Required resource unavailable.
}

// ClaimCheck is a Layer 2 deferred-processing receipt for async reprocessing.
message ClaimCheck {
  uint64 claim_id = 1;             // Unique claim check ID.
  uint32 entity_id = 2;            // Deferred entity ID.
  uint32 scope_id = 3;             // Scope of the deferred entity.
  uint64 expiry_timestamp = 4;     // Expiry time (Unix microseconds). Default 24h.
  StoppingPointValidation validation = 5; // State at point of deferral.
}

// StoppingPointValidation captures processing state at yield/defer for
// consistency verification upon resumption.
message StoppingPointValidation {
  bytes state_checksum = 1;        // SHA-256 of processor state at stop.
  uint64 bytes_processed = 2;      // Payload bytes consumed before stop.
  uint32 children_complete = 3;    // Children completed at stop.
  uint32 children_total = 4;       // Total children expected.
  bool is_resumable = 5;           // Whether resumption is supported.
  string checkpoint_ref = 6;       // Most recent satisfied checkpoint.
}

// ScopeDigest is a Layer 1 summary of scope completion with Merkle root.
message ScopeDigest {
  uint32 scope_id = 1;             // Scope being summarized.
  uint64 entities_processed = 2;   // Total entities processed in scope.
  uint64 entities_succeeded = 3;   // Entities completed successfully.
  uint64 entities_failed = 4;      // Entities that failed.
  uint64 entities_deferred = 5;    // Entities deferred via claim checks.
  bytes merkle_root = 6;           // Merkle root over (entity_id, status) pairs.
}
```

### A.2. Entity Data Messages

```protobuf
// === PipeDoc ===

message PipeDoc {
  string doc_id = 1;
  SearchMetadata search_metadata = 2;
  BlobBag blob_bag = 3;
  google.protobuf.Any structured_data = 4;
  map<string, ParsedMetadata> parsed_metadata = 5;
  SemanticProcessingResult semantic_result = 6;
  optional OwnershipContext ownership = 7;
  optional DocIdDerivation doc_id_derivation = 8;
}

// === Layer 0: BlobBag ===

message BlobBag {
  oneof blob_data {
    Blob blob = 1;
    Blobs blobs = 2;
  }
}

message Blobs {
  repeated Blob blobs = 1;
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

message FileStorageReference {
  string provider = 1;
  string bucket = 2;
  string key = 3;
  string region = 4;
  map<string, string> attrs = 5;
  EncryptionMetadata encryption = 6;
}

message EncryptionMetadata {
  string algorithm = 1;
  string key_provider = 2;
  string key_id = 3;
  bytes wrapped_key = 4;
  bytes iv = 5;
  map<string, string> context = 6;
}

enum ChecksumType {
  CHECKSUM_TYPE_UNSPECIFIED = 0;
  CHECKSUM_TYPE_MD5 = 1;
  CHECKSUM_TYPE_SHA1 = 2;
  CHECKSUM_TYPE_SHA256 = 3;
  CHECKSUM_TYPE_SHA512 = 4;
}

// === Layer 1: SemanticLayer ===

message SemanticProcessingResult {
  repeated SemanticChunk chunks = 1;
  string chunking_strategy = 2;
  map<string, string> processing_metadata = 3;
}

message SemanticChunk {
  string chunk_id = 1;
  int64 chunk_number = 2;
  ChunkEmbedding embedding_info = 3;
  map<string, google.protobuf.Value> metadata = 4;
  repeated NLPAnnotation annotations = 5;
}

message ChunkEmbedding {
  string text_content = 1;
  repeated float vector = 2;
  optional string model_id = 3;
  optional int32 original_char_start_offset = 4;
  optional int32 original_char_end_offset = 5;
}

message NLPAnnotation {
  string type = 1;              // "NER", "POS", "SENTIMENT", etc.
  string label = 2;             // "PERSON", "ORG", "POSITIVE", etc.
  int32 start_offset = 3;
  int32 end_offset = 4;
  float confidence = 5;
  map<string, string> attributes = 6;
}

// === Layer 2: ParsedData ===

message ParsedMetadata {
  string parser_id = 1;
  map<string, google.protobuf.Value> fields = 2;
  repeated TableData tables = 3;
  string raw_output = 4;
}

message TableData {
  string table_id = 1;
  repeated string headers = 2;
  repeated TableRow rows = 3;
}

message TableRow {
  repeated string cells = 1;
}

// === Supporting Types ===

message SearchMetadata {
  string title = 1;
  repeated string keywords = 2;
  string description = 3;
  map<string, string> custom_fields = 4;
}

message OwnershipContext {
  string tenant_id = 1;
  string owner_id = 2;
  repeated string acl = 3;
}

message DocIdDerivation {
  string strategy = 1;
  string source_field = 2;
  string hash_algorithm = 3;
}
```

---

## Appendix B: Protocol Layer Capability Matrix

| Feature | Layer 0 | Layer 1 | Layer 2 |
|---------|---------|---------|---------|
| Basic ledger frame (32-bit) | ✓ | ✓ | ✓ |
| Entity streaming | ✓ | ✓ | ✓ |
| PENDING/PROCESSING/COMPLETE/FAILED | ✓ | ✓ | ✓ |
| Checkpoint blocking | ✓ | ✓ | ✓ |
| Parts Ledger | ✓ | ✓ | ✓ |
| Cursor-based ID recycling | ✓ | ✓ | ✓ |
| Scoped ledger frame | | ✓ | ✓ |
| Hierarchical scopes | | ✓ | ✓ |
| Scope digest (Merkle) | | ✓ | ✓ |
| Barrier (subtree sync) | | ✓ | ✓ |
| YIELDED status | | | ✓ |
| DEFERRED status | | | ✓ |
| Claim checks | | | ✓ |
| Completion policies | | | ✓ |
| SKIPPED/ABANDONED statuses | | | ✓ |

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
- [FIPS 180-4] National Institute of Standards and Technology, "Secure Hash Standard (SHS)", FIPS PUB 180-4, August 2015.

---

## Authors' Addresses

Kevin Rickert
Email: [To be completed]

---

*PipeStream draft-01: Added protocol layering (Core/Recursive/Resilience), 32-bit aligned ledger frames, cursor-based ID recycling, hierarchical scopes with digest propagation, yield/resume with claim checks, completion policies, and cloud-agnostic storage references.*
