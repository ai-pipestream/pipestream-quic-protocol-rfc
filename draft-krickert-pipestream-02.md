---
title: "PipeStream: A Recursive Entity Streaming Protocol for Distributed Processing over QUIC"
abbrev: "PipeStream"
docname: draft-krickert-pipestream-02
category: std
submissiontype: IETF
number:
date: 2026-02-24
consensus: true
v: 3
area: "Applications and Real-Time"
workgroup: "Individual Submission"
keyword:
 - quic
 - streaming
 - recursive
 - document-processing
 - consistency
venue:
  group: Individual
  mail: kristian.rickert@pipestream.ai
  github: ai-pipestream/pipestream-quic-protocol-rfc

author:
 -
    fullname: Kristian Rickert
    organization: PipeStream AI
    email: kristian.rickert@pipestream.ai

informative:
  FIPS-180-4:
    title: "Secure Hash Standard (SHS)"
    author:
      org: National Institute of Standards and Technology
    date: 2015-08
    seriesinfo:
      FIPS: PUB 180-4

--- abstract

This document specifies PipeStream, a recursive entity streaming protocol designed for distributed document processing over QUIC transport. PipeStream enables the decomposition ("dehydration") of documents into constituent entities, their transmission across distributed processing nodes, and subsequent rehydration at destination endpoints.

The protocol employs a dual-stream architecture consisting of a data stream for entity payload transmission and a control stream for tracking entity completion status and maintaining consistency. PipeStream defines four hierarchical data layers for entity representation: BlobBag for raw binary data, SemanticLayer for annotated content with metadata, ParsedData for structured extracted information, and CustomEntity for application-specific extensions.

PipeStream is organized into three protocol layers: Layer 0 (Core) provides basic streaming with dehydrate/rehydrate semantics; Layer 1 (Recursive) adds hierarchical scoping and digest propagation; Layer 2 (Resilience) adds yield/resume, claim checks, and completion policies. Implementations MUST support Layer 0 and MAY support Layers 1 and 2.

To ensure consistency across distributed processing pipelines, PipeStream implements checkpoint blocking, whereby processing nodes MUST synchronize at defined points before proceeding. This mechanism guarantees that all constituent parts of a dehydrated document are successfully processed before rehydration operations commence.

--- middle

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

Current approaches based on batch processing and store-and-forward architectures are inefficient for large documents and fail to exploit the inherent parallelism available in distributed processing environments. Furthermore, existing streaming protocols do not provide the consistency semantics required for document processing where the integrity of the rehydrated output depends on the successful processing of all constituent parts.

### 1.2. PipeStream Overview

PipeStream addresses these challenges by defining a streaming protocol that enables incremental processing with strong consistency guarantees. The protocol is built upon QUIC {{RFC9000}} transport, leveraging its native support for multiplexed streams, low-latency connection establishment, and reliable delivery semantics.

The fundamental innovation of PipeStream is its treatment of documents as recursive compositions of entities. A document MAY be decomposed into multiple entities, each of which MAY itself be further decomposed, creating a tree structure of processing tasks. This recursive decomposition enables fine-grained parallelism while the protocol's control stream mechanism ensures that all branches of the decomposition tree are tracked and synchronized.

PipeStream employs a dual-stream design:

1. **Data Stream**: Carries entity payloads through the processing pipeline. Entities flow through this stream with minimal buffering, enabling low-latency incremental processing.

2. **Control Stream**: Carries control information tracking the status of entity decomposition and rehydration. The control stream ensures that all parts of a dehydrated document are accounted for before rehydration proceeds.

### 1.3. Design Philosophy

PipeStream implements a recursive scatter-gather pattern {{?scatter-gather=DOI.10.1007/978-1-4612-1260-6}} over QUIC streams. A document is "dehydrated" (scattered) at the source into constituent entities, these entities are transmitted and processed in parallel across distributed pipeline stages, and finally the entities are "rehydrated" (gathered) at the destination to reconstitute the complete processed document. The checkpoint blocking mechanism (Section 9.3) provides barrier synchronization semantics analogous to the barrier pattern in parallel computing.

This approach provides several advantages:

- **Incremental Processing**: Processing nodes MAY begin work on early entities before the complete document has been transmitted.

- **Parallelism**: Independent entities MAY be processed concurrently across multiple worker nodes.

- **Memory Efficiency**: No single node is required to hold the complete document in memory.

- **Fault Isolation**: Failures in processing individual entities can be detected, reported, and potentially retried without affecting other entities.

- **Consistency**: The checkpoint blocking mechanism ensures that rehydration operations proceed only when all constituent parts have been successfully processed.

### 1.4. Protocol Layering

PipeStream is organized into three protocol layers to accommodate varying deployment requirements:

| Protocol Layer | Name | Description |
|----------------|------|-------------|
| Layer 0 | Core | Basic streaming, dehydrate/rehydrate, checkpoint |
| Layer 1 | Recursive | Hierarchical scopes, digest propagation, barriers |
| Layer 2 | Resilience | Yield/resume, claim checks, completion policies |

Implementations MUST support Layer 0. Support for Layers 1 and 2 is OPTIONAL and negotiated during connection establishment.

### 1.5. Scope

This document specifies the PipeStream protocol including message formats, state machines, error handling, and the interaction between data and control streams. The document defines the four standard data layers but does not mandate specific processing semantics, which are left to application-layer specifications.

---

## 2. Terminology

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "NOT RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in BCP 14 {{RFC2119}} {{RFC8174}} when, and only when, they appear in all capitals, as shown here.

### 2.1. Protocol Entities

**Entity**
:   The fundamental unit of data flowing through a PipeStream pipeline. An Entity represents either a complete document or a constituent part of a decomposed document. Each Entity possesses a unique identifier within its processing scope and carries payload data in one of the four defined Layer formats. Entities are immutable once created; transformations produce new Entities rather than modifying existing ones.

**Document**
:   A logical unit of content submitted to a PipeStream pipeline for processing. A Document enters the pipeline as a single root Entity and MAY be decomposed into multiple Entities during processing. The Document is considered complete when its root Entity (or the rehydrated result of its decomposition) exits the pipeline.

**Scope**
:   A hierarchical namespace for Entity IDs. Each scope maintains its own Entity ID space, cursor, and Assembly Manifest. Scopes enable collections to contain documents, documents to contain parts, and parts to contain jobs, each with independent ID management. (Protocol Layer 1)

### 2.2. Dehydration and Rehydration

**Scatter-Gather**
:   The distributed processing pattern implemented by PipeStream. A single input is "scattered" (dehydrated) into multiple parts for parallel processing, and the results are "gathered" (rehydrated) back into a single output. PipeStream extends classical scatter-gather with recursive nesting: any scattered part may itself be scattered further.

**Dehydrate (Scatter)**
:   The operation of decomposing a document or Entity into multiple constituent Entities for parallel or distributed processing. When an Entity is dehydrated, the originating node MUST create an Assembly Manifest entry recording the identifiers of all resulting sub-entities. The dehydration operation is recursive; a sub-entity produced by dehydration MAY itself be dehydrated, creating a tree of decomposition. Dehydration transitions data from a solid state (a single stored record) to a fluid state (multiple in-flight entities).

**Rehydrate (Gather)**
:   The operation of reassembling multiple Entities back into a single composite Entity or Document. A rehydrate operation MUST NOT proceed until all constituent Entities listed in the corresponding Assembly Manifest entry have been received and processed (or handled according to the Completion Policy). Rehydration transitions data from a fluid state back to a solid state.

**Solid State**
:   A document or Entity that exists as a complete, stored record — either at rest in storage or as a single root Entity entering or exiting a pipeline. Contrast with "fluid state".

**Fluid State**
:   A document that has been decomposed into multiple in-flight Entities being processed in parallel across distributed nodes. A document is in the fluid state between dehydration and rehydration. Contrast with "solid state".

### 2.3. Consistency Mechanisms

**Checkpoint**
:   A synchronization point in the processing pipeline where all in-flight Entities MUST reach a consistent state before processing may continue. A checkpoint is considered "satisfied" when all Assembly Manifest entries created before the checkpoint have been resolved.

**Barrier**
:   A synchronization point scoped to a specific subtree. Unlike checkpoints which are global, barriers block only entities dependent on a specific parent's descendants. (Protocol Layer 1)

**Control Stream**
:   The control stream that tracks Entity completion status throughout the processing pipeline. The Control Stream is transmitted on a dedicated QUIC stream parallel to the data streams.

**Assembly Manifest**
:   A data structure within the Control Stream that tracks the relationship between a composite Entity and its constituent sub-entities produced by dehydration.

**Cursor**
:   A pointer to the lowest unresolved Entity ID within a scope. Entity IDs behind the cursor are considered resolved and MAY be recycled. The cursor enables efficient ID space management without global coordination.

### 2.4. Resilience Mechanisms (Protocol Layer 2)

**Yield**
:   A temporary pause in Entity processing, typically due to external dependencies (API calls, rate limiting, human approval). A yielded Entity carries a continuation token enabling resumption without reprocessing.

**Claim Check**
:   A detached reference to a deferred Entity that can be queried or resumed independently, potentially in a different session. Claim checks enable asynchronous processing patterns and retry queues.

**Completion Policy**
:   A configuration specifying how to handle partial failures during dehydration. Policies include STRICT (all must succeed), LENIENT (continue with partial results), BEST_EFFORT (complete with whatever succeeds), and QUORUM (require minimum success ratio).

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

- Status frame (32-bit, word-aligned)
- Entity frame (header + payload)
- Status codes: PENDING, PROCESSING, COMPLETE, FAILED, CHECKPOINT
- Assembly Manifest for parent-child tracking
- Cursor-based Entity ID recycling
- Single-level dehydrate/rehydrate
- Checkpoint blocking

All implementations MUST support Layer 0.

### 3.2. Layer 1: Recursive Extension

Layer 1 adds hierarchical processing capabilities:

- Scoped Entity ID namespaces (collection → document → part → job)
- SCOPE_OPEN and SCOPE_CLOSE frames
- SCOPE_DIGEST for Merkle-based subtree completion
- BARRIER for subtree-scoped synchronization
- Nested dehydration with depth tracking

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
  uint32 max_entities_per_scope = 5;  // Default: 4,294,967,294 (2^32-2)
  uint32 max_window_size = 6;     // Default: 2,147,483,648 (2^31)
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

---

## 5. QUIC Stream Mapping

### 5.1. Control Stream (Stream 0)

The Control Stream provides the control plane for PipeStream operations.

#### 5.1.1. Stream Identification

The Control Stream MUST use QUIC Stream ID 0, which per {{RFC9000}} Section 2.1 is a client-initiated bidirectional stream.

#### 5.1.2. Stream Properties

1. The client MUST open Stream 0 before any Entity Streams.
2. Stream 0 MUST remain open for the duration of the PipeStream session.
3. Stream 0 MUST NOT carry entity payload data.
4. Implementations SHOULD assign the Control Stream higher priority than Entity Streams.

#### 5.1.3. Flow Control Considerations

The Control Stream carries small, fixed-size frames (4 octets each for basic frames). Implementations MUST ensure adequate flow control credits:

- The initial MAX_STREAM_DATA for Stream 0 SHOULD be at least 8192 octets.
- Implementations SHOULD NOT block Entity Stream transmission due to Control Stream flow control exhaustion.

#### 5.1.4. Heartbeat Mechanism

To maintain session liveness:

```
   Heartbeat Frame (8 octets):
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1|
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0|0 0 0 0|0|0|0 0 0 0 0 0 0 0 0 0|
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

   Entity ID = 0xFFFFFFFF (CONNECTION_LEVEL)
   Scope ID = 0x0000
   Status = 0x0 (UNSPECIFIED, used as heartbeat signal)
   E=0 (no extended data), C=0 (no cursor update)
   Flags = 0x000
```

When no status updates have been transmitted for KEEPALIVE_TIMEOUT (default: 30 seconds), an endpoint SHOULD send a heartbeat frame. If no data is received on Stream 0 for 3 * KEEPALIVE_TIMEOUT, the connection SHOULD be closed with PIPESTREAM_IDLE_TIMEOUT (0x02).

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

### 6.1. Status Frames (Layer 0)

#### 6.1.1. Status Frame Format (64 bits)

```
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                       Entity ID (32 bits)                     |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |        Scope ID (16 bits)       | Stat  |E|C|   Flags (10)   |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

   Entity ID (32 bits):
      Unsigned integer identifying the entity.
      Range 0x00000001-0xFFFFFFFD for regular entities.
      0x00000000: NULL_ENTITY (reserved; MUST NOT be used)
      0xFFFFFFFE: SCOPE_MARKER (Layer 1)
      0xFFFFFFFF: CONNECTION_LEVEL (heartbeat, shutdown)

   Scope ID (16 bits):
      Identifier for the scope to which this entity belongs.
      Layer 0 implementations set this to 0x0000 (root scope).
      Layer 1 uses this field for hierarchical scope tracking.

   Stat (4 bits):
      Status code (see Section 6.1.2).

   E (1 bit):
      Extended frame flag. When set, additional data follows the
      basic 8-octet frame.

   C (1 bit):
      Cursor update flag. When set, a 4-octet cursor value follows.

   Flags (10 bits):
      Bit 0: Scope is root of a new document (Layer 1)
      Bit 1: Fail-fast on first child failure (Layer 1)
      Bits 2-4: Scope depth (0-7; Layer 1, default 0)
      Bits 5-9: Reserved. MUST be zero when sent.
      Receivers MUST ignore non-zero reserved flags.
```

This unified 64-bit frame replaces both the Layer 0 basic frame and the Layer 1 scoped frame from earlier protocol versions. Layer 0 implementations set Scope ID to zero and ignore scope-related flag bits. Layer 1 implementations populate the Scope ID and depth fields to enable hierarchical scope tracking within the same frame format.

#### 6.1.2. Status Codes

| Value | Name        | Layer | Description                            |
|-------|-------------|-------|----------------------------------------|
| 0x0   | UNSPECIFIED | -     | Protobuf default / heartbeat signal      |
| 0x1   | PENDING     | 0     | Entity announced, not yet transmitting |
| 0x2   | PROCESSING  | 0     | Entity transmission in progress        |
| 0x3   | COMPLETE    | 0     | Entity successfully processed          |
| 0x4   | FAILED      | 0     | Entity processing failed               |
| 0x5   | CHECKPOINT  | 0     | Synchronization barrier                |
| 0x6   | DEHYDRATING  | 0     | Dehydrating into children              |
| 0x7   | REHYDRATING | 0     | Rehydrating children                     |
| 0x8   | YIELDED     | 2     | Paused with continuation token         |
| 0x9   | DEFERRED    | 2     | Detached with claim check              |
| 0xA   | RETRYING    | 2     | Retry in progress                      |
| 0xB   | SKIPPED     | 2     | Intentionally skipped (lenient mode)   |
| 0xC   | ABANDONED   | 2     | Timed out, cursor advanced past        |
| 0xD-0xF | Reserved  | -     | Reserved for future use                |

#### 6.1.3. Cursor Update Extension

When C=1, a 4-octet cursor update follows the status frame:

```
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                  New Cursor Value (32 bits)                   |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

The cursor indicates the lowest unresolved Entity ID. IDs below the cursor are considered resolved and MAY be recycled.

#### 6.1.4. Reserved Entity ID Values

| Value      | Name              | Purpose                            |
|------------|-------------------|------------------------------------|
| 0x00000000 | NULL_ENTITY       | Reserved; MUST NOT be used         |
| 0xFFFFFFFE | SCOPE_MARKER      | Scope operations (Layer 1)         |
| 0xFFFFFFFF | CONNECTION_LEVEL  | Connection-wide control messages   |

### 6.2. Scoped Status Frames (Layer 1)

When Protocol Layer 1 is negotiated, the unified 64-bit status frame (Section 6.1.1) carries hierarchical scope information:

- **Scope ID (16 bits)**: Identifies the scope within the session. Derived from parent path hash. Allows 65,536 concurrent scopes across all depth levels.

- **Depth (Flags bits 2-4)**: Encodes the scope nesting depth. 0=root/collection, 1=document, 2=part, etc. Maximum depth of 7 (negotiated, default: 7).

- **Scope root flag (Flags bit 0)**: Indicates that this scope is the root of a new document decomposition.

Layer 1 implementations MUST populate the Scope ID and depth fields for all status frames within hierarchical scopes. Layer 0 implementations set Scope ID to 0x0000 and depth to 0.

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

The Merkle root is computed as SHA-256 over all child status entries in Entity ID order.

### 6.4. Yield Frame (Layer 2)

When Status = YIELDED (0x8) and E=1, the yield extension follows the 8-octet status frame:

```
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                       Entity ID (32)                          |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |        Scope ID (16)            |1000 |1|C|   Flags (10)     |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   | Yield Reason  |         Token Length (20 bits)                |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                                                               |
   |                  Yield Token (variable)                       |
   |                  (up to 1,048,575 bytes)                      |
   |                                                               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

   Yield Reason (4 bits):
     0x1 = EXTERNAL_CALL     (waiting on external service)
     0x2 = RATE_LIMITED      (voluntary throttle)
     0x3 = AWAITING_SIBLING  (waiting for specific sibling)
     0x4 = AWAITING_APPROVAL (human/workflow gate)
     0x5 = RESOURCE_BUSY     (semaphore/lock)
     0x0, 0x6-0xF = Reserved
```

### 6.5. Claim Check Frame (Layer 2)

When Status = DEFERRED (0x9) and E=1, the claim check extension follows the 8-octet status frame:

```
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                       Entity ID (32)                          |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |        Scope ID (16)            |1001 |1|C|   Flags (10)     |
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
   |                  Result Entity ID (32 bits)                   |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### 6.7. Barrier Frame (Layer 1)

```
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   | Frame Type = 0x55 (BARRIER)   |B|      Barrier ID (15 bits)  |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                    Parent Entity ID (32 bits)                 |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

   B (1 bit):
      Barrier satisfied (0 = waiting, 1 = released)
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
  uint32 entity_id = 1;         // Scope-local identifier
  uint32 parent_id = 2;         // 0 for root entities
  uint32 scope_id = 3;          // Layer 1: scope identifier
  uint32 layer = 4;             // Data layer (0-3)
  string content_type = 5;      // MIME type
  uint64 payload_length = 6;
  bytes checksum = 7;           // SHA-256 (32 bytes)
  map<string, string> metadata = 8;
  ChunkInfo chunk_info = 9;
  CompletionPolicy completion_policy = 10; // Layer 2: failure handling
}
```

#### 6.8.3. Checksum Algorithm

PipeStream uses SHA-256 {{FIPS-180-4}} for payload integrity verification. The checksum MUST be exactly 32 octets.

---

## 7. Entity Model

### 7.1. Core Fields

Every PipeStream entity is represented as a PipeDoc message:

| Field | Type | Requirement | Description |
|-------|------|-------------|-------------|
| doc_id | string | REQUIRED | Unique document identifier (UUID recommended) |
| entity_id | uint32 | REQUIRED | Scope-local identifier |
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
  string provider = 1;           // Storage provider identifier
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

## 8. Protocol Operations

This section defines the protocol-level operations that PipeStream endpoints perform during a session. These operations describe the phases of a PipeStream session lifecycle, from connection establishment through entity processing to terminal consumption.

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
                │        (Dehydration: 1:N possible)         │
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

The PARSE action performs dehydration with optional completion policy:

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
  COMPLETION_MODE_UNSPECIFIED = 0;  // Default; treat as STRICT
  COMPLETION_MODE_STRICT = 1;       // All children MUST complete
  COMPLETION_MODE_LENIENT = 2;      // Continue with partial results
  COMPLETION_MODE_BEST_EFFORT = 3;  // Complete with whatever succeeds
  COMPLETION_MODE_QUORUM = 4;       // Need min_success_ratio
}

enum FailureAction {
  FAILURE_ACTION_UNSPECIFIED = 0;  // Default; treat as FAIL
  FAILURE_ACTION_FAIL = 1;         // Propagate failure up
  FAILURE_ACTION_SKIP = 2;         // Skip, continue with siblings
  FAILURE_ACTION_RETRY = 3;        // Retry up to max_retries
  FAILURE_ACTION_DEFER = 4;        // Create claim check, continue
}
```

### 8.4. PROCESS Action

| Mode | Description |
|------|-------------|
| TRANSFORM | 1:1 entity transformation |
| REHYDRATE | N:1 merge of siblings from dehydration |
| AGGREGATE | N:1 with reduction function |
| PASSTHROUGH | Metadata-only modification |

### 8.5. SINK Action

| Type | Description |
|------|-------------|
| INDEX | Search engine integration (Elasticsearch, Solr, etc.) |
| STORAGE | Blob storage persistence (Object stores, Cloud storage) |
| NOTIFICATION | Webhook/messaging triggers |

---

## 9. Rehydration Semantics

### 9.1. Entity ID Lifecycle and Cursor

Entity IDs are managed using a cursor-based recycling scheme:

```
   Entity ID Space (32-bit circular buffer):

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

### 9.2. Assembly Manifest

Each Assembly Manifest entry tracks:

```protobuf
message AssemblyManifestEntry {
  uint32 parent_id = 1;
  uint32 scope_id = 2;           // Layer 1
  repeated uint32 children_ids = 3;
  repeated EntityStatus children_status = 4;
  CompletionPolicy policy = 5;   // Layer 2
  uint64 created_at = 6;
  ResolutionState state = 7;
}

enum ResolutionState {
  RESOLUTION_STATE_UNSPECIFIED = 0;
  RESOLUTION_STATE_ACTIVE = 1;
  RESOLUTION_STATE_RESOLVED = 2;
  RESOLUTION_STATE_PARTIAL = 3;      // Some children failed/skipped
  RESOLUTION_STATE_FAILED = 4;
}
```

### 9.3. Checkpoint Blocking

A checkpoint is satisfied when:

1. All entities with IDs less than checkpoint ID have reached terminal state
2. All Assembly Manifest entries within scope have been resolved
3. All nested checkpoints have been satisfied

### 9.4. Scope Digest Propagation (Layer 1)

When a scope completes, the endpoint MUST compute a Scope Digest and propagate it to the parent scope via a SCOPE_DIGEST frame (Section 6.3).

The Merkle root in the Scope Digest is computed as follows:

1. For each entity in the scope, ordered by Entity ID (ascending), construct a leaf value by concatenating the 4-byte big-endian Entity ID with the 1-byte status code.
2. Compute SHA-256 over each leaf to produce leaf hashes.
3. Build a binary Merkle tree by repeatedly hashing pairs of sibling nodes: `SHA-256(left || right)`. If the number of nodes at any level is odd, the last node is promoted without hashing.
4. The root of this tree is the `merkle_root` value in the SCOPE_DIGEST frame.

This construction is deterministic: any two implementations processing the same set of entity statuses MUST produce the same Merkle root. The parent scope MAY use the Merkle root to verify subtree integrity with a single hash comparison. Full status history remains available on request for audit.

### 9.5. Rehydration Readiness Tracking

Implementations MUST track Assembly Manifest resolution order using a mechanism that provides O(1) insertion and amortized O(log n) minimum extraction. The tracking mechanism MUST support efficient decrease-key operations to handle out-of-order status updates.

Implementations MAY choose any data structure that satisfies these complexity requirements. See the companion document `REFERENCE_IMPLEMENTATION.md` for a recommended approach using a Fibonacci heap with pseudocode and amortized complexity analysis.

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

PipeStream inherits security from QUIC {{RFC9000}} and TLS 1.3 {{RFC8446}}. All connections MUST use TLS 1.3 or later. Implementations MUST NOT provide mechanisms to disable encryption.

### 10.2. Entity Payload Integrity

Each Entity MUST include a SHA-256 checksum. Receivers MUST verify the checksum before processing the entity payload. Entities with invalid checksums MUST be rejected with PIPESTREAM_INTEGRITY_ERROR (0x04). Implementations MUST NOT process any portion of an entity payload before checksum verification succeeds.

### 10.3. Resource Exhaustion

| Limit | Default | Description |
|-------|---------|-------------|
| Max scope depth | 8 | Prevents recursive bombs |
| Max entities per scope | 4,294,967,294 | Memory bounds |
| Max window size | 2,147,483,648 | Backpressure threshold |
| Checkpoint timeout | 3600s | Prevents stuck state |
| Claim check expiry | 86400s | Garbage collection |

Implementations MUST enforce all resource limits listed above. Exceeding any limit MUST result in the corresponding error code (see Section 11.4). Implementations SHOULD allow operators to configure stricter limits than the defaults shown here.

### 10.4. Amplification Attacks

A single dehydration operation can produce an arbitrary number of child entities from a small input, creating a potential amplification vector. To mitigate this:

1. Implementations MUST enforce the max_entities_per_scope limit negotiated during capability exchange (Section 3.4). Any dehydration that would exceed this limit MUST be rejected.

2. Implementations MUST enforce the max_scope_depth limit. A dehydration chain deeper than this limit MUST be rejected with PIPESTREAM_DEPTH_EXCEEDED (0x07).

3. Implementations SHOULD enforce a configurable ratio between input entity size and total child entity count. A recommended default is no more than 1,000 children per megabyte of parent payload.

4. The backpressure mechanism (Section 9.1) provides a natural throttle: when the in-flight window fills, no new Entity IDs can be assigned until existing entities complete and the cursor advances. Implementations MUST NOT bypass backpressure for dehydration-generated entities.

### 10.5. Privacy Considerations

PipeStream entity headers and control stream frames carry metadata that may reveal information about the documents being processed, even when payloads are encrypted at the application layer:

1. **Document structure leakage**: The number of child entities produced by dehydration, the scope depth, and the Entity ID assignment pattern may reveal the structure of the document being processed (e.g., a document that dehydrates into 50 children is likely a multi-page document). Implementations that require structural privacy SHOULD pad dehydration counts or use fixed decomposition granularity.

2. **Metadata in headers**: The `content_type`, `metadata` map, and `payload_length` fields in EntityHeader (Section 6.8.2) are transmitted in cleartext within the QUIC-encrypted stream. Implementations that require metadata confidentiality beyond transport encryption SHOULD encrypt EntityHeader fields at the application layer and use an opaque content_type such as `application/octet-stream`.

3. **Traffic analysis**: The timing and size of status frames on the Control Stream may correlate with document processing patterns. Implementations operating in privacy-sensitive environments SHOULD send status frames at fixed intervals with padding to obscure processing timing.

4. **Identifiers**: The `doc_id` field in PipeDoc (Section 7.1) and filenames in BlobBag entries are application-layer data but may be logged by intermediate processing nodes. Implementations SHOULD provide mechanisms to redact or pseudonymize identifiers at pipeline boundaries.

### 10.6. Replay and Token Reuse

#### 10.6.1. Yield Token Replay

Yield tokens (Section 6.4) contain opaque continuation state that enables resumption of paused entity processing. A replayed yield token could cause an entity to be processed multiple times or to resume from a stale state. To prevent this:

1. Implementations MUST associate each yield token with a unique session identifier and Entity ID. A yield token MUST be rejected if presented in a session other than the one that issued it, unless the token was explicitly transferred via a claim check.

2. Implementations MUST invalidate a yield token after it has been consumed for resumption. A second resumption attempt with the same token MUST be rejected.

3. The StoppingPointValidation (Section 9.6) provides integrity checking at resume time. Implementations MUST verify the `state_checksum` field before accepting a resumed entity. If the checksum does not match the current state, the resumption MUST be rejected and the entity MUST be reprocessed from the beginning.

#### 10.6.2. Claim Check Replay

Claim checks (Section 6.5) are long-lived references that can be redeemed in different sessions. To prevent misuse:

1. Each claim check carries an `expiry_timestamp`. Implementations MUST reject expired claim checks.

2. Implementations MUST track redeemed claim check IDs and reject duplicate redemptions. The tracking state MUST persist for at least the claim check expiry duration.

3. Claim check IDs MUST be generated using a cryptographically secure random number generator to prevent guessing.

### 10.7. Encryption Key Management

When using FileStorageReference with encryption:

1. Key IDs MUST reference keys in approved providers.
2. Wrapped keys MUST use approved envelope encryption.
3. Key rotation MUST be supported via key_id versioning.
4. Implementations MUST NOT log key material.
5. Implementations MUST NOT include unwrapped data encryption keys in EntityHeader metadata or Control Stream frames.

---

## 11. IANA Considerations

This document requests the creation of several new registries and one ALPN identifier registration. All registries defined in this section use the "Expert Review" policy {{RFC8126}} for new assignments. The designated expert(s) should verify that proposed values do not conflict with existing assignments, that the semantics are clearly documented, and that the proposed protocol layer is appropriate for the value.

### 11.1. ALPN Identifier Registration

| Protocol | Identification Sequence | Reference |
|----------|------------------------|-----------|
| PipeStream Version 1 | "pipestream/1" | [this document] |

### 11.2. PipeStream Frame Type Registry

IANA is requested to create the "PipeStream Frame Types" registry with the following initial entries. Values in the range 0x00-0x7F are assigned by Expert Review. Values in the range 0x80-0xFF are reserved for private use.

| Value | Frame Type Name | Layer | Reference |
|-------|-----------------|-------|-----------|
| 0x50 | STATUS | 0 | Section 6.1 |
| 0x51 | CHECKPOINT | 0 | Section 9.3 |
| 0x52 | STATUS_ACK | 0 | Section 6.1 |
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

IANA is requested to create the "PipeStream Status Codes" registry with the following initial entries. Status codes are 4-bit values (0x0-0xF). Values 0x0-0xC are defined by this document. Values 0xD-0xF are reserved for future Standards Action.

| Value | Name | Layer | Description |
|-------|------|-------|-------------|
| 0x0 | UNSPECIFIED | - | Protobuf default / heartbeat |
| 0x1 | PENDING | 0 | Entity announced |
| 0x2 | PROCESSING | 0 | In progress |
| 0x3 | COMPLETE | 0 | Success |
| 0x4 | FAILED | 0 | Failed |
| 0x5 | CHECKPOINT | 0 | Barrier |
| 0x6 | DEHYDRATING | 0 | Dehydrating into children |
| 0x7 | REHYDRATING | 0 | Rehydrating children |
| 0x8 | YIELDED | 2 | Paused |
| 0x9 | DEFERRED | 2 | Claim check issued |
| 0xA | RETRYING | 2 | Retry in progress |
| 0xB | SKIPPED | 2 | Intentionally skipped (lenient mode) |
| 0xC | ABANDONED | 2 | Timed out |
| 0xD-0xF | Reserved | - | Reserved for future use |

### 11.4. PipeStream Error Code Registry

IANA is requested to create the "PipeStream Error Codes" registry with the following initial entries. Values in the range 0x00-0x3F are assigned by Expert Review. Values in the range 0x40-0xFF are reserved for private use.

| Value | Name | Description |
|-------|------|-------------|
| 0x00 | PIPESTREAM_NO_ERROR | Graceful shutdown |
| 0x01 | PIPESTREAM_INTERNAL_ERROR | Implementation error |
| 0x02 | PIPESTREAM_IDLE_TIMEOUT | Idle timeout |
| 0x03 | PIPESTREAM_CONTROL_RESET | Control stream must reset |
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

--- back

## Appendix A: Protobuf Schema Reference

### A.1. Protocol-Level Messages

```protobuf
// Copyright 2026 PipeStream Authors
//
// PipeStream Protocol - IETF draft protocol for recursive entity streaming
// over QUIC. Defines the wire-format messages for Layers 0-2 of the
// PipeStream architecture: core streaming, recursive scoping, and resilience.
//
// Edition 2023 is used for closed enums (critical for wire-protocol safety)
// and implicit field presence (distinguishing "not set" from zero values).

edition = "2023";

package pipestream.protocol.v1;

import "google/protobuf/any.proto";

// All enums in this file are CLOSED. Unknown enum values received on the wire
// MUST be rejected.
option features.enum_type = CLOSED;

// Capabilities describes the feature set supported by a PipeStream endpoint.
message Capabilities {
  // Whether the endpoint supports Layer 0 (core entity streaming).
  bool layer0_core = 1;

  // Whether the endpoint supports Layer 1 (recursive scoping and dehydration).
  bool layer1_recursive = 2;

  // Whether the endpoint supports Layer 2 (resilience, yield, and claim-check).
  bool layer2_resilience = 3;

  // Maximum nesting depth allowed for recursive scopes.
  uint32 max_scope_depth = 4;

  // Maximum number of entities permitted within a single scope.
  uint32 max_entities_per_scope = 5;

  // Maximum flow-control window size, in number of entities.
  uint32 max_window_size = 6;
}

// EntityHeader is sent at the beginning of each entity stream.
message EntityHeader {
  // Unique identifier for this entity within the session.
  uint32 entity_id = 1;

  // Identifier of the parent entity that spawned this entity.
  uint32 parent_id = 2;

  // Identifier of the scope to which this entity belongs.
  uint32 scope_id = 3;

  // Protocol layer at which this entity was created (0, 1, or 2).
  uint32 layer = 4;

  // MIME content type of the entity payload.
  string content_type = 5;

  // Length in bytes of the complete entity payload.
  uint64 payload_length = 6;

  // Integrity checksum of the complete entity payload.
  bytes checksum = 7;

  // Arbitrary key-value metadata.
  map<string, string> metadata = 8;

  // Chunking information for this entity.
  ChunkInfo chunk_info = 9;

  // Completion policy that governs retry, timeout, and failure behavior.
  CompletionPolicy completion_policy = 10;
}

// ChunkInfo describes how a single entity payload is divided into ordered chunks.
message ChunkInfo {
  uint32 total_chunks = 1;
  uint32 chunk_index = 2;
  uint64 chunk_offset = 3;
}

// CompletionPolicy controls Layer 2 resilience behavior.
message CompletionPolicy {
  CompletionMode mode = 1;
  uint32 max_retries = 2;
  uint32 retry_delay_ms = 3;
  uint32 timeout_ms = 4;
  float min_success_ratio = 5;
  FailureAction on_timeout = 6;
  FailureAction on_failure = 7;
}

enum CompletionMode {
  COMPLETION_MODE_UNSPECIFIED = 0;
  COMPLETION_MODE_STRICT = 1;
  COMPLETION_MODE_LENIENT = 2;
  COMPLETION_MODE_BEST_EFFORT = 3;
  COMPLETION_MODE_QUORUM = 4;
}

enum FailureAction {
  FAILURE_ACTION_UNSPECIFIED = 0;
  FAILURE_ACTION_FAIL = 1;
  FAILURE_ACTION_SKIP = 2;
  FAILURE_ACTION_RETRY = 3;
  FAILURE_ACTION_DEFER = 4;
}

// EntityStatus represents the lifecycle state of an entity.
enum EntityStatus {
  ENTITY_STATUS_UNSPECIFIED = 0;
  ENTITY_STATUS_PENDING = 1;
  ENTITY_STATUS_PROCESSING = 2;
  ENTITY_STATUS_COMPLETE = 3;
  ENTITY_STATUS_FAILED = 4;
  ENTITY_STATUS_CHECKPOINT = 5;
  ENTITY_STATUS_DEHYDRATING = 6;
  ENTITY_STATUS_REHYDRATING = 7;
  ENTITY_STATUS_YIELDED = 8;
  ENTITY_STATUS_DEFERRED = 9;
  ENTITY_STATUS_RETRYING = 10;
  ENTITY_STATUS_SKIPPED = 11;
  ENTITY_STATUS_ABANDONED = 12;
}

enum ResolutionState {
  RESOLUTION_STATE_UNSPECIFIED = 0;
  RESOLUTION_STATE_ACTIVE = 1;
  RESOLUTION_STATE_RESOLVED = 2;
  RESOLUTION_STATE_PARTIAL = 3;
  RESOLUTION_STATE_FAILED = 4;
}

// StatusFrame is sent on the control stream.
message StatusFrame {
  uint32 entity_id = 1;
  uint32 scope_id = 2;
  EntityStatus status = 3;
  google.protobuf.Any extended_data = 4;
}

// CheckpointFrame defines a synchronization barrier.
message CheckpointFrame {
  string checkpoint_id = 1;
  uint64 sequence_number = 2;
  uint32 flags = 3;
  uint32 timeout_ms = 4;
}

// AssemblyManifestEntry tracks parent-child relationships.
message AssemblyManifestEntry {
  uint32 parent_id = 1;
  uint32 scope_id = 2;
  repeated uint32 children_ids = 3;
  repeated EntityStatus children_status = 4;
  CompletionPolicy policy = 5;
  uint64 created_at = 6;
  ResolutionState state = 7;
}

// YieldToken allows a Layer 2 processor to pause processing.
message YieldToken {
  YieldReason reason = 1;
  bytes continuation_state = 2;
  StoppingPointValidation validation = 3;
}

enum YieldReason {
  YIELD_REASON_UNSPECIFIED = 0;
  YIELD_REASON_EXTERNAL_CALL = 1;
  YIELD_REASON_RATE_LIMITED = 2;
  YIELD_REASON_AWAITING_SIBLING = 3;
  YIELD_REASON_AWAITING_APPROVAL = 4;
  YIELD_REASON_RESOURCE_BUSY = 5;
}

// ClaimCheck is a Layer 2 deferred-processing reference.
message ClaimCheck {
  uint64 claim_id = 1;
  uint32 entity_id = 2;
  uint32 scope_id = 3;
  uint64 expiry_timestamp = 4;
  StoppingPointValidation validation = 5;
}

// StoppingPointValidation captures a snapshot of processing progress.
message StoppingPointValidation {
  bytes state_checksum = 1;
  uint64 bytes_processed = 2;
  uint32 children_complete = 3;
  uint32 children_total = 4;
  bool is_resumable = 5;
  string checkpoint_ref = 6;
}

// ScopeDigest is a Layer 1 summary of a completed scope.
message ScopeDigest {
  uint32 scope_id = 1;
  uint64 entities_processed = 2;
  uint64 entities_succeeded = 3;
  uint64 entities_failed = 4;
  uint64 entities_deferred = 5;
  bytes merkle_root = 6;
}
```

### A.2. Entity Data Messages

```protobuf
// PipeStream Data Model
//
// Defines the core document representation for the PipeStream ingestion and
// processing pipeline. A PipeDoc carries raw binary payloads (Layer 0),
// semantic chunks with embeddings (Layer 1), and structured parsed output
// (Layer 2) through every stage of the pipeline.

edition = "2023";

package pipestream.data.v1;

import "google/protobuf/any.proto";
import "google/protobuf/struct.proto";

// All enums in this file are CLOSED to ensure that receivers reject unknown
// values, which is critical for consistent processing in distributed pipelines.
option features.enum_type = CLOSED;

// PipeDoc is the root document entity that flows through the PipeStream
// pipeline. It aggregates every data layer -- raw blobs, semantic analysis
// results, and structured parsed metadata -- under a single deterministic
// document identifier.
message PipeDoc {
  // Globally unique document identifier. When a DocIdDerivation strategy is
  // configured, this value is computed deterministically from the source
  // content so that duplicate ingestion is idempotent.
  string doc_id = 1;

  // Discovery metadata used by search and retrieval systems to index the
  // document (title, keywords, description, and custom fields).
  SearchMetadata search_metadata = 2;

  // Layer 0 payload container holding one or more raw binary blobs. Each
  // blob may be stored inline or referenced via cloud storage.
  BlobBag blob_bag = 3;

  // Arbitrary strongly-typed structured data attached to the document.
  // Encoded as google.protobuf.Any to allow pipeline stages to pass
  // domain-specific messages without altering the core schema.
  google.protobuf.Any structured_data = 4;

  // Layer 2 parsed metadata produced by one or more parser stages. The map
  // key is the parser identifier, allowing multiple parsers to contribute
  // non-overlapping metadata to the same document.
  map<string, ParsedMetadata> parsed_metadata = 5;

  // Layer 1 semantic processing results containing chunked content,
  // vector embeddings, and NLP annotations generated by semantic analysis
  // pipeline stages.
  SemanticProcessingResult semantic_result = 6;

  // Multi-tenant ownership and access-control context.
  OwnershipContext ownership = 7;

  // Strategy descriptor that explains how doc_id was derived.
  DocIdDerivation doc_id_derivation = 8;
}

// ============================================================================
// Layer 0: BlobBag -- Raw binary data storage
// ============================================================================

// BlobBag is the Layer 0 container for raw binary data. It holds either a
// single blob or a collection of blobs and supports both inline byte
// payloads and cloud storage references.
message BlobBag {
  // Selector between a single blob and a multi-blob collection. Exactly one
  // must be set.
  oneof blob_data {
    // A single binary blob payload.
    Blob blob = 1;

    // A collection of binary blob payloads.
    Blobs blobs = 2;
  }
}

// Blobs is a simple wrapper that holds a repeated list of Blob messages,
// used when a document contains more than one binary payload.
message Blobs {
  // Ordered list of binary blob payloads belonging to the parent document.
  repeated Blob blobs = 1;
}

// Blob represents a single binary payload. Content may be stored inline as
// raw bytes or externalized to cloud object storage via a
// FileStorageReference.
message Blob {
  // Unique identifier for this blob within the document.
  string blob_id = 1;

  // Logical drive or volume identifier grouping related blobs (e.g., an
  // ingest source name or storage partition).
  string drive_id = 2;

  // The binary content of the blob, provided either inline or by reference.
  oneof content {
    // Raw binary data stored inline within the protobuf message.
    bytes data = 3;

    // Cloud-agnostic pointer to the binary data stored in an external
    // object store (e.g., a cloud-based or on-premises object bucket).
    FileStorageReference storage_ref = 4;
  }

  // IANA media type of the blob content (e.g., "application/pdf",
  // "image/png").
  string mime_type = 5;

  // Original filename of the ingested content, if available.
  string filename = 6;

  // Size of the blob content in bytes. A value of zero indicates that the
  // size has not been computed yet.
  int64 size_bytes = 8;

  // Hex-encoded checksum of the blob content, used for integrity
  // verification.
  string checksum = 9;

  // Algorithm used to compute the checksum value.
  ChecksumType checksum_type = 10;
}

// FileStorageReference is a cloud-agnostic pointer to an object stored in a
// remote object store. It supports standard object storage providers using
// bucket and key semantics.
message FileStorageReference {
  // Storage provider identifier (e.g., "provider-name").
  string provider = 1;

  // Bucket or container name in the target object store.
  string bucket = 2;

  // Object key (path) within the bucket that identifies the stored object.
  string key = 3;

  // Cloud region where the bucket resides (e.g., "us-east-1",
  // "westeurope"). May be empty for region-agnostic providers.
  string region = 4;

  // Provider-specific attributes such as storage class, content encoding,
  // or custom metadata headers.
  map<string, string> attrs = 5;

  // Encryption metadata describing how the stored object is encrypted at
  // rest, including the key provider and wrapped data-encryption key.
  EncryptionMetadata encryption = 6;
}

// EncryptionMetadata describes the encryption envelope applied to a stored
// object. It provides a key-management abstraction that supports AWS KMS,
// Azure Key Vault, Google Cloud KMS, HashiCorp Vault, and custom providers.
message EncryptionMetadata {
  // Encryption algorithm identifier (e.g., "AES-256-GCM", "AES-256-CBC").
  string algorithm = 1;

  // Key management provider that owns the master key (e.g., "aws-kms",
  // "azure-keyvault", "gcp-kms", "hashicorp-vault").
  string key_provider = 2;

  // Provider-specific identifier for the master encryption key used to
  // wrap the data encryption key.
  string key_id = 3;

  // Data encryption key wrapped (encrypted) by the master key. The
  // recipient must unwrap this key via the key_provider before decrypting
  // the object content.
  bytes wrapped_key = 4;

  // Initialization vector (nonce) used by the encryption algorithm.
  bytes iv = 5;

  // Additional authenticated data (AAD) or encryption context key-value
  // pairs required by the key provider for key unwrapping.
  map<string, string> context = 6;
}

// ChecksumType enumerates the supported hash algorithms for blob integrity
// verification.
enum ChecksumType {
  // Default value indicating that no checksum algorithm has been specified.
  CHECKSUM_TYPE_UNSPECIFIED = 0;

  // MD5 message-digest algorithm (128-bit hash).
  CHECKSUM_TYPE_MD5 = 1;

  // SHA-1 secure hash algorithm (160-bit hash).
  CHECKSUM_TYPE_SHA1 = 2;

  // SHA-256 secure hash algorithm (256-bit hash).
  CHECKSUM_TYPE_SHA256 = 3;

  // SHA-512 secure hash algorithm (512-bit hash).
  CHECKSUM_TYPE_SHA512 = 4;
}

// ============================================================================
// Layer 1: SemanticLayer -- Chunked content with embeddings and NLP
// ============================================================================

// SemanticProcessingResult holds the output of Layer 1 semantic analysis,
// including chunked text segments, their vector embeddings, and any NLP
// annotations produced during processing.
message SemanticProcessingResult {
  // Ordered list of semantic chunks produced by the chunking strategy.
  repeated SemanticChunk chunks = 1;

  // Identifier of the chunking strategy used to segment the source content
  // (e.g., "sliding-window-512", "sentence-boundary").
  string chunking_strategy = 2;

  // Arbitrary key-value metadata about the processing run, such as model
  // version, processing duration, or pipeline stage name.
  map<string, string> processing_metadata = 3;
}

// SemanticChunk represents a single segment of the document produced by a
// chunking strategy. Each chunk carries its text, vector embedding, and
// any NLP annotations that apply to its span.
message SemanticChunk {
  // Unique identifier for this chunk within the document.
  string chunk_id = 1;

  // Zero-based ordinal position of this chunk in the document's chunk
  // sequence.
  int64 chunk_number = 2;

  // Text content and corresponding vector embedding for this chunk.
  ChunkEmbedding embedding_info = 3;

  // Flexible metadata associated with this chunk, stored as protobuf Value
  // types to accommodate heterogeneous data (strings, numbers, booleans).
  map<string, google.protobuf.Value> metadata = 4;

  // NLP annotations (named entities, POS tags, sentiment, etc.) that fall
  // within this chunk's character span.
  repeated NLPAnnotation annotations = 5;
}

// ChunkEmbedding pairs the textual content of a chunk with its dense vector
// embedding and records the embedding model and character offsets into the
// original document.
message ChunkEmbedding {
  // Plain-text content of the chunk that was embedded.
  string text_content = 1;

  // Dense vector embedding of the text content, produced by the model
  // identified in model_id.
  repeated float vector = 2;

  // Identifier of the embedding model used to generate the vector.
  string model_id = 3;

  // Zero-based character offset in the original document where this chunk
  // begins.
  int32 original_char_start_offset = 4;

  // Zero-based character offset in the original document where this chunk
  // ends (exclusive).
  int32 original_char_end_offset = 5;
}

// NLPAnnotation captures a single natural language processing annotation
// over a character span within a chunk, such as a named entity, part-of-
// speech tag, or sentiment label.
message NLPAnnotation {
  // Annotation category (e.g., "NER", "POS", "SENTIMENT", "RELATION").
  string type = 1;

  // Annotation label within the category (e.g., "PERSON", "ORG",
  // "POSITIVE", "VERB").
  string label = 2;

  // Zero-based character offset where the annotated span begins within the
  // chunk text.
  int32 start_offset = 3;

  // Zero-based character offset where the annotated span ends (exclusive)
  // within the chunk text.
  int32 end_offset = 4;

  // Model confidence score for this annotation, in the range [0.0, 1.0].
  float confidence = 5;

  // Additional annotation-specific attributes (e.g., linked entity URI,
  // dependency relation type, or normalization form).
  map<string, string> attributes = 6;
}

// ============================================================================
// Layer 2: ParsedData -- Structured extracted data
// ============================================================================

// ParsedMetadata holds the structured output produced by a single parser
// stage. It contains extracted key-value fields, tabular data, and the
// parser's raw output for debugging or reprocessing.
message ParsedMetadata {
  // Identifier of the parser that produced this metadata (e.g.,
  // "tika-1.28", "custom-invoice-parser").
  string parser_id = 1;

  // Extracted key-value fields produced by the parser. Values are stored as
  // protobuf Value types to support strings, numbers, booleans, and nested
  // structures.
  map<string, google.protobuf.Value> fields = 2;

  // Tabular data extracted from the document (e.g., HTML tables, CSV
  // sections, spreadsheet sheets).
  repeated TableData tables = 3;

  // Raw textual output from the parser before field extraction, useful for
  // debugging or downstream reprocessing.
  string raw_output = 4;
}

// TableData represents a single extracted table with column headers and
// rows of cell values.
message TableData {
  // Unique identifier for this table within the parsed metadata.
  string table_id = 1;

  // Ordered list of column header names for the table.
  repeated string headers = 2;

  // Ordered list of data rows in the table.
  repeated TableRow rows = 3;
}

// TableRow represents a single row within an extracted table.
message TableRow {
  // Ordered list of cell values corresponding to the table's column
  // headers. Each cell value is stored as a string.
  repeated string cells = 1;
}

// ============================================================================
// Supporting Types
// ============================================================================

// SearchMetadata carries discovery metadata that search and retrieval
// systems use to index, rank, and present the document in query results.
message SearchMetadata {
  // Human-readable title of the document.
  string title = 1;

  // Keywords or tags associated with the document for search indexing.
  repeated string keywords = 2;

  // Brief textual description or abstract of the document's content.
  string description = 3;

  // Arbitrary custom fields for domain-specific search facets or filters.
  map<string, string> custom_fields = 4;
}

// OwnershipContext provides multi-tenant access control metadata for the
// document. It identifies the owning tenant, the individual owner, and an
// access control list of principals authorized to read or modify the
// document.
message OwnershipContext {
  // Identifier of the tenant that owns this document in a multi-tenant
  // deployment.
  string tenant_id = 1;

  // Identifier of the individual user or service account that owns this
  // document.
  string owner_id = 2;

  // Access control list of principal identifiers (user IDs, group IDs, or
  // role names) that are granted access to this document.
  repeated string acl = 3;
}

// DocIdDerivation describes the deterministic strategy used to generate the
// document's doc_id from its content or metadata. This allows the pipeline
// to detect and deduplicate identical documents across ingestion runs.
message DocIdDerivation {
  // Name of the derivation strategy (e.g., "content-hash",
  // "field-composite", "external-id").
  string strategy = 1;

  // Dot-delimited path to the source field whose value is hashed or used
  // as the document identifier (e.g., "blob_bag.blob.checksum",
  // "search_metadata.title").
  string source_field = 2;

  // Hash algorithm applied to the source field value to produce the
  // doc_id (e.g., "SHA-256", "MD5"). Empty when the strategy does not
  // involve hashing.
  string hash_algorithm = 3;
}
```

---

## Appendix B: Protocol Layer Capability Matrix

| Feature | Layer 0 | Layer 1 | Layer 2 |
|---------|---------|---------|---------|
| Unified status frame (64-bit) | ✓ | ✓ | ✓ |
| Entity streaming | ✓ | ✓ | ✓ |
| PENDING/PROCESSING/COMPLETE/FAILED | ✓ | ✓ | ✓ |
| Checkpoint blocking | ✓ | ✓ | ✓ |
| Assembly Manifest | ✓ | ✓ | ✓ |
| Cursor-based ID recycling | ✓ | ✓ | ✓ |
| Scoped status fields (Scope ID, depth) | | ✓ | ✓ |
| Hierarchical scopes | | ✓ | ✓ |
| Scope digest (Merkle) | | ✓ | ✓ |
| Barrier (subtree sync) | | ✓ | ✓ |
| YIELDED status | | | ✓ |
| DEFERRED status | | | ✓ |
| Claim checks | | | ✓ |
| Completion policies | | | ✓ |
| SKIPPED/ABANDONED statuses | | | ✓ |
