---
title: "PipeStream: A Recursive Entity Streaming Protocol for Distributed Processing over QUIC"
abbrev: "PipeStream"
docname: draft-krickert-pipestream-00
category: std
submissiontype: IETF
number:
date: 2026-03-01
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

normative:
  RFC2119:
  RFC8174:
  RFC9000:
  RFC8446:
  RFC8126:

informative:
  FIPS-180-4:
    title: "Secure Hash Standard (SHS)"
    author:
      org: National Institute of Standards and Technology
    date: 2015-08
    seriesinfo:
      FIPS: PUB 180-4
  scatter-gather:
    title: "The Scatter-Gather Design Pattern"
    author:
      - ins: "D. Lea"
        name: "Doug Lea"
    date: 1996
    seriesinfo:
      DOI: 10.1007/978-1-4612-1260-6

--- abstract
This document specifies PipeStream, a recursive entity streaming protocol designed for distributed document processing over QUIC transport. PipeStream enables the decomposition ("dehydration") of documents into constituent entities, their transmission across distributed processing nodes, and subsequent rehydration at destination endpoints.

The protocol employs a dual-stream architecture consisting of a data stream for entity payload transmission and a control stream for tracking entity completion status and maintaining consistency. PipeStream defines four hierarchical data layers for entity representation: BlobBag for raw binary data, SemanticLayer for annotated content with metadata, ParsedData for structured extracted information, and CustomEntity for application-specific extensions.

PipeStream is organized into three protocol layers: Layer 0 (Core) provides basic streaming with dehydrate/rehydrate semantics; Layer 1 (Recursive) adds hierarchical scoping and digest propagation; Layer 2 (Resilience) adds yield/resume, claim checks, and completion policies. Implementations MUST support Layer 0 and MAY support Layers 1 and 2.

To ensure consistency across distributed processing pipelines, PipeStream implements checkpoint blocking, whereby processing nodes MUST synchronize at defined points before proceeding. This mechanism guarantees that all constituent parts of a dehydrated document are successfully processed before rehydration operations commence.
# Introduction

## Problem Statement

Distributed document processing pipelines face significant challenges when handling large, complex documents that require multiple stages of transformation, analysis, and enrichment. Traditional batch processing approaches require entire documents to be loaded into memory, processed sequentially, and transmitted in their entirety between processing stages. This methodology introduces substantial latency, excessive memory consumption, and poor utilization of distributed computing resources.

Modern document processing workflows increasingly demand the ability to:

- Process documents incrementally as data becomes available
- Distribute processing load across heterogeneous worker nodes
- Maintain consistency guarantees across parallel processing paths
- Handle documents of arbitrary size without memory constraints
- Support recursive decomposition where document parts may themselves be decomposed
- Scale from single documents to collections of millions of documents

Current approaches based on batch processing and store-and-forward architectures are inefficient for large documents and fail to exploit the inherent parallelism available in distributed processing environments. Furthermore, existing streaming protocols do not provide the consistency semantics required for document processing where the integrity of the rehydrated output depends on the successful processing of all constituent parts.

## PipeStream Overview

PipeStream addresses these challenges by defining a streaming protocol that enables incremental processing with strong consistency guarantees. The protocol is built upon QUIC {{RFC9000}} transport, leveraging its native support for multiplexed streams, low-latency connection establishment, and reliable delivery semantics.

The fundamental innovation of PipeStream is its treatment of documents as recursive compositions of entities. A document MAY be decomposed into multiple entities, each of which MAY itself be further decomposed, creating a tree structure of processing tasks. This recursive decomposition enables fine-grained parallelism while the protocol's control stream mechanism ensures that all branches of the decomposition tree are tracked and synchronized.

PipeStream employs a dual-stream design:

1. **Data Stream**: Carries entity payloads through the processing pipeline. Entities flow through this stream with minimal buffering, enabling low-latency incremental processing.

2. **Control Stream**: Carries control information tracking the status of entity decomposition and rehydration. The control stream ensures that all parts of a dehydrated document are accounted for before rehydration proceeds.

## Design Philosophy

PipeStream implements a recursive scatter-gather pattern {{?scatter-gather}} over QUIC streams. A document is "dehydrated" (scattered) at the source into constituent entities, these entities are transmitted and processed in parallel across distributed pipeline stages, and finally the entities are "rehydrated" (gathered) at the destination to reconstitute the complete processed document. The checkpoint blocking mechanism (Section 9.3) provides barrier synchronization semantics analogous to the barrier pattern in parallel computing.

This approach provides several advantages:

- **Incremental Processing**: Processing nodes MAY begin work on early entities before the complete document has been transmitted.

- **Parallelism**: Independent entities MAY be processed concurrently across multiple worker nodes.

- **Memory Efficiency**: No single node is required to hold the complete document in memory.

- **Fault Isolation**: Failures in processing individual entities can be detected, reported, and potentially retried without affecting other entities.

- **Consistency**: The checkpoint blocking mechanism ensures that rehydration operations proceed only when all constituent parts have been successfully processed.

## Protocol Layering

PipeStream is organized into three protocol layers to accommodate varying deployment requirements:

| Protocol Layer | Name | Description |
|----------------|------|-------------|
| Layer 0 | Core | Basic streaming, dehydrate/rehydrate, checkpoint |
| Layer 1 | Recursive | Hierarchical scopes, digest propagation, barriers |
| Layer 2 | Resilience | Yield/resume, claim checks, completion policies |

Implementations MUST support Layer 0. Support for Layers 1 and 2 is OPTIONAL and negotiated during connection establishment.

## Scope

This document specifies the PipeStream protocol including message formats, state machines, error handling, and the interaction between data and control streams. The document defines the four standard data layers but does not mandate specific processing semantics, which are left to application-layer specifications.
# Terminology

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "NOT RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in BCP 14 {{RFC2119}} {{RFC8174}} when, and only when, they appear in all capitals, as shown here.

## Protocol Entities

**Entity**
:   The fundamental unit of data flowing through a PipeStream pipeline. An Entity represents either a complete document or a constituent part of a decomposed document. Each Entity possesses a unique identifier within its processing scope and carries payload data in one of the four defined Layer formats. Entities are immutable once created; transformations produce new Entities rather than modifying existing ones.

**Document**
:   A logical unit of content submitted to a PipeStream pipeline for processing. A Document enters the pipeline as a single root Entity and MAY be decomposed into multiple Entities during processing. The Document is considered complete when its root Entity (or the rehydrated result of its decomposition) exits the pipeline.

**Scope**
:   A hierarchical namespace for Entity IDs. Each scope maintains its own Entity ID space, cursor, and Assembly Manifest. Scopes enable collections to contain documents, documents to contain parts, and parts to contain jobs, each with independent ID management. (Protocol Layer 1)

## Dehydration and Rehydration

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

## Consistency Mechanisms

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

## Resilience Mechanisms (Protocol Layer 2)

**Yield**
:   A temporary pause in Entity processing, typically due to external dependencies (API calls, rate limiting, human approval). A yielded Entity carries a continuation token enabling resumption without reprocessing.

**Claim Check**
:   A detached reference to a deferred Entity that can be queried or resumed independently, potentially in a different session. Claim checks enable asynchronous processing patterns and retry queues.

**Completion Policy**
:   A configuration specifying how to handle partial failures during dehydration. Policies include STRICT (all must succeed), LENIENT (continue with partial results), BEST_EFFORT (complete with whatever succeeds), and QUORUM (require minimum success ratio).

## Data Representation

**Data Layer**
:   One of four defined representations for Entity payload data:

    1. **BlobBag**: Raw binary data with minimal metadata
    2. **SemanticLayer**: Annotated content with structural and semantic metadata
    3. **ParsedData**: Structured information extracted from document content
    4. **CustomEntity**: Application-specific extension Layer

## Additional Terms

**Pipeline**
:   A configured sequence of processing stages through which Entities flow.

**Processor**
:   A node in the mesh that performs operations on entities (e.g., transformation, dehydration, or rehydration).

**Sink**
:   A terminal stage in a pipeline where rehydrated documents are persisted or delivered to an external system.

**Stage**
:   A single processing step within a Pipeline.

**Scope Digest**
:   A cryptographic summary (Merkle root) of all Entity statuses within a completed scope, propagated to parent scopes for efficient verification. (Protocol Layer 1)
# Protocol Layers

PipeStream defines three protocol layers that build upon each other. This layered approach allows simple deployments to use only the core protocol while complex deployments can leverage advanced features.

## Layer 0: Core Protocol

Layer 0 provides the fundamental streaming capabilities:

- Unified Control Frame (UCF) header (1-octet type)
- Status frame (8-octet bit-packed frame)
- Entity frame (header + payload)
- Status codes: PENDING, PROCESSING, COMPLETE, FAILED, CHECKPOINT
- Assembly Manifest for parent-child tracking
- Cursor-based Entity ID recycling
- Single-level dehydrate/rehydrate
- Checkpoint blocking

All implementations MUST support Layer 0.

## Layer 1: Recursive Extension

Layer 1 adds hierarchical processing capabilities:

- Scoped Entity ID namespaces (collection → document → part → job)
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
- Claim check query/response frames
- Stopping point validation

Layer 2 is OPTIONAL and requires Layer 1. Implementations advertise Layer 2 support during capability negotiation.

## Capability Negotiation

During CONNECT, endpoints exchange supported capabilities:

```protobuf
message Capabilities {
  bool layer0_core = 1;           // Always true
  bool layer1_recursive = 2;      // Scoped IDs, digests
  bool layer2_resilience = 3;     // Yield, claim checks
  uint32 max_scope_depth = 4;     // Default: 7 (8 levels, 0-7)
  uint32 max_entities_per_scope = 5;  // Default: 4,294,967,294 (2^32-2)
  uint32 max_window_size = 6;     // Default: 2,147,483,648 (2^31)
}
```

Peers negotiate down to common capabilities. If Layer 2 is requested but Layer 1 is not supported, Layer 2 MUST be disabled.
# Protocol Overview

This section provides a high-level overview of the PipeStream protocol architecture, design principles, and operational model.

## Design Goals

### True Streaming Processing

PipeStream MUST enable true streaming document processing where entities are transmitted and processed incrementally as they become available. Implementations MUST NOT buffer complete documents before initiating transmission.

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
# QUIC Stream Mapping

PipeStream leverages the native multiplexing capabilities of QUIC {{RFC9000}} to provide a clean separation between control coordination and data transmission.

## Control Stream (Stream 0)

The Control Stream provides the control plane for PipeStream operations.

### Stream Identification

The Control Stream MUST use QUIC Stream ID 0, which per RFC 9000 is a bidirectional, client-initiated stream.

### Usage Rules

1. The Control Stream MUST be opened immediately upon connection establishment.
2. Capability negotiation (Section 3.4) MUST occur on Stream 0 before any Entity Streams are opened.
3. Stream 0 MUST NOT carry entity payload data.
4. Implementations SHOULD assign the Control Stream a high priority to ensure timely delivery of status updates.

### Flow Control Considerations

The Control Stream carries small, bit-packed control frames. STATUS frames are 12 octets base. Implementations MUST ensure adequate flow control credits:

- The initial MAX_STREAM_DATA for Stream 0 SHOULD be at least 8192 octets.
- Implementations SHOULD NOT block Entity Stream transmission due to Control Stream flow control exhaustion.

### Heartbeat Mechanism

QUIC already provides native transport liveness signals (for example, PING and idle timeout handling). Implementations SHOULD rely on those transport mechanisms for connection liveness.

PipeStream heartbeat frames are OPTIONAL and are intended for application-level responsiveness checks (for example, detecting stalled processing logic even when the transport remains healthy). When used, an endpoint sends a STATUS frame with all fields set to their heartbeat values:

| Field | Value | Description |
|-------|-------|-------------|
| Type | 0x50 (STATUS) | |
| Stat | 0x0 (UNSPECIFIED) | Heartbeat signal |
| Entity ID | 0xFFFFFFFF | CONNECTION_LEVEL |
| Scope ID | 0x0000 | Root scope |
| Reserved | 0x0000 | MUST be zero |

When no status updates have been transmitted for KEEPALIVE_TIMEOUT (default: 30 seconds), an endpoint MAY send a heartbeat frame. If no data is received on Stream 0 for 3 * KEEPALIVE_TIMEOUT, the endpoint SHOULD first apply transport-native liveness policy; it MAY close the connection with PIPESTREAM_IDLE_TIMEOUT (0x02) when application-level inactivity policy requires it.

### Transport Session vs. Application Session Context

The `session-id` segment identifies application context for detached or resumable resources (for example, Layer 2 yield/claim-check flows). PipeStream Layer 0 streaming semantics do not depend on this URI scheme.

## Entity Streams (Streams 2+)

Entity Streams carry the actual document entity data.

### Unidirectional Data Flow

Entity Streams MUST be unidirectional streams:

| Stream Type | Client to Server | Server to Client |
|-------------|-------------------|----------|
| Client-Initiated | 4n + 2 (n >= 0) | 2, 6, 10, 14, ... |
| Server-Initiated | 4n + 3 (n >= 0) | 3, 7, 11, 15, ... |

### One Entity Per Stream

1. Each Entity Stream MUST carry exactly one entity.
2. The entity_id in the Entity Frame header MUST be unique within its scope.
3. Once an entity has been completely transmitted, the sender MUST close the stream.

## Transport Error Mapping

PipeStream error signaling on Stream 0 and QUIC transport signals are complementary. Endpoints SHOULD bridge them so peers receive both transport-level and protocol-level context.

1. If an Entity Stream is aborted with `RESET_STREAM` or `STOP_SENDING`, the endpoint SHOULD emit a corresponding terminal status (`FAILED`, `ABANDONED`, or policy-driven equivalent) for that entity on Stream 0.
2. If PipeStream determines a terminal entity error first (for example, checksum failure or invalid frame), the endpoint SHOULD abort the affected Entity Stream with an appropriate QUIC error and emit the corresponding PipeStream status/error context on Stream 0.
3. If Stream 0 is reset or becomes unusable, endpoints SHOULD treat this as a control-plane failure and close the connection with `PIPESTREAM_CONTROL_RESET (0x03)`.
4. On QUIC connection termination (`CONNECTION_CLOSE`), entities without a previously observed terminal status MUST be treated as failed by local policy.
# Frame Formats

This section defines the wire formats for PipeStream frames. All multi-octet integer fields are encoded in network byte order (big-endian).

## Control Stream Framing (Stream 0)

To support mixed content (bit-packed frames and Protobuf messages) on the Control Stream, PipeStream uses a Unified Control Frame (UCF) header.

### UCF Header

Every message on Stream 0 MUST begin with a 1-octet Frame Type.

| Value | Frame Class | Length Encoding | Description |
|-------|-------------|-----------------|-------------|
| 0x50-0x7F | Fixed | No length prefix | Bit-packed control frames with type-defined sizes |
| 0x80-0xFF | Variable | 4-octet Length + N | Variable-size Protobuf-encoded control messages |

For Fixed frames, the receiver determines frame size from the Frame Type value. For Variable frames, the Type is followed by a 4-octet unsigned integer (big-endian) indicating the length of the Protobuf message that follows.

Variable-frame Length (32 bits):
:   The payload length in octets, excluding the 1-octet Type and the 4-octet Length field. Receivers MUST reject lengths greater than 16,777,215 octets (16 MiB - 1) with PIPESTREAM_ENTITY_TOO_LARGE (0x06).

### Fixed Frame Sizes

The following fixed-size frame types are defined by this document:

| Type | Name | Total Size | Notes |
|------|------|------------|-------|
| 0x50 | STATUS | 12 octets (base) | 16 octets when C=1; larger when E=1 with extension data |
| 0x54 | SCOPE_DIGEST | 68 octets | Includes 32-octet Merkle root and 64-bit counters |
| 0x55 | BARRIER | 8 octets | No variable extension |

## Status Frames (Layer 0)

### Status Frame Format (0x50)

The Status Frame reports lifecycle transitions for entities.

```
    0                   1                   2                   3
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |  Type (0x50)  | Stat(4)|E|C|D|      Flags (15 bits)          |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |                       Entity ID (32 bits)                     |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |        Scope ID (16 bits)       |      Reserved (16 bits)     |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

Stat (4 bits):
:   Status code (see Section 6.2.2).

E (1 bit):
:   Extended frame flag. Additional extension data follows (Section 6.5).

C (1 bit):
:   Cursor update flag. A 4-octet cursor value follows (Section 6.2.3).

D (3 bits):
:   Explicit scope nesting depth (0-7). 0=Root. Layer 1.

Flags (15 bits):
:   Reserved for future use. MUST be zero when sent and MUST be ignored by receivers.

Entity ID (32 bits):
:   Unsigned integer identifying the entity.

Scope ID (16 bits):
:   Identifier for the scope to which this entity belongs.

Reserved (16 bits):
:   Reserved for future use. MUST be zero when sent and MUST be ignored by receivers.

### Status Codes

| Value | Name        | Layer | Description                            |
|-------|-------------|-------|----------------------------------------|
| 0x0   | UNSPECIFIED | -     | Protobuf default / heartbeat signal      |
| 0x1   | PENDING     | 0     | Entity announced, not yet transmitting |
| 0x2   | PROCESSING  | 0     | Entity transmission in progress        |
| 0x3   | COMPLETE    | 0     | Entity successfully processed          |
| 0x4   | FAILED      | 0     | Entity processing failed               |
| 0x5   | CHECKPOINT  | 0     | Synchronization barrier                |
| 0x6   | DEHYDRATING | 0     | Dehydrating into children              |
| 0x7   | REHYDRATING | 0     | Rehydrating children                   |
| 0x8   | YIELDED     | 2     | Paused with continuation token         |
| 0x9   | DEFERRED    | 2     | Detached with claim check              |
| 0xA   | RETRYING    | 2     | Retry in progress                      |
| 0xB   | SKIPPED     | 2     | Intentionally skipped                  |
| 0xC   | ABANDONED   | 2     | Timed out                              |

The base STATUS frame is 12 octets. When C=1, a 4-octet cursor value follows (total 16 octets before any E=1 extension data). When E=1, additional extension bytes follow as defined in Section 6.5.

### Cursor Update Extension

When C=1, a 4-octet cursor update follows the status frame:

```
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                  New Cursor Value (32 bits)                   |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

New Cursor Value (32 bits):
:   The numeric value of the new cursor. Entities with IDs lower than this value (modulo circular ID rules) are considered resolved and their IDs MAY be recycled.

## Scope Digest Frame (0x54)

When Protocol Layer 1 is negotiated, a scope completion is summarized:

```
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |  Type (0x54)  |  Flags (8)      |        Scope ID (16)        |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                                                               |
   |                   Entities Processed (64 bits)                |
   |                                                               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                                                               |
   |                   Entities Succeeded (64 bits)                |
   |                                                               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                                                               |
   |                    Entities Failed (64 bits)                  |
   |                                                               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                                                               |
   |                    Entities Deferred (64 bits)                |
   |                                                               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                                                               |
   |                    Merkle Root (256 bits)                     |
   |                                                               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

Flags (8 bits):
:   Reserved for future use. MUST be zero when sent and MUST be ignored by receivers.

Scope ID (16 bits):
:   Identifier of the scope being summarized.

Entities Processed (64 bits):
:   The total number of entities that were processed within the scope.

Entities Succeeded (64 bits):
:   The number of entities that reached a terminal success state.

Entities Failed (64 bits):
:   The number of entities that reached a terminal failure state.

Entities Deferred (64 bits):
:   The number of entities that were deferred via claim checks.

Merkle Root (256 bits):
:   The SHA-256 Merkle root covering all entity statuses in the scope (see Section 9.4).

The SCOPE_DIGEST frame is 68 octets total. The Scope ID MUST match the 16-bit identifier defined in Section 6.2.1.

## Barrier Frame (0x55)

```
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |  Type (0x55)  |S|  Reserved (7) |        Barrier ID (16)      |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                    Parent Entity ID (32 bits)                 |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

S (1 bit):
:   Status (0 = waiting, 1 = released).

Reserved (7 bits):
:   Reserved for future use. MUST be zero when sent and MUST be ignored by receivers.

Barrier ID (16 bits):
:   Identifier for the barrier within the scope.

Parent Entity ID (32 bits):
:   The identifier of the parent entity whose sub-tree is blocked by this barrier.

## Yield and Claim Check Extensions (Layer 2)

When E=1 in a status frame, extension data follows. The length of extension data is determined by the Status code.

If E=1 is set for a Status code that does not define an extension layout in this specification (or a negotiated extension), the receiver MUST treat the frame as malformed and fail processing with PIPESTREAM_ENTITY_INVALID (0x05).

### Yield Extension (Stat = 0x8)

```
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   | Yield Reason  |           Token Length (24 bits)              |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                                                               |
   |                  Yield Token (variable)                       |
   |                                                               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

Yield Reason (8 bits):
:   The reason for yielding (see Section 6.5.1.1).

Token Length (24 bits):
:   The length of the Yield Token in bytes (maximum 16,777,215).

Yield Token (variable):
:   The opaque continuation state.

#### Yield Reason Codes

| Value | Name | Description |
|-------|------|-------------|
| 0x1 | EXTERNAL_CALL | Waiting on external service |
| 0x2 | RATE_LIMITED | Voluntary throttle |
| 0x3 | AWAITING_SIBLING | Waiting for specific sibling |
| 0x4 | AWAITING_APPROVAL | Human/workflow gate |
| 0x5 | RESOURCE_BUSY | Semaphore/lock |
| 0x0, 0x06-0xFF | Reserved | Reserved for future use |

### Claim Check Extension (Stat = 0x9)

```
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                                                               |
   |                    Claim Check ID (64 bits)                   |
   |                                                               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                                                               |
   |                Expiry Timestamp (64 bits, Unix micros)        |
   |                                                               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

Claim Check ID (64 bits):
:   A cryptographically secure random identifier for the claim.

Expiry Timestamp (64 bits):
:   Unix epoch timestamp in microseconds when the claim expires.

## Protobuf-Encoded Messages (0x80-0xFF)

Messages in this range are preceded by a 4-octet length field.

| Type | Message Name | Reference |
|-------|--------------|-----------|
| 0x80 | Capabilities | Section 3.4 |
| 0x81 | Checkpoint | Section 9.3 |

## Entity Frames

Entity frames carry the actual document entity data on Entity Streams.

### Entity Frame Structure

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

Header Length (4 octets):
:   The length of the Protobuf-encoded EntityHeader in bytes.

Header (Protobuf):
:   The serialized EntityHeader message (see Section 6.7.2).

Payload (variable):
:   The raw entity data.

### Entity Header (Protobuf)

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

### Checksum Algorithm

PipeStream uses SHA-256 {{FIPS-180-4}} for payload integrity verification. The checksum MUST be exactly 32 octets.
# Entity Model

## Core Fields

Every PipeStream entity is represented as a PipeDoc message:

| Field | Type | Requirement | Description |
|-------|------|-------------|-------------|
| doc_id | string | REQUIRED | Unique document identifier (UUID recommended) |
| entity_id | uint32 | REQUIRED | Scope-local identifier |
| ownership | OwnershipContext | OPTIONAL | Multi-tenancy tracking |

## Four Data Layers

Each PipeDoc carries entity payload in one of four data layers:

| Layer | Name | Content |
|-------|------|---------|
| 0 | BlobBag | Raw binary data: original document bytes, images, attachments |
| 1 | SemanticLayer | Annotated content: text segments with vector embeddings, NLP annotations, NER, classifications |
| 2 | ParsedData | Structured extraction: key-value pairs, tables, structured fields |
| 3 | CustomEntity | Extension point: domain-specific protobuf via `google.protobuf.Any` |

## Cloud-Agnostic Storage Reference

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
# Protocol Operations

This section defines the protocol-level operations that PipeStream endpoints perform during a session. These operations describe the phases of a PipeStream session lifecycle, from connection establishment through entity processing to terminal consumption.

## Overview

A PipeStream session proceeds through four sequential actions:

| Phase | Action | Cardinality | Description |
|-------|--------|-------------|-------------|
| 1 | CONNECT | 1:1 | Session establishment and capability negotiation |
| 2 | PARSE | 1:N | Dehydration: decompose input into entities |
| 3 | PROCESS | 1:1 or N:1 | Transform, rehydrate, aggregate, or pass through entities (parallel) |
| 4 | SINK | N:1 | Terminal consumption: index, store, or notify |

## CONNECT Action

The CONNECT action establishes the session with capability negotiation.

### ALPN Identifier

ALPN Protocol ID: `pipestream/1`

### Capability Exchange

Immediately after QUIC handshake, peers exchange Capabilities messages on Stream 0.

## PARSE Action

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

## PROCESS Action

| Mode | Description |
|------|-------------|
| TRANSFORM | 1:1 entity transformation |
| REHYDRATE | N:1 merge of siblings from dehydration |
| AGGREGATE | N:1 with reduction function |
| PASSTHROUGH | Metadata-only modification |

## SINK Action

| Type | Description |
|------|-------------|
| INDEX | Search engine integration (Elasticsearch, Solr, etc.) |
| STORAGE | Blob storage persistence (Object stores, Cloud storage) |
| NOTIFICATION | Webhook/messaging triggers |
# Rehydration Semantics

## Entity ID Lifecycle and Cursor

Entity IDs are managed using a cursor-based circular recycling scheme within the 32-bit ID space. The ID space is divided into three logical regions relative to the current `cursor` and `last_assigned` pointers:

| Region | ID Range | Description |
|--------|----------|-------------|
| Recyclable | IDs behind `cursor` | Resolved entities; IDs may be reused |
| In-flight | `cursor` to `last_assigned` | Active entities (PENDING, PROCESSING, etc.) |
| Free | Beyond `last_assigned` | Available for new entity assignment |

The window size is computed as `(last_assigned - cursor) mod 0xFFFFFFFD`. If `window_size >= max_window`, the sender MUST apply backpressure and stop assigning new IDs until the cursor advances.

**Rules:**
1. `new_id = (last_assigned + 1) % 0xFFFFFFFD`
2. If `new_id == 0`, `new_id = 1` (skip reserved NULL_ENTITY)
3. If `(new_id - cursor) % 0xFFFFFFFD >= max_window` → STOP, apply backpressure
4. On COMPLETE/FAILED: mark resolved; if `entity_id == cursor`, advance cursor
5. IDs behind cursor are implicitly recyclable

## Assembly Manifest

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

## Checkpoint Blocking

A checkpoint is satisfied when:

1. All entities in the checkpoint scope with IDs less than `checkpoint_entity_id` (considering circular wrap) have reached terminal state.
2. All Assembly Manifest entries within the checkpoint scope have been resolved.
3. All nested checkpoints within the checkpoint scope have been satisfied.

CheckpointFrame (Section 6.6 / Appendix A) carries both:

```protobuf
message CheckpointFrame {
  string checkpoint_id = 1;
  uint64 sequence_number = 2;
  uint32 checkpoint_entity_id = 3;
  uint32 scope_id = 4;
  uint32 flags = 5;
  uint32 timeout_ms = 6;
}
```

- `checkpoint_id`: an opaque identifier for logging and correlation.
- `checkpoint_entity_id`: the numeric ordering key used for barrier evaluation.

Implementations MUST use `checkpoint_entity_id` (not `checkpoint_id`) when evaluating Condition 1.

For circular comparison in Condition 1, implementations MUST use the same modulo ordering as cursor management. Define `MAX = 0xFFFFFFFD` and:

`is_before(a, b) = ((b - a + MAX) % MAX) < (MAX / 2)`

An entity ID `a` is considered "less than checkpoint_entity_id `b`" iff `is_before(a, b)` is true.

## Scope Digest Propagation (Layer 1)

When a scope completes, the endpoint MUST compute a Scope Digest and propagate it to the parent scope via a SCOPE_DIGEST frame (Section 6.3).

The Merkle root in the Scope Digest is computed as follows:

1. For each entity in the scope, ordered by Entity ID (ascending), construct a 5-octet leaf value by concatenating:
   - The 4-octet big-endian Entity ID.
   - A 1-octet status field where the lower 4 bits contain the `Stat` code (Section 6.2.2) and the upper 4 bits are zero.
2. Compute SHA-256 over each 5-octet leaf to produce leaf hashes.
3. Build a binary Merkle tree by repeatedly hashing pairs of sibling nodes: `SHA-256(left || right)`. If the number of nodes at any level is odd, the last node is promoted to the next level without hashing.
4. The root of this tree is the `merkle_root` value in the SCOPE_DIGEST frame.

This construction is deterministic: any two implementations processing the same set of entity statuses MUST produce the same Merkle root.

## Rehydration Readiness Tracking

Implementations MUST track Assembly Manifest resolution order using a mechanism that provides O(1) insertion and amortized O(log n) minimum extraction. The tracking mechanism MUST support efficient decrease-key operations to handle out-of-order status updates.

Implementations MAY choose any data structure that satisfies these complexity requirements. See the companion document `REFERENCE_IMPLEMENTATION.md` for a recommended approach using a Fibonacci heap.

## Stopping Point Validation (Layer 2)

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
# Security Considerations

## Transport Security

PipeStream inherits security from QUIC {{RFC9000}} and TLS 1.3 {{RFC8446}}. All connections MUST use TLS 1.3 or later. Implementations MUST NOT provide mechanisms to disable encryption.

## Entity Payload Integrity

Each Entity MUST include a SHA-256 checksum in its EntityHeader. 

To support true streaming of large entities, implementations MAY begin processing an entity payload before the complete payload has been received and verified. However, the final rehydration or terminal SINK operation MUST NOT be committed until the complete payload checksum has been verified. 

If a checksum verification fails, the implementation MUST:
1. Reject the entity with PIPESTREAM_INTEGRITY_ERROR (0x04).
2. Discard any partial results or temporary state associated with the entity.
3. Propagate the failure according to the Completion Policy (Section 8.3).

Implementations that require immediate consistency SHOULD buffer the entire entity and verify the checksum before initiating processing.

## Resource Exhaustion

| Limit | Default | Description |
|-------|---------|-------------|
| Max scope depth | 7 | Prevents recursive bombs (8 levels: 0-7) |
| Max entities per scope | 4,294,967,294 | Memory bounds |
| Max window size | 2,147,483,648 | Backpressure threshold |
| Checkpoint timeout | 30s | Prevents stuck state |
| Claim check expiry | 86400s | Garbage collection |

Implementations MUST enforce all resource limits listed above. Exceeding any limit MUST result in the corresponding error code (see Section 11.4). Implementations SHOULD allow operators to configure stricter limits than the defaults shown here.

## Amplification Attacks

A single dehydration operation can produce an arbitrary number of child entities from a small input, creating a potential amplification vector. To mitigate this:

1. Implementations MUST enforce the max_entities_per_scope limit negotiated during capability exchange (Section 3.4). Any dehydration that would exceed this limit MUST be rejected.

2. Implementations MUST enforce the max_scope_depth limit. A dehydration chain deeper than this limit MUST be rejected with PIPESTREAM_DEPTH_EXCEEDED (0x07).

3. Implementations SHOULD enforce a configurable ratio between input entity size and total child entity count. A recommended default is no more than 1,000 children per megabyte of parent payload.

4. The backpressure mechanism (Section 9.1) provides a natural throttle: when the in-flight window fills, no new Entity IDs can be assigned until existing entities complete and the cursor advances. Implementations MUST NOT bypass backpressure for dehydration-generated entities.

## Privacy Considerations

PipeStream entity headers and control stream frames carry metadata that may reveal information about the documents being processed, even when payloads are encrypted at the application layer:

1. **Document structure leakage**: The number of child entities produced by dehydration, the scope depth, and the Entity ID assignment pattern may reveal the structure of the document being processed (e.g., a document that dehydrates into 50 children is likely a multi-page document). Implementations that require structural privacy SHOULD pad dehydration counts or use fixed decomposition granularity.

2. **Metadata in headers**: The `content_type`, `metadata` map, and `payload_length` fields in EntityHeader (Section 6.7) are transmitted in cleartext within the QUIC-encrypted stream. Implementations that require metadata confidentiality beyond transport encryption SHOULD encrypt EntityHeader fields at the application layer and use an opaque content_type such as `application/octet-stream`.

3. **Traffic analysis**: The timing and size of status frames on the Control Stream may correlate with document processing patterns. Implementations operating in privacy-sensitive environments SHOULD send status frames at fixed intervals with padding to obscure processing timing.

4. **Identifiers**: The `doc_id` field in PipeDoc (Section 7.1) and filenames in BlobBag entries are application-layer data but may be logged by intermediate processing nodes. Implementations SHOULD provide mechanisms to redact or pseudonymize identifiers at pipeline boundaries.

## Replay and Token Reuse

### Yield Token Replay

Yield tokens (Section 6.5.1) contain opaque continuation state that enables resumption of paused entity processing. A replayed yield token could cause an entity to be processed multiple times or to resume from a stale state. To prevent this:

1. Implementations MUST associate each yield token with a stable application context identifier (for example, a session identifier) and Entity ID. In Layer 0-only operation, this context MAY be implicit in the active transport connection. For Layer 2 resumptions that can occur across reconnects or different nodes, the context identifier MUST remain stable across transport connections. A yield token MUST be rejected if presented in a different context than the one that issued it, unless the token was explicitly transferred via a claim check.

2. Implementations MUST invalidate a yield token after it has been consumed for resumption. A second resumption attempt with the same token MUST be rejected.

3. The StoppingPointValidation (Section 9.6) provides integrity checking at resume time. Implementations MUST verify the `state_checksum` field before accepting a resumed entity. If the checksum does not match the current state, the resumption MUST be rejected and the entity MUST be reprocessed from the beginning.

### Claim Check Replay

Claim checks (Section 6.5.2) are long-lived references that can be redeemed in different sessions. To prevent misuse:

1. Each claim check carries an `expiry_timestamp` (Unix epoch microseconds). Implementations MUST reject expired claim checks.

2. Implementations MUST track redeemed claim check IDs and reject duplicate redemptions. The tracking state MUST persist for at least the claim check expiry duration.

3. Claim check IDs MUST be generated using a cryptographically secure random number generator to prevent guessing.

## Encryption Key Management

When using FileStorageReference with encryption:

1. Key IDs MUST reference keys in approved providers.
2. Wrapped keys MUST use approved envelope encryption.
3. Key rotation MUST be supported via key_id versioning.
4. Implementations MUST NOT log key material.
5. Implementations MUST NOT include unwrapped data encryption keys in EntityHeader metadata or Control Stream frames.
# IANA Considerations

This document requests the creation of several new registries and one ALPN identifier registration. All registries defined in this section use the "Expert Review" policy {{RFC8126}} for new assignments.

## ALPN Identifier Registration

| Protocol | Identification Sequence | Reference |
|----------|------------------------|-----------|
| PipeStream Version 1 | "pipestream/1" | [this document] |

## PipeStream Frame Type Registry

IANA is requested to create the "PipeStream Frame Types" registry. Values are categorized into Fixed (type-sized, no length prefix) frames in 0x50-0x7F and Variable (4-octet length prefix) frames in 0x80-0xFF. Values 0xC0-0xFF are reserved for private use.

| Value | Frame Type Name | Class | Size | Layer | Reference |
|-------|-----------------|-------|------|-------|-----------|
| 0x50 | STATUS | Fixed | 12 octets base | 0 | Section 6.2 |
| 0x54 | SCOPE_DIGEST | Fixed | 68 octets | 1 | Section 6.3 |
| 0x55 | BARRIER | Fixed | 8 octets | 1 | Section 6.4 |
| 0x56-0x7F | Reserved | Fixed | - | - | [this document] |
| 0x80 | CAPABILITIES | Var | Length-prefixed | 0 | Section 3.4 |
| 0x81 | CHECKPOINT | Var | Length-prefixed | 0 | Section 9.3 |
| 0x82-0xBF | Reserved | Var | - | - | [this document] |

## PipeStream Status Code Registry

IANA is requested to create the "PipeStream Status Codes" registry. Status codes are 4-bit values (0x0-0xF). Values 0xD-0xF are reserved for future Standards Action.

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
| 0xB | SKIPPED | 2 | Intentionally skipped |
| 0xC | ABANDONED | 2 | Timed out |

## PipeStream Error Code Registry

IANA is requested to create the "PipeStream Error Codes" registry. Values in the range 0x00-0x3F are assigned by Expert Review. Values in the range 0x40-0xFF are reserved for private use.

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

## URI Scheme Registration

The `session-id` segment identifies application context for detached or resumable resources (for example, Layer 2 yield/claim-check flows). PipeStream Layer 0 streaming semantics do not depend on this URI scheme.

```
pipestream-URI = "pipestream://" authority "/" session-id ["/" scope-path] ["/" entity-id]

scope-path = scope-id *("." scope-id)
```

Examples:
- `pipestream://processor.example.com/a1b2c3d4`
- `pipestream://processor.example.com:8443/a1b2c3d4/1.42/e5f6`
## Appendix A: Protobuf Schema Reference

### A.1. Protocol-Level Messages

```protobuf
// Copyright 2026 PipeStream AI
//
// PipeStream Protocol - IETF draft protocol for recursive entity streaming
// over QUIC. Defines the wire-format messages for Layers 0-2 of the
// PipeStream architecture: core streaming, recursive scoping, and resilience.
//
// Edition 2023 is used for closed enums (critical for wire-protocol safety)
// and implicit field presence (distinguishing "not set" from zero values).
// In this edition, all fields have explicit presence by default, making the
// 'optional' keyword unnecessary.

edition = "2023";

package pipestream.protocol.v1;

import "google/protobuf/any.proto";

// All enums in this file are CLOSED. Unknown enum values received on the wire
// MUST be rejected. This is essential because status codes are encoded as
// 4-bit values in the status frame wire format; accepting unknown values
// could cause undefined behavior in state machines and cursor advancement.
option features.enum_type = CLOSED;

// Capabilities describes the feature set supported by a PipeStream endpoint.
// Exchanged during the CONNECT handshake so that both sides can negotiate
// which protocol layers and resource limits apply to the session.
message Capabilities {
  // Whether the endpoint supports Layer 0 (core entity streaming).
  // MUST always be true; Layer 0 support is mandatory.
  bool layer0_core = 1;

  // Whether the endpoint supports Layer 1 (recursive scoping and dehydration).
  bool layer1_recursive = 2;

  // Whether the endpoint supports Layer 2 (resilience, yield, and claim-check).
  // Requires Layer 1 support; if layer1_recursive is false, this MUST be false.
  bool layer2_resilience = 3;

  // Maximum nesting depth allowed for recursive scopes.
  // Default is 7. Range 0-7 (constrained by 3-bit depth field in status frame flags).
  uint32 max_scope_depth = 4;

  // Maximum number of entities permitted within a single scope.
  // Default is 4,294,967,294 (2^32-2), matching the 32-bit entity ID space
  // (excluding reserved values NULL_ENTITY and CONNECTION_LEVEL).
  uint32 max_entities_per_scope = 5;

  // Maximum flow-control window size, in number of entities, that the
  // endpoint is willing to buffer before requiring cursor advancement.
  // Default is 2,147,483,648 (2^31).
  uint32 max_window_size = 6;
}

// EntityHeader is sent at the beginning of each entity stream to describe
// the payload that follows. It carries identity, lineage, content metadata,
// chunking information, and the completion policy that governs how partial
// failures of this entity's children are handled.
message EntityHeader {
  // Scope-local entity identifier (32-bit, range 1 to 0xFFFFFFFD).
  // Assigned by the sender using a cursor-based circular buffer.
  // Reserved values: 0x00000000 (NULL_ENTITY), 0xFFFFFFFE (SCOPE_MARKER),
  // 0xFFFFFFFF (CONNECTION_LEVEL).
  uint32 entity_id = 1;

  // Identifier of the parent entity that spawned this entity, or zero if
  // this entity is a root-level entity with no parent.
  uint32 parent_id = 2;

  // Identifier of the scope to which this entity belongs. Scopes group
  // related entities for recursive processing and completion tracking.
  // Set to 0 when Layer 1 is not negotiated.
  uint32 scope_id = 3;

  // Data layer of this entity's payload (0=BlobBag, 1=Semantic, 2=Parsed,
  // 3=Custom).
  uint32 layer = 4;

  // MIME content type of the entity payload (e.g. "application/json",
  // "application/x-protobuf").
  string content_type = 5;

  // Length in bytes of the complete entity payload, before any chunking.
  uint64 payload_length = 6;

  // SHA-256 integrity checksum of the complete entity payload (32 bytes).
  // Receivers MUST verify this before committing terminal output.
  // Incremental pre-verification processing is allowed for streaming
  // implementations, but MUST be rolled back on checksum failure.
  bytes checksum = 7;

  // Arbitrary key-value metadata attached to this entity by the producer.
  map<string, string> metadata = 8;

  // Chunking information for this entity. Present only when the payload
  // is split across multiple frames.
  ChunkInfo chunk_info = 9;

  // Completion policy that governs retry, timeout, and failure behavior
  // for this entity's children. Applies at Layer 2 (resilience).
  CompletionPolicy completion_policy = 10;
}

// ChunkInfo describes how a single entity payload is divided into ordered
// chunks when it is too large to send in a single frame.
message ChunkInfo {
  // Total number of chunks that make up the complete entity payload.
  uint32 total_chunks = 1;

  // Zero-based index of this chunk within the sequence.
  uint32 chunk_index = 2;

  // Byte offset within the complete payload where this chunk begins.
  uint64 chunk_offset = 3;
}

// CompletionPolicy controls Layer 2 resilience behavior for an entity or
// scope. It specifies how strictly all children must complete, how many
// retries are attempted, and what action to take on timeout or failure.
//
// With Edition 2023 field presence, "not set" is distinguishable from zero.
// When not set, implementations MUST use the documented defaults.
message CompletionPolicy {
  // Mode that determines how child-entity completion is evaluated.
  CompletionMode mode = 1;

  // Maximum number of retry attempts before the failure action is taken.
  // Default: 3. A value of 0 means no retries.
  uint32 max_retries = 2;

  // Delay in milliseconds between successive retry attempts.
  // Default: 1000.
  uint32 retry_delay_ms = 3;

  // Maximum time in milliseconds to wait for completion before the
  // timeout action is triggered. Default: 300000 (5 minutes).
  // A value of 0 means no timeout.
  uint32 timeout_ms = 4;

  // Minimum ratio of successful children (0.0 to 1.0) required for the
  // QUORUM completion mode to consider the scope complete.
  float min_success_ratio = 5;

  // Action to take when the timeout expires before completion.
  FailureAction on_timeout = 6;

  // Action to take when a child entity reports a terminal failure.
  FailureAction on_failure = 7;
}

// CompletionMode specifies the strategy used to decide whether a scope
// has completed successfully based on its children's statuses.
// CLOSED enum: unknown values MUST be rejected on the wire.
enum CompletionMode {
  // Default unspecified value. Implementations MUST treat this as STRICT.
  COMPLETION_MODE_UNSPECIFIED = 0;

  // All children must complete successfully for the scope to succeed.
  COMPLETION_MODE_STRICT = 1;

  // The scope succeeds if at least one child completes successfully;
  // failures in other children are tolerated.
  COMPLETION_MODE_LENIENT = 2;

  // The scope always succeeds regardless of individual child outcomes,
  // recording whatever results are available.
  COMPLETION_MODE_BEST_EFFORT = 3;

  // The scope succeeds when the ratio of successful children meets or
  // exceeds the min_success_ratio threshold.
  COMPLETION_MODE_QUORUM = 4;
}

// FailureAction specifies what a processor should do when an entity or
// scope encounters an error or timeout condition.
// CLOSED enum: unknown values MUST be rejected on the wire.
enum FailureAction {
  // Default unspecified value. Implementations MUST treat this as FAIL.
  FAILURE_ACTION_UNSPECIFIED = 0;

  // Propagate the failure immediately, aborting the scope.
  FAILURE_ACTION_FAIL = 1;

  // Skip the failed entity and continue processing remaining siblings.
  FAILURE_ACTION_SKIP = 2;

  // Retry the failed entity up to the configured max_retries limit.
  FAILURE_ACTION_RETRY = 3;

  // Defer the failed entity for later reprocessing via a claim check.
  FAILURE_ACTION_DEFER = 4;
}

// EntityStatus represents the lifecycle state of an entity as tracked
// on the control stream. Encoded as 4-bit values (0x0-0xC) in the 96-bit
// (12-octet) status frame wire format. Transitions follow the PipeStream
// state machine.
// CLOSED enum: unknown status values on the wire MUST cause a
// PIPESTREAM_INTEGRITY_ERROR (0x04).
enum EntityStatus {
  // Default unspecified value. Used as heartbeat signal on the wire (0x0).
  // MUST NOT appear in well-formed status frames for real entities.
  ENTITY_STATUS_UNSPECIFIED = 0;

  // The entity has been registered but processing has not yet started.
  ENTITY_STATUS_PENDING = 1;

  // The entity is currently being processed by a downstream consumer.
  ENTITY_STATUS_PROCESSING = 2;

  // The entity has been processed successfully.
  ENTITY_STATUS_COMPLETE = 3;

  // The entity encountered a terminal failure.
  ENTITY_STATUS_FAILED = 4;

  // The entity has reached a checkpoint barrier and is waiting for
  // sibling entities to catch up.
  ENTITY_STATUS_CHECKPOINT = 5;

  // The entity is being dehydrated (decomposed) into child entities
  // within a recursive scope.
  ENTITY_STATUS_DEHYDRATING = 6;

  // The entity's child results are being rehydrated (gathered) back
  // into the parent scope after recursive processing.
  ENTITY_STATUS_REHYDRATING = 7;

  // The entity has yielded processing and holds a yield token for
  // later resumption (Layer 2).
  ENTITY_STATUS_YIELDED = 8;

  // The entity has been deferred via a claim check for asynchronous
  // reprocessing at a later time (Layer 2).
  ENTITY_STATUS_DEFERRED = 9;

  // The entity is being retried after a transient failure (Layer 2).
  ENTITY_STATUS_RETRYING = 10;

  // The entity was skipped due to a SKIP failure action policy.
  ENTITY_STATUS_SKIPPED = 11;

  // The entity was abandoned after exhausting all retry and deferral
  // options. No further processing will be attempted.
  ENTITY_STATUS_ABANDONED = 12;
}

// ResolutionState tracks the completion state of an Assembly Manifest entry.
// CLOSED enum: unknown values MUST be rejected.
enum ResolutionState {
  // Default unspecified value.
  RESOLUTION_STATE_UNSPECIFIED = 0;

  // Dehydration is active; children are still being processed.
  RESOLUTION_STATE_ACTIVE = 1;

  // All children completed according to the completion policy.
  RESOLUTION_STATE_RESOLVED = 2;

  // Some children failed or were skipped, but the policy allowed
  // partial completion (LENIENT, BEST_EFFORT, or QUORUM met).
  RESOLUTION_STATE_PARTIAL = 3;

  // The dehydration failed; too many children failed to meet the
  // completion policy requirements.
  RESOLUTION_STATE_FAILED = 4;
}

// StatusFrame is sent on the control stream (QUIC Stream 0) to report
// status transitions for individual entities. The control stream provides a
// global, ordered view of entity lifecycle events across all scopes.
message StatusFrame {
  // Identifier of the entity whose status is being reported.
  uint32 entity_id = 1;

  // Scope to which the entity belongs.
  // Set to 0 when Layer 1 is not negotiated.
  uint32 scope_id = 2;

  // Current lifecycle status of the entity.
  EntityStatus status = 3;

  // Optional extension data associated with this status transition,
  // encoded as a protobuf Any for forward compatibility. Carried when
  // the E (extension) flag is set in the wire-format status frame.
  google.protobuf.Any extended_data = 4;
}

// CheckpointFrame defines a synchronization barrier. When a checkpoint
// is issued, all entities within the scope must reach a terminal state
// before processing may continue past it. This ensures consistency
// across parallel entity streams.
message CheckpointFrame {
  // Unique identifier for this checkpoint, scoped to the session.
  string checkpoint_id = 1;

  // Monotonically increasing sequence number used to order checkpoints
  // and detect gaps.
  uint64 sequence_number = 2;

  // Numeric ordering key used for barrier evaluation. Entities in this
  // scope with IDs lower than this value (modulo circular ID rules)
  // must reach terminal state before the checkpoint is satisfied.
  uint32 checkpoint_entity_id = 3;

  // Scope to which this checkpoint applies.
  uint32 scope_id = 4;

  // Bitfield of checkpoint flags.
  // Bit 0: MANDATORY (must block processing).
  // Bit 1: SCOPE_LOCAL (applies only within the current scope).
  uint32 flags = 5;

  // Maximum time in milliseconds to wait for all entities to reach the
  // checkpoint before it is considered timed out. Default: 30000.
  uint32 timeout_ms = 6;
}

// AssemblyManifestEntry tracks parent-child relationships created during
// entity dehydration (decomposition). It records which child entities
// were spawned from a parent and their individual completion statuses,
// enabling the rehydration phase to synthesize results.
message AssemblyManifestEntry {
  // Identifier of the parent entity that was dehydrated into children.
  uint32 parent_id = 1;

  // Scope in which the dehydration occurred.
  uint32 scope_id = 2;

  // Ordered list of child entity identifiers produced by dehydration.
  repeated uint32 children_ids = 3;

  // Status of each child entity, positionally corresponding to children_ids.
  repeated EntityStatus children_status = 4;

  // Completion policy governing how child results are rehydrated and
  // when the parent may be considered complete.
  CompletionPolicy policy = 5;

  // Timestamp (Unix epoch microseconds) when the dehydration occurred
  // and child entities were created.
  uint64 created_at = 6;

  // Current resolution state of this Assembly Manifest entry.
  ResolutionState state = 7;
}

// YieldToken allows a Layer 2 processor to pause processing of an entity
// and resume it later. The token captures the reason for yielding, an
// opaque continuation state, and validation data to ensure consistency
// when the entity is rehydrated.
message YieldToken {
  // Reason the processor is yielding control of this entity.
  YieldReason reason = 1;

  // Opaque continuation state that the processor will need to resume
  // work on this entity. The contents are processor-defined.
  bytes continuation_state = 2;

  // Validation data used to verify that the entity state has not
  // changed between yield and resume.
  StoppingPointValidation validation = 3;
}

// YieldReason describes why a processor chose to yield processing of
// an entity rather than completing it immediately.
// CLOSED enum: unknown values MUST be rejected on the wire.
enum YieldReason {
  // Default unspecified value. MUST NOT appear in well-formed yield tokens.
  YIELD_REASON_UNSPECIFIED = 0;

  // The processor needs to make an external call (e.g. network request)
  // and does not want to hold the stream open while waiting.
  YIELD_REASON_EXTERNAL_CALL = 1;

  // The processor has been rate-limited and must back off before
  // continuing work on this entity.
  YIELD_REASON_RATE_LIMITED = 2;

  // The processor is waiting for a sibling entity to reach a certain
  // state before it can continue.
  YIELD_REASON_AWAITING_SIBLING = 3;

  // The processor requires human or external approval before proceeding
  // with the next stage of processing.
  YIELD_REASON_AWAITING_APPROVAL = 4;

  // A shared resource required by the processor is currently busy or
  // locked by another operation.
  YIELD_REASON_RESOURCE_BUSY = 5;
}

// ClaimCheck is a Layer 2 deferred-processing reference. When an entity
// cannot be completed immediately, a claim check is issued so that the
// entity can be reclaimed and processed asynchronously at a later time.
message ClaimCheck {
  // Unique identifier for this claim check within the session.
  uint64 claim_id = 1;

  // Identifier of the entity that has been deferred.
  uint32 entity_id = 2;

  // Scope to which the deferred entity belongs.
  uint32 scope_id = 3;

  // Unix epoch timestamp (in microseconds) after which this claim check
  // expires and the entity may be considered abandoned.
  // Default: 86,400,000,000 (24 hours).
  uint64 expiry_timestamp = 4;

  // Validation data that must be checked when the claim is redeemed to
  // ensure the entity state is still consistent.
  StoppingPointValidation validation = 5;
}

// StoppingPointValidation captures a snapshot of processing progress at
// the moment an entity is yielded or deferred. When the entity is later
// resumed, these fields are checked to confirm that no state corruption
// or unexpected changes occurred during the pause.
message StoppingPointValidation {
  // SHA-256 checksum of the processor's internal state at the stopping
  // point, used to detect tampering or corruption.
  bytes state_checksum = 1;

  // Total number of payload bytes the processor had consumed when it
  // stopped, enabling position-based resumption.
  uint64 bytes_processed = 2;

  // Number of child entities that had completed at the stopping point.
  uint32 children_complete = 3;

  // Total number of child entities expected, allowing the validator to
  // confirm no children were added or removed during the pause.
  uint32 children_total = 4;

  // Whether the processor's state supports resumption. If false, the
  // entity must be reprocessed from the beginning.
  bool is_resumable = 5;

  // Reference to the most recent checkpoint that the entity had passed
  // at the time of stopping, for cross-referencing with the control stream.
  string checkpoint_ref = 6;
}

// ScopeDigest is a Layer 1 summary of a completed scope. It provides
// aggregate counters and a Merkle root hash that covers all entity
// outcomes within the scope, enabling efficient integrity verification
// without replaying the full control stream.
message ScopeDigest {
  // Identifier of the scope being summarized.
  uint32 scope_id = 1;

  // Total number of entities that were processed in this scope,
  // regardless of outcome.
  uint64 entities_processed = 2;

  // Number of entities that completed successfully.
  uint64 entities_succeeded = 3;

  // Number of entities that terminated with a failure status.
  uint64 entities_failed = 4;

  // Number of entities that were deferred via claim checks and have
  // not yet been reclaimed.
  uint64 entities_deferred = 5;

  // Merkle root hash computed over all entity outcomes in the scope,
  // providing a single cryptographic digest for integrity verification.
  bytes merkle_root = 6;
}

// PipeDoc represents the top-level document envelope for an entity.
message PipeDoc {
  // Unique document identifier (UUID recommended).
  string doc_id = 1;

  // Identifier of the entity represented by this document.
  uint32 entity_id = 2;

  // Multi-tenancy and access control context.
  OwnershipContext ownership = 3;
}

// OwnershipContext defines multi-tenancy and access control for entities.
message OwnershipContext {
  // Unique identifier of the entity owner.
  string owner_id = 1;

  // Unique identifier of the group with access to the entity.
  string group_id = 2;

  // List of access scopes or roles associated with the entity.
  repeated string scopes = 3;
}

// FileStorageReference provides a location for data stored in cloud or local
// storage, rather than carried in the entity stream.
message FileStorageReference {
  // Storage provider identifier (e.g. "s3", "blob", "gcs").
  string provider = 1;

  // Name of the bucket or container where the file is stored.
  string bucket = 2;

  // Object key or path within the bucket.
  string key = 3;

  // Optional region hint for the storage provider.
  string region = 4;

  // Provider-specific attributes or metadata for the storage reference.
  map<string, string> attrs = 5;

  // Encryption metadata for the stored file, if encrypted.
  EncryptionMetadata encryption = 6;
}

// EncryptionMetadata defines encryption parameters for stored data.
message EncryptionMetadata {
  // Encryption algorithm used (e.g. "AES-256-GCM").
  string algorithm = 1;

  // Identifier of the key provider (e.g. "aws-kms", "vault").
  string key_provider = 2;

  // Identifier or URI of the encryption key.
  string key_id = 3;

  // Optional client-side wrapped Data Encryption Key (DEK).
  bytes wrapped_key = 4;

  // Initialization vector used for encryption.
  bytes iv = 5;

  // Additional encryption context for the key provider.
  map<string, string> context = 6;
}
```
## Appendix B: Protocol Layer Capability Matrix

| Feature | Layer 0 | Layer 1 | Layer 2 |
|---------|---------|---------|---------|
| Unified status frame (96-bit base) | ✓ | ✓ | ✓ |
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
