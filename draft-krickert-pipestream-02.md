---
title: "PipeStream: A Recursive Entity Streaming Protocol for Distributed Processing over QUIC"
abbrev: "PipeStream"
docname: draft-krickert-pipestream-02
category: std
submissiontype: IETF
number:
date: 2026-02-19
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

--- abstract

This document specifies PipeStream, a recursive entity streaming protocol designed for distributed document processing over QUIC transport. PipeStream enables the decomposition ("vaporization") of documents into constituent entities, their transmission across distributed processing nodes, and subsequent reassembly ("rejoining") at destination endpoints. The protocol employs a dual-stream architecture consisting of a bit-packed ledger stream for high-frequency state coordination and multiplexed entity streams for payload transmission.

--- middle

# Introduction

### 1.1. Problem Statement

Distributed document processing pipelines face significant challenges when handling large, complex documents that require multiple stages of transformation, analysis, and enrichment. Traditional approaches require entire documents to be buffered in memory, processed sequentially, and transmitted in their entirety between stages. This introduces substantial latency and memory exhaustion risks.

Modern workflows demand the ability to process documents incrementally, distribute load across heterogeneous nodes, and maintain strict consistency across parallel processing paths where document parts may themselves be decomposed recursively.

### 1.2. Protocol Overview

PipeStream addresses these challenges by treating documents as recursive compositions of **Entities**. It leverages **QUIC [RFC9000]** to provide native multiplexing and low-latency delivery. The protocol is organized into three layers:

*   **Layer 0 (Core):** Basic streaming, checkpoints, and dematerialization/rematerialization semantics.
*   **Layer 1 (Recursive):** Hierarchical scoping, Merkle-based digest propagation, and barriers.
*   **Layer 2 (Resilience):** Yield/resume mechanics, claim checks, and completion policies.

# Terminology

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "NOT RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in BCP 14 {{RFC2119}} {{RFC8174}} when, and only when, they appear in all capitals, as shown here.

Entity:
: The fundamental unit of data. Represents either a complete document or a constituent part. Entities are immutable once created.

Dematerialize:
: The operation of decomposing an Entity into N constituent sub-entities for parallel processing.

Rematerialize:
: The inverse of dematerialization; reassembling N sub-entities back into a single parent based on a completion policy.

Ledger:
: The control plane transmitted on Stream 0, tracking Entity completion status using bit-packed frames.

Cursor:
: A pointer to the lowest unresolved Entity ID within a scope, enabling sliding-window ID recycling.

# QUIC Stream Mapping

### 3.1. Ledger Stream (Stream 0)

The Ledger Stream MUST use QUIC Stream ID 0 (client-initiated bidirectional). Both endpoints transmit 32-bit word-aligned frames on this stream to coordinate state.

### 3.2. Entity Streams (Streams 2+)

Entity Streams MUST be unidirectional. Each stream carries exactly one Entity payload.
*   **Client-Initiated:** Stream IDs 2, 6, 10... (4n + 2)
*   **Server-Initiated:** Stream IDs 3, 7, 11... (4n + 3)

# Wire Format

### 4.1. Ledger Frame (Stream 0)

Ledger frames are 4 octets (32 bits) in length:

~~~
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |E|C|              Entity ID (20 bits)         |Stat |  Flags  |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
~~~

E (1 bit):
: Extended frame flag.

C (1 bit):
: Cursor update flag.

Entity ID (20 bits):
: Scope-local identifier.

Stat (4 bits):
: Status code (0x0=UNSPECIFIED, 0x1=PENDING, 0x2=PROCESSING, 0x3=COMPLETE, 0x4=FAILED, 0x5=CHECKPOINT, 0x6=DEMATERIALIZING, 0x7=REMATERIALIZING, 0x8=YIELDED, 0x9=DEFERRED, 0xA=RETRYING, 0xB=SKIPPED, 0xC=ABANDONED).

### 4.2. Entity Header (Streams 2+)

Every Entity stream MUST begin with a length-prefixed Protobuf **EntityHeader** defined in Appendix A. This header carries MIME types, checksums, and the parent-child relationship needed for rematerialization.

# Consistency and Flow Control

### 5.1. Checkpoint Blocking

Checkpoints are synchronization points where all Entities with an ID less than the checkpoint MUST reach a terminal state before processing proceeds. This ensures that side effects (e.g., database writes) are committed in order.

### 5.2. Application Windowing

PipeStream implements a sliding window using the 20-bit Entity ID space.
*   **Backpressure:** Senders MUST NOT allocate a new ID if `(ID - Cursor) % 2^20` exceeds the negotiated `max_window_size`.
*   **Transport Interaction:** Implementations MUST NOT depend on QUIC flow control for application-level windowing. If the application window is full, the sender SHOULD stop opening new QUIC streams.

# Security Considerations

### 6.1. Dematerialization Depth

To prevent recursive "zip-bomb" attacks, implementations MUST enforce `max_scope_depth` (default: 8). Dematerialization exceeding this limit MUST result in a `PIPESTREAM_DEPTH_EXCEEDED` error.

### 6.2. Payload Integrity

Every Entity stream MUST include a SHA-256 checksum in the header. Receivers MUST verify the payload before marking the status as `COMPLETE` on the ledger.

# IANA Considerations

### 7.1. ALPN Identifier

This document registers the ALPN ID "pipestream/1".

### 7.2. Port Number

The default UDP port for PipeStream is 8443 (subject to formal assignment).

# Implementation Status

The PipeStream protocol is currently implemented in the **PipeStream Application Suite**, providing a reference for recursive document dematerialization and rematerialization in distributed cloud environments.

--- back

# Appendix A: Protobuf Definitions

### A.1. Protocol Schema (Edition 2023)

```protobuf
edition = "2023";
package pipestream.protocol.v1;

option features.enum_type = CLOSED;

message EntityHeader {
  uint32 entity_id = 1;
  uint32 parent_id = 2;
  uint32 scope_id = 3;
  uint32 layer = 4;
  string content_type = 5;
  uint64 payload_length = 6;
  bytes checksum = 7;
  map<string, string> metadata = 8;
  ChunkInfo chunk_info = 9;
  CompletionPolicy completion_policy = 10;
}

enum EntityStatus {
  ENTITY_STATUS_UNSPECIFIED = 0;
  ENTITY_STATUS_PENDING = 1;
  ENTITY_STATUS_PROCESSING = 2;
  ENTITY_STATUS_COMPLETE = 3;
  ENTITY_STATUS_FAILED = 4;
  ENTITY_STATUS_CHECKPOINT = 5;
  ENTITY_STATUS_DEMATERIALIZING = 6;
  ENTITY_STATUS_REMATERIALIZING = 7;
  ENTITY_STATUS_YIELDED = 8;
  ENTITY_STATUS_DEFERRED = 9;
  ENTITY_STATUS_RETRYING = 10;
  ENTITY_STATUS_SKIPPED = 11;
  ENTITY_STATUS_ABANDONED = 12;
}

enum YieldReason {
  YIELD_REASON_UNSPECIFIED = 0;
  YIELD_REASON_EXTERNAL_CALL = 1;
  YIELD_REASON_RATE_LIMITED = 2;
  YIELD_REASON_AWAITING_SIBLING = 3;
  YIELD_REASON_AWAITING_APPROVAL = 4;
  YIELD_REASON_RESOURCE_BUSY = 5;
}
```

# Acknowledgments

The authors would like to thank the IETF Transport and Application communities for their foundational work on QUIC and Protocol Buffers.
