# PipeStream: A Recursive Entity Streaming Protocol for Distributed Document Processing

**Internet-Draft**

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

## References

### Normative References

**[RFC2119]**
:   Bradner, S., "Key words for use in RFCs to Indicate Requirement Levels", BCP 14, RFC 2119, DOI 10.17487/RFC2119, March 1997.

**[RFC8174]**
:   Leiba, B., "Ambiguity of Uppercase vs Lowercase in RFC 2119 Key Words", BCP 14, RFC 8174, DOI 10.17487/RFC8174, May 2017.

**[RFC9000]**
:   Iyengar, J., Ed. and M. Thomson, Ed., "QUIC: A UDP-Based Multiplexed and Secure Transport", RFC 9000, DOI 10.17487/RFC9000, May 2021.
