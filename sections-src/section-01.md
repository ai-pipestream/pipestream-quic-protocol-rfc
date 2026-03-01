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
