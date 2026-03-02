# Introduction

## Problem Statement

Distributed processing pipelines face significant challenges when handling large, complex work units that require multiple stages of transformation, analysis, and enrichment. Traditional batch processing approaches require entire inputs to be loaded into memory, processed sequentially, and transmitted in their entirety between processing stages. This methodology introduces substantial latency, excessive memory consumption, and poor utilization of distributed computing resources.

Modern distributed processing workflows increasingly demand the ability to:

- Process inputs incrementally as data becomes available
- Distribute processing load across heterogeneous worker nodes
- Maintain consistency guarantees across parallel processing paths
- Handle inputs of arbitrary size without memory constraints
- Support recursive decomposition where constituent parts may themselves be decomposed
- Scale from single work units to collections of millions of inputs

Current approaches based on batch processing and store-and-forward architectures are inefficient for large inputs and fail to exploit the inherent parallelism available in distributed processing environments. Furthermore, existing streaming protocols do not provide the consistency semantics required for hierarchical processing where the integrity of the rehydrated output depends on the successful processing of all constituent parts.

## Applicability

Although PipeStream was originally motivated by distributed document processing pipelines, its recursive scatter-gather design is domain-neutral. The protocol is applicable to any workload that can be modeled as hierarchical decomposition of a root entity into sub-entities, parallel processing of those sub-entities, and deterministic reassembly of results. Example domains include:

- Distributed document processing and content enrichment pipelines
- Hierarchical video transcoding and rendering (scene decomposition)
- Federated computation pipelines such as distributed machine learning
- Genomic sequencing and assembly workflows

Throughout this document, "document" is used as the canonical example of a root entity, but all protocol mechanisms apply equally to any decomposable work unit.

## PipeStream Overview

PipeStream addresses these challenges by defining a streaming protocol that enables incremental processing with strong consistency guarantees. The protocol is built upon QUIC {{RFC9000}} transport, leveraging its native support for multiplexed streams, low-latency connection establishment, and reliable delivery semantics.

The fundamental innovation of PipeStream is its treatment of inputs as recursive compositions of entities. A root entity MAY be decomposed into multiple sub-entities, each of which MAY itself be further decomposed, creating a tree structure of processing tasks. This recursive decomposition enables fine-grained parallelism while the protocol's control stream mechanism ensures that all branches of the decomposition tree are tracked and synchronized.

PipeStream employs a dual-stream design:

1. **Data Stream**: Carries entity payloads through the processing pipeline. Entities flow through this stream with minimal buffering, enabling low-latency incremental processing.

2. **Control Stream**: Carries control information tracking the status of entity decomposition and rehydration. The control stream ensures that all parts of a dehydrated entity are accounted for before rehydration proceeds.

## Design Philosophy

PipeStream implements a recursive scatter-gather pattern {{scatter-gather}} over QUIC streams. An input entity is "dehydrated" (scattered) at the source into constituent sub-entities, these sub-entities are transmitted and processed in parallel across distributed pipeline stages, and finally the results are "rehydrated" (gathered) at the destination to reconstitute the complete processed output. The checkpoint blocking mechanism (Section 9.3) provides barrier synchronization semantics analogous to the barrier pattern in parallel computing.

This approach provides several advantages:

- **Incremental Processing**: Processing nodes MAY begin work on early entities before the complete input has been transmitted.

- **Parallelism**: Independent entities MAY be processed concurrently across multiple worker nodes.

- **Memory Efficiency**: No single node is required to hold the complete input in memory.

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
