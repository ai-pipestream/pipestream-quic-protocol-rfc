# Introduction

## Problem Statement

Distributed processing systems often decompose a root task into child tasks,
process those children independently, and combine their results. When stages
belong to different operators, their coordination interfaces must agree on
identity, completion, failure, and resumption, in addition to moving bytes.

PipeStream proposes an application protocol over QUIC for these interactions.
It assigns explicit wire semantics to entity lifecycle statuses, scoped
completion summaries, checkpoints, and durable continuation references.
It does not replace application processing logic, a scheduler, or durable
storage. Existing transports and RPC systems can carry equivalent application
logic; the interoperability question is whether a shared protocol vocabulary
reduces the coordination that each pair of applications must define.

## Applicability

Candidate workloads include document enrichment, chunked file processing,
video processing, and hierarchical work orchestration. Payload meanings,
processing authorization, and result validation belong to application
profiles. The prototypes and tested scenarios are described in Appendix D;
this document makes no measured performance or deployment-scale claim for
these candidate workloads.

PipeStream can operate between standalone servers or embedded endpoints.
Embedding a protocol library in a process does not require a network hop
between that library and its application. Across a transport boundary, the
QUIC mapping and negotiated lifecycle semantics still apply.

## PipeStream Overview

Entity payloads use independent unidirectional QUIC streams. A bidirectional
Control Stream carries capability negotiation, lifecycle reports, and
synchronization messages. Stream independence allows unrelated payloads to
progress despite loss or slow consumption on one Entity Stream, subject to
connection-level flow control and implementation resource limits.

PipeStream is an application protocol over QUIC {{RFC9000}}, not an extension
to QUIC transport frames. Routers and QUIC stacks need not understand work
semantics. A PipeStream endpoint interprets the control messages and applies
the state machine alongside its QUIC transport implementation.

## Design Philosophy

The scatter-gather pattern {{scatter-gather}} provides the organizing model.
A decomposed root waits for its children to resolve according to its
completion policy before rehydration. STRICT requires success of every
child; partial-success policies deliberately have weaker outcomes.

The protocol reports what peers claim to have completed. A status digest
does not prove correct computation or authenticate a content lineage.
Transport security, payload integrity, completion policy, and application
authorization have distinct roles.

Incremental reception can avoid buffering an entire workload. Implementations
still require explicit byte, stream, entity, and metadata budgets.
Appendix E identifies remaining design choices that must be resolved before
claiming a complete interoperable implementation.

## Protocol Layering

PipeStream is organized into three protocol layers to accommodate varying deployment requirements:

| Protocol Layer | Name | Description |
|----------------|------|-------------|
| Layer 0 | Core | Basic streaming, dehydrate/rehydrate, checkpoint |
| Layer 1 | Recursive | Hierarchical scopes, digest propagation, barriers |
| Layer 2 | Resilience | Yield/resume, claim checks, completion policies |

Implementations MUST support Layer 0. Support for Layers 1 and 2 is OPTIONAL and negotiated during connection establishment.

## Scope

This document specifies the PipeStream protocol including message
formats, state machines, error handling, and the interaction between
data and control streams. The document defines the transport-level layer
field and recursive processing semantics but leaves concrete payload
meaning to application profile specifications.
