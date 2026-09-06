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

Version 1 is organized into three protocol layers to accommodate varying deployment requirements:

| Protocol Layer | Name | Description |
|----------------|------|-------------|
| Layer 0 | Core | Basic streaming, dehydrate/rehydrate, checkpoint |
| Layer 1 | Recursive | Hierarchical scopes, digest propagation, barriers |
| Layer 2 | Resilience | Yield/resume, claim checks, completion policies |

Version-1 implementations MUST support Layer 0. Support for Layers 1 and 2 is OPTIONAL and negotiated during connection establishment. Version 2 uses the distinct Core/profile mapping in Section 12; it does not inherit these layer promises.

## Scope

This document specifies the PipeStream protocol including message
formats, state machines, error handling, and the interaction between
data and control streams. The document defines the transport-level layer
field and recursive processing semantics but leaves concrete payload
meaning to application profile specifications.

## Protocol Model and Trust Boundaries

Following the reviewer-oriented model in {{RFC4101}}, the protocol separates
four observations: declaration of intended work, admission of validated
input, authoritative processing outcome, and closure of a covered work set.
A declaration ACK is not payload admission; admission is not successful
execution; a scope digest is not proof that the computation was correct.

The originator assigns identity and sends payloads. The receiver is the
processing authority for their lifecycle. Both rely on authenticated
transport, but durable work additionally requires authorization of the
requesting principal. Application code defines what processing means,
how output is delivered, and how external effects are fenced or made
idempotent. A transport disconnect leaves unobserved outcomes unknown.

Core supplies the stream and status vocabulary. The sealed-work profile
fixes the membership that must close; the authenticated-session profile
binds durable access to a principal; the authenticated-recovery profile
correlates retained admission and terminal outcomes after reconnect.
Only negotiated combinations are valid. In version 1, authenticated
recovery does not combine with sealed work. These profiles do
not together imply an unspecified general-purpose workflow engine.

Section 12 defines the successor version-2 contract, in which authenticated
durable work includes sealed membership and retained replay, and an additional
profile defines result delivery. This is a normative design, not a claim that
the existing version-1 prototypes implement it; Appendix D records that boundary.
