# PipeStream Protocol

**PipeStream: A Recursive Entity Streaming Protocol for Distributed Document Processing**

**Internet-Draft: draft-krickert-pipestream-02**

## Overview

PipeStream is a recursive entity streaming protocol designed for high-performance distributed document processing over QUIC transport. It implements a scatter-gather pattern where documents are "dehydrated" (scattered) into constituent entities, processed in parallel across distributed nodes, and "rehydrated" (gathered) back into complete processed documents with strong consistency guarantees.

The protocol is developed by **PipeStream AI**, an open-source focused organization aimed at creating free tools with the goal of establishing a standard document protocol optimized for AI processing.

## Key Features

- **Recursive Dehydration**: Decompose documents into hierarchical entities (e.g., Doc -> Section -> Paragraph).
- **Dual-Stream Architecture**: Separate bit-packed Control stream (status tracking) and multiplexed Entity streams (data).
- **Protocol Layering**: Modular Core, Recursive, and Resilience layers.
- **AI-Optimized Data Model**: Built-in support for semantic chunks, vector embeddings, and NLP annotations.
- **Edition 2023 Protobuf**: Modern, safe, and extensible header definitions.
- **Cloud-Agnostic Storage**: Generic references for object storage providers.
- **Resilience Mechanics**: Native support for yield/resume, claim checks, and completion policies.

## Repository Structure

```
.
├── draft-krickert-pipestream-02.md    # Current Internet-Draft (Markdown)
├── draft-krickert-pipestream-02.xml   # IETF submission source (RFC XML v3)
├── OVERVIEW.md                        # High-level architectural summary
├── proto/                             # Protobuf definitions (Edition 2023)
│   ├── pipestream/data/v1/            # Data layer schemas
│   └── pipestream/protocol/v1/        # Framing and coordination schemas
├── sections/                          # Individual specification sections
└── examples/                          # Protocol usage examples
```

## Getting Started

1.  Review the [OVERVIEW.md](OVERVIEW.md) for a technical summary.
2.  Read the full specification in [draft-krickert-pipestream-02.md](draft-krickert-pipestream-02.md).
3.  Examine the Protobuf definitions in the `proto/` directory.

## Authors

- **Kristian Rickert** (PipeStream AI) - <kristian.rickert@pipestream.ai>

## Status

This is an active Internet-Draft. See `draft-krickert-pipestream-02.md` for the current specification.
