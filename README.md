# PipeStream Protocol

**PipeStream: A Recursive Entity Streaming Protocol for Distributed Document Processing over QUIC**

**Internet-Draft: draft-krickert-pipestream-00**

## Overview

PipeStream is a recursive entity streaming protocol designed for high-performance distributed document processing over QUIC transport. It implements a scatter-gather pattern where documents are "dehydrated" (scattered) into constituent entities, processed in parallel across distributed nodes, and "rehydrated" (gathered) back into complete processed documents with strong consistency guarantees.

## Key Features

- **Recursive Dehydration/Rehydration**: Decompose documents into hierarchical entities (Doc → Section → Paragraph) and reassemble them after distributed processing.
- **Unified Control Plane (Stream 0)**: A 64-bit bit-packed Control Stream with a 1-octet Type header (UCF) for status tracking.
- **Protocol Layering**: Three modular layers — Layer 0 (Core), Layer 1 (Recursive), Layer 2 (Resilience).
- **AI-Optimized Data Model**: Four data layers with built-in support for semantic chunks, vector embeddings, NLP annotations, and structured parsed metadata.
- **Stateless Depth Tracking**: Explicit 3-bit depth fields in status frames for robust hierarchical synchronization.
- **Cloud-Agnostic Storage**: Generic `FileStorageReference` supporting AWS, Azure, GCP, and custom providers with envelope encryption.

## Repository Structure

This repository uses a modular authoring workflow for IETF drafts:

- **`sections-src/`**: **The Source of Truth.** Individual Markdown files for each RFC section. Edit these files directly.
- **`draft-template.md`**: The kramdown-rfc assembly template that includes all sections.
- **`proto/`**: Canonical Protobuf definitions (Edition 2023). Inline protobuf blocks in the spec MUST match these files.
- **`REFERENCE_IMPLEMENTATION.md`**: Informative guidance on algorithms (Fibonacci heaps, Merkle trees).
- **`OVERVIEW.md`**: High-level architectural summary.

### Building the Draft

The monolithic draft is treated as a build artifact and is not checked into the repository. To generate the final draft from sections:

```bash
# Using kramdown-rfc (Ruby):
gem install kramdown-rfc
kdrfc draft-template.md          # Produces XML, then txt/html via xml2rfc
```

## Authors

- **Kristian Rickert** (PipeStream AI) — <kristian.rickert@pipestream.ai>

## Status

This is an active Internet-Draft targeting IETF standards track. The current revision is **draft-krickert-pipestream-00**.
