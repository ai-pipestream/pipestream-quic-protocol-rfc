# PipeStream Protocol

**PipeStream: A Recursive Entity Streaming Protocol for Distributed Document Processing over QUIC**

**Internet-Draft: draft-krickert-pipestream-02**

## Overview

PipeStream is a recursive entity streaming protocol designed for high-performance distributed document processing over QUIC transport. It implements a scatter-gather pattern where documents are "dehydrated" (scattered) into constituent entities, processed in parallel across distributed nodes, and "rehydrated" (gathered) back into complete processed documents with strong consistency guarantees.

The protocol is developed by **PipeStream AI**, an open-source organization building free tools with the goal of establishing a standard document protocol optimized for AI-driven ingestion, semantic analysis, and enterprise search.

## Key Features

- **Recursive Dehydration/Rehydration**: Decompose documents into hierarchical entities (Doc → Section → Paragraph) and reassemble them after distributed processing.
- **Dual-Stream Architecture**: A 64-bit bit-packed Control Stream (Stream 0) for status tracking, plus multiplexed unidirectional Entity Streams for data.
- **Protocol Layering**: Three modular layers — Layer 0 (Core), Layer 1 (Recursive scoping), Layer 2 (Resilience) — negotiated per session.
- **AI-Optimized Data Model**: Four data layers with built-in support for semantic chunks, vector embeddings, NLP annotations, and structured parsed metadata.
- **32-bit Entity IDs**: Full `uint32` address space with cursor-based circular-buffer recycling and backpressure.
- **Edition 2023 Protobuf**: Closed enums and implicit field presence for wire-protocol safety.
- **Cloud-Agnostic Storage**: Generic `FileStorageReference` supporting AWS, Azure, GCP, and custom providers with envelope encryption.
- **Resilience Mechanics**: Yield/resume with continuation tokens, claim checks for async deferral, and configurable completion policies (STRICT, LENIENT, BEST_EFFORT, QUORUM).

## How the RFC Is Governed

This specification is authored as an IETF Internet-Draft following the standards-track process. The repository is structured to support collaborative, agent-friendly development:

### Source of Truth

The specification is written in **kramdown-rfc** Markdown — the standard authoring format for IETF drafts. Individual sections live in `sections-src/` as independent files, and a top-level `draft-template.md` assembles them using kramdown-rfc `{::include}` directives. This means:

- **Each section can be edited independently** — by a human, an AI agent, or a CI pipeline — without merge conflicts across the rest of the document.
- **The monolithic draft** (`draft-krickert-pipestream-02.md`) is the current assembled output and is what gets submitted to the IETF datatracker.
- **Protobuf schemas** in `proto/` are the canonical wire-format definitions. Inline protobuf blocks in the spec text MUST match the standalone `.proto` files.

### Section Layout (`sections-src/`)

| File | Contents |
|------|----------|
| `frontmatter.md` | YAML metadata, author info, references |
| `abstract.md` | Abstract |
| `section-01.md` | Introduction |
| `section-02.md` | Terminology |
| `section-03.md` | Protocol Layers |
| `section-04.md` | Protocol Overview |
| `section-05.md` | QUIC Stream Mapping |
| `section-06.md` | Frame Formats (wire diagrams) |
| `section-07.md` | Entity Model (data layers) |
| `section-08.md` | Protocol Operations (CONNECT/PARSE/PROCESS/SINK) |
| `section-09.md` | Rehydration Semantics |
| `section-10.md` | Security Considerations |
| `section-11.md` | IANA Considerations |
| `appendix-a.md` | Protobuf Schema Reference |
| `appendix-b.md` | Protocol Layer Capability Matrix |

### Building the Draft

The `draft-template.md` file is the assembly template. To produce the final draft from sections:

```bash
# Using kramdown-rfc (Ruby):
gem install kramdown-rfc
kdrfc draft-template.md          # Produces XML, then txt/html via xml2rfc

# Or use the IETF Author Tools web service:
# https://author-tools.ietf.org/
```

For development, the assembled Markdown (`draft-krickert-pipestream-02.md`) can also be read directly.

### Companion Documents

- **`REFERENCE_IMPLEMENTATION.md`** — Informative implementation guidance (Fibonacci heap pseudocode, out-of-order completion handling, memory bounds). Not part of the normative specification.
- **`OVERVIEW.md`** — High-level architectural summary for newcomers.

### Versioning

Draft revisions are tracked via the `docname` field in `sections-src/frontmatter.md` (e.g., `draft-krickert-pipestream-02`). When a new revision is cut, bump the version number there and regenerate the assembled draft.

## Repository Structure

```
.
├── draft-template.md                  # kramdown-rfc assembly template ({::include} directives)
├── draft-krickert-pipestream-02.md    # Current assembled Internet-Draft
├── sections-src/                      # Source-of-truth section files
│   ├── frontmatter.md                 #   YAML metadata + references
│   ├── abstract.md                    #   Abstract
│   ├── section-01.md ... section-11.md#   RFC sections 1-11
│   ├── appendix-a.md                  #   Protobuf schema reference
│   └── appendix-b.md                  #   Capability matrix
├── proto/                             # Canonical Protobuf definitions (Edition 2023)
│   └── pipestream/
│       ├── data/v1/                   #   Data model (PipeDoc, BlobBag, SemanticLayer, etc.)
│       └── protocol/v1/              #   Wire-format messages (StatusFrame, EntityHeader, etc.)
├── OVERVIEW.md                        # High-level architectural summary
├── REFERENCE_IMPLEMENTATION.md        # Informative implementation guidance
├── sections/                          # Legacy section files (deprecated)
└── build/                             # Build tooling
```

## Getting Started

1. Review the [OVERVIEW.md](OVERVIEW.md) for a high-level architectural summary.
2. Read the full specification in [draft-krickert-pipestream-02.md](draft-krickert-pipestream-02.md).
3. Examine the Protobuf definitions in the `proto/` directory.
4. See [REFERENCE_IMPLEMENTATION.md](REFERENCE_IMPLEMENTATION.md) for implementation guidance.

## Contributing

Contributions follow the IETF intellectual property guidelines. To propose changes:

1. Edit the relevant file in `sections-src/` (not the assembled draft directly).
2. Ensure inline protobuf blocks match the standalone `.proto` files in `proto/`.
3. Submit a pull request with a clear description of the normative impact.

## Authors

- **Kristian Rickert** (PipeStream AI) — <kristian.rickert@pipestream.ai>

## Status

This is an active Internet-Draft targeting IETF standards track. The current revision is **draft-krickert-pipestream-02**.
