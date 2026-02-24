# PipeStream: Recursive Entity Streaming Protocol

PipeStream is a high-performance application-layer protocol designed for the distributed dematerialization, processing, and rematerialization of complex data structures. Built on **QUIC [RFC 9000]**, it treats data as a recursive tree of **Entities** that flow through a stateless mesh.

## 1. The Core Innovation: Dematerialize & Rematerialize
PipeStream formalizes the transition of data between "Solid" (stored records) and "Fluid" (streaming) states:
*   **Dematerialization (Vaporize):** A node takes a solid Entity (e.g., a PDF) and decomposes it into a cloud of sub-entities (e.g., Pages, Images, Metadata).
*   **Rematerialization (Rejoin):** The protocol converges these "floating" entities back into a new, solid Record. This outcome is typically versioned, hashed, and issued a permanent identifier (e.g., DOI).

## 2. Stateless Mesh Architecture (The Baton-Passing Model)
Unlike traditional pipelines that rely on a central orchestrator, PipeStream is designed for a peer-to-peer mesh:
*   **Mobile Ledger:** The state of an entity (its "Parts Ledger") is passed between nodes like a baton. Each node can "decorate" the entity with new data layers (AI analysis, embeddings) without needing to contact a master server.
*   **Convergence Points:** Any node in the mesh can act as a convergence point. When a node detects that all constituent parts of a parent ID have reached a terminal state, it automatically initiates the **Rematerialization** process.
*   **Statelessness:** If a node yields processing (Layer 2), the entity and its continuation token can be saved to any storage sink and "re-materialized" later by any other node in the mesh.

## 3. Dual-Stream Interpretation
PipeStream separates the **Control Plane** from the **Data Plane** at the stream level:
*   **The Ledger (Stream 0):** Bit-packed, 32-bit word-aligned frames for high-frequency status coordination. It acts as the "Header" for the entire document-process.
*   **Entity Streams (Streams 2+):** Protobuf-encoded **EntityHeaders** followed by raw binary payloads. This ensures data isolation and prevents head-of-line blocking.

## 4. Hierarchy & AI Data Model
Entities flow through four standard layers, specifically optimized for AI ingestion:
*   **Layer 0 (BlobBag):** Original binary octets and storage references.
*   **Layer 1 (Semantic):** Text chunks, vector embeddings, and NLP annotations.
*   **Layer 2 (Parsed):** Structured extracted data (tables, key-values).
*   **Layer 3 (Custom):** Domain-specific extensions.

## 5. Provenance & Long-Term Storage
Every completed PipeStream rematerialization results in a **Permanent Record**:
*   **Versioning:** Incremental updates create a lineage of document states.
*   **Integrity:** SHA-256 hashes are calculated for every entity and the final aggregate result.
*   **DOI Integration:** Designed to support formal reference identifiers for long-term archival.

---

*This document provides a high-level technical summary of the PipeStream protocol architecture.*
