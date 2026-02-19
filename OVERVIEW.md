# PipeStream: Recursive Entity Streaming Protocol

PipeStream is a high-performance application-layer protocol designed for the distributed decomposition and reassembly of complex data structures. Built on **QUIC [RFC 9000]**, it treats data not as a flat stream of bytes, but as a recursive tree of **Entities**.

## 1. The Core Innovation: Vaporization & Rejoin
Unlike standard streaming protocols that focus on 1:1 delivery, PipeStream formalizes the **1:N (Vaporize)** and **N:1 (Rejoin)** pattern:
*   **Vaporization:** A node takes an Entity (e.g., a PDF) and decomposes it into sub-entities (e.g., Pages, Images).
*   **Rejoining:** The protocol guarantees that a parent Entity is only marked "Complete" when its recursive child tree satisfies a defined **Completion Policy**.

## 2. Dual-Stream Architecture
PipeStream maximizes efficiency by separating the **Control Plane** from the **Data Plane**:

### A. The Ledger (Stream 0)
*   **Format:** Bit-packed, 32-bit word-aligned frames.
*   **Purpose:** High-frequency status updates (PENDING, PROCESSING, COMPLETE).
*   **Efficiency:** Designed for hardware-level processing and minimal parsing overhead.

### B. Entity Streams (Streams 2+)
*   **Format:** Protobuf-encoded **EntityHeader** followed by raw binary payload.
*   **Purpose:** Carrying the actual content (BlobBag, SemanticLayer, etc.).
*   **Isolation:** Each Entity gets its own QUIC stream, eliminating head-of-line blocking between unrelated parts of a document.

## 3. Hierarchical Data Model
Entities flow through four distinct layers, allowing processors to operate at the appropriate level of abstraction:
*   **Layer 0 (BlobBag):** Raw binary octets and storage references.
*   **Layer 1 (Semantic):** Text chunks, vector embeddings, and NLP metadata.
*   **Layer 2 (Parsed):** Structured key-value pairs and tables.
*   **Layer 3 (Custom):** Domain-specific application extensions.

## 4. Consistency & Resilience
*   **Checkpoint Blocking:** Ensures deterministic boundaries across parallel workers.
*   **ID Recycling:** A 20-bit sliding window (TCP-style) allows for millions of entities per session with minimal memory footprint.
*   **Layer 2 Resilience:** Native support for **Yielding** (pausing for external dependencies) and **Claim Checks** (asynchronous deferred processing).

---

*This document provides a high-level technical summary of the PipeStream protocol architecture.*
