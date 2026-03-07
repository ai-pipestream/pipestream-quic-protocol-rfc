# Data Layer Definitions

## Layer 0: BlobBag

BlobBag is the Layer 0 representation for raw binary document data. It
holds one or more blobs that together represent the original source
material entering the pipeline, such as PDFs, images, office
attachments, or archive members.

Each Blob MAY embed bytes inline or MAY reference externally stored data
via `FileStorageReference`. Blob metadata MAY include MIME type,
filename, size, and checksum information.

## Layer 1: SemanticLayer

SemanticLayer is the Layer 1 representation for annotated content
produced by semantic processing stages. Typical contents include:

- chunked text segments
- dense vector embeddings
- named-entity annotations
- model metadata and chunking strategy metadata

This layer is intended for enrichment stages that transform raw bytes
into semantically meaningful segments while preserving enough context
for downstream search, retrieval, and ranking systems.

## Layer 2: ParsedData

ParsedData is the Layer 2 representation for structured extraction
output. Typical contents include:

- extracted key-value fields
- normalized metadata attributes
- extracted tables
- parser-specific raw textual output

This layer is intended for downstream systems that require normalized
records rather than raw documents or semantic chunks.

## Layer 3: CustomEntity

CustomEntity is the Layer 3 representation for application-specific
extensions. This profile reserves Layer 3 for payloads that build on the
document-processing model but require data structures not standardized
by this document.

Receivers that implement this profile but do not understand a Layer 3
payload MAY pass it through unchanged, provided that PipeStream Core
processing requirements are still met.
