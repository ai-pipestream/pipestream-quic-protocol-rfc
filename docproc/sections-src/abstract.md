This document defines an Application Profile for the PipeStream core
protocol, mapping its generic recursive scatter-gather semantics to the
domain of distributed document processing, AI ingestion, and
retrieval-augmented generation (RAG) pipelines.

This profile defines the concrete semantics for the four PipeStream Data
Layers: BlobBag (Layer 0), SemanticLayer (Layer 1), ParsedData (Layer
2), and CustomEntity (Layer 3). It also specifies the PipeDoc
application-level envelope, ownership contexts for multi-tenant
environments, and standardized SINK actions for search engine indexing,
storage persistence, and event notification.
