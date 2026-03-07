# Introduction

## Purpose

This document defines an Application Profile for PipeStream
{{PIPESTREAM}} that specifies entity payload formats and processing
semantics for distributed document processing pipelines. This profile
assigns concrete meanings to PipeStream's four data layers, defines the
PipeDoc application-level entity envelope, and specifies interoperable
payload conventions for document ingestion, enrichment, and indexing
workflows.

## Relationship to PipeStream Core

PipeStream Core defines the transport mapping, control stream framing,
recursive entity lifecycle, and resilience semantics. This profile does
not modify any PipeStream Core wire format. Instead, it defines how
document-processing implementations interpret the payload bytes carried
within PipeStream entities.

This document is intended as an independent industry profile rather than
as a standards-track extension to PipeStream Core. It can evolve on a
faster cadence than the core transport specification while preserving
wire compatibility with PipeStream entities and control frames.

Implementations of this profile MUST implement PipeStream Core
{{PIPESTREAM}} Layer 0 at minimum. Implementations that require
recursive document decomposition, such as processing embedded documents
within archives, SHOULD implement Layer 1. Implementations that interact
with external services, such as third-party NLP APIs or human review
workflows, SHOULD implement Layer 2.

## Terminology

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT",
"SHOULD", "SHOULD NOT", "RECOMMENDED", "NOT RECOMMENDED", "MAY", and
"OPTIONAL" in this document are to be interpreted as described in
BCP 14 {{RFC2119}} {{RFC8174}} when, and only when, they appear in all
capitals, as shown here.

This profile uses all capitalized PipeStream Core terms as defined in
{{PIPESTREAM}}. In addition, the following profile terms are used:

**PipeDoc**
:   The application-level document envelope carried within an entity
    payload. PipeDoc provides a stable document identifier and ownership
    context for document-processing pipelines.

**Profile Version**
:   A profile-level schema version carried within PipeDoc. It identifies
    which revision of this document defined the payload layout for a
    given document-processing entity.

**BlobBag**
:   The Layer 0 representation for raw binary document content and
    related attachments.

**SemanticLayer**
:   The Layer 1 representation for annotated content, chunking output,
    embeddings, and NLP annotations.

**ParsedData**
:   The Layer 2 representation for structured extracted metadata,
    including fields and tables.

**CustomEntity**
:   The Layer 3 representation for application-specific extensions to
    this profile.
