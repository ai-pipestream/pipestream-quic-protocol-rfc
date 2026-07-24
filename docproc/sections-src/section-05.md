# Security Considerations

## PII in Document Metadata

This profile commonly carries document metadata that may contain
personally identifiable information or commercially sensitive content.
Implementations SHOULD minimize exposure of titles, filenames, semantic
annotations, and extracted fields to only those processing stages that
require them.

When documents are routed through multi-tenant infrastructure,
implementations SHOULD encrypt sensitive application-level metadata at
rest and SHOULD avoid exposing unnecessary identifiers in operational
logs.

## Multi-Tenancy via OwnershipContext

OwnershipContext is an application-layer construct and therefore is not
protected by PipeStream Core semantics beyond QUIC transport security.
Implementations that rely on OwnershipContext for authorization MUST
treat it as security-sensitive metadata and validate it against local
policy before granting access to document payloads or derived results.

## Malicious Content and Resource Exhaustion

Document-processing pipelines routinely ingest untrusted payloads.
Implementations SHOULD defend against malformed archives, zip bombs,
polyglot files, decompression attacks, path traversal in embedded
filenames, and parser-specific exploit inputs. BlobBag processing SHOULD
apply bounded resource policies for maximum object size, expansion
ratios, page counts, recursion depth, and extraction time.

## Metadata and Structured Content Injection

Titles, filenames, annotations, extracted fields, and table content MAY
contain attacker-controlled strings. Implementations SHOULD treat these
values as untrusted input when rendering search results, constructing
queries, invoking downstream tools, or generating prompts for language
models.

## Cross-Tenant Trust Boundaries

Shared indexing, storage, or enrichment infrastructure can create
cross-tenant leakage risks if document-processing metadata is reused
outside its intended scope. Implementations SHOULD isolate tenant data
paths, avoid sharing authorization context across tenants, and ensure
that cached semantic or structured outputs are keyed by both `doc-id`
and the relevant ownership boundary.
