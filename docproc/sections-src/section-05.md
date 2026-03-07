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
