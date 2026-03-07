# PipeDoc Entity Envelope

## Core Fields

This profile defines PipeDoc as the application-level envelope carried
within document-processing entity payloads.

| Field | Type | Requirement | Description |
|-------|------|-------------|-------------|
| `doc_id` | string | REQUIRED | Stable document identifier for the pipeline run or deduplicated source object |
| `entity_id` | uint32 | REQUIRED | Profile-visible copy of the enclosing PipeStream Entity ID |
| `ownership` | OwnershipContext | OPTIONAL | Multi-tenant ownership and access metadata |

The `entity_id` in PipeDoc MUST match the `entity-id` value in the
enclosing PipeStream Entity Header.

PipeDoc MAY carry additional document metadata and layer-specific
payload structures as defined in Appendix A and Appendix B.

## OwnershipContext

OwnershipContext provides application-layer multi-tenancy and
authorization metadata for document-processing deployments. It is not
interpreted by PipeStream Core; it is consumed only by implementations
of this profile and related applications.

| Field | Type | Requirement | Description |
|-------|------|-------------|-------------|
| `tenant_id` | string | OPTIONAL | Administrative tenant or account boundary |
| `owner_id` | string | OPTIONAL | Individual owner or service principal |
| `acl` | repeated string | OPTIONAL | Access control principals allowed to access the document |

Implementations MAY omit OwnershipContext in single-tenant or otherwise
trusted environments.
