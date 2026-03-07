# PipeDoc Entity Envelope

## Core Fields

This profile defines PipeDoc as the application-level envelope carried
within document-processing entity payloads.

| Field | Type | Requirement | Description |
|-------|------|-------------|-------------|
| `profile_version` | uint32 | REQUIRED | Version of this document-processing profile used to encode the envelope |
| `doc_id` | string | REQUIRED | Stable document identifier for the pipeline run or deduplicated source object |
| `entity_id` | uint32 | REQUIRED | Profile-visible copy of the enclosing PipeStream Entity ID for archival and off-transport correlation |
| `ownership` | OwnershipContext | OPTIONAL | Multi-tenant ownership and access metadata |

The `entity_id` in PipeDoc MUST match the `entity-id` value in the
enclosing PipeStream Entity Header. This field is retained so that
archived payloads, detached artifacts, and reprocessed document
representations can be correlated after transport headers have been
stripped or normalized away.

PipeDoc MAY carry additional document metadata and layer-specific
payload structures as defined in Appendix A and Appendix B.

The `profile_version` field MUST be set to `1` for payloads conforming
to this document.

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
