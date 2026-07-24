# PipeDoc Entity Envelope

## Core Fields

This profile defines PipeDoc as the application-level envelope carried
within document-processing entity payloads.

| Field | Type | Requirement | Description |
|-------|------|-------------|-------------|
| `profile-version` | uint | REQUIRED | Version of this document-processing profile used to encode the envelope |
| `doc-id` | tstr | REQUIRED | Stable document identifier for the pipeline run or deduplicated source object |
| `entity-id` | uint | REQUIRED | Profile-visible copy of the enclosing PipeStream Entity ID for archival and off-transport correlation |
| `ownership` | ownership-context | OPTIONAL | Multi-tenant ownership and access metadata |

Field names and types follow the CDDL schema in Appendix A.

The `entity-id` field in PipeDoc MUST match the `entity-id` value in the
enclosing PipeStream Entity Header. This field is retained so that
archived payloads, detached artifacts, and reprocessed document
representations can be correlated after transport headers have been
stripped or normalized away.

PipeDoc MAY carry additional document metadata and layer-specific
payload structures as defined in Appendix A.

The `profile-version` field MUST be set to `1` for payloads conforming
to this document.

## OwnershipContext

OwnershipContext provides application-layer multi-tenancy and
authorization metadata for document-processing deployments. It is not
interpreted by PipeStream Core; it is consumed only by implementations
of this profile and related applications.

| Field | Type | Requirement | Description |
|-------|------|-------------|-------------|
| `tenant-id` | tstr | OPTIONAL | Administrative tenant or account boundary |
| `owner-id` | tstr | OPTIONAL | Individual owner or service principal |
| `acl` | [* tstr] | OPTIONAL | Access control principals allowed to access the document |

Implementations MAY omit OwnershipContext in single-tenant or otherwise
trusted environments.
