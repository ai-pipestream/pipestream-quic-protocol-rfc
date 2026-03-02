# Entity Model

## Core Fields

Every PipeStream entity is represented as a PipeDoc message:

| Field | Type | Requirement | Description |
|-------|------|-------------|-------------|
| doc_id | string | REQUIRED | Unique document identifier (UUID recommended) |
| entity_id | uint32 | REQUIRED | Scope-local identifier |
| ownership | OwnershipContext | OPTIONAL | Multi-tenancy tracking |

## Four Data Layers

Each PipeDoc carries entity payload in one of four data layers:

| Layer | Name | Content |
|-------|------|---------|
| 0 | BlobBag | Raw binary data: original document bytes, images, attachments |
| 1 | SemanticLayer | Annotated content: text segments with vector embeddings, NLP annotations, NER, classifications |
| 2 | ParsedData | Structured extraction: key-value pairs, tables, structured fields |
| 3 | CustomEntity | Extension point: domain-specific extension types |

## Cloud-Agnostic Storage Reference

~~~~ cddl
file-storage-reference = {
  provider: tstr,                ; Storage provider identifier
  bucket: tstr,                  ; Bucket/container name
  key: tstr,                     ; Object key/path
  ? region: tstr,                ; Optional region hint
  ? attrs: { * tstr => tstr },   ; Provider-specific attributes
  ? encryption: encryption-metadata,
}

encryption-metadata = {
  algorithm: tstr,               ; "AES-256-GCM", "AES-256-CBC"
  ? key-provider: tstr,          ; "aws-kms", "azure-keyvault",
                                 ; "gcp-kms", "vault"
  ? key-id: tstr,                ; Key ARN/URI/ID
  ? wrapped-key: bstr,           ; Client-side encrypted DEK
  ? iv: bstr,                    ; Initialization vector
  ? context: { * tstr => tstr }, ; Encryption context
}
~~~~
