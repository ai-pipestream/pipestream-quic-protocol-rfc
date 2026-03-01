## 7. Entity Model

### 7.1. Core Fields

Every PipeStream entity is represented as a PipeDoc message:

| Field | Type | Requirement | Description |
|-------|------|-------------|-------------|
| doc_id | string | REQUIRED | Unique document identifier (UUID recommended) |
| entity_id | uint32 | REQUIRED | Scope-local identifier |
| ownership | OwnershipContext | OPTIONAL | Multi-tenancy tracking |

### 7.2. Four Data Layers

Each PipeDoc carries entity payload in one of four data layers:

| Layer | Name | Content |
|-------|------|---------|
| 0 | BlobBag | Raw binary data: original document bytes, images, attachments |
| 1 | SemanticLayer | Annotated content: text segments with vector embeddings, NLP annotations, NER, classifications |
| 2 | ParsedData | Structured extraction: key-value pairs, tables, structured fields |
| 3 | CustomEntity | Extension point: domain-specific protobuf via `google.protobuf.Any` |

### 7.3. Cloud-Agnostic Storage Reference

```protobuf
message FileStorageReference {
  string provider = 1;           // Storage provider identifier
  string bucket = 2;             // Bucket/container name
  string key = 3;                // Object key/path
  string region = 4;             // Optional region hint
  map<string, string> attrs = 5; // Provider-specific attributes
  EncryptionMetadata encryption = 6;
}

message EncryptionMetadata {
  string algorithm = 1;          // "AES-256-GCM", "AES-256-CBC"
  string key_provider = 2;       // "aws-kms", "azure-keyvault", "gcp-kms", "vault"
  string key_id = 3;             // Key ARN/URI/ID
  bytes wrapped_key = 4;         // Optional: client-side encrypted DEK
  bytes iv = 5;                  // Initialization vector
  map<string, string> context = 6; // Encryption context
}
```
