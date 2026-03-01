## 7. Entity Model

### 7.1. Core Fields

Every PipeStream entity is represented as a PipeDoc message:

| Field | Type | Requirement | Description |
|-------|------|-------------|-------------|
| doc_id | string | REQUIRED | Unique document identifier (UUID recommended) |
| entity_id | uint32 | REQUIRED | Scope-local identifier |
| ownership | OwnershipContext | OPTIONAL | Multi-tenancy tracking |

### 7.2. Four Data Layers

```
+------------------------------------------------------------------+
|                          PipeDoc                                 |
|  +------------------------------------------------------------+  |
|  |  Layer 0: BlobBag (Raw Binary Data)                        |  |
|  |  - Original document bytes, images, attachments            |  |
|  +------------------------------------------------------------+  |
|  +------------------------------------------------------------+  |
|  |  Layer 1: SemanticLayer (Semantic Chunks)                  |  |
|  |  - Text segments with vector embeddings                     |  |
|  |  - NLP annotations, NER, classifications                    |  |
|  +------------------------------------------------------------+  |
|  +------------------------------------------------------------+  |
|  |  Layer 2: ParsedData (Structured Extraction)               |  |
|  |  - Key-value pairs, tables, structured fields               |  |
|  +------------------------------------------------------------+  |
|  +------------------------------------------------------------+  |
|  |  Layer 3: CustomEntity (Extension Point)                   |  |
|  |  - Domain-specific protobuf via google.protobuf.Any        |  |
|  +------------------------------------------------------------+  |
+------------------------------------------------------------------+
```

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
