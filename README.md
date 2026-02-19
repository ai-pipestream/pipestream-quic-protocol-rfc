# PipeStream QUIC Protocol RFC

**draft-krickert-pipestream** - A Recursive Entity Streaming Protocol for Distributed Document Processing over QUIC.

## Overview

PipeStream defines a protocol for decomposing ("vaporizing") documents into constituent entities, transmitting them across distributed processing nodes over QUIC transport, and reassembling ("rejoining") them at destination endpoints.

### Protocol Layers

| Layer | Name | Description |
|-------|------|-------------|
| 0 | Core | Basic entity streaming with vaporize/rejoin semantics |
| 1 | Recursive | Hierarchical scoping, scope digests, Merkle propagation |
| 2 | Resilience | Yield/resume, claim checks, completion policies |

Implementations MUST support Layer 0 and MAY support Layers 1 and 2.

### Data Layers

| Layer | Name | Description |
|-------|------|-------------|
| 0 | BlobBag | Raw binary data with cloud storage references |
| 1 | SemanticLayer | Chunked content with vector embeddings and NLP annotations |
| 2 | ParsedData | Structured extracted metadata |
| 3 | CustomEntity | Application-specific extensions |

## Repository Structure

```
.
├── draft-krickert-pipestream-00.md   # Initial draft
├── draft-krickert-pipestream-01.md   # Current draft (protocol layers, scoping, resilience)
├── proto/                            # Protobuf definitions (buf-managed)
│   ├── buf.yaml                      # buf configuration (STANDARD + COMMENTS)
│   └── pipestream/
│       ├── protocol/v1/              # Wire-format protocol messages
│       │   └── pipestream_protocol.proto
│       └── data/v1/                  # Entity data model messages
│           └── pipestream_data.proto
└── sections/                         # Expanded section documents
    ├── 01-abstract-intro-terminology.md
    ├── 03-protocol-overview.md
    ├── 04-05-quic-frames.md
    ├── 06-entity-model.md
    ├── 07-processing-actions.md
    ├── 08-reassembly-semantics.md
    ├── 09-10-security-iana.md
    └── appendix-a-protobuf.md
```

## Protobuf

Proto definitions are linted with [buf](https://buf.build) using the strictest configuration (STANDARD + COMMENTS rules, `_UNSPECIFIED` zero values, FILE breaking change detection).

```bash
# Lint
cd proto && buf lint

# Build
cd proto && buf build
```

## Key Design Decisions

- **QUIC Transport**: Leverages QUIC (RFC 9000) multiplexed streams with TLS 1.3 built-in
- **Dual-Stream Architecture**: Ledger stream (Stream 0) for status tracking; entity streams (Stream 2+) for payloads
- **32-bit Word-Aligned Ledger Frames**: E(1) + C(1) + EntityID(20) + Stat(4) + Flags(6) = 32 bits
- **Cursor-Based Entity ID Recycling**: 20-bit circular buffer with sliding window (TCP-inspired)
- **Cloud-Agnostic Storage**: `FileStorageReference` supports S3, Azure Blob, GCS, MinIO
- **Encryption Abstraction**: `EncryptionMetadata` supports AWS KMS, Azure KeyVault, GCP KMS, HashiCorp Vault

## References

- [RFC 9000 - QUIC: A UDP-Based Multiplexed and Secure Transport](https://www.rfc-editor.org/rfc/rfc9000)
- [RFC 9001 - Using TLS to Secure QUIC](https://www.rfc-editor.org/rfc/rfc9001)
- [RFC 2119 - Key words for use in RFCs](https://www.rfc-editor.org/rfc/rfc2119)

## License

This Internet-Draft is submitted in full conformance with the provisions of BCP 78 and BCP 79.
