# PipeStream QUIC Protocol RFC

**Internet-Draft: draft-krickert-pipestream-01**

PipeStream is a recursive entity streaming protocol for distributed document processing over QUIC transport.

## Overview

PipeStream enables the decomposition ("vaporization") of documents into constituent entities, their transmission across distributed processing nodes, and subsequent reassembly ("rejoining") at destination endpoints.

### Protocol Layers

| Layer | Name | Description |
|-------|------|-------------|
| 0 | Core | Basic streaming with vaporize/rejoin semantics. MUST be supported. |
| 1 | Recursive | Hierarchical scoping, scope digests, Merkle propagation. MAY be supported. |
| 2 | Resilience | Yield/resume, claim checks, completion policies. MAY be supported. |

### Key Features

- **Dual-stream architecture**: Ledger stream for status tracking + Entity streams for payload
- **Built on QUIC** (RFC 9000) with TLS 1.3
- **32-bit word-aligned frames** for efficient parsing
- **Four data layers**: BlobBag, SemanticLayer, ParsedData, CustomEntity
- **Cursor-based Entity ID recycling** (TCP sliding window pattern)
- **Cloud-agnostic storage**: Supports standard object storage providers
- **Encryption key abstraction**: Master-key envelope encryption with provider abstraction

## Repository Structure

```
.
├── draft-krickert-pipestream-00.md   # Original draft
├── draft-krickert-pipestream-01.md   # Current draft (with protocol layering)
├── proto/                            # Protobuf schemas (buf-lint compliant)
│   ├── buf.yaml                      # Buf configuration (STANDARD + COMMENTS)
│   └── pipestream/
│       ├── protocol/v1/              # Protocol-level messages
│       │   └── pipestream_protocol.proto
│       └── data/v1/                  # Entity data messages
│           └── pipestream_data.proto
└── sections/                         # Individual spec sections
    ├── 01-abstract-intro-terminology.md
    ├── 03-protocol-overview.md
    ├── 04-05-quic-frames.md
    ├── 06-entity-model.md
    ├── 07-processing-actions.md
    ├── 08-reassembly-semantics.md
    ├── 09-10-security-iana.md
    └── appendix-a-protobuf.md
```

## Protobuf Linting

Protobuf schemas are linted with [buf](https://buf.build/) using the strictest rules:

```bash
cd proto
buf lint    # STANDARD + COMMENTS rules
buf build   # Verify compilation
```

## References

- [QUIC: A UDP-Based Multiplexed and Secure Transport (RFC 9000)](https://www.rfc-editor.org/rfc/rfc9000)
- [Using TLS to Secure QUIC (RFC 9001)](https://www.rfc-editor.org/rfc/rfc9001)
- [QUIC Loss Detection and Congestion Control (RFC 9002)](https://www.rfc-editor.org/rfc/rfc9002)

## Status

This is an active Internet-Draft. See `draft-krickert-pipestream-01.md` for the current specification.
