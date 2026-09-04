# Implementation Status

**RFC Editor Note:** Please remove this entire appendix, and the reference to
{{RFC7942}}, before publication.

This appendix records the status of known implementations of the
protocol defined by this specification at the time of posting of this
Internet-Draft, following the process described in {{RFC7942}}. The
description of implementations in this appendix is intended to assist
the IETF in its decision processes in progressing drafts to RFCs.
Please note that the listing of any individual implementation here does
not imply endorsement by the IETF. Furthermore, no effort has been
spent to verify the information presented here that was supplied by
IETF contributors. This is not intended as, and must not be construed
to be, a catalog of available implementations or their features.
Readers are advised to note that other implementations may exist.

## Java/Netty Reference Implementation

Organization:
:   PipeStream AI

Description:
:   Java 21 implementation using Netty QUIC for transport and Jackson CBOR for an independently implemented Layer 0 codec. It is available as a reusable Java library and a standalone client/server executable.

Maturity:
:   Prototype, publicly available in the `implementations/java-netty` directory of this document's source repository.

Coverage:
:   TLS 1.3 with ALPN `pipestream/1`; no 0-RTT; deterministic CBOR Capabilities, EntityHeader, and Checkpoint messages; STATUS heartbeat and entity progression; cursor advancement; parent identity; SHA-256 payload validation; checkpoint request/acknowledgement; and GOAWAY. The standalone command handles one entity per connection and does not implement Layers 1 or 2.

Licensing:
:   MIT.

Implementation:
:   `https://github.com/ai-pipestream/pipestream-quic-protocol-rfc/tree/main/implementations/java-netty`

Contact:
:   Kristian Rickert (kristian.rickert@pipestream.ai)

## Rust/Quinn Reference Implementation

Organization:
:   PipeStream AI

Description:
:   Rust feature-complete exemplar using Quinn and Minicbor. Transport-independent protocol logic, Quinn transport, and the runnable server are separate crates. The implementation remains non-normative and is implemented independently from the Java and C++ implementations.

Maturity:
:   Prototype, publicly available in the `implementations/rust-quinn` directory of this document's source repository.

Coverage:
:   Layer 0 plus Layer 1 recursive scopes, cross-scope parent identity, nested out-of-order completion, SCOPE_DIGEST verification, BARRIER, scoped checkpoints, rehydration, and lineage digests. Its Layer 2 subset provides durable yield, claim checks, cross-connection CLAIM_REDEMPTION, replay refusal, SQLite WAL recovery, and immutable payload storage. TLS 1.3 with ALPN `pipestream/1` is mandatory and 0-RTT is disabled. The original one-entity Layer 0 command remains available for the polyglot interoperability matrix.

Licensing:
:   MIT.

Implementation:
:   `https://github.com/ai-pipestream/pipestream-quic-protocol-rfc/tree/main/implementations/rust-quinn`

Contact:
:   Kristian Rickert (kristian.rickert@pipestream.ai)

## C++/MsQuic Reference Implementation

Organization:
:   PipeStream AI

Description:
:   C++20 implementation using Microsoft MsQuic and a manually implemented deterministic CBOR codec. It contains reusable wire and transport libraries and a standalone client/server executable. It does not share protocol implementation code with the Java or Rust implementations.

Maturity:
:   Prototype, publicly available in the `implementations/cpp-msquic` directory of this document's source repository.

Coverage:
:   TLS 1.3 with ALPN `pipestream/1`; no 0-RTT; deterministic CBOR Capabilities, EntityHeader, and Checkpoint messages; STATUS heartbeat and entity progression; cursor advancement; parent identity; SHA-256 payload validation; checkpoint request/acknowledgement; and GOAWAY. The standalone command handles one entity per connection and does not implement Layers 1 or 2.

Licensing:
:   MIT.

Implementation:
:   `https://github.com/ai-pipestream/pipestream-quic-protocol-rfc/tree/main/implementations/cpp-msquic`

Contact:
:   Kristian Rickert (kristian.rickert@pipestream.ai)

## Interoperability Evidence

The repository's protocol-neutral Rust driver starts each executable as a separate process and tests all nine client/server pairings. The driver has no dependency on a PipeStream implementation and does not encode or decode PipeStream frames. The implementations share the normative specification, CDDL, and golden vector corpus, but no protocol implementation code. The current suite verifies binary and UTF-8 payload transfer, parent identity, status progression, checkpoint acknowledgement, cursor advancement, graceful GOAWAY, and byte-exact delivery. The result is reproducible evidence for the listed protocol subset, not a claim of complete support for every optional field or extension in this document.

The authors welcome reports of additional implementations for inclusion
in future revisions of this appendix.
