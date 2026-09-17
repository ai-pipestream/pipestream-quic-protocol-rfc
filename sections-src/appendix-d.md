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


## Summary as of 2026-09-16

Three implementations live in the `implementations/` directory of this
document's source repository, each with an independent codec and state
machine and no shared protocol code.

- Java/Netty and Rust/Quinn implement version 2 (Section 12) completely:
  a processing authority with durable stores, execution, retention and result
  delivery; a durable client with an exclusive journal; and a command-line
  launcher. Callers authenticate with mutual TLS. A protocol-neutral driver
  (the `durable` command of the conformance crate, which depends on no
  PipeStream implementation) certified the two against each other in both
  client and server roles on a 66-row failure and resource matrix: 64 rows
  pass and 2 are waived for named missing fixture capabilities (no fixture
  clock, no cleanup boundary), none fail. The matrix injects dropped replies,
  process kills at recorded boundaries and disconnects through a fixture
  schedule both subjects honour. The archive, the driver's handoff and a
  clause-level traceability of Section 12 to Java code and tests are in the
  repository (`conformance/results/`, `docs/standards/`).
- All three implement documented version-1 Layer 0 subsets and interoperate
  on one-entity transfers through a common command in every client/server
  pairing. Rust additionally implements Layer 1, a Layer 2 subset and the
  three version-1 profiles; Java implements the sealed and authenticated
  profiles in separate libraries and both directions against Rust are tested.
  C++/MsQuic is Layer 0 only.
- Two applications run on version 2: a transform workload measured against a
  gRPC arm on loopback, with fault suites, and a multi-stage index build using
  descendant scopes, cross-authority result references, scope cancellation and
  kill/resume.

The dated notes that recorded how this state was reached are kept in the
repository as `docs/standards/implementation-status-notebook.md`.

## Java/Netty Reference Implementation

Organization:
:   PipeStream AI

Description:
:   Java 21 with Netty QUIC (a source-built, patched QUIC transport
    extension) and deterministic CBOR. Reusable library plus a standalone
    client and server JAR. Authority state is SQLite with a bounded object
    directory; a small native SQLite extension enforces file-length bounds
    and contains no protocol code.

Maturity:
:   Version 2: complete and certified as described in the summary.
    Version 1: Layer 0 endpoint plus sealed-profile libraries.

Coverage:
:   Version 2 Core, durable work and result delivery in both roles, including
    authorization, restart and replay, retention, refusals, connection ceilings
    and draining. Version 1: TLS 1.3 with ALPN `pipestream/1`, no 0-RTT,
    deterministic CBOR control messages, STATUS, CHECKPOINT and GOAWAY; the
    Section 9.8 sealed profile and the Section 10.6.4 authenticated binding
    in separate libraries.

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
:   Rust with Quinn and Minicbor. Transport-independent protocol logic, the
    Quinn transport, the runnable server and the neutral conformance driver
    are separate crates; the driver depends on none of the others.

Maturity:
:   Version 2: complete and certified as described in the summary.
    Version 1: Layer 0, Layer 1 and a Layer 2 subset with the three profiles.

Coverage:
:   Version 2 Core, durable work and result delivery in both roles. Version 1:
    recursive scopes, scope digests, barriers, scoped checkpoints, sealed work
    sets, authenticated session binding and retained authenticated recovery.
    Its Layer 2 advertisement does not name the narrower implemented subset
    (Appendix E).

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
:   C++20 with Microsoft MsQuic and a hand-written deterministic CBOR codec.
    Reusable wire and transport libraries plus a standalone client and server.

Maturity:
:   Version 1 Layer 0 prototype. No version 2.

Coverage:
:   TLS 1.3 with ALPN `pipestream/1`, no 0-RTT, deterministic CBOR control
    messages, STATUS, CHECKPOINT and GOAWAY, one-entity transfers.

Licensing:
:   MIT.

Implementation:
:   `https://github.com/ai-pipestream/pipestream-quic-protocol-rfc/tree/main/implementations/cpp-msquic`

Contact:
:   Kristian Rickert (kristian.rickert@pipestream.ai)

## Interoperability Evidence and Its Limits

Version 2 evidence is the neutral matrix described in the summary, run on
the subjects the repository ships, plus the two applications. Version 1
evidence is the nine client/server pairings on Layer 0 one-entity transfers
and the Java/Rust sealed and authenticated scenarios in both directions.

What this evidence does not show, so that no reader infers it:

- No implementation claims complete version-1 Layer 0 conformance beyond the
  documented subsets; the common command does not exercise the entire
  manifest and cursor-recycling lifecycle.
- The two waived matrix rows (clock-unsafe refusal, cleanup-interrupted
  refund) have no evidence on either subject until the fixture interface
  gains those boundaries.
- All measurements are loopback on one host; there is no WAN, multi-host or
  deployment-scale evidence, and the implementations are not hardened for
  untrusted multi-tenant use.
- Passing the frozen vectors or the document checks resolves none of the
  open questions in Appendix E.
