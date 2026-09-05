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

    Separate Java libraries implement the Section 9.8 declaration codec,
    durable SQLite membership and closure state, and a public Netty producer.
    A file-backed payload library adds bounded incremental reception and
    immutable retained inputs. Payload installation does not itself admit or
    complete work. A separate executor commits processing and rehydration jobs
    with state transitions, then runs fenced callbacks in bounded workers.
    A separate public SealedServer integrates these libraries with bounded
    Netty ingress, asynchronous execution, pending checkpoint deadlines,
    durable request/ACK replay identity, and recursive completion.
    A small native SQLite extension supplies JDBC file-length enforcement;
    it contains no PipeStream protocol or state-machine code.

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
:   Rust prototype using Quinn and Minicbor. Transport-independent protocol
    logic, Quinn transport, and the runnable server are separate crates.
    It is not feature-complete or fully conformant. Implementations and
    test vectors are non-normative and may require correction against the text.

Maturity:
:   Prototype, publicly available in the `implementations/rust-quinn` directory of this document's source repository.

Coverage:
:   Layer 0 plus Layer 1 recursive scopes, cross-scope parent identity, nested out-of-order completion, SCOPE_DIGEST verification, BARRIER, scoped checkpoints, rehydration, and lineage digests. Its Layer 2 subset provides durable yield, claim checks, cross-connection CLAIM_REDEMPTION, replay refusal, SQLite WAL recovery, and immutable payload storage. TLS 1.3 with ALPN `pipestream/1` is mandatory and 0-RTT is disabled. The original one-entity Layer 0 command remains available for the polyglot interoperability matrix.

    The separate private-use profile in Section 9.8 provides client-owned
    work-set declarations and seals, durable declaration ACK replay,
    non-reused identities, and fixed full-scope completion cuts. It excludes
    Layer 2. The separately negotiated authenticated-session binding in
    Section 10.6.4 adds mutual TLS, certificate-to-principal mapping, durable
    principal/authority ownership, and session-access revocation.

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

As of 2026-09-05, none of these prototypes demonstrates complete Layer 0
conformance. The common command exercises one-entity transfers, not the
entire mandatory manifest and recycling lifecycle. Rust's recursive path
adds independent control/data reception, identity-based stream dispatch,
pending checkpoints with deadlines, negotiated depth enforcement, and
fenced recovery-result publication. Its recursive receiver incrementally
spools payloads to temporary files with byte and file quotas. Application
callbacks consume file-backed readers in bounded asynchronous workers.

The durable Rust prototype now has optional mutual-TLS principal/session
binding, retained authenticated recovery and retained-storage quotas, but lacks
completion-space reservations and explicit orphan reconciliation,
automatic retry scheduling, bidirectional work-set origination, scoped
cursor recycling, and full resilience semantics. Its
Layer 2 advertisement does not identify the narrower implemented subset.
It is unsuitable for untrusted multi-tenant deployment without additional
implementation work. Java's independent sealed producer exercises recursive
work against Rust, and a Rust public-client scenario exercises the Java sealed
server. Persistent producer observations and broader crash/resource evidence
remain unfinished. C++ does not yet provide recursive or resilience evidence.
These limitations remain open;
passing vectors or document checks does not resolve them.

Rust tests exercise Section 9.8 with frozen wire fixtures, reordered
descendants, missing declarations and payloads, immutable seal refusals,
and a public-client reconnect after an unobserved declaration ACK and
server restart. Java-to-Rust QUIC tests additionally cover nested/chunked
completion, scoped checkpoints, replay after restart and a discarded declaration
ACK, and named protocol refusals. Fault-injection peers check Java's rejection
of changed ACKs, downgrade, oversized replies, and Layer 2 frames. Reverse
Rust-to-Java tests cover recursive/chunked completion, reconnect replay, and
changed-owner/checkpoint refusals. These scenarios do not prove the entire
profile. The original Java listener/CLI and C++ endpoints remain Layer 0;
Java's separate SealedServer requires the sealed profile.

Java payload-library tests additionally cover chunk geometry, immutable replay,
file-length and file-count quotas, cancellation-safe accounting, writer
exclusion, corruption, and abrupt exit between installation and admission.
A 32 MiB receive/install/read test runs with a 24 MiB Java heap limit. It does
not exercise QUIC or establish native-memory, RSS, physical filesystem, or
concurrent-workload bounds. No new network interoperability is inferred from
these storage tests.

Java's durable execution tests cover atomic admission and dispatch, bounded
queued jobs and retained descriptors, recursive rehydration, stale executor
refusal after restart, and independent progress during a stalled callback.
Callbacks run outside database transactions and consume verified file-backed
input. Shutdown retains physical ownership until active work returns. These
are local library tests. Separate real-QUIC listener tests hold SQLite writes,
stall a callback, reset input, discard ACK observations, and restart the server.
They check pending deadlines, independent completion, replay, capacity refusal,
STRICT child failure, and rollback of forged scope summaries.
Per-session labels are not
authenticated tenants, and logical record quotas do not bound SQLite/WAL
pages or every future completion allocation.

Authentication tests cover missing, untrusted, expired and unmapped client
certificates; refusal of anonymous downgrade; principal and authority checks;
certificate rotation; live and reconnected session revocation; and background
recovery authorization. These authentication tests do not themselves establish
execution durability or solve ambiguous outcomes after a lost redemption ACK.

The Rust service durably fences process, rehydrate, and resume result
publication and no longer invokes application callbacks under database
transactions. Tests cover simultaneous lease acquisition, expiry, stale
publication after reopen and reacquisition, callback database re-entry, and
revocation during a callback. Storage quotas do not reserve all future completion
space or establish a complete resource guarantee.

The separate Rust-only Section 10.6.5 profile uses authority-qualified request
identities and immutable acceptance receipts with 24-hour retention. Recovery
acceptance commits redemption and a resume job together. Terminal outcomes
explicitly distinguish completion from refusal and echo the complete receipt.
Tests cover owner and authority refusals, expiry, irreversible claim revocation,
concurrent acceptance, queue rollback, abrupt process exit, lost receipt replay
after restart, and retained application refusal without automatic retry. Public
clients reject malformed responses and mismatched receipts or outcomes.
Twenty frozen wire cases and separate CDDL fixtures cover the new frames.
This does not add recovery to the sealed-work profile or establish independent
cross-language recovery interoperability.

The Rust core has typed job descriptors and a transactionally bounded
unfinished-job index with retained outcomes. Storage tests exercise limits,
rollback, interrupted attempts, and index integrity. The transport service
uses this queue for processing, rehydration, and resume operations. Bounded
admission workers install payloads before committing their job descriptors;
execution workers reopen and verify retained input before callbacks. Tests
cover abrupt process exit after admission and detached execution after reopen.
Raw QUIC tests exercise independent completion and deadline progress during
stalled callbacks. Shutdown stops dispatch without falsely releasing physical
capacity still occupied by a callback. Listener cancellation aborts its owned
connection and ingress tasks. Tests also cover pipelined first admission and
checkpoint accounting for received payloads awaiting installation.
Physical permits are shared within one
process, not across independent writer processes. Temporary quotas and
worker counts are not a complete multi-tenant resource guarantee.

Connection metadata and lineage I/O use separately bounded blocking workers.
Checkpoint clocks start at control-frame reception and are enforced independently
of storage completion. Tests hold SQLite writes and lineage persistence while
checking timely refusal, bounded control backlogs, and another connection's
progress. Cancelled waiters do not release still-running storage slots. Ordered
state-dependent dispatch can still wait behind storage; these tests do not
establish disk latency or concurrent-workload performance bounds.

Rust now applies persistent global and authority/principal quotas to serialized
session bytes and retained-session counts, including completed and revoked work.
State, accounting, and job-index changes share a transaction; readers validate
state and accounting in one snapshot. Tests cover concurrent capacity admission,
restart, atomic refusal, missing accounting, bounded serialization, and real-QUIC
declaration replay and rollback at quota limits. This is not a physical database
or payload-file quota, nor a reservation for every future completion record.

A separate Rust guard now bounds main database, WAL, rollback-journal, and
shared-memory file lengths for the bundled SQLite Unix backend. An immutable,
checksummed policy precedes database creation; nonempty unaccounted stores and
policy changes are refused without conversion. Growth is checked at file writes,
truncates, and shared-memory mappings, with preallocation and database mmap
disabled. Tests exhaust each file budget, hold WAL readers, interrupt a process,
and verify transaction rollback and a named capacity refusal over real QUIC.
This is a file-length boundary for cooperating writers in a private directory,
not a filesystem-allocation quota. Completion reservations and orphan
reconciliation remain unfinished.

Java separately enforces main database, WAL, rollback-journal and shared-memory
file lengths through a non-default VFS over Xerial's bundled Unix SQLite engine.
The packaged native extension does not link a second SQLite runtime or share
Rust protocol code. Private bootstrap registration is bounded, and VFS callbacks
remain loaded after bootstrap closure. Ordinary connections cannot manage the
registry or load extensions.
An immutable checksummed policy is synced before database creation. Every store
connection sets a main-page cap, and writes, truncates and shared-memory maps
check growth before delegation. Preallocation and database mmap are disabled.
Existing nonempty stores without policy, changed policies, incompatible backends,
corruption and aliases refuse without conversion. Native file-method and JDBC
tests cover actual file exhaustion, rollback, held WAL readers, registry capacity
and abrupt exit with an uncheckpointed WAL. A real-QUIC test verifies named
capacity refusal, retained membership, and replay after checkpointing and reopen.
The current backend supports private local directories and cooperating writers
on 64-bit Linux. These are file-length limits, not filesystem-allocation quotas,
authenticated principal quotas or future completion-space reservations.

The Rust retained-payload store separately reserves global and authority/principal
bytes and object counts, including lineage, before disk creation. An immutable
checksummed policy survives reopen. Interrupted copies retain staging credit;
incomplete metadata and empty canonical directories remain globally charged.
Matching prefix replay can finish publication without overwriting admitted
input. A verified, synced receipt precedes successful installation. Tests cover
process exit, prefix images, policy and alias refusal, shared handles, and an
exclusive writer lock retained by readers and outstanding I/O. A real-QUIC test
checks named exhaustion, unchanged declared membership and independent principal
progress. These are bounded file-length reservations for a private single-writer
root, not filesystem-allocation, full power-loss or concurrent-tenant performance
proof. No orphan is silently deleted and completion reservations remain due.

Spool tests cover quota exhaustion, file-backed chunk assembly, corruption
before assembly, cancellation-safe disk credit, and abandoned-file accounting.
A real-QUIC 32 MiB transfer measures Rust heap allocations while streaming
input and verifies the persisted payload digest and released temporary credit.
This is not a total process-memory or concurrent-workload performance claim.
Temporary quotas remain separate from retained-storage quotas. Their accounting
does not coordinate independent writer processes; the retained root now refuses
a second cooperating writer process.

The repository's protocol-neutral Rust driver starts each executable as a separate process and tests all nine client/server pairings. The driver has no dependency on a PipeStream implementation and does not encode or decode PipeStream frames. The implementations share the normative specification, CDDL, and golden vector corpus, but no protocol implementation code. The current suite verifies binary and UTF-8 payload transfer, parent identity, status progression, checkpoint acknowledgement, cursor advancement, graceful GOAWAY, and byte-exact delivery. The result is reproducible evidence for the listed protocol subset, not a claim of complete support for every optional field or extension in this document.

The authors welcome reports of additional implementations for inclusion
in future revisions of this appendix.
