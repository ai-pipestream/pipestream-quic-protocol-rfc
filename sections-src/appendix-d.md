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

The implementations below implement documented version-1 subsets. They do not
yet implement or advertise the version-2 profiles in Section 12. Independent
Rust abstract models explore durable attempts/results/retention and sealed
scope closure. A third bounded model composes a branch and leaf with attempts,
worker epochs, ancestor cancellation, output read/dependency pins and closure.
These are design evidence, not wire interoperability, real storage
crash-consistency evidence or an unbounded composition proof.

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

As of 2026-09-06, none of these prototypes demonstrates complete Layer 0
conformance. The common command exercises one-entity transfers, not the
entire mandatory manifest and recycling lifecycle. Rust's recursive path
adds independent control/data reception, identity-based stream dispatch,
pending checkpoints with deadlines, negotiated depth enforcement, and
fenced recovery-result publication. Its recursive receiver incrementally
spools payloads to temporary files with byte and file quotas. Application
callbacks consume file-backed readers in bounded asynchronous workers.

The durable Rust prototype now has optional mutual-TLS principal/session
binding, retained authenticated recovery, retained-storage quotas and admitted-job
SQLite completion reservations and explicit offline orphan reconciliation,
but lacks automatic retry scheduling, bidirectional work-set origination, scoped
cursor recycling, and full resilience semantics. Its
Layer 2 advertisement does not identify the narrower implemented subset.
It is unsuitable for untrusted multi-tenant deployment without additional
implementation work. Java's independent sealed producer exercises recursive
work against Rust, and a Rust public-client scenario exercises the Java sealed
server. Persistent Java producer observations are implemented and tested
across restart. Broader crash/resource and profile-conformance evidence remains
incomplete. C++ does not yet provide recursive or resilience evidence.
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
Java's separate SealedServer requires the sealed profile. It does not yet
authenticate a client principal or implement the authenticated-session and
authenticated-recovery profiles. Its server-authenticated TLS lifecycle
fixtures are not evidence of compliance with Section 10.6.1's requirement
to authenticate and authorize callers before durable work admission.

Java payload-library tests additionally cover chunk geometry, immutable replay,
file-length and file-count quotas, cancellation-safe accounting, writer
exclusion, corruption, and abrupt exit between installation and admission.
A 32 MiB receive/install/read test runs with a 24 MiB Java heap limit.
Separate executor and actual-QUIC 32 MiB tests now run under the same Java
heap limit. Neither set establishes native-memory, RSS, physical filesystem,
or concurrent-workload bounds; library storage tests alone are not network
interoperability evidence.

Java's managed execution path now binds its database and payload root using
persistent store identities. Admission revalidates retained input and keeps the
store open through the transaction. Closed or foreign-store input handles are
refused. A complete file-side ownership claim can survive a failed database claim
and be retried by the same pair; a corrupt marker is refused. This is local
storage ownership, not producer authentication, a wire extension or orphan cleanup.
Earlier Java storage layouts are refused without conversion.

Rust now also pairs its retained root and SQLite database before service admission
or dispatch. Independently generated store identities and a synced file-first
claim prevent another database/root pair from being adopted implicitly. Complete
claims replay after a failed database transaction or process exit; partial,
corrupt or missing bound claims refuse without repair. The database metadata write
preserves admitted completion reservations. Tests cover competing roots, corruption,
interruption and WAL saturation; authenticated recovery over QUIC retains the pair
across restart and receipt replay. Older Rust storage policies are refused without
conversion. This local ownership prerequisite is not an orphan-cleanup API or a
replacement for principal authentication.

Java also provides explicit offline orphan reconciliation under exclusive payload
ownership and a database writer transaction. It audits managed input and retained
objects before deleting abandoned staging names. Unadmitted payload bodies become
commitment-only records retaining their immutable metadata and digest. Missing
input remains pending, changed retransmission is refused, and all admitted payloads
remain retained. Interrupted cleanup resumes only through another explicit call;
ordinary reopen counts remaining bytes without deleting them. Tests cover full
quota, concurrent and chunked restoration, process exit at filesystem boundaries,
and a real-QUIC timeout/refusal followed by matching restoration and completion.
Payload policy 3 refuses earlier policies without conversion; schema 6 and the
wire profile are unchanged. This is not retention expiry or whole-process resource
evidence. Rust now has its own explicit reconciliation path described below.

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
not a filesystem-allocation quota. Admitted-job completion reservations are
described below; the file-length guard alone is not orphan reconciliation.

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

Java version-4 stores separately reserve logical rehydration descriptor bytes
and completion slots at processing admission. Waiting parents retain that credit
across reopen, without occupying ordinary processing slots needed by children.
Closure converts the reservation and queues rehydration in the same transaction;
unrelated admissions cannot consume its metadata allowance. Processing stays
bounded at 128 global and 32 per-session queued/running jobs. Reserved or active
rehydration slots are separately bounded by 65,536 global and 16,384 per-session
entities within the combined metadata quota; physical worker limits are unchanged.
Tests cover queue and metadata saturation, rollback, exact descriptor conversion,
abrupt exit and real-QUIC completion. Discovery interleaves sessions in bounded
pages. These are not physical DB/WAL publication reservations or guarantees of
admitting unknown future descendants. Older Java schemas are refused without
conversion. Rust's admitted-job publication reservations are described below;
Java physical publication headroom and the full resource matrix remain due.

Rust storage policy version 4 reserves logical outcome, entity-digest and executor
record growth for admitted processing, rehydration and resume jobs. Layer 2
processing additionally reserves its configured continuation-token budget and
bounded claim metadata. The default token budget is 64 KiB and is exposed to the
application before dispatch, capped by the usable STATUS frame limit; this is a
local policy, not a wire-format reduction.
An oversized application result becomes a retained named refusal, without a
claim or successful entity transition. Checksummed actual/reserved charges commit
with session and job state and survive reopen. Old storage policies are refused
without conversion; session payload format 7 is unchanged. Tests pin serialized
growth, exact-quota publication, process exit, concurrent principal admission and
authenticated QUIC yield/recovery. Processing also reserves a possible rehydration
descriptor, outcome, attempt, parent output and scope-close digest. Queue policy
version 3 separates future/active rehydration from ordinary processing/resume slots:
65,536 global and 16,384 per authority/principal, versus 128 and 32 ordinary jobs.
Waiting parents retain credit without blocking their children; closure converts
the reservation atomically. Job discovery interleaves principals in bounded pages.
Store writes audit the bounded queue against retained session state before using
capacity. Tests exercise byte/slot exhaustion, interrupted conversion, corruption
and a sealed QUIC parent completing while ordinary processing remains full.
These reservations do not fund new child membership or payload admission,
checkpoint requests or filesystem blocks. They fund the fixed-capacity state
images and WAL stages described next; final-lineage file quota is separate.

Rust now preallocates a fixed-capacity session image containing a checksummed
header, serialized state and zero padding for protected growth. Mutable dispatch
and accounting use fixed images with immutable SQL keys. Future rehydration rows
are allocated with processing admission and become active or retired in place.
Within capacity, incremental BLOB writes preserve rows and allocated database
pages; unused logical credit does not shrink the retained allocation. The new
image and physical policies refuse older layouts without conversion; the session
payload remains version 7 and normative wire/CDDL are unchanged.

Before writing, a SQLite writer transaction funds every remaining acquisition,
publication and future rehydration-conversion stage. A per-connection VFS ceiling
protects that reserve against unrelated writes, including across rollback and
reopen. The ceiling also accounts for WAL-index shared-memory capacity. Expired
lease renewal retains publication credit rather than spending another job's
allowance. The stage bound covers the whole image, changed dispatch/accounting
pages, frame overhead, commit repetition and sector padding under pinned bundled
SQLite 3.53.2. Unsupported page geometry refuses explicitly.

Tests saturate ordinary WAL capacity with a pinned reader, then finish admitted
processing, rehydration and authenticated resume, including full-budget tokens,
two principals, concurrent admission, lease renewal and abrupt process exit.
A real authenticated QUIC test verifies token publication while the reader
remains pinned. A two-page-cache matrix measures complete acquisition/publication
transactions across three page sizes and token boundaries through 8 MiB, under
a fixed database page cap. Corrupt images and changed owned schemas are refused
without silent repair. These are cooperating-writer file-length reservations,
not allocated filesystem blocks or guarantees against I/O failure. Whole-session
serialization, integrity audits and scans including retired dispatch rows remain;
large sessions require proportionally more reserved WAL. No throughput or full
multi-tenant resource guarantee is inferred.

The Rust retained-payload store separately reserves global and authority/principal
bytes and object counts before disk creation. An immutable
checksummed policy survives reopen. Interrupted copies retain staging credit;
incomplete metadata and empty canonical directories remain globally charged.
Matching prefix replay can finish publication without overwriting admitted
input. A verified, synced receipt precedes successful installation. Tests cover
process exit, prefix images, policy and alias refusal, shared handles, and an
exclusive writer lock retained by readers and outstanding I/O. A real-QUIC test
checks named exhaustion, unchanged declared membership and independent principal
progress. These are bounded file-length reservations for a private single-writer
root, not filesystem-allocation, full power-loss or concurrent-tenant performance
proof. No orphan is silently deleted.

Rust payload installation now protects a separate 1,120-byte final-lineage
allowance and object slot per session before work admission. A checksummed
ownership marker covers the future digest, final metadata, receipt and stage
without inventing an output value. Partial markers stay globally charged;
matching replay establishes their owner without double charging. Final
publication uses prepaid staging credit even at the full ordinary quota,
and the complete allowance remains charged after publication. Tests exercise
partial metadata/receipts, process exit, owner limits, exact-quota publication,
and authenticated QUIC callbacks held while independent principals fill storage.
Missing declared payloads still prevent a successful checkpoint. The version-2
retained policy refuses old stores without conversion. SQLite completion capacity
is protected separately above; filesystem allocation and orphan reconciliation
are not established by these tests.

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

Rust's explicit offline reconciliation now audits the paired, writer-locked
database and payload root before reclaiming unadmitted bodies. It retains
immutable commitments, rejects changed retransmission and preserves admitted
input, declarations, and completion reservations. Tests cover concurrent
ownership refusals, interruption boundaries, quota restoration, and an actual
QUIC sealed session that remains pending until missing input is restored.
An isolated 32 MiB install/reclaim/restore test measures Rust heap allocations;
it does not establish total RSS or safe automatic cleanup under arbitrary
external writers. Reconciliation remains an explicit local operation.
