# PipeStream Protocol

**PipeStream: A Recursive Entity Streaming Protocol for Distributed Document Processing over QUIC**

**Internet-Draft: draft-krickert-pipestream**

## Overview

PipeStream is a proposed application protocol over QUIC for recursive work
decomposition, entity streaming, completion barriers, and durable continuation
references. The draft and implementations are under development. They are not
an approved IETF standard or a fully conformant production implementation.
See [draft-04 readiness](docs/standards/draft04-readiness.md) for tested changes
and the remaining interoperability and security work. The
[recovery, bounded execution and Java acceptance record](docs/standards/recovery-execution-java-acceptance.md)
maps those three delivered features to implementation and tests, with explicit
limits on the claims.

The [2026-09-06 protocol corrections](docs/standards/protocol-review-corrections-2026-09.md)
clarify transport-loss outcomes, streaming flow control, validated admission,
payload integrity, and durable caller authentication. Appendix E records the
version-1 limitations and remaining evidence needed for the successor contract;
this is a reviewed draft, not a claim those features are already implemented.

The [durable-work/results goal](docs/standards/durable-work-results-goal.md)
tracks the successor contract, independent Rust/Java implementation, and equivalent
streaming-gRPC workload evidence. Local draft -05 defines the version-2 contract
in Section 12 and Appendix F, with frozen wire examples. Its executable lifecycle
models are bounded design checks. Rust's embeddable
`v2_authority::server::Server` now integrates authenticated QUIC, durable dispatch,
bounded input/result file workers and the execution/maintenance runtime. Actual
wire tests exercise admission, output retrieval, reconnect, quotas and shutdown.
The Core-only listener remains separate. The Rust V2 client and runnable commands
now compose durable recovery and file delivery. Independent
Java V2 and neutral cross-language failure evidence remain unfinished; existing
full interoperability evidence is for version 1.
The Rust client journal now retains creation, immutable mutation intent/receipts,
revisioned work observations and full manifests with explicit output selections.
It also verifies complete sealed membership and bottom-up status coverage before
constructing an exact root-completion request. Parent/child commitments are checked
in both observation orders, including when a later sealing receipt verifies a
parent's cached membership. Reopen preserves those commitments; reply reordering,
contradictions, local write failures and physical exhaustion have regression tests.
The blocking core APIs now have a bounded asynchronous journal owner, including
cancel-safe capacity and cross-process ownership/recovery tests. A bounded V2
client wire transport now supplies authenticated connections, multiplexed control
and incremental input/result streams. The durable session client now composes
them: it persists intent before transmission and records receipts/observations
before returning success, including after waiter cancellation. File adapters now
prehash/stream inputs and install only verified results without overwriting local
files. The [V2 CLI guide](implementations/rust-quinn/docs/v2-cli.md) covers explicit
initialization, authenticated serving, original-operation recovery, both branch
modes, cancellation, retry and verified downloads. A separate managed-result library
now provides exclusive local storage, shared disk quotas, restart cleanup and
verified local reads. Explicit CLI commands now initialize managed copies and raw
exports, download into an existing root, and verify/export saved selections offline
without remote fallback. Independent Java V2 and the neutral failure/workload
comparison remain open.
Java now has an independent [V2 typed library foundation](implementations/java-netty/README.md#version-2-typed-library-foundation)
for the frozen wire schemas and commitments, including bounded streaming scope
hashes. Its authenticated Core-only Netty listener now implements negotiation,
correlated refusals, detach/half-close, connection quotas and live deadlines.
The Java Core client now verifies selection and waits for the correlated detach
response and actual peer FIN, with independently bounded lifecycle and cleanup.
Its independent [authority storage](docs/standards/java-v2-authority-store.md)
now commits sessions and caller declarations with immutable operation replay,
streamed seals and bounded membership/work reads. Recovery checks receipt/member
consistency, including real crash, capacity and contradictory-storage cases.
Java V2 format 3 additionally preallocates mutable scope/work/fence/clock images
and retains credits for their bounded rewrites; ordinary mutations preserve
the promised WAL headroom. Those image credits do not yet fund entire jobs.
Its independent [immutable input store](docs/standards/java-v2-input-store.md)
now verifies and installs bounded payload files; it is not yet integrated with
the authority's required atomic job/admission transaction.
Independent Java durable execution/results/recovery, complete cross-language
failure evidence and the workload comparison remain unfinished.
The Rust `pipestream_core::v2` library now implements typed codecs for every
version-2 message and record, frozen commitments, negotiation checks, bounded
client correlation and incremental object validation. This is a library
foundation used by the durable version-2 endpoint. Its `v2::authority` module now
adds transactional session identity, declaration/operation replay and bounded
retained-state reads, with subprocess commit-crash tests. Authority storage format
11 retains checksummed operation receipts and bounded original declaration intent;
replay and recovery reconcile exact membership, batch counts, seals and charges.
Earlier authority formats are refused without conversion. Bounded payload staging,
immutable installation, header preflight and reference-safe orphan collection are
now present as well. Work views and scope summaries have preallocated storage
and persistent fixed-record rewrite credits. Durable output reservations and
bounded unknown-length output materialization now preserve promised file capacity
across restart. Input preparation now joins that output reservation with
transaction-safe expansion of the work-view record, without accepting a job.
Mutable scope state and the shared clock now participate in reserved record
writes, with clock-counter and commit-crash tests.
`admit_input` now atomically commits the validated input reference, attempt,
deadline, child scope, fixed job record, reservations and immutable receipt.
Executor and byte limits span the retained jobs; commit-crash tests reopen real
input bytes and replay lost acknowledgments. The library now runs registered
streamed callbacks under durable worker leases, supports lease renewal and
explicit retry, and commits manifests or failures with attempt/lease/ancestor
fences. Process-death tests cover claim, publication, retry and unpublished-output
recovery. A fixed worker pool now discovers the durable backlog in bounded scans,
limits global/per-owner callback concurrency and reserves reusable output I/O
capacity before claiming work. Durable cancel/skip receipts and revocation now
fence unresolved work; independent bounded maintenance settles deadlines and
descendants and commits nonempty scope seals, counts and status roots. Crash,
publication-race and pinned-journal tests cover these transitions. Registered
authority expansion now creates actual children through the same admission path;
both branch modes stream retained child outputs into application reassembly.
Expansion completion is durable and separate from the membership seal. Process
death, retry, stale producer grants and reserved child-reader capacity have
focused tests. `ResultService` now authorizes retained manifest lookup and bounded
object-read leases against the supplied verified owner identity. Reads enforce
commitments, expiry, revocation, send progress and bounded maintenance without
running application code. Crash/corruption tests and a 32 MiB resource gate cover
these library APIs. Dependency-aware retention now commits deletion eligibility,
collects unpinned files, then releases logical capacity. Restart/admission audits
distinguish interrupted cleanup from missing live storage. Session retirement now
commits eligibility before bounded metadata deletion and preserves generation and
owner creation history. The listener and explicit `v2` command group integrate
these authority libraries; original version-1 commands remain separate. The
[V2 acceptance ledger](docs/standards/durable-work-v2-test-plan.md) distinguishes
the Rust evidence from unfinished Java parity, neutral process
failures and workload/resource comparison gates.

Draft -04 now defines supported/required extension negotiation, implemented
independently in Rust, Java, and C++. Unknown requirements fail CONNECT;
optional unknown identifiers are not activated. Sealed work sets and
their durable producer/session binding are now available in Rust through
the opt-in private-use `sealed-work-sets-v1` profile (Section 9.8).
Bidirectional producers and a complete profile conformance matrix remain
unfinished. C++ still implements only the Layer 0 subset.

Java now has an independent sealed declaration codec, SQLite state machine,
file-backed payload store, and public Netty `SealedClient`. The payload library
validates incremental reception and immutable installation before admission;
`SealedExecutor` commits durable processing/rehydration jobs and runs fenced
callbacks in bounded workers. The separate public `SealedServer` integrates
these components into a sealed-only Netty listener with bounded ingress and
metadata pools, pending checkpoint deadlines, and durable replay identity.
Real Java-to-Rust tests exercise nested work,
out-of-order chunks, scoped checkpoints, declaration replay after restart,
and malformed responses. A Rust public-client scenario now exercises the Java
server's nested/chunked completion, reconnect replay, and named refusals.
The existing Java standalone commands remain Layer 0. The opt-in Java durable
client now journals request intents and verified responses, restores recursive
observations across restart, and exposes uncertain inputs without blindly
resending them. Both Java client modes check the server's DNS/IP certificate SAN
before sending application frames. Retained-outcome lookup for uncertain sealed
inputs and a complete production conformance matrix remain future work; the
acceptance record maps the implemented crash/resource guarantees and their tests.
The Java sealed server does not yet authenticate client principals. Its
server-authenticated TLS fixtures are not evidence of compliance with the
draft's requirement to authorize a principal before durable work admission.
See the
[Java implementation boundary](implementations/java-netty/README.md#sealed-work-library-foundation).

The Rust durable service also supports negotiated mutual-TLS session binding:
certificate-mapped principals, retained authority/owner records, and session
revocation. The separate opt-in `authenticated-recovery-v1` profile adds
authority-qualified requests, immutable 24-hour acceptance receipts, and
correlated retained completion or refusal outcomes across reconnects and
restarts. It does not activate Layer 2 recovery in sealed-work sessions.
Durable attempt
fences now protect result publication, and callbacks run outside database
transactions. Receive payloads are now incrementally spooled to bounded
temporary files and processed through readers. The service submits typed jobs
to a transactionally bounded queue and dispatches processing, rehydration, and
resume callbacks in bounded workers, independently of connection control handling.
Retained payloads are reopened and verified before interrupted work is executed.
Retained serialized session state now has persistent global and per-principal
byte/count quotas and bounded serialization. The bundled Unix SQLite backend
now guards database, WAL, rollback-journal, and shared-memory file lengths under
an immutable on-disk policy. Retained Rust payloads and lineage files now have
persistent global and authority/principal reservations, bounded staging, and
exclusive writer ownership. Interrupted copies and incomplete metadata stay
charged across reopen. Java now has independent database/WAL/journal/shared-memory
file-length enforcement through a small SQLite extension packaged with JDBC.
Java admission now protects logical rehydration descriptor bytes and completion
slots, including across restart and processing-queue saturation. Rust now reserves
logical result and attempt growth for admitted processing, rehydration and resume
jobs, with an explicit callback continuation-token budget. Processing also protects
its possible rehydration descriptor, publication bytes and a separate completion
slot, so waiting parents do not fill the processing queue needed by their children.
Rust payload installation now also reserves final-lineage file quota before
admission, including its metadata, receipt and staging allowance across restart.
This protects configured file-length headroom, not allocated filesystem blocks.
Rust now allocates fixed-capacity session, dispatch and accounting images at
admission and protects WAL/shared-memory headroom for the remaining execution
stages. Unrelated writes cannot spend an admitted job's acquisition/publication
credit, including with a pinned WAL reader. The bound is tied to bundled SQLite
3.53.2 and the cooperating-writer file-length policy, not filesystem-block
preallocation. Whole-session serialization and retained-row scans remain;
large sessions require proportionally more completion credit. Older storage
layouts are refused without conversion. Java now preallocates job/entity/closure
images and possible rehydration rows, and independently funds remaining stages
under SQLite 3.53.4 WAL/shared-memory limits. Pinned-reader tests cover completion,
recursive conversion and rollback followed by retry. Java now durably pairs each
managed database with one payload-store identity and revalidates/pins retained
input through admission; closed or foreign-store handles cannot admit cached
metadata. Earlier Java database/payload policies are refused without conversion.
Java now provides explicit offline orphan reconciliation: it audits the matched
database and payload root before removing abandoned staging files or replacing
unadmitted bodies with retained immutable commitments. Matching retransmission
can restore those bodies; missing input remains pending. Rust now also pairs its
session database and retained root before service startup, with immutable store
identities and replayable file-first binding. Older Rust storage policies are
refused without conversion. Rust now also supplies explicit offline orphan
reconciliation under exclusive root ownership and the paired database's writer
lock. It audits admitted input before reclaiming abandoned files, preserves
immutable orphan commitments, and allows matching retransmission without
inventing completion. The Java producer now persists its own observations,
separately from server state; this does not discover unobserved server outcomes.
Independent cross-language authenticated recovery remains future work; Java's
independent implementation covers the sealed-work profile, not Layer 2 recovery.
Connection metadata and lineage operations now run in a bounded storage pool.
An independent control reader enforces checkpoint deadlines during those
operations; held-storage tests also exercise protocol refusals and progress
on another connection. This is not a disk-latency or throughput guarantee.
The [implementation plan](docs/standards/recovery-execution-java-plan.md) preserves
the requirements and incremental history; the acceptance record gives current
verification evidence for the delivered goal.

## Authoring Workflow

This repository uses a modular authoring workflow for IETF drafts. The monolithic draft is treated as a build artifact and is not checked into the repository.

### Source Structure

- **`sections-src/`**: **The Source of Truth.** Individual Markdown files for each RFC section. Edit these files directly.
- **`draft-template.md`**: The master kramdown-rfc template that includes all sections in the correct order.
- **`cddl/`**: Machine-readable serialized-message CDDL, checked against Appendices C and F.
- **`test-vectors/`**: Checked-in golden valid and invalid wire inputs with named expected refusals.
- **`conformance/`**: Vector checks and the black-box client/server interoperability runner.
- **`implementations/`**: Independent Java/Netty, Rust/Quinn, and C++/MsQuic libraries and executables.
- **`examples/`**: Language-native Java and Rust applications that use the reusable implementations.
- **`proto/`**: Non-normative Protocol Buffers definitions used by implementation tooling. Not part of the Internet-Draft; the specification's normative schemas use CDDL (Appendix C). An alternative serialization format may be registered separately via the PipeStream Serialization Formats registry.

## Reference Suite

The Layer 0 reference suite is intentionally polyglot. Each implementation owns its codec and protocol state machine; no implementation imports protocol code from another. The shared artifacts are the specification, CDDL, and language-neutral binary corpus.

The common standalone interface supports `serve` and `send`. A successful transfer negotiates capabilities, sends one immutable Entity Stream, validates SHA-256, crosses a CHECKPOINT request/acknowledgement barrier, advances the connection cursor, and completes a GOAWAY exchange. TLS 1.3 and ALPN `pipestream/1` are mandatory and 0-RTT is disabled.

Run the complete suite after installing the prerequisites below and the language toolchains:

```bash
bundle install
./conformance/run_all.sh
```

That command checks frozen vectors, runs every implementation and example's
tests, builds the three servers and language-native applications, executes all
nine black-box client/server pairings, and runs the three external scenarios.
See [`conformance/README.md`](conformance/README.md) for the command contract and
[`examples/README.md`](examples/README.md) for the application sources.

Dependency versions are reproducible: Ruby dependencies are exact in
`Gemfile.lock`, Rust dependencies in `Cargo.lock`, Java dependencies and build
plugins in `pom.xml`, and MsQuic at an immutable Git tag in `CMakeLists.txt`.
The direct dependencies were checked against their upstream registries on
2026-09-04; the reference suite uses the latest compatible stable releases at
that point. The Java reference and example were subsequently migrated on
2026-09-07 to the maintained Netty `4.2.17.Final` QUIC source and matching BOM.
They now select a separately identified [transport extension](implementations/java-netty/transport/README.md)
built from exact upstream revisions, checked patches and a Cargo lock. The full
conformance command verifies and installs it into an isolated Maven repository,
not the global cache. This does not establish Java V2 durable-profile parity.
The [Java transport review](docs/standards/java-v2-transport-credit.md) records
the pinned native provenance and remaining control/data-credit work. Rustls
remains on the 0.23 series required by Quinn rather than the 0.24 development
series.

### Styling Conventions for Rendering

To ensure the IETF Datatracker renders diagrams and code correctly in both HTML and Plain Text, use the following fences:

#### 1. Packet Diagrams (ASCII Art)
Always use the `ascii-art` type to force monospaced rendering:
```markdown
~~~~
    0 1 2 3
   +-+-+-+-+
   | Data  |
   +-+-+-+-+
~~~~
{: type="ascii-art"}
```

#### 2. Structured Metadata
Use appropriate syntax highlighters for schema blocks:
```markdown
~~~~ cddl
example = {
  id: uint
}
~~~~
```

## Build Instructions

### 1. Prerequisites

You need the following tools installed:

- **Ruby and Bundler**: For the pinned `kramdown-rfc` and CDDL validator gems
- **xml2rfc 3.34.0 via uv**: External IETF document rendering only
- **idnits**: For final validation

```bash
# Install toolchain (macOS example)
brew install ruby idnits
gem install bundler
bundle install
uv tool install xml2rfc==3.34.0
```

There are no checked-in Python sources, and the reference implementations,
vector checks, interoperability matrix, and examples do not invoke Python.
`xml2rfc` is an external IETF authoring tool used only by `build.sh` to render
the draft. It is not part of the protocol or its conformance evidence.

### 2. Generating the Draft

To build all formats (XML, TXT, HTML) in one pass:

```bash
./build.sh core 05
```

The script runs the pinned `kramdown-rfc` and `xml2rfc` toolchain, emits XML,
TXT, and HTML, and finishes with `idnits` validation. Generated drafts are
ignored build artifacts.

### 3. Validation

Always run `idnits` on the generated `.txt` file before submitting to ensure there are no formatting errors or non-ASCII characters:

```bash
idnits --verbose draft-krickert-pipestream-05.txt
```

## Submission

Submit the generated **`.xml`** file to the IETF Datatracker:
[https://datatracker.ietf.org/submit/](https://datatracker.ietf.org/submit/)

---

## Repository Contents

- **`REFERENCE_IMPLEMENTATION.md`**: Reference-suite status plus informative guidance on algorithms.
- **`OVERVIEW.md`**: High-level architectural summary.
- **`advocacy/`**: IETF process materials (prior-art survey, DISPATCH kit, ANRW paper draft, submission checklist). Not part of any Internet-Draft.
- **`build/`**: (Ignored) Temporary build artifacts.

## Authors

- **Kristian Rickert** (PipeStream AI) — <kristian.rickert@pipestream.ai>

## Status

This is an active Internet-Draft targeting IETF standards track.
