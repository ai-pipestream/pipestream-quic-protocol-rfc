# Recovery, execution, and Java acceptance evidence

Audited on 2026-09-06 against implementation commit
`5df4ec3d467ebde616fb47c16a40b6033d266206`. This record maps every acceptance
requirement in [the implementation plan](recovery-execution-java-plan.md) to
implementation and executable evidence. The plan's increment sections preserve
historical results; this record supplies the current assessment.

The three deliverables are implemented: authenticated recovery in Rust, bounded
asynchronous execution, and independent Java sealed-work client/server behavior.
This is acceptance of those deliverables, not full protocol conformance, IETF
approval, production certification, deployment, or a draft submission.

## 1. Authenticated recovery

### Certificates, principals, authority, and revocation

Rust's [authentication policy](../../implementations/rust-quinn/quinn/src/authentication.rs)
uses rustls client-certificate verification and an explicit, bounded certificate
fingerprint-to-principal map. A mapped leaf still needs a valid trusted chain.
The configured authority and stable principal, not peer metadata or the sealed
producer label, identify the caller.
[Session authorization](../../implementations/rust-quinn/src/authorization.rs)
persists ownership and revocation. Admission, retained lookup, worker acquisition
and publication check that binding. Existing anonymous sessions cannot be
implicitly converted, and a configured authenticated client cannot downgrade to
anonymous work.

Evidence: [authenticated-session QUIC tests](../../implementations/rust-quinn/quinn/tests/draft04/authenticated_sessions.rs)
exercise missing, untrusted, unmapped and expired credentials; required-profile
negotiation; cross-principal and cross-authority refusals; credential rotation
under the same explicit principal; live/reconnected session revocation; revocation
inside a callback; and authorization of background recovery. Assertions include
retained state and callback counts, not just connection failure.

### Negotiation, correlated receipts, and immutable outcomes

[Section 10.6.5](../../sections-src/section-10.md) specifies
`authenticated-recovery-v1` (private-use 65283), requiring authenticated-session
binding (65282) and Layer 2. It excludes the sealed-work profile. The
[recovery state machine](../../implementations/rust-quinn/src/recovery.rs)
and [wire codec](../../implementations/rust-quinn/src/recovery/wire.rs) validate
authority, session, request ID, claim ID and state checksum. Claim redemption,
the queued resume job and its acceptance receipt commit in one transaction.
Replay returns the same retained receipt and terminal outcome, rather than
redeeming the claim or invoking a completed resume again. Success and refusal
are distinct outcomes; a zero diagnostic code is not success.

The client requires exact request/receipt correlation and consumes the pending
outcome before another operation. Cancellation of a partial recovery exchange
closes the connection without claiming that admitted remote work was cancelled.

Evidence: [retained-recovery QUIC tests](../../implementations/rust-quinn/quinn/tests/draft04/retained_recovery.rs)
cover unobserved receipts, cancelled waits, restart, retained application refusal,
wrong owner/authority/request, malformed or changed replies, pending-outcome
guards, partial-frame cancellation and prohibited legacy/profile combinations.
They inspect retained jobs and execution counts across restart. The core
[recovery tests](../../implementations/rust-quinn/src/recovery/tests.rs)
also check frozen wire inputs and the distinction between receipt and completion.

### Expiry, retention, concurrency, crashes, and fencing

Receipts have an immutable 24-hour retention interval. Access still requires
current ownership and non-revocation. Changed requests, expired claims, expired
receipt access and corrupt state refuse without mutation. Capacity exhaustion
does not evict replay history or partially redeem a claim.

The core recovery tests include
`concurrent_requests_have_one_receipt_and_one_job`,
`abrupt_exit_retains_acceptance_and_queued_resume_as_one_transaction`,
`queue_exhaustion_rolls_back_redemption_receipt_and_job`, and replay after
reopening SQLite. [Execution fencing](../../implementations/rust-quinn/src/execution.rs)
checks the durable job identity, epoch, executor identity, expiry and owner at
publication. [Execution tests](../../implementations/rust-quinn/src/execution/tests.rs)
reject stale publication both before and after a successor acquires the job,
and verify transactional rollback and revocation.

Application callbacks run outside the database transaction. Durable fencing
protects protocol publication, not arbitrary external effects; applications still
need idempotency or transactional fencing at their external effect boundary.

## 2. Bounded asynchronous execution

### Admission, memory, files, metadata, and concurrency

Rust [ingress](../../implementations/rust-quinn/quinn/src/recursive/ingress.rs)
validates bounded headers and incrementally writes payloads through the
[spool](../../implementations/rust-quinn/quinn/src/recursive/spool.rs).
The spool charges byte and object limits before growth, including zero-byte
files and incomplete reception. FIN, length and checksum validation precede
admission. Streams, chunk assemblies, complete control-event backlog and retained
client status histories have independent limits and named refusals.

The [worker pool](../../implementations/rust-quinn/quinn/src/recursive/executor.rs)
shares global and authority/principal permits across service handles for one
store. It acquires a permit before spawning a callback. The separate
[storage pool](../../implementations/rust-quinn/quinn/src/recursive/storage.rs)
likewise bounds physical blocking operations. Cancelling a waiter does not free
credit while the underlying callback or storage operation remains active.
Dispatch reads a bounded durable queue; it does not spawn one task per payload.

The [retained file store](../../implementations/rust-quinn/quinn/src/recursive/retained.rs)
enforces persistent global and authority/principal byte/object accounting,
immutable payload identity, bounded staging and final-lineage reservations.
Database, WAL, journal and shared-memory file-length policies are separate from
temporary spool accounting. The [SQLite completion reservations](../../implementations/rust-quinn/src/persistence/physical/reservation.rs)
protect admitted acquisition/publication and possible rehydration stages from
unrelated writes consuming their remaining capacity.

Evidence: spool unit tests and
[wire ingress tests](../../implementations/rust-quinn/quinn/tests/draft04/spooled_ingress.rs),
[retained-store tests](../../implementations/rust-quinn/quinn/src/recursive/retained/tests.rs),
[logical completion tests](../../implementations/rust-quinn/src/persistence/storage/completion/tests.rs),
[physical reservation tests](../../implementations/rust-quinn/src/persistence/physical/reservation_tests.rs),
[whole-stage cost tests](../../implementations/rust-quinn/src/persistence/physical/reservation/cost_tests.rs),
and [authenticated wire quota tests](../../implementations/rust-quinn/quinn/tests/draft04/storage_quotas.rs)
exercise boundary admission, rollback, reopen, queue saturation, retained-file
exhaustion, pinned-reader WAL exhaustion and publication using reserved credit.

Java independently implements these local execution concerns in
`SealedServer`, `SealedExecutor`, `SealedJobs`, `SealedPayloadStore`,
`SealedSessionStore` and `SealedCompletionReservations`. Its server bounds
connections, streams, read buffers, control backlog, storage actions, output,
observed jobs and chunk assemblies. Its executor bounds global and per-session
callbacks and storage calls. These per-session limits are not authenticated
tenant isolation: the Java sealed listener does not implement the Rust
authenticated-recovery profile.

Evidence: Java `SealedPayloadStoreTest`, `SealedSqliteFilesTest`,
`SealedJobsTest`, `SealedExecutorTest`, `SealedCompletionReservationsTest` and
`SealedServerTest` under the [Java test directory](../../implementations/java-netty/src/test/java/ai/pipestream/quic/).
The [Java reservation contract](java-completion-reservations.md) includes its
independently implemented whole-transaction cost derivation and tested geometry.

### Responsive control, durable work, and interruption

Rust's independent [control reader and checkpoint clock](../../implementations/rust-quinn/quinn/src/recursive/control.rs)
start deadlines when a request arrives, rather than after a blocked transaction.
Duplicate requests do not extend the deadline. Java similarly keeps ingress and
metadata work off the Netty event loop and retains the original checkpoint clock.

[Storage-stall tests](../../implementations/rust-quinn/quinn/tests/draft04/storage_stalls.rs)
hold actual SQLite writers and lineage operations while testing deadline expiry,
protocol refusals, bounded backlog and progress on another connection.
[Asynchronous wire tests](../../implementations/rust-quinn/quinn/tests/draft04/asynchronous_execution.rs)
hold processing, rehydration and resume callbacks while independent work and
checkpoint deadlines progress. They verify overload rollback, retained refusal
after callback panic, listener shutdown without loss of admitted jobs, and
rehydration when the ordinary processing queue is full.

Java `SealedExecutorTest` verifies independent-session progress, callback reentry
into storage, physical permits after expiry, bounded stalled admission, retained
callback exceptions and shutdown ownership. `SealedServerTest` exercises held
SQLite, queue and WAL exhaustion, original checkpoint deadlines, missing input,
RESET_STREAM versus FIN, and declaration/checkpoint replay after restart over
actual QUIC.

Both implementations persist dispatch descriptors and publication outcomes,
verify retained inputs before callbacks, and refuse incompatible storage policies
without conversion. Store-binding and orphan-reconciliation tests in both trees
cover process exit, interrupted file operations, corrupt/foreign pairs and
immutable matching retransmission. Reconciliation never deletes admitted work or
turns missing payloads into completion. Lease expiry does not cancel a callback;
shutdown retains physical ownership until the operation actually returns.

### Measured resource evidence

The isolated Rust [allocation test](../../implementations/rust-quinn/quinn/tests/spool_resources.rs)
streams 32 MiB over QUIC to a reader-backed callback, verifies the retained file
and checksum, and checks spool cleanup. The 2026-09-06 isolated run measured
129,739 bytes of additional Rust heap and a 15,972-byte largest allocation.
The checked limits are less than 12 MiB extra heap and less than 4 MiB for any
single allocation. Its separate 32 MiB orphan installation/reclamation/restoration
path measured 13,048 additional bytes and a 2,216-byte largest allocation, under
separate 1 MiB gates. These are allocator measurements, not native RSS bounds.

Java executes a 32 MiB retained payload under `-Xmx24m` in `SealedExecutorTest`.
`SealedServerTest.realQuicTransfersAndProcesses32MiBUnderA24MiBJavaHeap` runs the
actual QUIC client and server under that heap with both ephemeral and durable
clients. It checks the stored checksum, one callback, zero temporary bytes and
zero active payload handles. Java completion-cost tests measure 378 whole
transactions across 54 page-size/cache-size/metadata/outcome scenarios and assert
each stage fits its own reservation without growing the main database page count.

## 3. Independent Java sealed work

### Independent implementation and complete scoped lifecycle

The [Java sources](../../implementations/java-netty/src/main/java/ai/pipestream/quic/)
contain independent CBOR and WORK_SET codecs, seal hashing, scoped status Merkle
construction, durable SQLite declarations/lifecycle/checkpoints, payload storage,
worker dispatch, a Netty `SealedServer` and public `SealedClient`. The
[Maven dependencies](../../implementations/java-netty/pom.xml) import no Rust
protocol or state-machine code. The native SQLite file guard is a storage helper,
not a shared protocol implementation. The standalone Java CLI remains Layer 0;
the separate sealed APIs are the Layer 1 implementation.

`SealedWorkTest` and `SealedScopeTest` verify independent frozen encodings,
unsigned values, malformed fields, exact declaration ACKs and seal/status hashes.
`SealedSessionStoreTest` verifies multi-entity membership, immutable parent and
producer binding, sequence/replay rules, missing children, nested closure,
failure policy, rollback, capacity and abrupt exit/reopen. `SealedCheckpointStoreTest`
covers retained checkpoint identity and corruption. The public client validates
each scope's declared parent identity and depth, exact checkpoint fields and
recursive readiness before GOAWAY. A failed parent is not reported as successful
rehydration, and a missing declared input cannot be omitted from a completion cut.

`SealedProducerJournalTest`, `SealedProducerInputsTest` and server restart tests
also verify durable producer observations, persisted request intent and explicit
uncertain inputs. Client-side TLS checks verify DNS/IP subject alternative names
before sending application frames; the producer label is not authentication.

### Real Java-to-Rust and Rust-to-Java evidence

[SealedInteropTest](../../implementations/java-netty/src/test/java/ai/pipestream/quic/SealedInteropTest.java)
uses the public Java producer against a separately launched Rust server. Tests
cover multiple roots, nested scopes, out-of-order chunks, scoped checkpoints,
lost declaration ACKs, exact replay, changed owner or negotiated limits, malformed
responses, deadline refusal and durable client/server restarts. The retained-work
scenario spans three Rust server lifetimes. The failed-descendant scenario
propagates a failed leaf through two parent levels across restart.

[SealedServerTest](../../implementations/java-netty/src/test/java/ai/pipestream/quic/SealedServerTest.java)
launches the public Rust `sealed-scenario` producer against the independent Java
listener, checking its recursive/replay/refusal result and nine application
callbacks. The `sealed-interop` Maven profile is enabled by the full suite;
a missing Rust executable fails the test rather than skipping it. These tests
are separate from the nine Layer 0 language pairings.

The acceptance audit found and corrected a real Rust failure-propagation gap in
[Forgejo PR #38](https://git.rokkon.com/ai-pipestream/pipestream-quic-protocol-rfc/pulls/38).
Its tests include forged-digest rollback, lost resolution ACK replay, no unwanted
rehydration callback, core policy/retry alternatives, queue/logical capacity and
pinned-reader WAL completion. This finding was fixed in code before acceptance.

## Verification and publication evidence

The following local gates passed on the audited implementation tree:

```bash
./conformance/run_all.sh
cargo test --locked --manifest-path implementations/rust-quinn/Cargo.toml \
  -p pipestream-quinn --test spool_resources -- --nocapture
RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps \
  --manifest-path implementations/rust-quinn/Cargo.toml
./build.sh core 04
```

The complete runner passed 314 Rust workspace tests and 193 Java tests, with
zero Java failures, errors or skips counted from Surefire XML. It also ran the
external example tests, native SQLite/C++ checks, frozen vectors/CDDL verification,
formatting, strict clippy, release builds, all nine Layer 0 client/server pairs,
32 raw capability probes, recursive/recovery CLI scenarios and all three runnable
examples. Test counts are not the coverage argument; the mappings above identify
the relevant assertions and their implementation boundaries.

Draft -04 builds XML, text and HTML and passes idnits with zero errors, flaws or
warnings and the existing FIPS normative-reference comment. This is a document
validation result, not a protocol-conformance or standards-approval result.
The normative sources, CDDL and frozen vectors are versioned together; generated
drafts remain ignored artifacts. No Python sources are tracked under
implementations, examples or conformance. The external xml2rfc authoring tool is
not an implementation or conformance driver.

Java Javadoc generation still reports missing-comment warnings. This audit does
not claim warning-free full Javadoc coverage. Doclint passes with the missing
comment category excluded, using plugin 3.12.0, `-Ddoclint=all,-missing` and
`-Dmaven.javadoc.failOnWarnings=true`. The executable Java compiler gate remains
`-Xlint:all -Werror`; documentation coverage is separate from compilation and
protocol behavior.

The audited implementation commit is PR #38's normal Forgejo merge. Live
Forgejo and GitHub `main` both resolved to the full SHA above during this audit.
This acceptance record is a documentation-only follow-up, published through a
Forgejo PR and checked against the downstream mirror. Local validation is not
hosted CI, image publication, a release, deployment, operational migration or
IETF submission; none of those actions is part of this landing.

## Explicit boundaries, not implied guarantees

- Section 9.8 is client-produced Layer 1 sealed work, not bidirectional work or
  Layer 2 recovery. Java sealed interoperability does not establish independent
  cross-language authenticated recovery. C++ remains a Layer 0 implementation.
- Sealed declarations and checkpoint ACKs are replayable. Automatic payload retry
  and retained-outcome lookup for an input whose result the producer never observed
  are not specified by this profile. Such input remains explicitly unresolved;
  no implementation substitutes a successful barrier or silently drops the ID.
- Legacy CLAIM_REDEMPTION still refuses duplicate redemption after a lost ACK.
  Callers requiring retained acceptance and completion must negotiate the separate
  authenticated-recovery profile and preserve their original request identity.
- Resource limits cover library-owned work in the documented single-writer/store
  model. They do not bound arbitrary application allocations, native RSS, external
  effects or threads created by callbacks. File-length reservations are not
  filesystem-block preallocation or a power-loss proof. New children, payloads and
  checkpoint records require their own admission capacity.
- Physical callback limits remain owned until callbacks return. Recovery of
  expired unfinished attempts does not make callback termination possible and
  does not automatically retry retained application refusals.
- Full retained-state audits and some whole-session serialization remain on the
  storage path. These correctness/resource tests do not establish constant-time
  admission, large-session throughput, multi-tenant fairness, arbitrary disk
  latency bounds or a complete production conformance matrix.

These boundaries are unchanged from the negotiated contracts and implementation
requirements. They do not excuse any missing declaration, unresolved descendant,
unauthorized recovery, stale publication or over-budget admission in this goal.
