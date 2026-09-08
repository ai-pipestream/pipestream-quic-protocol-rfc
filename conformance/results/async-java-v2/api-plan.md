# Java V2 durable endpoints: public API, configuration and test-adapter contract

Owner: Claude (A-SERVER + A-CLIENT). Branch `agent/rfc-claude-java-v2`, base
`8eb5a17`. This document is published before consumers write against invented
APIs. Names below are the committed public surface; later checkpoints on the
board pin the commit that implements each item. Anything marked *planned* is
not yet callable. Nothing here advertises a profile until the implementing
checkpoint says so.

All types live in `ai.pipestream.quic.v2` (Java package of the existing typed
wire/storage foundation) so they can consume the package-private storage,
runtime and transport owners without reflection or code copies. Legacy
`ai.pipestream.quic.Main` (`serve`/`send`) stays untouched; V2 has a separate
entry point.

## 1. Server: `DurableHost` (authority composition) and `DurableServer` (listener)

### `DurableHost` (public, `AutoCloseable`)

One host owns one matched `SessionStore`/`InputStore` pair plus every bounded
authority service. It is created explicitly as *initialize* (new roots) or
*open* (existing roots; missing/empty history is an error, never an empty
authority). Recovery/reconciliation runs inside `open` before the host reports
ready; no capacity is admitted before that.

```java
public static DurableHost initialize(Path root, Configuration configuration,
    List<Application> applications, OwnerPolicy owners, UtcClock clock)
    throws IOException, SQLException;
public static DurableHost open(Path root, Configuration configuration,
    List<Application> applications, OwnerPolicy owners, UtcClock clock)
    throws IOException, SQLException;
public Status status();                 // scheduler/retention/result/wait counters
public void revoke(long generation);    // offline operator revocation (host must not be serving)
@Override public void close() throws IOException; // documented shutdown order, see §1.4
```

Root layout (fixed): `root/authority.sqlite` (+ sidecars) and `root/objects/`.
Both are paired; a root cannot switch either side.

`Configuration` (record, validated, immutable, byte-for-byte compared on open):

| field | meaning | default (`Configuration.defaults(authority, resultAuthority)`) |
| --- | --- | --- |
| `authority` | issuer label (1..128 `[A-Za-z0-9-._~]`) | required |
| `resultAuthority` | `host:port` written into result locators | required |
| `sessionLimits` | `Records.Limits` returned by every created session | scopes 4096, entities 1,000,000, operations 1,000,000, input/output bytes 1 GiB, active jobs 16 |
| `maximumPolicy` | largest `Records.Policy` accepted exactly | 60,000 / 3,600,000 / 86,400,000 ms |
| `maxOwners`, `maxSessions`, `maxSessionsPerOwner` | retained-history bounds | 1024 / 1024 / 64 |
| `files` | `BoundedSqlite.Limits` for the authority DB | 256 MiB db, 64 MiB WAL/journal, 512 KiB shm |
| `objects` | `ObjectLimits(bytes, files, objectBytes, handles)` | 8 GiB, 10,000, 16 MiB, 128 |
| `maxJobs`, `maxJobsPerOwner` | funded-job ceilings (persisted) | 64 / 16 |
| `execution` | `ExecutionLimits(workers, workersPerOwner, leaseMillis, bufferBytes)` | 4 / 2 / 30,000 / 65,536 |
| `scheduler` | `SchedulerLimits(pageSize, pollMillis)` | 64 / 50 |
| `retention` | `RetentionLimits(pageSize, pollMillis)` | 64 / 1000 |
| `results` | `ResultLimits(reads, readsPerOwner, bufferBytes, pollMillis)` | 128 / 32 / 65,536 / 250 |
| `waits` | `WaitLimits(pending, perOwner, workers, pollMillis)` | 1024 / 64 / 4 / 50 |
| `storageWorkers` | `WorkerLimits(threads, queued, queuedPerOwner)` off-loop SQLite/file pool | 8 / 512 / 64 |
| `producer` | local producer-1 transport-equivalent limits (`Messages.Capabilities` response form) used by mode-2 expansion | control 1 MiB, stream 64, pending 64, object 16 MiB, idle 30 s, lifetime 300 s |

Applications, `maxJobs`/`maxJobsPerOwner` and the application registry are part
of the persisted installation commitment (`AdmissionStore.ExecutionPolicy`);
`open` refuses a changed registry.

### 1.1 Application registration (for Meta / external hosts)

```java
public record Application(String label, Set<Integer> modes, RestartSafety safety,
                          Processor processor, Producer producer) // producer required iff modes contains 2
public enum RestartSafety { IDEMPOTENT, EXTERNALLY_FENCED, TRANSACTIONAL }

@FunctionalInterface public interface Processor { Result process(Work work) throws Exception; }
@FunctionalInterface public interface Producer  { Expansion produce(Production production) throws Exception; }

public interface Work {            // thread-confined, revocable; no raw file handles
  Records.Input input();           // admitted input commitment
  Records.WorkKey key(); long attempt(); long generation(); String owner();
  int bufferLimit();
  void check() throws Exception;   // current owner policy, ancestor fences, attempt, lease, deadline
  void renew() throws Exception;   // cooperative lease renewal, never a new wire attempt
  int readInput(byte[] b, int off, int len) throws Exception;          // -1 at verified EOF
  void beginOutput(long length, String contentType) throws Exception;  // funded by admission budget
  void writeOutput(ByteBuffer bytes) throws Exception;
  int finishOutput() throws Exception;                                  // returns index; install != publish
  // mode 1 (caller-expanded) reassembly over the parent's own closed successful child scope:
  ChildPage children(long afterEntity, int limit) throws Exception;     // record ChildPage(List<Records.WorkKey> members, boolean more)
  Records.Output beginChildOutput(long entity, int index) throws Exception;
  int readChildOutput(byte[] b, int off, int len) throws Exception;
  void finishChildOutput() throws Exception;
}
public interface Production {      // mode 2 authority expansion, parent-fenced
  Records.Input input(); long childScope(); int bufferLimit();
  void check() throws Exception; void renew() throws Exception;
  int readInput(byte[] b, int off, int len) throws Exception;
  Records.OperationReceipt declare(Records.OperationId op, List<Long> members, boolean seal) throws Exception;
  Optional<Records.OperationReceipt> beginInput(Records.OperationId op, Records.AdmitParameters p) throws Exception;
  void writeInput(ByteBuffer bytes) throws Exception;
  Records.OperationReceipt finishInput() throws Exception;
}
public record Result(boolean success, boolean retryable, Records.Diagnostic diagnostic)
  { static Result succeeded(); static Result failed(long code, String detail); static Result retryable(long code, String detail); }
public record Expansion(Disposition disposition, Records.Diagnostic diagnostic)
  { enum Disposition { COMPLETE, YIELD, FAILED, RETRYABLE } ... }
```

Callbacks run on bounded host worker threads, outside metadata transactions and
never on a Netty event loop. Publication rechecks owner policy, revocation,
ancestor fences, attempt, deadline and the worker lease at commit. Multiple
physical invocations are possible; the declared `RestartSafety` is the
application's promise, not the host's.

Reference applications shipped for tests and cross-language runs (labels match
the Rust CLI so one scenario can target either authority): `copy/v2` (mode 0),
`consume/v2` (mode 0, zero outputs), `retry-copy/v2` (mode 0; attempt 1 reports
retryable), `reassemble/v2` (mode 1), `chunk-copy/v2` (mode 2, 65,536-byte
chunks, ≤256 children). They are exposed as `ReferenceApplications.all()` and
individually.

### 1.2 Owner policy and clock

```java
public interface OwnerPolicy {
  boolean authorized(String owner);                        // current retained-grant gate (workers, publication, reads)
  boolean applicationPermitted(String owner, String application);
  boolean skipPermitted(String owner);                     // Section 12.6 explicit skip authorization
  static OwnerPolicy fromPrincipals(Supplier<Map<Records.Digest,String>> mapping, boolean allowSkip);
}
public interface UtcClock { AdmissionStore-equivalent sample: record Sample(long utcMillis, boolean trusted); Sample sample();
  static UtcClock system(boolean trusted); }
```

`OwnerPolicy.fromPrincipals` treats an owner as authorized while at least one
configured credential still maps to it; live remapping/removal denies further
commits. The TLS guard's `requireOwner()` supplies the per-connection check; the
host wraps both into a thread-safe `SessionStore.Access` that is re-evaluated on
every storage worker call and at every commit edge.

### 1.3 `DurableServer` (public, `AutoCloseable`)

```java
public static DurableServer start(InetSocketAddress bind, TlsAuthentication authentication,
    DurableHost host, DurableOptions options) throws InterruptedException;
public InetSocketAddress address();      // actual bound address (ephemeral port allowed)
public CompletionStage<Snapshot> snapshot();
@Override public void close();           // stop acceptance, drain per §1.4; never a work outcome
```

`DurableOptions` = `CoreOptions` fields + `dataStreams` (per-connection
concurrent incoming inputs and outgoing results, 1..128), `maxDataStreams`
(lifetime ordinal ceiling per connection), `dataSendBytes`, `streamWindowBytes`,
`objectLimit`, `headerTimeoutMs`, `requireDurable` (whether the offer *requires*
65284/65285; default false: profiles offered, unauthenticated callers get Core).
Server offer: supported `[65284, 65285]`; required `[]` unless configured.

Wire behavior implemented by the listener: everything in Section 12 for
SESSION 0..4, SCOPE 0..7, WORK 1..11, RESULT 0..2, DRAIN 0..3, input streams,
result streams, correlated refusals, detach/half-close ordering and COMPLETE
exclusion. Section 12.8 rules exactly: DETACH acknowledged after existing
requests/transfers drain; refusals after DETACH consume IDs; server sends control
FIN only after every preceding response is written; graceful close is left to
the client.

### 1.4 Shutdown dependency order (documented and tested)

1. Listener stops accepting connections; existing connections refuse new
   requests (`NOT_READY`), in-flight responses/transfers continue.
2. Scheduler stops discovery/dispatch; retention stops new pages.
3. Connection-local waits and transfers are drained or explicitly failed
   (`CANCELLED`) within the negotiated lifetimes; tickets stay charged until
   physical owners return.
4. Await every busy worker/read/write owner (bounded by
   `DurableOptions.shutdownTimeoutMs`); a deadline leaves work recoverable.
5. Release services, then store handles. Stores are never closed under a live
   callback or collector.

### 1.5 Launcher: `ai.pipestream.quic.v2.V2Main` (shipped; separate from legacy `Main`)

```
V2Main init-authority --root DIR --authority LABEL --result-authority HOST:PORT [limits...]
V2Main serve --root DIR --authority LABEL --result-authority HOST:PORT
             --bind HOST:PORT --cert PEM --key PEM --client-ca PEM --principal-map TSV
             --trust-system-clock [--allow-skip] [--ready-file PATH] [--object-limit N]
V2Main next-sequence  <connection args>
V2Main init-client    --journal FILE --authority LABEL --owner LABEL --creation-sequence N [--no-results] [policy ms...]
V2Main client         --journal FILE <connection args> <operation> ...
```

Connection args: `--connect HOST:PORT --server-name NAME --ca PEM --cert PEM --key PEM [--object-limit N]`.
Principal map format is identical to Rust (`sha256\tprincipal` TSV of leaf-DER
SHA-256). `--ready-file` writes `host:port\n` to a new 0600 file only after the
listener is bound and recovery finished; an existing file fails startup. Exit 0
after SIGTERM prints `DRAINED` meaning local owners drained, not work completion.
Argument names are deliberately Rust-compatible where the concept is the same so
one fixture schedule can drive either implementation.

## 2. Client: `DurableClient`, `ClientJournal`, `ResultFiles`

### 2.1 `ClientJournal` (public, SQLite via `BoundedSqlite`, exclusive single owner)

```java
public static ClientJournal initialize(Path file, Intent intent, JournalLimits limits) throws IOException, SQLException;
public static ClientJournal open(Path file, JournalLimits limits) throws IOException, SQLException;
public record Intent(String authority, String owner, long creationSequence, Records.Policy policy, boolean results)
public Intent intent(); public Optional<Messages.Binding> binding();
public List<PendingOperation> unresolved(long after, int limit);   // journaled-before-send, no validated receipt
public Optional<Records.OperationReceipt> receipt(Records.OperationId op);
public Optional<Observed> observedWork(Records.WorkKey work);      // newest validated view + revision
public Optional<Records.Manifest> manifest(Records.WorkKey work, long attempt);
public Optional<Selection> selection(Records.WorkKey, long attempt, int index);
public Optional<ScopeEvidence> scope(long scope);                  // producer, parent, verified sealed membership, summary
```

Journal rules: creation intent is committed before the first CAPABILITIES
offer; every mutation's operation ID + immutable parameters are committed
before the frame is written; receipts/views/pages/summaries/manifests are
validated (identity, recomputed request digest, typed outcome, known
commitments) and committed before the API reports them; a connection loss or
process death leaves the operation `unresolved`, never new work. Bounded:
`JournalLimits(maxOperations, maxObservations, maxObservationBytes, files)`
defaults 4096 / 4096 / 64 MiB / same SQLite caps as the authority.

### 2.2 `DurableClient` (public, async, one connection + one journal)

```java
public static DurableClient connect(InetSocketAddress remote, TlsAuthentication authentication,
    ClientJournal journal, ClientOptions options) throws InterruptedException;
public CompletionStage<Messages.Binding> binding();          // replay create or attach per journal; validated+journaled
public CompletionStage<Long> nextSequence();                 // no session required beyond Core+durable
public CompletionStage<Records.OperationReceipt> declare(Records.OperationId op, long scope, List<Long> entities, boolean seal);
public CompletionStage<Records.OperationReceipt> admit(Records.OperationId op, Records.AdmitParameters p, InputSource input);
public CompletionStage<Records.OperationReceipt> lookup(Records.OperationId op);
public CompletionStage<Observed> watch(Records.WorkKey work, long afterRevision, long waitMs);
public CompletionStage<ScopePage> page(long scope, long afterEntity, int limit);   // verifies + accumulates seal
public CompletionStage<Records.ScopeSummary> checkpoint(long scope, Records.Digest seal, long waitMs);
public CompletionStage<Records.OperationReceipt> cancelScope(Records.OperationId op, long scope);
public CompletionStage<Records.OperationReceipt> retry(Records.OperationId op, Records.WorkKey work, long expectedAttempt);
public CompletionStage<Records.OperationReceipt> cancel(Records.OperationId op, Records.WorkKey work);
public CompletionStage<Records.OperationReceipt> skip(Records.OperationId op, Records.WorkKey work);
public CompletionStage<Records.Manifest> manifest(Records.WorkKey work, long attempt);   // journaled selection source
public CompletionStage<Delivered> read(Records.WorkKey work, long attempt, int index, ResultFiles.Destination destination);
public CompletionStage<Records.ScopeSummary> complete();     // exact journaled root summary
public CompletionStage<Void> detach(); public CompletionStage<Void> closed(); @Override public void close();
```

`InputSource.file(Path)` opens one stable handle, prehashes, then streams the
same handle (length/digest fixed before send). A refusal completes the stage
with `ProtocolError` carrying the named code. `read` verifies header, bytes,
length, SHA-256 and FIN before installing the destination; `ResultFiles`
provides bounded owned staging (`Destination.newFile(Path)` never overwrites;
`Destination.managed(root)` for a quota-bounded owned copy store), crash-safe
cleanup on exclusive reopen, and reports `Delivered(local=false)` versus a
`ResultFiles.localCopy(...)` hit (`local=true`) so applications distinguish a
local copy from a newly authorized remote transfer.

Parent/child rules: views/pages are checked in both arrival orders; pending
relationships are retained bounded and rechecked when the other side arrives;
contradictions are `INTEGRITY_ERROR` and never overwrite validated evidence.

Client CLI operations (via `V2Main client`): `binding`, `declare`, `admit`,
`replay`, `lookup`, `unresolved`, `watch`, `page`, `checkpoint`, `complete`,
`detach`, `retry`, `cancel`, `skip`, `cancel-scope`, `select`, `read`,
`manifest`. Names and arguments mirror the Rust CLI guide
(`implementations/rust-quinn/docs/v2-cli.md`) where the concept exists.

## 3. Test-only fixture adapter (for Kimi's driver)

Not reachable from `V2Main`. Separate entry point
`ai.pipestream.quic.v2.fixture.FixtureMain` with the same `serve`/`client`
arguments plus:

```
--fixture-events FILE      bounded UTF-8 TSV event log, records appended atomically (write temp + rename per record batch is NOT used; each record is one write+fsync of a complete line; truncated/partial lines are invalid by contract)
--fixture-schedule FILE    frozen schedule TSV (Kimi's schema; my parser accepts columns: version, run_id, scenario_id, target, boundary, action, seed, deadline_ms)
--fixture-run RUNID --fixture-scenario ID
```

Event TSV columns (v1, exact order):
`version` (`1`), `run_id`, `scenario_id`, `subject_lang` (`java`), `subject_role`
(`server`|`client`), `process_start_id` (pid-`start_nanos` hex), `seq` (decimal,
per process from 1), `boundary`, `operation_id` (32 hex or empty), `work_key`
(`scope:producer:entity` or empty), `attempt` (decimal or empty),
`refusal_code` (1..18 or empty), `artifact_path` (relative to events dir or
empty), `artifact_len` (decimal or empty), `artifact_sha256` (64 hex or empty).
Labels are escaped: tab→`\t`, newline→`\n`, backslash→`\\`.

Boundaries reported (server): `LISTENING`, `CONNECTION_AUTHENTICATED`,
`SESSION_COMMITTED`, `SESSION_RESPONSE_SENT`, `DECLARATION_COMMITTED`,
`DECLARATION_RESPONSE_SENT`, `INPUT_INSTALLED`, `ADMISSION_COMMITTED`,
`ADMISSION_RESPONSE_SENT`, `EXECUTION_CLAIMED`, `OUTPUT_INSTALLED`,
`PUBLICATION_COMMITTED`, `RETRY_COMMITTED`, `FENCE_COMMITTED` (cancel/skip/
scope-cancel), `CLOSURE_COMMITTED`, `RESULT_HEADER_SENT`, `RESULT_FIN_SENT`,
`COMPLETE_RESPONSE_SENT`, `DETACH_ACKNOWLEDGED`, `REFUSAL_SENT`, `SHUTDOWN_DRAINED`.
Client: `INTENT_JOURNALED`, `REQUEST_SENT`, `RECEIPT_VALIDATED`,
`RECEIPT_JOURNALED`, `OBSERVATION_JOURNALED`, `RESULT_VERIFIED`,
`RESULT_INSTALLED`, `REFUSAL_RECEIVED`.

Actions accepted from a schedule row: `pause` (block that boundary until the
release file `<events dir>/release-<seq>` appears or `deadline_ms` elapses,
then continue), `drop-reply` (server only: the committed response is withheld
and the connection is closed with `CONTROL_RESET`; this is the lost-ACK
boundary), `exit` (`Runtime.halt(137)` immediately after the committed
boundary, before any reply is written). Hooks cannot forge commits, receipts,
callbacks or results; they only pause/report/halt at the real boundary the
production code reached. `SENT` boundaries are recorded after Netty native
write acceptance, never claimed as peer receipt.

The schema hash for this section is recorded on the board once Kimi's
`interface-v1.md` is published; I will adopt Kimi's column names/order if they
differ, with the version bumped in this file.

## 4. mTLS setup used by tests and cross-language runs

Tests generate temporary EC P-256 CA/server/client certificates with `openssl`
(same helper as `V2CoreServerTest`/`V2TlsTest`); server SAN `DNS:localhost,
IP:127.0.0.1`; client EKU `clientAuth`. Principal map rows are SHA-256 of the
leaf DER. Rust peer launched from
`implementations/rust-quinn/target/release/pipestream-quinn` (release, `--locked`).

## 5. Explicit non-goals of this document

No wire/schema change, no new profile identifier, no protocol behavior beyond
Section 12 / Appendix F. CLI argument names may differ from Rust where noted;
the adapter documents them instead of adding protocol.
