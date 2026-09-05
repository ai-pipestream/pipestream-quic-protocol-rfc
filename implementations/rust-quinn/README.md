# Rust protocol prototype

The Rust implementation exercises selected PipeStream Layer 0, Layer 1,
and Layer 2 behaviors. It is not feature-complete or fully conformant.
The specification is authoritative; codecs and vectors must be corrected
when they contradict it. See [draft-04 readiness](../../docs/standards/draft04-readiness.md)
for tested requirements and remaining design and implementation gaps.

The workspace separates protocol behavior from transport and deployment:

- `pipestream-core` contains the independent wire codec, entity and scope state
  machines, manifests, completion policies, checkpoints, claim state, lineage
  digests, and persistence interfaces. It has no Quinn dependency.
- `pipestream-quinn` contains TLS 1.3 and ALPN setup, QUIC stream handling,
  out-of-order chunk reassembly, the reusable client, and the embeddable server.
- `pipestream-server` builds the `pipestream-quinn` command-line server and
  scenario client.
- `pipestream-conformance` is a protocol-neutral process driver. It does not
  depend on any PipeStream crate and is not a fourth implementation.

```bash
cargo test --locked
cargo build --release --locked
```

The original `serve` and `send` commands retain the common Layer 0 black-box
contract documented under `conformance/`. The durable server uses:

```bash
target/release/pipestream-quinn serve-recursive \
  --bind 127.0.0.1:9443 \
  --cert server.crt --key server.key \
  --state-db state/sessions.sqlite3 \
  --entity-dir state/entities
```

`serve-recursive` accepts concurrent connections with a configurable bound,
enforces scope depth, entity-count, frame, chunk-count, and payload limits, and
persists state transitions before reporting them. SQLite runs in WAL mode with
full synchronous durability and checksummed, versioned session records. Payload
objects and final lineage digests are immutable, fsynced files. Startup resumes
claims that were durably redeemed but interrupted before completion.

The public `RecursiveService`, `RecursiveServer`, `RecursiveClient`,
`EntityProcessor`, `EntityStore`, and `SessionStore` APIs support embedding the
same behavior without the command-line process. Applications provide processing
and storage behavior while the service owns protocol transitions and durable
state.

## Sealed work sets

`RecursiveClient::connect_sealed` requires private-use extension 65281
(`sealed-work-sets-v1`), Layer 1, and no Layer 2. `serve-recursive` supports
it without requiring it from legacy clients. The profile is defined in
Section 9.8, not just by these APIs.

The client calls `declare_work` with a `work_set::WorkSetFrame` before
sending any declared entity. Start with root scope 0, sequence 0, a stable
nonzero 16-octet producer label, and a unique session ID. Batches contain
up to 256 strictly increasing IDs. The final batch sets `SEAL` and includes
`work_set::seal_digest` over the entire set. `declare_work` waits for and
compares the durable ACK. IDs cannot be removed or reused after declaration.
The producer label is not an authentication credential.

Child declarations require an admitted DEHYDRATING parent. Scope digests
and checkpoints wait for the immutable set, including missing declared
entities. Checkpoints name the inclusive largest declared ID in their scope;
GOAWAY names the largest root ID after an acknowledged root checkpoint.
Payloads can arrive out of ID order after their declaration ACK.

After a connection loss, connect with the same profile and replay the original
root sequence-0 request to attach to the retained session. Identical batches
replay the same ACK; changed identities, sequences, or seals are refused.
This is declaration replay, not automatic retry or recovery of application
effects. A rejected or missing payload remains outstanding. No cancellation
tombstone, authenticated claim redemption, or server-originated work is
implemented in this profile.

Stored session format is now version 5, including durable owner, revocation,
execution-attempt state, and typed job records. Versions 1 through 4 are refused without conversion or
modification; preserve old databases with their matching binary.
The implementation caps each session at 1,000,000 declared IDs, in addition
to negotiated per-scope limits. SQLite still serializes the whole session
on each transaction, and final sealing walks the full identifier set.
These bounds are not a throughput or large-session performance claim.

## Mutual TLS and session ownership

Configure all three authentication settings together:

```bash
target/release/pipestream-quinn serve-recursive \
  --bind 127.0.0.1:9443 --cert server.crt --key server.key \
  --state-db state/sessions.sqlite3 --entity-dir state/entities \
  --client-ca client-ca.crt --authority example-authority \
  --principal-map principals.tsv
```

`principals.tsv` starts with `sha256<TAB>principal`, followed by one hexadecimal
SHA-256 fingerprint of a DER client leaf certificate and its stable principal
per row. Trust-chain validation and certificate proof of possession still
apply; a fingerprint is not a credential. The map permits 1..4096 certificates;
multiple certificates may map to one principal for rotation. Authority and
principal identifiers contain 1..128 ASCII alphanumeric or `-._~` characters.

Recursive client commands accept `--client-cert` and `--client-key` together.
The library uses `RecursiveClientOptions::identity` and `ClientIdentity`;
embedded servers call `RecursiveService::with_authentication` before binding.
Both sides require private-use extension 65282, `authenticated-session-v1`.
A configured client refuses an anonymous server instead of dropping its
authentication requirement. No new recovery extension is advertised.
TLS session resumption is disabled on this path so reconnects recheck client
credentials; 0-RTT remains disabled on every path.

The first admission atomically records the principal and issuing authority.
All subsequent mutations check ownership and revocation inside the session
transaction. `Session::revoke_access` is an operator API to invoke through the
store's transaction mechanism; it disables live/reconnected access and
background recovery. An unprotected listener sharing the database cannot
access a bound session. An authenticated listener does not adopt anonymous
sessions. Neither producer labels nor metadata identify the caller.

Principal maps are loaded at startup. Reconfigure them to withdraw certificates
from future connections; revoke a session to deny its existing connections.
This is not per-claim revocation, online certificate-status checking, or
portable recovery between unrelated authorities.

## Other prototype paths and limitations

The Layer 1 end-to-end scenario is runnable with `recursive-scenario`. It sends
one root, three children, and two grandchildren; completes descendants out of
order; verifies nested scope digests; crosses scope barriers and checkpoints;
rehydrates both parents; and persists a deterministic final lineage digest.
`begin-yield` and `redeem` exercise disconnect, cross-server claim redemption,
single-use replay refusal, and recovery.

All transport paths require TLS 1.3 with ALPN `pipestream/1`; 0-RTT is never
enabled. The implemented Layer 2 scope is intentionally narrow. Automatic retry
scheduling, claim federation between unrelated persistence domains, and the
other optional resilience behaviors are not claimed.

## File-backed receive payloads

The recursive receiver reads and validates the bounded CBOR header first,
then writes payload octets in 8 KiB pieces to temporary files. Measured length
and SHA-256 are checked at FIN before admission. It retains up to eight stream
readers per connection. QUIC receive windows are explicitly 1 MiB per connection
and 64 KiB per stream; these flow-control windows are not a total memory limit.

`ProcessContext::payload` is now a file-backed `spool::Payload`, not a byte
slice. `reader()` implements `std::io::Read`; `len()` and `digest()` describe the
validated input. Processing returns `Result<ProcessingDisposition, ProtocolError>`
so read failures cannot become successful processing. The exemplar hashes input
through an 8 KiB buffer. Chunk assemblies retain ordered file segments and
verify each segment before calculating the combined digest. They do not build
a contiguous payload buffer or a second assembled temporary file.

`FileEntityStore::open_with_spool_limits` configures temporary receive quotas:

| Scope | Bytes | Files |
|---|---:|---:|
| Store directory, shared by handles in one process | 256 MiB | 4,096 |
| Authenticated authority/principal, across connections | 128 MiB | 1,024 |
| Connection | `max_entity_bytes` | 512 |

At most 1,024 principal budget entries may be active; anonymous connections
share one identity bucket. A payload reader or clone retains its disk credit.
Empty files consume file credit. Exhaustion is `PIPESTREAM_LIMIT_EXCEEDED`,
not an unbounded wait while incomplete items hold all the capacity. File I/O
owns its credit until it finishes even if the receiver is cancelled. Failed
cleanup retains credit instead of claiming disk space was reclaimed.

Restart counts abandoned files against the store quota without deleting them.
Live handles for the same directory share accounting and cannot reset it with
different limits. Accounting is not coordinated between separate operating
system processes. Temporary receive budgets do not limit retained entity files,
SQLite state, or the filesystem cache. Durable storage quotas, restartable job
descriptors, and explicit orphan reclamation remain required before production
multi-tenant use. Do not run multiple writer processes against this spool root.

`cargo test -p pipestream-quinn --test spool_resources -- --nocapture` sends
32 MiB over real QUIC without allocating an input-sized client buffer. It gates
instrumented Rust heap growth below 12 MiB and individual allocations below
4 MiB, verifies the persisted digest, and requires temporary disk credit to
return to zero. One local run on 2026-09-05 measured 132,968 bytes of heap growth
and a largest allocation of 15,972 bytes. This is a single-transfer allocation
measurement, not a process-RSS, concurrency, or throughput claim.

## Execution still to finish

### Durable queue APIs

The core provides `Session::enqueue_job`, `acquire_job`, `publish_job`, and
`refuse_job` for processing, rehydration, and resume operations. Invoke these
through a store transaction. Inputs retain the validated header, measured
length and digest, negotiated layers, or the specific closed scope/claim.
Publication retains the outcome together with the execution fence and computed
protocol state. Input replay cannot replace the original descriptor, and saves
cannot remove a retained job or rewrite a terminal outcome. An application
refusal is retained separately from entity completion.

`SqliteSessionStore::open_with_job_limits` sets database-wide unfinished-job
limits. Defaults are 128 queued/running jobs globally and 32 per authority and
principal; anonymous work shares one bucket. Limits persist across reopen, and
a handle cannot silently replace them. Queue admission and the session revision
commit together. Exhaustion returns `PIPESTREAM_LIMIT_EXCEEDED` and rolls both
back, including through `create` and `save`. Revoked work remains charged but
is not returned for execution.

`ready_jobs(now, limit)` uses a bounded SQLite index. An unexpired attempt is
not returned; lease expiry makes it discoverable again but does not grant
execution. `acquire_job` still checks authorization and the durable fence.
`integrity_check` audits queue rows against checksummed session records in one
read snapshot, including missing and extra entries. This full audit scans one
session at a time, not an in-memory list of all sessions. It is an explicit
operation, not a periodic background task.

These core APIs are tested but not yet used by the transport service. They do
not reopen retained payload files or provide workers, cancellation, scheduling,
or recovery-request ACKs. Finished records and permanent storage are not covered
by unfinished-job limits. The full executor must integrate admission and file
installation, verify the queue before dispatch, and observe outcomes without
blocking control parsing. No asynchronous service claim is made here.

### Current service path

Pending checkpoints use monotonic deadlines, but a synchronous callback still
blocks the connection's dispatch loop. Application processing, rehydration,
and resume callbacks now run outside database transactions and may re-enter
the store. Each runs with a durably acquired `ExecutionLease`. Publication
atomically checks the session owner, revocation, operation, epoch, executor
identity, and expiry before applying its result and marking the attempt done.
Expired or superseded attempts cannot publish, including after reopening the
database. An active attempt prevents another store handle from acquiring it.

The default lease is 300 seconds; embedded services can use
`with_execution_lease` to choose 1 microsecond through 300 seconds. Lease
expiry rejects publication; it does not cancel a callback, bound its memory,
or renew automatically. The issuer uses Unix microseconds, so clock changes
can delay recovery or expire work early. Epoch checks remain necessary even
when the clock moves backward. Applications must use idempotency or enforce
their own transactional fence for external effects. A lease is not a wire
credential and does not prove exactly-once execution.

Resume recovery can reacquire expired attempts through the existing recovery
entry point. There is not yet a periodic durable job dispatcher, automatic
processing/rehydration replay, or retained recovery-request outcome. In
particular, the service's persisted processing attempts do not yet retain a reconstructible
header/spool descriptor. An interrupted admission remains incomplete, never
successful. The next execution increment must cover those restart boundaries.

Without the explicit mutual-TLS settings, the standalone prototype authenticates
only the server and remains suitable solely for trusted local demonstrations.
Even with mutual TLS, per-principal resource gates, retained recovery outcomes,
asynchronous dispatch, durable storage quotas and restartable inputs, and a complete resilience capability remain
unfinished. It MUST NOT yet be described as a production multi-tenant durable
work service. Its Layer 2 boolean still advertises more than the tested subset.

The core `uri` module parses typed `pipestream://` session, entity, and claim
locators with explicit ports. Parsing grants no access and does not perform
network I/O. The scheme is proposed, not registered by this repository.
