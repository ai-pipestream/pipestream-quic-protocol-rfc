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

Stored session format is now version 3, including durable owner and revocation
state. Version-1 and version-2 records are refused without conversion or
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

The recursive receiver reads control and data independently, identifies
streams from their headers, and bounds buffered payload octets across
incomplete streams and chunk assemblies. It supports up to eight concurrent
stream readers. This is bounded whole-entity processing, not a spool-backed
incremental payload API. Allocation capacity, decoded metadata, QUIC receive
buffers, and transient reassembly copies add overhead beyond payload bytes.

Pending checkpoints keep processing active and use monotonic deadlines.
Application callbacks remain synchronous and must be bounded. Resume callbacks
execute under an SQLite IMMEDIATE transaction, excluding simultaneous recovery
executors that use the same database. Callbacks must not re-enter the store.
This deliberately serializes writers; long-running applications need an
asynchronous fenced executor. A crash after an external effect but before
commit still requires application idempotency.

Without the explicit mutual-TLS settings, the standalone prototype authenticates
only the server and remains suitable solely for trusted local demonstrations.
Even with mutual TLS, per-principal resource gates, retained recovery outcomes,
fenced asynchronous execution, and a complete resilience capability remain
unfinished. It MUST NOT yet be described as a production multi-tenant durable
work service. Its Layer 2 boolean still advertises more than the tested subset.

The core `uri` module parses typed `pipestream://` session, entity, and claim
locators with explicit ports. Parsing grants no access and does not perform
network I/O. The scheme is proposed, not registered by this repository.
