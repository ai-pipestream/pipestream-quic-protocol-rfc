# Java/Netty implementation

This independent PipeStream Layer 0 implementation uses Netty QUIC and Jackson
CBOR. It publishes a reusable Java library and a shaded standalone client/server
JAR.

```bash
mvn verify
java -jar target/pipestream-quic-netty-0.1.0-SNAPSHOT-all.jar --help
```

The current build uses Netty's `linux-x86_64` native classifier. The client
requires a CA certificate and the server requires an end-entity certificate and
private key. QUIC 0-RTT is never enabled.

The public `PipeStreamClient` and `PipeStreamServer` classes implement the same
transfer used by the CLI: deterministic CBOR capability negotiation, one
SHA-256-protected entity with optional `parent-id`, checkpoint acknowledgement,
cursor advancement, and GOAWAY. They currently implement Layer 0 only.

## Sealed-work library foundation

The independent Java `SealedWork`, `SealedScope`, and `SealedSessionStore`
APIs implement declaration encoding, durable membership, admission, and
recursive closure for Section 9.8. They do not import Rust protocol or state
code. `SealedCbor` handles the sealed Layer 1 schema's deterministic CBOR
types, including the full unsigned 64-bit range; it is not a generic Layer 2
codec. `SealedWorkTest` consumes all 20 frozen declaration inputs, and
`SealedScopeTest` consumes the frozen scope-digest inputs and pins the Merkle
construction, including odd-node promotion.

`SealedSessionStore.open(path)` creates a separate SQLite database with strict
typed tables, WAL, and synchronous FULL commits. Do not point it at a Rust
store. Unknown table sets or policy versions are refused without conversion.
The JDBC driver requires native access; run an embedding application with
`--enable-native-access=ALL-UNNAMED`. Tests enable this explicitly.

- `declare` commits before returning the exact ACK. Replays check retained
  history against the scope's complete membership, identity, sequence, and seal.
- `admit` requires the caller to validate and durably retain the payload first.
  Missing payloads remain declared; no API deletes or recycles identifiers.
- `processed` records COMPLETE, FAILED, or DEHYDRATING after application work.
- `closeScope` returns a durable status summary only after the child scope is
  sealed, every declared entity resolves, and all descendant scopes close.
- `resolveChildren` enforces STRICT: successful children allow REHYDRATING;
  a closed set containing failure instead makes its parent FAILED.
  `rehydrated` records the subsequent application outcome.
- `checkpointReady` checks the durable, whole-scope inclusive cut. It is not
  a checkpoint ACK; the connection must additionally enforce deadlines, exact
  request correlation, outstanding ingress, and nested checkpoints.

All storage calls are blocking and must run outside Netty event loops.
Application callbacks are not executed under storage transactions. Producer
labels are not credentials, and these APIs do not provide executor fencing,
payload storage, authenticated recovery, or exactly-once external effects.

Persistent local policy allows 512 sessions, 65,536 declared entities globally,
and 16,384 per session, in addition to negotiated per-scope and depth limits.
Declaration history and scope trees are checked with bounded scans, not a
constant-time readiness index. These are logical record limits, not physical
database/WAL quotas or a throughput claim. Reopen cannot reset the policy.

The Netty listener and standalone commands still expose only Layer 0 and do
not advertise `sealed-work-sets-v1`. The separate public client below requires
that profile. A sealed server integrating this store with incremental payload
and chunk ingestion, asynchronous processing, pending checkpoints, and GOAWAY
remains required, together with Rust-to-Java recursive/reconnect tests.

## Sealed-work network producer

`SealedClient.connect(remote, caCertificate, serverName, limits, timeout)`
opens real QUIC with server-certificate validation and requires extension
65281, Layer 1, and no Layer 2. It does not retry with weaker capabilities,
accept server-originated work, or enable 0-RTT. This is a separate reusable
API, not a change to the Layer 0 `PipeStreamClient` or CLI.

1. Call `declare` for the root batch, then any subsequent batches. It checks
   every ACK field before remembering membership. Keep the original requests
   for explicit replay after reconnecting.
2. `send(header, path)` streams a regular file through 8 KiB buffers and waits
   for its processing outcome. `sendChunks` streams a complete file-backed
   chunk set in caller order, checking identity and contiguous nonoverlapping
   geometry. It reports one entity lifecycle, not one per chunk.
3. Declare child scopes for dehydrated parents, send descendants, then call
   `closeScope` from the leaves upward. The client verifies the status Merkle
   digest and the parent's returned lifecycle. `barrier` correlates both scope
   and parent identity.
4. Seal membership and request a whole-scope `checkpoint`. Its ACK must preserve
   scope/timeout presence and the full uint64 sequence; known unfinished work
   cannot be accepted as completion. `goaway` requires the acknowledged sealed
   root cut and closed descendants.

Operations are blocking and serialized per client. Run them outside Netty event
loops. The monotonic operation budget bounds network waits, not blocking local
file I/O. The receive backlog is limited to 128 frames and 4 MiB of encoded
frames, with a 1 MiB individual control limit. Local bookkeeping allows 65,536
entities, 65,536 chunks per send, and 1,024 checkpoint identities. These limits
and fixed-size file reads are not a measured whole-process memory bound.

This client does not persist its producer ledger or observed statuses, export
resume tokens, retry payloads, or provide authenticated-session/recovery APIs.
A new client can replay declaration history and send work not previously
admitted. Replaying declarations alone does not reconstruct prior admission or
completion observations for checkpointing already-processed work. A pending
checkpoint blocks this client's next operation; concurrent submission needs
additional client API work. Closing never claims successful completion.

The `sealed-interop` Maven profile runs five actual-QUIC tests, including
Java-to-Rust nested/chunked completion, declaration replay and checkpoint ACK
replay after Rust restarts, a deliberately discarded declaration ACK, and named
refusals for changed ownership labels, lower retained limits, missing seals,
wrong checkpoint bounds, changed ACKs, downgrade, oversized frames, and Layer 2
responses. Scripted test peers inject faults; they are not reference servers.
They do not replace the independent Java server required by the goal.

```bash
cargo build --release --locked --manifest-path ../rust-quinn/Cargo.toml
mvn test -Psealed-interop
```

Default `mvn test` runs the independent Java codec/store tests without requiring
a Rust executable. The repository's `conformance/run_all.sh` explicitly enables
the interoperability profile after building Rust; a missing executable is a
failure, not a skipped integration test.
