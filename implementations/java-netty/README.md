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

The Netty listener and public network client still expose only Layer 0 and do
not advertise `sealed-work-sets-v1`. Connection codecs/state, incremental entity
and chunk ingestion, pending checkpoints, GOAWAY, and real Java-to-Rust and
Rust-to-Java recursive/reconnect tests remain required. Passing the storage
tests is not evidence of cross-language sealed-work interoperability.
