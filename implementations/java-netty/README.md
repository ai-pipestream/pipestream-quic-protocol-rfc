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
