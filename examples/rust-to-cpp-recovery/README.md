# Rust to C++ recovery

This is a Rust application. Its [`main.rs`](src/main.rs) writes a durable binary
sender journal, validates the staged payload digest after a simulated process
boundary, calls the reusable Quinn transport, waits for the checkpoint-confirmed
Layer 0 completion, and atomically marks the journal complete. The receiving
process can be the C++/MsQuic server or any conforming Layer 0 server.

This is deliberately described as application-profile recovery, not QUIC 0-RTT,
TLS session resumption, or a Layer 2 PipeStream continuation. Layer 0 does not
define cross-connection resume. The example shows how a product can recover on
top of the stable Layer 0 entity contract without claiming wire semantics that
the RFC does not provide.

The recovery profile is at-least-once. A process can fail after the remote
checkpoint is acknowledged but before the local journal is marked complete.
On restart it will replay the same Entity ID and payload, so a receiver using
this profile must make that pair idempotent. This example does not claim
exactly-once delivery.

```sh
cargo build --release --locked

target/release/rust-to-cpp-recovery prepare \
  --journal sender.journal --input input.bin --entity-id 201

target/release/rust-to-cpp-recovery recover \
  --journal sender.journal --input input.bin \
  --connect 127.0.0.1:9443 --ca /path/to/ca.crt
```

The `pipestream-conformance examples` command starts the C++ server and invokes
this Rust program during the full repository gate. The driver performs process
and artifact assertions; the recovery behavior is Rust.
