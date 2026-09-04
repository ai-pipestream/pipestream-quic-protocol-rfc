# Rust/Quinn implementation

This is an independent PipeStream Layer 0 implementation using Quinn and
Minicbor. It provides a reusable Rust library and the `pipestream-quinn`
standalone client/server binary.

```bash
cargo test --locked
cargo build --release --locked
```

The executable follows the black-box command contract documented under
`conformance/`. It requires a TLS certificate and never enables QUIC 0-RTT.

The public `transport::serve` and `transport::send` functions implement the
same transfer as the CLI: deterministic CBOR capability negotiation, one
SHA-256-protected entity with optional `parent-id`, checkpoint acknowledgement,
cursor advancement, and GOAWAY. They currently implement Layer 0 only.
