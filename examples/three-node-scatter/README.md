# Three-node scatter and reassembly

This is a Rust coordinator application. Its [`main.rs`](src/main.rs) divides a
root entity into three immutable children, calls the reusable Quinn client to
send them concurrently to independent Java/Netty, Rust/Quinn, and C++/MsQuic
servers, waits for each checkpoint-confirmed completion, verifies the shared
Layer 0 `parent-id`, and reassembles the result in Entity ID order.

```sh
cargo build --release --locked

target/release/three-node-scatter \
  --input input.bin --ca /path/to/ca.crt \
  --java-server 127.0.0.1:9441 --java-output /tmp/java-received \
  --rust-server 127.0.0.1:9442 --rust-output /tmp/rust-received \
  --cpp-server 127.0.0.1:9443 --cpp-output /tmp/cpp-received
```

The three servers must already be running with the output directories supplied
above. `conformance/run_examples.py` starts those processes and invokes this
Rust application during the full repository gate. Reading those output
directories is a local demonstration adapter, not a Layer 0 network gather
operation.
