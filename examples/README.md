# External demonstrations

These examples are process orchestrators, not a fourth protocol
implementation. They call the standalone programs and inspect only their
documented command contract and output artifacts.

- `java-to-rust`: Java/Netty client to Rust/Quinn server transfer.
- `rust-to-cpp-recovery`: durable application-profile replay from Rust to C++.
  It does not claim QUIC connection resumption or Layer 2 continuation.
- `three-node-scatter`: one Java, one Rust, and one C++ server receive children
  with a common parent identity, followed by checksum-checked reassembly.

Build the implementations first, then run all demonstrations:

```bash
python3 conformance/run_interop.py --build
python3 examples/run_all.py
```
