# External demonstrations

These are language-native applications built against the reusable reference
libraries:

- `java-to-rust` is Java 21 source in a standalone Maven project. It imports
  the Java/Netty library and sends to a Rust/Quinn server.
- `rust-to-cpp-recovery` is Rust source in a standalone Cargo project. It owns
  durable sender state and replays through the Quinn library to C++/MsQuic.
- `three-node-scatter` is a Rust coordinator in a standalone Cargo project. It
  scatters to Java, Rust, and C++ servers and performs checked reassembly.

The protocol-neutral Rust driver under `implementations/rust-quinn/conformance`
starts external servers, invokes these compiled programs, and checks process
and filesystem results. It contains no example behavior or protocol codec.

Build and run every implementation, example, interop pair, and scenario with:

```bash
./conformance/run_all.sh
```
