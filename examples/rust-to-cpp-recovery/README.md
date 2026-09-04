# Rust to C++ recovery

This demo models an application that durably staged an immutable entity before
its prior transport session was interrupted. A fresh Rust/Quinn client process
validates the staged checksum and replays the same Entity ID to a C++/MsQuic
server, then marks the external sender journal complete.

This is deliberately described as application-profile recovery, not QUIC 0-RTT,
TLS session resumption, or a Layer 2 PipeStream continuation. Layer 0 does not
define cross-connection resume. The example shows how a product can recover on
top of the stable Layer 0 entity contract without claiming wire semantics that
the RFC does not provide.

```sh
python3 conformance/run_interop.py --build
python3 examples/rust-to-cpp-recovery/run.py [INPUT]
```
