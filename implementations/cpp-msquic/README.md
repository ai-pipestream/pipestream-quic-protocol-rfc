# C++ / MsQuic reference implementation

This implementation has an independent C++20 Layer 0 codec and uses Microsoft's
MsQuic transport. CMake fetches the immutable MsQuic `v2.6.1` tag and builds it
against the system OpenSSL 3.5 installation. It does not share protocol code
with the Java/Netty, Rust/Quinn, or Python conformance implementations.

```sh
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build
ctest --test-dir build --output-on-failure
```

The standalone CLI follows the common reference-suite interface:

```sh
build/pipestream-msquic serve --cert server.crt --key server.key \
  --output-dir received --bind 127.0.0.1:9443

build/pipestream-msquic send --connect 127.0.0.1:9443 --ca ca.crt \
  --server-name localhost --entity-id 1 --parent-id 42 --input input.bin
```

`pipestream_wire` and `pipestream_transport` are reusable shared libraries. The
transport implements deterministic CBOR capability negotiation, one
SHA-256-protected entity, checkpoint acknowledgement, cursor advancement, and
GOAWAY. It currently implements Layer 0 only and never enables QUIC 0-RTT.
