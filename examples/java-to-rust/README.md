# Java to Rust transfer

This demo launches the Rust/Quinn server as a separate process and sends one
immutable entity with the Java/Netty client. The processes share only the
published wire contract and test certificate.

```sh
python3 conformance/run_interop.py --build
python3 examples/java-to-rust/run.py [INPUT]
```
