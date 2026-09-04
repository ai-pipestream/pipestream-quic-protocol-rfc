# Three-node scatter and reassembly

This demo launches one Java/Netty, one Rust/Quinn, and one C++/MsQuic server.
An external coordinator divides a root entity into three immutable children,
sends each child to a different process with the same Layer 0 `parent-id`, waits
for each checkpoint-confirmed completion, and reassembles the children in Entity
ID order. Each server independently validates its child's SHA-256 checksum.

```sh
python3 conformance/run_interop.py --build
python3 examples/three-node-scatter/run.py [INPUT]
```
