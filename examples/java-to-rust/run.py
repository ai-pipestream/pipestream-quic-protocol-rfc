#!/usr/bin/env python3
"""Transfer one immutable entity from the Netty client to the Quinn server."""

from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from support import certificates, finish_server, send_entity, start_server, stop_server  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", nargs="?", type=Path)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="pipestream-java-rust-") as temporary:
        root = Path(temporary)
        payload = args.input
        if payload is None:
            payload = root / "input.bin"
            payload.write_bytes("Java to Rust over PipeStream QUIC\n東京\n".encode())
        certs = root / "certs"
        certificates(certs)
        server = start_server("rust-quinn", root, certs)
        try:
            send_entity("java-netty", server, certs, 101, payload)
            finish_server(server)
        finally:
            stop_server(server)
        received = server.output / "101.bin"
        if received.read_bytes() != payload.read_bytes():
            raise RuntimeError("received payload differs from input")
        print("PASS Java/Netty client -> Rust/Quinn server")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
