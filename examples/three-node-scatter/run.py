#!/usr/bin/env python3
"""Scatter an entity across all three servers and reassemble it externally."""

from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from support import certificates, finish_server, send_entity, start_server, stop_server  # noqa: E402

SERVERS = ("java-netty", "rust-quinn", "cpp-msquic")
CLIENTS = ("rust-quinn", "cpp-msquic", "java-netty")
PARENT_ID = 77


def split(payload: bytes) -> list[bytes]:
    boundaries = [0, len(payload) // 3, (2 * len(payload)) // 3, len(payload)]
    return [payload[boundaries[index]:boundaries[index + 1]] for index in range(3)]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", nargs="?", type=Path)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="pipestream-three-node-") as temporary:
        root = Path(temporary)
        payload = args.input.read_bytes() if args.input else (bytes(range(256)) * 23 + "東京\n".encode())
        chunks = split(payload)
        certs = root / "certs"
        certificates(certs)
        servers = []
        try:
            for name in SERVERS:
                servers.append(start_server(name, root, certs))
            for index, (server, client, chunk) in enumerate(zip(servers, CLIENTS, chunks, strict=True)):
                entity_id = 301 + index
                chunk_path = root / f"chunk-{index}.bin"
                chunk_path.write_bytes(chunk)
                send_entity(client, server, certs, entity_id, chunk_path, parent_id=PARENT_ID)
            for server in servers:
                finish_server(server)
        finally:
            for server in servers:
                stop_server(server)
        received = []
        for index, server in enumerate(servers):
            entity_id = 301 + index
            if (server.output / f"{entity_id}.parent").read_text(encoding="utf-8").strip() != str(PARENT_ID):
                raise RuntimeError(f"{server.name} lost the parent relationship")
            received.append((server.output / f"{entity_id}.bin").read_bytes())
        if b"".join(received) != payload:
            raise RuntimeError("rehydrated entity differs from the root entity")
        print("PASS three-node scatter, checksum processing, and ordered reassembly")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
