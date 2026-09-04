#!/usr/bin/env python3
"""Replay durable Rust sender state to a C++/MsQuic server after interruption."""

from __future__ import annotations

import argparse
import hashlib
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from support import certificates, finish_server, send_entity, start_server, stop_server  # noqa: E402


def prepare_interrupted_state(journal: Path, payload: Path, entity_id: int) -> None:
    digest = hashlib.sha256(payload.read_bytes()).hexdigest()
    journal.write_text(f"pending\t{entity_id}\t{digest}\t{payload.resolve()}\n", encoding="utf-8")


def recover(journal: Path, root: Path, certs: Path) -> None:
    state, raw_id, digest, raw_path = journal.read_text(encoding="utf-8").rstrip("\n").split("\t")
    if state != "pending":
        raise RuntimeError("journal has no interrupted entity")
    payload = Path(raw_path)
    if hashlib.sha256(payload.read_bytes()).hexdigest() != digest:
        raise RuntimeError("staged entity changed after interruption")
    entity_id = int(raw_id)
    server = start_server("cpp-msquic", root, certs)
    try:
        send_entity("rust-quinn", server, certs, entity_id, payload)
        finish_server(server)
    finally:
        stop_server(server)
    if (server.output / f"{entity_id}.bin").read_bytes() != payload.read_bytes():
        raise RuntimeError("replayed payload differs from staged entity")
    journal.write_text(f"complete\t{entity_id}\t{digest}\t{payload.resolve()}\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", nargs="?", type=Path)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="pipestream-rust-cpp-recovery-") as temporary:
        root = Path(temporary)
        payload = args.input
        if payload is None:
            payload = root / "input.bin"
            payload.write_bytes(b"durably staged entity before transport interruption\n")
        journal = root / "sender.state"
        prepare_interrupted_state(journal, payload, 201)
        certs = root / "certs"
        certificates(certs)
        recover(journal, root, certs)
        if not journal.read_text(encoding="utf-8").startswith("complete\t201\t"):
            raise RuntimeError("recovery journal did not reach complete")
        print("PASS Rust/Quinn durable replay -> C++/MsQuic server")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
