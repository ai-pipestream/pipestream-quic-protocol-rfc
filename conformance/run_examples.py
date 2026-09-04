#!/usr/bin/env python3
"""Run language-native examples against independent server processes."""

from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path

from process_support import (
    certificates,
    finish_server,
    java_example_command,
    run_checked,
    rust_example_command,
    start_server,
    stop_server,
)


def java_to_rust() -> None:
    with tempfile.TemporaryDirectory(prefix="pipestream-java-example-") as directory:
        root = Path(directory)
        certs = root / "certs"
        certificates(certs)
        payload = root / "input.bin"
        payload.write_bytes(b"Java API example\x00" + bytes(range(256)) * 3)
        server = start_server("rust-quinn", root, certs)
        try:
            result = run_checked([
                *java_example_command(),
                "--connect", server.address,
                "--ca", str(certs / "ca.crt"),
                "--server-name", "localhost",
                "--entity-id", "101",
                "--input", str(payload),
            ])
            finish_server(server)
        finally:
            stop_server(server)
        if b"JAVA EXAMPLE COMPLETE entity=101" not in result.stdout:
            raise RuntimeError("Java example did not report application completion")
        if (server.output / "101.bin").read_bytes() != payload.read_bytes():
            raise RuntimeError("Java example payload differs after Rust receipt")
    print("PASS Java source -> Rust/Quinn server")


def rust_to_cpp_recovery() -> None:
    with tempfile.TemporaryDirectory(prefix="pipestream-rust-recovery-") as directory:
        root = Path(directory)
        certs = root / "certs"
        certificates(certs)
        payload = root / "input.bin"
        payload.write_bytes(b"Rust durable recovery example\x00" + bytes(reversed(range(256))))
        journal = root / "sender.journal"
        command = rust_example_command("rust-to-cpp-recovery")
        run_checked([
            *command,
            "prepare",
            "--journal", str(journal),
            "--input", str(payload),
            "--entity-id", "201",
        ])
        server = start_server("cpp-msquic", root, certs)
        try:
            result = run_checked([
                *command,
                "recover",
                "--journal", str(journal),
                "--input", str(payload),
                "--connect", server.address,
                "--ca", str(certs / "ca.crt"),
            ])
            finish_server(server)
        finally:
            stop_server(server)
        if b"RUST RECOVERY COMPLETE entity=201" not in result.stdout:
            raise RuntimeError("Rust recovery example did not report durable completion")
        if (server.output / "201.bin").read_bytes() != payload.read_bytes():
            raise RuntimeError("Rust recovery payload differs after C++ receipt")
        replay = subprocess.run(
            [
                *command,
                "recover",
                "--journal", str(journal),
                "--input", str(payload),
                "--connect", server.address,
                "--ca", str(certs / "ca.crt"),
            ],
            cwd=root,
            capture_output=True,
            timeout=10,
        )
        if replay.returncode == 0 or b"journal is already complete" not in replay.stderr:
            raise RuntimeError("completed Rust recovery journal allowed a second replay")
    print("PASS Rust source recovery -> C++/MsQuic server")


def three_node_scatter() -> None:
    with tempfile.TemporaryDirectory(prefix="pipestream-rust-scatter-") as directory:
        root = Path(directory)
        certs = root / "certs"
        certificates(certs)
        payload = root / "input.bin"
        payload.write_bytes(b"Rust scatter coordinator\x00" + bytes(range(256)) * 17)
        servers = []
        try:
            for name in ("java-netty", "rust-quinn", "cpp-msquic"):
                servers.append(start_server(name, root, certs))
            java, rust, cpp = servers
            result = run_checked([
                *rust_example_command("three-node-scatter"),
                "--input", str(payload),
                "--ca", str(certs / "ca.crt"),
                "--java-server", java.address,
                "--java-output", str(java.output),
                "--rust-server", rust.address,
                "--rust-output", str(rust.output),
                "--cpp-server", cpp.address,
                "--cpp-output", str(cpp.output),
            ])
            for server in servers:
                finish_server(server)
        finally:
            for server in servers:
                stop_server(server)
        if b"RUST SCATTER COMPLETE parent=77 entities=301,302,303" not in result.stdout:
            raise RuntimeError("Rust scatter example did not report reassembly completion")
    print("PASS Rust source scatter -> Java, Rust, and C++ servers")


def main() -> int:
    java_to_rust()
    rust_to_cpp_recovery()
    three_node_scatter()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
