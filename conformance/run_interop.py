#!/usr/bin/env python3
"""Run all client/server pairs as black-box QUIC processes."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Implementation:
    name: str
    command: tuple[str, ...]


def run(command: list[str], *, cwd: Path = ROOT) -> None:
    subprocess.run(command, cwd=cwd, check=True)


def build_all() -> None:
    run(["mvn", "verify", "-q"], cwd=ROOT / "implementations/java-netty")
    run([
        "cargo", "build", "--release", "--locked", "--manifest-path",
        str(ROOT / "implementations/rust-quinn/Cargo.toml"),
    ])
    run([
        "cmake", "-S", str(ROOT / "implementations/cpp-msquic"),
        "-B", str(ROOT / "implementations/cpp-msquic/build"),
        "-G", "Ninja", "-DCMAKE_BUILD_TYPE=Release",
    ])
    run(["cmake", "--build", str(ROOT / "implementations/cpp-msquic/build"), "-j", "4"])
    run(["ctest", "--test-dir", str(ROOT / "implementations/cpp-msquic/build"), "--output-on-failure"])


def implementations() -> list[Implementation]:
    java_jars = sorted((ROOT / "implementations/java-netty/target").glob("*-all.jar"))
    if len(java_jars) != 1:
        raise RuntimeError("expected one shaded Java implementation JAR; run with --build")
    values = [
        Implementation(
            "java-netty",
            ("java", "--enable-native-access=ALL-UNNAMED", "-jar", str(java_jars[0])),
        ),
        Implementation(
            "rust-quinn",
            (str(ROOT / "implementations/rust-quinn/target/release/pipestream-quinn"),),
        ),
        Implementation(
            "cpp-msquic",
            (str(ROOT / "implementations/cpp-msquic/build/pipestream-msquic"),),
        ),
    ]
    for implementation in values:
        executable = implementation.command[0]
        if os.sep in executable and not Path(executable).is_file():
            raise RuntimeError(f"missing {implementation.name} executable; run with --build")
    return values


def wait_ready(process: subprocess.Popen[bytes], ready_file: Path) -> str:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if ready_file.is_file() and ready_file.stat().st_size:
            return ready_file.read_text(encoding="utf-8").strip()
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise RuntimeError(
                f"server exited before readiness ({process.returncode})\n"
                f"stdout:\n{stdout.decode(errors='replace')}\n"
                f"stderr:\n{stderr.decode(errors='replace')}"
            )
        time.sleep(0.025)
    process.terminate()
    process.wait(timeout=5)
    raise RuntimeError("server readiness timed out")


def one_pair(
    server: Implementation,
    client: Implementation,
    root: Path,
    certs: Path,
    payload: Path,
    entity_id: int,
) -> None:
    pair = root / f"{client.name}-to-{server.name}"
    output = pair / "received"
    output.mkdir(parents=True)
    ready = pair / "ready"
    server_process = subprocess.Popen(
        [
            *server.command,
            "serve",
            "--bind", "127.0.0.1:0",
            "--cert", str(certs / "server.crt"),
            "--key", str(certs / "server.key"),
            "--output-dir", str(output),
            "--ready-file", str(ready),
            "--once",
        ],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        address = wait_ready(server_process, ready)
        client_result = subprocess.run(
            [
                *client.command,
                "send",
                "--connect", address,
                "--ca", str(certs / "ca.crt"),
                "--server-name", "localhost",
                "--entity-id", str(entity_id),
                "--input", str(payload),
                "--content-type", "application/octet-stream",
                "--parent-id", "42",
            ],
            cwd=ROOT,
            capture_output=True,
            timeout=30,
        )
        if client_result.returncode:
            raise RuntimeError(
                f"{client.name} client failed against {server.name}\n"
                f"stdout:\n{client_result.stdout.decode(errors='replace')}\n"
                f"stderr:\n{client_result.stderr.decode(errors='replace')}"
            )
        server_stdout, server_stderr = server_process.communicate(timeout=30)
        if server_process.returncode:
            raise RuntimeError(
                f"{server.name} server failed for {client.name}\n"
                f"stdout:\n{server_stdout.decode(errors='replace')}\n"
                f"stderr:\n{server_stderr.decode(errors='replace')}"
            )
        received = output / f"{entity_id}.bin"
        if received.read_bytes() != payload.read_bytes():
            raise RuntimeError(f"payload mismatch for {client.name} -> {server.name}")
        if (output / f"{entity_id}.parent").read_text(encoding="utf-8").strip() != "42":
            raise RuntimeError(f"parent identity mismatch for {client.name} -> {server.name}")
    finally:
        if server_process.poll() is None:
            server_process.terminate()
            try:
                server_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server_process.kill()
                server_process.wait(timeout=5)
    print(f"PASS {client.name} -> {server.name}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", action="store_true", help="build and unit-test all implementations first")
    args = parser.parse_args()
    if args.build:
        build_all()
    values = implementations()
    with tempfile.TemporaryDirectory(prefix="pipestream-interop-") as temporary:
        temporary_root = Path(temporary)
        certs = temporary_root / "certs"
        run([str(ROOT / "conformance/generate_test_certs.sh"), str(certs)])
        payload = temporary_root / "payload.bin"
        payload.write_bytes(b"PipeStream interop\x00" + bytes(range(256)) * 17 + "\n東京\n".encode())
        entity_id = 100
        for server in values:
            for client in values:
                one_pair(server, client, temporary_root, certs, payload, entity_id)
                entity_id += 1
    print(f"all {len(values) ** 2} black-box pairs passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, subprocess.SubprocessError) as error:
        print(error, file=sys.stderr)
        raise SystemExit(1)
