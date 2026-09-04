"""Process-only support for external interoperability demos.

This module knows how to launch the standalone programs. It contains no
PipeStream framing, serialization, state-machine, or transport code.
"""

from __future__ import annotations

import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def command(name: str) -> list[str]:
    if name == "java-netty":
        jars = sorted((ROOT / "implementations/java-netty/target").glob("*-all.jar"))
        if len(jars) != 1:
            raise RuntimeError("Java reference JAR is missing; run conformance/run_interop.py --build")
        return ["java", "--enable-native-access=ALL-UNNAMED", "-jar", str(jars[0])]
    if name == "rust-quinn":
        executable = ROOT / "implementations/rust-quinn/target/release/pipestream-quinn"
    elif name == "cpp-msquic":
        executable = ROOT / "implementations/cpp-msquic/build/pipestream-msquic"
    else:
        raise ValueError(f"unknown implementation {name}")
    if not executable.is_file():
        raise RuntimeError(f"{name} is missing; run conformance/run_interop.py --build")
    return [str(executable)]


def certificates(output: Path) -> None:
    subprocess.run(
        [str(ROOT / "conformance/generate_test_certs.sh"), str(output)],
        cwd=ROOT,
        check=True,
    )


@dataclass
class Server:
    name: str
    process: subprocess.Popen[bytes]
    address: str
    output: Path


def start_server(name: str, root: Path, certs: Path) -> Server:
    output = root / name / "received"
    output.mkdir(parents=True)
    ready = root / name / "ready"
    process = subprocess.Popen(
        [
            *command(name),
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
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if ready.is_file() and ready.stat().st_size:
            return Server(name, process, ready.read_text(encoding="utf-8").strip(), output)
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise RuntimeError(
                f"{name} exited before readiness\n"
                f"stdout:\n{stdout.decode(errors='replace')}\n"
                f"stderr:\n{stderr.decode(errors='replace')}"
            )
        time.sleep(0.025)
    stop_server(Server(name, process, "", output))
    raise RuntimeError(f"{name} readiness timed out")


def send_entity(
    name: str,
    server: Server,
    certs: Path,
    entity_id: int,
    payload: Path,
    *,
    parent_id: int | None = None,
) -> None:
    arguments = [
        *command(name),
        "send",
        "--connect", server.address,
        "--ca", str(certs / "ca.crt"),
        "--server-name", "localhost",
        "--entity-id", str(entity_id),
        "--input", str(payload),
        "--content-type", "application/octet-stream",
    ]
    if parent_id is not None:
        arguments.extend(("--parent-id", str(parent_id)))
    result = subprocess.run(arguments, cwd=ROOT, capture_output=True, timeout=30)
    if result.returncode:
        raise RuntimeError(
            f"{name} client failed against {server.name}\n"
            f"stdout:\n{result.stdout.decode(errors='replace')}\n"
            f"stderr:\n{result.stderr.decode(errors='replace')}"
        )


def finish_server(server: Server) -> None:
    stdout, stderr = server.process.communicate(timeout=30)
    if server.process.returncode:
        raise RuntimeError(
            f"{server.name} failed\n"
            f"stdout:\n{stdout.decode(errors='replace')}\n"
            f"stderr:\n{stderr.decode(errors='replace')}"
        )


def stop_server(server: Server) -> None:
    if server.process.poll() is None:
        server.process.terminate()
        try:
            server.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.process.kill()
            server.process.wait(timeout=5)
