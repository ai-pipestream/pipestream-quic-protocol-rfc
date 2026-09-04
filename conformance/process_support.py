"""Black-box process support for language-native example verification.

This module launches executables and checks readiness. It contains no
PipeStream framing, serialization, state-machine, or transport code.
"""

from __future__ import annotations

import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def single_match(directory: Path, pattern: str, description: str) -> Path:
    matches = sorted(directory.glob(pattern))
    if len(matches) != 1:
        raise RuntimeError(f"expected one {description}; run ./conformance/run_all.sh")
    return matches[0]


def implementation_command(name: str) -> list[str]:
    if name == "java-netty":
        jar = single_match(
            ROOT / "implementations/java-netty/target", "*-all.jar", "Java implementation JAR"
        )
        return ["java", "--enable-native-access=ALL-UNNAMED", "-jar", str(jar)]
    if name == "rust-quinn":
        executable = ROOT / "implementations/rust-quinn/target/release/pipestream-quinn"
    elif name == "cpp-msquic":
        executable = ROOT / "implementations/cpp-msquic/build/pipestream-msquic"
    else:
        raise ValueError(f"unknown implementation {name}")
    if not executable.is_file():
        raise RuntimeError(f"{name} executable is missing; run ./conformance/run_all.sh")
    return [str(executable)]


def java_example_command() -> list[str]:
    jar = single_match(
        ROOT / "examples/java-to-rust/target", "*-all.jar", "Java-to-Rust example JAR"
    )
    return ["java", "--enable-native-access=ALL-UNNAMED", "-jar", str(jar)]


def rust_example_command(name: str) -> list[str]:
    executable = ROOT / "examples" / name / "target/release" / name
    if not executable.is_file():
        raise RuntimeError(f"{name} executable is missing; run ./conformance/run_all.sh")
    return [str(executable)]


def certificates(output: Path) -> None:
    subprocess.run(
        [str(ROOT / "conformance/generate_test_certs.sh"), str(output)],
        cwd=ROOT,
        check=True,
    )


def run_checked(command: list[str]) -> subprocess.CompletedProcess[bytes]:
    result = subprocess.run(command, cwd=ROOT, capture_output=True, timeout=30)
    if result.returncode:
        raise RuntimeError(
            f"command failed ({result.returncode}): {' '.join(command)}\n"
            f"stdout:\n{result.stdout.decode(errors='replace')}\n"
            f"stderr:\n{result.stderr.decode(errors='replace')}"
        )
    return result


@dataclass
class Server:
    name: str
    process: subprocess.Popen[bytes]
    address: str
    output: Path


def start_server(name: str, root: Path, certs: Path) -> Server:
    output = root / f"{name}-server" / "received"
    output.mkdir(parents=True)
    ready = root / f"{name}-server" / "ready"
    process = subprocess.Popen(
        [
            *implementation_command(name),
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
    server = Server(name, process, "", output)
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if ready.is_file() and ready.stat().st_size:
            server.address = ready.read_text(encoding="utf-8").strip()
            return server
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise RuntimeError(
                f"{name} exited before readiness\n"
                f"stdout:\n{stdout.decode(errors='replace')}\n"
                f"stderr:\n{stderr.decode(errors='replace')}"
            )
        time.sleep(0.025)
    stop_server(server)
    raise RuntimeError(f"{name} readiness timed out")


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
