#!/usr/bin/env python3
"""Validate checked-in CBOR instances against the normative Layer 0 CDDL."""

from __future__ import annotations

import shutil
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = ROOT / "cddl/pipestream-layer0.cddl"


def bundle_command() -> str:
    for candidate in ("bundle", "bundle3.3"):
        executable = shutil.which(candidate)
        if executable:
            return executable
    raise RuntimeError("Bundler is required; install it with: gem install bundler")


def cbor_instance(name: str, expectation: str) -> bytes:
    data = (ROOT / "test-vectors" / expectation / f"{name}.bin").read_bytes()
    if name.startswith(("capabilities-", "checkpoint-")):
        if len(data) < 5:
            raise RuntimeError(f"{name}: truncated UCF")
        length = int.from_bytes(data[1:5], "big")
        if len(data) != 5 + length:
            raise RuntimeError(f"{name}: inconsistent UCF length")
        return data[5:]
    if name.startswith("entity-"):
        if len(data) < 4:
            raise RuntimeError(f"{name}: truncated EntityHeader length")
        length = int.from_bytes(data[:4], "big")
        if len(data) < 4 + length:
            raise RuntimeError(f"{name}: truncated EntityHeader")
        return data[4 : 4 + length]
    raise RuntimeError(f"{name}: no CDDL extraction rule")


def validate(executable: str, instances: list[Path]) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        [executable, "exec", "cddl", str(SCHEMA), "validate", *map(str, instances)],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )


def main() -> int:
    accepted = (
        "capabilities-default",
        "checkpoint-request",
        "checkpoint-ack",
        "entity-text",
        "entity-child",
    )
    refused = (
        "capabilities-window-overflow",
        "capabilities-unknown-field",
        "checkpoint-bad-flags",
        "checkpoint-unknown-field",
        "entity-reserved-id",
        "entity-reserved-parent",
        "entity-unknown-field",
    )
    executable = bundle_command()
    with tempfile.TemporaryDirectory(prefix="pipestream-cddl-") as directory:
        temporary = Path(directory)
        accepted_paths = []
        for name in accepted:
            path = temporary / f"valid-{name}.cbor"
            path.write_bytes(cbor_instance(name, "valid"))
            accepted_paths.append(path)
        result = validate(executable, accepted_paths)
        if result.returncode:
            raise RuntimeError(result.stdout.decode(errors="replace"))

        for name in refused:
            path = temporary / f"invalid-{name}.cbor"
            path.write_bytes(cbor_instance(name, "invalid"))
            result = validate(executable, [path])
            if result.returncode == 0:
                raise RuntimeError(f"normative CDDL accepted invalid vector {name}")

    print(f"normative CDDL accepted {len(accepted)} and refused {len(refused)} checked instances")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
