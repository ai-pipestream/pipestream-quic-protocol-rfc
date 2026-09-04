#!/usr/bin/env python3
"""Run the three external reference-suite demonstrations."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    for script in (
        ROOT / "examples/java-to-rust/run.py",
        ROOT / "examples/rust-to-cpp-recovery/run.py",
        ROOT / "examples/three-node-scatter/run.py",
    ):
        subprocess.run([sys.executable, str(script)], cwd=ROOT, check=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
