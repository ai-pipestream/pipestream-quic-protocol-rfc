#!/usr/bin/env python3
"""Verify the checked-in PipeStream Layer 0 vector corpus."""

from __future__ import annotations

import csv
import hashlib
from pathlib import Path

from wire import (
    FRAME_CAPABILITIES,
    FRAME_CHECKPOINT,
    FRAME_GOAWAY,
    FRAME_STATUS,
    WireError,
    parse_entity_frame,
    parse_checkpoint,
    parse_status,
    parse_ucf,
    validate_capabilities,
)

ROOT = Path(__file__).resolve().parents[1]


def verify(path: Path, kind: str) -> None:
    data = path.read_bytes()
    if kind == "entity":
        parse_entity_frame(data)
        return
    frame_type, payload = parse_ucf(data)
    if frame_type == FRAME_CAPABILITIES:
        validate_capabilities(payload)
    elif frame_type == FRAME_STATUS:
        parse_status(payload)
    elif frame_type == FRAME_GOAWAY:
        if len(payload) != 8:
            raise WireError(0x0D, "PIPESTREAM_FRAME_ERROR", "invalid GOAWAY payload length")
    elif frame_type == FRAME_CHECKPOINT:
        parse_checkpoint(payload)


def main() -> int:
    failures: list[str] = []
    with (ROOT / "test-vectors" / "index.tsv").open(encoding="utf-8", newline="") as source:
        for row in csv.DictReader(source, delimiter="\t"):
            path = ROOT / "test-vectors" / row["expectation"] / f"{row['name']}.bin"
            data = path.read_bytes()
            if hashlib.sha256(data).hexdigest() != row["sha256"] or len(data) != int(row["octets"]):
                failures.append(f"{row['name']}: digest or length differs from index")
                continue
            try:
                verify(path, row["kind"])
                observed = ""
            except WireError as exc:
                observed = exc.name
            if row["expectation"] == "valid" and observed:
                failures.append(f"{row['name']}: expected valid, observed {observed}")
            if row["expectation"] == "invalid" and observed != row["error"]:
                failures.append(f"{row['name']}: expected {row['error']}, observed {observed or 'valid'}")
    if failures:
        print("\n".join(failures))
        return 1
    print("all checked-in Layer 0 vectors passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
