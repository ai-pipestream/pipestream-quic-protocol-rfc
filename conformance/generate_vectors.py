#!/usr/bin/env python3
"""Generate deterministic valid and invalid PipeStream Layer 0 vectors."""

from __future__ import annotations

import argparse
import hashlib
import sys
from pathlib import Path

from wire import (
    FRAME_CAPABILITIES,
    FRAME_STATUS,
    MAX_WINDOW,
    CONNECTION_LEVEL,
    STATUS_COMPLETE,
    STATUS_PENDING,
    STATUS_PROCESSING,
    STATUS_UNSPECIFIED,
    capabilities,
    checkpoint,
    encode_cbor,
    entity_frame,
    goaway,
    status,
    ucf,
)

ROOT = Path(__file__).resolve().parents[1]


def vectors() -> list[tuple[str, str, str, str, bytes]]:
    payload = b"PipeStream Layer 0\n"
    header = {
        "entity-id": 7,
        "layer": 0,
        "content-type": "text/plain; charset=utf-8",
        "payload-length": len(payload),
        "checksum": hashlib.sha256(payload).digest(),
    }
    encoded_header = encode_cbor(header)
    wrong_length_header = dict(header)
    wrong_length_header["payload-length"] = len(payload) + 1
    wrong_checksum_header = dict(header)
    wrong_checksum_header["checksum"] = bytes(32)
    missing_id_header = dict(header)
    del missing_id_header["entity-id"]
    reserved_id_header = dict(header)
    reserved_id_header["entity-id"] = 0
    reserved_parent_header = dict(header)
    reserved_parent_header["parent-id"] = 0
    invalid_caps = {
        "layer0-core": False,
        "layer1-recursive": False,
        "layer2-resilience": False,
    }
    oversized_caps = {
        "layer0-core": True,
        "layer1-recursive": False,
        "layer2-resilience": False,
        "max-window-size": MAX_WINDOW + 1,
    }
    unknown_caps = {
        "layer0-core": True,
        "layer1-recursive": False,
        "layer2-resilience": False,
        "bogus": 1,
    }
    nondeterministic_caps = (
        b"\xa3"
        + encode_cbor("layer1-recursive")
        + encode_cbor(False)
        + encode_cbor("layer0-core")
        + encode_cbor(True)
        + encode_cbor("layer2-resilience")
        + encode_cbor(False)
    )
    unknown_checkpoint = {
        "checkpoint-id": "entity-7",
        "sequence-number": 1,
        "checkpoint-entity-id": 8,
        "bogus": 1,
    }
    unknown_entity_header = dict(header)
    unknown_entity_header["bogus"] = 1
    return [
        ("capabilities-default", "control", "valid", "", capabilities()),
        ("status-pending", "control", "valid", "", status(7, STATUS_PENDING)),
        ("status-processing", "control", "valid", "", status(7, STATUS_PROCESSING)),
        ("status-complete", "control", "valid", "", status(7, STATUS_COMPLETE)),
        ("status-cursor", "control", "valid", "", status(0xFFFFFFFF, 0, cursor=8)),
        ("status-heartbeat", "control", "valid", "", status(CONNECTION_LEVEL, STATUS_UNSPECIFIED)),
        ("goaway", "control", "valid", "", goaway(7)),
        ("goaway-reserved", "control", "valid", "", ucf(0x56, bytes.fromhex("0102030400000007"))),
        ("checkpoint-request", "control", "valid", "", checkpoint("entity-7", 1, 8)),
        ("checkpoint-ack", "control", "valid", "", checkpoint("entity-7", 1, 8, acknowledgement=True)),
        ("entity-text", "entity", "valid", "", entity_frame(7, payload, content_type="text/plain; charset=utf-8")),
        ("entity-child", "entity", "valid", "", entity_frame(8, payload, content_type="text/plain; charset=utf-8", parent_id=7)),
        ("ucf-truncated", "control", "invalid", "PIPESTREAM_FRAME_ERROR", b"\x80\x00\x00"),
        ("ucf-length-mismatch", "control", "invalid", "PIPESTREAM_FRAME_ERROR", b"\x80\x00\x00\x00\x02\xf5"),
        ("capabilities-layer0-false", "control", "invalid", "PIPESTREAM_LAYER_UNSUPPORTED", ucf(FRAME_CAPABILITIES, encode_cbor(invalid_caps))),
        ("capabilities-window-overflow", "control", "invalid", "PIPESTREAM_LIMIT_EXCEEDED", ucf(FRAME_CAPABILITIES, encode_cbor(oversized_caps))),
        ("capabilities-unknown-field", "control", "invalid", "PIPESTREAM_FRAME_ERROR", ucf(FRAME_CAPABILITIES, encode_cbor(unknown_caps))),
        ("cbor-indefinite-map", "control", "invalid", "PIPESTREAM_FRAME_ERROR", ucf(FRAME_CAPABILITIES, b"\xbf\xff")),
        ("cbor-nondeterministic-width", "control", "invalid", "PIPESTREAM_FRAME_ERROR", ucf(FRAME_CAPABILITIES, b"\xa1\x78\x0blayer0-core\xf5")),
        ("cbor-nondeterministic-map-order", "control", "invalid", "PIPESTREAM_FRAME_ERROR", ucf(FRAME_CAPABILITIES, nondeterministic_caps)),
        ("status-bad-version", "control", "invalid", "PIPESTREAM_LAYER_UNSUPPORTED", ucf(FRAME_STATUS, bytes.fromhex("21000000000000070000000000000000"))),
        ("status-short", "control", "invalid", "PIPESTREAM_FRAME_ERROR", ucf(FRAME_STATUS, bytes(15))),
        ("status-cursor-flag-mismatch", "control", "invalid", "PIPESTREAM_FRAME_ERROR", ucf(FRAME_STATUS, bytes.fromhex("11400000000000070000000000000000"))),
        ("status-entity-cursor", "control", "invalid", "PIPESTREAM_ENTITY_INVALID", status(7, STATUS_PROCESSING, cursor=8)),
        ("checkpoint-bad-flags", "control", "invalid", "PIPESTREAM_FRAME_ERROR", ucf(0x81, encode_cbor({"checkpoint-id": "entity-7", "sequence-number": 1, "checkpoint-entity-id": 8, "flags": 2}))),
        ("checkpoint-unknown-field", "control", "invalid", "PIPESTREAM_FRAME_ERROR", ucf(0x81, encode_cbor(unknown_checkpoint))),
        ("entity-missing-id", "entity", "invalid", "PIPESTREAM_ENTITY_INVALID", len(encode_cbor(missing_id_header)).to_bytes(4, "big") + encode_cbor(missing_id_header) + payload),
        ("entity-reserved-id", "entity", "invalid", "PIPESTREAM_ENTITY_INVALID", len(encode_cbor(reserved_id_header)).to_bytes(4, "big") + encode_cbor(reserved_id_header) + payload),
        ("entity-reserved-parent", "entity", "invalid", "PIPESTREAM_ENTITY_INVALID", len(encode_cbor(reserved_parent_header)).to_bytes(4, "big") + encode_cbor(reserved_parent_header) + payload),
        ("entity-unknown-field", "entity", "invalid", "PIPESTREAM_FRAME_ERROR", len(encode_cbor(unknown_entity_header)).to_bytes(4, "big") + encode_cbor(unknown_entity_header) + payload),
        ("entity-length-mismatch", "entity", "invalid", "PIPESTREAM_ENTITY_INVALID", len(encode_cbor(wrong_length_header)).to_bytes(4, "big") + encode_cbor(wrong_length_header) + payload),
        ("entity-checksum-mismatch", "entity", "invalid", "PIPESTREAM_INTEGRITY_ERROR", len(encode_cbor(wrong_checksum_header)).to_bytes(4, "big") + encode_cbor(wrong_checksum_header) + payload),
        ("entity-header-truncated", "entity", "invalid", "PIPESTREAM_FRAME_ERROR", len(encoded_header).to_bytes(4, "big") + encoded_header[:-1]),
    ]


def render_index(rows: list[tuple[str, str, str, str, bytes]]) -> str:
    lines = ["name\tkind\texpectation\terror\tsha256\toctets"]
    for name, kind, expectation, error, data in rows:
        lines.append(f"{name}\t{kind}\t{expectation}\t{error}\t{hashlib.sha256(data).hexdigest()}\t{len(data)}")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="fail instead of updating stale vectors")
    args = parser.parse_args()
    stale: list[str] = []
    rows = vectors()
    for name, _kind, expectation, _error, data in rows:
        path = ROOT / "test-vectors" / expectation / f"{name}.bin"
        if path.exists() and path.read_bytes() == data:
            continue
        if args.check:
            stale.append(str(path.relative_to(ROOT)))
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
    index = render_index(rows)
    index_path = ROOT / "test-vectors" / "index.tsv"
    if not index_path.exists() or index_path.read_text(encoding="utf-8") != index:
        if args.check:
            stale.append(str(index_path.relative_to(ROOT)))
        else:
            index_path.write_text(index, encoding="utf-8")
    if stale:
        print("stale generated vectors:", file=sys.stderr)
        for path in stale:
            print(f"  {path}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
