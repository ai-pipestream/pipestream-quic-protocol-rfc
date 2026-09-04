"""Independent PipeStream Layer 0 wire oracle used by test-vector tooling."""

from __future__ import annotations

import hashlib
import struct
from dataclasses import dataclass
from typing import Any

ALPN = b"pipestream/1"
FRAME_STATUS = 0x50
FRAME_GOAWAY = 0x56
FRAME_CAPABILITIES = 0x80
FRAME_CHECKPOINT = 0x81

STATUS_UNSPECIFIED = 0x0
STATUS_PENDING = 0x1
STATUS_PROCESSING = 0x2
STATUS_COMPLETE = 0x3
STATUS_FAILED = 0x4
STATUS_CHECKPOINT = 0x5
STATUS_DEHYDRATING = 0x6
STATUS_REHYDRATING = 0x7

NULL_ENTITY = 0x00000000
MAX_ENTITY = 0xFFFFFFFC
CONNECTION_LEVEL = 0xFFFFFFFF
ID_MODULUS = 0xFFFFFFFD
MAX_WINDOW = 0x7FFFFFFE

ERROR_NO_ERROR = 0x00
ERROR_INTEGRITY = 0x04
ERROR_ENTITY_INVALID = 0x05
ERROR_LIMIT_EXCEEDED = 0x06
ERROR_LAYER_UNSUPPORTED = 0x0C
ERROR_FRAME = 0x0D

MAX_CONTROL_FRAME = 1 << 20
MAX_ENTITY_HEADER = 1 << 16
CHECKPOINT_ACK = 1


class WireError(ValueError):
    """A named PipeStream wire refusal."""

    def __init__(self, code: int, name: str, message: str):
        super().__init__(f"{name}: {message}")
        self.code = code
        self.name = name


def _head(major: int, value: int) -> bytes:
    if value < 0:
        raise ValueError("negative values are not supported")
    prefix = major << 5
    if value < 24:
        return bytes((prefix | value,))
    if value <= 0xFF:
        return bytes((prefix | 24, value))
    if value <= 0xFFFF:
        return bytes((prefix | 25,)) + struct.pack(">H", value)
    if value <= 0xFFFFFFFF:
        return bytes((prefix | 26,)) + struct.pack(">I", value)
    if value <= 0xFFFFFFFFFFFFFFFF:
        return bytes((prefix | 27,)) + struct.pack(">Q", value)
    raise ValueError("integer exceeds CBOR uint64")


def encode_cbor(value: Any) -> bytes:
    """Encode the constrained deterministic CBOR subset used by Layer 0."""
    if value is False:
        return b"\xf4"
    if value is True:
        return b"\xf5"
    if isinstance(value, int):
        return _head(0, value)
    if isinstance(value, bytes):
        return _head(2, len(value)) + value
    if isinstance(value, str):
        encoded = value.encode("utf-8")
        return _head(3, len(encoded)) + encoded
    if isinstance(value, list):
        return _head(4, len(value)) + b"".join(encode_cbor(item) for item in value)
    if isinstance(value, dict):
        pairs = [(encode_cbor(key), encode_cbor(item)) for key, item in value.items()]
        pairs.sort(key=lambda pair: (len(pair[0]), pair[0]))
        return _head(5, len(pairs)) + b"".join(key + item for key, item in pairs)
    raise TypeError(f"unsupported CBOR value: {type(value).__name__}")


@dataclass
class _Decoder:
    data: bytes
    offset: int = 0
    items: int = 0

    def read(self, size: int) -> bytes:
        end = self.offset + size
        if end > len(self.data):
            raise WireError(ERROR_FRAME, "PIPESTREAM_FRAME_ERROR", "truncated CBOR item")
        value = self.data[self.offset:end]
        self.offset = end
        return value

    def length(self, additional: int) -> int:
        if additional < 24:
            return additional
        sizes = {24: 1, 25: 2, 26: 4, 27: 8}
        size = sizes.get(additional)
        if size is None:
            raise WireError(ERROR_FRAME, "PIPESTREAM_FRAME_ERROR", "indefinite or reserved CBOR length")
        raw = self.read(size)
        value = int.from_bytes(raw, "big")
        minimum = {1: 24, 2: 0x100, 4: 0x10000, 8: 0x100000000}[size]
        if value < minimum:
            raise WireError(ERROR_FRAME, "PIPESTREAM_FRAME_ERROR", "non-deterministic CBOR integer width")
        return value

    def value(self, depth: int = 0) -> Any:
        if depth > 16:
            raise WireError(ERROR_LIMIT_EXCEEDED, "PIPESTREAM_LIMIT_EXCEEDED", "CBOR nesting exceeds 16")
        self.items += 1
        if self.items > 4096:
            raise WireError(ERROR_LIMIT_EXCEEDED, "PIPESTREAM_LIMIT_EXCEEDED", "CBOR item count exceeds 4096")
        initial = self.read(1)[0]
        major = initial >> 5
        additional = initial & 0x1F
        if major in (0, 2, 3, 4, 5):
            length = self.length(additional)
        if major == 0:
            return length
        if major == 2:
            return self.read(length)
        if major == 3:
            try:
                return self.read(length).decode("utf-8")
            except UnicodeDecodeError as exc:
                raise WireError(ERROR_FRAME, "PIPESTREAM_FRAME_ERROR", "invalid UTF-8 map key or text") from exc
        if major == 4:
            return [self.value(depth + 1) for _ in range(length)]
        if major == 5:
            result: dict[Any, Any] = {}
            previous_key: bytes | None = None
            for _ in range(length):
                key_start = self.offset
                key = self.value(depth + 1)
                encoded_key = self.data[key_start:self.offset]
                if previous_key is not None and (len(previous_key), previous_key) >= (len(encoded_key), encoded_key):
                    raise WireError(ERROR_FRAME, "PIPESTREAM_FRAME_ERROR", "map keys are duplicate or not deterministic")
                previous_key = encoded_key
                if key in result:
                    raise WireError(ERROR_FRAME, "PIPESTREAM_FRAME_ERROR", "duplicate CBOR map key")
                result[key] = self.value(depth + 1)
            return result
        if major == 7 and additional in (20, 21):
            return additional == 21
        raise WireError(ERROR_FRAME, "PIPESTREAM_FRAME_ERROR", "unsupported CBOR type")


def decode_cbor(data: bytes) -> Any:
    decoder = _Decoder(data)
    value = decoder.value()
    if decoder.offset != len(data):
        raise WireError(ERROR_FRAME, "PIPESTREAM_FRAME_ERROR", "trailing CBOR octets")
    return value


def ucf(frame_type: int, payload: bytes) -> bytes:
    if not 0 <= frame_type <= 0xFF:
        raise ValueError("frame type must fit one octet")
    return bytes((frame_type,)) + struct.pack(">I", len(payload)) + payload


def parse_ucf(data: bytes, *, max_frame: int = MAX_CONTROL_FRAME) -> tuple[int, bytes]:
    if len(data) < 5:
        raise WireError(ERROR_FRAME, "PIPESTREAM_FRAME_ERROR", "truncated UCF header")
    frame_type = data[0]
    length = int.from_bytes(data[1:5], "big")
    if length > max_frame:
        raise WireError(ERROR_LIMIT_EXCEEDED, "PIPESTREAM_LIMIT_EXCEEDED", "control frame exceeds local limit")
    if len(data) != length + 5:
        raise WireError(ERROR_FRAME, "PIPESTREAM_FRAME_ERROR", "UCF length does not match payload")
    return frame_type, data[5:]


def capabilities(*, max_window: int = 1024, keepalive_ms: int = 30_000) -> bytes:
    body = {
        "layer0-core": True,
        "layer1-recursive": False,
        "layer2-resilience": False,
        "max-window-size": max_window,
        "serialization-format": 0,
        "keepalive-timeout-ms": keepalive_ms,
    }
    return ucf(FRAME_CAPABILITIES, encode_cbor(body))


def validate_capabilities(payload: bytes) -> dict[str, Any]:
    value = decode_cbor(payload)
    if not isinstance(value, dict):
        raise WireError(ERROR_FRAME, "PIPESTREAM_FRAME_ERROR", "capabilities must be a map")
    allowed = {
        "layer0-core",
        "layer1-recursive",
        "layer2-resilience",
        "max-scope-depth",
        "max-entities-per-scope",
        "max-window-size",
        "serialization-format",
        "keepalive-timeout-ms",
    }
    if set(value) - allowed:
        raise WireError(ERROR_FRAME, "PIPESTREAM_FRAME_ERROR", "unknown capabilities field")
    for field in ("layer0-core", "layer1-recursive", "layer2-resilience"):
        if not isinstance(value.get(field), bool):
            raise WireError(ERROR_FRAME, "PIPESTREAM_FRAME_ERROR", f"missing boolean {field}")
    if not value["layer0-core"]:
        raise WireError(ERROR_LAYER_UNSUPPORTED, "PIPESTREAM_LAYER_UNSUPPORTED", "Layer 0 is mandatory")
    if value["layer2-resilience"] and not value["layer1-recursive"]:
        value["layer2-resilience"] = False
    max_window = value.get("max-window-size", MAX_WINDOW)
    if not isinstance(max_window, int) or not 1 <= max_window <= MAX_WINDOW:
        raise WireError(ERROR_LIMIT_EXCEEDED, "PIPESTREAM_LIMIT_EXCEEDED", "invalid max-window-size")
    serialization = value.get("serialization-format", 0)
    if not isinstance(serialization, int):
        raise WireError(ERROR_FRAME, "PIPESTREAM_FRAME_ERROR", "serialization-format must be uint")
    return value


def checkpoint(
    checkpoint_id: str,
    sequence_number: int,
    checkpoint_entity_id: int,
    *,
    acknowledgement: bool = False,
    timeout_ms: int | None = None,
) -> bytes:
    body: dict[str, Any] = {
        "checkpoint-id": checkpoint_id,
        "sequence-number": sequence_number,
        "checkpoint-entity-id": checkpoint_entity_id,
        "flags": CHECKPOINT_ACK if acknowledgement else 0,
    }
    if timeout_ms is not None:
        body["timeout-ms"] = timeout_ms
    return ucf(FRAME_CHECKPOINT, encode_cbor(body))


def parse_checkpoint(payload: bytes) -> dict[str, Any]:
    value = decode_cbor(payload)
    if not isinstance(value, dict):
        raise WireError(ERROR_FRAME, "PIPESTREAM_FRAME_ERROR", "checkpoint must be a map")
    allowed = {
        "checkpoint-id",
        "sequence-number",
        "checkpoint-entity-id",
        "scope-id",
        "flags",
        "timeout-ms",
    }
    unknown = set(value) - allowed
    if unknown:
        raise WireError(ERROR_FRAME, "PIPESTREAM_FRAME_ERROR", "unknown checkpoint field")
    checkpoint_id = value.get("checkpoint-id")
    if not isinstance(checkpoint_id, str) or not checkpoint_id or len(checkpoint_id.encode("utf-8")) > 256:
        raise WireError(ERROR_FRAME, "PIPESTREAM_FRAME_ERROR", "invalid checkpoint-id")
    for field in ("sequence-number", "checkpoint-entity-id"):
        if not isinstance(value.get(field), int):
            raise WireError(ERROR_FRAME, "PIPESTREAM_FRAME_ERROR", f"missing uint {field}")
    entity_id = value["checkpoint-entity-id"]
    if not 1 <= entity_id <= MAX_ENTITY:
        raise WireError(ERROR_ENTITY_INVALID, "PIPESTREAM_ENTITY_INVALID", "invalid checkpoint-entity-id")
    scope_id = value.get("scope-id", 0)
    if scope_id != 0:
        raise WireError(ERROR_LAYER_UNSUPPORTED, "PIPESTREAM_LAYER_UNSUPPORTED", "checkpoint scope requires Layer 1")
    flags = value.get("flags", 0)
    if flags not in (0, CHECKPOINT_ACK):
        raise WireError(ERROR_FRAME, "PIPESTREAM_FRAME_ERROR", "unknown checkpoint flags")
    timeout_ms = value.get("timeout-ms")
    if timeout_ms is not None and not isinstance(timeout_ms, int):
        raise WireError(ERROR_FRAME, "PIPESTREAM_FRAME_ERROR", "timeout-ms must be uint")
    return value


def status(
    entity_id: int,
    state: int,
    *,
    scope_id: int = 0,
    cursor: int | None = None,
    depth: int = 0,
) -> bytes:
    if not 0 <= depth <= 7:
        raise ValueError("depth must be 0 through 7")
    word = (1 << 28) | ((state & 0xF) << 24) | ((1 if cursor is not None else 0) << 22) | (depth << 19)
    payload = struct.pack(">IIII", word, entity_id, scope_id, 0)
    if cursor is not None:
        payload += struct.pack(">I", cursor)
    return ucf(FRAME_STATUS, payload)


def parse_status(payload: bytes, *, layer1: bool = False, layer2: bool = False) -> dict[str, int | None]:
    if len(payload) not in (16, 20):
        raise WireError(ERROR_FRAME, "PIPESTREAM_FRAME_ERROR", "invalid STATUS payload length")
    word, entity_id, scope_id, _reserved = struct.unpack(">IIII", payload[:16])
    version = word >> 28
    state = (word >> 24) & 0xF
    extended = (word >> 23) & 1
    has_cursor = (word >> 22) & 1
    depth = (word >> 19) & 0x7
    if version != 1:
        raise WireError(ERROR_LAYER_UNSUPPORTED, "PIPESTREAM_LAYER_UNSUPPORTED", "unsupported STATUS version")
    if extended:
        raise WireError(ERROR_FRAME, "PIPESTREAM_FRAME_ERROR", "Layer 0 STATUS cannot carry extensions")
    if bool(has_cursor) != (len(payload) == 20):
        raise WireError(ERROR_FRAME, "PIPESTREAM_FRAME_ERROR", "STATUS cursor flag and length disagree")
    if depth and not layer1:
        raise WireError(ERROR_LAYER_UNSUPPORTED, "PIPESTREAM_LAYER_UNSUPPORTED", "scope depth requires Layer 1")
    if state >= 8 and not layer2:
        raise WireError(ERROR_LAYER_UNSUPPORTED, "PIPESTREAM_LAYER_UNSUPPORTED", "status requires Layer 2")
    if state == STATUS_UNSPECIFIED and entity_id != CONNECTION_LEVEL:
        raise WireError(ERROR_ENTITY_INVALID, "PIPESTREAM_ENTITY_INVALID", "UNSPECIFIED is connection-level only")
    cursor = int.from_bytes(payload[16:20], "big") if has_cursor else None
    if cursor is not None and (state != STATUS_UNSPECIFIED or entity_id != CONNECTION_LEVEL or scope_id != 0 or depth != 0):
        raise WireError(ERROR_ENTITY_INVALID, "PIPESTREAM_ENTITY_INVALID", "cursor update must be connection-level")
    return {"state": state, "entity_id": entity_id, "scope_id": scope_id, "cursor": cursor, "depth": depth}


def goaway(last_entity_id: int) -> bytes:
    return ucf(FRAME_GOAWAY, struct.pack(">II", 0, last_entity_id))


def entity_frame(
    entity_id: int,
    payload: bytes,
    *,
    content_type: str = "application/octet-stream",
    parent_id: int | None = None,
) -> bytes:
    header = {
        "entity-id": entity_id,
        "layer": 0,
        "content-type": content_type,
        "payload-length": len(payload),
        "checksum": hashlib.sha256(payload).digest(),
    }
    if parent_id is not None:
        header["parent-id"] = parent_id
    encoded = encode_cbor(header)
    return struct.pack(">I", len(encoded)) + encoded + payload


def parse_entity_frame(data: bytes, *, max_header: int = MAX_ENTITY_HEADER, max_payload: int = 64 << 20) -> tuple[dict[str, Any], bytes]:
    if len(data) < 4:
        raise WireError(ERROR_FRAME, "PIPESTREAM_FRAME_ERROR", "truncated entity header length")
    header_length = int.from_bytes(data[:4], "big")
    if header_length > max_header:
        raise WireError(ERROR_LIMIT_EXCEEDED, "PIPESTREAM_LIMIT_EXCEEDED", "entity header exceeds local limit")
    if len(data) < 4 + header_length:
        raise WireError(ERROR_FRAME, "PIPESTREAM_FRAME_ERROR", "truncated entity header")
    header = decode_cbor(data[4:4 + header_length])
    payload = data[4 + header_length:]
    if not isinstance(header, dict):
        raise WireError(ERROR_ENTITY_INVALID, "PIPESTREAM_ENTITY_INVALID", "entity header must be a map")
    allowed = {
        "entity-id",
        "parent-id",
        "scope-id",
        "layer",
        "content-type",
        "payload-length",
        "checksum",
        "metadata",
        "chunk-info",
        "completion-policy",
    }
    if set(header) - allowed:
        raise WireError(ERROR_FRAME, "PIPESTREAM_FRAME_ERROR", "unknown entity header field")
    entity_id = header.get("entity-id")
    if not isinstance(entity_id, int) or not 1 <= entity_id <= MAX_ENTITY:
        raise WireError(ERROR_ENTITY_INVALID, "PIPESTREAM_ENTITY_INVALID", "entity-id is reserved or absent")
    layer = header.get("layer")
    if not isinstance(layer, int) or not 0 <= layer <= 3:
        raise WireError(ERROR_ENTITY_INVALID, "PIPESTREAM_ENTITY_INVALID", "layer must be 0 through 3")
    parent_id = header.get("parent-id")
    if parent_id is not None and (not isinstance(parent_id, int) or not 1 <= parent_id <= MAX_ENTITY):
        raise WireError(ERROR_ENTITY_INVALID, "PIPESTREAM_ENTITY_INVALID", "parent-id is reserved or invalid")
    if len(payload) > max_payload:
        raise WireError(ERROR_LIMIT_EXCEEDED, "PIPESTREAM_LIMIT_EXCEEDED", "entity payload exceeds local limit")
    expected_length = header.get("payload-length")
    if expected_length is not None and expected_length != len(payload):
        raise WireError(ERROR_ENTITY_INVALID, "PIPESTREAM_ENTITY_INVALID", "payload-length mismatch")
    checksum = header.get("checksum")
    if checksum is not None:
        if not isinstance(checksum, bytes) or len(checksum) != 32:
            raise WireError(ERROR_INTEGRITY, "PIPESTREAM_INTEGRITY_ERROR", "checksum must contain 32 octets")
        if checksum != hashlib.sha256(payload).digest():
            raise WireError(ERROR_INTEGRITY, "PIPESTREAM_INTEGRITY_ERROR", "checksum mismatch")
    return header, payload


VALID_TRANSITIONS = {
    STATUS_PENDING: {STATUS_PROCESSING, STATUS_DEHYDRATING, STATUS_FAILED},
    STATUS_PROCESSING: {STATUS_COMPLETE, STATUS_FAILED, STATUS_DEHYDRATING, STATUS_CHECKPOINT},
    STATUS_DEHYDRATING: {STATUS_REHYDRATING, STATUS_FAILED},
    STATUS_REHYDRATING: {STATUS_COMPLETE, STATUS_FAILED},
    STATUS_CHECKPOINT: {STATUS_PROCESSING},
}


def validate_transitions(frames: list[bytes]) -> None:
    states: dict[int, int] = {}
    for frame in frames:
        frame_type, payload = parse_ucf(frame)
        if frame_type != FRAME_STATUS:
            raise WireError(ERROR_FRAME, "PIPESTREAM_FRAME_ERROR", "status transcript contains another frame type")
        parsed = parse_status(payload)
        entity_id = int(parsed["entity_id"])
        state = int(parsed["state"])
        if entity_id == CONNECTION_LEVEL:
            continue
        previous = states.get(entity_id, STATUS_PENDING)
        if entity_id not in states and state == STATUS_PENDING:
            states[entity_id] = state
            continue
        if state not in VALID_TRANSITIONS.get(previous, set()):
            raise WireError(ERROR_ENTITY_INVALID, "PIPESTREAM_ENTITY_INVALID", f"invalid transition {previous}->{state}")
        states[entity_id] = state
