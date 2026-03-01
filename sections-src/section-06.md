## 6. Frame Formats

This section defines the wire formats for PipeStream frames. All multi-octet integer fields are encoded in network byte order (big-endian).

### 6.1. Control Stream Framing (Stream 0)

To support mixed content (bit-packed frames and Protobuf messages) on the Control Stream, PipeStream uses a Unified Control Frame (UCF) header.

#### 6.1.1. UCF Header

Every message on Stream 0 MUST begin with a 1-octet Frame Type.

| Value | Frame Class | Length Encoding | Description |
|-------|-------------|-----------------|-------------|
| 0x50-0x7F | Fixed | No length prefix | Bit-packed control frames with type-defined sizes |
| 0x80-0xFF | Variable | 4-octet Length + N | Variable-size Protobuf-encoded control messages |

For Fixed frames, the receiver determines frame size from the Frame Type value. For Variable frames, the Type is followed by a 4-octet unsigned integer (big-endian) indicating the length of the Protobuf message that follows.

Variable-frame Length (32 bits):
:   The payload length in octets, excluding the 1-octet Type and the 4-octet Length field. Receivers MUST reject lengths greater than 16,777,215 octets (16 MiB - 1) with PIPESTREAM_ENTITY_TOO_LARGE (0x06).

#### 6.1.2. Fixed Frame Sizes

The following fixed-size frame types are defined by this document:

| Type | Name | Total Size | Notes |
|------|------|------------|-------|
| 0x50 | STATUS | 12 octets (base) | 16 octets when C=1; larger when E=1 with extension data |
| 0x54 | SCOPE_DIGEST | 68 octets | Includes 32-octet Merkle root and 64-bit counters |
| 0x55 | BARRIER | 8 octets | No variable extension |

### 6.2. Status Frames (Layer 0)

#### 6.2.1. Status Frame Format (0x50)

The Status Frame reports lifecycle transitions for entities.

```
Octet 0      : Type (0x50)
Octets 1-3   : Stat(4) | E(1) | C(1) | D(3) | Flags(15)
Octets 4-7   : Entity ID (32 bits)
Octets 8-9   : Scope ID (16 bits)
Octets 10-11 : Reserved (16 bits)
```

| Bit Range | Field | Notes |
|-----------|-------|-------|
| 0-7 | Type | `0x50` for STATUS |
| 8-11 | Stat | 4-bit status code |
| 12 | E | Extension flag |
| 13 | C | Cursor update flag |
| 14-16 | D | Explicit depth (0-7) |
| 17-31 | Flags | Reserved; MUST be zero when sent |
| 32-63 | Entity ID | 32-bit entity identifier |
| 64-79 | Scope ID | 16-bit scope identifier |
| 80-95 | Reserved | MUST be zero when sent |

Stat (4 bits):
:   Status code (see Section 6.2.2).

E (1 bit):
:   Extended frame flag. Additional extension data follows (Section 6.5).

C (1 bit):
:   Cursor update flag. A 4-octet cursor value follows (Section 6.2.3).

D (3 bits):
:   Explicit scope nesting depth (0-7). 0=Root. Layer 1.

Flags (15 bits):
:   Reserved for future use. MUST be zero when sent and MUST be ignored by receivers.

Entity ID (32 bits):
:   Unsigned integer identifying the entity.

Scope ID (16 bits):
:   Identifier for the scope to which this entity belongs.

Reserved (16 bits):
:   Reserved for future use. MUST be zero when sent and MUST be ignored by receivers.

#### 6.2.2. Status Codes

| Value | Name        | Layer | Description                            |
|-------|-------------|-------|----------------------------------------|
| 0x0   | UNSPECIFIED | -     | Protobuf default / heartbeat signal      |
| 0x1   | PENDING     | 0     | Entity announced, not yet transmitting |
| 0x2   | PROCESSING  | 0     | Entity transmission in progress        |
| 0x3   | COMPLETE    | 0     | Entity successfully processed          |
| 0x4   | FAILED      | 0     | Entity processing failed               |
| 0x5   | CHECKPOINT  | 0     | Synchronization barrier                |
| 0x6   | DEHYDRATING | 0     | Dehydrating into children              |
| 0x7   | REHYDRATING | 0     | Rehydrating children                   |
| 0x8   | YIELDED     | 2     | Paused with continuation token         |
| 0x9   | DEFERRED    | 2     | Detached with claim check              |
| 0xA   | RETRYING    | 2     | Retry in progress                      |
| 0xB   | SKIPPED     | 2     | Intentionally skipped                  |
| 0xC   | ABANDONED   | 2     | Timed out                              |

The base STATUS frame is 12 octets. When C=1, a 4-octet cursor value follows (total 16 octets before any E=1 extension data). When E=1, additional extension bytes follow as defined in Section 6.5.

#### 6.2.3. Cursor Update Extension

When C=1, a 4-octet cursor update follows the status frame:

```
Octets 0-3 : New Cursor Value (32 bits)
```

| Bit Range | Field | Notes |
|-----------|-------|-------|
| 0-31 | New Cursor Value | Cursor update value |

New Cursor Value (32 bits):
:   The numeric value of the new cursor. Entities with IDs lower than this value (modulo circular ID rules) are considered resolved and their IDs MAY be recycled.

#### 6.2.4. Entity Status State Machine

Status updates for a given `(scope_id, entity_id)` pair form a finite-state machine. Senders MUST emit only legal transitions. Receivers MUST enforce the transition rules in this section.

`UNSPECIFIED (0x0)` is reserved for heartbeat/connection signaling and MUST NOT be used as a lifecycle state for real entities.

Layer 2 statuses (`YIELDED`, `DEFERRED`, `RETRYING`, `SKIPPED`, `ABANDONED`) MUST NOT appear unless Layer 2 has been negotiated (Section 3.4). If received without Layer 2 negotiation, the receiver MUST fail with `PIPESTREAM_LAYER_UNSUPPORTED (0x0C)`.

##### 6.2.4.1. Legal Transitions

| Current State | Next State(s) | Conditions |
|---------------|---------------|------------|
| UNSPECIFIED | - | Heartbeat only; not a real entity lifecycle state |
| PENDING | PROCESSING, FAILED | Initial activation or immediate failure |
| PROCESSING | COMPLETE, FAILED, CHECKPOINT, DEHYDRATING | Core transitions |
| PROCESSING | YIELDED, DEFERRED | Layer 2 negotiated |
| CHECKPOINT | PROCESSING | Barrier satisfied; resume processing |
| DEHYDRATING | REHYDRATING, FAILED | Child decomposition complete or failure |
| REHYDRATING | COMPLETE, FAILED | Gather complete or failure |
| YIELDED | PROCESSING, FAILED, ABANDONED | Resume, terminal failure, or expiry/abort (Layer 2) |
| DEFERRED | PROCESSING, FAILED, ABANDONED | Claim redeemed, terminal failure, or expiry/abort (Layer 2) |
| FAILED | RETRYING, SKIPPED | Only when Layer 2 policy permits retry/skip |
| RETRYING | PROCESSING, FAILED, SKIPPED, ABANDONED | Retry attempt, failure, policy skip, or timeout/abort |
| COMPLETE | - | Terminal |
| SKIPPED | - | Terminal |
| ABANDONED | - | Terminal |

`FAILED` is terminal in Layer 0-only operation. When Layer 2 is active, `FAILED` MAY transition to `RETRYING` or `SKIPPED` only if the effective Completion Policy allows it.

##### 6.2.4.2. State Diagram (Informative)

```
PENDING -> PROCESSING -> COMPLETE
   |          |   |  \-> CHECKPOINT -> PROCESSING
   |          |   \----> DEHYDRATING -> REHYDRATING -> COMPLETE
   |          \--------> FAILED --(L2 policy)--> RETRYING -> PROCESSING
   |                                 |                    \-> FAILED
   \--------------------------------> FAILED --(L2 policy)--> SKIPPED

PROCESSING --(L2)--> YIELDED  -> PROCESSING
PROCESSING --(L2)--> DEFERRED -> PROCESSING
YIELDED/DEFERRED/RETRYING ----> ABANDONED
```

##### 6.2.4.3. Error Handling

For a given `(scope_id, entity_id)`, a transition not listed in Table 6.2.4.1 is invalid. Receivers MUST treat such transitions as protocol violations and fail processing with `PIPESTREAM_ENTITY_INVALID (0x05)`.

Status frames for a given `(scope_id, entity_id)` MUST be processed in Control Stream order.

### 6.3. Scope Digest Frame (0x54)

When Protocol Layer 1 is negotiated, a scope completion is summarized:

```
Octet 0      : Type (0x54)
Octet 1      : Flags (8 bits)
Octets 2-3   : Scope ID (16 bits)
Octets 4-11  : Entities Processed (64 bits)
Octets 12-19 : Entities Succeeded (64 bits)
Octets 20-27 : Entities Failed (64 bits)
Octets 28-35 : Entities Deferred (64 bits)
Octets 36-67 : Merkle Root (256 bits)
```

| Bit Range | Field | Notes |
|-----------|-------|-------|
| 0-7 | Type | `0x54` for SCOPE_DIGEST |
| 8-15 | Flags | Reserved; MUST be zero when sent |
| 16-31 | Scope ID | 16-bit scope identifier |
| 32-95 | Entities Processed | 64-bit counter |
| 96-159 | Entities Succeeded | 64-bit counter |
| 160-223 | Entities Failed | 64-bit counter |
| 224-287 | Entities Deferred | 64-bit counter |
| 288-543 | Merkle Root | 256-bit SHA-256 Merkle root |

Flags (8 bits):
:   Reserved for future use. MUST be zero when sent and MUST be ignored by receivers.

Scope ID (16 bits):
:   Identifier of the scope being summarized.

Entities Processed (64 bits):
:   The total number of entities that were processed within the scope.

Entities Succeeded (64 bits):
:   The number of entities that reached a terminal success state.

Entities Failed (64 bits):
:   The number of entities that reached a terminal failure state.

Entities Deferred (64 bits):
:   The number of entities that were deferred via claim checks.

Merkle Root (256 bits):
:   The SHA-256 Merkle root covering all entity statuses in the scope (see Section 9.4).

The SCOPE_DIGEST frame is 68 octets total. The Scope ID MUST match the 16-bit identifier defined in Section 6.2.1.

### 6.4. Barrier Frame (0x55)

```
Octet 0    : Type (0x55)
Octet 1    : S(1) | Reserved(7)
Octets 2-3 : Barrier ID (16 bits)
Octets 4-7 : Parent Entity ID (32 bits)
```

| Bit Range | Field | Notes |
|-----------|-------|-------|
| 0-7 | Type | `0x55` for BARRIER |
| 8 | S | Status bit: 0 waiting, 1 released |
| 9-15 | Reserved | MUST be zero when sent |
| 16-31 | Barrier ID | 16-bit barrier identifier |
| 32-63 | Parent Entity ID | 32-bit parent entity identifier |

S (1 bit):
:   Status (0 = waiting, 1 = released).

Reserved (7 bits):
:   Reserved for future use. MUST be zero when sent and MUST be ignored by receivers.

Barrier ID (16 bits):
:   Identifier for the barrier within the scope.

Parent Entity ID (32 bits):
:   The identifier of the parent entity whose sub-tree is blocked by this barrier.

### 6.5. Yield and Claim Check Extensions (Layer 2)

When E=1 in a status frame, extension data follows. The length of extension data is determined by the Status code.

If E=1 is set for a Status code that does not define an extension layout in this specification (or a negotiated extension), the receiver MUST treat the frame as malformed and fail processing with PIPESTREAM_ENTITY_INVALID (0x05).

#### 6.5.1. Yield Extension (Stat = 0x8)

```
Octet 0        : Yield Reason (8 bits)
Octets 1-3     : Token Length (24 bits)
Octets 4-(N+3) : Yield Token (N octets, where N = Token Length)
```

| Bit Range | Field | Notes |
|-----------|-------|-------|
| 0-7 | Yield Reason | See Section 6.5.1.1 |
| 8-31 | Token Length | 24-bit unsigned length in octets |
| 32-(31+8N) | Yield Token | Opaque continuation state |

Yield Reason (8 bits):
:   The reason for yielding (see Section 6.5.1.1).

Token Length (24 bits):
:   The length of the Yield Token in bytes (maximum 16,777,215).

Yield Token (variable):
:   The opaque continuation state.

##### 6.5.1.1. Yield Reason Codes

| Value | Name | Description |
|-------|------|-------------|
| 0x1 | EXTERNAL_CALL | Waiting on external service |
| 0x2 | RATE_LIMITED | Voluntary throttle |
| 0x3 | AWAITING_SIBLING | Waiting for specific sibling |
| 0x4 | AWAITING_APPROVAL | Human/workflow gate |
| 0x5 | RESOURCE_BUSY | Semaphore/lock |
| 0x0, 0x06-0xFF | Reserved | Reserved for future use |

#### 6.5.2. Claim Check Extension (Stat = 0x9)

```
Octets 0-7  : Claim Check ID (64 bits)
Octets 8-15 : Expiry Timestamp (64 bits, Unix micros)
```

| Bit Range | Field | Notes |
|-----------|-------|-------|
| 0-63 | Claim Check ID | 64-bit cryptographically random identifier |
| 64-127 | Expiry Timestamp | 64-bit Unix epoch time in microseconds |

Claim Check ID (64 bits):
:   A cryptographically secure random identifier for the claim.

Expiry Timestamp (64 bits):
:   Unix epoch timestamp in microseconds when the claim expires.

### 6.6. Protobuf-Encoded Messages (0x80-0xFF)

Messages in this range are preceded by a 4-octet length field.

| Type | Message Name | Reference |
|-------|--------------|-----------|
| 0x80 | Capabilities | Section 3.4 |
| 0x81 | Checkpoint | Section 9.3 |

### 6.7. Entity Frames

Entity frames carry the actual document entity data on Entity Streams.

#### 6.7.1. Entity Frame Structure

```
Octets 0-3      : Header Length (4 octets, big-endian uint32)
Octets 4-(3+H)  : Header (Protobuf), where H = Header Length
Octets (4+H)-.. : Payload (variable length per header)
```

| Octet Range | Field | Notes |
|-------------|-------|-------|
| 0-3 | Header Length | 32-bit unsigned length of protobuf header |
| 4-(3+H) | Header | Protobuf `EntityHeader`, `H` octets |
| (4+H)-.. | Payload | Entity payload bytes |

Header Length (4 octets):
:   The length of the Protobuf-encoded EntityHeader in bytes.

Header (Protobuf):
:   The serialized EntityHeader message (see Section 6.7.2).

Payload (variable):
:   The raw entity data.

#### 6.7.2. Entity Header (Protobuf)

```protobuf
message EntityHeader {
  uint32 entity_id = 1;         // Scope-local identifier
  uint32 parent_id = 2;         // 0 for root entities
  uint32 scope_id = 3;          // Layer 1: scope identifier
  uint32 layer = 4;             // Data layer (0-3)
  string content_type = 5;      // MIME type
  uint64 payload_length = 6;
  bytes checksum = 7;           // SHA-256 (32 bytes)
  map<string, string> metadata = 8;
  ChunkInfo chunk_info = 9;
  CompletionPolicy completion_policy = 10; // Layer 2: failure handling
}
```

#### 6.7.3. Checksum Algorithm

PipeStream uses SHA-256 {{FIPS-180-4}} for payload integrity verification. The checksum MUST be exactly 32 octets.
