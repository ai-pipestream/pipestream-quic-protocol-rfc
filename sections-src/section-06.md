## 6. Frame Formats

This section defines the wire formats for PipeStream frames. All multi-octet integer fields are encoded in network byte order (big-endian).

### 6.1. Status Frames (Layer 0)

#### 6.1.1. Status Frame Format (64 bits)

```
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                       Entity ID (32 bits)                     |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |        Scope ID (16 bits)       | Stat  |E|C|   Flags (10)   |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

   Entity ID (32 bits):
      Unsigned integer identifying the entity.
      Range 0x00000001-0xFFFFFFFD for regular entities.
      0x00000000: NULL_ENTITY (reserved; MUST NOT be used)
      0xFFFFFFFE: SCOPE_MARKER (Layer 1)
      0xFFFFFFFF: CONNECTION_LEVEL (heartbeat, shutdown)

   Scope ID (16 bits):
      Identifier for the scope to which this entity belongs.
      Layer 0 implementations set this to 0x0000 (root scope).
      Layer 1 uses this field for hierarchical scope tracking.

   Stat (4 bits):
      Status code (see Section 6.1.2).

   E (1 bit):
      Extended frame flag. When set, additional data follows the
      basic 8-octet frame.

   C (1 bit):
      Cursor update flag. When set, a 4-octet cursor value follows.

   Flags (10 bits):
      Bit 0: Scope is root of a new document (Layer 1)
      Bit 1: Fail-fast on first child failure (Layer 1)
      Bits 2-4: Scope depth (0-7; Layer 1, default 0)
      Bits 5-9: Reserved. MUST be zero when sent.
      Receivers MUST ignore non-zero reserved flags.
```

This unified 64-bit frame replaces both the Layer 0 basic frame and the Layer 1 scoped frame from earlier protocol versions. Layer 0 implementations set Scope ID to zero and ignore scope-related flag bits. Layer 1 implementations populate the Scope ID and depth fields to enable hierarchical scope tracking within the same frame format.

#### 6.1.2. Status Codes

| Value | Name        | Layer | Description                            |
|-------|-------------|-------|----------------------------------------|
| 0x0   | UNSPECIFIED | -     | Protobuf default / heartbeat signal      |
| 0x1   | PENDING     | 0     | Entity announced, not yet transmitting |
| 0x2   | PROCESSING  | 0     | Entity transmission in progress        |
| 0x3   | COMPLETE    | 0     | Entity successfully processed          |
| 0x4   | FAILED      | 0     | Entity processing failed               |
| 0x5   | CHECKPOINT  | 0     | Synchronization barrier                |
| 0x6   | DEHYDRATING  | 0     | Dehydrating into children              |
| 0x7   | REHYDRATING | 0     | Rehydrating children                     |
| 0x8   | YIELDED     | 2     | Paused with continuation token         |
| 0x9   | DEFERRED    | 2     | Detached with claim check              |
| 0xA   | RETRYING    | 2     | Retry in progress                      |
| 0xB   | SKIPPED     | 2     | Intentionally skipped (lenient mode)   |
| 0xC   | ABANDONED   | 2     | Timed out, cursor advanced past        |
| 0xD-0xF | Reserved  | -     | Reserved for future use                |

#### 6.1.3. Cursor Update Extension

When C=1, a 4-octet cursor update follows the status frame:

```
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                  New Cursor Value (32 bits)                   |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

The cursor indicates the lowest unresolved Entity ID. IDs below the cursor are considered resolved and MAY be recycled.

#### 6.1.4. Reserved Entity ID Values

| Value      | Name              | Purpose                            |
|------------|-------------------|------------------------------------|
| 0x00000000 | NULL_ENTITY       | Reserved; MUST NOT be used         |
| 0xFFFFFFFE | SCOPE_MARKER      | Scope operations (Layer 1)         |
| 0xFFFFFFFF | CONNECTION_LEVEL  | Connection-wide control messages   |

### 6.2. Scoped Status Frames (Layer 1)

When Protocol Layer 1 is negotiated, the unified 64-bit status frame (Section 6.1.1) carries hierarchical scope information:

- **Scope ID (16 bits)**: Identifies the scope within the session. Derived from parent path hash. Allows 65,536 concurrent scopes across all depth levels.

- **Depth (Flags bits 2-4)**: Encodes the scope nesting depth. 0=root/collection, 1=document, 2=part, etc. Maximum depth of 7 (negotiated, default: 7).

- **Scope root flag (Flags bit 0)**: Indicates that this scope is the root of a new document decomposition.

Layer 1 implementations MUST populate the Scope ID and depth fields for all status frames within hierarchical scopes. Layer 0 implementations set Scope ID to 0x0000 and depth to 0.

### 6.3. Scope Digest Frame (Layer 1)

When a scope completes, a digest summarizes its processing:

```
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   | Frame Type = 0x54 (SCOPE_DIGEST)      |    Scope ID (14)     |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                    Entities Processed (32)                    |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                    Entities Succeeded (32)                    |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                    Entities Failed (32)                       |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                    Entities Deferred (32)                     |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                                                               |
   |                    Merkle Root (256 bits)                     |
   |                                                               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

The Merkle root is computed as SHA-256 over all child status entries in Entity ID order.

### 6.4. Yield Frame (Layer 2)

When Status = YIELDED (0x8) and E=1, the yield extension follows the 8-octet status frame:

```
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                       Entity ID (32)                          |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |        Scope ID (16)            |1000 |1|C|   Flags (10)     |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   | Yield Reason  |         Token Length (20 bits)                |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                                                               |
   |                  Yield Token (variable)                       |
   |                  (up to 1,048,575 bytes)                      |
   |                                                               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

   Yield Reason (4 bits):
     0x1 = EXTERNAL_CALL     (waiting on external service)
     0x2 = RATE_LIMITED      (voluntary throttle)
     0x3 = AWAITING_SIBLING  (waiting for specific sibling)
     0x4 = AWAITING_APPROVAL (human/workflow gate)
     0x5 = RESOURCE_BUSY     (semaphore/lock)
     0x0, 0x6-0xF = Reserved
```

### 6.5. Claim Check Frame (Layer 2)

When Status = DEFERRED (0x9) and E=1, the claim check extension follows the 8-octet status frame:

```
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                       Entity ID (32)                          |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |        Scope ID (16)            |1001 |1|C|   Flags (10)     |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                                                               |
   |                    Claim Check ID (64 bits)                   |
   |                                                               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                    Expiry Timestamp (32 bits)                 |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

The Claim Check ID can be used to query status or trigger retry in any session.

### 6.6. Claim Check Query/Response Frames (Layer 2)

```
   CLAIM_CHECK_QUERY (Frame Type = 0x70):
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   | Frame Type = 0x70             |           Flags              |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                                                               |
   |                    Claim Check ID (64 bits)                   |
   |                                                               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

   CLAIM_CHECK_RESPONSE (Frame Type = 0x71):
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   | Frame Type = 0x71             | Status        |    Flags     |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                                                               |
   |                    Claim Check ID (64 bits)                   |
   |                                                               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                  Result Entity ID (32 bits)                   |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### 6.7. Barrier Frame (Layer 1)

```
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   | Frame Type = 0x55 (BARRIER)   |B|      Barrier ID (15 bits)  |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                    Parent Entity ID (32 bits)                 |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

   B (1 bit):
      Barrier satisfied (0 = waiting, 1 = released)
```

### 6.8. Entity Frames

Entity frames carry the actual document entity data on Entity Streams.

#### 6.8.1. Entity Frame Structure

```
   +---------------------------+
   |    Header Length (4)      |   4 octets, big-endian uint32
   +---------------------------+
   |                           |
   |    Header (Protobuf)      |   Variable length
   |                           |
   +---------------------------+
   |                           |
   |    Payload                |   Variable length (per header)
   |                           |
   +---------------------------+
```

#### 6.8.2. Entity Header (Protobuf)

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

#### 6.8.3. Checksum Algorithm

PipeStream uses SHA-256 {{FIPS-180-4}} for payload integrity verification. The checksum MUST be exactly 32 octets.
