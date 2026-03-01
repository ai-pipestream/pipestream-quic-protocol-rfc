## 6. Frame Formats

This section defines the wire formats for PipeStream frames. All multi-octet integer fields are encoded in network byte order (big-endian).

### 6.1. Control Stream Framing (Stream 0)

To support mixed content (bit-packed frames and Protobuf messages) on the Control Stream, PipeStream uses a Unified Control Frame (UCF) header.

#### 6.1.1. UCF Header

Every message on Stream 0 MUST begin with a 1-octet Frame Type.

| Value | Frame Class | Length Encoding | Description |
|-------|-------------|-----------------|-------------|
| 0x50-0x5F | Fixed | No length prefix | Bit-packed control frames with type-defined sizes |
| 0x80-0xAF | Variable | 4-octet Length + N | Variable-size Protobuf-encoded control messages |

For Fixed frames, the receiver determines frame size from the Frame Type value. For Variable frames, the Type is followed by a 4-octet unsigned integer (big-endian) indicating the length of the Protobuf message that follows.

#### 6.1.2. Fixed Frame Sizes

The following fixed-size frame types are defined by this document:

| Type | Name | Total Size | Notes |
|------|------|------------|-------|
| 0x50 | STATUS | 12 octets (base) | 16 octets when C=1; larger when E=1 with extension data |
| 0x54 | SCOPE_DIGEST | 52 octets | Includes 32-octet Merkle root |
| 0x55 | BARRIER | 8 octets | No variable extension |

### 6.2. Status Frames (Layer 0)

#### 6.2.1. Status Frame Format (0x50)

The Status Frame reports lifecycle transitions for entities.

```
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |  Type (0x50)  |  Stat (4) |E|C| Depth (3) |    Flags (15 bits)    |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                       Entity ID (32 bits)                     |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |        Scope ID (16 bits)       |      Reserved (16 bits)     |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

   Stat (4 bits):
      Status code (see Section 6.2.2).

   E (1 bit):
      Extended frame flag. Additional extension data follows (Section 6.5).

   C (1 bit):
      Cursor update flag. A 4-octet cursor value follows (Section 6.2.3).

   Depth (3 bits):
      Explicit scope nesting depth (0-7). 0=Root. Layer 1.

   Flags (15 bits):
      Reserved for future use. MUST be zero when sent and MUST be ignored by receivers.

   Entity ID (32 bits):
      Unsigned integer identifying the entity.

   Scope ID (16 bits):
      Identifier for the scope to which this entity belongs.

   Reserved (16 bits):
      Reserved for future use. MUST be zero when sent and MUST be ignored by receivers.
```

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
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                  New Cursor Value (32 bits)                   |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### 6.3. Scope Digest Frame (0x54)

When Protocol Layer 1 is negotiated, a scope completion is summarized:

```
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |  Type (0x54)  |  Flags (8)      |        Scope ID (16)        |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

   Flags (8 bits):
      Reserved for future use. MUST be zero when sent and MUST be ignored by receivers.

   Scope ID (16 bits):
      Identifier of the scope being summarized.
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

The SCOPE_DIGEST frame is 52 octets total. The Scope ID MUST match the 16-bit identifier defined in Section 6.2.1.

### 6.4. Barrier Frame (0x55)

```
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |  Type (0x55)  |S|  Reserved (7) |        Barrier ID (16)      |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                    Parent Entity ID (32 bits)                 |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

   S (1 bit):
      Status (0 = waiting, 1 = released).

   Reserved (7 bits):
      Reserved for future use. MUST be zero when sent and MUST be ignored by receivers.
```

### 6.5. Yield and Claim Check Extensions (Layer 2)

When E=1 in a status frame, extension data follows. The length of extension data is determined by the Status code.

#### 6.5.1. Yield Extension (Stat = 0x8)

```
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   | Yield Reason  |         Token Length (20 bits)                |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                                                               |
   |                  Yield Token (variable)                       |
   |                                                               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

#### 6.5.2. Claim Check Extension (Stat = 0x9)

```
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                                                               |
   |                    Claim Check ID (64 bits)                   |
   |                                                               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                    Expiry Timestamp (32 bits)                 |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### 6.6. Protobuf-Encoded Messages (0x80-0xAF)

Messages in this range are preceded by a 4-octet length field.

| Type | Message Name | Reference |
|-------|--------------|-----------|
| 0x80 | Capabilities | Section 3.4 |
| 0x81 | Checkpoint | Section 9.3 |

### 6.7. Entity Frames

Entity frames carry the actual document entity data on Entity Streams.

#### 6.7.1. Entity Frame Structure

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
