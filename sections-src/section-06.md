# Frame Formats

This section defines the wire formats for PipeStream frames. All multi-octet integer fields are encoded in network byte order (big-endian).

## Control Stream Framing (Stream 0)

To support mixed content (bit-packed frames and serialized messages) on the Control Stream, PipeStream uses a Unified Control Frame (UCF) header.

### UCF Header

Every message on Stream 0 MUST begin with a 1-octet Frame Type.

| Value | Frame Class | Length Encoding | Description |
|-------|-------------|-----------------|-------------|
| 0x50-0x7F | Fixed | No length prefix | Bit-packed control frames with type-defined sizes |
| 0x80-0xFF | Variable | 4-octet Length + N | Variable-size serialized control messages (encoding per Section 3.5) |

For Fixed frames, the receiver determines frame size from the Frame Type value. For Variable frames, the Type is followed by a 4-octet unsigned integer (big-endian) indicating the length of the serialized message that follows.

Variable-frame Length (32 bits):
:   The payload length in octets, excluding the 1-octet Type and the 4-octet Length field. Receivers MUST reject lengths greater than 16,777,215 octets (16 MiB - 1) with PIPESTREAM_ENTITY_TOO_LARGE (0x06).

### Fixed Frame Sizes

The following fixed-size frame types are defined by this document:

| Type | Name | Total Size | Notes |
|------|------|------------|-------|
| 0x50 | STATUS | 16 octets (base) | 20 octets when C=1; larger when E=1 with extension data |
| 0x54 | SCOPE_DIGEST | 72 octets | Includes 32-octet Merkle root and 64-bit counters |
| 0x55 | BARRIER | 12 octets | No variable extension |

## Status Frames (Layer 0)

### Status Frame Format (0x50)

The Status Frame reports lifecycle transitions for entities. The frame is 128-bit aligned for efficient parsing on 64-bit architectures.

~~~~

    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |  Type (0x50)  |Ver(4) |Stat(4)|E|C|D(3) |    Flags (11 bits)    |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                       Entity ID (32 bits)                     |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                       Scope ID (32 bits)                      |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                       Reserved (32 bits)                      |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
~~~~
{: type="ascii-art"}

Ver (4 bits):
:   Protocol version. MUST be set to 0x1 for this specification. Receivers MUST treat any other value as malformed and close the connection with PIPESTREAM_ENTITY_INVALID (0x05).

Stat (4 bits):
:   Status code (see Section 6.2.2).

E (1 bit):
:   Extended frame flag. If set, an Extension Header (Section 6.5) MUST follow the base frame (and any cursor update).

C (1 bit):
:   Cursor update flag. A 4-octet cursor value follows (Section 6.2.3).

D (3 bits):
:   Explicit scope nesting depth (0-7). 0=Root. Layer 1.

Flags (11 bits):
:   Reserved for future use. MUST be zero when sent and MUST be ignored by receivers.

Entity ID (32 bits):
:   Unsigned integer identifying the entity.

Scope ID (32 bits):
:   Identifier for the scope to which this entity belongs. Expanding to 32 bits ensures uniqueness across high-frequency document sessions.

Reserved (32 bits):
:   Reserved for future use. MUST be zero when sent and MUST be ignored by receivers.

### Status Codes

| Value | Name        | Layer | Description                            |
|-------|-------------|-------|----------------------------------------|
| 0x0   | UNSPECIFIED | -     | Default / heartbeat signal      |
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

The base STATUS frame is 16 octets. When C=1, a 4-octet cursor value follows (total 20 octets). When E=1, an Extension Header follows all other STATUS fields.

### Cursor Update Extension

When C=1, a 4-octet cursor update follows the status frame:

~~~~

    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                  New Cursor Value (32 bits)                   |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
~~~~
{: type="ascii-art"}

New Cursor Value (32 bits):
:   The numeric value of the new cursor. Entities with IDs lower than this value (modulo circular ID rules) are considered resolved and their IDs MAY be recycled.

## Scope Digest Frame (0x54)

When Protocol Layer 1 is negotiated, a scope completion is summarized:

~~~~

    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |  Type (0x54)  |  Flags (8)      |        Reserved (16)        |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                       Scope ID (32 bits)                      |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                                                               |
   |                   Entities Processed (64 bits)                |
   |                                                               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                                                               |
   |                   Entities Succeeded (64 bits)                |
   |                                                               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                                                               |
   |                    Entities Failed (64 bits)                  |
   |                                                               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                                                               |
   |                    Entities Deferred (64 bits)                |
   |                                                               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                                                               |
   |                    Merkle Root (256 bits)                     |
   |                                                               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
~~~~
{: type="ascii-art"}

Flags (8 bits):
:   Reserved for future use. MUST be zero when sent and MUST be ignored by receivers.

Scope ID (32 bits):
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

The SCOPE_DIGEST frame is 72 octets total. The Scope ID MUST match the 32-bit identifier defined in Section 6.2.1.

## Barrier Frame (0x55)

~~~~

    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |  Type (0x55)  |S|  Reserved (23 bits)                         |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                       Scope ID (32 bits)                      |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                    Parent Entity ID (32 bits)                 |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
~~~~
{: type="ascii-art"}

S (1 bit):
:   Status (0 = waiting, 1 = released).

Reserved (23 bits):
:   Reserved for future use. MUST be zero when sent and MUST be ignored by receivers.

Scope ID (32 bits):
:   Identifier for the scope to which this barrier applies.

Parent Entity ID (32 bits):
:   The identifier of the parent entity whose sub-tree is blocked by this barrier.

## Yield and Claim Check Extensions (Layer 2)

When E=1 in a status frame, an Extension Header MUST follow.

### Extension Header

~~~~

    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |     Extension Length (32 bits)                                |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
~~~~
{: type="ascii-art"}

Extension Length (32 bits):
:   The total length of the extension data that follows this header, in octets. This allows receivers that do not support specific status extensions to skip the extension data and continue parsing the control stream.

If E=1 is set for a Status code that does not define an extension layout in this specification (or a negotiated extension), the receiver MUST use the Extension Length to skip the data. If the length is zero or missing, the frame MUST be treated as malformed.

### Yield Extension (Stat = 0x8)

~~~~

    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   | Yield Reason  |           Token Length (24 bits)              |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                                                               |
   |                  Yield Token (variable)                       |
   |                                                               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
~~~~
{: type="ascii-art"}

Yield Reason (8 bits):
:   The reason for yielding (see Section 6.5.1.1).

Token Length (24 bits):
:   The length of the Yield Token in bytes (maximum 16,777,215).

Yield Token (variable):
:   The opaque continuation state.

#### Yield Reason Codes

| Value | Name | Description |
|-------|------|-------------|
| 0x1 | EXTERNAL_CALL | Waiting on external service |
| 0x2 | RATE_LIMITED | Voluntary throttle |
| 0x3 | AWAITING_SIBLING | Waiting for specific sibling |
| 0x4 | AWAITING_APPROVAL | Human/workflow gate |
| 0x5 | RESOURCE_BUSY | Semaphore/lock |
| 0x0, 0x06-0xFF | Reserved | Reserved for future use |

### Claim Check Extension (Stat = 0x9)

~~~~

    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                                                               |
   |                    Claim Check ID (64 bits)                   |
   |                                                               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                                                               |
   |                Expiry Timestamp (64 bits, Unix micros)        |
   |                                                               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
~~~~
{: type="ascii-art"}

Claim Check ID (64 bits):
:   A cryptographically secure random identifier for the claim.

Expiry Timestamp (64 bits):
:   Unix epoch timestamp in microseconds when the claim expires.

## Variable-Length Serialized Messages (0x80-0xFF)

Messages in this range are preceded by a 4-octet length field. The message body is encoded using the serialization format negotiated during capability exchange (Section 3.5). If no format was negotiated, CBOR {{RFC8949}} is the default.

| Type | Message Name | Reference |
|-------|--------------|-----------|
| 0x80 | Capabilities | Section 3.4 |
| 0x81 | Checkpoint | Section 9.3 |

## Entity Frames

Entity frames carry the actual document entity data on Entity Streams.

### Entity Frame Structure

~~~~

   +---------------------------+
   |    Header Length (4)      |   4 octets, big-endian uint32
   +---------------------------+
   |                           |
   |    Header (serialized)    |   Variable length
   |                           |
   +---------------------------+
   |                           |
   |    Payload                |   Variable length (per header)
   |                           |
   +---------------------------+
~~~~
{: type="ascii-art"}

Header Length (4 octets):
:   The length of the serialized EntityHeader in bytes.

Header (serialized):
:   The EntityHeader message encoded in the negotiated serialization format (see Section 6.7.2).

Payload (variable):
:   The raw entity data.

### Message Schema (CDDL)

Normative definitions for serialized PipeStream messages use CDDL {{RFC8610}} notation. An informational Protocol Buffers equivalent is provided in Appendix D.

#### Entity Header

~~~~ cddl
entity-header = {
  entity-id: uint,               ; Scope-local identifier
  ? parent-id: uint,             ; 0 for root entities
  ? scope-id: uint,              ; Layer 1: scope identifier
  layer: uint .le 3,             ; Data layer (0-3)
  ? content-type: tstr,          ; MIME type
  payload-length: uint,
  ? checksum: bstr .size 32,     ; SHA-256 (32 bytes)
  ? metadata: { * tstr => tstr },
  ? chunk-info: chunk-info,
  ? completion-policy: completion-policy,
}
~~~~

#### Chunk Info

~~~~ cddl
chunk-info = {
  total-chunks: uint,
  chunk-index: uint,
  chunk-offset: uint,
}
~~~~

#### Yield and Deferral

~~~~ cddl
yield-token = {
  reason: uint,
  ? continuation-state: bstr,
  ? validation: stopping-point-validation,
}

claim-check = {
  claim-id: uint,
  entity-id: uint,
  ? scope-id: uint,
  expiry-timestamp: uint,        ; Unix epoch microseconds
  ? validation: stopping-point-validation,
}
~~~~

#### Support Types

~~~~ cddl
entity-status = uint .size 1 ; Values 0x0-0xC per Section 6.2.2

stopping-point-validation = {
  ? state-checksum: bstr,        ; Hash of processing state
  ? bytes-processed: uint,       ; Progress marker
  ? children-complete: uint,
  ? children-total: uint,
  ? is-resumable: bool,
  ? checkpoint-ref: tstr,
}
~~~~

### Checksum Algorithm

PipeStream uses SHA-256 {{FIPS-180-4}} for payload integrity verification. The checksum MUST be exactly 32 octets.
