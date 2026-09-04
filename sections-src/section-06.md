# Frame Formats

This section defines the wire formats for PipeStream frames. All multi-octet integer fields are encoded in network byte order (big-endian).

**Forward Compatibility:** All fields named Reserved in this section MUST be set to zero when sent and MUST be ignored by receivers. This convention enables future specifications to assign meaning to currently-reserved bits without breaking deployed implementations. Receivers that encounter non-zero values in reserved fields MUST NOT treat this as an error. A field named Flags follows its frame-specific rules; a receiver MUST NOT assume that unknown flag bits are ignorable unless that frame definition says so.

## Control Stream Framing (Stream 0)

To ensure forward compatibility and consistent parsing, all messages on the Control Stream use a Unified Control Frame (UCF) structure with an explicit length prefix.

### UCF Header

Every message on Stream 0 MUST begin with a 1-octet Frame Type and a 4-octet Length field.

| Field | Type | Description |
|-------|------|-------------|
| Type | 1 octet | Frame Type identifier |
| Length | 4 octets | Total length of the frame payload (excluding Type/Length) |
| Payload | variable | Frame-specific data |

This structure allows any implementation to skip an unknown frame type by reading its length and discarding the payload.

For frame types with a fixed or computable payload layout defined by this specification, the Length field MUST equal the length implied by that layout (for STATUS frames, the base payload plus any cursor update and extension; see Section 6.2.2). A receiver that detects a Length value inconsistent with the frame's type-specific layout MUST close the connection with PIPESTREAM_FRAME_ERROR (0x0D).

Although the Length field can express values up to 2^32 - 1, implementations SHOULD enforce a locally configured maximum control frame size and MAY treat frames exceeding it as a connection error of type PIPESTREAM_FRAME_ERROR (0x0D). See also the incremental allocation requirement in Section 10.3.

For compactness, the fixed-frame diagrams below show the frame payload that follows the common UCF header. On the wire, each Stream 0 frame is encoded as `Type (1 octet) | Length (4 octets) | Payload`.

### Frame Types

The following frame types are defined by this document. All frames use the common UCF header:

| Type | Name | Layer | Payload Format |
|------|------|-------|----------------|
| 0x50 | STATUS | 0 | Fixed 16-octet payload, with optional cursor and extension payloads |
| 0x54 | SCOPE_DIGEST | 1 | Fixed 72-octet payload |
| 0x55 | BARRIER | 1 | Fixed 12-octet payload |
| 0x56 | GOAWAY | 0 | Fixed 8-octet payload |
| 0x80 | CAPABILITIES | 0 | Serialized message payload (negotiated format) |
| 0x81 | CHECKPOINT | 0 | Serialized message payload (negotiated format) |

## Status Frames (Layer 0)

### Status Frame Format (0x50)

The Status Frame payload reports lifecycle transitions for entities. The payload is 128-bit aligned for efficient parsing on 64-bit architectures.

~~~~

    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   | Ver(4) |Stat(4)|E|C|D(3) |           Flags (19 bits)          |
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
:   Protocol version. MUST be set to 0x1 for this specification. Receivers that encounter an unsupported version MUST close the connection with PIPESTREAM_LAYER_UNSUPPORTED (0x0C).

Stat (4 bits):
:   Status code (see Section 6.2.2).

E (1 bit):
:   Extended frame flag. If set, an Extension Header (Section 6.6.1) MUST follow the base frame (and any cursor update).

C (1 bit):
:   Cursor update flag. A 4-octet cursor value follows (Section 6.2.4).

D (3 bits):
:   Explicit scope nesting depth (0-7). 0=Root. Layer 1.

Flags (19 bits):
:   Reserved for future use. MUST be zero when sent and MUST be ignored by receivers.

Entity ID (32 bits):
:   Unsigned integer identifying the entity.

Scope ID (32 bits):
:   Identifier for the scope to which this entity belongs. Expanding to 32 bits ensures uniqueness across high-frequency workloads.

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

The base STATUS payload is 16 octets (21 octets on the wire including the UCF header). When C=1, a 4-octet cursor value follows (20-octet payload, 25 octets on the wire). When E=1, an Extension Header follows all other STATUS fields.

### Entity Status State Machine

The following table defines the complete set of valid status transitions. A receiver that observes a transition not listed in this table MUST treat the status frame as a protocol error and close the connection with PIPESTREAM_ENTITY_INVALID (0x05) as the QUIC Application Error Code.

| From State | Valid Transitions (To) |
|------------|------------------------|
| PENDING | PROCESSING, DEHYDRATING, FAILED, SKIPPED, ABANDONED |
| PROCESSING | COMPLETE, FAILED, DEHYDRATING, CHECKPOINT, YIELDED, DEFERRED, ABANDONED |
| DEHYDRATING | REHYDRATING, FAILED, ABANDONED |
| REHYDRATING | COMPLETE, FAILED, ABANDONED |
| CHECKPOINT | PROCESSING |
| YIELDED | PROCESSING, FAILED, DEFERRED, ABANDONED |
| DEFERRED | PROCESSING, FAILED, SKIPPED, ABANDONED |
| FAILED | RETRYING, ABANDONED |
| RETRYING | PROCESSING, FAILED, ABANDONED |
| COMPLETE | (terminal -- no transitions) |
| SKIPPED | (terminal -- no transitions) |
| ABANDONED | (terminal -- no transitions) |

Notes:

1. PENDING is the implicit initial state for every entity upon ID assignment.
2. The FAILED -> RETRYING transition is valid only when the entity's completion policy permits retries (max-retries > 0) and the retry count has not been exhausted. If retries are not permitted or are exhausted, FAILED MUST be treated as a terminal state.
3. Layer 2 states (YIELDED, DEFERRED, RETRYING, SKIPPED, ABANDONED) MUST NOT appear when Layer 2 has not been negotiated. A receiver operating at Layer 0 or Layer 1 that observes a Layer 2 status code MUST treat it as PIPESTREAM_LAYER_UNSUPPORTED (0x0C).
4. The UNSPECIFIED (0x0) status is used only for heartbeat frames (Section 5.1.4) and connection-level signals. It is not a valid entity lifecycle state and MUST NOT appear in transitions for entity IDs other than 0xFFFFFFFF.

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

The C flag is a connection-level signal. It MUST appear only on an UNSPECIFIED status whose Entity ID is CONNECTION_LEVEL (0xFFFFFFFF), Scope ID is the root scope (0), and depth is 0. A STATUS frame that carries C=1 in any other state or scope MUST be rejected with PIPESTREAM_ENTITY_INVALID (0x05). The same connection-level UNSPECIFIED status with C=0 is a heartbeat (Section 5.1.4).

### Status Reporting Direction

STATUS frames flow in both directions on Stream 0. For each entity, responsibility for status reporting is divided between the originating endpoint (the endpoint that assigned the Entity ID and transmits the entity) and the processing endpoint (the endpoint that receives the corresponding Entity Stream):

1. The originating endpoint MAY announce an entity with a PENDING status frame before opening its Entity Stream.
2. Once the Entity Stream has been opened, the processing endpoint is authoritative for the entity's lifecycle and emits the STATUS frames for subsequent transitions (PROCESSING, DEHYDRATING, REHYDRATING, COMPLETE, FAILED, and the Layer 2 statuses).
3. The originating endpoint applies terminal statuses (typically FAILED or ABANDONED) on its own authority only as part of transport error handling (Section 5.5) or local failure policy, for example when the Entity Stream is reset or the connection is lost before a terminal report arrives.
4. The originating endpoint advances its cursor based on observed terminal statuses and announces cursor advancement using the C flag (Section 6.2.4).

If an endpoint receives a STATUS frame for an entity from a peer that is not authoritative for that transition under these rules, the frame MUST be validated against the state machine of Section 6.2.3; conflicting reports are resolved in favor of the processing endpoint.

## Scope Digest Frame (0x54)

When Protocol Layer 1 is negotiated, a scope completion is summarized:

~~~~

    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |  Flags (8)      |        Reserved (24)                        |
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
:   The SHA-256 Merkle root covering all entity statuses in the scope (see Section 9.5).

The SCOPE_DIGEST payload is 72 octets (77 octets on the wire including the UCF header). The Scope ID MUST match the 32-bit identifier defined in Section 6.2.1.

## Barrier Frame (0x55)

~~~~

    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |S|  Reserved (31 bits)                                         |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                       Scope ID (32 bits)                      |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                    Parent Entity ID (32 bits)                 |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
~~~~
{: type="ascii-art"}

S (1 bit):
:   Status (0 = waiting, 1 = released).

Reserved (31 bits):
:   Reserved for future use. MUST be zero when sent and MUST be ignored by receivers.

Scope ID (32 bits):
:   Identifier for the scope to which this barrier applies.

Parent Entity ID (32 bits):
:   The identifier of the parent entity whose sub-tree is blocked by this barrier.

The BARRIER payload is 12 octets (17 octets on the wire including the UCF header).

## GOAWAY Frame (0x56)

The GOAWAY frame signals that the sender will not accept new entities beyond a specified Entity ID. It enables graceful shutdown: in-flight entities with IDs at or below the Last Entity ID are processed to completion, while the peer refrains from opening new Entity Streams.

~~~~

    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                           Reserved (32 bits)                  |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                   Last Entity ID (32 bits)                    |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
~~~~
{: type="ascii-art"}

Reserved (32 bits):
:   Reserved for future use. MUST be zero when sent and MUST be ignored by receivers.

Last Entity ID (32 bits):
:   The highest Entity ID that the sender will process. Entities with IDs greater than this value (per the circular ordering defined in Section 9.1) MUST NOT be sent by the peer after receiving this frame.

The GOAWAY payload is 8 octets (13 octets on the wire including the UCF header).

### Graceful Shutdown Procedure

1. An endpoint wishing to shut down sends a GOAWAY frame on Stream 0 with Last Entity ID set to the highest entity it is willing to process.
2. Upon receiving GOAWAY, the peer MUST NOT open new Entity Streams for entities with IDs above Last Entity ID. Entity Streams already open for IDs at or below Last Entity ID continue to completion.
3. Both peers continue processing status updates on Stream 0 until all in-flight entities reach terminal state.
4. Once all entities are resolved, either peer MAY close the QUIC connection with PIPESTREAM_NO_ERROR (0x00).
5. If an endpoint receives an entity with an ID above the Last Entity ID after sending GOAWAY, it MUST reject it with PIPESTREAM_ENTITY_INVALID (0x05).

An endpoint MAY send multiple GOAWAY frames to progressively lower the Last Entity ID. The Last Entity ID MUST NOT increase across successive GOAWAY frames within the same connection, where "increase" is evaluated using the circular ordering function `is_before` defined in Section 9.3.

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

If E=1 is set for a Status code that does not define an extension layout in this specification (or a negotiated extension), the receiver MUST use the Extension Length to skip the data. If the Extension Length is zero, extends beyond the end of the frame as delimited by the UCF Length field, or is missing, the receiver MUST treat the frame as malformed and close the connection with PIPESTREAM_FRAME_ERROR (0x0D).

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
:   The reason for yielding (see Section 6.6.2.1).

Token Length (24 bits):
:   The length of the Yield Token in bytes (maximum 16,777,215).

Yield Token (variable):
:   The opaque continuation state.

For a Yield Extension, the Extension Length (Section 6.6.1) MUST equal 4 + Token Length. A mismatch MUST be treated as PIPESTREAM_FRAME_ERROR (0x0D).

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

## Serialized Message Frames (0x80-0xFF)

These frame types use the common UCF header defined in Section 6.1. The payload body is encoded using the serialization format negotiated during capability exchange (Section 3.4.2). If no format was negotiated, deterministic CBOR {{RFC8949}} is the default.

| Type | Message Name | Reference |
|-------|--------------|-----------|
| 0x80 | Capabilities | Section 3.4 |
| 0x81 | Checkpoint | Section 9.3 |

## Entity Frames

Entity frames carry the actual entity payload data on Entity Streams.

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
:   The EntityHeader message encoded in the negotiated serialization format (see Section 6.8.2).

Payload (variable):
:   The raw entity data. The payload extends to the end of the Entity Stream (QUIC FIN); see Section 5.2. When the `payload-length` field is present in the EntityHeader, it MUST equal the number of payload octets.

### Message Schema (CDDL)

Normative definitions for serialized PipeStream messages use CDDL {{RFC8610}} notation; Appendix C consolidates the complete schema.

#### Entity Header

~~~~ cddl
entity-header = {
  entity-id: entity-id,
  ? parent-id: entity-id,        ; Scope-local parent
  ? scope-id: uint32,            ; Section 6.2.1
  layer: uint .le 3,             ; Data layer 0-3
  ? content-type: tstr,          ; MIME type
  ? payload-length: uint,        ; Octet count of the payload
                                 ; carried in this frame; SHOULD be
                                 ; present. Omitted only when the
                                 ; final length is unknown at
                                 ; header-emission time (the stream
                                 ; FIN delimits the payload).
  ? checksum: bstr .size 32,     ; SHA-256; SHOULD be present
  ? metadata: { * tstr => tstr },
  ? chunk-info: chunk-info,
  ? completion-policy: completion-policy, ; Layer 2
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
  reason: yield-reason,            ; See Appendix C for enum values
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
; entity-status enumerates the status codes of Section 6.2.2;
; the full definition appears in Appendix C.

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

### Chunked Transfer

An entity whose payload is too large to transmit conveniently as a single frame MAY be split into chunks. Each chunk is transmitted as its own Entity Frame on its own Entity Stream, subject to the following rules:

1. Every chunk of an entity MUST carry the same `entity-id`, `parent-id` (if present), `scope-id` (if present), and `layer` values, and MUST include a `chunk-info` structure.
2. The `payload-length` and `checksum` fields in a chunk's EntityHeader, when present, cover only that chunk's payload octets (see Section 10.2 for whole-entity integrity requirements).
3. `chunk-offset` is the octet offset of the chunk's first payload octet within the complete entity payload. `total-chunks` MUST be identical across all chunks of an entity, and the `chunk-index` values MUST cover the range 0 to total-chunks - 1 with no duplicates.
4. Chunks MAY be transmitted concurrently on separate Entity Streams and MAY arrive in any order. The receiver reassembles the payload by `chunk-offset`.
5. The entity payload is complete when all `total-chunks` chunks have been received and the reassembled ranges are contiguous and non-overlapping. Overlapping or duplicated ranges MUST be treated as PIPESTREAM_ENTITY_INVALID (0x05).
6. Lifecycle status for a chunked entity is reported once for the entity as a whole (Section 6.2), not per chunk.
