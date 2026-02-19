# PipeStream: Recursive Entity Streaming Protocol

## Section 4: QUIC Stream Mapping

### 4.1. Ledger Stream (Stream 0)

The Ledger Stream provides the control plane for PipeStream operations, carrying status updates and synchronization information between endpoints.

#### 4.1.1. Stream Identification

The Ledger Stream MUST use QUIC Stream ID 0, which per [RFC 9000] Section 2.1 is a client-initiated bidirectional stream. Both client and server transmit ledger frames on this single bidirectional stream.

```
   +------------------+
   |  Stream ID = 0   |
   |  Type: Bidi      |
   |  Initiator: CLT  |
   +------------------+
          |
          v
   +------+------+
   |             |
   v             v
 Client        Server
 (writes)      (writes)
```

#### 4.1.2. Stream Properties

Implementations MUST adhere to the following requirements for the Ledger Stream:

1. The client MUST open Stream 0 before any Entity Streams.

2. Stream 0 MUST remain open for the duration of the PipeStream session.

3. Stream 0 MUST NOT carry entity payload data; it is reserved exclusively for ledger frames.

4. Implementations SHOULD assign the Ledger Stream higher priority than Entity Streams using QUIC priority mechanisms [RFC 9218].

#### 4.1.3. Flow Control Considerations

The Ledger Stream carries small, fixed-size frames (4 octets each for basic frames). Implementations MUST ensure adequate flow control credits are maintained:

- The initial MAX_STREAM_DATA for Stream 0 SHOULD be at least 8192 octets, allowing approximately 2048 ledger frames before requiring credit updates.

- Implementations SHOULD NOT block Entity Stream transmission due to Ledger Stream flow control exhaustion.

#### 4.1.4. Keep-Alive and Heartbeat Mechanisms

To maintain session liveness and detect connection failures, PipeStream defines a heartbeat mechanism on the Ledger Stream:

```
   Heartbeat Frame (4 octets):
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |0|0|1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1|0 0 0 0|0 0 0 0 0 0|
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |E|C|<-- Entity ID = 0xFFFFF (20 bits) -->|Stat=0| Flags=0    |
```

The following requirements apply:

1. When no ledger updates have been transmitted for a duration exceeding the KEEPALIVE_TIMEOUT (default: 30 seconds), an endpoint SHOULD send a heartbeat frame.

2. Heartbeat frames use the reserved Entity ID 0xFFFFF with status PENDING (0x0).

3. Upon receiving a heartbeat, an endpoint MAY respond with a corresponding heartbeat frame, though this is NOT REQUIRED.

4. If no data (including heartbeats) is received on Stream 0 for a duration exceeding 3 * KEEPALIVE_TIMEOUT, an endpoint SHOULD consider the connection failed and close it with error code PIPESTREAM_IDLE_TIMEOUT (0x01).

5. Implementations MAY rely on QUIC-level keep-alives [RFC 9000] Section 10.1.2 as an alternative or supplement to PipeStream heartbeats.

#### 4.1.5. Error Handling

If Stream 0 is reset by either endpoint, the PipeStream session MUST be considered terminated. Implementations MUST close the QUIC connection with application error code PIPESTREAM_LEDGER_RESET (0x02).

### 4.2. Entity Streams (Streams 2+)

Entity Streams carry the actual document entity payloads. Each entity is transmitted on a dedicated unidirectional stream, leveraging QUIC's native multiplexing capabilities.

#### 4.2.1. Stream Type and Allocation

Entity Streams MUST be unidirectional streams as defined in [RFC 9000] Section 2.1. The stream allocation follows QUIC conventions:

```
   Client-Initiated Unidirectional Streams:
   Stream IDs: 2, 6, 10, 14, ... (4n + 2 where n >= 0)

   Server-Initiated Unidirectional Streams:
   Stream IDs: 3, 7, 11, 15, ... (4n + 3 where n >= 0)

   +--------------------------------------------------+
   | Stream ID Allocation                             |
   +--------------------------------------------------+
   |  Bits 0-1  |  Stream Type                        |
   +------------+-------------------------------------+
   |    0x0     |  Client-Initiated, Bidirectional   |
   |    0x1     |  Server-Initiated, Bidirectional   |
   |    0x2     |  Client-Initiated, Unidirectional  |
   |    0x3     |  Server-Initiated, Unidirectional  |
   +------------+-------------------------------------+

   Entity Streams use types 0x2 and 0x3 only.
```

#### 4.2.2. One Entity Per Stream

PipeStream employs a strict one-entity-per-stream model:

1. Each Entity Stream MUST carry exactly one entity.

2. The entity_id in the Entity Frame header MUST be unique within the session for that direction (client-to-server or server-to-client).

3. Once an entity has been completely transmitted, the sender MUST close the stream (FIN bit set on final STREAM frame).

4. Implementations MUST NOT reuse a stream for multiple entities.

This design provides several benefits:

- Natural entity delimitation via QUIC stream boundaries
- Independent flow control per entity
- Simplified error handling (stream reset affects single entity)
- Optimal head-of-line blocking characteristics

#### 4.2.3. Stream Lifecycle

The lifecycle of an Entity Stream follows this state machine:

```
                          +------------+
                          |   IDLE     |
                          +-----+------+
                                |
                    Open Stream |
                    Send PENDING|
                    to Ledger   |
                                v
                          +------------+
                          |   OPEN     |
                          +-----+------+
                                |
                    First frame |
                    transmitted |
                    PROCESSING  |
                    to Ledger   |
                                v
                          +------------+
                          | TRANSMIT   |<----+
                          +-----+------+     |
                                |            |
                    +--------+--+--+-----+   |
                    |        |     |     |   |
                    v        v     |     +---+
              +-------+  +------+  | More data
              |COMPLETE| |FAILED|  |
              +---+----+  +--+---+
                  |          |
      FIN stream  |          | RESET_STREAM
      COMPLETE    |          | FAILED
      to Ledger   |          | to Ledger
                  v          v
              +----------------+
              |    CLOSED      |
              +----------------+
```

The following requirements apply to Entity Stream lifecycle:

1. Before opening an Entity Stream, the sender MUST transmit a ledger frame with status PENDING (0x00) for that entity_id.

2. Upon transmitting the first byte of entity data, the sender MUST transmit a ledger frame with status PROCESSING (0x01).

3. Upon successful completion (stream closed with FIN), the sender MUST transmit a ledger frame with status COMPLETE (0x02).

4. If the stream is reset (RESET_STREAM frame), the sender MUST transmit a ledger frame with status FAILED (0x03).

5. A receiver that resets an Entity Stream (STOP_SENDING) SHOULD expect a corresponding FAILED status on the Ledger Stream.

#### 4.2.4. Entity ID to Stream Correlation

The correlation between Entity Streams and ledger updates is established through the entity_id field:

```
   Ledger Stream (Stream 0)         Entity Streams
   +------------------------+       +------------------------+
   | entity_id=0x0001       |       | Stream 2               |
   | status=PENDING         |  +--> | entity_id=0x0001       |
   +------------------------+  |    | [payload...]           |
             |                 |    +------------------------+
             +-----------------+

   +------------------------+       +------------------------+
   | entity_id=0x0001       |       | Stream 6               |
   | status=PROCESSING      |       | entity_id=0x0002       |
   +------------------------+       | [payload...]           |
                                    +------------------------+
   +------------------------+
   | entity_id=0x0002       |
   | status=PENDING         |
   +------------------------+
```

Implementations MUST track the mapping between entity_id values and their corresponding QUIC stream IDs. This mapping is implicit: the entity_id in the Entity Frame header on a given stream establishes the correlation.

#### 4.2.5. Concurrency and Ordering

1. Multiple Entity Streams MAY be open concurrently. The maximum number of concurrent streams is governed by QUIC transport parameters (initial_max_streams_uni).

2. Ledger updates for different entities MAY arrive out of order relative to entity payload receipt due to QUIC's per-stream ordering guarantees.

3. Implementations MUST be prepared to receive ledger updates (e.g., PROCESSING) before the corresponding Entity Stream data arrives.

4. For recursive entities (entities with parent_id referencing another entity), implementations SHOULD ensure the parent entity's stream is opened before child entity streams, though this is NOT REQUIRED.

#### 4.2.6. Priority

Entity Streams SHOULD be assigned lower priority than the Ledger Stream. Among Entity Streams:

- Implementations MAY assign equal priority to all Entity Streams.
- Implementations MAY prioritize based on entity layer (lower layer = higher priority).
- Implementations MAY prioritize based on parent-child relationships.

Priority signaling SHOULD use the Extensible Priority scheme defined in [RFC 9218].

---

## Section 5: Frame Formats

This section defines the wire formats for PipeStream frames. All multi-octet integer fields are encoded in network byte order (big-endian) unless otherwise specified.

### 5.1. Ledger Frames

Ledger frames are fixed-size control messages transmitted on Stream 0. They provide lightweight status updates for entity processing coordination.

#### 5.1.1. Basic Ledger Frame Format

A basic ledger frame is exactly 4 octets (32 bits), word-aligned:

```
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |E|C|              Entity ID (20 bits)         |Stat |  Flags  |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

   E (1 bit):
      Extended frame flag. When set, additional data follows the
      basic 4-octet frame.

   C (1 bit):
      Cursor update flag. When set, a 3-octet cursor value follows.

   Entity ID (20 bits):
      Unsigned integer identifying the entity within the current scope.
      Range 0x00000-0xFFFFD for regular entities.
      0xFFFFE: SCOPE_MARKER (Layer 1)
      0xFFFFF: CONNECTION_LEVEL (heartbeat, shutdown)

   Stat (4 bits):
      Status code (see Section 5.1.3).

   Flags (6 bits):
      Reserved for future use. MUST be zero when sent.
      Receivers MUST ignore non-zero flags.
```

#### 5.1.2. Entity ID Encoding

The Entity ID field is a 20-bit unsigned integer. Within a basic frame, it occupies the last 4 bits of the first octet, all of the second octet, and the first 8 bits of the third octet.

#### 5.1.3. Status Code Encoding

The status field (4 bits) indicates the current processing state of an entity:

| Value | Name        | Layer | Description                            |
|-------|-------------|-------|----------------------------------------|
| 0x0   | PENDING     | 0     | Entity announced, not yet transmitting |
| 0x1   | PROCESSING  | 0     | Entity transmission in progress        |
| 0x2   | COMPLETE    | 0     | Entity successfully processed          |
| 0x3   | FAILED      | 0     | Entity processing failed               |
| 0x4   | CHECKPOINT  | 0     | Synchronization barrier                |
| 0x5   | VAPORIZING  | 0     | Decomposing into children              |
| 0x6   | AGGREGATING | 0     | Rejoining children                     |
| 0x7   | Reserved    | -     | Reserved                               |
| 0x8   | YIELDED     | 2     | Paused with continuation token         |
| 0x9   | DEFERRED    | 2     | Detached with claim check              |
| 0xA   | RETRYING    | 2     | Retry in progress                      |
| 0xB   | SKIPPED     | 2     | Intentionally skipped (lenient mode)   |
| 0xC   | ABANDONED   | 2     | Timed out, cursor advanced past        |
| 0xD-0xF | Reserved  | -     | Reserved for future use                |

#### 5.1.4. Reserved Entity ID Values

| Value   | Name              | Purpose                            |
|---------|-------------------|------------------------------------|
| 0x00000 | NULL_ENTITY       | Reserved; MUST NOT be used         |
| 0xFFFFE | SCOPE_MARKER      | Scope operations (Layer 1)         |
| 0xFFFFF | CONNECTION_LEVEL  | Connection-wide control messages   |

##### 5.1.4.1. CONNECTION_LEVEL Frames (0xFFFFF)

Frames with Entity ID 0xFFFFF apply to the entire connection. Status codes for connection-level frames may have different semantics (e.g., 0x0 for Heartbeat).

#### 5.1.5. Extended Ledger Frames (E=1)

The format of the extended data depends on the Stat field and negotiated capabilities.

##### 5.1.5.1. Yield Frame (Status = 0x8)

When Status = YIELDED (0x8) and E=1:

```
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |1|C|              Entity ID (20)              |1000 |  Flags  |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   | Yield Reason  |         Token Length (12 bits)               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                                                               |
   |                  Yield Token (variable)                       |
   |                  (up to 4095 bytes)                           |
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

##### 5.1.5.2. Claim Check Frame (Status = 0x9)

When Status = DEFERRED (0x9) and E=1:

```
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |1|C|              Entity ID (20)              |1001 |  Flags  |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                                                               |
   |                    Claim Check ID (64 bits)                   |
   |                                                               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                    Expiry Timestamp (32 bits)                 |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

#### 5.1.6. Cursor Update Extension (C=1)

When C=1, a 3-octet cursor update follows the basic (or extended) frame:

```
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |        New Cursor Value (20 bits)    |Reserv |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

The cursor indicates the lowest unresolved Entity ID. IDs below the cursor are considered resolved and MAY be recycled.


### 5.2. Entity Frames

Entity frames carry the actual document entity data on Entity Streams. They consist of a length-prefixed header followed by the payload.

#### 5.2.1. Entity Frame Overview

```
   Entity Frame Structure:

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

#### 5.2.2. Entity Frame Header

The entity frame header is encoded using Protocol Buffers (proto3). The wire format is length-prefixed to allow efficient parsing:

```
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                      Header Length                            |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                                                               |
   |                   Protobuf-Encoded Header                     |
   |                      (Header Length octets)                   |
   |                                                               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

   Header Length (32 bits):
      Unsigned integer specifying the length of the Protobuf-encoded
      header in octets. Big-endian byte order. MUST NOT exceed
      65535 (64 KiB - 1).
```

The Protobuf schema for the header is:

```protobuf
   syntax = "proto3";

   message EntityHeader {
       // Scope-local entity identifier (20-bit)
       uint32 entity_id = 1;

       // Entity ID of the parent entity (0 if root-level)
       uint32 parent_id = 2;

       // Identifier of the scope (Layer 1)
       uint32 scope_id = 3;

       // Data layer (0=BlobBag, 1=Semantic, 2=Parsed, 3=Custom)
       uint32 layer = 4;

       // MIME type of the payload
       string content_type = 5;

       // Length of the payload in octets
       uint64 payload_length = 6;

       // SHA-256 checksum of the payload (32 octets)
       bytes checksum = 7;

       // Optional metadata fields
       map<string, string> metadata = 8;

       // Chunking information (if payload is chunked)
       ChunkInfo chunk_info = 9;

       // Completion policy (Layer 2)
       CompletionPolicy completion_policy = 10;
   }
```

#### 5.2.3. Header Field Specifications

##### 5.2.3.1. entity_id (Field 1)

- MUST be a non-zero unsigned integer in the range 1-1,048,573 (0x00001-0xFFFFD).
- MUST be unique within its scope for the sending direction.
- MUST correspond to the entity_id used in ledger frame updates for this entity.

##### 5.2.3.2. parent_id (Field 2)

- For root-level entities (original documents), MUST be 0.
- For extracted/child entities, MUST contain the entity_id of the parent entity from which this entity was derived.
- The referenced parent entity MUST have been announced (PENDING status) before this entity.

##### 5.2.3.3. scope_id (Field 3)

- Identifies the scope to which this entity belongs.
- Set to 0 when Layer 1 (Recursive) is not negotiated.

##### 5.2.3.4. layer (Field 4)

- Indicates the data layer of the entity's payload.
- 0: BlobBag, 1: SemanticLayer, 2: ParsedData, 3: CustomEntity.

##### 5.2.3.5. content_type (Field 5)

- MUST be a valid MIME type as defined in [RFC 2046].

##### 5.2.3.6. payload_length (Field 6)

- MUST accurately specify the total length of the payload in octets.
- Receivers MUST validate that the actual payload matches this length.

##### 5.2.3.7. checksum (Field 7)

- MUST contain the SHA-256 hash of the payload.
- MUST be exactly 32 octets.
- Receivers MUST validate the checksum and report FAILED status if validation fails.

#### 5.2.4. Payload Format

The payload immediately follows the header with no padding.

#### 5.2.5. Chunking for Large Payloads

For entities exceeding a configurable threshold (default: 16 MiB), implementations MAY split the payload across multiple Entity Streams using the chunking mechanism.

#### 5.2.6. Checksum Algorithm

PipeStream uses SHA-256 [FIPS 180-4] for payload integrity verification.
