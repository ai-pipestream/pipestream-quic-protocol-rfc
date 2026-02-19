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

The Ledger Stream carries small, fixed-size frames (3 octets each). Implementations MUST ensure adequate flow control credits are maintained:

- The initial MAX_STREAM_DATA for Stream 0 SHOULD be at least 4096 octets, allowing approximately 1365 ledger frames before requiring credit updates.

- Implementations SHOULD NOT block Entity Stream transmission due to Ledger Stream flow control exhaustion. If Ledger Stream credits are depleted, implementations MUST prioritize sending STREAM_DATA_BLOCKED frames for Stream 0.

- Receivers MUST process Ledger Stream data promptly and issue MAX_STREAM_DATA frames to prevent sender blocking.

#### 4.1.4. Keep-Alive and Heartbeat Mechanisms

To maintain session liveness and detect connection failures, PipeStream defines a heartbeat mechanism on the Ledger Stream:

```
   Heartbeat Frame (3 octets):
   +----------------+----------+
   | Entity ID      | Status   |
   | 0xFFFF         | 0x00     |
   +----------------+----------+
```

The following requirements apply:

1. When no ledger updates have been transmitted for a duration exceeding the KEEPALIVE_TIMEOUT (default: 30 seconds), an endpoint SHOULD send a heartbeat frame.

2. Heartbeat frames use the reserved Entity ID 0xFFFF with status PENDING (0x00).

3. Upon receiving a heartbeat, an endpoint MAY respond with a corresponding heartbeat frame, though this is NOT REQUIRED.

4. If no data (including heartbeats) is received on Stream 0 for a duration exceeding 3 * KEEPALIVE_TIMEOUT, an endpoint SHOULD consider the connection failed and close it with error code PIPESTREAM_IDLE_TIMEOUT (0x01).

5. Implementations MAY rely on QUIC-level keep-alives [RFC 9000] Section 10.1.2 as an alternative or supplement to PipeStream heartbeats.

#### 4.1.5. Error Handling

If Stream 0 is reset by either endpoint, the PipeStream session MUST be considered terminated. Implementations MUST close the QUIC connection with application error code PIPESTREAM_LEDGER_RESET (0x02).

### 4.2. Entity Streams (Streams 1+)

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

A basic ledger frame is exactly 3 octets:

```
    0                   1                   2
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |         Entity ID             |    Status     |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

   Entity ID (16 bits):
      Unsigned integer identifying the entity. Range 0x0000-0xFFFD
      for regular entities; 0xFFFE-0xFFFF reserved.

   Status (8 bits):
      Single octet indicating entity processing state.
```

#### 5.1.2. Entity ID Encoding

The Entity ID field is a 16-bit unsigned integer encoded in big-endian (network) byte order:

```
   Bit Layout:

    0                   1
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |    High Octet |   Low Octet   |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

   Value = (High Octet << 8) | Low Octet

   Examples:
      Entity ID 1:      0x00 0x01
      Entity ID 256:    0x01 0x00
      Entity ID 65533:  0xFF 0xFD
```

Entity IDs MUST be allocated sequentially starting from 0x0001 within each session direction. Entity ID 0x0000 is reserved and MUST NOT be used for regular entities.

#### 5.1.3. Status Byte Encoding

The status byte indicates the current processing state of an entity:

```
   +-------+-------------+----------------------------------------+
   | Value | Name        | Description                            |
   +-------+-------------+----------------------------------------+
   | 0x00  | PENDING     | Entity announced, transmission not     |
   |       |             | yet started                            |
   +-------+-------------+----------------------------------------+
   | 0x01  | PROCESSING  | Entity data transmission in progress   |
   +-------+-------------+----------------------------------------+
   | 0x02  | COMPLETE    | Entity successfully transmitted and    |
   |       |             | stream closed                          |
   +-------+-------------+----------------------------------------+
   | 0x03  | FAILED      | Entity transmission failed; stream     |
   |       |             | reset or error occurred                |
   +-------+-------------+----------------------------------------+
   | 0x04  | CHECKPOINT  | Synchronization point (see Section     |
   |       |             | 5.1.5)                                 |
   +-------+-------------+----------------------------------------+
   | 0x05- | Reserved    | Reserved for future use                |
   | 0xFF  |             |                                        |
   +-------+-------------+----------------------------------------+
```

Implementations receiving an unrecognized status value MUST ignore the ledger frame and MAY log a warning. They MUST NOT close the connection.

#### 5.1.4. Reserved Entity ID Values

The following Entity ID values are reserved for special purposes:

```
   +--------+-------------------+------------------------------------+
   | Value  | Name              | Purpose                            |
   +--------+-------------------+------------------------------------+
   | 0x0000 | NULL_ENTITY       | Reserved; MUST NOT be used         |
   +--------+-------------------+------------------------------------+
   | 0xFFFE | CHECKPOINT_MARKER | Used with CHECKPOINT status for    |
   |        |                   | synchronization points             |
   +--------+-------------------+------------------------------------+
   | 0xFFFF | CONNECTION_LEVEL  | Connection-wide control messages   |
   |        |                   | (heartbeat, shutdown, etc.)        |
   +--------+-------------------+------------------------------------+
```

##### 5.1.4.1. CONNECTION_LEVEL Frames (0xFFFF)

Frames with Entity ID 0xFFFF apply to the entire connection:

```
   Connection-Level Status Values:

   +--------+-------------------+------------------------------------+
   | Status | Name              | Purpose                            |
   +--------+-------------------+------------------------------------+
   | 0x00   | HEARTBEAT         | Keep-alive signal                  |
   +--------+-------------------+------------------------------------+
   | 0x01   | SHUTDOWN_INIT     | Graceful shutdown initiated        |
   +--------+-------------------+------------------------------------+
   | 0x02   | SHUTDOWN_COMPLETE | All entities complete, ready to    |
   |        |                   | close                              |
   +--------+-------------------+------------------------------------+
   | 0x03   | ERROR             | Connection-level error occurred    |
   +--------+-------------------+------------------------------------+

   Example - Heartbeat Frame:
    0                   1                   2
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1|0 0 0 0 0 0 0 0|
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |<-------- 0xFFFF ------------>|<---- 0x00 --->|
```

##### 5.1.4.2. CHECKPOINT_MARKER Frames (0xFFFE)

Frames with Entity ID 0xFFFE signal synchronization checkpoints:

```
   Checkpoint Frame:
    0                   1                   2
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 0|0 0 0 0 0 1 0 0|
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |<-------- 0xFFFE ------------>|<---- 0x04 --->|
```

Checkpoint frames indicate that all entities announced prior to this frame have been fully transmitted. Upon receiving a checkpoint frame, an implementation MAY:

- Commit processed entities to stable storage
- Release buffered entities to downstream processors
- Acknowledge the checkpoint to the sender

#### 5.1.5. Extended Ledger Frames

For use cases requiring additional metadata (such as Parts Ledger updates in recursive processing), PipeStream defines an extended ledger frame format:

```
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |         Entity ID             |E|  Status     | Ext Length    |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                                                               |
   |                    Extension Data                             |
   |                    (variable length)                          |
   |                                                               |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

   E (Extended Flag, 1 bit):
      If set to 1, indicates this is an extended ledger frame.
      The frame includes an Ext Length field and Extension Data.
      If set to 0, this is a basic 3-octet ledger frame.

   Status (7 bits):
      Same semantics as basic frame, but limited to 7 bits.
      Values 0x00-0x7F are valid.

   Ext Length (8 bits):
      Length of Extension Data in octets. Range 0-255.

   Extension Data (variable):
      Extension-specific payload.
```

##### 5.1.5.1. Parts Ledger Extension

The Parts Ledger extension (Extension Type 0x01) reports child entity counts for recursive processing:

```
   Parts Ledger Extension Format:

    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   | Ext Type=0x01 |         Child Count           | Reserved      |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

   Ext Type (8 bits):
      0x01 for Parts Ledger extension.

   Child Count (16 bits):
      Number of child entities that will be produced from this
      parent entity. Big-endian unsigned integer.

   Reserved (8 bits):
      Reserved for future use. MUST be set to 0x00.
      Receivers MUST ignore this field.
```

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
       // Unique identifier for this entity within the session
       // MUST match the entity_id used in ledger frames
       uint32 entity_id = 1;

       // Entity ID of the parent entity (0 if root-level)
       uint32 parent_id = 2;

       // Recursion depth (0 = root document, 1 = first-level
       // extracted entity, etc.)
       uint32 layer = 3;

       // MIME type of the payload (e.g., "application/pdf",
       // "text/plain")
       string content_type = 4;

       // Length of the payload in octets
       uint64 payload_length = 5;

       // SHA-256 checksum of the payload (32 octets)
       bytes checksum = 6;

       // Optional metadata fields
       map<string, string> metadata = 7;

       // Chunking information (if payload is chunked)
       ChunkInfo chunk_info = 8;
   }

   message ChunkInfo {
       // Total number of chunks for this entity
       uint32 total_chunks = 1;

       // Index of this chunk (0-based)
       uint32 chunk_index = 2;

       // Offset of this chunk within the complete payload
       uint64 chunk_offset = 3;
   }
```

#### 5.2.3. Header Field Specifications

##### 5.2.3.1. entity_id (Field 1)

- MUST be a non-zero unsigned integer in the range 1-65533 (0x0001-0xFFFD).
- MUST be unique within the session for the sending direction.
- MUST correspond to the entity_id used in ledger frame updates for this entity.

##### 5.2.3.2. parent_id (Field 2)

- For root-level entities (original documents), MUST be 0.
- For extracted/child entities, MUST contain the entity_id of the parent entity from which this entity was derived.
- The referenced parent entity MUST have been announced (PENDING status) before this entity.

##### 5.2.3.3. layer (Field 3)

- Indicates the recursion depth in the entity hierarchy.
- Root documents MUST have layer = 0.
- Entities extracted from a layer N entity MUST have layer = N + 1.
- Implementations SHOULD enforce a maximum layer depth (recommended: 16).

##### 5.2.3.4. content_type (Field 4)

- MUST be a valid MIME type as defined in [RFC 2046].

##### 5.2.3.5. payload_length (Field 5)

- MUST accurately specify the total length of the payload in octets.
- Receivers MUST validate that the actual payload matches this length.

##### 5.2.3.6. checksum (Field 6)

- MUST contain the SHA-256 hash of the payload.
- MUST be exactly 32 octets.
- Receivers MUST validate the checksum and report FAILED status if validation fails.

#### 5.2.4. Payload Format

The payload immediately follows the header with no padding.

#### 5.2.5. Chunking for Large Payloads

For entities exceeding a configurable threshold (default: 16 MiB), implementations MAY split the payload across multiple Entity Streams using the chunking mechanism.

#### 5.2.6. Checksum Algorithm

PipeStream uses SHA-256 [FIPS 180-4] for payload integrity verification.
