## 5. QUIC Stream Mapping

### 5.1. Control Stream (Stream 0)

The Control Stream provides the control plane for PipeStream operations.

#### 5.1.1. Stream Identification

The Control Stream MUST use QUIC Stream ID 0, which per {{RFC9000}} Section 2.1 is a client-initiated bidirectional stream.

#### 5.1.2. Stream Properties

1. The client MUST open Stream 0 before any Entity Streams.
2. Stream 0 MUST remain open for the duration of the PipeStream session.
3. Stream 0 MUST NOT carry entity payload data.
4. Implementations SHOULD assign the Control Stream higher priority than Entity Streams.

#### 5.1.3. Flow Control Considerations

The Control Stream carries small, fixed-size frames (4 octets each for basic frames). Implementations MUST ensure adequate flow control credits:

- The initial MAX_STREAM_DATA for Stream 0 SHOULD be at least 8192 octets.
- Implementations SHOULD NOT block Entity Stream transmission due to Control Stream flow control exhaustion.

#### 5.1.4. Heartbeat Mechanism

To maintain session liveness:

```
   Heartbeat Frame (8 octets):
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1|
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0|0 0 0 0|0|0|0 0 0 0 0 0 0 0 0 0|
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

   Entity ID = 0xFFFFFFFF (CONNECTION_LEVEL)
   Scope ID = 0x0000
   Status = 0x0 (UNSPECIFIED, used as heartbeat signal)
   E=0 (no extended data), C=0 (no cursor update)
   Flags = 0x000
```

When no status updates have been transmitted for KEEPALIVE_TIMEOUT (default: 30 seconds), an endpoint SHOULD send a heartbeat frame. If no data is received on Stream 0 for 3 * KEEPALIVE_TIMEOUT, the connection SHOULD be closed with PIPESTREAM_IDLE_TIMEOUT (0x02).

### 5.2. Entity Streams (Streams 2+)

Entity Streams carry the actual document entity payloads.

#### 5.2.1. Stream Type and Allocation

Entity Streams MUST be unidirectional streams:

```
   Client-Initiated Unidirectional Streams:
   Stream IDs: 2, 6, 10, 14, ... (4n + 2 where n >= 0)

   Server-Initiated Unidirectional Streams:
   Stream IDs: 3, 7, 11, 15, ... (4n + 3 where n >= 0)
```

#### 5.2.2. One Entity Per Stream

1. Each Entity Stream MUST carry exactly one entity.
2. The entity_id in the Entity Frame header MUST be unique within its scope.
3. Once an entity has been completely transmitted, the sender MUST close the stream.
