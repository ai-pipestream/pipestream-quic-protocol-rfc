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

The Control Stream carries bit-packed control frames. STATUS frames are 12 octets base (16 with cursor extension), and additional fixed/variable UCF frames may be present. Implementations MUST ensure adequate flow control credits:

- The initial MAX_STREAM_DATA for Stream 0 SHOULD be at least 8192 octets.
- Implementations SHOULD NOT block Entity Stream transmission due to Control Stream flow control exhaustion.

#### 5.1.4. Heartbeat Mechanism

To maintain session liveness, an endpoint sends a STATUS frame with all fields set to their heartbeat values:

| Field | Value | Description |
|-------|-------|-------------|
| Type | 0x50 | STATUS frame |
| Stat | 0x0 (UNSPECIFIED) | Heartbeat signal |
| E | 0 | No extension data |
| C | 0 | No cursor update |
| D | 0 | Root depth |
| Flags | 0 | Reserved bits zero |
| Entity ID | 0xFFFFFFFF | CONNECTION_LEVEL reserved identifier |
| Scope ID | 0x0000 | Root scope |
| Reserved | 0x0000 | MUST be zero |

When no status updates have been transmitted for KEEPALIVE_TIMEOUT (default: 30 seconds), an endpoint SHOULD send a heartbeat frame. If no data is received on Stream 0 for 3 * KEEPALIVE_TIMEOUT, the connection SHOULD be closed with PIPESTREAM_IDLE_TIMEOUT (0x02).

#### 5.1.5. Transport Session vs. Application Session Context

QUIC (and HTTP/3 when used as a substrate) already provides transport-level streaming semantics: ordered byte delivery per stream, multiplexing, and flow control. Therefore, Layer 0 PipeStream operation does not require a separate application-level `session_id`; connection and stream identifiers are sufficient for in-band streaming.

An application-level session context is required only for workflows that outlive a single transport connection, such as Layer 2 yield/resume and claim-check redemption across reconnects or different processing nodes. Implementations MAY represent this context with a `session_id` or an equivalent stable context identifier.

### 5.2. Entity Streams (Streams 2+)

Entity Streams carry the actual document entity payloads.

#### 5.2.1. Stream Type and Allocation

Entity Streams MUST be unidirectional streams:

| Direction | Stream ID Formula | Examples |
|-----------|-------------------|----------|
| Client-Initiated | 4n + 2 (n >= 0) | 2, 6, 10, 14, ... |
| Server-Initiated | 4n + 3 (n >= 0) | 3, 7, 11, 15, ... |

#### 5.2.2. One Entity Per Stream

1. Each Entity Stream MUST carry exactly one entity.
2. The entity_id in the Entity Frame header MUST be unique within its scope.
3. Once an entity has been completely transmitted, the sender MUST close the stream.
