# QUIC Stream Mapping

PipeStream leverages the native multiplexing capabilities of QUIC {{RFC9000}} to provide a clean separation between control coordination and data transmission.

## Control Stream (Stream 0)

The Control Stream provides the control plane for PipeStream operations.

### Stream Identification

The Control Stream MUST use QUIC Stream ID 0, which per RFC 9000 is a bidirectional, client-initiated stream.

### Usage Rules

1. The Control Stream MUST be opened immediately upon connection establishment.
2. Capability negotiation (Section 3.4) MUST occur on Stream 0 before any Entity Streams are opened.
3. Stream 0 MUST NOT carry entity payload data.
4. Implementations SHOULD assign the Control Stream a high priority to ensure timely delivery of status updates.

### Flow Control Considerations

The Control Stream carries small, bit-packed control frames. STATUS frames are 12 octets base. Implementations MUST ensure adequate flow control credits:

- The initial MAX_STREAM_DATA for Stream 0 SHOULD be at least 8192 octets.
- Implementations SHOULD NOT block Entity Stream transmission due to Control Stream flow control exhaustion.

### Heartbeat Mechanism

QUIC already provides native transport liveness signals (for example, PING and idle timeout handling). Implementations SHOULD rely on those transport mechanisms for connection liveness.

PipeStream heartbeat frames are OPTIONAL and are intended for application-level responsiveness checks (for example, detecting stalled processing logic even when the transport remains healthy). When used, an endpoint sends a STATUS frame with all fields set to their heartbeat values:

| Field | Value | Description |
|-------|-------|-------------|
| Type | 0x50 (STATUS) | |
| Stat | 0x0 (UNSPECIFIED) | Heartbeat signal |
| Entity ID | 0xFFFFFFFF | CONNECTION_LEVEL |
| Scope ID | 0x0000 | Root scope |
| Reserved | 0x0000 | MUST be zero |

When no status updates have been transmitted for KEEPALIVE_TIMEOUT (default: 30 seconds), an endpoint MAY send a heartbeat frame. If no data is received on Stream 0 for 3 * KEEPALIVE_TIMEOUT, the endpoint SHOULD first apply transport-native liveness policy; it MAY close the connection with PIPESTREAM_IDLE_TIMEOUT (0x02) when application-level inactivity policy requires it.

### Transport Session vs. Application Session Context

The `session-id` segment identifies application context for detached or resumable resources (for example, Layer 2 yield/claim-check flows). PipeStream Layer 0 streaming semantics do not depend on this URI scheme.

## Entity Streams (Streams 2+)

Entity Streams carry the actual document entity data.

### Unidirectional Data Flow

Entity Streams MUST be unidirectional streams:

| Stream Type | Client to Server | Server to Client |
|-------------|-------------------|----------|
| Client-Initiated | 4n + 2 (n >= 0) | 2, 6, 10, 14, ... |
| Server-Initiated | 4n + 3 (n >= 0) | 3, 7, 11, 15, ... |

### One Entity Per Stream

1. Each Entity Stream MUST carry exactly one entity.
2. The entity_id in the Entity Frame header MUST be unique within its scope.
3. Once an entity has been completely transmitted, the sender MUST close the stream.

## Transport Error Mapping

PipeStream error signaling on Stream 0 and QUIC transport signals are complementary. Endpoints SHOULD bridge them so peers receive both transport-level and protocol-level context.

1. If an Entity Stream is aborted with `RESET_STREAM` or `STOP_SENDING`, the endpoint SHOULD emit a corresponding terminal status (`FAILED`, `ABANDONED`, or policy-driven equivalent) for that entity on Stream 0.
2. If PipeStream determines a terminal entity error first (for example, checksum failure or invalid frame), the endpoint SHOULD abort the affected Entity Stream with an appropriate QUIC error and emit the corresponding PipeStream status/error context on Stream 0.
3. If Stream 0 is reset or becomes unusable, endpoints SHOULD treat this as a control-plane failure and close the connection with `PIPESTREAM_CONTROL_RESET (0x03)`.
4. On QUIC connection termination (`CONNECTION_CLOSE`), entities without a previously observed terminal status MUST be treated as failed by local policy.
