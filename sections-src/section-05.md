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
4. Implementations SHOULD assign the Control Stream a high priority to ensure timely delivery of status updates. An implementation MAY choose a different priority policy when operating in constrained environments where QUIC stream scheduling overhead must be minimized.

### Flow Control Considerations

The Control Stream carries small, bit-packed control frames. STATUS frame payloads are 16 octets base (21 octets on wire including the 5-octet UCF header). Implementations MUST ensure adequate flow control credits:

- The initial MAX_STREAM_DATA for Stream 0 SHOULD be at least 8192 octets. A lower value is permissible for extremely constrained devices but risks stalling status delivery.
- Implementations SHOULD NOT block Entity Stream transmission due to Control Stream flow control exhaustion. In rare cases where strict ordering between control and data planes is required by the application, an implementation MAY temporarily pause entity transmission until control stream credits are replenished.

### Heartbeat Mechanism

QUIC already provides native transport liveness signals (for example, PING and idle timeout handling). Implementations SHOULD rely on those transport mechanisms for connection liveness.

PipeStream heartbeat frames are OPTIONAL and are intended for application-level responsiveness checks (for example, detecting stalled processing logic even when the transport remains healthy). When used, an endpoint sends a STATUS frame with all fields set to their heartbeat values:

| Field | Value | Description |
|-------|-------|-------------|
| Type | 0x50 (STATUS) | |
| Length | 16 | STATUS payload length with no cursor or extension |
| Stat | 0x0 (UNSPECIFIED) | Heartbeat signal |
| Entity ID | 0xFFFFFFFF | CONNECTION_LEVEL |
| Scope ID | 0x00000000 | Root scope |
| Reserved | 0x00000000 | MUST be zero |

The KEEPALIVE_TIMEOUT defaults to 30 seconds. Endpoints MAY negotiate a different value by including a `keepalive-timeout-ms` field (in milliseconds) in the capabilities exchange (Section 3.4); the effective timeout is the minimum of the two peers' advertised values.

When no status updates have been transmitted for KEEPALIVE_TIMEOUT, an endpoint MAY send a heartbeat frame. If no data is received on Stream 0 for 3 * KEEPALIVE_TIMEOUT, the endpoint SHOULD first apply transport-native liveness policy (e.g., QUIC PING); it MAY close the connection with PIPESTREAM_IDLE_TIMEOUT (0x02) when application-level inactivity policy requires it.

A valid heartbeat does not change entity state, advance the cursor, or require a response. A receiver MUST continue parsing subsequent control frames normally.

### Transport Session vs. Application Session Context

The `session-id` segment of the pipestream URI scheme (Section 11.6) identifies application context for detached or resumable resources (for example, Layer 2 yield/claim-check flows). PipeStream Layer 0 streaming semantics do not depend on this URI scheme.

### Interaction with QUIC Flow Control and Congestion Control

PipeStream relies on the flow control and congestion control mechanisms provided by QUIC {{RFC9000}} and does not define its own transport-layer congestion control. QUIC provides flow control at both the stream level (MAX_STREAM_DATA) and the connection level (MAX_DATA). PipeStream's cursor-based backpressure (Section 9.1) operates at the application layer and is complementary to QUIC flow control:

- QUIC flow control limits cumulative stream offsets and aggregate stream data on a connection. Receivers extend these limits as data is consumed; congestion control separately limits bytes in flight.
- PipeStream backpressure limits the number of entities in flight (i.e., the number of Entity IDs between cursor and last_assigned).

When connection-level flow control credit is exhausted, Entity Streams cannot transmit additional flow-controlled data even if the entity window has capacity. Conversely, an entity window that is full prevents new announcements or Entity Streams under the negotiated lifecycle even when QUIC credit is available. Implementations MUST respect both limits.

A streaming receiver SHOULD consume payloads incrementally and extend QUIC credit while enforcing separate limits on admitted entities, buffered bytes, and spooled storage. A sender need not wait for credit covering an entire entity before opening its stream. Requiring that credit while the receiver waits for payload consumption before extending it can deadlock an entity larger than the initial window. A whole-message admission strategy is also possible, but it needs explicit bounded message sizes and sufficient credit, or a refusal before transfer; it is not a prerequisite for streaming. These alternatives and cross-stream dependencies are discussed in Section 4.4 of {{RFC9308}}.

Implementations MUST keep control consumption able to progress independently of blocked payload processing. Receive-credit policy and send scheduling SHOULD preserve capacity for control traffic, including when entity streams share the connection budget. Stream priority alone does not create flow-control credit or guarantee progress.

## Entity Streams

Entity Streams carry entity payload data using the Entity Frame format defined in Section 6.8.

### Stream Identification and Direction

Each Entity Stream is a QUIC unidirectional stream. Either endpoint MAY open Entity Streams once the initial Capabilities exchange (Section 3.4) has completed. Per {{RFC9000}}, client-initiated unidirectional streams carry Stream IDs 2, 6, 10, and so on, and server-initiated unidirectional streams carry Stream IDs 3, 7, 11, and so on.

Endpoints MUST NOT open bidirectional streams other than Stream 0. An endpoint that receives a bidirectional stream other than Stream 0 MUST treat this as a connection error of type PIPESTREAM_FRAME_ERROR (0x0D).

### One Entity per Stream

Each Entity Stream carries exactly one Entity Frame (Section 6.8): a Header Length prefix, a serialized EntityHeader, and the payload octets. The following rules apply:

1. The sender MUST close the stream (QUIC FIN) immediately after the final payload octet. The end of the stream delimits the payload.
2. When the EntityHeader includes a `payload-length` field, the receiver MUST verify that the number of payload octets received before the FIN equals that value. A mismatch MUST be treated as a stream error of type PIPESTREAM_ENTITY_INVALID (0x05).
3. A sender that abandons transmission of an entity SHOULD abort the stream with RESET_STREAM carrying an appropriate PipeStream error code; the receiver MUST discard any partial payload data (see Section 5.5).
4. A receiver that no longer requires an entity MAY send STOP_SENDING on the corresponding Entity Stream.
5. Additional data received on an Entity Stream after a complete Entity Frame MUST be treated as a stream error of type PIPESTREAM_FRAME_ERROR (0x0D).

## Prohibition of 0-RTT Early Data

QUIC permits applications to send data in 0-RTT early data before the TLS handshake completes; such data is replayable by an attacker. PipeStream capability negotiation establishes per-connection state whose replay could alter negotiated limits or serialization formats.

Endpoints MUST NOT send PipeStream frames in 0-RTT early data, and a server MUST NOT process PipeStream frames received in early data before the QUIC handshake is confirmed. A future extension may define 0-RTT semantics along with the required replay protections.

## Performance Considerations

### Entity Granularity and Stream Overhead

PipeStream's "one entity per stream" model (Section 5.2) provides clean multiplexing and flow control but introduces per-stream overhead (QUIC STREAM frame headers and internal stack state).

1. **Small Payloads:** For workloads consisting of many small entities (e.g., <1 KiB), the per-stream overhead may become significant. Implementations SHOULD avoid excessive fragmentation and prefer coarser entity granularity where possible.
2. **Aggregation:** Application profiles MAY define mechanisms for bundling multiple small logical units into a single PipeStream entity to reduce transport-layer overhead.
3. **Stream Limits:** Senders MUST respect the peer's QUIC `initial_max_streams_uni` and `MAX_STREAMS` limits. High-frequency entity producers SHOULD monitor stream credit availability and adjust dehydration rates accordingly to avoid stalling the pipeline.

### Control Stream Priority

Implementations SHOULD assign the Control Stream (Stream 0) a higher priority than Entity Streams. Timely delivery of STATUS and SCOPE_DIGEST frames is critical for advancing the cursor and releasing backpressure (Section 9.1).

## Transport Error Mapping

Transport observations and authoritative work outcomes are distinct. A reset or lost connection can occur after work committed but before its report arrived. Endpoints MUST NOT infer a terminal work outcome, advance a completion cursor, or authorize re-execution solely from such an observation.

1. On `RESET_STREAM` or `STOP_SENDING`, discard incomplete receive buffers as appropriate, but retain declared obligations and already admitted durable state. Only the processing authority may report a lifecycle transition, and only when permitted by the negotiated state machine and completion policy. A transport abort is not an application cancellation command.
2. If the processing authority determines an entity error (for example, checksum failure), it SHOULD abort the affected Entity Stream and report the applicable PipeStream error or authorized lifecycle transition on Stream 0 when usable. An invalid or unadmitted declared payload remains outstanding under Section 9.8; a stream error cannot manufacture its resolution.
3. If Stream 0 is reset or becomes unusable, endpoints MUST close the connection with `PIPESTREAM_CONTROL_RESET (0x03)`. Any supported reconnect establishes a separate connection and revalidates authentication and negotiated lifecycle; it does not repair the unusable Control Stream.
4. On connection termination, preserve previously validated terminal observations. Other outcomes remain locally unknown unless the negotiated lifecycle provides authoritative evidence. "Unknown" is a local observation, not a new wire status. Recovery follows that lifecycle's retained-state rules, not an implicit `FAILED` or `ABANDONED` transition.
