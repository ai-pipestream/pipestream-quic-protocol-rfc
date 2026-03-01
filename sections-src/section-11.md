## 11. IANA Considerations

This document requests the creation of several new registries and one ALPN identifier registration. All registries defined in this section use the "Expert Review" policy {{RFC8126}} for new assignments. The designated expert(s) should verify that proposed values do not conflict with existing assignments, that the semantics are clearly documented, and that the proposed protocol layer is appropriate for the value.

### 11.1. ALPN Identifier Registration

| Protocol | Identification Sequence | Reference |
|----------|------------------------|-----------|
| PipeStream Version 1 | "pipestream/1" | [this document] |

### 11.2. PipeStream Frame Type Registry

IANA is requested to create the "PipeStream Frame Types" registry with the following initial entries. Values in the range 0x00-0x7F are assigned by Expert Review. Values in the range 0x80-0xFF are reserved for private use.

| Value | Frame Type Name | Layer | Reference |
|-------|-----------------|-------|-----------|
| 0x50 | STATUS | 0 | Section 6.1 |
| 0x51 | CHECKPOINT | 0 | Section 9.3 |
| 0x52 | STATUS_ACK | 0 | Section 6.1 |
| 0x53 | CHECKPOINT_ACK | 0 | Section 9.3 |
| 0x54 | SCOPE_DIGEST | 1 | Section 6.3 |
| 0x55 | BARRIER | 1 | Section 6.7 |
| 0x56 | SCOPE_OPEN | 1 | Section 6.2 |
| 0x57 | SCOPE_CLOSE | 1 | Section 6.2 |
| 0x60 | ENTITY | 0 | Section 6.8 |
| 0x61 | ENTITY_START | 0 | Section 6.8 |
| 0x62 | ENTITY_CONTINUATION | 0 | Section 6.8 |
| 0x63 | ENTITY_END | 0 | Section 6.8 |
| 0x70 | CLAIM_CHECK_QUERY | 2 | Section 6.6 |
| 0x71 | CLAIM_CHECK_RESPONSE | 2 | Section 6.6 |
| 0x72 | COMPLETION_POLICY | 2 | Section 8.3 |

### 11.3. PipeStream Status Code Registry

IANA is requested to create the "PipeStream Status Codes" registry with the following initial entries. Status codes are 4-bit values (0x0-0xF). Values 0x0-0xC are defined by this document. Values 0xD-0xF are reserved for future Standards Action.

| Value | Name | Layer | Description |
|-------|------|-------|-------------|
| 0x0 | UNSPECIFIED | - | Protobuf default / heartbeat |
| 0x1 | PENDING | 0 | Entity announced |
| 0x2 | PROCESSING | 0 | In progress |
| 0x3 | COMPLETE | 0 | Success |
| 0x4 | FAILED | 0 | Failed |
| 0x5 | CHECKPOINT | 0 | Barrier |
| 0x6 | DEHYDRATING | 0 | Dehydrating into children |
| 0x7 | REHYDRATING | 0 | Rehydrating children |
| 0x8 | YIELDED | 2 | Paused |
| 0x9 | DEFERRED | 2 | Claim check issued |
| 0xA | RETRYING | 2 | Retry in progress |
| 0xB | SKIPPED | 2 | Intentionally skipped (lenient mode) |
| 0xC | ABANDONED | 2 | Timed out |
| 0xD-0xF | Reserved | - | Reserved for future use |

### 11.4. PipeStream Error Code Registry

IANA is requested to create the "PipeStream Error Codes" registry with the following initial entries. Values in the range 0x00-0x3F are assigned by Expert Review. Values in the range 0x40-0xFF are reserved for private use.

| Value | Name | Description |
|-------|------|-------------|
| 0x00 | PIPESTREAM_NO_ERROR | Graceful shutdown |
| 0x01 | PIPESTREAM_INTERNAL_ERROR | Implementation error |
| 0x02 | PIPESTREAM_IDLE_TIMEOUT | Idle timeout |
| 0x03 | PIPESTREAM_CONTROL_RESET | Control stream must reset |
| 0x04 | PIPESTREAM_INTEGRITY_ERROR | Checksum failed |
| 0x05 | PIPESTREAM_ENTITY_INVALID | Invalid format |
| 0x06 | PIPESTREAM_ENTITY_TOO_LARGE | Size exceeded |
| 0x07 | PIPESTREAM_DEPTH_EXCEEDED | Scope depth exceeded |
| 0x08 | PIPESTREAM_WINDOW_EXCEEDED | Window full |
| 0x09 | PIPESTREAM_SCOPE_INVALID | Invalid scope |
| 0x0A | PIPESTREAM_CLAIM_EXPIRED | Claim check expired |
| 0x0B | PIPESTREAM_CLAIM_NOT_FOUND | Claim check not found |
| 0x0C | PIPESTREAM_LAYER_UNSUPPORTED | Protocol layer not supported |

### 11.5. URI Scheme Registration

```
pipestream-URI = "pipestream://" authority "/" session-id ["/" scope-path] ["/" entity-id]

scope-path = scope-id *("." scope-id)
```

Examples:
- `pipestream://processor.example.com/a1b2c3d4`
- `pipestream://processor.example.com:8443/a1b2c3d4/1.42/e5f6`
