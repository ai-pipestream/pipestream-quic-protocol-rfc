## 8. Protocol Operations

This section defines the protocol-level operations that PipeStream endpoints perform during a session. These operations describe the phases of a PipeStream session lifecycle, from connection establishment through entity processing to terminal consumption.

### 8.1. Overview

```
                +─────────────────────────────────────────────+
                │           PipeStream Action Flow            │
                +─────────────────────────────────────────────+
                                     │
                                     ▼
                ┌─────────────────────────────────────────────┐
                │                  CONNECT                    │
                │    (Session + Capability Negotiation)       │
                └─────────────────────────────────────────────┘
                                     │
                                     ▼
                ┌─────────────────────────────────────────────┐
                │                   PARSE                     │
                │        (Dehydration: 1:N possible)         │
                └─────────────────────────────────────────────┘
                                     │
                       ┌─────────────┼─────────────┐
                       ▼             ▼             ▼
                ┌───────────┐ ┌───────────┐ ┌───────────┐
                │  PROCESS  │ │  PROCESS  │ │  PROCESS  │
                │   (1:1)   │ │   (1:1)   │ │   (N:1)   │
                └───────────┘ └───────────┘ └───────────┘
                       │             │             │
                       └─────────────┼─────────────┘
                                     ▼
                ┌─────────────────────────────────────────────┐
                │                   SINK                      │
                │          (Terminal Consumption)             │
                └─────────────────────────────────────────────┘
```

### 8.2. CONNECT Action

The CONNECT action establishes the session with capability negotiation.

#### 8.2.1. ALPN Identifier

```
   ALPN Protocol ID: "pipestream/1"
```

#### 8.2.2. Capability Exchange

Immediately after QUIC handshake, peers exchange Capabilities messages on Stream 0.

### 8.3. PARSE Action

The PARSE action performs dehydration with optional completion policy:

```protobuf
message CompletionPolicy {
  CompletionMode mode = 1;
  uint32 max_retries = 2;        // Default: 3
  uint32 retry_delay_ms = 3;     // Default: 1000
  uint32 timeout_ms = 4;         // Default: 300000 (5 min)
  float min_success_ratio = 5;   // For QUORUM mode
  FailureAction on_timeout = 6;
  FailureAction on_failure = 7;
}

enum CompletionMode {
  COMPLETION_MODE_UNSPECIFIED = 0;  // Default; treat as STRICT
  COMPLETION_MODE_STRICT = 1;       // All children MUST complete
  COMPLETION_MODE_LENIENT = 2;      // Continue with partial results
  COMPLETION_MODE_BEST_EFFORT = 3;  // Complete with whatever succeeds
  COMPLETION_MODE_QUORUM = 4;       // Need min_success_ratio
}

enum FailureAction {
  FAILURE_ACTION_UNSPECIFIED = 0;  // Default; treat as FAIL
  FAILURE_ACTION_FAIL = 1;         // Propagate failure up
  FAILURE_ACTION_SKIP = 2;         // Skip, continue with siblings
  FAILURE_ACTION_RETRY = 3;        // Retry up to max_retries
  FAILURE_ACTION_DEFER = 4;        // Create claim check, continue
}
```

### 8.4. PROCESS Action

| Mode | Description |
|------|-------------|
| TRANSFORM | 1:1 entity transformation |
| REHYDRATE | N:1 merge of siblings from dehydration |
| AGGREGATE | N:1 with reduction function |
| PASSTHROUGH | Metadata-only modification |

### 8.5. SINK Action

| Type | Description |
|------|-------------|
| INDEX | Search engine integration (Elasticsearch, Solr, etc.) |
| STORAGE | Blob storage persistence (Object stores, Cloud storage) |
| NOTIFICATION | Webhook/messaging triggers |
