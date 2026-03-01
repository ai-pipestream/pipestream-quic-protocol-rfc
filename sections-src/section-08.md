# Protocol Operations

This section defines the protocol-level operations that PipeStream endpoints perform during a session. These operations describe the phases of a PipeStream session lifecycle, from connection establishment through entity processing to terminal consumption.

## Overview

A PipeStream session proceeds through four sequential actions:

::: artwork
                +---------------------------------------------+
                |           PipeStream Action Flow            |
                +---------------------------------------------+
                                     |
                                     v
                +---------------------------------------------+
                |                  CONNECT                    |
                |    (Session + Capability Negotiation)       |
                +---------------------------------------------+
                                     |
                                     v
                +---------------------------------------------+
                |                   PARSE                     |
                |        (Dehydration: 1:N possible)         |
                +---------------------------------------------+
                                     |
                       +-------------+-------------+
                       v             v             v
                +-----------+ +-----------+ +-----------+
                |  PROCESS  | |  PROCESS  | |  PROCESS  |
                |   (1:1)   | |   (1:1)   | |   (N:1)   |
                +-----------+ +-----------+ +-----------+
                       |             |             |
                       +-------------+-------------+
                                     |
                                     v
                +---------------------------------------------+
                |                   SINK                      |
                |          (Terminal Consumption)             |
                +---------------------------------------------+
:::

| Phase | Action | Cardinality | Description |
|-------|--------|-------------|-------------|
| 1 | CONNECT | 1:1 | Session establishment and capability negotiation |
| 2 | PARSE | 1:N | Dehydration: decompose input into entities |
| 3 | PROCESS | 1:1 or N:1 | Transform, rehydrate, aggregate, or pass through entities (parallel) |
| 4 | SINK | N:1 | Terminal consumption: index, store, or notify |

## CONNECT Action

The CONNECT action establishes the session with capability negotiation.

### ALPN Identifier

ALPN Protocol ID: `pipestream/1`

### Capability Exchange

Immediately after QUIC handshake, peers exchange Capabilities messages on Stream 0.

## PARSE Action

The PARSE action performs dehydration with optional completion policy:

::: sourcecode protobuf
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
:::

## PROCESS Action

| Mode | Description |
|------|-------------|
| TRANSFORM | 1:1 entity transformation |
| REHYDRATE | N:1 merge of siblings from dehydration |
| AGGREGATE | N:1 with reduction function |
| PASSTHROUGH | Metadata-only modification |

## SINK Action

| Type | Description |
|------|-------------|
| INDEX | Search engine integration (Elasticsearch, Solr, etc.) |
| STORAGE | Blob storage persistence (Object stores, Cloud storage) |
| NOTIFICATION | Webhook/messaging triggers |
