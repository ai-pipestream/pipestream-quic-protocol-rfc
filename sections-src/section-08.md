# Protocol Operations

This section defines the protocol-level operations that PipeStream endpoints perform during a session. These operations describe the phases of a PipeStream session lifecycle, from connection establishment through entity processing to terminal consumption.

## Overview

A PipeStream session proceeds through four sequential actions:

~~~~
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
~~~~

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

The Capabilities exchange includes serialization format negotiation (Section 3.5). The agreed-upon format applies to all subsequent variable-length serialized messages on Stream 0 and to all entity headers on Entity Streams.

## PARSE Action

The PARSE action performs dehydration with optional completion policy:

~~~~ cddl
completion-policy = {
  ? mode: completion-mode,
  ? max-retries: uint,           ; Default: 3
  ? retry-delay-ms: uint,        ; Default: 1000
  ? timeout-ms: uint,            ; Default: 300000 (5 min)
  ? min-success-ratio: float32,  ; For QUORUM mode
  ? on-timeout: failure-action,
  ? on-failure: failure-action,
}

completion-mode = &(
  unspecified: 0,                ; Default; treat as STRICT
  strict: 1,                    ; All children MUST complete
  lenient: 2,                   ; Continue with partial results
  best-effort: 3,               ; Complete with whatever succeeds
  quorum: 4,                    ; Need min-success-ratio
)

failure-action = &(
  unspecified: 0,                ; Default; treat as FAIL
  fail: 1,                      ; Propagate failure up
  skip: 2,                      ; Skip, continue with siblings
  retry: 3,                     ; Retry up to max-retries
  defer: 4,                     ; Create claim check, continue
)
~~~~

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
