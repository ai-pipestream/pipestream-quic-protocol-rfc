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
| 2 | PARSE | 1:N | Decomposition: dehydrate a root entity into sub-entities |
| 3 | PROCESS | 1:1 or N:1 | Transform, rehydrate, aggregate, or pass through entities (parallel) |
| 4 | SINK | N:1 | Terminal consumption by an application-defined sink |

## CONNECT Action

The CONNECT action establishes the session with capability negotiation.

### ALPN Identifier

ALPN Protocol ID: `pipestream/1`

### Capability Exchange

Immediately after QUIC handshake, peers exchange Capabilities messages on Stream 0.

The Capabilities exchange includes serialization format negotiation
(Section 3.4.2). The agreed-upon format applies to all subsequent
variable-length serialized messages on Stream 0 and to all entity
headers on Entity Streams.

## PARSE Action

The PARSE action performs decomposition by dehydrating an input entity
into one or more sub-entities. When Layer 2 is negotiated, the sender
MAY attach a completion policy that governs how partial success,
timeouts, or retries are handled during recursive processing.

~~~~ cddl
completion-policy = {
  ? mode: completion-mode,
  ? max-retries: uint,           ; Default: 3
  ? retry-delay-ms: uint,        ; Default: 1000
  ? timeout-ms: uint,            ; Default: 300000 (5 min)
  ? min-success-ratio: float16 / float32, ; For QUORUM mode
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

For QUORUM, `min-success-ratio` MUST be present and finite, with a value
between zero and one, inclusive.
Its semantic value is the exact binary rational represented by an IEEE 754
binary32 value. The sender MUST use binary16 when it preserves that value
exactly, and binary32 otherwise. For example, 0.75 is encoded as
`f9 3a 00`, not `fa 3f 40 00 00`. Binary64, NaN, and infinities are invalid.
For N declared children, at least `ceil(N * min-success-ratio)` children
MUST be COMPLETE. Implementations MUST compute this threshold without
rounding the product down; integer arithmetic on the binary rational is
one implementation strategy.

All children MUST reach a terminal state before any completion policy
permits rehydration. STRICT requires every child to be COMPLETE. LENIENT
requires at least one COMPLETE child. BEST_EFFORT accepts any terminal
mixture. QUORUM uses the threshold above. SKIPPED and ABANDONED do not
count as successes. DEFERRED is not terminal; a parent does not become
ready merely because a claim was issued. An input that produces no
children completes directly without allocating an empty child scope.
Partial completion is not a claim that all work succeeded.

Once the child scope has closed with a verified SCOPE_DIGEST, if its terminal
outcomes do not satisfy the parent's completion policy, the receiver MUST
resolve the parent as FAILED without invoking rehydration. Without Layer 2,
the default STRICT policy applies. The receiver MUST retain the child scope,
its digest, and the parent's failed resolution together before reporting that
resolution. It MUST NOT fabricate a successful result or discard failed
children. The receiver echoes the verified SCOPE_DIGEST followed by a FAILED
STATUS identifying that scope's parent; no REHYDRATING status is required for
this path. An identical closure replay MUST report the same failed parent.
A missing, unresolved, or still-retryable child does not meet these conditions
and MUST NOT be treated as a closed scope with a failed policy.

| Mode | Description |
|------|-------------|
| TRANSFORM | 1:1 entity transformation |
| REHYDRATE | N:1 merge of siblings from dehydration |
| AGGREGATE | N:1 with reduction function |
| PASSTHROUGH | Metadata-only modification |

## SINK Action

The SINK action represents terminal consumption of processed entities.
Sink implementations are application-specific and are defined by
Application Profile specifications. Common sink patterns include
persistent storage, search engine indexing, event notification, and
downstream service delivery.
