# Section 7: Processing Actions

## 7.1. Overview

This section defines the four fundamental processing actions in PipeStream: CONNECT, PARSE, PROCESS, and SINK. These actions form the operational vocabulary through which entities traverse the processing DAG. Each action carries specific semantic guarantees and MUST be implemented according to the requirements specified herein.

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in [RFC2119].

```
                    +─────────────────────────────────────────────+
                    │           PipeStream Action Flow            │
                    +─────────────────────────────────────────────+
                                         │
                                         ▼
                    ┌─────────────────────────────────────────────┐
                    │                  CONNECT                    │
                    │         (Session Establishment)             │
                    └─────────────────────────────────────────────┘
                                         │
                                         ▼
                    ┌─────────────────────────────────────────────┐
                    │                   PARSE                     │
                    │        (Vaporization: 1:N possible)         │
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

           Figure 1: High-Level Action Flow in PipeStream Pipeline
```

## 7.2. CONNECT Action

### 7.2.1. Purpose

The CONNECT action establishes the entry point to a PipeStream pipeline. It is responsible for session initialization, capability negotiation, authentication, and the submission of initial entities for processing.

### 7.2.2. QUIC Connection Establishment

#### 7.2.2.1. Transport Requirements

PipeStream operates over QUIC [RFC9000]. Implementations MUST use QUIC version 1 or later. The Application-Layer Protocol Negotiation (ALPN) [RFC7301] token for PipeStream version 1 is:

```
   ALPN Protocol ID: "pipestream/1"
```

Implementations MUST include this ALPN token during the TLS handshake. If ALPN negotiation fails, the connection MUST be terminated with a QUIC CONNECTION_CLOSE frame carrying error code 0x50530001 (PIPESTREAM_ALPN_MISMATCH).

#### 7.2.2.2. Connection Parameters

The following QUIC transport parameters are REQUIRED:

```
   +──────────────────────────────────┬────────────────┬─────────────────+
   │ Parameter                        │ Default Value  │ Notes           │
   +──────────────────────────────────┼────────────────┼─────────────────+
   │ initial_max_streams_bidi         │ 100            │ Per direction   │
   │ initial_max_streams_uni          │ 100            │ Per direction   │
   │ initial_max_data                 │ 16777216       │ 16 MiB          │
   │ initial_max_stream_data_bidi     │ 1048576        │ 1 MiB           │
   │ max_idle_timeout                 │ 30000          │ milliseconds    │
   │ max_udp_payload_size             │ 1472           │ bytes           │
   +──────────────────────────────────┴────────────────┴─────────────────+

                Table 1: Required QUIC Transport Parameters
```

Implementations MAY negotiate higher values. Implementations MUST NOT negotiate values below the specified defaults except for `max_idle_timeout`, which MAY be set to 0 to disable idle timeout.

### 7.2.3. Capability Negotiation

#### 7.2.3.1. Capability Frame Format

Following successful QUIC connection establishment, the client MUST send a CAPABILITY_REQUEST frame on stream 0. The server MUST respond with a CAPABILITY_RESPONSE frame.

```
   CAPABILITY_REQUEST Frame {
     Type (8) = 0x01,
     Length (32),
     Protocol Version (16),
     Client Capabilities (..),
   }

   Client Capabilities {
     Supported Layers Bitmap (8),
     Supported Actions Bitmap (8),
     Max Entity Size (64),
     Extension Count (16),
     Extensions (..) ...,
   }

   Supported Layers Bitmap:
     Bit 0: BlobBag
     Bit 1: SemanticLayer
     Bit 2: ParsedData
     Bit 3: CustomEntity
     Bits 4-7: Reserved (MUST be zero)

   Supported Actions Bitmap:
     Bit 0: CONNECT
     Bit 1: PARSE
     Bit 2: PROCESS
     Bit 3: SINK
     Bits 4-7: Reserved (MUST be zero)
```

#### 7.2.3.2. Capability Response

```
   CAPABILITY_RESPONSE Frame {
     Type (8) = 0x02,
     Length (32),
     Status (8),
     Negotiated Capabilities (..),
   }

   Status Values:
     0x00: SUCCESS
     0x01: VERSION_MISMATCH
     0x02: CAPABILITY_INCOMPATIBLE
     0x03: SERVER_OVERLOADED
     0x04: AUTHENTICATION_REQUIRED

             Figure 2: Capability Negotiation Frame Structures
```

The negotiated capabilities represent the intersection of client and server capabilities. Both parties MUST use only the negotiated capabilities for the duration of the connection.

### 7.2.4. Authentication and Authorization

#### 7.2.4.1. Authentication Methods

PipeStream supports multiple authentication methods. The server advertises supported methods in the CAPABILITY_RESPONSE frame:

```
   Authentication Methods:
     0x00: NONE (development/testing only)
     0x01: MUTUAL_TLS (client certificate)
     0x02: TOKEN_BEARER (JWT or opaque token)
     0x03: HMAC_SIGNED (request signing)
```

Implementations intended for production deployments MUST support at least MUTUAL_TLS (0x01) or TOKEN_BEARER (0x02). The NONE method MUST NOT be used in production environments.

#### 7.2.4.2. Authorization Model

Authorization in PipeStream operates at three levels:

1. **Connection Level**: Determines which pipelines a client may access
2. **Action Level**: Determines which actions (PARSE, PROCESS, SINK) are permitted
3. **Layer Level**: Determines which data layers may be read or modified

```
   AUTH_CONTEXT Frame {
     Type (8) = 0x03,
     Length (32),
     Principal ID (variable),
     Pipeline Permissions (..),
     Action Permissions (8),
     Layer Read Permissions (8),
     Layer Write Permissions (8),
     Expiration Timestamp (64),
   }
```

Servers MUST reject operations that exceed the granted permissions with error code 0x50530403 (PIPESTREAM_UNAUTHORIZED).

### 7.2.5. Initial Entity Submission

#### 7.2.5.1. Entity Submission Frame

After successful capability negotiation and authentication, the client MAY submit entities for processing:

```
   ENTITY_SUBMIT Frame {
     Type (8) = 0x10,
     Length (32),
     Entity ID (20),               ; Scope-local ID
     Parent ID (20),               ; 0 if root entity
     Scope ID (12),                ; Layer 1
     Layer Type (4),
     Priority (8),
     Flags (16),
     Metadata Length (32),
     Metadata (..),
     Payload Length (64),
     Payload (..),
   }

   Flags:
     Bit 0: CHECKPOINT_REQUIRED
     Bit 1: ORDERED_DELIVERY
     Bit 2: IDEMPOTENT
     Bit 3: COMPRESSIBLE
     Bits 4-15: Reserved
```

#### 7.2.5.2. Submission Acknowledgment

The server MUST acknowledge entity submission:

```
   ENTITY_ACK Frame {
     Type (8) = 0x11,
     Length (32),
     Entity ID (20),
     Status (4),
     Reserved (8),
     Assigned Worker ID (64),
     Estimated Processing Time (32),  ; milliseconds, 0 = unknown
     Queue Position (32),             ; 0 = immediate processing
   }

   Status Values:
     0x00: ACCEPTED
     0x01: QUEUED
     0x02: REJECTED_INVALID
     0x03: REJECTED_QUOTA
     0x04: REJECTED_LAYER_UNSUPPORTED
```

### 7.2.6. Connection State Machine

```
   ┌──────────────┐
   │              │
   │    IDLE      │
   │              │
   └──────┬───────┘
          │ QUIC Established + ALPN Matched
          ▼
   ┌──────────────┐
   │              │  CAPABILITY_REQUEST
   │  HANDSHAKE   │◄─────────────────────┐
   │              │                      │
   └──────┬───────┘                      │
          │ CAPABILITY_RESPONSE(SUCCESS) │ VERSION_MISMATCH
          ▼                              │
   ┌──────────────┐                      │
   │              │──────────────────────┘
   │ NEGOTIATING  │
   │              │
   └──────┬───────┘
          │ AUTH Success
          ▼
   ┌──────────────┐
   │              │  ENTITY_SUBMIT / ENTITY_ACK
   │    ACTIVE    │◄────────────────────────────┐
   │              │─────────────────────────────┘
   └──────┬───────┘
          │ GOAWAY or Connection Error
          ▼
   ┌──────────────┐
   │              │
   │   DRAINING   │
   │              │
   └──────┬───────┘
          │ All streams complete
          ▼
   ┌──────────────┐
   │              │
   │    CLOSED    │
   │              │
   └──────────────┘

              Figure 3: CONNECT Action State Machine
```

### 7.2.7. Connection Maintenance

#### 7.2.7.1. Keep-Alive

Implementations SHOULD send PING frames at intervals not exceeding half the negotiated `max_idle_timeout`. The responder MUST reply with a corresponding PONG frame.

#### 7.2.7.2. Graceful Shutdown

To initiate graceful shutdown, an endpoint sends a GOAWAY frame:

```
   GOAWAY Frame {
     Type (8) = 0x07,
     Length (32),
     Last Accepted Stream ID (64),
     Reason Code (32),
     Debug Data Length (16),
     Debug Data (..),
   }
```

Upon receiving GOAWAY, the peer MUST NOT initiate new streams but MUST complete processing of existing streams. The connection enters the DRAINING state and MUST remain open until all in-flight entities reach terminal state.

## 7.3. PARSE Action

### 7.3.1. Purpose

The PARSE action performs document structure analysis and serves as the primary vaporization point in PipeStream, enabling 1:N decomposition of complex documents into constituent entities. This action transforms opaque data (BlobBag layer) into structured representations (SemanticLayer or ParsedData layer).

### 7.3.2. Vaporization Semantics

#### 7.3.2.1. Vaporization Definition

Vaporization is the controlled decomposition of a single input entity into multiple output entities while maintaining referential integrity and processing lineage. The PARSE action MAY produce:

- **1:1 Mapping**: Single input produces single output (simple documents)
- **1:N Mapping**: Single input produces multiple outputs (compound documents)
- **1:0 Mapping**: Single input produces no outputs (filtered/empty documents)

#### 7.3.2.2. Vaporization Constraints

The following constraints MUST be observed during vaporization:

1. All child entities MUST reference the parent entity ID
2. Child entity IDs MUST be deterministically derivable from parent ID and content hash
3. The sum of child entity sizes SHOULD NOT exceed 10x the parent size
4. Vaporization depth (recursive parsing) MUST NOT exceed the configured maximum (default: 16)

```
   Entity ID Derivation:
     child_id = HASH(parent_id || child_index || content_hash)
     where HASH is SHA-256 truncated to 128 bits
```

### 7.3.3. Layer Transitions

#### 7.3.3.1. Permitted Transitions

```
   ┌───────────────────────────────────────────────────────────────┐
   │                    Layer Transition Rules                     │
   ├───────────────────┬───────────────────────────────────────────┤
   │   Source Layer    │           Permitted Targets               │
   ├───────────────────┼───────────────────────────────────────────┤
   │   BlobBag         │   SemanticLayer, ParsedData, CustomEntity │
   │   SemanticLayer   │   ParsedData, CustomEntity                │
   │   ParsedData      │   CustomEntity                            │
   │   CustomEntity    │   CustomEntity (type change only)         │
   └───────────────────┴───────────────────────────────────────────┘

               Table 2: PARSE Action Layer Transitions
```

Implementations MUST NOT permit transitions that move "down" the layer hierarchy (e.g., ParsedData to BlobBag). Such reverse transitions require explicit PROCESS action with conversion semantics.

#### 7.3.3.2. Transition Frame

```
   PARSE_REQUEST Frame {
     Type (8) = 0x20,
     Length (32),
     Entity ID (20),
     Source Layer (8),
     Target Layer (8),
     Parser ID (32),
     Parser Config Length (16),
     Parser Config (..),
     Vaporization Hints (..),
   }

   Vaporization Hints {
     Expected Child Count (32),      ; 0 = unknown
     Max Depth (8),
     Preserve Ordering (1),
     Generate Ledger (1),
     Reserved (6),
   }
```

### 7.3.4. Parts Ledger

#### 7.3.4.1. Ledger Purpose

The Parts Ledger is a metadata structure that tracks the relationship between parent entities and their vaporized children. It serves as the authoritative record for:

- Reconstitution (rejoining child entities)
- Progress tracking
- Failure recovery
- Audit and lineage

#### 7.3.4.2. Ledger Structure

```
   Parts Ledger {
     Ledger ID (128),
     Parent Entity ID (20),
     Creation Timestamp (64),
     Total Parts (32),
     Completed Parts (32),
     Failed Parts (32),
     Ledger State (8),
     Parts Index (..),
   }

   Parts Index Entry {
     Part Number (32),
     Child Entity ID (20),
     Offset in Parent (64),
     Length in Parent (64),
     State (8),
     Checksum (64),
   }

   Ledger State:
     0x00: INITIALIZING
     0x01: ACTIVE
     0x02: COMPLETE
     0x03: PARTIAL_FAILURE
     0x04: FAILED
     0x05: RECONSTITUTING

   Part State:
     0x00: PENDING
     0x01: PARSING
     0x02: PARSED
     0x03: PROCESSING
     0x04: COMPLETE
     0x05: FAILED
     0x06: RETRYING

              Figure 4: Parts Ledger Data Structures
```

#### 7.3.4.3. Ledger Operations

```
   LEDGER_CREATE Frame {
     Type (8) = 0x21,
     Length (32),
     Parent Entity ID (20),
     Expected Parts (32),
     Ledger Options (16),
   }

   LEDGER_UPDATE Frame {
     Type (8) = 0x22,
     Length (32),
     Ledger ID (128),
     Update Type (8),
     Part Number (32),
     New State (8),
     Additional Data (..),
   }

   LEDGER_QUERY Frame {
     Type (8) = 0x23,
     Length (32),
     Ledger ID (128),
     Query Type (8),
   }

   Query Types:
     0x00: FULL_STATE
     0x01: SUMMARY_ONLY
     0x02: INCOMPLETE_PARTS
     0x03: FAILED_PARTS
```

### 7.3.5. Parent-Child Relationship

#### 7.3.5.1. Relationship Model

PipeStream maintains a directed acyclic graph (DAG) of entity relationships:

```
                      ┌─────────────────────┐
                      │   Root Document     │
                      │   (BlobBag Layer)   │
                      │   Entity: 0xA1B2... │
                      └──────────┬──────────┘
                                 │ PARSE (vaporize)
                    ┌────────────┼────────────┐
                    ▼            ▼            ▼
             ┌──────────┐ ┌──────────┐ ┌──────────┐
             │ Chapter 1│ │ Chapter 2│ │ Chapter 3│
             │ Semantic │ │ Semantic │ │ Semantic │
             │ 0xC3D4...│ │ 0xE5F6...│ │ 0x1728...│
             └────┬─────┘ └──────────┘ └────┬─────┘
                  │ PARSE (vaporize)        │
            ┌─────┴─────┐             ┌─────┴─────┐
            ▼           ▼             ▼           ▼
       ┌────────┐ ┌────────┐    ┌────────┐ ┌────────┐
       │  Para  │ │  Para  │    │  Para  │ │ Image  │
       │  1.1   │ │  1.2   │    │  3.1   │ │  3.1   │
       │Parsed  │ │Parsed  │    │Parsed  │ │BlobBag │
       └────────┘ └────────┘    └────────┘ └────────┘

          Figure 5: Entity Relationship DAG After Vaporization
```

#### 7.3.5.2. Relationship Metadata

Each entity MUST carry relationship metadata:

```
   Relationship Metadata {
     Parent ID (20),               ; 0 for root entities
     Root ID (20),                 ; Original document ID
     Depth (8),                    ; Distance from root
     Sibling Index (32),           ; Position among siblings
     Total Siblings (32),          ; At time of vaporization
     Ledger ID (128),              ; Tracking ledger reference
   }
```

### 7.3.6. Recursive Parsing

#### 7.3.6.1. Recursive Parsing Model

PipeStream supports recursive parsing where child entities may themselves be parsed into further children. This enables processing of nested document structures (e.g., ZIP containing PDFs containing images).

#### 7.3.6.2. Recursion Control

```
   RECURSIVE_PARSE_CONFIG {
     Max Depth (8),                ; Hard limit on recursion
     Current Depth (8),            ; Inherited from parent
     Recursion Policy (8),
     Type Filters (..),            ; MIME types to recurse into
   }

   Recursion Policy:
     0x00: NEVER              ; Never recurse
     0x01: SAME_TYPE          ; Recurse only for same MIME type
     0x02: KNOWN_CONTAINERS   ; Recurse for known container formats
     0x03: ALWAYS             ; Recurse all parseable content
```

Implementations MUST track current depth and MUST refuse to parse when `Current Depth >= Max Depth`. The default Max Depth is 16.

### 7.3.7. PARSE State Machine

```
         ┌─────────────────┐
         │                 │
         │    RECEIVED     │
         │                 │
         └────────┬────────┘
                  │ Validate Entity
                  ▼
         ┌─────────────────┐     Invalid
         │                 │────────────────┐
         │   VALIDATING    │                │
         │                 │                ▼
         └────────┬────────┘         ┌─────────────┐
                  │ Valid            │   REJECTED  │
                  ▼                  └─────────────┘
         ┌─────────────────┐
         │                 │
         │    ANALYZING    │ ◄───────────┐
         │                 │             │
         └────────┬────────┘             │ Retry
                  │ Structure Determined │
                  ▼                      │
         ┌─────────────────┐             │
         │                 │ Parse Error │
         │   VAPORIZING    │─────────────┘
         │                 │
         └────────┬────────┘
                  │ Children Created
                  ▼
         ┌─────────────────┐
         │                 │
         │ LEDGER_CREATING │
         │                 │
         └────────┬────────┘
                  │ Ledger Populated
                  ▼
         ┌─────────────────┐
         │                 │
         │   DISPATCHING   │
         │                 │
         └────────┬────────┘
                  │ Children Queued
                  ▼
         ┌─────────────────┐
         │                 │
         │    COMPLETE     │
         │                 │
         └─────────────────┘

             Figure 6: PARSE Action State Machine
```

### 7.3.8. PARSE Response

```
   PARSE_RESPONSE Frame {
     Type (8) = 0x24,
     Length (32),
     Request ID (64),
     Status (8),
     Original Entity ID (20),
     Child Count (32),
     Ledger ID (128),
     Child Summaries (..),
   }

   Child Summary {
     Child Entity ID (20),
     Target Layer (8),
     Estimated Size (64),
     Content Type Length (8),
     Content Type (..),
   }

   Status Values:
     0x00: SUCCESS
     0x01: PARTIAL_SUCCESS (some parts failed)
     0x02: NO_CHILDREN (1:0 mapping)
     0x03: PARSE_ERROR
     0x04: DEPTH_EXCEEDED
     0x05: TIMEOUT
```

## 7.4. PROCESS Action

### 7.4.1. Purpose

The PROCESS action performs content transformation on entities, supporting both 1:1 transformations (enrichment, conversion) and N:1 operations (rejoin, aggregation). This action operates primarily within a single layer or performs layer enrichment.

### 7.4.2. Processing Modes

#### 7.4.2.1. Transformation Mode (1:1)

In transformation mode, a single input entity produces a single output entity. The entity ID MAY remain unchanged (in-place modification) or MAY be replaced (copy-on-write).

```
   Processing Mode: TRANSFORM (1:1)

   ┌─────────────────┐                ┌─────────────────┐
   │  Input Entity   │                │ Output Entity   │
   │                 │   PROCESS      │                 │
   │  ParsedData     │ ─────────────► │  ParsedData     │
   │  (text only)    │   (enrich)     │  (text + embed) │
   │                 │                │                 │
   └─────────────────┘                └─────────────────┘
```

#### 7.4.2.2. Rejoin Mode (N:1)

In rejoin mode, multiple input entities (typically siblings from a vaporization) are merged into a single output entity. This is the inverse of vaporization.

```
   Processing Mode: REJOIN (N:1)

   ┌───────────┐
   │  Child 1  │───┐
   │  Parsed   │   │
   └───────────┘   │
                   │   PROCESS
   ┌───────────┐   │   (rejoin)    ┌─────────────────┐
   │  Child 2  │───┼─────────────► │ Merged Entity   │
   │  Parsed   │   │               │ SemanticLayer   │
   └───────────┘   │               └─────────────────┘
                   │
   ┌───────────┐   │
   │  Child 3  │───┘
   │  Parsed   │
   └───────────┘

               Figure 7: REJOIN Processing Mode
```

### 7.4.3. Layer Enrichment

#### 7.4.3.1. Enrichment Operations

Layer enrichment adds computed or derived data to an entity without changing its fundamental layer type:

```
   ┌───────────────────────────────────────────────────────────────┐
   │                  Enrichment Operations                        │
   ├────────────────────┬──────────────────────────────────────────┤
   │   Layer            │   Supported Enrichments                  │
   ├────────────────────┼──────────────────────────────────────────┤
   │   BlobBag          │   Checksums, MIME detection, previews    │
   │   SemanticLayer    │   Embeddings, classifications, entities  │
   │   ParsedData       │   Schema validation, normalization       │
   │   CustomEntity     │   Application-specific enrichment        │
   └────────────────────┴──────────────────────────────────────────┘

                  Table 3: Layer Enrichment Operations
```

#### 7.4.3.2. Enrichment Frame

```
   PROCESS_REQUEST Frame {
     Type (8) = 0x30,
     Length (32),
     Request ID (64),
     Mode (8),
     Input Count (32),
     Input Entity IDs (20) ...,
     Output Layer (8),
     Processor ID (32),
     Processor Config Length (16),
     Processor Config (..),
     Enrichment Requests (..),
   }

   Mode Values:
     0x00: TRANSFORM
     0x01: REJOIN
     0x02: AGGREGATE (N:1 with reduction)
     0x03: PASSTHROUGH (metadata only)

   Enrichment Request {
     Enrichment Type (16),
     Priority (8),
     Config Length (16),
     Config (..),
   }

   Enrichment Types:
     0x0001: EMBEDDING_VECTOR
     0x0002: NAMED_ENTITY_RECOGNITION
     0x0003: CLASSIFICATION
     0x0004: SUMMARIZATION
     0x0005: LANGUAGE_DETECTION
     0x0006: SENTIMENT_ANALYSIS
     0x0007-0x7FFF: Reserved
     0x8000-0xFFFF: Application-defined
```

### 7.4.4. Rejoin Operations

#### 7.4.4.1. Rejoin Semantics

Rejoin operations MUST satisfy the following requirements:

1. All input entities MUST share the same Parent ID (siblings only)
2. All input entities MUST be in terminal state (COMPLETE or FAILED)
3. The Ledger MUST indicate all parts are accounted for
4. The output entity ID SHOULD be derivable from input IDs

#### 7.4.4.2. Rejoin Strategies

```
   Rejoin Strategy {
     Strategy Type (8),
     Ordering (8),
     Conflict Resolution (8),
     Missing Part Policy (8),
   }

   Strategy Type:
     0x00: CONCATENATE      ; Sequential combination
     0x01: MERGE            ; Deep merge of structures
     0x02: REDUCE           ; Apply reduction function
     0x03: SELECT           ; Choose best candidate

   Ordering:
     0x00: ORIGINAL         ; By sibling index
     0x01: COMPLETION_TIME  ; By processing completion
     0x02: CUSTOM           ; Application-defined

   Conflict Resolution:
     0x00: FIRST_WINS       ; Keep first value
     0x01: LAST_WINS        ; Keep last value
     0x02: MERGE_ARRAYS     ; Combine array values
     0x03: ERROR            ; Fail on conflict

   Missing Part Policy:
     0x00: FAIL             ; Require all parts
     0x01: SKIP             ; Proceed without missing
     0x02: PLACEHOLDER      ; Insert placeholder values
```

#### 7.4.4.3. Rejoin Coordination

```
   REJOIN_READY Frame {
     Type (8) = 0x31,
     Length (32),
     Ledger ID (128),
     Ready Parts Bitmap (..),
     Total Ready (32),
     Total Expected (32),
   }

   REJOIN_INITIATE Frame {
     Type (8) = 0x32,
     Length (32),
     Ledger ID (128),
     Strategy (..),
     Timeout (32),
   }
```

### 7.4.5. Progress Reporting

#### 7.4.5.1. Progress Model

PROCESS actions MUST report progress for operations exceeding 1 second duration:

```
   PROGRESS_REPORT Frame {
     Type (8) = 0x33,
     Length (32),
     Request ID (64),
     Entity ID (20),
     Phase (8),
     Progress Numerator (64),
     Progress Denominator (64),
     Estimated Remaining (32),    ; milliseconds
     Status Message Length (16),
     Status Message (..),
   }

   Phase:
     0x00: INITIALIZING
     0x01: LOADING
     0x02: PROCESSING
     0x03: ENRICHING
     0x04: FINALIZING
```

Progress reports SHOULD be sent at least every 5 seconds during active processing. Clients MAY request higher frequency reporting.

### 7.4.6. Error Handling

#### 7.4.6.1. Error Categories

```
   ┌───────────────────────────────────────────────────────────────┐
   │                    Error Categories                           │
   ├────────────────┬──────────────────────────────────────────────┤
   │   Category     │   Description                                │
   ├────────────────┼──────────────────────────────────────────────┤
   │   TRANSIENT    │   Temporary failure, retry recommended       │
   │   PERMANENT    │   Unrecoverable, do not retry                │
   │   PARTIAL      │   Some operations succeeded                  │
   │   RESOURCE     │   Resource exhaustion (memory, CPU)          │
   │   TIMEOUT      │   Operation exceeded time limit              │
   │   DEPENDENCY   │   Required service unavailable               │
   └────────────────┴──────────────────────────────────────────────┘

                   Table 4: PROCESS Error Categories
```

#### 7.4.6.2. Error Frame

```
   PROCESS_ERROR Frame {
     Type (8) = 0x3F,
     Length (32),
     Request ID (64),
     Entity ID (20),
     Error Category (8),
     Error Code (32),
     Retryable (1),
     Reserved (7),
     Retry After (32),         ; seconds, 0 if not retryable
     Error Details Length (16),
     Error Details (..),
   }
```

### 7.4.7. Retry Semantics

#### 7.4.7.1. Retry Policy

For TRANSIENT errors, implementations SHOULD retry with exponential backoff:

```
   Retry Delay = min(Initial Delay * (2 ^ attempt), Max Delay)

   Default Values:
     Initial Delay: 100ms
     Max Delay: 30s
     Max Attempts: 5
```

#### 7.4.7.2. Retry Frame

```
   PROCESS_RETRY Frame {
     Type (8) = 0x34,
     Length (32),
     Original Request ID (64),
     Retry Attempt (8),
     Modified Config (..),     ; Optional adjustments
   }
```

### 7.4.8. Idempotency Requirements

#### 7.4.8.1. Idempotency Keys

All PROCESS requests MUST include an idempotency key:

```
   Idempotency Key = HASH(Entity ID || Processor ID || Config Hash)
```

Servers MUST cache the result of idempotent operations for at least the configured TTL (default: 1 hour). Duplicate requests with matching idempotency keys MUST return the cached result.

#### 7.4.8.2. Idempotency Semantics

```
   ┌────────────────────────────────────────────────────────────────┐
   │                  Idempotency Guarantees                        │
   ├─────────────────────┬──────────────────────────────────────────┤
   │   Mode              │   Guarantee                              │
   ├─────────────────────┼──────────────────────────────────────────┤
   │   TRANSFORM         │   Same input always produces same output │
   │   REJOIN            │   Repeatable given same complete inputs  │
   │   AGGREGATE         │   Deterministic reduction function       │
   │   PASSTHROUGH       │   Always idempotent                      │
   └─────────────────────┴──────────────────────────────────────────┘

               Table 5: Idempotency Guarantees by Mode
```

### 7.4.9. PROCESS State Machine

```
         ┌─────────────────┐
         │                 │
         │    RECEIVED     │
         │                 │
         └────────┬────────┘
                  │ Check Idempotency Key
                  ▼
         ┌─────────────────┐
         │                 │──── Cache Hit ────► Return Cached
         │  DEDUPLICATING  │
         │                 │
         └────────┬────────┘
                  │ Cache Miss
                  ▼
         ┌─────────────────┐
         │                 │     Mode = REJOIN
         │  MODE_ROUTING   │─────────────────────┐
         │                 │                     │
         └────────┬────────┘                     │
                  │ Mode = TRANSFORM             ▼
                  ▼                     ┌─────────────────┐
         ┌─────────────────┐           │                 │
         │                 │           │ AWAITING_PARTS  │◄──┐
         │    LOADING      │           │                 │   │
         │                 │           └────────┬────────┘   │
         └────────┬────────┘                    │ All Ready  │ Part
                  │                             ▼            │ Arrived
                  ▼                    ┌─────────────────┐   │
         ┌─────────────────┐           │                 │───┘
         │                 │◄──────────│   COLLECTING    │
         │   PROCESSING    │           │                 │
         │                 │           └─────────────────┘
         └────────┬────────┘
                  │
        ┌─────────┼─────────┐
        ▼         │         ▼
   ┌────────┐     │    ┌────────┐
   │ENRICHING│    │    │ ERROR  │───► Retry Logic
   └────┬───┘     │    └────────┘
        │         │
        ▼         │
   ┌─────────────────┐
   │                 │
   │   COMPLETING    │
   │                 │
   └────────┬────────┘
            │ Update Cache
            ▼
   ┌─────────────────┐
   │                 │
   │    COMPLETE     │
   │                 │
   └─────────────────┘

            Figure 8: PROCESS Action State Machine
```

### 7.4.10. PROCESS Response

```
   PROCESS_RESPONSE Frame {
     Type (8) = 0x35,
     Length (32),
     Request ID (64),
     Status (8),
     Output Entity ID (20),
     Output Layer (8),
     Enrichments Applied (16),
     Processing Duration (32),    ; milliseconds
     Output Metadata (..),
   }

   Status Values:
     0x00: SUCCESS
     0x01: PARTIAL_SUCCESS
     0x02: CACHED_RESULT
     0x03: TRANSFORM_ERROR
     0x04: REJOIN_INCOMPLETE
     0x05: ENRICHMENT_FAILED
     0x06: TIMEOUT
```

## 7.5. SINK Action

### 7.5.1. Purpose

The SINK action represents the terminal consumption of entities in the PipeStream pipeline. It performs final operations such as indexing, storage, and notification, and is responsible for completion acknowledgment and resource cleanup.

### 7.5.2. Sink Types

#### 7.5.2.1. Index Sink

Index sinks integrate with search engines and vector databases:

```
   INDEX_SINK_CONFIG {
     Sink Type (8) = 0x01,
     Index Target (8),
     Index Name Length (16),
     Index Name (..),
     Field Mappings (..),
     Upsert Mode (8),
   }

   Index Target:
     0x01: ELASTICSEARCH
     0x02: OPENSEARCH
     0x03: SOLR
     0x04: VECTOR_DB (generic)
     0x05: PINECONE
     0x06: WEAVIATE
     0x07: MILVUS
     0x08-0x7F: Reserved
     0x80-0xFF: Custom

   Upsert Mode:
     0x00: INSERT_ONLY      ; Fail on duplicate
     0x01: UPDATE_ONLY      ; Fail if not exists
     0x02: UPSERT           ; Insert or update
     0x03: REPLACE          ; Delete and insert
```

#### 7.5.2.2. Storage Sink

Storage sinks persist entities to blob storage or filesystems:

```
   STORAGE_SINK_CONFIG {
     Sink Type (8) = 0x02,
     Storage Backend (8),
     Bucket/Container Length (16),
     Bucket/Container (..),
     Key Template Length (16),
     Key Template (..),
     Storage Class (8),
     Retention Policy (..),
   }

   Storage Backend:
     0x01: S3_COMPATIBLE
     0x02: AZURE_BLOB
     0x03: GCS
     0x04: LOCAL_FILESYSTEM
     0x05: HDFS
     0x06-0x7F: Reserved
     0x80-0xFF: Custom

   Storage Class:
     0x00: STANDARD
     0x01: INFREQUENT_ACCESS
     0x02: ARCHIVE
     0x03: DEEP_ARCHIVE
```

#### 7.5.2.3. Notification Sink

Notification sinks trigger webhooks or messaging systems:

```
   NOTIFICATION_SINK_CONFIG {
     Sink Type (8) = 0x03,
     Notification Target (8),
     Endpoint Length (16),
     Endpoint (..),
     Auth Config (..),
     Payload Template Length (32),
     Payload Template (..),
     Retry Config (..),
   }

   Notification Target:
     0x01: WEBHOOK_HTTP
     0x02: WEBHOOK_HTTPS
     0x03: KAFKA
     0x04: RABBITMQ
     0x05: AWS_SNS
     0x06: AWS_SQS
     0x07: AZURE_EVENT_HUB
     0x08: GCP_PUBSUB
     0x09-0x7F: Reserved
     0x80-0xFF: Custom
```

### 7.5.3. Sink Request

```
   SINK_REQUEST Frame {
     Type (8) = 0x40,
     Length (32),
     Request ID (64),
     Entity ID (20),
     Sink Count (8),
     Sink Configs (..),
     Completion Requirements (..),
   }

   Completion Requirements {
     All Sinks Required (1),
     Minimum Sinks (7),
     Timeout (32),
     On Failure (8),
   }

   On Failure:
     0x00: FAIL_FAST         ; Abort on first failure
     0x01: CONTINUE          ; Continue with remaining sinks
     0x02: ROLLBACK          ; Attempt to undo completed sinks
```

### 7.5.4. Index Operations

#### 7.5.4.1. Document Indexing

```
   INDEX_OPERATION Frame {
     Type (8) = 0x41,
     Length (32),
     Request ID (64),
     Entity ID (20),
     Index Config (..),
     Document ID Length (32),
     Document ID (..),           ; May differ from Entity ID
     Document Body Length (64),
     Document Body (..),
   }
```

#### 7.5.4.2. Index Response

```
   INDEX_RESPONSE Frame {
     Type (8) = 0x42,
     Length (32),
     Request ID (64),
     Status (8),
     Index Name Length (16),
     Index Name (..),
     Document ID Length (32),
     Document ID (..),
     Version (64),               ; Index version/sequence number
   }

   Status Values:
     0x00: INDEXED
     0x01: UPDATED
     0x02: ALREADY_EXISTS
     0x03: INDEX_ERROR
     0x04: MAPPING_ERROR
     0x05: QUOTA_EXCEEDED
```

### 7.5.5. Storage Operations

#### 7.5.5.1. Blob Persistence

```
   STORAGE_OPERATION Frame {
     Type (8) = 0x43,
     Length (32),
     Request ID (64),
     Entity ID (20),
     Storage Config (..),
     Object Key Length (32),
     Object Key (..),
     Content Type Length (8),
     Content Type (..),
     Metadata Entry Count (16),
     Metadata Entries (..),
     Payload Length (64),
     Payload (..),
   }

   Metadata Entry {
     Key Length (16),
     Key (..),
     Value Length (32),
     Value (..),
   }
```

#### 7.5.5.2. Storage Response

```
   STORAGE_RESPONSE Frame {
     Type (8) = 0x44,
     Length (32),
     Request ID (64),
     Status (8),
     Storage URI Length (32),
     Storage URI (..),
     ETag Length (16),
     ETag (..),
     Version ID Length (32),
     Version ID (..),
   }

   Status Values:
     0x00: STORED
     0x01: UPDATED
     0x02: STORAGE_ERROR
     0x03: QUOTA_EXCEEDED
     0x04: ACCESS_DENIED
```

### 7.5.6. Notification/Webhook Triggers

#### 7.5.6.1. Notification Dispatch

```
   NOTIFICATION_OPERATION Frame {
     Type (8) = 0x45,
     Length (32),
     Request ID (64),
     Entity ID (20),
     Notification Config (..),
     Payload (..),
   }
```

#### 7.5.6.2. Webhook Delivery Semantics

Webhook delivery MUST implement at-least-once semantics:

1. Webhooks MUST be retried on failure
2. Webhook endpoints MUST be prepared for duplicate delivery
3. Each delivery attempt MUST include a unique Delivery ID
4. Delivery status MUST be tracked and reported

```
   Webhook Delivery Headers:
     X-PipeStream-Delivery-ID: <unique-id>
     X-PipeStream-Entity-ID: <entity-id>
     X-PipeStream-Attempt: <attempt-number>
     X-PipeStream-Timestamp: <iso8601-timestamp>
     X-PipeStream-Signature: <hmac-sha256-signature>
```

#### 7.5.6.3. Notification Response

```
   NOTIFICATION_RESPONSE Frame {
     Type (8) = 0x46,
     Length (32),
     Request ID (64),
     Status (8),
     Delivery ID (128),
     Attempts (8),
     Final Response Code (16),
   }

   Status Values:
     0x00: DELIVERED
     0x01: QUEUED
     0x02: DELIVERY_FAILED
     0x03: ENDPOINT_UNREACHABLE
     0x04: TIMEOUT
```

### 7.5.7. Completion Acknowledgment

#### 7.5.7.1. Sink Completion

Upon successful completion of all configured sinks, the worker MUST send a completion acknowledgment:

```
   SINK_COMPLETE Frame {
     Type (8) = 0x47,
     Length (32),
     Request ID (64),
     Entity ID (20),
     Status (8),
     Sinks Completed (8),
     Sinks Failed (8),
     Sink Results (..),
     Total Duration (32),
   }

   Sink Result {
     Sink Index (8),
     Sink Type (8),
     Status (8),
     Result Data Length (32),
     Result Data (..),
   }
```

#### 7.5.7.2. Pipeline Completion

When an entity and all its descendants have completed:

```
   PIPELINE_COMPLETE Frame {
     Type (8) = 0x48,
     Length (32),
     Root Entity ID (20),
     Total Entities Processed (64),
     Total Entities Failed (64),
     Start Timestamp (64),
     End Timestamp (64),
     Final Status (8),
   }

   Final Status:
     0x00: SUCCESS           ; All entities completed
     0x01: PARTIAL           ; Some entities failed
     0x02: FAILED            ; Root or critical path failed
```

### 7.5.8. Cleanup and Resource Release

#### 7.5.8.1. Resource Lifecycle

```
                    ┌─────────────────────────────────────────┐
                    │         Resource Lifecycle              │
                    └─────────────────────────────────────────┘
                                      │
                    ┌─────────────────┼─────────────────┐
                    ▼                 ▼                 ▼
             ┌──────────┐      ┌──────────┐      ┌──────────┐
             │  Entity  │      │  Ledger  │      │  Cache   │
             │   Data   │      │   Data   │      │  Entry   │
             └────┬─────┘      └────┬─────┘      └────┬─────┘
                  │                 │                 │
                  ▼                 ▼                 ▼
             ┌──────────┐      ┌──────────┐      ┌──────────┐
             │ Retained │      │ Retained │      │   TTL    │
             │   Until  │      │   Until  │      │  Based   │
             │   Sink   │      │ Pipeline │      │  Expiry  │
             │ Complete │      │ Complete │      │          │
             └────┬─────┘      └────┬─────┘      └────┬─────┘
                  │                 │                 │
                  ▼                 ▼                 ▼
             ┌──────────┐      ┌──────────┐      ┌──────────┐
             │  CLEANUP │      │  ARCHIVE │      │  EVICT   │
             │          │      │    or    │      │          │
             │          │      │  DELETE  │      │          │
             └──────────┘      └──────────┘      └──────────┘

              Figure 9: Resource Lifecycle and Cleanup
```

#### 7.5.8.2. Cleanup Request

```
   CLEANUP_REQUEST Frame {
     Type (8) = 0x49,
     Length (32),
     Entity ID (20),
     Cleanup Scope (8),
     Cascade (1),
     Force (1),
     Reserved (6),
   }

   Cleanup Scope:
     0x00: ENTITY_ONLY       ; Just this entity
     0x01: WITH_CHILDREN     ; Entity and descendants
     0x02: LEDGER_ONLY       ; Just the ledger
     0x03: FULL_CLEANUP      ; All associated resources
```

#### 7.5.8.3. Resource Release Timing

Implementations MUST release resources according to the following schedule:

```
   ┌───────────────────────────────────────────────────────────────┐
   │              Resource Release Timing                          │
   ├──────────────────┬────────────────────────────────────────────┤
   │   Resource       │   Release Condition                        │
   ├──────────────────┼────────────────────────────────────────────┤
   │   Entity Payload │   After successful SINK_COMPLETE           │
   │   Parts Ledger   │   After PIPELINE_COMPLETE or 24h timeout   │
   │   Idempotency    │   After configured TTL (default 1h)        │
   │   Progress Data  │   After entity terminal state              │
   │   Connection     │   After GOAWAY + drain complete            │
   └──────────────────┴────────────────────────────────────────────┘

                 Table 6: Resource Release Timing
```

### 7.5.9. SINK State Machine

```
         ┌─────────────────┐
         │                 │
         │    RECEIVED     │
         │                 │
         └────────┬────────┘
                  │ Validate Sink Configs
                  ▼
         ┌─────────────────┐
         │                 │
         │   VALIDATING    │
         │                 │
         └────────┬────────┘
                  │ Configs Valid
                  ▼
         ┌─────────────────┐
         │                 │
         │   DISPATCHING   │──────────────────────┐
         │                 │                      │
         └────────┬────────┘                      │
                  │                               │
      ┌───────────┼───────────┐                   │
      ▼           ▼           ▼                   │
 ┌─────────┐ ┌─────────┐ ┌─────────┐             │
 │ INDEX   │ │ STORAGE │ │ NOTIFY  │             │
 │ PENDING │ │ PENDING │ │ PENDING │             │
 └────┬────┘ └────┬────┘ └────┬────┘             │
      │           │           │                   │
      ▼           ▼           ▼                   │
 ┌─────────┐ ┌─────────┐ ┌─────────┐             │
 │ INDEX   │ │ STORAGE │ │ NOTIFY  │             │ Sink
 │ ACTIVE  │ │ ACTIVE  │ │ ACTIVE  │             │ Failed
 └────┬────┘ └────┬────┘ └────┬────┘             │
      │           │           │                   │
      ▼           ▼           ▼                   │
 ┌─────────┐ ┌─────────┐ ┌─────────┐             │
 │ INDEX   │ │ STORAGE │ │ NOTIFY  │             │
 │COMPLETE │ │COMPLETE │ │COMPLETE │─────────────┤
 └────┬────┘ └────┬────┘ └────┬────┘             │
      │           │           │                   │
      └───────────┼───────────┘                   │
                  │ All Sinks Complete            │
                  ▼                               ▼
         ┌─────────────────┐            ┌─────────────────┐
         │                 │            │                 │
         │   COMPLETING    │            │  PARTIAL_FAIL   │
         │                 │            │                 │
         └────────┬────────┘            └────────┬────────┘
                  │                              │
                  ▼                              │
         ┌─────────────────┐                     │
         │                 │◄────────────────────┘
         │    CLEANUP      │
         │                 │
         └────────┬────────┘
                  │ Resources Released
                  ▼
         ┌─────────────────┐
         │                 │
         │    COMPLETE     │
         │                 │
         └─────────────────┘

               Figure 10: SINK Action State Machine
```

## 7.6. Action Composition

### 7.6.1. Action Pipelines

Actions MAY be composed into processing pipelines. The following compositions are valid:

```
   Valid Compositions:
     CONNECT -> PARSE -> PROCESS* -> SINK
     CONNECT -> PROCESS -> SINK
     CONNECT -> SINK (passthrough)

   Where * indicates zero or more repetitions

   Invalid Compositions:
     SINK -> *          (SINK is always terminal)
     * -> CONNECT       (CONNECT is always initial)
     PARSE -> PARSE     (without intervening PROCESS)
```

### 7.6.2. Cross-Action State

State shared across actions within a pipeline:

```
   Pipeline Context {
     Pipeline ID (128),
     Root Entity ID (20),
     Current Depth (8),
     Checkpoint Sequence (64),
     Accumulated Metadata (..),
     Error History (..),
   }
```

## 7.7. Security Considerations

### 7.7.1. Action-Level Security

Each action type carries specific security implications:

- **CONNECT**: Authentication and authorization boundary
- **PARSE**: Potential for zip bombs, recursive expansion attacks
- **PROCESS**: Resource exhaustion, side-channel attacks
- **SINK**: Data exfiltration, injection attacks

Implementations MUST apply appropriate rate limiting and resource quotas at each action boundary.

### 7.7.2. Cross-Action Integrity

Entity integrity MUST be verified at action boundaries using the Entity ID derivation formula specified in Section 7.3.2.2. Any integrity violation MUST result in action rejection with error code 0x50530100 (PIPESTREAM_INTEGRITY_ERROR).

## 7.8. IANA Considerations

### 7.8.1. Action Type Registry

This document establishes the "PipeStream Action Types" registry:

```
   ┌──────────────┬─────────────┬─────────────────────────────────┐
   │ Value        │ Name        │ Reference                       │
   ├──────────────┼─────────────┼─────────────────────────────────┤
   │ 0x01         │ CONNECT     │ Section 7.2                     │
   │ 0x02         │ PARSE       │ Section 7.3                     │
   │ 0x03         │ PROCESS     │ Section 7.4                     │
   │ 0x04         │ SINK        │ Section 7.5                     │
   │ 0x05-0x7F    │ Reserved    │ Future standardization          │
   │ 0x80-0xFF    │ Private Use │ Application-defined             │
   └──────────────┴─────────────┴─────────────────────────────────┘

               Table 7: PipeStream Action Type Registry
```

### 7.8.2. Frame Type Registry

This document establishes the "PipeStream Frame Types" registry with initial entries as defined throughout Section 7.

---

*End of Section 7*
