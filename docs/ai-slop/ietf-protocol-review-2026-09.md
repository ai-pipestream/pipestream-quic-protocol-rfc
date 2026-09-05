# PipeStream proposal and implementation review

Historical pre-fix review, retained as the basis for the draft-04 work.
See [current readiness and regression evidence](../standards/draft04-readiness.md)
for subsequent fixes. Diagnostic paths below refer to the original local review;
the committed regression suite does not depend on those temporary files.

Reviewed 2026-09-04 against commit `00531de71132aa5ecad0ab11c4af7e6a7bbb98a1`, branch `feat/rust-feature-complete-exemplar`, in `/work/main/pipestream-ai/dev-tools/pipestream-quic-protocol-rfc`. The repository was clean. This review did not modify, commit, push, or submit the proposal or implementations. Diagnostic programs are alongside this report.

The reviewed proposal is the modular `sections-src/` source for revision -04, including all appendices. The [Datatracker page](https://datatracker.ietf.org/doc/draft-krickert-pipestream/) currently shows -03, with IESG state “I-D Exists” and no responsible AD. The local -04 proposal and published -03 are different review targets.

## Assessment and implementation choice

Rust with Quinn is the best primary implementation choice for this project. The existing separation of the protocol core, transport, and executable supports an embedded library and a standalone server. Keep Java/Netty as the second independent implementation, and complete C++/MsQuic after the protocol's core behavior is consistent. This is an engineering recommendation based on the code and project requirements, not a measured language performance ranking.

The proposal has a useful subject for standardization: interoperable recursive work tracking, completion barriers, and recovery. The current code is a prototype implementing selected scenarios. It does not fully implement the draft's mandatory behavior, and the current draft leaves important interoperability decisions unresolved. The earlier description of the Rust implementation as “feature-complete” was too strong.

The specification must be authoritative. Vectors and implementations need correction when they contradict it. Three separately written codecs can share the same interpretation error, as the optional-field experiment below demonstrates.

## Reproduced implementation findings

Severity P1 means the issue should be resolved before claiming conformance or exposing the durable service as a general implementation. These are local review priorities, not IESG ballot positions.

| ID | Priority | Requirement and observed behavior | Source |
|---|---|---|---|
| R1 | P1 | Capabilities permits omission of optional fields. A deterministic map containing only the three required booleans passes the repository CDDL validator but is rejected by freshly compiled Rust, Java, and C++ codecs as non-deterministic. Re-encoding a decoded structure inserts defaulted fields, changing the map being validated. | Rust `src/lib.rs:373,497`; Java `Wire.java:195,213`; C++ `wire.cpp:337` |
| R2 | P1 | CHECKPOINT flags is optional and defaults to zero. Rust rejects a deterministic request omitting it because its encoder adds `flags: 0` before comparing bytes. | Rust `src/lib.rs:520,611`; draft Section 9.3 |
| R3 | P1 | After successful negotiation, the recursive server closes on unknown frame type 0xE0 with error 0x0D. Section 11.2 requires receivers to skip unknown frames using their length. | `quinn/src/recursive.rs:486,598` |
| R4 | P1 | The recursive server makes no progress on a complete Entity Stream until PENDING is supplied. Section 6.2.5 makes the announcement optional. The probe sent the payload first, observed no response for 300 ms, then supplied PENDING and immediately received PROCESSING and COMPLETE. The loop confirms that it cannot accept an entity without an announcement. | `quinn/src/recursive.rs:474,500` |
| R5 | P1 | With announcements for entity 1 then entity 2, delivering entity 2's stream first closes the connection with “PENDING and EntityHeader identity differ.” QUIC streams have no cross-stream delivery order guarantee. The receiver associates the next accepted stream with the next announcement instead of dispatching by header identity. | `quinn/src/recursive.rs:500,616`; Section 5.2 |
| R6 | P1 | A checkpoint requested while a root is DEHYDRATING closes the connection with “checkpoint barrier is not satisfied.” The request should remain pending while applicable work resolves, subject to specified timeout behavior. It is not an invalid state merely because the barrier is not yet satisfied. | `quinn/src/recursive.rs:540`; Section 9.3 |
| R7 | P1 | A connection negotiating maximum scope depth 0 still accepts and completes a child at depth 1. Negotiated limits are returned to the peer, but session construction and receive processing use server-wide limits. | `quinn/src/recursive.rs:458,500,662` |
| R8 | P1 | A connection negotiating only Layer 0 receives YIELDED and DEFERRED when the exemplar processor chooses yield. These statuses are explicitly forbidden without Layer 2 negotiation. All outbound recursive statuses are encoded with `LayerSupport::LAYER2`. | `quinn/src/recursive.rs:645,754,1771`; Section 6.2.3 |
| R9 | P1 | The recursive client accepts a CHECKPOINT ACK with a different checkpoint ID and sequence number. It checks only the ACK flag. Section 9.3 requires exact identifying-field comparison and connection closure on mismatch. | `quinn/src/recursive.rs:1267` |
| R10 | P1 | The encoder emits quorum ratio 0.75 as `fa3f400000` (binary32). Section 3.4.2 invokes RFC 8949 Section 4.2, under which `f93a00` (binary16) preserves that value and is the required shorter encoding. The draft's `float32` schema and its general deterministic-encoding requirement conflict; the current “valid” recursive vector also contains the longer encoding. | `src/lib.rs:1594`; `test-vectors/recursive/index.tsv:2`; Section 8.3 |
| R11 | P1 | Two concurrent recovery invocations on the same database both call the application `resume` function for the same already-redeemed claim. Each reads PROCESSING before either persists completion. Atomic redemption does not make execution exclusive. The existing test exercises sequential recovery, which does not detect this race. | `quinn/src/recursive.rs:412` |

For R10, see [RFC 8949 Section 4.2](https://www.rfc-editor.org/rfc/rfc8949.html#section-4.2). Fix the specification and codec together: either define a deliberate representation rule, or preferably express quorum as an exact integer threshold or bounded integer ratio with explicit rounding. A self-generated round trip is insufficient evidence of standards compliance.

For R11, the trait documentation already asks application processors to make external effects idempotent. That remains necessary even after adding execution ownership. The missing recovery exclusion is additional to the general impossibility of making arbitrary external effects transactional through a transport protocol alone.

## Further findings from source inspection

These were inspected in code but were not all exercised by a dedicated network probe.

- **Processing precedes admission checks.** `receive_entity` calls `processor.process` and writes payload storage before checking whether the first entity is a root, whether the parent exists, whether the depth is valid, or whether the session/entity can be inserted. `receive_scope_digest` calls `processor.rehydrate` before validating the supplied digest and readiness. A rejected request can therefore invoke application behavior. Validate identity, authorization, policy, limits, and readiness before callbacks or writes. See `quinn/src/recursive.rs:645,655,803`.
- **Chunk memory is not bounded by the advertised per-entity limit while receiving.** Every chunk is accumulated in a map before the assembled length is checked. Memory can grow toward chunk-count times per-chunk limit. Enforce aggregate bytes on every insertion, use bounded spool storage, and keep control processing active. No exhaustion test was run. See `quinn/src/recursive.rs:1584,1609,1631`.
- **Large payloads are fully buffered.** `read_to_end`, payload copies, and full chunk assembly precede processing. This implements bounded transfers, not the stated incremental processing objective for arbitrarily large inputs. See `quinn/src/recursive.rs:1645` and `quinn/src/transport.rs:147`.
- **Control-frame allocation contradicts the draft.** The recursive reader allocates `vec![0; length]` after a one-MiB size check but before verifying message type/context or receiving the body. Section 10.3 prohibits allocation solely from the untrusted length, requiring incremental allocation or a smaller initial buffer until validation. A cap reduces impact but does not satisfy the stated rule. See `quinn/src/recursive.rs:1722`.
- **Mandatory Layer 0 is only partially implemented.** The common Java, Rust, and C++ commands implement a one-entity transfer sequence. Java/C++ do not implement the Layer 0 DEHYDRATING/REHYDRATING state machine, manifest resolution, general multi-entity windows, or all core optional fields. The recursive Rust path rejects a second root, requires `pipestream.session-id` metadata, and has no per-scope cursor implementation. These limitations cannot be inferred from `layer0-core: true` by an independent peer.
- **Capabilities from a richer peer cannot always downgrade.** Java and C++ reject core capability keys `max-scope-depth` and `max-entities-per-scope` as unknown. A Layer 0-only peer still needs to parse the base capability grammar and negotiate away unsupported optional layers.
- **Additional ACK checks are missing.** The recursive claim client checks the ACK bit but not equality of claim ID, session ID, or state checksum. Section 6.7.1 requires equality. Its barrier and scope-digest response helpers also do not fully correlate responses to requests. See `quinn/src/recursive.rs:1211,1250,1284`.
- **Layer 2 advertisement exceeds its implemented contract.** The runtime advertises the single Layer 2 boolean, while retry scheduling and several policy actions are not implemented. The draft has no negotiated identifier for the README's narrow durable-yield subset. A README limitation cannot amend what a capability means on the wire. See `quinn/src/recursive.rs:942` and Section 3.3.
- **Authentication and authorization are not wired into the service.** TLS verifies the server for clients, but the server uses `with_no_client_auth` and exposes no admission/claim authorization decision in this service path. Session metadata is supplied by the sender. Define whether a claim is an authenticated principal's resource or a bearer capability, then bind claims, state, quotas, and callbacks to that model. See `quinn/src/recursive.rs:1709` and Section 10.6. This is particularly relevant to the proposal's cross-organization use case.

## Proposal improvements, in priority order

### 1. Specify identity, manifest closure, and what completion proves

The central semantic question is how a receiver knows that it has seen **every** child, rather than every child that has arrived so far.

Section 9.2 says the manifest is reconstructed locally from headers and statuses. There is no explicit declaration/seal of the complete child set. Layer 0 has no SCOPE_DIGEST; PENDING announcements are optional. A checkpoint on Stream 0 can overtake unseen Entity Streams. Layer 1 also needs a defined scope-closure producer and lifecycle instead of the exemplar's implicit client-request/server-echo convention.

Specify an explicit work-set declaration and closure mechanism. It can be streamed and compact; it need not put the entire manifest in one large frame. Define its identity, final count or sequence watermark, parent relationship, closure acknowledgment, treatment of omitted/cancelled children, and whether further descendants may be created after closure. Establish the checkpoint cut using this information.

Also resolve:

- Both endpoints may originate entities and allocate scopes, but no allocation partition or owner-qualified namespace ensures uniqueness when both choose the same ID.
- Entity IDs are scope-local, while GOAWAY carries only one unscoped Last Entity ID.
- Scopes have their own cursors, but Section 6.2.4 only allows cursor updates for root scope 0.
- Recycling, late frames, and durable claim identity need a generation/epoch rule or a simpler non-recycling identifier design.
- Quorum, lenient completion, zero-child decomposition, skipped children, and retry exhaustion need precise readiness and digest-count rules. The introductory “all parts successfully processed” guarantee is inconsistent with partial-success policies.

The current Merkle leaf binds only entity ID and terminal status. The transmitted root does not commit to payload, output, parent linkage, or nested digest values. A different processed document with the same IDs and statuses produces the same scope root. This is a completion-status digest, not proof of correct computation or end-to-end content lineage. Either narrow the claim, or define a separate versioned receipt that commits to the sealed manifest, payload/result digests, policy, and child-scope receipts. If proofs cross trust boundaries, specify their authentication. A hash supplied by a worker cannot prove that the worker performed the claimed computation.

### 2. Specify bounded asynchronous operation and extension negotiation

Keep control-frame parsing, data-stream reception, worker completion, and timer handling independent. A stalled entity must not block checkpoint/cancellation/status processing for unrelated work. Define what a peer does on early data streams, reordered streams, duplicate announcements, stream resets, unfinished checkpoints, and shutdown races. [RFC 9308 Sections 4.3 and 4.4](https://www.rfc-editor.org/rfc/rfc9308.html#section-4.3) specifically discuss ordering and flow-control deadlocks when mapping applications to QUIC.

Specify enforceable per-connection, per-entity, and aggregate byte/metadata/chunk limits with clear error scope and negotiated meanings. Checkpoint deadlines need timer origins, timeout outcomes, and interaction with nested scopes. Reserve sufficient progress for control traffic and descendant work so an unresolved parent cannot consume the only credit needed to complete its children.

Use a base extension-negotiation mechanism that existing peers can parse. The current combination of closed capabilities maps and future algorithms advertised through new capability fields lacks that bootstrap. Define supported and required extension identifiers, activation boundaries, unknown-extension behavior, and the minimum behavior promised by each capability. A registered extension must be able to carry its negotiated semantics without requiring older peers to interpret new fields.

### 3. Define authenticated recovery and ambiguous-outcome handling

Specify the principal/session/claim binding, target authority, retention period, maximum lifetime, clock assumptions, revocation, and permissions needed to resume. Distinguish a secret bearer token from a public lookup identifier. If bearer security is intended, consider at least 128 random bits rather than treating a 64-bit identifier as the sole secret. A checksum is an integrity value, not automatically a credential. [RFC 3552 Section 4.4](https://www.rfc-editor.org/rfc/rfc3552.html#section-4.4) distinguishes authentication from authorization.

Handle the case where redemption commits but its ACK is lost. A second redemption should not re-execute, but the authorized requester still needs a defined way to discover acceptance and retrieve the durable result. Specify an idempotent request identity and a retained outcome or a status operation. Recovery executors need durable ownership/fencing, while application effects need explicit idempotency or transactional integration. Define the boundary of the guarantee without promising generic exactly-once external execution.

A small, precisely named recovery extension would be stronger than a broad Layer 2 boolean that includes partly unspecified retry, timeout, and completion behaviors. Put provider-specific storage/encryption conventions in application profiles unless cross-vendor interoperability requires them in the core.

## IETF process and document quality

A Proposed Standard should resolve known design choices and receive community review. There is no general rule that three languages, containers, or two implementations automatically qualify a proposal. Two independent interoperating implementations with deployment experience are among the criteria for later advancement to Internet Standard. Working groups can impose additional implementation requirements. See [RFC 7127](https://www.rfc-editor.org/rfc/rfc7127.html) and [RFC 6410 Section 2.2](https://www.rfc-editor.org/rfc/rfc6410.html#section-2.2).

Keep the implementation status factual and explicitly scoped. [RFC 7942](https://www.rfc-editor.org/rfc/rfc7942.html) supports reporting implementation experience, including version and coverage; it is not a certification process. The current appendix already acknowledges prototypes and a tested subset, but “feature-complete” elsewhere overstates it.

Before seeking a standards-track adoption discussion:

- Replace unsupported uniqueness, performance, and “stateless control plane” claims with bounded statements and reproducible comparisons. PipeStream's STATUS payload has no dependency on compression history, but lifecycle validation depends on substantial prior state.
- Correct the WebTransport citation. [RFC 9297](https://www.rfc-editor.org/rfc/rfc9297.html) defines HTTP Datagrams and the Capsule Protocol. The [WebTransport over HTTP/3 specification](https://datatracker.ietf.org/doc/draft-ietf-webtrans-http3/) explicitly permits both endpoints to open streams after establishment. Its client-initiated CONNECT handshake does not by itself disqualify symmetric stream use. Explain the measured cost and deployment tradeoff of the chosen direct QUIC mapping.
- Include the normative QUIC/TLS mapping dependency, [RFC 9001](https://www.rfc-editor.org/rfc/rfc9001.html), in addition to QUIC transport and TLS references. RFC 9846 is a real TLS 1.3 specification; it should not be flagged as an invented reference.
- Resolve URI ambiguity: `/session/123` can match either the optional scope path or the optional entity reference. Define authority restrictions, endpoint/port discovery, numeric bounds, and whether userinfo is allowed. Alternatively defer the scheme until it has a clear interoperability role.
- Make registries complete: field widths and unassigned ranges, extension/status/yield-reason handling, error scope, and registration templates. Align “Expert Review” with the requirement for a permanent public specification, or explain the choice. Use [RFC 8126](https://www.rfc-editor.org/rfc/rfc8126.html) as the review guide.
- Explain connectivity behavior when UDP is unavailable. A fallback transport is a design choice, not an automatic requirement, but failure/fallback behavior should be explicit.
- Treat `idnits` as a document check. It cannot validate distributed state machines, authorization, or interoperability.

## Conformance milestone

The next milestone should be a requirement-to-test matrix, with a stable requirement ID for every normative rule. For each applicable implementation, record success, malformed input, state-ordering, and resource-limit coverage. Add whole wire sequences, independently reviewed Merkle vectors, reordered streams, slow readers, cancellations, mismatched acknowledgments, ID reuse, partial policies, and recovery at each persistence boundary.

The current driver is independent of the protocol crates, which is a useful property. But its nine pairings exercise the shared one-entity Layer 0 scenario. It does not establish full Layer 0 conformance or cross-implementation Layer 1/2 conformance. The recursive CLI runner checks that a lineage file has 32 bytes; that alone is not an independent expected-digest check. The advertised CDDL-to-appendix synchronization is also not tested by `validate_cddl`, which validates standalone fixtures without comparing the appendix definitions.

Have the second implementation built from the clarified specification and frozen examples without translating Rust's state machine. Cross-test it with Rust and resolve disagreements against the text. An outside implementer or reviewer will add evidence beyond having three languages maintained within the same project.

## Validation and reproduction

- Fresh `cargo test --locked --offline --workspace` for the unchanged Rust implementation: 18 core tests and 9 Quinn tests passed. The initial sandbox blocked UDP sockets; the authorized rerun with localhost sockets enabled passed.
- `src/main.rs` here reproduced ten distinct codec and wire-behavior findings, including a deliberately incorrect peer ACK. These are diagnostic witnesses with assertions documenting current defects, not conformance-pass tests.
- `src/bin/recovery_probe.rs` reproduced concurrent duplicate recovery execution using two callers and a barrier against one SQLite database.
- Freshly compiled `Wire.java` plus `ProtocolException.java`, and freshly compiled C++ `wire.cpp`, both rejected the same valid minimal capability map. The Java probe used the existing shaded jar only to supply Jackson dependencies.
- The repository's CDDL validator accepted that same map: `bundle3.3 exec cddl cddl/pipestream-layer0.cddl validate /tmp/pipestream-ietf-review.3hYfxw/minimal-capabilities.cbor`.
- The full Java/C++ test suites and nine-pair matrix were not rerun in this review. The new wire probes target the freshly built Rust library. No destructive, overload, or live-service tests were run.

Build diagnostic programs with:

```sh
CARGO_TARGET_DIR=/tmp/pipestream-ietf-review-target cargo build --locked --offline --manifest-path /tmp/pipestream-ietf-review.3hYfxw/Cargo.toml
```

Then run `/tmp/pipestream-ietf-review-target/debug/pipestream-ietf-review` with localhost UDP permitted, and `/tmp/pipestream-ietf-review-target/debug/recovery_probe` for the concurrent recovery case. All listener ports and persistence directories are temporary. The report and probes are local review artifacts, not committed product changes.
