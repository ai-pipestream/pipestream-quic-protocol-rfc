# Appendix C: Schema Reference (CDDL)

This appendix consolidates the normative CDDL {{RFC8610}} schema
definitions for PipeStream Core messages that use negotiated
serialization. These definitions are authoritative for the wire format
when CBOR {{RFC8949}} is the negotiated serialization format (the
default). Fixed control frames on Stream 0, such as STATUS and
SCOPE_DIGEST, use the bit-packed wire formats defined in Section 6 and
are not serialized as CDDL messages.

An informational Protocol Buffers equivalent is provided in Appendix D
for implementations that negotiate Protobuf encoding. Application-
specific payload envelopes and profile-specific schemas are outside the
scope of this appendix; see [PIPESTREAM-DOCPROC] for an example
companion profile that defines such messages.

~~~~ cddl
; -----------------------------------------------------------
; Integer size convention
; -----------------------------------------------------------
; CDDL "uint" is an unbounded unsigned integer. When
; encoded in CBOR, the encoder MUST use the smallest
; CBOR major-type-0 encoding that fits the value.
; The following aliases document the wire-format field
; widths used in fixed-size frames; they do not constrain
; the CBOR encoding but record the maximum value each
; field may carry.
;
;   uint32  values 0..4294967295     (Entity ID, Scope ID)
;   uint64  values 0..2^64-1         (counters, timestamps)
;
; For variable-length serialized messages (CBOR), the
; natural uint encoding applies and receivers MUST accept
; any valid CBOR unsigned integer.
; -----------------------------------------------------------

; -----------------------------------------------------------
; Serialization format negotiation
; -----------------------------------------------------------

serialization-format = &(
  cbor: 0,
  protobuf: 1,
)

; -----------------------------------------------------------
; Capabilities (exchanged during CONNECT on Stream 0)
; -----------------------------------------------------------

capabilities = {
  layer0-core: bool,
  layer1-recursive: bool,
  layer2-resilience: bool,
  ? max-scope-depth: uint .le 7,
  ? max-entities-per-scope: uint,
  ? max-window-size: uint,
  ? serialization-format: serialization-format,
  ? keepalive-timeout-ms: uint,   ; Default: 30000 (30s)
}

; -----------------------------------------------------------
; Entity header (prefixes each entity on Entity Streams)
; -----------------------------------------------------------

entity-header = {
  entity-id: uint,                ; 32-bit on wire (STATUS frame)
  ? parent-id: uint,              ; 32-bit scope-local
  ? scope-id: uint,               ; 32-bit (Section 6.2.1)
  layer: uint .le 3,              ; Data layer 0-3
  ? content-type: tstr,
  payload-length: uint,           ; Payload byte count
  ? checksum: bstr .size 32,      ; SHA-256; SHOULD be present
  ? metadata: { * tstr => tstr },
  ? chunk-info: chunk-info,
  ? completion-policy: completion-policy,  ; Layer 2
}

chunk-info = {
  total-chunks: uint,
  chunk-index: uint,
  chunk-offset: uint,
}

; -----------------------------------------------------------
; Completion policy (Layer 2)
; -----------------------------------------------------------

completion-policy = {
  ? mode: completion-mode,
  ? max-retries: uint,
  ? retry-delay-ms: uint,
  ? timeout-ms: uint,
  ? min-success-ratio: float32,
  ? on-timeout: failure-action,
  ? on-failure: failure-action,
}

completion-mode = &(
  unspecified: 0,
  strict: 1,
  lenient: 2,
  best-effort: 3,
  quorum: 4,
)

failure-action = &(
  unspecified: 0,
  fail: 1,
  skip: 2,
  retry: 3,
  defer: 4,
)

; -----------------------------------------------------------
; Checkpoint frame (Type 0x81)
; -----------------------------------------------------------

checkpoint-frame = {
  checkpoint-id: tstr,
  sequence-number: uint,
  checkpoint-entity-id: uint,
  ? scope-id: uint,
  ? flags: uint,
  ? timeout-ms: uint,
}

; -----------------------------------------------------------
; Entity status codes
; -----------------------------------------------------------

entity-status = &(
  unspecified: 0,
  pending: 1,
  processing: 2,
  complete: 3,
  failed: 4,
  checkpoint: 5,
  dehydrating: 6,
  rehydrating: 7,
  yielded: 8,
  deferred: 9,
  retrying: 10,
  skipped: 11,
  abandoned: 12,
)

; -----------------------------------------------------------
; Assembly Manifest entry
; -----------------------------------------------------------

assembly-manifest-entry = {
  parent-id: uint,
  ? scope-id: uint,
  children-ids: [* uint],
  ? children-status: [* entity-status],
  ? policy: completion-policy,
  ? created-at: uint,
  ? state: resolution-state,
}

resolution-state = &(
  unspecified: 0,
  active: 1,
  resolved: 2,
  partial: 3,
  failed: 4,
)

; -----------------------------------------------------------
; Yield token (Layer 2)
; -----------------------------------------------------------

yield-token = {
  reason: yield-reason,
  ? continuation-state: bstr,
  ? validation: stopping-point-validation,
}

yield-reason = &(
  unspecified: 0,
  external-call: 1,
  rate-limited: 2,
  awaiting-sibling: 3,
  awaiting-approval: 4,
  resource-busy: 5,
)

; -----------------------------------------------------------
; Claim check (Layer 2)
; -----------------------------------------------------------

claim-check = {
  claim-id: uint,
  entity-id: uint,
  ? scope-id: uint,
  expiry-timestamp: uint,
  ? validation: stopping-point-validation,
}

; -----------------------------------------------------------
; Stopping point validation (Layer 2)
; -----------------------------------------------------------

stopping-point-validation = {
  ? state-checksum: bstr,
  ? bytes-processed: uint,
  ? children-complete: uint,
  ? children-total: uint,
  ? is-resumable: bool,
  ? checkpoint-ref: tstr,
}

; -----------------------------------------------------------
; File storage reference
; -----------------------------------------------------------

file-storage-reference = {
  provider: tstr,
  bucket: tstr,
  key: tstr,
  ? region: tstr,
  ? attrs: { * tstr => tstr },
  ? encryption: encryption-metadata,
}

encryption-metadata = {
  algorithm: tstr,
  ? key-provider: tstr,
  ? key-id: tstr,
  ? wrapped-key: bstr,
  ? iv: bstr,
  ? context: { * tstr => tstr },
}
~~~~
