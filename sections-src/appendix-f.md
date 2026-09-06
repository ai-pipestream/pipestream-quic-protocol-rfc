# Version 2 Serialized Messages

These schemas are normative for Section 12 only. Array positions and lengths
are exact; optional values use explicit null, not omitted positions. Version-1
schemas in Appendix C are unchanged. Section 12 supplies semantic constraints,
authentication, direction, correlation, framing, hashing and state transitions
that CDDL alone does not express. All text sizes are UTF-8 octet counts.

~~~~ cddl
; PipeStream version 2. Normative counterpart: Appendix F.
; Array cardinality and CBOR encoding are exact.

v2-message = v2-capabilities / v2-session-message / v2-scope-message
           / v2-work-message / v2-result-request / v2-drain-message
           / v2-refusal / v2-input-header / v2-result-header

v2-number = 0..9223372036854775807
v2-id = 1..9223372036854775807
v2-time = v2-number
v2-duration = 1..31536000000
v2-label = tstr .size (1..128)
v2-digest = bstr .size 32
v2-operation-id = bstr .size 16
v2-producer = 0..1
v2-work-key = [scope: v2-number, producer: v2-producer,
               entity: v2-id]
v2-request-tag = [0, v2-id] / [1, 0..4611686018427387903]
v2-extension-list = [0*32 (1..65534)]
v2-policy = [execution-limit-ms: v2-duration,
             output-retention-ms: v2-duration,
             receipt-retention-ms: v2-duration]
v2-limits = [scopes: v2-id, entities: v2-id, operations: v2-id,
             retained-input-bytes: v2-number,
             retained-output-bytes: v2-number,
             active-jobs: v2-id]
v2-input = [length: v2-number, sha256: v2-digest,
            content-type: v2-label]
v2-output-budget = [count: 0..256, total-bytes: v2-number]
v2-state = 0..8
v2-diagnostic = [code: 0..4294967295, detail: tstr .size (0..512)]
v2-child-scope = [scope: v2-id, producer: v2-producer]
v2-counts = [success: v2-number, failure: v2-number,
             cancelled: v2-number, skipped: v2-number]

v2-capabilities = [
  response: 0..1, supported: v2-extension-list,
  required: v2-extension-list,
  control-limit: 4096..1048576, stream-limit: 1..1024,
  pending-limit: 1..1024, object-limit: v2-number,
  stream-idle-ms: 1000..300000, stream-lifetime-ms: 1000..86400000
]

v2-session-message =
    [0, request: v2-id, creation-sequence: v2-id, policy: v2-policy]
  / [1, request: v2-id, authority: v2-label, owner: v2-label,
     generation: v2-id, creation-sequence: v2-id, policy: v2-policy,
     limits: v2-limits]
  / [2, request: v2-id, authority: v2-label, owner: v2-label,
     generation: v2-id]
  / [3, request: v2-id]
  / [4, request: v2-id, next-creation-sequence: v2-id]

v2-scope-message =
    [0, request: v2-id, operation: v2-operation-id, scope: v2-number,
     entity-ids: [0*256 v2-id], seal: bool]
  / [1, request: v2-id, receipt: v2-operation-receipt]
  / [2, request: v2-id, scope: v2-number, after-entity: v2-number,
     limit: 1..256]
  / [3, request: v2-id, scope: v2-number, producer: v2-producer,
     parent: v2-work-key / null, sealed: bool,
     seal: v2-digest / null, declared: v2-number,
     entries: [0*256 [v2-id, v2-state]], more: bool]
  / [4, request: v2-id, scope: v2-number, seal: v2-digest,
     wait-ms: 0..30000]
  / [5, request: v2-id, summary: v2-scope-summary]
  / [6, request: v2-id, operation: v2-operation-id, scope: v2-number]
  / [7, request: v2-id, receipt: v2-operation-receipt]

v2-scope-summary = [
  scope: v2-number, producer: v2-producer,
  parent: v2-work-key / null,
  seal: v2-digest, declared: v2-number, counts: v2-counts,
  status-root: v2-digest, closed-at: v2-time
]

v2-admit-parameters = [
  work: v2-work-key, input: v2-input, application: v2-label,
  mode: 0..2, execution-ms: v2-duration, outputs: v2-output-budget
]

v2-input-header = [
  0, generation: v2-id, operation: v2-operation-id,
  parameters: v2-admit-parameters
]

v2-work-message =
    [1, request: v2-request-tag, receipt: v2-operation-receipt]
  / [2, request: v2-id, operation: v2-operation-id]
  / [3, request: v2-id, receipt: v2-operation-receipt]
  / [4, request: v2-id, work: v2-work-key, after-revision: v2-number,
     wait-ms: 0..30000]
  / [5, request: v2-id, revision: v2-id, work: v2-work-view]
  / [6, request: v2-id, operation: v2-operation-id,
     work: v2-work-key, expected-attempt: v2-id]
  / [7, request: v2-id, receipt: v2-operation-receipt]
  / [8, request: v2-id, operation: v2-operation-id,
     work: v2-work-key]
  / [9, request: v2-id, receipt: v2-operation-receipt]
  / [10, request: v2-id, operation: v2-operation-id,
     work: v2-work-key]
  / [11, request: v2-id, receipt: v2-operation-receipt]

v2-operation-receipt = [
  operation: v2-operation-id, request-digest: v2-digest,
  body: v2-operation-outcome
]

v2-operation-outcome =
    [0, work: v2-work-key, attempt: v2-id, admitted-at: v2-time,
     deadline: v2-time, child: v2-child-scope / null]
  / [1, scope: v2-number, producer: v2-producer,
     accepted-count: 0..256, declared: v2-number,
     seal: v2-digest / null]
  / [2, work: v2-work-key, expected-attempt: v2-id,
     replacement-attempt: v2-id, accepted-at: v2-time]
  / [3, work: v2-work-key, accepted-at: v2-time,
     disposition: 0..1, state-at-commit: v2-state]
  / [4, scope: v2-number, accepted-at: v2-time]
  / [5, work: v2-work-key, accepted-at: v2-time,
     disposition: 0..1, state-at-commit: v2-state]

v2-work-view = [
  work: v2-work-key, state: v2-state, attempt: v2-number,
  input: v2-input / null, admitted-at: v2-time / null,
  deadline: v2-time / null, terminal-at: v2-time / null,
  receipt-until: v2-time / null, output-until: v2-time / null,
  child: v2-child-scope / null, manifest: v2-result-manifest / null,
  diagnostic: v2-diagnostic / null
]

v2-result-locator = tstr .size (1..1024)
v2-output = [
  index: 0..255, length: v2-number, sha256: v2-digest,
  content-type: v2-label, locator: v2-result-locator
]
v2-result-manifest = [
  2, authority: v2-label, owner: v2-label, generation: v2-id,
  work: v2-work-key, attempt: v2-id, input-sha256: v2-digest,
  committed-at: v2-time, available-until: v2-time,
  outputs: [0*256 v2-output]
]

v2-result-request =
    [0, request: v2-id, work: v2-work-key, attempt: v2-id,
     index: 0..255, expected-sha256: v2-digest]
  / [1, request: v2-id, work: v2-work-key, attempt: v2-id]
  / [2, request: v2-id, manifest: v2-result-manifest]

v2-result-header = [
  1, request: v2-id, generation: v2-id, work: v2-work-key,
  attempt: v2-id, index: 0..255, length: v2-number, sha256: v2-digest
]

v2-drain-message =
    [0, request: v2-id, generation: v2-id,
     root-summary: v2-scope-summary]
  / [1, request: v2-id, generation: v2-id,
     root-summary: v2-scope-summary]
  / [2, request: v2-id]
  / [3, request: v2-id]

v2-refusal = [
  request: v2-request-tag, code: 1..31, detail: tstr .size (0..512)
]
~~~~
