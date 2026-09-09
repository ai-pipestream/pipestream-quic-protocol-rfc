# Scenario matrix detail — group G6 (wire abuse) and group R (resources)

Requirement families: V2-WIRE 1–5, V2-NEG 3–4, V2-RESULT 4, V2-STORE 6–7,
V2-RESOURCE (cross-cutting). G6 probes are raw-QUICK/CBOR fixtures the
driver frames itself with generic libraries (the conformance crate already
does this for extension probes — same independence rules; narrow malformed
subset only, kept distinct from the real client/server directions).

## G6 — malformed-peer probes (driver acts as the peer)

### g6-canonical-violations
- Hand-frame control bodies violating deterministic CBOR: non-minimal
  integers, indefinite lengths, tags, floats, trailing items, bad UTF-8,
  wrong array cardinality, forbidden identity characters.
- Expected: FRAME_ERROR (1), connection-fatal where specified; reuse the
  24 refuse-rows of frozen `test-vectors/v2/wire.tsv` as the case list
  (driver checks, never regenerates, the corpus).

### g6-direction-and-correlation
- Server-direction message from the client, second CAPABILITIES,
  unsolicited selection, increased limits in a capabilities response,
  duplicate/unsolicited control response, response kind/identity
  mismatch, repeated or decreasing request IDs, a bare control FIN
  before any detach (FRAME_ERROR, QUIC application error 0x201).
- Expected: FRAME_ERROR / EXTENSION_UNSUPPORTED per the negotiation
  rules. Stream-COUNT ceilings (a second client bidirectional stream;
  more than `dataStreams` concurrent unidirectional streams) are
  enforced by QUIC transport parameters, not application refusals: the
  driver asserts the transport error class (client observes
  STREAM_LIMIT_ERROR locally) and never waits for a LIMIT_EXCEEDED
  frame there (normative-clarifications-review.md item 4).

### g6-stream-identity-and-fin
- Input header naming the wrong stream geometry (admission headers must
  not claim their own stream ID), wrong declared length (over/under),
  wrong digest, missing FIN, trailing bytes after FIN, error after a
  result header, duplicate response on a result request, result header
  naming an unknown/non-result/already-started request.
- Expected: named refusals per Section 12.2 (FRAME_ERROR /
  INTEGRITY_ERROR); invalid input discards the partial reception, never
  the declaration; a refused result header stops only that stream.

### g6-stopped-control-and-transfers
- RESET_STREAM on control (Stream 0), STOP_SENDING mid-input and
  mid-result, connection loss/reordering/replacement streams.
- Expected: CONTROL_RESET (14) terminates the connection; STOP_SENDING
  alone is never an admission receipt; no declared obligation changes,
  no failure commits, no new attempt is authorized by transport events.
  NOTE: this row exercises the path of Claude's quiche pipestream.4 fix
  (MAX_STREAMS credit after refused inputs) — record credit behavior.

## R — resource boundaries (B3)

### r-capability-manifest (runs first, always)
- Record host capabilities, selected collectors, permissions, and
  collector overhead/calibration BEFORE any measurement row. An
  unsupported mandatory metric leaves its row INCOMPLETE, never zero.

### r-connection-ceiling
- Per-principal and global connection limits: open connections up to
  the configured bound from one principal and from several; excess
  refused pre-authentication (CONNECTION_REFUSED) or per the documented
  bound. Incomplete handshakes counted in the bound.

### r-stalled-principal-progress
- One abusive principal: stalls input streams (send partial payloads,
  never FIN), holds pending requests, keeps result streams open without
  reading. A healthy principal alongside must retain control progress
  (next-sequence, declare, admit, lookup) within a stated test deadline
  while the transport functions. No assertion requires progress the
  network itself withholds.

### r-memory-ladder
- Payload ladder (e.g. 64 KiB / 1 MiB / 16 MiB / 64 MiB) and inventory
  ladder (1 / 16 / 64 concurrent works) against configured limits:
  measure per-process RSS/HWM, threads, FDs, and (Java) heap +
  native/direct where collectable, (Rust) heap via the crate's
  allocator gates where present. Memory must plateau at the configured
  bounds, not scale with payload/inventory beyond them. Limits and the
  environment allowance are frozen BEFORE the decisive run (recorded
  with rationale); internal counter plateaus alone are not RSS
  evidence; one small payload proves nothing.

### r-staging-and-journal-bounds
- Staging objects, journals, retained data at configured ceilings:
  exhaustion refuses NEW work (named refusal) without breaking existing
  promises; file handles bounded; capacity stays charged while physical
  I/O is busy and reconciles after safe cleanup/restart.

### r-network-bytes
- Fixture-scoped network measurement (interface counters or packet
  capture; loopback double-counting rules stated): handshake,
  retransmit, TLS, and retry bytes included; recorded separately from
  logical payload bytes. Dead collectors, omitted worker samples, and
  truncated records are detected and fail the row.

### r-native-credit
- Borrowed native flow credit vs application queue bytes vs actual
  transport completion, evidenced at packet level from the
  source-pinned transport where the API cannot establish the property
  (wrapper counters are not a substitute). Pairs with g6-stopped-*
  observations of MAX_STREAMS/flow-credit release.

## Measurement-scope rules (all R rows)
- Rust heap, Java heap, whole-process RSS/HWM, native/direct, threads,
  FDs, file lengths, allocated filesystem blocks, actual disk I/O, and
  network bytes are SEPARATE scopes; every child process and native
  component of the stated process group is included; collection method
  and scope are recorded per sample and never substituted mid-matrix.
