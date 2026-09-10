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

## Group R status (milestone 16 — batch A)

Implemented and green in dev (INCOMPLETE-labelled, run
`durable-18d3d1d3e0f91dd0`): `r-capability-manifest`,
`r-connection-ceiling` (rust-raw-client/rust-server and
rust-raw-client/java-server), `r-stalled-principal-progress`
(rust-cli-client(bob) + rust-raw-client(alice) against both servers).
Collectors live in `conformance/src/durable/resources.rs`:

- Process scopes at 100 ms over the anchor pid plus every transitive
  ppid descendant (`/proc/<pid>/status` VmRSS/VmHWM/Threads,
  `/proc/<pid>/fd` count, `/proc/<pid>/io` read/write bytes) into a
  per-direction `resources.tsv`.
- Java heap at a 1000 ms cadence (`jstat -gc`, S0U+S1U+EU+OU; each probe
  is itself a JVM launch, so it deliberately runs slower than the /proc
  scopes). Ticks that collected it carry a `heap:jstat` method note;
  ticks that did not leave the column absent (`-`), never zero.
- The Rust heap scope has no black-box collector and is a NAMED GAP;
  RSS/HWM is a separate scope and is never reported as either heap.
- Store scopes (`st_size` and `st_blocks*512` per file) at named
  scenario checkpoints into `store.tsv`.

Still SPEC: `r-memory-ladder`, `r-staging-and-journal-bounds`,
`r-network-bytes`, `r-native-credit`.

Observed in batch A (see the archived run, not quoted as acceptance):

- Connection bounds. rust: 4 admitted per principal, refusal on attempt
  5; 16 admitted globally; both refusals post-authentication
  (APPLICATION_CLOSE 0x204 `LIMIT_EXCEEDED`). java: 8 per principal,
  refusal on attempt 9; 32 globally; the per-principal refusal is a
  post-auth APPLICATION_CLOSE 0x204 with an empty reason and the global
  refusal is a transport-level close ("the server refused to accept a
  new connection"). Capacity recovered from the first (rust) and second
  (java) attempt after every held connection closed. Incomplete-handshake
  accounting is a NAMED GAP: the quinn client completes handshakes
  atomically, so half-open attempts are not observable black-box.
- Stall enforcement differs in KIND between subjects and both are
  recorded: the rust server aborts each stalled input stream individually
  (STOP_SENDING 0x204) and additionally queues a per-stream
  LIMIT_EXCEEDED Refusal ("input receive deadline") on the control
  stream, leaving the connection usable; the java server enforces at the
  CONNECTION level, closing the whole connection with APPLICATION_CLOSE
  0x204 at its idle bound, so no per-stream refusal is readable
  afterwards. A row asserts enforcement per stream through either
  channel and records which one fired.
- Measuring transport-level enforcement requires driving the client
  runtime before judging a write: a quinn write can be accepted into an
  undriven connection whose CONNECTION_CLOSE has not been processed yet,
  which reads as "still open" when the peer closed seconds earlier. The
  probe therefore polls connection liveness first and records it.

## Measurement-scope rules (all R rows)
- Rust heap, Java heap, whole-process RSS/HWM, native/direct, threads,
  FDs, file lengths, allocated filesystem blocks, actual disk I/O, and
  network bytes are SEPARATE scopes; every child process and native
  component of the stated process group is included; collection method
  and scope are recorded per sample and never substituted mid-matrix.
