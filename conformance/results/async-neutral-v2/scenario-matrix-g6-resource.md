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

#### Frozen limits and environment allowance (fixed at milestone 17)

The JVM heap limit is now FROZEN in the fixture launch itself, before the
decisive run of this row, and is not a per-row choice:
`java -Xms256m -Xmx2g …` on every Java subject process the driver spawns
(`conformance/src/durable/process.rs`, `JAVA_MEMORY_FLAGS`). It is recorded
in `run.tsv` (`java_memory_flags`), in the r-capability-manifest
`artifacts/manifest.tsv` (`java_memory_freeze`, with this rationale) and in
each R row's `observed.tsv`. The Rust subject has no equivalent knob and is
launched unchanged; that asymmetry is stated, not equalised.

Rationale for the values, from M16 evidence rather than taste:

1. Why freeze at all. At M16 the Java subject ran with the JVM default max
   heap, which is a fraction of host RAM. On this host (121 GiB) that is a
   ~30 GiB ceiling, so the collector had no reason to run: the M16 heap
   scope showed used heap climbing 8 MiB → 1.17 GiB over 152 samples with
   RSS plateauing near 1.4 GiB. A measurement whose bound is "whatever this
   machine happens to have" is not reproducible and cannot support a
   plateau claim on any other host.
2. Why 2g for `-Xmx`. The largest heap occupancy actually observed at M16
   was 1.17 GiB, and that figure is a GC sawtooth peak with garbage
   included, not a live set. 2 GiB leaves headroom above the observed peak
   while still being far below the host default, so a genuine leak in the
   ladder hits the ceiling and shows up as GC pressure or an OOM instead of
   being absorbed by the host. It also bounds the direct/native scope: the
   JVM's default direct-memory ceiling tracks max heap, so `-Xmx2g` caps
   the Netty direct buffers too without a second flag whose interaction
   would have to be argued separately.
3. Why 256m for `-Xms`, not 2g. Setting `-Xms` equal to `-Xmx` commits the
   plateau at launch: RSS would start at the ceiling and the plateau
   assertion ("tail p90 ≤ baseline median + allowance") would pass for a
   subject that leaks, because the baseline already contains the ceiling.
   A small initial heap keeps growth observable. The cost is that JVM
   warm-up growth is real growth in the samples, which is exactly why the
   Java allowance in the plateau assertion stays baseline/2 + 128 MiB
   rather than the rust baseline/4 + 64 MiB.
4. What a change would require. If a ladder rung genuinely needs more than
   2 GiB, the ceiling is RE-frozen and re-recorded before that row's
   decisive run, with the new value and reason written here; it is never
   raised mid-matrix or per direction. The `-Xmx` figure is a frozen limit,
   not a measurement, and no row may quote it as one.

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
  `/proc/<pid>/fd` count, `/proc/<pid>/io` read/write bytes — and, from
  milestone 17, `cancelled_write_bytes`) into a per-direction
  `resources.tsv`.
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
- Stall enforcement differed in KIND between subjects at this pin and both
  were recorded: the rust server aborts each stalled input stream
  individually (STOP_SENDING 0x204) and additionally queues a per-stream
  LIMIT_EXCEEDED Refusal ("input receive deadline") on the control
  stream, leaving the connection usable; the java server enforced at the
  CONNECTION level, closing the whole connection with APPLICATION_CLOSE
  0x204 at its idle bound, so no per-stream refusal was readable
  afterwards. A row asserts enforcement per stream through either
  channel and records which one fired. SUPERSEDED at milestone 17b: at the
  Java pin `7585a9dc` the kinds CONVERGED — the java server no longer
  closes a durable connection for control silence, so both subjects now
  enforce per stalled stream on a surviving connection and both queue the
  same named refusal. The either-channel rule stays as written, because it
  is what let the row record the difference while it existed.
- Measuring transport-level enforcement requires driving the client
  runtime before judging a write: a quinn write can be accepted into an
  undriven connection whose CONNECTION_CLOSE has not been processed yet,
  which reads as "still open" when the peer closed seconds earlier. The
  probe therefore polls connection liveness first and records it.

## Group R status (milestone 17 — batch A at the 0176855 Java pin)

Batch A rerun green in dev against Java subject `0176855` (all-jar
`d658fe9e…`) with the JVM heap frozen at `-Xms256m -Xmx2g`, archived run
`durable-18d3ea398f09f12e`. Row statuses are unchanged (three DONE, four
SPEC); what changed is the evidence and two fixture corrections.

- Resources schema is now `# pipestream-resources-v2`, 14 columns: the
  new one is `/proc/<pid>/io cancelled_write_bytes` after `write_bytes`.
  All three io counters are MANDATORY; an absent one is an `error:` note
  that fails the row, never a zero. The validating reader refuses a v1
  header rather than reading 13 columns at v2 offsets.
- Connection bounds are unchanged at the new pin (rust 4/16, java 8/32,
  recovery on attempt 1 / 2), but the java per-owner refusal now carries
  a named reason: `APPLICATION_CLOSE 0x204 reason="owner connection
  ceiling"` where M16 saw an empty reason. The java global refusal is
  still a transport-level close.
- Stall enforcement KIND is unchanged on both subjects, and the java
  close reason is now named. rust: 3/3 stalled inputs aborted per stream
  (`STOP_SENDING 0x204`) by idle+10 s and 3/3 `LIMIT_EXCEEDED` refusals
  ("input receive deadline") drained from a still-live control stream at
  window end. java: the whole connection closed at the idle bound with
  `APPLICATION_CLOSE 0x204 reason="idle control deadline"` (M16: empty
  reason), 0/3 per-stream refusals readable. NAMED GAP: an
  APPLICATION_CLOSE discards control frames the peer queued and the
  client had not read, so this row cannot distinguish "no per-stream
  refusal was sent" from "one was sent and the close discarded it".
- Disk-I/O scopes over the stall window, anchor pid: rust `write_bytes`
  11.52 MB/s of which `cancelled_write_bytes` 11.40 MB/s (98–99%
  cancelled before writeback); java `write_bytes` 53.8 KB/s with
  `cancelled_write_bytes` 0 over the window (M16 java: ~4.3 MB/s). The
  rust figure is an observation about the authority's storage layer, not
  a bound this row asserts.
- Memory at the frozen heap: java RSS baseline median 348,084 KiB → tail
  p90 367,760 KiB (growth 19,676), used heap max 161,068 KiB over 152
  jstat samples with 0 probe gaps; M16, with the JVM default max heap,
  showed 982,212 → 1,403,300 KiB and a used-heap max of 1,171,499 KiB.
  Rust: RSS 19,916 → 21,152 KiB, FDs 12 → 15.
- Healthy-principal progress: worst latency 150.5 ms (rust, 23 rounds ×
  5 ops) and 150.7 ms (java, 38 rounds × 5 ops) against the 10 s
  deadline.

Two fixture measurement corrections, both in
`r-stalled-principal-progress`:

1. The abusive principal's connection now sends QUIC PINGs every 5 s with
   an explicit 60 s transport idle timeout, because the row leaves it
   silent for 90 s between enforcement probes — longer than the default
   idle timeout. Without this the fixture's own transport tore the
   connection down and every stalled stream died with it, which reads
   exactly like subject enforcement and is not. PINGs are transport
   traffic carrying no object-stream data, so they must not renew an
   application receive deadline. A probe that finds the connection gone
   with a transport idle timeout now records its stream aborts as NOT
   attributable and does not count them.
2. The row waits for its own endpoint to drain and then a fixed 30 s
   settle before signalling the subject to stop, so a connection that was
   alive one instant earlier is not still on the subject's books when
   SIGTERM lands. No measurement is taken during that window and the
   collector is already stopped.

## Group R status (milestone 17b — batch A at the 7585a9dc Java pin)

Batch A rerun green in dev against Java subject `7585a9dc` (all-jar
`61ab64a3…`), same frozen JVM limits `-Xms256m -Xmx2g`, same rust subject
pin, archived run `durable-18d3ed1531daad17` (318/318 manifest entries
verified). Row statuses are unchanged (three DONE, four SPEC); no fixture,
scenario or collector code changed this milestone, so every difference
below is subject behaviour or run-to-run variation, not a measurement
change.

- STALL ENFORCEMENT KINDS HAVE CONVERGED. java: the connection is LIVE at
  both enforcement probes (`idle-bound+10s` and `lifetime-bound+10s`), the
  three stalled inputs are aborted per stream by lifetime+10 s
  (`STOP_SENDING 0x204`, 3/3 distinct), and 3/3 `LIMIT_EXCEEDED` (code 4)
  Refusals with detail "input receive deadline" — one per stalled tag
  (6, 10, 14) — are read from a still-open control stream at window end.
  No APPLICATION_CLOSE appears on this row; M17's `0x204 reason="idle
  control deadline"` at the idle bound is gone. rust is unchanged: 3/3
  aborted by idle+10 s and 3/3 refused, connection live throughout. The M17
  NAMED GAP (a close discarding queued control frames, so the row could not
  tell "none sent" from "sent and discarded") does not apply here, because
  nothing is discarded.
- What the java close was hiding: at M17 the java direction showed 3/3
  streams aborted by idle+10 s, but the connection close took them down.
  With the connection kept, all three survive idle+10 s (0/3 aborted) and
  are aborted per stream by lifetime+10 s. The java input receive deadline
  therefore fires between idle+10 s (40 s) and lifetime+10 s (130 s); the
  row brackets it and claims no value inside the bracket.
- Connection bounds unchanged on both subjects: rust 4 per principal
  (refusal on attempt 5) / 16 global, both refusals post-auth
  `APPLICATION_CLOSE 0x204 reason="LIMIT_EXCEEDED"`, recovery on attempt 1;
  java 8 per principal (refusal on attempt 9) / 32 global, per-owner
  refusal post-auth `APPLICATION_CLOSE 0x204 reason="owner connection
  ceiling"`, recovery on attempt 2. The java GLOBAL refusal is again a
  transport-level refusal with the same peer text ("the server refused to
  accept a new connection"), classified this run as `pre-auth transport
  refusal (connect failed): open control stream` where M17 recorded the
  same text as a post-auth close — a race in the client between the
  handshake completing and the abort arriving, not a change of bound or
  channel. Incomplete-handshake accounting is still a NAMED GAP.
- Disk I/O over the stall window, anchor pid: rust `write_bytes` 11.60 MB/s
  of which `cancelled_write_bytes` 11.48 MB/s (98% cancelled before
  writeback; M17: 11.52 / 11.40 MB/s — unchanged, still the open question
  to Meta about the Rust authority's storage layer); java `write_bytes`
  56.3 KB/s with `cancelled_write_bytes` 2.7 KB/s, i.e. 4% cancelled where
  M17 measured 0 over a slightly shorter window (M17: 53.8 KB/s, 0). This
  row asserts no bound on either counter.
- Memory and FDs at the frozen heap: java RSS baseline median 349,296 KiB →
  tail p90 371,752 KiB (growth 22,456; M17: 348,084 → 367,760, growth
  19,676), FDs 20 → 21, used heap min 9,370 / max 160,895 KiB over 152
  jstat samples with 0 probe gaps (M17: 8,396 / 161,068, 0 gaps). rust RSS
  19,684 → 20,844 KiB (growth 1,160; M17: 19,916 → 21,152), FDs 12 → 15.
  Holding the abusive connection open for the whole window costs the JVM
  about 4 MiB more tail RSS and nothing measurable in heap.
- Healthy-principal progress: worst latency 150.528 ms (rust, 23 rounds × 5
  ops) and 150.648 ms (java, 38 rounds × 5 ops) against the 10 s deadline
  (M17: 150.534 / 150.677 ms).

## Group R status (milestone 18a — r-memory-ladder)

`r-memory-ladder` is IMPLEMENTED and green in dev against both subjects
(rust-raw-client/rust-server and rust-raw-client/java-server), archived run
`durable-18d428da0717c79d` (421/421 manifest entries verified) together with
the `g1-leaf-copy` regression. Row statuses are now four DONE
(`r-capability-manifest`, `r-connection-ceiling`,
`r-stalled-principal-progress`, `r-memory-ladder`) and three SPEC
(`r-staging-and-journal-bounds`, `r-network-bytes`, `r-native-credit`).

Registered row ids were reconciled with this document, which is canonical:
the milestone-16 placeholders `r-pending-ceiling`, `r-staging-quota` and
`r-journal-bounds` were never implemented under those ids and are retired
from `scenarios.rs`; the registry now carries exactly the seven names above.
The mapping is recorded in traceability.md.

### What the row does

Two ladders on one raw-peer connection per direction, against the limits the
SUBJECT declares in the capability selection the row reads on the wire (it
never assumes a documented default):

- payload ladder 64 KiB / 1 MiB / 16 MiB, four admissions per rung;
- an over-limit rung: one input header declaring 64 MiB against a declared
  `object_limit` of 16 MiB, payload never sent (the rung measures a refusal,
  not memory);
- inventory ladder 1 / 16 / 64 cumulative resident works at a fixed 64 KiB
  payload.

Admissions are paced in batches of two and each batch is settled to a
terminal state before the next. Both subjects declare a concurrent-job
ceiling far below the top inventory rung — an unpaced 48-admission burst was
refused `LIMIT_EXCEEDED` "retained input, output or executor capacity" by the
Java subject during development — and a memory ladder that tripped a
concurrency ceiling would be measuring that refusal instead of memory. Those
ceilings are `r-staging-and-journal-bounds`, not this row. What the inventory
ladder therefore varies is RETAINED inventory.

Each rung is measured over the last 6 s of a 12 s quiet settle, so a rung's
plateau statistics contain none of its own transfer activity, and every
figure is a per-tick SUM over the whole sampled process group (anchor plus
every transitive descendant) before it is a statistic — summing per-pid
statistics would invent a number no instant had. Per-rung ticks, group size,
RSS median/p90/max, HWM, threads, FDs and Java heap are in
`artifacts/rungs.tsv`.

### Frozen before the decisive run

`expected.tsv` is written after negotiation and BEFORE the first rung's
traffic, and the allowances in it are derived from the declared limits, not
from what the run produced:

- payload allowance = `stream_limit x object_limit` (the in-flight object
  bytes the subject is configured to hold) + per-subject slack;
- inventory allowance = `pending_limit x control_limit` (the in-flight
  control state it is configured to hold) + the same slack;
- slack is 64 MiB (rust: allocator retention and page-cache-backed store
  mappings in a native process with no heap ceiling) and 256 MiB (java: JVM
  warm-up, code cache, GC sawtooth and metaspace under the frozen 2 GiB max
  heap).

That gives rust 131,072 KiB / 66,560 KiB and java 524,288 KiB / 278,528 KiB.
The JVM heap ceiling itself is the milestone-17 freeze `-Xms256m -Xmx2g`,
unchanged; `jcmd VM.flags` on the live subject confirms it in the archive
(`MaxHeapSize=2147483648`, `InitialHeapSize=268435456`).

### Observed (dev evidence, never an acceptance claim)

1. Declared limits differ between the subjects and are recorded from the wire
   rather than from source: rust `object_limit=16777216 stream_limit=4
   pending_limit=16 control_limit=65536 idle_ms=5000 lifetime_ms=30000`;
   java `object_limit=16777216 stream_limit=16 pending_limit=32
   control_limit=524288 idle_ms=30000 lifetime_ms=120000`. The java
   `pending_limit` and `stream_lifetime_ms` the LISTENER offers (32 /
   120000 ms) are not `DurableOptions.defaults()` (64 / 300000 ms), which is
   why the row reads the selection instead of quoting the library defaults.
2. MEMORY DOES NOT SCALE WITH PAYLOAD on either subject. A 256x payload
   increase (65,536 B -> 16,777,216 B per admission, four admissions per
   rung) moved the group's tail p90 RSS by 1,568 KiB on rust (18,924 ->
   20,492) and 25,008 KiB on java (341,944 -> 366,952). A subject that merely
   buffered one payload once would have grown by at least 16,320 KiB, and one
   that held `stream_limit` of them by 65,280 KiB (rust) / 261,120 KiB
   (java). Both are inside their frozen allowances (131,072 / 524,288 KiB).
3. MEMORY DOES NOT SCALE WITH RETAINED INVENTORY. 1 -> 64 resident works at a
   fixed payload moved tail p90 RSS by 1,532 KiB on rust (20,896 -> 22,428)
   and by nothing measurable on java (367,532 -> 341,948, i.e. the 64-work
   rung sat BELOW the 1-work rung; recorded as growth 0, never as a negative
   number). Allowances 66,560 / 278,528 KiB.
4. The over-limit rung is refused by both, and the two detail strings differ
   and are recorded verbatim: rust `LIMIT_EXCEEDED (4) "input exceeds
   retained duration, bytes or response limits"`, java `LIMIT_EXCEEDED (4)
   "input exceeds negotiated object limit"`. Both name the input stream tag
   (kind 1). Membership is checked before length on both subjects — an
   undeclared entity is refused `CONFLICT` "input membership was not
   declared" first — so the row declares the over-limit entity like any
   other.
5. Handles and threads stay bounded across the whole ladder: rust FDs
   baseline median 12 -> last-rung p90 15 (max 20), threads 49 -> 50; java
   FDs 18 -> 22 (max 24), threads 32 -> 44. HWM over the run: rust 16,260 ->
   23,692 KiB, java 386,004 -> 621,300 KiB (HWM is a high-water mark and
   never falls; it is reported as a separate scope from RSS).
6. Java heap through the rung windows: 42 jstat ticks, 0 probe gaps, min
   10,637 KiB, max 156,058 KiB against the frozen 2 GiB ceiling. The Rust
   heap scope remains a NAMED GAP — there is no black-box allocator counter
   for the subject binary and RSS/HWM is never substituted for it.
7. JAVA NATIVE/DIRECT IS UNAVAILABLE ON THIS HOST, with the exact check
   recorded: `jcmd <pid> VM.native_memory summary` answers "Native memory
   tracking is not enabled". Enabling it requires adding
   `-XX:NativeMemoryTracking` to the JVM launch flags, which are FROZEN
   before this matrix's measurement rows; changing them here would re-open
   every earlier R row's frozen environment, and NMT also adds its own
   overhead to the measurement it would be added to serve. Recorded as a
   named gap with the jcmd transcript archived
   (`artifacts/jcmd-native.txt`), never inferred from RSS minus heap. A
   future milestone may re-freeze the launch flags WITH NMT and rerun every R
   row against the new freeze; it is not a mid-matrix substitution.
8. Collector health: 1,014 ticks / 1,014 sample lines / 0 error lines (rust)
   and 997 / 997 / 0 (java), every sample carrying all five mandatory scopes.

### Fixture timing recorded, never evidence

After the last rung's window closes, the row waits (bounded, 120 s) for every
admitted work to reach a terminal state before it signals the subject to
stop. Without that wait the rust authority's fixed 5 s shutdown grace was
observed to be consumed by the execution-pool wind-down of 76 works before
its transport wait even started, and `OwnedServer::stop` then failed its
drain assertion with `transport_idle: false` and everything else idle — the
same mechanism milestone 17 recorded for the stall row. No measurement is
taken during that wait, and the collector windows have already closed.

## Measurement-scope rules (all R rows)
- Rust heap, Java heap, whole-process RSS/HWM, native/direct, threads,
  FDs, file lengths, allocated filesystem blocks, actual disk I/O, and
  network bytes are SEPARATE scopes; every child process and native
  component of the stated process group is included; collection method
  and scope are recorded per sample and never substituted mid-matrix.
