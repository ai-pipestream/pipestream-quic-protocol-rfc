# Handoff — B: neutral failure/resource certification (Kimi)

Status: IN PROGRESS — NOT REVIEW_READY. Rust-only preparation and the
first both-direction rows exist; the whole B contract has NOT passed.
This file is maintained as work lands; the REVIEW_READY mark is only
valid when this section says the complete both-direction matrix, resource
gates and acceptance integration have passed, with evidence below.

## 1. Branch / base / state

- Branch: `agent/rfc-kimi-neutral-v2`, worktree `/work/worktrees/pipestream-rfc-kimi`.
- Base: `8eb5a17` on `feat/durable-work-results-v2` (contains `82a1b11`).
- Peer dependency consumed: Claude `5e3138a` (SERVER_READY+CLIENT_READY
  provisional) merged at `bc791fb`; merge-scope checked (25 files, all
  Claude-owned, no new transitive deps).
- Nothing merged into the shared feature branch or main; no push
  performed (push authorization not given); no force operations. The only
  merges into this branch are the peer's own Java subject commits, most
  recently `0176855` at `9119627` (M17 re-pin).
- Dirty state: none at M17; the fmt-only diffs in
  `src/v2/authority/admission.rs:1` and `scopes.rs:478` noted at M16 were
  committed with that milestone and the release binary rebuilt at M17.

## 2. Delivered so far

| Commit | Content |
|---|---|
| 1452f60 | interface-v1.md (fixture event/schedule schemas; schema sha256 c566751a… over §2–3); Meta C1 scoped review |
| c3061af | driver-design.md |
| c53037f | scenario-matrix-g2.md |
| 9bb9dbc | M1: durable command skeleton — mTLS, events/schedule modules, process machinery, oracle, g1-leaf-copy, negative controls, independence gate |
| 0beeaa8 | rust-hook-proposal.md (placement proposal) |
| 261e3b8 | M2: hook-free G2 rows (duplicate/changed-param, simultaneous duplicate, uncontrolled crash recovery) |
| 5bd0425 | scenario-matrix-g5.md |
| 08f13c6 | peer-review-claude-java-hooks.md (commit-side sound; 4 SENT-side defects; accepted by Claude, fixes in his next checkpoint) |
| 2162f08 | M3: Java subjects; g1-leaf-copy three directions byte-identical |
| 70393a0 / 0fa10cd | scenario-matrix-g1.md / -g4.md |
| e6bafa4 | M4: G5 identity rows ×5 vs both servers |
| 9a5f390 | hook placement agreement addendum |
| 152c280 | M5: test-only fixture hooks in Rust production crates (separately reviewable) |
| be43a36 / 1f4f35e / 166b679 | scenario-matrix-g3.md / -g7-g8.md / -g6-resource.md |
| 1b9debf | traceability.md (requirement→scenario map + explicit gaps) |
| c1265f0 | interface-v1 clarification: optional schedule header, no event-file header (schema hash now ceb31294…) |
| 8ed8084 | this handoff (living document) |
| 4f015b6 | M6: driver consumes fixture hooks; full G2 lost-ACK matrix ×2 client directions; --archive + MANIFEST.sha256 |
| a5ec836 (+f62abb0) | M7: consume Claude f582341; java-server hook directions on 5 G2 rows; FixtureMain fresh-commit-gating finding |
| 5ec9529 | normative-clarifications-review.md (Claude's 5 items dispositioned; g8-half-close reshaped to §12.8 text) |
| d2207ce | M8: G1 batch A ×5 rows three directions; independent scope-seal oracle, byte-identical across implementations |
| 76a02dd / 85b8412 | M9: G1 branch-mode rows batch B; traceability statuses refreshed |
| ba78098 / 51b0977 / 85b8412 | M10–M12: G3 storage batch A, G3 batch B + G7 expiry, G4 race/fence rows |
| fd52a9f / fe08970 | M13–M14: G8 completion/detach rows; G6 raw wire-abuse probes |
| e219f11 | M15: Java subject re-pinned to 63d03a0/pipestream.4; full 50-row matrix rerun, no regressions |
| 5993042 | M16: R batch A — resource collectors (`durable/resources.rs`) plus `r-capability-manifest`, `r-connection-ceiling` and `r-stalled-principal-progress` against both servers |
| this commit | M17: Java subject re-pinned to `0176855`; JVM heap frozen before measurement; resources schema v2 (`cancelled_write_bytes`); R batch A + full matrix rerun |

## 3. Verification evidence (M8 snapshot; superseded by §3a for the
current milestone's gates, counts and subject pins)

- `cargo test -p pipestream-conformance`: 65 passed / 0 failed (baseline
  was 24 before this assignment).
- `cargo clippy --all-targets -p pipestream-conformance -- -D warnings`:
  clean. `cargo fmt --check`: clean for the crate. Production crates
  after 152c280: clippy/fmt clean; core 470, quinn 236, server 12 tests
  green.
- One load-sensitive 20 s watch timeout in
  `cli_reopens_committed_work…` observed under concurrent build load;
  passed isolated and full re-runs. Recorded, not dismissed; watch for
  recurrence.
- Dev runs (all INCOMPLETE-labelled, never PASS): archived under
  `conformance/results/async-neutral-v2/runs/` with MANIFEST.sha256 per
  run: `durable-18d3849c6bd74b8b` (M6), `durable-18d385c7335e12e0` (M7),
  `durable-18d388dba0c3da34` (M8, 433 entries re-verified).
- Implemented rows at M8: 21 (g1-leaf-copy, g1 batch A ×5, g2 ×7 hook
  rows + 3 hook-free, g5 ×5) with direction coverage per traceability.md;
  the G2 hooked rows cover all three directions except where the
  named Java gaps (below) force INCOMPLETE markers.
- Subject binary pins: rust release `pipestream-quinn` sha256
  `097829fa45d8c03eb0a5594badf0cfceababdc6d8c4898d8ce497426ec7406d7`
  (post-152c280); Java all-jar `ff537a609119534db8caca50d2828a4d421b8b0997ca04896883fff9cc60375b`
  (f582341 build; shaded-jar timestamps make cross-host hash equality
  unlikely — per-run hashes are recorded in run.tsv instead).

## 3a. Milestone 16 — R resource batch A

Gates: `cargo fmt --all -- --check` clean; `cargo clippy --all-targets -p
pipestream-conformance -- -D warnings` clean; `cargo test -p
pipestream-conformance` 86 passed / 0 failed (83 before the milestone's
last three collector tests, 72 at M15). Dev run
`durable-18d3d1d3e0f91dd0` exit 0 over `r-capability-manifest`,
`r-connection-ceiling`, `r-stalled-principal-progress` and the
`g1-leaf-copy` regression with `--java-jar`; archived under `runs/` with
MANIFEST.sha256 (320 files, all verified). Subject pins unchanged: rust
`097829fa45d8…`, java all-jar `9b23a14c36e3…`.

Collector methods (all in `conformance/src/durable/resources.rs`, still
zero new crate dependencies — `/proc`, `stat(2)` and JDK CLI tools only):

| Scope | Method | Cadence | Availability |
|---|---|---|---|
| RSS / HWM | `/proc/<pid>/status` VmRSS, VmHWM | 100 ms | both subjects |
| Threads | `/proc/<pid>/status` Threads | 100 ms | both subjects |
| FDs | `/proc/<pid>/fd` entry count | 100 ms | both subjects |
| Disk I/O | `/proc/<pid>/io` read_bytes, write_bytes | 100 ms | both subjects |
| Java heap | `jstat -gc` S0U+S1U+EU+OU | 1000 ms | java only; `heap:jstat` note per collecting sample |
| Rust heap | — | — | NAMED GAP: no black-box allocator counter |
| File lengths | `stat(2)` st_size | per checkpoint | both subjects |
| Allocated blocks | `stat(2)` st_blocks × 512 | per checkpoint | both subjects |
| Network bytes | — | — | not in batch A (`r-network-bytes`) |

The process group is the anchor server pid plus every transitive ppid
descendant; every sample line carries pid/ppid/pgrp so the discovery is
auditable. Absent scopes are `-`, never 0. A tick that cannot read a
mandatory scope writes an `error:` note (its pid columns become `-`); the
validating reader reads that line back and surfaces the note so the ROW
fails on it, and it still rejects torn final lines, wrong field counts
and non-numeric metrics.

Observed, batch A (dev evidence, never an acceptance claim):

1. Connection ceilings. rust: 4 per principal (refusal on attempt 5), 16
   global; both refusals post-authentication, APPLICATION_CLOSE 0x204
   reason `LIMIT_EXCEEDED`. java: 8 per principal (refusal on attempt 9),
   32 global; per-principal refusal post-auth APPLICATION_CLOSE 0x204
   with an empty reason, global refusal a transport close ("the server
   refused to accept a new connection"). Both match the reviewed subject
   defaults (rust `Options::default`, java `DurableOptions.defaults`).
   Capacity recovered on attempt 1 (rust) / 2 (java) after full close.
   Incomplete-handshake accounting: NAMED GAP (quinn completes handshakes
   atomically; half-open attempts are not observable black-box).
2. Stalled principal. Healthy principal bob completed
   next-sequence/declare/admit/lookup/page every round for the whole
   window with worst latency 150.5 ms (rust, 23 rounds × 5 ops) and
   151.0 ms (java, 38 rounds × 5 ops) against a 10 s deadline.
3. Stall enforcement is enforced in different KINDS and both are
   recorded per stream: rust aborts each stalled input stream
   (STOP_SENDING 0x204) and queues a per-stream LIMIT_EXCEEDED Refusal
   ("input receive deadline") on control, keeping the connection usable
   (3/3 aborted, 3/3 refused); java enforces at the connection level,
   closing the whole connection with APPLICATION_CLOSE 0x204 at its idle
   bound, after which no per-stream refusal is readable (3/3 aborted, 0/3
   refused). The row accepts either channel per stream.
4. Plateaus over the window. rust: RSS baseline median 20276 KiB → tail
   p90 21496 KiB (growth 1220 KiB); FDs 12 → 15. java: RSS 982212 KiB →
   1403300 KiB (growth 421088 KiB) — inside the stated JVM allowance
   (baseline/2 + 128 MiB = 622178 KiB) but NOT a steady-state claim: the
   heap scope shows used heap climbing from 8233 KiB to 1171499 KiB over
   152 samples with no collection returning it, so the java memory
   question is deferred to `r-memory-ladder`, which freezes limits before
   its decisive run. FDs 14 → 17.
5. Measurement correctness note. A quinn write can be accepted into a
   connection whose peer CONNECTION_CLOSE has not yet been processed by
   an idle current-thread runtime, which reads as "the stream is still
   open" when the peer closed seconds earlier. The stall probe now polls
   connection liveness before judging a write and records the result;
   without that, the java direction reported 0/3 enforcement where the
   subject had in fact closed the connection at its idle bound.
6. Java heap availability. `jstat -gc` prints decimal KiB, so the first
   collector parsed every column as an integer and recorded a false
   "jstat unavailable" gap for every JVM sample. Fixed (parsed as f64 and
   rounded); heap is now collected for the java subject and the gap text
   is reserved for a genuine absence.

Deviation recorded: the release subject binary was NOT rebuilt for this
milestone. The only uncommitted production-source delta is the two
rustfmt-mandated reformattings in `src/v2/authority/admission.rs:1` and
`scopes.rs:478` (import order and tuple-index spacing under the current
toolchain — `cargo fmt --check` fails without them), which change no
behaviour, so the pinned rust binary `097829fa45d8…` is still the build
of the committed sources.

## 3b. Milestone 17 — Java re-pin to 0176855, frozen JVM heap, resources v2 (this commit)

Gates: `cargo fmt --all -- --check` exit 0; `cargo clippy --all-targets -p
pipestream-conformance -- -D warnings` exit 0; `cargo test -p
pipestream-conformance` 88 passed / 0 failed (86 at M16; the two new tests
are the frozen-JVM-flag argument order and the byte-counter rate helper).

Subject pins for this milestone:

| Subject | Pin | sha256 |
|---|---|---|
| rust `pipestream-quinn` | committed sources of this branch | `097829fa45d8c03eb0a5594badf0cfceababdc6d8c4898d8ce497426ec7406d7` |
| java all-jar | Claude `0176855` (merged here as `9119627`) | `d658fe9e6fe923dad4c98198e6fe94e1dc92fb9aef4f084661cc8be4340cdfb6` |
| Java launch limits | frozen before any measurement row | `-Xms256m -Xmx2g` |

The M16 deviation is CLOSED: `cargo build --release` was rerun on the
committed sources (all four crates recompiled) and produced a
byte-identical `pipestream-quinn`, so the pin `097829fa45d8…` is confirmed
as the build of the committed tree rather than assumed.

Archived dev runs (both exit 0, both INCOMPLETE-labelled, each with a
MANIFEST.sha256 over every archived file):

- `durable-18d3ea398f09f12e` — R batch A (`r-capability-manifest`,
  `r-connection-ceiling`, `r-stalled-principal-progress`) plus the
  `g1-leaf-copy` regression.
- `durable-18d3ea906e9d11bd` — the full matrix (57 row-direction groups OK, 0
  FAIL). The per-direction INCOMPLETE markers are byte-for-byte the same
  set as the M15 archive `durable-18d3a1b4b749ed81`
  (g2-crash-after-create-commit ×2, g2-crash-before-create-commit,
  g2-kill-after-admission-before-publication, g2-kill-at-publication-commit,
  g3-store-ownership, g4-revocation-vs-publication,
  g8-timeout-no-completion-claim) — no regression at the new pin, and no
  new gap opened by the frozen heap or the keep-alive.

### Frozen JVM heap (r-memory-ladder prerequisite)

`java -Xms256m -Xmx2g` is applied by the fixture to every Java subject
process it spawns (`conformance/src/durable/process.rs`,
`JAVA_MEMORY_FLAGS`), i.e. before any measurement row, identically in every
direction. It is visible in `run.tsv` (`java_memory_flags`), in
r-capability-manifest's `artifacts/manifest.tsv` and `observed.tsv`
(`java_memory_freeze`) and in each R row's `observed.tsv`. The full
rationale — why freeze at all, why 2g, why not `-Xms` = `-Xmx`, and what a
change would require — is written into scenario-matrix-g6-resource.md under
r-memory-ladder. The Rust subject has no equivalent knob and is launched
unchanged; the asymmetry is stated, not equalised.

### Collector schema v2

`resources.tsv` is now `# pipestream-resources-v2` with 14 columns: the new
one is `/proc/<pid>/io cancelled_write_bytes`, placed after `write_bytes`.
All three /proc io counters are MANDATORY (same file, same permission), so
an unreadable one is an `error:` note and fails the row, never a zero; the
`-` never-0 rule is unchanged for every absent scope. The validating reader
rejects a v1 header outright rather than reading a 13-column record with v2
offsets, and the r-stalled row asserts both io scopes on every sample.
Without the new column a large `write_bytes` rate cannot be distinguished
from page-cache writeback that was cancelled before reaching the device —
which is exactly the question M16 left open on both subjects.

### Observed at the new pin (dev evidence, never an acceptance claim)

1. Named close reasons — CONFIRMED. r-connection-ceiling java-server: the
   per-owner refusal is now `APPLICATION_CLOSE code=0x204 reason="owner
   connection ceiling"` (M16: empty reason). r-stalled-principal-progress
   java-server: `APPLICATION_CLOSE code=0x204 reason="idle control
   deadline"` (M16: empty reason). Bounds are unchanged and still match the
   documented Java ceilings: 8 admitted per owner with the refusal on
   attempt 9, 32 admitted globally with the global refusal arriving as a
   transport close ("the server refused to accept a new connection"),
   capacity recovered on attempt 2 after every held connection closed.
   Rust is unchanged: 4 per principal / 16 global, both refusals post-auth
   `APPLICATION_CLOSE 0x204 reason="LIMIT_EXCEEDED"`, recovery on attempt 1.
2. Per-stream stall enforcement — NOT OBSERVED on the Java server. The
   expectation at this pin was a per-stream `LIMIT_EXCEEDED` Refusal with
   detail "input receive deadline" on a connection that survives. What the
   row records is the whole connection closed at the idle bound before the
   first enforcement probe (idle+10 s), with the new named reason, and
   0/3 per-stream refusals readable afterwards. The rust server behaves as
   at M16: 3/3 stalled inputs aborted individually (`STOP_SENDING 0x204`)
   by idle+10 s AND 3/3 `LIMIT_EXCEEDED` refusals with detail "input
   receive deadline" drained from a still-live control stream at window
   end. NAMED GAP, not a defect claim: a QUIC APPLICATION_CLOSE discards
   whatever the peer had queued and the client had not yet read, so this
   row cannot distinguish "the Java server queued no per-stream refusal"
   from "it queued them and its own connection close discarded them". The
   observation and that ambiguity are the M17 question to Claude.
3. Java idle write rate — CONFIRMED and larger than predicted. Over the
   r-stalled window the Java server group's anchor pid moved `write_bytes`
   by 8,187,904 B in 152,237 ms = 53.8 KB/s (M16: ~4.3 MB/s), with
   `cancelled_write_bytes` unchanged at 0 over the whole window. The
   predicted drop was ~2.5 MB/s; the measured drop is ~4.25 MB/s, and what
   remains is real accounted traffic rather than cancelled writeback.
4. Rust idle write rate — UNCHANGED and almost entirely cancelled. Same
   row, rust direction: `write_bytes` 1,068,654,592 B in 92,750 ms =
   11.52 MB/s, of which `cancelled_write_bytes` is 1,057,660,928 B =
   11.40 MB/s, i.e. 98–99% cancelled before writeback. That is the same
   signature Claude diagnosed on the Java host at M16 (>99.9% cancelled,
   a WAL index torn down and rebuilt per store call). This is an
   observation about the Rust authority, which is Meta's component; it is
   raised as a question on the board, not asserted here as a defect.
5. Frozen heap changes the Java memory picture completely. With
   `-Xms256m -Xmx2g` the Java server's RSS plateau over the same row is
   baseline median 348,084 KiB → tail p90 367,760 KiB (growth 19,676 KiB),
   against M16's 982,212 → 1,403,300 KiB (growth 421,088 KiB); used heap
   over 152 jstat samples is 8,396 → max 161,068 KiB against M16's max
   1,171,499 KiB. The M16 "heap climbing with nothing returning it" was an
   artefact of an effectively unbounded default max heap on a 121 GiB host
   — uncollected garbage, not retention. FDs 20 → 21. Rust in the same
   run: RSS 19,916 → 21,152 KiB (growth 1,236), FDs 12 → 15.
6. Healthy-principal progress holds on both subjects: bob completed
   next-sequence/declare/admit/lookup/page every round with worst latency
   150.5 ms (rust, 23 rounds × 5 ops) and 150.7 ms (java, 38 rounds × 5
   ops) against the 10 s deadline.
7. Heap scope: java collected on every heap tick (0 probe gaps); rust heap
   remains a NAMED GAP with no black-box allocator counter, and RSS/HWM is
   never substituted for either heap.

### Fixture measurement defects found and fixed this milestone

Both were found because the re-pin changed the Java server's behaviour and
the row's evidence stopped making sense; both are recorded rather than
quietly patched.

1. The abusive principal's connection died of the FIXTURE's own transport
   idle timeout. The row leaves that connection silent for 90 s between the
   idle-bound and lifetime-bound probes, which is longer than the quinn
   default idle timeout. At M16 this was invisible because the Java server
   closed the connection itself first. At 0176855 it produced
   "aborted (connection lost: timed out)" for 3/3 streams — evidence that
   looks like subject enforcement and is not. Fixed two ways: the stall
   row's peer now sends QUIC PINGs every 5 s with an explicit 60 s idle
   timeout (`Peer::with_keep_alive`; PINGs are transport traffic and carry
   no object-stream data, so they must not renew an application receive
   deadline — a subject that treats them as activity would itself be a
   finding), and a probe that finds the connection gone with a transport
   idle timeout now records its stream aborts as NOT attributable and does
   not count them towards enforcement.
2. Keeping that connection alive to the end of the window then exposed a
   fixture race at shutdown: SIGTERM landed while the subject still had a
   just-closed connection on its books, and `OwnedServer::stop` failed its
   drain assertion intermittently (`transport_idle: false` with every other
   scope idle — the rust authority's 5 s shutdown grace can be consumed by
   the execution-pool wind-down before the transport wait starts). The row
   now closes the peer, waits (bounded) for its own endpoint to go idle,
   takes every measurement, stops the collector, and only then waits a
   fixed 30 s settle before signalling the subject. That window is fixture
   timing, never evidence: no sample is taken during it.

### Deviations recorded

- The Java all-jar was COPIED from Claude's worktree (read-only for peers,
  by his instruction) rather than rebuilt here, so the archived
  `java_sha256` is byte-for-byte the artifact he published at 0176855;
  a local rebuild would have produced a different shaded-jar hash for the
  same sources. The sources themselves ARE in this branch: 0176855 is
  merged at `9119627` (Java and Java-doc files only, no
  conflicts, nothing Kimi-owned touched), so the pin is an ancestor of this
  commit and the copied jar is not an unexplained binary.
- Four earlier M17 archives were produced and deleted before commit: the
  first recorded the non-attributable transport-timeout evidence described
  above, and three more failed the drain assertion while that fix was being
  sized. Only green archives are kept; each deleted directory was confirmed
  untracked (`??`) first.
- `STALL_CLOSE_SETTLE` is 30 s. It was sized from an observed intermittent
  failure at 5 s, not from a specification.

## 4. Interface and peer-review artifacts

- interface-v1.md: event + schedule schemas; acknowledged by Claude
  (1452f60, hash independently recomputed) and Meta (schedule schema).
- rust-hook-proposal.md + addendum: placement agreed with Claude
  (his peer-review-kimi-rust-hooks.md); M5 implements the amended
  contract.
- peer-review-claude-java-hooks.md: 4 defects + 2 gap groups; Claude
  accepted, fixes landing in his FixtureMain checkpoint.
- peer-review-meta-contract-c1.md: no blocking mismatch; Meta's answers
  accepted (reviews/kimi-interface-v1.md).

## 5. Known limitations / open gates

1. Acceptance mode has never passed: the full matrix, both directions,
   resource gates and run_all.sh integration are unfinished.
2. Java-server hook directions are live for 5 G2 rows (M7), but
   `g2-crash-after-create-commit` rust-client/java-server stays
   INCOMPLETE: Java FixtureMain re-fires drop-reply on the REPLAYED
   commit (no fresh-commit gating) — subject-side fix reported to Claude
   with archived transcript.
3. Java findings reported to Claude (all with archived reproducers):
   CLI ignores `--max-execution-ms`; client graceful shutdown hangs
   after a server kill; watch output lacks the deadline field; select
   on an empty manifest throws client-side FRAME_ERROR instead of
   surfacing wire NOT_FOUND; `--entities` required blocks empty-batch
   declaration cases; pause release-file naming
   (`release-<target>-<boundary>`) differs from interface-v1
   (`release-<boundary>`) — reconciliation proposed.
4. g7-unsafe-clock-refusal needs a subject fixture clock (proposal
   pending); host UTC is never used.
5. require-durable and wire-level cross-owner paths are unreachable via
   the published CLIs (recorded findings, M4) — final certification of
   those arms needs either CLI surface or documented implementation-test
   mapping. Rust authority capacity bounds make the 256/batch schema
   bound wire-unreachable (single-tx cap binds first); 257-batch is
   preempted by clap arity — both named gaps.
6. CLOSED at M15, re-pinned at M17: the all-jar is built on the
   pipestream.4 transport fix and the subject is now Claude `0176855`
   (all-jar `d658fe9e…`, merged here at `9119627`); every row in §3b ran
   against it. OPEN at M17: the Java server's stall enforcement is still
   observed as a connection-level close at the idle bound (now with the
   named reason "idle control deadline") and no per-stream
   `LIMIT_EXCEEDED` "input receive deadline" Refusal is readable, contrary
   to the behaviour expected at this pin — with the named ambiguity that
   an APPLICATION_CLOSE discards unread queued control frames, so the row
   cannot prove none were sent.
7. Client-side commit boundaries are driver-side observations only;
   uncontrolled client-death rows are labelled as such.
8. g2-drop-reply-publication is registered but unimplemented: neither
   subject exposes a PUBLICATION reply pair to withhold (publication is
   observed via watch, not a correlated reply); the kill-at-boundary
   variant is the delivered evidence.

## 6. Safe next action

Finish group R: `r-memory-ladder` — its prerequisite is now met, the
limits and the environment allowance are frozen and recorded (`-Xms256m
-Xmx2g`, rationale in scenario-matrix-g6-resource.md), and the M16 java
heap-growth question is answered in §3b item 5 (it was an unbounded
default heap, not retention), so the row can be built on ladders rather
than on that doubt. Then `r-staging-and-journal-bounds`, `r-network-bytes`
(needs a network-bytes collector — not built in batch A) and
`r-native-credit`. Then acceptance-mode integration into
conformance/run_all.sh and the final full-matrix run.
