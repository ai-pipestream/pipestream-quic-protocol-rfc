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
  recently `7585a9d` at `c5b4f30` (M17b re-pin; `0176855` at `9119627`
  was the M17 re-pin).
- Base after M18a: the branch was fast-forwarded once to `85887911` (the
  coordinating owner's combined review commit, which already contains
  `73766f6a`); this was a pure `--ff-only` merge of `main`, nothing was
  merged INTO main or the feature branch, and no push or rebase was made.
- Dirty state: none at M17b; the fmt-only diffs in
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
| add98fd | M17: Java subject re-pinned to `0176855`; JVM heap frozen before measurement; resources schema v2 (`cancelled_write_bytes`); R batch A + full matrix rerun |
| 73766f6a | M17b: Java subject re-pinned to `7585a9d`; R batch A + full matrix rerun; the Java stall enforcement kind is now per-stream and matches the Rust reference |
| 7544b134 | M18a: `r-memory-ladder` implemented against both subjects (payload and retained-inventory ladders, over-limit rung, group-window statistics); R row ids reconciled with the canonical matrix names |
| 9deed6a0 | M18b: `r-staging-and-journal-bounds` implemented against both subjects (PARTIAL: journal/retained-byte ceilings recorded, not driven); the raw peer's tokio runtime is now driven continuously, which withdraws the M17b stalled-abort bracket as a client artefact |
| 0d5dded6 | M18c: `r-stalled-principal-progress` re-run with non-writing probes on the driven client — both subjects enforce at their own negotiated idle bound inside a two-second bracket; every R row plus `g1-leaf-copy` re-run as the group regression |
| 1d3569ee | M18d: `r-network-bytes` implemented against both subjects (PARTIAL: no network namespace and no packet capture on this host, both recorded by the checks that failed); new network scope with its own schema and validating reader |
| 54d93487 | M18e: `r-native-credit` implemented against both subjects (PARTIAL: no packet capture, and one endpoint's view only); group R is now four DONE and three PARTIAL, with every row implemented |
| 2a9aad5d / b849d8c5 | M19a (work in Kimi's role): attempt-2 hold at EXECUTION_CLAIMED for g4-stale-attempt-retry on the Java server, release file under both subject spellings, work-view parser reads the Java record form; affected rows rerun, all directions green (durable-18d4aacfca57bb0b) |
| 69b197aa | M19b (work in Kimi's role): the ten remaining matrix rows; seven green on both servers (g2-not-found-in-flight, g5-cert-rotation-same-owner, g5-remapped-owner, g5-cross-authority-reference, g7-read-pin-past-expiry, g7-deadline-queue-time, g5-expired-identity), three INCOMPLETE with the named missing capability; archives durable-18d4ab23bb15105e and durable-18d4abe3eae3a613 |
| 03ce9a44 / this commit | M19c (work in Kimi's role): full-matrix rerun durable-18d4ac9f7d4e9880 (67 rows, 64 OK, 3 missing-capability INCOMPLETE) compared line by line with M17b durable-18d3ed6c2f040515; changed-policy probe flag fix and its supplementary rerun durable-18d4af67e48effbd; acceptance-mode proposal for run_all.sh |

## 3. Verification evidence (M8 snapshot; superseded by §3c for the
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

## 3b. Milestone 17 — Java re-pin to 0176855, frozen JVM heap, resources v2 (add98fd)

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

## 3c. Milestone 17b — Java re-pin to 7585a9dc, matrix rerun (this commit)

No fixture, scenario or collector source changed this milestone: the only
content changes are the merge of the Java subject commit, the re-pinned jar
and these documents. Gates were rerun on that tree anyway: `cargo fmt --all
-- --check` exit 0; `cargo clippy --all-targets -p pipestream-conformance --
-D warnings` exit 0; `cargo test -p pipestream-conformance` exit 0, 88
passed / 0 failed (same 88 as M17). `cargo build --release` was rerun and
had nothing to do (0.05 s, exit 0); both release binaries are byte-identical
to the M17 pins.

Subject pins for this milestone:

| Subject | Pin | sha256 |
|---|---|---|
| rust `pipestream-quinn` | committed sources of this branch (unchanged since M16) | `097829fa45d8c03eb0a5594badf0cfceababdc6d8c4898d8ce497426ec7406d7` |
| rust `pipestream-conformance` (the driver itself) | committed sources of this branch | `4616ef1116163931bd046e831021c23aca672d816d16de6f0fd51f549197174c` |
| java all-jar | Claude `7585a9dc` (merged here as `c5b4f30`) | `61ab64a312908dad40f458bf32ce8d8dadcb8a13a0aea489e44dd87e918a458a` |
| Java launch limits | unchanged, frozen before any measurement row | `-Xms256m -Xmx2g` |

Archived dev runs (both exit 0, both INCOMPLETE-labelled, each with a
MANIFEST.sha256 over every archived file, each verified after archiving):

- `durable-18d3ed1531daad17` — R batch A (`r-capability-manifest`,
  `r-connection-ceiling`, `r-stalled-principal-progress`) plus the
  `g1-leaf-copy` regression; 318/318 manifest entries verified.
- `durable-18d3ed6c2f040515` — the full matrix, 53 row-direction groups OK
  and 0 FAIL; 4893/4893 manifest entries verified. The per-direction
  INCOMPLETE markers are byte-for-byte the same set as the M17 archive
  `durable-18d3ea906e9d11bd` and the M15 archive `durable-18d3a1b4b749ed81`
  (g2-crash-after-create-commit ×2, g2-crash-before-create-commit,
  g2-kill-after-admission-before-publication, g2-kill-at-publication-commit,
  g3-store-ownership, g4-revocation-vs-publication,
  g8-timeout-no-completion-claim) — no regression at the new pin and no new
  gap opened by the listener change.

No run directory was deleted this milestone: both runs were green on the
first attempt and both are kept.

### Observed at the new pin (dev evidence, never an acceptance claim)

1. Stall enforcement kind — the M17 question is ANSWERED and the gap is
   CLOSED. `r-stalled-principal-progress` rust-client/java-server now
   records the connection LIVE at both enforcement probes (idle+10 s and
   lifetime+10 s) and, at window end, 3/3 per-stream `LIMIT_EXCEEDED`
   (code 4) Refusals with detail `input receive deadline`, one per stalled
   input tag (6, 10, 14), drained from a still-open control stream. There
   is no APPLICATION_CLOSE on this row at all: M17's `0x204 reason="idle
   control deadline"` at the idle bound is gone, which is exactly the
   behaviour change `7585a9dc` describes (a durable-profile connection is
   no longer closed for control silence; a core-only connection with
   nothing outstanding still is). The two subjects now enforce in the same
   KIND — per stalled stream, on a surviving connection — and the M17 named
   gap ("an APPLICATION_CLOSE discards queued control frames, so the row
   cannot tell 'none sent' from 'sent and discarded'") no longer applies to
   this row, because nothing is discarded.
2. WHEN the Java per-stream abort lands moved with it, and this is new
   information rather than a regression. At M17 the java direction showed
   3/3 streams "aborted" by idle+10 s, but that was the connection close
   taking every stream down with it. With the connection kept, the streams
   survive idle+10 s (0/3 aborted, all three `still-open-at-idle-bound+10s`)
   and are aborted per stream by lifetime+10 s (3/3, `STOP_SENDING 0x204`,
   distinct streams), with the refusals readable afterwards. So the Java
   input receive deadline fires somewhere between the negotiated idle bound
   +10 s (40 s) and the lifetime bound +10 s (130 s); this row brackets it
   and does not claim a value inside the bracket. Rust is unchanged: 3/3
   aborted by idle+10 s (negotiated idle 5 s) and 3/3 refused, connection
   live throughout.
3. `r-connection-ceiling` bounds are unchanged on both subjects: rust 4 per
   principal (refusal on attempt 5) / 16 global, both refusals post-auth
   `APPLICATION_CLOSE 0x204 reason="LIMIT_EXCEEDED"`, capacity recovered on
   attempt 1; java 8 per principal (refusal on attempt 9) / 32 global,
   per-owner refusal post-auth `APPLICATION_CLOSE 0x204 reason="owner
   connection ceiling"`, capacity recovered on attempt 2. The java GLOBAL
   refusal is again a transport-level refusal with the same peer text ("the
   server refused to accept a new connection"), but this run classified it
   as `pre-auth transport refusal (connect failed): open control stream`
   where M17 classified the same text as a post-auth close. That is a race
   in the client between the handshake future completing and the peer's
   abort arriving, not a change of bound or of refusal channel; both
   classifications are recorded verbatim and neither is asserted as the
   subject's contract. Incomplete-handshake accounting remains a NAMED GAP.
4. Healthy-principal progress is unchanged and still well inside the 10 s
   deadline: bob completed next-sequence/declare/admit/lookup/page every
   round, worst latency 150.528 ms (rust, 23 rounds × 5 ops) and 150.648 ms
   (java, 38 rounds × 5 ops), against 150.534 ms / 150.677 ms at M17.
5. Disk I/O over the stall window (anchor pid, both io counters MANDATORY
   and collected on every sample). rust: `write_bytes` 1,075,953,664 B in
   92,749 ms = 11.60 MB/s, of which `cancelled_write_bytes` 1,064,800,256 B
   = 11.48 MB/s (98% cancelled before writeback) — unchanged from M17's
   11.52 / 11.40 MB/s, and still the open question to Meta about the Rust
   authority's storage layer. java: `write_bytes` 8,597,504 B in 152,840 ms
   = 56.3 KB/s (M17: 53.8 KB/s) with `cancelled_write_bytes` 405,504 B =
   2.7 KB/s, i.e. 4% cancelled where M17 measured 0 over the window. Both
   java figures are small absolute numbers on a longer window; the row
   asserts no bound on either and records them so an idle write rate can be
   told apart from cancelled writeback.
6. Memory and FDs at the frozen heap, same window. java: RSS baseline
   median 349,296 KiB → tail p90 371,752 KiB (growth 22,456; M17: 348,084 →
   367,760, growth 19,676), FDs 20 → 21, used heap over 152 jstat samples
   min 9,370 KiB, max 160,895 KiB, 0 probe gaps (M17: min 8,396, max
   161,068, 0 gaps). rust: RSS 19,684 → 20,844 KiB (growth 1,160; M17:
   19,916 → 21,152, growth 1,236), FDs 12 → 15. The plateau assertions pass
   on both; keeping the connection open for the whole window cost the JVM
   about 4 MiB more tail RSS and nothing measurable in heap.
7. Heap scope unchanged: java collected on every heap tick (0 gaps); the
   rust heap remains a NAMED GAP with no black-box allocator counter, and
   RSS/HWM is never substituted for either.

### Deviations recorded

- The Java all-jar was again COPIED from Claude's worktree (read-only for
  peers, by his instruction) rather than rebuilt here, so the archived
  `java_sha256` is byte-for-byte the artifact he published at `7585a9dc`;
  a local rebuild would produce a different shaded-jar hash for the same
  sources. The sources ARE in this branch: `7585a9dc` is merged at
  `c5b4f30` (three Java/Java-doc files, no conflicts, nothing Kimi-owned
  touched), so the pin is an ancestor of this commit.
- The `STALL_CLOSE_SETTLE` 30 s and the stall keep-alive introduced at M17
  are unchanged and still fixture timing, never evidence.

## 3d. Milestone 18a — r-memory-ladder

First of the four remaining R rows. `r-memory-ladder` is implemented against
both subjects and green in dev; the three placeholder row ids left over from
milestone 16 are retired and the registry now carries exactly the canonical
matrix names.

Gates on this tree: `cargo fmt --all -- --check` exit 0; `cargo clippy
--all-targets -p pipestream-conformance -- -D warnings` exit 0; `cargo test -p
pipestream-conformance` exit 0, 90 passed / 0 failed (88 at M17b; the two new
tests cover the group-window statistics and the nearest-rank percentile);
`cargo build --release` exit 0.

Subject pins for this milestone:

| Subject | Pin | sha256 |
|---|---|---|
| rust `pipestream-quinn` | committed sources of this branch (unchanged since M16) | `097829fa45d8c03eb0a5594badf0cfceababdc6d8c4898d8ce497426ec7406d7` |
| rust `pipestream-conformance` (the driver) | committed sources at this milestone | `733fa7b899157924794f71377e61646af6616a3f9ca7f7ca7d199cf61b0eb1b8` |
| java all-jar | Claude `7585a9dc` (merged here as `c5b4f30`) | `61ab64a312908dad40f458bf32ce8d8dadcb8a13a0aea489e44dd87e918a458a` |
| Java launch limits | unchanged, frozen before any measurement row | `-Xms256m -Xmx2g` |

The Java jar is the byte-identical artifact copied from Claude's read-only
tree and hash-verified before the run (`61ab64a3…`); it is never rebuilt
here. The rust subject binary was rebuilt from the committed sources and came
out byte-identical to the M16/M17/M17b pin, so no reference-implementation
source changed with this milestone. The driver binary hash moves, because the
driver is what this milestone changes.

Archived dev run (exit 0, INCOMPLETE-labelled, MANIFEST.sha256 over every
archived file, verified after archiving):

- `durable-18d428da0717c79d` — `r-memory-ladder` (both directions) plus the
  `g1-leaf-copy` regression; 421/421 manifest entries verified.

No archived run directory was deleted. Three pre-archive development runs
under `target/durable-runs` (untracked, never archived) found three real
fixture defects before the decisive run and are described in the deviations
below.

### Row design and what is frozen

Detail is in scenario-matrix-g6-resource.md under "Group R status (milestone
18a)". In short: two ladders over one raw-peer connection per direction —
payload 64 KiB / 1 MiB / 16 MiB with four admissions per rung, and inventory
1 / 16 / 64 cumulative resident works at a fixed 64 KiB payload — plus an
over-limit rung that declares 64 MiB against a 16 MiB `object_limit` and
records the refusal. `expected.tsv` is written after negotiation and BEFORE
the first rung's traffic; the plateau allowances in it are computed from the
limits the SUBJECT declared on the wire (`stream_limit x object_limit` for
the payload ladder, `pending_limit x control_limit` for the inventory ladder)
plus a stated per-subject slack, and are never widened afterwards. The JVM
heap ceiling is the milestone-17 freeze, unchanged.

Every rung figure is a per-tick SUM over the whole sampled process group
before it is a statistic, measured over the last 6 s of a 12 s quiet settle.

### Observed (dev evidence, never an acceptance claim)

1. Memory does not scale with PAYLOAD on either subject. 256x payload
   (65,536 -> 16,777,216 B per admission) moved the group tail p90 RSS by
   1,568 KiB on rust (18,924 -> 20,492) and 25,008 KiB on java (341,944 ->
   366,952), against frozen allowances of 131,072 and 524,288 KiB. Buffering
   a single largest payload once would already cost 16,320 KiB.
2. Memory does not scale with RETAINED INVENTORY. 1 -> 64 resident works at a
   fixed payload moved tail p90 RSS by 1,532 KiB on rust and by nothing
   measurable on java (367,532 -> 341,948; recorded as growth 0, never as a
   negative figure), against allowances of 66,560 and 278,528 KiB.
3. The two subjects refuse the over-limit rung with the same code and
   different text, both recorded verbatim: rust `LIMIT_EXCEEDED (4) "input
   exceeds retained duration, bytes or response limits"`, java
   `LIMIT_EXCEEDED (4) "input exceeds negotiated object limit"`, both tagged
   to the input stream.
4. The Java LISTENER's declared selection is not `DurableOptions.defaults()`:
   it offers `pending_limit=32` and `stream_lifetime_ms=120000` where the
   library defaults are 64 and 300000. The row reads the selection off the
   wire and sizes its allowances from that, which is why it never quotes a
   documented default as a subject bound.
5. Handles and threads stay bounded across the ladder (rust FDs 12 -> 15,
   threads 49 -> 50; java FDs 18 -> 22, threads 32 -> 44) and the Java heap
   is collected on every heap tick inside the rung windows (42 ticks, 0
   gaps, 10,637 -> 156,058 KiB against the frozen 2 GiB ceiling). Collector
   health: 1,014/1,014/0 error lines (rust), 997/997/0 (java).
6. JAVA NATIVE/DIRECT IS A NAMED GAP ON THIS HOST, with the exact failing
   check archived: `jcmd <pid> VM.native_memory summary` answers "Native
   memory tracking is not enabled" (transcript in
   `artifacts/jcmd-native.txt`, alongside `jcmd VM.flags` confirming
   `MaxHeapSize=2147483648` / `InitialHeapSize=268435456` in the live
   subject). Enabling NMT means changing the frozen launch flags mid-matrix
   and adding the collector's own overhead to the measurement it serves; it
   is recorded as unavailable rather than substituted by RSS minus heap. A
   future milestone may re-freeze WITH NMT and rerun every R row.
7. The Rust heap scope remains a NAMED GAP (no black-box allocator counter);
   RSS/HWM is a separate scope and is never reported as either heap.

### Fixture defects found and fixed before the decisive run

All three were found by development runs that were never archived, and all
three are recorded rather than quietly patched.

1. The over-limit rung was refused `CONFLICT` "input membership was not
   declared" instead of `LIMIT_EXCEEDED`, because both subjects check scope
   membership before the declared length. The row now declares the
   over-limit entity like any other, so the refusal it records is the
   object-limit decision it exists to observe.
2. An unpaced burst of 48 admissions in the top inventory rung was refused by
   the Java subject with `LIMIT_EXCEEDED` "retained input, output or executor
   capacity" — a concurrent-job ceiling, not a memory result. Admissions are
   now paced in batches of two and each batch settled before the next, so
   these ladders vary payload and retained inventory and never executor
   concurrency. The ceilings themselves belong to
   `r-staging-and-journal-bounds`.
3. Signalling the subject to stop while 76 works were still winding down made
   `OwnedServer::stop` fail its drain assertion with `transport_idle: false`
   and every other scope idle: the rust authority's fixed 5 s shutdown grace
   was consumed by the execution-pool wind-down before its transport wait
   started. This is the same mechanism recorded at M17 for the stall row. The
   row now waits (bounded, 120 s) for every admitted work to be terminal
   before it signals anything; no measurement is taken during that wait and
   the rung windows have already closed.

### Deviations recorded

- The Java all-jar was again COPIED from Claude's read-only tree rather than
  rebuilt here, so the archived `java_sha256` is byte-for-byte the artifact
  he published at `7585a9dc`; the sources are in this branch at `c5b4f30`.
- The 64 MiB rung of the matrix's suggested payload ladder is NOT measured.
  Both subjects declare `object_limit` = 16 MiB, so a 64 MiB object never
  becomes resident and measuring it would measure a refusal; it is probed
  once as the over-limit rung and its refusal recorded instead. This is the
  matrix's own "select feasible sizes with explicit ceilings" case and the
  ceiling is named in `expected.tsv`.
- The "concurrent works" wording of the matrix's inventory ladder is realised
  as RETAINED resident works, for the concurrency reason in fixture defect 2
  above. The row says so in `expected.tsv` rather than implying it held 64
  works in flight.

## 3e. Milestone 18b — r-staging-and-journal-bounds (PARTIAL) and a raw-client measurement fix

Second of the four remaining R rows, plus a driver-side measurement fix that
applies to every raw row.

Gates on this tree: `cargo fmt --all -- --check` exit 0; `cargo clippy
--all-targets -p pipestream-conformance -- -D warnings` exit 0; `cargo test -p
pipestream-conformance` exit 0, 90 passed / 0 failed (unchanged from M18a);
`cargo build --release` exit 0.

Subject pins are unchanged from M18a — rust `pipestream-quinn`
`097829fa45d8…` (rebuilt from committed sources, byte-identical), java
all-jar `61ab64a312908dad…` copied from Claude's read-only tree and
hash-verified, JVM launch limits `-Xms256m -Xmx2g` still frozen. The driver
binary at this milestone is `bd2a6ae050dea92b5d31a03ad9d883a7d17c98b5e45d0a021f3612c95d04e786`.

Archived dev runs — ALL THREE ARE KEPT, including the two that are not the
decisive one, and each MANIFEST.sha256 verifies after archiving:

- `durable-18d42cb15342e2b8` — DECISIVE: `r-staging-and-journal-bounds`
  (both directions) plus the `g1-leaf-copy` regression, exit 0, 142/142
  manifest entries verified.
- `durable-18d42c12afac14aa` — FAILED and recorded as failed: the row's own
  session attach was refused `LIMIT_EXCEEDED "metadata concurrency
  exhausted"` because the connection it needed was attached after the
  pending phase had already filled the subject's control capacity. 101/101
  manifest entries verified.
- `durable-18d42bb4e9e2c860` — SUPERSEDED: the row reported SCENARIO OK, but
  its rust reconciliation evidence was wrong (the probe read its own
  `"input receive deadline"` refusal as a capacity refusal, and every later
  attempt then hit an immutable-intent CONFLICT from a re-used operation
  id). It is kept because it is the evidence for fixture defects 5 and 6
  below, and because the row was not asserting reconciliation at all at that
  point — which is itself the finding that produced the assertion.
  138/138 manifest entries verified.

### The row

Design and full observations are in scenario-matrix-g6-resource.md under
"Group R status (milestone 18b)". Status PARTIAL, with the reason carried in
the row's own `observed.tsv` (`row_status`): the pending-control-work and
staging-object ceilings are driven to exhaustion and every property the
matrix asks for is asserted around them — a named refusal for NEW work with
the attempt number it arrived on, existing promises still completing, file
handles returning to baseline, capacity still charged while the transfers
are held, and capacity reconciling after safe cleanup and after a restart on
the same roots — but the JOURNAL and RETAINED-BYTE ceilings are recorded from
the subject's declarations and sampled per file at six checkpoints rather
than driven to exhaustion, because driving them needs hundreds of megabytes
of committed records.

The driver now parses the session binding receipt's retained-record limits
(`scopes`, `entities`, `operations`, `input_bytes`, `output_bytes`,
`active_jobs`), so the ceilings the row quotes come from the subject on the
wire rather than from a library default.

Headline numbers: rust refuses the 17th pending control request
(`"connection pending limit"`, declared `pending_limit` 16) and the 5th
concurrent incomplete transfer of one owner (`"input transfer capacity
exhausted"`, documented `active_per_owner` 4); java refuses the 33rd
(`"request refused"`, declared `pending_limit` 32) and the 129th
(`"input handle capacity exhausted"`, documented 128 handles). 16/16 and
32/32 granted waits were answered afterwards. FDs: rust 12 → 17 held → 14
released; java 18 → 150 held → 22 released.

### Client-side observation defect fixed — affects every raw row

The coordinating owner supplied a timestamped reproduction (Java DIAG build
plus a quinn trace subscriber on a copy of the driver) showing that the Java
subject refuses each stalled input at idle+0.1 s
(`sinceProgressMs=30094`), retransmits its STOP_SENDING 0x204 for about a
minute, and that every "got frame StopSending" line in the client's own
trace lands within one millisecond of the others. Root cause is in the
conformance crate: `rawclient::Peer` built a `new_current_thread` tokio
runtime, so quinn's endpoint driver — the task that reads inbound packets
and applies their frames — only progressed while a `block_on` was pending on
the scenario thread. A row that slept between probes, or whose probe
returned immediately out of local send credit, left inbound packets
unprocessed until the next call that actually waited, and then reported the
time of its own probe as the time of the subject's action.

Fix: `Peer` now builds a two-worker multi-threaded runtime, so the driver
runs while the scenario thread sleeps. Only the conformance crate changed.
The tokio dependency gained the `rt-multi-thread` FEATURE of a crate it
already depends on — no new crate — and `Cargo.lock` is byte-identical
before and after, as is the rust subject binary.

Consequence for the record: the 40–130 s bracket that
`r-stalled-principal-progress` reported for the Java input receive deadline
at M17b was an OBSERVATION ARTEFACT OF THIS DRIVER, not the subject's
timing. The bracket is WITHDRAWN rather than restated. That row is not
re-run in this milestone; re-running it properly needs the second half of
the fix as well — probes that poll the send stream's stopped state instead
of writing payload bytes (which are progress and renew the Java deadline),
with intermediate probes at idle+2 s, +5 s and +10 s and first-observation
brackets recorded. Kimi's M17b request (2) to Claude is ANSWERED and closed
by his finding; what remains open is this driver-side re-run, which is
Kimi's own work and not a question to Claude.

### Fixture defects found and fixed before the decisive run

1. A filler watch waiting past a revision that could never arrive can only
   be answered by its own deadline, which proves nothing about a promise
   being kept; the watches now wait past the DECLARATION revision so the
   admission answers them.
2. `WaitMs` is bounded at 30 s by the protocol — a 60 s wait is a
   FRAME_ERROR, not a longer wait.
3. Reading one control frame after a burst of attempts attributes an early
   refusal to the last attempt. Refusals carry the request or input-stream
   tag they belong to, and the row now matches on that tag and asserts the
   match before recording an attempt number.
4. A per-attempt control wait made the staging sweep outlast the Java
   subject's own input receive deadline, which reaped the earliest transfers
   while later ones were still being opened; batched opens with one bounded
   drain per batch keep the sweep inside the deadline.
5. A probe holding an incomplete transfer open for longer than the subject's
   object idle bound reads the subject's `"input receive deadline"` refusal
   as a capacity refusal; probe waits are now 2 s, below the smaller of the
   two negotiated idle bounds (rust 5 s).
6. Each recovery attempt declares a new entity and therefore needs a new
   operation id; re-using one is an immutable-intent CONFLICT that masks the
   capacity answer entirely.
7. Reconciliation after cleanup and after restart was recorded but not
   ASSERTED, so a run whose reconciliation evidence was wrong still reported
   SCENARIO OK. Both are now assertions, conditional on the ceiling having
   actually been reached.

### Deviations recorded

- Row status PARTIAL: the journal/retained-BYTE ceilings are recorded, not
  driven. Named above and in the row's `observed.tsv`.
- The java-direction sweep reaches the 128-handle ceiling only by walking to
  a second principal, because the per-owner CONNECTION ceiling (8) turns the
  ninth alice connection away first. That turn-away is logged as
  `setup-refused` with the peer's text and is not counted as a staging
  refusal.
- `r-stalled-principal-progress` is NOT re-run at this milestone even though
  the runtime fix changes what it would observe. Its M17b numbers therefore
  stand in §3c as recorded, with the bracket withdrawn here; the row is
  re-run before any acceptance claim.

## 3f. Milestone 18c — the stalled-principal re-run on a driven client

The measurement fix landed at M18b changed what the raw peer can observe, so
`r-stalled-principal-progress` is re-run here with the second half of that
fix — non-writing enforcement probes — and every other R row is re-run
alongside it, which makes this milestone's archive the group's regression
evidence as well.

Gates on this tree: `cargo fmt --all -- --check` exit 0; `cargo clippy
--all-targets -p pipestream-conformance -- -D warnings` exit 0; `cargo test -p
pipestream-conformance` exit 0, 90 passed / 0 failed; `cargo build --release`
exit 0, reproducing the unchanged rust subject binary `097829fa45d8…`.

### What changed in the row

1. The enforcement probes no longer WRITE. The earlier probe wrote ten
   one-byte payloads per stalled stream per probe; on a subject whose input
   receive deadline is measured from the last payload byte (the Java
   server's `DurableServer.InputTransfer.lastProgress`) those bytes are
   progress and renew the deadline the probe exists to observe. The probe
   now polls quinn's own `stopped()` future with a 250 ms bounded wait and
   sends nothing.
2. Six probe marks instead of two, one of them BELOW the bound: idle-2 s,
   idle+0 s, idle+2 s, idle+5 s, idle+10 s, lifetime+10 s. Without a probe
   that sees a stream open there is no lower end to a bracket.
3. Probes run inside the round's idle time in 200 ms slices rather than once
   per four-second round, which is affordable only because they are
   non-writing.
4. The row records, per stream, the last probe that saw it open and the
   first that saw it stopped, and claims nothing inside that bracket.

### Archived dev run

`durable-18d42e1bffb6a51b` — `r-capability-manifest`, `r-connection-ceiling`,
`r-stalled-principal-progress`, `r-memory-ladder`,
`r-staging-and-journal-bounds` and the `g1-leaf-copy` regression, all exit 0,
INCOMPLETE-labelled, 731/731 manifest entries verified after archiving. This
single archive is both the milestone's evidence and group R's
no-regression evidence at the fixed runtime, which is the "batch A plus
g1-leaf-copy once at the end" rerun the assignment asks for.

Subject pins unchanged: rust `pipestream-quinn` `097829fa45d8…` (rebuilt
byte-identical), java all-jar `61ab64a312908dad…` copied and hash-verified,
JVM limits `-Xms256m -Xmx2g` still frozen. Driver binary for this milestone:
`d7b2eb76ae6e51c7ba52b8161b982dadf53dd2c8229dc7a2f1002773adfc2377`.

### Observed (dev evidence, never an acceptance claim)

1. BOTH SUBJECTS ENFORCE AT THEIR OWN NEGOTIATED IDLE BOUND, and the bracket
   is about two seconds wide instead of ninety. java (negotiated idle 30 s):
   all three stalled inputs OPEN at +28.000 s and all three STOPPED by
   +30.156 s. rust (negotiated idle 5 s): all three OPEN at +3.102 s and all
   three STOPPED by +5.101 s. Both then read 3/3 `LIMIT_EXCEEDED` (code 4)
   Refusals with detail `input receive deadline` from a still-open control
   stream at window end, so the transport channel and the protocol channel
   agree on both subjects.
2. THE MILESTONE-17b BRACKET (40 s–130 s on java) IS SUPERSEDED BY A
   MEASUREMENT, not merely withdrawn. It was the product of two client
   defects at once — probes that wrote payload and so renewed the deadline,
   and a current-thread runtime that only applied inbound frames inside
   `block_on` — and neither was the subject's timing. Claude's finding and
   the coordinating owner's reproduction are both confirmed by this run.
3. Healthy-principal progress is unchanged and still far inside the 10 s
   deadline (worst 150.537 ms rust over 23 rounds, 150.445 ms java over 38),
   so removing the writes did not remove the pressure the row applies: the
   stalls, the held pending request and the unread result stream are all
   still there.
4. No regression anywhere else in group R at the fixed runtime; the
   per-row figures are in scenario-matrix-g6-resource.md under "Group R
   status (milestone 18c)". Two observations are new there: the java
   connection-ceiling recovery now lands on attempt 1 where M17/M17b saw
   attempt 2 (a continuously-driven client sends its CONNECTION_CLOSE when
   it closes rather than at its next blocking call — recorded, not asserted
   as a subject change), and the rust control-capacity ceiling is NOT stable
   between runs (attempt 17 `"connection pending limit"` at M18b, attempt 8
   `"metadata concurrency exhausted"` here). The staging row already
   declines to claim that the ceiling which fires is the declared
   `pending_limit`, and asserts only that NEW work is refused by a named
   code while existing promises complete; both held in both runs.

### Deviations recorded

- `r-stalled-principal-progress` is the only row whose LOGIC changed this
  milestone; the other four R rows and `g1-leaf-copy` are unchanged code
  re-run for regression. Their differences from M17b are therefore subject
  behaviour, run-to-run variation, or the runtime fix — never a measurement
  change, except where the runtime fix is named as the likely cause.
- The full 53-row matrix is NOT re-run at this milestone. The runtime fix
  touches every raw row, so a full-matrix rerun is required before any
  acceptance claim; what exists here is the R group plus the G1 regression.

## 3g. Milestone 18d — r-network-bytes (PARTIAL)

Third of the four remaining R rows, and the first that needed a new
measurement scope rather than a new use of an existing one.

Gates: `cargo fmt --all -- --check` exit 0; `cargo clippy --all-targets -p
pipestream-conformance -- -D warnings` exit 0; `cargo test -p
pipestream-conformance` exit 0, 92 passed / 0 failed (90 at M18c; the two new
tests cover the `/proc/net/dev` parser against the live kernel and the
network artifact's truncation, missing-method and wrong-header rejections);
`cargo build --release` exit 0, reproducing the unchanged rust subject binary
`097829fa45d8…`. Driver binary
`4dc7ddcf4ff5ed3c98b806d3c5e16663f28ede02bf64317b171f973670f900c1`.

Archived dev run: `durable-18d42f5607941dc5` — `r-network-bytes` both
directions plus the `g1-leaf-copy` regression, exit 0, INCOMPLETE-labelled,
120/120 manifest entries verified after archiving. No run directory deleted.

### The new scope

`network.tsv` is its own schema (`# pipestream-network-v1`, eight columns:
checkpoint, method, interface, elapsed_ms, rx_bytes, rx_packets, tx_bytes,
tx_packets), separate from the process schema because network bytes are a
separate scope and are never mixed into a resources record. Its validating
reader rejects a torn final line, a wrong header version, a wrong field count
and — new here — a record whose METHOD column is empty or `-`, so a
collection method can never be inferred after the fact.

Full design and observations are in scenario-matrix-g6-resource.md under
"Group R status (milestone 18d)". In short: two methods side by side and
never substituted — host-scoped kernel loopback counters with the loopback
double-counting rule stated, and the source-pinned transport's own
per-connection UDP byte totals, which are fixture-scoped by construction —
with handshake/TLS measured in its own phase before any payload exists,
retransmission reported from the transport's path counters rather than
subtracted, and the logical payload recorded as its own number and compared,
never used to derive a network figure.

### Observed

1. The host-scoped method is unusable alone here, and the row quantifies
   that: the same 10 s idle baseline measured 0 B in the archived run and
   150,144,677 B in a development run twenty minutes earlier.
2. Handshake with no payload in existence: rust sent 8,473 B / 15 datagrams
   and received 7,357 B / 13; java sent 10,945 B / 15 and received 2,967 B /
   12, already with one lost packet and one congestion event.
3. Against 4,194,304 B of logical payload the transfer cost 4,302,837 B of
   UDP payload on rust (2% overhead) and 4,337,607 B on java (3%).
4. Retransmission over the whole connection: rust 0 lost packets, java 9
   (11,682 B) with 4 congestion events — on loopback.
5. The dead-collector and truncation rules are PROVED in-row, not asserted:
   a deliberately truncated copy of the artifact must be rejected by the
   reader and a counter read against a non-existent interface must fail.

### Deviations recorded

- Row status PARTIAL: neither a network namespace nor packet capture is
  granted on this host, so per-packet accounting of the SUBJECT's side is not
  observable and no fixture-scoped INTERFACE counter exists. The exact
  failing checks are run by the row and archived verbatim in
  `artifacts/capability-probes.txt`.
- `network.tsv` lives under `artifacts/` rather than at the scenario root
  (where `resources.tsv` and `store.tsv` sit) because an event record's
  artifact label must be relative to the scenario directory and contain a
  path separator.

## 3h. Milestone 18e — r-native-credit (PARTIAL); group R complete

The last of the four remaining R rows. With it every row of group R is
implemented: four DONE (`r-capability-manifest`, `r-connection-ceiling`,
`r-stalled-principal-progress`, `r-memory-ladder`) and three PARTIAL
(`r-staging-and-journal-bounds`, `r-network-bytes`, `r-native-credit`), each
PARTIAL carrying its reason in its own `observed.tsv` as well as here.

Gates: `cargo fmt --all -- --check` exit 0; `cargo clippy --all-targets -p
pipestream-conformance -- -D warnings` exit 0; `cargo test -p
pipestream-conformance` exit 0, 92 passed / 0 failed; `cargo build --release`
exit 0, reproducing the unchanged rust subject binary `097829fa45d8…`. Driver
binary `1cf0757fe6cd9f8b0f266c0d5e89d72f9caafe98fd9949960fc246200e34f836`.

Archived dev run `durable-18d42ff8392fc9ef` — `r-native-credit` both
directions plus the `g1-leaf-copy` regression, exit 0, INCOMPLETE-labelled,
118/118 manifest entries verified. No run directory deleted.

### Observed

1. A write returning is a QUEUEING event, not completion, and the row
   measures the gap: 16,777,216 application bytes handed over in one call
   that returned after 87.19 ms (rust) / 87.31 ms (java), with the transport
   reporting the stream Open at that instant and actual completion following
   the FIN by 565 µs (rust) / 26.73 ms (java).
2. The transport sent more than the application queued: 17,220,072 B in
   13,331 datagrams (rust) and 17,334,102 B in 11,959 (java) against
   16,777,216 application bytes, with 15 packets (20,394 B) lost on the java
   path — retransmission reported, not subtracted.
3. Borrowed credit is visible as frames the peer put on the wire: 341
   MAX_DATA and 2,047 MAX_STREAM_DATA from rust, 125 and 125 from java.
   Neither side ever sent DATA_BLOCKED or STREAM_DATA_BLOCKED, so on
   loopback the application never outran its lent credit — recorded as an
   observation, not asserted as a property.
4. Stream credit IS released after refused streams, pairing with the
   `g6-stopped-control-and-transfers` MAX_STREAMS note: every object-stream
   slot filled with a stream declaring four times the negotiated
   `object_limit`, rust refusing 4/4 and java 16/16 with LIMIT_EXCEEDED plus
   a STOP_SENDING per stream; after retiring them MAX_STREAMS_UNI arrived
   (rust 0 → 2, java 1 → 2) and a further object stream DID open, so the
   credit was usable and not merely reported.

### Why PARTIAL

No byte-for-byte packet capture on this host — the row runs
`tcpdump -i lo -c 1 -w /dev/null` itself and archives its refusal — so the
evidence is the source-pinned transport's own per-frame and per-datagram
accounting rather than a capture; and it is one endpoint's view, so the
SUBJECT's internal credit ledger is not observable. Both are named; neither
is inferred or substituted.

### Fixture defects found and fixed before the decisive run

1. `execution_ms` above the session policy's 60 s execution limit is a
   LIMIT_EXCEEDED refusal of the admission, not a longer deadline.
2. A subject that refuses on the header can STOP_SENDING before the header
   write returns; that is the refusal arriving through the transport channel
   rather than the control channel, and it is counted as such rather than
   treated as a fixture error. A stream can be refused on BOTH channels, so
   the two counts are explicitly not disjoint.
3. The refusal phase can leave a late FRAME_ERROR on control for a header
   the peer stopped mid-write (the Java subject's header timeout is 10 s).
   The row therefore does not use the control stream again for its own
   housekeeping after that phase; it drains what it can, records the count,
   and closes.

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

1. Acceptance mode has never passed: the run_all.sh integration landed at
   9842e655 (opt-in `PIPESTREAM_DURABLE_ACCEPTANCE=1` block), but the
   acceptance run itself is pending three subject-side markers (section
   3k): the two Java findings reported to Claude and the rust client
   CLI's missing control-timeout option (lane question posted).
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
   pending); host UTC is never used. Since M19b the row runs and archives
   both subjects' usage texts and clock-set refusals as its evidence.
5. require-durable and wire-level cross-owner paths are unreachable via
   the published CLIs (recorded findings, M4) — final certification of
   those arms needs either CLI surface or documented implementation-test
   mapping. Rust authority capacity bounds make the 256/batch schema
   bound wire-unreachable (single-tx cap binds first); 257-batch is
   preempted by clap arity — both named gaps.
6. CLOSED at M15, re-pinned at M17 and again at M17b: the all-jar is built
   on the pipestream.4 transport fix and the subject is now Claude
   `7585a9dc` (all-jar `61ab64a3…`, merged here at `c5b4f30`); every row in
   §3c ran against it. The stall-enforcement item that was OPEN at M17 is
   CLOSED at M17b: the Java server no longer closes the connection for
   control silence, the connection is live at both enforcement probes, and
   3/3 per-stream `LIMIT_EXCEEDED` "input receive deadline" Refusals are
   readable on control at window end — so the M17 ambiguity (an
   APPLICATION_CLOSE discarding unread queued control frames) no longer
   applies to this row. What replaces it is a bracket, not a gap: the Java
   per-stream abort lands after idle+10 s and by lifetime+10 s, and the row
   does not claim a value inside that bracket.
7. Client-side commit boundaries are driver-side observations only;
   uncontrolled client-death rows are labelled as such.
8. g2-drop-reply-publication RUNS since M19b and reports the missing
   capability: neither subject exposes a PUBLICATION reply pair to withhold
   (publication is observed via watch, not a correlated reply), both refuse
   the schedule at parse; the kill-at-boundary variant is the delivered
   evidence and a proposal to accept it as such awaits Kimi's decision.
9. Group R at M18b: `r-capability-manifest`, `r-connection-ceiling`,
   `r-stalled-principal-progress` and `r-memory-ladder` are DONE;
   `r-staging-and-journal-bounds` is PARTIAL (pending and staging ceilings
   driven and asserted; journal/retained-byte ceilings recorded, not
   driven); `r-network-bytes` is PARTIAL (two methods recorded per sample,
   handshake and retransmission separated, dead-collector and truncation
   proved in-row; no fixture-scoped interface counter and no packet capture
   on this host, both recorded by the checks that failed); `r-native-credit`
   is PARTIAL (application queue bytes, borrowed flow credit and actual
   transport completion separated and measured from the source-pinned
   transport's own frame and datagram accounting, with stream-credit release
   after refused streams observed and re-used; no packet capture on this
   host and one endpoint's view only). EVERY R ROW IS NOW IMPLEMENTED:
   four DONE, three PARTIAL, nothing skipped.
   `r-stalled-principal-progress` was RE-RUN at M18c on the fixed runtime
   with non-writing probes and now brackets both subjects inside two seconds
   of their own negotiated idle bound; the M17b 40-130 s bracket is
   superseded. The FULL 53-row matrix has not been re-run since the runtime
   fix and must be before any acceptance claim. NAMED GAPS in group R: the Rust heap scope has no
   black-box collector; Java native/direct is unavailable on this host
   because NMT is off and the launch flags are frozen; incomplete-handshake
   accounting is not observable through the quinn client. None of the three
   is substituted by another scope.

## 6. Safe next action

Updated at M19c (work in Kimi's role): the full 67-row matrix HAS now been
rerun on the fixed runtime (durable-18d4ac9f7d4e9880, section 3j.19c) and
the acceptance-mode integration is written up as a proposal
(acceptance-mode-proposal.md) awaiting Kimi's decisions on the three
missing-capability rows and the remaining per-direction markers. The text
below is Kimi's M18 wording, kept for the record.

Finish group R. `r-memory-ladder` is DONE (M18a) and
`r-staging-and-journal-bounds` is PARTIAL (M18b). Two things are queued:

1. DONE at M18c: `r-stalled-principal-progress` re-run on the fixed runtime
   with non-writing probes at idle-2 s, +0 s, +2 s, +5 s, +10 s and
   lifetime+10 s. Both subjects enforce inside a two-second bracket at their
   own negotiated idle bound. What remains from it is a FULL 53-row matrix
   rerun on the fixed runtime, which no milestone here has done.
2. DONE at M18d and M18e: `r-network-bytes` and `r-native-credit`, both
   PARTIAL for named host-capability reasons. Group R has no unimplemented
   row left.

The two things group R still needs before any acceptance claim are a FULL
53-row matrix rerun on the fixed runtime (no milestone here has done one
since the raw-client runtime fix at M18b) and acceptance-mode integration
into conformance/run_all.sh.

## 3i. Driver-binary hashes: what they are and are not (recorded at M18e)

The per-milestone `pipestream-conformance` hashes quoted in §3d–§3h identify
the binary that produced that milestone's archive, and nothing more. They are
NOT reproducible across cargo invocations: from identical sources,
`cargo build --release -p pipestream-conformance` and `cargo build --release`
over the whole workspace produce different `pipestream-conformance` binaries
(`1cf0757fe6cd9f8b…` and `3e2a31918c428a8e…` at this commit), because the two
invocations give the crate a different `-C metadata` and cargo replaces the
artifact in place.

The pin that matters is unaffected: the rust SUBJECT binary
`pipestream-quinn` is byte-identical under both invocations and has been
`097829fa45d8c03eb0a5594badf0cfceababdc6d8c4898d8ce497426ec7406d7` at every
milestone of M18, including across the tokio `rt-multi-thread` feature
change. `run.tsv` in every archive records the subject hashes the run
actually used, and the nc-stale-binary control re-hashes them after every
scenario.

## 3j. Milestone 19 (work in Kimi's role, 2026-09-12)

Everything in this section was done by a follow-on agent in Kimi's role with
the project owner's written authorization, on `agent/rfc-kimi-neutral-v2` in
Kimi's worktree, while Kimi is away until about 2026-09-17. It is REVIEW_READY
for Kimi's review and accepted by nobody. Nothing was merged, fast-forwarded,
pushed or rebased. The Java subject for this milestone is Claude's all-jar
`32360ec3dbff58a1581c9b64f8afca32dfe7b6c42c49bf5c43d6fad64d19aa7c`, copied
from his read-only tree to `~/.rfc-tmp/jars/pipestream-quic-netty-32360ec3-all.jar`
and hash-verified before every run (never rebuilt here); its sources are NOT
merged into this branch (the last merged Java pin is `7585a9dc` at `c5b4f30`),
so unlike M17b the jar is a pinned external artifact and `run.tsv` names the
copy's path. The Rust subject is the release build of this tree,
`097829fa45d8…`, rebuilt before every run and byte-identical throughout (the
server crate is untouched). Java storage funding flags (`--db-mib`,
`--wal-mib`) are NOT passed: both subjects run their defaults, as every
earlier milestone did, so the M17b comparison stays like for like. Stores,
artifacts and `TMPDIR` for every run are under `~/.rfc-tmp/kimi-m19/` on the
root drive (never `/work`, never `/tmp`), and every run and release build was
taken under the coordination `BENCHMARK.lock`.

### 19a. Two driver fixes (commit 2a9aad5d), rerun archived as durable-18d4aacfca57bb0b

Gates on the fix commit: `cargo fmt --all -- --check` exit 0; `cargo clippy
--all-targets -p pipestream-conformance -- -D warnings` exit 0; `cargo test -p
pipestream-conformance` 93 passed / 0 failed (92 at M18e; the new test pins
the attempt-2 hold schedule). Both fixes carry a unit test that was RED on the
old code and is green now, checked by running the new tests against the old
function bodies before committing: `watch_field_parser_reads_deadline_values`
(the Java record string) and `release_file_targets_the_events_directory` (the
Java FixtureMain release name). Driver binary for the rerun
`bac2dbceee8094cf7ed444afb87ea950cd153834ae7b5d4a725dfa77cac3f9da`; rust
subject `097829fa45d8…` reproduced byte-identical by the release build.

(a) g4-stale-attempt-retry. The rust-client/java-server direction answered
ALREADY_TERMINAL instead of CONFLICT because the copy under attempt 2 finished
before the driver's stale-retry process arrived; both authorities check
terminal state before the attempt mismatch. The Java direction now holds
attempt 2 at its claim: a schedule of `pause`, `release`, `pause` at
EXECUTION_CLAIMED (Claude's FixtureMain consumes one pause row per reached
boundary, so attempt 1's claim takes the first row and is released the moment
the subject records it, attempt 2's takes the second), the stale retry is
sent into the hold, CONFLICT "retry attempt changed" is observed, and only
then is the release written. The subject's own record is the evidence:
EXECUTION_CLAIMED appears twice in the Java events before the stale retry
(`attempt_2_live_evidence`). The Rust subject rejects a pause outside its
three reply pairs (`src/v2/fixture.rs` REPLY_PAIRS, checked at schedule
parse), so that direction keeps attempt 2 live with a 16 MiB copy (the
negotiated object limit) and names the gap in `expected.tsv`/`observed.tsv`
(`attempt_2_hold`); a deterministic hold there needs a server-crate hook,
which this milestone does not touch and which is a request to Meta. The
driver now writes the release file under both spellings the subjects poll
(`release-<BOUNDARY>` for the Rust hooks, `release-server-<BOUNDARY>` for the
Java FixtureMain; the interface-v1 reconciliation is still a proposal) and
can clear it so a later pause on the same boundary holds again.

(b) `parse_field_u64` matched only the Rust CLI's Debug form. The Java client
prints `VIEW` plus a Java record (`Records.WorkView`), so `deadline=N` and
`deadline=null`; the parser now accepts the Rust view form
`deadline: Some(Number(N))`, the Rust receipt form `deadline: Number(N)`, and
the Java record form with the camelCase spelling derived from the snake_case
field name (`admitted_at` reads `admittedAt=`), `null` read as absent.

Rerun (dev, INCOMPLETE-labelled, 384/384 manifest entries verified after
archiving, exit 0): `durable-18d4aacfca57bb0b` over g4-stale-attempt-retry,
g2-kill-after-admission-before-publication, g2-kill-at-publication-commit
and g3-restart-same-roots. Every direction of every row ran green:
g4-stale-attempt-retry rust-client/java-server records
`stale_retry_refusal authority refusal CONFLICT: retry attempt changed`
(M17b: ALREADY_TERMINAL, direction INCOMPLETE); the java-client/rust-server
directions of the two kill rows, INCOMPLETE at M17b with "post-restart watch
did not report a deadline" (that message was the parser, but the M17b marker
text was "process timed out", see the note below), now record terminal
state 5 under attempt 1 with `automatic-redispatch-under-attempt-1`;
g3-restart-same-roots java-client/rust-server records identical attempts and
deadlines across the restart with the Java client's `deadline=` values now
read. No run directory was deleted.

Note on the M17b markers: the M17b archive's java-client/rust-server
INCOMPLETE markers for the two kill rows read "process timed out" (the Java
client's graceful shutdown after a server kill, a Java finding already
reported), not the parser message; the parser failure was what the
coordinating owner's 2026-09-12 rerun on the 28c3369b jar exposed once the
timeout no longer occurred. At the 32360ec3 jar neither failure occurs in
these rows.

### 19b. The ten rows the matrix still listed as unimplemented (this commit)

Every row of the matrix is now registered as implemented (67 of 67): seven
run green in dev on every direction their subjects allow, and three run what
they can and report the subject capability they lack (a `MissingCapability`
outcome: INCOMPLETE with the named reason in dev, FAIL in acceptance).

Gates on this tree: `cargo fmt --all -- --check` exit 0; `cargo clippy
--all-targets -p pipestream-conformance -- -D warnings` exit 0; `cargo test -p
pipestream-conformance` exit 0, 96 passed / 0 failed (93 at 19a; the three
new tests cover the raw operation-lookup and receipt-byte parsers, the result
header parser, and short-lived mapped principals with a second authority's
map); `cargo build --release` exit 0 reproducing the rust subject
`097829fa45d8…` byte-identical. The conformance crate gained no dependency:
the short-lived leaf windows are built from rcgen's 1975 anchor plus a std
`Duration`, so no calendar crate is named.

Archived dev runs, both INCOMPLETE-labelled, both kept, each MANIFEST.sha256
verified after archiving:

- `durable-18d4ab23bb15105e` (driver `cd4ef23c…`) — all ten rows, exit 0,
  457/457 entries. Seven outcomes stand; two rows failed on FIXTURE defects
  that are recorded rather than hidden: g7-read-pin-past-expiry spawned its
  16 MiB admission without the session's short policy triple (the Rust
  client refuses "journal format, identity or policy changed"), and
  g7-deadline-queue-time admitted its two load parents concurrently on one
  Rust client journal (CONFLICT "client journal already owned"). The run is
  kept as the evidence for those two defects and for the other eight rows.
- `durable-18d4abe3eae3a613` (driver `95b87a32…`) — the decisive rerun of
  g7-read-pin-past-expiry, g7-deadline-queue-time and g5-expired-identity
  after the fixes (admission through the session, load parents admitted
  back to back, and the live-connection refusal frame decoded instead of
  reported as an unexpected frame), exit 0, 203/203 entries.

Per row (dev evidence, never an acceptance claim; details in the matrix
files):

1. g2-not-found-in-flight, three directions. The pending admission is a raw
   peer holding the input header plus half the payload open without FIN; a
   second raw connection of the same principal sends the wire lookup and is
   refused NOT_FOUND (5) on its own request tag on both subjects (rust
   "operation not retained", java "operation receipt unavailable"); after
   the FIN the Work::Admitted and Work::OperationResponse receipts are
   byte-identical; the CLI replay of the same admission returns the durable
   receipt with the same deadline, two CLI lookups are identical to it, the
   page shows one member, the result reads byte-exact, and the Java subject
   recorded EXECUTION_CLAIMED exactly once for the work. The raw hold is
   used for both servers because the Rust subject never reaches
   INPUT_INSTALLED and pauses only at reply pairs.
2. g5-cert-rotation-same-owner, both servers: identical BINDING under the
   rotated leaf after a stop/restart on the same roots, the cert-1 admission
   looked up, retry (to attempt 2, SUCCEEDED, byte-exact) and cancel
   (disposition 0, CANCELLED) accepted as the same owner.
3. g5-remapped-owner, both servers: after alice's hash is remapped to
   mallory between a stop and a restart, attach, retry, cancel, lookup and
   read are each refused UNAUTHORIZED (3) with no state-disclosing code, the
   refused read writes nothing, and the pre-remap output still hashes to the
   oracle. This attach is the wire Attach carrying the journaled owner from
   a credential the server now maps elsewhere, so the authority-side
   cross-owner branch g5-foreign-owner could not reach is exercised here.
4. g5-cross-authority-reference, both servers: X's journal against
   authority Y (`issuer-b`, own roots and map) is refused on attach, read,
   lookup and watch with no bytes written; rust UNAUTHORIZED "authority
   access denied", java CONFLICT "authority differs"; a Y-bound journal
   selecting X's identifiers is refused NOT_FOUND on both with X's digest
   absent from the transcript; X reads byte-exact. The code class Java
   picks (CONFLICT) under the authorization-before-existence precedence
   rule is raised as a question to Claude, not scored.
5. g7-read-pin-past-expiry, both servers: a raw reader draining 256 KiB
   every 150 ms had 8.1 MiB of 16 MiB when availability passed; a CLI read
   issued then refused named EXPIRED (6) on both; the pinned read completed
   byte-exact; the Rust object directory was unchanged during the read and
   emptied within 30 s after it; the Java directory lost one 16 MiB object
   WHILE the read was open (5 files / 33.5 MB to 4 / 16.8 MB) and the read
   still completed byte-exact — either the expired output unlinked under the
   open descriptor or the retained input reclaimed; recorded as an
   observation for Claude, not a defect.
6. g7-deadline-queue-time, both servers: the receipt deadline equals
   admitted_at + 1000 ms (the Java client honours --execution-ms), the probe
   settled FAILED with DEADLINE_EXCEEDED (11) while still queued on both, on
   Java before the two EXECUTION_CLAIMED pauses were released and with zero
   claim records for it; retry then refuses ALREADY_TERMINAL (18) on both
   (matrix: DEADLINE_EXCEEDED; both named codes accepted, as in
   g4-deadline-settlement); read refuses NOT_FOUND. The Rust direction has no
   hold and uses two 4 MiB chunk-copy parents as load; it names the
   mechanism and would report INCOMPLETE rather than claim the property if
   the load did not outlast the deadline in three fresh authorities.
7. g5-expired-identity, both servers: a 25 s leaf; after expiry the Rust
   server refuses the next request on the LIVE connection UNAUTHORIZED
   "credential validity or mapping changed" with the connection kept, the
   Java server closes it APPLICATION_CLOSE 0x203 "caller credential
   unavailable" (S12-098); a fresh connection with the expired leaf fails
   the handshake on both (TLS alert 45, no application refusal); the renewed
   leaf attaches to the identical binding and sees the declaration. Host UTC
   was never changed; the leaf windows come from the fixture's own UTC.
8. g2-drop-reply-publication: INCOMPLETE, missing capability. Both subjects
   refuse drop-reply at PUBLICATION_COMMITTED at schedule parse (rust:
   "requires a committed reply-pair boundary"; java: "withholds a reply at
   PUBLICATION_COMMITTED, which has no pending reply"), archived verbatim.
   Proposal for Kimi in scenario-matrix-g2.md: accept the kill variant as
   the boundary's evidence and retire this row.
9. g7-unsafe-clock-refusal: INCOMPLETE, missing capability. Both usage texts
   name only --trust-system-clock; both subjects refuse clock-set at parse
   (archived). The request for a fixture clock to both owners stands.
10. g7-cleanup-interrupted-refund: INCOMPLETE, missing capability.
    interface-v1 has no cleanup boundary (29 labels archived, none matches);
    adding one is an interface revision proposal for both subjects.

Candidate defects: none scored this milestone. Two observations go to Claude
(the Java object removed under an open read; CONFLICT "authority differs"
as the cross-authority class) and two hook requests go to Meta (a pause at
EXECUTION_CLAIMED and an emit-only arming for the Rust subject).

Driver defects found and fixed before the decisive run: the two named above
(short-policy flags on a spawned admission; concurrent admissions on one
Rust client journal), both fixture-side.

### 19c. The full matrix rerun on the fixed runtime, compared line by line with M17b

Archive `durable-18d4ac9f7d4e9880`: every registered row (67; the 53 that M17b
ran plus the 4 rows implemented in M18 and the 10 of 19b), `--dev`,
INCOMPLETE-labelled, exit 0, 40 minutes of wall time (lock acquired
2026-09-12T20:25:43Z, driver exit 21:05:18Z), 5591/5591 manifest entries
verified after archiving. 64 rows SCENARIO OK, 3 rows INCOMPLETE with a named
missing capability. Driver binary `18966bbd…` (commit 69b197aa); rust subject
`097829fa45d8…` rebuilt byte-identical; Java all-jar `32360ec3…` hash-verified.
Supplementary archive `durable-18d4af67e48effbd`: g2-crash-after-create-commit
rerun after the driver fix in 03ce9a44 (below), 89/89 entries verified.

Comparison method: `compare.sh` from the coordinating owner's 2026-09-12
rerun, copied to the scratch area unchanged: every `observed.tsv` and
`INCOMPLETE` file of the new archive is diffed against the same path in the
baseline (M17b `durable-18d3ed6c2f040515` for every G row and
r-capability-manifest, r-connection-ceiling and r-stalled-principal-progress;
the M18c/M18d/M18e archives for the R rows added or re-run since), with
12-digit-or-longer numbers and durations masked, and any baseline INCOMPLETE
marker the new run no longer has is listed. 94 entries came out CHANGED or
NO BASELINE FILE; every one is accounted for below. The full diff is
`runs/durable-18d4ac9f7d4e9880/COMPARE-M17B.txt` (checked in next to the
archive, outside the manifest).

Per-direction INCOMPLETE set, M19c against M17b:

| Marker | M17b | M19c | Explanation |
|---|---|---|---|
| g2-kill-after-admission-before-publication java-client/rust-server | INCOMPLETE ("process timed out") | green | The parser fix (19a) reads the Java deadline; at the 32360ec3 jar the Java client no longer times out on this row |
| g2-kill-at-publication-commit java-client/rust-server | INCOMPLETE ("process timed out") | green | same |
| g2-crash-before-create-commit java-client/rust-server | INCOMPLETE | INCOMPLETE ("process timed out") | unchanged: the Java client hangs in its graceful shutdown after the server is killed at CONNECTION_AUTHENTICATED (Java finding already reported in section 5.3) |
| g2-crash-after-create-commit java-client/rust-server | INCOMPLETE ("process timed out") | INCOMPLETE in the matrix run ("changed-policy replay must refuse named CONFLICT", the Java client exited 0 with the ORIGINAL binding); GREEN in the supplementary archive | A DRIVER defect, found by this rerun and fixed in 03ce9a44: the changed-policy probe passed --max-execution-ms (the Rust spelling) to the Java client, whose option parser ignores unknown keys, so the "changed" policy was the default and the replay identical. With the Java spelling the Rust server refuses CONFLICT "creation policy changed" and CONFLICT "creation sequence is ahead of authority" (durable-18d4af67e48effbd). This direction had never tested what it claimed; at M17b the shutdown timeout hid it |
| g2-crash-after-create-commit rust-client/java-server | INCOMPLETE (CANCELLED "client transport closed") | INCOMPLETE, same text | unchanged at the 32360ec3 jar: after the drop-reply at SESSION_COMMITTED and the restart, the Rust client's creation replay against the Java server has its transport closed; Kimi's section 5.2 finding (FixtureMain re-fires drop-reply on the REPLAYED commit, no fresh-commit gating), reported to Claude with the M7 transcript, still stands as a candidate defect: expected BINDING with generation 1, actual transport close, direction rust-client/java-server, runs durable-18d3ed6c2f040515 and durable-18d4ac9f7d4e9880 |
| g3-store-ownership java-client/rust-server | named gap | named gap | unchanged (the client subject never owns the store) |
| g4-revocation-vs-publication rust-client/java-server | named gap | named gap | unchanged (no Java operator revoke command) |
| g8-timeout-no-completion-claim kill-variant rust-client/java-server | INCOMPLETE ("process timed out") | INCOMPLETE, same | unchanged: the Rust client's op against the Java server killed mid-complete does not return inside the driver's 30 s op timeout; recorded at M13/M15, not new |
| g2-drop-reply-publication, g7-unsafe-clock-refusal, g7-cleanup-interrupted-refund | not run ("not implemented yet") | whole-row INCOMPLETE, missing capability | 19b |

So the per-direction set shrank from 8 markers to 6 (5 after the
supplementary run), two closed by the 19a parser fix, one closed by the
03ce9a44 probe fix, none opened.

Every other CHANGED entry, grouped:

1. `java_jar_sha256` only (the pin moved from 61ab64a3 to 32360ec3): all
   sixteen G1 mixed directions, g3-input-before-metadata java-client,
   g4-publication-vs-cancel java-server, g5-foreign-owner,
   g5-missing-client-cert, g5-no-existence-disclosure, g5-unmapped-principal
   java-server, g7-no-deadline-extension, g7-output-before-receipt-expiry,
   g7-receipt-before-output-expiry, g3-terminal-cleanup java-server, the
   g8 mixed directions of g8-child-cut-conflict, g8-complete-with-pending,
   g8-exact-root-complete and g8-half-close-preserves-responses. No other
   line differs in any of them.
2. Per-run identifiers and timings: g2-simultaneous-duplicate first_pid;
   g5-untrusted-identity foreign_ca_sha256 (minted per run; the masker
   turned a 12-digit run inside one hash into `<T>`); g1-oversize-payload
   control_latency_ms (1353 -> 1478 java client, 126 -> 100 java server);
   g8-detach-drains detach_wall_ms (100 -> 125/150, 1328 -> 1378) and the
   count of concurrent same-journal watch probes (12 -> 8); g4-eventual-
   settlement settlement_wall_ms (250 -> 351 rust, 276 -> 175 java).
3. The host rule moved every store from `/work` (xfs, the RAID with the 31 ms
   fsync latency) to the root drive (ext4): r-capability-manifest records
   `fixture_fs_type ext4`, `fixture_mount /` (M17b: xfs, /work) and
   mem_total_kb 127121904 (M17b: 127121896, the kernel's own reading). This
   is the single largest cause of the timing shifts in group 4.
4. Race and timing outcomes that the rows record and never assert:
   g4-ancestor-fence-publication rust/rust now observes BOTH orders in one run
   (publication:1, fence:2; M17b publication:3, fence:0), which the matrix
   asks for; g4-deadline-settlement iteration 3 (1 MiB) went deadline on
   rust/rust and iteration 4 (256 KiB) went deadline on the Java server
   (orders 1:3 and 0:4 against 2:2 and 1:3), the deterministic leg still
   producing the retry evidence on both; g4-publication-vs-skip java
   iteration 2 flipped fence -> publication (5:1 against 4:2);
   g4-eventual-settlement rust children 1 succeeded / 127 cancelled against
   8 / 120 and objects 7 -> 4 against 22 -> 18; g3-restart-same-roots
   pre-crash state of the in-flight mode-2 parent 1 (ACTIVE) against 3
   (WAITING_CHILDREN) on both servers (the seeded kill lands earlier in the
   expansion on the faster store). None of these changes any assertion.
5. Object-directory figures that follow the uncontrolled kill's timing:
   g3-orphan-cleanup rust/rust retained orphan 66207 -> 70883 B (M17b
   16844587 -> 16849263 B: the kill landed before the 16 MiB staging file
   existed this time) and java-server baseline/final figures swapped the
   same way (5 files 131865 B against 4 files 66141 B); g3-input-before-metadata
   java-server object_dir_after_readmission 5 files / 25.2 MB against 4 /
   12.6 MB (the staged orphan of the mid-stream kill survived alongside the
   readmission this time); g3-nonreusable-history and g3-partial-retirement
   java-server object_metrics_before swapped between the same two shapes.
   The rows' assertions (lookup NOT_FOUND then clean re-admission; no
   restart-time cleanup on the Rust CLI; reclamation during store
   operations on Java) held in every case.
6. g3-restart-same-roots java-client/rust-server: the three views now carry
   `deadline=Some(<T>)` where M17b recorded `deadline=None` — the 19a parser
   fix reads the Java client's deadline, so the pre/post-restart comparison
   is now a real comparison instead of None == None.
7. g4-stale-attempt-retry both directions: two NEW lines (`attempt_2_hold`,
   `attempt_2_live_evidence`, 19a); every M17b line unchanged, and the
   rust-client/java-server direction is green where M17b had the
   ALREADY_TERMINAL marker.
8. Group R against the M18c/M18d/M18e baselines, all within the rows' own
   allowances and with no assertion change: r-stalled-principal-progress
   brackets unchanged to within 30 ms (rust open +3128 ms / stopped +5102 ms
   against +3102 / +5101; java +28000 / +30031 against +28000 / +30156),
   3/3 refusals readable on both, cancelled-write share 99% rust / 4% java as
   before; r-memory-ladder rust payload growth 816 KiB (1744), inventory
   1724 (1108), java payload 21352 (8792), inventory 0 (4396), all against
   allowances of 131072/66560/524288/278528 KiB; r-staging-and-journal-bounds
   rust pending ceiling fired at attempt 14 ("metadata concurrency
   exhausted") against attempt 8 at M18c and 17 at M18b, the instability
   already raised with Meta, java figures within 2% (fds 150 held, 22
   released, unchanged); r-native-credit java lost 12 packets / 16038 B
   against 15 / 20394, rust 0 as before; r-network-bytes java lost 13
   packets against 9 and the rust host idle baseline moved 1495 B this run
   against 0 (the host-scoped method's contamination floor, which is the
   point the row makes).
9. NO BASELINE FILE: the ten 19b rows and the java-client directions of the
   two g2 kill rows (M17b had markers there, now observed.tsv).

Candidate defects with evidence (none scored; all raised, two of them
already known):

- Java subject, still open from Kimi's M7 finding: g2-crash-after-create-commit
  rust-client/java-server, expected the replayed creation to return the
  generation-1 binding after the drop-reply and restart, actual
  `CANCELLED: client transport closed` on the replay; runs
  durable-18d3ed6c2f040515 and durable-18d4ac9f7d4e9880, both jars.
- Java client, still open from section 5.3: graceful shutdown hangs after a
  server kill (g2-crash-before-create-commit java-client/rust-server,
  "process timed out", both archives).
- Java subject, observation for Claude (19b): one 16 MiB object leaves the
  object directory while a pinned result read is open and the read still
  completes byte-exact (g7-read-pin-past-expiry rust-client/java-server,
  durable-18d4abe3eae3a613 and durable-18d4ac9f7d4e9880).
- Java subject, question for Claude (19b): CONFLICT "authority differs" as
  the refusal class when a journal bound to another authority attaches
  (g5-cross-authority-reference java-server); the Rust subject answers
  UNAUTHORIZED. Neither discloses the session.
- Driver (this milestone, fixed): the changed-policy probe's flag spelling
  (03ce9a44); the two 19b fixture defects (69b197aa).

Rows left INCOMPLETE and why: g2-drop-reply-publication (no PUBLICATION
reply pair on either subject), g7-unsafe-clock-refusal (no fixture clock on
either subject), g7-cleanup-interrupted-refund (no cleanup boundary in
interface-v1). Directions left INCOMPLETE: the four in the table above
(one Java subject candidate defect, two Java client timeouts, and the two
named gaps that are not defects). The rust-client/rust-server direction of
g7-deadline-queue-time ran green with the load-based queue and names the
missing EXECUTION_CLAIMED hold as a gap rather than a marker.

Acceptance-mode integration: `acceptance-mode-proposal.md` in this
directory is the written proposal (not an edit) for `conformance/run_all.sh`,
with the block to insert, the preconditions that would make it red today,
and what a reviewer must decide first.

Deviations recorded for 19c: the matrix was run once and its archive kept
with its two failed-by-driver-defect directions rather than rerun whole after
03ce9a44; the supplementary archive covers the one row the fix touches, and
the fix does not affect any other row (only binding_attempt calls it). No run
directory was deleted. The Java subject's sources at 32360ec3 are not merged
into this branch; the jar is a pinned external artifact whose path and hash
`run.tsv` records.

## 3k. Milestone 20 (work in Kimi's role, 2026-09-15)

Everything in this section was started by a follow-on agent in Kimi's role
with the coordinating owner's written authorization (milestone M20 mandate of
2026-09-15), on `agent/rfc-kimi-neutral-v2` in Kimi's worktree, and reviewed
and confirmed by Kimi on return the same day (board update and the run_all.sh
integration at 9842e655 are Kimi's own). Nothing was pushed, merged or
rebased. The branch fast-forwarded
from ab40d2e0 to afbb948a (the Java peer's in-tree subject code: defect 16 at
3e1547dd, the owner's 2026-09-13 Section 12 decisions at a3b14725/802f1edc).
All evidence here is DEV evidence, INCOMPLETE-labelled; nothing in this
section is an acceptance claim. Stores, artifacts and TMPDIR for every run
are under `~/.rfc-tmp/kimi-m20/` on the root drive; every build and run was
taken under the coordination `BENCHMARK.lock`.

### The decisions implemented (coordinator board, 2026-09-13/15)

1. **g2-drop-reply-publication RETIRED** (commit a1616584). The row is
   removed from the driver matrix (67 to 66 rows), its match arm and
   MissingCapability body are deleted, and it no longer runs or FAILs in
   either mode. The retirement is pinned by the
   `drop_reply_publication_is_retired_from_the_matrix` driver test and
   recorded in scenario-matrix-g2.md: publication is watch-observed on both
   subjects, not a correlated reply, so a withheld reply that does not exist
   cannot be lost; `g2-kill-at-publication-commit` is the boundary's
   evidence; adding a correlated publication reply is a protocol change, out
   of scope. The M19b MissingCapability evidence for the row remains in the
   M19 archives.
2. **Acceptance-gate waiver mechanism instead of weakened rows** (commit
   5d65b661). `--waive 'ROW[:DIRECTION][=REASON]'` (repeatable) is parsed
   and validated against the matrix before anything runs, recorded with the
   reason string in run.tsv (`waive\t<row>\t<direction|->\t<reason>`), and
   named in the run output. In acceptance mode a whole-row waiver converts a
   row FAIL into a named WAIVED outcome; a direction waiver accepts only a
   NAMED gap and requires the direction directory's INCOMPLETE marker, else
   the waiver itself fails the row. Dev reporting is untouched (dev keeps
   its INCOMPLETE labelling and never neutralises a row). PARTIAL row_status
   is acceptable in acceptance mode with its named unmeasured scopes intact:
   every observed.tsv under the row directory is scanned, a `PARTIAL`
   marker without a `PARTIAL: <scopes>` reason fails the row, and PASS lines
   are annotated with the evidence path. The marker itself is never
   stripped.
3. **Kill rows pass the Java client control deadline** (commit b55d8bad).
   The four hooked G2 kill rows carry `--control-timeout-ms 10000`
   (`JAVA_CLIENT_KILL_CONTROL_TIMEOUT_MS`, ClientCommands.java at 3e1547dd)
   on every op of their java-client directions, built through one
   `client_command` helper so `run_client_op_with` and `spawn_client_op`
   cannot drift. See "Mandate correction" below for why this could not be
   done for the Rust client.
4. **Owner wire-code decisions asserted** (commit 138c566f).
   g5-cross-authority-reference arm A now asserts CONFLICT (7) for the
   attach, read, lookup and watch probes on BOTH subjects (owner decision
   2026-09-13, Section 12.3, a3b14725: a foreign expected authority is a
   contradiction with retained identity); the M20 dev run records
   `refused CONFLICT (7): authority refusal CONFLICT: authority differs` in
   both directions, resolving the M19 question to Claude. The
   over-limit-control-body half of the audit needed no driver change: no row
   asserted the old FRAME_ERROR for it (the G6 wire vectors are S12-338
   decoded aggregates, which stay FRAME_ERROR), and the remaining
   UNAUTHORIZED expectations are the owner-authorization class, which keeps
   its code.

### Mandate correction (reported, not worked around)

The mandate expected a control-timeout option on BOTH subject client CLIs.
It exists only on the Java client launcher (`--control-timeout-ms`, added at
3e1547dd). The Rust client CLI (`pipestream-quinn v2 client`) exposes no
such option: its quinn v2_client `response_timeout` (default 60 s) has no
CLI surface, and clap would reject an unknown flag, so the driver cannot
pass one and none was fabricated. The Rust-client kill directions keep their
honest bounded-op handling and name the deviation in observed.tsv
(`client_control_deadline`). This is why g8-timeout-no-completion-claim's
kill variant rust-client/java-server cannot be fixed by passing an option.

### Java subject build gate at this pin (named finding)

The Java all-jar was built from the in-tree sources with the mandate's
explicit fallback `mvn install -q -DskipTests -Psealed-interop` after the
test gate failed THREE times on ONE test:
`PeerRuleWireTest.streamIdsAreNeverRecycledAcrossALongConnection`
(813 tests, 1 failure each time; logs /tmp/kimi-m20-mvn.log and
/tmp/kimi-m20-mvn2.log, report copied to ~/.rfc-tmp/kimi-m20/).
The failure is `LIMIT_EXCEEDED "retained input, output or executor
capacity"` at entity ~28 of 100; the test's own comment says the default
funding "refuses admissions past about 28" and it funds db-mib 1024 to get
past that. The same test passed 8/8 twice on 2026-09-13 at tree af4cef6d
(17.9 s per run) and fails 3/3 today (3.9-4.6 s per run) on unchanged code:
an environment-speed margin, deterministic under today's host conditions,
not a code regression and not load-herd noise. For Claude: worth checking
whether the retained-promise reservation vs funded-log arithmetic is
time-sensitive. The all-jar bytes are identical with or without surefire;
the conformance matrix remains the subject certification gate.

### Dev-run evidence (dev evidence, never an acceptance claim)

Full matrix, `--dev`, INCOMPLETE-labelled, exit 0, 39.8 min of wall time
(lock held 2026-09-15 10:38-11:18 EDT), archived as
`durable-18d5856a211db6af`, **5732/5732 manifest entries verified after
archiving, 0 mismatches**. Subjects: rust
`4156c642b7bea6e4d8df0d32475573df53f6276d0f977c0968ca51506cb26cb6`
(release build of this tree, reproduced; driver binary
`9863d8d99af8423c8aa759b9f476c26b421a02b8824cdd7c055554aa04c57421` at
commit 138c566f) and the in-tree Java all-jar
`91c1842f0a4fe55e7667174f680ebd427d4f70600322f63a09d01039584ccb08`.
run.tsv records the four waivers with their reasons. 60 rows SCENARIO OK,
6 named INCOMPLETE markers (the four direction markers below plus the two
waived whole rows), no FAIL, no new failure.

Per-marker resolution, M19c set against M20:

| Marker | M19c | M20 | Resolution |
|---|---|---|---|
| g2-crash-before-create-commit java-client/rust-server | INCOMPLETE "process timed out" | INCOMPLETE, same text | resolved at M20b as a DRIVER budget matter: named refusal observed inside the 90 s kill-row budget (see M20b below) |
| g2-crash-after-create-commit rust-client/java-server | INCOMPLETE "client transport closed" | INCOMPLETE, same text | NOT resolved; defect 16 held for the committed boundary but the withhold hook still fires on the replay (below) |
| g8-timeout-no-completion-claim kill-variant rust-client/java-server | INCOMPLETE "process timed out" | INCOMPLETE, same text | NOT a budget matter at M20b: third Java fixture defect — kill rows never fire at SENT boundaries (see M20b below) |
| g3-store-ownership java-client/rust-server | named gap | named gap | unchanged; waived direction with its INCOMPLETE marker |
| g4-revocation-vs-publication rust-client/java-server | named gap | named gap | unchanged; waived direction with its INCOMPLETE marker |
| g2-drop-reply-publication (whole row) | INCOMPLETE missing capability | RETIRED | row removed from the matrix (decision 1) |
| g7-unsafe-clock-refusal (whole row) | INCOMPLETE missing capability | INCOMPLETE missing capability | waived whole row; reason in run.tsv |
| g7-cleanup-interrupted-refund (whole row) | INCOMPLETE missing capability | INCOMPLETE missing capability | waived whole row; reason in run.tsv |

The three PARTIAL R rows (`r-staging-and-journal-bounds`, `r-network-bytes`,
`r-native-credit`) keep their `row_status PARTIAL` markers verbatim with the
named unmeasured scopes, and the new acceptance check accepted them with
the evidence paths annotated on the run lines (dev mode annotates; the
named-scope rule is enforced in acceptance mode).

### Two Java-subject findings with M20 evidence (for Claude; no driver patch)

- **g2-crash-before-create-commit java-client/rust-server: the control
  deadline does not bound a dead-transport request stall.** Reproduced by
  hand against the row's own schedule: the Java client WITH
  `--control-timeout-ms 10000` (flag confirmed parsed: `--control-timeout-ms
  0` answers `FRAME_ERROR: integer outside schema range`) exits 1 after
  **61 s** with `CONTROL_RESET: connection ended before drain`, while the
  Rust server kills itself correctly (exit 86, one CONNECTION_AUTHENTICATED
  record). The 10 s control-response deadline covers a LIVE authority
  holding a request (ClientControlTimeoutOptionTest's case) but not a
  silently dead connection whose create write never completes; the client's
  own bound there is ~60 s, above the driver's 30 s op bound. Either the
  control deadline should also bound the request write/dead-connection
  discovery, or the launcher should expose the stage bound. (Observed O-2's
  "pass a value below its operation bound" is therefore NOT sufficient for
  kills at CONNECTION_AUTHENTICATED; it remains correct for kills after a
  reply boundary, where the op already has its answer.)
- **g2-crash-after-create-commit rust-client/java-server: defect 16's scope
  ends at the committed boundary.** M20 events show exactly ONE
  SESSION_COMMITTED across both server processes (the fix held) — yet the
  restarted server's `withhold(SESSION_RESPONSE_SENT)` hook still consumed
  the re-armed drop-reply row for the REPLAYED create (CONTROL_RESET record
  on the restart; replay refused `CANCELLED: client transport closed`).
  FixtureMain's withhold path has no fresh-commit gating; HookPlacementTest
  replays within one process and does not cover a cross-restart re-armed
  schedule. Either the withhold hook needs the same fresh/replay
  discrimination, or the driver restarts this row with an empty schedule
  (a coordinator decision; NOT taken — it would stop exercising the
  property). The same restart-rearm shape is why g8's kill variant
  rust-client direction still hangs a post-restart probe against the
  restarted plain Java server (unchanged from M19c; restarted server writes
  READY, the Rust client's next-sequence then hangs >30 s with no output).

### M20b: root-cause correction and the kill-row op budget (same day, commit 8f5c4a7e)

Coordinator root-cause review re-classified two of the three blocking
markers as DRIVER budget problems, not subject defects: a killed authority
is noticed at the NEGOTIATED TRANSPORT BOUND, not the application control
deadline. The Rust authority sets max_idle_timeout(60 s)
(quinn/src/v2_authority/server.rs:225); the Java client's idle is
max(handshake 10 s, control deadline, stream lifetime 300 s)
(DurableClient.java:203-207), so QUIC negotiates 60 s and the Java client
surfaces CONTROL_RESET "connection ended before drain" at ~61 s
(DurableClient.java:271-278) — `--control-timeout-ms` bounds only
live-connection control work. The Rust client's per-request response_timeout
is 60 s (quinn/src/v2_client/transport.rs:127, swept at :538), refusing
LIMIT_EXCEEDED "client response deadline" at ~60 s. Both exceed the driver's
default 30 s op wait.

The fixture now carries a per-op client wait (default 30 s, unchanged for
every row) that the two affected kill rows widen to 90 s
(`KILL_ROW_OP_TIMEOUT`, process.rs); the restarted session inherits it
through the fixture clone. Only the WAIT BUDGET changed — every row
assertion is byte-for-byte the same — and the rows' observed.tsv gained a
`driver_op_budget` line naming the derivation and citations.

Targeted dev rerun (both subjects, `--dev`, under the lock, archive
`durable-18d589547e933f9b`, 132/132 manifest entries verified, exit 0):

- **g2-crash-before-create-commit java-client/rust-server: GREEN.** The
  binding op now observes the named refusal inside the 90 s budget —
  transcript verbatim `CONTROL_RESET: CONTROL_RESET: connection ended
  before drain` (exit 1; the hand repro of the identical op measured ~61 s
  to that refusal). The row then completes: subject exit 86,
  NEXT_SEQUENCE 1 after the restart, generation-1 replay, NEXT_SEQUENCE 2.
  The marker is closed as a driver budget matter.
- **g8-timeout-no-completion-claim kill-variant rust-client/java-server:
  STILL INCOMPLETE, and the budget was NOT raised further (stop-and-report
  rule).** The rerun disproved the budget hypothesis: the direction's
  subject/ directory holds exactly ONE server log and ONE ready file — the
  restart is never reached — and the timed-out wait is the driver's
  `wait_exit` on the Java server, not any client op. Root cause, verified
  in FixtureMain.java: `Hooks.committed()` (line 198) alone consumes
  kill/exit rows, while `Hooks.sent()` (line 223) only records — so the
  armed `kill@COMPLETE_RESPONSE_SENT` is accepted by the schedule parser
  (line 114 accepts kill at any boundary; only drop-reply/disconnect are
  reply-gated) and then SILENTLY NEVER FIRES. The Java server records
  COMPLETE_RESPONSE_SENT and keeps serving; the Rust subject's fixture does
  fire the kill at that boundary, which is why the rust/rust kill variant
  is green. This is a THIRD Java fixture defect (kill at SENT boundaries;
  plus the parser should refuse unactionable kill rows), in Claude's scope
  like the withhold finding — reported, not patched: re-arming the Java
  side at CLOSURE_COMMITTED would change what the row models (the reply
  could no longer race through) and would arm the two subjects at different
  boundaries, a coordinator decision. The 90 s budget stays: it is the
  correct bound for the rust-client response_timeout class and harmless
  elsewhere in these two rows.

Gates on commit 8f5c4a7e: fmt exit 0, clippy -D warnings exit 0, 108
passed / 0 failed (the two new tests pin the default/widened/clone-travel
op-wait semantics and the citation list in the observed line).

### M20c: two holes found by an acceptance-mode smoke test of the run_all.sh block (commit 81ff4fe6)

An acceptance-mode smoke test of the run_all.sh block shape (2 rows,
scratch store) found two real holes before the opt-in block could ever
run clean:

- **Relative subject paths broke spawning.** The driver validated
  `--rust-bin` with `is_file()` from the launch cwd, but subjects spawn
  with a scenario-owned working directory, so an explicit RELATIVE
  --rust-bin was resolved by the child against ITS cwd: the smoke test
  failed with `start ./target/release/pipestream-quinn v2 init-authority:
  No such file or directory (os error 2)` on a binary that exists. The
  default (absolute) path is why no M20 dev run ever saw this; the
  run_all.sh block committed at 9842e655 passed relative paths and would
  have failed the same way. Fix: `--rust-bin`/`--java-jar` are
  canonicalized after the is_file gate, and `--artifacts`/`--archive`
  are joined onto the launch cwd when relative (store roots may not
  exist yet, so they are absolutized, not canonicalized). Every recorded
  and spawned path is now cwd-independent.
- **Whole-row waivers converted ANY Fail into WAIVED.** The smoke run
  WAIVED g7-unsafe-clock-refusal over a spawn ENOENT — a real defect
  hidden behind a waiver, against the never-weaken-expectations rule.
  Fix: a whole-row waiver now converts a Fail ONLY when the failure
  names the row's missing capability (the `MissingCapability` display
  prefix, a public constant in scenarios.rs); any other failure stays
  FAILED with "waiver did not match". A waiver over a passing row is
  named "unused whole-row waiver (the row passed)" on the PASS line
  (named as unused — validation cannot see outcomes; rejection there
  was the alternative). Direction-waiver behavior is unchanged.
  `conformance/run_all.sh` now spells every block path absolute via
  $repository_root; the block is otherwise as committed.

Gates on 81ff4fe6: fmt exit 0, clippy -D warnings exit 0, 110 passed /
0 failed (three new tests: matched/unmatched waiver outcomes with the
unused-waiver naming, and the canonicalize/absolutize path handling).
`cargo build --release --locked` reproduces the rust subject
byte-identical (`4156c642b7be…`); only the conformance crate changed.

Evidence (acceptance-mode smokes, both with the deliberately RELATIVE
subject paths, under BENCHMARK.lock, stores under
~/.rfc-tmp/kimi-m20/smoke2/; dev-labelled scratch evidence, never a
full-matrix claim):

- `durable-18d58a7d01f54e67`: exit 0. `PASS g1-leaf-copy
  rust-client/rust-server, rust-client/java-server,
  java-client/rust-server`; `WAIVED g7-unsafe-clock-refusal:
  g7-unsafe-clock-refusal INCOMPLETE: missing subject capability: no
  subject fixture clock: … (waiver: no fixture clock on either subject;
  interface-v1 has no clock-set boundary …)` — the waiver matched the
  missing-capability failure, not the spawn error that provoked this
  fix. run.tsv records the canonical absolute rust_bin and the jar's
  hash 91c1842f… next to the four waiver reasons.
- Wrong-waiver negative check (`--waive g1-leaf-copy=should-not-apply`
  on the green row): exit 0, `PASS g1-leaf-copy … (unused whole-row
  waiver (the row passed): should-not-apply)`.

The full acceptance matrix was NOT run (mandate); the coordination
board is untouched.

### Gates on this tree

`cargo fmt --all -- --check` exit 0; `cargo clippy --all-targets
-p pipestream-conformance -- -D warnings` exit 0; `cargo test -p
pipestream-conformance` 106 passed / 0 failed (98 at the retirement commit,
99 after the kill-row flag, 106 after the waiver mechanism; the new tests
pin the waiver parsing/validation/run.tsv rendering, the marker evidence,
the PARTIAL named-scope rule, the acceptance/dev outcome split, the
retirement, and the flag's Java-only placement). `cargo build --release
--locked` exit 0 reproducing the rust subject byte-identical. The
conformance crate gained no dependency.

Acceptance mode was NOT run (mandate: next milestone after the owner's
review of this dev run).

Follow-up of the same day (the real Kimi, resumed): the M18/M19 review and
both reserved decisions were confirmed as implemented above; the dev-run
evidence was spot-verified (archive `durable-18d5856a211db6af`,
MANIFEST.sha256 5732 entries, four waiver lines in run.tsv). Commit 9842e655
then integrated the acceptance matrix into `conformance/run_all.sh` as an
opt-in block (`PIPESTREAM_DURABLE_ACCEPTANCE=1`, after the Java build, with
the four milestone-20 waivers spelled out), so the proposal in
acceptance-mode-proposal.md is now applied rather than proposed. The
coordination board's Kimi section was updated the same day with the M20
state and the three blocking subject-side markers (two Java findings for
Claude and the rust-CLI control-timeout lane question). The acceptance run
itself remains pending those resolutions.
