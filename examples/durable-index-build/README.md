# durable-index-build (M2)

Small real multi-stage application on the v2 protocol. Builds an inverted
index over a seeded text corpus using protocol parts the transform workload
does not: authority-side expansion (descendant scopes), parent reads of
child outputs, cross-authority result references (merge executor as a
Section-12 consumer), scope cancellation, kill/resume.

## Run

`./run-index.sh WORK` — self-contained gate (~15 min). Builds the Rust
binaries and the Java example jar, mints test PKI, launches one authority
pair per direction, and gates every run. Whole-run lock plus per-command
timeouts; servers are torn down at exit.

## What the gate proves

- Direction symmetry: rust/rust, java-stages+rust-merge, and
  rust-stages+java-merge each run x3 (seed 6, 2 files x 128 words). All
  nine produce index digest
  `693796e4…13596c5` — TF records and the index format are
  byte-deterministic across implementations.
- Cancellation: `--cancel-file 1` at 2 files x 8192 words. The cancel
  waits for the first admitted child, then seals the scope: 4x CANCELLED
  members, scope sums `declared=4 cancelled=4`, scope 0 sums
  `declared=2 success=1 failure=1`, final index excludes exactly the
  cancelled file (digest `5e0c8c58…a87c`).
- Kill/resume: coordinator self-kill after stage-2 admits and after the
  stage-3 merge admit; restart with `--resume` finishes byte-exact with no
  new declarations (replay uses retained operation ids).

## Operating notes (learned the hard way)

- Creation sequences are gapless per authority on both implementations:
  a fresh creation number must be the next one, and reusing a creation
  with different parameters is CONFLICT. The runner numbers creations
  sequentially per authority pair.
- The gate uses 2 files, not the DESIGN default of 8x2048: the example
  authority's default aggregate admission capacity fits ~a dozen
  concurrent admissions, and 8 files x 4 children overshoot it
  (`LIMIT_EXCEEDED: aggregate admission capacity exhausted`). Throughput
  sizing is an operator concern; the gate stays inside the default
  envelope on purpose.
- Each demo group gets a fresh authority pair (wipe + re-init +
  relaunch), and the kill demos settle 40s before resume. The Rust
  authority allows 4 connections per principal (16 in total) and closes
  a connection over that ceiling with LIMIT_EXCEEDED; every actor here is
  one principal, each coordinator run held 2 connections without
  detaching, the merge reader opened one per TF reference, and a killed
  coordinator's connections stay counted until the 60s idle timeout.
  The Java client names that refusal; the Rust client reports it as a
  bare `connection lost` (a client bug, SPEC-FRICTION F3). Readers now
  share one connection per merge and the coordinator detaches at clean
  exit; the kill path stays abrupt (crash simulation).
- Cancel at tiny sizes is timing-sensitive: sent immediately, the cancel
  can seal the scope before expansion admits anything (empty scope,
  vacuous close). The coordinator therefore waits for the first
  admitted member before cancelling; TF work at 8192 words/file is slow
  enough that the queued members then settle CANCELLED.
- Java clients must call `binding()` after `ready()` before any other
  operation (`NOT_READY: session not bound`); the Rust client binds
  implicitly on connect. The Java merge reader does this (see F2).
- Both merge readers detach their per-reference session after the
  digest check (Java always did; the Rust reader learned it here),
  so consumer reads don't linger on the peer.
