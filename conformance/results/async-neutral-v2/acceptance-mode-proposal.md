# Proposal: acceptance-mode integration of the durable driver into conformance/run_all.sh

Status: PROPOSAL, written at milestone 19c in Kimi's role. This is a
proposal for Kimi's review, not an edit: `conformance/run_all.sh` is
unchanged by this milestone. Nothing here is an acceptance claim; the dev
archives under `runs/` are the only evidence that exists.

## 1. What run_all.sh does today

`run_all.sh` builds the Rust workspace (`fmt --check`, `clippy -D warnings`,
`test`, `build --release --locked`), runs `pipestream-conformance verify`
and `modelcheck`, builds and verifies the Java subject with Maven, builds
and tests the C++ subject, builds the examples, and finally runs the
`interop`, `extensions`, `recursive` and `examples` conformance commands.
The durable driver (`pipestream-conformance durable`) is not invoked
anywhere, so the neutral failure/resource matrix is not part of the gate.

## 2. Preconditions the durable command already enforces

Acceptance mode is the driver's default (`--dev` is the opt-out), and it
refuses to start without `--java-jar` or with a jar that does not exist
(`conformance/src/durable.rs`, "acceptance prerequisites unmet"). In
acceptance mode a direction failure is a row FAIL, not an INCOMPLETE marker,
and a row that reports a missing subject capability (`MissingCapability`,
milestone 19b) is a FAIL as well, because a certification cannot be
claimed around a capability nobody has. The run exits nonzero on any FAIL.

## 3. Proposed integration (one block, after the Java build)

Insert after `mvn install ... -f implementations/java-netty/pom.xml`, which
is the point at which the shaded all-jar exists, and before the interop
commands:

```
durable_jar=$(ls implementations/java-netty/target/pipestream-quic-netty-*-all.jar)
durable_store=${PIPESTREAM_DURABLE_STORE:-"$repository_root/implementations/rust-quinn/target/durable-runs"}
implementations/rust-quinn/target/release/pipestream-conformance durable \
  --rust-bin implementations/rust-quinn/target/release/pipestream-quinn \
  --java-jar "$durable_jar" \
  --artifacts "$durable_store" \
  --archive conformance/results/async-neutral-v2/runs
```

Notes on each line:

- No `--dev`: acceptance mode, the whole matrix, both directions, every
  R row. The command fails the script on any FAIL row through `set -e`.
- `--rust-bin` is the release binary the script just built, so the subject
  hash recorded in `run.tsv` is the build under test, not a stale one.
- `--java-jar` is the all-jar the Maven step just produced; the driver
  records its hash in `run.tsv` and re-hashes it after every scenario
  (`nc-stale-binary`).
- `--artifacts` is the store for subject roots, journals and payloads.
  On this host it must NOT be under `/work` (the RAID fsync latency
  documented in async-java-v2/raw/host-fsync-2026-09-12.md distorts every
  timing row) and not under `/tmp`; the environment override
  `PIPESTREAM_DURABLE_STORE` lets a host point it at a suitable drive, with
  `TMPDIR` set alongside it. The default stays inside the repository's
  target directory for hosts without that constraint.
- `--archive` copies the run into `runs/` with `MANIFEST.sha256`, which is
  the form every milestone's evidence already takes, so an acceptance run
  is reviewable the same way as a dev run.

## 4. What must be true before the block is enabled

1. The three rows that report a missing subject capability
   (`g2-drop-reply-publication`, `g7-unsafe-clock-refusal`,
   `g7-cleanup-interrupted-refund`) FAIL in acceptance mode by design. Kimi
   must decide, per row, between an interface revision that both subjects
   implement, a matrix decision that retires or re-scopes the row (the
   g2-drop-reply-publication proposal in scenario-matrix-g2.md), or an
   explicit `--waive <row>` flag that records the waiver in `run.tsv`.
   Without one of those the acceptance run cannot exit zero, and it should
   not be made to by silently downgrading those rows.
2. The per-direction INCOMPLETE set of the dev matrix (handoff section 3j,
   M19c) must be empty or each entry must be accepted as a named gap with
   the same explicit mechanism, because in acceptance mode those directions
   FAIL the row: the four java-client/rust-server directions that time out
   on the Java client's graceful shutdown after a server kill (a Java
   finding already reported), the Java-server named gaps
   (g4-revocation-vs-publication, g3-store-ownership's client direction),
   and the Rust-server hold gap of g7-deadline-queue-time.
3. The three PARTIAL R rows (`r-staging-and-journal-bounds`,
   `r-network-bytes`, `r-native-credit`) return SCENARIO OK with
   `row_status PARTIAL` in `observed.tsv`. A reviewer must decide whether
   PARTIAL is acceptable for certification on a host without packet capture
   or a network namespace, or whether the acceptance run must be made on a
   host that grants them.
4. Wall time. The M19c dev matrix took about one hour on this host with the
   Java subject at `-Xms256m -Xmx2g`; `run_all.sh` currently takes minutes.
   The block should be guarded by an opt-in variable
   (`PIPESTREAM_DURABLE_ACCEPTANCE=1`) until the gate is expected to pass,
   so the existing script does not become red for every contributor while
   the decisions above are pending.
5. The benchmark lock. On the shared host every durable run must be taken
   under `flock /work/worktrees/pipestream-rfc-coordination/BENCHMARK.lock`;
   that is a host rule, not a script rule, and belongs in the invoking
   environment, not in `run_all.sh`.

## 5. What this proposal does not do

It does not change `run_all.sh`, does not add a `--waive` flag to the
driver, and does not claim that an acceptance run would pass today: with
the M19c dev archive as the best available prediction it would FAIL on the
three missing-capability rows and on the per-direction INCOMPLETE set in
section 3j of the handoff.
