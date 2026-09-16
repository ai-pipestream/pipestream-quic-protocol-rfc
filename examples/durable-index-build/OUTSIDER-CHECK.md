# M4 outsider check — fresh-clone build, run, and gate (2026-09-16)

Method: fresh `git clone` of `origin/agent/rfc-meta-workload-v2` at
`b7d8f5f8` to `/home/krickert/.rfc-tmp/outsider` ( Rev-parse matches the
worktree). Followed only the checked-in READMEs. Same machine, so
`~/.m2`, `~/.cargo`, and `/work/reference-code` caches are shared —
this is a fresh-CLONE check, not an air-gapped-machine check.

## Results

- Rust (`implementations/rust-quinn/README.md`): `cargo test --locked`
  PASS — 888 passed, 0 failed. First-compile wall ~2:14.
- Java (`implementations/java-netty/README.md`): transport `build.sh`
  OK (~2:12, artifacts + isolated repo under `/work/reference-code/`);
  `mvn -Dmaven.repo.local=$transport_repository verify` FAILS — 799
  tests, 1 failure (see Bug 2). Not clone-specific: identical failure
  in the main worktree at the same commit.
- C++ (`implementations/cpp-msquic/README.md`): `cmake -S . -B build`
  OK (~1:33, fetches MsQuic v2.6.1), `cmake --build build` OK (~0:02),
  `ctest` 1/1 PASS.
- Example gate from the clone:
  `examples/durable-index-build/run-index.sh` → GATE PASS: 9 direction
  reps (rust x3, mixed x3, rev x3 — both cross-client directions),
  cancel demo, 2 kill/resume stages, all digests pinned `693796e4…`
  (~4:59 wall).

## Bug 1: transport/build.sh not executable in a fresh clone

`implementations/java-netty/transport/build.sh` is mode 100644 in the
git index (every other repo `.sh` is 100755, including the top-level
`build.sh` and the example `run-index.sh`). The README's first command,
`./build.sh`, fails with permission denied on a fresh clone.
Workaround: `bash build.sh`. Fix: `git update-index --chmod=+x` on
that file (applied in this branch — mode change only).

## Bug 2 (pre-existing, not clone-specific): `mvn verify` has 1 failure

`PeerRuleWireTest.streamIdsAreNeverRecycledAcrossALongConnection`
fails: entity ~28 of 100 transfers is refused with
`LIMIT_EXCEEDED "retained input, output or executor capacity"`
(`AdmissionStore.java:297`) where an `AdmissionResponse` is expected.
The test's own `host()` helper comment (lines 49-50) acknowledges the
funding "refuses admissions past about 28", while the test loop demands
100 — the test is internally inconsistent. Suggested fix for the impl
owner: fund the test host for the 100-member scope or cut transfers to
within the funded envelope. Reproduces in isolation and in the main
worktree at the same commit — no environment or clone-setup cause
(32 cores / 121 GB box; failure is deterministic, entity 28 then 29).

## Caveats for a truly fresh machine

- `run-index.sh` builds Rust with `cargo build --offline`: needs a warm
  cargo cache or it fails where network crates are unavailable.
- The Java example (`mvn -q package -DskipTests`) resolves the
  `pipestream-quic-netty` artifact from `~/.m2`, not from the clone —
  the clone's own `verify` currently cannot `install` it (Bug 2 blocks
  the `package` phase; `install -DskipTests` would be required).
