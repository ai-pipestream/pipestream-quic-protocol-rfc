# Storage SPI verification record

Verification evidence for the pluggable storage SPI (design:
[storage-spi-design.md](storage-spi-design.md)). All results reproducible from
the pinned artifacts in `test-vectors/storage-spi/`.

## Lanes

| Lane | Branch | Head | Suite |
|---|---|---|---|
| Java (`ai.pipestream.quic.storage`) | `agent/storage-spi-java` | `cfe3c1ed` | 225 tests, 0 failures (`mvn -q test -Dtest='ai.pipestream.quic.storage.**'`) |
| Rust (`pipestream-storage`) | `agent/storage-spi-rust` | `f7a37288` | 28+ tests incl. docker contract runs, 0 failures; `clippy -D warnings` clean |

Both lanes implement all six backends — in-memory, filesystem, Postgres, S3
(object role), Kafka (journal role), MongoDB — against the pinned decisions in
the design note. Every backend passes the shared per-role contract suites; the
docker-backed backends run the suites against real services (postgres:16-alpine,
quay.io/minio/minio, apache/kafka:4.2.0, mongo:8.2).

## Cross-language layout verification (2026-09-20)

Probe: `test-vectors/storage-spi/probe-script-v1.txt`; canonical report:
`expected-report-v1.txt` (computed independently via python3 hashlib).

| Run | Result |
|---|---|
| Rust probe, fresh fs store | byte-identical |
| Java probe, fresh fs store | byte-identical |
| Java `--report` on the Rust-written fs store | byte-identical |
| Rust `--report` on the Java-written fs store | byte-identical |
| Java probe, all six backends (docker) | byte-identical per backend (role-omission rules for S3/Kafka) |
| Rust probe, docker backends | byte-identical per backend |

Crossed-store note: the fs store written by one language verifies clean under
the other (`verify_scope` passes; canonical report matches). The writer-lock
interlock across languages is fcntl `F_SETLK` on both sides (pinned decision 7).

## Audit history

- Java slice 1: independent audit, 2 blockers + 2 should-fix + 5 nits, all
  fixed with proving tests (commit `8cc7a158`).
- Rust crate: independent audit, 4 blockers + 10 should-fix + 4 nits; round-2
  re-audit at `d13a5a13` confirmed all blockers byte-verified fixed; follow-ups
  (fcntl lock family, journaled Mongo counters) at `01033587`.
- Contract divergence arbitration during the work: entity/scope ids are 32-bit
  per RFC Section 6 (both lanes narrowed to match); `verify_scope` covers
  store-owned state only (payload rehash + entity count) and returns the
  recorded seal verbatim — the protocol SCOPE_DIGEST covers statuses the store
  does not hold.

## SPI-plane certification runs (2026-09-20)

Driver `fa3f393e` (`--storage-plane spi`, recorded in run.tsv), subjects built
from `agent/storage-spi-rust` (`06c27638`) and `agent/storage-spi-java`
(`1c729222`), acceptance mode, full 66-row matrix:

| Run | Result |
|---|---|
| `durable-18d6fbf9b0f2cd9f` | FAIL at startup on every row — rustls CryptoProvider panic in the Rust subject (feature-resolution shift from the new crate edge); fixed by `06c27638` (ring pinned). Process worked as intended: the matrix caught what unit gates missed. |
| `durable-18d6fd296148339e` | 63 PASS + 4 waivers + 1 FAIL: the documented g2 host-load flake |
| `durable-18d702e2f77fb30a` | 63 PASS + 4 waivers + 1 FAIL: identical flake signature |

The g2 row fires identically on the v2 plane with identical builds (control
runs), so the SPI plane introduces no divergence; every other row passes with
byte-identical finals to the baseline. A clean full-pass run on a quiet host
remains the final certification artifact.

## Known limits

- The SPI layer is additive; the protocol stacks' existing SQLite stores are
  untouched. Adoption (wiring subjects to the SPI) and the conformance
  driver's per-backend acceptance runs are the follow-on work.
- Mongo backend tested against mongo:8.2 (8.0 refuses kernels ≥ 6.19,
  SERVER-121912).
- Row `g2-kill-after-admission-before-publication` (java-client/rust-server
  direction) is a known host-load flake on any driver build: the fixture kill
  at EXECUTION_CLAIMED races the Java client's post-receipt detach handshake
  (strace-verified 2026-09-20; 16-run repro shows both driver builds flaking
  on both local drives; identical failure exists in pre-change archive
  durable-18d3ea906e9d11bd). Signature: `admit operation failed` +
  `LIMIT_EXCEEDED: control response deadline`. Acceptance runs need a quiet
  host for this row; the fix options are subject/row-side, recorded in the
  run-2 analysis.

## Adoption slice 1: SPI over the existing client journals (2026-09-20)

Both lanes now expose their proven v2 client journals through the SPI
`Journal` role, each certified by the shared journal contract suite:

- Java: `ClientJournalAdapter` (`3b91437c`) — bridge table in the same SQLite
  file (dense global cursors; commits consume cursors), subprocess
  SIGKILL-recovery proof through SPI replay. 240 storage tests green; the v2
  `ClientJournal` suite untouched and green. The typed protocol journaling
  surface (`journalMutation`/`journalInput`/`journalReceipt`) is documented
  as unbridgeable for opaque SPI bytes rather than faked.
- Rust: `ClientJournalBridge` (`c64fcb1e`) — `append_intent = prepare`,
  `commit = record_receipt`, replay = ordered read-only scan (not
  `unresolved`, which filters committed work). Cursor mapping is sparse
  (intent rowid r → cursor 2r-1, commit → 2r) because the existing journal
  gives receipts no independent position; the contract suite runs it under a
  `JournalProfile::SPARSE` mode instead of faking dense cursors. Verified
  locally: full `pipestream-storage` suite green (incl. docker backends),
  clippy `-D warnings` clean, 63/63 `v2::client` regression tests pass.

Asymmetry, documented: the Java bridge produces dense cursors (mapping
table), the Rust bridge runs sparse. The six certified SPI backends are all
dense; the profile concept exists so adapters over pre-existing journals can
declare their cursor shape honestly. `pipestream-core` now depends on
`pipestream-storage` for SPI types only — no behavior change.
