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

## Known limits

- The SPI layer is additive; the protocol stacks' existing SQLite stores are
  untouched. Adoption (wiring subjects to the SPI) and the conformance
  driver's per-backend acceptance runs are the follow-on work.
- Mongo backend tested against mongo:8.2 (8.0 refuses kernels ≥ 6.19,
  SERVER-121912).
