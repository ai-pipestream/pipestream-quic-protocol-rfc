# Storage SPI verification record

Verification evidence for the pluggable storage SPI (design:
[storage-spi-design.md](storage-spi-design.md)). All results reproducible from
the pinned artifacts in `test-vectors/storage-spi/`.

## Lanes

| Lane | Branch | Head | Suite |
|---|---|---|---|
| Java (`ai.pipestream.quic.storage`) | `agent/storage-spi-java` | `1996085f` | 409 tests, 0 failures (`mvn -q test -Dtest='ai.pipestream.quic.storage.**'`) |
| Rust (`pipestream-storage`) | `agent/storage-spi-rust` | `1cab5237` | 81 tests incl. docker contract runs + adversarial/concurrency/leak/property suites, 0 failures; `clippy -D warnings` clean |

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
- Adversarial swarm round (2026-09-20/21): dedicated adversarial, concurrency,
  leak-audit, and property suites on both lanes. Java: 8 defects fixed
  (sealed-retire scope-locality `fbc5ddb8`/`aa056e1b`, forged-entity mapping
  `87f95782`, journal marker data-loss guard + crafted mid-log garbage +
  partial-magic mapping `ac06a4ab`, Postgres bootstrap connection leak
  `5ed7108d`, DurableHost close-abandonment `fe219e73`, cold-root open race
  mapping `7292b74b`), 406-test gate green twice. Rust: no product defects;
  the four special-attention items were confirmed correct with named tests.
- Cross-language parity follow-up: the Rust journal adopted the Java marker
  semantics (a wiped log on an initialized root is CORRUPT, never a silent
  fresh start) in `2f961afb`, so both lanes fail data loss identically.

## SPI-plane certification runs (2026-09-20)

Driver `fa3f393e` (`--storage-plane spi`, recorded in run.tsv), subjects built
from `agent/storage-spi-rust` (`06c27638`) and `agent/storage-spi-java`
(`1c729222`), acceptance mode, full 66-row matrix:

| Run | Result |
|---|---|
| `durable-18d6fbf9b0f2cd9f` | FAIL at startup on every row — rustls CryptoProvider panic in the Rust subject (feature-resolution shift from the new crate edge); fixed by `06c27638` (ring pinned). Process worked as intended: the matrix caught what unit gates missed. |
| `durable-18d6fd296148339e` | 63 PASS + 4 waivers + 1 FAIL: the documented g2 host-load flake |
| `durable-18d702e2f77fb30a` | 63 PASS + 4 waivers + 1 FAIL: identical flake signature |
| `durable-18d7051e17c5c0f2` | **GREEN: exit 0, 64 PASS + 4 named waivers, all directions of all rows including g2-kill-after-admission-before-publication.** The SPI plane (fs backend) is certified equivalent to the v2 baseline. |

### Per-backend runs (subjects wired to SPI service backends)

| Backend | Run | Result |
|---|---|---|
| postgres (docker postgres:16-alpine) | `durable-18d70ba1e24ae5d7` | invalid: stale driver binary ran fs-backed (caught by root-cause) |
| postgres | `durable-18d711b081707730` | 62 PASS + 1 flake + 1 real FAIL: r-memory-ladder — the Rust postgres backend buffered whole payloads (BYTEA materialization); fixed by `a011e529` (honest streaming in postgres/s3/mongo) |
| postgres | `durable-18d7160e15db8238` | 63 PASS + g2 flake only; memory ladder flat (streaming verified by the matrix) |
| postgres | `durable-18d718628b307c3e` | 63 PASS + g2 flake only |
| s3 (docker quay.io/minio/minio) | `durable-18d73070ac406e00` | 55 PASS + 8 FAIL: the matrix caught two Rust S3 writer defects the contract suite missed — a panic on 5-10 MiB payloads (single part under the multipart threshold with a deferred part 1; fixed by `4251690e`) and a panic on zero-byte payloads (entity header staged lazily on the first `write()`, which never comes for an empty payload; fixed by `4a6ef2dd`, with a contract-suite regression case every backend now runs) |
| s3 | `durable-18d73368a32a51c7` | invalid: operator error — the conformance server binary is package `pipestream-server`, and rebuilding `-p pipestream-server` was missed (only `-p pipestream-quinn`, the quic library), so the run exercised the pre-fix binary; recorded so the trap is named |
| s3 | `durable-18d735d11e098c40` | 63 PASS + g2 flake only; row-for-row identical to the postgres pattern |
| mongo (docker mongo:8.2) | `durable-18d7380f776e6c78` | 19 PASS: cross-language schema collision in the shared probe database — the lanes' mongo document shapes differ (Rust: compound `_id`; Java: top-level `scope_path`), and Java's `listScopes` distinct NPE'd on Rust documents; fixed by `14938090` (typed distinct + partial unique indexes, Java) and `2f961afb` ($type-filtered distinct, Rust) so each lane's global enumeration sees only its own schema |
| mongo | `durable-18d73c1789d2c4fc` | 61 PASS + 3 FAIL: (1) the Java backend stored payloads inline in one BSON document, so 16 MiB entities exceeded the document limit — fixed by `073c1ce0` (1 MiB chunking, streamed reads/verify, retire sweeps chunks); (2) both r-rows failed at their closing step: the authority SPI plane's orphan sweep treated the shared database as single-owner and a concurrent installation's sweep retired this installation's committed entities — fixed by `dc24816c` (Rust) and `1996085f` (Java mirror): the plane scope's first component is now an installation tag from the store binding, and sweeps skip every other namespace |
| mongo | `durable-18d753f0bf0b1623` | **GREEN: exit 0, 64 PASS + 4 named waivers, all directions of all rows** — including g2-kill-after-admission-before-publication (quiet host) and the two previously-failing resource rows |

Postgres certification statement: row-for-row identical to the certified fs
plane, including the flake pattern; the only backend-caused failure found
(buffered payloads) was fixed and its fix is matrix-verified.

S3 certification statement: after the two matrix-caught writer fixes, the S3
object plane is row-for-row identical to the certified fs plane (63/64, only
the documented g2 host-load flake, which is plane-independent).

Mongo certification statement: three matrix-caught defects (schema-collision
NPE, the 16 MiB document limit, and the shared-database sweep race) were each
fixed with pinning tests in both lanes where applicable; the rerun is fully
green with zero flake allowances.

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

Known finding, plane-independent (not fixed here; pre-existing v2 authority
behavior): FixedRecords' per-entity WAL charge (~250 KB,
FixedRecords.java:376-377, 640-667) makes the g1-declaration-capacity funding
refusal borderline against the physical WAL offset (bounded_vfs.c:130-133),
so the refusal at the capacity boundary is checkpoint-timing nondeterministic
on the Java authority (3/6 repro iterations committed instead of refusing).
The harness is being fixed to source refusal evidence from the first failure.
