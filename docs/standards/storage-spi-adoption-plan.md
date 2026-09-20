# Authority-side storage SPI adoption plan (Java)

Status: decision proposal, 2026-09-20. Slice 1 put the v2 client journal behind
the SPI `Journal` role (`ClientJournalAdapter`, commit `3b91437c`, recorded in
`storage-spi-verification.md`). This note plans slice 2: the authority-side
object plane — v2 `InputStore`/`OutputStore` payload bytes — behind the SPI
`ObjectStore` role. It is a plan only; no production code changes.

## 1. What the code does today

`InputStore` (`implementations/java-netty/src/main/java/ai/pipestream/quic/v2/InputStore.java`)
owns one private directory (`writer.lock`, `policy.cbor`, `pending/`,
`objects/`, `reservations/`, `outputs/`, `output-pending/`, `ROOT_NAMES`
InputStore.java:50-58) for one authority SQLite installation. Public surface:

- Reception: `begin(...)` (InputStore.java:510) reserves quota *2 for staging
  plus installed copy, returns a streaming `Receiver` writing a
  `pending/<uuid>.part` file; `Receiver.finish` (InputStore.java:1455) verifies
  FIN, forces the file, hard-links it to the immutable name, syncs `objects/`,
  and optionally pins it (`installedPins`, the admission-handoff pin).
- Input reads: `find` (InputStore.java:595) fully re-verifies header + digest,
  forces the file and directory on every call; `Stored.openStream`
  (InputStore.java:1598) re-verifies again and pins a reader.
- Output funding: `reserveOutputs`/`findReservation` (InputStore.java:625,
  697) install a `[0-9a-f]{64}.funding` record prepaying two copies of every
  output byte/name; `outputFunding`/`outputReference` (InputStore.java:911,
  928) re-derive names for recovery and job rows.
- Outputs: delegated to `OutputStore`, same file: streaming `Writer` into
  `output-pending/`, `finish()` rewrites the metadata header with the payload
  digest, links into `outputs/` (OutputStore.java:677-719); `find`,
  `openPublished`, `verifyCount` (OutputStore.java:338, 370, 395) verify +
  force + sync on every access.
- Reclamation: `reclaimInput` (InputStore.java:1266), `reclaimOutput` (973),
  `reclaimOrphan` (1149), `OrphanScan` (1030) — every deletion requires a
  paired committed SQLite decision and refunds quota only after the directory
  sync. `recover()` (InputStore.java:1699) audits every retained body and
  funding record *before* deleting staging, and asserts the name grammar
  (`[0-9a-f]{64}\.input|\.funding`, UUID `.part`, InputStore.java:1729-1736).

Where `SessionStore` touches this: `bindInputs`/`verifyInputs` write and
re-check `ps_v2_meta.input_id` inside a SQLite transaction while holding the
input-store monitor (SessionStore.java:347-419); the generation gate
(SessionStore.java:422) re-checks session liveness from the database under the
same monitor; `AdmissionStore.admit` calls `inputs.find` + `inputs.reserveOutputs`
inside the admission writer transaction (AdmissionStore.java:379-385); the
release path re-derives `inputReference`/`outputReference` and refuses any
mismatch (SessionStore.java:2910-2916), then deletes under the monitor
(2967-2968); `reclaimOrphan` pairs each physical deletion with a `BEGIN
IMMEDIATE` metadata reconciliation (SessionStore.java:3024-3079).

## 2. Mapping SPI operations onto the v2 stores

| SPI (`ObjectStore.java`) | v2 counterpart | Gap |
|---|---|---|
| `put` (:29) idempotent, digest-verified | `Receiver.write/finish` staging → link → sync (:1405-1488); duplicate-link branch re-verifies (:1469-1476) | v2 streams from transport under idle deadlines; SPI takes a whole `byte[]`. Names derive from the envelope digest, not caller-chosen ids |
| `openRead` (:43) read survives removal | `Stored.openStream` (:1598), `OutputStore.Stored.openStream` (:867) with pin accounting | v2 additionally re-verifies identity header + full digest on every open; SPI verifies at `put` |
| `sealScope` (:60) | No counterpart. Nearest: funding-record install + SQLite job commit | v2 has no per-scope seal record or SCOPE_DIGEST; admission state lives in SQLite |
| `verifyScope` (:76) rehash-all | `find`/`inspect` per-object rehash (:1883-1946); `recover()` audit (:1699-1755) | per-scope batch verification with caller-recorded digest does not exist |
| `list` (:87) | `OrphanScan` pages `objects/` + `reservations/` (:1030-1089) | returns envelope-keyed candidates, not entity ids; scoped listing absent |
| `retire` (:100) with retention cursor | `reclaimInput`/`reclaimOutput`/`reclaimOrphan`, `OutputStore.reclaim`/`reclaimTerminal` (:423, 595) | keyed by committed eligibility evidence, not a numeric cursor; cursor semantics have no v2 analog |

SPI semantics with **no** v2 counterpart: scope/entity addressing and the
scope tree (`ScopePath`), random content-addressed keys (v2 names are derived
from a signed envelope; an uploader cannot choose its object name), the
`StorageException` taxonomy (Transient/Corrupt/AlreadySealed/NotFound vs
`IOException`/`ProtocolError`), and sealed-scope retire-whole.

v2 semantics with **no** SPI counterpart (must survive any adoption): output
funding/reservations (prepaid two-copy allowances, `funding()` :1818), the
in-memory quota and handle accounting (`reserve`/`release`/`pin`,
:1985-2123), `.part` staging with link+sync+delete, `writer.lock` +
`policy.cbor` installation identity, the generation gate, `installedPins` and
reader/writer pins, the pending-removal charge maps (`orphanRemovals`,
`inputRemovals`, `outputRemovals`), and the recovery name-grammar assertions.

## 3. The two-system commit, and where ObjectStore slots in

The design note's rule (`storage-spi-design.md`: object write first, journal
commit second, reconcile on recovery) is already the v2 authority's operating
mode, with SQLite as the "journal":

1. Object first: `DurableServer` finishes the receiver — object linked, file
   forced, `objects/` synced, pinned (DurableServer.java:1243) — before any
   metadata transaction exists.
2. Metadata second: `host.sessions().admit(...)` runs the SQLite writer
   transaction; `inputs.find` + `inputs.reserveOutputs` install/check the
   funding file inside it (AdmissionStore.java:379-385) and the job row +
   receipt commit (AdmissionStore.java:446-475); only then `stored.release()`
   drops the pin (DurableServer.java:1268-1270).
3. Reconcile: a kill between (1) and (2) leaves a pinned-then-orphaned object
   or funding record; `recover()` audits and recharges it (:1699-1753), and
   the retention sweep reclaims it only after `SessionStore.reclaimOrphan`
   proves from committed metadata that it was never admitted
   (SessionStore.java:3055-3075). Installed inputs *without* the pin would be
   reclaimed mid-handoff — that was handoff defect 10 (InputStore.java:1444-1449).

So the SPI does not introduce a new consistency model here; it replaces the
payload-byte plane that steps (1) and (3) touch. The seam is inside
`InputStore`/`OutputStore`, between what must stay v2 (envelope identity,
funding/quota, pins, orphan charging, grammar) and what is plain durable byte
storage: `Receiver.write/finish`, `Stored.openStream`, `OutputStore.Writer`,
`Stored.openStream`, and the payload hashing in `inspect`. Plane selection
belongs at the construction boundary, `DurableHost.initialize/open`
(DurableHost.java:1164-1166, 1205), where the store is created and bound.

## 4. Options

**(a) Adapter over the existing InputStore** (mirrors `ClientJournalAdapter`).
Implement `ObjectStore` by delegating to `Receiver`/`find`/`Stored`. Rejected
as the primary path: SPI addressing is caller-chosen scope/entity ids and SPI
`put` takes a whole `byte[]`, while v2 names derive from the full signed
envelope and reception is streaming under transport deadlines — the adapter
would need an id↔envelope dictionary and would buffer payloads, breaking the
bounded-memory property. Worth keeping later as a read-only audit bridge.

**(b) New SPI-native payload plane behind a config flag (recommended).**
Keep `InputStore` as the authority for envelope identity, funding, quota,
pins, orphan charging, and recovery grammar; move payload bytes to an
`ObjectStore` backend, selected per installation by a new authority config
flag (default: v2). Mapping: one scope per operation (scope components derived
deterministically from the envelope digest), entity 0 = input payload,
entities 1..n (or a child scope) = outputs. Reception staging stays a local
scratch file under the SPI root (FileObjectStore already stages scratch under
its root); on FIN the bounded `objectBytes` payload is `put` once with its
verified digest — a single bounded copy, the same class of bound as today's
`METADATA_LIMIT`. Envelope metadata stays in a small v2 sidecar so `recover()`
keeps its identity audit; the SPI seal digest can be the canonical content
digest (`StoreLayout.scopeDigest`) but no protocol SCOPE_DIGEST exists yet, so
`sealScope`/`verifyScope` are used as store-internal bookkeeping, not as a
wire-visible commitment. Funding (`reserveOutputs`) stays entirely v2: SPI has
no reservation semantics and faking them in an adapter repeats the slice-1
lesson — document unbridgeable mappings honestly rather than fake them
(`ClientJournalAdapter.java:43-59`).

**(c) Full replacement** of `InputStore`/`OutputStore` by an SPI-native
implementation. Rejected: it would re-derive funding, orphan charging,
generation gating, and recovery auditing on a contract that has no words for
them, and it invalidates the recovery grammar and every crash test in one
step.

### Stepwise plan for (b)

1. **Pin the grammar before touching it.** Add focused tests asserting
   today's `recover()` name rules, `ROOT_NAMES`, and `outputFunding`'s
   `.funding` check (InputStore.java:913). Proof: new tests pass unmodified on
   the current tree; they are the regression net for every later step.
2. **Config surface + binding.** Authority `Configuration` gains
   `objectPlane: v2|spi` plus backend selection; `ps_v2_meta` records the
   plane so `open` refuses a mismatched installation, and `DurableHost`
   constructs the plane at the existing seam. Proof: `StoreBindingTest`,
   `StoreBindingRecoveryTest`, and config-parse tests green; default is v2
   with zero behavioral delta.
3. **Input payload plane (flag on).** Route `Receiver.finish` storage and
   `Stored.openStream` through the backend; envelope sidecar keeps `find`'s
   identity check. Proof: the full `InputStoreTest`/`InputStoreRecoveryTest`
   suite passes under the flag, including the `Probe`-phase crash rows
   (`Runtime.halt` per phase), plus a new kill/resume row with the flag on.
4. **Output payload plane (flag on).** Same for `OutputStore.Writer`/
   `Stored.openStream`; funding, budget, and reclaim checks stay v2.
   Proof: `InputStoreFunding*`, output publication, and branch-recovery suites
   under the flag.
5. **Backend-swap certification.** Run the authority conformance matrix with
   filesystem, then S3/Postgres backends; byte-identical finals to the v2
   baseline, including mixed-backend runs, per `storage-spi-design.md`'s
   certification section. Record results in `storage-spi-verification.md`.
6. **Flip and migration path.** Only after step 5's evidence: new
   installations default to the SPI plane, v2 plane stays readable; an
   offline tool (or lazy recovery-time copy) migrates retained v2 payloads.

**Rollback.** The flag is per installation and pinned in `ps_v2_meta`; until
step 6 both planes are maintained, so rollback is reopening with the v2 flag.
After step 6, rollback requires the migration tool to copy payloads back —
which is why the flip waits for full certification.

## 5. Out of scope, and why

- **Fixed-capacity records and rewrite credits** (`FixedRecords.java:38-50`,
  `JOB_CREDITS`, `ADMITTED_WORK_CREDITS`; `protect`/`grow`/`allocate` at
  AdmissionStore.java:395-454). These bound and fund SQLite metadata rewrites;
  they are part of the metadata plane's capacity proof, not object storage.
  Moving them changes the database format and the promised-write accounting.
- **The bounded SQLite VFS quota machinery** (`BoundedSqlite.Limits`,
  BoundedSqlite.java:23-56: 256 MiB database / 64 MiB WAL/journal ceilings).
  It protects the metadata file the SPI explicitly does not own; object-plane
  capacity remains the v2 `Limits` quota enforced above the backend.
- **Authority metadata behind the SPI `Journal` role.** SessionStore is
  already a transactional store with its own recovery; the two-system rule is
  realized by monitor-paired bind/verify + orphan charging, not by a journal.
  A separate evaluation would be needed before proposing that, and this plan
  does not.

## 6. Risks

- **Recovery-grammar coupling.** `recover()`, `OrphanScan`,
  `sessionHasResources`, `retentionTargets`, and `OutputStore.parse` all parse
  or assert the flat name grammar (InputStore.java:1729-1736, 2053-2088;
  OutputStore.java:1214-1220). Moving payloads to the scope tree changes what
  a "foreign entry" is. Mitigation: the v2 namespaces shrink to sidecars +
  funding records; step 1's grammar tests pin what remains; plane-specific
  audits stay behind the plane boundary.
- **Concurrent sweeps.** Orphan scanning, session retirement, and result
  sweeps serialize on the input-store monitor and are paced by page sizes
  (RetentionService.java:179-229). An SPI backend has its own concurrency
  (FileObjectStore's single-writer fcntl lock; S3 list latency and weak
  consistency) that does not compose with that monitor. Mitigation: keep all
  eligibility decisions and pacing in v2 code; the backend sees only single
  scoped operations; certify sweep behavior under kill/resume rows, not just
  steady state.
- **Fixture hooks that touch file layout.** The `Probe` phases drive
  injected-failure crash tests (InputStore.java:129-185); fixture boundaries
  `INPUT_INSTALLED`/`ADMISSION_COMMITTED` fire between object install and
  admission commit (Boundaries.java:28-30; DurableServer.java:1243-1266);
  tests read `policy.cbor` directly (StoreBindingRecoveryTest.java:101), list
  `outputs/` (BranchExecutionRecoveryTest.java:180), and assert `usage()`
   accounting (SessionRetirementTest.java:151). Mitigation: the plane must
  expose probe boundaries and byte/file accounting equivalents under the
  flag, or those tests legitimately fork by plane.
- **Accounting drift.** Quota stays v2 while bytes live elsewhere; a backend
  that succeeds at `put` but whose bytes exceed the v2 reservation (or vice
  versa) needs a defined order — v2 reservation first, SPI `put` second, and
  a failed `put` after a successful reservation leaves a funded-but-empty
  scope the orphan path must reclaim.
