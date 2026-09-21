# Storage SPI adoption plan — Rust authority side

Status: analysis. No production code changes proposed here; the SPI crate
(`implementations/rust-quinn/storage`) and its client-journal bridge
(`src/v2/client/bridge.rs`) are landed and certified. This document plans
how the **authority-side** payload store could adopt the SPI.

References: `src/v2/authority/payload.rs` (PayloadStore),
`src/v2/authority/payload/audit.rs`, `src/v2/authority/ingress.rs`,
`docs/standards/storage-spi-design.md`.

## 1. Operation mapping: PayloadStore → SPI ObjectStore

| PayloadStore operation | SPI ObjectStore | Fit |
|---|---|---|
| `stage()` + `StagedPayload` writes + `finish()` (payload.rs:567, 1087) | `put(scope, entity, bytes, digest)` | Partial. SPI `put` is one-shot atomic; staging is multi-call streaming with a deadline (`receiver.check_deadline`, payload.rs:1073). The install step (`install_file` rename + dir fsync, payload.rs:266, 1107-1115) is exactly what an SPI filesystem backend does internally. |
| `open_object(key, owner, input)` (payload.rs:636) | `open_read(scope, entity)` | Partial. SPI reads are bare streaming; PayloadStore readers verify the descriptor against inventory and re-hash content on read (`ObjectReader`, payload.rs:690-700). The SPI's digest-at-put + verify_scope gives equivalent integrity, weaker per-read liveness (no pin counters). |
| seal record in `binding` config file (payload.rs:376-393) | `seal_scope` + `verify_scope` | No semantic overlap: the config file binds store identity, not scope digests. SPI scope sealing has no PayloadStore counterpart at all (see §2). |
| `collect_payload_orphans` / `collect_until` (payload.rs:884, 716) | `retire(scope, cursor)` | Conceptual match only: orphan collection is reference-driven (payload_refs join, payload.rs:917-940), not cursor-driven. |
| `usage()` inventory scan (payload.rs:532) | `list(scope)` | Conceptual match; keyed differently. |
| reservations (`reserve-*`/`funding-*`, payload/reservations.rs) | none | No SPI counterpart (see §2). |

## 2. SPI semantics with no counterpart in PayloadStore

- **Addressing.** PayloadStore keys are random 128-bit hex generated at
  stage time (payload.rs:596-606); the SPI addresses `(ScopePath,
  EntityId)` deterministically with fanout buckets. An adapter must keep a
  key ↔ (scope, entity) side table — the authority's `payload_refs` table
  already carries `object_key` next to `(generation, scope, entity)`
  (payload.rs:920), so the mapping exists in SQLite today.
- **Sealing.** PayloadStore objects are immutable once installed, but there
  is no scope seal/verify lifecycle; integrity is per-object (digest header
  + read-time rehash). SPI `seal_scope`/`verify_scope` would be new
  authority-side state, stored in the SQLite authority db, not in the
  payload root.
- **Reservations.** `reserve-*`/`funding-*` files and their accounting
  (payload/reservations.rs; scanned at payload.rs:486-490) fund future
  output writes. The SPI has no reservation concept; they would stay in
  the filesystem backend's root (or be re-implemented per backend).
- **Quota machinery.** `check_capacity`/`check_handles`/charges are
  policy-driven and in-memory-inventory backed (payload.rs:594-595); the
  SPI is deliberately silent about quotas.

## 3. Where the two-system rule already holds

PayloadStore is already operated as the object role of the design note's
two-system configuration:

- **Object install before DB commit**: reception finishes (stage file →
  object rename + fsync, payload.rs:1087-1117) *before* the authority's
  admission/reference transaction commits (ingress.rs:300, `commit(tx,
  "prepare-input")`). Objects never become referenceable before install
  (comment at payload.rs:3).
- **Reconcile on recovery**: orphans are objects without a committing
  reference; `collect_payload_orphans` (payload.rs:884) runs under the
  authority writer transaction and treats a missing `payload_refs` row as
  uncommitted (payload.rs:925). Abandoned stage files are reclaimed at
  open (payload.rs:474-510). This is the kill/resume reconciliation the
  design note describes.
- **Durable pairing**: `bind_payloads` (payload.rs:852) binds the payload
  root to the authority `store_id`, refusing adoption of a foreign or
  replaced root — the "same store pair" discipline the SPI's durability
  rule assumes.

An SPI backend would slot in at exactly one seam: the object read/write
primitives inside PayloadStore (`new_file`/`install_file`/`sync`,
payload.rs:252-293) and the inventory. Everything above (reservations,
quota, readers, orphan policy) is authority logic that must remain.

## 4. Options

**(a) Adapter over the existing PayloadStore.** Implement
`ObjectStore` for a wrapper whose `(scope, entity)` key is the random
payload key, looked up in an adapter table. Cost: low; the store keeps its
recovery grammar and quota engine intact. Value: proves the SPI object
contract against the real authority workload without touching it. But the
mapping is degenerate (scope tree unused; seal/verify meaningless) — it
certifies the adapter, not the authority.

**(b) SPI-native payload backend behind config.** Replace the
file-at-a-time internals of PayloadStore with `pipestream-storage` calls:
objects addressed `(scope, entity)` — available from the admission's
`(generation, scope, entity)` — while reservations, readers, and quota
stay authority-side. The fs backend's stage-file grammar differs
(`write-*.tmp` + rename vs `stage-*` → `object-*` rename), so stage
recovery at open (payload.rs:474-510) must be re-expressed as scratch
sweeps; both are "incomplete writes are never referenced". Cost: moderate;
real win: one certified object layer shared with the client side and the
Java lane, cross-backend (S3 for payloads!) for free.

**(c) Full replacement.** PayloadStore deleted, authority rewritten onto
SPI + its own reservation/quota layer. Cost: high; benefit over (b)
mostly cosmetic. Not recommended.

**Recommendation: (b), staged.** The two-system rule is already the
architecture, so the cut is clean: keep the SQLite authority db as the
journal/reference store, move object bytes onto an SPI backend selected by
config (fs default), keep reservations/quota/readers authority-side, and
carry over the recovery grammar deliberately (§6). Do (a) first as a
one-PR proof if review wants the SPI exercised against the real workload
before committing to (b).

### Landing plan and proof tests

1. **(a) adapter PR** — `ObjectStore` over PayloadStore via a side-table
   mapping; runs `contract::run_object_store` (in-memory reopen N/A — the
   fs durability roundtrip runs against a real root). Proves: the SPI
   semantics hold over the production store's install/open/retire paths.
2. **(b) backend swap PR** — config-selected SPI object backend for
   payload bytes; the existing authority kill/resume and orphan-collection
   tests (`src/v2/authority/ingress.rs:613-623`, payload audit tests in
   payload/audit.rs) must pass unmodified against the fs SPI backend;
   new tests: interrupted stage leaves no referenced object (crash-point
   equivalents of payload.rs:608-629), S3-backed authority runs the
   conformance kill/resume rows.
3. **Conformance** — the storage-backend flag sweeps the matrix;
   byte-identical finals against the fs baseline (design note,
   "Certification").

## 5. Out of scope

- **The fixed-capacity records layer** (`src/v2/authority/records.rs`,
  `records::protect`): slot-addressed, fixed-size, quota-accounted rows
  are a different storage shape from entity content; the SPI models
  content-addressed objects, not slots. Forcing it through `put` would
  discard the capacity invariants the records layer exists to enforce.
- **The bounded-VFS quota machinery** (`crate::persistence::GUARDED_VFS`,
  `PhysicalGuard`, payload.rs:11, 101): it enforces physical byte budgets
  at the SQLite VFS boundary. It is a guard *around* storage, not a
  storage backend; nothing in the SPI could express it, and both lanes
  keep it as-is.

## 6. Risks

- **Stage-file recovery grammar.** The SPI fs backend sweeps `write-*.tmp`
  scratch; PayloadStore reclaims `stage-*`/`funding-*` by header rules
  (payload.rs:474-490). An (b) swap must preserve "unreferenced partial
  write is reclaimed at exclusive open" and the per-name invariants the
  open audit enforces — a sweep that deletes the wrong grammar corrupts
  recovery. Mitigation: dedicated crash-point tests before enabling (b).
- **Orphan sweeps.** `collect_until` assumes flat keys and live-pin
  exclusion (payload.rs:788-800). Under an SPI backend, "referenced" must
  still consult `payload_refs` under the writer transaction, and retire
  must remain reference-driven — SPI `retire(scope, cursor)` alone would
  orphan differently. The authority's sweep policy stays; only the byte
  operations move.
- **Conformance fixture hooks.** Crash points (`tests::crash_point`,
  payload.rs:609, 629, 1102-1117) and the reserved-key fixture paths are
  wired to the current file grammar; moving to SPI layouts requires
  re-pointing those hooks at SPI equivalents (the SPI fs backend has its
  own scratch naming) without weakening the kill/resume rows.
