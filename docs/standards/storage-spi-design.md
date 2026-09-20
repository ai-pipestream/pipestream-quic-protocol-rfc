# Reference storage SPI design

This document defines the pluggable storage layer for the PipeStream reference
implementations (Java and Rust). It is INFORMATIVE. The protocol specification
does not mention storage location, layout, or backend, and this layer must not
change anything observable on the wire: frames, commitments, digests, and
refusal behavior are identical for every backend. The test for whether
something belongs here instead of the spec is: can a peer observe it, or is it
needed to compute identical commitments? Everything in this document fails
that test, by design.

Purpose: an implementer can bring any storage mechanism — a filesystem, an
object store, a database, a log — by implementing one contract, and certify it
against the existing conformance matrix. Compatibility between backends and
between languages is required to be 100%: the same workload produces
byte-identical commitments regardless of backend, and mixed-backend sessions
(one peer on S3, the other on a filesystem) are indistinguishable from
same-backend sessions.

## Two roles, not one interface

The protocol's durable state splits into two roles with different semantics.
Backends map to them differently, so the SPI models them separately.

**ObjectStore** — sealed, immutable entities and scope metadata. Objects are
write-once and content-verified. After a scope seals, its objects never
change; verification is against the scope's SCOPE_DIGEST (Section 9.5).

**Journal** — the intent and commit records used for crash recovery.
Append-only, ordered, replayable from a cursor. This is the record that lets a
restarted endpoint distinguish committed work from admitted-but-uncommitted
work.

A single backend may implement both roles (filesystem, Postgres), or a
deployment may mix roles across systems (S3 objects + Kafka journal). The
mixed case is the two-system commit configuration and carries an explicit
rule: object write first, journal commit second, reconcile on recovery by
replaying the journal and treating objects without a commit record as
uncommitted. This is exactly what the driver's kill/resume rows exercise, so
mixed deployments are certified by the same rows as single-system ones.

## Logical layout (shared by all backends)

All backends use the same logical layout so that a store written by one
implementation is readable by the other, and so backend choice never changes
behavior.

- **Scope tree.** The protocol guarantees scope nesting depth 0-7 (Section 6,
  3-bit depth field; negotiated `max-scope-depth` cannot exceed 7). Map the
  scope hierarchy to a path of at most 8 components: root scope at the top,
  each child scope one level down. On a filesystem this is a directory tree;
  on S3 a key prefix; in a database a path column or nested document key.
- **Fanout within a leaf.** Depth is protocol-bounded but breadth is not — a
  single scope's entity count is bounded by capacity, not by a small constant.
  Backends must not place all entities of a large scope in one flat container.
  Within a leaf, shard by a hash prefix of the entity id (2 hex chars = 256
  buckets is sufficient for the reference workloads; the shard width is a
  backend-local constant, not part of the shared layout).
- **Sealing is per-scope.** A sealed scope's container holds its SCOPE_DIGEST
  and entity count alongside the objects, so verification and audit are
  scope-local operations.

The shared layout is what makes the 100% compatibility requirement testable:
identical logical content per scope on every backend, differing only in
physical representation.

## Backend set

Both reference implementations ship the same set:

| Backend | ObjectStore | Journal | Notes |
|---|---|---|---|
| Filesystem | yes | yes | Default. Fixes the flat-directory failure mode via the scope tree + leaf fanout above. |
| Postgres | yes | yes | Metadata and journal in tables, payloads as bytea or offloaded to the filesystem backend. Transactional outbox covers the two-role case atomically. |
| S3 / object store | yes | no | Natural fit for sealed immutable objects. Test against MinIO. Pair with another backend's journal. |
| Kafka | no | yes | Journal only; payloads do not belong in the log. |
| MongoDB | yes | yes | Document per scope; included for enterprise parity. Lowest priority of the set. |
| In-memory | yes | yes | Volatile. Test and development backbone; also the fast lane for conformance fuzzing. |

Implementation order: filesystem first (it is the fix for the observed
flat-temp/store failure that motivated this layer), then Postgres, then S3,
then Kafka, then Mongo. In-memory lands with the SPI itself.

## SPI contract

Language-neutral semantics; each implementation names these natively (Java
interfaces, Rust traits). Error taxonomy is part of the contract: backends
must distinguish NotFound, AlreadySealed, Corrupt (digest mismatch), and
Transient (retryable) failures, because recovery behavior differs per class.

ObjectStore:

- `put(scope, entity_id, bytes, expected_digest)` — idempotent. Re-put of
  identical bytes with the same digest succeeds as a no-op; conflicting bytes
  for the same id are Corrupt.
- `open_read(scope, entity_id)` — streaming read. A live read holds the
  object; removal of an unsealed object under an open read does not
  invalidate the read.
- `seal_scope(scope, digest, entity_count)` — atomically marks the scope
  sealed and records its SCOPE_DIGEST. After sealing, `put` into the scope is
  AlreadySealed.
- `verify_scope(scope)` — recompute and compare against the recorded digest.
- `list(scope)` — entity ids, for audit and orphan reconciliation.
- `retire(scope, retention_cursor)` — remove entities past the retention
  point; sealed scopes retire whole.

Journal:

- `append_intent(record)` — durably record admitted-but-uncommitted work.
- `commit(intent_id, commit_record)` — mark an intent committed. In the
  two-system configuration this happens after the object write.
- `replay(since_cursor)` — ordered replay of intents and commits from a
  cursor; the recovery path's only read.

Durability rule for every backend: an operation returns success only when the
record survives a process kill. Backends document their flush/fsync/ack
configuration; the kill/resume conformance rows verify it empirically.

## Pinned decisions (binding on both lanes)

The first implementations (Java: `ai.pipestream.quic.storage`; Rust:
`pipestream-storage`) pinned what this note deliberately left open. Both lanes
must match these exactly; they are what the cross-language layout vectors test.

1. **Scope address.** A scope path is 1-8 unsigned 32-bit components (root =
   depth 0), rendered as zero-padded 8-hex characters per level joined by `/`.
2. **Fanout bucket.** First byte of SHA-256 over the 8-byte big-endian entity
   id, rendered as 2 lowercase hex chars (256 buckets).
3. **Canonical scope digest.** SHA-256 over the concatenation, in ascending
   entity-id order, of `(0x00 || id-be64 || object-sha256-digest)` per entity;
   the empty scope's digest is SHA-256 of empty input. `seal_scope` records the
   caller-supplied digest verbatim; `verify_scope` recomputes per-object
   digests, the entity count, and this canonical digest — any mismatch is
   Corrupt.
4. **Idempotency** covers seal/commit/append as well as put: identical re-seal,
   re-commit, or re-append is a no-op success; the same key with changed bytes
   is Corrupt. `put` into a sealed scope is AlwaysSealed regardless of bytes
   (seal checked before digest).
5. **Retire cursor.** Unsealed scopes remove entities with
   `id <= retention_cursor`; retiring an absent or empty unsealed scope is
   NotFound. Journal commits consume a cursor in the global order.
6. **Filesystem physical formats** (backend-local, but both lanes match them so
   stores are cross-language readable): entity file =
   `[8B length][32B sha256][payload]`; scope seal file = `[32B digest][8B
   count]`; journal record = `[type][cursor][idLen][payloadLen][id][payload]
   [sha256 checksum]` under magic `PSJSL01`.
7. **Error mapping.** I/O failures map to Transient; double-writer and
   use-after-close map to the general failure category.

## Certification

No backend ships without passing the full acceptance matrix against it. The
conformance driver takes a per-subject storage-backend flag; each backend
runs the complete row set, and the stage-2/stage-3 kill/resume rows are the
durability proof. Mixed runs (e.g. Rust server on S3+Kafka journal, Java
client on filesystem) must produce byte-identical finals to the filesystem
baseline. Results per backend are recorded alongside the existing acceptance
evidence.

Temp-file hygiene is part of this layer: backends place scratch files under a
configurable base directory, never an ambient shared temp dir, and clean them
on close. (The Java lane sets `java.io.tmpdir` and `org.sqlite.tmpdir`
explicitly per run; see the M22 driver change.)
