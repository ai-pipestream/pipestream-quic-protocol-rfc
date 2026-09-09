# Scenario matrix detail — group G1: lifecycle and full root coverage

Requirement families: V2-SET 1–4, V2-ADMIT 1–4, V2-RESULT 1–5, V2-CLOSE 1–2.
All rows run all three directions (rust/rust, rust-client/java-server,
java-client/rust-server) now that both peers are published. Reference
applications (labels identical on both subjects): `copy/v2` (mode 0),
`consume/v2` (mode 0, zero outputs), `retry-copy/v2` (attempt 1 retryable),
`reassemble/v2` (mode 1), `chunk-copy/v2` (mode 2, 65,536-byte chunks,
≤256 children).

## g1-leaf-copy — DONE (M1/M3, three directions, byte-identical output)

## g1-empty-input

- Declare + admit a zero-length input to `copy/v2`.
- Expected: admission succeeds (SHA-256 of the empty string required);
  terminal success; manifest with one zero-length object; read returns
  zero bytes with correct length/hash/FIN. Zero-length is not an error
  anywhere in the chain.

## g1-zero-output

- Admit non-empty input to `consume/v2` (zero outputs).
- Expected: terminal success with an EMPTY manifest (zero objects);
  result read of index 0 refuses NOT_FOUND (5); the success is
  distinguishable from failure (work view state + manifest count), and
  the receipt/manifest deadlines run from the same terminal commit.

## g1-oversize-payload

- Input larger than the negotiated stream flow window (fixture negotiates
  a small `stream-limit`/window where configurable; payload e.g. 8 MiB
  against a 64 KiB-class window) to `copy/v2`; result likewise exceeds
  the window.
- Expected: incremental reception with credit replenishment; admission
  and byte-exact result; control progress independent of the blocked
  transfer (a concurrent next-sequence/lookup completes while the large
  stream is in flight). Record negotiated limits in observed.tsv.

## g1-out-of-order-pages

- Declare entities in multiple batches (strictly increasing within and
  across batches), admit a subset, then page the scope with small page
  limits from several `after-entity` cursors concurrently with admits.
- Expected: pages are ordered, bounded (≤256), producer/parent/seal
  fields correct; `more` flags chain exactly; an empty page never
  treated as completeness; unsealed growth between snapshots visible;
  final seal digest matches the independently computed
  `pipestream-scope-seal-v2` commitment (driver computes from its own
  fixture configuration, not from server output).

## g1-mode1-branch (reassemble/v2)

- Admit a multi-part input to `reassemble/v2` (mode 1: caller-expanded
  branch). Parent admission atomically allocates a producer-0 child
  scope (in the admission receipt).
- Expected: child scope identity in the receipt; parent cannot complete
  until the child scope seals and all descendants settle; the
  application reads its own closed successful children via the child
  page/output API and reassembles byte-exact output; STRICT parent
  rehydration refuses missing/failed child cuts (paired with g8 rows).

## g1-mode2-descendants (chunk-copy/v2)

- Admit input to `chunk-copy/v2` (mode 2: authority-expanded, producer-1
  child scope, ≤256 children of 65,536 bytes).
- Expected: admission receipt names the producer-1 child scope; the
  authority's expansion declares/admits children (parent-fenced);
  external client attempts to declare or admit INTO the producer-1
  scope refuse UNAUTHORIZED (3); parent settles only after every child
  is terminal; aggregate output equals input byte-exact.

## g1-declaration-capacity

- Declare batches up to the 256-entity bound; an empty batch without
  seal refuses; an empty batch with seal succeeds and seals; entity IDs
  not strictly increasing (within or across batches) refuse.
- Expected: named refusals for each violation; capacity accounting
  visible via subsequent page counts; declaration alone never admits
  (no execution before input).

## Cross-cutting checks applied to every G1 row

- Operation request digests: the driver independently recomputes the
  `pipestream-operation-v2` digest for every mutation it sends where the
  subject CLI exposes the digest (record where each subject surfaces it;
  absence = measurement gap, noted not waived).
- Attempt identity: declared entity shows attempt 0 in views before
  admission, attempt 1 after; retry rows are G4, not G1.
- Every row verifies output bytes against the driver's own transform
  oracle AND records length + SHA-256 + identity fields (work key,
  attempt, manifest index) — digest-only success is not accepted.
