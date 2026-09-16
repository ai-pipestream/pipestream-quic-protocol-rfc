# durable-index-build (M2): design

Small real multi-stage application on the v2 protocol. Builds an inverted
index over a seeded text corpus using protocol parts the transform workload
does not: authority-side expansion (descendant scopes), parent reads of child
outputs, cross-authority result references, scope cancellation.

## Topology (one direction; the reverse swaps authorities)

- Authority A (Java) holds stages 1-2. Authority B (Rust) holds stage 3.
- Coordinator (Rust binary, public `v2_client` API only) drives, then verifies.

## Data flow

1. Corpus: N text files, seeded RNG, deterministic vocabulary. Coordinator
   generates bytes locally AND admits one parent unit per file to A with
   application `index-file/v1`, input = file bytes.
2. Expansion (authority-side): the parent's expansion splits its input into
   K fixed chunks, declares a sealed child scope of K members, admits each
   child with application `tf/v1` (input = chunk bytes, streamed
   parent->child inside the authority, never through the coordinator).
3. Stage 2: each `tf/v1` child computes a deterministic term-frequency
   record (`term doc-freq` lines, sorted) and publishes it as output 0.
   The parent's execute phase reads every child output 0 (child-output
   reads) and publishes its own output 0: the newline list of
   `scope entity attempt digest` for its TF children (= stage-2 manifests).
4. Coordinator watches parents, fetches parent outputs (small), concatenates
   them into the merge input (references only, no TF bytes locally except
   for the independent single-process reference computation).
5. Stage 3: coordinator admits one merge unit to B, application
   `index-merge/v1`, input = reference list. The merge executor acts as a
   spec section-12 consumer: it attaches to A with separately configured
   owner credentials + endpoint (example authority flags, never inferred
   from a URI), performs manifest/RESULT select+read per reference
   verifying digests, merges into one inverted index file as output 0.
6. Coordinator fetches the index, byte-compares against the single-process
   reference computed by the same TF/merge code without the protocol.

## Demonstrations

- Kill/resume: coordinator killed mid-stage-2 and mid-stage-3 (separate
  runs), restarted with journal resume; finishes byte-exact with no new
  declarations (replay uses retained operation ids).
- Cancellation: one extra file admitted, then its subtree scope-cancelled;
  final index excludes exactly that file; v2-counts show the CANCELLED
  member and the sums match.

## Direction symmetry

Both directions produce the same index digest: TF records and the index
format are byte-deterministic across implementations (fixed line format,
sorted terms, u64 counts). Verified by comparing digests, not just local
references.

## Custom contracts

New application labels (`index-file/v1`, `tf/v1`, `index-merge/v1`) via
public registration APIs only (Rust `Applications::register`, Java
`DurableHost.open` application list in the example launcher). V2Main's
hardcoded reference list is untouched. No test hooks in the app path.

## Scale (gate run ~15 minutes)

Gate corpus: 2 files x 128 words (seed 6), x3 per direction; cancel
demo at 2 files x 8192 words. TF/index fit in one output each. JVM +
Rust startup dominate. The coordinator defaults (8 files x 2048 words)
are NOT in the gate: that size admits ~40 concurrent units and exceeds
the example authority's default aggregate admission capacity. Sizing
the authority is an operator concern; the gate stays inside the
default envelope on purpose (see README).
