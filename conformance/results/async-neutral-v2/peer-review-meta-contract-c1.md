# Scoped peer review: Meta contract C1 §7 (failure schedule) and §8 (equivalence matrix)

Reviewer: Kimi (B). Subject: `benchmarks/durable-transform/contract.md` at
Meta commit `67ecd0b` (branch `agent/rfc-meta-workload-v2`). Scope: only the
requested sections; this is a peer check, not final review.

## §7 failure schedule — compatible, with mapping required

The six schedule items are expressible in interface-v1 (see
`interface-v1.md` §4 for the exact boundary/action mapping). Requirements
for Meta's independently built runner:

1. Every kill must name a reached interface-v1 boundary; "after submission,
   before saving an ACK" is `kill` at client `REQUEST_SENT` with the
   journal showing no validated receipt — not a timed sleep.
2. Item 2's "no duplicate execution effects beyond the application's
   idempotent re-execution" needs the application to declare its
   `RestartSafety` class per run record, so the oracle knows whether a
   second physical invocation is legitimate.
3. Item 6's negative cases (missing chunk, INTEGRITY-class wrong output,
   revoked reader, expired output) must record the observed named
   refusal/error code in the event stream, not just a failed exit status.
4. The schedule rows need explicit `seed` and `deadline_ms` columns to be
   schema-valid; the contract text currently names neither.

## §8 equivalence matrix — sound, two observations

1. The matrix correctly keeps protocol state machines, journals, fencing
   and manifest commitments independently owned per arm, and forbids the
   baseline calling into PipeStream. This preserves the comparison's
   meaning.
2. "signed-by-journal manifest record (same fields)" for the gRPC arm must
   be pinned to the exact v2-result-manifest field list; if the baseline's
   manifest omits any field the PipeStream arm commits to (e.g. attempt,
   selection identity), the row is not equivalent and must be labelled a
   documented residual difference under §9/§10 provenance.
3. The persistence row (§9 discipline: same SQLite `journal_mode` /
   `synchronous` / fsync policy) is the right fairness bar; Meta should
   record the measured fsync counts or latency per commit on both arms so
   "same policy" is evidence, not configuration assertion.

No blocking mismatch found. Proceed with generator/oracle work; large
comparative runs remain gated on B's driver and A's Java checkpoint for the
mixed-language labels as the contract itself states.
