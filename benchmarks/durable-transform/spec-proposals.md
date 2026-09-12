# Clause-level spec proposals (C15)

No clause-level correction to the normative text is proposed
from C15 evidence. The bounds met during measurement (V2
256-entry list bound, single-transaction fit, cumulative
record-completion funding) behaved as specified; the
application was fixed to respect them, not the reverse.

Open requests (not spec corrections, not evidence gaps):

1. Client-library owner: idle/lifetime timer disable/extend
   knob (blocks a true disable-timer negative control).
2. Client-library owner: causal deadline evidence
   (post-deadline writes surface Cancelled; exact death
   instants unobservable).
3. Java authority owner (Claude): raise or expose storage
   funding for >=256-entity scopes, or document the Java
   bound (mixed 48 MiB blocked; repro in
   results/c4-large48-seed6/CELL.txt).

# Clause-level spec proposals (C16)

No clause-level correction to the normative text is proposed
from C16 evidence either. Nothing measured suggested weakening
a guarantee: pipelining stays inside journal-before-send /
validate-before-journal / one-receipt-per-operation, the
authority's admission capacity behaved as a named
LIMIT_EXCEEDED (backpressure, retried to byte-exact finals),
and the unfunded/oversize cases stayed refused. Two
implementation observations are recorded here because they
touch specified surfaces, without proposing text changes:

1. Refusal diagnostics (Section 11.10 registry): the Rust
   authority maps every non-protocol StoreError to a static
   detail ("authority storage operation failed" on admission,
   "application storage failure" at execution commit). The
   code is named and the behavior is specified; only the
   underlying cause is unobservable, which cost diagnosis
   time during the C16g slow-fsync episode. A future
   registry guidance note could ask implementations to log
   the underlying store cause server-side. No weakening,
   no behavior change proposed.
2. Contention posture: the codebase already classifies SQLite
   busy/locked as retryable, but only the maintenance path
   retries; request and execution paths fail fast. Whether
   those paths should share the retry is an owner decision
   for the subject implementations (Rust/Claude/Java), not a
   spec change: fail-fast on storage error is specified
   behavior, and the benchmark treats it as such.

Open requests carried forward: items 1-3 above unchanged
(item 3 still open, 2nd request sent; xlarge64 still blocked
on it). New: none.
