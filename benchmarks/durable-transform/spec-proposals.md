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
