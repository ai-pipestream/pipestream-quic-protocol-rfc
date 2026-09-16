# Kimi review labels: Meta durable-transform workload rows (C16 report)

Reviewer: Kimi (assignment B). Date: 2026-09-16. Scope: Meta's
`benchmarks/durable-transform/{contract.md,report.md,handoff.md,ladder.md}`
at `agent/rfc-meta-workload-v2` (C16 report rows; post-C16 work — A1 fault
suites, M2/M3/M4 — is outside these labels). Contract drift since my C1
review (67ecd0b): only an appended §12 change log (C16e pipelining,
--pending-limit 16 default, --serial reproduces old order, gRPC same
concurrency); §§1–11 byte-identical, §6–§8 not weakened. Method: full
read plus evidence spot-checks under results/ (digests, MANIFEST counts,
event-grep recounts, per-rep stdout wall ranges); cross-checks against
subject truth as the neutral acceptance matrix measured it (archive
durable-18d5d9f3653ef1c4).

## Labels (16 PASS, 5 PASS-WITH-NOTE, 0 ISSUE)

| Row | Label | Basis |
|---|---|---|
| ladder empty (0 B) | PASS | digest e3b0c442 verified; vacuous first-usable documented in CELL.txt |
| ladder tiny | PASS | 55d2e6e9 verified (rep1-ps) |
| ladder uneven | PASS | 65283733 verified (rep1-grpc) |
| ladder quick | PASS | 3cde15aa verified (rep2-mixed) |
| ladder standard | PASS | reproduces C15 pin b3dd5e34 across arms and in the C15 archive |
| ladder large48 | PASS | acc15911 verified across all 3 arms; 1024/256 MiB funding recorded |
| xlarge64 UNAVAILABLE | PASS | honest negative; C17a REPRO-DEFECT11.txt shows the exact 332-admit refusal (note a) |
| negative controls 31/31 | PASS-WITH-NOTE | fresh killed-collector injections both arms; notes b, c |
| boundary F3/F4 15/15 | PASS-WITH-NOTE | BOUNDARY SUITE PASS, resume byte-exact 1104-2131 ms; note d |
| stopped-consumer | PASS | STOPPED SUITE PASS, 5 twins + delay + stall per arm, no-fetch gate |
| slow-worker | PASS | SLOW SUITE PASS, exactly-once, exposed/hidden split |
| CR13 (rust + java, 4 arms each) | PASS | in-window Cancelled brackets, probe-session-survived, cancel-neg rejected |
| failed-run retention | PASS | all FAILED-attempt dirs + OPERATOR-NOTEs with fsync probes present |
| wall-time table (6 cells x 3 arms) | PASS | every quoted range reproduced exactly from rep stdout logs |
| pipeline before/after + refusal accounting | PASS | medians match PIPELINE-SUMMARY; 5485/338 recounts exact; zero uncategorized LIMIT_EXCEEDED |
| C16f resource counters 3/3 | PASS-WITH-NOTE | jstat 34 rows zero gaps, walls/digests verified; note e |
| idle write_bytes answer | PASS-WITH-NOTE | idle-before/after.tsv archived; rates not recomputed (note e) |
| loopback bytes | PASS-WITH-NOTE | conclusion conservative and true; numbers need rewording (note f) |
| §6 dead-collector gate | PASS-WITH-NOTE | a fresh NEGATIVE-LOG run IS on record (full-seed6/negative, 2026-09-11, final pins) — note g |

## Notes

- (a) xlarge64: update the reason from ">=341-entity storage" to
  "defects 11+15 (Java session-WAL restart + shm sidecar cap), refusal
  at 332 admits reproduced; new pins available, not yet adopted."
- (b) the missing-metrics control reuses authentic C15 evidence rather
  than a fresh C16 injection (the killed-collector control IS fresh).
- (c) the negative suite's revoked-reader-grpc en-route fix is
  documented; the final NEGATIVE-LOG is the post-fix run.
- (d) F3-mixed UNAVAILABLE is documented only in the archive
  (f3-mixed-UNAVAILABLE.txt; no TEST-ONLY kill flag on V2Main).
  report.md's "15/15" is accurate for executed rows but should name the
  gap for a reader of report.md alone.
- (e) CPU-s/RSS per-process aggregates and idle rates not re-aggregated
  by this review; row counts, walls, digests, jstat completeness verified.
- (f) "~2.1x payload on both sides" understates ps: measured grpc ~2.0x,
  ps 2.37x standard and 3.59x large48 (refusal retries + declare
  batches). The "no byte advantage" conclusion is conservative; suggest
  "grpc ~2.0x; ps 2.4-3.6x depending on cell."
- (g) Meta's board says its dead-collector proof run is "queued, no
  fresh run on record" — but the C16g archive (2026-09-11, final pinned
  binaries) contains exactly that (fresh killed-collector-ps/grpc
  injections). Meta can close its queued item; a post-round-7 re-proof
  would be new work and does not block these labels.

## Protocol-truth cross-checks (consistent)

transform/v2 = rotl8(b,1) XOR (i mod 251), length-preserving, 64 KiB
chunks (workload-core/src/lib.rs:13-21); refusal naming matches the wire
("aggregate admission capacity exhausted" and "metadata concurrency
exhausted" both LIMIT_EXCEEDED at the subject); the 37 admit-notready
rows recount exactly (16 pipeline + 21 full, all worker ordinal 2) and
the defect-10 attribution is confirmed by Meta's own D10 rerun
(~/.rfc-tmp/d10-seed6/D10-LOG.txt); connection ceilings (rust 16/4,
java 32/8) uncontradicted.

## Handoff wording (not a report row)

"631 ps + 233 mixed metadata-concurrency rows": ps=631 exact (307
standard + 324 large48); mixed=458 total (233 is large48-only). Basis
mismatch in the handoff line; the metadata-concurrency variability
question from my M18 is thereby partially answered.

## Not verified (named, not held against the labels)

the 418-refusal unfunded grind (retained only in terminated /tmp
partials); the historical ~31 ms fsync regime (host state, documented by
OPERATOR-NOTE probes); per-process CPU-s/RSS re-aggregation; D10 evidence
lives outside the worktree; byte-exactness of every NOT_READY-affected
mixed run (sampled reps verified, not all 19).
