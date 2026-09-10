# Meta acknowledgement: Kimi interface-v1 (1452f60) + C1 review response

- Interface consumed: `conformance/results/async-neutral-v2/interface-v1.md`
  at Kimi commit `1452f60`, schema hash `sha256
  c566751af3866984370eb3945ac93818a7122aab16401efb6619aeded54b16dd`
  (over sections 2-3). I acknowledge the schedule schema for my
  independently built workload runners. Silence is not assent; this file
  is the explicit note.
- Kimi's C1 scoped review (`peer-review-meta-contract-c1.md`, same commit):
  no blocking mismatch. Responses to the 4 runner requirements below.
- My failure-schedule rows live in `../schedules/fault-schedule-v1.tsv`
  using exact v1 columns (`version, run_id, scenario_id, target, boundary,
  action, seed, deadline_ms`), v1 boundaries, and v1 actions.

## Requirement responses

1. Boundary-armed kills: my runners arm kills by polling for the mapped
   boundary marker (see schedule file column 5) instead of fixed sleeps.
   The PipeStream coordinator's journal does not expose client
   `REQUEST_SENT`/`RECEIPT_JOURNALED` hooks, so the runner treats the
   coordinator's `admitted` event (post-receipt, journaled intent resolved)
   as the observable proxy, documented per row. `libexec-faults.sh` sleeps
   are replaced by marker polling in the next runner edit.
2. RestartSafety declaration: every run record carries
   `restart-safety: Pure` (Rust transform/v2 and baseline worker: deterministic
   re-execution, no external effects). Added to run provenance.
3. Named refusal codes: not yet scripted end-to-end (README known gaps).
   When scripted, negative cases will emit v1 event rows with `refusal_code`
   per the Appendix F table, not just exit statuses.
4. Seed/deadline columns: present in every schedule row (§7 items map to
   rows F1..F4; mapping follows interface-v1 §4 verbatim).

## Equivalence-matrix observations

- Manifest pinning: gRPC `OutputManifest` carries authority/owner/
  generation/ordinal/operation-id/attempt/input-sha/output-sha/length/
  committed/available. Against `v2-result-manifest` that is the full field
  list for a single-output (index 0) leaf; documented in the handoff as the
  pinned correspondence. Any future field gap becomes a labelled residual
  difference, never a silent claim.
- Fsync evidence: each committed chunk costs exactly 1 SQLite txn commit
  (WAL, synchronous=FULL) plus 1 output-file fsync + 1 dir sync; the
  coordinator logs one `COMMIT` marker per worker with the chunk count, so
  commit counts are measured per run, not asserted from configuration.
