# Handoff — B: neutral failure/resource certification (Kimi)

Status: IN PROGRESS — NOT REVIEW_READY. Rust-only preparation and the
first both-direction rows exist; the whole B contract has NOT passed.
This file is maintained as work lands; the REVIEW_READY mark is only
valid when this section says the complete both-direction matrix, resource
gates and acceptance integration have passed, with evidence below.

## 1. Branch / base / state

- Branch: `agent/rfc-kimi-neutral-v2`, worktree `/work/worktrees/pipestream-rfc-kimi`.
- Base: `8eb5a17` on `feat/durable-work-results-v2` (contains `82a1b11`).
- Peer dependency consumed: Claude `5e3138a` (SERVER_READY+CLIENT_READY
  provisional) merged at `bc791fb`; merge-scope checked (25 files, all
  Claude-owned, no new transitive deps).
- Nothing merged into the shared feature branch or main; no push
  performed (push authorization not given); no force operations.
- Dirty state: none at last update except pre-existing fmt-only diffs in
  `src/v2/authority/admission.rs:1` and `scopes.rs:478` (not mine, not
  touched).

## 2. Delivered so far

| Commit | Content |
|---|---|
| 1452f60 | interface-v1.md (fixture event/schedule schemas; schema sha256 c566751a… over §2–3); Meta C1 scoped review |
| c3061af | driver-design.md |
| c53037f | scenario-matrix-g2.md |
| 9bb9dbc | M1: durable command skeleton — mTLS, events/schedule modules, process machinery, oracle, g1-leaf-copy, negative controls, independence gate |
| 0beeaa8 | rust-hook-proposal.md (placement proposal) |
| 261e3b8 | M2: hook-free G2 rows (duplicate/changed-param, simultaneous duplicate, uncontrolled crash recovery) |
| 5bd0425 | scenario-matrix-g5.md |
| 08f13c6 | peer-review-claude-java-hooks.md (commit-side sound; 4 SENT-side defects; accepted by Claude, fixes in his next checkpoint) |
| 2162f08 | M3: Java subjects; g1-leaf-copy three directions byte-identical |
| 70393a0 / 0fa10cd | scenario-matrix-g1.md / -g4.md |
| e6bafa4 | M4: G5 identity rows ×5 vs both servers |
| 9a5f390 | hook placement agreement addendum |
| 152c280 | M5: test-only fixture hooks in Rust production crates (separately reviewable) |
| be43a36 / 1f4f35e / 166b679 | scenario-matrix-g3.md / -g7-g8.md / -g6-resource.md |
| 1b9debf | traceability.md (requirement→scenario map + explicit gaps) |

## 3. Verification evidence (latest full state)

- `cargo test -p pipestream-conformance`: 61 passed / 0 failed (baseline
  was 24 before this assignment).
- `cargo clippy --all-targets -p pipestream-conformance -- -D warnings`:
  clean. `cargo fmt --check`: clean for the crate. Production crates
  after 152c280: clippy/fmt clean; core 470, quinn 236, server 12 tests
  green.
- One load-sensitive 20 s watch timeout in
  `cli_reopens_committed_work…` observed under concurrent build load;
  passed isolated and full re-runs. Recorded, not dismissed; watch for
  recurrence.
- Dev runs (all INCOMPLETE-labelled, never PASS): see
  `conformance/results/async-neutral-v2/runs/` archives with
  MANIFEST.sha256 per run (archiving added in M6).
- Subject binary pins: rust release `pipestream-quinn` sha256 recorded
  per run in run.tsv (M5 changed it — see latest run); Java all-jar
  sha256 `94af1aac…f0635a62` (mismatch vs Claude's pinned `fc2e1fcf…`
  flagged as probable shaded-jar timestamps; my hash pinned).

## 4. Interface and peer-review artifacts

- interface-v1.md: event + schedule schemas; acknowledged by Claude
  (1452f60, hash independently recomputed) and Meta (schedule schema).
- rust-hook-proposal.md + addendum: placement agreed with Claude
  (his peer-review-kimi-rust-hooks.md); M5 implements the amended
  contract.
- peer-review-claude-java-hooks.md: 4 defects + 2 gap groups; Claude
  accepted, fixes landing in his FixtureMain checkpoint.
- peer-review-meta-contract-c1.md: no blocking mismatch; Meta's answers
  accepted (reviews/kimi-interface-v1.md).

## 5. Known limitations / open gates

1. Acceptance mode has never passed: the full matrix, both directions,
   resource gates and run_all.sh integration are unfinished.
2. Java-server hook directions INCOMPLETE until Claude's FixtureMain
   checkpoint; rust-client/java-server lost-ACK rows blocked on it.
3. g7-unsafe-clock-refusal needs a subject fixture clock (proposal
   pending); host UTC is never used.
4. require-durable and wire-level cross-owner paths are unreachable via
   the published CLIs (recorded findings, M4) — final certification of
   those arms needs either CLI surface or documented implementation-test
   mapping.
5. My all-jar predates Claude's quiche pipestream.4 transport fix
   (MAX_STREAMS credit leak); re-pin when his rebuilt artifact is
   published; r-* and g6-stopped-* rows must run against the FIXED
   transport.
6. Client-side commit boundaries are driver-side observations only;
   uncontrolled client-death rows are labelled as such.

## 6. Safe next action

Continue matrix implementation per traceability.md (G2 lost-ACK rows
landing in M6, then G1 lifecycle three directions, G3 storage, G4 races
with hooks, G7/G8, G6 probes, R resources), then acceptance-mode
integration into conformance/run_all.sh and the final full-matrix run.
