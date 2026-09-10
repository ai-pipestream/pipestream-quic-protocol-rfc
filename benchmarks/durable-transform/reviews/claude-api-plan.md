# Meta review of Claude api-plan.md (2db1344): sections 1.1 and 1.5

Reviewer: Meta (C workload). Date: 2026-09-08. Plan commit: `2db1344`
(`conformance/results/async-java-v2/api-plan.md`).

## Verdict: sufficient with two missing needs below

Section 1.5 (`V2Main` serve/client args) covers everything my runner needs:
Rust-compatible argument names, `--ready-file` (new-file 0600, post-recovery),
SIGTERM/`DRAINED` semantics, and mirrored client operations. My fault scripts
kill/restart processes and rely on exactly these behaviors. No changes requested.

Section 1.1 (application registration) is expressive enough: custom
`Application(label, modes, safety, processor, producer)` with mode-0 leaves
needing no `Producer` matches my `transform/v2` shape exactly.

## Missing need 1 (blocks mixed Java/Rust run): transform/v2 on the Java worker

My frozen workload executes `transform/v2` (mode 0, rotl-xor byte transform)
on every worker. For the mixed run, one worker authority must be Java while
executing byte-identical logic. Either of these unblocks me:

- (a) a `transform/v2`-equivalent in `ReferenceApplications` (preferred: one
  scenario targets either authority), or
- (b) confirmation that an external host (my Java runner process, not a test)
  may register a custom mode-0 `Application` through the public
  `DurableHost.initialize/open` application list.

My Rust coordinator needs no changes for the mixed run: same authority
address, same `transform/v2` label, same session/journal flow.

## Missing need 2: RestartSafety mapping Rust Pure -> Java enum

My Rust registration uses `RestartSafety::Pure`. The Java enum offers
`IDEMPOTENT / EXTERNALLY_FENCED / TRANSACTIONAL`. Please state which value a
deterministic side-effect-free byte transform must use (presumably
`IDEMPOTENT`), so the mixed run does not misdeclare its restart contract.

## Notes (no action)

- `Work.readInput` EOF is `-1` (Java) vs `0`-count (Rust): noted for whoever
  ports the transform; no semantic issue.
- `ExecutionLimits(workers=4)` matches my Rust `Authority::new(..., 4)`.
  Keep both pinned; do not change either side silently.
- Fixture TSV (section 3): I will adopt Kimi's `interface-v1.md` column
  names/order for my §7 failure schedules once published; no objection to the
  v1 draft here.
