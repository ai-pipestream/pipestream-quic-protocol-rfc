# Neutral durable driver design (assignment B)

Owner: Kimi. Implements B1–B4 from `B-neutral-failure-driver.md`. This note
is the working design; the scenario matrix and requirement traceability grow
in this directory as rows are implemented. Event/schedule wire contract is
frozen separately in `interface-v1.md`.

## Command shape

```
pipestream-conformance durable [OPTIONS]
  --dev                      development partial mode: runs available
                             prerequisites, labels every artifact INCOMPLETE,
                             can never emit a full-conformance PASS
  --scenario <id>            run only named matrix rows (repeatable);
                             acceptance mode runs the whole matrix
  --seed <n>                 base seed recorded into every schedule row
  --artifacts <dir>          fixture-owned run output (default fresh
                             target/durable-runs/<run_id>)
  --rust-bin <path>          pipestream-quinn binary (default workspace
                             release build, hash recorded)
  --java-jar <path>          Java -all.jar (absent in acceptance mode =
                             explicit FAIL naming the missing prerequisite;
                             in --dev the Java-direction rows report
                             INCOMPLETE, never skip-pass)
```

Acceptance mode is the default so `run_all.sh` needs no hidden flag.
Every run records: binary paths + SHA-256, exact CLI invocations, seeds,
negotiated profiles/limits, child exits, refusal codes, resource samples.
Expected and observed values are stored separately.

## Module layout (conformance crate, no production deps)

Mechanical gate: a source-scan test fails if any file under
`conformance/src/` mentions `pipestream_core`, `pipestream-quinn` (as a
crate import), `pipestream-quic`, or subject test-helper symbols. The crate
keeps its existing dependency set only.

- `durable.rs` — `Task::Durable` dispatch, run orchestration, registry.
- `durable/events.rs` — interface-v1 event writer/reader (append+fsync,
  torn-line detection, seq monotonicity, bounds 65,536 records/process).
- `durable/schedule.rs` — schedule parser/executor (8 columns, actions
  pause/release/disconnect/drop-reply/stop/kill/restart/clock-set; missing
  boundary before deadline = run failure).
- `durable/mtls.rs` — rcgen EC P-256 CA + server cert (SAN localhost) +
  per-principal client certs (clientAuth EKU) + `sha256<TAB>principal`
  principal-map TSV (SHA-256 over leaf DER), isolated per run.
- `durable/process.rs` — V2 process ownership: `init-authority`, `serve`
  (ready-file readiness THEN an authenticated client handshake before the
  server counts as ready), client one-shot ops, bounded stdout/stderr
  draining, kill only fixture-owned process groups, reaping on all paths.
- `durable/oracle.rs` — independently derived expectations: dataset bytes
  from seeds, expected transforms (copy/consume/chunk/reassemble
  applications), SHA-256 commitments, expected named refusals per row.
  Never parsed from server summaries, journals, or production codecs.
- `durable/scenarios.rs` — the matrix (below).
- `durable/resources.rs` — capability manifest + collectors (RSS/HWM,
  threads, FDs, file lengths, allocated blocks, disk I/O, network bytes);
  separate scopes per B3; unavailable mandatory metric = INCOMPLETE row.

## Scenario matrix skeleton (row IDs stable)

Group G1 lifecycle: `g1-leaf-copy`, `g1-empty-input`, `g1-zero-output`,
`g1-mode1-branch`, `g1-mode2-descendants`, `g1-oversize-payload`,
`g1-out-of-order-pages`.
Group G2 crash/lost-ACK: `g2-crash-before-create-commit`,
`g2-crash-after-create-commit` (lost SESSION response; identical replay),
`g2-drop-reply-declaration`, `g2-drop-reply-admission`,
`g2-kill-after-admission-before-publication`,
`g2-drop-reply-publication`, `g2-kill-client-after-request-sent`,
`g2-duplicate-op-changed-params` (CONFLICT), `g2-not-found-in-flight`.
Group G3 storage: `g3-input-before-metadata`, `g3-orphan-cleanup`,
`g3-terminal-cleanup`, `g3-partial-retirement`, `g3-restart-same-roots`.
Group G4 races/fences: `g4-publication-vs-cancel` (both orders),
`g4-publication-vs-skip`, `g4-stale-attempt-retry`,
`g4-ancestor-fence-publication`, `g4-deadline-settlement`.
Group G5 auth: `g5-cert-rotation-same-owner`, `g5-foreign-owner`,
`g5-untrusted-identity`, `g5-expired-identity`, `g5-remapped-owner`,
`g5-cross-authority-reference`, `g5-no-existence-disclosure`.
Group G6 wire abuse: `g6-canonical-violations`, `g6-wrong-length-hash-fin`,
`g6-duplicate-response`, `g6-error-after-result-header`,
`g6-stopped-control`, `g6-frames-from-raw-probe`.
Group G7 time/expiry: `g7-receipt-before-output-expiry`,
`g7-output-before-receipt-expiry`, `g7-read-pin-past-expiry`,
`g7-unsafe-clock-refusal`, `g7-deadline-queue-time`,
`g7-cleanup-interrupted-refund`.
Group G8 completion/detach: `g8-exact-root-complete`,
`g8-child-cut-conflict`, `g8-detach-drains`, `g8-refusals-after-detach`,
`g8-half-close-preserves-responses`, `g8-timeout-no-completion-claim`.
Group R resource: `r-connection-ceiling`, `r-pending-ceiling`,
`r-stalled-principal-progress`, `r-memory-ladder`, `r-staging-quota`,
`r-journal-bounds`.

Every externally meaningful row runs both directions
(`rust-client/rust-server` now; `java-client/rust-server` and
`rust-client/java-server` when Claude's checkpoints land; both caller and
authority death where applicable). Rows whose Java peer is absent are
INCOMPLETE in dev, FAIL in acceptance.

## Negative controls (must each fail the driver)

`nc-wrong-bytes` (oracle expectation mutated), `nc-wrong-attempt`,
`nc-missing-descendant`, `nc-altered-root`, `nc-unexpected-refusal`,
`nc-stale-binary` (recorded hash != actual), `nc-missing-scenario`,
`nc-torn-event-line`, `nc-dead-collector`. Controls run as part of the
durable command's self-check stage and their failures are required for a
PASS.

## Subject hooks (separate reviewable commits)

Rust CLI gets no production-code change in this slice. If a boundary is
unreachable black-box (commit-before-reply), a test-only `--fixture-*`
flag set on the server binary is proposed as a separate commit and Claude
peer-checks placement against the actual transaction path before use.
Hooks pause/report/halt at real boundaries only.
