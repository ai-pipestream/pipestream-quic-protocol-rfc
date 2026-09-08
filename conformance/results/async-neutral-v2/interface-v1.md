# Neutral fixture interface v1: event records and fault schedules

Owner: Kimi (assignment B). Status: PROPOSAL v1, published for Claude/Meta
acknowledgement. This is fixture orchestration, not a PipeStream protocol
change and not a production admin API. Interface revisions are explicit
proposals with a new version number, never silent edits.

Canonical schema hash (SHA-256 over the exact text of sections 2 and 3 of
this file, computed with `sha256sum` over the extracted section bytes,
UTF-8, LF endings): recorded on the team board with the publishing commit.

## 1. Scope and rules

- Bounded UTF-8 TSV records, LF line endings, no embedded NUL. Exact decimal
  integers; binary fields are lowercase hex. Labels escaped unambiguously:
  tab → `\t`, newline → `\n`, backslash → `\\`, carriage return → `\r`.
- No credentials, private key contents, or bearer tokens in any record.
- Records are published inside fixture-owned directories:
  `runs/<run_id>/<scenario_id>/events.tsv` plus artifact files under
  `runs/<run_id>/<scenario_id>/artifacts/`. A run directory is written only
  by the fixture that owns it; consumers treat it as immutable once the
  fixture writes `runs/<run_id>/<scenario_id>/SEALED` (empty file, fsync'd).
- Bounds: at most 65,536 event records per process per run, at most 1,024
  artifact references per scenario, each artifact at most 64 MiB unless the
  scenario row declares a larger explicit bound. Overflow fails the run.
- A truncated final line, a record with an unknown `version`, a record whose
  `run_id`/`scenario_id` does not match its directory, a non-monotonic `seq`
  within one `process_start_id`, or a missing referenced artifact all fail
  the run. Readiness is not acceptance; events are not outcomes — the
  independent oracle validates wire/refusal/output evidence separately.
- Event append discipline: each record is one complete line written and
  fsync'd as a unit; a torn line is invalid by contract.

## 2. Event record schema (version 1)

One record per reached boundary or observed refusal, appended by the subject
adapter. Columns in exact order, tab-separated:

| # | column | format | meaning |
|---|--------|--------|---------|
| 1 | `version` | `1` | schema version of this record |
| 2 | `run_id` | label | fixture run identifier (directory name component) |
| 3 | `scenario_id` | label | scenario row identifier from the matrix |
| 4 | `subject_lang` | `rust` \| `java` | implementation under test |
| 5 | `subject_role` | `server` \| `client` | protocol role in this scenario |
| 6 | `process_start_id` | `<pid>-<start_nanos hex>` | distinguishes restarts of one fixture target |
| 7 | `seq` | decimal, from 1 | per-process monotonic event sequence |
| 8 | `boundary` | label, §2.1 | reached boundary, or empty for pure observations |
| 9 | `operation_id` | 32 hex, or empty | original immutable operation identity |
| 10 | `work_key` | `<scope>:<producer>:<entity>` decimal, or empty | work identity |
| 11 | `attempt` | decimal, or empty | work attempt identity |
| 12 | `refusal_code` | decimal per Appendix F refusal table, or empty | observed named refusal |
| 13 | `artifact_path` | label relative to the scenario directory, or empty | referenced evidence artifact |
| 14 | `artifact_len` | decimal bytes, or empty | exact artifact length |
| 15 | `artifact_sha256` | 64 hex, or empty | artifact content hash |

All three of `artifact_path`/`artifact_len`/`artifact_sha256` are present or
all empty. `operation_id` is always the original immutable identity; a
retry never mints a new one.

### 2.1 Boundary labels

Server (authority) boundaries, in lifecycle order:

`LISTENING`, `CONNECTION_AUTHENTICATED`, `SESSION_COMMITTED`,
`SESSION_RESPONSE_SENT`, `DECLARATION_COMMITTED`, `DECLARATION_RESPONSE_SENT`,
`INPUT_INSTALLED`, `ADMISSION_COMMITTED`, `ADMISSION_RESPONSE_SENT`,
`EXECUTION_CLAIMED`, `OUTPUT_INSTALLED`, `PUBLICATION_COMMITTED`,
`RETRY_COMMITTED`, `FENCE_COMMITTED`, `CLOSURE_COMMITTED`,
`RESULT_HEADER_SENT`, `RESULT_FIN_SENT`, `COMPLETE_RESPONSE_SENT`,
`DETACH_ACKNOWLEDGED`, `REFUSAL_SENT`, `SHUTDOWN_DRAINED`.

Client boundaries:

`INTENT_JOURNALED`, `REQUEST_SENT`, `RECEIPT_VALIDATED`,
`RECEIPT_JOURNALED`, `OBSERVATION_JOURNALED`, `RESULT_VERIFIED`,
`RESULT_INSTALLED`, `REFUSAL_RECEIVED`.

`*_COMMITTED` means the durable metadata commit finished, before any reply
is written. `*_SENT` means the local transport accepted the write; it never
claims peer receipt. `FENCE_COMMITTED` covers cancel, skip and scope-cancel.
Hooks may pause or report at a real boundary; they cannot forge commits,
receipts, callbacks or protocol results.

## 3. Fault schedule schema (version 1)

One schedule per scenario, TSV with the same escaping rules. Columns in
exact order:

| # | column | format | meaning |
|---|--------|--------|---------|
| 1 | `version` | `1` | schema version |
| 2 | `run_id` | label | must match the run directory |
| 3 | `scenario_id` | label | scenario row this schedule drives |
| 4 | `target` | label | fixture-owned process target (e.g. `server`, `client`, `worker-2`) |
| 5 | `boundary` | label, §2.1 | the reached boundary that arms the action |
| 6 | `action` | §3.1 | what happens at that boundary |
| 7 | `seed` | decimal 64-bit | deterministic selection/replay seed |
| 8 | `deadline_ms` | decimal | hard bound for the action's completion |

Rows apply in file order. A boundary that never arrives before the
scenario's overall deadline fails the run; it is never silently skipped.
No arbitrary shell commands anywhere in a schedule.

### 3.1 Actions

- `pause` — hold the target at the boundary until a matching `release` row
  fires or the fixture releases it; `deadline_ms` bounds the hold. A paused
  boundary has already committed whatever the boundary name says; pausing
  never alters the committed operation.
- `release` — release a prior `pause` on the same target/boundary.
- `disconnect` — close the target's network connection/transport at the
  boundary without process death.
- `drop-reply` — specialization of `disconnect` for lost-ACK: the boundary
  must be a `*_COMMITTED` boundary with a pending reply; the reply is
  withheld and the connection is reset. A lost-ACK claim requires this
  action (reached committed boundary + withheld reply), never a guessed
  sleep before a kill.
- `stop` — graceful termination signal (SIGTERM) at the boundary.
- `kill` — immediate hard kill (SIGKILL / `halt`) at the boundary, after
  the boundary's commit, before any further reply. A kill is not physical
  power-loss coverage and is labelled as process death only.
- `restart` — start the same target again with the same roots/journals and
  a new `process_start_id`; valid only after a `stop`/`kill` on that target.
- `clock-set` — advance the target's fixture clock to a scenario-relative
  offset; never changes host UTC. Requires a subject fixture clock.

### 3.2 Mapping note for existing adapters

Claude's Java fixture adapter (`FixtureMain`) accepts `pause`, `drop-reply`,
`exit`: `exit` is this schema's `kill`; the rest are name-identical.
Schedule columns above are exactly the adapter's accepted columns
(`version, run_id, scenario_id, target, boundary, action, seed,
deadline_ms`).

## 4. Consumption and acknowledgement

- Claude acknowledges the event/schedule columns his Java adapter emits and
  parses; Meta acknowledges the schedule schema for independently built
  workload runners. Acknowledgement is an explicit board note with the
  consumed commit hash and schema hash; silence is not assent.
- Scenario matrix rows (requirement IDs, direction, setup, trigger,
  expected outcome/refusal, artifact and resource checks) reference these
  boundary and action names verbatim; a matrix needing a new boundary or
  action is an interface revision proposal.
- Meta's workload failure schedule (contract §7) maps onto this schema as:
  "kill coordinator after submission, before saving an ACK" → `kill` at
  client `REQUEST_SENT` (before `RECEIPT_JOURNALED`); "kill worker after
  admission, before terminal commit" → `kill` at server
  `ADMISSION_RESPONSE_SENT` (before `PUBLICATION_COMMITTED`); "kill worker
  after result commit, before delivery" → `kill` at server
  `PUBLICATION_COMMITTED`; "kill coordinator during result download" →
  `kill` at client `RESULT_VERIFIED` of a partial selection.
