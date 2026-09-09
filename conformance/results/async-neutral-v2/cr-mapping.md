# CR01–CR14 mapping to neutral driver cases

Owner: Kimi (B). Requested by the coordinating handoff
`SPEC-CLIENT-RECOVERY-2026-09-09.md` (spec branch
`docs/client-recovery-guidance-2026-09` @ `7315713`, user-approved).
Pins: driver head `fd52a9f`; rust subject `097829fa…`; java jar
`ff537a60…` (re-pin to Claude FINAL `63d03a0`/`.4` queued — see handoff).
Status convention: COVERED = an existing row exercises the required
observation; PARTIAL = some arms exist, named gaps for the rest; NEW =
needs a new row/fixture. CR rows are required scenarios; nothing here is
retrospective relabeling — final CR evidence lands in a separately
identified checkpoint.

| CR | Map | Status |
|---|---|---|
| CR01 refuse-N-then-allow | g2 replay rows prove same-op recovery after loss, but a *finite pre-commit refusal injection* does not exist in either subject's hooks. NEW fixture action needed → interface proposal below. | NEW (fixture) |
| CR02 LIMIT_EXCEEDED contexts | r-connection-ceiling, r-staging-and-journal-bounds, g1-oversize-payload, g1-declaration-capacity (capacity refusals recorded with the limiting context string); client-side budget reporting is driver-side policy, recorded per run. | PARTIAL (r-* pending) |
| CR03 NOT_READY/WAIT_TIMEOUT recovery | g8-detach-drains (post-detach correlated NOT_READY + ID consumption); checkpoint WAIT_TIMEOUT observed in g1-mode1-branch/g8 rows; watch unchanged-view in every watch row. | COVERED |
| CR04 lost reply + NOT_FOUND in flight | g2-crash-after-create-commit, g2-drop-reply-declaration/-admission, g2-kill-client-after-request-sent (resent-after-not-found path), g8-timeout-no-completion-claim. Same identity, one effect, matching receipt — all asserted. | COVERED |
| CR05 CONFLICT identity preservation | g2-duplicate-op-changed-params, g2-simultaneous-duplicate, g4-stale-attempt-retry (no automatic identity cycling anywhere; client journal guards recorded). | COVERED |
| CR06 revoke/restore + CLOCK_UNSAFE | g4-revocation-vs-publication (revoke leg); RESTORE leg new (re-add principal post-revoke — revocation is durable per spec, so restore means new session: row addition needed); CLOCK_UNSAFE = named gap (no fixture clock). | PARTIAL |
| CR07 EXPIRED/OUTPUT_UNAVAILABLE/DEADLINE/CANCELLED/ALREADY_TERMINAL distinctions | g7-receipt-before-output-expiry (EXPIRED≠OUTPUT_UNAVAILABLE asserted), g4-deadline-settlement, g4-publication-vs-cancel/-skip, g2-kill-at-publication-commit (ALREADY_TERMINAL 18). OUTPUT_UNAVAILABLE never observed (correct: no committed object went missing — a storage-corruption probe is a possible new row). Local-vs-remote copy distinction: named gap (CLI surface). | PARTIAL |
| CR08 FRAME_ERROR/EXTENSION_UNSUPPORTED/APPLICATION_UNSUPPORTED/INTEGRITY_ERROR scopes | G6 raw probes (M14 in flight: canonical violations, direction/correlation, stream identity/FIN). APPLICATION_UNSUPPORTED (unknown app label) trivially addable — new mini-row queued. | PARTIAL (M14) |
| CR09 CONTROL_RESET/disconnect/cancel/deadline during unobserved admission | g2/g8 kill + drop-reply rows (CONTROL_RESET close observed); uncontrolled disconnects labelled; transport failure never becomes authoritative cancellation — asserted via post-restart views. | COVERED |
| CR10 journal failure before send / while saving receipt | NEW row: kill client before intent persistence (no send), read-only journal path (send must fail), restart resolution without discarding uncertainty. Uses existing client-death machinery + fs permissions. | NEW (row, no fixture change) |
| CR11 pacing: executor credit ≠ retained-byte release | r-connection-ceiling, r-stalled-principal-progress, r-staging-and-journal-bounds (admission receipts do not replenish executor credit; terminal≠byte-release — measured). Awaits .4 transport re-pin. | PARTIAL (r-* pending) |
| CR12 shared contention below ceilings | r-stalled-principal-progress (abusive + healthy principal; healthy progress asserted within deadline). | PARTIAL (r-* pending) |
| CR13 idle vs lifetime vs unrelated traffic | g6-stopped-control-and-transfers covers reset/loss classes; the keepalive/non-renewal arms (unrelated traffic never renews idle; progress never extends lifetime; no late FIN revives) need explicit raw probes — added to the G6 follow-up list. | PARTIAL |
| CR14 refused/redundant streams then valid transfer | g6-stream-identity-and-fin + g6-stopped rows (M14); bounded pending/handle evidence; MAX_STREAMS credit regression on .4 transport (Claude's quiche fix) — my probes will record credit behavior per transport pin. | PARTIAL (M14 + .4) |

## Interface proposal: finite refusal injection (`refuse` action)

Needed for CR01 and cleaner CR02 arms. Proposed as an interface-v1
revision (minor, backward-compatible addition; peers confirm before
adoption):

- New schedule action `refuse`, with two additional columns APPENDED to
  the schedule row (new columns are a minor revision; old parsers must
  reject unknown column counts, so this is version `1.1` of the schedule
  schema): `refusal_code` (decimal, Appendix F table) and
  `refuse_count` (decimal, finite, ≥1).
- Semantics: at the named PRE-COMMIT boundary, the subject answers the
  operation with the named refusal, at most `refuse_count` times, then
  lets the next attempt proceed. The refusal must be pre-commit (no
  state change); after the operation commits the armed row is spent.
  A `refuse` row at a `*_COMMITTED` boundary is rejected (cannot
  synthesize a pre-commit refusal after commit — the CR fault-fixture
  rule).
- Versioning: schedule `version` column becomes `1.1`; parsers accepting
  `1` continue to accept `1` files (8 columns). Event schema unchanged.

Posted on the board for Claude/Meta acknowledgement before any subject
implements it. No subject change is made in this mapping checkpoint.

## Known gaps retained (unchanged by the spec delta)

Fixture clock (CLOCK_UNSAFE rows), java-server fresh-commit gating,
java restart readiness >30 s, revoke CLI on Java, OUTPUT_UNAVAILABLE
probe, local-vs-remote copy surface, java CLI quirks
(--max-execution-ms, empty-manifest select, watch deadline field).
