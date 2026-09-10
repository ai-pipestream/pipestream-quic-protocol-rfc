# Scenario matrix detail — group G3: storage boundaries, cleanup, retirement

Requirement families: V2-STORE 1–5, V2-ADMIT 3–4, V2-RESULT 2,5, V2-TIME 4–6.
Private storage probes (commit-key hooks, filesystem inspection of fixture
roots) are SUPPLEMENTARY: every row pairs them with a black-box network
recovery observation. No store/generation is ever deleted to make a
recovery test pass.

## g3-input-before-metadata

- Probe: arm `exit` at `prepare-input:before` and `admit-input:before`;
  after each kill, inspect the fixture root: staged payload bytes may
  exist (bounded orphan) but no metadata references them; a black-box
  operation lookup after restart returns NOT_FOUND (the admission never
  committed) and the SAME immutable operation re-admits cleanly.
- Expected: bounded orphan storage only; orphan cleanup reclaims it
  (file lengths + allocated blocks recorded before/after); no phantom
  work in any view/page.

## g3-orphan-cleanup

- Create orphans deliberately (kill between payload staging and
  admission commit, several runs), then restart and let cleanup run.
- Expected: orphan bytes reclaimed without touching committed objects;
  cleanup is replayable after interruption (kill DURING cleanup, then
  restart — cleanup resumes and completes; no double-refund of
  capacity; conservative over-charging acceptable, manufactured
  capacity is a defect).

## g3-terminal-cleanup

- Publish results, let output availability expire (short
  output-retention policy), keep receipt retention longer.
- Expected: output bytes reclaimed at expiry while the manifest/receipt
  remain readable; result read after expiry refuses EXPIRED (6), never
  OUTPUT_UNAVAILABLE; a read admitted BEFORE expiry finishes within its
  negotiated stream lifetime (bytes pinned until FIN/abort — pair with
  g7 read-pin rows); expiry is never a fresh admission.

## g3-partial-retirement

- Root-close a session (complete), wait out receipt+output promises
  (short policies), trigger retirement.
- Expected: retirement is durably recorded as a lifecycle transition
  BEFORE metadata removal; during incomplete cleanup authorized
  requests get EXPIRED (never partial replay); after removal, operation
  lookup may be NOT_FOUND but creation-sequence replay still refuses
  EXPIRED; owner creation high-water marks are preserved (re-creation
  with a retired sequence never mints a fresh session); anti-reuse
  tombstones outlive payload/receipt retention.

## g3-restart-same-roots

- Uncontrolled kill (SIGKILL) at a seeded point in a loaded session
  (declarations, admissions, publications, fences present), restart the
  same roots.
- Expected: recovery/reconciliation completes BEFORE new capacity is
  admitted (probe: immediate admit after restart either succeeds only
  after readiness or is refused NOT_READY — record which); declaration
  receipt/membership/seal consistency is exact (a receipt with missing
  members and matching counts must NOT report successful replay);
  reservations remain charged across the restart; fenced settlements
  from expiry/revocation complete after restart.

## g3-store-ownership

- Start a second server process against the SAME roots while the first
  is live.
- Expected: exclusive-ownership refusal (the second process fails
  startup explicitly; never two live writers); after the first stops,
  the second opens cleanly; a copied store opened by a non-owner guard
  must not unlock the live process's store (probe documented per
  implementation evidence).

## g3-nonreusable-history

- After full retirement, attempt: creation replay with a retired
  sequence (EXPIRED), operation replay with a retired operation ID
  (never reapplied), attach to a retired generation.
- Expected: named refusals everywhere; quota pressure never evicts a
  live promise (fill the store near limits with retired history, verify
  live sessions unaffected).
