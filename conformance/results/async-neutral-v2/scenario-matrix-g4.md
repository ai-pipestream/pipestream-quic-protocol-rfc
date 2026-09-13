# Scenario matrix detail — group G4: publication races, fences, settlement

Requirement families: V2-ATTEMPT 2–4, V2-CANCEL 1–4, V2-CLOSE 5, V2-TIME 2.
Most rows need the subject hooks (pause at a committed boundary to create
a deterministic race). Until hooks land, each row notes a hook-free
statistical variant that is valid evidence but weaker: it proves the
outcome is one of the two legal orders, not that BOTH orders occur.

## g4-publication-vs-cancel (both orders)

- Arm a staged callback result (application computed, publication not yet
  committed), then race cancel against publication.
- Hook form: `pause` the server at `EXECUTION_CLAIMED`, stage the result,
  release into a simultaneous cancel; repeat with the pause forcing the
  opposite order (cancel fence first via `pause` before
  `PUBLICATION_COMMITTED`).
- Expected: exactly one wins. Publication wins → cancel returns the
  existing terminal outcome (disposition 1), result readable byte-exact.
  Fence wins → old attempt cannot publish, outcome is cancelled, result
  read refuses appropriately. Never both, never neither; exactly one
  terminal commit exists (single status leaf for the work).

## g4-publication-vs-skip

- Same shape with skip (requires skip-authorized owner: `--allow-skip`).
- Expected: skip fence fixes the outcome SKIPPED; unresolved descendants
  settle CANCELLED without success counting; a later publication is
  fenced; a later conflicting cancel/skip refuses CANCELLED (12).

## g4-publication-vs-deadline

- Admission with a short execution deadline; hold the callback (pause)
  past the deadline, then release into publication.
- Expected: deadline expiry triggers fenced settlement (not silent
  eviction); the late publication cannot commit; work view shows the
  deadline-settled outcome; explicit retry afterwards refuses
  DEADLINE_EXCEEDED (11) since the original deadline passed — and does
  not erase the declared obligation.

## g4-stale-attempt-retry

- Retry to attempt 2 (fence attempt 1), then attempt a SECOND retry
  naming expected-attempt 1 (stale) with a new operation ID.
- Expected: CONFLICT (7) for the stale expected-attempt; replaying the
  FIRST retry operation (same immutable ID) returns its original receipt
  without advancing the fence again; attempt is exactly 2, never 3.
- Status (milestone 19a, work in Kimi's role): attempt 2 must be LIVE when
  the stale retry arrives, because both authorities check terminal state
  before the attempt mismatch and answer ALREADY_TERMINAL (18) for a work
  that already finished. The Java server direction holds attempt 2
  deterministically with a schedule pause at EXECUTION_CLAIMED (pause,
  release, pause: the FixtureMain consumes one pause row per reached
  boundary, so the first row is attempt 1's claim and is released as soon as
  the subject records it, the second holds attempt 2's claim until the
  CONFLICT "retry attempt changed" has been observed). The Rust subject's
  hooks accept pause only at the three reply pairs (src/v2/fixture.rs
  REPLY_PAIRS), so that direction keeps attempt 2 live with a 16 MiB copy
  (the negotiated object limit) and records the mechanism as a named gap in
  expected.tsv/observed.tsv (`attempt_2_hold`, `attempt_2_live_evidence`); a
  deterministic hold there is a server-crate hook request.

## g4-ancestor-fence-publication

- Mode-1/2 branch: cancel the parent (ancestor fence) while a child
  publication is staged.
- Expected: the child publication is fenced by the ancestor; parent
  stays effectively CANCELLING until descendant scopes close; committed
  successful descendants survive; the accepted fence is never
  overwritten with FAILED while descendants settle.

## g4-stale-lease-publication

- Worker lease expires (callback held past lease via pause), the job is
  reclaimed, then the old worker attempts publication.
- Expected: publication commit rechecks the durable worker lease — the
  stale worker cannot publish; no new wire attempt is created by the
  internal reclaim; renewal with an already-expired replacement lease
  refuses (CLOCK_UNSAFE/lease refusal per implementation, recorded).

## g4-revocation-vs-publication

- Offline operator revocation of the session while a publication is
  staged (revocation is owner-independent root cancellation).
- Expected: publication fenced; existing and new connections see the
  revocation; scheduling stops; transmitted bytes not retracted.

## g4-eventual-settlement

- After any fence above with unresolved descendants: bounded eventual
  settlement — every nonterminal descendant reaches CANCELLED within a
  stated test deadline under a functioning transport; the status tree
  converges to exactly one terminal state per work; cleanup/retirement
  never runs before root closure plus all longer promises.

## Evidence rules for this group

- Every race row records which order occurred (observed.tsv
  `winning_order=publication|fence`), and the matrix is incomplete until
  BOTH orders are evidenced per externally meaningful row.
- Disposition-0 fence receipts may already carry a terminal state
  (spec contradiction #8) — the driver accepts either, records which.
- Cancellation/skip/deadline/refusal labels stay distinct in evidence;
  an injected exception is never logged as a cancel.
