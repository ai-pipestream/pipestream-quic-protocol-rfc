# Scenario matrix detail — group G2: crashes, lost ACKs, operation uncertainty

Requirement families: V2-OP 3–4, V2-SESSION 2, V2-STORE 1, V2-TIME 1.
Interface: boundary/action names per `interface-v1.md`. Directions: every
row runs rust-client/rust-server now; java directions join when Claude's
checkpoints land (INCOMPLETE until then, never skip-pass).

Lost-ACK rows use `drop-reply` at a `*_COMMITTED` boundary (committed
metadata + withheld reply + connection reset). A guessed sleep before a
kill is not a lost-ACK test. Process-death rows use `kill` (SIGKILL) and
are labelled process death, never power loss. After `restart`, the same
roots/journals are reopened; a new `process_start_id` must appear in the
event stream.

## g2-crash-before-create-commit

- Setup: fresh authority + client journal; schedule `kill` on server at
  `CONNECTION_AUTHENTICATED` (before `SESSION_COMMITTED`).
- Expected: session create does not commit; after server `restart`,
  client replays the SAME creation (same creation-sequence, same policy)
  and receives a first-generation binding; no phantom session exists
  (subsequent attach with that generation before successful create =
  NOT_FOUND/CONFLICT per existence rules).
- Oracle: driver derives the expected binding fields (authority, owner,
  policy echoed exactly) from its own fixture configuration.

## g2-crash-after-create-commit (lost SESSION response)

- Schedule `drop-reply` at `SESSION_COMMITTED`; client observes connection
  loss with the create unresolved in its journal.
- After `restart` (server) and client reconnect: identical creation replay
  returns the identical generation + receipt, creation high-water did not
  double-allocate (a following `next-sequence` matches the first create's
  sequence + 1, not + 2).
- Changed-policy replay under the same sequence MUST refuse CONFLICT (7);
  sequence > next expected MUST refuse CONFLICT (7).

## g2-drop-reply-declaration

- Schedule `drop-reply` at `DECLARATION_COMMITTED` for a declared batch.
- Expected: replayed identical declare returns the identical declaration
  receipt (scope, accepted-count, seal-or-null unchanged); changed
  membership under the same operation ID refuses CONFLICT (7); the
  declaration consumed membership capacity exactly once (subsequent pages
  show each entity once).

## g2-drop-reply-admission

- Declare entity; schedule `drop-reply` at `ADMISSION_COMMITTED`; input
  stream was fully received and installed before the commit.
- Expected: identical admission replay (same operation ID, same header
  commitment) returns the same admission receipt with attempt 1, without
  re-execution (application invocation count observable via the
  application contract / event boundary `EXECUTION_CLAIMED` appearing
  once); changed input bytes under the same operation ID refuse
  CONFLICT (7); a matching replay may either return the receipt or stop
  the redundant stream with application error 0 plus a successful
  operation lookup — STOP_SENDING alone is not admission evidence.

## g2-kill-after-admission-before-publication

- Admission receipt observed; schedule `kill` on server at
  `EXECUTION_CLAIMED` (worker claimed, no publication).
- After `restart`: work remains admitted with attempt 1 and its original
  deadline; restart reconciliation does not fabricate a failure outcome
  before deadline; the job is re-dispatchable (application eventually
  publishes under attempt 1, or explicit retry is required per the
  restartable-job contract — expected behavior pinned per row evidence).
  No new wire attempt is created by the restart itself.

## g2-kill-at-publication-commit

- Schedule `kill` on server at `PUBLICATION_COMMITTED` (the terminal
  commit is durable, then the subject exits before/while the reply is
  delivered).
- Expected: the terminal outcome + manifest committed exactly once;
  post-restart WORK view shows SUCCEEDED with the same attempt; result
  read returns the exact object (driver-verified SHA-256); a duplicate
  publication callback cannot occur (fence advanced); retry after the
  terminal commit refuses ALREADY_TERMINAL (18) or CANCELLED (12) — the
  spec overlap is recorded, both accepted, determinism per implementation
  noted in evidence.
- Implemented in milestone 6 (rust direction; java-client/rust-server
  direction runs when the jar publishes the client ops). The matrix's
  `g2-drop-reply-publication` variant (drop-reply at the same boundary)
  is not implemented yet: the kill variant exercises the same durable
  expectations, and the withheld-reply half is covered by the
  declaration/admission drop-reply rows.

## g2-drop-reply-publication

- Schedule `drop-reply` at `PUBLICATION_COMMITTED`.
- Same observable expectations as `g2-kill-at-publication-commit`; the
  row is registered in the driver matrix but not implemented yet (the
  kill variant above is the milestone-6 evidence for this boundary).
- Status (milestone 19b, work in Kimi's role): the row now RUNS and reports
  the capability it lacks instead of "not implemented yet". Neither subject
  exposes a PUBLICATION reply pair to withhold: publication is observed via
  a watch, not answered on a correlated reply, so interface-v1's drop-reply
  (a committed boundary with a pending reply) has nothing to withhold. Both
  subjects refuse the schedule row at parse time (the Rust hooks accept
  drop-reply only at the three reply pairs; the Java FixtureMain requires a
  reply boundary) and the row archives each refusal verbatim
  (`artifacts/subject-schedule-refusal.txt`, `observed.tsv` row_status
  INCOMPLETE).
- Status (milestone 20, DECIDED by the coordinating owner 2026-09-13,
  implemented in Kimi's role): RETIRED. The row is removed from the driver
  matrix (66 of 67 rows remain); it no longer runs or FAILs in either dev
  or acceptance mode. Rationale: both subjects expose publication via a
  watch rather than a correlated reply pair, so a withheld reply that does
  not exist cannot be lost; the kill variant `g2-kill-at-publication-commit`
  is the delivered boundary evidence. The alternative (adding a correlated
  publication reply) is a protocol change and out of scope. The retirement
  is pinned by the `drop_reply_publication_is_retired_from_the_matrix`
  driver test; the milestone-19b MissingCapability evidence for this row
  remains in the M19 archives.

## g2-not-found-in-flight

- Setup: session created and one entity declared by the CLI client; a raw
  peer of the same principal, attached to the same session, writes the
  admission header and half of the declared payload on an open object
  stream and does not FIN; a second raw connection of the same principal
  sends the wire operation lookup (`Work::Operation`) for that id.
- Expected: NOT_FOUND (5) on the lookup's own control request tag while the
  admission is genuinely pending before its commit (nothing can commit
  before the FIN: the subject must verify the whole payload against the
  header's length and digest first); after the FIN the holder's
  `Work::Admitted` receipt and the prober's `Work::OperationResponse` receipt
  are byte-identical (attempt 1); the CLI client then replays the SAME
  admission (same id, same bytes, same parameters) and receives the durable
  receipt rather than a second effect, two CLI lookups are identical to it,
  the scope page shows one member, the work settles SUCCEEDED under attempt
  1 and the result reads back byte-exact. Where the subject records every
  reached boundary (the Java FixtureMain), EXECUTION_CLAIMED is recorded
  exactly once for the work.
- Mechanics (milestone 19b): the stream held open mid-transfer is the one
  construction deterministic on BOTH subjects. The Rust subject never
  reaches INPUT_INSTALLED and its hooks pause only at the reply pairs, so a
  pause at INPUT_INSTALLED would hold only the Java server; the raw hold is
  used for both and the direction label says so (rust-raw-peer + CLI
  client). The 500 ms wait between writing the header and sending the
  lookup is fixture timing, never evidence: the evidence is the refusal and
  the receipt that later arrives on the same stream.

## g2-kill-client-after-request-sent

- Schedule `kill` on client at `REQUEST_SENT` for an admit (client
  journaled the operation before transmission; server may or may not have
  committed — uncertainty is the point).
- Client `restart` + journal replay: issue operation lookup for the
  journaled operation ID. If NOT_FOUND (5): re-send the SAME immutable
  operation (same ID, same parameters) — NOT_FOUND while the original
  could still commit never authorizes a new identity. If the receipt
  returns: adopt it, no re-transmission. Either way exactly one admission
  exists afterwards (verified via scope page + work view).

## g2-duplicate-op-changed-params

- Same operation ID, same namespace, changed digest/type/parameters
  (e.g. different input length+hash) in one session.
- Expected: CONFLICT (7); the original committed operation is unaltered
  (lookup returns the first receipt); no second job exists.

## g2-simultaneous-duplicate

- Two concurrent connections (same principal) send the identical
  admission operation simultaneously.
- Expected: both callers receive the identical admission receipt; exactly
  one job/attempt 1 exists; serialization through the durable uniqueness
  constraint, never two jobs.

## Open mechanics

- Server-side `*_COMMITTED` boundaries need the test-only Rust hook
  (separate reviewable commit; Claude peer-checks placement). Until it
  lands, G2 rows that require `drop-reply` run in client-death variants
  only and the server-boundary rows report INCOMPLETE in dev mode.
