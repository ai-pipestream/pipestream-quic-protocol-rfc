# Scenario matrix detail — groups G7 (time/expiry) and G8 (completion/detach)

Requirement families: V2-TIME 1–6, V2-VIEW 3, V2-CLOSE 1–5, V2-RESULT 5–6.
Clock rules: test clocks advance only through an explicit fixture clock;
host UTC is never changed. Where a subject lacks a fixture clock today,
the row uses short policy durations and real elapsed time, and records
`clock_mode=real-short-policy` — slower but honest.

## G7 — independent lifetimes and trusted clocks

### g7-receipt-before-output-expiry
- Policy: output-retention < receipt-retention. Publish, wait past
  output availability.
- Expected: result read refuses EXPIRED (6); manifest and work view
  remain readable (retained receipts after output expiry); the terminal
  outcome never flips; no re-execution on read.

### g7-output-before-receipt-expiry
- Inverse policy (receipt < output is permitted where the spec allows;
  record if a subject rejects the policy at create).
- Expected: operation lookup after the receipt deadline refuses EXPIRED
  (never a fresh mutation); retained output still readable while its
  own availability promise holds; identity/digest retained for
  anti-reuse after full receipt expiry.

### g7-read-pin-past-expiry
- Start a result read (large object, slow reader: pause the client
  mid-stream via fixture client or bounded reader) admitted BEFORE
  output expiry; let expiry pass mid-read.
- Expected: the admitted read completes byte-exact (bytes pinned until
  FIN/abort); a NEW read after expiry refuses; the pinned object is
  reclaimed only after the read finishes (file length evidence
  supplementary); disk reads/enqueueing do not renew the sender idle
  deadline.
- Status (milestone 19b, work in Kimi's role): implemented, green on both
  servers (durable-18d4abe3eae3a613). Output retention 5 s; a 16 MiB copy
  (the negotiated object limit) is published and selected; a raw reader
  opens the result stream about 4.7 s before availability passes and drains
  256 KiB every 150 ms, so about 8.1 MiB of 16 MiB had arrived at expiry on
  both subjects; a CLI read issued after expiry while the transfer was open
  refuses named EXPIRED (6) on both (rust "result availability expired",
  java "output availability expired"); the pinned read then completes
  byte-exact. Object-directory totals: the Rust directory is unchanged while
  the read is open and drops to metadata only within 30 s of the read
  finishing; the Java directory loses one 16 MiB object WHILE the read is
  open (5 files / 33.5 MB after publication, 4 / 16.8 MB at expiry) and the
  read still completes byte-exact, so either the expired output was unlinked
  under the open descriptor or the retained input was reclaimed; directory
  totals do not distinguish the two and the row records it as an
  observation for Claude, not a defect. The sender idle bound was never
  renewed by anything but the reader's own consumption (the paced drain).

### g7-unsafe-clock-refusal
- Requires the fixture clock (or a subject's untrusted-clock mode):
  regress the clock, then attempt admission, retry, publication, and
  destructive expiry.
- Expected: fresh promises refuse CLOCK_UNSAFE (17); read-only
  retrieval of retained evidence continues; NO destructive expiry runs
  under unsafe time; after the clock recovers, persisted watermark
  checks still refuse back-dated commits (final sample earlier than the
  transaction's initial observation = CLOCK_UNSAFE).
- Status (milestone 19b, work in Kimi's role): the row RUNS and reports the
  capability it lacks. Neither subject has a fixture clock or an
  untrusted-clock mode: the only clock flag either `serve` offers is
  `--trust-system-clock` (both usage texts archived), the Rust hooks reject
  `clock-set` as driver-side and the Java FixtureMain refuses it as "driven
  by the fixture, not the subject" (both refusals archived verbatim).
  INCOMPLETE with that named missing capability; the request to both owners
  stands (rust-hook-proposal.md, A7). Host UTC is never changed.

### g7-deadline-queue-time
- Saturate the worker pool (hold callbacks via pause hooks), admit work
  whose execution deadline includes the queue wait.
- Expected: elapsed deadline accounting includes queue time from
  admission; a job whose original deadline passed while queued settles
  via fenced deadline expiry, never silent eviction; explicit retry
  after that refuses DEADLINE_EXCEEDED (11).
- Status (milestone 19b, work in Kimi's role): implemented, green on both
  servers (durable-18d4abe3eae3a613). Java server: two `pause` rows at
  EXECUTION_CLAIMED (pause, release, pause) hold alice's two per-owner
  workers (ExecutionLimits(4, 2, ...)); the probe (64 KiB copy,
  --execution-ms 1000, which the Java client honours: receipt deadline =
  admittedAt + 1000) queues behind them, its deadline passes, and it settles
  FAILED with the DEADLINE_EXCEEDED diagnostic BEFORE the pool is released
  with zero EXECUTION_CLAIMED records for it (fenced deadline expiry of a
  never-claimed job, not silent eviction). Rust server: the fixture cannot
  pause at EXECUTION_CLAIMED, so alice's two slots (workers_per_owner 2)
  are loaded with two 4 MiB chunk-copy parents and the probe is admitted
  behind them; the property held on the first attempt (settled FAILED/11 at
  terminal_at 141 ms after the deadline, never claimed to success) and the
  direction names the hook-free mechanism in expected.tsv; had the load not
  outlasted the deadline in three fresh authorities the direction would
  report the missing hold as INCOMPLETE rather than claim the property. The
  explicit retry afterwards refuses ALREADY_TERMINAL (18) on both, as in
  g4-deadline-settlement (the matrix says DEADLINE_EXCEEDED; both named codes
  are accepted and the one observed is recorded); the read refuses NOT_FOUND.

### g7-cleanup-interrupted-refund
- Kill the server during terminal cleanup (after output expiry, before
  accounting reconciliation finishes).
- Expected: cleanup is replayable; reserved capacity is never refunded
  while a dependent read-pin/callback retains it; restart completes the
  refund exactly once (capacity accounting before/after recorded;
  double-refund = defect).
- Status (milestone 19b, work in Kimi's role): the row RUNS and reports the
  capability it lacks. interface-v1 section 2.1 has no cleanup boundary (the
  row archives the 29 labels; none matches CLEANUP, RETIRE or REFUND);
  CLOSURE_COMMITTED is scope closure, not cleanup, and the Rust subject's
  retention-* and retirement-* commit keys are supplementary and unarmable
  by design (src/v2/fixture.rs commit_label). INCOMPLETE with that named
  missing capability; adding a boundary is an interface revision proposal
  that both subjects would have to implement.

### g7-no-deadline-extension
- For one admitted work: reconnect, re-read the manifest, replay the
  admission operation, and issue an explicit retry — then compare all
  deadline fields.
- Expected: execution deadline = original admission + original
  execution-ms everywhere; receipt/output deadlines = original terminal
  commit + policy; no operation extends any of them.

## G8 — exact completion and distinct detach

### g8-exact-root-complete
- Build a root scope with leaf + mode-1 branch + mode-2 descendants;
  seal; settle everything; issue DRAIN complete with the exact root
  summary.
- Expected: complete accepted (op 1 echo); the summary's four counters
  and declared-count equality verified independently by the driver;
  status-tree root recomputed from domain-separated leaves/nodes
  (pipestream-status-leaf-v2/-node-v2/-empty-v2; odd-node duplication)
  matches the server's summary exactly.

### g8-child-cut-conflict
- Attempt complete with a child-scope summary instead of the root, and
  with an altered root summary (one counter changed).
- Expected: CONFLICT (7) both times; the session is not closed; a
  subsequent correct complete succeeds.

### g8-complete-with-pending
- Attempt complete while work is nonterminal, and while a result
  transfer is live on the connection.
- Expected: NOT_READY (9); nothing settles early; after settlement the
  same complete succeeds.

### g8-detach-drains
- Open requests + a live transfer, then DRAIN detach.
- Expected: ack only after existing requests/transfers drain (bounded
  wait); new requests after the accepted detach get correlated
  NOT_READY and still consume request IDs; detach claims nothing about
  durable completion (work continues to settle afterwards, verified by
  a fresh attach).

### g8-half-close-preserves-responses
- Client sends requests, then requests detach, THEN half-closes (FIN) its
  control send direction before the remaining responses arrive (the §12.8
  MAY is scoped to "after requesting detach" — see
  normative-clarifications-review.md item 5).
- Expected: pre-FIN responses — including post-detach correlated
  refusals — are still delivered in full within their bounded delivery
  attempt; if the server closes gracefully it first finishes its control
  send direction and observes QUIC ACK of bytes+FIN (bounded, never
  extending the detach lifetime); abrupt disconnect has the same
  non-effect on durable work. A bare control FIN BEFORE detach is instead
  a g6 framing row expecting FRAME_ERROR (0x201).

### g8-timeout-no-completion-claim
- Kill the connection mid-complete (drop-reply at the reply stage or
  disconnect).
- Expected: a control timeout/loss is never durable completion; the
  un-ACKed complete leaves the session open; the client must not report
  completion without the echo (driver checks client output/journal
  claims nothing).
