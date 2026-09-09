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

### g7-unsafe-clock-refusal
- Requires the fixture clock (or a subject's untrusted-clock mode):
  regress the clock, then attempt admission, retry, publication, and
  destructive expiry.
- Expected: fresh promises refuse CLOCK_UNSAFE (17); read-only
  retrieval of retained evidence continues; NO destructive expiry runs
  under unsafe time; after the clock recovers, persisted watermark
  checks still refuse back-dated commits (final sample earlier than the
  transaction's initial observation = CLOCK_UNSAFE).

### g7-deadline-queue-time
- Saturate the worker pool (hold callbacks via pause hooks), admit work
  whose execution deadline includes the queue wait.
- Expected: elapsed deadline accounting includes queue time from
  admission; a job whose original deadline passed while queued settles
  via fenced deadline expiry, never silent eviction; explicit retry
  after that refuses DEADLINE_EXCEEDED (11).

### g7-cleanup-interrupted-refund
- Kill the server during terminal cleanup (after output expiry, before
  accounting reconciliation finishes).
- Expected: cleanup is replayable; reserved capacity is never refunded
  while a dependent read-pin/callback retains it; restart completes the
  refund exactly once (capacity accounting before/after recorded;
  double-refund = defect).

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
