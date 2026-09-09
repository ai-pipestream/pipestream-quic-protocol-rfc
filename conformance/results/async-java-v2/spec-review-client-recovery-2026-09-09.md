# Review: `docs/client-recovery-guidance-2026-09` (7315713) against the Java V2 endpoints

Reviewer: Claude (A). Subject: branch `docs/client-recovery-guidance-2026-09`
at `7315713`, one commit on `8eb5a17`, 300 added lines in `section-12.md`,
new `appendix-g.md`, `durable-work-v2-test-plan.md` (CR01-CR14) and the
disposition note. Java state reviewed: `agent/rfc-claude-java-v2` at
`417ba83` on transport `pipestream.4`. This records implementation deltas as
the disposition note asks; it is not Java evidence for the new scenarios.

## 1. Verdict on the text

The change is a clarification, not a wire change: no message, code, CBOR
cardinality, commitment or frozen vector moved, and the numbering it cites
(12.4 identity and replay, 12.6 attempts, 12.7 result delivery, 12.9 clocks)
matches the branch's headings. Every new MUST is one the Java endpoints
already satisfy or one whose gap is named below; none contradicts the Rust
behaviour observed in interop. Three points deserve attention before merge:

1. **Refused-stream retirement (12.1) is now normative, and the reference
   transport violated it until yesterday.** Upstream quiche never collected a
   peer-unidirectional stream after a local read shutdown once the peer's
   RESET or FIN arrived, so every refused input consumed a MAX_STREAMS slot
   for the life of the connection. The `pipestream.4` bundle (`e1533d3`)
   fixes it; the Rust authority (quinn) has not been measured on this rule.
   CR14 should require the stream-credit observation explicitly ("a
   replacement stream can be opened after N refusals with a stream allowance
   of N"), which is what `DurableWireNegativeTest.stalledInputs…` and
   `sequentialInputsBeyond…` assert, so both stacks are held to the same
   measurable bar.
2. **The "local bound that ended a transfer" SHOULD (12.1) has no wire
   carrier.** The refusal frame carries only a code and a bounded detail
   string, and the text rightly says the detail is not machine-readable.
   Either state that the diagnostic is local (logs, fixture events) or add a
   field in a later revision; as written, an implementation can satisfy the
   SHOULD only out of band. See delta D1.
3. **12.2.1 conditions its MUSTs on "a client that performs automatic
   recovery".** Both reference clients are one-shot: recovery is a new
   invocation that replays the journal. That is compliant, but CR01 ("a
   recovering client uses the same operation… with bounded waits") then needs
   a driver-side retry loop or a client flag; the plan should say which, or
   Kimi's CR01 row cannot be run against either reference client. See D2.

Smaller wording notes:

- 12.2.1 NOT_READY row: "After detach, use a new connection and attachment
  rather than retrying on the drained connection" is right, but Section 12
  elsewhere says a connection binds one session and DETACH ends it; a
  cross-reference would prevent readers from trying ATTACH on the same
  connection.
- 12.3 pacing paragraph: "An admission receipt resolves its request but is
  not evidence that its job released an executor slot" is consistent with the
  Java scheduler (claim happens later, off the request path); worth adding
  that the same holds for `EXECUTION_CLAIMED`-style fixture boundaries, which
  are host events, not client-visible.
- Appendix G: "An API enum such as `RestartSafety` is an application
  declaration, not an independent certification" matches
  `DurableHost.RestartSafety` and the api-plan wording; no delta.
- Disposition note, Rust `DurableSession::receipt`: the Java journal makes the
  same distinction (`ClientJournal.operation` absent = unjournaled intent,
  present with no receipt = pending), so the SDK note applies to both.

## 2. Java implementation deltas against the new text

| ID | Rule | Java at 417ba83 | Delta |
|---|---|---|---|
| D1 | 12.1 SHOULD expose offered/selected deadlines and the bound that ended a transfer | Server refusals carry constant details ("input refused", "refused"); the internal `ProtocolError` detail naming header/idle/lifetime is dropped at `DurableServer.sendInputRefusal`/`respondRefusal`. Client-side aborts keep their detail in the exception. Selected limits are available from `Capabilities` on both sides. | Put the internal detail (bounded label) into the refusal detail and log the offered/selected values in the fixture event; keep it non-normative. Small change. |
| D2 | 12.2.1 automatic recovery bounds; CR01 | `DurableClient` performs no automatic retry; `ClientCommands` is one-shot and replays journaled intent on the next invocation with the same operation id and a fresh request number (covered by `DurableLifecycleTest`, `FixtureMainTest`). | Compliant as a non-automatic client. If CR01 is to run against the Java client, add an opt-in bounded retry (`--retry-budget N --backoff-ms`) in `ClientCommands` that reuses the journaled intent and stops with an "unresolved" report; not needed for conformance of the library. |
| D3 | 12.2.1 LIMIT_EXCEEDED "not proof of executor saturation"; CR02 | Server answers LIMIT_EXCEEDED for oversize input, storage-worker exhaustion (`DurableLifecycleTest`), stream deadlines (`DurableWireNegativeTest`) and header ceilings. Client surfaces the code unchanged. | None for the library. `V2Main` exits 1 for every failure and prints the message only; a driver cannot distinguish codes by exit status. Consider printing `REFUSED <CODE>` on stdout for the neutral driver (CR-table "named code"). |
| D4 | 12.2.1 CONFLICT: no parameter change under one operation id | `ClientJournal.journalMutation` refuses a different template under the same id with CONFLICT before anything is sent; the authority refuses the same on replay. `DurableMutationTest` covers a stale expected attempt. | None. CR05 "same id, different parameters" is covered locally before transmission; a driver wanting the *server's* CONFLICT needs a raw peer, which `DurableWireNegativeTest` already uses. |
| D5 | 12.2.1 NOT_FOUND with an earlier transmission in flight (CR04) | The client never infers a new identity from absence; `unresolved` lists journaled intents without receipts. | No test drives "lookup says NOT_FOUND while the earlier request later commits"; needs the fixture `pause` at `DECLARATION_COMMITTED` plus a lookup on a second connection. Addable with existing hooks. |
| D6 | 12.2.1 local journal failure surfaced (CR10) | Every mutation journals before sending (`INTENT_JOURNALED` precedes `REQUEST_SENT`); receipt-save failure completes the operation exceptionally. | No test injects a journal I/O failure (process death is covered). Needs a fault hook in `ClientJournal` (test-only) or a read-only journal file; not yet built. |
| D7 | 12.3 pacing with `v2-limits`; CR11/CR12 | `Binding.limits()` is journaled and printed by `client binding`. The client does not pace; the server enforces per-owner and global job ceilings and answers LIMIT_EXCEEDED. | Compliant (pacing is a SHOULD on clients that admit concurrently). No Java test measures admissions against the advertised active-job ceiling; CR11 needs a workload-style driver (Meta's coordinator already does this against the Rust authority). |
| D8 | 12.1 idle renewal only by payload progress (CR13) | Server input: `lastProgress` moves only on non-empty chunks; client result: `Payload.feed` moves it only when `count != 0`. Keepalives never touch stream state. | Covered at unit level only; no wire test sends empty frames/keepalives during a stalled transfer. Addable to `DurableWireNegativeTest` with the raw peer. |
| D9 | 12.1 refused-stream retirement (CR14) | Satisfied on transport `.4`; `stalledInputs…`, `sequentialInputsBeyond…`, `DurableClientResultNegativeTest` (aborted result streams then a valid transfer) and the native `QuicStreamCreditTest`. | None for Java. Rust authority not measured on this rule by me. |
| D10 | 12.2.1 EXPIRED/OUTPUT_UNAVAILABLE and CLOCK_UNSAFE rows (CR06/CR07) | Expiry under a pinned read covered; the client reports the code and never re-admits. CLOCK_UNSAFE and "restore authorization" are not exercised. | Add a clock-regression wire test (host clock injectable through `UtcClock`) and an authorization-restore step to `DurableAuthorizationTest`. |

## 3. Mapping of CR01-CR14 to existing Java evidence

Covered now: CR02 (three of four LIMIT_EXCEEDED sources), CR03 (detach then
NOT_READY on the drained connection; watch WAIT_TIMEOUT in `DurableServerTest`),
CR05 (stale expected attempt), CR07 (expiry), CR08 (framing/integrity scopes,
server and client side), CR09 (disconnect, lost ACK, process death), CR14.
Partly covered: CR01 (replay after refusal, without an automatic budget),
CR04 (lost reply replay, without the concurrent-lookup shape), CR06 (revoke
without restore), CR13 (unit level). Not covered: CR10 journal I/O failure,
CR11/CR12 pacing measurements, CLOCK_UNSAFE.

None of these are claimed as passed for the new plan; the existing evidence
keeps its original scope.

## 4. Resolution (later on 2026-09-09)

The three merge points were resolved as follows. Spec text: on the guidance
worktree of `docs/client-recovery-guidance-2026-09` (edits left uncommitted
for the coordinating owner), Section 12.1 now says the refused-stream rule
includes replenishing the peer's concurrent-stream allowance once the
reset/FIN exchange completes and that the "bound that ended a transfer"
diagnostic is local with no wire carrier (a REFUSAL detail MAY name it, a
peer MUST NOT depend on it); Section 12.2.1 says a one-shot client that
reopens its journal on the next invocation is a conforming recovery mode with
the waiting bounds on the driver; the test plan states the CR01 recovery
mode(s) and the CR14 stream-credit observation; the disposition note records
this outcome.

Java (head of `agent/rfc-claude-java-v2`, evidence in
`conformance/results/durable-work-v2-java-client-recovery-2026-09-09.txt`):
D1 closed (REFUSAL details name the local bound; `REFUSED code= detail=` and
`client capabilities` output), D2 closed (`--retry-budget`/`--retry-backoff-ms`
with `RECOVERING`/`UNRESOLVED` reporting; both modes tested against real
refusals in `ClientRecoveryTest`), D3 closed (driver-readable refusal line),
D9 extended to the Rust authority (`RawPeerRustAuthorityTest`: twelve refused
inputs, credit back to the selected limit after each, valid transfer after).
D4-D8 and D10 stand as recorded above.
