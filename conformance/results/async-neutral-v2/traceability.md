# Requirement → scenario traceability (living document, assignment B)

Maps the V2-WIRE…V2-STORE acceptance-ledger families to the neutral
driver's scenario rows. Ledger bullets are unnumbered in the source; the
index below is this document's (family, bullet-order) convention. Status:
DONE = implemented and green in dev (INCOMPLETE-labelled until acceptance
mode + final review); SPEC = matrix detailed; HOOK = needs subject hooks
(rust hooks landed 152c280; java FixtureMain pending); OPEN.

Scenario matrix details: scenario-matrix-g1.md, -g2.md, -g3.md, -g4.md,
-g5.md, -g6-resource.md (G6+R), -g7-g8.md. Interface: interface-v1.md.

| Family | Bullets covered by neutral scenarios | Rows | Status |
|---|---|---|---|
| V2-WIRE 1–5 | framing/canonical/direction/correlation violations; unknown type classes; profile-dependent without profile | g6-canonical-violations, g6-direction-and-correlation, g6-stream-identity-and-fin | SPEC (raw probes; frozen wire.tsv 24 refuse-rows as case list) |
| V2-NEG 1–4 | capability intersection/dependency (result-delivery without durable-work); unsolicited selection; increased limits; request-ID accounting; duplicate/unsolicited response; named REFUSAL↔QUIC error mapping; stream-0 reset | g6-direction-and-correlation, g6-stopped-control-and-transfers, g5-unmapped-principal (0x200+3 close observed), g8-half-close-preserves-responses | PARTIAL DONE (error mapping observed in G5); rest SPEC |
| V2-AUTH 1–4 | untrusted/missing/unmapped/expired identity; rotation; remap; foreign owner; recheck at commits and reads; no existence disclosure | g5-* (9 rows) | 9 DONE (g5-cert-rotation-same-owner, g5-remapped-owner, g5-expired-identity, g5-cross-authority-reference added at M19b in Kimi's role; both subjects enforce expiry on a LIVE connection in different kinds, recorded) |
| V2-SESSION 1–3 | create/attach/anti-reuse; lost-ACK creation replay; changed-policy CONFLICT; sequence overflow CONFLICT; retirement EXPIRED | g2-crash-after-create-commit DONE (rust-client/java-server direction still INCOMPLETE at M20b: Java fixture withhold lacks the fresh/replay gate, finding to Claude), g2-crash-before-create-commit DONE (all directions green at M20b after the 90 s kill-row op budget), g3-partial-retirement SPEC, g3-nonreusable-history SPEC | 2 DONE |
| V2-OP 1–4 | immutable ops; replay; changed-digest CONFLICT; NOT_FOUND uncertainty; no reapplication after retirement | g2-duplicate-op-changed-params DONE, g2-simultaneous-duplicate DONE, g2-kill-client-after-request-sent DONE, g2-not-found-in-flight DONE (M19b: NOT_FOUND on the wire lookup while an admission is held mid-transfer on both subjects, then byte-identical receipts on every path), g3-nonreusable-history SPEC | 4 DONE |
| V2-SET 1–4 | declare/seal rules; increasing IDs; empty-batch rules; pages/more/seal; producer-1 restrictions; child scope allocation | g1-declaration-capacity DONE, g1-out-of-order-pages DONE, g1-mode1-branch DONE, g1-mode2-descendants DONE (seal oracle byte-identical both implementations; empty-batch + 257-batch wire-unreachable named gaps) | 4 DONE |
| V2-ADMIT 1–4 | header/stream identity; incremental bytes/hash/FIN; install-before-commit; replay without re-execution; funded reservations | g1-oversize-payload DONE, g2-drop-reply-admission DONE, g6-stream-identity-and-fin SPEC, g3-input-before-metadata impl | 2 DONE |
| V2-ATTEMPT 1–4 | retry fences; stale attempt; publication rechecks; lease; restart≠new attempt | g2-kill-after-admission-before-publication DONE, g2-kill-server-after-admission-recovery DONE, g2-kill-at-publication-commit DONE (post-terminal retry ALREADY_TERMINAL), g4-stale-attempt-retry SPEC, g4-stale-lease-publication SPEC | 3 DONE |
| V2-CANCEL 1–4 | cancel/skip/scope fences; races; ancestor fences; descendant settlement | g4-publication-vs-cancel/-vs-skip, g4-ancestor-fence-publication, g4-eventual-settlement | SPEC (hooks needed) |
| V2-VIEW 1–3 | revision monotonicity; wait bounds; retained views after expiry | g7-no-deadline-extension, g7-receipt-before-output-expiry, (revision waits in every watch-using row) | SPEC |
| V2-RESULT 1–7 | manifest commitments; local-vs-remote copies; read pins; expiry refusal; integrity; cross-authority references | g1-zero-output DONE, g1-empty-input DONE, g7-read-pin-past-expiry DONE (M19b), g7-receipt-before-output-expiry DONE, g5-cross-authority-reference DONE (M19b), g8-complete-with-pending DONE | 6 DONE |
| V2-CLOSE 1–5 | exact root complete; child-cut/altered CONFLICT; pending NOT_READY; detach semantics; half-close; fence precedence over STRICT | g8-* (6 rows), g4-ancestor-fence-publication | SPEC |
| V2-TIME 1–6 | deadline independence; unsafe clock; queue time; cleanup refund; unsafe-time non-destructivity | g7-* (7 rows) | 5 DONE (g7-deadline-queue-time and g7-read-pin-past-expiry at M19b), 2 INCOMPLETE with a named missing capability (g7-unsafe-clock-refusal: no subject fixture clock; g7-cleanup-interrupted-refund: no cleanup boundary in interface-v1) |
| V2-STORE 1–7 | crash both sides of commits; restart reconciliation; ownership; cleanup replay; retirement order; measured limits; separate metric scopes | g2 crash rows DONE (7), g3-* (6 rows) batch A impl, r-capability-manifest DONE (separate scopes recorded per sample; mandatory-metric and dead-collector rules enforced by the validating reader; resources schema v2 adds cancelled_write_bytes and the frozen JVM limits are recorded here; re-verified unchanged at the 7585a9dc Java pin), r-connection-ceiling DONE, r-stalled-principal-progress DONE (re-observed at the 7585a9dc Java pin and again at M18c with non-writing probes on a continuously-driven client: both subjects enforce per stalled stream on a surviving connection, 3/3 named LIMIT_EXCEEDED refusals readable on each, and each subject's input receive deadline is bracketed inside two seconds of its own negotiated idle bound), r-memory-ladder DONE (payload and retained-inventory ladders against the limits the subject declares on the wire; allowances frozen before the first rung; over-limit rung refused LIMIT_EXCEEDED on both, details differ and are recorded verbatim), r-staging-and-journal-bounds PARTIAL (pending and staging ceilings driven to exhaustion on both subjects with the attempt number and verbatim detail recorded, existing promises kept, handles returning to baseline, capacity charged while busy and reconciling after cleanup and after restart; journal/retained-byte ceilings recorded, not driven), r-network-bytes PARTIAL (two collection methods recorded per sample and never substituted, loopback double-counting stated, handshake/TLS measured in its own phase and retransmission reported from the transport's own path counters, dead-collector and truncation detection proved in-row; no network namespace and no packet capture on this host, both recorded by the exact failing check), r-native-credit PARTIAL (application queue bytes, borrowed native flow credit and actual transport completion separated and measured from the source-pinned transport's own frame and datagram accounting; stream credit released after refused object streams and re-used; no packet capture and one endpoint's view only) | 11 DONE, 3 PARTIAL |
| V2-RESOURCE (x-cut) | capacity ceilings; journal single-owner; adapter byte ceilings; cleanup credits | r-connection-ceiling DONE (both servers; bounds and refusal classes recorded, recovery asserted), r-stalled-principal-progress DONE (both servers), r-memory-ladder DONE (both servers; memory plateaus at the configured bounds under a 256x payload ladder and a 1->64 retained-inventory ladder), r-staging-and-journal-bounds PARTIAL (both servers; pending-control-work and staging-object ceilings driven to a named LIMIT_EXCEEDED refusal, existing promises kept, handles bounded, capacity charged while busy and reconciled after cleanup and restart; journal/retained-byte ceilings recorded, not driven), r-native-credit PARTIAL (both servers; borrowed native flow credit, application queue bytes and actual transport completion kept apart at frame level, and stream credit observed released after refused object streams and then re-used), g3-store-ownership impl | 3 DONE, 2 PARTIAL |

## Explicit gaps (visible, not waived)

1. Java-server hook directions for G2/G4 rows: INCOMPLETE until Claude's
   FixtureMain checkpoint (his SENT-side fixes + missing boundaries land
   there per his board acknowledgement).
2. Java hooked rows at M20b: g2-crash-after-create-commit rust-client/java-server direction INCOMPLETE — defect 16's fresh gate covers committed() only; the withhold path (DurableServer.java:872) re-fires the re-armed drop-reply on the REPLAYED create (finding 2 to Claude, blocks acceptance). g8-timeout-no-completion-claim kill-variant rust-client/java-server INCOMPLETE — FixtureMain accepts kill@COMPLETE_RESPONSE_SENT but Hooks.sent() never consumes kill rows, so the kill never fires (finding 3 to Claude, blocks acceptance). g2-drop-reply-publication RETIRED at M20 (a1616584): publication is watch-observed on both subjects, no correlated reply exists to withhold, g2-kill-at-publication-commit is the boundary's evidence. g7-unsafe-clock-refusal needs a subject fixture clock or untrusted-clock
   mode; neither subject exposes one, so the row reports the named missing
   capability and is WAIVED for acceptance with that reason in run.tsv
   (never simulated by host clock changes); g7-cleanup-interrupted-refund
   likewise (no cleanup boundary in interface-v1).
3. V2-WIRE accept-side codec coverage lives in the implementations' own
   test suites (Rust codec executes all 70 frozen vectors; Java codec
   tests were outstanding at the base checkpoint) — the neutral driver
   covers the wire-abuse/refusal side black-box; codec-unit coverage is
   mapped to implementation tests in the final handoff, not claimed here.
3a. Resource-scope gaps named at M16: the Rust heap scope has no
   black-box collector (RSS/HWM is never substituted for it);
   incomplete-handshake accounting in r-connection-ceiling is not
   observable through the quinn client; network bytes are not collected
   until r-network-bytes. The last of those is CLOSED at M18d for what the
   host allows and replaced by a narrower named gap: this host grants no
   network namespace (`unshare -n` and `unshare -r -n` both refused) and no
   packet capture (`tcpdump -i lo` refused, CAP_NET_RAW absent), so there is
   no fixture-scoped INTERFACE counter and no per-packet accounting of the
   SUBJECT side. What exists is host-scoped interface counters with a
   measured idle baseline and the source-pinned transport's own
   per-connection UDP totals, both recorded per sample with their method.
3b. Added at M17, at the Java `0176855` pin, and CLOSED at M17b: in
   r-stalled-principal-progress the Java server closed the whole
   connection at its idle bound (APPLICATION_CLOSE 0x204, reason "idle
   control deadline"), and because that close discards control frames the
   peer queued but the client had not read, the row could not distinguish
   "no per-stream LIMIT_EXCEEDED refusal was sent" from "one was sent and
   the close discarded it". It was recorded as a named gap and raised with
   Claude, not scored as a subject defect. At the Java `7585a9dc` pin the
   connection stays open through control silence, the row reads 3/3
   per-stream LIMIT_EXCEEDED "input receive deadline" refusals at window
   end, and nothing is discarded, so the gap no longer applies. What it
   leaves behind is a bracket rather than a gap: the Java per-stream abort
   lands after idle+10 s and by lifetime+10 s and the row claims no value
   inside it. Disk I/O is three separate MANDATORY counters
   (read/write/cancelled_write bytes) so a write rate can be told apart
   from cancelled page-cache writeback.
3c. Added at M18a. (i) ROW-ID RECONCILIATION. The matrix document
   scenario-matrix-g6-resource.md is canonical for group R. Three ids were
   registered in `scenarios.rs` at M16 that the matrix does not name —
   `r-pending-ceiling`, `r-staging-quota`, `r-journal-bounds` — and none was
   ever implemented under those ids, so none was ever run, skipped or
   passed. They are retired at M18a and the registry now carries exactly the
   seven canonical names (`r-capability-manifest`, `r-connection-ceiling`,
   `r-stalled-principal-progress`, `r-memory-ladder`,
   `r-staging-and-journal-bounds`, `r-network-bytes`, `r-native-credit`).
   Subject-matter mapping of the retired ids: `r-staging-quota` and
   `r-journal-bounds` are both covered by the canonical
   `r-staging-and-journal-bounds`; `r-pending-ceiling` (the declared
   `pending_limit` of pending control responses) has no separate canonical
   row and is folded into `r-staging-and-journal-bounds` as one of the
   ceilings that row drives, because `pending_limit` is a capability the
   subject declares in its manifest and its exhaustion is the same
   "refuse NEW work, keep existing promises" property.
   (ii) JAVA NATIVE/DIRECT IS A NAMED GAP on this host. `jcmd <pid>
   VM.native_memory summary` answers "Native memory tracking is not
   enabled"; enabling it means adding `-XX:NativeMemoryTracking` to the
   launch flags that are frozen before this matrix's measurement rows, and
   the collector's own overhead would then be inside the measurement it
   serves. Recorded with the exact failing check and the jcmd transcript
   archived, never inferred from RSS minus heap. Re-freezing WITH NMT and
   rerunning every R row is a future milestone's decision, not a mid-matrix
   substitution.
3d. Added at M18b, and it changes an earlier RECORD rather than adding a new
   gap. A driver-side measurement defect was found by the coordinating
   owner's timestamped reproduction: the raw peer built a current-thread
   tokio runtime, so quinn's endpoint driver only progressed inside
   `block_on`, and a row that slept between probes observed the subject's
   STOP_SENDING or refusal at the time of its own next blocking call rather
   than at the time the subject acted. The 40-130 s bracket that
   `r-stalled-principal-progress` reported for the Java input receive
   deadline at M17b is therefore WITHDRAWN as a client artefact; the Java
   subject in fact refuses at idle+0.1 s. The peer now runs a two-worker
   multi-threaded runtime (conformance crate only; the tokio dependency
   gained a feature of a crate it already used and Cargo.lock is unchanged).
   Re-running the row also needs non-writing enforcement probes, because the
   existing ones write payload bytes that legitimately renew the Java
   deadline. BOTH halves landed: the runtime at M18b and the non-writing
   probes at M18c. The row now brackets each subject inside two seconds of
   its own negotiated idle bound (java open at +28.000 s, stopped by
   +30.156 s against a 30 s bound; rust open at +3.102 s, stopped by
   +5.101 s against a 5 s bound), so the timing is measured rather than
   unclaimed and the enforcement KIND finding is unchanged. What is still
   outstanding is a FULL 53-row matrix rerun on the fixed runtime.
4. Client-side boundaries for rust/java clients are driver-side only until
   client hooks exist (Java client hooks promised in Claude's next
   checkpoint; rust client hooks not proposed — client-death rows use
   uncontrolled kills, labelled).
 3e. Added at M19b (work in Kimi's role), updated at M20/M20b. Every matrix row is
   registered as implemented (66 of 66 after the M20 retirement of
   g2-drop-reply-publication, a1616584). The two rows that cannot produce
   their evidence RUN and report the named missing capability instead of
   "not implemented yet" (g7-unsafe-clock-refusal: no fixture clock on
   either subject; g7-cleanup-interrupted-refund: no cleanup boundary in
   interface-v1); both are WAIVED for acceptance with reasons recorded in
   run.tsv (5d65b661), and the run_all.sh acceptance block (9842e655,
   paths absolutized 81ff4fe6) spells the four waivers. Two hook gaps of
   the Rust subject are named by rows rather than worked around: its
   fixture pauses only at the three reply pairs, so g4-stale-attempt-retry
   keeps attempt 2 live with a 16 MiB copy and g7-deadline-queue-time
   saturates the pool with load instead of a hold (both record the
   mechanism; the Java directions use EXECUTION_CLAIMED pauses); and
   unarmed boundaries are never emitted, so per-work EXECUTION_CLAIMED
   counts exist only on the Java side. M20b: kill rows carry a 90 s op
   budget where clients learn of a dead server via the negotiated
   transport bound (8f5c4a7e; g2-crash-before-create-commit
   java-client/rust-server green at ~61 s CONTROL_RESET).
