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
| V2-AUTH 1–4 | untrusted/missing/unmapped/expired identity; rotation; remap; foreign owner; recheck at commits and reads; no existence disclosure | g5-* (9 rows) | 5 DONE, 4 SPEC |
| V2-SESSION 1–3 | create/attach/anti-reuse; lost-ACK creation replay; changed-policy CONFLICT; sequence overflow CONFLICT; retirement EXPIRED | g2-crash-after-create-commit DONE, g2-crash-before-create-commit DONE, g3-partial-retirement SPEC, g3-nonreusable-history SPEC | 2 DONE |
| V2-OP 1–4 | immutable ops; replay; changed-digest CONFLICT; NOT_FOUND uncertainty; no reapplication after retirement | g2-duplicate-op-changed-params DONE, g2-simultaneous-duplicate DONE, g2-kill-client-after-request-sent DONE, g3-nonreusable-history SPEC | 3 DONE |
| V2-SET 1–4 | declare/seal rules; increasing IDs; empty-batch rules; pages/more/seal; producer-1 restrictions; child scope allocation | g1-declaration-capacity DONE, g1-out-of-order-pages DONE, g1-mode1-branch DONE, g1-mode2-descendants DONE (seal oracle byte-identical both implementations; empty-batch + 257-batch wire-unreachable named gaps) | 4 DONE |
| V2-ADMIT 1–4 | header/stream identity; incremental bytes/hash/FIN; install-before-commit; replay without re-execution; funded reservations | g1-oversize-payload DONE, g2-drop-reply-admission DONE, g6-stream-identity-and-fin SPEC, g3-input-before-metadata impl | 2 DONE |
| V2-ATTEMPT 1–4 | retry fences; stale attempt; publication rechecks; lease; restart≠new attempt | g2-kill-after-admission-before-publication DONE, g2-kill-server-after-admission-recovery DONE, g2-kill-at-publication-commit DONE (post-terminal retry ALREADY_TERMINAL), g4-stale-attempt-retry SPEC, g4-stale-lease-publication SPEC | 3 DONE |
| V2-CANCEL 1–4 | cancel/skip/scope fences; races; ancestor fences; descendant settlement | g4-publication-vs-cancel/-vs-skip, g4-ancestor-fence-publication, g4-eventual-settlement | SPEC (hooks needed) |
| V2-VIEW 1–3 | revision monotonicity; wait bounds; retained views after expiry | g7-no-deadline-extension, g7-receipt-before-output-expiry, (revision waits in every watch-using row) | SPEC |
| V2-RESULT 1–7 | manifest commitments; local-vs-remote copies; read pins; expiry refusal; integrity; cross-authority references | g1-zero-output DONE, g1-empty-input DONE, g7-read-pin-past-expiry SPEC, g7-receipt-before-output-expiry SPEC, g5-cross-authority-reference SPEC, g8-complete-with-pending SPEC | 2 DONE |
| V2-CLOSE 1–5 | exact root complete; child-cut/altered CONFLICT; pending NOT_READY; detach semantics; half-close; fence precedence over STRICT | g8-* (6 rows), g4-ancestor-fence-publication | SPEC |
| V2-TIME 1–6 | deadline independence; unsafe clock; queue time; cleanup refund; unsafe-time non-destructivity | g7-* (7 rows) | SPEC (fixture clock proposal needed for g7-unsafe-clock-refusal) |
| V2-STORE 1–7 | crash both sides of commits; restart reconciliation; ownership; cleanup replay; retirement order; measured limits; separate metric scopes | g2 crash rows DONE (7), g3-* (6 rows) batch A impl, r-* (7 rows) SPEC | 7 DONE |
| V2-RESOURCE (x-cut) | capacity ceilings; journal single-owner; adapter byte ceilings; cleanup credits | r-connection-ceiling, r-staging-and-journal-bounds, g3-store-ownership, r-memory-ladder | SPEC |

## Explicit gaps (visible, not waived)

1. Java-server hook directions for G2/G4 rows: INCOMPLETE until Claude's
   FixtureMain checkpoint (his SENT-side fixes + missing boundaries land
   there per his board acknowledgement).
2. Java hooked rows: g2-crash-after-create-commit java-server direction INCOMPLETE (FixtureMain re-fires drop-reply on replayed commit — reported with transcript); g2-drop-reply-publication unimplemented (no PUBLICATION reply pair on either subject). g7-unsafe-clock-refusal needs a subject fixture clock or untrusted-clock
   mode; if neither subject exposes one, the row stays INCOMPLETE with the
   named missing capability (never simulated by host clock changes).
3. V2-WIRE accept-side codec coverage lives in the implementations' own
   test suites (Rust codec executes all 70 frozen vectors; Java codec
   tests were outstanding at the base checkpoint) — the neutral driver
   covers the wire-abuse/refusal side black-box; codec-unit coverage is
   mapped to implementation tests in the final handoff, not claimed here.
4. Client-side boundaries for rust/java clients are driver-side only until
   client hooks exist (Java client hooks promised in Claude's next
   checkpoint; rust client hooks not proposed — client-death rows use
   uncontrolled kills, labelled).
