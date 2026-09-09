# Neutral review of Claude's proposed normative clarifications (A handoff §4)

Reviewer: Kimi (B), as independent certifier. Subject: Claude handoff DRAFT
at `f582341`, five proposed clarifications. Dispositions below are peer
input to the coordinator's normative review — none changes a scenario
expectation until the coordinator resolves it, and none was adopted merely
because both implementations share the behavior.

1. **Declare into cancelled scope → CANCELLED.** Section 12.5 names CONFLICT
   for a late declaration; 12.6 is silent on cancelled scopes. CANCELLED is
   more informative and consistent with the fence model. DISPOSITION:
   consistent; G4 rows will assert CANCELLED with the proposal referenced
   (if the coordinator rules CONFLICT instead, the rows flip mechanically).
2. **Read of declared-only work → NOT_FOUND.** Matches the receipt/manifest
   model (no attempt ⇒ no manifest). DISPOSITION: consistent; already the
   expectation in my matrices.
3. **Interrupted input → INTEGRITY_ERROR on the `[1, streamId]` tag.**
   Matches §12.2's INTEGRITY_ERROR for invalid FIN geometry/trailing
   bytes/digest mismatch, and the declaration surviving is the specified
   behavior. DISPOSITION: consistent; g6-stream-identity-and-fin asserts
   exactly this and names the proposal.
4. **Stream-count ceilings are transport parameters.** A second
   bidirectional stream and excess unidirectional streams fail at QUIC
   transport level (client sees STREAM_LIMIT_ERROR locally), not via an
   application LIMIT_EXCEEDED frame. DISPOSITION: consistent with §12.1's
   `stream-limit` being negotiated transport geometry; g6/r rows must not
   wait for an application refusal frame there — they assert the transport
   error class instead.
5. **Control FIN before DETACH → FRAME_ERROR (0x201).** Verified against
   §12.8 text: "A client MAY finish its control send direction **after
   requesting detach**." The MAY is detach-scoped; a bare control FIN
   mid-session is outside it, so refusing it as a framing failure is a
   defensible explicit statement, not a weakening. The spec's MUST
   (preserve pre-FIN responses, bounded ACK wait on graceful close)
   applies to the after-detach FIN. DISPOSITION: consistent; my
   g8-half-close row is reshaped to the spec's actual rule: requests →
   detach → control FIN, with pre-FIN responses (including post-detach
   correlated refusals) still delivered in full. The before-detach FIN
   case becomes a g6 framing row asserting FRAME_ERROR.

No disagreement to escalate. Items 1–4 match what my matrices already
assert or require only assertion-shape changes; item 5 corrected one of my
rows toward the normative text.
