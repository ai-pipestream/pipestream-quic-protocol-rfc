# Scenario matrix detail — group G5: authentication and current authorization

Requirement families: V2-AUTH 1–4, V2-SESSION 3, V2-RESULT 7.
Most rows are hook-free: the driver controls certificate provisioning and
principal maps, so identity faults are injected at fixture setup, not at
commit boundaries. Precedence rule under test everywhere: authentication
and authorization are checked BEFORE any existence-revealing refusal —
denials must not disclose another owner's retained state.

## g5-untrusted-identity

- Client presents a certificate from a CA the server does not trust.
- Expected: QUIC CRYPTO_ERROR (RFC 9001 §4.8) during handshake; no
  CAPABILITIES exchange, no application REFUSAL, no fabricated outcome.
  Driver observes: client op fails at transport; server emitted no
  session state (next-sequence for the owner unchanged).

## g5-missing-client-cert (durable required vs optional)

- Client offers no certificate. Server configured with durable profiles
  offered-but-not-required: connection gets Core only; a SESSION create
  attempt is refused without activating durable profiles. Server
  configured require-durable: connection closed with UNAUTHORIZED (3)
  BEFORE the capabilities response.
- Evidence: negotiated profile set recorded in observed.tsv; the close
  precedes any CAPABILITIES response (driver-side connection log).

## g5-unmapped-principal

- Valid client certificate from the trusted CA, but its leaf-DER SHA-256
  is absent from the server's principal map.
- Expected: same as missing identity — durable activation refused
  (UNAUTHORIZED before capabilities when required); no disclosure of any
  owner's retained sessions.

## g5-foreign-owner

- Owner A admits work; owner B (valid mapped principal, same authority)
  attempts: attach to A's session generation, WORK view on A's work key,
  operation lookup of A's operation ID, result read of A's output.
- Expected: every attempt refused UNAUTHORIZED (3) or the
  existence-hiding equivalent — and the refusal MUST NOT confirm the
  work's existence. Driver checks B's observed refusals are identical in
  kind whether the target exists or not (paired probe against a
  never-created generation: same code, no distinguishable detail).

## g5-cert-rotation-same-owner

- Principal map contains TWO leaf-DER hashes mapping to owner A (old +
  rotated certificate). Session created with cert-1; server roots
  restarted; client reconnects with cert-2 and attaches.
- Expected: attach succeeds with the identical binding; committed
  operations under cert-1 remain valid; retry/cancel under cert-2 are
  accepted as the same owner. The accepted job's retained grant survives
  the presenting certificate's lifetime subject to current policy.
- Status (milestone 19b, work in Kimi's role): implemented, green on both
  servers (durable-18d4ab23bb15105e). Both leaf hashes are minted into the
  map at fixture time; two retry-copy works are parked AWAITING_RETRY under
  cert 1, the server is stopped (SIGTERM, DRAINED) and restarted on the same
  roots, and cert 2 attaches with the byte-identical BINDING, looks up the
  cert-1 admission, retries one work to attempt 2 (SUCCEEDED, result
  byte-exact) and cancels the other (disposition 0, CANCELLED). Both
  fingerprints are in expected.tsv and observed.tsv.

## g5-remapped-owner-denies

- Owner A admits work. Operator REMOVES A's leaf hash from the principal
  map (or remaps it to owner B) and the server re-evaluates current
  authorization (new connection; document whether live connections are
  re-checked per the implementation's evidence).
- Expected: subsequent committing mutations and result reads by that
  credential are refused; already-transmitted bytes are not retracted
  (previously delivered output stays delivered — driver verifies the
  pre-remap read artifact remains valid); no silent success.
- Status (milestone 19b, work in Kimi's role; driver row id
  `g5-remapped-owner`): implemented, green on both servers. The map is edited
  between a graceful stop and the restart so alice's leaf hash maps to owner
  mallory; attach, retry, cancel, lookup and result read by that credential
  are each refused UNAUTHORIZED (3) on both subjects (rust "authority access
  denied", java "session access denied"), none names a state-disclosing
  code, the refused read writes no bytes, and the output read before the
  remap still hashes to the oracle. The attach here is the wire Attach
  carrying the journaled owner alice from a credential the server now maps
  to mallory, so the authority-side cross-owner attach branch that
  g5-foreign-owner could not reach through the CLIs is exercised. Live
  connections are not re-checked by this row because the stop kills them;
  recorded, not claimed either way.

## g5-expired-identity

- Client certificate with a short validity; connect and bind while valid,
  then let it expire (short-lived cert minted at fixture time; no host
  clock change).
- Expected: new requests on the existing connection are blocked per the
  implementation's documented expiry handling (record actual behavior);
  a fresh connection with the expired certificate fails TLS validation
  (CRYPTO_ERROR); a renewed certificate mapping to the same principal
  reconnects to the same retained session.
- Status (milestone 19b, work in Kimi's role): implemented, green on both
  servers (durable-18d4abe3eae3a613). The short leaf is minted with a 25 s
  validity from the fixture's own host UTC (rcgen not_before/not_after;
  host UTC never changed). While valid the CLI creates the session and
  declares, and a raw connection attaches and is kept open with QUIC PINGs.
  After expiry the two subjects enforce on the LIVE connection in different
  kinds, both recorded: the Rust server refuses the next request UNAUTHORIZED
  "credential validity or mapping changed" on a connection that stays open,
  the Java server closes the connection APPLICATION_CLOSE 0x203 "caller
  credential unavailable" (S12-098). A fresh connection with the expired leaf
  fails the handshake on both (TLS alert 45 certificate expired, no
  application refusal), and the renewed leaf mapped to the same owner
  attaches to the identical binding and sees the declaration.

## g5-cross-authority-reference

- Authority X publishes a result; the driver obtains the result locator
  (V2 URI) and resolves it against authority Y (separate roots, separate
  principal maps) where the same principal is NOT authorized for X's
  session.
- Expected: no implicit access; attach/reference resolution refuses
  without disclosure; locators grant no access by possession — an
  unauthorized holder of the exact locator learns nothing. The
  positive arm (same owner authorized on the issuing authority) reads
  the exact bytes via manifest commitments.
- Status (milestone 19b, work in Kimi's role): implemented, green on both
  servers. Authority Y shares X's CA and client identities, has its own
  roots and principal map (alice mapped) and the label `issuer-b`; it holds
  no session of X's. Arm A points X's journal (bound to X, selection
  saved) at Y: attach, read, lookup and watch are each refused with a named
  code and no result bytes are written; the two subjects name different
  codes, both recorded verbatim (rust UNAUTHORIZED "authority access
  denied", java CONFLICT "authority differs"; neither discloses anything of
  X's session, and whether CONFLICT is the right class under the
  precedence rule is a question for the owners, not a defect claim). Arm B
  binds a journal on Y and selects X's identifiers: NOT_FOUND on both
  (rust "work not declared", java "work identity is undeclared"), X's
  output digest absent from the transcript. The positive arm reads the
  exact bytes on X. The REFERENCE text is archived; neither client
  dereferences a locator's authority (recorded from the sources, not
  asserted by the row).

## g5-no-existence-disclosure (paired-probe matrix)

- Systematic pairing: for each of {session attach, operation lookup,
  work view, result read}, probe (a) an existing target owned by another
  principal and (b) a never-created target, both from an unauthorized
  connection. Refusal code AND detail-class must match between (a) and
  (b); any distinguishable difference is an existence-disclosure defect
  reported with the exact pair of transcripts.

## Mechanics

- All identity material is fixture-generated per run (mtls.rs): short
  validity certs, multiple CAs, rotation pairs, remap edits between
  restarts. No host UTC changes anywhere.
- `g5-remapped-owner-denies` restart uses `stop` (SIGTERM drain) then
  `restart` with the edited principal map — a graceful operator action,
  not a crash row.
