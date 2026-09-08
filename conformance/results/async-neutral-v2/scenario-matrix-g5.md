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

## g5-remapped-owner-denies

- Owner A admits work. Operator REMOVES A's leaf hash from the principal
  map (or remaps it to owner B) and the server re-evaluates current
  authorization (new connection; document whether live connections are
  re-checked per the implementation's evidence).
- Expected: subsequent committing mutations and result reads by that
  credential are refused; already-transmitted bytes are not retracted
  (previously delivered output stays delivered — driver verifies the
  pre-remap read artifact remains valid); no silent success.

## g5-expired-identity

- Client certificate with a short validity; connect and bind while valid,
  then let it expire (short-lived cert minted at fixture time; no host
  clock change).
- Expected: new requests on the existing connection are blocked per the
  implementation's documented expiry handling (record actual behavior);
  a fresh connection with the expired certificate fails TLS validation
  (CRYPTO_ERROR); a renewed certificate mapping to the same principal
  reconnects to the same retained session.

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
