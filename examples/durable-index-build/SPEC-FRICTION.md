# SPEC-FRICTION.md (M3, runs alongside M2)

For each entry: the Section 12 clause quoted; what was ambiguous, missing
or awkward; what was done in code; the proposed text change, or
"no change, documentation only".

## F1. Executor as a result consumer needs application-authorized credentials

Clause (Section 12, output references): "The consumer obtains owner
credentials and trusted authority-to-endpoint configuration separately; it
MUST NOT infer either from an untrusted URI. ... Do not follow redirects,
send credentials to another authority, or dereference a caller-supplied URL
without separate application authorization."

Friction: the merge executor (running inside authority B) must open an
owner-authenticated client session to authority A. The clause permits this
only with "separate application authorization", but the profile defines no
shape for recording that authorization. In this example the authorization
is (a) operator flags on the example authority binary
(`--reader-cert/--reader-key/--reader-endpoint`, configured out of band,
never taken from a URI or from unit input bytes), and (b) this log entry.
A reader of the index output cannot tell from protocol evidence alone that
the cross-authority read was application-authorized rather than
credential-smuggling.

Done in code: reader credentials only from process flags; unit input
carries manifests + locators, never key material.

Proposed text change: in the output-reference paragraph, add: "An
application that authorizes its executor to act as a consumer SHOULD
record the authorization (credential source and endpoint source) in its
own durable evidence, e.g. the merge unit's input manifest, so a third
party can distinguish authorized consumer reads from misdirected
credentials."

## F2. Client bind is implicit in Rust, explicit in Java

Clause (session binding): a client binds its session before operating;
the Java `DurableClient` enforces this with
`NOT_READY: session not bound; call binding()` until `binding()` is
called, while the Rust `Client::connect` binds implicitly as part of
connect.

Friction: the same example reader code shape works on Rust with no bind
call and fails on Java without one. The first reverse-direction run
(Java merge reading Rust TF outputs) failed with exactly this error;
adding `get(client.binding())` after `get(client.ready())` fixed it.
Both behaviors are defensible, but portable application code cannot
assume either.

Done in code: Java reader binds explicitly
(`IndexContracts.readReference`); Rust reader relies on connect-time
bind.

Proposed text change: none (implementation guidance only). A porting
note in the client guide would do: "call binding() explicitly after
ready() even where one implementation binds implicitly."

## F3. BUG: the Rust client reports a connection-ceiling refusal as a bare `connection lost`

Symptom: the merge reader's connect intermittently failed with a bare
`connection lost` (Rust client) or `LIMIT_EXCEEDED: authority refused:
peer closed connection` (Java client). Reproduced deterministically:
three rapid stage-3 kill/resume cycles on one authority pair; the
second or third resume's first reader connect fails; a restart (state
kept, connections dropped) or an idle wait heals it.

Root cause, verified in the code of both implementations:

- Both authorities bound connections in two tiers. The Rust durable
  authority (`v2_authority/server.rs` `Options` defaults) allows 16
  connections in total and 4 per principal; the Java authority
  (`DurableOptions.defaults`) allows 32 in total and 8 per owner. Over
  the global ceiling both refuse at the transport level (Rust
  `incoming.refuse()`, QUIC CONNECTION_REFUSED), which is what Section
  12 asks for and carries no application code by design. Over the
  per-principal ceiling, which can only be checked after the TLS
  handshake, both close the connection with the application code
  LIMIT_EXCEEDED (`server.rs:148` "principal connection ceiling",
  `CoreServer.java:320` "owner connection ceiling").
- Every actor in this example is the one principal `workload`. Each
  coordinator run held two connections and never detached on exit; the
  merge reader opened one connection per TF reference. A killed
  coordinator's connections stay counted until the authority's idle
  timeout (60 s, keep-alive 5 s), so a resume seconds after a kill
  plus its merge reader met the per-principal ceiling of 4.
- The Java client reads the close code and names the refusal, so its
  message above is correct, not mislabeled. The Rust client does not:
  its handshake propagates the failed capabilities write as the
  transport library's `connection lost` and never inspects the close
  reason, so the same refusal is indistinguishable from a broken
  network. That is the bug, and it is a parity bug between the two
  reference clients.

Fixed example-side: one reader connection per merge (Rust
`connect_reader`, Java session block in `runMerge`), the coordinator
detaches both sessions at clean exit, the kill path stays abrupt
(crash simulation), the gate uses a fresh pair per group and settles
40 s between kill and resume as margin (the corpses are reaped at
60 s; the fix, not the settle, keeps the resume under the ceiling).
Per-step reader context (`reader connect|watch|... for
scope:producer:entity`) stays so any recurrence names the failing op.

Resolved on the platform side 2026-09-16: (1) the Rust client now maps
an application close during negotiation to the peer's code, so the
per-principal refusal reads `LIMIT_EXCEEDED: peer closed connection` on
both clients (`v2_client/transport.rs negotiation_failure`, pinned by
`public_client_names_a_connection_ceiling_refusal_and_reconnects_once_the_holder_leaves`
and, on the Java side, `DurableClientCeilingTest`); (2) Section 12.1 now
states the post-authentication per-principal refusal as an application
close carrying LIMIT_EXCEEDED before capabilities, reported by code;
(3) for the global tier the Rust client now keeps the peer's reason
("the server refused to accept a new connection") in its message, and
the Java client reports the transport close code. An earlier draft of this
note claimed the refuse path races slot accounting; that was not
established and is withdrawn unless a refusal is observed with fewer
than four live-plus-lingering connections for the principal.
