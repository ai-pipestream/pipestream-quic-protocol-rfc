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

## F3. BUG: new connections refused opaquely past the connection ceiling

Symptom: the merge reader's connect intermittently fails with a bare
`connection lost` (Rust) or a mislabeled `LIMIT_EXCEEDED: authority
refused: peer closed connection` (Java). Reproduced deterministically:
three rapid stage-3 kill/resume cycles on one authority pair — the
second or third resume's first reader connect fails; restart (state
kept, connections dropped) heals it; ~12 idle minutes also heal it.

Root cause (verified in code + behavior): the authority refuses new
handshakes past `Options::connections` (16, default) or
`connections_per_principal` (4, default) via a
stateless refuse (`v2_authority/server.rs`: `incoming.refuse()` when
`tasks.len()` or `open_connections()` is at ceiling). No refusal frame
reaches the client, so neither client can report a code. Client-side
residue that fills the ceiling: every coordinator run holds 2
connections and never detached on exit, and (pre-fix) the merge reader
opened one connection per TF reference (8 per merge). Abrupt exits
(the kill demos exit without detach, like a real crash) leave corpses
until the 30s idle backstop reaps them; a resume seconds later plus
its merge reader then sits at 4-5 concurrent against the per-principal
cap of 4 and the reader connect dies.

Fixed example-side: one reader connection per merge (Rust
`connect_reader`, Java session block in `runMerge`), coordinator
detaches both sessions at clean exit, kill path stays abrupt, gate
settles 40s between kill and resume and uses a fresh pair per group.
Per-step reader context (`reader connect|watch|... for
scope:producer:entity`) stays so any recurrence names the failing op.

Still owed by the platform (not hand-waved): (1) the refuse path
should be atomic with slot accounting — a kill→resume burst trips it
intermittently even well below any steady load, which smells like
close-processing racing new accepts; (2) both clients must surface a
real busy/backpressure code for a refused handshake instead of
`connection lost` / a mislabeled `LIMIT_EXCEEDED`, so a consumer can
tell "peer is shedding" from "network is broken."
