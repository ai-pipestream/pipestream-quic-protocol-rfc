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

## F3. Dropped reader connections under accumulated/abrupt-exit state

Clause (connection lifecycle after client disappearance): the profile
does not say how fast an authority reaps sessions and connections left
behind by a client that vanishes without detach, nor what a new
connection observes while that residue is outstanding.

Friction: the merge reader opens one short session per TF reference.
On fresh authority state every direction passes repeatably, but once
sessions accumulate across runs plus an abrupt coordinator exit (the
kill demos exit without detach, like a real crash), later reader
connects intermittently fail with a bare `connection lost` at connect
— no refusal, no diagnostic, retries on a fresh creation hit the same
wall until the servers restart (state kept, connections dropped).
Fresh-state-per-group in `run-index.sh` keeps the gate deterministic;
the residue/reap interaction underneath is unresolved.

Done in code: both readers detach per reference after the digest check
(the Rust reader did not at first); per-step reader context
(`reader connect|watch|select|read for scope:producer:entity`) so the
next occurrence names the failing op.

Proposed text change: none yet — needs a platform verdict first. At
minimum the failure deserves a refusal code (e.g. a busy/backpressure
signal) instead of a transport-level drop, so a consumer can tell
"peer is shedding" from "network is broken."
