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
`connection lost` (Rust) or `LIMIT_EXCEEDED: authority refused: peer
closed connection` (Java). Reproduced deterministically: two to three
rapid kill/resume cycles on one authority pair — the second or third
resume's first reader connect fails; restart (state kept, connections
dropped) heals it; ~12 idle minutes also heal it.

Root cause (verified in code + behavior, corrected per Claude's
2026-09-16 review): the authority sheds new handshakes two ways. Past
the GLOBAL 16 (`Options::connections`) it stateless-refuses
(`incoming.refuse()` when `tasks.len()` or `open_connections()` is at
ceiling): CONNECTION_REFUSED per Section 12, and no code can reach the
client. Past the PER-PRINCIPAL 4 (`connections_per_principal`) it
sends an application close LIMIT_EXCEEDED "principal connection
ceiling" (`server.rs:148`) — so the Java report is the correct named
refusal, not mislabeled (this log first called it mislabeled; wrong).
The Rust client then loses that code: `prepare()` propagates the
failed capabilities write as bare `connection lost`
(`quinn/src/v2_client/transport.rs`), while the Java client maps close
codes 0x201..0x212 to the named refusal. Client-side residue that
fills the ceiling: every coordinator run held 2 connections and never
detached on exit, and (pre-fix) the merge reader opened one connection
per TF reference (8 per merge). Abrupt exits (the kill demos exit
without detach, like a real crash) leave corpses until the 60s
authority idle timeout (keep-alive 5s) reaps them; a resume seconds
later plus its merge reader then sits at 4-5 concurrent against the
per-principal cap of 4 and the reader connect dies.

Fixed example-side: one reader connection per merge (Rust
`connect_reader`, Java session block in `runMerge`), coordinator
detaches both sessions at clean exit and readers detach on error paths
too, kill path stays abrupt, gate settles 90s between kill and resume
and uses a fresh pair per group. Per-step reader context
(`reader connect|watch|... for scope:producer:entity`) stays so any
recurrence names the failing op.

Still owed by the platform (filed, not fixed here — the client tree
belongs to its owner): fix the Rust client to surface the peer's close
code during negotiation (owner-authorized fix pending), and add the
Section 12 sentence for the post-authentication per-principal refusal
(the text names CONNECTION_REFUSED for pre-authentication admission
only). The atomic-refuse race is NOT filed: 4-per-principal plus
corpses held until the idle timeout already explains the intermittent
burst, and a further claim needs a count with fewer than 4 live plus
lingering connections and its timing.

## F4. Cancelling admitted work vs cancelling an empty scope

Clause (Section 12, scope cancellation): "It seals current membership,
including an empty set, and settles existing nonterminal members as
CANCELLED." Also: "Cancellation may settle an unadmitted declared
entity."

Friction: the parent's admission view carries the allocated child
scope BEFORE authority-side expansion admits any members, so "scope
exists" does not mean "members exist". A coordinator that sends
ScopeCancel the moment it sees the scope usually seals an empty set —
spec-conformant (the empty set is explicitly allowed) but useless as
a cancellation demo: zero CANCELLED members, only exclusion to show.
There is no view that says "expansion ran"; the only signal is members
appearing in a scope page.

Done in code: the coordinator pages the child scope until at least
one member is admitted (`cancel-target-ready` event), then sends
ScopeCancel. Members already SUCCEEDED before the cancel are logged
and allowed (a cancel cannot retroactively un-complete work); every
other admitted member must settle CANCELLED(7); a scope that still
seals empty is logged (`cancelled-scope-settled ... 0 admitted
members`) and passes on exclusion alone. At 8192 words/file the
authority stays busy long enough that the cancel lands mid-flight
(4xCANCELLED every gate run); at 128 words/file the outcome was a
coin flip.

Proposed text change: no change, documentation only. The pattern
(page-for-members before cancelling admitted work) belongs in a client
guide, not the wire text.

## F5. Checkpoint prerequisites live in the implementation, not the text

Clause (Section 12, checkpoints): "Checkpoint completion still
requires the committed full seal and immutable closure summary." And:
"The client verifies identity, seal, count partition and known
commitments before acknowledging coverage in its own durable
observations."

Friction: "known commitments" does not say what the client must have
observed, and the answer is strict: a scope page observation alone is
not enough. Checkpointing the cancelled scope failed with journal
errors `terminal work view missing` (per-member terminal WORK views
were never journaled — the page lists members but the client had only
watched the scope, not the members) and scope 0 failed with
`descendant coverage missing` (child scopes were never checkpointed).
Both requirements were found by reading the journal code, not the
spec: the client must hold a terminal work view per admitted member
(a plain watch returns instantly for terminal members) and must
checkpoint every descendant scope before the parent. The cancelled
parent itself must also be watched to terminal (it settles FAILED(6):
it cannot assemble references from cancelled children) or scope 0
reports nonterminal work.

Done in code: the coordinator's cancel path pages each child scope,
watches every admitted member to terminal (CANCELLED for the cancelled
scope, SUCCEEDED for the rest), checkpoints each child scope, watches
the cancelled parent, then checkpoints scope 0. Gate evidence:
`declared=4 cancelled=4`, `declared=4 success=4`,
`declared=2 success=1 failure=1`.

Proposed text change: documentation only — one sentence in the
checkpoint paragraph: "A client acknowledges coverage only over
members for which it holds terminal work views, and a parent scope
only after committing checkpoints for every descendant scope."
