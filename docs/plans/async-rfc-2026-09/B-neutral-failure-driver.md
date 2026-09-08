# B: Independent durable interoperability and resource certification

Read [the shared handoff](README.md) first. This finishes original task 2's
neutral failure evidence. Certify actual behavior, not process exit banners,
shared implementation assumptions or a model of the behavior you wanted.

## Ownership and starting points

Own `implementations/rust-quinn/conformance/`, a narrow integration into
`conformance/run_all.sh`, and new evidence under
`conformance/results/async-neutral-v2/`. Necessary small Rust process fixtures
may live in a new dedicated test module; coordinate before editing shared
production CLI code. A owns the Java adapter. Do not edit either implementation
to conceal a failing external scenario.

Read `conformance/README.md`, `implementations/rust-quinn/conformance/src/main.rs`,
its `schema`, `receipts`, extension probes and bounded child-process helpers,
plus `implementations/rust-quinn/docs/v2-cli.md`. The current driver has Verify,
Interop, Recursive, Examples, Extensions and Modelcheck, but no complete V2
durable matrix. Its crate intentionally does not depend on `pipestream-core`
or production transport/client libraries. Preserve that independence.
Add a mechanical source/dependency-graph gate that fails if the neutral crate
imports either production codec, authority/client crate or subject test helper.

The existing 3x3 Layer-0 matrix, Rust legacy recursive tests and external
version-1 examples are regressions, not substitutes for this assignment.

## Deliverables, in order

### B1. Freeze the process contract and independent oracle

Add a dedicated durable command with reproducible scenario selection, seed,
bounded timeout, input sizes, binary paths, artifact output and both language
directions. Missing prerequisites must fail explicitly in the acceptance mode;
a development-only partial mode must label its output incomplete and cannot
produce a full-conformance PASS.

Launch actual reference client/server processes. Require readiness plus actual
authenticated connection, not a marker from a process that already exited.
Generate isolated temporary mTLS test identities and distinct stores/journals.
Pin binary/commit/dependency hashes and record the exact negotiated profiles and
limits. Preserve bounded concurrent stdout/stderr draining and child reaping on
all timeout/failure paths. Kill only fixture-owned processes and process groups.

Agree with A on thin application adapters for configuration, deterministic
callback barriers and reporting. An adapter must call the real public client
and authority paths; it cannot simulate the response or read a server database
to stand in for a protocol operation. Use bounded typed/TSV evidence and exact
integers/binary artifacts; no JSON oracle or stdout string meaning 'success'.

Derive expected commitments and outcomes from the normative source and frozen
vectors, not from a production codec, server summary or copied journal. Compare
actual transferred bytes to independently computed expected transforms/hashes,
plus all identity/count/attempt/manifest fields. Narrow raw malformed-peer probes
may independently encode/decode the necessary bounded binary subset with generic
QUIC/CBOR libraries; do not import production protocol code. Keep those probes
distinct from the real Java-to-Rust and Rust-to-Java client/server directions.

Add deliberately broken observations/fixtures as negative controls. Demonstrate
that wrong bytes, wrong attempt, missing descendant, altered root, unexpected
refusal, stale binary and missing scenario all fail the driver. A production
bug must never become a new expected success merely because both peers share it.

### B2. Exercise both implementations across real failure boundaries

Use an explicit scenario matrix with row ID, requirement IDs, direction,
setup, trigger, expected outcome/refusal, observed artifact and cleanup/resource
checks. At minimum cover:

1. Real create/declaration/admission/result retrieval and full root coverage:
   empty, leaf, mode-1 nested branch, mode-2 produced descendants, zero-output
   application, payloads larger than flow windows, out-of-order streams/pages.
2. Crashes before/after creation, declaration, admission, retry, publication
   and client observation commits. Lost ACK with exact original replay after
   restart; simultaneous duplicate and changed-parameter operations; NOT_FOUND
   while the original mutation may still commit. No new identity or attempt.
3. Input file install before metadata, staged output before publication, orphan
   cleanup, terminal cleanup and partial retirement. Real process death without
   graceful destructors; restart the same roots. Verify recoverable accounting,
   promised bytes, receipt/manifest consistency and non-reusable history.
4. Publication racing cancel/skip/revocation/deadline in both orders; stale
   attempt, lease epoch and ancestor grants; accepted fence precedence;
   missing descendants and bounded eventual settlement after safe progress.
5. Certificate rotation retaining owner, remap/foreign owner, missing/untrusted/
   expired identity, current authorization after staging, cross-authority result
   reference, profile dependency/downgrade and no retained-existence disclosure.
6. Canonical/schema/direction/correlation violations, actual input-stream IDs,
   wrong-length/hash/FIN results, duplicate response, error after result header,
   stopped control, stopped input/result, and loss/reordering/replacement streams.
7. Receipt-before-output and output-before-receipt expiry, dependent-parent and
   busy-reader pins, unsafe/regressing time, elapsed equality, deadline queue
   time, cleanup interrupted before refund and unresolved sessions never retired.
8. Exact root COMPLETE versus child/altered cuts; competing transfers; DETACH
   while requests/physical owners drain, valid refusals after detach, control
   half-close preserving earlier responses, timeout without a completion claim.

Fault hooks must signal a real reached boundary and pause without altering the
committed operation. Use the shared plan's versioned fixture-interface and hook
ownership rules; the existing CLIs do not expose all required commit barriers.
Keep separately reviewed subject-hook commits distinct from the neutral oracle.
Each externally meaningful scenario family must run Java-client/Rust-server and
Rust-client/Java-server, with both caller and authority death where applicable.
Document exact exceptions for private storage boundary probes, paired with a
black-box recovery row; aggregate coverage in only one direction is insufficient.

Label an injected exception, connection loss, hard
process kill and actual storage exhaustion distinctly. Do not call a graceful
restart a commit-crash test or claim physical power-loss coverage from SIGKILL.
Advance test clocks only through an explicit fixture clock; never change host UTC.

Start against Rust while A is in development. Incorporate reviewed Java artifacts
and run both directions for the final gate. Implementation-private storage fault
fixtures supplement, not replace, the authenticated process-level outcomes.

### B3. Measure and enforce resource boundaries

Implement observable limits for per-owner/global connections, incomplete
handshakes, requests, workers, input/result streams, staging objects, handles,
retained data and journals. Exercise a stalled or abusive principal alongside a
healthy principal and verify continued control progress within a stated test
deadline under a functioning transport. Avoid assertions that require progress
when the peer/network withholds the necessary control delivery itself.

Measure separately Rust/Java heap, Java native/direct allocations where available,
whole-process RSS/HWM, threads/FDs, data/file lengths, allocated filesystem blocks,
actual disk I/O and network bytes. Include every child process/native component
in the stated process group. Record collection method and unsupported measurements;
an unavailable mandatory metric leaves that acceptance row incomplete, not zero.
First record a host capability manifest, selected collectors, permissions and
collector overhead/calibration. For network bytes use fixture-scoped network
namespace/interface counters or packet capture, including handshake/retransmit
traffic; state loopback double-counting rules. Detect omitted worker samples,
dead collectors and truncated records. Do not silently substitute a different
collection scope mid-matrix.

Use fixed buffer sizes and a payload/inventory/concurrency ladder to test whether
memory or pending state scales beyond configured limits. Define limits and the
environment allowance before the decisive run, with rationale. Do not keep
raising gates to fit observed failures. Internal counter plateaus alone do not
prove RSS bounds; a single small payload does not prove constant memory.

Prove capacity remains charged while physical I/O is busy and is eventually
reconciled after safe cleanup/restart. Separate borrowed native flow credit,
application queue bytes and actual transport completion. Include packet-level
credit evidence from the source-pinned transport where the API cannot establish
that property; wrapper counters cannot replace it.

### B4. Integrate reproducible acceptance

Provide one command for the entire V2 matrix and named focused commands for
reproducing each failure. Integrate full acceptance into the conformance gate
with all prerequisites, no hidden feature flag that CI never enables. Preserve
frozen examples and existing model/legacy gates. Document runnable reduced
developer selections separately from final acceptance.

Retain scenario inputs/configurations/seeds, artifact hashes, timestamps, child
exits, actual refusal codes, output bytes/hashes, resource samples and failure
traces in a stable sanitized archive/checked-in record. Keep expected and observed
values separate. Prove the evidence generator's negative controls fail, and
record the fresh tested binary hashes, not merely source HEAD.

## Acceptance and review handoff

Map every V2-WIRE through V2-STORE requirement to Java source/tests, Rust
source/tests and the independent scenario or justified supporting evidence.
Absence is visible; there is no blanket 'unit tests cover the rest' waiver.
Both directions must agree on successful output and exact named refusals,
survive real restart and meet the declared resource gates.

Run focused driver tests, the relevant Rust workspace/full conformance suites,
and affected Java/native suites through the test runner. Record any external
environment constraint without turning it into a pass. Propose discovered spec
corrections separately for coordinated review. B completes task 2 only after
A's full contract and this complete independent evidence are both accepted;
task 3 remains separate.
