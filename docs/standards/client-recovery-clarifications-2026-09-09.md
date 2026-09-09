# Client recovery and workload review disposition

Date: 2026-09-09. Scope: specification and acceptance-plan documentation only.
Branch: `docs/client-recovery-guidance-2026-09`, based on `8eb5a17`.
The Section 12 and Appendix F sources at that base were compared with Meta's
reviewed checkout `4f56143` and were identical before this change.

Source review: `benchmarks/durable-transform/reviews/spec-weaknesses-and-proposals.md`
in Meta's `agent/rfc-meta-workload-v2` checkout (`/work/worktrees/pipestream-muse`).
This change does not modify Meta's workload or overwrite another agent's branch.

## Decisions implemented

- Section 12.2.1 adds contextual client recovery rules for every named refusal,
  transport uncertainty, bounded automatic recovery and local journal failure.
  It preserves immutable operation/creation identities and separates replay
  from explicit new attempts. It does not mandate retry-until-success.
- Section 12.3 explains pacing with the existing session `v2-limits`, including
  active jobs and retained input/output bytes. Admission receipt, executor
  release and retained-byte release are different events. Shared capacity can
  still refuse a client below its session ceilings.
- Section 12.1 retains the existing negotiated deadlines and payload-progress
  idle rule, clarifies diagnostics and adds refused-stream resource retirement.
  Unrelated wire activity does not keep an object alive.
- New informative Appendix G explains pure recomputation, transactional effect
  deduplication, sink-enforced fencing and their evidence limits. Lease renewal
  alone is not external-effect fencing; no new conformance labels are minted.
- The acceptance ledger adds CR01-CR14 covering client refusal/recovery,
  pacing, journal failure, deadlines and repeated-stream resource cleanup.
  These are required scenarios, not a claim they have already passed.

Appendix F, message codes, CBOR cardinalities, schema/commitment definitions,
and frozen vectors are unchanged. This is a clarification/conformance update
to local draft -05, not a protocol-version bump, IETF submission or release.

## Disposition of the workload proposals

3.1 is accepted with contextual recovery instead of a code-only retry policy.
`LIMIT_EXCEEDED` is not necessarily temporary; `NOT_READY` after detach cannot
be fixed by waiting on that connection. An operation lookup's `NOT_FOUND`
cannot prove a prior in-flight request will never commit.

3.2's advertised fields already exist in the SESSION response (Appendix F's
`v2-limits`). No new limits message is added. Workload pacing can use the
existing response but must not refund an executor credit at admission or
assume terminal work released retained bytes. Zero-refusal completion is a
controlled benchmark objective, not a cross-deployment guarantee.

3.3's non-normative recipes are accepted. Broad conformance labels are deferred
until their exact application/sink/failure scope is specified and tested.

3.4's deadline negotiation already exists in CAPABILITIES. The existing minimum
selection and independent absolute lifetime remain. Resetting object idle time
on any connection activity is rejected because it can pin stalled transfers.

3.5's resource obligation and cross-stack regression scenarios are accepted.
The protocol states the invariant; the acceptance ledger requires evidence
from both implementations without prescribing a particular QUIC API.

1.1-1.4 and 2.1 remain workload/SDK/runner concerns. In particular, the Rust
`DurableSession::receipt` delegates to the local journal: an unjournaled intent
is different from a known intent with no saved receipt. Document that SDK
distinction rather than changing remote operation-lookup semantics. Ready-file
reclamation must establish ownership; changing benchmark lock granularity must
preserve resource isolation and frozen-run comparability. Neither is silently
authorized as a code change by this spec patch.

## Shared-agent handoff

- Coordinating owner: publish this documentation branch and its exact commit.
  Keep the main/shared feature branches and in-progress peer work untouched.
- Claude: review the public Java client/API documentation against Section
  12.2.1 and the selected-deadline diagnostics. Record implementation deltas;
  do not report this document as completed Java evidence.
- Kimi: map CR01-CR14 to existing neutral cases, retaining exact pins and known
  gaps. Propose any finite refuse-N fixture change through the existing
  interface review. Preserve current frozen runs; new requirements need a
  separately identified checkpoint, not retrospective relabeling.
- Meta: use existing session limits in any subsequent pacing work. Keep active
  jobs, pending admissions and retained bytes separate, and distinguish
  benchmark policy from protocol requirements. No new wire negotiation is
  needed for the limits/deadlines named in the review.

Each peer owns its implementation. This handoff shares the agreed spec delta;
it does not merge peer branches, launch benchmarks, deploy services or claim
the original multi-agent goal is complete.
