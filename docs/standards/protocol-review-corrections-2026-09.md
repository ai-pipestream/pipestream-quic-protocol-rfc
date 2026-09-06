# Protocol review corrections, 2026-09-06

Scope: correct the existing draft before expanding its reference implementations.
Baseline: `3dd5a4df877b3dfa666c35142d840b6264655ca2`. This increment changes
specification and review material, not executable code, wire encodings, extension
identifiers, storage formats, or deployed services. Local -04 remains separate
from an author-approved Datatracker submission.

## Corrected requirements

1. Sections 5.5 and 6.2.5 distinguish transport loss from authoritative work
   failure. Commit followed by lost acknowledgment cannot become an originator's
   invented FAILED state, cursor advance, or execution grant. Reset preserves
   declared and admitted obligations; an unusable Control Stream requires a
   separate connection for any supported recovery.
2. Section 5.1.6 distinguishes cumulative QUIC flow-control limits from congestion
   control and removes the blanket whole-entity credit prerequisite. Incremental
   consumption and bounded spooling permit entities larger than a window.
   Whole-message strategies remain possible with an explicit size/credit policy.
3. Section 6.2.2 defines PROCESSING as validated admission, not bytes arriving.
   Declaration, admission, successful computation, and covered-set closure are
   separated in the new Section 1.7 model. Section 9.8 explicitly overrides the
   unsealed checkpoint admission-before-request rule.
4. Section 10.2 reconciles optional checksums with validation requirements.
   Supplied per-chunk checksums must pass; a locally calculated digest is not
   verification against a sender's whole-entity commitment. Profiles requiring
   such a commitment must define its authenticated association and verification
   point. Irreversible effects wait for all applicable validation, authorization,
   and completion checks. A failed resume checksum does not authorize re-execution.
5. Section 9.8 explicitly applies the existing principal-authentication requirement
   to durable sealed declarations. A producer label, successful sealed negotiation,
   server-only TLS, or trusted network does not authenticate the requester.
   Java's current sealed fixtures do not demonstrate compliance with that requirement.
6. Sections 7 and 10 distinguish opaque application encryption from required
   parseable Core fields. Padding cannot falsify declared membership or add bytes
   to fixed control frames. Encrypted storage-reference metadata is not a complete
   cryptographic suite or a grant to resolve arbitrary keys.
7. Terminology now describes scoped checkpoints and endpoint-local manifests.
   Connection setup opens Stream 0 before capability exchange. The processing
   action diagram is an application-role illustration, not four new wire commands
   or a mandatory sequential schedule for the entire workload.

The implementation appendix and submission material now acknowledge completed
Java producer observations and Rust orphan reconciliation. Outreach text no
longer treats status digests as proofs of correct computation, claim URIs as
bearer credentials, or lower latency than streaming RPC as an established fact.

## Evidence and review traces

The following existing tests support specific corrected rules, not complete
protocol conformance:

- Rust `quinn/tests/draft04/spooled_ingress.rs`,
  `fin_length_and_checksum_validation_precede_admission_and_clean_spool_files`:
  invalid length or checksum produces a named refusal with no callback or durable
  admission and releases temporary spool accounting.
- Java `SealedServerTest.resetStreamNeverCompletesDeclaredWorkAndCheckpointAckReplaysAfterRestart`:
  reset leaves declared work pending; later valid input completes once and its
  checkpoint acknowledgment can replay after restart.
- Rust `quinn/tests/draft04/retained_recovery.rs`: authenticated retained receipts,
  field correlation, restart, expiry, refusal, and terminal outcomes are separate
  from transport observations. This is not Java recovery interoperability.
- Rust `quinn/tests/spool_resources.rs`: actual QUIC transfer streams 32 MiB
  incrementally, validates the stored digest, and measures Rust heap allocations.
  The sender's configured 1 MiB send budget is not a receiver MAX_DATA measurement.
- Java's independent sealed tests and both Java/Rust directions exercise declaration
  replay, fixed scope closure, missing input, and protocol refusals. They do not
  prove caller authentication for the Java server.

Review the corrected text against these failure traces:

1. Server commits completion; acknowledgment is lost. The client records an unknown
   outcome and uses the negotiated retained-outcome procedure, if any. It cannot
   create a failure, retry grant, or closure proof from the disconnect.
2. A declared payload stream resets before FIN. Discard partial reception, not
   the declaration. A sealed checkpoint remains pending without complete input.
3. An entity exceeds the current QUIC window. Incremental consumption can replenish
   credit; requiring the whole entity before either sending or releasing credit
   is not a valid generic streaming strategy.
4. A checksum-free entity arrives with valid FIN and length. Core does not invent
   a missing expected digest. Any stronger application integrity requirement must
   have an explicitly specified verification mechanism before publication.
5. An anonymous caller negotiates sealed work. Negotiation alone cannot authorize
   a durable declaration. An authenticated application binding is still required.

These are review traces and mappings to existing tests, not newly implemented
model checking or a claim that every prototype now enforces every requirement.

## Decisions deliberately not made implicitly

Appendix E now records result delivery and input/output binding, sealed/recovery
composition, authenticated profile selection, long-running work beyond the fixed
24-hour receipt interval, minimal Core scope, and SCOPE_DIGEST count semantics.
Bidirectional identity allocation and safe durable identifier reuse remain open.
Changing these requires explicit wire/version decisions and new failure tests,
not relaxing current refusals or advertising incomplete Layer 2 as complete.

Before adding another full server, settle those contracts, obtain independent
implementer review, and compare an equivalent streaming-RPC coordinator with
the same processing, persistence, authentication, and failure semantics.

## Standards basis

- [RFC 4101](https://www.rfc-editor.org/rfc/rfc4101.html): reviewer-oriented protocol model.
- [RFC 9000, Section 4.1](https://www.rfc-editor.org/rfc/rfc9000.html#section-4.1): stream and connection flow-control limits.
- [RFC 9308, Section 4.4](https://www.rfc-editor.org/rfc/rfc9308.html#section-4.4): application flow-control strategies and dependency deadlocks.
- [RFC 5116](https://www.rfc-editor.org/rfc/rfc5116.html): authenticated-encryption interface, associated data, and nonce requirements.

## Validation

All checks below passed locally on 2026-09-06:

- `./build.sh core 04`: XML, text, and HTML generated; idnits reported zero
  errors, flaws, and warnings, with the existing FIPS 180-4 reference comment.
  The rendered text was checked for the model, transport-error, integrity, and
  open-issue sections and their cross-references.
- `./conformance/run_all.sh`: exit 0. Rust formatting and strict clippy, 314
  Rust workspace tests, 193 Java reference tests (zero failures/errors/skips),
  native SQLite and C++ checks, all nine executable pairings, 32 raw QUIC
  capability probes, recursive/recovery scenarios, and all three external
  examples passed. The run includes the independent Java/Rust sealed tests.
- Frozen wire vectors and normative CDDL checks passed. No executable source,
  wire/CDDL schema, or frozen-vector changes are included in this increment.
- `git diff --check`: passed.

Logs on the validation host are `/tmp/pipestream-protocol-review-draft.log`
and `/tmp/pipestream-protocol-review-conformance.log`; they are local evidence,
not committed artifacts. Java totals were checked from Surefire XML rather
than inferred from a quiet Maven banner. Netty emits its existing deprecated
`sun.misc.Unsafe` runtime warning; no warning-free Java runtime claim is made.
Document validation and passing subset interoperability do not imply IETF
adoption, full conformance, hosted CI, deployment, or submission.
