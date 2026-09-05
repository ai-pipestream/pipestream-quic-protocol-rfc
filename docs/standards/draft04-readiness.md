# Draft-04 readiness and implementation evidence

Updated 2026-09-05. This landing starts from `00531de` and addresses the
[protocol review](../ai-slop/ietf-protocol-review-2026-09.md).
The specification remains an individual Internet-Draft, not an approved
standard. No implementation in this repository demonstrates full conformance.

## Changes and regression coverage

| Review item | Implemented change | Evidence |
|---|---|---|
| R1 | Validate received maps before applying optional defaults; Java/C++ accept core recursive capability limits when downgrading | `test-vectors/optional-fields.tsv`, consumed by all three codecs |
| R2 | Omitted checkpoint flags decode as zero without false deterministic-encoding refusal | Same shared vectors |
| R3 | Recursive Rust skips unknown control frames after negotiation | `r3_r4_unknown_frame_and_entity_without_pending` |
| R4 | Entity reception does not require PENDING | Same raw-QUIC test |
| R5 | Announcements are matched by entity identity, not stream acceptance order | `r5_announcements_do_not_order_entity_streams` |
| R6 | Pending checkpoints allow descendant progress, use monotonic deadlines, and emit a named timeout | Two checkpoint wire tests |
| R7 | Session construction uses negotiated depth and entity limits | `r7_negotiated_depth_is_enforced_before_payload_storage` |
| R8 | A processor yield cannot emit Layer 2 statuses on a Layer 0 connection | `r8_layer0_never_receives_layer2_statuses` |
| R9 | Checkpoint ACK identity is compared exactly; claim, digest, and barrier correlation is also checked | `r9_mismatched_checkpoint_ack_is_refused`; happy-path recursive and recovery scenarios |
| R10 | Quorum uses shortest exact CBOR floats and an integer-computed success threshold | Deterministic encoding tests, corrected recursive vector, `quorum_threshold_uses_exact_integer_rounding` |
| R11 | Recovery and redemption callbacks serialize under the same SQLite transaction mechanism | `concurrent_recovery_serializes_resume_across_store_handles` checks one callback across concurrent recoverers |
| Admission | Invalid parent/depth/identity and digest failures occur before application callbacks or payload writes | Callback counters and absent-child-storage assertions in wire tests |
| Resource handling | Independent bounded stream readers; aggregate receive/chunk budgets; incremental control-body allocation | Stalled-stream and aggregate-chunk-limit wire tests |
| URI | Typed session/entity/claim locators, explicit port and numeric bounds, no userinfo or bearer secrets | Core URI acceptance and refusal tests |
| Evidence integrity | Actual Appendix C/CDDL definition comparison; independent expected local receipt calculation | Conformance schema drift test and recursive CLI receipt equality checks |
| Extension negotiation | Bounded supported/required sets, exact intersection and requirement union, named refusal, client response validation | 35 shared codec cases and raw QUIC probes, detailed below |
| Sealed work sets | Opt-in client producer binding, durable declaration/seal ACKs, immutable full-scope cuts, and declaration replay | 20 frozen wire inputs, core storage tests, and raw/public-client QUIC tests, detailed below |

The wire tests are in `implementations/rust-quinn/quinn/tests/draft04_wire.rs`.
The coverage above is a review-finding matrix, not a requirement-to-test matrix
for every MUST in the specification. Unlisted behaviors are not implicitly
verified. All implementations still need that complete matrix.

## Standard changes

- Normative QUIC TLS mapping reference, corrected WebTransport comparison,
  and explicit UDP connection-failure behavior.
- Precise omitted-field and binary16/binary32 representation rules.
- Exact quorum threshold and partial-success semantics.
- Pending-checkpoint timeout, duplicate-request deadline behavior, and an
  admission requirement preventing checkpoints from overtaking their own cut.
- Scope status hashes explicitly distinguished from content receipts and
  proof of correct computation.
- Authentication, authorization, principal/session/authority binding,
  retention, revocation, and external-effect idempotency requirements.
- Tagged `pipestream://` resource paths, a required port, bounded identifiers,
  and explicit separation of locators from access credentials.
- Specification Required registry policies, registration templates, yield
  reasons, and checkpoint-timeout error 0x0E. Error 0x06 is named
  PIPESTREAM_LIMIT_EXCEEDED to cover payload and aggregate resource limits.
- Factual implementation status and an explicit open-issues appendix.
- Supported/required extension negotiation, one exchange per connection,
  explicit activation and downgrade rules, and a proposed 16-bit registry.
  No public extension identifiers are assigned. The Rust recursive service
  additionally offers private-use profile 65281 by explicit peer agreement.
- Section 9.8's client-owned work-set lifecycle, identity non-reuse, declaration
  and seal hashing, scope-qualified checkpoints, and root GOAWAY cut.

## Extension requirement coverage

The negotiation landing (`d10d9e2`) builds on the first draft-04 landing
(`89711e1`). It supplied the base mechanism for the sealed-work profile below
and future authenticated recovery profiles.

All three independent codecs consume `test-vectors/extension-negotiation.tsv`.
The positive selection cases use synthetic identifiers solely as test data;
they do not assert implementation or registration of an extension.

| Section 3.4.3 requirement | Evidence |
|---|---|
| Bounded, sorted, unique identifier lists and required subset | `too-many`, `unsorted`, `duplicate`, reserved/type cases, `required-not-supported` |
| Received CBOR determinism before defaults | Empty-list, non-minimal array/integer, indefinite array, float and trailing-item cases |
| Intersection of supported sets and union of requirements | `intersection-required-union`, `maximum-required-union` |
| Both parties' requirements must be supported | `client-required-unknown`, `server-required-unknown` |
| Client rejects omitted requirements or unsolicited selections | Missing-required, missing-echo and unsolicited-response cases |
| Response cannot escalate capabilities | Layer, window, timeout and serialization cases |
| Unsupported requirements fail before admission with error 0x0F | Raw QUIC probes verify the application close code and no stored entity |
| Optional unknown IDs are not activated; no repeated negotiation | Raw QUIC probes compare exact response bytes, then require duplicate-CAPABILITIES refusal |
| Application work waits for a valid response | Malformed-server probes against Java, C++, Rust one-entity and Rust recursive clients |
| Negotiation refusal is terminal | `rejected-then-valid` pipelines a second offer, PENDING and an Entity Stream; no stored entity after server exit |

The raw probes use Quinn as a transport and frozen message bodies, without
importing any PipeStream codec or state machine. Sealed-profile dependency
tests additionally require Layer 1 and exclude Layer 2. Authenticated profiles
and principal-bound durable capability binding remain open.

## Sealed-work requirement coverage

Section 9.8 uses private-use identifier 65281, `sealed-work-sets-v1`.
Only Rust implements it. The public `connect_sealed` and `declare_work`
APIs require negotiation and exact ACK correlation. The producer label is
durable identity data, not a principal or credential.

| Profile requirement | Evidence |
|---|---|
| Deterministic bounded WORK_SET fields | 20 independent frozen inputs in `test-vectors/work-sets.tsv`; separate CDDL fixtures |
| Seal binds the complete set independently of batching | Fixed independently calculated SHA-256 and 1,024-ID, four-batch test |
| Missing declarations/payloads cannot disappear from completion | Core maximum-ID cut test and pending-checkpoint QUIC test |
| Child sets remain accountable through root completion | Out-of-order child payloads, scope closure, parent rehydration, root ACK test |
| Identity, sequence, and seal failures do not mutate declarations | Core state-equality and SQLite transaction rollback checks |
| Unobserved ACK can be replayed after restart | Public sealed client attaches to retained SQLite state and completes the original set |
| ACK must exactly match the request | Changed-owner and malformed-ACK tests check client error and actual QUIC close code |
| No unnegotiated declarations, undeclared admission, or early GOAWAY | Named-refusal wire tests and absent payload storage checks |
| Announcement budget and no STATUS cursor recycling | Bounded-window and cursor refusal wire tests |
| No mode conversion or unsafe old-format load | Legacy-session declaration refusal and version-1 row refusal without writes |

Payloads can arrive before a final seal, but only after their declaration
ACK. A missing or rejected declared payload stays outstanding; cancellation
tombstones and automatic retries are not implemented. The profile has a
single client producer and excludes Layer 2. It is not an authenticated
multi-tenant session or a claim-redemption protocol.

Stored session format changes from version 1 to version 2. Old records are
refused before deserialization, not converted. No running service or existing
application database was migrated. Keep old databases with their matching
binary. Tests cover declaration commit/reopen and lost-ACK replay, not every
payload/application-effect crash boundary.

The local aggregate budget is 1,000,000 declared IDs per session; each batch
is limited to 256 and per-scope negotiated limits still apply. The SQLite
adapter rewrites a serialized session per transaction, and sealing hashes
the complete ID set. No large-session throughput or resource-efficiency claim
is made. The local lineage digest now commits to the profile's producer and
scope seals under a distinct domain tag; it is still not an authenticated
content receipt or proof of correct computation.

## Validation

`./conformance/run_all.sh` runs Rust formatting, Clippy with warnings denied,
the workspace tests, Java tests, C++ CTest, both Rust example suites, the
frozen-vector and CDDL checks, all nine client/server pairings, both recursive
and recovery CLI scenarios, and all three cross-language applications.
The first landing passed locally on 2026-09-05 with 45 Rust workspace tests.
The follow-up passed the complete command locally on 2026-09-05 with
46 Rust workspace tests, 35 shared extension cases, and 32 raw QUIC
capability probes. All three language suites, nine transfer pairings,
recursive/recovery scenarios, and external examples passed. The conformance
runner also builds separately from the rest of the Cargo workspace.

The sealed-work landing passed the complete command locally on 2026-09-05
with 63 Rust workspace tests (including nine sealed-work core tests and
eight sealed-work QUIC tests), 20 new frozen work-set inputs, the Java and
C++ suites, all nine transfer pairings, 32 capability probes, and every
recursive/recovery scenario and external example.

The adversarial pass also fixed terminal failure handling: Java completes
both public waiters and ignores callbacks after the first failure; C++
will not process buffered frames or entities after failure; Rust drains
its endpoint before a one-shot server exits following a refusal. The raw
probes require the actual close code and inspect storage after server exit.

`./build.sh core 04` produced XML, text, and HTML. idnits reported zero errors,
zero flaws, zero warnings, and one informational possible-downref comment
for the NIST FIPS 180-4 normative reference. This is document validation,
not distributed-protocol validation. The build now refuses a filename
revision that differs from the source XML docName.

These are local results, not hosted CI or independent implementer review.
No Python implementation, protocol oracle, or example was added. The external
xml2rfc renderer remains confined to document authoring.

## Still required before a conformance or deployment claim

1. Define and implement an authenticated resilience profile using the new
   supported/required negotiation mechanism, with principal-bound claims, durable
   executor fencing, and retained outcomes for lost redemption ACKs. The
   current Layer 2 boolean advertises more than the prototype implements.
2. Add bounded spool-backed payload processing and an asynchronous fenced
   application executor, with crash-boundary and measured resource gates.
3. Implement the clarified sealed-work profile independently in Java, and
   cross-test declaration, lost ACK, reconnect, and descendant completion.
   C++ follows as the third implementation; the full requirement matrix
   remains necessary for every implementation.

Additional gaps include Layer 0 dehydration, arbitrary bidirectional work
origination, child-before-parent admission buffering, automatic retry/timer
policies, authorization tests, crash-boundary coverage, and measured resource
and performance gates. The standalone durable service authenticates only the
server and must not be exposed as an untrusted multi-tenant service.

SQLite IMMEDIATE transactions prevent simultaneous recovery callbacks through
one database, including separate store handles. They serialize writers and
require bounded, non-reentrant callbacks. They do not guarantee exactly-once
effects across a crash, nor do they fence unrelated databases.

## Submission and adoption

See [the draft-04 checklist](../../advocacy/SUBMISSION-CHECKLIST-04.md).
A published draft, a merged repository branch, working-group adoption, IESG
approval, and IANA registration are separate milestones. Document checks
and language count do not establish adoption, approval, or registration.
