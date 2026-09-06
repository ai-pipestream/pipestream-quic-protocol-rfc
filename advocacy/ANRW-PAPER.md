# ANRW Paper Draft: PipeStream

*Target venue: ACM/IRTF Applied Networking Research Workshop (ANRW '27),
co-located with the July 2027 IETF meeting. CFP typically opens ~March
2027, deadline ~May 2027 (ANRW '26 extended to late May). Format: ACM
two-column; short papers ~2–3 pages, long ~6. This draft is written to
long-paper scope; cut Sections 5–6 to fit short-paper scope.*

*ANRW explicitly favors papers with direct IETF relevance — the
"relation to IETF/IRTF work" angle is a feature, not an apology. The
2026 program was dominated by QUIC application papers (QUIC in space,
QUIC for WebRTC, MoQ partial reliability, MASQUE scheduling), so this
sits squarely in scope.*

**Research outline, not submission-ready prose. Section 5 still needs an
equivalent gRPC-streaming baseline and measured results. The reference suite
exists, but open protocol decisions in Appendix E also constrain the claims
this paper can make. Venue dates, format, and submission instructions below
are planning assumptions that must be checked against the actual CFP.**

---

## Title

**PipeStream: Protocol-Level Scatter-Gather over QUIC**

Alternative: *Completion Is Not Delivery: A QUIC-Native Protocol for
Hierarchical Scatter-Gather Processing*

## Abstract (draft)

Distributed processing pipelines decompose large inputs into parts,
process the parts in parallel, and reassemble results under an
all-parts-completed requirement. Deployments can implement this pattern
over streaming RPCs or message brokers with application coordination.
We investigate whether standardizing a shared lifecycle vocabulary reduces
integration work across independently developed endpoints. PipeStream is
an application protocol over QUIC that makes the
scatter-gather state machine a protocol mechanism: a bidirectional
control stream tracks per-entity lifecycle, unidirectional entity
streams carry payloads with independent flow control, hierarchical
scopes support recursive decomposition, and scope digests summarize
reported terminal statuses. The sealed-work profile fixes declared
membership; neither its seal nor a status digest proves correct computation.
We describe the design, independent-code interoperability for documented
subsets, a proposed equivalent-semantics evaluation, and unresolved issues
for IETF review. No comparative performance result is available yet.
PipeStream is specified in
draft-krickert-pipestream.

## 1. Introduction (outline + key prose)

- The pattern: dehydrate → parallel process → rehydrate; industries:
  document/search ingestion, media asset pipelines, ML data prep,
  genomics.
- The gap to evaluate: application-specific completion contracts and
  integration costs across organizational boundaries. Existing systems
  can stream and implement backpressure; do not claim otherwise.
- Hypothesis: a shared application-protocol lifecycle vocabulary can
  reduce coordination glue. QUIC multiplexing does not eliminate durable
  state, application authorization, or external-effect fencing.
- Contributions: (1) protocol design making determinate-set completion a
  first-class mechanism; (2) recursive scope/digest construction with
  RFC 6962-style domain separation; (3) implementation + evaluation vs.
  gRPC streaming; (4) lessons for IETF standardization of
  pipeline-coordination protocols.

Key sentence for the related-work paragraph: *Media over QUIC Transport
shares our architecture (control stream plus unidirectional object
streams) but inverts our problem: MOQT optimizes delivery of objects to
a dynamic subscriber set and may abandon stale objects, whereas
PipeStream drives a determinate entity set to terminal status —
completion, not delivery, is the invariant.*

## 2. Design (outline)

- 2.1 Dual plane: Control Stream 0 (bit-packed STATUS at 21 octets,
  SCOPE_DIGEST, BARRIER, GOAWAY; CAPABILITIES/CHECKPOINT serialized as
  CBOR) + one entity per unidirectional stream, FIN-delimited.
- 2.2 Recursive scopes: distinguish Core's circular ID proposal from the
  sealed profile's durable, scope-qualified identity with no recycling.
  Bidirectional allocation and durable reuse remain open design questions.
- 2.3 Verifiable completion: per-scope Merkle digest, leaf =
  SHA-256(0x00 ‖ entity record incl. terminal status), interior =
  SHA-256(0x01 ‖ L ‖ R); barrier gates reassembly on digest match.
  (This is the most novel section for a networking audience — spend a
  figure on it.)
- 2.4 Resilience: yield/resume, claim locators that are not bearer
  credentials, authenticated retained recovery, and completion policies.
  Recovery and sealed work are not currently composable profiles.
- 2.5 What is deliberately excluded: scheduling, retry policy decisions,
  payload semantics (profiles) — the workflow-engine boundary.

## 3. Why Not X (compressed from draft Appendix B)

One table + two paragraphs: gRPC (stream-per-RPC lifecycle mismatch,
5-octet envelope vs 21-octet total), HTTP/3 (request-response binding),
WebTransport (CONNECT asymmetry, web security model), MOQT (delivery vs
completion), brokers (externalized completion, persist-and-forward).

## 4. Implementation

Java/Netty, Rust/Quinn, and C++/MsQuic have separate codecs and state
machines for a documented Layer 0 subset. A protocol-neutral Rust runner
tests nine executable client/server pairings. Java and Rust additionally
interoperate on sealed recursive work in both directions. Rust implements
optional authenticated-session and retained recovery profiles; Java does
not yet authenticate sealed callers. None demonstrates full conformance.
Use the dated [acceptance audit](../docs/standards/recovery-execution-java-acceptance.md)
for exact tests, resource gates, and remaining limitations rather than
inferring coverage from the implementation language count.

## 5. Evaluation (measurement plan — the paper's make-or-break)

Baseline: gRPC bidirectional streaming with application-level completion
tracking, the same processing logic, durability, authentication, failure
semantics, hardware, and workloads. Select and document the state backend;
an extra network database is not mandatory for an RPC implementation.
Optional second baseline: Kafka pipeline (stage-per-topic).

Workloads: (a) 10k-document corpus, mixed sizes 1 KB–100 MB, 3-stage
pipeline, fan-out 1→32; (b) recursive decomposition depth 3 (container
→ documents → chunks); (c) failure injection: kill a worker mid-scope,
measure detection-to-retry latency.

Metrics:
1. End-to-end latency: first-byte-to-last-result per document.
2. Coordination overhead: bytes on the wire for control traffic per
   entity, including framing, transport, persistence, and retries on both sides.
3. Coordination state: memory/storage held by the tracker at peak.
4. Completion detection latency: last child terminal → parent knows
   (digest/barrier vs polling the DB).
5. Failure detection: injected failure → retry dispatched.

Report measured positive and negative results with repeatability and cost
boundaries. Smaller frames, fewer components, or a QUIC transport do not
by themselves establish lower latency, stronger durability, or less memory.

## 6. Standardization Discussion (ANRW's favorite section)

- Draft history: published individual -03 and locally prepared -04.
  Attribute changes to specific review evidence; repository review is not
  evidence of IETF adoption or feedback from an IETF working group.
- Open questions for the community: is pipeline coordination chartered
  work? WIT or ART area? Experimental vs standards-track?
- Explicit ask: co-authors and second implementations.

## 7. Related Work (academic)

MoQ/Warp/RUSH lineage; SCTP partial reliability; PPSPP (RFC 7574);
workflow systems (Airflow/Temporal — cite as systems, contrast scope);
dataflow systems (Dryad, Naiad — completion via epochs/watermarks is the
closest academic ancestor of scope digests; one paragraph on this
lineage strengthens novelty rather than weakening it); exactly-once
stream processing (Flink checkpointing) vs wire-level completion.

## 8. Submission Logistics

- HotCRP (anrw27.hotcrp.com when it opens); ACM format
  (acmart/sigconf); not double-blind historically (check CFP).
- ANRW allows and encourages work-in-progress; a short paper with
  honest preliminary numbers beats a long paper with padded ones.
- Co-author from a university networking group both strengthens the
  paper and satisfies the multi-org goal from PRIOR-ART-SURVEY.md —
  recruit before writing Section 5, so the evaluation design carries
  their methods.
- Presenting at ANRW '27 (Monday of IETF week) puts the work in front
  of exactly the people a BoF would need — treat the talk as the BoF
  warm-up.
