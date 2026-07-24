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

**Blocking dependency: Sections 5 (Evaluation) requires a working
implementation and a gRPC-streaming baseline. Everything else can be
finalized from the spec today.**

---

## Title

**PipeStream: Protocol-Level Scatter-Gather over QUIC**

Alternative: *Completion Is Not Delivery: A QUIC-Native Protocol for
Hierarchical Scatter-Gather Processing*

## Abstract (draft)

Distributed processing pipelines decompose large inputs into parts,
process the parts in parallel, and reassemble results under an
all-parts-completed correctness requirement. No standardized wire
protocol expresses this pattern: deployments layer ad-hoc completion
tracking over streaming RPCs or message brokers, externalizing
coordination state and preventing end-to-end incremental streaming. We
present PipeStream, a QUIC-native application protocol that makes the
scatter-gather state machine a protocol mechanism: a bidirectional
control stream tracks per-entity lifecycle, unidirectional entity
streams carry payloads with independent flow control, hierarchical
scopes support recursive decomposition, and Merkle-tree scope digests
make completion of a decomposition cryptographically verifiable before
reassembly. We describe the design and its rationale against gRPC,
WebTransport, and Media over QUIC Transport, report [preliminary
measurements] from our implementation showing [X]% lower end-to-end
latency and [Y]× less coordination-state memory than a gRPC-streaming
baseline on document-processing workloads, and discuss open issues for
IETF standardization. PipeStream is specified in
draft-krickert-pipestream.

## 1. Introduction (outline + key prose)

- The pattern: dehydrate → parallel process → rehydrate; industries:
  document/search ingestion, media asset pipelines, ML data prep,
  genomics.
- The gap: transport moves bytes; *completion* lives in external state
  (workflow DBs, offsets, sagas). Consequences: no end-to-end
  backpressure, persist-and-forward at every broker hop, vendor-SDK
  coupling across organizational boundaries.
- Claim: completion tracking is a *transport-adjacent* concern — like
  flow control, it is only correct when it sees every message — and QUIC's
  stream model finally makes it cheap to express on the wire.
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
- 2.2 Recursive scopes: parent/child IDs, circular 32-bit ID space
  (modulo 0xFFFFFFFD) with windowed comparison — explain why circular
  (long-lived sessions, millions of entities) and the 2^31−2 window cap.
- 2.3 Verifiable completion: per-scope Merkle digest, leaf =
  SHA-256(0x00 ‖ entity record incl. terminal status), interior =
  SHA-256(0x01 ‖ L ‖ R); barrier gates reassembly on digest match.
  (This is the most novel section for a networking audience — spend a
  figure on it.)
- 2.4 Resilience layer: yield/resume, claim checks as bearer URIs,
  completion policies (strict/quorum/best-effort).
- 2.5 What is deliberately excluded: scheduling, retry policy decisions,
  payload semantics (profiles) — the workflow-engine boundary.

## 3. Why Not X (compressed from draft Appendix B)

One table + two paragraphs: gRPC (stream-per-RPC lifecycle mismatch,
5-octet envelope vs 21-octet total), HTTP/3 (request-response binding),
WebTransport (CONNECT asymmetry, web security model), MOQT (delivery vs
completion), brokers (externalized completion, persist-and-forward).

## 4. Implementation (to write when true)

Language, LOC, QUIC library used, what layers are implemented, interop
harness. Honesty note: single implementation; state what exists and
what is stubbed.

## 5. Evaluation (measurement plan — the paper's make-or-break)

Baseline: gRPC bidirectional streaming with application-level completion
tracking in Redis/Postgres (the realistic incumbent), same workload.
Optional second baseline: Kafka pipeline (stage-per-topic).

Workloads: (a) 10k-document corpus, mixed sizes 1 KB–100 MB, 3-stage
pipeline, fan-out 1→32; (b) recursive decomposition depth 3 (container
→ documents → chunks); (c) failure injection: kill a worker mid-scope,
measure detection-to-retry latency.

Metrics:
1. End-to-end latency: first-byte-to-last-result per document
   (incremental streaming should dominate here).
2. Coordination overhead: bytes on the wire for control traffic per
   entity (21-octet STATUS vs gRPC+DB round trips).
3. Coordination state: memory/storage held by the tracker at peak.
4. Completion detection latency: last child terminal → parent knows
   (digest/barrier vs polling the DB).
5. Failure detection: injected failure → retry dispatched.

Expected shape of results (validate or report honestly): PipeStream wins
1, 2, 4 substantially; comparable on 3; brokers win durability (say so —
credibility).

## 6. Standardization Discussion (ANRW's favorite section)

- Draft history: individual -00..-03; what IETF review changed (0-RTT
  prohibition, domain-separated digests, registry design with DE
  guidance — concrete examples of process improving the protocol).
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
