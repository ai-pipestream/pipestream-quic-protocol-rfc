# DISPATCH Kit for draft-krickert-pipestream

*Target: DISPATCH session at IETF 127 (San Francisco, November 14–20, 2026).
Fallback: a side meeting at IETF 127. See PRIOR-ART-SURVEY.md for the
evidence behind the strategy.*

## Logistics (do these in order)

1. **Watch the IETF 127 important dates page** for the I-D submission cutoff
   (typically ~2 weeks before the meeting; late October 2026). Submit -03
   before it.
2. **Email the DISPATCH chairs** (addresses on
   <https://datatracker.ietf.org/group/dispatch/about/>) requesting agenda
   time, ideally 4–6 weeks before the meeting. Attach the one-page problem
   statement below. DISPATCH agendas fill early; ask in September.
3. **Post an introduction to the dispatch@ietf.org mailing list** when you
   request the slot. A draft that arrives at the meeting with zero prior
   list discussion reads as a surprise; one with a thread reads as momentum.
4. If the agenda is full, **book a side meeting** via the IETF 127 side
   meeting wiki and announce it on dispatch@ and quic@.
5. Expected DISPATCH outcomes, best to worst: recommend a BoF; recommend
   further list discussion / a side meeting; recommend the Independent
   Submission Stream; no interest. Any outcome except silence is progress —
   even "go to the ISE" yields a citable RFC path.

## One-Page Problem Statement

*(Paste-ready for the chairs' email and the mailing list.)*

---

**Coordinating hierarchical scatter-gather processing lacks a wire protocol**

Distributed processing pipelines — document ingestion and enrichment,
media asset decomposition, ML feature extraction, genomic assembly —
share one structural pattern: a large input is decomposed into parts,
the parts are processed in parallel across nodes operated by different
teams or vendors, and the results are reassembled with a correctness
requirement that *every* part completed. Today no standardized protocol
expresses this pattern. Deployments assemble it from streaming RPCs plus
external coordination state (databases, queues, workflow engines), which
couples multi-organization pipelines through vendor SDKs, externalizes
completion tracking, and prevents incremental end-to-end streaming of
large inputs.

PipeStream (draft-krickert-pipestream) is a QUIC-native application
protocol that makes the scatter-gather state machine a protocol
mechanism: a bidirectional control stream tracks the lifecycle of every
entity, unidirectional entity streams carry payloads with per-entity flow
control, hierarchical scopes support recursive decomposition, and
Merkle-tree scope digests make completion of an entire decomposition
cryptographically verifiable before reassembly. The design follows the
RFC 9308 application-mapping guidance and the architectural precedent of
DNS-over-QUIC (RFC 9250) and Media over QUIC Transport: a dedicated ALPN,
a control stream, and data objects on unidirectional streams. Unlike
MOQT's publish/subscribe delivery, PipeStream tracks a determinate entity
set to terminal status — a completion-oriented, not delivery-oriented,
protocol.

The draft (-03) specifies the full protocol: three negotiable capability
layers, CBOR/CDDL message schemas, IANA registries, and detailed
security considerations. We are seeking dispatch guidance: is there
interest in a BoF toward chartered work on pipeline coordination
protocols, and which venue (WIT-area or ART-area) is the right home?

---

## Slide Outline (10 minutes, ~8 slides)

1. **The pattern** — one diagram: root entity → dehydrate → N workers →
   rehydrate. Name the industries that run this daily (search/document
   processing, media pipelines, ML data prep, bioinformatics).
2. **What's missing** — the coordination gap: RPC streams move bytes;
   completion tracking lives in ad-hoc external state. Multi-vendor
   pipelines interoperate through SDKs, not a protocol.
3. **Why the transport matters** — large entities need incremental
   streaming end-to-end; broker hops force persist-and-forward; HTTP
   request/response binds streams to the wrong lifecycle. (One slide, no
   protocol bashing — frame as "requirements existing tools don't target.")
4. **PipeStream in one slide** — dual plane: Control Stream 0 (STATUS,
   digests, barriers) + unidirectional Entity Streams (one entity each,
   FIN-delimited). Three capability layers: Core / Recursive / Resilience.
5. **The consistency mechanism** — scope digests (Merkle, RFC 6962-style
   domain separation) + barriers: reassembly is gated on verifiable
   completion of a determinate child set.
6. **Precedent and fit** — RFC 9308 guidance followed; DoQ/MOQT
   architecture pattern; ALPN `pipestream/1`; CBOR/CDDL, IANA registries,
   full security considerations. Draft is complete, not a sketch.
7. **Status and ask** — draft-krickert-pipestream-03; implementation in
   progress (update when true); seeking: list discussion, co-authors from
   other organizations, and dispatch guidance (BoF? venue?).
8. **Backup slide** — "why not X" table: gRPC / HTTP/3 / WebTransport /
   MOQT / brokers, one row each (from Appendix B).

## Talking Points and Anticipated Questions

**"Why not build this on MOQT?"**
MOQT delivers objects to whoever is subscribed and may abandon delivery
past a deadline; nobody tracks aggregate completion. PipeStream's entire
purpose is aggregate completion of a determinate set. The overlap is the
architectural pattern (control stream + uni streams), which we follow
deliberately per RFC 9308 — the semantics are disjoint. (Appendix B now
has this argument in full.)

**"Why not gRPC / why does this need to be a protocol?"**
Interoperability across organizations. Within one company, an SDK
suffices; across companies, only a wire protocol does. The pattern is as
common as media delivery was before MoQ — everyone builds it privately.

**"Isn't this a workflow engine? That's application logic."**
No — PipeStream deliberately excludes scheduling, retries policy
decisions, and payload semantics (those live in profiles). It
standardizes only what must cross the wire: entity lifecycle, completion
tracking, integrity, and flow control. Analogy: QUIC didn't standardize
the application either.

**"What's the deployment story / who else wants this?"**
The honest answer today: one implementer, seeking co-authors — say so
plainly; DISPATCH punishes overselling. Name concrete conversations in
progress if any exist by November.

**"Experimental or standards-track?"**
Be flexible: "We believe the mechanism space (circular ID arithmetic,
digest construction) benefits from IETF review regardless of track.
Experimental with a registry would serve early deployments."

## Pre-Meeting Checklist

- [ ] -03 submitted before the cutoff (build with ./build.sh, idnits clean)
- [ ] Agenda request emailed to DISPATCH chairs (September 2026)
- [ ] Intro thread posted to dispatch@ietf.org
- [ ] At least one non-author voice prepared to speak in support (recruit
      via quic@/moq@ side conversations or the ANRW community)
- [ ] Implementation Status appendix updated to reflect reality
- [ ] Slides uploaded to the meeting materials page before the session
