# DISPATCH Kit for draft-krickert-pipestream

*Working outreach material, updated 2026-09-06. Confirm the current routing
venue, chairs, meeting dates, and deadlines before sending anything.
No agenda slot, chair contact, or submission is implied by this document.*

## Logistics (do these in order)

1. **Check the current IETF meeting and submission dates.** Published -03
   is separate from local -04; use SUBMISSION-CHECKLIST-04.md to prepare
   an author-approved revision before any announcement.
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

**A shared lifecycle contract for hierarchical scatter-gather processing**

Distributed processing pipelines — document ingestion and enrichment,
media asset decomposition, ML feature extraction, genomic assembly —
share one structural pattern: a large input is decomposed into parts,
the parts are processed in parallel across nodes operated by different
teams or vendors, and the results are reassembled with a correctness
requirement that *every* part completed. Deployments can assemble this from streaming RPCs plus
external coordination state (databases, queues, workflow engines), which
requires agreement on application-specific completion and recovery
contracts. Such systems can already stream large inputs incrementally;
the proposal concerns shared coordination semantics, not exclusive access
to streaming or backpressure.

PipeStream (draft-krickert-pipestream) is a QUIC-native application
protocol that makes the scatter-gather state machine a protocol
mechanism: a bidirectional control stream tracks the lifecycle of every
entity, unidirectional entity streams carry payloads with per-entity flow
control, hierarchical scopes support recursive decomposition, and
scope digests summarize reported terminal statuses. The private-use sealed
profile fixes declared membership; neither a seal nor a digest proves correct
computation. The design draws on the
RFC 9308 application-mapping guidance and the architectural precedent of
DNS-over-QUIC (RFC 9250) and Media over QUIC Transport: a dedicated ALPN,
a control stream, and data objects on unidirectional streams. Unlike
MOQT's publish/subscribe delivery, PipeStream tracks a determinate entity
set to terminal status — a completion-oriented, not delivery-oriented,
protocol.

Published -03 and locally prepared -04 describe negotiable capability
layers, CBOR/CDDL schemas, proposed IANA registries, and security requirements.
The current reference suite tests three independent-code Layer 0 subsets,
Java/Rust sealed-work interoperability, and Rust authenticated recovery.
Open identity, result-delivery, retention, and profile-composition questions
remain in Appendix E. We are seeking routing guidance: is there
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
3. **Why this mapping** — independent entity streams plus coordinated work
   state. Compare equivalent streaming RPC designs; explain the proposed
   interoperability benefit without presuming a performance advantage.
4. **PipeStream in one slide** — dual plane: Control Stream 0 (STATUS,
   digests, barriers) + unidirectional Entity Streams (one entity each,
   FIN-delimited). Three capability layers: Core / Recursive / Resilience.
5. **The consistency mechanism** — scope digests (Merkle, RFC 6962-style
   domain separation) + barriers: closure over the declared set is distinct
   from verification of payload, output, and computation.
6. **Precedent and fit** — RFC 9308 guidance followed; DoQ/MOQT
   architecture pattern; ALPN `pipestream/1`; CBOR/CDDL, IANA registries,
   security requirements and unresolved design decisions.
7. **Status and ask** — published -03, local -04; documented subset
   interoperability, not full conformance; seeking list discussion, co-authors from
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
An RPC service can express the same behavior. The research question is
whether a common lifecycle contract reduces integration work across
independent implementations. The draft and evaluation must demonstrate
that value; choosing a dedicated ALPN does not establish it.

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

- [ ] Author-approved revision submitted and verified on Datatracker
      (build with ./build.sh core 04, idnits clean)
- [ ] Agenda request emailed to DISPATCH chairs (September 2026)
- [ ] Intro thread posted to dispatch@ietf.org
- [ ] At least one non-author voice prepared to speak in support (recruit
      via quic@/moq@ side conversations or the ANRW community)
- [ ] Implementation Status appendix updated to reflect reality
- [ ] Slides uploaded to the meeting materials page before the session
