# Prior-Art Trajectory Survey: QUIC Application Protocols at the IETF

*Survey date: 2026-07-23. Purpose: identify which QUIC application-protocol
drafts advanced past the "first round" (dispatch, WG adoption, or BoF), which
stalled, and what differentiated them — to inform the strategy for
draft-krickert-pipestream.*

## Summary of the pattern

QUIC application drafts at the IETF fall into three trajectories:

**A. Existing standardized protocol mapped onto QUIC, brought to the WG that
already owns that protocol.** These get adopted routinely. The WG already has
a constituency, a charter, and reviewers; the draft is an incremental mapping.

**B. New QUIC-native protocol with no home WG.** These have never advanced as
individual drafts. Every success required a BoF and a newly chartered WG, and
the chartered WG produced a *merged* protocol from several input drafts rather
than adopting any one of them verbatim.

**C. Single-organization individual drafts.** These expire, regardless of the
size of the organization behind them, unless they convert into category B by
building a multi-vendor coalition.

PipeStream is category B with current category-C authorship. That is the
strategic problem to solve; the technical content is secondary to it.

## Case studies

### Adopted: existing protocol × home WG (category A)

| Protocol | Draft / RFC | WG | Status (July 2026) |
|---|---|---|---|
| DNS over QUIC (DoQ) | RFC 9250 | DPRIVE | Published 2022 |
| RTP over QUIC (RoQ) | draft-ietf-avtcore-rtp-over-quic-14 | AVTCORE | Adopted, in progress; not yet an RFC |
| NETCONF over QUIC | draft-ietf-netconf-over-quic-07 (Jan 2026) | NETCONF | Adopted (from individual draft-dai-netconf-quic-netconf-over-quic) |
| EPP over QUIC | draft-ietf-regext-epp-quic-12 | REGEXT | Adopted, advanced revision |

Links:
- <https://datatracker.ietf.org/doc/draft-ietf-avtcore-rtp-over-quic/>
- <https://datatracker.ietf.org/doc/draft-ietf-netconf-over-quic/>
- <https://datatracker.ietf.org/doc/draft-ietf-regext-epp-quic/>

Takeaway: adoption is cheap when a WG already owns the application semantics.
PipeStream has no such WG — no IETF group owns "distributed document/entity
processing" — so this path is closed, and comparisons to these drafts
under-estimate the work required.

### Chartered via BoF: new QUIC-native protocols (category B)

**Media over QUIC (MoQ)** — the closest structural cousin to PipeStream
(control stream + unidirectional object/data streams, QUIC-native, own ALPN):

- Meta's **RUSH** (draft-kpugin-rush): authors from a single company (Meta).
  Individual submission, never adopted, expired (last revision 2025-04-21).
  <https://datatracker.ietf.org/doc/draft-kpugin-rush/>
- Twitch's **Warp** (draft-lcurley-warp): authors from Twitch, Meta, Cisco,
  and Google. Replaced by draft-lcurley-moq-transport, which the chartered WG
  took as the basis for draft-ietf-moq-transport (now at revision -18).
  <https://datatracker.ietf.org/doc/draft-lcurley-warp/>
- BoF request (bofreq-hardie-media-over-quic) led to a BoF at IETF 113 and a
  chartered WG in 2022. <https://datatracker.ietf.org/group/moq/about/>

RUSH vs. Warp is a controlled experiment: same problem space, same era, same
venue. The single-company draft expired; the four-company draft became the WG
document. Author diversity was the differentiator, not technical quality.

**WebTransport** and **MASQUE** followed the same arc: BoF → chartered WG →
merged deliverables (e.g., CONNECT-UDP, RFC 9298). No individual draft in
either space advanced without the WG being created first.

### Stalled: individual drafts without a coalition (category C)

- **SSH over QUIC** (draft-bider-ssh-quic, through -09, Dec 2020): expired.
  Technically thorough; no co-author coalition, no WG home.
  <https://datatracker.ietf.org/doc/draft-bider-ssh-quic/>
- **MQTT over QUIC**: EMQ shipped it in production (EMQX 5.0) but pursued
  standardization at **OASIS** (which owns MQTT) rather than the IETF —
  consistent with the category-A rule: go where the application protocol's
  owner is.
- **CoAP over QUIC**: despite RFC 8323 (CoAP over TCP/TLS/WebSockets) and
  years of academic interest, no adopted QUIC mapping exists in CoRE.

## What differentiated the drafts that advanced

1. **Multi-organization author lists** (Warp: 4 companies; DoQ, RoQ: mixed
   vendor/operator/academic). No adopted draft in this survey had a
   single-organization author list at adoption time.
2. **Running code before adoption.** MoQ inputs had deployed precursors
   (Twitch, Meta production systems); NETCONF/EPP mappings had implementations.
   An RFC 7942 Implementation Status section signals this cheaply.
3. **A measured problem, not a described one.** BoF-approved efforts quantified
   the deficiency of existing options (e.g., latency numbers vs. HLS/WebRTC for
   MoQ). PipeStream needs equivalent numbers vs. gRPC streaming / Kafka
   pipelines / HTTP APIs.
4. **A constituency that shows up.** The MoQ BoF had 58 participants from two
   distinct industries. Adoption is a head-count exercise.

## Venue and timing facts (verified July 2026)

- **IETF 126** is this week (Vienna, co-located events July 18–24, 2026).
- **ANRW '26** (Applied Networking Research Workshop) ran Monday, July 20,
  2026, in Vienna. Its program is dominated by QUIC application papers
  (QUIC in space, QUIC for WebRTC multiplexing, MoQ partial reliability,
  MASQUE scheduling) — a PipeStream paper fits this venue precisely.
  Next cycle: **ANRW '27**, co-located with the July 2027 IETF; CFP typically
  opens ~March with a deadline ~May. <https://www.irtf.org/anrw/>
- **IETF 127: November 14–20, 2026, San Francisco.** This is the realistic
  target for a DISPATCH slot and/or a side meeting. Draft submission cutoff
  will be roughly two weeks prior (watch
  <https://datatracker.ietf.org/meeting/important-dates/>).
- The ALLDISPATCH experiment has concluded; new ART-area work goes to
  **DISPATCH** again. <https://wiki.ietf.org/en/group/alldispatch>
- No competing or overlapping draft exists for the distributed
  entity-processing niche: Datatracker searches surface only
  draft-krickert-pipestream itself. First-mover advantage, but also proof
  there is no ready-made constituency.

## Implications for draft-krickert-pipestream

1. **Do not expect adoption of the draft as-is.** The realistic best case is
   the Warp outcome: the draft becomes the primary input to a chartered
   effort, possibly renamed and co-authored. Plan for that emotionally and
   strategically; it is the *success* scenario.
2. **Recruit 2–3 co-authors from other organizations before IETF 127.**
   Candidate pools: document-processing/search vendors, data-pipeline
   operators, an academic networking group (which also unlocks ANRW).
   This is the single highest-leverage action available.
3. **Target DISPATCH at IETF 127** (SF, Nov 14–20, 2026) with a 1-page
   problem statement and a 10-minute deck. A side meeting at 127 is the
   fallback if the DISPATCH agenda is full.
4. **Publish measurements.** An ANRW '27 paper (deadline ~May 2027) with
   latency/throughput comparisons vs. gRPC streaming and Kafka would serve as
   both academic validation and BoF ammunition.
5. **Keep the ISE option open.** The Independent Submission Stream can publish
   PipeStream as an Informational RFC without WG adoption — a stable, citable
   RFC number for commercial conversations. This does not preclude a later
   standards-track effort, but signals lower IETF consensus; sequence it
   after a DISPATCH attempt, not before.
6. **Add an RFC 7942 Implementation Status section** as soon as any
   implementation is public, even partial.

## Sources

- <https://datatracker.ietf.org/group/moq/about/>
- <https://datatracker.ietf.org/doc/bofreq-hardie-media-over-quic/>
- <https://datatracker.ietf.org/doc/draft-kpugin-rush/>
- <https://datatracker.ietf.org/doc/draft-lcurley-warp/>
- <https://datatracker.ietf.org/doc/draft-ietf-avtcore-rtp-over-quic/>
- <https://datatracker.ietf.org/doc/draft-ietf-netconf-over-quic/>
- <https://datatracker.ietf.org/doc/draft-ietf-regext-epp-quic/>
- <https://datatracker.ietf.org/doc/draft-bider-ssh-quic/>
- <https://www.emqx.com/en/blog/mqtt-over-quic>
- <https://www.irtf.org/anrw/2026/>
- <https://www.ietf.org/meeting/127/>
- <https://wiki.ietf.org/en/group/alldispatch>
- <https://www.ietf.org/process/new-work/>
