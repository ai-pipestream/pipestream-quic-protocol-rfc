# Lane divergence register

The Java/Netty and Rust/Quinn lanes are independent implementations of the
same specification; sharing no code is the point. That independence also means
the two lanes sometimes do the same thing two different ways. This register
lists every such case that has been found, with the evidence that found it and
the decision taken, so that no one has to rediscover a divergence or guess
which lane the specification sides with.

Rules:

- A divergence is decided one case at a time by the owner. Nothing here
  implies that either lane is the reference for the other.
- An entry needs evidence a reader can open: a run id, a file and line, a
  test, or a commit. A suspicion goes in a commit message or a review, not
  here.
- "Accepted" means both behaviours stay and the difference is stated where a
  reader would otherwise be surprised. "Resolved" names the commit that made
  the lanes agree. "Open" means the owner has not decided.
- Measurement and harness asymmetries count when they change what the
  acceptance matrix can conclude about one lane and not the other.
- This register was started on 2026-10-29 from divergences already written
  down elsewhere. No systematic code-level audit of the two lanes has been
  done; add entries as they are found.

| Id | Where the lanes differ | Java | Rust | Specification | Status |
|---|---|---|---|---|---|
| D1 | BARRIER request whose Parent Entity ID does not match the scope's registered parent | `PIPESTREAM_SCOPE_INVALID` (0x09): `implementations/java-netty/src/main/java/ai/pipestream/quic/SealedServer.java:492` via `SealedScope.invalid` | `PIPESTREAM_ENTITY_INVALID` (0x05) | Section 6, "Barrier Procedure", step 5: MUST reject with 0x05 (written down in the 2026-10-12 review pass, `1394f284`) | **Resolved 2026-10-29** by `d986abb4`: `SealedServer` now splits the root scope (still 0x09, step 4) from a parent mismatch (0x05, step 5). `SealedTestPeer.assertBarrierProcedure` drives raw BARRIER frames for steps 3, 4 and 5 against the Java server (`SealedServerTest`) and the Rust server (`SealedInteropTest`); both pass. Found during the draft-06 review-fix plan, follow-up 1. |
| D2 | Client journal cursor shape when the SPI journal is bridged onto the pre-existing client journal | Dense cursors through a bridge table in the same SQLite file (`ClientJournalAdapter`, `3b91437c`) | Sparse cursors (intent rowid r → 2r−1, receipt → 2r) under `JournalProfile::SPARSE` (`c64fcb1e`) | Not a wire matter; the SPI contract suite runs each bridge under its declared profile | **Accepted 2026-09-21.** Recorded in `storage-spi-verification.md` ("Asymmetry, documented"). The six certified backends are all dense; the profile exists so adapters over older journals can declare their shape instead of faking density. |
| D3 | MongoDB document shape for scope rows in a database shared by both lanes | Top-level `scope_path`; `listScopes` distinct NPE'd on Rust documents | Compound `_id` | None; the SPI fixes layout bytes for the fs backend only (`test-vectors/storage-spi/`) | **Resolved 2026-09-21** by schema separation: `14938090` (Java: typed distinct, partial unique indexes) and `2f961afb` (Rust: `$type`-filtered distinct). Each lane enumerates only its own schema. Surfaced by run `durable-18d7380f776e6c78`. |
| D4 | Heap measurement available to the neutral driver's resource collectors | `jstat -gc` sample every 1000 ms; heap frozen by `JAVA_MEMORY_FLAGS` before any measurement row | No black-box allocator counter; launched with no equivalent knob | None; the matrix's resource rows state what each collector could see | **Accepted.** `conformance/results/async-neutral-v2/handoff.md`: "the asymmetry is stated, not equalised"; the Rust heap is a NAMED GAP in the collector table. |
| D5 | Determinism of the `g1-declaration-capacity` funding refusal at the capacity boundary | Checkpoint-timing nondeterministic: FixedRecords' per-entity WAL charge (~250 KB, `FixedRecords.java:376-377, 640-667`) against the bounded VFS offset; 3/6 repro iterations committed instead of refusing | Not reported as affected; the finding is recorded against the Java authority only and the Rust side was not measured the same way | Section 12 requires the refusal; it does not fix when the charge is taken | **Open.** Pre-existing version-2 authority behaviour, not introduced by the SPI. The driver was changed to fund the refusal from the refusing attempt (`1abb3f70`) so the row can pass on both; whether the Java charge itself changes is undecided. Recorded at the end of `storage-spi-verification.md`. |

## Interoperability assessment (2026-10-29)

The specification constrains what a peer can observe on the wire and is
silent on everything else. That is the test for each entry: a divergence a
peer can see is a defect in one lane or a hole in the text; a divergence a
peer cannot see is the independence the suite exists to demonstrate.

| Id | Can a peer observe it? | Where the text stands | Consequence |
|---|---|---|---|
| D1 | **Yes.** A requester sending a wrong parent receives 0x09 from Java and 0x05 from Rust, and the two codes call for different recovery: 0x09 ("invalid scope", Section 11) tells a client its scope is unknown or the root; 0x05 ("invalid format or state") tells it the scope is fine and its parent claim is wrong. The procedure uses 0x05 for every mismatch (steps 3, 5, 7) and 0x09 only for a bad scope (step 4), so the text is internally consistent. | Section 6 "Barrier Procedure" step 5: MUST 0x05. The same section states that both reference implementations implement the procedure, which is not true of Java for step 5. | Java fixed (`d986abb4`); text kept, with a rationale clause added to step 5 and the implementation claim moved from Section 6 to Appendix D. Steps 3, 4 and 5 are now tested against both servers over raw frames. They could not become frozen vectors: the index is stateless per frame and the same BARRIER bytes are a valid response and an invalid request. Step 7 (requester-side check) remains covered only by each client's own code path. |
| D2 | **No.** Journal cursors never cross the wire; the spec's only "cursor" is the Layer 0 connection cursor (Section 9). What a peer observes is replay: the same intent, the same identity. | Section 12 names the journal only as obligations (bounded, failures never manufacture an outcome, replay on reopen, reservations charged across restart). | Nothing. The text must stay silent on cursor shape. |
| D3 | **No.** Authorities never share a store in the protocol model; cross-authority references travel on the wire. The collision arose because the harness pointed both subjects at one probe database. | None. The SPI design note promises cross-lane *physical* readability for the filesystem backend only and a byte-identical canonical *report* per backend kind; per-lane Mongo schemas with installation-tagged sweeps satisfy both. | Nothing for the spec. One sentence in `storage-spi-design.md` could say outright that non-filesystem backends are not cross-lane readable and that the installation tag is the non-interference mechanism, the way `fcntl` locks are for files. |
| D4 | **No.** How a harness measures a subject's heap is not protocol. | Section 12 phrases every resource rule in wire terms (bound bytes, time, connections, journals; exhaustion refuses new work, not existing promises). | Nothing. The text must not grow a memory-measurement requirement. |
| D5 | **Partly.** A peer sees whether a declaration is refused with LIMIT_EXCEEDED and left uncommitted, and that existing promises hold; it does not and should not see where the physical boundary falls. | Section 12: "Negotiated limits are ceilings, not an unconditional reservation"; capacity refusal "leaves the admission and job uncommitted"; the LIMIT_EXCEEDED client guidance says the code may be a fixed bound or may clear later. The 256-member bound is a CDDL hard limit (Appendix F), not a storage matter. | The text already says the right things and must not acquire a determinism requirement. The one point to confirm from the row's evidence, not assume: no iteration that committed exceeded the 256 bound (the finding is about WAL space, not the member count). |

Not divergences, listed so they are not re-filed as such: the clippy failures
under rust 1.99 (a toolchain matter, draft-06 plan follow-up 2); the Netty
4.2.19 BOM against the 4.2.17 transport baseline (one lane's dependency
arrangement, README "Reference Suite"); the Section 12.6 versus 12.8
wait-timeout asymmetry (internal to the specification, addressed in WP4).
