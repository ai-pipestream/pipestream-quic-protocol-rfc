# Draft-06 review-fix plan (2026-10-11)

STATUS: EXECUTED 2026-10-12. All six work packages landed on main
(WP1 `03f42f20`, WP2 `1394f284`/`e61ecc98`/`b408dee0`, WP3 `0c0e6dde`,
WP4 `e7468ae0`/`c3d0bd2d`, WP5 `faa46ef1`, WP6 `a72d7546`, gate fixes
`f1b9c5a8`). Final gate: idnits 0 errors/0 flaws/0 warnings (2 pre-existing
comments), test-vectors untouched, cddl/ carries only the claim-id bound,
pipestream-conformance 121/121, drift test green across all inline CDDL
copies. WP3/WP4 were executed by the krick-1 agent and pulled via ssh.

Follow-ups found during execution (not part of this plan):

1. BARRIER parent-mismatch error code diverges: Rust returns
   PIPESTREAM_ENTITY_INVALID (0x05), Java returns PIPESTREAM_SCOPE_INVALID
   (0x09) (SealedServer.java via SealedScope.invalid). The spec now
   documents 0x05; the Java lane needs a one-line code fix plus a
   regression test.
2. Pre-existing clippy errors under rust 1.99
   (chunks_exact_to_as_chunks in durable/scenarios.rs and main.rs) are
   unrelated to this pass; either fix or pin the clippy toolchain.
3. Before this pass the rendered draft artifacts had been stale relative
   to sections-src since 84d1e17e; the final gate now regenerates them.

Baseline: tag `draft05-review-baseline` (main `b80f8775`). Source of truth for
the draft is `sections-src/*.md`; regenerate with `./build.sh core 05`.

Goal: strengthen the draft's credibility without adding scope. Every change is
a clarification, a citation of existing evidence, a stale-claim removal, or a
registry/editorial fix. No new frames, codes, messages, or protocol behavior.

## Ground rules (binding on every work package)

1. Edit only the files your WP owns. The final text for any item shared across
   WPs is fixed in this plan; use it verbatim.
2. No wire changes: frame layouts, message encodings, frozen test vectors
   (`test-vectors/`), and digests stay byte-identical. The single permitted
   schema-text tightening is the `claim-id` bound in WP2 (implementations
   already treat it as 64-bit; verify before editing).
3. Normative wording follows RFC 2119/8174 exactly; do not invent new MUSTs
   beyond what the reference implementations already enforce. When an item
   says "verify against implementations first," read the cited code and write
   what the code does.
4. After your edits: `./build.sh core 05` must succeed and idnits must report
   zero new errors/warnings. Run `git diff --check`. Do not run the long
   conformance suites.
5. Commit on your WP branch with a plain message (no em dashes; a commit hook
   rejects them). Do not merge to main and do not push anywhere.
6. The rendered `draft-krickert-pipestream-05.{xml,txt,html}` are generated
   artifacts: regenerate them as part of your commit so reviewers see the
   result, and so the final gate can diff them.

## Work packages

### WP1 - Front matter and Sections 1-5 (branch `review-fix/wp1-front`, krick)

Files: `sections-src/frontmatter.md`, `abstract.md`, `section-01.md`,
`section-02.md`, `section-03.md`, `section-04.md`, `section-05.md`.

- [MED] frontmatter: the "Individual Group mailing list" sentence is a template
  placeholder; drop the sentence (no real list exists yet).
- [LOW] abstract: split into two paragraphs (version-1 description, then
  version-2); change "The draft records implementation coverage..." to
  "Appendices D and E record implementation coverage...".
- [LOW] 1.1: "PipeStream proposes an application protocol" -> "specifies".
- [MED] 1.2: "video processing" -> "media processing" (no repo evidence for
  video specifically; the "Candidate workloads" hedge stays).
- [MED] 1.5 vs 3.1: in 3.1 change "All implementations MUST support Layer 0"
  to "All version-1 implementations MUST support Layer 0" (mirrors 1.5 and
  Appendix A).
- [HIGH] layering gap: add to the end of 3.1: "At Layer 0, all constituent
  Entities of a composite Entity MUST resolve successfully before rehydration;
  Completion Policies (Section 2.4, Layer 2) relax this requirement." This
  makes the existing 8.4 STRICT default reachable from the layering model.
- [LOW] 2.4 QUORUM entry: cite "(Section 8.4)" for min-success-ratio.
- [LOW] 2.2: the Dehydrate/Rehydrate definitions embed MUST/MUST NOT; the same
  rules are normative in 9.2/9.3. Downgrade the keywords in 2.2 to descriptive
  text with "(see Section 9.2)" / "(see Section 9.3)" pointers.
- [LOW] 2.x: delete the [scatter-gather] book-chapter citation (and its 13.2
  entry); describe the pattern inline in one clause.
- [HIGH] 3.4.2: "the shortest exact representation rule in Section 8.4" is
  ambiguous (RFC 8949 has no such rule; it is THIS document's 8.4). Change to
  "the shortest exact representation rule in Section 8.4 of this document".
- [MED] 3.4.2: "MUST normatively define the encoding of every serialized
  message in this document" -> "...every serialized message defined by that
  specification" (a future format spec cannot redefine this document's v1
  messages).
- [MED] 3.4.1: after the closed-map rule add: "Unknown frame types are handled
  as specified in Section 11.2.1."
- [MED] 3.4 capabilities inline CDDL copy has drifted from Appendix C (which is
  authoritative): the inline copy leaves `max-entities-per-scope` and
  `max-window-size` unbounded while Appendix C bounds them
  (`uint .le 4294967292`, `uint .le 2147483646`). Make the 3.4 copy byte-
  identical to Appendix C and add the shared sentence (exact text):
  "The authoritative definition is Appendix C." Do the same for any other
  inline CDDL copy in your files.
- [LOW] 3.4 intro vs 3.4.2: the "initial Capabilities exchange MUST use the
  default CBOR format" appears twice; keep it in 3.4.2, shorten 3.4's intro to
  a forward reference.
- [LOW] 3.4.2: "one golden byte representation" -> "a single canonical byte
  representation".
- [LOW] 3.4.1/4.3: normalize the ALPN identifier to quoted form "pipestream/1"
  and cite Section 11.1.
- [LOW] 4.3 lifecycle step 2: add "(Section 5.1)".
- [MED] 5.2.2 rules 2 and 5 overlap with different error codes (0x05 vs 0x0D)
  for extra bytes when the header carries payload-length. Add precedence:
  "When the EntityHeader carries a payload-length, rule 2 applies; rule 5
  applies when the payload length is inferred from FIN."
- [MED] 5.1.4 idle-close has no default: add one sentence - "The decision to
  close an idle connection is application policy; this document does not
  mandate closure."
- [MED] 5.1.4 heartbeat table: add a row "Flags word: 0 (E=0, C=0, D=0; see
  Section 6.2.5)".
- [LOW] 5.5 item 2: add a general clause "a declared or admitted payload
  remains outstanding under the negotiated lifecycle" and keep the 9.8 pointer
  as the profile-specific citation.
- [LOW] 5.3: cite RFC 9308 Section 3 alongside the 0-RTT prohibition.
- [LOW] 5.1.3: "A lower value is permissible" -> "permitted".

### WP2 - Sections 6-8, Appendix C, CDDL file (branch `review-fix/wp2-frames`, krick)

Files: `sections-src/section-06.md`, `section-07.md`, `section-08.md`,
`appendix-c.md`, `cddl/pipestream-layer0.cddl`.

- [HIGH] 6.4 BARRIER has a wire format and IANA registration but no semantics.
  Verify against Rust `implementations/rust-quinn/quinn/src/recursive.rs`
  (around line 1147: requires Layer 1, request released bit, parent/scope
  match, response echo) and Java `SealedTransport.java`, then add subsection
  "6.4.1 Barrier Procedure" documenting exactly that behavior: request has
  S=0; the receiver responds with S=1 for the same Scope ID once the parent
  entity's subtree has quiesced; scope/parent mismatch is
  PIPESTREAM_ENTITY_INVALID (0x05); a peer that has not negotiated Layer 1
  MUST reject with PIPESTREAM_LAYER_UNSUPPORTED. Write what the code does.
- [MED] 6.6.3 Claim Check: add "The Extension Length MUST equal 16; a mismatch
  MUST be treated as PIPESTREAM_FRAME_ERROR (0x0D)." (mirrors 6.6.2's rule).
- [MED] 6.6.1: add "The Extension Length MUST equal the number of octets
  remaining in the frame payload." Verify implementations do not chain
  multiple extensions in one frame before writing this.
- [MED] claim-id width: `claim-redemption-frame` says `claim-id: uint .gt 0`
  (unbounded) but the wire space is 64-bit (6.6.3, 10.6.5, Rust `u64`).
  Verify Java also decodes as 64-bit, then change to
  `claim-id: 1..18446744073709551615` in all three copies: 6.7.1 inline,
  Appendix C, and `cddl/pipestream-layer0.cddl`. Add the shared sentence
  "The authoritative definition is Appendix C." to the 6.7.1 inline copy.
- [MED] 6.3 "Entities Deferred": define the counting rule - "a cumulative
  count of entities that entered DEFERRED at least once; this count MAY
  overlap with Entities Succeeded and Entities Failed" - plus one sentence
  that a scope containing a non-terminal DEFERRED entity does not close until
  redemption resolves it (consistent with 6.2.3, 8.4, 9.4).
- [LOW] 6.2.5 item 2: add CHECKPOINT to the authoritative status list; in
  6.2.2 add one line distinguishing status 0x5 (CHECKPOINT) from frame type
  0x81 (Section 9.3).
- [LOW] 6.5/6.5.1 circular-ordering references diverge (9.1 vs 9.3): point
  both at 9.3 and add to 9.3... NOTE: 9.3 lives in WP3's file. WP2 edits only
  the 6.5/6.5.1 citations to read "the circular comparison function is_before
  (Section 9.3)". WP3 owns the 9.3-side sentence; exact text fixed in WP3.
- [LOW] 6.3 stale cross-reference: "the 32-bit identifier defined in Section
  6.2.1" -> cite Section 9.4 for allocation semantics.
- [LOW] 6.2.1: drop the changelog-style sentence "Expanding to 32 bits ensures
  uniqueness across high-frequency workloads" or reword as present tense.
- [LOW] 6.6.2 Yield: add "Yield Tokens SHOULD be at most 4096 octets unless a
  profile states otherwise."
- [LOW] 7.2: "2-bit Data Layer field" is wrong (CBOR map, `layer: uint .le 3`)
  -> "an integer Data Layer field with values 0-3".
- [MED] 7.4: add one sentence: "The file-storage-reference type is currently
  specification-only; no reference implementation emits or parses it on the
  wire (see Appendix D)."
- [MED] 8 Table 13: add "Process modes are application-role labels; no
  protocol field signals them and Core imposes no behavior per mode."
- [LOW] 8.4: add a forward pointer at the top ("completion-policy semantics
  from Section 8.3 apply here").
- [LOW] Appendix C: align alias use (`claim-check` / `claim-redemption-frame`
  use the bounded `claim-id` and `entity-id` aliases), broaden the header
  comment (uint32 also types failure-code and scope ids), and note the
  assembly-manifest's plain uints are a local structure (Section 9.2).
- Gate addition: `cargo test -p pipestream-conformance` must stay green (the
  schema sync test compares Appendix C against the cddl/ file; keep them in
  lockstep).

### WP3 - Sections 9-11 (branch `review-fix/wp3-rehydration-security-iana`, krick-1)

Files: `sections-src/section-09.md`, `section-10.md`, `section-11.md`.

- [HIGH] 9.8.3 seal domain string: the text "pipestream-work-set-v1." reads as
  carrying a trailing period. Both implementations hash the 22-octet string
  with no period (Rust `src/work_set.rs` ~line 207; Java `SealedWork.java`
  ~line 186). Fix: quote it and state it exactly: the 22-octet ASCII domain
  separation string "pipestream-work-set-v1", with no terminator and no
  trailing punctuation.
- [MED] 9.7 vs 10.6.2: 9.7's CDDL makes every stopping-point-validation member
  optional while 10.6.2 says implementations MUST verify state_checksum before
  accepting resumption. In 9.7 add the receiver rule: "A receiver MUST verify
  state-checksum when present; resumption without a state-checksum is
  permitted only when the negotiated lifecycle or application profile
  explicitly allows it, and MUST otherwise be rejected with
  PIPESTREAM_INTEGRITY_ERROR (0x04)." Check the implementations first.
- [MED] 9.7 has no normative text and duplicates the CDDL three times: replace
  9.7's CDDL block with the normative sentence above plus a pointer ("the type
  is defined in Section 6.8.2; the authoritative definition is Appendix C").
- [MED] 9.8.1 session-id charset: Appendix C's work-set-frame allows any
  1..128 tstr, but 9.8.1 requires the 11.6.1 ASCII syntax and 9.8.3 hashes the
  ASCII octets. Add normative text in 9.8.1: "The session-id MUST match the
  session-id production of Section 11.6.1." (CDDL tightening is WP2-free:
  text only.)
- [MED] 10.2.1 vs 3.4.2: a wrong-length checksum - syntax or integrity error?
  Add: "A well-formed message whose checksum member is present but not 32
  octets is rejected with PIPESTREAM_INTEGRITY_ERROR (0x04); a malformed
  encoding is rejected under Section 3.4.2."
- [MED] 11.6 URI scheme: "Status: Permanent" -> "Status: Provisional"
  (individual submission; RFC 7595 practice).
- [MED] 11.2 Table 19: CHECKPOINT description "Global synchronization" ->
  "Scoped completion barrier" (matches the scoped model and Table 10).
- [LOW] 9.1 naming drift: use the hyphenated capability names from 3.4
  (max-window-size, max-entities-per-scope, max-scope-depth) instead of
  underscore forms here and in 10.4.
- [LOW] 9.1 window exhaustion: name the error - a sender exceeding the peer's
  advertised max-window-size is in error; the receiver MUST close with
  PIPESTREAM_WINDOW_EXCEEDED (0x08). Verify against implementations.
- [LOW] 9.1: "the state transition table in Section 6.2" -> Section 6.2.3.
- [LOW] 9.6: "Implementations MUST track ... using a local data structure"
  legislates internals; rephrase as observable behavior ("Endpoints track
  resolution order so that ...") without the data-structure mandate.
- [LOW] Section 9 intro: "Other connections continue to use Sections 9.1
  through 9.7" -> "Sessions not using this profile continue to use Sections
  9.1 through 9.7" (the profile binds the session, not a connection).
- [LOW] 9.8.2: add the numeric code for PIPESTREAM_LIMIT_EXCEEDED (0x06);
  align 10.6.5's client-side invalid-response rejection to name its code.
- [LOW] 9.8.4: expand "a wrong bound is PIPESTREAM_ENTITY_INVALID" to name the
  field (a checkpoint-entity-id that does not equal the inclusive largest
  declared ID).
- [LOW] 10.1: delete the DTLS non-sequitur ("TLS is not a separate record-
  layer wrapper around UDP datagrams.").
- [LOW] 10.3: "MUST enforce all resource limits listed above" -> enforce the
  NEGOTIATED limits (Section 3.4); the Table 17 defaults are identifier-space
  bounds, not memory guidance.
- [LOW] 11.4: qualify "values above 0xFF are reserved..." with "under the
  version-1 mapping (ALPN pipestream/1)" (11.10's 0x200+ mapping is a
  different ALPN/registry).
- [LOW] 11.2.1: add "The 4-octet UCF Length prefix (Section 6.1.1) makes
  skipping possible even for unrecognized fixed-layout types"; replace the
  duplicated normative framing sentence with a pointer to 6.1.1.
- [LOW] 11.8: move the unknown-status-code MUST-reject paragraph to 11.3
  (status codes) - it is not about yield reasons; fix the 6.6.2 reference to
  6.6.2.1.
- [LOW] 11.9: add "(Private Use per RFC 8126; no IANA action)" to the
  65280-65534 policy.
- 9.3-side sentence for WP2's is_before fix (exact text): add to 9.3:
  "The is_before function is the circular comparison used throughout this
  document for cursor and identifier ordering."

### WP4 - Section 12 and Appendix F (branch `review-fix/wp4-v2-contract`, krick-1)

Files: `sections-src/section-12.md`, `appendix-f.md`.

- [HIGH] 12.1: `control-limit` is negotiated ("the minimum of each offer") and
  bounded in Appendix F (4096..1048576) but never defined. Check the Rust and
  Java v2 code for what it actually caps, then add one sentence defining it
  (expected: it bounds a control-frame body, and the initial CAPABILITIES body
  cap of 4096 equals its minimum) - write what the code does.
- [HIGH] 12.4 vs Appendix F: "Each mutation has a nonzero 16-octet operation
  ID" but SESSION create/attach carry no operation ID and no digest rule.
  Verify how the implementations replay session creation (expected: by
  (owner, creation-sequence)), then qualify the sentence accordingly, e.g.:
  "SCOPE and WORK mutations and input admissions carry the operation ID;
  session creation replays by (owner, creation-sequence) and has no wire
  operation ID."
- [MED] 12.6 vs 12.8 wait-timeout asymmetry: add one justifying sentence -
  a WORK view can encode "unchanged" while a scope summary cannot, so the
  checkpoint wait reports WAIT_TIMEOUT.
- [MED] 12.2: state that CONTROL_RESET is delivered as the QUIC application
  close code 0x20E (0x200 plus refusal code 14) when the control stream is
  reset - verify against implementations.
- [MED] 12.5: state the label bound in prose: "1..128 octets of printable
  ASCII without control characters" (the CDDL has .size (1..128); the text
  does not).
- [MED] 12.6 diagnostics: add "nonterminal views other than AWAITING_RETRY
  have null diagnostic."
- [LOW] 12.1 "Other bidirectional streams are forbidden" -> both endpoints
  MUST NOT open them.
- [LOW] 12.2 "a named code" -> "a code from the Section 11.10 registry".
- [LOW] 12.6 "waits up to 30000 ms" -> tie to the Appendix F wait-ms wire
  maximum.
- [LOW] 12.3: add a clause that the "creation receipt" is the durable binding
  record, distinct from the v2-operation-receipt wire type.
- [LOW] 12.3 "v2-limits already advertises" - editorial cleanup.
- Appendix F: add a comment naming which limit caps control bodies
  (control-limit); add v2-diagnostic code semantics ("application-defined;
  a zero code is not success; detail is explanatory only"); add a comment
  that SESSION messages intentionally carry no operation ID.

### WP5 - Appendices A, B, D, E, G (branch `review-fix/wp5-appendices`, krick)

Files: `sections-src/appendix-a.md`, `appendix-b.md`, `appendix-d.md`,
`appendix-e.md`, `appendix-g.md`.

Evidence sources (read them before writing): 
`conformance/results/async-neutral-v2/handoff.md` (66-row neutral matrix,
archive durable-18d5e94dc4306b7a, 64 PASS + 2 WAIVED with named reasons,
g2 host-load flake note), `docs/standards/storage-spi-verification.md`
(fs/postgres/s3/mongo each 64/64), `docs/standards/recovery-execution-java-
acceptance.md`, `docs/standards/section-12-java-traceability.md`,
`docs/standards/protocol-review-corrections-2026-09.md` (2026-09-21
disposition), `benchmarks/durable-transform/` (gRPC comparison),
`REFERENCE_IMPLEMENTATION.md`.

- [HIGH] Appendix D headline is stale ("Java V2 unfinished", "neither endpoint
  advertises the V2 profiles", "gRPC comparison unfinished"). Prepend a dated
  "Status as of 2026-10-11" paragraph with the current state (independent Java
  V2 done; both subjects advertise the v2 durable profiles; matrix green; gRPC
  comparison delivered; four storage planes certified), and mark the older
  chronological increments as historical snapshots with their dates.
- [HIGH] D.4: add a subsection citing the neutral cross-language matrix: 66
  rows, 64 PASS + 2 WAIVED, archive id, the two waiver reasons (no fixture
  clock; no cleanup boundary), and the documented g2 host-load flake. This is
  the RFC 7942 evidence section - it must lead with the strongest evidence.
- [MED] D.1/D.2/D.3: extend the per-implementation coverage bullets with the
  V2 items (Rust v2 listener black-box tests; Java v2 library and endpoint;
  pointer to section-12-java-traceability.md).
- [MED] Appendix E: annotate every item with its disposition, per the
  2026-09-21 disposition doc: E.1 OPEN for v1 (unsealed lifecycle complete-set
  evidence; bidirectional/cancellation); E.2 RESOLVED for v2 (authority-issued
  generations, disjoint producer namespaces); E.3 OPEN (Layer 2 boolean);
  E.4 DELIVERED (independent recovery implementations exist); E.5 DELIVERED
  (matrix groups G1-G8 cover the listed categories; keep the "one-entity
  matrix is not evidence" qualifier); E.6 RESOLVED by Section 12/Appendix F;
  E.7 RESOLVED by the v2 retention rules; E.8 SCOPE_DIGEST count question
  ANSWERED for v2 (v2-counts), minimal-Core question OPEN. Keep the RFC Editor
  Note; the annotations are what it asks for.
- [MED] Appendix E intro: update "remaining successor acceptance work includes
  independent implementations, authenticated failure interoperability and the
  equivalent-workload comparison" - all three delivered.
- [MED] Appendix titles: mark A, B, D, E as "(Informative)" in their titles
  (G already is; F states its own normative scope).
- [MED] B.5: it issues MUSTs in an informative appendix - either move the
  behavior to the referenced body section or downgrade to descriptive text.
  Choose downgrade unless the body already has the rule.
- [LOW] B intro: add a framing sentence that the comparisons use version-1
  terms and apply equally to Section 12's substrate.
- [LOW] B.3: "RFC 9297 defines HTTP Datagrams and the Capsule Protocol" has no
  reference entry - add RFC 9297 to 13.2 (WP5 owns appendix-b.md; the 13.2
  entry lives in references - check which file holds 13.2 and coordinate: if
  it is its own sections-src file, add the entry there too and note it in your
  commit message).
- [LOW] B intro / B.2: add one informative sentence citing the external
  loopback comparison in benchmarks/durable-transform/ (a measurement exists,
  even though the draft presents none).
- [LOW] Appendix A: add under the matrix: "This matrix describes version 1.
  Version 2 (Section 12) defines no layers; it negotiates the profiles in
  Section 11.10."
- [LOW] G.4: genericize the "RestartSafety" API enum mention ("an application
  API label") and optionally note the matrix's g2-* crash rows exercise the
  protocol-side boundaries.

### WP6 - Code: CDDL drift test (branch `review-fix/wp6-schema-drift-test`, krick)

Files: `implementations/rust-quinn/conformance/src/schema.rs` (plus fixtures
if needed).

- Extend the schema sync test so it extracts EVERY inline CDDL copy from
  `sections-src/` (currently it checks Appendix C against
  `cddl/pipestream-layer0.cddl` only) and fails on drift. After WP1/WP2 land,
  all copies must be identical; this test keeps them that way.
- `cargo test -p pipestream-conformance` must pass. TMPDIR under ~/.rfc-tmp,
  never /tmp.

### WP7 - Final gate (coordinator, after all merges)

- Merge WP1-WP6 into main sequentially.
- `./build.sh core 05`: idnits zero NEW errors/warnings vs the baseline.
- `git diff draft05-review-baseline -- test-vectors/ cddl/` must show ONLY
  the WP2 claim-id bound.
- `cargo test -p pipestream-conformance` green on merged main.
- `git diff --check` clean.

## Machine assignment and coordination

- krick (coordinator session): WP2 and WP5 first (parallel subagents), then
  WP1 and WP6, then WP7.
- krick-1 (kimi web agent): WP3 and WP4, one branch each, worked in its own
  clone at /work/main/pipestream-ai/dev-tools/pipestream-quic-protocol-rfc.
  It fetches main from krick first
  (`git fetch ssh://krick/work/main/pipestream-ai/dev-tools/pipestream-quic-protocol-rfc main`).
  krick pulls its branches back the same way. No pushes to forgejo/origin.
- Max 2 concurrent agents per machine.
