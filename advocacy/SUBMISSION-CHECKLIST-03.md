# Submission Checklist: draft-krickert-pipestream-03

## Before building

- [ ] Review the full working-tree diff (`git diff` from 924589f) — all -03
      changes are uncommitted until you approve them.
- [ ] Verify the Implementation Status appendix (sections-src/appendix-d.md)
      matches reality — update Maturity/Coverage/Licensing or remove the
      Reference Implementation entry if premature.
- [ ] Confirm date in sections-src/frontmatter.md is the submission date.

## Build (local; toolchain not available in the agent environment)

- [ ] `kdrfc draft-template.md`
- [ ] `mv draft-template.xml draft-krickert-pipestream-03.xml`
- [ ] `xml2rfc draft-krickert-pipestream-03.xml --text --html`
- [ ] `idnits --verbose draft-krickert-pipestream-03.txt` — zero errors;
      pay attention to: unused references, non-ASCII characters, lines
      over 72 chars in ascii-art blocks.

## Spot-checks in the rendered text (things a build can't catch)

- [ ] Section numbers: IANA subsections must land as 11.1 ALPN … 11.7
      Designated Expert guidance (hardcoded cross-refs depend on this).
- [ ] Appendices render as A (Reference Algorithms), B (Relationship to
      Existing Protocols — now includes MOQT + broker sections), C (CDDL),
      D (Implementation Status).
- [ ] Zero occurrences of "protobuf"/"Protocol Buffers" in the rendered txt.
- [ ] {{RFC3986}} and {{RFC7942}} resolve in the references sections
      (RFC3986 normative, RFC7942 informative).
- [ ] CDDL blocks: run the consolidated Appendix C schema through the
      `cddl` tool (`gem install cddl; cddl schema.cddl generate`) if
      available — reviewers do.

## Submit

- [ ] Upload the .xml at <https://datatracker.ietf.org/submit/>.
- [ ] Confirm the Datatracker diff vs -02 shows what you expect (it
      renders an automatic diff — skim it once).

## docproc profile (separate submission, when ready)

- [ ] Same build/idnits cycle from docproc/draft-template.md.
- [ ] Note: its Appendix B (Protobuf) was removed; appendices are now
      A (CDDL) and B (Example Processing Patterns); body field names are
      kebab-case matching the CDDL.
- [ ] Decide its draft name before first submission
      (e.g., draft-krickert-pipestream-docproc-00).

## After submission (from DISPATCH-KIT.md)

- [ ] Email DISPATCH chairs for IETF 127 agenda time (September 2026).
- [ ] Post introduction thread to dispatch@ietf.org.
