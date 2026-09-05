# Submission checklist: draft-krickert-pipestream-04

Prepared 2026-09-05. This is a revision for technical review, not a claim that
the design is complete or that the IETF has adopted it.

## Completed locally

- [x] Review the previously published implementation claims against code.
- [x] Address the reproduced codec, wire-ordering, and recovery failures.
- [x] Document remaining implementation and protocol design gaps explicitly.
- [x] Run the local cross-language conformance and example suite.
- [x] Build XML, text, and HTML with `./build.sh core 04`.
- [x] Check idnits: zero errors, flaws, or warnings; one FIPS 180-4 comment.
- [x] Confirm IANA section numbering and Appendix A through E in rendered text.
- [x] Compare shared CDDL definitions to Appendix C, not only to fixtures.
- [x] Define supported/required extension negotiation and its proposed registry,
      with shared independent-codec cases and actual QUIC refusal tests.

## Author confirmation and submission

- [ ] Review the generated `draft-krickert-pipestream-04.html`, particularly
      the new security requirements, URI syntax, and Appendix E open issues.
- [ ] Confirm the author contact details and submission date.
- [ ] Review the Datatracker comparison against the published -03; the local
      -04 source previously existed without having been submitted.
- [ ] Upload `draft-krickert-pipestream-04.xml` at the
      [Datatracker submission page](https://datatracker.ietf.org/submit/).
- [ ] Complete any author confirmation required by Datatracker and verify
      the new revision appears on the public document page.

Generated files are ignored build artifacts; canonical source remains in
`sections-src/`. Rebuild if the source or submission date changes. Do not
change the filename alone to advance the draft revision.

No mailing-list announcement, chair contact, draft submission, or IANA
registration is implied by merging this repository. Before requesting
adoption, resolve Appendix E's identity, work-set closure, and authenticated
profile choices and obtain review from independent implementers. Two or three
languages maintained by one project do not constitute IETF approval.
