# Version-2 frozen examples

These examples accompany Section 12 and Appendix F. They are not evidence that
an endpoint implements version 2. The 70 rows in `wire.tsv` are independent
examples, not one sequential session transcript. Each names an exact CDDL root,
framing, schema classification, expected codec result, named refusal, SHA-256
and immutable hexadecimal bytes.

- `control:NN` includes the one-octet type and four-octet body length.
- `input-header` and `result-header` include the four-octet header length but
  no following object bytes. The success examples commit input `abc` and
  output `ABC`.
- `record` is the CBOR record itself, without a stream prefix.

CDDL validation and protocol validity are separate. A `valid` CDDL row can
still require refusal for a noncanonical integer, duplicate profile IDs,
inconsistent counts, forbidden identity characters or another semantic rule.
An `invalid` row must fail its exact schema root. `skip` is used for trailing
CBOR that must be rejected by a strict wire decoder before schema validation.

`commitments.tsv` freezes 12 domain-separated hash inputs and their expected
SHA-256 values. Hash input is the ASCII domain immediately followed by the
listed octets, without a separator or additional framing. These cover operation
identity, scope seals, a result manifest, status leaves, an empty root and a
two-leaf internal node.

The bytes were authored with a one-off, independent minimal JavaScript CBOR
encoder, not a protocol implementation codec. That authoring tool is not in the
normal test path. No test regenerates or rewrites the corpus. The Rust process
driver checks hashes, framing, exact roots, and Appendix F synchronization, then
uses the pinned CDDL library with CBOR-only decoding and no JSON fallback.

Semantic and canonical refusal expectations still require independent Rust and
Java codec tests and authenticated transport/state scenarios. Passing the current
schema checks does not establish those refusals or version-2 interoperability.
