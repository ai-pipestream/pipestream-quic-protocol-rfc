# PipeStream test vectors

The binary files in `valid/` and `invalid/` are language-neutral wire inputs.
`index.tsv` records their type, expected outcome, digest, and exact length.

These bytes are frozen review artifacts. Normal tests never generate or modify
them. Adding or changing a vector requires the same review as a normative wire
change: update the specification or CDDL first, add the exact bytes, and then
update `index.tsv` with an independently calculated SHA-256 and octet count.
Each implementation must parse the resulting bytes with its own codec.

`cddl/index.tsv` is a separate set of frozen hexadecimal CBOR instances. The
schema validator consumes these instances directly. They are deliberately not
derived from framed wire vectors during a test, because a shared extractor
would become another protocol implementation and weaken the evidence.

Run the non-mutating corpus and CDDL checks with:

```bash
cargo run --release --locked \
  --manifest-path implementations/rust-quinn/Cargo.toml \
  -p pipestream-conformance -- verify
```

The vectors are protocol evidence, not serialized internal state.

`optional-fields.tsv` contains independently specified hexadecimal maps
covering omitted defaults, richer capability maps, non-minimal integers,
duplicate keys, key ordering, and invalid UTF-8. All three codecs consume it.
The -04 review corrected the recursive quorum fixture from binary32 0.75
to its required binary16 encoding and reduced its header length accordingly.
Tests do not regenerate either corpus.

The current corpus covers deterministic CBOR, capability bounds, entity and
parent identifiers, exact payload length, SHA-256 integrity, Layer 0 status and
cursor rules, heartbeat handling, CHECKPOINT request/acknowledgement flags,
GOAWAY reserved-field tolerance, UCF framing, and named protocol errors.
