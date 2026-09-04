# PipeStream Layer 0 test vectors

The binary files in `valid/` and `invalid/` are language-neutral wire inputs.
`index.tsv` records their type, expected outcome, digest, and exact length.

Regenerate them with:

```bash
python3 conformance/generate_vectors.py
```

CI and implementations must use the non-mutating checks:

```bash
python3 conformance/generate_vectors.py --check
python3 conformance/verify_vectors.py
```

The vectors are protocol evidence, not serialized internal state. Implementations
must parse them using their own codec and state machine.

The current corpus covers deterministic CBOR, capability bounds, entity and
parent identifiers, exact payload length, SHA-256 integrity, Layer 0 status and
cursor rules, heartbeat handling, CHECKPOINT request/acknowledgement flags,
GOAWAY reserved-field tolerance, UCF framing, and named protocol errors.
