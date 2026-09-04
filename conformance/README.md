# Layer 0 conformance

The suite has two independent levels of evidence.

`test-vectors/index.tsv` inventories language-neutral binary inputs. Each row
names the parser class, expected acceptance or refusal, exact error name,
SHA-256 digest, and octet count. The Python, Java, Rust, and C++ codecs each
consume the entire corpus. Regeneration is explicit; normal validation uses
`generate_vectors.py --check` so a test cannot silently rewrite its oracle.
`validate_cddl.py` also extracts serialized messages from selected vectors and
proves that the normative CDDL accepts valid instances and refuses the schema
violations assigned to it.

`run_interop.py` is a black-box process runner. It generates a temporary CA and
server certificate, starts each standalone server on an ephemeral UDP port,
and invokes every standalone client against it. A pair passes only when the
processes complete the Layer 0 state machine and the runner observes byte-exact
payload and parent identity on disk. The current matrix is 3 clients by 3
servers, or nine pairings.

## Standalone command contract

Every executable provides equivalent commands:

```text
IMPLEMENTATION serve \
  --bind HOST:PORT --cert SERVER_CERT --key SERVER_KEY \
  --output-dir DIRECTORY --ready-file FILE [--once]

IMPLEMENTATION send \
  --connect HOST:PORT --ca CA_CERT --server-name DNS_NAME \
  --entity-id ID [--parent-id ID] --input FILE \
  [--content-type MEDIA_TYPE]
```

The server writes `ID.bin` only after validating the complete Entity Stream. If
`parent-id` is present, it also writes `ID.parent`. `--ready-file` is the
readiness boundary used by tests; the runner never treats a merely spawned
process as ready.

## Running the suite

```bash
./conformance/run_all.sh
```

Use `python3 conformance/run_interop.py --build` when only the implementations
and all black-box pairings are needed. Test certificates are generated in a
temporary directory and are never production credentials.
