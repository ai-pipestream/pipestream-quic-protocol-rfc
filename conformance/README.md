# Conformance and interoperability

The suite has three independent levels of evidence. Python is not a reference
implementation, a vector generator, or a conformance oracle in this repository.

`test-vectors/index.tsv` inventories language-neutral binary inputs. Each row
names the parser class, expected acceptance or refusal, exact error name,
SHA-256 digest, and octet count. The Java, Rust, and C++ codecs each consume the
corpus using independent protocol code. The checked-in bytes are frozen; the
normal test path cannot regenerate or rewrite them.

`test-vectors/cddl/index.tsv` contains separate frozen, hexadecimal CBOR
instances for schema validation. The Rust driver decodes hexadecimal text to
temporary files and gives those files directly to the CDDL validator. It does
not extract messages from PipeStream frames or otherwise parse protocol bytes.

`pipestream-conformance` is a protocol-neutral Rust process driver. It has no
dependency on `pipestream-core` or either transport library. It generates
temporary test certificates, starts each compiled server on an ephemeral UDP
port, and invokes every compiled client against it. A pair passes only when the
processes complete their own state machines and the driver observes byte-exact
payload and parent identity on disk. The current Layer 0 matrix is 3 clients by
3 servers, or nine pairings. The same driver launches the external examples;
the examples contain their own application behavior.

The `verify` command also refuses checked-in `.py`, `.pyi`, or `.pyx` sources
outside ignored build and dependency directories. This prevents a script codec
or vector generator from silently reappearing in the reference suite.

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

For focused checks after the Rust workspace has been built:

```bash
implementations/rust-quinn/target/release/pipestream-conformance verify
implementations/rust-quinn/target/release/pipestream-conformance interop
implementations/rust-quinn/target/release/pipestream-conformance recursive
implementations/rust-quinn/target/release/pipestream-conformance examples
```

Test certificates are generated in a temporary directory and are never
production credentials.
