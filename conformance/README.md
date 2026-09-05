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
dependency on `pipestream-core` or the PipeStream transport libraries. It generates
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

Rust execution tests also verify durable acquisition/publication fences,
reopen and stale-epoch refusal, transactional rollback, callback database
re-entry over QUIC, callback expiry, and revocation during processing. They
do not claim bounded asynchronous dispatch or full crash-boundary coverage.

The Rust `spool_resources` test runs in its own test binary with an instrumented
allocator. It sends 32 MiB over real QUIC from fixed-size input blocks, checks
heap growth below 12 MiB and individual allocations below 4 MiB, verifies the
persisted SHA-256, and requires temporary disk credit to return to zero. Other
spool tests cover per-principal/global limits, zero-byte file exhaustion,
cancelled I/O, handle reopening, abandoned files, and chunk corruption. The
measurement excludes native allocations and the filesystem cache; it is not
a total RSS bound or a concurrent-workload benchmark.

```bash
./conformance/run_all.sh
```

For focused checks after the Rust workspace has been built:

```bash
implementations/rust-quinn/target/release/pipestream-conformance verify
implementations/rust-quinn/target/release/pipestream-conformance interop
implementations/rust-quinn/target/release/pipestream-conformance extensions
implementations/rust-quinn/target/release/pipestream-conformance recursive
implementations/rust-quinn/target/release/pipestream-conformance examples
```

Test certificates are generated in a temporary directory and are never
production credentials.

## Draft-04 regression evidence

`quinn/tests/draft04_wire.rs` uses raw QUIC peers for reordered and stalled
streams, optional PENDING, unknown frames, pending checkpoints, timeout,
negotiated depth and layer refusals, and incorrect ACK identity.
`conformance/src/schema.rs` compares every shared machine-readable CDDL
definition to Appendix C after normalizing comments and formatting.
`conformance/src/receipts.rs` independently computes expected local exemplar
receipts from the scenario inputs; a file containing any 32 octets no longer
passes the recursive checks. It does not encode or decode protocol frames.

These checks strengthen evidence for the tested subset. They do not establish
complete protocol conformance. The full list of open work is in
[draft-04 readiness](../docs/standards/draft04-readiness.md).

## Extension negotiation

`test-vectors/extension-negotiation.tsv` freezes 35 codec/negotiation cases
consumed by Rust, Java, and C++. The positive cases select synthetic test
identifiers; none is advertised by the shipped services. The additional
`test-vectors/cddl/extensions.tsv` fixtures check the schema's list size,
identifier range, and type constraints. Ordering and subset constraints
are tested in the codecs, not claimed as CDDL validation.

The `extensions` command sends frozen CAPABILITIES bodies over raw Quinn
connections to all three standalone servers and the Rust recursive server.
Its 32 probes check exact response bytes, named QUIC refusal codes, and no
entity storage after server exit, including an invalid offer followed by
a valid offer, PENDING and a submitted Entity Stream. It also presents
invalid responses to all four client paths and
checks that they fail before sending an Entity Stream. The probe parses
only UCF type/length framing and does not use a PipeStream implementation
to decide expected semantics. No production extension is enabled to make
these tests pass.

## Sealed-work profile evidence

`test-vectors/work-sets.tsv` adds 20 frozen UCF inputs for the Rust
`sealed-work-sets-v1` codec. The CBOR was constructed independently of the
production codec; the root-seal SHA-256 was calculated separately from the
specified concatenation using Node's crypto library. The test path only
reads these files and compares exact encoding and the seal digest.
`test-vectors/cddl/work-sets.tsv` tests the same message shapes. Sorted IDs,
paired parents, flag/digest agreement, and other semantic constraints are
checked by the Rust codec, not claimed as CDDL validation.

`quinn/tests/draft04/sealed_work.rs` tests missing declared payloads, missing
seals, out-of-order descendants, invalid admission, late declarations, early
GOAWAY, declaration ACK correlation, and public-client replay after a server
restart with an unobserved ACK. Core tests also reopen the SQLite WAL store,
pin immutable state on refusal, check the maximum entity ID, and refuse old
session-format records without conversion.

The independent Java `SealedInteropTest`, enabled through Maven's
`sealed-interop` profile, runs the public Netty producer against the compiled
Rust server over real QUIC. It covers nested completion, out-of-order chunks,
scoped cuts, restart/replay, discarded declaration ACKs, retained-limit and
ownership-label refusals, and checkpoint timeouts. Scripted fault-injection
transports also verify changed ACK, downgrade, oversized-frame, and Layer 2
refusals. The full suite explicitly enables this profile after building Rust;
a missing Rust executable fails the tests.

The Java listener/CLI and C++ endpoints still refuse the required sealed
extension. The existing nine-pair Layer 0 matrix is not sealed-work evidence,
and Rust-to-Java tests still require an independent sealed Java server. These
tests imply neither authenticated recovery nor bidirectional producer support.

## Authenticated-session evidence

The Rust-only `quinn/tests/draft04/authenticated_sessions.rs` tests actual
mutual TLS, required session-profile negotiation, missing/untrusted/expired/
unmapped certificates, principal and authority binding, anonymous-listener
bypass refusal, certificate rotation, live/reconnected revocation, and
background recovery authorization. Core tests reopen ownership records and
refuse old stored formats without modifying them. This prerequisite is not
the retained-outcome recovery or asynchronous execution implementation; those
remain in the [active goal plan](../docs/standards/recovery-execution-java-plan.md).
