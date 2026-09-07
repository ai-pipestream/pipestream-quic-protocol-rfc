# Rust V2 command guide

The Unix `pipestream-quinn v2` commands run the durable authority and client over
authenticated QUIC, ALPN `pipestream/2`. They use the same bounded runtime,
durable journal and file adapters as the library. No Python runner, JSON command
protocol, anonymous durable fallback or locator-derived trust is involved.
The original V1 commands remain separate.

From `implementations/rust-quinn`:

```bash
cargo build --release --locked -p pipestream-server
target/release/pipestream-quinn v2 --help
```

## Trusted configuration and authority history

Provision a server certificate/key, a client certificate/key, and explicit trust
roots. The server certificate must cover the client's configured server name;
client certificates must support client authentication. Keep the containing
directories trusted and stable. Startup configuration must be regular files,
not final-component symlinks or FIFOs. The private key limit is 64 KiB; certificate
PEM input is limited to 128 KiB, at most 16 certificates and 65,535 decoded bytes.

The principal map is UTF-8 TSV, with literal tabs and this header:

```text
sha256	principal
```

Each following row contains the 64 hexadecimal digits of a client **leaf DER**
certificate's SHA-256 and its stable owner label. An operator can inspect the
fingerprint with `openssl x509 -in client.pem -outform DER | openssl dgst -sha256`.
Do not hash the PEM text. The map permits at most 4,096 entries and 1 MiB; duplicate
fingerprints, empty maps and invalid labels fail startup. Multiple certificates
may map to the same owner for rotation. Mapped owners have the supported session,
inspection, admission, cancellation and retry permissions; skip additionally
requires `--allow-skip`. This reference policy is not a general role-management API.

The following Bash arrays are conveniences for passing explicit configuration.
Replace paths, labels, and addresses with your provisioned values. No command
generates or installs credentials implicitly.

```bash
cli=(target/release/pipestream-quinn v2)
authority_args=(
  --state-db /srv/pipestream/authority.sqlite
  --object-dir /srv/pipestream/objects
  --authority issuer-a
  --principal-map /etc/pipestream/principals.tsv
  --trust-system-clock
)
"${cli[@]}" init-authority "${authority_args[@]}"
"${cli[@]}" serve "${authority_args[@]}" \
  --bind 127.0.0.1:7443 \
  --cert /etc/pipestream/server.pem --key /etc/pipestream/server.key \
  --client-ca /etc/pipestream/client-ca.pem \
  --result-authority localhost:7443
```

Run initialization once, then use `serve` to reopen existing history. Missing,
incompatible or existing-at-initialization state is an error, not permission to
replace it. Database and object root are paired. If initialization fails partway,
retain both paths for diagnosis; do not delete or substitute one side to make
startup succeed. Do not restore stale authority history under the same identity.

`--trust-system-clock` is an explicit operator assertion that system UTC is
trustworthy across restart. Forward jumps count as elapsed deadline/retention
time. The persisted clock-regression guard still applies; a monotonic process
timer cannot establish trusted UTC after restart.

Optional `--ready-file PATH` creates a new mode-0600 file containing the bound
socket address. An existing marker fails startup. Use a new path each run; a
marker alone does not prove the process is still alive. SIGTERM or SIGINT requests
shutdown. `DRAINED` and exit zero confirm local owners drained, **not** that all
durable work succeeded. Unfinished work remains in authority history.

## Original client identity and operations

In another shell, define `cli` as above and configure the caller:

```bash
connection_args=(
  --connect 127.0.0.1:7443 --server-name localhost
  --ca /etc/pipestream/server-ca.pem
  --cert /etc/pipestream/client.pem --key /etc/pipestream/client.key
)
"${cli[@]}" next-sequence "${connection_args[@]}"
```

This authenticated query prints `NEXT_SEQUENCE N` without allocating a session.
Use that exact sequence for a genuinely new session, coordinating creation with
other clients of this owner. For example, if it reports 1:

```bash
journal_args=(
  --journal /srv/client/session-1.sqlite
  --authority issuer-a --owner alice --creation-sequence 1
)
"${cli[@]}" init-client "${journal_args[@]}"
client=("${cli[@]}" client "${journal_args[@]}" "${connection_args[@]}")
"${client[@]}" binding
```

`init-client` persists original creation intent locally. Every `client` invocation
**reopens** that journal, replays original creation if necessary, or attaches to
its saved binding. It does not invent another session after connection loss.
The authority, owner, sequence, policy durations and selected profile combination
must match on every reopen. Defaults select durable work plus results, with a
60,000 ms execution ceiling, 3,600,000 ms output retention and 86,400,000 ms receipt
retention. Use `--no-results` at initialization and on every reopen for durable
work only. It cannot later be upgraded silently to result delivery.

Operation IDs below are illustrative nonzero 16-byte values, written as 32 hex
digits. Allocate a distinct ID for each new logical mutation within the session;
recovery uses its original ID and immutable arguments. Work keys use decimal
`scope:producer:entity`; root caller work is `0:0:1`.

```bash
"${client[@]}" declare --operation 01010101010101010101010101010101 \
  --entities 1 --seal
"${client[@]}" admit --operation 02020202020202020202020202020202 \
  --declaration 01010101010101010101010101010101 --work 0:0:1 \
  --input /srv/client/input.bin --application copy/v2
"${client[@]}" lookup --operation 02020202020202020202020202020202
"${client[@]}" watch --work 0:0:1
```

Admission prehashes the same open file it streams. Do not modify it during
transfer. `lookup` recovers a retained receipt without the original local file.
`replay --operation ID` resends the exact saved mutation; admission replay also
requires `--input PATH --declaration ORIGINAL_DECLARATION_ID`. Missing or changed
input is an error, not permission to re-embed, regenerate or submit replacement work.
`unresolved --after N --limit N` lists locally uncertain original operations.

`watch --after REVISION --wait-ms N` requests a bounded observation, not an
automatic polling loop. An unchanged view does not mean completion. Once work
has succeeded, use its actual producing attempt:

```bash
"${client[@]}" select --work 0:0:1 --attempt 1 --index 0
"${client[@]}" read --work 0:0:1 --attempt 1 --index 0 \
  --output /srv/client/output.bin
```

`select` fetches and saves the verified manifest and explicit output index.
`read` uses that saved selection, configured endpoint and caller credentials.
It installs only after length, SHA-256 and FIN verification, synchronizes the
file/directory, and never overwrites an existing destination. `VERIFIED` describes
that transfer; diagnostic stdout is not a stable wire format or portable proof.

## Branch applications and closure

Application names are explicit reference contracts, not protocol profiles:

- `copy/v2`, mode 0: one byte-exact streamed output.
- `consume/v2`, mode 0: verify/consume input, no output. Request `--output-count 0`;
  this works with durable work only.
- `retry-copy/v2`, mode 0: attempt 1 requests a caller-authorized retry; subsequent
  attempts copy. It never advances its own attempt. This is a failure exercise.
- `reassemble/v2`, mode 1: caller supplies children under the allocated child
  scope, producer 0. Concatenate their output index 0 in increasing entity order.
  The parent input is the expected complete object; length/hash must match it.
- `chunk-copy/v2`, mode 2: the authority declares/seals producer-1 children,
  splitting input into 65,536-byte chunks, at most 256 (16 MiB). Empty input has
  an empty sealed child scope. Children copy; the parent verifies reassembly.
  Larger input, if transport permits it, fails the application rather than
  yielding indefinitely. Capacity yields replay the same child identities.

Admit branch work with the matching explicit `--mode`. Unknown applications or
unsupported modes do not fall back to copy. Output budgets are immutable:
`--output-count` defaults to 1 and `--output-bytes` defaults to input length
(zero when count is zero). Each child's execution duration is copied from the
original parent request; this does not extend the parent's fixed deadline.

`page --scope S --after ENTITY --limit N` retains a membership page. Continue
from the last listed entity until the complete sealed membership is verified;
an empty page or a seal alone is insufficient. `watch` each member to save its
terminal evidence. For branches, collect descendants and checkpoint bottom-up
before checkpointing the parent. Use the exact printed 64-hex-digit seal:

```bash
"${client[@]}" page --scope 0
# Substitute the actual seal from that page, after all terminal evidence is saved.
"${client[@]}" checkpoint --scope 0 --seal ACTUAL_SEAL
"${client[@]}" complete
```

`complete` uses saved exact root coverage. Missing evidence remains NOT_READY.
`detach` cuts only the connection; it does not assert durable completion.
`retry --operation ID --work KEY --expected-attempt N` requires an authoritative
retryable outcome and explicit authorization. `cancel`, `skip`, and `cancel-scope`
are journaled mutations with their own original operation IDs. Terminal outcomes
are not rewritten by a later cancellation.

An operator can revoke a generation offline, **after stopping its listener**:

```bash
"${cli[@]}" revoke "${authority_args[@]}" --owner alice --generation 1
```

## Limits and remaining work

Defaults include 16 MiB per transferred object, 1,024 sessions (at most 64 per
owner), 64 active jobs globally and 16 per owner/session. Authority session limits
are 4,096 scopes, 1,000,000 entities/operations, and 1 GiB each retained input and
output. The payload store defaults to 10,000 objects/8 GiB, 8 KiB chunks,
256 handles globally and 64 per owner. `--sessions`, `--payload-objects`, and
`--payload-bytes` configure persisted limits; reopen must match. Set
`--object-limit` on both client and server when changing transport admission.

Client journals default to 4,096 operations and 4,096 observations. Authority and
client SQLite limits are 256 MiB database, 64 MiB WAL, 64 MiB rollback journal and
512 KiB shared memory per store. These are file-length/count bounds, not measured
whole-process heap/RSS or allocated filesystem blocks.

Client downloads currently require a trusted, stable destination directory.
Ordinary failure cleans its own staging, but process death can leave
`.pipestream-result-*` files. The separate library
[`ManagedResults`](../README.md#version-2-managed-local-result-copies) now supplies
exclusive result-root ownership, shared disk quotas and restart cleanup, but
these CLI commands do not yet use it. Do not delete files by
prefix while another transfer may own them. A download's ambiguous local outcome
does not authorize another remote execution.

The subprocess tests in `server/tests/v2_cli.rs` exercise the Rust implementation.
They do not replace independent Java V2, the neutral cross-language failure driver,
measured resource gates, or the external transform/reassemble workload and
equivalent authenticated, durable streaming-gRPC baseline. Those remain part of
the full [goal](../../../docs/standards/durable-work-results-goal.md).
