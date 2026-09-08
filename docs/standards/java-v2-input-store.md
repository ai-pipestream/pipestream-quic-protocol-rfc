# Java V2 immutable input storage

`v2.InputStore` implements bounded, immutable reception for Sections 12.5 and
12.9 using Java file channels and the independent V2 object verifier. It does
not admit work, issue an operation receipt, authenticate a caller or create a
job. The Java endpoint still advertises Core only. The authority must validate
the header and authorization before starting reception, then atomically commit
its complete funded admission after the immutable bytes have been installed.

## Installation and recovery

`initialize` requires a new directory under an existing parent. `open` requires
an existing complete installation with exactly the retained policy. Neither
adopts a foreign directory nor converts an older payload format. The new root
has owner-only permissions; its files have owner read/write permissions. This
implementation requires a local filesystem supporting POSIX permissions,
hard links, file/directory synchronization and cooperative file locks.

One process owns the directory through `writer.lock`. An additional process-local
canonical-path registry refuses another handle before opening a competing lock
channel: closing that channel could otherwise affect the first handle's POSIX
lock. Closing the store refuses while any receiver or reader remains active.
The store is not a multi-host shared-filesystem authority.

`policy.cbor` format 3 retains a fresh local installation identity, an optional
immutable database-installation identity, file-byte, file-name, per-object and
active-handle limits, and a checksum. These identities do not replace protocol
non-reuse rules or confer authorization. Earlier private policies are refused,
not converted; the object envelope and wire format are unchanged.

`initializeForAuthority` creates a root for one already initialized database
UUID. `SessionStore.bindInputs` then commits its exact input-store UUID as the
reverse binding. `verifyInputs` requires the committed pair and never binds
implicitly. Both operations hold the input owner's monitor through the database
transaction and verify/synchronize the retained policy. Setup may resume after
input-root initialization or replay after the database commit, but cannot adopt
another empty root or an independently initialized database with the same
protocol authority name. See the [paired-store contract](java-v2-authority-store.md#paired-input-store-ownership).

The ordinary `initialize` API creates standalone, unbound storage for independent
file-store use and tests. Its immutable policy cannot later be adopted by an
authority. Pairing creates no protocol operation or job; admission must recheck
the pair in the committing writer transaction, not cache a prior setup check.

Recovery streams directory entries and input bytes with bounded buffers. It
checks the complete layout, policy, object identities, checksums, lengths and
accounting before removing abandoned `pending` names. Installed objects remain
charged, including objects left unreferenced by an interrupted future admission.
No object is reclaimed merely because its age or filename suggests it is stale.
Reclamation of installed objects requires the future authority's transactional
liveness proof, read/dependency pins and replayable deletion accounting.

## Incremental reception and immutable installation

After validating the header, the caller invokes `begin` with the authenticated
context and negotiated object/timer limits. Context generation must match the
header. The storage primitive supports either local producer namespace but
grants neither producer authorization. The external dispatcher must reject
producer-1 input streams as required by the protocol.

Reception reserves the complete private header and declared payload length
twice, and two file names, before creating a temporary file. This conservatively
funds both names during hard-link installation. Empty inputs consume bounded
header space and file-name capacity too. These are file-length/name limits,
not preallocated filesystem blocks or a guarantee that the device cannot fail.

`write` hashes incrementally and writes the supplied buffer without retaining a
payload-sized array. `checkDeadline` permits the dispatcher to enforce idle and
absolute lifetime limits even when no further bytes arrive. `finish` must be
called only for actual FIN: receiving the declared byte count alone is not proof
of completion. Truncation, trailing bytes and digest disagreement are
INTEGRITY_ERROR; no installed input is returned. Abort discards only temporary
reception, never a declaration or authoritative outcome.

The private object contains an eight-byte format marker, four-byte header
length, bounded deterministic-CBOR identity/header, its SHA-256 checksum and
the exact payload. The identity includes the installation, authority, owner,
generation and complete immutable input header. The object filename is the
hexadecimal SHA-256 of that bounded metadata; no caller label becomes a path.
Input content type remains part of the header commitment. This private format
does not change Appendix F wire encoding.

Successful FIN forces the complete file before creating an immutable hard-link
name under `objects`. Installation never overwrites an existing name. An
existing name must have the exact identity and valid complete bytes. The object
directory is synchronized before the temporary name is removed; temporary-name
capacity is refunded only after that deletion is synchronized. File visibility
alone is not durable installation evidence after an interrupted sync.

Lookups verify the whole retained body with a fixed-size buffer and repeat file
force and object-directory sync before returning. This completes a possible
earlier interrupted installation; readable bytes alone are insufficient.
Returning an installed object does not resolve whether its operation was admitted; the
authority must use its operation history. Readers verify the same file
descriptor before exposing payload-only bytes and count against the handle
limit. They must be opened only after current execution/read authorization.

## Durable output funding

`reserveOutputs(context, header)` installs an immutable checksummed record under
`reservations`, keyed by the store/database identities, owner-qualified context,
admission producer and operation ID. Its content commits the full input header.
Changed parameters under that identity are CONFLICT; exact replay verifies and
forces the record and directory again without adding another charge.

For output count `n`, total payload budget `b` and funding record length `r`,
the retained byte charge is `r + 2*b + 2*n*8236` and the name charge is `1 + 2*n`.
The 8,236-byte per-object allowance covers the bounded private header, prefix and
checksum. The two copies/names cover pending and installed output simultaneously.
Installing funding additionally reserves one temporary record of length `r` and
one name, released only after synchronized staging cleanup. Even a zero-output
admission has a durable funding identity. Arithmetic or quota overflow refuses
before creating a record. There is no payload-sized allocation.

These allowances participate in ordinary input capacity checks and survive
restart. Recovery validates all funding records and reconstructs future charges
before removing abandoned staging files. A funding record linked before a crash
remains charged even when no database job references it. A failed installation
sync cannot refund its future allowances; exact replay re-establishes durability.
No funding release or output writer is implemented here yet. The future result
writer must consume these prepaid allowances and enforce their descriptor bounds,
not add an independent quota or pretend that absent output bytes are free capacity.

## Remaining gates

The authority admission transaction now validates the exact pair, declared
membership, application, producer and cancellation fences, and atomically commits
input identity, timestamps, job, receipt and output/metadata funding. It rechecks
authorization and trusted time at commitment. The remaining executor/result
paths must preserve dependencies across retry/restart, consume the funded
allowances and reconcile orphans with authoritative references. No durable profile
can be advertised until the full Java execution/results/retirement paths and
their failure/resource acceptance gates are implemented and tested.

The acceptance ledger and current-unit raw evidence distinguish ordinary
correctness tests, real process interruption tests and bounded-heap streaming
measurements. Process interruption is not a power-loss or storage-device proof;
fixed buffers and file quotas alone are not a whole-process RSS bound or a
performance comparison against gRPC.
