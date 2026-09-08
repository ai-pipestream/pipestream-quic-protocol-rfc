# Java V2 immutable input and output storage

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

`policy.cbor` format 4 retains a fresh local installation identity, an optional
immutable database-installation identity, file-byte, file-name, per-object and
active-handle limits, and a checksum. These identities do not replace protocol
non-reuse rules or confer authorization. Earlier private policies are refused,
not converted; the input envelope and wire format are unchanged. The layout also
includes `outputs` and `output-pending` namespaces for prepaid result objects.

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
Reclamation of installed objects requires the paired authority's transactional
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

Each input reader also pins its exact immutable object, including during file
verification. Multiple readers release that pin only after the last descriptor
closes; EOF alone does not release it. Per-object pin entries are bounded by the
same global handle limit. Closing a reader does not refund durable bytes or
file names. The local `inputPinned` observation is not deletion authority:
cleanup must hold the store monitor through its physical action and separately
prove durable eligibility. See the remaining
[retention and retirement implementation plan](java-v2-retention.md).

The stronger `inputInUse` gate also tracks active receivers through synchronized
staging cleanup, including duplicate uploads that could otherwise recreate a
reclaimed name. Unborrowed receiver credits remain identity-free. Input reclamation
requires a committed authority release record; it retains an exact same-process
physical charge across unlink/sync failures until synchronized removal succeeds.
Only then may the authority commit its logical input-quota refund.

Authority expansion reserves one store-bound `ReceiverCredit` before invoking its
expander. The credit charges one global handle without granting producer authority;
the fenced authority layer must still validate each declaration and admission. A
credited `begin` borrows that same handle, so unrelated reception cannot consume the
capacity reserved for the running phase. One credit supports sequential receivers,
but a foreign, closed or already borrowed credit is CONFLICT. Header and quota
validation happen before borrowing, leaving the credit reusable after a refused begin.

Finishing or abandoning reception returns the borrow only after the physical descriptor
is closed and staging cleanup and accounting complete. Closing a borrowed credit refuses
and retains its charge. If cleanup durability is uncertain, both the receiver and credit
remain conservatively charged; bounded idempotent receiver-close retry completes cleanup
before reuse or release. Protocol failure performs the same safe abandonment, while
successful credit close returns the reserved handle. Payload bytes and file names remain
charged by the ordinary reception rules and are never prepaid by the credit.

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
`OutputStore` now consumes these prepaid allowances through `beginOutput`, without
adding another quota or treating absent output bytes as free capacity. The writer
streams an exact declared length and computes SHA-256 before immutable installation.
It checks per-object, aggregate-byte and count bounds and shares the input store's
handle pool. A slot is qualified by its funding identity and output index, with
the exact store, authority, owner, input, attempt and local lease committed in its
private metadata. Renewing a lease's timestamp does not change that identity.

Installed objects remain immutable and charged. Pending and installed names fit
inside the same funded allowance. Recovery audits installed bodies and funding
before removing exclusively abandoned staging, and does not turn an orphan into
a successful result. A newly committed current replacement claim can authorize
explicit reclamation of strictly older unpublished slots. Live per-funding
readers/writers prevent that reclamation. All targets are checked before unlink,
and both namespaces must be synchronized before reuse. Funding stays charged;
this is slot recycling, not retention expiry or a capacity refund.

The authority's separate `succeedExecution` transaction verifies the exact output
set before publishing a manifest and terminal success. A local verified file
reader is not a protocol result-read lease. The local `ResultService` now supplies
current owner authorization, fresh external availability checks, bounded pending
and active lifetimes, and revocation checks. It owns one registry per exclusive
input installation and shares this store's physical handle pool. Dependency-safe
cleanup and the Netty result-stream adapter remain separate required work.

Caller-expanded branch execution reserves one store-bound sequential child-reader
credit before invoking application code, alongside its own input and optional
output-writer credit. Borrowing an output reader pins that funding without charging
a second global handle; closing the physical reader returns the credit for another
child. A foreign, closed or already borrowed credit refuses reuse. The credit cannot
be released while its physical reader remains open. This reserves capacity only:
the parent execution authority must separately validate every selected dependency.
Fresh admission rejects a handle policy below this callback's intrinsic minimum
before output funding. Transient handle occupancy is checked at dispatch.

## Remaining gates

The authority admission transaction now validates the exact pair, declared
membership, application, producer and cancellation fences, and atomically commits
input identity, timestamps, job, receipt and output/metadata funding. It rechecks
authorization and trusted time at commitment. The local executor now preserves
expansion and reassembly phases across replacement leases and consumes phase-specific
handle credits. The local result service now enforces read authorization and
physical read pins. The remaining transport, cleanup and retirement paths must
preserve dependencies and reconcile orphans with authoritative references. No durable profile
can be advertised until the full Java execution/results/retirement paths and
their failure/resource acceptance gates are implemented and tested.

The acceptance ledger and current-unit raw evidence distinguish ordinary
correctness tests, real process interruption tests and bounded-heap streaming
measurements. Process interruption is not a power-loss or storage-device proof;
fixed buffers and file quotas alone are not a whole-process RSS bound or a
performance comparison against gRPC.
