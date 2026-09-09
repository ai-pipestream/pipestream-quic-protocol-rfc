# Archived neutral-driver dev runs (async-neutral-v2)

Each directory is one `pipestream-conformance durable --dev` run, copied here
via `--archive` after the run finished, together with its
`MANIFEST.sha256` (sha256 of every archived file, sorted relative paths).

These are **development-mode archives**: every run directory carries an
`INCOMPLETE` marker, dev mode can never emit a full-conformance PASS, and
java-client/rust-server direction failures land as per-direction `INCOMPLETE`
markers instead of row failures (the rust-client/rust-server direction is the
row evidence). Nothing in this directory is acceptance evidence; acceptance
runs are reproduced from the recorded subject hashes in each run's
`run.tsv`, not quoted from here.

Files larger than 2 MiB are represented by a `<name>.LARGE.txt` note
(sha256 + length); their bytes are not archived.
