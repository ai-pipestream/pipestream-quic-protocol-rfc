# Storage SPI layout probe — script format v1

This directory holds the language-neutral probe script and its expected
canonical report, per docs/standards/storage-spi-design.md "Cross-language
verification". Each lane's probe CLI executes `probe-script-v1.txt` against a
configured backend and must print a byte-identical report to
`expected-report-v1.txt`.

## Script format

One operation per line; blank lines and lines starting with `#` are ignored.
Scope paths are `/`-joined 8-hex components, root = `00000000`. Payloads are
lowercase hex.

```
put <scope> <entity-id-dec> <payload-hex>
seal <scope> <entity-count>        # seal digest = canonical content digest at this point
append <intent-id> <payload-hex>
commit <intent-id> <payload-hex>
retire <scope> <cursor-dec>
```

## Report format

Deterministic, one line per item, LF endings, trailing newline:

- `scope <path>` — for every scope touched by the script, ascending path order
- `  entity <id-8hex> sha256=<payload-digest-hex>` — live entities, ascending id
- `  content-digest <hex>` — canonical content digest (pinned decision 3)
- `  sealed digest=<hex> count=<n>` — only when sealed
- `journal` — then every entry of replay(0), ascending cursor:
  `  <cursor> intent <id> sha256=<payload-digest-hex>` or
  `  <cursor> commit <id> sha256=<payload-digest-hex>`

A backend with no Journal role omits the journal section; a journal-only
backend reports only it. Digests are lowercase hex SHA-256.
