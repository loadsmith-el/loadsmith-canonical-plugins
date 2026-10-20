# Changelog

Release notes for the canonical plugins. Each plugin is versioned and released
on its own (tag `<name>-v<version>`); entries are grouped per plugin. The
format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## Unreleased — SDK for engine 1.0

Every plugin moves to the Loadsmith SDK at engine commit `7648368`, which
wires the engine's parser stage. What that brings to all of them:

- **Abort rules.** The SDK reads the control channel while the plugin works,
  answers `ping` and `resend`, and calls `cancel()` whenever a run will not
  complete after `start`: a `cancel`/`error` from the core, a lost core, a
  truncated input stream, or the plugin's own failure. A destination never
  finalizes on a stream that ended without the end-of-stream marker
  (`INPUT_TRUNCATED`). A plugin that fails after `start` exits `1`.
- **Exact schema messages.** A source's `schema` message is derived from its
  Arrow schema exactly (timestamp unit and time zone, decimal precision and
  scale, lists, per-field `nullable`) instead of nine lossy labels. The engine
  no longer warns that the `schema` message disagrees with the stream.

**Compatibility.** The wire protocol is still v1, but the new `schema`
vocabulary (`timestamp` with a `unit`, `decimal128`, `list`, …) is only
understood by engines that have it (the 1.0 line). The **source** plugins
below therefore need that engine, hence their minor bump; the destinations and
the sink still work with older engines.

### file 0.2.0 (breaking)

- New **raw source** (`kind: source`, binary `loadsmith-source-file`,
  capability `object_output`) for the engine's `parser:` stage. Config:
  `path` (a directory), `glob` (a file-name pattern, default `*`), optional
  `recursive`. It announces the matching regular files in sorted path order,
  read in place (no copy, no `scratch_dir`). An unreadable directory fails the
  run; an empty match does not. See [File](docs/src/config/file.md#source).
- The package now provides two kinds (`file:provider`, `file:source`); its
  Cargo package is renamed `loadsmith-file`.
- Now a **provider** (`kind: provider`, binary `loadsmith-provider-file`) for
  the engine's `providers:` block and `{{ secret('<instance>', '<ref>') }}`.
  The `config-provider` kind, its `fetch` and the `uri` config are gone.
- Config: optional `base_dir`, against which relative paths resolve.
- References: `path` (the trimmed file) or `path#key` (a scalar picked by a
  dotted key from a JSON or YAML document). Errors `NOT_FOUND`,
  `INVALID_REF`, `ACCESS_DENIED`; no message carries a value.

### csv 0.1.0 (new)

- A **parser** (`kind: parser`, binary `loadsmith-parser-csv`, capability
  `parse`) that streams CSV files into Arrow: it holds the inference sample
  and one batch, never a whole file.
- Options: `delimiter` (one ASCII character), `encoding` (WHATWG labels; a
  byte-order mark wins), `header`, `infer_rows` (default 1000), `batch_size`
  (default 1000), and declared `columns:` in the protocol type vocabulary.
- Inferred columns are nullable: `int64`, `float64`, `bool`, `date32`,
  `timestamp(ms)` or `utf8` (the `sharepoint` CSV rules plus booleans and
  dates). Values that do not fit their column, ragged records and duplicate
  header names fail with the file, line and column; a later file whose schema
  differs fails with `SCHEMA_MISMATCH`. See [CSV](docs/src/config/csv.md).

### excel 0.1.0 (new)

- A **parser** (`kind: parser`, binary `loadsmith-parser-excel`, capability
  `parse`) for one sheet of each xlsx/xlsm/xlsb/xls/ods workbook. Options:
  `sheet` (a name or a 0-based index), `header`, `skip_rows`, `batch_size`,
  declared `columns:`. Types are inferred over the whole sheet with the
  `sharepoint` rules; a workbook is read whole, one file at a time. See
  [Excel](docs/src/config/excel.md).

### jsonl 0.2.0

- `cancel()` deletes the partial output file (it only flushed before).
- Values follow the SDK's total Arrow → JSON mapping: decimals are exact
  strings, lists are arrays, structs and maps are objects, every timestamp
  unit is written as epoch milliseconds, and NaN/±Infinity are `null` (they
  were `0`). See [JSONL](docs/src/config/jsonl.md#value-mapping).

### postgres 0.3.0

- SDK bump (abort rules, exact schema messages).
- Source: `cancel()` bounds its `CLOSE`/`ROLLBACK` to 5 s. After the SDK drops
  an in-flight batch its FETCH still runs server-side, and the rollback could
  otherwise outlast the core's grace period.

### mysql 0.2.0

- SDK bump (abort rules, exact schema messages).
- Source: `cancel()` bounds the disconnect to 2 s, then drops the connection.
  The connection can be mid-result after the SDK drops an in-flight batch.

### oracle 0.4.0

- SDK bump (abort rules, exact schema messages).

### sharepoint 0.2.0

- SDK bump (abort rules, exact schema messages).
- Internal: the CSV/Excel parsing and the cell table moved to the shared
  `tabular` library (also used by the `csv` and `excel` parsers), with no
  behavior change.

### parquet 0.1.2

- SDK bump (abort rules). `cancel()` already removed the run's files.

### null 0.1.3

- SDK bump (abort rules).

### local-copy 0.1.2

- SDK bump (abort rules).
