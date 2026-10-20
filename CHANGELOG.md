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

### postgres 0.3.0 (breaking)

- SDK bump (abort rules, exact schema messages).
- **Unknown config keys are rejected** (`UNKNOWN_CONFIG_KEYS`, with a "did you
  mean"), at any depth — the flattened connection fields and `tls` included —
  unless the run uses `--ignore-unknown-keys`.
- **Destination write modes renamed and extended**: `mode: append | overwrite |
  merge` (default `append`). `atomic` → `append`, `staged_merge` → `merge`; the
  old spellings are rejected with the new one named. `overwrite` (new)
  `TRUNCATE`s the target and loads it in one transaction. `merge_key` is the
  match key of `merge` and an error in the other modes; `update_columns` (new)
  picks the updated columns (`[]`: insert new keys only). Duplicate keys within
  one load no longer break the `MERGE`: the last row of a key wins.
- **Enum values are kebab-case**, and a wrong one names the valid values and the
  likely one (`read_only` → `read-only`, also in `tls.mode`).
- Source: `timestamp_unit: s | ms | us | ns` (default `ms`); `us` keeps
  Postgres' full precision.
- Source: `incremental.cursor_column` must be a query column (a typo silently
  reloaded everything on every run).
- Source: arrays are parsed strictly — a quoted `"NULL"` element is a string,
  explicit bounds (`[0:2]={…}`) are accepted, and a multi-dimensional value
  fails with a hint (`array_to_json(col)`) instead of decoding garbage.
- `password` is optional (certificate or `trust` authentication).
- Destination: accepts every timestamp unit (with or without a zone),
  `Time32`/`Time64`, `Date64`, `Decimal128`/`Decimal256`, all integer widths,
  view/large strings and binaries, and **`List` columns** (written as Postgres
  arrays). Float NaN/±Infinity are spelled the Postgres way.
- Malformed `bytea` text no longer panics.
- Source: `cancel()` bounds its `CLOSE`/`ROLLBACK` to 5 s. After the SDK drops
  an in-flight batch its FETCH still runs server-side, and the rollback could
  otherwise outlast the core's grace period.

### mysql 0.2.0 (breaking)

- SDK bump (abort rules, exact schema messages).
- **Unknown config keys are rejected**, at any depth, unless the run uses
  `--ignore-unknown-keys`. Enum values are kebab-case with a "did you mean".
- **Destination write modes**: `mode: append | overwrite | merge` (default
  `append`; `atomic`/`staged_merge` are rejected, naming the new value).
  `overwrite` (new) deletes the target's rows and loads it in one transaction.
  `merge` now matches on `merge_key` (`merge_strategy: keyed`, the default): an
  `UPDATE … JOIN` then an `INSERT` of the new keys, like postgres and oracle. The
  0.1 behavior — `ON DUPLICATE KEY UPDATE` on any unique index, `merge_key`
  only excluded from the update — is `merge_strategy: on-duplicate-key`.
  `update_columns` (new) picks the updated columns. Duplicate keys within one
  load: the last row wins (the staging table no longer copies the target's
  unique indexes).
- `target_table` is quoted the same way in every mode (`table` or
  `database.table`); `append` used to interpolate it raw.
- **`tls.mode: prefer` is rejected** (it silently meant `require`).
- **The session `time_zone` is pinned to `'+00:00'`**, so `TIMESTAMP` values
  cross as UTC instants, now typed `Timestamp(unit, "UTC")`; `session.time_zone`
  is rejected.
- Source: rows are **streamed** from the server instead of buffering the whole
  result set.
- Source: `INT UNSIGNED` → `Int64`, `BIGINT UNSIGNED` → `UInt64` (large values
  failed); `BIT` → `UInt64` and `GEOMETRY` → `Binary` (WKB) instead of lossy
  text.
- Source: `timestamp_unit: s | ms | us | ns` (default `ms`); `us` keeps
  `DATETIME(6)`/`TIMESTAMP(6)` whole.
- Source: `incremental.cursor_column` must be a query column; the watermark
  literal escapes backslashes.
- Destination: accepts every timestamp unit and zone, `Time32`/`Time64`,
  `Date64`, decimals, all integer widths, view/large strings and binaries.
- Source: `cancel()` bounds the disconnect to 2 s, then drops the connection.
  The connection can be mid-result after the SDK drops an in-flight batch.

### oracle 0.4.0 (breaking)

- SDK bump (abort rules, exact schema messages).
- **Unknown config keys are rejected** at any depth (the `tls` block's own
  `deny_unknown_fields` is gone: `--ignore-unknown-keys` now applies to it like
  everywhere else). Enum values are kebab-case with a "did you mean".
- **Destination write modes**: `mode: append | overwrite | merge` (default
  `append`; `atomic`/`staged_merge` are rejected, naming the new value).
  `overwrite` (new) deletes the target's rows and loads it in one transaction.
  `update_columns` (new) picks the updated columns. Duplicate keys within one
  load no longer fail the `MERGE` with ORA-30926: the last row of a key wins. The
  staging table is now `LS_MRG_<hash>` (with a load-order column); old
  `LS_STG_*` tables can be dropped.
- Source: `DATE`/`TIMESTAMP` → `Timestamp(unit)` and `TIMESTAMP WITH [LOCAL]
  TIME ZONE` → `Timestamp(unit, "UTC")`, instead of `FF6` text;
  `timestamp_unit: s | ms | us | ns` (default `ms`), `ns` keeps `TIMESTAMP(9)`.
- Source: `incremental.cursor_column` must be a query column.
- Destination: accepts every timestamp unit and zone (spelled for the target
  column type), `Time32`/`Time64`, `Date64`, decimals, all integer widths,
  view/large strings and binaries.

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
