# Changelog

Release notes for the canonical plugins. Each plugin is versioned and released
on its own (tag `<name>-v<version>`); entries are grouped per plugin. The
format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## 2026-10-29 — postgres 0.8.1, oracle 0.9.2: partitioned loads no longer stall

The plugins move to the SDK of loadsmith `de670cb`.

### postgres 0.8.1 / oracle 0.9.2

- **A partitioned load (`parallel`) could stop for good mid-run**, idle on
  both ends. A worker whose range turned out empty gave its credit back, but
  a worker already waiting for the next grant was not woken, and with no
  batch in flight the grant never grew. Likely with large rows — the core's
  memory budget then grants one batch at a time — and an Oracle table of
  CLOB/BLOB rows (50k rows, 1.2 GB) stalled on every run. It now wakes the
  waiting workers.

## 2026-10-29 — oracle 0.9.1: oracledb beta.5

### oracle 0.9.1

- **Driver `oracledb` 26.0.0-beta.5.** Connects to Oracle Database 12c (it
  panicked on 12.1, oracle/rust-oracledb#55) and to databases that send
  Application Continuity or LTXID data (`unknown TTC message type` before);
  re-parses a statement retried after an error (#51); fixes piggybacked
  messages on non-data packets (#59), the per-description SDU and integer
  encoding with trailing zeros (#37). No configuration or schema change.

## 2026-10-29 — native TIME/UUID/INTERVAL/DECIMAL, `parallel.partitions`, AWS roles

The plugins move to the SDK of loadsmith `5d9ec7b` (arrow 60), and to arrow /
parquet 60 and reqwest 0.13.

### postgres 0.8.0

- **`TIME`, `UUID` and `INTERVAL` are read as native Arrow types** instead of
  text: `Time32`/`Time64` in `timestamp_unit`, `FixedSizeBinary(16)` with the
  `arrow.uuid` extension, and `Interval(MonthDayNano)` (read under
  `IntervalStyle = iso_8601`, pinned on the session). `uuid_as: text` and
  `interval_as: text` keep the text. An incremental pipeline's `state:`
  refuses the new schema until reset or `on_schema_change: continue` (see
  *Migrating from 0.7*).
- **`parallel.partitions`** sets how many ranges a partitioned load is cut
  into (default `8 × workers`, up to 4096): more, smaller partitions make a
  resumed run re-read less.
- Destination: a UUID is written as its text, an interval as ISO-8601,
  `Decimal32`/`Decimal64` as decimals, and `24:00:00` as itself.

### oracle 0.9.0

- **`INTERVAL YEAR TO MONTH` / `DAY TO SECOND` are read as
  `Interval(MonthDayNano)`** (`interval_as: text` keeps the `TO_CHAR` text).
- Destination: an interval goes into an INTERVAL column through
  `TO_YMINTERVAL` / `TO_DSINTERVAL` (a value the column can't hold fails),
  into any other column as ISO-8601; a UUID as its text (its bytes into RAW).
- **`parallel.partitions`**, as in postgres.
- **`target_table: schema.table`**: a table in another schema, each part
  quoted on its own.
- **CLOBs over 4000 bytes are read whole**: fetched inline like BLOBs, no
  longer through `TO_CHAR` (which failed past 4000 bytes).

### mysql 0.3.0

- **`DECIMAL(p,s)` is read as `Decimal128(p, s)`** (`Decimal256` past 38
  digits) instead of text. See *Migrating from 0.2*.
- Destination: a UUID is written as its text, an interval as ISO-8601.

### parquet 0.5.0

- A UUID (`arrow.uuid`) is written as Parquet's `UUID` type.
- An `Interval(MonthDayNano)`, which Parquet cannot store, is written as an
  exact `struct<months: int32, days: int32, nanoseconds: int64>`.

### jsonl 0.3.0

- A UUID (`arrow.uuid`) is written as its canonical string, not base64.
- `Decimal32`/`Decimal64` as exact strings.

### s3 0.2.0

- **The AWS credential chain**: without keys in the config or `AWS_*`
  variables, the sink reads the shared credentials file, EKS IRSA (web
  identity at STS), the container endpoint (ECS task roles, EKS Pod Identity)
  and the EC2 instance profile (IMDSv2), and fetches role credentials again
  before they expire.

### sharepoint 0.2.1

- reqwest 0.13 (no behavior change).

## 2026-10-24 — continuing failed runs, COPY reads

The plugins move to the SDK of engine 1.3 (commit `3a10004`: resume tokens,
`ObjectKey`).

### postgres 0.7.0

- **Reads with `COPY (<query>) TO STDOUT` by default** (`fetch: copy`): rows
  arrive as tab-separated text, with no per-value length and no round trip
  per batch. 429 MiB per million rows of the bench's 34-column table, where a
  cursor moves 525 MiB (the binary protocol would move ~495 MiB). Same values,
  parsed by the same decoder. `fetch: cursor` keeps the `DECLARE`/`FETCH` read,
  for servers without `COPY … TO STDOUT`.
- **`parallel.split_by: <column>`**: partitions by ranges of an integer key
  (open at both ends, plus its NULLs) instead of page ranges.
- **A partitioned full load reports the partitions it finished** (resume
  token). Engine 1.3 continues a failed run with it: by default the load
  starts over (the snapshot ended with its transaction), replacing the failed
  run's files; with `parallel.resume_snapshot: new` and a key `split_by` it
  reads only the partitions not finished, at a new snapshot.
- **Destination: incoming columns match the target ignoring case** when no
  column is spelled exactly the same, so an Oracle source's `EVENT_SEQUENCE`
  loads `event_sequence` (before, the quoted upper-case name failed the
  `COPY`). `merge_key`/`update_columns` match the same way; an ambiguous or
  unknown name fails at the first batch, listing the target's columns.

### oracle 0.8.0

- **`parallel.split_by: <column>`**: partitions by ranges of an integer key
  instead of ROWID ranges.
- **A partitioned full load reports its SCN and the partitions it finished**
  (resume token). Engine 1.3 continues a failed run at the same SCN, reading
  only the partitions not finished, while the undo reaches back to it;
  otherwise the load starts over, or with `parallel.resume_snapshot: new` and
  a key `split_by` reads the remaining partitions at a new SCN.
- **Fix: the same partitioned load took 1.8 or 5.8 s by the luck of the
  sample.** `SAMPLE BLOCK (1)` samples whole multiblock reads: on a table of
  ~50,000 blocks it came back empty (and the source then read every ROWID of
  the table to find its extent, 3.6 s on 1M rows), or covered only the first
  fifth of the table, whose rest then fell in one open-ended range read by one
  worker (800,000 of 1M rows). The sample now grows (1%, 5%, 25%), its spans
  adding up, until they cover three quarters of the blocks the table's
  statistics count (`ALL_TABLES.BLOCKS`): the 1M-row load ×8 takes 1.8–3.0 s.
- **Fix: a partitioned load could miss the last seconds of commits — a table
  loaded just before it read as empty, and succeeded.** The snapshot SCN came
  from `TIMESTAMP_TO_SCN(SYSTIMESTAMP)`, which the database maps in steps of
  about 3 seconds (from an older entry right after a restart): commits made
  just before the run were after it (the lab's freshly loaded table: 0 of
  60,000 rows, status success). That SCN is now only the start of a search:
  an `AS OF SCN` read of the table fails (ORA-08181) past the current SCN, so
  a few probes of a millisecond find the current one, with no privilege.

### local-copy 0.3.0

- **`key`: the s3 sink's object-name template**, default
  `{run_date}/{run_id}/{file}`: each run's files land in their own directory.
  Before, every file kept its flat name in `dest`, so a later incremental run
  replaced an earlier run's files of the same name. `key: "{file}"` restores
  the flat layout.

### s3 0.1.2

- The `key` template comes from the SDK (`ObjectKey`), shared with
  `local-copy`. No change in behavior.

## 2026-10-23 — parallel Parquet encoding

### parquet 0.4.0

- **The columns of each batch are encoded in parallel** (`encode_threads`,
  default: the CPUs, at most 8), each column by its own encoder, with the
  same row groups, pages and footer as before: the files are byte for byte
  the ones 0.3 writes. Postgres 5M rows → Parquet → S3 (8 source workers):
  8.4 s → 5.5 s; the writer's own thread went from 8.0 to 1.6 CPU seconds.

## 2026-10-23 — partitioned reads

### postgres 0.6.0

- **`parallel: { workers: N }`: a partitioned full load** (PostgreSQL 14+).
  N connections read disjoint page ranges of the query's table (`ctid`, TID
  range scans) in one snapshot the source exports and every worker imports,
  each fetching and converting under the engine's shared flow credit. The
  ranges are half-open and the last is open, so every row is read exactly
  once. The query must read one table (`SELECT … FROM t [alias] [WHERE …]`);
  not with `incremental`. 5M rows to `null`: 7.1 s on one connection, 3.3 s
  with 8 workers.

### oracle 0.7.0

- **`parallel: { workers: N }`: a partitioned full load.** N connections read
  disjoint ROWID ranges of the query's table, all `AS OF` the same SCN, each
  fetching and converting on its own thread under the engine's shared flow
  credit. The ranges are cut from a 1% block sample and are half-open, so
  every row is read exactly once; only `SELECT` on the table is needed. The
  query must read one table (`SELECT … FROM t [alias] [WHERE …]`); not with
  `incremental`. 5M rows to `null`: 11.8 s on one connection, 4.9 s with 8
  workers.

## 2026-10-23 — resume at what reached the target

The plugins move to the SDK at engine commit `261870f`.

### parquet 0.3.0

- Each file announces the batches it holds. With engine 1.2 and a sink, the
  incremental watermark then advances as each file is delivered, and a run
  whose sink fails for good leaves it at the last delivered file: the next run
  reads only what never reached the target.
- On a run the engine retries (`pipeline.recovery`), the files of the new
  attempt are numbered after the ones already staged, so delivered files keep
  their names.

### postgres 0.5.1, oracle 0.6.1, mysql 0.2.1

- **Fix:** an incremental watermark persisted mid-run could skip rows tied on
  the cursor (a resume reads strictly after it). While streaming, the sources
  now report only a cursor value no later row can repeat, and the last value
  at the end. Rows of the boundary value are read again on resume
  (at-least-once, as documented).

## 2026-10-23 — faster sources under the engine's flow control

The plugins move to the SDK at engine commit `1ca7af3`: every source
advertises `flow_credit` and reads ahead only as far as the engine grants
(engine 1.1, `pipeline.flow`), and writes its Arrow stream on its own thread.
With an older engine the sources behave as before. The delivered data is
byte-identical to the previous versions.

### postgres 0.5.0

- The source fetches the next chunk while it converts the last one, so the
  database and the conversion overlap instead of taking turns.
- `numeric` and `timestamp` text is parsed without allocating.
- 5M rows to the `null` destination: 16.9 s → 7.4 s.

### oracle 0.6.0

- The fetch thread only decodes the wire; values are read in their native
  form (numbers, floats, booleans, text and bytes without a copy) when the
  batch is built, instead of becoming a `String` per cell and being parsed
  back.
- 5M rows to the `null` destination: 19.2 s → 12.2 s.

## 2026-10-23 — native temporal reads

### oracle 0.5.2

- The source fetches `DATE` and `TIMESTAMP` in Oracle's binary form instead
  of rendering them as text with `TO_CHAR`: 11 bytes a timestamp on the wire
  instead of 29. The zoned timestamps are converted with `SYS_EXTRACT_UTC` and
  fetched the same way. The delivered values are unchanged, and so is the
  persisted incremental watermark: state saved by 0.5.1 resumes as before.
- A `TIMESTAMP WITH TIME ZONE` stored with a region name
  (`America/Sao_Paulo`) reads correctly; the driver can't decode a region
  itself, so the conversion to UTC happens in the database.

## 2026-10-22 — sinks on slow links

The sinks move to the SDK at engine commit `08291ff`.

### s3 0.1.1

- An upload that takes longer than the core's 5 s liveness ping no longer
  looks like a hang. The sink used to stop answering `ping` while it
  uploaded, so the core respawned it and re-sent the object: two extra
  attempts per run for 64 MiB objects at 100 Mbit/s, and a failed run
  ("sink failed after 5 restarts") at 20 Mbit/s. Now the same 1M-row load
  completes at 20 Mbit/s in one attempt per object.

### local-copy 0.2.1

- The same fix for a copy slower than the ping interval (a slow network
  mount).

## 2026-10-22 — bounded memory for large tables

### oracle 0.5.1

- The source streams: the query's cursor stays open and a fetch thread reads
  `batch_size` rows per round trip (`fetch_array_size`), one batch ahead of
  the pump. Memory holds a few batches whatever the table's size — a
  1M-row table drops from ~3 GiB to ~380 MiB peak at the same speed. Before,
  the whole result set was buffered before the first batch.

### parquet 0.2.1

- `row_group_size` defaults to 131,072 rows (was the `parquet` crate's
  1,048,576). A row group is buffered until written, so this bounds the
  writer's memory: a 1M-row load drops from ~480 MiB to ~215 MiB peak.

## 2026-10-22 — S3 delivery and typed numbers

Every plugin moves to the SDK at engine commit `27e692d` (`configure.run`:
the run's id, pipeline and start time reach every plugin).

### s3 0.1.0 (new)

- A sink that delivers staged files (e.g. Parquet) to AWS S3 or any
  S3-compatible store: SigV4 in pure Rust, TLS via rustls, nothing to install
  on the host. One `PUT` per file up to `part_size_mib`, multipart above it
  (aborted on failure); retries on network errors, `429` and `5xx`. Keys are
  `prefix` + a template naming the run (`{run_date}/{run_id}/{file}` by
  default), so incremental runs add objects side by side. Static credentials
  in the config or the `AWS_*` variables. See [S3](docs/src/config/s3.md).

### oracle 0.5.0 — breaking

- `NUMBER` is an exact Arrow number instead of text: `Int32`/`Int64` for
  `NUMBER(p)` up to 18 digits, `Decimal128(p, s)` for `NUMBER(p,s)` (and
  `NUMBER(p)` past 18 digits), `Float64` for `FLOAT(b)`, and
  `Decimal128(38, number_scale)` for a `NUMBER` without a declared precision
  (new option `number_scale`, default 10; a value that doesn't fit fails the
  run). Parquet columns become `INT32`/`INT64`/`DECIMAL`. An incremental
  pipeline's saved schema no longer matches: reset its state or run once with
  `on_schema_change: continue`.

### postgres 0.4.0 — breaking

- `NUMERIC(p,s)` is an exact `Decimal128(p, s)` (`Decimal256` past 38 digits)
  instead of text; an unconstrained `NUMERIC` is `Decimal128(38,
  number_scale)` (new option, default 10) and fails on a value that doesn't
  fit or on `NaN`/`Infinity` — cast it in the query to keep it as text. Same
  state note as oracle.

## 2026-10-21 — SDK for engine 1.0

Every plugin moves to the Loadsmith SDK at engine commit `757edf6`, which
wires the engine's parser stage and OpenTelemetry. What that brings to all
of them:

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
- **Telemetry (OpenTelemetry).** When the run has telemetry on, the SDK
  records spans (`configure`, `start`, `loadsmith.batch`, `finish`, …) and the
  `loadsmith.plugin.*` metrics, and each plugin adds its own domain spans and
  metrics under the OpenTelemetry semantic conventions (listed below and in
  each plugin's *Telemetry* section). The engine exports them over OTLP; the
  plugins open no connection and record nothing when telemetry is off. Spans
  are per statement, batch, request or file — never per row — and never
  carry a bound value, row data, token or secret.

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
- Config is parsed with the SDK's `PluginConfig::parse`: an unknown key is
  rejected (`UNKNOWN_CONFIG_KEYS`, with a "did you mean"), or only warned about
  with `--ignore-unknown-keys`.
- Telemetry: a `list` span for the source's directory listing (matched files,
  bytes); a `read <file>` span per provider reference (`file.path`,
  `file.size`, `error.type`), never the value.

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
- Config is parsed with the SDK's `PluginConfig::parse`: an unknown key is
  rejected (`UNKNOWN_CONFIG_KEYS`, with a "did you mean"), or only warned about
  with `--ignore-unknown-keys`.
- Telemetry: a `read <file>` span per file opened (`file.path`, `file.size`,
  column count, inferred or declared).

### excel 0.1.0 (new)

- A **parser** (`kind: parser`, binary `loadsmith-parser-excel`, capability
  `parse`) for one sheet of each xlsx/xlsm/xlsb/xls/ods workbook. Options:
  `sheet` (a name or a 0-based index), `header`, `skip_rows`, `batch_size`,
  declared `columns:`. Types are inferred over the whole sheet with the
  `sharepoint` rules; a workbook is read whole, one file at a time. See
  [Excel](docs/src/config/excel.md).
- Config is parsed with the SDK's `PluginConfig::parse`: an unknown key is
  rejected (`UNKNOWN_CONFIG_KEYS`, with a "did you mean"), or only warned about
  with `--ignore-unknown-keys`.
- Telemetry: a `read <file>` span per workbook read (`file.path`,
  `file.size`, sheet rows, column count).

### jsonl 0.2.0 (breaking)

- **`path` is required** (writing to stdout is gone: stdout is the protocol's
  control channel).
- **Dates, times, timestamps and durations are ISO-8601 text** with the
  column's own precision (`2026-10-20T12:00:00.123456` for a microsecond
  column, `Z` for a zoned one, `2026-10-20`, `12:00:00.123`, `PT1.500S`).
  `temporal_format: epoch` (new) writes integers instead, in `epoch_unit:
  column | s | ms | us | ns` (default `ms`, the 0.1 output; `column` keeps each
  column's unit).
- The plugin owns its JSON mapping (no longer the engine's): every Arrow type
  has a defined form and none panics — 0.1 panicked on a non-millisecond
  timestamp and wrote `null` for decimals, lists, structs, unsigned integers…
  Decimals are exact strings, NaN/±Infinity are `null` (they were `0`). See
  [JSONL](docs/src/config/jsonl.md#value-mapping).
- **Unknown config keys are rejected** (or warned with `--ignore-unknown-keys`);
  enum values are kebab-case with a "did you mean".
- `cancel()` deletes the partial output file (it only flushed before).
- Telemetry: a `write <file>` span (`file.path`, `file.size`,
  `loadsmith.object.rows`).

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
- Telemetry: `connect` span (TLS mode, server); one `client` span per
  statement, `COPY` (per batch) and merge phase (`TRUNCATE`, staging `CREATE
  TABLE`, `MERGE`) with `db.system.name=postgresql`, `db.namespace`,
  `db.collection.name`, `db.operation.name`, `db.query.text` (generated
  statement only; the watermark shows as `?`), returned/affected rows,
  SQLSTATE `error.type`; `db.client.operation.duration` histogram.

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
- Telemetry: `connect` span; one `client` span per statement, batch insert
  and merge phase (`DELETE` for overwrite, staging, `UPDATE`/`INSERT` merge)
  with the `db.*` attributes (`db.system.name=mysql`; `db.query.text` with
  `?` placeholders, never values), affected rows, server-code `error.type`;
  `db.client.operation.duration` histogram.

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
- Telemetry: `connect` span; one `client` span per describe, query, array
  insert and merge phase (`DELETE` for overwrite, staging, `MERGE`, `COMMIT`)
  with the `db.*` attributes (`db.system.name=oracle.db`; `db.query.text`
  with binds, never values), returned/affected rows, `ORA-NNNNN`
  `error.type`; `db.client.operation.duration` histogram.

### sharepoint 0.2.0 (breaking)

- SDK bump (abort rules, exact schema messages).
- **Unknown config keys are rejected** at any depth — `input`, `parser`, each
  `transform` step and each `filter` node included — or warned with
  `--ignore-unknown-keys`.
- **`kind`/`op` values are kebab-case**: `fix-merged-columns`,
  `drop-empty-rows`, `not-null`; the snake_case spellings are rejected with the
  new one named.
- HTTP: a per-request **`request_timeout`** (default `60s`; there was none) and
  **bounded retries** (`max_retries`, default `5`) of `429`/`503` answers —
  after their `Retry-After`, else an exponential backoff, capped at 120 s —
  and of timeouts and failed connections.
- The token **`scope`** is derived from `graph_base_url` (sovereign clouds work
  with the two base URLs alone) and can be set explicitly; it was fixed to the
  global Graph.
- Internal: the CSV/Excel parsing and the cell table moved to the shared
  `tabular` library (also used by the `csv` and `excel` parsers), with no
  behavior change.
- Telemetry: an HTTP `client` span per Graph/token call
  (`http.request.method`, `url.full` with query values redacted,
  `server.address`, `http.response.status_code`, resend count), a `retry`
  event per retry (cause, wait, `Retry-After`), a `token` span when minting a
  token (never the token or secret); `http.client.request.duration`
  histogram.

### parquet 0.2.0 (breaking)

- SDK bump (abort rules). `cancel()` already removed the run's files.
- **Re-runs replace the prefix's previous output**: a chunked run removes the
  previous set before writing (a run with fewer chunks left stale ones behind);
  a single-file run renames its new file over the old one, then removes other
  files of the prefix. Only files named `<prefix>[.<seq>].<codec>.parquet` are
  touched.
- Every file is written under a hidden temp name and renamed when complete: a
  reader or a sink never sees a partial file.
- **`max_file_size` needs a unit**: a bare number (it meant KiB) is rejected
  with the spelling it meant (`"500"` → `"500KiB"`).
- `path` is created when missing.
- New `compression_level` (gzip 0–10, zstd 1–22) and `row_group_size`.
- **Unknown config keys are rejected** (or warned with
  `--ignore-unknown-keys`); a wrong `compression` gets a "did you mean".
- Telemetry: a `write <file>` span per file (`file.path`, `file.size`,
  `loadsmith.object.rows`), a `rename` span into place, a `cleanup` span for
  the previous run's files.

### null 0.2.0 (breaking)

- SDK bump (abort rules).
- A key in its (empty) config block is rejected like any unknown key (or warned
  with `--ignore-unknown-keys`); it used to be ignored.
- Telemetry: the SDK baseline only.

### local-copy 0.2.0 (breaking)

- SDK bump (abort rules).
- **Atomic delivery**: each file is copied to a hidden temp name in `dest`,
  synced, and renamed, so `dest` never holds a partial copy. Same-name objects:
  the last one delivered wins (documented).
- **`LOADSMITH_SINK_CRASH_AFTER` is gone from the released binary.** The lab's
  sink-resume case uses `local-copy-chaos`, a never-released workspace member
  (binary `loadsmith-sink-local-copy-chaos`) wrapping the same delivery code.
- **Unknown config keys are rejected** (or warned with
  `--ignore-unknown-keys`).
- Telemetry: inside the SDK's `loadsmith.deliver`, a `copy <file>` span
  (`file.path`, `file.size`) and a `rename` span for the atomic move.
