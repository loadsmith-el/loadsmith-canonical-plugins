# MySQL (`type: mysql`)

The `mysql` connector provides **both** a source and a destination on the
pure-Rust `mysql_async` driver (rustls TLS — the same `rustls-rustcrypto`
provider as postgres, no ring/aws-lc). They share one connection block.

- Source binary: `loadsmith-source-mysql`
- Destination binary: `loadsmith-destination-mysql`

**Both server auth plugins are supported automatically** — the driver
negotiates whichever the user is configured with. Modern `caching_sha2_password`
(default since 8.0, the only option in MySQL 9; handled over plaintext via the
pure-Rust RSA public-key exchange) and legacy `mysql_native_password` (MySQL
5.x). No client-side configuration is needed for either.

## Connection fields (shared)

| Field | Type | Default | Description |
|---|---|---|---|
| `host` | string | `"localhost"` | Server hostname. |
| `port` | integer | `3306` | TCP port. |
| `dbname` | string | — | Database (schema) name. |
| `user` | string | — | Login user. |
| `password` | string | — | Login password. Supports `{{ env(...) }}`. |
| `tls` | object | — | The shared [`tls` block](./overview.md#the-tls-block), mapped onto `mysql_async`'s `SslOpts`: `require` encrypts without verification; `verify-ca`/`verify-full` verify against `root_cert` (inline PEM); plus mTLS via `client_cert`/`client_key`. **`prefer` is rejected**: the driver has no plaintext fallback, so it could only mean `require` — say `require` (or `disable`). |
| `statement_timeout` | duration | — | Applied as the session `max_execution_time`. |
| `session` | map | — | `SET SESSION <k> = <v>` applied on connect. Values are inserted as raw SQL — quote string values yourself (e.g. `sql_mode: "'ANSI'"`); unlike postgres, nothing is bound. Applied after `statement_timeout`, so a `max_execution_time` key here overrides it. `time_zone` can't be set (see below). |
| `tcp_keepalive` | duration | — | TCP keepalive idle time. |

**The session `time_zone` is pinned to `'+00:00'`** on every connection (after
`session`), so `TIMESTAMP` columns are read and written as UTC instants
whatever the server or account default. `DATETIME` is unaffected. A
`session.time_zone` is rejected; use `CONVERT_TZ(col, '+00:00', 'Europe/Paris')`
in the query when you want local wall-clock times.

Every key is checked, the flattened connection fields and the `tls` block
included: an unknown or misspelled key fails the run (`UNKNOWN_CONFIG_KEYS`,
with a "did you mean" when one is close) unless the run uses
`--ignore-unknown-keys`. Enum values are kebab-case; any other spelling is
rejected with the valid values.

## Source

| Field | Type | Default | Description |
|---|---|---|---|
| `query` | string | — | The SQL to extract. Schema is inferred from a prepared statement. |
| `batch_size` | integer | `1000` | Rows per Arrow batch. |
| `timestamp_unit` | `s` / `ms` / `us` / `ns` | `ms` | Arrow unit of `DATETIME`/`TIMESTAMP` columns. The default is the same in every canonical plugin and truncates below the millisecond; **`us` keeps the full precision of `DATETIME(6)`/`TIMESTAMP(6)`**. `ns` only covers the years 1677–2262. |
| `incremental` | object | — | `cursor_column` (+ optional `initial_value`). The core persists the high watermark; the source resumes `> watermark ORDER BY` it. `cursor_column` must be one of the query's columns (matched case-insensitively, like MySQL); any other name fails at startup. Requires a pipeline `state:` block. See [Incremental State](https://loadsmith-el.github.io/loadsmith/architecture/incremental-state.html). |

Rows are **streamed** from the server (`batch_size` at a time), so memory holds
one batch, not the result set.

Type mapping (text protocol, so text-rendered types round-trip losslessly):

| MySQL type | Arrow type | Notes |
|---|---|---|
| `TINYINT`, `SMALLINT`, `MEDIUMINT`, `INT`, `YEAR` (signed or unsigned, except `INT UNSIGNED`) | `Int32` | |
| `INT UNSIGNED` | `Int64` | Every value fits. |
| `BIGINT` | `Int64` | |
| `BIGINT UNSIGNED` | `UInt64` | Every value fits (up to 18446744073709551615). |
| `FLOAT` / `DOUBLE` | `Float32` / `Float64` | |
| `DATE` | `Date32` | |
| `DATETIME` | `Timestamp(unit, None)` | `unit` is `timestamp_unit`. |
| `TIMESTAMP` | `Timestamp(unit, "UTC")` | An instant (the session zone is UTC). |
| `BIT(n)` | `UInt64` | The bit value (`b'101'` → `5`). |
| `GEOMETRY` (and its subtypes) | `Binary` | Standard WKB. MySQL's SRID prefix is dropped; select `ST_SRID(col)` alongside to keep it. |
| `BLOB`, `BINARY`, `VARBINARY` (binary collation) | `Binary` | |
| `DECIMAL(p,s)` | `Decimal128(p, s)` | Exact; `Decimal256(p, s)` past 38 digits (MySQL allows 65). Nothing is rounded. Keep the text with `CAST(col AS CHAR)`. |
| everything else (`TIME`, `JSON`, `ENUM`, `SET`, text types, …) | `Utf8` | Exact server text. (`TIME` is a duration up to ±838 h, not a time of day.) |

Zero dates/datetimes (`0000-00-00…`) fail to parse; map them in SQL, e.g.
`NULLIF(col, '0000-00-00')`.

```yaml
source:
  type: mysql
  config:
    host: localhost
    port: 3306                 # default 3306
    dbname: lab
    user: lab
    password: "{{ env('MYSQL_PASSWORD') }}"
    query: "SELECT * FROM events ORDER BY event_sequence"
    batch_size: 2000           # default 1000
    timestamp_unit: us         # default ms
    tls:
      mode: require            # disable | require | verify-ca | verify-full
    incremental:
      cursor_column: updated_at
      initial_value: "2026-01-01 00:00:00"   # optional first-run watermark
```

## Destination

Writes Arrow batches via batched, parameter-bound `INSERT`. The target table
must already exist.

Accepted Arrow input types: every integer width (signed and unsigned), floats,
`Decimal128`/`Decimal256` (exact text), `Boolean`, a UUID (`arrow.uuid`, as its canonical text), an `Interval` (ISO-8601 text: MySQL has no interval type), the string and binary types
(plain, large, view, fixed-size), `Date32`/`Date64`, `Timestamp` in **every
unit** with or without a zone (written as the UTC wall clock — right for a
`TIMESTAMP` column, since the session zone is UTC; MySQL keeps up to
microseconds), and `Time32`/`Time64` in every unit. Lists, structs and maps
fail the load.

| Field | Type | Required | Description |
|---|---|---|---|
| connection fields | — | yes (port default `3306`) | The shared connection block above, including `tls`. |
| `target_table` | string | yes | Destination table: `table`, or `database.table`. Each part is quoted (backticks) in every mode, so it is taken literally. Must already exist. |
| `mode` | `append` / `overwrite` / `merge` | no (default `append`) | How the load changes the table (below). |
| `merge_key` | list of strings | for `merge` (keyed) | The key columns of the merge. Only valid with `mode: merge`. |
| `merge_strategy` | `keyed` / `on-duplicate-key` | no (default `keyed`) | `merge` only: how a target row is found (below). |
| `update_columns` | list of strings | no | `merge` only: the columns a matched row updates. Default: every incoming column that isn't a key. `[]` inserts new rows and leaves existing ones alone. |

Every mode runs in **one transaction**, committed at the end of the load.

- **`append`** (default) — `INSERT` straight into the target. At-least-once.
- **`overwrite`** — `DELETE FROM` the target, then `INSERT`: the contents are
  replaced by exactly the load's rows, atomically. (`DELETE`, not `TRUNCATE`:
  MySQL's `TRUNCATE` commits implicitly and could not roll back. Deleting is
  row by row, so it is slower on big tables, and `DELETE` triggers fire.)
- **`merge`** — `INSERT` into a temporary staging table, then apply it to the
  target in the same transaction. Idempotent ⇒ exactly-once effective. When one
  load holds several rows with the same key, **the last one (load order)
  wins**. Two strategies:
  - **`keyed`** (default) — matches on `merge_key`, like the postgres and
    oracle `MERGE`: an `UPDATE … JOIN` of the matching rows, then an `INSERT`
    of the rows whose key isn't in the target. Needs no index (an index on the
    key makes it fast). Rows whose key is `NULL` never match: keep key columns
    `NOT NULL`. Needs MySQL 8.0+ (window functions).
  - **`on-duplicate-key`** — MySQL's native `INSERT … SELECT … ON DUPLICATE KEY
    UPDATE`: a row colliding with the target on **any** `PRIMARY`/`UNIQUE`
    index updates that row. `merge_key` is optional here and only names columns
    left out of the update. This is what `staged_merge` did in 0.1.
  (The staging table has an extra `_ls_seq` column: a target column with that
  name clashes.)

```yaml
destination:
  type: mysql
  config:
    host: localhost
    dbname: warehouse
    user: loadsmith_writer
    password: "{{ env('MYSQL_PASSWORD') }}"
    target_table: orders
    mode: merge               # append (default) | overwrite | merge
    merge_key: [id]
    # merge_strategy: on-duplicate-key   # match on the table's unique indexes instead
```

### Migrating from 0.2

| 0.2 | 0.3 |
|---|---|
| `DECIMAL` → `Utf8` text | an exact `Decimal128(p, s)` (`Decimal256` past 38 digits); Parquet columns become `DECIMAL`. Keep the text with `CAST(col AS CHAR)`. An incremental pipeline's next run sees the new schema: its `state:` refuses it (`on_schema_change: refuse`, the default) until the state is reset (`loadsmith state rm`) or the run uses `on_schema_change: continue`. |

### Migrating from 0.1

| 0.1 | 0.2 |
|---|---|
| `mode: atomic` | `mode: append` |
| `mode: staged_merge` + `merge_key` (columns *excluded* from the update; conflicts found by any unique index) | `mode: merge` + `merge_strategy: on-duplicate-key` + the same `merge_key` — or the new default `merge_strategy: keyed`, where `merge_key` is the match key |
| `tls.mode: prefer` (silently `require`) | rejected: write `require` |
| `session.time_zone` | rejected: the session zone is pinned to UTC |
| `TIMESTAMP` → `Timestamp(ms)` | `Timestamp(ms, "UTC")` (the instant, now that the zone is pinned) |
| `INT UNSIGNED` / `BIGINT UNSIGNED` failed above the signed range | `Int64` / `UInt64` |
| `BIT`, `GEOMETRY` → lossy `Utf8` | `UInt64`, `Binary` (WKB) |
| the result set buffered in memory | streamed |
| unknown keys ignored | rejected (or warned with `--ignore-unknown-keys`) |

## Telemetry

When the run has telemetry on (the engine's `telemetry:` block or `OTEL_*`
settings), the plugin records spans and metrics in-process and the engine
exports them over OTLP under the run's trace — the plugin itself opens no
connection for it, and records nothing when telemetry is off. Every plugin gets
the SDK's baseline: spans `configure`, `start`, `finish` and the per-batch /
per-object `loadsmith.*` spans, plus the `loadsmith.plugin.*` metrics
([engine design](https://github.com/loadsmith-el/loadsmith/blob/main/docs/src/architecture/telemetry.md)). On top of it, following the OpenTelemetry database semantic
conventions:

| Span (`client`) | When | Attributes |
|---|---|---|
| `connect` | each connection (TCP, TLS, authentication, session setup) | `db.system.name` = `mysql`, `db.namespace`, `server.address`, `server.port`, `loadsmith.tls.mode` |
| `PREPARE` | source: schema inference | statement attributes below |
| `SELECT` | source: the query's execution (rows are then streamed) | |
| `CREATE TABLE _ls_staging` | destination, `mode: merge` | |
| `START TRANSACTION`, `COMMIT` | destination: the load's transaction | |
| `DELETE <table>` | destination, `mode: overwrite` | `loadsmith.db.affected_rows` |
| `INSERT <table>` | destination: once per batch (the batch insert; into the staging table for `merge`) | `db.operation.batch.size`, `loadsmith.db.affected_rows` |
| `UPDATE <table>`, `INSERT <table>` | destination, `mode: merge`: each merge phase at the end | `loadsmith.db.affected_rows` |

Every statement span carries `db.system.name`, `db.namespace` (the `dbname`),
`db.collection.name` (when it targets a table), `db.operation.name` and
`db.query.text`; a failure marks it `error` with `error.type` and
`db.response.status_code` (the server's error code). Metric:
`db.client.operation.duration` (histogram, seconds; by `db.system.name`,
`db.namespace`, `db.operation.name`, `db.collection.name`, `error.type`).

`db.query.text` is only the statement the plugin generates or runs — with
placeholders, and the incremental watermark shown as `?` — never a bound value
or row data. One span per statement, batch or merge phase, never per row.
