# Postgres (`type: postgres`)

The `postgres` connector provides **both** a source and a destination. They
share one connection block (host/TLS/timeouts/…); only the role-specific fields
(`query` vs `target_table`) differ.

- Source binary: `loadsmith-source-postgres`
- Destination binary: `loadsmith-destination-postgres`

## Connection fields (shared)

Both roles accept these. Connection defaults make a local dev DB a one-liner.

| Field | Type | Default | Description |
|---|---|---|---|
| `host` | string or list | `"localhost"` | Server hostname(s). Use a list for multi-node clusters (see `target_session_attrs`). |
| `port` | integer | `5432` | TCP port. Shared by all hosts in a multi-host list. |
| `dbname` | string | — | Database name. |
| `user` | string | — | Login role. |
| `password` | string | — | Login password. Supports `{{ env(...) }}`. Optional: leave it out for certificate (`tls.client_cert`) or `trust` authentication. |
| `application_name` | string | — | Appears in `pg_stat_activity`. Useful for spotting pipelines in monitoring. |
| `connect_timeout` | duration | — | Max time to establish a connection (e.g. `10s`, `500ms`). |
| `statement_timeout` | duration | — | Kills a query exceeding this wall time. Applied via `set_config`. |
| `options` | map | — | Extra GUCs in the startup message (e.g. `search_path: myschema`). |
| `tls` | object | — | The shared [`tls` block](./overview.md#the-tls-block). |
| `channel_binding` | `disable` / `prefer` / `require` | — | SCRAM channel binding. `require` ties auth to the TLS session, preventing credential relay. |
| `target_session_attrs` | `any` / `read-write` / `read-only` | — | With multi-host, which node to accept (e.g. point at a replica). |
| `session` | map | — | GUCs applied via `set_config($1,$2,false)` after connect (bind params, no SQL interpolation) — e.g. `work_mem: 256MB`. |
| `tcp_keepalives` | object | — | TCP keepalives — `idle` (duration), `interval` (duration), `retries` (int). |

Every key is checked, the flattened connection fields and the `tls` block
included: an unknown or misspelled key fails the run (`UNKNOWN_CONFIG_KEYS`,
with a "did you mean" when one is close) unless the run uses
`--ignore-unknown-keys`. Enum values are kebab-case; any other spelling is
rejected with the valid values (`read_only` → "did you mean `read-only`?").

## Source

| Field | Type | Default | Description |
|---|---|---|---|
| `query` | string | — | Any valid `SELECT` — joins, CTEs, window functions. |
| `batch_size` | integer | `1000` | Rows per Arrow batch (per `FETCH FORWARD n` with `fetch: cursor`). |
| `fetch` | `copy` / `cursor` | `copy` | How rows are read (below): `COPY (<query>) TO STDOUT`, or a cursor (`DECLARE` + `FETCH`) for servers without `COPY … TO STDOUT` (Redshift, some proxies). |
| `transaction_isolation` | `read-committed` / `repeatable-read` / `serializable` | server default (`BEGIN`; normally `read-committed`) | Isolation for the cursor transaction. `repeatable-read` gives a consistent snapshot across all `FETCH`es. |
| `incremental` | object | — | Activates incremental reads (below). Requires a pipeline `state:` block. |
| `timestamp_unit` | `s` / `ms` / `us` / `ns` | `ms` | Arrow unit of `timestamp`/`timestamptz` columns (and arrays of them). The default is the same in every canonical plugin and truncates below the millisecond; **`us` keeps Postgres' full precision**. `ns` is the µs value × 1000 and only covers the years 1677–2262 (a value outside fails the batch). |
| `number_scale` | integer 0–38 | `10` | Scale of a `NUMERIC` **without a declared precision** (a bare `numeric` column, `sum(x)`, …), emitted as `Decimal128(38, number_scale)`; see the type table. |
| `parallel` | object | — | **Partitioned full load**: `workers` (1–64) connections read disjoint partitions (`partitions`, default `8 × workers`) of the query's table in one snapshot. See [Partitioned reads](#partitioned-reads-parallel). Not with `incremental`. |

The query runs in a transaction and streams in `batch_size` chunks, so large
results never buffer fully in memory. By default it is read with `COPY
(<query>) TO STDOUT` (text format): the rows come as tab-separated lines, with
no per-value length and no round trip per chunk. On the bench's 34-column
table that is 429 MiB per million rows where a cursor's `FETCH` moves 525 MiB
(the binary protocol would move ~495 MiB), which is the run's time on a link
to a distant database. `fetch: cursor` reads through a server-side cursor
(`DECLARE … CURSOR FOR` + `FETCH FORWARD`) instead. Either way the next chunk
is read while the last one is converted to Arrow, as far ahead as the engine's
flow control allows
([`pipeline.flow`](https://loadsmith-el.github.io/loadsmith/reference/pipeline-yaml.html#pipelineflow);
one chunk with an engine older than 1.1), and the values are parsed from the
same text format.

### Partitioned reads (`parallel`)

```yaml
source:
  type: postgres
  config:
    # …connection…
    query: "SELECT * FROM sales.orders WHERE region = 'BR'"
    batch_size: 10000
    parallel:
      workers: 8
      split_by: id              # optional: ctid (default) or an integer column
      partitions: 256           # optional: default 8 × workers
      resume_snapshot: new      # optional: same (default) or new; see Resume
```

A full load of one large table, read by `workers` connections at once
(PostgreSQL 14 or later):

- **The query** must read one table: `SELECT <columns> FROM <table> [[AS]
  <alias>] [WHERE <filter>]`. No joins, no subquery in `FROM`, no CTE,
  `GROUP BY`, `ORDER BY`, `LIMIT`, `DISTINCT`, set operators or `FOR UPDATE` —
  anything else fails at startup with the reason.
- **One snapshot.** The source's own transaction (`REPEATABLE READ READ ONLY`)
  exports its snapshot (`pg_export_snapshot()`); every worker imports it (`SET
  TRANSACTION SNAPSHOT`), so all of them see the same data even while the table
  changes. `transaction_isolation` does not apply.
- **The split** (`split_by`). By default (`ctid`) the table's pages
  (`pg_relation_size`) are cut into `8 × workers` ranges of about equal pages,
  read with `ctid` conditions (a TID range scan: each worker reads only its
  pages); the ranges are half-open and the last one is open. With an integer
  column (`split_by: id`; ideally indexed), the range between its minimum and
  maximum in the snapshot is cut into `8 × workers` ranges of about equal
  width, open at both ends, plus one partition for its `NULL`s. Either way
  every row is read exactly once. `partitions` (from `workers` to 4096) sets
  the number of ranges instead of `8 × workers`: more, smaller ranges cost a
  query each but make a resumed run re-read less (see Resume).
- **Each worker fetches and converts** its rows, so the conversion runs in
  parallel too. Batches leave in the order workers finish them, not in table
  order.
- **Memory** stays bounded by the engine's flow control: the workers share one
  credit, so `workers` adds connections, not buffered batches.

Measured on 5M rows (34 columns, the `null` destination, one machine): one
connection 7.1 s, 4 workers 4.3 s, 8 workers 3.3 s.

**Resume** (with a `state:` block and engine 1.3+). The source reports the
partitions it has fully read as its resume token, and the engine keeps the
last one whose rows reached the target. When the run fails, the next run
continues it — but the failed run's snapshot ended with its transaction:

- by default (`resume_snapshot: same`) the load **starts over**, under the
  failed run's id; its files replace the failed run's;
- with `resume_snapshot: new` (key `split_by` only) it reads **only the
  partitions not finished**, at a new snapshot. The output then mixes two
  points in time, and the rows of a partition that was in progress when the
  run failed are read again (at-least-once) — about one partition per worker,
  so raise `partitions` to shrink that. Page partitions cannot: an
  `UPDATE` between the two snapshots can move a row to another page, where it
  would be read twice or not at all.

### `incremental`

When present, the source wraps `query` as
`SELECT * FROM (<query>) AS _ls WHERE _ls."<cursor_column>" > <watermark> ORDER BY _ls."<cursor_column>" ASC`,
so the last row of each batch carries the new high watermark (reported to the
core). See [Incremental
State](https://loadsmith-el.github.io/loadsmith/architecture/incremental-state.html).

| Field | Type | Description |
|---|---|---|
| `cursor_column` | string | Ordered, monotonically-advancing column to resume on (e.g. an `updated_at`). Must be one of the query's output columns, spelled exactly (it is quoted); any other name fails at startup. |
| `initial_value` | string | Watermark for the very first run (before any state). Omit to read everything first time. |

> **Cursor choice matters.** Use a column capturing both inserts *and* updates
> (an `updated_at` timestamp, not a bare integer PK). The boundary is compared
> with `>`; rows sharing the exact watermark plus in-flight transactions are the
> classic at-least-once hazard — pair with an idempotent destination (the
> postgres destination's `mode: merge`), or have the source subtract a safety lag.

### Type mapping

| PostgreSQL type | Arrow type | Notes |
|---|---|---|
| `SMALLINT`, `INT`, `INT4` | `Int32` | |
| `BIGINT`, `INT8` | `Int64` | |
| `REAL`, `FLOAT4` | `Float32` | |
| `DOUBLE PRECISION`, `FLOAT8` | `Float64` | |
| `BOOLEAN` | `Boolean` | |
| `DATE` | `Date32` | BC dates supported; `infinity`/`-infinity` are rejected with a clear error (map them in the query, e.g. `NULLIF(col, 'infinity')`). |
| `TIMESTAMP` | `Timestamp(unit, None)` | `unit` is `timestamp_unit` (default `ms`: sub-millisecond digits are truncated; `us` keeps them all). BC and `infinity` as for `DATE`. |
| `TIMESTAMPTZ` | `Timestamp(unit, "UTC")` | The session zone's offset (incl. local-mean-time `+HH:MM:SS`) is parsed and the value normalized to UTC, so the session `TimeZone` doesn't change the instant. |
| `BYTEA` | `Binary` | |
| `anytype[]` (arrays) | `List(inner)` | Inner type follows the same mapping; `NULL` elements preserved, a quoted `"NULL"` element is the string. One-dimensional values only (explicit bounds like `[0:2]={…}` are fine): a multi-dimensional value fails with a hint — select `array_to_json(col)` to keep its shape as JSON, or `col::text`. |
| `NUMERIC(p,s)` | `Decimal128(p, s)` | Exact; `Decimal256(p, s)` past 38 digits. A negative scale (Postgres 15+) is a whole-number decimal. |
| `NUMERIC` (no precision) | `Decimal128(38, number_scale)` | Default scale 10. A value with more decimal places or more than 38 digits, or `NaN`/`Infinity`, fails the run naming the column — nothing is rounded. Declare the type in the query instead (`col::numeric(20,4)`), or keep it as text (`col::text`) / floating point (`col::float8`). |
| `hstore` | `Utf8` (JSON) | `"k"=>"v"` → `{"k":"v"}`; null values become JSON `null`. |
| Everything else (`TEXT`, `UUID`, `TIME`, `INTERVAL`, `JSON`, …) | `Utf8` | Text as returned by the Postgres text protocol. |

Rows are read over the text protocol, so the source pins the two output
settings its decoder depends on — `DateStyle = 'ISO'` (output style only; your
DMY/MDY input order is kept) and `bytea_output = 'hex'` — after any `session`
GUCs you set. `TimeZone` is left as you configure it.

On the destination side, `Timestamp` values with a zone are written with an
explicit `+00` offset (correct for a `timestamptz` column whatever the session
`TimeZone`), and BC dates in Postgres's `YYYY-MM-DD BC` form.

### Source examples

**Minimal (dev local):**
```yaml
source:
  type: postgres
  config:
    host: localhost
    dbname: lab
    user: lab
    password: "{{ env('PG_PASSWORD') }}"
    query: "SELECT * FROM events WHERE created_at >= now() - interval '1 day'"
```

**Encrypt without certificates (`useSSL=true` equivalent):**
```yaml
source:
  type: postgres
  config:
    host: legacy-db.internal
    dbname: billing
    user: loadsmith_reader
    password: "{{ env('PG_PASSWORD') }}"
    query: "SELECT * FROM invoices WHERE status = 'closed'"
    tls:
      mode: require
```

**Managed cloud DB, strict TLS, session tuning:**
```yaml
source:
  type: postgres
  config:
    host: prod-db.cluster-xyz.us-east-1.rds.amazonaws.com
    dbname: analytics
    user: loadsmith_reader
    password: "{{ env('PG_PASSWORD') }}"
    query: "SELECT * FROM orders WHERE updated_at >= '2026-01-01'"
    application_name: loadsmith-orders-export
    connect_timeout: 10s
    statement_timeout: 30m
    tls:
      mode: verify-full
      root_cert: "{{ file('/etc/ssl/certs/rds-ca-bundle.pem') }}"
    channel_binding: require
    session:
      work_mem: 256MB
      lock_timeout: 30s
```

**HA cluster, replicas, mTLS, consistent snapshot:**
```yaml
source:
  type: postgres
  config:
    host: ["pg-node-a.internal", "pg-node-b.internal", "pg-node-c.internal"]
    port: 5432
    dbname: warehouse
    user: loadsmith_replica_reader
    password: "{{ env('PG_PASSWORD') }}"
    query: "SELECT * FROM shipments_view"
    target_session_attrs: read-only
    transaction_isolation: repeatable-read
    tls:
      mode: verify-ca
      root_cert:   "{{ file('/etc/ssl/certs/internal-ca.crt') }}"
      client_cert: "{{ file('/etc/ssl/certs/loadsmith-client.crt') }}"
      client_key:  "{{ file('/etc/ssl/private/loadsmith-client.key') }}"
    tcp_keepalives:
      idle: 30s
      interval: 10s
      retries: 3
    batch_size: 5000
```

## Destination

Writes Arrow batches via `COPY` (text format, symmetric with the source's
text-protocol read path so `NUMERIC`/`TIME`/`TIMESTAMP` round-trip cleanly).

Accepted Arrow input types: every integer width (signed and unsigned),
`Float16`/`Float32`/`Float64` (NaN and ±Infinity included), `Decimal128` and
`Decimal256` (exact text), `Boolean`, `Utf8`/`LargeUtf8`/`Utf8View`,
`Binary`/`LargeBinary`/`FixedSizeBinary`/`BinaryView` (as `bytea`),
`Date32`/`Date64`, `Timestamp` in **every unit** (`s`/`ms`/`us`/`ns`) with or
without a zone, `Time32`/`Time64` in every unit, and `List`/`LargeList`/
`FixedSizeList` of any of these, written as Postgres arrays (a nested list
becomes a multi-dimensional array, which Postgres requires to be rectangular).
A zoned timestamp is written as its UTC instant with an explicit `+00` offset
(right for a `timestamptz` column whatever the session `TimeZone`); BC dates
use Postgres's `YYYY-MM-DD BC` form; nanoseconds are rounded to Postgres'
microseconds. Any other type (struct, map, …) fails the load.

| Field | Type | Required | Description |
|---|---|---|---|
| connection fields | — | yes (host/port default `localhost`/`5432`) | The shared connection block above, including `tls`. |
| `target_table` | string | yes | Destination table, optionally schema-qualified (`public.events`). Used verbatim in SQL, so quote mixed-case names yourself (`'"MyTable"'`). Must already exist. |

Each incoming column goes to the target column spelled the same, or else to
the one column whose name differs only in case: an Oracle source's
`EVENT_SEQUENCE` loads `event_sequence`. A name matching two columns ignoring
case (`"Id"` and `id`), or none, fails at the first batch with the target's
columns listed. `merge_key` and `update_columns` are matched the same way.
| `mode` | `append` / `overwrite` / `merge` | no (default `append`) | How the load changes the table (below). |
| `merge_key` | list of strings | for `merge` | The key columns the `MERGE` matches on (the table's PK / natural key). Only valid with `mode: merge`. |
| `update_columns` | list of strings | no | `merge` only: the columns a matched row updates. Default: every incoming column that isn't a key. `[]` inserts new keys and leaves existing rows alone. |

Every mode runs in **one transaction**, committed at the end of the load:
nothing is visible before, and a failed or cancelled load leaves the table as
it was.

- **`append`** (default) — `COPY` straight into the target. At-least-once with
  incremental state (best for append-only tables).
- **`overwrite`** — `TRUNCATE` the target, then `COPY` into it: the table's
  contents are replaced by exactly the load's rows (a load of zero rows empties
  it). `TRUNCATE` is transactional in Postgres, so readers keep seeing the old
  rows until the commit — but it takes an `ACCESS EXCLUSIVE` lock, so they
  *wait* for the load to finish, and it fails on a table referenced by a
  foreign key.
- **`merge`** — `COPY` into a temporary staging table, then one `MERGE` by
  `merge_key` into the target (update on match, insert otherwise). **Idempotent
  by key** ⇒ exactly-once effective. Requires Postgres 15+. When one load holds
  several rows with the same key, **the last one (load order) wins** — the
  staging rows are reduced to the latest per key before the `MERGE`, which could
  not apply two rows to one target row. Rows whose key is `NULL` never match, so
  they are inserted on every run: keep key columns `NOT NULL`. (The staging
  table has an extra `_ls_seq` column: a target column with that name clashes.)

Durability is at the final `COMMIT`, so the core persists the incremental
watermark at end of run.

```yaml
destination:
  type: postgres
  config:
    host: localhost
    dbname: warehouse
    user: loadsmith_writer
    password: "{{ env('PG_PASSWORD') }}"
    target_table: public.orders
    mode: merge              # append (default) | overwrite | merge
    merge_key: [id]          # required for merge
    update_columns: [status, updated_at]   # optional; default: every non-key column
```

### Migrating from 0.3

| 0.3 | 0.4 |
|---|---|
| `NUMERIC` → `Utf8` text | an exact `Decimal128(p, s)` (`Decimal256` past 38 digits); an unconstrained `NUMERIC` is `Decimal128(38, number_scale)` and fails on a value that doesn't fit (or `NaN`/`Infinity`) — cast it in the query (`::numeric(p,s)`, `::text`, `::float8`). Parquet columns become `DECIMAL`. An incremental pipeline's next run sees the new schema: its `state:` refuses it (`on_schema_change: refuse`, the default) until the state is reset (`loadsmith state rm`) or the run uses `on_schema_change: continue`. |

### Migrating from 0.2

| 0.2 | 0.3 |
|---|---|
| `mode: atomic` | `mode: append` |
| `mode: staged_merge` | `mode: merge` |
| — | `mode: overwrite`, `update_columns`, `timestamp_unit` (new) |
| a `merge_key` with `atomic` (ignored) | an error: `merge_key` only applies to `mode: merge` |
| unknown keys ignored | rejected (or warned with `--ignore-unknown-keys`) |

The old mode spellings are rejected with the new one named:
``unknown `mode` value `atomic` (did you mean `append`?)``.

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
| `connect` | each connection (TCP, TLS, authentication, session setup) | `db.system.name` = `postgresql`, `db.namespace`, `server.address`, `server.port`, `loadsmith.tls.mode` |
| `SET`, `PREPARE`, `BEGIN`, `DECLARE`, `COMMIT` | source: session pinning, schema inference, opening the cursor, closing it | statement attributes below |
| `FETCH` | source: once per batch | `db.response.returned_rows` |
| `BEGIN`, `COMMIT` | destination: the load's transaction | |
| `TRUNCATE <table>` | destination, `mode: overwrite` | |
| `CREATE TABLE _ls_staging` | destination, `mode: merge` | |
| `COPY <table>` | destination: once per batch (into the staging table for `merge`) | `loadsmith.db.affected_rows` |
| `MERGE <table>` | destination, `mode: merge`, at the end | `loadsmith.db.affected_rows` |

Every statement span carries `db.system.name`, `db.namespace` (the `dbname`),
`db.collection.name` (when it targets a table), `db.operation.name` and
`db.query.text`; a failure marks it `error` with `error.type` and
`db.response.status_code` (the server's error code). Metric:
`db.client.operation.duration` (histogram, seconds; by `db.system.name`,
`db.namespace`, `db.operation.name`, `db.collection.name`, `error.type`).

`db.query.text` is only the statement the plugin generates or runs — with
placeholders, and the incremental watermark shown as `?` — never a bound value
or row data. One span per statement, batch or merge phase, never per row.
