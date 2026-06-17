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
| `password` | string | — | Login password. Supports `{{ env(...) }}`. |
| `application_name` | string | — | Appears in `pg_stat_activity`. Useful for spotting pipelines in monitoring. |
| `connect_timeout` | duration | — | Max time to establish a connection (e.g. `10s`, `500ms`). |
| `statement_timeout` | duration | — | Kills a query exceeding this wall time. Applied via `set_config`. |
| `options` | map | — | Extra GUCs in the startup message (e.g. `search_path: myschema`). |
| `tls` | object | — | The shared [`tls` block](./overview.md#the-tls-block). |
| `channel_binding` | `disable` / `prefer` / `require` | — | SCRAM channel binding. `require` ties auth to the TLS session, preventing credential relay. |
| `target_session_attrs` | `any` / `read-write` / `read-only` | — | With multi-host, which node to accept (e.g. point at a replica). |
| `session` | map | — | GUCs applied via `set_config($1,$2,false)` after connect (bind params, no SQL interpolation) — e.g. `work_mem: 256MB`. |
| `tcp_keepalives` | object | — | TCP keepalives — `idle` (duration), `interval` (duration), `retries` (int). |

## Source

| Field | Type | Default | Description |
|---|---|---|---|
| `query` | string | — | Any valid `SELECT` — joins, CTEs, window functions. |
| `batch_size` | integer | `1000` | Rows per `FETCH FORWARD n`. |
| `transaction_isolation` | `read-committed` / `repeatable-read` / `serializable` | `read-committed` | Isolation for the cursor transaction. `repeatable-read` gives a consistent snapshot across all `FETCH`es. |
| `incremental` | object | — | Activates incremental reads (below). Requires a pipeline `state:` block. |

The query runs through a server-side cursor (`DECLARE … CURSOR FOR`) in a
transaction, streamed in `batch_size` chunks, so large results never buffer
fully in memory.

### `incremental`

When present, the source wraps `query` as
`SELECT * FROM (<query>) AS _ls WHERE _ls."<cursor_column>" > <watermark> ORDER BY _ls."<cursor_column>" ASC`,
so the last row of each batch carries the new high watermark (reported to the
core). See [Incremental
State](https://loadsmith-el.github.io/loadsmith/architecture/incremental-state.html).

| Field | Type | Description |
|---|---|---|
| `cursor_column` | string | Ordered, monotonically-advancing column to resume on (e.g. an `updated_at`). Must be in the query's output. |
| `initial_value` | string | Watermark for the very first run (before any state). Omit to read everything first time. |

> **Cursor choice matters.** Use a column capturing both inserts *and* updates
> (an `updated_at` timestamp, not a bare integer PK). The boundary is compared
> with `>`; rows sharing the exact watermark plus in-flight transactions are the
> classic at-least-once hazard — pair with an idempotent destination (the
> postgres destination's `staged_merge`), or have the source subtract a safety lag.

### Type mapping

| PostgreSQL type | Arrow type | Notes |
|---|---|---|
| `SMALLINT`, `INT`, `INT4` | `Int32` | |
| `BIGINT`, `INT8` | `Int64` | |
| `REAL`, `FLOAT4` | `Float32` | |
| `DOUBLE PRECISION`, `FLOAT8` | `Float64` | |
| `BOOLEAN` | `Boolean` | |
| `DATE` | `Date32` | |
| `TIMESTAMP`, `TIMESTAMPTZ` | `Timestamp(Millisecond, None)` | |
| `BYTEA` | `Binary` | |
| `anytype[]` (arrays) | `List(inner)` | Inner type follows the same mapping; `NULL` elements preserved. |
| `hstore` | `Utf8` (JSON) | `"k"=>"v"` → `{"k":"v"}`; null values become JSON `null`. |
| Everything else (`NUMERIC`, `TEXT`, `UUID`, `TIME`, …) | `Utf8` | Text as returned by the Postgres text protocol. |

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

| Field | Type | Required | Description |
|---|---|---|---|
| connection fields | — | yes (host/port default `localhost`/`5432`) | The shared connection block above, including `tls`. |
| `target_table` | string | yes | Destination table, optionally schema-qualified. Must already exist. |
| `mode` | `atomic` / `staged_merge` | no (default `atomic`) | Commit strategy (below). |
| `merge_key` | list of strings | for `staged_merge` | Key columns for the `MERGE` (the table's PK / natural key). |

**`atomic`** — one transaction: `COPY` straight into the target, `COMMIT` at the
end. All-or-nothing; a crash leaves the target untouched. At-least-once with
incremental state (best for append-only or truncate-load tables).

**`staged_merge`** — `COPY` into a temporary staging table, then `MERGE` by
`merge_key` into the target in the same transaction. **Idempotent by key** ⇒
exactly-once effective. Requires Postgres 15+ (`MERGE`).

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
    mode: staged_merge       # "atomic" (default) | "staged_merge"
    merge_key: [id]          # required for staged_merge
```
