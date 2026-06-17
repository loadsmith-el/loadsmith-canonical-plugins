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
| `tls` | object | — | The shared [`tls` block](./overview.md#the-tls-block). MySQL maps it onto `mysql_async`'s `SslOpts`: `require`/`prefer` encrypt without verification; `verify-ca`/`verify-full` verify against `root_cert` (inline PEM); plus mTLS via `client_cert`/`client_key`. |
| `statement_timeout` | duration | — | Applied as the session `max_execution_time`. |
| `session` | map | — | `SET SESSION <k> = <v>` applied on connect. |
| `tcp_keepalive` | duration | — | TCP keepalive idle time. |

## Source

| Field | Type | Default | Description |
|---|---|---|---|
| `query` | string | — | The SQL to extract. Schema is inferred from a prepared statement. |
| `batch_size` | integer | `1000` | Rows per Arrow batch. |
| `incremental` | object | — | `cursor_column` (+ optional `initial_value`). The core persists the high watermark; the source resumes `> watermark ORDER BY` it. Requires a pipeline `state:` block. See [Incremental State](https://loadsmith-el.github.io/loadsmith/architecture/incremental-state.html). |

Like the postgres source, it reads over the text protocol, so text-rendered
types round-trip losslessly to Arrow `Utf8`: `TINYINT`/`SMALLINT`/`INT` →
`Int32`, `BIGINT` → `Int64`, `FLOAT`/`DOUBLE` → float, `DATE` → `Date32`,
`DATETIME`/`TIMESTAMP` → `Timestamp(ms)`, `BLOB`/binary-collation strings →
`Binary`, everything else (`DECIMAL`, `TIME`, …) → `Utf8`.

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
    tls:
      mode: require            # disable | prefer | require | verify-ca | verify-full
    incremental:
      cursor_column: updated_at
      initial_value: "2026-01-01 00:00:00"   # optional first-run watermark
```

## Destination

Writes Arrow batches via batched, parameter-bound `INSERT`. The target table
must already exist.

| Field | Type | Required | Description |
|---|---|---|---|
| connection fields | — | yes (port default `3306`) | The shared connection block above, including `tls`. |
| `target_table` | string | yes | Destination table. Must already exist. |
| `mode` | `atomic` / `staged_merge` | no (default `atomic`) | Commit strategy (below). |
| `merge_key` | list of strings | for `staged_merge` | Columns **excluded** from the upsert's `UPDATE` set. (Conflict detection keys off the table's PRIMARY/UNIQUE index — MySQL semantics.) |

**`atomic`** — one transaction: `INSERT` straight into the target, `COMMIT` at
the end. All-or-nothing, at-least-once.

**`staged_merge`** — `INSERT` into a temporary staging table, then
`INSERT … SELECT … ON DUPLICATE KEY UPDATE` into the target in the same
transaction. Idempotent by the target's unique key ⇒ exactly-once effective.

```yaml
destination:
  type: mysql
  config:
    host: localhost
    dbname: warehouse
    user: loadsmith_writer
    password: "{{ env('MYSQL_PASSWORD') }}"
    target_table: orders
    mode: staged_merge       # "atomic" (default) | "staged_merge"
    merge_key: [id]
```
