# Oracle (`type: oracle`)

The `oracle` connector provides **both** a source and a destination on Oracle's
official pure-Rust thin driver, [`oracledb`](https://github.com/oracle/rust-oracledb).
It speaks Oracle Net itself, so there's **nothing to install besides the
plugin**: no Instant Client, no `libclntsh`, no `LD_LIBRARY_PATH`, no OTN
license, and the same build runs on `linux/amd64` and `linux/arm64`. TLS goes
through rustls with the same `rustls-rustcrypto` provider as postgres/mysql.

- Source binary: `loadsmith-source-oracle`
- Destination binary: `loadsmith-destination-oracle`

Supported servers: Oracle Database 12c, 18c, 19c, 21c and 23ai/26ai (what the
driver supports; the lab validates against Oracle Free).

## Connection fields (shared)

| Field | Type | Default | Description |
|---|---|---|---|
| `host` | string | `"localhost"` | Server hostname. With TLS it must match the server certificate's SAN. |
| `port` | integer | `1521` | Listener port (typically `2484` for TCPS). |
| `service_name` | string | — | Service name (e.g. `FREEPDB1`). Either this or `sid`. |
| `sid` | string | — | Legacy SID, as an alternative to `service_name`. Ignored if `service_name` is set. |
| `user` | string | — | Login user. |
| `password` | string | — | Login password. Supports `{{ env(...) }}`. |
| `connect_timeout` | duration | `10s` | Connection establishment timeout. Whole seconds only (sub-second values round down, minimum 1s). |
| `tls` | object | — | The shared [`tls` block](./overview.md#the-tls-block), limited to what the thin driver supports (see [TLS](#tls-tcps) below). |

Every session pins `NLS_NUMERIC_CHARACTERS = '.,'`, and temporal values cross
the connector through explicit `TO_CHAR`/`TO_*` formats, so data is
NLS-independent whatever the server or account defaults are. (`NLS_LANG` is
ignored, as with every thin driver.)

## Source

| Field | Type | Default | Description |
|---|---|---|---|
| `query` | string | — | The SQL to extract. |
| `batch_size` | integer | `1000` | Rows per Arrow batch. |
| `incremental` | object | — | `cursor_column` (+ optional `initial_value`). The core persists the high watermark; the source resumes `> watermark ORDER BY` it. `cursor_column` must be one of the query's columns (a plain name folds to upper case, like unquoted SQL); any other name fails at startup. Requires a pipeline `state:` block. See [Incremental State](https://loadsmith-el.github.io/loadsmith/architecture/incremental-state.html). |
| `timestamp_unit` | `s` / `ms` / `us` / `ns` | `ms` | Arrow unit of `DATE`/`TIMESTAMP` columns. The default is the same in every canonical plugin and truncates below the millisecond; **`ns` keeps `TIMESTAMP(9)` whole** (years 1677–2262 only), `us` covers the default `TIMESTAMP(6)`. |

| Oracle type | Arrow type | Notes |
|---|---|---|
| `NUMBER` | `Utf8` | Exact decimal text, any precision. |
| `BINARY_FLOAT` / `BINARY_DOUBLE` | `Float32` / `Float64` | |
| `BOOLEAN` (23ai+) | `Boolean` | |
| `DATE` | `Timestamp(unit, None)` | Oracle `DATE` carries a time of day. |
| `TIMESTAMP` | `Timestamp(unit, None)` | Digits finer than `unit` are truncated. |
| `TIMESTAMP WITH TIME ZONE`, `… WITH LOCAL TIME ZONE` | `Timestamp(unit, "UTC")` | Normalized to the UTC instant. The original offset/region is not kept: select `TO_CHAR(col, 'YYYY-MM-DD HH24:MI:SS.FF9 TZR')` to carry it as text. |
| `RAW`, `LONG RAW`, `BLOB` | `Binary` | |
| everything else (`VARCHAR2`, `CHAR`, `CLOB`, `INTERVAL`, `ROWID`, …) | `Utf8` | |

Temporal columns are rendered server-side with `TO_CHAR` and fixed formats, so
values are NLS-independent. `INTERVAL` and `LONG` go through `TO_CHAR` too.
CLOBs are read through `TO_CHAR`, so values over 4000 bytes aren't supported
yet. `BFILE` is not supported (its contents are not fetched). Years BC are not
supported.

The source buffers the whole result set in memory before emitting batches;
`batch_size` only sets the Arrow batch size.

```yaml
source:
  type: oracle
  config:
    host: db.example.com
    port: 1521
    service_name: ORCLPDB1      # or `sid: ORCL`
    user: app
    password: "{{ env('ORACLE_PASSWORD') }}"
    query: "SELECT * FROM events ORDER BY event_sequence"
    batch_size: 2000            # default 1000
    incremental:
      cursor_column: event_sequence
      initial_value: "0"        # optional first-run watermark
```

## Destination

| Field | Type | Default | Description |
|---|---|---|---|
| `target_table` | string | — | Table to load, in the login user's schema (schema-qualified names aren't supported yet). A plain identifier folds to upper case like unquoted SQL (`events` → `EVENTS`); anything else (mixed symbols, spaces) is used verbatim as a quoted name. `merge_key` columns resolve the same way. |
| `mode` | `append` / `overwrite` / `merge` | `append` | How the load changes the table (below). |
| `merge_key` | list of strings | — | The key columns of the `MERGE … ON (…)` match. Required for `merge`, rejected in the other modes. |
| `update_columns` | list of strings | every non-key column | `merge` only: the columns a matched row updates. `[]` inserts new keys and leaves existing rows alone. |

Every mode runs in **one transaction**, committed at the end of the load.

- **`append`** (default) — array `INSERT` straight into the target.
- **`overwrite`** — `DELETE FROM` the target, then array `INSERT`: the contents
  are replaced by exactly the load's rows, atomically. (`DELETE`, not
  `TRUNCATE`: `TRUNCATE` is DDL and commits implicitly, so it could not roll
  back. Deleting generates undo, so it is slower on big tables.)
- **`merge`** — loads a global temporary staging table (`LS_MRG_<hash>`, created
  once per target and reused; `ON COMMIT DELETE ROWS`), then runs one set-based
  `MERGE` into the target. Idempotent by key ⇒ exactly-once effective. When one
  load holds several rows with the same key, **the last one (load order) wins**
  — the staging rows are reduced to the latest per key first, so the `MERGE`
  never raises ORA-30926. Rows whose key is `NULL` never match: keep key columns
  `NOT NULL`. The staging table mirrors the target's columns at creation (plus
  `_LS_SEQ`); after altering the target, drop `LS_MRG_<hash>` so it is
  recreated.

Accepted Arrow input types: every integer width (signed and unsigned), floats,
`Decimal128`/`Decimal256`, `Boolean`, the string and binary types, `Date32`/
`Date64`, `Timestamp` in **every unit** with or without a zone, and
`Time32`/`Time64` (bound as `HH:MM:SS[.f]` text, e.g. for a `VARCHAR2`).
Temporal values are spelled for the target column: whole seconds for a `DATE`,
nine fraction digits for a `TIMESTAMP`, and the UTC instant with `+00:00` for a
`TIMESTAMP WITH (LOCAL) TIME ZONE` (a timestamp without a zone is taken as UTC).

```yaml
destination:
  type: oracle
  config:
    host: db.example.com
    service_name: ORCLPDB1
    user: app
    password: "{{ env('ORACLE_PASSWORD') }}"
    target_table: events_copy
    mode: merge                # append (default) | overwrite | merge
    merge_key: [id]
```

Every key is checked, the flattened connection fields and the `tls` block
included: an unknown or misspelled key fails the run (`UNKNOWN_CONFIG_KEYS`)
unless the run uses `--ignore-unknown-keys`. Enum values are kebab-case; any
other spelling is rejected with the valid values.

### Migrating from 0.3

| 0.3 | 0.4 |
|---|---|
| `mode: atomic` | `mode: append` |
| `mode: staged_merge` | `mode: merge` (staging table renamed `LS_STG_*` → `LS_MRG_*`; the old one can be dropped) |
| a duplicate key within one load failed the `MERGE` (ORA-30926) | the last row of the key wins |
| `DATE`/`TIMESTAMP`/`TIMESTAMP WITH TZ` → text (`FF6`) | `Timestamp(ms)` / `Timestamp(ms, "UTC")`; `timestamp_unit` for finer units |
| unknown keys ignored (except in `tls`) | rejected (or warned with `--ignore-unknown-keys`) |

## TLS (TCPS)

The `tls` block has the same shape as postgres and mysql (`mode`, `root_cert`,
`client_cert`, `client_key`), but the thin driver always verifies the
certificate chain **and** the host name, so only two modes work:

| `mode` | What it does |
|---|---|
| `disable` (default) | Plaintext `PROTOCOL=TCP`. |
| `verify-full` | `PROTOCOL=TCPS`. Chain and host name are verified against `root_cert` plus the public webpki roots. `root_cert` is optional: leave it out when the server certificate comes from a public CA. |
| `prefer` / `require` / `verify-ca` | Rejected at startup: the driver can't skip verification. |

`client_cert` / `client_key` (mTLS) are rejected for now: the driver can't
combine a client identity with a private CA yet (oracle/rust-oracledb#54).

The `host` you connect to **must match the certificate's SAN**. `root_cert` is
inline PEM, like the other connectors; load it with `{{ file(...) }}`. The
connector writes it to a private temporary directory for the driver
(`ewallet.pem`, mode 0600) and deletes it right after connecting. An auto-login
binary wallet (`cwallet.sso`) can't be used directly: export its trusted
certificate to PEM once (e.g. `orapki wallet export`).

```yaml
tls:
  mode: verify-full
  root_cert: "{{ file('/etc/ssl/oracle-ca.pem') }}"   # omit for a public CA
```

> **Migrating from v0.2.** v0.2 used an Oracle-specific block
> (`enabled` / `wallet_dir` / `server_dn_match` / `distinguished_name`). Those
> keys are now rejected, so a stale block fails at startup instead of silently
> connecting in plaintext. Replace `enabled: true` + `wallet_dir: /w` with
> `mode: verify-full` + `root_cert: "{{ file('/w/ewallet.pem') }}"`.

## Known limitations (driver beta)

`oracledb` is a **26.0.0 beta**; the plugin pins an exact version.

- **10G password verifier.** Accounts that still carry only the legacy 10G
  verifier fail with `ORA-01017` even with the right password. Check
  `SELECT password_versions FROM dba_users WHERE username = '…'` and re-set the
  password (`ALTER USER … IDENTIFIED BY …`) to issue 11G/12C verifiers
  (oracle/rust-oracledb#6).
- **TLS modes.** Only `disable` and `verify-full` (see above); no mTLS yet
  (oracle/rust-oracledb#54).
- **Build-time C.** The driver pulls rustls with default features, so
  `aws-lc-sys` is compiled (it needs `cmake` and a C compiler) even though the
  runtime crypto is `rustls-rustcrypto` (oracle/rust-oracledb#34).

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
| `connect` | each connection (TCP, TLS, authentication, session setup) | `db.system.name` = `oracle.db`, `db.namespace`, `server.address`, `server.port`, `loadsmith.tls.mode` |
| `SELECT` | source: the column describe, then the query (execution and fetch of every row) | `db.response.returned_rows` |
| `SELECT <table>` | destination: describing the target table | |
| `DELETE <table>` | destination, `mode: overwrite` | `loadsmith.db.affected_rows` |
| `CREATE TABLE LS_MRG_…` | destination, `mode: merge` (create-if-missing) | |
| `INSERT <table>` | destination: once per batch (the array insert; into the staging table for `merge`) | `db.operation.batch.size`, `loadsmith.db.affected_rows` |
| `MERGE <table>` | destination, `mode: merge`, at the end | `loadsmith.db.affected_rows` |
| `COMMIT` | destination | |

Every statement span carries `db.system.name`, `db.namespace` (the `service_name`, else the `sid`),
`db.collection.name` (when it targets a table), `db.operation.name` and
`db.query.text`; a failure marks it `error` with `error.type` and
`db.response.status_code` (the server's error code). Metric:
`db.client.operation.duration` (histogram, seconds; by `db.system.name`,
`db.namespace`, `db.operation.name`, `db.collection.name`, `error.type`).

`db.query.text` is only the statement the plugin generates or runs — with
placeholders, and the incremental watermark shown as `?` — never a bound value
or row data. One span per statement, batch or merge phase, never per row.
