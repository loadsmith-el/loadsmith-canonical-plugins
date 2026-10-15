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
| `sid` | string | — | Legacy SID, as an alternative to `service_name`. |
| `user` | string | — | Login user. |
| `password` | string | — | Login password. Supports `{{ env(...) }}`. |
| `connect_timeout` | duration | `10s` | Connection establishment timeout. |
| `tls` | object | — | Oracle's own TLS block (see [TLS](#tls-tcps) below). It is **not** the shared `mode`-based [`tls` block](./overview.md#the-tls-block). |

Every session pins `NLS_NUMERIC_CHARACTERS = '.,'`, and temporal values cross
the connector through explicit `TO_CHAR`/`TO_*` formats, so data is
NLS-independent whatever the server or account defaults are. (`NLS_LANG` is
ignored, as with every thin driver.)

## Source

| Field | Type | Default | Description |
|---|---|---|---|
| `query` | string | — | The SQL to extract. |
| `batch_size` | integer | `1000` | Rows per Arrow batch. |
| `incremental` | object | — | `cursor_column` (+ optional `initial_value`). The core persists the high watermark; the source resumes `> watermark ORDER BY` it. Requires a pipeline `state:` block. See [Incremental State](https://loadsmith-el.github.io/loadsmith/architecture/incremental-state.html). |

`NUMBER` crosses as exact text (`Utf8`), `BINARY_FLOAT`/`BINARY_DOUBLE` as
floats, `DATE`/`TIMESTAMP*` as formatted text, `RAW`/`LONG RAW`/`BLOB` as
`Binary`, and everything else (`VARCHAR2`, `CHAR`, `CLOB`, …) as `Utf8`. CLOBs
are read through `TO_CHAR`, so values over 4000 bytes aren't supported yet.

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
| `mode` | `atomic` / `staged_merge` | `atomic` | `atomic`: array `INSERT` in one transaction, committed at the end. `staged_merge`: loads a global temporary staging table, then runs one set-based `MERGE` into the target (upsert by `merge_key`). |
| `merge_key` | list of strings | `[]` | Key columns for `staged_merge`. Required in that mode. |

```yaml
destination:
  type: oracle
  config:
    host: db.example.com
    service_name: ORCLPDB1
    user: app
    password: "{{ env('ORACLE_PASSWORD') }}"
    target_table: events_copy
    mode: staged_merge
    merge_key: [id]
```

## TLS (TCPS)

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | `false` | Connect with `PROTOCOL=TCPS`. |
| `wallet_dir` | string | — | Directory holding `ewallet.pem`: the trusted CA certificate(s) in PEM. Not needed when the server certificate chains to a public root. |
| `server_dn_match` | bool | `true` | Accepted for compatibility; not enforced yet (see below). |
| `distinguished_name` | string | — | Accepted for compatibility; not enforced yet. |

The server certificate is verified against the Mozilla/webpki roots **plus**
every certificate in `<wallet_dir>/ewallet.pem`, and the hostname is always
checked against the SAN. For a private CA, a plain `ca.crt` renamed to
`ewallet.pem` works; you don't need `orapki`. An auto-login binary wallet
(`cwallet.sso`) is **not** read: export its trusted certificate to PEM once
(e.g. `orapki wallet export`).

```yaml
tls:
  enabled: true
  wallet_dir: /wallet           # holds ewallet.pem with your CA
```

## Known limitations (driver beta)

`oracledb` is a **26.0.0 beta**; the plugin pins an exact version.

- **10G password verifier.** Accounts that still carry only the legacy 10G
  verifier fail with `ORA-01017` even with the right password. Check
  `SELECT password_versions FROM dba_users WHERE username = '…'` and re-set the
  password (`ALTER USER … IDENTIFIED BY …`) to issue 11G/12C verifiers
  (oracle/rust-oracledb#6).
- **DN pinning.** `server_dn_match` / `distinguished_name` aren't enforced; the
  driver does full hostname verification instead (oracle/rust-oracledb#25).
- **Build-time C.** The driver pulls rustls with default features, so
  `aws-lc-sys` is compiled (it needs `cmake` and a C compiler) even though the
  runtime crypto is `rustls-rustcrypto` (oracle/rust-oracledb#34).
