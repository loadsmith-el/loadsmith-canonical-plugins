# Configuration Reference

Every plugin in a pipeline gets a `config:` block — an arbitrary object the core
passes through verbatim (after [template
resolution](https://loadsmith-el.github.io/loadsmith/reference/pipeline-yaml.html#template-syntax))
to the plugin's `configure()`. This section documents the fields **each
canonical plugin accepts** in that block.

```yaml
source:
  type: postgres        # selects the plugin → binary loadsmith-source-postgres
  config:               # ← this object is what these pages describe
    host: localhost
    dbname: lab
    # …
```

The **top-level pipeline shape** (`pipeline` / `providers` / `source` /
`parser` / `destination` / `sink` / `state`), the **template syntax** (`{{ env(...) }}`,
`{{ file(...) }}`, `{{ secret(...) }}`), and
the core-owned **`state:` block** for incremental loads are engine concerns —
see the engine's [Pipeline YAML
reference](https://loadsmith-el.github.io/loadsmith/reference/pipeline-yaml.html).
These pages cover only what goes *inside* each plugin's `config:`.

## Per-plugin pages

| Plugin | Role(s) | Config page |
|---|---|---|
| `postgres` | source + destination | [Postgres](./postgres.md) |
| `mysql` | source + destination | [MySQL](./mysql.md) |
| `oracle` | source + destination | [Oracle](./oracle.md) |
| `sharepoint` | source | [SharePoint](./sharepoint.md) |
| `jsonl` | destination | [JSONL](./jsonl.md) |
| `parquet` | destination | [Parquet](./parquet.md) |
| `null` | destination | [Null](./null.md) |
| `local-copy` | sink | [Local-copy](./local-copy.md) |
| `file` | source + provider | [File](./file.md) |
| `csv` | parser | [CSV](./csv.md) |
| `excel` | parser | [Excel](./excel.md) |

---

## Shared building blocks

A few conventions recur across plugins. They are defined once here; the
per-plugin pages link back to them.

### The `tls` block

The database connectors (`postgres`, `mysql`, `oracle`) take an optional `tls:` block with
an identical shape — it comes from the engine's `loadsmith-tls` crate (pure-Rust
`rustls` + `rustls-rustcrypto`, no OpenSSL/`native-tls`). Omitting it means no
TLS (`mode: disable`). (`oracle` supports only `disable` and `verify-full`,
without mTLS, and `root_cert` is optional there — public CAs verify via webpki
roots; `mysql` rejects `prefer`, having no plaintext fallback; see
[Oracle](./oracle.md#tls-tcps) and [MySQL](./mysql.md).)

| Field | Type | Description |
|---|---|---|
| `mode` | `disable` / `prefer` / `require` / `verify-ca` / `verify-full` | TLS level. Default `disable`. |
| `root_cert` | string (PEM) | CA certificate for server verification. Required for `verify-ca` and `verify-full`. Load it with `{{ file(...) }}`. |
| `client_cert` | string (PEM) | Client certificate for mutual TLS (mTLS). Optional. |
| `client_key` | string (PEM) | Private key for `client_cert`. Required whenever `client_cert` is set. |

| Mode | What it does |
|---|---|
| `disable` | No TLS. |
| `prefer` | TLS if the server offers it; plaintext fallback. (`mysql`: rejected.) |
| `require` | Encrypted channel required. **No** certificate checks. (The `useSSL=true` / `sslmode=require` equivalent — no cert fields needed.) |
| `verify-ca` | Encrypt **and** verify the chain against `root_cert`. Hostname not checked. |
| `verify-full` | Encrypt **and** verify the chain *and* the server hostname. Strictest. |

```yaml
tls:
  mode: verify-full
  root_cert: "{{ file('/etc/ssl/certs/ca.pem') }}"
  # mTLS (optional):
  client_cert: "{{ file('/etc/ssl/certs/client.crt') }}"
  client_key:  "{{ file('/etc/ssl/private/client.key') }}"
```

### Duration strings

Timeout/keepalive fields accept a human-readable duration: an integer followed
by a unit suffix. Accepted units: `ms`, `s`, `m`, `h`, `d`. Examples: `"500ms"`,
`"10s"`, `"30m"`, `"2h"`, `"1d"`. A bare number with no unit is **rejected**.

### Size strings

Byte-size fields (e.g. parquet's `max_file_size`) accept a Docker-style size
string: binary suffixes `KiB`/`MiB`/`GiB` are 1024-based, SI suffixes
`kB`/`MB`/`GB` are 1000-based. See the [Parquet page](./parquet.md) for the
per-field specifics (including how a bare number is interpreted there).
