# SharePoint (`type: sharepoint`)

The `sharepoint` connector is a **source** that reads from Microsoft 365 via the
Microsoft Graph API: either an Excel/CSV **file** from a document library, or the
items of a **SharePoint List**. It applies optional in-plugin tabular transforms
and emits Arrow. It is source-only — SharePoint is never written back.

- Source binary: `loadsmith-source-sharepoint`

HTTPS to Graph uses `rustls` with the pure-Rust `rustls-rustcrypto` provider (the
same multi-arch posture as the other connectors — no `ring`/`aws-lc-rs`) and
webpki roots.

> **Why a transform stage here?** Loadsmith's engine is EL (extract-load), with no
> transform stage. SharePoint spreadsheets and lists routinely need light tabular
> cleanup (un-merging cells, normalizing column names) *at extract time*, so these
> transforms are an **internal concern of this connector**, configured under its
> own `transform:` list — not a new engine stage.

## Authentication & target

Uses the OAuth 2.0 **client-credentials** grant (an app registration with
`Sites.Read.All` / `Files.Read.All` application permissions).

| Field | Type | Default | Description |
|---|---|---|---|
| `tenant_id` | string | — | Entra (Azure AD) tenant id. |
| `client_id` | string | — | App registration (client) id. |
| `client_secret` | string | — | App client secret. Supports `{{ env(...) }}`. |
| `site` | string | — | Site by `host/server-relative-path` (e.g. `contoso.sharepoint.com/sites/Team`); resolved to an id via Graph. Provide this **or** `site_id`. |
| `site_id` | string | — | Pre-resolved site id (skips lookup). |
| `drive` | string | — | Document library by display name; resolved via Graph. File mode only. Provide this **or** `drive_id`. |
| `drive_id` | string | — | Pre-resolved drive id (skips lookup). File mode only. |
| `graph_base_url` | string | `https://graph.microsoft.com` | Graph base URL. Override to point tests at a mock. Sovereign clouds are not supported yet: the token scope is fixed to the global Graph (`https://graph.microsoft.com/.default`). |
| `authority_url` | string | `https://login.microsoftonline.com` | Login authority base URL. |
| `batch_size` | integer | `1000` | Rows per Arrow batch. |
| `input` | object | — | The input mode (below). |
| `transform` | list | `[]` | Transform chain (below), applied in order. |

## Input modes

### `file` — download and parse

```yaml
input:
  kind: file
  file: "/Reports/positions.xlsx"   # drive path
  parser:
    kind: excel                     # excel | csv
    sheet: "DATA"                   # excel: sheet name (default: first sheet)
    header: true                    # first row is the header (default true)
    skip_rows: 10                   # excel: rows to skip before the header (default 0)
```

The excel parser reads `.xlsx` only (not `.xls`/`.xlsb`/`.ods`). Excel numbers
are IEEE doubles in the file format, so numeric columns infer to Arrow
`Float64`; text → `Utf8`, booleans → `Boolean`, date/datetime cells →
`Timestamp(ms)` (never `Date32`).

CSV parser options: `delimiter` (a single-byte character, default `,`),
`encoding` (any WHATWG / `encoding_rs` label, default `utf-8`), `header`
(default true). CSV inference is simpler: a trimmed integer → `Int64`, a float →
`Float64`, blank/whitespace → null, anything else → `Utf8` (no boolean or date
inference).

With `header: false` (Excel or CSV), columns are named `col_0`, `col_1`, …

### `list` — read SharePoint List items

```yaml
input:
  kind: list
  list: "Tasks"     # list display name or id
```

Items are read with `$expand=fields`, following `@odata.nextLink` pagination.
Columns are the union of item field keys in first-seen order (Graph `@odata.*`
annotations are dropped); no parser is involved. Date fields arrive as Graph
ISO strings and stay `Utf8`.

## Transforms

Applied in order to the fully-materialized table before it is emitted. Transforms
are source-agnostic (they run on both `file` and `list` data); the notes below are
guidance, not enforcement.

| `kind` | Fields | Notes |
|---|---|---|
| `filter` | `where` (predicate) | Keep matching rows. Structured predicate (below). Suits both modes. |
| `normalize` | `columns` (bool, default true), `filler` (default `_`) | Normalize column names: NFKD→ASCII (strips accents), lowercase, non-alphanumeric runs → `filler`, de-duplicate. Especially useful for lists (SharePoint's internal field names are noisy, e.g. `_x0020_`). |
| `fix_merged_columns` | `column` | Forward-fill blanks in a column (un-merge merged spreadsheet cells). A **spreadsheet** concept — not meaningful for lists. |
| `drop_empty_rows` | `zero_is_empty` (bool, default false) | Drop fully-empty rows. With `zero_is_empty`, numeric/string zeros also count as empty (booleans never do). Low value on lists. |

### The `filter` predicate

`where` is a **structured predicate tree**, not an expression string — so config
errors point at the exact node (e.g. `transform[0].where.all[1].op`).

A node is either a **leaf** or a **combinator**:

- Leaf: `{ column: <name>, op: <op>, value: <v?> }` where `op` ∈ `eq`, `ne`,
  `lt`, `le`, `gt`, `ge`, `in` (value is a list), `null`, `not_null` (no value).
  `null`/`not_null` treat whitespace-only strings as blank.
- Combinator: `all: [ ... ]` (AND), `any: [ ... ]` (OR), `not: <node>`.

```yaml
transform:
  - kind: filter
    where:
      all:
        - { column: ATUACAO, op: not_null }
        - { column: area, op: in, value: [CIVEL, CRIMINAL] }
  - { kind: fix_merged_columns, column: "ENT." }
  - { kind: normalize, columns: true, filler: "_" }
```

## Full example

```yaml
source:
  type: sharepoint
  config:
    tenant_id: "{{ env('SP_TENANT_ID') }}"
    client_id: "{{ env('SP_CLIENT_ID') }}"
    client_secret: "{{ env('SP_CLIENT_SECRET') }}"
    site_id: "contoso.sharepoint.com,<guid>,<guid>"
    drive_id: "b!<drive-id>"
    input:
      kind: file
      file: "/Reports/positions.xlsx"
      parser: { kind: excel, sheet: "DATA", header: true, skip_rows: 10 }
    transform:
      - kind: filter
        where: { column: ATUACAO, op: not_null }
      - { kind: normalize, columns: true }
    batch_size: 1000
# destination/sink: reuse parquet/jsonl/local-copy (the load half is unchanged)
```
