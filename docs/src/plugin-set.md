# The Plugin Set

Each member of the workspace is a **plugin package** — a "connector" when it
provides more than one role (for example, `postgres` ships *both* a source and a
destination).

| Package | Provides | Notes |
|---|---|---|
| `postgres` | source + destination | Server-side cursor source with incremental watermark (`cursor_column`); `atomic` / `staged_merge` destination. TLS via rustls. |
| `mysql` | source + destination | Source with incremental watermark (`cursor_column`; buffers the result set in memory); `atomic` / `staged_merge` (ON DUPLICATE KEY) destination. TLS via rustls. |
| `oracle` | source + destination | Oracle's pure-Rust `oracledb` thin driver (no Instant Client); source with incremental watermark (`cursor_column`); `atomic` (array INSERT) / `staged_merge` (staging GTT + one `MERGE`) destination. TLS (TCPS) via rustls. |
| `sharepoint` | source | Microsoft Graph: Excel/CSV file download or SharePoint List items, with in-plugin tabular transforms (filter/normalize/fix-merged/drop-empty). TLS via rustls. |
| `jsonl` | destination | Newline-delimited JSON output. |
| `parquet` | destination | Single-file or chunked Parquet output. |
| `null` | destination | Discards rows — for read/pump throughput testing. |
| `local-copy` | sink | Delivers staged files to a local destination directory. |
| `file` | source + provider | Raw source: lists the files of a local directory (glob, optional recursion) for a parser, read in place. Provider: resolves `secret()` values from local files (a whole file, or one key of a JSON/YAML document). |
| `csv` | parser | Streams CSV files into Arrow: delimiter, encoding, header, type inference from a bounded sample or declared columns. |
| `excel` | parser | One sheet of each xlsx/xlsm/xlsb/xls/ods workbook into Arrow: sheet by name or index, `skip_rows`, inference or declared columns. |

> **Configuring a plugin?** Every plugin's `config:` fields — connection
> options, TLS, commit modes, incremental, output paths — are documented in the
> [Configuration Reference](./config/overview.md).

## Roles, briefly

Loadsmith plugins fall into five kinds:

- **source** — extracts rows and emits them as Apache Arrow batches. A *raw*
  source (capability `object_output`, e.g. `file`) produces files instead,
  which a parser turns into rows.
- **parser** — turns each file a raw source announces into Arrow batches
  (`csv`, `excel`), so one format parser serves every raw source and one raw
  source every format. Declared under `parser:`; binary
  `loadsmith-parser-<type>`.
- **destination** — receives Arrow batches and writes them out (to a database,
  files, or `/dev/null`).
- **sink** — takes *staged objects* a destination produced and delivers them to
  their final location, separately from the format. This is why there's no
  `s3_parquet` explosion: `parquet` (format) + a sink (location) compose.
- **provider** — not a data stage: it resolves the
  `{{ secret('<instance>', '<ref>') }}` values of a pipeline file (from a
  mounted file, a secret store…) before any data plugin starts. Declared under
  `providers:`; binary `loadsmith-provider-<type>`.

For the full design of each role — the protocol they speak, the lifecycle, and
how to author one from scratch — see the engine docs under
[Writing Plugins](https://loadsmith-el.github.io/loadsmith/plugins/writing-a-source.html).

## The `postgres` connector is the reference

`postgres` is the canonical example of a multi-role plugin and the template to
copy when adding a new one:

- one crate, `src/lib.rs` plus shared low-level modules (connection, type
  mapping, copy);
- two binaries — `src/bin/source.rs` and `src/bin/destination.rs`;
- source and destination stay **separate binaries** that share modules, *not* a
  merged read/write driver (see the engine's *Rejected Ideas*).

See [Plugin Anatomy](./anatomy.md) for the rules every plugin must follow.

## Shared code: `tabular`

`tabular` is a workspace **library**, not a plugin: no binary, no
`[package.metadata.loadsmith]`, never released. It holds the CSV/Excel
reading, the cell table and its type inference, and the declared-`columns:`
handling that `sharepoint`, `csv` and `excel` share, so the three read files
by the same rules.
