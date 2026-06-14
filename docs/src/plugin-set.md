# The Plugin Set

Each member of the workspace is a **plugin package** — a "connector" when it
provides more than one role (for example, `postgres` ships *both* a source and a
destination).

| Package | Provides | Notes |
|---|---|---|
| `postgres` | source + destination | Server-side cursor source; `atomic` / `staged_merge` destination. TLS via rustls. |
| `jsonl` | destination | Newline-delimited JSON output. |
| `parquet` | destination | Single-file or chunked Parquet output. |
| `null` | destination | Discards rows — for read/pump throughput testing. |
| `local-copy` | sink | Delivers staged files to a local destination directory. |
| `file` | config-provider | Loads pipeline config from a file. |

## Roles, briefly

Loadsmith plugins fall into four kinds, each a distinct stage in a pipeline:

- **source** — extracts rows and emits them as Apache Arrow batches.
- **destination** — receives Arrow batches and writes them out (to a database,
  files, or `/dev/null`).
- **sink** — takes *staged objects* a destination produced and delivers them to
  their final location, separately from the format. This is why there's no
  `s3_parquet` explosion: `parquet` (format) + a sink (location) compose.
- **config-provider** — supplies pipeline configuration to the core.

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
