# Parquet (`type: parquet`)

A destination that writes Apache Parquet files to a local directory, with a
configurable compression codec and optional size-based file splitting. Files are
streamed to disk one row group at a time — the whole dataset is never buffered
in memory, so a multi-gigabyte export is safe.

- Destination binary: `loadsmith-destination-parquet`

> Parquet is a **file-output** destination: it advertises the `object_output`
> capability, so it can be paired with a [`sink`](./local-copy.md) to deliver
> the finished files to a remote target (format and location stay separate).

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `path` | string | yes | — | Output **directory** (must already exist — files are created inside it). |
| `prefix` | string | yes | — | Filename prefix, e.g. `events`. |
| `compression` | `snappy` / `gzip` / `zstd` / `lz4` / `uncompressed` | no | `snappy` | Compression codec. |
| `max_file_size` | size string | no | *(single file)* | Size cap per file. Omit ⇒ a single file. |

```yaml
destination:
  type: parquet
  config:
    path: /output           # directory (must exist)
    prefix: events          # filename prefix
    compression: snappy     # optional, default: snappy
    max_file_size: "500KiB" # optional — omit for a single file
```

## `max_file_size` syntax

A Docker-style human size string: `"500KiB"`, `"10MB"`, `"2GiB"`. Binary
suffixes (`KiB`/`MiB`/`GiB`) are 1024-based; SI suffixes (`kB`/`MB`/`GB`) are
1000-based. A **bare number** (no suffix, e.g. `"500"`) is interpreted as
**KiB** — a raw-bytes default would be nonsensical for Parquet, whose per-file
footer overhead alone is multiple KiB. The minimum accepted value is **64 KiB**;
smaller values are **rejected** (not silently clamped). The 64 KiB floor is a
schema-agnostic sanity guardrail, not a hard Parquet limit.

## File naming

The codec is always embedded in the name so files are self-describing:

- **Single file** (`max_file_size` omitted): `<prefix>.<compression>.parquet` —
  e.g. `events.snappy.parquet`.
- **Split** (`max_file_size` set): `<prefix>.<sequence>.<compression>.parquet`
  with an 8-digit zero-padded sequence starting at `1` — e.g.
  `events.00000001.snappy.parquet`, `events.00000002.snappy.parquet`, … Once a
  file passes the size cap it is finalized and the next one is opened.

## Behavior notes

- **Zero rows ⇒ no file.** The writer opens lazily on the first batch, so a run
  that yields no rows writes nothing, and a sink has nothing to deliver.
- **Cancel removes the run's files.** When the core cancels the run, every file
  the run created in `path` is removed, including chunks already finalized
  (copies a sink already delivered elsewhere are not touched).
- **`max_file_size` is a rollover threshold**, checked after each batch is
  written, so a file can exceed it by up to one batch.
