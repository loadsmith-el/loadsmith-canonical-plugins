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
| `path` | string | yes | — | Output **directory**, created when missing. |
| `prefix` | string | yes | — | Filename prefix, e.g. `events` (no `/`, no leading `.`). |
| `compression` | `snappy` / `gzip` / `zstd` / `lz4` / `uncompressed` | no | `snappy` | Compression codec. |
| `compression_level` | integer | no | *(the codec's default)* | `gzip`: 0–10, `zstd`: 1–22; an error with any other codec. |
| `max_file_size` | size with a unit | no | *(single file)* | Size cap per file. Omit ⇒ a single file. |
| `row_group_size` | integer | no | `131072` | Maximum rows per row group. A row group is buffered in memory until written, so this bounds the writer's memory (0.2: the `parquet` crate's 1,048,576). |

```yaml
destination:
  type: parquet
  config:
    path: /output           # directory (created if missing)
    prefix: events          # filename prefix
    compression: zstd       # optional, default: snappy
    compression_level: 9    # optional, gzip/zstd only
    max_file_size: "500KiB" # optional — omit for a single file
```

Unknown keys are rejected (or only warned about with `--ignore-unknown-keys`),
and a wrong `compression` names the valid values and the likely one (`none` →
``did you mean `uncompressed`?``).

Every Arrow type the protocol carries is written natively (timestamps keep
their unit and zone, decimals their precision and scale, lists stay lists).

## `max_file_size` syntax

A size **with a unit**: `"500KiB"`, `"10MB"`, `"2GiB"`, `"65536B"`. Binary
suffixes (`KiB`/`MiB`/`GiB`) are 1024-based; SI suffixes (`kB`/`MB`/`GB`) are
1000-based. A **bare number** (`"500"`) is rejected, like a duration without a
unit, with the spelling it used to mean (`"500KiB"`). The minimum accepted value
is **64 KiB**; smaller values are **rejected** (not silently clamped). The
64 KiB floor is a schema-agnostic sanity guardrail, not a hard Parquet limit.

## File naming

The codec is always embedded in the name so files are self-describing:

- **Single file** (`max_file_size` omitted): `<prefix>.<compression>.parquet` —
  e.g. `events.snappy.parquet`.
- **Split** (`max_file_size` set): `<prefix>.<sequence>.<compression>.parquet`
  with an (at least) 8-digit zero-padded sequence starting at `1` — e.g.
  `events.00000001.snappy.parquet`, `events.00000002.snappy.parquet`, … Once a
  file passes the size cap it is finalized and the next one is opened.

Every file is written under a hidden temp name (`.<name>.tmp`) and renamed once
its footer is written, so a reader (or a sink) never sees a partial file.

## Re-runs

A run replaces the previous run's output for the same `path` + `prefix` — the
files named as above, with any codec, and leftover temp files:

- **Single file**: the new file is renamed over the old one when the run
  completes (an atomic replace: until then the previous file stays readable),
  then other files of the prefix (a previous chunked set, another codec) are
  removed.
- **Split**: the previous set is removed when the run starts, before the first
  chunk is written (each chunk goes to the sink as soon as it is finalized, so
  the set cannot be swapped at the end). A run with fewer chunks than the last
  one leaves no stale chunks behind.

Other files in `path` are never touched, including ones whose name merely
starts with the prefix (`events2.snappy.parquet`, `events.v2.snappy.parquet`).

## Behavior notes

- **Zero rows ⇒ no file.** The writer opens lazily on the first batch, so a run
  that yields no rows writes nothing (and, as a re-run, leaves no file of the
  prefix), and a sink has nothing to deliver.
- **Cancel removes the run's files.** When the core cancels the run, every file
  the run created in `path` is removed, including chunks already finalized
  (copies a sink already delivered elsewhere are not touched).
- **`max_file_size` is a rollover threshold**, checked after each batch is
  written, so a file can exceed it by up to one batch.

## Migrating from 0.1

| 0.1 | 0.2 |
|---|---|
| `max_file_size: "500"` (KiB) | rejected: write `"500KiB"` |
| `path` must exist | created when missing |
| re-runs left stale chunks / a file being written was visible | the prefix's previous files are replaced; files appear only when complete |
| — | `compression_level`, `row_group_size` (new) |
| unknown keys ignored | rejected (or warned with `--ignore-unknown-keys`) |

## Telemetry

When the run has telemetry on (the engine's `telemetry:` block or `OTEL_*`
settings), the plugin records spans and metrics in-process and the engine
exports them over OTLP under the run's trace — the plugin itself opens no
connection for it, and records nothing when telemetry is off. Every plugin gets
the SDK's baseline: spans `configure`, `start`, `finish` and the per-batch /
per-object `loadsmith.*` spans, plus the `loadsmith.plugin.*` metrics
([engine design](https://github.com/loadsmith-el/loadsmith/blob/main/docs/src/architecture/telemetry.md)). On top of it:

| Span | When | Attributes |
|---|---|---|
| `write <file>` | each file, from its creation to its rename into place (a chunk in chunked mode) | `file.path`, `file.size`, `loadsmith.object.rows` |
| `rename` | the finished temp file moved to its final name (inside `write <file>`) | `file.path` (the temp file), `loadsmith.file.target` |
| `cleanup` | removing the previous run's dataset files | `file.directory`, `loadsmith.files.removed` |

A cancelled run marks the open file's span `error`.
