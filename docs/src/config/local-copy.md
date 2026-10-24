# Local-copy (`type: local-copy`)

A **sink**: it delivers the staged files a file-output destination (e.g.
[`parquet`](./parquet.md)) produced to a local destination directory. This is
the "location" half of the format/location split — see [Sink
Delivery](https://loadsmith-el.github.io/loadsmith/architecture/sink-delivery.html).

- Sink binary: `loadsmith-sink-local-copy`

| Field | Type | Required | Description |
|---|---|---|---|
| `dest` | string | yes | Destination **directory** the files are delivered under. Created if missing. |
| `key` | string | no (default `{run_date}/{run_id}/{file}`) | Where under `dest` each file lands, the same template as the [`s3`](./s3.md) sink's: `{file}` (required), `{run_id}`, `{run_date}` (`YYYY-MM-DD`, UTC), `{run_time}` (`YYYYMMDDTHHMMSSZ`), `{pipeline}`. `key: "{file}"` keeps the files flat in `dest`. |

Unknown keys are rejected (or only warned about with `--ignore-unknown-keys`).

```yaml
destination:
  type: parquet
  config:
    path: /staging       # destination stages files here
    prefix: events
    max_file_size: "64MiB"

sink:
  type: local-copy
  config:
    dest: /delivered     # local-copy delivers each finished file here
```

## Delivery

- **Atomic.** Each file is copied to a hidden temp name in `dest`
  (`.<name>.<pid>.tmp`), synced, then renamed to its final name, so a reader of
  `dest` sees either the previous file or the complete new one — never a
  partial copy, even if the sink is killed mid-copy (a killed copy can leave a
  temp file behind; it never takes a real name).
- **Names.** A file lands at `dest/<key>`: by default under a
  `<date>/<run id>/` directory per run, so a later run never replaces an
  earlier run's files (a run continuing a failed one keeps its id, and so its
  directory). Within a run the name is stable: re-delivering an object (after a
  crash) replaces the target, which is what makes resume idempotent. A `key`
  must stay inside `dest` (no `..`).
- The core's sink supervisor tracks delivery in a ledger and resumes from it if
  the sink crashes mid-run, so each object is delivered at least once.

## Fault injection (lab only)

The released binary has no crash switch. The lab's resume case uses
`local-copy-chaos` instead — a never-released build of the same delivery code
(workspace member `local-copy-chaos`, binary
`loadsmith-sink-local-copy-chaos`) that exits abruptly once after
`LOADSMITH_SINK_CRASH_AFTER=<n>` deliveries, fails every delivery after
`LOADSMITH_SINK_FAIL_AFTER=<n>` (for `LOADSMITH_SINK_FAIL_COUNT=<k>` of them),
and with `LOADSMITH_SINK_FAIL_COPIES=1` copies a file before failing it (at
the target, unacknowledged: what a killed run leaves). It is only available when the lab
builds this workspace with `--plugin`; `sink: { type: local-copy-chaos }` takes
the same config as `local-copy`.

## Migrating from 0.2

| 0.2 | 0.3 |
|---|---|
| every file flat in `dest` (a later run's `events.00000001…` replaced an earlier run's) | `key`, default `{run_date}/{run_id}/{file}`; `key: "{file}"` for the flat layout |

## Migrating from 0.1

| 0.1 | 0.2 |
|---|---|
| copied in place (a partial file was visible during the copy) | temp name + rename |
| `LOADSMITH_SINK_CRASH_AFTER` honored by the release binary | removed; lab-only `local-copy-chaos` |
| unknown keys ignored | rejected (or warned with `--ignore-unknown-keys`) |

## Telemetry

When the run has telemetry on (the engine's `telemetry:` block or `OTEL_*`
settings), the plugin records spans and metrics in-process and the engine
exports them over OTLP under the run's trace — the plugin itself opens no
connection for it, and records nothing when telemetry is off. Every plugin gets
the SDK's baseline: spans `configure`, `start`, `finish` and the per-batch /
per-object `loadsmith.*` spans, plus the `loadsmith.plugin.*` metrics
([engine design](https://github.com/loadsmith-el/loadsmith/blob/main/docs/src/architecture/telemetry.md)). The SDK records one `loadsmith.deliver` span per object
(`loadsmith.object.path`); inside it the plugin adds:

| Span | Attributes |
|---|---|
| `copy <file>` — the copy to the temp name and its sync | `file.path` (the target), `file.size` |
| `rename` — the atomic move into place | `file.path` (the temp file), `loadsmith.file.target` |
