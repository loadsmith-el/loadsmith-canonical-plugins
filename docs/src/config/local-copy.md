# Local-copy (`type: local-copy`)

A **sink**: it delivers the staged files a file-output destination (e.g.
[`parquet`](./parquet.md)) produced to a local destination directory. This is
the "location" half of the format/location split — see [Sink
Delivery](https://loadsmith-el.github.io/loadsmith/architecture/sink-delivery.html).

- Sink binary: `loadsmith-sink-local-copy`

| Field | Type | Required | Description |
|---|---|---|---|
| `dest` | string | yes | Destination **directory** each finalized file is copied into. Created if missing. |

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
- **Names.** A file keeps its staged name. Two objects with the same file name
  (from different staging directories) land on the same target: **the last one
  delivered wins**. Re-delivering an object (after a crash) replaces the target,
  which is what makes resume idempotent.
- The core's sink supervisor tracks delivery in a ledger and resumes from it if
  the sink crashes mid-run, so each object is delivered at least once.

## Fault injection (lab only)

The released binary has no crash switch. The lab's resume case uses
`local-copy-chaos` instead — a never-released build of the same delivery code
(workspace member `local-copy-chaos`, binary
`loadsmith-sink-local-copy-chaos`) that exits abruptly once after
`LOADSMITH_SINK_CRASH_AFTER=<n>` deliveries. It is only available when the lab
builds this workspace with `--plugin`; `sink: { type: local-copy-chaos }` takes
the same config as `local-copy`.

## Migrating from 0.1

| 0.1 | 0.2 |
|---|---|
| copied in place (a partial file was visible during the copy) | temp name + rename |
| `LOADSMITH_SINK_CRASH_AFTER` honored by the release binary | removed; lab-only `local-copy-chaos` |
| unknown keys ignored | rejected (or warned with `--ignore-unknown-keys`) |
