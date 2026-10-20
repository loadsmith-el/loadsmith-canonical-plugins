# Local-copy (`type: local-copy`)

A **sink**: it delivers the staged files a file-output destination (e.g.
[`parquet`](./parquet.md)) produced to a local destination directory. This is
the "location" half of the format/location split — see [Sink
Delivery](https://loadsmith-el.github.io/loadsmith/architecture/sink-delivery.html).

- Sink binary: `loadsmith-sink-local-copy`

| Field | Type | Required | Description |
|---|---|---|---|
| `dest` | string | yes | Destination **directory** each finalized file is copied into. Created if missing. Files keep their staged name and overwrite any existing file of the same name (idempotent re-delivery). |

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

The core's sink supervisor tracks delivery in a ledger and resumes from it if
the sink crashes mid-run, so each object is delivered at least once.
