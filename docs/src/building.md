# Building & Verifying Locally

## Building

```bash
cargo build            # fetches the pinned SDK from the loadsmith repo, builds all plugins
cargo build -p loadsmith-destination-jsonl   # build a single plugin
```

The first build fetches the rev-pinned SDK crates from the
[`loadsmith`](https://loadsmith-el.github.io/loadsmith/) repo — see
[Distribution & Releases](./distribution.md).

## Real end-to-end validation

Unit-building a plugin proves it compiles; it does not prove it moves data
correctly. The **real** validation lives in
[`loadsmith-lab`](https://loadsmith-el.github.io/loadsmith-lab/) (a sibling
repo), which runs a plugin against a real seeded service and checks the output.

Point the lab at a local plugin build — it compiles the crate in a
`rust:bookworm` container:

```bash
cd ../loadsmith-lab
./target/debug/loadsmith-lab run \
  --loadsmith ../loadsmith \
  --plugin ../loadsmith-canonical-plugins/jsonl \
  --select catalog/postgres-to-jsonl
```

- `--loadsmith <path>` builds the core from source instead of pulling the
  published image.
- `--plugin <path>` builds and mounts a local plugin instead of installing the
  published one.

See the lab's
[Run Modes](https://loadsmith-el.github.io/loadsmith-lab/architecture/run-modes.html)
for the full set of options.
