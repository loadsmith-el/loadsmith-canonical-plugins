# loadsmith-canonical-plugins

> 📖 **Full documentation:** <https://loadsmith-el.github.io/loadsmith-canonical-plugins/>

The **official, supported plugin set** for [Loadsmith](https://github.com/loadsmith-el/loadsmith) — the
plugins `loadsmith plugin install <name>` resolves from the canonical index. ("Canonical"
mirrors `loadsmith-lab-canonical-data`: it marks the blessed set apart from
third-party/local plugins the installer also accepts.)

## Layout

A Cargo workspace with one member per **plugin package** (a "connector" when it
provides more than one role — e.g. `postgres` ships a source *and* a
destination). Each plugin is a self-describing directory:

```text
<name>/
  Cargo.toml     # [package.metadata.loadsmith]: name, summary, protocol, provides[{kind,bin}]
  src/           # the plugin implementation (one or more [[bin]])
```

| Package | Provides |
|---|---|
| `postgres` | source + destination (TLS via rustls) |
| `mysql` | source + destination (TLS via rustls) |
| `oracle` | source + destination (pure-Rust `oracledb` thin driver, no client libs; TLS via rustls) |
| `sharepoint` | source (Microsoft Graph: files + lists; TLS via rustls) |
| `jsonl` | destination |
| `parquet` | destination |
| `null` | destination (throughput testing) |
| `local-copy` | sink |
| `file` | config-provider |

## How a plugin is distributed

The SDK comes from the [loadsmith](https://github.com/loadsmith-el/loadsmith) repo as a **git dependency
pinned by rev** (the protocol/SDK is still evolving — bump the rev in the root
`[workspace.dependencies]` to move every plugin to a newer SDK).

Each plugin declares its install metadata in `[package.metadata.loadsmith]`
(the `[package] version` is the single source of truth for the version). CI
builds each plugin for `linux/amd64` + `linux/arm64` (static musl binaries
that run on any Linux), publishes a per-plugin
**GitHub Release** with the archives + sha256, and generates the published
`loadsmith-plugin.yaml` (artifact URLs + checksums filled in) + the index that
`loadsmith plugin install` reads.

## Building locally

```bash
cargo build            # fetches the pinned SDK from the loadsmith repo, builds all plugins
```

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
