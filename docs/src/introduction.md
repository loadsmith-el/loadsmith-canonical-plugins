# Introduction

This is the **official, supported plugin set** for
[Loadsmith](https://loadsmith-el.github.io/loadsmith/) — the plugins that
`loadsmith plugin install <name>` resolves from the canonical index.

Loadsmith's core is deliberately small: it knows nothing about Postgres, JSONL,
Parquet, or S3. Every source, destination, sink, and config provider is a
separate plugin, distributed and installed on demand on top of the **slim**
official core image. This repository is where the *blessed* set of those plugins
lives.

## What "canonical" means

"Canonical" marks the official set apart from the third-party or local plugins
the installer also accepts. It mirrors the naming of
[`loadsmith-lab-canonical-data`](https://loadsmith-el.github.io/loadsmith-lab-canonical-data/):
canonical = the supported, indexed set that the project maintains and tests.

`loadsmith plugin install` can install:

- a **canonical** plugin by name (resolved from this repo's published index);
- a plugin from any **manifest** URL (`--manifest`);
- a prebuilt **binary** directly (`--binary`).

Only the first comes from here.

## How this fits the bigger picture

```text
loadsmith (core + SDK)                  the engine and the plugin SDK crates
   │  git-deps the SDK, pinned by rev
   ▼
loadsmith-canonical-plugins  ◄── you are here
   │  per-plugin GitHub Releases + a canonical index.json
   ▼
loadsmith plugin install <name>         installs into ~/.loadsmith/plugins
```

- The **engine + SDK** live in
  [`loadsmith`](https://loadsmith-el.github.io/loadsmith/). The SDK crates
  (`loadsmith-plugin-sdk`, `loadsmith-arrow`, `loadsmith-tls`) are consumed here
  as **rev-pinned git dependencies** — see
  [Distribution & Releases](./distribution.md).
- Real end-to-end validation of a plugin happens in
  [`loadsmith-lab`](https://loadsmith-el.github.io/loadsmith-lab/), which can run
  a case against a local plugin build.

## Where to go next

- [The Plugin Set](./plugin-set.md) — what ships and what each plugin provides.
- [Plugin Anatomy](./anatomy.md) — how a plugin package is structured and the
  rules that govern it.
- [Distribution & Releases](./distribution.md) — the SDK pin, per-plugin
  versioning, and the release pipeline.
- [Installing Plugins](./installing.md) — how the engine consumes what this repo
  publishes.
- [Building & Verifying Locally](./building.md) — building and testing a plugin
  before release.
