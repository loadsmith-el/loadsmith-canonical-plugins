# Distribution & Releases

## The SDK is a rev-pinned git dependency

The plugin SDK comes from the [`loadsmith`](https://loadsmith-el.github.io/loadsmith/)
repo as a **git dependency pinned by rev** — `loadsmith-plugin-sdk`,
`loadsmith-arrow`, `loadsmith-tls` and `loadsmith-protocol` (the type
vocabulary of the parsers' declared `columns:`), set once in the root
`Cargo.toml` `[workspace.dependencies]`.

The SDK crates stay 0.x and are not on crates.io (the engine's
[Stability Policy](https://loadsmith-el.github.io/loadsmith/reference/stability.html)
covers the wire protocol, not the Rust APIs), so this is deliberate. To move
*every* plugin onto a newer SDK, bump the `rev` **there**: the four entries
share one commit, so it is a single substitution
(`sed -i 's/<old-rev>/<new-rev>/' Cargo.toml`), then `cargo update -p
loadsmith-plugin-sdk` and commit `Cargo.lock`.

## Per-plugin, independent versioning

Releases are **per-plugin and independently versioned**. The `[package] version`
in each plugin's `Cargo.toml` is the single source of truth.

To release a plugin:

1. Tag it `<name>-v<version>` — the tag's version must match the plugin's
   `Cargo.toml` version (for example `jsonl-v0.1.0`).
2. Push the tag.

The `release-plugin.yml` workflow then:

- checks the tag against the plugin (`ci/release.py check`), failing early if
  the plugin directory doesn't exist or the version doesn't match `Cargo.toml`;
- builds the plugin natively for `linux/amd64` + `linux/arm64` as **static musl
  binaries** (see below), runs its tests (`cargo test --release`) in the same
  alpine container, and fails if any binary is dynamically linked;
- packages one archive per arch, named `<name>-<version>-linux-<arch>.tar.gz`,
  holding **every** binary the plugin's `provides` lists (`ci/release.py bins`):
  a multi-kind package such as `file` (provider + source) ships both in one
  archive, and the installer's `:<kind>` selector picks among them;
- runs every amd64 binary with `--config-schema` and keeps each printed
  schema as `<name>-<version>-<kind>.config-schema.json` (a binary that
  publishes none exits `2` and is skipped; any other failure fails the
  release);
- generates the published `loadsmith-plugin.yaml` (artifact URLs + checksums
  filled in, and a `config_schema: {url, sha256}` on each provided binary
  that has a schema) — the generator is `ci/release.py` (stdlib-only);
- publishes a per-plugin **GitHub Release** with the archives + sha256, the
  config schemas and the generated `loadsmith-plugin.yaml` attached as assets;
- updates the canonical `index.json` that `loadsmith plugin install` reads — its
  entries link to that release asset.

### Static musl binaries

Release builds run in `rust:1-alpine`, whose host target is
`*-unknown-linux-musl`, so every plugin binary is fully static: no libc, no
dynamic loader. One archive per architecture runs on any Linux, whether that is
the glibc `loadsmith:slim` image, a distroless/static image or a bare host, and
there is no glibc version floor to track.

Every binary sets [mimalloc](https://github.com/microsoft/mimalloc) as its
global allocator (`#[global_allocator]` at the top of each `main`), because
musl's own allocator is slow under the many small allocations of Arrow batch
building. A new plugin binary should do the same. The workspace's release
profile strips the symbol table (`strip = true`), which makes the binaries about
25% smaller.

### Two operational gotchas

- **Push tags one at a time.** GitHub does not trigger a workflow run for tags
  pushed beyond the first in a single `git push`. Push each tag in its own push
  (if you batched them, delete and re-push individually).
- **`index.json` is committed by CI** as a `github-actions[bot]` commit per
  release. Run `git pull` before working so your local checkout isn't behind.

### Library members are not released

A workspace member without `[package.metadata.loadsmith]` (the shared
`tabular` library) is never released on its own: a `tabular-v*` tag fails the
workflow's first check. It is built into each plugin that depends on it (the
release builds the tagged plugin's manifest, path dependencies included).

## What the engine consumes

The result of all this is a published index plus a set of GitHub Releases. The
engine's installer reads the index to find the version's published manifest,
checks the protocol range, then downloads the right per-arch archive and
verifies its sha256 against the manifest. See
[Installing Plugins](./installing.md).

Tools that write pipelines read the same chain one step further: index →
manifest → `provides[].config_schema.url`, a JSON Schema (draft 2020-12) of
that binary's `config:` block to validate against before anything runs (see
[Configuration Reference](./config/overview.md#config-schemas)).
