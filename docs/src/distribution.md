# Distribution & Releases

## The SDK is a rev-pinned git dependency

The plugin SDK comes from the [`loadsmith`](https://loadsmith-el.github.io/loadsmith/)
repo as a **git dependency pinned by rev** — `loadsmith-plugin-sdk`,
`loadsmith-arrow`, and `loadsmith-tls`, set once in the root `Cargo.toml`
`[workspace.dependencies]`.

The protocol/SDK is still evolving, so this is deliberate (the crates are not on
crates.io). To move *every* plugin onto a newer SDK, bump the `rev` **there**, in
one place.

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
- packages one archive per arch, named `<name>-<version>-linux-<arch>.tar.gz`;
- generates the published `loadsmith-plugin.yaml` (artifact URLs + checksums
  filled in) — the generator is `ci/release.py` (stdlib-only);
- publishes a per-plugin **GitHub Release** with the archives + sha256 and the
  generated `loadsmith-plugin.yaml` attached as an asset;
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

## What the engine consumes

The result of all this is a published index plus a set of GitHub Releases. The
engine's installer reads the index to find the version's published manifest,
checks the protocol range, then downloads the right per-arch archive and
verifies its sha256 against the manifest. See
[Installing Plugins](./installing.md).
