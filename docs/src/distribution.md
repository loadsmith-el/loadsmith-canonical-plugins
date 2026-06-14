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

- builds the plugin natively for `linux/amd64` + `linux/arm64`;
- publishes a per-plugin **GitHub Release** with the archives + sha256;
- generates the published `loadsmith-plugin.yaml` (artifact URLs + checksums
  filled in) — the generator is `ci/release.py` (stdlib-only);
- updates the canonical `index.json` that `loadsmith plugin install` reads.

### Two operational gotchas

- **Push tags one at a time.** GitHub does not trigger a workflow run for tags
  pushed beyond the first in a single `git push`. Push each tag in its own push
  (if you batched them, delete and re-push individually).
- **`index.json` is committed by CI** as a `github-actions[bot]` commit per
  release. Run `git pull` before working so your local checkout isn't behind.

## What the engine consumes

The result of all this is a published index plus a set of GitHub Releases. The
engine's installer reads the index, downloads the right per-arch archive,
verifies its sha256, and checks the protocol range before installing. See
[Installing Plugins](./installing.md).
