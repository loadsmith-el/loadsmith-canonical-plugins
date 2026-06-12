# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with
code in this repository. It is **operating instructions only** — for what this
repo is and how a plugin is structured/released, read [README.md](README.md) and
the [loadsmith docs](../loadsmith/docs/src), or the source itself. Don't guess at
"why" — go read it.

## Conventions

- **English only.** All artifacts committed here — code, comments, commit
  messages, identifiers — must be in English, even when the user writes in
  Portuguese.
- **This repo is the official plugin set** that `loadsmith install <name>`
  resolves from the canonical index. It's a Cargo workspace with one member per
  **plugin package** (a "connector" when it provides more than one role, e.g.
  `postgres` ships a source *and* a destination); see [README.md](README.md).
- **The SDK is a rev-pinned git dependency** on the [loadsmith](../loadsmith)
  repo (`loadsmith-plugin-sdk` / `loadsmith-arrow` / `loadsmith-tls`), set once
  in the root [`Cargo.toml`](Cargo.toml) `[workspace.dependencies]`. Bump the
  `rev` **there** to move every plugin to a newer SDK — the protocol/SDK is still
  green, so this is deliberate (not crates.io).

## Hard rules — read before adding or changing a plugin

- **Each plugin declares its install metadata** in its `Cargo.toml`
  `[package.metadata.loadsmith]` (`name`, `summary`, `protocol`, and
  `provides = [{ kind, bin }]`). The `[package] version` is the **single source
  of truth** for the version — the CI fills in artifact URLs + checksums at
  release. There is no committed `loadsmith-plugin.yaml`.
- **Pure-Rust crypto only** (multi-arch posture inherited from loadsmith). TLS
  via `rustls` + `rustls-rustcrypto`; never `native-tls`/`openssl-sys`/`ring`/
  `aws-lc-rs`. `postgres` uses `tokio-postgres-rustls-improved` + `loadsmith-tls`.
- **Source and destination stay separate binaries** sharing low-level modules
  (conn/types/copy) — NOT a merged read/write driver (see loadsmith's
  `rejected-ideas.md`). `postgres` is the reference: one crate, `src/lib.rs` +
  shared modules, two `[[bin]]` (`src/bin/source.rs`, `src/bin/destination.rs`).

## Releasing a plugin

Releases are **per-plugin and independently versioned**. Tag a plugin
`<name>-v<version>` (matching its `Cargo.toml` version) and push the tag — the
[`release-plugin.yml`](.github/workflows/release-plugin.yml) workflow builds it
natively for `linux/amd64` + `linux/arm64`, publishes a GitHub Release (archives
+ sha256 + the generated `loadsmith-plugin.yaml`), and updates `index.json`. The
generator is [`ci/release.py`](ci/release.py) (stdlib-only).

- **Push tags ONE AT A TIME.** GitHub does not trigger a workflow run for tags
  pushed beyond the first in a single `git push` — push each tag in its own
  push (delete + re-push individually if you batched them).
- `index.json` is committed by the CI (a `github-actions[bot]` commit per
  release); `git pull` before working so your local isn't behind.

## Verifying a change

```bash
cargo build                          # fetches the pinned SDK, builds all plugins
cargo build -p loadsmith-destination-jsonl   # one plugin

# Real end-to-end validation is in loadsmith-lab (sibling repo): point it at a
# local plugin build — it compiles the crate in a rust:bookworm container.
cd ../loadsmith-lab
./target/debug/loadsmith-lab run --loadsmith ../loadsmith \
  --plugin ../loadsmith-canonical-plugins/jsonl --select catalog/postgres-to-jsonl
```
