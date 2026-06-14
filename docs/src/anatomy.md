# Plugin Anatomy

This repository is a **Cargo workspace** with one member per plugin package.
Each plugin is a self-describing directory:

```text
<name>/
  Cargo.toml     # [package.metadata.loadsmith]: name, summary, protocol, provides[{kind, bin}]
  src/           # the plugin implementation (one or more [[bin]])
```

## Install metadata lives in `Cargo.toml`

Each plugin declares its install metadata in its `Cargo.toml` under
`[package.metadata.loadsmith]`:

- `name` — the install name (`loadsmith plugin install <name>`);
- `summary` — a one-line description;
- `protocol` — the protocol version the plugin speaks;
- `provides` — a list of `{ kind, bin }` entries mapping each role the package
  provides to the binary that implements it.

The `[package] version` is the **single source of truth** for the version. CI
fills in the artifact URLs and checksums at release time — there is **no
committed `loadsmith-plugin.yaml`**; it is generated. See
[Distribution & Releases](./distribution.md).

## The rules

These are the hard rules for adding or changing a plugin. They exist because the
whole plugin set inherits the engine's posture.

### Pure-Rust crypto only

Loadsmith targets both `linux/amd64` and `linux/arm64` (AWS Graviton). The
release images are built `cargo build`-native inside each architecture, so any
C/assembly-per-arch crypto toolchain would mean slow emulated builds and fragile
tooling.

- TLS is done via **`rustls` + `rustls-rustcrypto`** (the pure-Rust crypto
  provider).
- Never use `native-tls`, `openssl-sys`, `ring`, or `aws-lc-rs`.
- `postgres` uses `tokio-postgres-rustls-improved` together with the engine's
  `loadsmith-tls` crate.

See the engine's
[Multi-arch & TLS](https://loadsmith-el.github.io/loadsmith/architecture/multi-arch-and-tls.html)
for the full rationale.

### Source and destination stay separate binaries

A multi-role package (like `postgres`) ships source and destination as
**separate binaries** that share low-level modules (connection, type mapping,
copy) — *not* a merged read/write driver. This was considered and rejected in
the engine's *Rejected Ideas*; don't re-derive it. `postgres` is the reference
implementation of the pattern.

### English only

All artifacts committed here — code, comments, commit messages, identifiers —
must be in English, even when the conversation happens in another language.
