# Installing Plugins

The official Loadsmith image is **slim** — the core only. Plugins are installed
on demand from what this repository publishes.

```bash
# Install one canonical plugin by name (resolved from the canonical index)
loadsmith plugin install postgres

# Install the whole canonical set
loadsmith plugin install --all

# Install from an arbitrary manifest or a prebuilt binary
loadsmith plugin install --manifest <url>
loadsmith plugin install --binary <path>

# Remove one
loadsmith plugin uninstall postgres
```

Installed plugins land in `~/.loadsmith/plugins/`, where the core discovers them
at run time (a binary named `loadsmith-{kind}-{type}`).

The installer is **sha256-verified** and **protocol-range-checked**: it confirms
the downloaded archive matches the checksum in the index and that the plugin's
protocol version is one the core speaks before installing it.

For the full command reference, options, and the manifest contract, see the
engine docs:

- [Installation](https://loadsmith-el.github.io/loadsmith/getting-started/installation.html)
- [CLI Reference](https://loadsmith-el.github.io/loadsmith/reference/cli.html)
