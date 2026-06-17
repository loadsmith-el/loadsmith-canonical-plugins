# Installing Plugins

The official Loadsmith image is **slim** — the core only. Plugins are installed
on demand from what this repository publishes.

A **package** (e.g. `postgres`) provides one or more **plugins** — its binaries,
one per kind. Install the whole package, or a single kind with `:<kind>` /
`--kind`. The full spec is `<name>[:<kind>][@<version>]`.

```bash
# Install a whole package (all its plugins) by name, from the canonical index
loadsmith plugin install postgres

# Install just one plugin of a package — usually you read from one DB and write
# to another, so you don't need both the source and the destination
loadsmith plugin install postgres:source           # only loadsmith-source-postgres
loadsmith plugin install mysql --kind destination  # same, via the flag

# Pin a version (composes with the kind selector)
loadsmith plugin install postgres@0.1.0
loadsmith plugin install postgres:source@0.1.0

# Install all plugins of one package (explicit), or the whole canonical set
loadsmith plugin install postgres --all
loadsmith plugin install --all

# Install from an arbitrary manifest or a prebuilt binary
loadsmith plugin install --manifest <url>
loadsmith plugin install --binary <path>

# Remove a whole package, or just one of its plugins
loadsmith plugin uninstall postgres
loadsmith plugin uninstall postgres:source
```

Installed plugins land in `~/.loadsmith/plugins/`, where the core discovers them
at run time (a binary named `loadsmith-{kind}-{type}`). Only one version of a
plugin is installed at a time — reinstalling a different version overwrites it.

The installer is **sha256-verified** and **protocol-range-checked**: it confirms
the downloaded archive matches the checksum in the index and that the plugin's
protocol version is one the core speaks before installing it.

For the full command reference, options, and the manifest contract, see the
engine docs:

- [Installation](https://loadsmith-el.github.io/loadsmith/getting-started/installation.html)
- [CLI Reference](https://loadsmith-el.github.io/loadsmith/reference/cli.html)
