#!/usr/bin/env python3
"""Release tooling for loadsmith-canonical-plugins (stdlib only).

Two steps the release workflow calls:

  manifest <plugin-dir> <tag> <amd64.tar.gz> <arm64.tar.gz> <out.yaml>
      Reads the plugin's [package.metadata.loadsmith] + [package] version and
      emits the PUBLISHED loadsmith-plugin.yaml — the same fields the author
      wrote, plus a runtime.artifacts block with the per-arch release-asset URLs
      and their sha256 (computed from the built tarballs).

  index <name> <version> <manifest-url> <index.json>
      Adds/updates the entry in the index that `loadsmith install` reads.

The tag is `<name>-v<version>` (e.g. postgres-v1.0.0).
"""
import hashlib
import json
import pathlib
import sys
import tomllib

REPO = "loadsmith-el/loadsmith-canonical-plugins"
API_VERSION = "loadsmith.dev/plugin/v1"


def sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def load_meta(plugin_dir: str):
    cargo = tomllib.loads((pathlib.Path(plugin_dir) / "Cargo.toml").read_text())
    pkg = cargo["package"]
    meta = pkg.get("metadata", {}).get("loadsmith")
    if meta is None:
        sys.exit(f"{plugin_dir}/Cargo.toml has no [package.metadata.loadsmith]")
    return pkg["version"], meta


def cmd_manifest(plugin_dir, tag, amd64_tar, arm64_tar, out):
    version, meta = load_meta(plugin_dir)
    base = f"https://github.com/{REPO}/releases/download/{tag}"
    artifacts = [
        ("amd64", pathlib.Path(amd64_tar).name, sha256(amd64_tar)),
        ("arm64", pathlib.Path(arm64_tar).name, sha256(arm64_tar)),
    ]
    # json.dumps quotes + escapes — guards YAML footguns (a plugin literally
    # named `null`/`yes`/`no`, em-dashes/colons in summaries, version "1.0" read
    # as a float). JSON scalars are valid YAML.
    lines = [
        f"apiVersion: {API_VERSION}",
        f"name: {json.dumps(meta['name'])}",
        f"version: {json.dumps(version)}",
    ]
    if meta.get("summary"):
        lines.append(f"summary: {json.dumps(meta['summary'])}")
    lines.append(f"protocol: {json.dumps(meta['protocol'])}")
    lines.append("provides:")
    for p in meta["provides"]:
        lines.append(f"  - {{ kind: {p['kind']}, bin: {p['bin']} }}")
    lines += ["runtime:", "  type: binary", "  artifacts:"]
    for arch, fname, digest in artifacts:
        lines.append(
            f'    - {{ os: linux, arch: {arch}, url: "{base}/{fname}", sha256: "{digest}" }}'
        )
    text = "\n".join(lines) + "\n"
    pathlib.Path(out).write_text(text)
    sys.stdout.write(text)


def cmd_bins(plugin_dir):
    """Print the binaries this plugin provides, one per line (for packaging)."""
    _, meta = load_meta(plugin_dir)
    for p in meta["provides"]:
        print(p["bin"])


def cmd_check(plugin_dir, tag):
    """Guard: the tag `<name>-v<version>` must match the plugin's metadata."""
    version, meta = load_meta(plugin_dir)
    expected = f"{meta['name']}-v{version}"
    if tag != expected:
        sys.exit(f"tag {tag!r} does not match {plugin_dir} metadata (expected {expected!r})")
    print(f"ok: {tag} matches {plugin_dir} (name={meta['name']} version={version})")


def cmd_index(name, version, manifest_url, index_path):
    p = pathlib.Path(index_path)
    data = json.loads(p.read_text()) if p.exists() else {"plugins": {}}
    plugins = data.setdefault("plugins", {})
    entry = plugins.setdefault(name, {"latest": version, "versions": {}})
    entry["versions"][version] = manifest_url
    entry["latest"] = version  # the just-released version becomes latest
    p.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    print(f"index: {name} {version} -> {manifest_url}")


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    cmd, args = sys.argv[1], sys.argv[2:]
    if cmd == "manifest":
        cmd_manifest(*args)
    elif cmd == "index":
        cmd_index(*args)
    elif cmd == "bins":
        cmd_bins(*args)
    elif cmd == "check":
        cmd_check(*args)
    else:
        sys.exit(f"unknown command {cmd!r}")


if __name__ == "__main__":
    main()
