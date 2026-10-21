# File (`type: file`)

Local files, in two independent roles:

- a **raw source** that lists the files of a directory for the pipeline's
  `parser:` (binary `loadsmith-source-file`) — see [Source](#source);
- a **provider** that resolves `{{ secret('<instance>', '<ref>') }}` values
  from local files (binary `loadsmith-provider-file`) — see
  [Provider](#provider).

Install both with `loadsmith plugin install file`, or one with `file:source` /
`file:provider`.

## Source

A **raw source** (capability `object_output`): it produces files, not rows.
Each matching file is announced to the core, which hands it to the pipeline's
[parser](https://loadsmith-el.github.io/loadsmith/plugins/writing-a-parser.html)
(`csv`, `excel`, …); the parser's Arrow stream goes to the destination. A raw
source needs a `parser:` block, and a `parser:` needs a raw source (otherwise
the configuration is rejected, exit `3`).

- Source binary: `loadsmith-source-file`

```yaml
source:
  type: file                    # → binary loadsmith-source-file
  config:
    path: /mnt/inbox
    glob: "*.csv"
    recursive: true
parser:
  type: csv                     # → binary loadsmith-parser-csv
  config: { delimiter: ";" }
destination:
  type: postgres
  config: { … }
```

### Source config

| Field | Type | Required | Description |
|---|---|---|---|
| `path` | string | yes | The directory to list. A relative path resolves against the engine's working directory. It must exist and be a directory, or the config is rejected (exit `3`). |
| `glob` | string | no (default `*`) | A pattern matched against each file's **name**: `*`, `?`, `[abc]`, `[!abc]`. It cannot contain `/`. `*` and `?` do not match a leading `.`: hidden files need a pattern that starts with `.`. |
| `recursive` | bool | no (default `false`) | Also list the subdirectories, at any depth (hidden directories are skipped). The pattern still matches file names. |

Unknown keys are rejected (`UNKNOWN_CONFIG_KEYS`, with a "did you mean"), or only warned about with `--ignore-unknown-keys`.

### Behavior

- **Sorted, in place.** The matching regular files (symlinks followed) are
  announced in sorted path order, so reruns parse them in the same order.
  Each file is read where it is: nothing is copied, the source asks for no
  `scratch_dir`, and the core never deletes the files.
- **Listed once.** The directory is listed when the run starts reading;
  files that appear later are not part of this run.
- **Errors.** A directory (or, with `recursive`, a subdirectory) that cannot
  be read fails the run (`OBJECT_FAILED`, exit `1`). No matching file is not
  an error: the parser decides what zero files means (an empty load when it
  declares its columns, `NO_INPUT` otherwise).
- **No incremental loads.** A `parser:` cannot be combined with `state:` in
  engine 1.0, so each run lists and loads every matching file.

## Provider

A **provider**: it resolves `{{ secret('<instance>', '<ref>') }}` values from
local files — a whole file, or one key of a JSON or YAML document. See the
engine's [`providers:` block](https://loadsmith-el.github.io/loadsmith/reference/pipeline-yaml.html#providers)
and [Writing a Provider](https://loadsmith-el.github.io/loadsmith/plugins/writing-a-provider.html)
for how the core runs a provider.

- Provider binary: `loadsmith-provider-file`

A provider is not a pipeline stage: you declare an **instance** of it under
`providers:` and read values from it with `secret()` anywhere in `source:`,
`destination:` or `sink:`.

```yaml
providers:
  creds:                          # instance name, used by secret()
    type: file                    # → binary loadsmith-provider-file
    config:
      base_dir: /run/secrets      # optional
source:
  type: postgres
  config:
    host: db
    user: "{{ secret('creds', 'pg.yaml#user') }}"
    password: "{{ secret('creds', 'pg.yaml#password') }}"
    tls:
      mode: verify-full
      root_cert: "{{ secret('creds', '/etc/ssl/certs/ca.pem') }}"
```

### Provider config

| Field | Type | Required | Description |
|---|---|---|---|
| `base_dir` | string | no | Directory that **relative** reference paths resolve against. Must exist and be a directory, or the config is rejected. Without it, relative paths resolve against the engine's working directory. |

The `config:` block may be omitted entirely. Unknown keys are rejected (`UNKNOWN_CONFIG_KEYS`, with a "did you mean"), or only warned about with `--ignore-unknown-keys`.

### References

A reference is `path[#key]`:

| Reference | Value |
|---|---|
| `token` | the whole file `<base_dir>/token`, **trimmed** of leading and trailing whitespace |
| `/abs/path/token` | the whole file at that absolute path (`base_dir` is ignored) |
| `db.json#password` | the `password` field of the JSON document |
| `db.yaml#pg.password` | a nested field, by **dotted path** |
| `hosts.json#replicas.0` | a numeric segment indexes a list |

- **Format.** With `#key`, the file is parsed by extension: `.json` as JSON,
  `.yaml`/`.yml` as YAML; any other extension is tried as JSON, then as YAML.
- **Selected value.** The key must select a scalar: a string is returned as-is
  (not trimmed), a number or boolean as its text. A `null`, an object or a list
  is an error.
- The path is everything before the first `#`. A key segment cannot contain a
  `.`.

### Errors

A reference that cannot be resolved fails the run (exit `3`), naming the
instance and the reference. The `resolved` error codes:

| Code | When |
|---|---|
| `NOT_FOUND` | The file does not exist, the key is missing, or it selects `null`. |
| `INVALID_REF` | An empty path or key segment, a directory, a file that is not UTF-8 or not valid JSON/YAML, or a key that selects an object or list. |
| `ACCESS_DENIED` | The file is not readable by the engine's user. |
| `RESOLVE_FAILED` | Any other I/O error. |

No error message or log line contains a value: messages name the path, the key
and, for a parse error, only the line and column (never the parser's text,
which could quote the document). The core masks every resolved value in its
own output as well.

### Migrating from 0.1.x

0.1.x was a `config-provider` (binary `loadsmith-config-provider-file`, config
`uri: file://…`) that no engine ever invoked. 0.2.0 is a `provider`: replace
`uri` with an optional `base_dir` and reference files through `secret()`.
Engines before the provider kind cannot install it, and this engine skips the
0.1.x manifest on `plugin install --all`.

## Telemetry

When the run has telemetry on (the engine's `telemetry:` block or `OTEL_*`
settings), the plugin records spans and metrics in-process and the engine
exports them over OTLP under the run's trace — the plugin itself opens no
connection for it, and records nothing when telemetry is off. Every plugin gets
the SDK's baseline: spans `configure`, `start`, `finish` and the per-batch /
per-object `loadsmith.*` spans, plus the `loadsmith.plugin.*` metrics
([engine design](https://github.com/loadsmith-el/loadsmith/blob/main/docs/src/architecture/telemetry.md)).

- **Source:** the SDK records one `loadsmith.object` span per announced file
  (`loadsmith.object.path`); the plugin adds one `list` span for the
  directory listing: `file.directory`, `loadsmith.glob`,
  `loadsmith.recursive`, `loadsmith.files.matched`, `loadsmith.files.bytes`.
- **Provider:** the SDK records one `loadsmith.resolve` span per reference
  (`loadsmith.provider.ref`); the plugin adds a `read <file>` span with
  `file.path`, `file.size` and, on failure, `error.type` (`NOT_FOUND`,
  `ACCESS_DENIED`, …). A value never appears in a span, attribute or log.
