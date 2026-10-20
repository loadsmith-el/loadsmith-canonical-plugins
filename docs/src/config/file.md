# File (`type: file`)

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

## Config

| Field | Type | Required | Description |
|---|---|---|---|
| `base_dir` | string | no | Directory that **relative** reference paths resolve against. Must exist and be a directory, or the config is rejected. Without it, relative paths resolve against the engine's working directory. |

The `config:` block may be omitted entirely. Unknown fields are rejected.

## References

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

## Errors

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

## Migrating from 0.1.x

0.1.x was a `config-provider` (binary `loadsmith-config-provider-file`, config
`uri: file://…`) that no engine ever invoked. 0.2.0 is a `provider`: replace
`uri` with an optional `base_dir` and reference files through `secret()`.
Engines before the provider kind cannot install it, and this engine skips the
0.1.x manifest on `plugin install --all`.
