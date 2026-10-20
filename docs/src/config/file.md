# File (`type: file`)

A **config provider**: it fetches a config document from a `file://` URI and
returns its raw bytes, implementing the config-provider contract. See [Writing a Config
Provider](https://loadsmith-el.github.io/loadsmith/plugins/writing-a-config-provider.html)
for how the core invokes a provider.

- Config-provider binary: `loadsmith-config-provider-file`

| Field | Type | Required | Description |
|---|---|---|---|
| `uri` | string | yes | A `file://` URI pointing at the config document. Anything without the `file://` scheme is rejected. |

```yaml
uri: "file:///etc/loadsmith/pipeline.yaml"
```

Unlike `source`/`destination`/`sink`, a config provider is **not** a stage
inside `pipeline.yaml`. Note that the core does **not** call config providers
yet: `loadsmith run` / `validate` read `pipeline.yaml` directly from the path
they are given, and there is no option to select a provider or pass it a `uri`.
This plugin ships for when provider selection is wired into the core.
