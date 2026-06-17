# File (`type: file`)

A **config provider**: it supplies the pipeline configuration to the core by
reading it from a local file. This is the role that loads your `pipeline.yaml`.
See [Writing a Config
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
inside `pipeline.yaml` — it is what the core uses to *obtain* the pipeline
config in the first place, so its `uri` is configured where the core selects the
provider, not in the pipeline document it loads.
