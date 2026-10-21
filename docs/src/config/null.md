# Null (`type: null`)

A destination that **discards** every row it receives. It exists to measure
read/pump throughput without a write bottleneck — point a source at it to time
the extract path alone, or to drive large volume/scale cases.

- Destination binary: `loadsmith-destination-null`

**No configuration.** It takes no fields; pass an empty `config` (or omit it).
Since 0.2 a key in it is rejected like an unknown key in any other plugin (or
only warned about with `--ignore-unknown-keys`); 0.1 ignored the block.

```yaml
destination:
  type: "null"      # quote it — bare null is a YAML null
  config: {}
```

> Quote the type: an unquoted `null` is parsed by YAML as the null value, not
> the string `"null"`.

## Telemetry

When the run has telemetry on (the engine's `telemetry:` block or `OTEL_*`
settings), the plugin records spans and metrics in-process and the engine
exports them over OTLP under the run's trace — the plugin itself opens no
connection for it, and records nothing when telemetry is off. Every plugin gets
the SDK's baseline: spans `configure`, `start`, `finish` and the per-batch /
per-object `loadsmith.*` spans, plus the `loadsmith.plugin.*` metrics
([engine design](https://github.com/loadsmith-el/loadsmith/blob/main/docs/src/architecture/telemetry.md)). The `null` destination adds nothing to it.
