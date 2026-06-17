# Null (`type: null`)

A destination that **discards** every row it receives. It exists to measure
read/pump throughput without a write bottleneck — point a source at it to time
the extract path alone, or to drive large volume/scale cases.

- Destination binary: `loadsmith-destination-null`

**No configuration.** It takes no fields; pass an empty `config` (or omit it).

```yaml
destination:
  type: "null"      # quote it — bare null is a YAML null
  config: {}
```

> Quote the type: an unquoted `null` is parsed by YAML as the null value, not
> the string `"null"`.
