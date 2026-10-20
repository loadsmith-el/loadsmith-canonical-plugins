# JSONL (`type: jsonl`)

A destination that writes newline-delimited JSON — one JSON object per row,
followed by a newline.

- Destination binary: `loadsmith-destination-jsonl`

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `path` | string | no | *(stdout)* | Output file path. **Omit to write to stdout.** |

```yaml
destination:
  type: jsonl
  config:
    path: /output/data.jsonl   # omit entirely to stream to stdout
```

Column values follow JSON types: strings as JSON strings, integers as JSON
numbers, booleans as `true`/`false`, nulls as `null`. Arrow `Date32` columns are
written as integer days since 1970-01-01, and `Timestamp` columns as integer
epoch milliseconds (the raw stored value; only millisecond-unit timestamps are
supported). Arrow `Binary` columns are written as base64-encoded strings. Arrow
types the converter does not handle (e.g. `List` from Postgres arrays) are
written as `null`, and NaN/±Inf floats are written as `0`.
