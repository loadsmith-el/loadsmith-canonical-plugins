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
numbers, booleans as `true`/`false`, nulls as `null`. Arrow `Timestamp` and
`Date32` columns are written as ISO 8601 strings; Arrow `Binary` columns as
base64-encoded strings.
