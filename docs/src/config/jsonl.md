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

## Value mapping

Every Arrow type has a defined JSON form (the conversion never fails a run):

| Arrow | JSON |
|---|---|
| null slot, `Null` | `null` |
| `Boolean` | `true` / `false` |
| `Int*`, `UInt*` | number (a `uint64` above 2^53 may lose precision in some JSON readers) |
| `Float16/32/64` | number; **NaN and ±Infinity are written as `null`** |
| `Decimal128/256` | **string** with the scale applied, e.g. `"123.45"` (exact) |
| `Utf8`, `LargeUtf8`, `Utf8View` | string |
| `Binary`, `LargeBinary`, `BinaryView`, `FixedSizeBinary` | base64 string (standard, padded) |
| `Date32` | integer days since 1970-01-01 |
| `Date64` | integer milliseconds since the epoch |
| `Timestamp` (any unit, any time zone) | integer **epoch milliseconds** (`us`/`ns` floored) |
| `Time32`, `Time64` | integer milliseconds since midnight (floored) |
| `Duration` | integer milliseconds (floored) |
| `Interval` | `{"months": n}` / `{"days": d, "milliseconds": ms}` / `{"months": m, "days": d, "nanoseconds": ns}` |
| `List`, `LargeList`, `FixedSizeList` | array of the converted items (e.g. Postgres arrays) |
| `Struct` | object keyed by child name |
| `Map` | object (a non-string key is written as its JSON text) |
| `Dictionary` | the decoded value |

A value that cannot be represented (a timestamp in seconds whose millisecond
form overflows) is `null`.

## Behavior notes

- **Cancel removes the file.** When the run does not complete (the core cancels
  it, the input stream is truncated, or a write fails), the partial file at
  `path` is deleted rather than left looking like a finished export. Output
  already written to stdout cannot be taken back.
- The file at `path` is created (truncated) when the destination starts, so a
  failed run also removes the previous run's output there.
