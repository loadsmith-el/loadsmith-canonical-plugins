# CSV (`type: csv`)

A **parser**: it turns the CSV files a raw source announces (for example the
[`file` source](./file.md#source)) into one Arrow stream for the destination.
See the engine's [`parser:` block](https://loadsmith-el.github.io/loadsmith/reference/pipeline-yaml.html#parser)
and [Writing a Parser](https://loadsmith-el.github.io/loadsmith/plugins/writing-a-parser.html)
for how the core runs a parser.

- Parser binary: `loadsmith-parser-csv`

```yaml
source:
  type: file
  config: { path: /mnt/inbox, glob: "*.csv" }
parser:
  type: csv                     # → binary loadsmith-parser-csv
  config:
    delimiter: ";"
    encoding: windows-1252
destination:
  type: postgres
  config: { … }
```

Each file is **streamed**: the parser holds the inference sample (at most
`infer_rows` records) and one batch, never the whole file, so a file larger
than memory loads fine.

## Config

| Field | Type | Required | Description |
|---|---|---|---|
| `delimiter` | string | no (default `,`) | The field delimiter: exactly one ASCII character other than `"` or a newline (`";"`, `"\|"`, `"\t"`). |
| `encoding` | string | no (default `utf-8`) | The files' text encoding, as a [WHATWG label](https://encoding.spec.whatwg.org/#names-and-labels): `utf-8`, `windows-1252` (also `latin1`, `iso-8859-1`), `utf-16le`, `shift_jis`, … A byte-order mark at the start of a file wins over this setting and is dropped. Bytes that are invalid in the encoding become U+FFFD. |
| `header` | bool | no (default `true`) | The first record names the columns. Without it, the columns are `col_0`, `col_1`, … |
| `infer_rows` | integer | no (default `1000`) | How many data records [type inference](#type-inference) reads, per file. At least `1`. Ignored with `columns`. |
| `columns` | list | no | [Declared columns](#declared-columns): the stream's schema, instead of inference. |
| `batch_size` | integer | no (default `1000`) | Rows per Arrow batch. At least `1`. |

The `config:` block may be omitted entirely. Unknown fields are rejected.

Quoting follows RFC 4180: a field in double quotes may contain the delimiter,
newlines and doubled quotes (`"say ""hi"""`). Empty lines are skipped.

## Type inference

Without `columns`, each file's schema is inferred from its first `infer_rows`
data records. Every inferred column is **nullable**, so files with and
without empty cells share one schema. A blank (empty or whitespace-only) cell
is a null and does not constrain the type. Each column becomes the first type
all of its sampled non-blank values fit:

| Values (trimmed) | Type |
|---|---|
| integers (`42`, `-7`) | `int64` |
| numbers, integers mixed with decimals (`1.5`, `1e3`, `NaN`) | `float64` |
| `true` / `false`, any case | `bool` |
| dates `YYYY-MM-DD` | `date32` |
| date-times `YYYY-MM-DD HH:MM:SS[.fff]` or with a `T` | `timestamp(ms)` (no time zone) |
| anything else, mixed kinds, or no value at all | `utf8` |

These are the [`sharepoint`](./sharepoint.md) CSV rules (integer, number,
else text) extended with booleans, dates and date-times.

Rows after the sample are converted to the inferred types **strictly**: a
value that does not fit (`n/a` in an `int64` column) fails the run, naming
the file, the line and the column. Raise `infer_rows`, or declare the
columns, when a column's first rows are not representative. A text (`utf8`)
column keeps each value exactly, untrimmed; blank cells are nulls in every
column type.

## Declared columns

`columns` fixes the stream's schema. It is a list of field objects in the
protocol's [type vocabulary](https://loadsmith-el.github.io/loadsmith/protocol/messages.html#schema):
`name`, `type` and the type's parameters, plus an optional `nullable`
(default `true`).

```yaml
parser:
  type: csv
  config:
    columns:
      - { name: id,         type: int32, nullable: false }
      - { name: amount,     type: decimal128, precision: 12, scale: 2 }
      - { name: paid,       type: bool }
      - { name: note,       type: utf8 }
      - { name: day,        type: date32 }
      - { name: created_at, type: timestamp, unit: us, timezone: UTC }
```

- Accepted types: `bool`, `int8`…`int64`, `uint8`…`uint64`, `float32`,
  `float64`, `decimal128`, `decimal256`, `utf8`, `large_utf8`, `date32`,
  `date64`, `time32`, `time64`, `timestamp`. Nested, binary and other types
  are rejected (exit `3`), as are an unknown type and two columns with one
  name.
- `bool` reads `true`/`t`/`1` and `false`/`f`/`0` (any case). Timestamps read
  `YYYY-MM-DD[ T]HH:MM:SS[.f]`, optionally followed by an offset (`Z`,
  `+02:00`): a value with an offset is converted to UTC.
- The declared **names** are the stream's. With `header: true` the header row
  is skipped and only its width is checked (it must have as many columns as
  `columns`), so a file's header may spell the names differently.
- A value that does not convert (`x` in an `int32`, `300` in an `int8`), or a
  blank cell in a `nullable: false` column, fails the run with its line and
  column.
- With `columns`, the schema is known before any file: a run whose source
  announces **no file** is a valid, empty load. Without `columns` it fails
  with `NO_INPUT`.

## One schema per run

All files of a run go into one stream with one schema. Without `columns`, the
first file's inferred schema is the stream's, and every later file must infer
the same columns — names, types, order — or the run fails with
`SCHEMA_MISMATCH` (exit `1`), for example:

```text
/mnt/inbox/2026-02.csv: column 2 (`amount`) is utf8, the stream has int64
```

A column that is entirely blank in one file's sample infers as `utf8`, which
can mismatch another file's `int64`. Declaring `columns` avoids both cases.

## Errors

A failure aborts the run (exit `1`) and nothing is committed: the destination
is cancelled, also for the files parsed before the failing one.

| Code | When |
|---|---|
| `CONFIGURE_FAILED` | An invalid option (exit `3`). |
| `OPEN_FAILED` | The file cannot be opened; an empty file without `columns` (with `columns` it has zero rows); two header columns with one name; a header whose width differs from `columns`; a malformed or ragged record in the inference sample. |
| `PARSE_FAILED` | A later record with another number of fields than the header (`line 3: 2 fields, expected 3`), or a value that does not fit its column. |
| `SCHEMA_MISMATCH` | A file's schema differs from the stream's ([above](#one-schema-per-run)). |
| `NO_INPUT` | No file arrived and `columns` is not declared. |

A blank header name becomes `col_<i>` (0-based position).
