# Excel (`type: excel`)

A **parser**: it turns one sheet of each workbook a raw source announces (for
example the [`file` source](./file.md#source)) into one Arrow stream for the
destination. See the engine's [`parser:` block](https://loadsmith-el.github.io/loadsmith/reference/pipeline-yaml.html#parser)
and [Writing a Parser](https://loadsmith-el.github.io/loadsmith/plugins/writing-a-parser.html)
for how the core runs a parser.

- Parser binary: `loadsmith-parser-excel`
- Formats: `.xlsx`, `.xlsm`, `.xlsb`, `.xls` and `.ods`, chosen by the file's
  extension (pure-Rust [calamine](https://crates.io/crates/calamine)).

```yaml
source:
  type: file
  config: { path: /mnt/reports, glob: "*.xlsx" }
parser:
  type: excel                   # → binary loadsmith-parser-excel
  config:
    sheet: DATA
    skip_rows: 2
destination:
  type: parquet
  config: { … }
```

**Memory.** A workbook is a compressed archive that is read whole: the
selected sheet of **one** file is held in memory while its batches are cut,
then released before the next file. (The [`csv`](./csv.md) parser streams.)

## Config

| Field | Type | Required | Description |
|---|---|---|---|
| `sheet` | string or integer | no (default: the first sheet) | The sheet to read: its name, or its 0-based position. A workbook that lacks it fails to open. |
| `header` | bool | no (default `true`) | The first row (after `skip_rows`) names the columns. Without it, the columns are `col_0`, `col_1`, … |
| `skip_rows` | integer | no (default `0`) | Rows dropped from the top of the sheet before the header (banners, titles). |
| `columns` | list | no | Declared columns, as for [`csv`](./csv.md#declared-columns): the stream's schema instead of inference. |
| `batch_size` | integer | no (default `1000`) | Rows per Arrow batch. At least `1`. |

The `config:` block may be omitted entirely. Unknown keys are rejected (`UNKNOWN_CONFIG_KEYS`, with a "did you mean"), or only warned about with `--ignore-unknown-keys`.

The sheet's used range defines its width: rows are padded with nulls to the
widest one. A cell with an Excel error (`#N/A`, `#DIV/0!`) is a null.

## Type inference

Without `columns`, each column's type is inferred from **every** row of the
sheet (it is in memory anyway), with the [`sharepoint`](./sharepoint.md)
rules. Every inferred column is nullable; empty cells do not constrain the
type.

| Cells | Type |
|---|---|
| all integers | `int64` |
| numbers (integers mixed with decimals) | `float64` |
| all booleans | `bool` |
| all date-times | `timestamp(ms)` (no time zone) |
| text, mixed kinds, or no value at all | `utf8` |

Excel stores every number as a double, so numeric columns usually infer as
`float64`, even when they hold whole numbers. Declare the column as `int64`
(or `int32`, `decimal128`, …) to get integers.

A blank header name becomes `col_<i>` (0-based position); two header columns
with one name fail the file.

## Declared columns

`columns` takes the same field list as the [csv parser](./csv.md#declared-columns)
and the same types. Each cell is read through its text (`1.0` and `1` are
both `1`; a date-time is `YYYY-MM-DD HH:MM:SS`) and converted **strictly**: a
cell that does not fit, or an empty cell in a `nullable: false` column, fails
the run with its row (1-based, counting the skipped and header rows) and
column. With `header: true` the header row is skipped; when the sheet is not
empty it must have as many columns as `columns`. A run whose source announces
no file is a valid, empty load.

## One schema per run

All workbooks of a run go into one stream with one schema: without
`columns`, every file must infer the same columns as the first (names, types,
order), or the run fails with `SCHEMA_MISMATCH` (exit `1`). See
[csv](./csv.md#one-schema-per-run).

## Errors

A failure aborts the run (exit `1`) and nothing is committed.

| Code | When |
|---|---|
| `CONFIGURE_FAILED` | An invalid option (exit `3`). |
| `OPEN_FAILED` | Not a readable workbook (by its extension); the sheet is missing (`sheet 'DATA' not found`, `sheet index 3 out of range`); an empty sheet without `columns`; duplicate header names; a width that differs from `columns`. |
| `PARSE_FAILED` | With `columns`, a cell that does not convert. |
| `SCHEMA_MISMATCH` | A file's schema differs from the stream's. |
| `NO_INPUT` | No file arrived and `columns` is not declared. |
