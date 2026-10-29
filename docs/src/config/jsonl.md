# JSONL (`type: jsonl`)

A destination that writes newline-delimited JSON — one JSON object per row,
followed by a newline — to a file.

- Destination binary: `loadsmith-destination-jsonl`

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `path` | string | **yes** | — | Output file path. |
| `temporal_format` | `iso8601` / `epoch` | no | `iso8601` | How dates, times, timestamps and durations are written (see [Temporal values](#temporal-values)). |
| `epoch_unit` | `column` / `s` / `ms` / `us` / `ns` | no | `ms` | Only with `temporal_format: epoch`: the unit of the integers. `column` keeps each column's own unit. |

```yaml
destination:
  type: jsonl
  config:
    path: /output/data.jsonl
    # temporal_format: epoch     # opt-in: integers instead of ISO-8601 text
    # epoch_unit: column         # …in each column's own unit (no precision lost)
```

Unknown keys are rejected (or only warned about with `--ignore-unknown-keys`),
and enum values are kebab-case: a wrong one names the valid values and the
likely one (`temporal_format: iso` → ``did you mean `iso8601`?``).

## Temporal values

**Default (`iso8601`)**: ISO-8601 text with **the column's own precision**, so a
microsecond timestamp keeps its six fractional digits (e.g. from a postgres
source with `timestamp_unit: us`):

| Arrow | JSON |
|---|---|
| `Timestamp(s)` | `"2026-10-20T12:00:00"` |
| `Timestamp(ms)` | `"2026-10-20T12:00:00.123"` |
| `Timestamp(us)` | `"2026-10-20T12:00:00.123456"` |
| `Timestamp(ns)` | `"2026-10-20T12:00:00.123456789"` |
| `Timestamp(unit, <any zone>)` | the same, **in UTC with a `Z`**: `"2026-10-20T12:00:00.123Z"` |
| `Date32`, `Date64` | `"2026-10-20"` |
| `Time32`, `Time64` | `"12:00:00.123"` (digits by unit, like timestamps) |
| `Duration` | seconds: `"PT90S"`, `"-PT1.500S"` (digits by unit) |

A timestamp without a zone is a wall clock and is written without one.

**Opt-in (`temporal_format: epoch`)**: integers. `epoch_unit` picks the unit:
`ms` by default (the 0.1 output), `s`/`us`/`ns`, or `column` for each column's
own unit — the lossless choice.

| Arrow | JSON (`epoch`) |
|---|---|
| `Timestamp` (any zone) | since 1970-01-01T00:00:00Z, in `epoch_unit` |
| `Time32`, `Time64` | since midnight, in `epoch_unit` |
| `Duration` | in `epoch_unit` |
| `Date32` | days since 1970-01-01 (its stored form; `epoch_unit` does not apply) |
| `Date64` | milliseconds since the epoch (its stored form) |

Converting to a coarser unit floors (`1.999 s` → `1` in `s`; before 1970 too:
`-0.5 s` → `-1`); a finer unit that overflows a 64-bit integer gives `null`.

## Value mapping

Every Arrow type has a defined JSON form, and the conversion never fails a run
or panics:

| Arrow | JSON |
|---|---|
| null slot, `Null` | `null` |
| `Boolean` | `true` / `false` |
| `Int*`, `UInt*` | number, exact (a `uint64` above 2^53 may lose precision in some JSON readers) |
| `Float16/32/64` | number; **NaN and ±Infinity are written as `null`** (JSON has no spelling for them) |
| `Decimal128/256` | **string** with the scale applied, e.g. `"123.45"` (exact) |
| `Utf8`, `LargeUtf8`, `Utf8View` | string |
| `Binary`, `LargeBinary`, `BinaryView`, `FixedSizeBinary` | base64 string (standard, padded) |
| `FixedSizeBinary(16)` of the `arrow.uuid` extension | the UUID's canonical string (`a0eebc99-9c0b-4ef8-…`) |
| dates, times, timestamps, durations | see [Temporal values](#temporal-values) |
| `Interval` | `{"months": n}` / `{"days": d, "milliseconds": ms}` / `{"months": m, "days": d, "nanoseconds": ns}` |
| `List`, `LargeList`, `FixedSizeList` | array of the converted items (e.g. Postgres arrays) |
| `Struct` | object keyed by child name |
| `Map` | object (a non-string key is written as its JSON text) |
| `Dictionary` | the decoded value |
| `Union`, `RunEndEncoded`, `ListView` | `null` (outside the protocol's type vocabulary) |

A value with no representation — a date or timestamp outside the years
-262143…262142, a time of day past midnight — is `null`.

## Behavior notes

- **`path` is required.** The plugin's stdout is the protocol's control
  channel, so there is no "write to stdout" mode.
- **Cancel removes the file.** When the run does not complete (the core cancels
  it, the input stream is truncated, or a write fails), the partial file at
  `path` is deleted rather than left looking like a finished export.
- The file at `path` is created (truncated) when the destination starts, so a
  failed run also removes the previous run's output there.

## Migrating from 0.1

| 0.1 | 0.2 |
|---|---|
| `path` omitted → stdout | `path` is required |
| `Timestamp(ms)` → epoch milliseconds; any other unit **panicked** | ISO-8601 text with the column's precision, every unit and zone; the 0.1 integers are `temporal_format: epoch` (`epoch_unit: ms`) |
| `Date32` → integer days | `"2026-10-20"`; the integer is `temporal_format: epoch` |
| decimals, times, durations, lists, structs, unsigned and small integers, … → `null` | their full value (see [Value mapping](#value-mapping)) |
| NaN / ±Infinity → `0` | `null` |
| unknown keys ignored | rejected (or warned with `--ignore-unknown-keys`) |

## Telemetry

When the run has telemetry on (the engine's `telemetry:` block or `OTEL_*`
settings), the plugin records spans and metrics in-process and the engine
exports them over OTLP under the run's trace — the plugin itself opens no
connection for it, and records nothing when telemetry is off. Every plugin gets
the SDK's baseline: spans `configure`, `start`, `finish` and the per-batch /
per-object `loadsmith.*` spans, plus the `loadsmith.plugin.*` metrics
([engine design](https://github.com/loadsmith-el/loadsmith/blob/main/docs/src/architecture/telemetry.md)). On top of it, one `write <file>` span from the file's creation
(`start`) to its final flush (`finish`), with `file.path`, `file.size` and
`loadsmith.object.rows`; a cancelled run marks it `error`.
