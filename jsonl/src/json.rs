//! Arrow → JSON rows: the jsonl destination's own output format.
//!
//! Total and panic-free over every Arrow type (adapted from the engine's
//! `loadsmith_arrow::convert`, which the plugin no longer uses so that it owns
//! its format). Dates, times, timestamps and durations follow [`Temporal`]:
//! ISO-8601 text with the column's precision by default, or epoch integers.
//!
//! | Arrow | JSON |
//! |---|---|
//! | any null slot, `Null` | `null` |
//! | `Boolean` | `true` / `false` |
//! | `Int*`, `UInt*` | number (exact) |
//! | `Float16/32/64` | number; NaN and ±Infinity become `null` |
//! | `Decimal32/64/128/256` | string with the scale applied (`"123.45"`, exact) |
//! | `Utf8`, `LargeUtf8`, `Utf8View` | string |
//! | `Binary`, `LargeBinary`, `BinaryView`, `FixedSizeBinary` | base64 string (standard, padded) |
//! | `FixedSizeBinary(16)` of the `arrow.uuid` extension | the UUID's canonical string (`a0eebc99-9c0b-…`) |
//! | `Interval` | `{"months"}` / `{"days", "milliseconds"}` / `{"months", "days", "nanoseconds"}` |
//! | `List`, `LargeList`, `FixedSizeList` | array |
//! | `Struct` | object keyed by child name |
//! | `Map` | object (a non-string key is written as its JSON text) |
//! | `Dictionary` | the decoded value |
//! | `Union`, `RunEndEncoded`, `ListView`, `LargeListView` | `null` (outside the protocol vocabulary) |
//!
//! A value that has no representation (out of chrono's range, a time of day
//! past midnight, an epoch value overflowing `i64` in the requested unit) is
//! `null`.

use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::{Array, ArrowPrimitiveType, RecordBatch};
use arrow_schema::{DataType, IntervalUnit, TimeUnit};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};
use serde_json::{Map, Number, Value};

/// How temporal values are written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Temporal {
    /// ISO-8601 text with the column's precision: `2026-10-20T12:00:00.123`
    /// (a zoned timestamp ends in `Z`: it is written in UTC), `2026-10-20`,
    /// `12:00:00.123456`, `PT1.500S`.
    Iso,
    /// Integers: timestamps since the epoch, times since midnight and
    /// durations in the given unit (`None` = the column's own unit). Dates
    /// keep their stored form (`date32` days, `date64` milliseconds).
    Epoch(Option<TimeUnit>),
}

/// Converts each row of `batch` to a JSON object.
pub fn rows(batch: &RecordBatch, t: Temporal) -> Vec<Value> {
    let num_rows = batch.num_rows();
    let schema = batch.schema();
    let columns: Vec<Vec<Value>> =
        batch.columns().iter().zip(schema.fields().iter()).map(|(c, f)| field_values(c.as_ref(), f, t)).collect();
    let mut rows: Vec<Map<String, Value>> = (0..num_rows).map(|_| Map::new()).collect();
    for (field, values) in schema.fields().iter().zip(columns) {
        for (row, value) in rows.iter_mut().zip(values) {
            row.insert(field.name().clone(), value);
        }
    }
    rows.into_iter().map(Value::Object).collect()
}

/// The values of `arr`, a column of `field`: the field's metadata names the
/// `arrow.uuid` extension, which the array alone doesn't carry.
fn field_values(arr: &dyn Array, field: &arrow_schema::Field, t: Temporal) -> Vec<Value> {
    if is_uuid(field) {
        if let Some(a) = arr.as_fixed_size_binary_opt() {
            return a.iter().map(|v| v.map(|b| Value::String(uuid_text(b))).unwrap_or(Value::Null)).collect();
        }
    }
    column_values(arr, t)
}

fn is_uuid(field: &arrow_schema::Field) -> bool {
    field.metadata().get("ARROW:extension:name").map(String::as_str) == Some("arrow.uuid")
        && field.data_type() == &DataType::FixedSizeBinary(16)
}

/// `a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11`.
fn uuid_text(b: &[u8]) -> String {
    let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!("{}-{}-{}-{}-{}", &hex[..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..])
}

/// One JSON value per slot of `arr` (`arr.len()` values).
fn column_values(arr: &dyn Array, t: Temporal) -> Vec<Value> {
    let len = arr.len();
    let mut out = convert(arr, t).unwrap_or_else(|| vec![Value::Null; len]);
    out.resize(len, Value::Null);
    // Parent-level nulls win over whatever the child data holds.
    if arr.null_count() > 0 {
        for (i, v) in out.iter_mut().enumerate() {
            if arr.is_null(i) {
                *v = Value::Null;
            }
        }
    }
    out
}

fn prim<T, F>(arr: &dyn Array, f: F) -> Option<Vec<Value>>
where
    T: ArrowPrimitiveType,
    F: Fn(T::Native) -> Value,
{
    let a = arr.as_primitive_opt::<T>()?;
    Some(a.values().iter().map(|v| f(*v)).collect())
}

fn float(v: f64) -> Value {
    Number::from_f64(v).map(Value::Number).unwrap_or(Value::Null)
}

fn int(v: Option<i64>) -> Value {
    v.map(Value::from).unwrap_or(Value::Null)
}

fn text(s: Option<String>) -> Value {
    s.map(Value::String).unwrap_or(Value::Null)
}

/// Units per second.
fn per_second(unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

/// `v` in `from` → `to`: floored when `to` is coarser, `None` on overflow.
fn rescale(v: i64, from: TimeUnit, to: TimeUnit) -> Option<i64> {
    let (f, t) = (per_second(from), per_second(to));
    if t >= f {
        v.checked_mul(t / f)
    } else {
        Some(v.div_euclid(f / t))
    }
}

/// `.fff` with the unit's digits (none for seconds).
fn frac_format(unit: TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Second => "",
        TimeUnit::Millisecond => "%.3f",
        TimeUnit::Microsecond => "%.6f",
        TimeUnit::Nanosecond => "%.9f",
    }
}

fn datetime(v: i64, unit: TimeUnit) -> Option<NaiveDateTime> {
    let dt = match unit {
        TimeUnit::Second => DateTime::from_timestamp(v, 0),
        TimeUnit::Millisecond => DateTime::from_timestamp_millis(v),
        TimeUnit::Microsecond => DateTime::from_timestamp_micros(v),
        TimeUnit::Nanosecond => Some(DateTime::from_timestamp_nanos(v)),
    };
    dt.map(|d| d.naive_utc())
}

fn timestamp_iso(v: i64, unit: TimeUnit, zoned: bool) -> Option<String> {
    let dt = datetime(v, unit)?;
    let mut s = dt.format(&format!("%Y-%m-%dT%H:%M:%S{}", frac_format(unit))).to_string();
    if zoned {
        s.push('Z');
    }
    Some(s)
}

fn time_iso(v: i64, unit: TimeUnit) -> Option<String> {
    let per = per_second(unit);
    let secs = u32::try_from(v.div_euclid(per)).ok()?;
    let nanos = u32::try_from(v.rem_euclid(per) * (1_000_000_000 / per)).ok()?;
    let t = NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)?;
    Some(t.format(&format!("%H:%M:%S{}", frac_format(unit))).to_string())
}

/// ISO-8601 duration in seconds: `PT90S`, `-PT1.500S`.
fn duration_iso(v: i64, unit: TimeUnit) -> String {
    let per = per_second(unit).unsigned_abs();
    let abs = v.unsigned_abs();
    let sign = if v < 0 { "-" } else { "" };
    let digits = per.ilog10() as usize;
    if digits == 0 {
        format!("{sign}PT{abs}S")
    } else {
        format!("{sign}PT{}.{:0digits$}S", abs / per, abs % per)
    }
}

fn date32_iso(days: i32) -> Option<String> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
    let d = epoch.checked_add_signed(chrono::Duration::try_days(i64::from(days))?)?;
    Some(d.format("%Y-%m-%d").to_string())
}

fn date64_iso(ms: i64) -> Option<String> {
    Some(DateTime::from_timestamp_millis(ms)?.date_naive().format("%Y-%m-%d").to_string())
}

/// A timestamp, time of day or duration value in `unit`.
fn temporal(v: i64, unit: TimeUnit, t: Temporal, iso: impl Fn(i64, TimeUnit) -> Option<String>) -> Value {
    match t {
        Temporal::Iso => text(iso(v, unit)),
        Temporal::Epoch(to) => int(rescale(v, unit, to.unwrap_or(unit))),
    }
}

fn base64_values<'a>(it: impl Iterator<Item = Option<&'a [u8]>>) -> Vec<Value> {
    it.map(|b| b.map(|b| Value::String(base64_encode(b))).unwrap_or(Value::Null)).collect()
}

fn string_values<'a>(it: impl Iterator<Item = Option<&'a str>>) -> Vec<Value> {
    it.map(|s| s.map(|s| Value::String(s.to_string())).unwrap_or(Value::Null)).collect()
}

/// Splits converted child values into one JSON array per offset window.
fn windows(child: &[Value], offsets: impl Iterator<Item = (usize, usize)>) -> Vec<Value> {
    offsets
        .map(|(start, end)| match child.get(start..end) {
            Some(items) => Value::Array(items.to_vec()),
            None => Value::Null,
        })
        .collect()
}

fn offset_pairs<O: Copy + TryInto<usize>>(offsets: &[O]) -> impl Iterator<Item = (usize, usize)> + '_ {
    offsets.windows(2).map(|w| {
        let start = w[0].try_into().unwrap_or(0);
        let end = w[1].try_into().unwrap_or(0);
        (start, end.max(start))
    })
}

fn convert(arr: &dyn Array, t: Temporal) -> Option<Vec<Value>> {
    let dt = arr.data_type();
    Some(match dt {
        DataType::Null => vec![Value::Null; arr.len()],
        DataType::Boolean => arr.as_boolean_opt()?.iter().map(|b| b.map(Value::Bool).unwrap_or(Value::Null)).collect(),
        DataType::Int8 => prim::<Int8Type, _>(arr, Value::from)?,
        DataType::Int16 => prim::<Int16Type, _>(arr, Value::from)?,
        DataType::Int32 => prim::<Int32Type, _>(arr, Value::from)?,
        DataType::Int64 => prim::<Int64Type, _>(arr, Value::from)?,
        DataType::UInt8 => prim::<UInt8Type, _>(arr, Value::from)?,
        DataType::UInt16 => prim::<UInt16Type, _>(arr, Value::from)?,
        DataType::UInt32 => prim::<UInt32Type, _>(arr, Value::from)?,
        DataType::UInt64 => prim::<UInt64Type, _>(arr, Value::from)?,
        DataType::Float16 => prim::<Float16Type, _>(arr, |v| float(v.to_f64()))?,
        DataType::Float32 => prim::<Float32Type, _>(arr, |v| float(f64::from(v)))?,
        DataType::Float64 => prim::<Float64Type, _>(arr, float)?,
        DataType::Decimal32(..) => {
            let a = arr.as_primitive_opt::<Decimal32Type>()?;
            (0..a.len()).map(|i| Value::String(a.value_as_string(i))).collect()
        }
        DataType::Decimal64(..) => {
            let a = arr.as_primitive_opt::<Decimal64Type>()?;
            (0..a.len()).map(|i| Value::String(a.value_as_string(i))).collect()
        }
        DataType::Decimal128(..) => {
            let a = arr.as_primitive_opt::<Decimal128Type>()?;
            (0..a.len()).map(|i| Value::String(a.value_as_string(i))).collect()
        }
        DataType::Decimal256(..) => {
            let a = arr.as_primitive_opt::<Decimal256Type>()?;
            (0..a.len()).map(|i| Value::String(a.value_as_string(i))).collect()
        }
        DataType::Utf8 => string_values(arr.as_string_opt::<i32>()?.iter()),
        DataType::LargeUtf8 => string_values(arr.as_string_opt::<i64>()?.iter()),
        DataType::Utf8View => string_values(arr.as_string_view_opt()?.iter()),
        DataType::Binary => base64_values(arr.as_binary_opt::<i32>()?.iter()),
        DataType::LargeBinary => base64_values(arr.as_binary_opt::<i64>()?.iter()),
        DataType::BinaryView => base64_values(arr.as_binary_view_opt()?.iter()),
        DataType::FixedSizeBinary(_) => base64_values(arr.as_fixed_size_binary_opt()?.iter()),
        DataType::Date32 => match t {
            Temporal::Iso => prim::<Date32Type, _>(arr, |v| text(date32_iso(v)))?,
            Temporal::Epoch(_) => prim::<Date32Type, _>(arr, Value::from)?,
        },
        DataType::Date64 => match t {
            Temporal::Iso => prim::<Date64Type, _>(arr, |v| text(date64_iso(v)))?,
            Temporal::Epoch(_) => prim::<Date64Type, _>(arr, Value::from)?,
        },
        DataType::Timestamp(unit, tz) => {
            let (unit, zoned) = (*unit, tz.is_some());
            let f = |v: i64| temporal(v, unit, t, |v, u| timestamp_iso(v, u, zoned));
            match unit {
                TimeUnit::Second => prim::<TimestampSecondType, _>(arr, f)?,
                TimeUnit::Millisecond => prim::<TimestampMillisecondType, _>(arr, f)?,
                TimeUnit::Microsecond => prim::<TimestampMicrosecondType, _>(arr, f)?,
                TimeUnit::Nanosecond => prim::<TimestampNanosecondType, _>(arr, f)?,
            }
        }
        DataType::Time32(unit) => {
            let unit = *unit;
            let f = |v: i32| temporal(i64::from(v), unit, t, time_iso);
            match unit {
                TimeUnit::Second => prim::<Time32SecondType, _>(arr, f)?,
                TimeUnit::Millisecond => prim::<Time32MillisecondType, _>(arr, f)?,
                _ => return None,
            }
        }
        DataType::Time64(unit) => {
            let unit = *unit;
            let f = |v: i64| temporal(v, unit, t, time_iso);
            match unit {
                TimeUnit::Microsecond => prim::<Time64MicrosecondType, _>(arr, f)?,
                TimeUnit::Nanosecond => prim::<Time64NanosecondType, _>(arr, f)?,
                _ => return None,
            }
        }
        DataType::Duration(unit) => {
            let unit = *unit;
            let f = |v: i64| temporal(v, unit, t, |v, u| Some(duration_iso(v, u)));
            match unit {
                TimeUnit::Second => prim::<DurationSecondType, _>(arr, f)?,
                TimeUnit::Millisecond => prim::<DurationMillisecondType, _>(arr, f)?,
                TimeUnit::Microsecond => prim::<DurationMicrosecondType, _>(arr, f)?,
                TimeUnit::Nanosecond => prim::<DurationNanosecondType, _>(arr, f)?,
            }
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            prim::<IntervalYearMonthType, _>(arr, |m| serde_json::json!({ "months": m }))?
        }
        DataType::Interval(IntervalUnit::DayTime) => prim::<IntervalDayTimeType, _>(arr, |v| {
            serde_json::json!({ "days": v.days, "milliseconds": v.milliseconds })
        })?,
        DataType::Interval(IntervalUnit::MonthDayNano) => prim::<IntervalMonthDayNanoType, _>(arr, |v| {
            serde_json::json!({ "months": v.months, "days": v.days, "nanoseconds": v.nanoseconds })
        })?,
        DataType::List(item) => {
            let a = arr.as_list_opt::<i32>()?;
            windows(&field_values(a.values().as_ref(), item, t), offset_pairs(a.value_offsets()))
        }
        DataType::LargeList(item) => {
            let a = arr.as_list_opt::<i64>()?;
            windows(&field_values(a.values().as_ref(), item, t), offset_pairs(a.value_offsets()))
        }
        DataType::FixedSizeList(_, size) => {
            let a = arr.as_fixed_size_list_opt()?;
            let size = usize::try_from(*size).ok()?;
            // The child array covers the whole (unsliced) parent: start at
            // this array's offset.
            let child = column_values(a.values().as_ref(), t);
            let first = a.offset();
            windows(&child, (0..a.len()).map(|i| ((first + i) * size, (first + i + 1) * size)))
        }
        DataType::Struct(_) => {
            let a = arr.as_struct_opt()?;
            let names = a.column_names();
            let children: Vec<Vec<Value>> = a.columns().iter().map(|c| column_values(c.as_ref(), t)).collect();
            (0..a.len())
                .map(|row| {
                    let mut obj = Map::new();
                    for (name, values) in names.iter().zip(&children) {
                        obj.insert((*name).to_string(), values.get(row).cloned().unwrap_or(Value::Null));
                    }
                    Value::Object(obj)
                })
                .collect()
        }
        DataType::Map(..) => {
            let a = arr.as_map_opt()?;
            let keys = column_values(a.keys().as_ref(), t);
            let values = column_values(a.values().as_ref(), t);
            offset_pairs(a.value_offsets())
                .map(|(start, end)| {
                    let mut obj = Map::new();
                    for i in start..end {
                        let key = match keys.get(i) {
                            Some(Value::String(s)) => s.clone(),
                            Some(other) => other.to_string(),
                            None => continue,
                        };
                        obj.insert(key, values.get(i).cloned().unwrap_or(Value::Null));
                    }
                    Value::Object(obj)
                })
                .collect()
        }
        DataType::Dictionary(..) => {
            let a = arr.as_any_dictionary_opt()?;
            let values = column_values(a.values().as_ref(), t);
            a.normalized_keys()
                .into_iter()
                .map(|k| values.get(k).cloned().unwrap_or(Value::Null))
                .collect()
        }
        DataType::Union(..)
        | DataType::RunEndEncoded(..)
        | DataType::ListView(_)
        | DataType::LargeListView(_) => return None,
    })
}

fn base64_encode(bytes: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
        let b2 = chunk.get(2).copied().unwrap_or(0) as usize;
        out.push(CHARS[b0 >> 2] as char);
        out.push(CHARS[((b0 & 3) << 4) | (b1 >> 4)] as char);
        out.push(if chunk.len() > 1 { CHARS[((b1 & 0xf) << 2) | (b2 >> 6)] as char } else { '=' });
        out.push(if chunk.len() > 2 { CHARS[b2 & 0x3f] as char } else { '=' });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{Int32Builder, ListBuilder, MapBuilder, StringBuilder};
    use arrow_array::*;
    use arrow_schema::{Field, Schema};
    use serde_json::json;
    use std::sync::Arc;

    fn values_as(arr: ArrayRef, t: Temporal) -> Vec<Value> {
        let schema = Arc::new(Schema::new(vec![Field::new("c", arr.data_type().clone(), true)]));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();
        rows(&batch, t).into_iter().map(|mut r| r["c"].take()).collect()
    }

    fn iso(arr: ArrayRef) -> Vec<Value> {
        values_as(arr, Temporal::Iso)
    }

    /// 2026-10-20T12:00:00.123456789 UTC in nanoseconds.
    const NS: i64 = 1_792_497_600_123_456_789;

    #[test]
    fn timestamps_keep_the_column_precision() {
        assert_eq!(iso(Arc::new(TimestampSecondArray::from(vec![NS / 1_000_000_000]))), [json!("2026-10-20T12:00:00")]);
        assert_eq!(
            iso(Arc::new(TimestampMillisecondArray::from(vec![NS / 1_000_000]).with_timezone("UTC"))),
            [json!("2026-10-20T12:00:00.123Z")]
        );
        assert_eq!(iso(Arc::new(TimestampMicrosecondArray::from(vec![NS / 1_000]))), [json!("2026-10-20T12:00:00.123456")]);
        assert_eq!(
            iso(Arc::new(TimestampNanosecondArray::from(vec![NS]).with_timezone("+02:00"))),
            [json!("2026-10-20T12:00:00.123456789Z")]
        );
        // Before the epoch, and out of chrono's range.
        assert_eq!(iso(Arc::new(TimestampMicrosecondArray::from(vec![-1]))), [json!("1969-12-31T23:59:59.999999")]);
        assert_eq!(iso(Arc::new(TimestampSecondArray::from(vec![i64::MAX]))), [Value::Null]);
    }

    #[test]
    fn dates_times_and_durations_in_iso() {
        assert_eq!(iso(Arc::new(Date32Array::from(vec![20746, -1]))), [json!("2026-10-20"), json!("1969-12-31")]);
        assert_eq!(iso(Arc::new(Date64Array::from(vec![86_400_000]))), [json!("1970-01-02")]);
        assert_eq!(iso(Arc::new(Time32SecondArray::from(vec![43_200]))), [json!("12:00:00")]);
        assert_eq!(iso(Arc::new(Time32MillisecondArray::from(vec![43_200_123]))), [json!("12:00:00.123")]);
        assert_eq!(iso(Arc::new(Time64MicrosecondArray::from(vec![1_000_001]))), [json!("00:00:01.000001")]);
        assert_eq!(iso(Arc::new(Time64NanosecondArray::from(vec![86_400_000_000_000]))), [Value::Null]);
        assert_eq!(iso(Arc::new(DurationSecondArray::from(vec![90]))), [json!("PT90S")]);
        assert_eq!(iso(Arc::new(DurationMillisecondArray::from(vec![-1_500]))), [json!("-PT1.500S")]);
        assert_eq!(iso(Arc::new(DurationNanosecondArray::from(vec![i64::MIN]))), [json!("-PT9223372036.854775808S")]);
    }

    #[test]
    fn epoch_in_the_column_unit_or_a_chosen_one() {
        let us: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![NS / 1_000, -1]));
        assert_eq!(values_as(us.clone(), Temporal::Epoch(None)), [json!(NS / 1_000), json!(-1)]);
        // Coarser is floored; finer overflows to null.
        assert_eq!(values_as(us, Temporal::Epoch(Some(TimeUnit::Millisecond))), [json!(NS / 1_000_000), json!(-1)]);
        let s: ArrayRef = Arc::new(TimestampSecondArray::from(vec![1, i64::MAX]));
        assert_eq!(values_as(s, Temporal::Epoch(Some(TimeUnit::Nanosecond))), [json!(1_000_000_000), Value::Null]);
        let ms = Temporal::Epoch(Some(TimeUnit::Millisecond));
        assert_eq!(values_as(Arc::new(Time64MicrosecondArray::from(vec![1_500_999])), ms), [json!(1500)]);
        assert_eq!(values_as(Arc::new(DurationSecondArray::from(vec![2])), ms), [json!(2000)]);
        // Dates keep their stored form.
        assert_eq!(values_as(Arc::new(Date32Array::from(vec![19000])), ms), [json!(19000)]);
        assert_eq!(values_as(Arc::new(Date64Array::from(vec![86_400_000])), ms), [json!(86_400_000)]);
    }

    #[test]
    fn uuids_are_their_canonical_text() {
        use arrow_array::FixedSizeBinaryArray;
        use arrow_schema::{Field, Schema};
        let bytes: Vec<u8> = (0u8..16).collect();
        let ids = FixedSizeBinaryArray::try_from_sparse_iter_with_size(vec![Some(bytes.clone()), None].into_iter(), 16).unwrap();
        let uuid = Field::new("id", DataType::FixedSizeBinary(16), true).with_metadata(
            std::collections::HashMap::from([("ARROW:extension:name".to_string(), "arrow.uuid".to_string())]),
        );
        let plain = Field::new("raw", DataType::FixedSizeBinary(16), true);
        let schema = Arc::new(Schema::new(vec![uuid, plain]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(ids.clone()), Arc::new(ids)]).unwrap();
        let out = rows(&batch, Temporal::Iso);
        assert_eq!(out[0]["id"], json!("00010203-0405-0607-0809-0a0b0c0d0e0f"));
        assert_eq!(out[1]["id"], json!(null));
        assert_eq!(out[0]["raw"], json!(base64_encode(&bytes)), "without the extension: bytes");
    }

    #[test]
    fn non_temporal_types() {
        assert_eq!(iso(Arc::new(UInt64Array::from(vec![u64::MAX]))), [json!(u64::MAX)]);
        assert_eq!(iso(Arc::new(Float64Array::from(vec![1.5, f64::NAN, f64::INFINITY]))), [json!(1.5), Value::Null, Value::Null]);
        let d = Decimal128Array::from(vec![Some(12345), Some(-5), None]).with_precision_and_scale(10, 2).unwrap();
        assert_eq!(iso(Arc::new(d)), [json!("123.45"), json!("-0.05"), Value::Null]);
        assert_eq!(iso(Arc::new(BinaryArray::from(vec![b"hi".as_ref()]))), [json!("aGk=")]);
        assert_eq!(iso(Arc::new(IntervalYearMonthArray::from(vec![14]))), [json!({"months": 14})]);
        assert_eq!(iso(Arc::new(NullArray::new(1))), [Value::Null]);
    }

    #[test]
    fn nested_values_follow_the_temporal_format() {
        let mut lb = ListBuilder::new(arrow_array::builder::Date32Builder::new());
        lb.values().append_value(0);
        lb.append(true);
        lb.append(false);
        assert_eq!(iso(Arc::new(lb.finish())), [json!(["1970-01-01"]), Value::Null]);

        let mut mb = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        mb.keys().append_value("a");
        mb.values().append_value(1);
        mb.append(true).unwrap();
        assert_eq!(iso(Arc::new(mb.finish())), [json!({"a": 1})]);

        let strukt = StructArray::from(vec![(
            Arc::new(Field::new("t", DataType::Time32(TimeUnit::Second), true)),
            Arc::new(Time32SecondArray::from(vec![1])) as ArrayRef,
        )]);
        assert_eq!(iso(Arc::new(strukt)), [json!({"t": "00:00:01"})]);
    }

    #[test]
    fn unsupported_layouts_are_null_not_a_panic() {
        let run_ends = Int32Array::from(vec![2]);
        let values = StringArray::from(vec!["x"]);
        let ree = RunArray::try_new(&run_ends, &values).unwrap();
        assert_eq!(iso(Arc::new(ree)), [Value::Null, Value::Null]);
    }
}
