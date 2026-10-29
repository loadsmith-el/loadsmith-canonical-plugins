//! MySQL ⇄ Arrow type mapping.
//!
//! Symmetric with the postgres plugin's text-protocol approach: rows come back
//! over `mysql_async`'s text protocol as `Value::Bytes` (the server's own text
//! rendering), so DECIMAL / TIME / and friends round-trip losslessly by mapping
//! to `Utf8`. A destination that needs native decimals parses the string itself.

use anyhow::{bail, Context, Result};
use arrow::array::AsArray;
use arrow::datatypes::{
    Date32Type, Date64Type, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Int8Type, Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
    UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{
    builder::{
        BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int32Builder,
        Int64Builder, StringBuilder, UInt64Builder,
    },
    Array, ArrayRef, Decimal128Array, Decimal256Array, RecordBatch, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::{NaiveDate, NaiveDateTime};
use mysql_async::consts::{ColumnFlags, ColumnType};
use mysql_async::{Column, Row, Value};
use std::sync::Arc;

use crate::vocab::{epoch_in, from_epoch, time_of_day};

/// MySQL `binary` collation id — distinguishes BLOB/STRING (binary) from
/// TEXT/CHAR (a real character set).
const BINARY_CHARSET: u16 = 63;

fn unix_epoch() -> NaiveDate {
    NaiveDate::default() // 1970-01-01
}

// ── Schema inference (source) ───────────────────────────────────────────────

/// Maps a MySQL column to an Arrow `DataType`.
///
/// - `TINYINT`/`SMALLINT`/`MEDIUMINT`/`INT`/`YEAR` → `Int32`, except `INT
///   UNSIGNED` → `Int64`; `BIGINT` → `Int64`, `BIGINT UNSIGNED` → `UInt64`
///   (every unsigned value fits, none fails).
/// - `FLOAT`/`DOUBLE` → float; `DATE` → `Date32`.
/// - `DATETIME` → `Timestamp(unit)`; `TIMESTAMP` → `Timestamp(unit, "UTC")`
///   (the session `time_zone` is pinned to UTC, so it reads as an instant).
/// - `BIT(n)` → `UInt64`; `GEOMETRY` → `Binary` (WKB).
/// - BLOB/STRING families split on the binary collation (binary → `Binary`,
///   else `Utf8`); everything else (DECIMAL, TIME, JSON, ENUM, SET, …) → `Utf8`.
pub fn column_to_arrow(col: &Column, unit: TimeUnit) -> DataType {
    use ColumnType::*;
    let binary = col.character_set() == BINARY_CHARSET;
    let unsigned = col.flags().contains(ColumnFlags::UNSIGNED_FLAG);
    match col.column_type() {
        MYSQL_TYPE_TINY | MYSQL_TYPE_SHORT | MYSQL_TYPE_INT24 | MYSQL_TYPE_YEAR => DataType::Int32,
        MYSQL_TYPE_LONG if unsigned => DataType::Int64,
        MYSQL_TYPE_LONG => DataType::Int32,
        MYSQL_TYPE_LONGLONG if unsigned => DataType::UInt64,
        MYSQL_TYPE_LONGLONG => DataType::Int64,
        MYSQL_TYPE_FLOAT => DataType::Float32,
        MYSQL_TYPE_DOUBLE => DataType::Float64,
        MYSQL_TYPE_DATE | MYSQL_TYPE_NEWDATE => DataType::Date32,
        MYSQL_TYPE_DATETIME | MYSQL_TYPE_DATETIME2 => DataType::Timestamp(unit, None),
        MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2 => DataType::Timestamp(unit, Some("UTC".into())),
        MYSQL_TYPE_BIT => DataType::UInt64,
        MYSQL_TYPE_GEOMETRY => DataType::Binary,
        MYSQL_TYPE_BLOB | MYSQL_TYPE_TINY_BLOB | MYSQL_TYPE_MEDIUM_BLOB | MYSQL_TYPE_LONG_BLOB
        | MYSQL_TYPE_STRING | MYSQL_TYPE_VAR_STRING | MYSQL_TYPE_VARCHAR => {
            if binary {
                DataType::Binary
            } else {
                DataType::Utf8
            }
        }
        _ => DataType::Utf8,
    }
}

/// Field metadata key carrying a MySQL type whose bytes need decoding:
/// `geometry` (a 4-byte SRID before the WKB) or `bit` (big-endian bytes).
/// Metadata is not part of the schema fingerprint.
const MYSQL_TYPE_KEY: &str = "mysql_type";

/// Builds an Arrow `Schema` from a slice of MySQL columns (all nullable — the
/// pump validates against the destination, not the source's NOT NULL flags).
pub fn columns_to_schema(cols: &[Column], unit: TimeUnit) -> Schema {
    let fields: Vec<Field> = cols
        .iter()
        .map(|c| {
            let f = Field::new(c.name_str().as_ref(), column_to_arrow(c, unit), true);
            let special = match c.column_type() {
                ColumnType::MYSQL_TYPE_GEOMETRY => Some("geometry"),
                ColumnType::MYSQL_TYPE_BIT => Some("bit"),
                _ => None,
            };
            match special {
                Some(t) => f.with_metadata(std::collections::HashMap::from([(MYSQL_TYPE_KEY.to_string(), t.to_string())])),
                None => f,
            }
        })
        .collect();
    Schema::new(fields)
}

fn mysql_type(field: &Field) -> Option<&str> {
    field.metadata().get(MYSQL_TYPE_KEY).map(String::as_str)
}

/// Whether a column was reported NOT NULL (used only for documentation/tests).
pub fn is_not_null(col: &Column) -> bool {
    col.flags().contains(ColumnFlags::NOT_NULL_FLAG)
}

// ── Row → RecordBatch (source) ──────────────────────────────────────────────

/// The text of one MySQL value, or `None` for SQL NULL.
pub(crate) fn value_text(v: &Value) -> Option<String> {
    match v {
        Value::NULL => None,
        Value::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
        Value::Int(i) => Some(i.to_string()),
        Value::UInt(u) => Some(u.to_string()),
        Value::Float(f) => Some(f.to_string()),
        Value::Double(d) => Some(d.to_string()),
        Value::Date(y, mo, d, h, mi, s, us) => Some(if *h == 0 && *mi == 0 && *s == 0 && *us == 0 {
            format!("{y:04}-{mo:02}-{d:02}")
        } else if *us == 0 {
            format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}")
        } else {
            format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}.{us:06}")
        }),
        Value::Time(neg, d, h, mi, s, us) => {
            let hours = *d * 24 + *h as u32;
            let sign = if *neg { "-" } else { "" };
            Some(if *us == 0 {
                format!("{sign}{hours:02}:{mi:02}:{s:02}")
            } else {
                format!("{sign}{hours:02}:{mi:02}:{s:02}.{us:06}")
            })
        }
    }
}

/// Converts a slice of rows (text protocol) into an Arrow `RecordBatch`.
pub fn rows_to_batch(rows: &[Row], schema: &Schema) -> Result<RecordBatch> {
    let arrays: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| build_column(rows, idx, field))
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(Arc::new(schema.clone()), arrays)
        .map_err(|e| anyhow::anyhow!("RecordBatch::try_new: {e}"))
}

fn build_column(rows: &[Row], idx: usize, field: &Field) -> Result<ArrayRef> {
    let name = field.name();
    macro_rules! parse_into {
        ($builder:expr, $ty:ty, $label:literal) => {{
            let mut b = $builder;
            for row in rows {
                match row.as_ref(idx).and_then(value_text) {
                    None => b.append_null(),
                    Some(v) => b.append_value(
                        v.parse::<$ty>()
                            .map_err(|e| anyhow::anyhow!("col {name}: {} parse '{v}': {e}", $label))?,
                    ),
                }
            }
            Ok(Arc::new(b.finish()) as ArrayRef)
        }};
    }

    match field.data_type() {
        DataType::Int32 => parse_into!(Int32Builder::with_capacity(rows.len()), i32, "int32"),
        DataType::Int64 => parse_into!(Int64Builder::with_capacity(rows.len()), i64, "int64"),
        DataType::Float32 => parse_into!(Float32Builder::with_capacity(rows.len()), f32, "float32"),
        DataType::Float64 => parse_into!(Float64Builder::with_capacity(rows.len()), f64, "float64"),

        // BIGINT UNSIGNED arrives as decimal text; BIT(n) as its raw
        // big-endian bytes (at most 8).
        DataType::UInt64 => {
            let bit = mysql_type(field) == Some("bit");
            let mut b = UInt64Builder::with_capacity(rows.len());
            for row in rows {
                match row.as_ref(idx) {
                    None | Some(Value::NULL) => b.append_null(),
                    Some(Value::UInt(u)) => b.append_value(*u),
                    Some(Value::Int(i)) => b.append_value(
                        u64::try_from(*i).map_err(|_| anyhow::anyhow!("col {name}: negative value {i}"))?,
                    ),
                    Some(Value::Bytes(bytes)) if bit => {
                        b.append_value(bit_value(bytes).with_context(|| format!("col {name}"))?)
                    }
                    Some(Value::Bytes(bytes)) => {
                        let s = String::from_utf8_lossy(bytes);
                        b.append_value(
                            s.parse::<u64>().map_err(|e| anyhow::anyhow!("col {name}: uint64 parse '{s}': {e}"))?,
                        )
                    }
                    Some(other) => bail!("col {name}: unexpected value {other:?} for an unsigned column"),
                }
            }
            Ok(Arc::new(b.finish()))
        }

        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                match row.as_ref(idx).and_then(value_text).as_deref() {
                    None => b.append_null(),
                    Some("1") | Some("true") | Some("t") => b.append_value(true),
                    Some("0") | Some("false") | Some("f") => b.append_value(false),
                    Some(v) => bail!("col {name}: unexpected bool value '{v}'"),
                }
            }
            Ok(Arc::new(b.finish()))
        }

        DataType::Date32 => {
            let mut b = Date32Builder::with_capacity(rows.len());
            let epoch = unix_epoch();
            for row in rows {
                match row.as_ref(idx).and_then(value_text) {
                    None => b.append_null(),
                    Some(v) => {
                        let d = NaiveDate::parse_from_str(&v, "%Y-%m-%d")
                            .map_err(|e| anyhow::anyhow!("col {name}: date parse '{v}': {e}"))?;
                        b.append_value(d.signed_duration_since(epoch).num_days() as i32);
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }

        DataType::Timestamp(unit, tz) => {
            let mut out: Vec<Option<i64>> = Vec::with_capacity(rows.len());
            for row in rows {
                out.push(match row.as_ref(idx).and_then(value_text) {
                    None => None,
                    Some(v) => Some(
                        parse_timestamp(&v, *unit).map_err(|e| anyhow::anyhow!("col {name}: ts parse '{v}': {e}"))?,
                    ),
                });
            }
            let tz = tz.clone();
            Ok(match unit {
                TimeUnit::Second => Arc::new(TimestampSecondArray::from(out).with_timezone_opt(tz)) as ArrayRef,
                TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from(out).with_timezone_opt(tz)),
                TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from(out).with_timezone_opt(tz)),
                TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from(out).with_timezone_opt(tz)),
            })
        }

        DataType::Binary => {
            let geometry = mysql_type(field) == Some("geometry");
            let mut b = BinaryBuilder::with_capacity(rows.len(), rows.len() * 16);
            for row in rows {
                match row.as_ref(idx) {
                    None | Some(Value::NULL) => b.append_null(),
                    // MySQL's geometry format is a 4-byte SRID, then the WKB.
                    Some(Value::Bytes(bytes)) if geometry => b.append_value(
                        bytes.get(4..).ok_or_else(|| anyhow::anyhow!("col {name}: truncated geometry value"))?,
                    ),
                    Some(Value::Bytes(bytes)) => b.append_value(bytes),
                    Some(other) => {
                        // Non-bytes value for a binary column: fall back to text.
                        match value_text(other) {
                            None => b.append_null(),
                            Some(s) => b.append_value(s.as_bytes()),
                        }
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }

        DataType::Utf8 => {
            let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
            for row in rows {
                match row.as_ref(idx).and_then(value_text) {
                    None => b.append_null(),
                    Some(v) => b.append_value(v),
                }
            }
            Ok(Arc::new(b.finish()))
        }

        other => bail!("col {name}: unsupported arrow type {other:?}"),
    }
}

/// The value of a `BIT(n)` column's raw big-endian bytes.
fn bit_value(bytes: &[u8]) -> Result<u64> {
    if bytes.len() > 8 {
        bail!("BIT value longer than 64 bits");
    }
    Ok(bytes.iter().fold(0u64, |acc, b| (acc << 8) | *b as u64))
}

fn parse_timestamp(s: &str, unit: TimeUnit) -> Result<i64> {
    let dt = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").map_err(|e| anyhow::anyhow!("{e}"))?;
    epoch_in(dt, unit).map_err(|e| anyhow::anyhow!(e))
}

// ── Arrow → MySQL Value (destination) ───────────────────────────────────────

/// Converts one Arrow cell to a `mysql_async::Value` for parameter binding.
/// Temporal and decimal types are rendered to their MySQL text form and sent
/// as `Bytes`; the server coerces them into the target column type. A
/// timestamp is written as its UTC wall clock (the session `time_zone` is
/// pinned to UTC, so a `TIMESTAMP` column stores the right instant), in any
/// unit; MySQL keeps up to microseconds.
pub fn cell_to_value(array: &dyn Array, row: usize) -> Result<Value> {
    if array.is_null(row) {
        return Ok(Value::NULL);
    }
    macro_rules! prim {
        ($t:ty) => {
            array.as_primitive_opt::<$t>().context("array type mismatch")?.value(row)
        };
    }
    let text = |s: String| Value::Bytes(s.into_bytes());
    let v = match array.data_type() {
        DataType::Int8 => Value::Int(prim!(Int8Type) as i64),
        DataType::Int16 => Value::Int(prim!(Int16Type) as i64),
        DataType::Int32 => Value::Int(prim!(Int32Type) as i64),
        DataType::Int64 => Value::Int(prim!(Int64Type)),
        DataType::UInt8 => Value::UInt(prim!(UInt8Type) as u64),
        DataType::UInt16 => Value::UInt(prim!(UInt16Type) as u64),
        DataType::UInt32 => Value::UInt(prim!(UInt32Type) as u64),
        DataType::UInt64 => Value::UInt(prim!(UInt64Type)),
        DataType::Float16 => Value::Float(prim!(Float16Type).to_f32()),
        DataType::Float32 => Value::Float(prim!(Float32Type)),
        DataType::Float64 => Value::Double(prim!(Float64Type)),
        DataType::Decimal128(..) => text(downcast::<Decimal128Array>(array)?.value_as_string(row)),
        DataType::Decimal256(..) => text(downcast::<Decimal256Array>(array)?.value_as_string(row)),
        DataType::Boolean => Value::Int(array.as_boolean_opt().context("array type mismatch")?.value(row) as i64),
        DataType::Utf8 => text(array.as_string_opt::<i32>().context("array type mismatch")?.value(row).to_string()),
        DataType::LargeUtf8 => text(array.as_string_opt::<i64>().context("array type mismatch")?.value(row).to_string()),
        DataType::Utf8View => text(array.as_string_view_opt().context("array type mismatch")?.value(row).to_string()),
        DataType::Binary => Value::Bytes(array.as_binary_opt::<i32>().context("array type mismatch")?.value(row).to_vec()),
        DataType::LargeBinary => {
            Value::Bytes(array.as_binary_opt::<i64>().context("array type mismatch")?.value(row).to_vec())
        }
        DataType::BinaryView => Value::Bytes(array.as_binary_view_opt().context("array type mismatch")?.value(row).to_vec()),
        DataType::FixedSizeBinary(_) => {
            Value::Bytes(array.as_fixed_size_binary_opt().context("array type mismatch")?.value(row).to_vec())
        }
        DataType::Date32 => {
            let days = prim!(Date32Type);
            let date = unix_epoch()
                .checked_add_signed(chrono::Duration::days(days as i64))
                .ok_or_else(|| anyhow::anyhow!("date32 out of range: {days}"))?;
            text(date.format("%Y-%m-%d").to_string())
        }
        DataType::Date64 => {
            let ms = prim!(Date64Type);
            let dt = from_epoch(ms, TimeUnit::Millisecond).ok_or_else(|| anyhow::anyhow!("date64 out of range: {ms}"))?;
            text(dt.format("%Y-%m-%d").to_string())
        }
        DataType::Timestamp(unit, _) => {
            let v = match unit {
                TimeUnit::Second => prim!(TimestampSecondType),
                TimeUnit::Millisecond => prim!(TimestampMillisecondType),
                TimeUnit::Microsecond => prim!(TimestampMicrosecondType),
                TimeUnit::Nanosecond => prim!(TimestampNanosecondType),
            };
            let dt = from_epoch(v, *unit).ok_or_else(|| anyhow::anyhow!("timestamp out of range: {v}"))?;
            text(dt.format("%Y-%m-%d %H:%M:%S%.f").to_string())
        }
        DataType::Time32(unit) | DataType::Time64(unit) => {
            let v = match array.data_type() {
                DataType::Time32(TimeUnit::Second) => prim!(Time32SecondType) as i64,
                DataType::Time32(_) => prim!(Time32MillisecondType) as i64,
                DataType::Time64(TimeUnit::Microsecond) => prim!(Time64MicrosecondType),
                _ => prim!(Time64NanosecondType),
            };
            let t = time_of_day(v, *unit).ok_or_else(|| anyhow::anyhow!("time of day out of range: {v}"))?;
            text(t.format("%H:%M:%S%.f").to_string())
        }
        other => bail!("the mysql destination does not support Arrow type {other:?}"),
    };
    Ok(v)
}

fn downcast<T: 'static>(array: &dyn Array) -> Result<&T> {
    array.as_any().downcast_ref::<T>().context("array type mismatch")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray, Time64MicrosecondArray, UInt64Array};

    #[test]
    fn value_text_renders_variants() {
        assert_eq!(value_text(&Value::NULL), None);
        assert_eq!(value_text(&Value::Int(42)).unwrap(), "42");
        assert_eq!(value_text(&Value::Bytes(b"hi".to_vec())).unwrap(), "hi");
        assert_eq!(value_text(&Value::Date(2024, 1, 15, 0, 0, 0, 0)).unwrap(), "2024-01-15");
        assert_eq!(
            value_text(&Value::Date(2024, 1, 15, 12, 30, 45, 0)).unwrap(),
            "2024-01-15 12:30:45"
        );
    }

    #[test]
    fn cell_to_value_maps_types() {
        let ids = Int64Array::from(vec![Some(7), None]);
        assert_eq!(cell_to_value(&ids, 0).unwrap(), Value::Int(7));
        assert_eq!(cell_to_value(&ids, 1).unwrap(), Value::NULL);
        let names = StringArray::from(vec![Some("ab")]);
        assert_eq!(cell_to_value(&names, 0).unwrap(), Value::Bytes(b"ab".to_vec()));
        assert_eq!(cell_to_value(&UInt64Array::from(vec![u64::MAX]), 0).unwrap(), Value::UInt(u64::MAX));
        let d = Decimal128Array::from(vec![12345]).with_precision_and_scale(10, 3).unwrap();
        assert_eq!(cell_to_value(&d, 0).unwrap(), Value::Bytes(b"12.345".to_vec()));
    }

    #[test]
    fn cell_to_value_renders_every_temporal_unit() {
        let ts = TimestampNanosecondArray::from(vec![1_705_321_845_123_456_789]).with_timezone("UTC");
        assert_eq!(cell_to_value(&ts, 0).unwrap(), Value::Bytes(b"2024-01-15 12:30:45.123456789".to_vec()));
        let ts = TimestampSecondArray::from(vec![1_705_321_845]);
        assert_eq!(cell_to_value(&ts, 0).unwrap(), Value::Bytes(b"2024-01-15 12:30:45".to_vec()));
        let t = Time64MicrosecondArray::from(vec![3_723_000_500]);
        assert_eq!(cell_to_value(&t, 0).unwrap(), Value::Bytes(b"01:02:03.000500".to_vec()));
    }

    #[test]
    fn timestamps_parse_in_every_unit() {
        let s = "2024-01-15 12:30:45.123456";
        assert_eq!(parse_timestamp(s, TimeUnit::Millisecond).unwrap(), 1_705_321_845_123);
        assert_eq!(parse_timestamp(s, TimeUnit::Microsecond).unwrap(), 1_705_321_845_123_456);
        assert_eq!(parse_timestamp(s, TimeUnit::Second).unwrap(), 1_705_321_845);
    }

    #[test]
    fn bit_and_unsigned_values() {
        assert_eq!(bit_value(&[0x01, 0x00]).unwrap(), 256);
        assert_eq!(bit_value(&[0xff; 8]).unwrap(), u64::MAX);
        assert!(bit_value(&[0; 9]).is_err());
    }

    #[test]
    fn schema_from_no_columns_is_empty() {
        let s = columns_to_schema(&[], TimeUnit::Millisecond);
        assert_eq!(s.fields().len(), 0);
    }
}
