//! MySQL ⇄ Arrow type mapping.
//!
//! Symmetric with the postgres plugin's text-protocol approach: rows come back
//! over `mysql_async`'s text protocol as `Value::Bytes` (the server's own text
//! rendering), so DECIMAL / TIME / and friends round-trip losslessly by mapping
//! to `Utf8`. A destination that needs native decimals parses the string itself.

use anyhow::{bail, Result};
use arrow_array::{
    builder::{
        BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
        Int32Builder, Int64Builder, StringBuilder, TimestampMillisecondBuilder,
    },
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array,
    Int32Array, Int64Array, LargeStringArray, RecordBatch, StringArray, TimestampMillisecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::{NaiveDate, NaiveDateTime};
use mysql_async::consts::{ColumnFlags, ColumnType};
use mysql_async::{Column, Row, Value};
use std::sync::Arc;

/// MySQL `binary` collation id — distinguishes BLOB/STRING (binary) from
/// TEXT/CHAR (a real character set).
const BINARY_CHARSET: u16 = 63;

static UNIX_EPOCH: std::sync::OnceLock<NaiveDate> = std::sync::OnceLock::new();
fn unix_epoch() -> NaiveDate {
    *UNIX_EPOCH.get_or_init(|| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
}

// ── Schema inference (source) ───────────────────────────────────────────────

/// Maps a MySQL column to an Arrow `DataType`.
///
/// Integers → `Int32`/`Int64`; `FLOAT`/`DOUBLE` → float; `DATE` → `Date32`;
/// `DATETIME`/`TIMESTAMP` → `Timestamp(ms)`; BLOB/STRING families split on the
/// binary collation (binary → `Binary`, else `Utf8`); everything else
/// (DECIMAL, TIME, BIT, JSON, ENUM, SET, …) → `Utf8` (text-preserving).
pub fn column_to_arrow(col: &Column) -> DataType {
    use ColumnType::*;
    let binary = col.character_set() == BINARY_CHARSET;
    match col.column_type() {
        MYSQL_TYPE_TINY | MYSQL_TYPE_SHORT | MYSQL_TYPE_INT24 | MYSQL_TYPE_LONG
        | MYSQL_TYPE_YEAR => DataType::Int32,
        MYSQL_TYPE_LONGLONG => DataType::Int64,
        MYSQL_TYPE_FLOAT => DataType::Float32,
        MYSQL_TYPE_DOUBLE => DataType::Float64,
        MYSQL_TYPE_DATE => DataType::Date32,
        MYSQL_TYPE_DATETIME | MYSQL_TYPE_TIMESTAMP => {
            DataType::Timestamp(TimeUnit::Millisecond, None)
        }
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

/// Builds an Arrow `Schema` from a slice of MySQL columns (all nullable — the
/// pump validates against the destination, not the source's NOT NULL flags).
pub fn columns_to_schema(cols: &[Column]) -> Schema {
    let fields: Vec<Field> = cols
        .iter()
        .map(|c| Field::new(c.name_str().as_ref(), column_to_arrow(c), true))
        .collect();
    Schema::new(fields)
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

        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let mut b = TimestampMillisecondBuilder::with_capacity(rows.len());
            for row in rows {
                match row.as_ref(idx).and_then(value_text) {
                    None => b.append_null(),
                    Some(v) => b.append_value(
                        parse_timestamp_millis(&v)
                            .map_err(|e| anyhow::anyhow!("col {name}: ts parse '{v}': {e}"))?,
                    ),
                }
            }
            Ok(Arc::new(b.finish()))
        }

        DataType::Binary => {
            let mut b = BinaryBuilder::with_capacity(rows.len(), rows.len() * 16);
            for row in rows {
                match row.as_ref(idx) {
                    None | Some(Value::NULL) => b.append_null(),
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

fn parse_timestamp_millis(s: &str) -> Result<i64> {
    let dt = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(dt.and_utc().timestamp_millis())
}

// ── Arrow → MySQL Value (destination) ───────────────────────────────────────

/// Converts one Arrow cell to a `mysql_async::Value` for parameter binding.
/// Temporal and decimal-like types are rendered to their MySQL text form and
/// sent as `Bytes`; the server coerces them into the target column type.
pub fn cell_to_value(array: &dyn Array, row: usize) -> Result<Value> {
    if array.is_null(row) {
        return Ok(Value::NULL);
    }
    let v = match array.data_type() {
        DataType::Int32 => Value::Int(downcast::<Int32Array>(array).value(row) as i64),
        DataType::Int64 => Value::Int(downcast::<Int64Array>(array).value(row)),
        DataType::Float32 => Value::Float(downcast::<Float32Array>(array).value(row)),
        DataType::Float64 => Value::Double(downcast::<Float64Array>(array).value(row)),
        DataType::Boolean => Value::Int(downcast::<BooleanArray>(array).value(row) as i64),
        DataType::Utf8 => Value::Bytes(downcast::<StringArray>(array).value(row).as_bytes().to_vec()),
        DataType::LargeUtf8 => {
            Value::Bytes(downcast::<LargeStringArray>(array).value(row).as_bytes().to_vec())
        }
        DataType::Binary => Value::Bytes(downcast::<BinaryArray>(array).value(row).to_vec()),
        DataType::Date32 => {
            let days = downcast::<Date32Array>(array).value(row);
            let date = unix_epoch()
                .checked_add_signed(chrono::Duration::days(days as i64))
                .ok_or_else(|| anyhow::anyhow!("date32 out of range: {days}"))?;
            Value::Bytes(date.format("%Y-%m-%d").to_string().into_bytes())
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let ms = downcast::<TimestampMillisecondArray>(array).value(row);
            let dt = chrono::DateTime::from_timestamp_millis(ms)
                .ok_or_else(|| anyhow::anyhow!("timestamp out of range: {ms}"))?;
            Value::Bytes(dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.f").to_string().into_bytes())
        }
        other => bail!("mysql destination does not support Arrow type {other:?} yet"),
    };
    Ok(v)
}

fn downcast<T: 'static>(array: &dyn Array) -> &T {
    array.as_any().downcast_ref::<T>().expect("arrow array downcast")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{Field, Schema};

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
    }

    #[test]
    fn schema_from_no_columns_is_empty() {
        let s = columns_to_schema(&[]);
        assert_eq!(s.fields().len(), 0);
        // keep helpers referenced
        let _ = Schema::new(vec![Field::new("x", DataType::Utf8, true)]);
    }
}
