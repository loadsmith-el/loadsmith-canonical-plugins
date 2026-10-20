//! Renders Arrow record batches into Postgres `COPY ... FROM STDIN (FORMAT
//! text)` payloads.
//!
//! We deliberately use the **text** COPY format (not binary), symmetric with the
//! postgres *source*'s text-protocol read path: Postgres parses each cell text
//! into the target column's own type, so NUMERIC / TIME / TIMESTAMP and friends
//! round-trip without the binary `ToSql`/`FromSql` pitfalls.
//!
//! Accepted Arrow types: every integer width (signed and unsigned), floats,
//! `Decimal128`/`Decimal256`, booleans, strings (`Utf8`, `LargeUtf8`,
//! `Utf8View`), binaries (`Binary`, `LargeBinary`, `FixedSizeBinary`,
//! `BinaryView`), `Date32`/`Date64`, `Timestamp` in every unit with or without
//! a zone, `Time32`/`Time64` in every unit, and `List`/`LargeList`/
//! `FixedSizeList` of any of these (written as Postgres arrays; nested lists
//! as multi-dimensional arrays, which Postgres requires to be rectangular).

use anyhow::{bail, Context, Result};
use arrow::array::AsArray;
use arrow::datatypes::{
    Date32Type, Date64Type, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Int8Type, Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
    UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{Array, ArrayRef, Decimal128Array, Decimal256Array, RecordBatch};
use arrow_schema::{DataType, TimeUnit};

use crate::vocab::{from_epoch, time_of_day};

/// Builds the full text payload for a batch: one line per row, cells separated
/// by tabs, terminated by `\n`. NULLs are rendered `\N`.
pub fn batch_to_copy_text(batch: &RecordBatch) -> Result<String> {
    let ncols = batch.num_columns();
    let nrows = batch.num_rows();
    let mut out = String::with_capacity(nrows * ncols * 8);

    for row in 0..nrows {
        for col in 0..ncols {
            if col > 0 {
                out.push('\t');
            }
            let name = batch.schema_ref().field(col).name().clone();
            match value_text(batch.column(col).as_ref(), row).with_context(|| format!("column `{name}`"))? {
                Some(s) => out.push_str(&escape(&s)),
                None => out.push_str("\\N"),
            }
        }
        out.push('\n');
    }
    Ok(out)
}

/// Escapes a cell for COPY text format: backslash and the field/row delimiters.
fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            _ => out.push(c),
        }
    }
    out
}

/// The Postgres input text of one value, or `None` for SQL NULL.
fn value_text(array: &dyn Array, row: usize) -> Result<Option<String>> {
    if array.is_null(row) {
        return Ok(None);
    }
    macro_rules! prim {
        ($t:ty) => {
            array.as_primitive_opt::<$t>().context("array type mismatch")?.value(row).to_string()
        };
    }
    let text = match array.data_type() {
        DataType::Int8 => prim!(Int8Type),
        DataType::Int16 => prim!(Int16Type),
        DataType::Int32 => prim!(Int32Type),
        DataType::Int64 => prim!(Int64Type),
        DataType::UInt8 => prim!(UInt8Type),
        DataType::UInt16 => prim!(UInt16Type),
        DataType::UInt32 => prim!(UInt32Type),
        DataType::UInt64 => prim!(UInt64Type),
        DataType::Float16 => prim!(Float16Type),
        DataType::Float32 => {
            let v = array.as_primitive_opt::<Float32Type>().context("array type mismatch")?.value(row);
            if v.is_finite() { v.to_string() } else { float_text(v as f64) }
        }
        DataType::Float64 => float_text(array.as_primitive_opt::<Float64Type>().context("array type mismatch")?.value(row)),
        DataType::Decimal128(..) => downcast::<Decimal128Array>(array)?.value_as_string(row),
        DataType::Decimal256(..) => downcast::<Decimal256Array>(array)?.value_as_string(row),
        DataType::Boolean => {
            if array.as_boolean_opt().context("array type mismatch")?.value(row) { "t" } else { "f" }.to_string()
        }
        DataType::Utf8 => array.as_string_opt::<i32>().context("array type mismatch")?.value(row).to_string(),
        DataType::LargeUtf8 => array.as_string_opt::<i64>().context("array type mismatch")?.value(row).to_string(),
        DataType::Utf8View => array.as_string_view_opt().context("array type mismatch")?.value(row).to_string(),
        DataType::Binary => bytea(array.as_binary_opt::<i32>().context("array type mismatch")?.value(row)),
        DataType::LargeBinary => bytea(array.as_binary_opt::<i64>().context("array type mismatch")?.value(row)),
        DataType::BinaryView => bytea(array.as_binary_view_opt().context("array type mismatch")?.value(row)),
        DataType::FixedSizeBinary(_) => bytea(array.as_fixed_size_binary_opt().context("array type mismatch")?.value(row)),
        DataType::Date32 => {
            let days = array.as_primitive_opt::<Date32Type>().context("array type mismatch")?.value(row);
            let date = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                .and_then(|e| e.checked_add_signed(chrono::Duration::days(days as i64)))
                .ok_or_else(|| anyhow::anyhow!("date32 out of range: {days}"))?;
            pg_date(date, "")
        }
        DataType::Date64 => {
            let ms = array.as_primitive_opt::<Date64Type>().context("array type mismatch")?.value(row);
            let dt = from_epoch(ms, TimeUnit::Millisecond).ok_or_else(|| anyhow::anyhow!("date64 out of range: {ms}"))?;
            pg_date(dt.date(), "")
        }
        DataType::Timestamp(unit, tz) => {
            let v = timestamp_value(array, row, *unit)?;
            let dt = from_epoch(v, *unit).ok_or_else(|| anyhow::anyhow!("timestamp out of range: {v}"))?;
            // A zone-aware value is a UTC instant (whatever its zone): send it
            // with an explicit offset so a timestamptz column stores the right
            // instant whatever the session TimeZone (a plain timestamp column
            // ignores it).
            let offset = if tz.is_some() { "+00" } else { "" };
            pg_date(dt.date(), &format!(" {}{offset}", dt.format("%H:%M:%S%.f")))
        }
        DataType::Time32(unit) | DataType::Time64(unit) => {
            let v = match array.data_type() {
                DataType::Time32(TimeUnit::Second) => {
                    array.as_primitive_opt::<Time32SecondType>().context("array type mismatch")?.value(row) as i64
                }
                DataType::Time32(_) => {
                    array.as_primitive_opt::<Time32MillisecondType>().context("array type mismatch")?.value(row) as i64
                }
                DataType::Time64(TimeUnit::Microsecond) => {
                    array.as_primitive_opt::<Time64MicrosecondType>().context("array type mismatch")?.value(row)
                }
                _ => array.as_primitive_opt::<Time64NanosecondType>().context("array type mismatch")?.value(row),
            };
            time_of_day(v, *unit)
                .ok_or_else(|| anyhow::anyhow!("time of day out of range: {v}"))?
                .format("%H:%M:%S%.f")
                .to_string()
        }
        DataType::List(_) => array_literal(&array.as_list_opt::<i32>().context("array type mismatch")?.value(row))?,
        DataType::LargeList(_) => array_literal(&array.as_list_opt::<i64>().context("array type mismatch")?.value(row))?,
        DataType::FixedSizeList(..) => {
            array_literal(&array.as_fixed_size_list_opt().context("array type mismatch")?.value(row))?
        }
        other => bail!("the postgres destination does not support Arrow type {other:?}"),
    };
    Ok(Some(text))
}

/// The raw epoch value of a timestamp cell, in its own unit.
fn timestamp_value(array: &dyn Array, row: usize, unit: TimeUnit) -> Result<i64> {
    use arrow::datatypes::{
        TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
    };
    match unit {
        TimeUnit::Second => array.as_primitive_opt::<TimestampSecondType>().map(|a| a.value(row)),
        TimeUnit::Millisecond => array.as_primitive_opt::<TimestampMillisecondType>().map(|a| a.value(row)),
        TimeUnit::Microsecond => array.as_primitive_opt::<TimestampMicrosecondType>().map(|a| a.value(row)),
        TimeUnit::Nanosecond => array.as_primitive_opt::<TimestampNanosecondType>().map(|a| a.value(row)),
    }
    .context("array type mismatch")
}

/// Floats in Postgres' spelling: `NaN`, `Infinity`, `-Infinity` (Rust prints
/// `inf`, which Postgres rejects).
fn float_text(v: f64) -> String {
    if v.is_nan() {
        "NaN".into()
    } else if v.is_infinite() {
        if v > 0.0 { "Infinity" } else { "-Infinity" }.into()
    } else {
        v.to_string()
    }
}

/// bytea in COPY text format is `\x<hex>` (the backslash is escaped to `\\x`
/// by `escape`, which Postgres reads back as a literal backslash).
fn bytea(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(2 + bytes.len() * 2);
    s.push_str("\\x");
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// A Postgres array literal of a list value: `{"1","2",NULL}`. Every non-null
/// element is quoted (with `\` and `"` escaped), which Postgres accepts for
/// any element type; a nested list becomes a sub-array.
fn array_literal(items: &ArrayRef) -> Result<String> {
    let mut out = String::from("{");
    for i in 0..items.len() {
        if i > 0 {
            out.push(',');
        }
        let nested = matches!(items.data_type(), DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(..));
        match value_text(items.as_ref(), i)? {
            None => out.push_str("NULL"),
            Some(s) if nested => out.push_str(&s),
            Some(s) => {
                out.push('"');
                for c in s.chars() {
                    if c == '"' || c == '\\' {
                        out.push('\\');
                    }
                    out.push(c);
                }
                out.push('"');
            }
        }
    }
    out.push('}');
    Ok(out)
}

/// Formats a date the way Postgres reads it — years before 1 AD as `YYYY-MM-DD
/// BC` (astronomical year 0 = 1 BC) — with `time` (already formatted, may be
/// empty) inserted before the era suffix.
fn pg_date(date: chrono::NaiveDate, time: &str) -> String {
    use chrono::Datelike;
    let y = date.year();
    let (year, era) = if y <= 0 { (1 - y, " BC") } else { (y, "") };
    format!("{year:04}-{:02}-{:02}{time}{era}", date.month(), date.day())
}

fn downcast<T: 'static>(array: &dyn Array) -> Result<&T> {
    array.as_any().downcast_ref::<T>().context("array type mismatch")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{Int32Builder, ListBuilder, StringBuilder};
    use arrow_array::{
        BooleanArray, Date32Array, Int64Array, StringArray, Time64NanosecondArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt64Array,
    };
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    fn one(col: ArrayRef) -> String {
        let schema = Arc::new(Schema::new(vec![Field::new("c", col.data_type().clone(), true)]));
        batch_to_copy_text(&RecordBatch::try_new(schema, vec![col]).unwrap()).unwrap()
    }

    #[test]
    fn renders_rows_with_nulls_and_escaping() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let ids = Arc::new(Int64Array::from(vec![Some(1), Some(2)]));
        let names = Arc::new(StringArray::from(vec![Some("a\tb"), None]));
        let batch = RecordBatch::try_new(schema, vec![ids, names]).unwrap();

        let text = batch_to_copy_text(&batch).unwrap();
        assert_eq!(text, "1\ta\\tb\n2\t\\N\n");
    }

    #[test]
    fn renders_bool_as_t_f() {
        assert_eq!(one(Arc::new(BooleanArray::from(vec![true, false]))), "t\nf\n");
    }

    #[test]
    fn renders_temporal_with_bc_and_utc_offset() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("d", DataType::Date32, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("tstz", DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())), true),
        ]));
        // 0044-03-15 BC / 0044-03-15 12:00:00 BC / 2024-01-15 12:30:45.123 UTC
        let d = Arc::new(Date32Array::from(vec![-735_160]));
        let ts = Arc::new(TimestampMillisecondArray::from(vec![-63_517_780_800_000]));
        let tstz = Arc::new(TimestampMillisecondArray::from(vec![1_705_321_845_123]).with_timezone("UTC"));
        let batch = RecordBatch::try_new(schema, vec![d, ts, tstz]).unwrap();
        assert_eq!(
            batch_to_copy_text(&batch).unwrap(),
            "0044-03-15 BC\t0044-03-15 12:00:00 BC\t2024-01-15 12:30:45.123+00\n"
        );
    }

    #[test]
    fn renders_every_timestamp_unit_and_zone() {
        assert_eq!(one(Arc::new(TimestampSecondArray::from(vec![1_705_321_845]))), "2024-01-15 12:30:45\n");
        assert_eq!(
            one(Arc::new(TimestampMicrosecondArray::from(vec![1_705_321_845_123_456]).with_timezone("+02:00"))),
            "2024-01-15 12:30:45.123456+00\n"
        );
        assert_eq!(
            one(Arc::new(TimestampNanosecondArray::from(vec![1_705_321_845_123_456_789]))),
            "2024-01-15 12:30:45.123456789\n"
        );
        assert_eq!(one(Arc::new(Time64NanosecondArray::from(vec![3_723_000_000_001]))), "01:02:03.000000001\n");
    }

    #[test]
    fn renders_decimals_unsigned_and_special_floats() {
        let d = Decimal128Array::from(vec![12345, -5]).with_precision_and_scale(10, 2).unwrap();
        assert_eq!(one(Arc::new(d)), "123.45\n-0.05\n");
        assert_eq!(one(Arc::new(UInt64Array::from(vec![u64::MAX]))), "18446744073709551615\n");
        let f = arrow_array::Float64Array::from(vec![f64::NAN, f64::NEG_INFINITY]);
        assert_eq!(one(Arc::new(f)), "NaN\n-Infinity\n");
    }

    #[test]
    fn renders_lists_as_quoted_array_literals() {
        let mut b = ListBuilder::new(StringBuilder::new());
        b.values().append_value("a\"b");
        b.values().append_null();
        b.values().append_value("c,d\\");
        b.append(true);
        b.append(false);
        // COPY escaping doubles the backslashes once more.
        assert_eq!(one(Arc::new(b.finish())), "{\"a\\\\\"b\",NULL,\"c,d\\\\\\\\\"}\n\\N\n");

        let mut nested = ListBuilder::new(ListBuilder::new(Int32Builder::new()));
        for row in [[1, 2], [3, 4]] {
            for v in row {
                nested.values().values().append_value(v);
            }
            nested.values().append(true);
        }
        nested.append(true);
        assert_eq!(one(Arc::new(nested.finish())), "{{\"1\",\"2\"},{\"3\",\"4\"}}\n");
    }
}
