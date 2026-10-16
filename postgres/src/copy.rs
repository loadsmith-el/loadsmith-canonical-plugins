//! Renders Arrow record batches into Postgres `COPY ... FROM STDIN (FORMAT
//! text)` payloads.
//!
//! We deliberately use the **text** COPY format (not binary), symmetric with the
//! postgres *source*'s text-protocol read path: Postgres parses each cell text
//! into the target column's own type, so NUMERIC / TIME / TIMESTAMP and friends
//! round-trip without the binary `ToSql`/`FromSql` pitfalls.

use anyhow::{bail, Result};
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array,
    Int64Array, LargeStringArray, RecordBatch, StringArray, TimestampMillisecondArray,
};
use arrow_schema::{DataType, TimeUnit};

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
            match cell_text(batch.column(col), row)? {
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

/// The text of one cell, or `None` for SQL NULL.
fn cell_text(array: &dyn Array, row: usize) -> Result<Option<String>> {
    if array.is_null(row) {
        return Ok(None);
    }
    let text = match array.data_type() {
        DataType::Int32 => downcast::<Int32Array>(array).value(row).to_string(),
        DataType::Int64 => downcast::<Int64Array>(array).value(row).to_string(),
        DataType::Float32 => downcast::<Float32Array>(array).value(row).to_string(),
        DataType::Float64 => downcast::<Float64Array>(array).value(row).to_string(),
        DataType::Boolean => {
            if downcast::<BooleanArray>(array).value(row) { "t".to_string() } else { "f".to_string() }
        }
        DataType::Utf8 => downcast::<StringArray>(array).value(row).to_string(),
        DataType::LargeUtf8 => downcast::<LargeStringArray>(array).value(row).to_string(),
        DataType::Date32 => {
            let days = downcast::<Date32Array>(array).value(row);
            let date = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap()
                .checked_add_signed(chrono::Duration::days(days as i64))
                .ok_or_else(|| anyhow::anyhow!("date32 out of range: {days}"))?;
            pg_date(date, "")
        }
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            let ms = downcast::<TimestampMillisecondArray>(array).value(row);
            let dt = chrono::DateTime::from_timestamp_millis(ms)
                .ok_or_else(|| anyhow::anyhow!("timestamp out of range: {ms}"))?
                .naive_utc();
            // A zone-aware value is a UTC instant: send it with an explicit
            // offset so a timestamptz column stores the right instant whatever
            // the session TimeZone (a plain timestamp column ignores it).
            let offset = if tz.is_some() { "+00" } else { "" };
            pg_date(dt.date(), &format!(" {}{offset}", dt.format("%H:%M:%S%.f")))
        }
        DataType::Binary => {
            let bytes = downcast::<BinaryArray>(array).value(row);
            // bytea in COPY text format is `\x<hex>` (the backslash is escaped to
            // `\\x` by `escape`, which Postgres reads back as a literal backslash).
            let mut s = String::with_capacity(2 + bytes.len() * 2);
            s.push_str("\\x");
            for b in bytes {
                s.push_str(&format!("{b:02x}"));
            }
            s
        }
        other => bail!("postgres destination does not support Arrow type {other:?} yet"),
    };
    Ok(Some(text))
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

fn downcast<T: 'static>(array: &dyn Array) -> &T {
    array.as_any().downcast_ref::<T>().expect("arrow array downcast")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

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
        let schema = Arc::new(Schema::new(vec![Field::new("ok", DataType::Boolean, false)]));
        let col = Arc::new(BooleanArray::from(vec![true, false]));
        let batch = RecordBatch::try_new(schema, vec![col]).unwrap();
        assert_eq!(batch_to_copy_text(&batch).unwrap(), "t\nf\n");
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
}
