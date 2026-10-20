//! Text cells → typed Arrow columns, shared by the tabular parsers (`csv`,
//! `excel`): the declared `columns:` option, value inference from text, and
//! the strict conversion of a batch of text cells to a schema.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Result};
use arrow::compute::{cast_with_options, CastOptions};
use arrow_array::{
    builder::{BooleanBuilder, Date32Builder, Float64Builder, Int64Builder, StringBuilder, TimestampMillisecondBuilder},
    Array, ArrayRef, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Schema, SchemaRef, TimeUnit};
use chrono::{NaiveDate, NaiveDateTime};
use serde_json::Value;

use crate::csv::csv_cell;
use crate::table::Cell;

/// Parses a declared `columns:` list — `[{name, type, …params, nullable?}]`,
/// in the protocol's field-type vocabulary (`int64`, `utf8`,
/// `decimal128` + `precision`/`scale`, `timestamp` + `unit`/`timezone`, …) —
/// into a schema. Columns are nullable unless they say `nullable: false`.
/// Only scalar types a text cell can be converted to are accepted.
pub fn declared_schema(columns: &Value) -> Result<Schema> {
    let Some(list) = columns.as_array() else {
        bail!("`columns` must be a list of {{name, type}} objects");
    };
    if list.is_empty() {
        bail!("`columns` is empty");
    }
    let mut fields = Vec::with_capacity(list.len());
    let mut names: HashMap<String, usize> = HashMap::new();
    for (i, item) in list.iter().enumerate() {
        let n = i + 1;
        let field = loadsmith_protocol::Field::from_json(item).map_err(|e| anyhow::anyhow!("column {n}: {e}"))?;
        if field.field_type.is_unknown() {
            bail!("column {n} (`{}`): unknown type `{}`", field.name, field.field_type.type_name());
        }
        let Some(arrow) = loadsmith_arrow::arrow_field_from_protocol(&field) else {
            bail!("column {n} (`{}`): unsupported type `{}`", field.name, field.field_type);
        };
        if !supported(arrow.data_type()) {
            bail!("column {n} (`{}`): type `{}` cannot be read from a text cell", field.name, field.field_type);
        }
        if let Some(first) = names.insert(field.name.clone(), n) {
            bail!("columns {first} and {n} are both named `{}`", field.name);
        }
        fields.push(arrow);
    }
    Ok(Schema::new(fields))
}

/// The types a declared column may have.
fn supported(dt: &DataType) -> bool {
    use DataType::*;
    matches!(
        dt,
        Boolean
            | Int8
            | Int16
            | Int32
            | Int64
            | UInt8
            | UInt16
            | UInt32
            | UInt64
            | Float32
            | Float64
            | Decimal128(..)
            | Decimal256(..)
            | Utf8
            | LargeUtf8
            | Date32
            | Date64
            | Time32(_)
            | Time64(_)
            | Timestamp(..)
    )
}

const DATETIME_FORMATS: [&str; 2] = ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"];

fn parse_date(t: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(t, "%Y-%m-%d").ok()
}

fn parse_datetime(t: &str) -> Option<NaiveDateTime> {
    DATETIME_FORMATS.iter().find_map(|f| NaiveDateTime::parse_from_str(t, f).ok())
}

fn parse_bool(t: &str) -> Option<bool> {
    match t.to_ascii_lowercase().as_str() {
        "true" | "t" | "1" => Some(true),
        "false" | "f" | "0" => Some(false),
        _ => None,
    }
}

/// Infers a typed cell from a text value: the `sharepoint` CSV rules
/// ([`csv_cell`]: blank → null, `i64`, `f64`, else text), plus `true`/`false`
/// (any case) → bool, `YYYY-MM-DD` → date and `YYYY-MM-DD[T ]HH:MM:SS[.f]` →
/// datetime. Fed to [`crate::table::infer_type`], a column becomes int64,
/// float64, boolean, date32, timestamp(ms) or utf8.
pub fn infer_cell(s: &str) -> Cell {
    match csv_cell(s) {
        Cell::Str(text) => {
            let t = text.trim();
            if t.eq_ignore_ascii_case("true") {
                Cell::Bool(true)
            } else if t.eq_ignore_ascii_case("false") {
                Cell::Bool(false)
            } else if let Some(d) = parse_date(t) {
                Cell::Date(d)
            } else if let Some(dt) = parse_datetime(t) {
                Cell::DateTime(dt)
            } else {
                Cell::Str(text)
            }
        }
        other => other,
    }
}

/// Column names from a header row, as the parsers use them: a blank name
/// becomes `col_<i>` (0-based), and two columns with one name are an error.
pub fn header_names<'a>(header: impl IntoIterator<Item = &'a str>) -> Result<Vec<String>> {
    let mut seen: HashMap<String, usize> = HashMap::new();
    let mut out = Vec::new();
    for (i, name) in header.into_iter().enumerate() {
        let name = if name.trim().is_empty() { format!("col_{i}") } else { name.to_string() };
        if let Some(first) = seen.insert(name.clone(), i + 1) {
            bail!("columns {first} and {} are both named `{name}` in the header", i + 1);
        }
        out.push(name);
    }
    Ok(out)
}

/// A cell that does not fit its column's type.
#[derive(Debug, Clone, PartialEq)]
pub struct CellError {
    /// 0-based row within the batch.
    pub row: usize,
    /// 0-based column.
    pub column: usize,
    pub message: String,
}

/// Builds one batch from text cells, `columns[c][r]` being column `c` of row
/// `r` (`None` or blank = null). Every value must convert to its field's
/// type, and a non-nullable field may hold no null; otherwise the first
/// offending cell is the error. Text columns keep their value as is; every
/// other type reads the trimmed value.
pub fn text_batch(schema: &SchemaRef, columns: &[Vec<Option<&str>>]) -> Result<RecordBatch, CellError> {
    let mut arrays = Vec::with_capacity(columns.len());
    for (c, (field, values)) in schema.fields().iter().zip(columns).enumerate() {
        let array = text_array(values, field.data_type()).map_err(|(row, message)| CellError { row, column: c, message })?;
        if !field.is_nullable() && array.null_count() > 0 {
            let row = (0..array.len()).find(|&i| array.is_null(i)).unwrap_or(0);
            return Err(CellError { row, column: c, message: "empty value in a non-nullable column".into() });
        }
        arrays.push(array);
    }
    let rows = columns.first().map_or(0, Vec::len);
    RecordBatch::try_new_with_options(
        Arc::clone(schema),
        arrays,
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .map_err(|e| CellError { row: 0, column: 0, message: e.to_string() })
}

fn blank(v: &Option<&str>) -> bool {
    v.is_none_or(|s| s.trim().is_empty())
}

/// One column; the error is `(row, message)`.
fn text_array(values: &[Option<&str>], dt: &DataType) -> Result<ArrayRef, (usize, String)> {
    let bad = |i: usize, v: &str| (i, format!("`{v}` is not a valid {}", type_label(dt)));
    macro_rules! typed {
        ($builder:expr, $parse:expr) => {{
            let mut b = $builder;
            for (i, v) in values.iter().enumerate() {
                match v {
                    _ if blank(v) => b.append_null(),
                    Some(s) => match $parse(s.trim()) {
                        Some(x) => b.append_value(x),
                        None => return Err(bad(i, s)),
                    },
                    None => unreachable!(),
                }
            }
            Ok(Arc::new(b.finish()) as ArrayRef)
        }};
    }
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid date");
    match dt {
        DataType::Utf8 => {
            let mut b = StringBuilder::with_capacity(values.len(), values.len() * 16);
            for v in values {
                match v {
                    _ if blank(v) => b.append_null(),
                    Some(s) => b.append_value(s),
                    None => unreachable!(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int64 => typed!(Int64Builder::with_capacity(values.len()), |t: &str| t.parse::<i64>().ok()),
        DataType::Float64 => typed!(Float64Builder::with_capacity(values.len()), |t: &str| t.parse::<f64>().ok()),
        DataType::Boolean => typed!(BooleanBuilder::with_capacity(values.len()), parse_bool),
        DataType::Date32 => typed!(Date32Builder::with_capacity(values.len()), |t: &str| parse_date(t)
            .map(|d| d.signed_duration_since(epoch).num_days() as i32)),
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            typed!(TimestampMillisecondBuilder::with_capacity(values.len()), |t: &str| parse_datetime(t)
                .or_else(|| parse_date(t).and_then(|d| d.and_hms_opt(0, 0, 0)))
                .map(|dt| dt.and_utc().timestamp_millis()))
        }
        // Every other declared type: Arrow's own text parsing (overflow and
        // malformed values come back as nulls, reported below).
        other => {
            let text: StringArray =
                values.iter().map(|v| if blank(v) { None } else { v.map(str::trim) }).collect();
            let out = cast_with_options(&text, other, &CastOptions { safe: true, ..Default::default() })
                .map_err(|e| (0, format!("cannot read {}: {e}", type_label(dt))))?;
            if let Some(i) = (0..text.len()).find(|&i| text.is_valid(i) && out.is_null(i)) {
                return Err(bad(i, text.value(i)));
            }
            Ok(out)
        }
    }
}

/// A type in the protocol's vocabulary (`int64`, `timestamp(ms)`, …).
pub fn type_label(dt: &DataType) -> String {
    loadsmith_arrow::protocol_type_from_arrow(dt).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::infer_type;
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Decimal128Type, Int32Type};
    use arrow_schema::Field;
    use serde_json::json;

    #[test]
    fn declared_columns_use_the_protocol_vocabulary() {
        let s = declared_schema(&json!([
            {"name": "id", "type": "int32", "nullable": false},
            {"name": "amount", "type": "decimal128", "precision": 10, "scale": 2},
            {"name": "at", "type": "timestamp", "unit": "us", "timezone": "UTC"},
            {"name": "note", "type": "utf8"},
        ]))
        .unwrap();
        assert_eq!(s.field(0), &Field::new("id", DataType::Int32, false));
        assert_eq!(s.field(1).data_type(), &DataType::Decimal128(10, 2));
        assert!(s.field(1).is_nullable());
        assert_eq!(s.field(2).data_type(), &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())));
    }

    #[test]
    fn declared_columns_reject_bad_lists() {
        let err = |v: Value| declared_schema(&v).unwrap_err().to_string();
        assert!(err(json!({"id": "int64"})).contains("must be a list"));
        assert!(err(json!([])).contains("empty"));
        assert!(err(json!([{"name": "a", "type": "nope"}])).contains("unknown type `nope`"));
        assert!(err(json!([{"name": "a", "type": "list", "item": {"name": "x", "type": "int64"}}]))
            .contains("cannot be read from a text cell"));
        assert!(err(json!([{"name": "a", "type": "int64"}, {"name": "a", "type": "utf8"}])).contains("both named `a`"));
        assert!(err(json!([{"type": "int64"}])).contains("name"));
    }

    #[test]
    fn inference_extends_the_sharepoint_csv_rules() {
        let col = |vals: &[&str]| infer_type(vals.iter().map(|v| infer_cell(v)).collect::<Vec<_>>().iter());
        assert_eq!(col(&["1", "2", ""]), DataType::Int64);
        assert_eq!(col(&["1", "2.5"]), DataType::Float64);
        assert_eq!(col(&["true", "FALSE", " "]), DataType::Boolean);
        assert_eq!(col(&["2024-01-02", "2024-02-29"]), DataType::Date32);
        assert_eq!(col(&["2024-01-02 03:04:05", "2024-01-02T03:04:05.250"]), DataType::Timestamp(TimeUnit::Millisecond, None));
        assert_eq!(col(&["1", "x"]), DataType::Utf8);
        assert_eq!(col(&["", ""]), DataType::Utf8);
    }

    #[test]
    fn header_names_fill_blanks_and_reject_duplicates() {
        assert_eq!(header_names(["a", " ", "c"]).unwrap(), ["a", "col_1", "c"]);
        assert!(header_names(["a", "b", "a"]).unwrap_err().to_string().contains("columns 1 and 3 are both named `a`"));
    }

    #[test]
    fn text_batch_converts_and_reports_the_first_bad_cell() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("amount", DataType::Decimal128(10, 2), true),
        ]));
        let ok = text_batch(&schema, &[vec![Some(" 1"), Some("2")], vec![Some("1.50"), Some("")]]).unwrap();
        assert_eq!(ok.column(0).as_primitive::<Int32Type>().values(), &[1, 2]);
        assert_eq!(ok.column(1).as_primitive::<Decimal128Type>().value(0), 150);
        assert!(ok.column(1).is_null(1));

        let e = text_batch(&schema, &[vec![Some("1"), Some("x")], vec![None, None]]).unwrap_err();
        assert_eq!((e.row, e.column), (1, 0));
        assert!(e.message.contains("`x` is not a valid int32"), "{}", e.message);
        let e = text_batch(&schema, &[vec![Some("1"), Some("99999999999")], vec![None, None]]).unwrap_err();
        assert_eq!(e.row, 1, "overflow is reported");
        let e = text_batch(&schema, &[vec![Some("1"), Some(" ")], vec![None, None]]).unwrap_err();
        assert!(e.message.contains("non-nullable"));
    }

    #[test]
    fn inferred_types_accept_what_inference_accepted() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int64, true),
            Field::new("f", DataType::Float64, true),
            Field::new("b", DataType::Boolean, true),
            Field::new("d", DataType::Date32, true),
            Field::new("t", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("s", DataType::Utf8, true),
        ]));
        let b = text_batch(
            &schema,
            &[
                vec![Some("-3")],
                vec![Some("1e3")],
                vec![Some("TRUE")],
                vec![Some("1970-01-02")],
                vec![Some("1970-01-01T00:00:01.5")],
                vec![Some(" padded ")],
            ],
        )
        .unwrap();
        assert_eq!(b.column(3).as_primitive::<arrow_array::types::Date32Type>().value(0), 1);
        assert_eq!(b.column(4).as_primitive::<arrow_array::types::TimestampMillisecondType>().value(0), 1500);
        assert_eq!(b.column(5).as_string::<i32>().value(0), " padded ");
    }
}
