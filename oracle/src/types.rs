//! Oracle ⇄ Arrow type mapping, for the pure-Rust `oracledb` thin driver.
//!
//! `NUMBER` (any precision/scale), `INTERVAL`, `ROWID` and the character types
//! cross as exact text (`Utf8`); `RAW`/`BLOB` as `Binary`; the IEEE
//! `BINARY_FLOAT`/`BINARY_DOUBLE` as Arrow floats; `DATE`, `TIMESTAMP` and
//! `TIMESTAMP WITH [LOCAL] TIME ZONE` as Arrow timestamps in the source's
//! `timestamp_unit` (the zoned ones normalized to UTC).
//!
//! Temporal/CLOB columns are rendered server-side with `TO_CHAR` against pinned
//! formats (see [`projected_select`]) so the wire text is exactly the canonical
//! format regardless of session NLS; the destination wraps temporal binds in the
//! matching `TO_DATE`/`TO_TIMESTAMP`/`TO_TIMESTAMP_TZ` ([`typed_expr`]).

use anyhow::{bail, Context, Result};
use arrow::array::AsArray;
use arrow::datatypes::{
    Date32Type, Date64Type, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Int8Type, Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
    UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{
    builder::{BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, StringBuilder},
    Array, ArrayRef, Decimal128Array, Decimal256Array, RecordBatch, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::NaiveDateTime;
use oracledb::{DbType, Row, ToDbValue};
use std::sync::Arc;

use crate::conn::{FMT_DATE, FMT_TIMESTAMP_IN, FMT_TIMESTAMP_OUT, FMT_TIMESTAMP_TZ_IN, FMT_TIMESTAMP_TZ_OUT};
use crate::vocab::{epoch_in, from_epoch, time_of_day};

// ── Schema inference (source) ───────────────────────────────────────────────

/// Maps an Oracle column type to an Arrow `DataType`.
pub fn oracle_to_arrow(t: &DbType, unit: TimeUnit) -> DataType {
    if t == oracledb::DB_TYPE_BINARY_FLOAT {
        DataType::Float32
    } else if t == oracledb::DB_TYPE_BINARY_DOUBLE {
        DataType::Float64
    } else if is_one_of(t, &[oracledb::DB_TYPE_RAW, oracledb::DB_TYPE_LONG_RAW, oracledb::DB_TYPE_BLOB, oracledb::DB_TYPE_BFILE]) {
        DataType::Binary
    } else if t == oracledb::DB_TYPE_BOOLEAN {
        DataType::Boolean
    } else if is_one_of(t, &[oracledb::DB_TYPE_DATE, oracledb::DB_TYPE_TIMESTAMP]) {
        DataType::Timestamp(unit, None)
    } else if is_zoned(t) {
        DataType::Timestamp(unit, Some("UTC".into()))
    } else {
        // NUMBER, INTERVAL, CHAR/VARCHAR2/CLOB/LONG, ROWID, JSON, and anything
        // exotic → text-preserving Utf8.
        DataType::Utf8
    }
}

/// `DbType` only derives `PartialEq` (no structural match), so type tests are
/// done by comparison against the driver's `DB_TYPE_*` constants.
fn is_one_of(t: &DbType, set: &[&DbType]) -> bool {
    set.contains(&t)
}

fn is_zoned(t: &DbType) -> bool {
    is_one_of(t, &[oracledb::DB_TYPE_TIMESTAMP_TZ, oracledb::DB_TYPE_TIMESTAMP_LTZ])
}

/// Whether a column must be read through `TO_CHAR`. Returns the `TO_CHAR(...)`
/// expression to use, or `None` to read the column directly.
fn to_char_expr(col: &str, t: &DbType) -> Option<String> {
    let quoted = quote_ident(col);
    if t == oracledb::DB_TYPE_DATE {
        Some(format!("TO_CHAR({quoted}, '{FMT_DATE}')"))
    } else if t == oracledb::DB_TYPE_TIMESTAMP {
        Some(format!("TO_CHAR({quoted}, '{FMT_TIMESTAMP_OUT}')"))
    } else if is_zoned(t) {
        // Normalized to UTC; the offset stays in the text so the incremental
        // watermark compares as the same instant.
        Some(format!("TO_CHAR({quoted} AT TIME ZONE '+00:00', '{FMT_TIMESTAMP_TZ_OUT}')"))
    } else if is_one_of(
        t,
        &[
            oracledb::DB_TYPE_CLOB,
            oracledb::DB_TYPE_NCLOB,
            oracledb::DB_TYPE_LONG,
            oracledb::DB_TYPE_INTERVAL_YM,
            oracledb::DB_TYPE_INTERVAL_DS,
        ],
    ) {
        // CLOB/LONG/INTERVAL render to text with the default model. (CLOB via
        // TO_CHAR is limited to 4000 bytes — fine for the canonical dataset;
        // streaming larger LOBs is a future improvement.)
        Some(format!("TO_CHAR({quoted})"))
    } else {
        None
    }
}

/// Builds the projected `SELECT` list and the matching Arrow `Schema` from the
/// described columns. Temporal/CLOB columns are wrapped in `TO_CHAR(...) AS
/// "name"`; every other column is selected verbatim. All fields are nullable.
pub fn projected_select(cols: &[ColumnDesc], unit: TimeUnit) -> (String, Schema) {
    let mut select = Vec::with_capacity(cols.len());
    let mut fields = Vec::with_capacity(cols.len());
    for c in cols {
        let expr = match to_char_expr(&c.name, c.oracle_type) {
            Some(e) => format!("{e} AS {}", quote_ident(&c.name)),
            None => quote_ident(&c.name),
        };
        select.push(expr);
        fields.push(Field::new(&c.name, oracle_to_arrow(c.oracle_type, unit), true));
    }
    (select.join(", "), Schema::new(fields))
}

/// A minimal, owned description of one result column (name + Oracle type).
#[derive(Debug, Clone)]
pub struct ColumnDesc {
    pub name: String,
    pub oracle_type: &'static DbType,
}

/// Extracts column descriptions from a result set's column metadata (obtained by
/// running the query with a `WHERE 1 = 0` describe wrapper).
pub fn describe(cols: &[oracledb::Metadata]) -> Vec<ColumnDesc> {
    cols.iter().map(|c| ColumnDesc { name: c.name().to_string(), oracle_type: c.db_type() }).collect()
}

/// Wraps a SQL token (`:1`, or a quoted literal) in the explicit `TO_*`
/// conversion for a temporal Oracle type, so text→value conversion never relies
/// on the session's NLS settings. Non-temporal types pass through unchanged
/// (Oracle's implicit char→NUMBER/char→CLOB conversion handles them).
pub fn typed_expr(inner: &str, t: &DbType) -> String {
    if t == oracledb::DB_TYPE_DATE {
        format!("TO_DATE({inner}, '{FMT_DATE}')")
    } else if t == oracledb::DB_TYPE_TIMESTAMP {
        format!("TO_TIMESTAMP({inner}, '{FMT_TIMESTAMP_IN}')")
    } else if is_zoned(t) {
        format!("TO_TIMESTAMP_TZ({inner}, '{FMT_TIMESTAMP_TZ_IN}')")
    } else {
        inner.to_string()
    }
}

// ── Row cells (source) ──────────────────────────────────────────────────────

/// One fetched Oracle value, already reduced to the representation Arrow needs:
/// text for everything except binary columns, which keep their raw bytes.
#[derive(Debug, Clone)]
pub enum Cell {
    Null,
    Text(String),
    Bytes(Vec<u8>),
}

impl Cell {
    /// The text of a cell, or `None` for NULL / raw bytes (used for the
    /// incremental watermark, which only ever tracks a text/number/temporal
    /// column).
    pub fn text(&self) -> Option<&str> {
        match self {
            Cell::Text(s) => Some(s),
            _ => None,
        }
    }
}

/// Reads column `i` of a fetched row as a [`Cell`], dispatching on the column's
/// *fetched* type (the projected query's, so `TO_CHAR`-wrapped columns arrive as
/// VARCHAR). The thin driver doesn't convert NUMBER/float/bool to
/// `String` on request, so those are read natively and rendered here.
/// `OracleNumber`'s `Display` is the exact decimal text (NLS-independent).
pub fn read_cell(row: &Row, i: usize) -> Result<Cell> {
    let meta = row.columns().get(i).context("column index out of range")?;
    let t = meta.db_type();
    let err = |e: oracledb::Error| anyhow::anyhow!("col {}: {e}", meta.name());
    let text = |v: Option<String>| v.map(Cell::Text).unwrap_or(Cell::Null);
    Ok(if is_one_of(t, &[oracledb::DB_TYPE_NUMBER, oracledb::DB_TYPE_BINARY_INTEGER]) {
        text(row.get::<Option<oracledb::OracleNumber>>(i).map_err(err)?.map(|n| n.to_string()))
    } else if t == oracledb::DB_TYPE_BINARY_FLOAT {
        text(row.get::<Option<f32>>(i).map_err(err)?.map(|n| n.to_string()))
    } else if t == oracledb::DB_TYPE_BINARY_DOUBLE {
        text(row.get::<Option<f64>>(i).map_err(err)?.map(|n| n.to_string()))
    } else if t == oracledb::DB_TYPE_BOOLEAN {
        text(row.get::<Option<bool>>(i).map_err(err)?.map(|b| if b { "1" } else { "0" }.to_string()))
    } else if is_one_of(t, &[oracledb::DB_TYPE_RAW, oracledb::DB_TYPE_LONG_RAW, oracledb::DB_TYPE_BLOB]) {
        // BLOBs are fetched inline as bytes (no `fetch_lobs()` on the statement).
        match row.get::<Option<Vec<u8>>>(i).map_err(err)? {
            Some(b) => Cell::Bytes(b),
            None => Cell::Null,
        }
    } else {
        // VARCHAR/CHAR/N*, inline CLOB, ROWID, and every TO_CHAR projection.
        text(row.get::<Option<String>>(i).map_err(err)?)
    })
}

/// Converts a slice of fetched rows into an Arrow `RecordBatch`.
pub fn rows_to_batch(rows: &[Vec<Cell>], schema: &Schema) -> Result<RecordBatch> {
    let arrays: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| build_column(rows, idx, field))
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(Arc::new(schema.clone()), arrays)
        .map_err(|e| anyhow::anyhow!("RecordBatch::try_new: {e}"))
}

/// The cell at `idx` in a row, treating a short row as NULL.
fn cell_at(row: &[Cell], idx: usize) -> &Cell {
    static NULL_CELL: Cell = Cell::Null;
    row.get(idx).unwrap_or(&NULL_CELL)
}

/// Parses a `TO_CHAR` temporal rendering — `YYYY-MM-DD HH24:MI:SS[.fffffffff][
/// ±HH:MM]` — into an epoch value in `unit` (UTC), floored to the unit.
fn parse_temporal(s: &str, unit: TimeUnit) -> Result<i64> {
    let (wall, offset_secs) = match s.rsplit_once(' ') {
        Some((wall, off)) if off.starts_with(['+', '-']) => {
            let sign = if off.starts_with('-') { -1 } else { 1 };
            let (h, m) = off[1..].split_once(':').ok_or_else(|| anyhow::anyhow!("bad UTC offset '{off}'"))?;
            let secs = h.trim().parse::<i64>()? * 3600 + m.trim().parse::<i64>()? * 60;
            (wall, sign * secs)
        }
        _ => (s, 0),
    };
    let dt = NaiveDateTime::parse_from_str(wall.trim(), "%Y-%m-%d %H:%M:%S%.f").map_err(|e| anyhow::anyhow!("{e}"))?;
    epoch_in(dt - chrono::Duration::seconds(offset_secs), unit).map_err(|e| anyhow::anyhow!(e))
}

fn build_column(rows: &[Vec<Cell>], idx: usize, field: &Field) -> Result<ArrayRef> {
    let name = field.name();
    match field.data_type() {
        DataType::Float32 => {
            let mut b = Float32Builder::with_capacity(rows.len());
            for row in rows {
                match cell_at(row, idx).text() {
                    None => b.append_null(),
                    Some(v) => b.append_value(
                        v.parse::<f32>().map_err(|e| anyhow::anyhow!("col {name}: f32 '{v}': {e}"))?,
                    ),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(rows.len());
            for row in rows {
                match cell_at(row, idx).text() {
                    None => b.append_null(),
                    Some(v) => b.append_value(
                        v.parse::<f64>().map_err(|e| anyhow::anyhow!("col {name}: f64 '{v}': {e}"))?,
                    ),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                match cell_at(row, idx).text() {
                    None => b.append_null(),
                    Some("1") | Some("true") | Some("TRUE") => b.append_value(true),
                    Some("0") | Some("false") | Some("FALSE") => b.append_value(false),
                    Some(v) => bail!("col {name}: unexpected bool value '{v}'"),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Timestamp(unit, tz) => {
            let mut out: Vec<Option<i64>> = Vec::with_capacity(rows.len());
            for row in rows {
                out.push(match cell_at(row, idx).text() {
                    None => None,
                    Some(v) => Some(
                        parse_temporal(v, *unit).map_err(|e| anyhow::anyhow!("col {name}: timestamp '{v}': {e}"))?,
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
            let mut b = BinaryBuilder::with_capacity(rows.len(), rows.len() * 16);
            for row in rows {
                match cell_at(row, idx) {
                    Cell::Null => b.append_null(),
                    Cell::Bytes(bytes) => b.append_value(bytes),
                    Cell::Text(s) => b.append_value(s.as_bytes()),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Utf8 => {
            let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
            for row in rows {
                match cell_at(row, idx).text() {
                    None => b.append_null(),
                    Some(v) => b.append_value(v),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        other => bail!("col {name}: unsupported arrow type {other:?}"),
    }
}

// ── Arrow → Oracle bind (destination) ────────────────────────────────────────

/// One bind value: text (the default — Oracle converts server-side under the
/// `TO_*` wrappers / implicit char→number) or raw bytes for binary columns.
/// `None` carries a typed NULL.
pub enum Bind {
    Text(Option<String>),
    Bytes(Option<Vec<u8>>),
}

impl Bind {
    /// A `&dyn ToDbValue` pointing at the inner typed `Option`, for `execute_batch`.
    pub fn as_sql(&self) -> &dyn ToDbValue {
        match self {
            Bind::Text(v) => v,
            Bind::Bytes(v) => v,
        }
    }
}

fn is_binary(dt: &DataType) -> bool {
    matches!(dt, DataType::Binary | DataType::LargeBinary | DataType::BinaryView | DataType::FixedSizeBinary(_))
}

/// How a temporal value is spelled for the target column's `TO_*` wrapper.
fn temporal_text(dt: NaiveDateTime, zoned_value: bool, target: Option<&DbType>) -> String {
    match target {
        // DATE has whole seconds; FMT_DATE has no fraction.
        Some(t) if t == oracledb::DB_TYPE_DATE => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
        Some(t) if t == oracledb::DB_TYPE_TIMESTAMP => dt.format("%Y-%m-%d %H:%M:%S%.9f").to_string(),
        // An Arrow timestamp is a UTC instant (or, without a zone, a wall
        // clock taken as UTC).
        Some(t) if is_zoned(t) => format!("{} +00:00", dt.format("%Y-%m-%d %H:%M:%S%.9f")),
        _ => {
            let s = dt.format("%Y-%m-%d %H:%M:%S%.f").to_string();
            if zoned_value { format!("{s} +00:00") } else { s }
        }
    }
}

/// Converts one Arrow cell to a [`Bind`] for a column whose target type is
/// `target` (when the target has the column). Everything but raw bytes is
/// bound as text and converted server-side under the pinned formats.
pub fn cell_to_bind(array: &dyn Array, row: usize, target: Option<&DbType>) -> Result<Bind> {
    if array.is_null(row) {
        return Ok(if is_binary(array.data_type()) { Bind::Bytes(None) } else { Bind::Text(None) });
    }
    macro_rules! prim {
        ($t:ty) => {
            array.as_primitive_opt::<$t>().context("array type mismatch")?.value(row)
        };
    }
    let text = |s: String| Bind::Text(Some(s));
    let b = match array.data_type() {
        DataType::Int8 => text(prim!(Int8Type).to_string()),
        DataType::Int16 => text(prim!(Int16Type).to_string()),
        DataType::Int32 => text(prim!(Int32Type).to_string()),
        DataType::Int64 => text(prim!(Int64Type).to_string()),
        DataType::UInt8 => text(prim!(UInt8Type).to_string()),
        DataType::UInt16 => text(prim!(UInt16Type).to_string()),
        DataType::UInt32 => text(prim!(UInt32Type).to_string()),
        DataType::UInt64 => text(prim!(UInt64Type).to_string()),
        DataType::Float16 => text(prim!(Float16Type).to_string()),
        DataType::Float32 => text(prim!(Float32Type).to_string()),
        DataType::Float64 => text(prim!(Float64Type).to_string()),
        DataType::Decimal128(..) => text(downcast::<Decimal128Array>(array)?.value_as_string(row)),
        DataType::Decimal256(..) => text(downcast::<Decimal256Array>(array)?.value_as_string(row)),
        DataType::Boolean => {
            text(if array.as_boolean_opt().context("array type mismatch")?.value(row) { "1" } else { "0" }.into())
        }
        DataType::Utf8 => text(array.as_string_opt::<i32>().context("array type mismatch")?.value(row).to_string()),
        DataType::LargeUtf8 => text(array.as_string_opt::<i64>().context("array type mismatch")?.value(row).to_string()),
        DataType::Utf8View => text(array.as_string_view_opt().context("array type mismatch")?.value(row).to_string()),
        DataType::Binary => Bind::Bytes(Some(array.as_binary_opt::<i32>().context("array type mismatch")?.value(row).to_vec())),
        DataType::LargeBinary => {
            Bind::Bytes(Some(array.as_binary_opt::<i64>().context("array type mismatch")?.value(row).to_vec()))
        }
        DataType::BinaryView => {
            Bind::Bytes(Some(array.as_binary_view_opt().context("array type mismatch")?.value(row).to_vec()))
        }
        DataType::FixedSizeBinary(_) => {
            Bind::Bytes(Some(array.as_fixed_size_binary_opt().context("array type mismatch")?.value(row).to_vec()))
        }
        DataType::Date32 => {
            let days = prim!(Date32Type);
            let dt = chrono::NaiveDate::default()
                .checked_add_signed(chrono::Duration::days(days as i64))
                .ok_or_else(|| anyhow::anyhow!("date32 out of range: {days}"))?
                .and_hms_opt(0, 0, 0)
                .context("midnight")?;
            text(match target {
                Some(t) if is_one_of(t, &[oracledb::DB_TYPE_DATE, oracledb::DB_TYPE_TIMESTAMP]) || is_zoned(t) => {
                    temporal_text(dt, false, target)
                }
                _ => dt.format("%Y-%m-%d").to_string(),
            })
        }
        DataType::Date64 => {
            let ms = prim!(Date64Type);
            let dt = from_epoch(ms, TimeUnit::Millisecond).ok_or_else(|| anyhow::anyhow!("date64 out of range: {ms}"))?;
            text(temporal_text(dt, false, target))
        }
        DataType::Timestamp(unit, tz) => {
            let v = match unit {
                TimeUnit::Second => prim!(TimestampSecondType),
                TimeUnit::Millisecond => prim!(TimestampMillisecondType),
                TimeUnit::Microsecond => prim!(TimestampMicrosecondType),
                TimeUnit::Nanosecond => prim!(TimestampNanosecondType),
            };
            let dt = from_epoch(v, *unit).ok_or_else(|| anyhow::anyhow!("timestamp out of range: {v}"))?;
            text(temporal_text(dt, tz.is_some(), target))
        }
        // Oracle has no time-of-day type: the value is bound as text
        // (`HH:MM:SS[.f]`), e.g. for a VARCHAR2 column.
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
        other => bail!("the oracle destination does not support Arrow type {other:?}"),
    };
    Ok(b)
}

fn downcast<T: 'static>(array: &dyn Array) -> Result<&T> {
    array.as_any().downcast_ref::<T>().context("array type mismatch")
}

// ── Identifier quoting ────────────────────────────────────────────────────────

/// Quotes an Oracle identifier the way Oracle's own parser resolves a name.
///
/// Oracle folds an *unquoted* identifier to upper-case, so a table created as
/// `events_sink` is stored — and must be referenced — as `EVENTS_SINK`. Config
/// values (`target_table`, `merge_key`) are natural names the user typically
/// writes in lower/mixed case, so a *simple* identifier is folded to upper-case
/// before quoting; that makes `events_sink` reach `EVENTS_SINK` while still
/// quoting the name (safe against reserved words). An identifier that isn't
/// simple (spaces, dots, embedded quotes, leading digit, …) is quoted verbatim,
/// case-preserved — the escape hatch for names that genuinely need it. Names
/// sourced from Oracle's catalog (column descriptions) already arrive
/// upper-cased, so folding is a no-op for them.
pub fn quote_ident(name: &str) -> String {
    if is_simple_ident(name) {
        format!("\"{}\"", name.to_uppercase())
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

/// The form a config identifier resolves to for *comparison* against Oracle's
/// catalog names (which arrive upper-cased): a simple identifier folded to
/// upper-case, anything else verbatim — i.e. [`quote_ident`] without the quotes.
/// Use this to match a config-supplied name (e.g. a `merge_key`) against the
/// incoming column names so `id` matches the catalog's `ID`.
pub fn fold_ident(name: &str) -> String {
    if is_simple_ident(name) {
        name.to_uppercase()
    } else {
        name.to_string()
    }
}

/// A "simple" Oracle identifier: a leading ASCII letter followed by ASCII
/// letters, digits, or `_ $ #` — the form Oracle accepts unquoted (and folds to
/// upper-case). Anything else must be quoted verbatim to be valid.
fn is_simple_ident(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '$' | '#'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray, UInt64Array};

    #[test]
    fn type_mapping() {
        let ms = TimeUnit::Millisecond;
        assert_eq!(oracle_to_arrow(oracledb::DB_TYPE_NUMBER, ms), DataType::Utf8);
        assert_eq!(oracle_to_arrow(oracledb::DB_TYPE_TIMESTAMP_TZ, ms), DataType::Timestamp(ms, Some("UTC".into())));
        assert_eq!(oracle_to_arrow(oracledb::DB_TYPE_DATE, TimeUnit::Second), DataType::Timestamp(TimeUnit::Second, None));
        assert_eq!(oracle_to_arrow(oracledb::DB_TYPE_BLOB, ms), DataType::Binary);
        assert_eq!(oracle_to_arrow(oracledb::DB_TYPE_BINARY_DOUBLE, ms), DataType::Float64);
    }

    #[test]
    fn projection_wraps_temporal_only() {
        let cols = vec![
            ColumnDesc { name: "EVENT_ID".into(), oracle_type: oracledb::DB_TYPE_NUMBER },
            ColumnDesc { name: "EVENT_TSTZ".into(), oracle_type: oracledb::DB_TYPE_TIMESTAMP_TZ },
            ColumnDesc { name: "TS".into(), oracle_type: oracledb::DB_TYPE_TIMESTAMP },
        ];
        let (select, schema) = projected_select(&cols, TimeUnit::Nanosecond);
        assert_eq!(
            select,
            "\"EVENT_ID\", TO_CHAR(\"EVENT_TSTZ\" AT TIME ZONE '+00:00', 'YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM') \
             AS \"EVENT_TSTZ\", TO_CHAR(\"TS\", 'YYYY-MM-DD HH24:MI:SS.FF9') AS \"TS\""
        );
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(2).data_type(), &DataType::Timestamp(TimeUnit::Nanosecond, None));
    }

    #[test]
    fn temporal_text_parses_in_every_unit() {
        assert_eq!(parse_temporal("2024-01-15 12:30:45", TimeUnit::Millisecond).unwrap(), 1_705_321_845_000);
        let s = "2024-01-15 12:30:45.123456789";
        assert_eq!(parse_temporal(s, TimeUnit::Nanosecond).unwrap(), 1_705_321_845_123_456_789);
        assert_eq!(parse_temporal(s, TimeUnit::Millisecond).unwrap(), 1_705_321_845_123);
        assert_eq!(parse_temporal("2024-01-15 18:00:45.000000000 +05:30", TimeUnit::Second).unwrap(), 1_705_321_845);
        assert_eq!(parse_temporal("2024-01-15 12:30:45.000000000 +00:00", TimeUnit::Second).unwrap(), 1_705_321_845);
    }

    #[test]
    fn typed_expr_wraps_temporal() {
        assert_eq!(typed_expr(":1", oracledb::DB_TYPE_DATE), "TO_DATE(:1, 'YYYY-MM-DD HH24:MI:SS')");
        assert_eq!(typed_expr(":1", oracledb::DB_TYPE_TIMESTAMP), "TO_TIMESTAMP(:1, 'YYYY-MM-DD HH24:MI:SS.FF')");
        assert_eq!(typed_expr(":1", oracledb::DB_TYPE_NUMBER), ":1");
    }

    #[test]
    fn cell_to_bind_binds_as_text() {
        let ids = Int64Array::from(vec![Some(7), None]);
        assert!(matches!(cell_to_bind(&ids, 0, None).unwrap(), Bind::Text(Some(s)) if s == "7"));
        assert!(matches!(cell_to_bind(&ids, 1, None).unwrap(), Bind::Text(None)));
        let names = StringArray::from(vec![Some("ab")]);
        assert!(matches!(cell_to_bind(&names, 0, None).unwrap(), Bind::Text(Some(s)) if s == "ab"));
        let big = UInt64Array::from(vec![u64::MAX]);
        assert!(matches!(cell_to_bind(&big, 0, None).unwrap(), Bind::Text(Some(s)) if s == "18446744073709551615"));
        let d = Decimal128Array::from(vec![-12345]).with_precision_and_scale(9, 2).unwrap();
        assert!(matches!(cell_to_bind(&d, 0, None).unwrap(), Bind::Text(Some(s)) if s == "-123.45"));
    }

    #[test]
    fn timestamps_bind_in_the_target_column_format() {
        let ts = TimestampNanosecondArray::from(vec![1_705_321_845_123_456_789]).with_timezone("UTC");
        let bind = |t| match cell_to_bind(&ts, 0, t).unwrap() {
            Bind::Text(Some(s)) => s,
            _ => unreachable!(),
        };
        assert_eq!(bind(Some(oracledb::DB_TYPE_DATE)), "2024-01-15 12:30:45");
        assert_eq!(bind(Some(oracledb::DB_TYPE_TIMESTAMP)), "2024-01-15 12:30:45.123456789");
        assert_eq!(bind(Some(oracledb::DB_TYPE_TIMESTAMP_TZ)), "2024-01-15 12:30:45.123456789 +00:00");
        assert_eq!(bind(None), "2024-01-15 12:30:45.123456789 +00:00");
        let secs = TimestampSecondArray::from(vec![1_705_321_845]);
        assert!(matches!(cell_to_bind(&secs, 0, Some(oracledb::DB_TYPE_TIMESTAMP)).unwrap(),
            Bind::Text(Some(s)) if s == "2024-01-15 12:30:45.000000000"));
    }

    #[test]
    fn quote_ident_escapes() {
        // Not a simple identifier (embedded quote) → quoted verbatim, escaped.
        assert_eq!(quote_ident("A\"B"), "\"A\"\"B\"");
    }

    #[test]
    fn quote_ident_folds_simple_names_to_upper() {
        // Config-supplied natural names reach Oracle's upper-cased catalog names.
        assert_eq!(quote_ident("events_sink"), "\"EVENTS_SINK\"");
        assert_eq!(quote_ident("id"), "\"ID\"");
        assert_eq!(quote_ident("MixedCase"), "\"MIXEDCASE\"");
        // Already upper-cased catalog names are unchanged.
        assert_eq!(quote_ident("EVENT_ID"), "\"EVENT_ID\"");
    }

    #[test]
    fn quote_ident_preserves_non_simple_names() {
        // A leading digit or a space is not a simple identifier → verbatim case.
        assert_eq!(quote_ident("1table"), "\"1table\"");
        assert_eq!(quote_ident("weird name"), "\"weird name\"");
    }
}
