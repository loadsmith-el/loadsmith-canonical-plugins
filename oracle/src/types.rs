//! Oracle ⇄ Arrow type mapping (text-preserving), for the pure-Rust `oracledb`
//! thin driver.
//!
//! Like postgres/mysql, values cross as their canonical *text* so precision is
//! never lost: NUMBER (any precision/scale), temporal types, INTERVAL, ROWID and
//! the character types all map to `Utf8`; RAW/BLOB map to `Binary`; the IEEE
//! `BINARY_FLOAT`/`BINARY_DOUBLE` map to Arrow floats.
//!
//! Temporal/CLOB columns are rendered server-side with `TO_CHAR` against pinned
//! formats (see [`projected_select`]) so the wire text is exactly the canonical
//! format regardless of session NLS; the destination wraps temporal binds in the
//! matching `TO_DATE`/`TO_TIMESTAMP`/`TO_TIMESTAMP_TZ` ([`typed_expr`]). Doing the
//! conversion in SQL keeps the exact format and matches the connector's
//! text-preserving contract.

use anyhow::{bail, Result};
use arrow_array::{
    builder::{BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, StringBuilder},
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array,
    Int32Array, Int64Array, LargeStringArray, RecordBatch, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use oracledb::{DbType, Row, ToDbValue};
use std::sync::Arc;

use crate::conn::{FMT_DATE, FMT_TIMESTAMP, FMT_TIMESTAMP_TZ};

// ── Schema inference (source) ───────────────────────────────────────────────

/// Maps an Oracle column type to an Arrow `DataType`.
pub fn oracle_to_arrow(t: &DbType) -> DataType {
    if t == oracledb::DB_TYPE_BINARY_FLOAT {
        DataType::Float32
    } else if t == oracledb::DB_TYPE_BINARY_DOUBLE {
        DataType::Float64
    } else if is_one_of(t, &[oracledb::DB_TYPE_RAW, oracledb::DB_TYPE_LONG_RAW, oracledb::DB_TYPE_BLOB, oracledb::DB_TYPE_BFILE]) {
        DataType::Binary
    } else if t == oracledb::DB_TYPE_BOOLEAN {
        DataType::Boolean
    } else {
        // NUMBER, all temporal/interval, CHAR/VARCHAR2/CLOB/LONG, ROWID, JSON,
        // and anything exotic → text-preserving Utf8.
        DataType::Utf8
    }
}

/// `DbType` only derives `PartialEq` (no structural match), so type tests are
/// done by comparison against the driver's `DB_TYPE_*` constants.
fn is_one_of(t: &DbType, set: &[&DbType]) -> bool {
    set.iter().any(|s| t == *s)
}

/// Whether a column must be read through `TO_CHAR`. Returns the `TO_CHAR(...)`
/// expression to use, or `None` to read the column directly.
fn to_char_expr(col: &str, t: &DbType) -> Option<String> {
    let quoted = quote_ident(col);
    if t == oracledb::DB_TYPE_DATE {
        Some(format!("TO_CHAR({quoted}, '{FMT_DATE}')"))
    } else if t == oracledb::DB_TYPE_TIMESTAMP {
        Some(format!("TO_CHAR({quoted}, '{FMT_TIMESTAMP}')"))
    } else if is_one_of(t, &[oracledb::DB_TYPE_TIMESTAMP_TZ, oracledb::DB_TYPE_TIMESTAMP_LTZ]) {
        Some(format!("TO_CHAR({quoted}, '{FMT_TIMESTAMP_TZ}')"))
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
pub fn projected_select(cols: &[ColumnDesc]) -> (String, Schema) {
    let mut select = Vec::with_capacity(cols.len());
    let mut fields = Vec::with_capacity(cols.len());
    for c in cols {
        let expr = match to_char_expr(&c.name, c.oracle_type) {
            Some(e) => format!("{e} AS {}", quote_ident(&c.name)),
            None => quote_ident(&c.name),
        };
        select.push(expr);
        fields.push(Field::new(&c.name, oracle_to_arrow(c.oracle_type), true));
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
        format!("TO_TIMESTAMP({inner}, '{FMT_TIMESTAMP}')")
    } else if is_one_of(t, &[oracledb::DB_TYPE_TIMESTAMP_TZ, oracledb::DB_TYPE_TIMESTAMP_LTZ]) {
        format!("TO_TIMESTAMP_TZ({inner}, '{FMT_TIMESTAMP_TZ}')")
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
    let t = row.columns()[i].db_type();
    let err = |e: oracledb::Error| anyhow::anyhow!("col {}: {e}", row.columns()[i].name());
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

/// Converts one Arrow cell to a [`Bind`]. Everything but raw bytes is bound as
/// text and converted server-side under the pinned formats.
pub fn cell_to_bind(array: &dyn Array, row: usize) -> Result<Bind> {
    if array.is_null(row) {
        return Ok(match array.data_type() {
            DataType::Binary => Bind::Bytes(None),
            _ => Bind::Text(None),
        });
    }
    let b = match array.data_type() {
        DataType::Int32 => Bind::Text(Some(downcast::<Int32Array>(array).value(row).to_string())),
        DataType::Int64 => Bind::Text(Some(downcast::<Int64Array>(array).value(row).to_string())),
        DataType::Float32 => {
            Bind::Text(Some(downcast::<Float32Array>(array).value(row).to_string()))
        }
        DataType::Float64 => {
            Bind::Text(Some(downcast::<Float64Array>(array).value(row).to_string()))
        }
        DataType::Boolean => Bind::Text(Some(
            if downcast::<BooleanArray>(array).value(row) { "1" } else { "0" }.into(),
        )),
        DataType::Utf8 => Bind::Text(Some(downcast::<StringArray>(array).value(row).to_string())),
        DataType::LargeUtf8 => {
            Bind::Text(Some(downcast::<LargeStringArray>(array).value(row).to_string()))
        }
        DataType::Binary => Bind::Bytes(Some(downcast::<BinaryArray>(array).value(row).to_vec())),
        DataType::Date32 => {
            let days = downcast::<Date32Array>(array).value(row);
            let date = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap()
                .checked_add_signed(chrono::Duration::days(days as i64))
                .ok_or_else(|| anyhow::anyhow!("date32 out of range: {days}"))?;
            // Match FMT_DATE ('YYYY-MM-DD HH24:MI:SS').
            Bind::Text(Some(date.format("%Y-%m-%d 00:00:00").to_string()))
        }
        DataType::Timestamp(unit, _) => {
            let dt = match unit {
                TimeUnit::Millisecond => chrono::DateTime::from_timestamp_millis(
                    downcast::<TimestampMillisecondArray>(array).value(row),
                ),
                TimeUnit::Microsecond => chrono::DateTime::from_timestamp_micros(
                    downcast::<TimestampMicrosecondArray>(array).value(row),
                ),
                other => bail!("oracle destination: unsupported timestamp unit {other:?}"),
            }
            .ok_or_else(|| anyhow::anyhow!("timestamp out of range"))?;
            // Match FMT_TIMESTAMP ('YYYY-MM-DD HH24:MI:SS.FF6').
            Bind::Text(Some(dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.6f").to_string()))
        }
        other => bail!("oracle destination does not support Arrow type {other:?} yet"),
    };
    Ok(b)
}

fn downcast<T: 'static>(array: &dyn Array) -> &T {
    array.as_any().downcast_ref::<T>().expect("arrow array downcast")
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
    use arrow_array::{Int64Array, StringArray};

    #[test]
    fn number_and_temporal_map_to_utf8() {
        assert_eq!(oracle_to_arrow(oracledb::DB_TYPE_NUMBER), DataType::Utf8);
        assert_eq!(oracle_to_arrow(oracledb::DB_TYPE_TIMESTAMP_TZ), DataType::Utf8);
        assert_eq!(oracle_to_arrow(oracledb::DB_TYPE_BLOB), DataType::Binary);
        assert_eq!(oracle_to_arrow(oracledb::DB_TYPE_BINARY_DOUBLE), DataType::Float64);
    }

    #[test]
    fn projection_wraps_temporal_only() {
        let cols = vec![
            ColumnDesc { name: "EVENT_ID".into(), oracle_type: oracledb::DB_TYPE_NUMBER },
            ColumnDesc { name: "EVENT_TSTZ".into(), oracle_type: oracledb::DB_TYPE_TIMESTAMP_TZ },
        ];
        let (select, schema) = projected_select(&cols);
        assert_eq!(
            select,
            "\"EVENT_ID\", TO_CHAR(\"EVENT_TSTZ\", 'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM') AS \"EVENT_TSTZ\""
        );
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    }

    #[test]
    fn typed_expr_wraps_temporal() {
        assert_eq!(typed_expr(":1", oracledb::DB_TYPE_DATE), "TO_DATE(:1, 'YYYY-MM-DD HH24:MI:SS')");
        assert_eq!(typed_expr(":1", oracledb::DB_TYPE_NUMBER), ":1");
    }

    #[test]
    fn cell_to_bind_binds_as_text() {
        let ids = Int64Array::from(vec![Some(7), None]);
        assert!(matches!(cell_to_bind(&ids, 0).unwrap(), Bind::Text(Some(s)) if s == "7"));
        assert!(matches!(cell_to_bind(&ids, 1).unwrap(), Bind::Text(None)));
        let names = StringArray::from(vec![Some("ab")]);
        assert!(matches!(cell_to_bind(&names, 0).unwrap(), Bind::Text(Some(s)) if s == "ab"));
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
