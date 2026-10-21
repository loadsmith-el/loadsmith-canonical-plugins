//! A small, fully-materialized cell table — the intermediate between
//! "bytes/list items in" and "Arrow batches out" of the `sharepoint` source
//! and the `excel` parser. Its type-inference rules ([`infer_type`]) are shared
//! by the streaming `csv` parser.
//!
//! Why not operate on Arrow directly? The transforms ported from the Python
//! `sp-bulk` are whole-table, order-dependent operations (forward-fill of merged
//! cells, row filtering, column renaming) that are awkward on immutable, typed
//! Arrow arrays. SharePoint files and lists are modest, operational-sized data
//! that we already fully materialize (calamine loads the whole workbook; Graph
//! list pages are collected), so a row-major cell table keeps the transforms
//! simple and pandas-like. We convert to Arrow exactly once, after the transform
//! chain, inferring each column's Arrow type from its cells.

use std::sync::Arc;

use anyhow::Result;
use arrow_array::{
    builder::{
        BooleanBuilder, Date32Builder, Float64Builder, Int64Builder, StringBuilder,
        TimestampMillisecondBuilder,
    },
    ArrayRef, RecordBatch,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use chrono::{NaiveDate, NaiveDateTime};

/// A single cell value. Mirrors the value space that calamine and Graph list
/// fields produce, plus an explicit null.
#[derive(Debug, Clone, PartialEq)]
pub enum Cell {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Date(NaiveDate),
    DateTime(NaiveDateTime),
}

impl Cell {
    /// True for null and whitespace-only strings — the "empty cell" notion shared
    /// by `fix_merged_columns` (merged-cell blanks) and `drop_empty_rows`.
    pub fn is_blank(&self) -> bool {
        match self {
            Cell::Null => true,
            Cell::Str(s) => s.trim().is_empty(),
            _ => false,
        }
    }

    /// Renders the cell to text (used for string columns and comparisons).
    pub fn as_text(&self) -> Option<String> {
        match self {
            Cell::Null => None,
            Cell::Bool(b) => Some(b.to_string()),
            Cell::Int(i) => Some(i.to_string()),
            Cell::Float(f) => Some(f.to_string()),
            Cell::Str(s) => Some(s.clone()),
            Cell::Date(d) => Some(d.format("%Y-%m-%d").to_string()),
            Cell::DateTime(dt) => Some(dt.format("%Y-%m-%d %H:%M:%S").to_string()),
        }
    }

    /// Numeric view for comparisons, if the cell is (or parses as) a number.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Cell::Int(i) => Some(*i as f64),
            Cell::Float(f) => Some(*f),
            Cell::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
            Cell::Str(s) => s.trim().parse::<f64>().ok(),
            _ => None,
        }
    }
}

/// Row-major cell table. Every row has `columns.len()` cells.
#[derive(Debug, Clone, Default)]
pub struct Table {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Cell>>,
}

impl Table {
    pub fn new(columns: Vec<String>) -> Self {
        Table { columns, rows: Vec::new() }
    }

    pub fn col_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c == name)
    }

    pub fn num_rows(&self) -> usize {
        self.rows.len()
    }

    /// Infers the Arrow `DataType` of a column from its cells (see
    /// [`infer_type`]).
    fn infer_type(&self, col: usize) -> DataType {
        infer_type(self.rows.iter().filter_map(|row| row.get(col)))
    }

    /// Builds the Arrow schema (every field nullable) from the inferred per-column
    /// types.
    pub fn arrow_schema(&self) -> SchemaRef {
        let fields: Vec<Field> = self
            .columns
            .iter()
            .enumerate()
            .map(|(i, name)| Field::new(name, self.infer_type(i), true))
            .collect();
        Arc::new(Schema::new(fields))
    }

    /// Converts the table to Arrow record batches of at most `batch_size` rows.
    pub fn to_record_batches(&self, batch_size: usize) -> Result<(SchemaRef, Vec<RecordBatch>)> {
        let schema = self.arrow_schema();
        let batch_size = batch_size.max(1);
        if self.rows.is_empty() {
            return Ok((schema, Vec::new()));
        }
        let mut batches = Vec::new();
        let mut start = 0;
        while start < self.rows.len() {
            let end = (start + batch_size).min(self.rows.len());
            let slice = &self.rows[start..end];
            let arrays: Vec<ArrayRef> = schema
                .fields()
                .iter()
                .enumerate()
                .map(|(idx, field)| build_array(slice, idx, field.data_type()))
                .collect::<Result<Vec<_>>>()?;
            batches.push(
                RecordBatch::try_new(schema.clone(), arrays)
                    .map_err(|e| anyhow::anyhow!("RecordBatch::try_new: {e}"))?,
            );
            start = end;
        }
        Ok((schema, batches))
    }
}

/// Infers the Arrow `DataType` of a column from its cells:
/// all-int → Int64; numeric mix → Float64; all-bool → Boolean; all-date →
/// Date32; all-datetime → Timestamp(ms); empty/all-null or anything mixed with
/// strings → Utf8. Blanks/nulls don't constrain the type.
pub fn infer_type<'a>(cells: impl IntoIterator<Item = &'a Cell>) -> DataType {
    let mut seen = false;
    let mut all_int = true;
    let mut all_num = true; // int or float
    let mut all_bool = true;
    let mut all_date = true;
    let mut all_dt = true;
    for cell in cells {
        match cell {
            Cell::Null => {}
            Cell::Str(s) if s.trim().is_empty() => {}
            cell => {
                seen = true;
                match cell {
                    Cell::Int(_) => {
                        all_bool = false;
                        all_date = false;
                        all_dt = false;
                    }
                    Cell::Float(_) => {
                        all_int = false;
                        all_bool = false;
                        all_date = false;
                        all_dt = false;
                    }
                    Cell::Bool(_) => {
                        all_int = false;
                        all_num = false;
                        all_date = false;
                        all_dt = false;
                    }
                    Cell::Date(_) => {
                        all_int = false;
                        all_num = false;
                        all_bool = false;
                        all_dt = false;
                    }
                    Cell::DateTime(_) => {
                        all_int = false;
                        all_num = false;
                        all_bool = false;
                        all_date = false;
                    }
                    Cell::Str(_) | Cell::Null => {
                        all_int = false;
                        all_num = false;
                        all_bool = false;
                        all_date = false;
                        all_dt = false;
                    }
                }
            }
        }
    }
    if !seen {
        return DataType::Utf8;
    }
    if all_int {
        DataType::Int64
    } else if all_num {
        DataType::Float64
    } else if all_bool {
        DataType::Boolean
    } else if all_date {
        DataType::Date32
    } else if all_dt {
        DataType::Timestamp(TimeUnit::Millisecond, None)
    } else {
        DataType::Utf8
    }
}

fn epoch() -> NaiveDate {
    chrono::DateTime::UNIX_EPOCH.date_naive()
}

/// Builds one column of `rows` as an Arrow array of type `dt` (the type
/// [`Table::arrow_schema`] inferred; a cell that does not fit becomes null).
pub fn build_array(rows: &[Vec<Cell>], col: usize, dt: &DataType) -> Result<ArrayRef> {
    let get = |row: &Vec<Cell>| -> Cell { row.get(col).cloned().unwrap_or(Cell::Null) };
    match dt {
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(rows.len());
            for row in rows {
                match get(row) {
                    Cell::Int(i) => b.append_value(i),
                    c if c.is_blank() => b.append_null(),
                    c => match c.as_f64() {
                        Some(f) => b.append_value(f as i64),
                        None => b.append_null(),
                    },
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(rows.len());
            for row in rows {
                let c = get(row);
                if c.is_blank() {
                    b.append_null();
                } else {
                    match c.as_f64() {
                        Some(f) => b.append_value(f),
                        None => b.append_null(),
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                match get(row) {
                    Cell::Bool(v) => b.append_value(v),
                    c if c.is_blank() => b.append_null(),
                    Cell::Str(s) => match s.trim().to_ascii_lowercase().as_str() {
                        "true" | "t" | "1" => b.append_value(true),
                        "false" | "f" | "0" => b.append_value(false),
                        _ => b.append_null(),
                    },
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Date32 => {
            let mut b = Date32Builder::with_capacity(rows.len());
            let e = epoch();
            for row in rows {
                match get(row) {
                    Cell::Date(d) => b.append_value(d.signed_duration_since(e).num_days() as i32),
                    Cell::DateTime(dt) => {
                        b.append_value(dt.date().signed_duration_since(e).num_days() as i32)
                    }
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let mut b = TimestampMillisecondBuilder::with_capacity(rows.len());
            for row in rows {
                match get(row) {
                    Cell::DateTime(dt) => b.append_value(dt.and_utc().timestamp_millis()),
                    Cell::Date(d) => b.append_value(
                        d.and_time(chrono::NaiveTime::MIN).and_utc().timestamp_millis(),
                    ),
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        // Utf8 and anything else → text rendering.
        _ => {
            let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
            for row in rows {
                match get(row).as_text() {
                    Some(s) => b.append_value(s),
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table() -> Table {
        let mut t = Table::new(vec!["id".into(), "name".into(), "score".into()]);
        t.rows.push(vec![Cell::Int(1), Cell::Str("a".into()), Cell::Float(1.5)]);
        t.rows.push(vec![Cell::Int(2), Cell::Str("b".into()), Cell::Null]);
        t
    }

    #[test]
    fn infers_int_float_utf8() {
        let t = table();
        let schema = t.arrow_schema();
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(2).data_type(), &DataType::Float64);
    }

    #[test]
    fn int_mixed_with_float_becomes_float() {
        let mut t = Table::new(vec!["x".into()]);
        t.rows.push(vec![Cell::Int(1)]);
        t.rows.push(vec![Cell::Float(2.5)]);
        assert_eq!(t.arrow_schema().field(0).data_type(), &DataType::Float64);
    }

    #[test]
    fn all_null_column_is_utf8() {
        let mut t = Table::new(vec!["x".into()]);
        t.rows.push(vec![Cell::Null]);
        assert_eq!(t.arrow_schema().field(0).data_type(), &DataType::Utf8);
    }

    #[test]
    fn batches_respect_size_and_rowcount() {
        let mut t = Table::new(vec!["x".into()]);
        for i in 0..2500 {
            t.rows.push(vec![Cell::Int(i)]);
        }
        let (_schema, batches) = t.to_record_batches(1000).unwrap();
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].num_rows(), 1000);
        assert_eq!(batches[2].num_rows(), 500);
    }

    #[test]
    fn blank_is_null_aware() {
        assert!(Cell::Null.is_blank());
        assert!(Cell::Str("   ".into()).is_blank());
        assert!(!Cell::Str("x".into()).is_blank());
        assert!(!Cell::Int(0).is_blank());
    }
}
