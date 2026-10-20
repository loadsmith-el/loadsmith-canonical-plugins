//! Spreadsheets → [`Table`], via `calamine` (pure Rust: no native deps).
//!
//! A workbook is read whole (the xlsx/ods formats are zip archives); the
//! selected sheet becomes a fully-materialized [`Table`].

use std::io::Cursor;
use std::path::Path;

use anyhow::{Context, Result};
use calamine::{Data, Range, Reader, Xlsx};

use crate::table::{Cell, Table};

/// Which sheet of a workbook to read.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Sheet {
    /// The first sheet.
    #[default]
    First,
    /// A sheet by name.
    Name(String),
    /// A sheet by 0-based position.
    Index(usize),
}

/// Maps one calamine cell to a [`Cell`].
pub fn excel_cell(data: &Data) -> Cell {
    match data {
        Data::Empty => Cell::Null,
        Data::String(s) => Cell::Str(s.clone()),
        Data::Float(f) => Cell::Float(*f),
        Data::Int(i) => Cell::Int(*i),
        Data::Bool(b) => Cell::Bool(*b),
        Data::DateTime(dt) => match dt.as_datetime() {
            Some(ndt) => Cell::DateTime(ndt),
            None => Cell::Str(data.to_string()),
        },
        Data::DateTimeIso(s) => Cell::Str(s.clone()),
        Data::DurationIso(s) => Cell::Str(s.clone()),
        Data::Error(_) => Cell::Null,
    }
}

fn header_name(data: &Data) -> String {
    match data {
        Data::Empty => String::new(),
        Data::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Parses xlsx bytes: the named sheet (or the first one), `skip_rows` rows
/// dropped, then an optional header row. Without a header, columns are named
/// `col_0`, `col_1`, …
pub fn parse_excel(bytes: &[u8], sheet: Option<&str>, header: bool, skip_rows: usize) -> Result<Table> {
    let mut workbook: Xlsx<_> =
        calamine::open_workbook_from_rs(Cursor::new(bytes.to_vec())).context("not a valid xlsx")?;
    let sheet = match sheet {
        Some(name) => Sheet::Name(name.to_string()),
        None => Sheet::First,
    };
    let range = select_sheet(&mut workbook, &sheet)?;
    Ok(range_to_table(&range, header, skip_rows))
}

/// Reads a workbook file of any format calamine knows (xlsx, xlsm, xlsb,
/// xls, ods — chosen by extension) into a [`Table`].
pub fn read_workbook(path: &Path, sheet: &Sheet, header: bool, skip_rows: usize) -> Result<Table> {
    let mut workbook = calamine::open_workbook_auto(path).context("not a readable workbook")?;
    let range = select_sheet(&mut workbook, sheet)?;
    Ok(range_to_table(&range, header, skip_rows))
}

fn select_sheet<R: Reader<RS>, RS: std::io::Read + std::io::Seek>(workbook: &mut R, sheet: &Sheet) -> Result<Range<Data>>
where
    R::Error: std::error::Error + Send + Sync + 'static,
{
    match sheet {
        Sheet::Name(name) => workbook
            .worksheet_range(name)
            .with_context(|| format!("sheet '{name}' not found")),
        Sheet::First => {
            let first = workbook
                .sheet_names()
                .first()
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("workbook has no sheets"))?;
            workbook.worksheet_range(&first).context("failed to read first sheet")
        }
        Sheet::Index(i) => {
            let names = workbook.sheet_names();
            let name = names.get(*i).cloned().ok_or_else(|| {
                anyhow::anyhow!("sheet index {i} out of range (the workbook has {} sheets)", names.len())
            })?;
            workbook.worksheet_range(&name).with_context(|| format!("failed to read sheet '{name}'"))
        }
    }
}

/// A sheet's cells as a [`Table`]: `skip_rows` rows dropped, then an
/// optional header row; every row padded/truncated to the column count.
pub fn range_to_table(range: &Range<Data>, header: bool, skip_rows: usize) -> Table {
    let all: Vec<&[Data]> = range.rows().skip(skip_rows).collect();
    if all.is_empty() {
        return Table::new(Vec::new());
    }

    let (columns, data_rows): (Vec<String>, &[&[Data]]) = if header {
        let cols = all[0].iter().map(header_name).collect();
        (cols, &all[1..])
    } else {
        let width = all.iter().map(|r| r.len()).max().unwrap_or(0);
        ((0..width).map(|i| format!("col_{i}")).collect(), &all[..])
    };

    let width = columns.len();
    let mut table = Table::new(columns);
    for row in data_rows {
        let mut cells: Vec<Cell> = row.iter().map(excel_cell).collect();
        cells.resize(width, Cell::Null);
        cells.truncate(width);
        table.rows.push(cells);
    }
    table
}
