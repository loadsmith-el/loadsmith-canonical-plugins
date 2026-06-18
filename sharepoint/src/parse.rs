//! File bytes → [`Table`] (file input mode only). Excel via `calamine`, CSV via
//! the `csv` crate — both pure Rust (multi-arch posture: no native deps).

use std::io::Cursor;

use anyhow::{Context, Result};
use calamine::{Data, Reader, Xlsx};

use crate::config::ParserConfig;
use crate::table::{Cell, Table};

/// Parses file bytes into a cell table according to the parser config.
pub fn parse(bytes: &[u8], parser: &ParserConfig) -> Result<Table> {
    match parser {
        ParserConfig::Excel { sheet, header, skip_rows } => {
            parse_excel(bytes, sheet.as_deref(), *header, *skip_rows)
        }
        ParserConfig::Csv { delimiter, encoding, header } => {
            parse_csv(bytes, *delimiter, encoding.as_deref(), *header)
        }
    }
}

fn excel_cell(data: &Data) -> Cell {
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

fn parse_excel(bytes: &[u8], sheet: Option<&str>, header: bool, skip_rows: usize) -> Result<Table> {
    let mut workbook: Xlsx<_> =
        calamine::open_workbook_from_rs(Cursor::new(bytes.to_vec())).context("not a valid xlsx")?;

    let range = match sheet {
        Some(name) => workbook
            .worksheet_range(name)
            .with_context(|| format!("sheet '{name}' not found"))?,
        None => {
            let first = workbook
                .sheet_names()
                .first()
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("workbook has no sheets"))?;
            workbook.worksheet_range(&first).context("failed to read first sheet")?
        }
    };

    let all: Vec<&[Data]> = range.rows().skip(skip_rows).collect();
    if all.is_empty() {
        return Ok(Table::new(Vec::new()));
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
    Ok(table)
}

/// Infers a typed cell from a raw CSV string (mirrors pandas' read_csv inference,
/// scoped to int/float/empty→null/else string).
fn csv_cell(s: &str) -> Cell {
    let t = s.trim();
    if t.is_empty() {
        return Cell::Null;
    }
    if let Ok(i) = t.parse::<i64>() {
        return Cell::Int(i);
    }
    if let Ok(f) = t.parse::<f64>() {
        return Cell::Float(f);
    }
    Cell::Str(s.to_string())
}

fn parse_csv(bytes: &[u8], delimiter: char, encoding: Option<&str>, header: bool) -> Result<Table> {
    let label = encoding.unwrap_or("utf-8");
    let enc = encoding_rs::Encoding::for_label(label.as_bytes())
        .ok_or_else(|| anyhow::anyhow!("unknown encoding '{label}'"))?;
    let (text, _, _) = enc.decode(bytes);

    let delim = u8::try_from(delimiter as u32)
        .map_err(|_| anyhow::anyhow!("delimiter must be a single-byte character"))?;

    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(delim)
        .has_headers(header)
        .flexible(true)
        .from_reader(text.as_bytes());

    let columns: Vec<String> = if header {
        rdr.headers().context("failed to read CSV header")?.iter().map(str::to_string).collect()
    } else {
        Vec::new()
    };

    let mut records = Vec::new();
    let mut width = columns.len();
    for rec in rdr.records() {
        let rec = rec.context("failed to read CSV record")?;
        width = width.max(rec.len());
        records.push(rec);
    }

    let columns = if columns.is_empty() {
        (0..width).map(|i| format!("col_{i}")).collect()
    } else {
        columns
    };

    let mut table = Table::new(columns);
    for rec in records {
        let mut cells: Vec<Cell> = rec.iter().map(csv_cell).collect();
        cells.resize(width, Cell::Null);
        table.rows.push(cells);
    }
    Ok(table)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csv_with_header_infers_types() {
        let bytes = b"id,name,score\n1,alice,1.5\n2,bob,\n";
        let t = parse_csv(bytes, ',', None, true).unwrap();
        assert_eq!(t.columns, vec!["id", "name", "score"]);
        assert_eq!(t.num_rows(), 2);
        assert_eq!(t.rows[0][0], Cell::Int(1));
        assert_eq!(t.rows[0][2], Cell::Float(1.5));
        assert_eq!(t.rows[1][2], Cell::Null);
    }

    #[test]
    fn csv_without_header_generates_names() {
        let bytes = b"a;b\nc;d\n";
        let t = parse_csv(bytes, ';', None, false).unwrap();
        assert_eq!(t.columns, vec!["col_0", "col_1"]);
        assert_eq!(t.num_rows(), 2);
    }
}
