//! CSV → [`Table`] (whole file in memory; the `csv` parser plugin streams
//! instead, with the same cell rules: [`csv_cell`]).

use anyhow::{Context, Result};
use encoding_rs::Encoding;

use crate::table::{Cell, Table};

/// Infers a typed cell from a raw CSV string (mirrors pandas' read_csv inference,
/// scoped to int/float/empty→null/else string).
pub fn csv_cell(s: &str) -> Cell {
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

/// The encoding for a WHATWG label (`utf-8`, `latin1`, `windows-1252`, …);
/// `None` is UTF-8.
pub fn encoding(label: Option<&str>) -> Result<&'static Encoding> {
    let label = label.unwrap_or("utf-8");
    Encoding::for_label(label.as_bytes()).ok_or_else(|| anyhow::anyhow!("unknown encoding '{label}'"))
}

/// A delimiter as the single byte the `csv` crate takes.
pub fn delimiter_byte(delimiter: char) -> Result<u8> {
    u8::try_from(delimiter as u32).map_err(|_| anyhow::anyhow!("delimiter must be a single-byte character"))
}

/// Parses CSV bytes (decoded from `encoding`, UTF-8 by default) into a
/// [`Table`]. Rows may be ragged: the table is as wide as the widest row,
/// and shorter rows are padded with nulls. Without a header, columns are
/// named `col_0`, `col_1`, …
pub fn parse_csv(bytes: &[u8], delimiter: char, encoding_label: Option<&str>, header: bool) -> Result<Table> {
    let enc = encoding(encoding_label)?;
    let (text, _, _) = enc.decode(bytes);

    let delim = delimiter_byte(delimiter)?;

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

    #[test]
    fn unknown_encoding_and_wide_delimiter_are_errors() {
        assert!(parse_csv(b"a\n", ',', Some("klingon"), true).unwrap_err().to_string().contains("klingon"));
        assert!(parse_csv(b"a\n", '€', None, true).is_err());
    }
}
