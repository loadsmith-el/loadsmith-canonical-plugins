//! File bytes → [`Table`] (file input mode only). Excel via `calamine`, CSV via
//! the `csv` crate — both pure Rust, shared with the parser plugins through the
//! `tabular` crate.

use anyhow::Result;

use crate::config::ParserConfig;
use crate::table::Table;

/// Parses file bytes into a cell table according to the parser config.
pub fn parse(bytes: &[u8], parser: &ParserConfig) -> Result<Table> {
    match parser {
        ParserConfig::Excel { sheet, header, skip_rows } => {
            tabular::excel::parse_excel(bytes, sheet.as_deref(), *header, *skip_rows)
        }
        ParserConfig::Csv { delimiter, encoding, header } => {
            tabular::csv::parse_csv(bytes, *delimiter, encoding.as_deref(), *header)
        }
    }
}
