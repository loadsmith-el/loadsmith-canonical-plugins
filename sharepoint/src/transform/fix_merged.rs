//! `fix_merged_columns` — forward-fill blanks in a column, undoing the
//! "merged cells" effect from spreadsheets. Blank = null or whitespace-only.
//! Port of `sp-bulk`'s `FixMergedColumnsTransform` (`ffill`).

use anyhow::{bail, Result};

use super::Transform;
use crate::table::{Cell, Table};

pub struct FixMergedColumns {
    pub column: String,
}

impl FixMergedColumns {
    pub fn new(column: String) -> Self {
        FixMergedColumns { column }
    }
}

impl Transform for FixMergedColumns {
    fn apply(&self, mut table: Table) -> Result<Table> {
        let idx = match table.col_index(&self.column) {
            Some(i) => i,
            None => bail!("fix_merged_columns: column '{}' not found in the data", self.column),
        };
        let mut last = Cell::Null;
        for row in &mut table.rows {
            if idx >= row.len() {
                continue;
            }
            if row[idx].is_blank() {
                row[idx] = last.clone();
            } else {
                last = row[idx].clone();
            }
        }
        Ok(table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forward_fills_blanks() {
        let mut t = Table::new(vec!["g".into(), "v".into()]);
        t.rows.push(vec![Cell::Str("A".into()), Cell::Int(1)]);
        t.rows.push(vec![Cell::Str(" ".into()), Cell::Int(2)]);
        t.rows.push(vec![Cell::Null, Cell::Int(3)]);
        t.rows.push(vec![Cell::Str("B".into()), Cell::Int(4)]);
        let out = FixMergedColumns::new("g".into()).apply(t).unwrap();
        let g: Vec<_> = out.rows.iter().map(|r| r[0].as_text()).collect();
        assert_eq!(
            g,
            vec![Some("A".into()), Some("A".into()), Some("A".into()), Some("B".into())]
        );
    }

    #[test]
    fn leading_blanks_stay_blank() {
        let mut t = Table::new(vec!["g".into()]);
        t.rows.push(vec![Cell::Null]);
        t.rows.push(vec![Cell::Str("A".into())]);
        let out = FixMergedColumns::new("g".into()).apply(t).unwrap();
        assert!(out.rows[0][0].is_blank());
        assert_eq!(out.rows[1][0].as_text(), Some("A".into()));
    }

    #[test]
    fn missing_column_errors() {
        let t = Table::new(vec!["g".into()]);
        assert!(FixMergedColumns::new("nope".into()).apply(t).is_err());
    }
}
