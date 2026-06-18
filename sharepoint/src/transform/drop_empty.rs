//! `drop_empty_rows` — remove rows whose every cell is empty. With
//! `zero_is_empty`, numeric/string zeros also count as empty (booleans never do).
//! Port of `sp-bulk`'s `DropEmptyRowsTransform`.

use anyhow::Result;

use super::Transform;
use crate::table::{Cell, Table};

pub struct DropEmptyRows {
    pub zero_is_empty: bool,
}

impl DropEmptyRows {
    pub fn new(zero_is_empty: bool) -> Self {
        DropEmptyRows { zero_is_empty }
    }
}

fn is_empty_cell(cell: &Cell, zero_is_empty: bool) -> bool {
    if cell.is_blank() {
        return true;
    }
    if zero_is_empty {
        match cell {
            Cell::Bool(_) => return false,
            Cell::Int(0) => return true,
            Cell::Float(f) if *f == 0.0 => return true,
            Cell::Str(s) => {
                let t = s.trim();
                if t == "0" || t == "0.0" {
                    return true;
                }
            }
            _ => {}
        }
    }
    false
}

impl Transform for DropEmptyRows {
    fn apply(&self, mut table: Table) -> Result<Table> {
        let zero = self.zero_is_empty;
        table.rows.retain(|row| !row.iter().all(|c| is_empty_cell(c, zero)));
        Ok(table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drops_all_blank_rows() {
        let mut t = Table::new(vec!["a".into(), "b".into()]);
        t.rows.push(vec![Cell::Str("x".into()), Cell::Null]);
        t.rows.push(vec![Cell::Null, Cell::Str("  ".into())]);
        t.rows.push(vec![Cell::Int(5), Cell::Int(6)]);
        let out = DropEmptyRows::new(false).apply(t).unwrap();
        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    fn zero_is_empty_drops_zero_rows() {
        let mut t = Table::new(vec!["a".into(), "b".into()]);
        t.rows.push(vec![Cell::Int(0), Cell::Str("0.0".into())]);
        t.rows.push(vec![Cell::Int(0), Cell::Int(1)]);
        let out = DropEmptyRows::new(true).apply(t).unwrap();
        assert_eq!(out.num_rows(), 1); // first row all-zero → dropped
    }

    #[test]
    fn bool_false_is_not_empty() {
        let mut t = Table::new(vec!["a".into()]);
        t.rows.push(vec![Cell::Bool(false)]);
        let out = DropEmptyRows::new(true).apply(t).unwrap();
        assert_eq!(out.num_rows(), 1);
    }
}
