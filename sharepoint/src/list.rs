//! SharePoint List items → [`Table`]. Each item's `fields` object (the user
//! columns, fetched with `$expand=fields`) becomes a row; columns are the union of
//! field keys in first-seen order, dropping Graph's `@odata.*` annotations.
//!
//! List items are already structured JSON, so — unlike file mode — they do not go
//! through [`crate::parse`].

use anyhow::Result;
use serde_json::Value;

use crate::table::{Cell, Table};

fn json_cell(v: &Value) -> Cell {
    match v {
        Value::Null => Cell::Null,
        Value::Bool(b) => Cell::Bool(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Cell::Int(i)
            } else {
                Cell::Float(n.as_f64().unwrap_or(f64::NAN))
            }
        }
        Value::String(s) => Cell::Str(s.clone()),
        // Arrays/objects (e.g. lookup/person fields) → compact JSON text.
        other => Cell::Str(other.to_string()),
    }
}

/// Builds a table from the `fields` objects of list items.
pub fn items_to_table(fields: &[Value]) -> Result<Table> {
    let mut columns: Vec<String> = Vec::new();
    for item in fields {
        if let Some(obj) = item.as_object() {
            for key in obj.keys() {
                if key.starts_with("@odata") {
                    continue;
                }
                if !columns.iter().any(|c| c == key) {
                    columns.push(key.clone());
                }
            }
        }
    }

    let mut table = Table::new(columns.clone());
    for item in fields {
        let obj = item.as_object();
        let row: Vec<Cell> = columns
            .iter()
            .map(|c| obj.and_then(|o| o.get(c)).map(json_cell).unwrap_or(Cell::Null))
            .collect();
        table.rows.push(row);
    }
    Ok(table)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unions_columns_in_first_seen_order() {
        let items = vec![
            serde_json::json!({ "Title": "a", "Qtd": 1, "@odata.etag": "x" }),
            serde_json::json!({ "Title": "b", "Area": "CIVEL" }),
        ];
        let t = items_to_table(&items).unwrap();
        assert_eq!(t.columns, vec!["Title", "Qtd", "Area"]);
        assert_eq!(t.num_rows(), 2);
        assert_eq!(t.rows[0][1], Cell::Int(1));
        assert_eq!(t.rows[1][1], Cell::Null); // second item lacks Qtd
        assert_eq!(t.rows[1][2], Cell::Str("CIVEL".into()));
    }

    #[test]
    fn drops_odata_annotations() {
        let items = vec![serde_json::json!({ "@odata.etag": "1", "Title": "x" })];
        let t = items_to_table(&items).unwrap();
        assert_eq!(t.columns, vec!["Title"]);
    }
}
