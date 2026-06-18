//! `filter` — keep rows matching a structured predicate.
//!
//! Unlike the Python `sp-bulk` (which used `pandas.DataFrame.query`, an arbitrary
//! Python expression), the predicate is a declarative YAML tree: a leaf
//! `{ column, op, value? }` or a combinator `all` (AND) / `any` (OR) / `not`. It
//! is parsed from the raw config value by [`Predicate::parse`], which threads a
//! config path so errors point at the exact offending node (e.g.
//! `transform[0].where.all[1].op`).

use anyhow::{bail, Result};
use serde_json::Value;

use super::Transform;
use crate::table::{Cell, Table};

/// Leaf comparison operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    In,
    Null,
    NotNull,
}

impl Op {
    fn parse(s: &str, path: &str) -> Result<Op> {
        Ok(match s {
            "eq" => Op::Eq,
            "ne" => Op::Ne,
            "lt" => Op::Lt,
            "le" => Op::Le,
            "gt" => Op::Gt,
            "ge" => Op::Ge,
            "in" => Op::In,
            "null" => Op::Null,
            "not_null" => Op::NotNull,
            other => bail!(
                "{path}.op: unknown operator '{other}' \
                 (expected eq/ne/lt/le/gt/ge/in/null/not_null)"
            ),
        })
    }

    fn needs_value(self) -> bool {
        !matches!(self, Op::Null | Op::NotNull)
    }
}

/// A predicate node.
#[derive(Debug, Clone)]
pub enum Predicate {
    All(Vec<Predicate>),
    Any(Vec<Predicate>),
    Not(Box<Predicate>),
    Leaf { column: String, op: Op, value: Option<Value> },
}

impl Predicate {
    /// Recursively parses a predicate from a raw config value, with a path for
    /// precise errors.
    pub fn parse(v: &Value, path: &str) -> Result<Predicate> {
        let obj = v
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("{path}: predicate must be a mapping"))?;

        if let Some(inner) = obj.get("all") {
            return Ok(Predicate::All(Self::parse_list(inner, &format!("{path}.all"))?));
        }
        if let Some(inner) = obj.get("any") {
            return Ok(Predicate::Any(Self::parse_list(inner, &format!("{path}.any"))?));
        }
        if let Some(inner) = obj.get("not") {
            return Ok(Predicate::Not(Box::new(Self::parse(inner, &format!("{path}.not"))?)));
        }

        // Otherwise it's a leaf: { column, op, value? }.
        let column = obj
            .get("column")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("{path}: leaf predicate requires a string 'column'"))?
            .to_string();
        let op_str = obj
            .get("op")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("{path}: leaf predicate requires a string 'op'"))?;
        let op = Op::parse(op_str, path)?;
        let value = obj.get("value").cloned();

        if op.needs_value() && value.is_none() {
            bail!("{path}: operator '{op_str}' requires a 'value'");
        }
        if op == Op::In && !value.as_ref().map(Value::is_array).unwrap_or(false) {
            bail!("{path}.value: operator 'in' requires an array");
        }
        Ok(Predicate::Leaf { column, op, value })
    }

    fn parse_list(v: &Value, path: &str) -> Result<Vec<Predicate>> {
        let arr = v
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("{path}: expected a list of predicates"))?;
        arr.iter()
            .enumerate()
            .map(|(i, item)| Predicate::parse(item, &format!("{path}[{i}]")))
            .collect()
    }

    /// Validates that every referenced column exists in the table.
    fn validate_columns(&self, table: &Table) -> Result<()> {
        match self {
            Predicate::All(v) | Predicate::Any(v) => {
                for p in v {
                    p.validate_columns(table)?;
                }
                Ok(())
            }
            Predicate::Not(p) => p.validate_columns(table),
            Predicate::Leaf { column, .. } => {
                if table.col_index(column).is_none() {
                    bail!("filter: column '{column}' not found in the data");
                }
                Ok(())
            }
        }
    }

    fn eval(&self, row: &[Cell], columns: &[String]) -> bool {
        match self {
            Predicate::All(v) => v.iter().all(|p| p.eval(row, columns)),
            Predicate::Any(v) => v.iter().any(|p| p.eval(row, columns)),
            Predicate::Not(p) => !p.eval(row, columns),
            Predicate::Leaf { column, op, value } => {
                let idx = match columns.iter().position(|c| c == column) {
                    Some(i) => i,
                    None => return false,
                };
                let cell = row.get(idx).cloned().unwrap_or(Cell::Null);
                eval_leaf(&cell, *op, value.as_ref())
            }
        }
    }
}

fn value_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        other => other.to_string(),
    }
}

fn eval_leaf(cell: &Cell, op: Op, value: Option<&Value>) -> bool {
    match op {
        Op::Null => cell.is_blank(),
        Op::NotNull => !cell.is_blank(),
        Op::In => match value.and_then(Value::as_array) {
            Some(arr) => arr.iter().any(|v| cells_equal(cell, v)),
            None => false,
        },
        Op::Eq => value.map(|v| cells_equal(cell, v)).unwrap_or(false),
        Op::Ne => value.map(|v| !cells_equal(cell, v)).unwrap_or(false),
        Op::Lt | Op::Le | Op::Gt | Op::Ge => {
            // Null/blank never satisfies an ordering comparison (SQL/pandas-like).
            if cell.is_blank() {
                return false;
            }
            let ord = match value {
                Some(v) => compare(cell, v),
                None => return false,
            };
            match ord {
                Some(std::cmp::Ordering::Less) => matches!(op, Op::Lt | Op::Le),
                Some(std::cmp::Ordering::Equal) => matches!(op, Op::Le | Op::Ge),
                Some(std::cmp::Ordering::Greater) => matches!(op, Op::Gt | Op::Ge),
                None => false,
            }
        }
    }
}

fn cells_equal(cell: &Cell, value: &Value) -> bool {
    if cell.is_blank() {
        return value.is_null();
    }
    // Prefer numeric equality when both sides are numbers.
    if let (Some(a), Some(b)) = (cell.as_f64(), value.as_f64()) {
        return a == b;
    }
    cell.as_text().as_deref() == Some(value_text(value).as_str())
}

fn compare(cell: &Cell, value: &Value) -> Option<std::cmp::Ordering> {
    if let (Some(a), Some(b)) = (cell.as_f64(), value.as_f64()) {
        return a.partial_cmp(&b);
    }
    cell.as_text().map(|t| t.cmp(&value_text(value)))
}

/// The `filter` transform.
pub struct Filter {
    predicate: Predicate,
}

impl Filter {
    pub fn new(predicate: Predicate) -> Self {
        Filter { predicate }
    }
}

impl Transform for Filter {
    fn apply(&self, mut table: Table) -> Result<Table> {
        self.predicate.validate_columns(&table)?;
        let pred = &self.predicate;
        let columns = table.columns.clone();
        let kept: Vec<Vec<Cell>> =
            table.rows.drain(..).filter(|row| pred.eval(row, &columns)).collect();
        table.rows = kept;
        Ok(table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table() -> Table {
        let mut t = Table::new(vec!["atuacao".into(), "area".into(), "idade".into()]);
        t.rows.push(vec![Cell::Str("X".into()), Cell::Str("CIVEL".into()), Cell::Int(30)]);
        t.rows.push(vec![Cell::Null, Cell::Str("CRIMINAL".into()), Cell::Int(20)]);
        t.rows.push(vec![Cell::Str(" ".into()), Cell::Str("CIVEL".into()), Cell::Int(40)]);
        t.rows.push(vec![Cell::Str("Y".into()), Cell::Str("TRAB".into()), Cell::Int(18)]);
        t
    }

    fn apply(json: serde_json::Value) -> Table {
        let pred = Predicate::parse(&json, "transform[0].where").unwrap();
        Filter::new(pred).apply(table()).unwrap()
    }

    #[test]
    fn not_null_keeps_only_filled() {
        let out = apply(serde_json::json!({ "column": "atuacao", "op": "not_null" }));
        // rows 0 and 3 have non-blank atuacao
        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    fn all_combines_with_in() {
        let out = apply(serde_json::json!({
            "all": [
                { "column": "atuacao", "op": "not_null" },
                { "column": "area", "op": "in", "value": ["CIVEL", "CRIMINAL"] }
            ]
        }));
        // only row 0 (filled + CIVEL); row 3 is TRAB
        assert_eq!(out.num_rows(), 1);
    }

    #[test]
    fn numeric_comparison() {
        let out = apply(serde_json::json!({ "column": "idade", "op": "ge", "value": 30 }));
        assert_eq!(out.num_rows(), 2); // 30 and 40
    }

    #[test]
    fn any_and_not() {
        let out = apply(serde_json::json!({
            "any": [
                { "column": "area", "op": "eq", "value": "TRAB" },
                { "not": { "column": "atuacao", "op": "not_null" } }
            ]
        }));
        // area=TRAB (row3) OR atuacao blank (rows 1,2) → 3 rows
        assert_eq!(out.num_rows(), 3);
    }

    #[test]
    fn unknown_op_errors_with_path() {
        let err = Predicate::parse(
            &serde_json::json!({ "column": "a", "op": "approx" }),
            "transform[0].where",
        )
        .unwrap_err();
        assert!(err.to_string().contains("transform[0].where.op"));
    }

    #[test]
    fn missing_column_errors_at_apply() {
        let pred = Predicate::parse(
            &serde_json::json!({ "column": "nope", "op": "not_null" }),
            "transform[0].where",
        )
        .unwrap();
        assert!(Filter::new(pred).apply(table()).is_err());
    }

    #[test]
    fn in_requires_array() {
        assert!(Predicate::parse(
            &serde_json::json!({ "column": "area", "op": "in", "value": "CIVEL" }),
            "w"
        )
        .is_err());
    }
}
