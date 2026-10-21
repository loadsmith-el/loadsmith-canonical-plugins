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

crate::vocab::config_enum! {
    /// Leaf comparison operator (`op`).
    pub enum Op for "op" {
        Eq = "eq",
        Ne = "ne",
        Lt = "lt",
        Le = "le",
        Gt = "gt",
        Ge = "ge",
        In = "in",
        Null = "null",
        NotNull = "not-null",
    }
    retired { "is-null" => "null", "is-not-null" => "not-null" }
}

impl Op {
    fn parse(s: &str, path: &str) -> Result<Op> {
        crate::vocab::choose("op", s, Op::VALUES, Op::RETIRED).map_err(|e| anyhow::anyhow!("{path}.op: {e}"))
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
    /// precise errors. Keys a node does not use (`{column, op, valeu}`, a
    /// second key next to `all`) are pushed to `unknown` as config paths, for
    /// the caller to apply the unknown-keys policy.
    pub fn parse(v: &Value, path: &str, unknown: &mut Vec<String>) -> Result<Predicate> {
        let obj = v
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("{path}: predicate must be a mapping"))?;

        for combinator in ["all", "any", "not"] {
            let Some(inner) = obj.get(combinator) else { continue };
            unknown.extend(obj.keys().filter(|k| *k != combinator).map(|k| format!("{path}.{k}")));
            let at = format!("{path}.{combinator}");
            return Ok(match combinator {
                "all" => Predicate::All(Self::parse_list(inner, &at, unknown)?),
                "any" => Predicate::Any(Self::parse_list(inner, &at, unknown)?),
                _ => Predicate::Not(Box::new(Self::parse(inner, &at, unknown)?)),
            });
        }
        unknown.extend(
            obj.keys().filter(|k| !matches!(k.as_str(), "column" | "op" | "value")).map(|k| format!("{path}.{k}")),
        );

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

    fn parse_list(v: &Value, path: &str, unknown: &mut Vec<String>) -> Result<Vec<Predicate>> {
        let arr = v
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("{path}: expected a list of predicates"))?;
        arr.iter()
            .enumerate()
            .map(|(i, item)| Predicate::parse(item, &format!("{path}[{i}]"), unknown))
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
        let pred = Predicate::parse(&json, "transform[0].where", &mut Vec::new()).unwrap();
        Filter::new(pred).apply(table()).unwrap()
    }

    #[test]
    fn not_null_keeps_only_filled() {
        let out = apply(serde_json::json!({ "column": "atuacao", "op": "not-null" }));
        // rows 0 and 3 have non-blank atuacao
        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    fn all_combines_with_in() {
        let out = apply(serde_json::json!({
            "all": [
                { "column": "atuacao", "op": "not-null" },
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
                { "not": { "column": "atuacao", "op": "not-null" } }
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
            &mut Vec::new(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("transform[0].where.op"));
        let err = Predicate::parse(&serde_json::json!({ "column": "a", "op": "not_null" }), "w", &mut Vec::new())
            .unwrap_err();
        assert!(err.to_string().contains("did you mean `not-null`?"), "{err}");
    }

    #[test]
    fn unused_keys_are_reported() {
        let mut unknown = Vec::new();
        Predicate::parse(
            &serde_json::json!({ "all": [{ "column": "a", "op": "eq", "valeu": 1, "value": 1 }], "x": 1 }),
            "w",
            &mut unknown,
        )
        .unwrap();
        assert_eq!(unknown, ["w.x", "w.all[0].valeu"]);
    }

    #[test]
    fn missing_column_errors_at_apply() {
        let pred = Predicate::parse(
            &serde_json::json!({ "column": "nope", "op": "not-null" }),
            "transform[0].where",
            &mut Vec::new(),
        )
        .unwrap();
        assert!(Filter::new(pred).apply(table()).is_err());
    }

    #[test]
    fn in_requires_array() {
        assert!(Predicate::parse(
            &serde_json::json!({ "column": "area", "op": "in", "value": "CIVEL" }),
            "w",
            &mut Vec::new()
        )
        .is_err());
    }
}
