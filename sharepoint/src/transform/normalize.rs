//! `normalize` — normalize column names. Port of `sp-bulk`'s
//! `NormalizeTransform._normalize_columns`: NFKD → drop non-ASCII (strips accents,
//! e.g. `ATUAÇÃO` → `atuacao`) → lowercase → non-alnum runs become `filler` →
//! collapse/trim filler → empty becomes `col` → leading digit gets a `col_`
//! prefix → de-duplicate with `_N` suffixes.

use std::collections::{HashMap, HashSet};

use anyhow::Result;
use unicode_normalization::UnicodeNormalization;

use super::Transform;
use crate::table::Table;

pub struct Normalize {
    pub columns: bool,
    pub filler: String,
}

impl Normalize {
    pub fn new(columns: bool, filler: String) -> Self {
        Normalize { columns, filler }
    }
}

impl Transform for Normalize {
    fn apply(&self, mut table: Table) -> Result<Table> {
        if !self.columns {
            return Ok(table);
        }
        table.columns = normalize_columns(&table.columns, &self.filler);
        Ok(table)
    }
}

fn normalize_one(original: &str, filler: &str) -> String {
    // NFKD decompose, then keep only ASCII (drops combining marks left by NFKD).
    let nfkd: String = original.trim().nfkd().collect();
    let ascii: String = nfkd.chars().filter(char::is_ascii).collect();
    let lower = ascii.to_lowercase();

    // Replace runs of non-[a-z0-9] with a single filler.
    let mut out = String::new();
    let mut in_run = false;
    for c in lower.chars() {
        if c.is_ascii_lowercase() || c.is_ascii_digit() {
            out.push(c);
            in_run = false;
        } else if !in_run {
            out.push_str(filler);
            in_run = true;
        }
    }

    // Trim filler characters from both ends (str.strip(filler) semantics).
    let filler_chars: Vec<char> = filler.chars().collect();
    let trimmed = out.trim_matches(|c| filler_chars.contains(&c)).to_string();

    let mut name = if trimmed.is_empty() { "col".to_string() } else { trimmed };
    if name.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false) {
        name = format!("col_{name}");
    }
    name
}

fn normalize_columns(columns: &[String], filler: &str) -> Vec<String> {
    let mut used: HashSet<String> = HashSet::new();
    let mut base_counts: HashMap<String, usize> = HashMap::new();
    let mut result: Vec<String> = Vec::with_capacity(columns.len());

    for original in columns {
        let name = normalize_one(original, filler);
        let candidate = if !used.contains(&name) {
            base_counts.insert(name.clone(), 1);
            name.clone()
        } else {
            let mut idx = *base_counts.get(&name).unwrap_or(&1);
            let mut candidate = format!("{name}_{idx}");
            while used.contains(&candidate) {
                idx += 1;
                candidate = format!("{name}_{idx}");
            }
            base_counts.insert(name.clone(), idx + 1);
            candidate
        };
        used.insert(candidate.clone());
        result.push(candidate);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn norm(cols: &[&str]) -> Vec<String> {
        let v: Vec<String> = cols.iter().map(|s| s.to_string()).collect();
        normalize_columns(&v, "_")
    }

    #[test]
    fn strips_accents_and_lowercases() {
        assert_eq!(norm(&["ATUAÇÃO"]), vec!["atuacao"]);
        assert_eq!(norm(&["ÁREA"]), vec!["area"]);
    }

    #[test]
    fn non_alnum_becomes_filler_and_trims() {
        assert_eq!(norm(&["ENT."]), vec!["ent"]);
        assert_eq!(norm(&["Data de Admissão"]), vec!["data_de_admissao"]);
    }

    #[test]
    fn empty_becomes_col_and_leading_digit_prefixed() {
        assert_eq!(norm(&["   "]), vec!["col"]);
        assert_eq!(norm(&["2026"]), vec!["col_2026"]);
    }

    #[test]
    fn deduplicates_collisions() {
        assert_eq!(norm(&["A", "a", "á"]), vec!["a", "a_1", "a_2"]);
    }

    #[test]
    fn columns_false_is_noop() {
        let mut t = Table::new(vec!["A B".into()]);
        t.rows.push(vec![]);
        let out = Normalize::new(false, "_".into()).apply(t).unwrap();
        assert_eq!(out.columns, vec!["A B".to_string()]);
    }
}
