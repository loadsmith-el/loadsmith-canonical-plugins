use std::collections::HashMap;

use anyhow::{bail, Context, Result};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use loadsmith_plugin_sdk::DestinationPlugin;
use oracledb::{Connection, DbType, ToDbValue};
use serde::Deserialize;

use crate::conn::ConnectionConfig;
use crate::types::{cell_to_bind, fold_ident, quote_ident, typed_expr, Bind};

#[derive(Debug, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CommitMode {
    /// One transaction for the whole load; array-`INSERT` straight into the
    /// target, `COMMIT` at the end. All-or-nothing, at-least-once.
    #[default]
    Atomic,
    /// Bulk-load into a session-private global temporary staging table, then a
    /// single set-based `MERGE` from staging into the target by `merge_key`
    /// (`WHEN MATCHED` update / `WHEN NOT MATCHED` insert), `COMMIT` at the end.
    /// Idempotent by the key ⇒ exactly-once effective. The staging insert is the
    /// same fast array-DML as `Atomic`, and the optimizer plans the one MERGE
    /// against the real staging cardinality — unlike a per-row `MERGE … FROM
    /// dual`, whose empty-target plan degrades to O(n²) as the target fills.
    StagedMerge,
}

#[derive(Debug, Deserialize)]
struct OracleDestConfig {
    #[serde(flatten)]
    conn: ConnectionConfig,

    /// Target table.
    target_table: String,
    #[serde(default)]
    mode: CommitMode,
    /// Key columns for `staged_merge`'s `ON (...)` match. Must be a unique key
    /// on the target.
    #[serde(default)]
    merge_key: Vec<String>,
}

pub struct OracleDestPlugin {
    config: Option<OracleDestConfig>,
    conn: Option<Connection>,
    columns: Vec<String>,
    /// Target column → Oracle type (upper-cased names), from describing the
    /// target table in `prepare`. Drives the explicit `TO_*` bind wrapping.
    target_types: HashMap<String, &'static DbType>,
    /// Per-batch array-DML `INSERT` SQL — into the target (`Atomic`) or into the
    /// staging table (`StagedMerge`). Built once the column set is known.
    sql: Option<String>,
    /// `StagedMerge` only: the session-private global temporary staging table,
    /// created in `prepare`.
    staging_table: Option<String>,
    /// `StagedMerge` only: the single set-based `MERGE` run at `finalize`. Built
    /// (and the `merge_key` validated) on the first batch.
    merge_sql: Option<String>,
    rows_written: u64,
}

impl Default for OracleDestPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl OracleDestPlugin {
    pub fn new() -> Self {
        Self {
            config: None,
            conn: None,
            columns: Vec::new(),
            target_types: HashMap::new(),
            sql: None,
            staging_table: None,
            merge_sql: None,
            rows_written: 0,
        }
    }

    fn cfg(&self) -> &OracleDestConfig {
        self.config.as_ref().expect("configured")
    }

    /// The explicit bind expression for one incoming column at placeholder `:idx`
    /// — `TO_DATE(:idx, …)` / `TO_TIMESTAMP*(:idx, …)` for temporal target
    /// columns, plain `:idx` otherwise (unknown columns pass through).
    fn bind_expr(&self, col: &str, idx: usize) -> String {
        let placeholder = format!(":{idx}");
        match self.target_types.get(&col.to_uppercase()) {
            Some(t) => typed_expr(&placeholder, *t),
            None => placeholder,
        }
    }
}

#[async_trait]
impl DestinationPlugin for OracleDestPlugin {
    fn plugin_name(&self) -> &str {
        "loadsmith-destination-oracle"
    }
    fn plugin_version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    fn capabilities(&self) -> Vec<String> {
        vec!["batch_write".into(), "staged_merge".into()]
    }

    async fn configure(&mut self, config: serde_json::Value) -> Result<()> {
        let cfg: OracleDestConfig =
            serde_json::from_value(config).map_err(|e| anyhow::anyhow!("invalid oracle destination config: {e}"))?;
        if cfg.mode == CommitMode::StagedMerge && cfg.merge_key.is_empty() {
            bail!("mode 'staged_merge' requires a non-empty merge_key");
        }
        self.config = Some(cfg);
        Ok(())
    }

    async fn prepare(&mut self) -> Result<()> {
        // Nothing is durable until finalize() commits (Oracle holds the
        // transaction open implicitly).
        let describe = format!("SELECT * FROM {} WHERE 1 = 0", quote_ident(&self.cfg().target_table));

        let (conn, target_types) = tokio::task::block_in_place(|| -> Result<_> {
            let conn = crate::conn::connect(&self.cfg().conn)?;
            let rs = conn
                .query(&describe, &[])
                .map_err(|e| anyhow::anyhow!("describing target table failed: {e}"))?;
            let target_types: HashMap<String, &'static DbType> =
                rs.columns().iter().map(|c| (c.name().to_uppercase(), c.db_type())).collect();
            Ok((conn, target_types))
        })?;

        self.target_types = target_types;
        self.conn = Some(conn);

        // staged_merge stages into a session-private global temporary table that
        // mirrors the target's shape. Created here (DDL auto-commits) before any
        // row is written; `ON COMMIT DELETE ROWS` clears it at finalize's commit.
        // Create-if-not-exists (ORA-00955) so reruns reuse it.
        if self.cfg().mode == CommitMode::StagedMerge {
            let staging = staging_table_name(&self.cfg().target_table);
            let create = format!(
                "DECLARE\n  e_exists EXCEPTION;\n  PRAGMA EXCEPTION_INIT(e_exists, -955);\nBEGIN\n  \
                 EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE {stg} ON COMMIT DELETE ROWS \
                 AS SELECT * FROM {tgt} WHERE 1 = 0';\nEXCEPTION WHEN e_exists THEN NULL;\nEND;",
                stg = quote_ident(&staging),
                tgt = quote_ident(&self.cfg().target_table),
            );
            let conn = self.conn.as_ref().unwrap();
            tokio::task::block_in_place(|| conn.execute(&create, &[]))
                .map_err(|e| anyhow::anyhow!("creating staging table failed: {e}"))?;
            self.staging_table = Some(staging);
        }
        Ok(())
    }

    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        if self.columns.is_empty() {
            self.columns = batch.schema().fields().iter().map(|f| f.name().clone()).collect();
            // Per-column bind expressions (temporal columns wrapped in TO_*).
            let binds: Vec<String> =
                self.columns.iter().enumerate().map(|(i, c)| self.bind_expr(c, i + 1)).collect();
            let target = self.cfg().target_table.clone();
            let merge_key = self.cfg().merge_key.clone();
            self.sql = Some(match self.cfg().mode {
                CommitMode::Atomic => build_insert_sql(&target, &self.columns, &binds),
                CommitMode::StagedMerge => {
                    let staging = self.staging_table.clone().expect("staging table prepared");
                    // Build (and validate the merge_key against the incoming
                    // columns) the one set-based MERGE up front; run it at finalize.
                    self.merge_sql =
                        Some(build_staged_merge_sql(&target, &staging, &self.columns, &merge_key)?);
                    // Each batch array-inserts into staging — same fast path as atomic.
                    build_insert_sql(&staging, &self.columns, &binds)
                }
            });
        }
        let nrows = batch.num_rows();
        if nrows == 0 {
            return Ok(());
        }

        // Materialise all binds (owned) up front, then run the array DML.
        let mut rows: Vec<Vec<Bind>> = Vec::with_capacity(nrows);
        for row in 0..nrows {
            let mut cells: Vec<Bind> = Vec::with_capacity(batch.num_columns());
            for col in 0..batch.num_columns() {
                cells.push(cell_to_bind(batch.column(col), row)?);
            }
            rows.push(cells);
        }

        let sql = self.sql.as_ref().unwrap().clone();
        let conn = self.conn.as_ref().context("not prepared")?;
        tokio::task::block_in_place(|| -> Result<()> {
            // Array DML: one execute_batch over a 2-D slice of `&dyn ToDbValue`.
            let params: Vec<Vec<&dyn ToDbValue>> =
                rows.iter().map(|row| row.iter().map(|b| b.as_sql()).collect()).collect();
            let slices: Vec<&[&dyn ToDbValue]> = params.iter().map(|r| r.as_slice()).collect();
            conn.execute_batch(&sql, slices.as_slice())
                .map_err(|e| anyhow::anyhow!("array DML failed: {e}"))?;
            Ok(())
        })?;

        self.rows_written += nrows as u64;
        Ok(())
    }

    async fn finalize(&mut self) -> Result<u64> {
        let conn = self.conn.as_ref().context("not prepared")?;
        // staged_merge: a single set-based MERGE from the staging table into the
        // target, in the same transaction as the staging inserts, then commit.
        if let Some(merge_sql) = self.merge_sql.clone() {
            tokio::task::block_in_place(|| conn.execute(&merge_sql, &[]))
                .map_err(|e| anyhow::anyhow!("staged MERGE failed: {e}"))?;
        }
        tokio::task::block_in_place(|| conn.commit().map_err(|e| anyhow::anyhow!("COMMIT failed: {e}")))?;
        Ok(self.rows_written)
    }

    async fn cancel(&mut self) {
        if let Some(conn) = self.conn.as_ref() {
            let _ = tokio::task::block_in_place(|| conn.rollback());
        }
    }
}

/// `INSERT INTO "target" ("c1", …) VALUES (<bind1>, …)`, where each `<bind>` is
/// the column's (possibly `TO_*`-wrapped) bind expression.
fn build_insert_sql(target: &str, columns: &[String], binds: &[String]) -> String {
    let col_list = columns.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
    let values = binds.join(", ");
    format!("INSERT INTO {} ({col_list}) VALUES ({values})", quote_ident(target))
}

/// A deterministic, session-private staging-table name for `staged_merge`.
/// Oracle identifiers are ≤30 bytes on 12c, so the target is hashed into a short
/// fixed-width suffix rather than embedded. The same target always maps to the
/// same name, so the global temporary table is created once and reused.
fn staging_table_name(target: &str) -> String {
    // FNV-1a (32-bit) over the resolved (folded) name — stable across runs.
    let mut hash: u32 = 0x811c_9dc5;
    for b in fold_ident(target).bytes() {
        hash ^= b as u32;
        hash = hash.wrapping_mul(0x0100_0193);
    }
    format!("LS_STG_{hash:08X}")
}

/// `MERGE INTO "target" t USING "staging" s ON (key match) WHEN MATCHED THEN
/// UPDATE SET <non-key> WHEN NOT MATCHED THEN INSERT (...) VALUES (...)`. A
/// single set-based statement over the already-typed staging rows (no binds,
/// no `TO_*` — those happened on the staging insert), so the optimizer joins by
/// the key index against the real staging cardinality.
fn build_staged_merge_sql(
    target: &str,
    staging: &str,
    columns: &[String],
    merge_key: &[String],
) -> Result<String> {
    // Compare config-supplied merge keys against the incoming column names using
    // Oracle name resolution (a simple `id` resolves to the catalog's `ID`).
    let is_key = |c: &str| merge_key.iter().any(|k| fold_ident(k) == fold_ident(c));
    for k in merge_key {
        if !columns.iter().any(|c| fold_ident(c) == fold_ident(k)) {
            bail!("merge_key column '{k}' is not present in the incoming data");
        }
    }
    let on = merge_key
        .iter()
        .map(|k| format!("t.{0} = s.{0}", quote_ident(k)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let col_list = columns.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
    let src_list = columns.iter().map(|c| format!("s.{}", quote_ident(c))).collect::<Vec<_>>().join(", ");

    let non_key: Vec<&String> = columns.iter().filter(|c| !is_key(c)).collect();
    let matched = if non_key.is_empty() {
        // All columns are keys → nothing to update; only insert new rows.
        String::new()
    } else {
        let set = non_key
            .iter()
            .map(|c| format!("t.{0} = s.{0}", quote_ident(c)))
            .collect::<Vec<_>>()
            .join(", ");
        format!(" WHEN MATCHED THEN UPDATE SET {set}")
    };

    Ok(format!(
        "MERGE INTO {target} t USING {staging} s ON ({on}){matched} \
         WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({src_list})",
        target = quote_ident(target),
        staging = quote_ident(staging),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn config_requires_merge_key_for_staged_merge() {
        let mut p = OracleDestPlugin::new();
        assert!(p
            .configure(serde_json::json!({
                "service_name": "S", "user": "u", "password": "p",
                "target_table": "events", "mode": "staged_merge"
            }))
            .await
            .is_err());
        assert!(p
            .configure(serde_json::json!({
                "service_name": "S", "user": "u", "password": "p",
                "target_table": "events", "mode": "staged_merge", "merge_key": ["id"]
            }))
            .await
            .is_ok());
    }

    #[test]
    fn default_mode_is_atomic() {
        let json = serde_json::json!({
            "service_name": "S", "user": "u", "password": "p", "target_table": "t"
        });
        let cfg: OracleDestConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.mode, CommitMode::Atomic);
    }

    // Plain positional binds (no temporal columns): bind == ":idx".
    fn plain_binds(n: usize) -> Vec<String> {
        (1..=n).map(|i| format!(":{i}")).collect()
    }

    #[test]
    fn insert_sql_positional_binds() {
        let cols = vec!["ID".to_string(), "NAME".to_string()];
        assert_eq!(
            build_insert_sql("EVENTS", &cols, &plain_binds(2)),
            "INSERT INTO \"EVENTS\" (\"ID\", \"NAME\") VALUES (:1, :2)"
        );
    }

    #[test]
    fn insert_sql_wraps_temporal_binds() {
        let cols = vec!["ID".to_string(), "TS".to_string()];
        let binds = vec![":1".to_string(), "TO_TIMESTAMP(:2, 'YYYY-MM-DD HH24:MI:SS.FF6')".to_string()];
        assert_eq!(
            build_insert_sql("EVENTS", &cols, &binds),
            "INSERT INTO \"EVENTS\" (\"ID\", \"TS\") VALUES (:1, TO_TIMESTAMP(:2, 'YYYY-MM-DD HH24:MI:SS.FF6'))"
        );
    }

    #[test]
    fn staged_merge_sql_updates_non_key() {
        let cols = vec!["ID".to_string(), "NAME".to_string(), "TS".to_string()];
        // Config keys in lower case must resolve to the catalog's upper-cased ID.
        let keys = vec!["id".to_string()];
        let sql = build_staged_merge_sql("events", "LS_STG_DEADBEEF", &cols, &keys).unwrap();
        assert!(sql.contains("MERGE INTO \"EVENTS\" t USING \"LS_STG_DEADBEEF\" s"));
        assert!(sql.contains("ON (t.\"ID\" = s.\"ID\")"));
        assert!(sql.contains("WHEN MATCHED THEN UPDATE SET t.\"NAME\" = s.\"NAME\", t.\"TS\" = s.\"TS\""));
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT (\"ID\", \"NAME\", \"TS\") VALUES (s.\"ID\", s.\"NAME\", s.\"TS\")"));
        // No per-row `FROM dual`, no binds — set-based over staging.
        assert!(!sql.contains("FROM dual"));
    }

    #[test]
    fn staged_merge_sql_all_keys_inserts_only() {
        let cols = vec!["A".to_string(), "B".to_string()];
        let keys = vec!["A".to_string(), "B".to_string()];
        let sql = build_staged_merge_sql("T", "S", &cols, &keys).unwrap();
        assert!(!sql.contains("WHEN MATCHED"));
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT"));
    }

    #[test]
    fn staged_merge_sql_rejects_unknown_key() {
        let cols = vec!["A".to_string()];
        assert!(build_staged_merge_sql("T", "S", &cols, &["B".to_string()]).is_err());
    }

    #[test]
    fn staging_name_is_deterministic_and_simple() {
        // Folds the target first → case-insensitive, stable across runs.
        assert_eq!(staging_table_name("events_sink"), staging_table_name("EVENTS_SINK"));
        let name = staging_table_name("events_sink");
        assert!(name.starts_with("LS_STG_"));
        assert_eq!(name.len(), 15);
    }
}
