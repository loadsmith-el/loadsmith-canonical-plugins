use std::collections::HashMap;

use anyhow::{bail, Context, Result};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use loadsmith_plugin_sdk::DestinationPlugin;
use oracledb::{Connection, DbType, ToDbValue};
use serde::Deserialize;

use crate::conn::ConnectionConfig;
use crate::telemetry::{op, DbOp};
use crate::types::{cell_to_bind, fold_ident, quote_ident, typed_expr, Bind};
use crate::vocab::config_enum;

/// Load-order column of the staging table: the last row of a key wins.
const STAGING_SEQ: &str = "_LS_SEQ";

config_enum! {
    /// How the load changes the target table. Every mode is one transaction,
    /// committed at the end of the load.
    #[derive(Default)]
    enum WriteMode for "mode" {
        /// Array-`INSERT` straight into the target. At-least-once with
        /// incremental state.
        #[default]
        Append = "append",
        /// `DELETE` every row of the target, then array-`INSERT`: the table's
        /// contents are replaced by the load's rows, atomically.
        Overwrite = "overwrite",
        /// Bulk-load into a session-private global temporary staging table, then
        /// a single set-based `MERGE` from staging into the target by
        /// `merge_key`. Idempotent by the key ⇒ exactly-once effective. The
        /// staging insert is the same fast array-DML as `append`, and the
        /// optimizer plans the one MERGE against the real staging cardinality.
        Merge = "merge",
    }
    retired {
        "atomic" => "append",
        "staged_merge" => "merge",
        "upsert" => "merge",
        "replace" => "overwrite",
        "truncate" => "overwrite",
    }
}

#[derive(Debug, Deserialize)]
struct OracleDestConfig {
    #[serde(flatten)]
    conn: ConnectionConfig,

    /// Target table.
    target_table: String,
    #[serde(default)]
    mode: WriteMode,
    /// `merge`: the key columns of the `MERGE … ON (…)` match (required).
    #[serde(default)]
    merge_key: Vec<String>,
    /// `merge`: the columns updated when a key matches. Default: every
    /// incoming column that isn't a key. `[]` inserts new keys only.
    #[serde(default)]
    update_columns: Option<Vec<String>>,
}

impl OracleDestConfig {
    fn validate(&self) -> Result<()> {
        if self.mode == WriteMode::Merge {
            if self.merge_key.is_empty() {
                bail!("`mode: merge` requires `merge_key` (the key columns to match on)");
            }
            if let Some(update) = &self.update_columns {
                if let Some(k) =
                    update.iter().find(|c| self.merge_key.iter().any(|k| fold_ident(k) == fold_ident(c)))
                {
                    bail!("`update_columns` lists `{k}`, a `merge_key` column; key columns are never updated");
                }
            }
        } else {
            let mode = self.mode.as_str();
            if !self.merge_key.is_empty() {
                bail!("`merge_key` only applies to `mode: merge` (mode is `{mode}`)");
            }
            if self.update_columns.is_some() {
                bail!("`update_columns` only applies to `mode: merge` (mode is `{mode}`)");
            }
        }
        Ok(())
    }
}

pub struct OracleDestPlugin {
    config: Option<OracleDestConfig>,
    conn: Option<Connection>,
    columns: Vec<String>,
    /// Target column → Oracle type (upper-cased names), from describing the
    /// target table in `prepare`. Drives the explicit `TO_*` bind wrapping.
    target_types: HashMap<String, &'static DbType>,
    /// Per-batch array-DML `INSERT` SQL — into the target (`append`,
    /// `overwrite`) or into the staging table (`merge`). Built once the column
    /// set is known.
    sql: Option<String>,
    /// `merge` only: the session-private global temporary staging table,
    /// created in `prepare`.
    staging_table: Option<String>,
    /// `merge` only: the single set-based `MERGE` run at `finalize`. Built
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

    fn cfg(&self) -> Result<&OracleDestConfig> {
        self.config.as_ref().context("oracle destination: not configured")
    }

    /// The target column type of an incoming column, when the target has it.
    fn target_type(&self, col: &str) -> Option<&'static DbType> {
        self.target_types.get(&fold_ident(col)).copied()
    }

    /// The explicit bind expression for one incoming column at placeholder `:idx`
    /// — `TO_DATE(:idx, …)` / `TO_TIMESTAMP*(:idx, …)` for temporal target
    /// columns, plain `:idx` otherwise (unknown columns pass through).
    fn bind_expr(&self, col: &str, idx: usize) -> String {
        let placeholder = format!(":{idx}");
        match self.target_type(col) {
            Some(t) => typed_expr(&placeholder, t),
            None => placeholder,
        }
    }

    /// Builds the per-batch INSERT (and, for `merge`, the final MERGE) once
    /// the incoming column set is known.
    fn build_statements(&mut self) -> Result<()> {
        let binds: Vec<String> =
            self.columns.iter().enumerate().map(|(i, c)| self.bind_expr(c, i + 1)).collect();
        let cfg = self.cfg()?;
        let target = cfg.target_table.clone();
        let (sql, merge_sql) = match cfg.mode {
            WriteMode::Append | WriteMode::Overwrite => (build_insert_sql(&target, &self.columns, &binds, None), None),
            WriteMode::Merge => {
                let staging =
                    self.staging_table.clone().context("oracle destination: staging table not prepared")?;
                // Build (and validate the merge_key against the incoming
                // columns) the one set-based MERGE up front; run it at finalize.
                let merge = build_staged_merge_sql(
                    &target,
                    &staging,
                    &self.columns,
                    &cfg.merge_key,
                    cfg.update_columns.as_deref(),
                )?;
                // Each batch array-inserts into staging (plus the load-order
                // bind) — the same fast path as `append`.
                let seq_bind = format!(":{}", self.columns.len() + 1);
                (build_insert_sql(&staging, &self.columns, &binds, Some(&seq_bind)), Some(merge))
            }
        };
        self.sql = Some(sql);
        self.merge_sql = merge_sql;
        Ok(())
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

    async fn configure(&mut self, config: loadsmith_plugin_sdk::PluginConfig) -> Result<()> {
        // Unknown keys (at any depth, flattened blocks included) follow the
        // operator's policy: an error by default, a warning with
        // `--ignore-unknown-keys`.
        let cfg: OracleDestConfig = config.parse()?;
        cfg.validate()?;
        self.config = Some(cfg);
        Ok(())
    }

    async fn prepare(&mut self) -> Result<()> {
        // Nothing is durable until finalize() commits (Oracle holds the
        // transaction open implicitly).
        let cfg = self.cfg()?;
        let target = quote_ident(&cfg.target_table);
        let describe = format!("SELECT * FROM {target} WHERE 1 = 0");

        let ns = crate::conn::namespace(&cfg.conn);
        let (conn, target_types) = tokio::task::block_in_place(|| -> Result<_> {
            let conn = crate::conn::connect(&cfg.conn)?;
            let target_types = op("SELECT", ns, Some(&cfg.target_table), Some(&describe), || {
                let rs = conn
                    .query(&describe, &[])
                    .map_err(|e| anyhow::anyhow!("describing target table failed: {e}"))?;
                Ok(rs.columns().iter().map(|c| (c.name().to_uppercase(), c.db_type())).collect::<HashMap<_, _>>())
            })?;
            Ok((conn, target_types))
        })?;

        let mut staging_table = None;
        match cfg.mode {
            WriteMode::Append => {}
            // DELETE, not TRUNCATE: TRUNCATE is DDL and commits implicitly, so
            // it could not roll back with a failed load.
            WriteMode::Overwrite => {
                let delete = format!("DELETE FROM {target}");
                let phase = DbOp::new("DELETE", ns, Some(&cfg.target_table), Some(&delete));
                let span = phase.span().clone();
                let rows = tokio::task::block_in_place(|| {
                    phase.run(|| conn.execute(&delete, &[]).map_err(|e| anyhow::anyhow!("{delete} failed: {e}")))
                })?;
                span.record("loadsmith.db.affected_rows", rows.rows_affected());
            }
            // `merge` stages into a session-private global temporary table with
            // the target's columns plus the load-order column. Created here (DDL
            // auto-commits) before any row is written; `ON COMMIT DELETE ROWS`
            // clears it at finalize's commit. Create-if-not-exists (ORA-00955)
            // so reruns reuse it.
            WriteMode::Merge => {
                let staging = staging_table_name(&cfg.target_table);
                let create = format!(
                    "DECLARE\n  e_exists EXCEPTION;\n  PRAGMA EXCEPTION_INIT(e_exists, -955);\nBEGIN\n  \
                     EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE {stg} ON COMMIT DELETE ROWS \
                     AS SELECT t.*, CAST(NULL AS NUMBER(19)) AS {seq} FROM {target} t WHERE 1 = 0';\n\
                     EXCEPTION WHEN e_exists THEN NULL;\nEND;",
                    stg = quote_ident(&staging),
                    seq = quote_ident(STAGING_SEQ),
                );
                tokio::task::block_in_place(|| {
                    op("CREATE TABLE", ns, Some(&staging), Some(&create), || {
                        conn.execute(&create, &[]).map_err(|e| anyhow::anyhow!("creating staging table failed: {e}"))
                    })
                })?;
                staging_table = Some(staging);
            }
        }

        self.staging_table = staging_table;
        self.target_types = target_types;
        self.conn = Some(conn);
        Ok(())
    }

    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        if self.columns.is_empty() {
            self.columns = batch.schema().fields().iter().map(|f| f.name().clone()).collect();
            self.build_statements()?;
        }
        let nrows = batch.num_rows();
        if nrows == 0 {
            return Ok(());
        }
        let staged = self.staging_table.is_some();
        let targets: Vec<Option<&'static DbType>> = self.columns.iter().map(|c| self.target_type(c)).collect();
        // A UUID goes as its text, unless the column holds raw bytes (RAW(16)).
        let uuid_text: Vec<bool> = batch
            .schema_ref()
            .fields()
            .iter()
            .zip(&targets)
            .map(|(f, t)| crate::types::is_uuid(f) && !t.is_some_and(|t| t == oracledb::DB_TYPE_RAW))
            .collect();

        // Materialise all binds (owned) up front, then run the array DML.
        let mut rows: Vec<Vec<Bind>> = Vec::with_capacity(nrows);
        for row in 0..nrows {
            let mut cells: Vec<Bind> = Vec::with_capacity(batch.num_columns() + 1);
            for (col, &as_text) in uuid_text.iter().enumerate() {
                cells.push(if as_text {
                    Bind::Text(crate::types::uuid_text(batch.column(col), row)?)
                } else {
                    cell_to_bind(batch.column(col), row, targets.get(col).copied().flatten())?
                });
            }
            if staged {
                cells.push(Bind::Text(Some((self.rows_written + row as u64).to_string())));
            }
            rows.push(cells);
        }

        let sql = self.sql.clone().context("oracle destination: no INSERT statement")?;
        let conn = self.conn.as_ref().context("oracle destination: not prepared")?;
        // One span per array insert: the statement with its binds, never the
        // bound rows.
        let cfg = self.cfg()?;
        let into = self.staging_table.as_deref().unwrap_or(&cfg.target_table);
        let insert = DbOp::new("INSERT", crate::conn::namespace(&cfg.conn), Some(into), Some(&sql));
        insert.span().record("db.operation.batch.size", nrows as u64);
        insert.span().record("loadsmith.db.affected_rows", nrows as u64);
        tokio::task::block_in_place(|| -> Result<()> {
            insert.run(|| {
                // Array DML: one execute_batch over a 2-D slice of `&dyn ToDbValue`.
                let params: Vec<Vec<&dyn ToDbValue>> =
                    rows.iter().map(|row| row.iter().map(|b| b.as_sql()).collect()).collect();
                let slices: Vec<&[&dyn ToDbValue]> = params.iter().map(|r| r.as_slice()).collect();
                conn.execute_batch(&sql, slices.as_slice())
                    .map_err(|e| anyhow::anyhow!("array DML failed: {e}"))?;
                Ok(())
            })
        })?;

        self.rows_written += nrows as u64;
        Ok(())
    }

    async fn finalize(&mut self) -> Result<u64> {
        let conn = self.conn.as_ref().context("oracle destination: not prepared")?;
        let cfg = self.cfg()?;
        let ns = crate::conn::namespace(&cfg.conn);
        // merge: a single set-based MERGE from the staging table into the
        // target, in the same transaction as the staging inserts, then commit.
        if let Some(merge_sql) = self.merge_sql.clone() {
            let phase = DbOp::new("MERGE", ns, Some(&cfg.target_table), Some(&merge_sql));
            let span = phase.span().clone();
            let rows = tokio::task::block_in_place(|| {
                phase.run(|| conn.execute(&merge_sql, &[]).map_err(|e| anyhow::anyhow!("staged MERGE failed: {e}")))
            })?;
            span.record("loadsmith.db.affected_rows", rows.rows_affected());
        }
        tokio::task::block_in_place(|| {
            op("COMMIT", ns, None, None, || conn.commit().map_err(|e| anyhow::anyhow!("COMMIT failed: {e}")))
        })?;
        Ok(self.rows_written)
    }

    async fn cancel(&mut self) {
        if let Some(conn) = self.conn.as_ref() {
            let _ = tokio::task::block_in_place(|| conn.rollback());
        }
    }
}

/// `INSERT INTO "target" ("c1", …[, "_LS_SEQ"]) VALUES (<bind1>, …[, <seq>])`,
/// where each `<bind>` is the column's (possibly `TO_*`-wrapped) bind
/// expression and `seq_bind` the staging table's load-order bind.
fn build_insert_sql(target: &str, columns: &[String], binds: &[String], seq_bind: Option<&str>) -> String {
    let mut col_list = columns.iter().map(|c| quote_ident(c)).collect::<Vec<_>>();
    let mut values = binds.to_vec();
    if let Some(seq) = seq_bind {
        col_list.push(quote_ident(STAGING_SEQ));
        values.push(seq.to_string());
    }
    format!("INSERT INTO {} ({}) VALUES ({})", quote_ident(target), col_list.join(", "), values.join(", "))
}

/// A deterministic, session-private staging-table name for `merge`. Oracle
/// identifiers are ≤30 bytes on 12c, so the target is hashed into a short
/// fixed-width suffix rather than embedded. The same target always maps to the
/// same name, so the global temporary table is created once and reused.
/// (`LS_MRG_`: the `LS_STG_` tables of earlier versions lack the load-order
/// column.)
fn staging_table_name(target: &str) -> String {
    // FNV-1a (32-bit) over the resolved (folded) name — stable across runs.
    let mut hash: u32 = 0x811c_9dc5;
    for b in fold_ident(target).bytes() {
        hash ^= b as u32;
        hash = hash.wrapping_mul(0x0100_0193);
    }
    format!("LS_MRG_{hash:08X}")
}

/// `MERGE INTO "target" t USING (<latest row per key in staging>) s ON (key
/// match) WHEN MATCHED THEN UPDATE SET <update set> WHEN NOT MATCHED THEN
/// INSERT (...) VALUES (...)`. A single set-based statement over the
/// already-typed staging rows (no binds, no `TO_*` — those happened on the
/// staging insert). Within one load the **last row of each key wins**: the
/// staging rows are first reduced to the latest per key (load order), since
/// a MERGE cannot apply two source rows to one target row (ORA-30926).
fn build_staged_merge_sql(
    target: &str,
    staging: &str,
    columns: &[String],
    merge_key: &[String],
    update_columns: Option<&[String]>,
) -> Result<String> {
    // Compare config-supplied names against the incoming column names using
    // Oracle name resolution (a simple `id` resolves to the catalog's `ID`).
    let present = |k: &str| columns.iter().any(|c| fold_ident(c) == fold_ident(k));
    for k in merge_key {
        if !present(k) {
            bail!("merge_key column '{k}' is not present in the incoming data");
        }
    }
    let is_key = |c: &str| merge_key.iter().any(|k| fold_ident(k) == fold_ident(c));
    let on = merge_key
        .iter()
        .map(|k| format!("t.{0} = s.{0}", quote_ident(k)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let keys = merge_key.iter().map(|k| quote_ident(k)).collect::<Vec<_>>().join(", ");
    let col_list = columns.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
    let src_list = columns.iter().map(|c| format!("s.{}", quote_ident(c))).collect::<Vec<_>>().join(", ");
    let latest = format!(
        "SELECT {col_list} FROM (SELECT {col_list}, ROW_NUMBER() OVER (PARTITION BY {keys} \
         ORDER BY {seq} DESC) AS \"_LS_RN\" FROM {staging}) WHERE \"_LS_RN\" = 1",
        seq = quote_ident(STAGING_SEQ),
        staging = quote_ident(staging),
    );

    let set: Vec<String> = match update_columns {
        None => columns.iter().filter(|c| !is_key(c)).cloned().collect(),
        Some(list) => {
            for c in list {
                if !present(c) {
                    bail!("update_columns column '{c}' is not present in the incoming data");
                }
            }
            list.to_vec()
        }
    };
    let matched = if set.is_empty() {
        // Nothing to update; only insert new keys.
        String::new()
    } else {
        let set = set.iter().map(|c| format!("t.{0} = s.{0}", quote_ident(c))).collect::<Vec<_>>().join(", ");
        format!(" WHEN MATCHED THEN UPDATE SET {set}")
    };

    Ok(format!(
        "MERGE INTO {target} t USING ({latest}) s ON ({on}){matched} \
         WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({src_list})",
        target = quote_ident(target),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use loadsmith_plugin_sdk::{PluginConfig, UnknownKeys};

    async fn configure(extra: serde_json::Value) -> Result<()> {
        let mut v = serde_json::json!({ "service_name": "S", "user": "u", "password": "p", "target_table": "events" });
        v.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
        OracleDestPlugin::new().configure(PluginConfig::new(v, UnknownKeys::Error)).await
    }

    fn strings(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[tokio::test]
    async fn merge_requires_a_merge_key_and_merge_options_need_merge() {
        assert!(configure(serde_json::json!({ "mode": "merge" })).await.is_err());
        assert!(configure(serde_json::json!({ "mode": "merge", "merge_key": ["id"] })).await.is_ok());
        let e = configure(serde_json::json!({ "mode": "overwrite", "merge_key": ["id"] }))
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("only applies to `mode: merge`"), "{e}");
        let e = configure(serde_json::json!({ "mode": "merge", "merge_key": ["id"], "update_columns": ["ID"] }))
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("never updated"), "{e}");
    }

    #[tokio::test]
    async fn retired_modes_name_the_new_value() {
        let e = configure(serde_json::json!({ "mode": "atomic" })).await.unwrap_err().to_string();
        assert!(e.contains("did you mean `append`?"), "{e}");
    }

    #[test]
    fn default_mode_is_append() {
        let json = serde_json::json!({
            "service_name": "S", "user": "u", "password": "p", "target_table": "t"
        });
        let cfg: OracleDestConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.mode, WriteMode::Append);
    }

    // Plain positional binds (no temporal columns): bind == ":idx".
    fn plain_binds(n: usize) -> Vec<String> {
        (1..=n).map(|i| format!(":{i}")).collect()
    }

    #[test]
    fn insert_sql_positional_binds() {
        let cols = strings(&["ID", "NAME"]);
        assert_eq!(
            build_insert_sql("EVENTS", &cols, &plain_binds(2), None),
            "INSERT INTO \"EVENTS\" (\"ID\", \"NAME\") VALUES (:1, :2)"
        );
        assert_eq!(
            build_insert_sql("LS_MRG_1", &cols, &plain_binds(2), Some(":3")),
            "INSERT INTO \"LS_MRG_1\" (\"ID\", \"NAME\", \"_LS_SEQ\") VALUES (:1, :2, :3)"
        );
    }

    #[test]
    fn insert_sql_wraps_temporal_binds() {
        let cols = strings(&["ID", "TS"]);
        let binds = vec![":1".to_string(), "TO_TIMESTAMP(:2, 'YYYY-MM-DD HH24:MI:SS.FF')".to_string()];
        assert_eq!(
            build_insert_sql("EVENTS", &cols, &binds, None),
            "INSERT INTO \"EVENTS\" (\"ID\", \"TS\") VALUES (:1, TO_TIMESTAMP(:2, 'YYYY-MM-DD HH24:MI:SS.FF'))"
        );
    }

    #[test]
    fn staged_merge_sql_dedupes_and_updates_non_key() {
        let cols = strings(&["ID", "NAME", "TS"]);
        // Config keys in lower case must resolve to the catalog's upper-cased ID.
        let sql = build_staged_merge_sql("events", "LS_MRG_DEADBEEF", &cols, &strings(&["id"]), None).unwrap();
        assert!(sql.contains(
            "MERGE INTO \"EVENTS\" t USING (SELECT \"ID\", \"NAME\", \"TS\" FROM (SELECT \"ID\", \"NAME\", \"TS\", \
             ROW_NUMBER() OVER (PARTITION BY \"ID\" ORDER BY \"_LS_SEQ\" DESC) AS \"_LS_RN\" FROM \"LS_MRG_DEADBEEF\") \
             WHERE \"_LS_RN\" = 1) s"
        ), "{sql}");
        assert!(sql.contains("ON (t.\"ID\" = s.\"ID\")"));
        assert!(sql.contains("WHEN MATCHED THEN UPDATE SET t.\"NAME\" = s.\"NAME\", t.\"TS\" = s.\"TS\""));
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT (\"ID\", \"NAME\", \"TS\") VALUES (s.\"ID\", s.\"NAME\", s.\"TS\")"));
    }

    #[test]
    fn staged_merge_sql_update_columns_and_insert_only() {
        let cols = strings(&["ID", "NAME", "TS"]);
        let sql = build_staged_merge_sql("T", "S", &cols, &strings(&["ID"]), Some(&strings(&["name"]))).unwrap();
        assert!(sql.contains("WHEN MATCHED THEN UPDATE SET t.\"NAME\" = s.\"NAME\" WHEN NOT MATCHED"), "{sql}");
        let sql = build_staged_merge_sql("T", "S", &cols, &strings(&["ID"]), Some(&[])).unwrap();
        assert!(!sql.contains("WHEN MATCHED"));
        assert!(build_staged_merge_sql("T", "S", &cols, &strings(&["ID"]), Some(&strings(&["X"]))).is_err());
    }

    #[test]
    fn staged_merge_sql_all_keys_inserts_only() {
        let cols = strings(&["A", "B"]);
        let sql = build_staged_merge_sql("T", "S", &cols, &cols, None).unwrap();
        assert!(!sql.contains("WHEN MATCHED"));
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT"));
    }

    #[test]
    fn staged_merge_sql_rejects_unknown_key() {
        assert!(build_staged_merge_sql("T", "S", &strings(&["A"]), &strings(&["B"]), None).is_err());
    }

    #[test]
    fn staging_name_is_deterministic_and_simple() {
        // Folds the target first → case-insensitive, stable across runs.
        assert_eq!(staging_table_name("events_sink"), staging_table_name("EVENTS_SINK"));
        let name = staging_table_name("events_sink");
        assert!(name.starts_with("LS_MRG_"));
        assert_eq!(name.len(), 15);
    }
}
