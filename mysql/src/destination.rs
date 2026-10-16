use anyhow::{bail, Context, Result};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use loadsmith_plugin_sdk::DestinationPlugin;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Value};
use serde::Deserialize;

use crate::conn::ConnectionConfig;

/// Temporary staging table used by `staged_merge`. Session-scoped; created
/// inside the load transaction (CREATE TEMPORARY does not implicitly commit).
const STAGING: &str = "_ls_staging";

#[derive(Debug, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CommitMode {
    /// One transaction for the whole load; INSERT straight into the target,
    /// COMMIT at the end. All-or-nothing, at-least-once.
    #[default]
    Atomic,
    /// INSERT into a staging table, then upsert into the target via
    /// `INSERT ... SELECT ... ON DUPLICATE KEY UPDATE` in one transaction.
    /// Idempotent by the target's unique key ⇒ exactly-once effective.
    StagedMerge,
}

#[derive(Debug, Deserialize)]
struct MysqlDestConfig {
    #[serde(flatten)]
    conn: ConnectionConfig,

    /// Target table.
    target_table: String,
    #[serde(default)]
    mode: CommitMode,
    /// Key columns excluded from the `ON DUPLICATE KEY UPDATE` SET list. The
    /// actual conflict detection keys off the target table's PRIMARY/UNIQUE
    /// index (MySQL semantics), so `merge_key` should match that index.
    #[serde(default)]
    merge_key: Vec<String>,
}

pub struct MysqlDestPlugin {
    config: Option<MysqlDestConfig>,
    conn: Option<Conn>,
    columns: Vec<String>,
    rows_written: u64,
}

impl Default for MysqlDestPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl MysqlDestPlugin {
    pub fn new() -> Self {
        Self { config: None, conn: None, columns: Vec::new(), rows_written: 0 }
    }

    fn cfg(&self) -> &MysqlDestConfig {
        self.config.as_ref().expect("configured")
    }

    /// Where INSERT writes: the staging table for `staged_merge`, else the target.
    fn insert_target(&self) -> &str {
        match self.cfg().mode {
            CommitMode::StagedMerge => STAGING,
            CommitMode::Atomic => self.cfg().target_table.as_str(),
        }
    }
}

#[async_trait]
impl DestinationPlugin for MysqlDestPlugin {
    fn plugin_name(&self) -> &str {
        "loadsmith-destination-mysql"
    }
    fn plugin_version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    fn capabilities(&self) -> Vec<String> {
        vec!["batch_write".into(), "staged_merge".into()]
    }

    async fn configure(&mut self, config: serde_json::Value) -> Result<()> {
        let cfg: MysqlDestConfig =
            serde_json::from_value(config).map_err(|e| anyhow::anyhow!("invalid mysql destination config: {e}"))?;
        if cfg.mode == CommitMode::StagedMerge && cfg.merge_key.is_empty() {
            bail!("mode 'staged_merge' requires a non-empty merge_key");
        }
        self.config = Some(cfg);
        Ok(())
    }

    async fn prepare(&mut self) -> Result<()> {
        let mut conn = crate::conn::connect(&self.cfg().conn).await?;

        // One transaction for the whole load — nothing is durable until
        // finalize() commits.
        conn.query_drop("START TRANSACTION").await.context("START TRANSACTION failed")?;

        if self.cfg().mode == CommitMode::StagedMerge {
            let target = quote_ident(&self.cfg().target_table);
            conn.query_drop(format!(
                "CREATE TEMPORARY TABLE {STAGING} LIKE {target}"
            ))
            .await
            .context("create staging table failed")?;
        }

        self.conn = Some(conn);
        Ok(())
    }

    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        if self.columns.is_empty() {
            self.columns = batch.schema().fields().iter().map(|f| f.name().clone()).collect();
        }
        let nrows = batch.num_rows();
        if nrows == 0 {
            return Ok(());
        }
        self.rows_written += nrows as u64;

        // Build positional params: one Vec<Value> per row.
        let mut params: Vec<Vec<Value>> = Vec::with_capacity(nrows);
        for row in 0..nrows {
            let mut cells: Vec<Value> = Vec::with_capacity(batch.num_columns());
            for col in 0..batch.num_columns() {
                cells.push(crate::types::cell_to_value(batch.column(col), row)?);
            }
            params.push(cells);
        }

        let col_list =
            self.columns.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
        let placeholders = vec!["?"; self.columns.len()].join(", ");
        let sql = format!(
            "INSERT INTO {} ({col_list}) VALUES ({placeholders})",
            self.insert_target()
        );

        let conn = self.conn.as_mut().unwrap();
        conn.exec_batch(sql, params).await.context("INSERT failed")?;
        Ok(())
    }

    async fn finalize(&mut self) -> Result<u64> {
        let mode = self.cfg().mode == CommitMode::StagedMerge && !self.columns.is_empty();
        if mode {
            let sql = build_upsert_sql(
                &self.cfg().target_table,
                STAGING,
                &self.columns,
                &self.cfg().merge_key,
            );
            let conn = self.conn.as_mut().unwrap();
            conn.query_drop(sql).await.context("upsert from staging failed")?;
        }
        let conn = self.conn.as_mut().unwrap();
        conn.query_drop("COMMIT").await.context("COMMIT failed")?;
        Ok(self.rows_written)
    }

    async fn cancel(&mut self) {
        if let Some(conn) = self.conn.as_mut() {
            let _ = conn.query_drop("ROLLBACK").await;
        }
    }
}

/// Quotes a MySQL identifier, escaping embedded backticks.
fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// `INSERT INTO target (cols) SELECT cols FROM staging ON DUPLICATE KEY UPDATE
/// <non-key = VALUES(col)>`. Conflict detection uses the target's unique index;
/// non-key columns are refreshed on conflict.
fn build_upsert_sql(target: &str, staging: &str, columns: &[String], merge_key: &[String]) -> String {
    let col_list = columns.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
    let non_key: Vec<&String> = columns.iter().filter(|c| !merge_key.contains(c)).collect();

    let base = format!(
        "INSERT INTO {} ({col_list}) SELECT {col_list} FROM {}",
        quote_ident(target),
        staging
    );
    if non_key.is_empty() {
        // Nothing to update on conflict — keep the existing row.
        let any = quote_ident(&columns[0]);
        format!("{base} ON DUPLICATE KEY UPDATE {any} = {any}")
    } else {
        let set = non_key
            .iter()
            .map(|c| format!("{0} = VALUES({0})", quote_ident(c)))
            .collect::<Vec<_>>()
            .join(", ");
        format!("{base} ON DUPLICATE KEY UPDATE {set}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn config_requires_merge_key_for_staged_merge() {
        let mut p = MysqlDestPlugin::new();
        let err = p
            .configure(serde_json::json!({
                "dbname": "d", "user": "u", "password": "p",
                "target_table": "events", "mode": "staged_merge"
            }))
            .await;
        assert!(err.is_err());
        assert!(p
            .configure(serde_json::json!({
                "dbname": "d", "user": "u", "password": "p",
                "target_table": "events", "mode": "staged_merge", "merge_key": ["id"]
            }))
            .await
            .is_ok());
    }

    #[test]
    fn default_mode_is_atomic() {
        let json = serde_json::json!({
            "dbname": "d", "user": "u", "password": "p", "target_table": "t"
        });
        let cfg: MysqlDestConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.mode, CommitMode::Atomic);
    }

    #[test]
    fn upsert_sql_updates_non_key() {
        let cols = vec!["id".to_string(), "name".to_string(), "updated_at".to_string()];
        let keys = vec!["id".to_string()];
        let sql = build_upsert_sql("events", "_ls_staging", &cols, &keys);
        assert!(sql.contains("INSERT INTO `events` (`id`, `name`, `updated_at`) SELECT `id`, `name`, `updated_at` FROM _ls_staging"));
        assert!(sql.contains("ON DUPLICATE KEY UPDATE `name` = VALUES(`name`), `updated_at` = VALUES(`updated_at`)"));
        assert!(!sql.contains("`id` = VALUES(`id`)"));
    }

    #[test]
    fn upsert_sql_all_keys_is_noop_update() {
        let cols = vec!["a".to_string(), "b".to_string()];
        let keys = vec!["a".to_string(), "b".to_string()];
        let sql = build_upsert_sql("t", "_ls_staging", &cols, &keys);
        assert!(sql.contains("ON DUPLICATE KEY UPDATE `a` = `a`"));
    }
}
