use anyhow::{bail, Context, Result};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use loadsmith_plugin_sdk::DestinationPlugin;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Value};
use serde::Deserialize;

use crate::conn::ConnectionConfig;
use crate::telemetry::{op, DbOp};
use crate::vocab::config_enum;

/// Staging table of `mode: merge`. Session-scoped (`TEMPORARY`).
const STAGING: &str = "_ls_staging";
/// Load-order column of the staging table: the last row of a key wins.
const STAGING_SEQ: &str = "_ls_seq";

config_enum! {
    /// How the load changes the target table. Every mode is one transaction,
    /// committed at the end of the load.
    #[derive(Default)]
    enum WriteMode for "mode" {
        /// `INSERT` straight into the target. At-least-once with incremental state.
        #[default]
        Append = "append",
        /// `DELETE` every row of the target, then `INSERT`: the table's contents
        /// are replaced by the load's rows, atomically.
        Overwrite = "overwrite",
        /// `INSERT` into a staging table, then upsert into the target.
        /// Idempotent ⇒ exactly-once effective.
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

config_enum! {
    /// How `mode: merge` finds the target row of an incoming row.
    #[derive(Default)]
    enum MergeStrategy for "merge_strategy" {
        /// By `merge_key`: an `UPDATE … JOIN` of the matching rows, then an
        /// `INSERT` of the rest. Needs no index (one on the key makes it fast).
        #[default]
        Keyed = "keyed",
        /// MySQL's own `INSERT … ON DUPLICATE KEY UPDATE`: a row that collides
        /// with the target on **any** PRIMARY/UNIQUE index updates that row.
        OnDuplicateKey = "on-duplicate-key",
    }
    retired {}
}

#[derive(Debug, Deserialize)]
struct MysqlDestConfig {
    #[serde(flatten)]
    conn: ConnectionConfig,

    /// Target table: `table` or `database.table`.
    target_table: String,
    #[serde(default)]
    mode: WriteMode,
    /// `merge`: the key columns. Required with `merge_strategy: keyed`; with
    /// `on-duplicate-key` (where the table's unique indexes decide) they are
    /// only left out of the update.
    #[serde(default)]
    merge_key: Vec<String>,
    /// `merge`: how the target row is found.
    #[serde(default)]
    merge_strategy: Option<MergeStrategy>,
    /// `merge`: the columns updated when a row matches. Default: every
    /// incoming column that isn't a key. `[]` inserts new rows only.
    #[serde(default)]
    update_columns: Option<Vec<String>>,
}

impl MysqlDestConfig {
    fn strategy(&self) -> MergeStrategy {
        self.merge_strategy.unwrap_or_default()
    }

    fn validate(&self) -> Result<()> {
        if self.mode == WriteMode::Merge {
            if self.strategy() == MergeStrategy::Keyed && self.merge_key.is_empty() {
                bail!(
                    "`mode: merge` requires `merge_key` (the key columns to match on), or \
                     `merge_strategy: on-duplicate-key` to match on the table's unique indexes"
                );
            }
            if let Some(update) = &self.update_columns {
                if let Some(k) = update.iter().find(|c| self.merge_key.contains(c)) {
                    bail!("`update_columns` lists `{k}`, a `merge_key` column; key columns are never updated");
                }
            }
        } else {
            let mode = self.mode.as_str();
            if !self.merge_key.is_empty() {
                bail!("`merge_key` only applies to `mode: merge` (mode is `{mode}`)");
            }
            if self.merge_strategy.is_some() {
                bail!("`merge_strategy` only applies to `mode: merge` (mode is `{mode}`)");
            }
            if self.update_columns.is_some() {
                bail!("`update_columns` only applies to `mode: merge` (mode is `{mode}`)");
            }
        }
        quote_table(&self.target_table)?;
        Ok(())
    }
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

    fn cfg(&self) -> Result<&MysqlDestConfig> {
        self.config.as_ref().context("mysql destination: not configured")
    }

    /// Where INSERT writes: the staging table for `merge`, else the target.
    fn insert_target(&self) -> Result<String> {
        let cfg = self.cfg()?;
        match cfg.mode {
            WriteMode::Merge => Ok(STAGING.to_string()),
            WriteMode::Append | WriteMode::Overwrite => quote_table(&cfg.target_table),
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

    async fn configure(&mut self, config: loadsmith_plugin_sdk::PluginConfig) -> Result<()> {
        // Unknown keys (at any depth, flattened blocks included) follow the
        // operator's policy: an error by default, a warning with
        // `--ignore-unknown-keys`.
        let cfg: MysqlDestConfig = config.parse()?;
        cfg.validate()?;
        self.config = Some(cfg);
        Ok(())
    }

    async fn prepare(&mut self) -> Result<()> {
        let cfg = self.cfg()?;
        let target = quote_table(&cfg.target_table)?;
        let mut conn = crate::conn::connect(&cfg.conn).await?;
        let ns = cfg.conn.dbname.as_str();

        if cfg.mode == WriteMode::Merge {
            // Created before the transaction: CREATE TEMPORARY TABLE … SELECT
            // is not allowed inside one under GTID consistency. Only the
            // column types are copied (no indexes), so duplicate keys within
            // the load reach the merge, where the last row of a key wins.
            let sql = format!(
                "CREATE TEMPORARY TABLE {STAGING} ({seq} BIGINT AUTO_INCREMENT PRIMARY KEY) \
                 SELECT * FROM {target} LIMIT 0",
                seq = quote_ident(STAGING_SEQ),
            );
            op("CREATE TABLE", ns, Some(STAGING), Some(&sql), async {
                conn.query_drop(&sql).await.context("create staging table failed")
            })
            .await?;
        }

        // One transaction for the whole load — nothing is durable until
        // finalize() commits.
        op("START TRANSACTION", ns, None, Some("START TRANSACTION"), async {
            conn.query_drop("START TRANSACTION").await.context("START TRANSACTION failed")
        })
        .await?;

        if cfg.mode == WriteMode::Overwrite {
            // DELETE, not TRUNCATE: TRUNCATE commits implicitly in MySQL, so it
            // could not roll back with a failed load.
            let sql = format!("DELETE FROM {target}");
            let delete = DbOp::new("DELETE", ns, Some(&cfg.target_table), Some(&sql));
            let span = delete.span().clone();
            let rows = delete
                .run(async {
                    conn.query_drop(&sql).await.with_context(|| format!("DELETE FROM {target} failed"))?;
                    Ok(conn.affected_rows())
                })
                .await?;
            span.record("loadsmith.db.affected_rows", rows);
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

        let col_list = self.columns.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
        let placeholders = vec!["?"; self.columns.len()].join(", ");
        let target = self.insert_target()?;
        let sql = format!("INSERT INTO {target} ({col_list}) VALUES ({placeholders})");

        // One span per batch insert: the statement with its `?` placeholders,
        // never the bound rows.
        let cfg = self.config.as_ref().context("mysql destination: not configured")?;
        let collection = if cfg.mode == WriteMode::Merge { STAGING } else { cfg.target_table.as_str() };
        let insert = DbOp::new("INSERT", &cfg.conn.dbname, Some(collection), Some(&sql));
        insert.span().record("db.operation.batch.size", nrows as u64);
        insert.span().record("loadsmith.db.affected_rows", nrows as u64);
        let conn = self.conn.as_mut().context("mysql destination: not prepared")?;
        insert.run(async { conn.exec_batch(sql.as_str(), params).await.context("INSERT failed") }).await?;
        Ok(())
    }

    async fn finalize(&mut self) -> Result<u64> {
        let cfg = self.cfg()?;
        let statements = if cfg.mode == WriteMode::Merge && !self.columns.is_empty() {
            let target = quote_table(&cfg.target_table)?;
            let update = cfg.update_columns.as_deref();
            match cfg.strategy() {
                MergeStrategy::Keyed => build_keyed_merge_sql(&target, &self.columns, &cfg.merge_key, update)?,
                MergeStrategy::OnDuplicateKey => {
                    vec![build_upsert_sql(&target, &self.columns, &cfg.merge_key, update)?]
                }
            }
        } else {
            Vec::new()
        };
        let cfg = self.config.as_ref().context("mysql destination: not configured")?;
        let ns = cfg.conn.dbname.as_str();
        let conn = self.conn.as_mut().context("mysql destination: not prepared")?;
        for sql in statements {
            // Each merge phase (UPDATE … JOIN, INSERT … SELECT) is one span.
            let operation = if sql.starts_with("UPDATE") { "UPDATE" } else { "INSERT" };
            let phase = DbOp::new(operation, ns, Some(&cfg.target_table), Some(&sql));
            let span = phase.span().clone();
            let rows = phase
                .run(async {
                    conn.query_drop(&sql).await.context("merge from staging failed")?;
                    Ok(conn.affected_rows())
                })
                .await?;
            span.record("loadsmith.db.affected_rows", rows);
        }
        op("COMMIT", ns, None, Some("COMMIT"), async { conn.query_drop("COMMIT").await.context("COMMIT failed") })
            .await?;
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

/// Quotes `target_table`: `table`, or `database.table` (each part quoted, so
/// a part is taken literally — case, spaces and reserved words included).
pub(crate) fn quote_table(name: &str) -> Result<String> {
    let parts: Vec<&str> = name.split('.').collect();
    if parts.len() > 2 || parts.iter().any(|p| p.is_empty()) {
        bail!("`target_table` must be `table` or `database.table`, got `{name}`");
    }
    Ok(parts.iter().map(|p| quote_ident(p)).collect::<Vec<_>>().join("."))
}

/// The columns a match updates: `update_columns` when given, else every
/// incoming column that isn't a key. Each must be an incoming column.
fn update_set<'a>(
    columns: &'a [String],
    merge_key: &[String],
    update_columns: Option<&'a [String]>,
) -> Result<Vec<&'a String>> {
    match update_columns {
        None => Ok(columns.iter().filter(|c| !merge_key.contains(c)).collect()),
        Some(list) => {
            for c in list {
                if !columns.contains(c) {
                    bail!("update_columns column '{c}' is not present in the data");
                }
            }
            Ok(list.iter().collect())
        }
    }
}

/// The staging rows reduced to the last one (load order) per key, as a
/// derived table: `staging` is referenced once, as MySQL requires of a
/// temporary table within one statement.
fn latest_per_key(col_list: &str, merge_key: &[String]) -> String {
    let keys = merge_key.iter().map(|k| quote_ident(k)).collect::<Vec<_>>().join(", ");
    format!(
        "(SELECT {col_list} FROM (SELECT {col_list}, ROW_NUMBER() OVER (PARTITION BY {keys} \
         ORDER BY {seq} DESC) AS `_ls_rn` FROM {STAGING}) `_ls_d` WHERE `_ls_rn` = 1)",
        seq = quote_ident(STAGING_SEQ),
    )
}

/// `merge_strategy: keyed`: update the target rows whose `merge_key` matches,
/// then insert the incoming rows whose key is not in the target. Within one
/// load the last row of each key wins.
fn build_keyed_merge_sql(
    target: &str,
    columns: &[String],
    merge_key: &[String],
    update_columns: Option<&[String]>,
) -> Result<Vec<String>> {
    for k in merge_key {
        if !columns.iter().any(|c| c == k) {
            bail!("merge_key column '{k}' is not present in the data");
        }
    }
    let col_list = columns.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
    let latest = latest_per_key(&col_list, merge_key);
    let on = merge_key
        .iter()
        .map(|k| format!("t.{0} = s.{0}", quote_ident(k)))
        .collect::<Vec<_>>()
        .join(" AND ");

    let mut out = Vec::new();
    let set = update_set(columns, merge_key, update_columns)?;
    if !set.is_empty() {
        let set = set.iter().map(|c| format!("t.{0} = s.{0}", quote_ident(c))).collect::<Vec<_>>().join(", ");
        out.push(format!("UPDATE {target} t JOIN {latest} s ON {on} SET {set}"));
    }
    let s_cols = columns.iter().map(|c| format!("s.{}", quote_ident(c))).collect::<Vec<_>>().join(", ");
    out.push(format!(
        "INSERT INTO {target} ({col_list}) SELECT {s_cols} FROM {latest} s \
         WHERE NOT EXISTS (SELECT 1 FROM {target} t WHERE {on})"
    ));
    Ok(out)
}

/// `merge_strategy: on-duplicate-key`: `INSERT INTO target (cols) SELECT cols
/// FROM staging ORDER BY load order ON DUPLICATE KEY UPDATE <set>`. Conflicts
/// are found by the target's PRIMARY/UNIQUE indexes; rows are applied in load
/// order, so the last row of a key wins.
fn build_upsert_sql(
    target: &str,
    columns: &[String],
    merge_key: &[String],
    update_columns: Option<&[String]>,
) -> Result<String> {
    let first = columns.first().context("no columns to merge")?;
    let col_list = columns.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
    let base = format!(
        "INSERT INTO {target} ({col_list}) SELECT {col_list} FROM {STAGING} ORDER BY {seq}",
        seq = quote_ident(STAGING_SEQ),
    );
    let set = update_set(columns, merge_key, update_columns)?;
    Ok(if set.is_empty() {
        // Nothing to update on conflict — keep the existing row.
        let any = quote_ident(first);
        format!("{base} ON DUPLICATE KEY UPDATE {any} = {any}")
    } else {
        let set = set.iter().map(|c| format!("{0} = VALUES({0})", quote_ident(c))).collect::<Vec<_>>().join(", ");
        format!("{base} ON DUPLICATE KEY UPDATE {set}")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use loadsmith_plugin_sdk::{PluginConfig, UnknownKeys};

    async fn configure(extra: serde_json::Value) -> Result<()> {
        let mut v = serde_json::json!({ "dbname": "d", "user": "u", "password": "p", "target_table": "events" });
        v.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
        MysqlDestPlugin::new().configure(PluginConfig::new(v, UnknownKeys::Error)).await
    }

    fn strings(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[tokio::test]
    async fn merge_needs_a_key_unless_on_duplicate_key() {
        let e = configure(serde_json::json!({ "mode": "merge" })).await.unwrap_err().to_string();
        assert!(e.contains("requires `merge_key`"), "{e}");
        assert!(configure(serde_json::json!({ "mode": "merge", "merge_key": ["id"] })).await.is_ok());
        assert!(configure(serde_json::json!({ "mode": "merge", "merge_strategy": "on-duplicate-key" }))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn merge_options_are_rejected_outside_merge() {
        for extra in [
            serde_json::json!({ "merge_key": ["id"] }),
            serde_json::json!({ "mode": "overwrite", "merge_strategy": "keyed" }),
            serde_json::json!({ "mode": "append", "update_columns": [] }),
        ] {
            let e = configure(extra).await.unwrap_err().to_string();
            assert!(e.contains("only applies to `mode: merge`"), "{e}");
        }
    }

    #[tokio::test]
    async fn retired_and_misspelled_values_name_the_new_value() {
        let e = configure(serde_json::json!({ "mode": "staged_merge" })).await.unwrap_err().to_string();
        assert!(e.contains("did you mean `merge`?"), "{e}");
        let e = configure(serde_json::json!({ "mode": "merge", "merge_strategy": "on_duplicate_key" }))
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("did you mean `on-duplicate-key`?"), "{e}");
    }

    #[test]
    fn default_mode_is_append() {
        let json = serde_json::json!({ "dbname": "d", "user": "u", "password": "p", "target_table": "t" });
        let cfg: MysqlDestConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.mode, WriteMode::Append);
    }

    #[test]
    fn target_table_quotes_each_part() {
        assert_eq!(quote_table("events").unwrap(), "`events`");
        assert_eq!(quote_table("lab.events").unwrap(), "`lab`.`events`");
        assert_eq!(quote_table("we`ird").unwrap(), "`we``ird`");
        assert!(quote_table("a.b.c").is_err());
        assert!(quote_table("a.").is_err());
    }

    #[test]
    fn keyed_merge_updates_then_inserts_the_latest_row_per_key() {
        let cols = strings(&["id", "name", "updated_at"]);
        let sql = build_keyed_merge_sql("`events`", &cols, &strings(&["id"]), None).unwrap();
        assert_eq!(sql.len(), 2);
        let latest = "(SELECT `id`, `name`, `updated_at` FROM (SELECT `id`, `name`, `updated_at`, \
                      ROW_NUMBER() OVER (PARTITION BY `id` ORDER BY `_ls_seq` DESC) AS `_ls_rn` \
                      FROM _ls_staging) `_ls_d` WHERE `_ls_rn` = 1)";
        assert_eq!(
            sql[0],
            format!("UPDATE `events` t JOIN {latest} s ON t.`id` = s.`id` SET t.`name` = s.`name`, t.`updated_at` = s.`updated_at`")
        );
        assert_eq!(
            sql[1],
            format!(
                "INSERT INTO `events` (`id`, `name`, `updated_at`) SELECT s.`id`, s.`name`, s.`updated_at` \
                 FROM {latest} s WHERE NOT EXISTS (SELECT 1 FROM `events` t WHERE t.`id` = s.`id`)"
            )
        );
    }

    #[test]
    fn keyed_merge_insert_only_and_unknown_columns() {
        let cols = strings(&["id", "name"]);
        let sql = build_keyed_merge_sql("`t`", &cols, &strings(&["id"]), Some(&[])).unwrap();
        assert_eq!(sql.len(), 1);
        assert!(sql[0].starts_with("INSERT INTO"));
        assert!(build_keyed_merge_sql("`t`", &cols, &strings(&["nope"]), None).is_err());
        assert!(build_keyed_merge_sql("`t`", &cols, &strings(&["id"]), Some(&strings(&["nope"]))).is_err());
    }

    #[test]
    fn upsert_sql_updates_non_key_in_load_order() {
        let cols = strings(&["id", "name", "updated_at"]);
        let sql = build_upsert_sql("`events`", &cols, &strings(&["id"]), None).unwrap();
        assert!(sql.contains(
            "INSERT INTO `events` (`id`, `name`, `updated_at`) SELECT `id`, `name`, `updated_at` FROM _ls_staging ORDER BY `_ls_seq`"
        ));
        assert!(sql.contains("ON DUPLICATE KEY UPDATE `name` = VALUES(`name`), `updated_at` = VALUES(`updated_at`)"));
        assert!(!sql.contains("`id` = VALUES(`id`)"));
        // Without merge_key every column is updated.
        let sql = build_upsert_sql("`t`", &cols, &[], None).unwrap();
        assert!(sql.contains("`id` = VALUES(`id`)"));
    }

    #[test]
    fn upsert_sql_nothing_to_update_is_noop_update() {
        let cols = strings(&["a", "b"]);
        let sql = build_upsert_sql("`t`", &cols, &cols, None).unwrap();
        assert!(sql.contains("ON DUPLICATE KEY UPDATE `a` = `a`"));
    }
}
