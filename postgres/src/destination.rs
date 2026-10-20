use anyhow::{bail, Context, Result};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::SinkExt;
use loadsmith_plugin_sdk::DestinationPlugin;
use serde::Deserialize;
use tokio_postgres::Client;

use crate::conn::ConnectionConfig;
use crate::copy::batch_to_copy_text;
use crate::vocab::config_enum;

/// Staging table of `mode: merge`. Temporary, dropped on commit.
const STAGING: &str = "_ls_staging";
/// Load-order column of the staging table: the last row of a key wins.
const STAGING_SEQ: &str = "_ls_seq";

config_enum! {
    /// How the load changes the target table. Every mode is one transaction,
    /// committed at the end of the load.
    #[derive(Default)]
    enum WriteMode for "mode" {
        /// `COPY` straight into the target. At-least-once with incremental state.
        #[default]
        Append = "append",
        /// `TRUNCATE` the target, then `COPY` into it: the table's contents are
        /// replaced by the load's rows, atomically.
        Overwrite = "overwrite",
        /// `COPY` into a staging table, then one `MERGE` by `merge_key` into the
        /// target. Idempotent by key ⇒ exactly-once effective.
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
struct PostgresDestConfig {
    /// All connection-level fields (host/port/tls/keepalives/session/…), shared
    /// with the source plugin via the crate's own `conn` module.
    #[serde(flatten)]
    conn: ConnectionConfig,

    /// Target table (optionally schema-qualified, e.g. `public.events`). Used
    /// verbatim in SQL.
    target_table: String,
    #[serde(default)]
    mode: WriteMode,
    /// `merge`: the key columns the `MERGE` matches on (required).
    #[serde(default)]
    merge_key: Vec<String>,
    /// `merge`: the columns updated when a key matches. Default: every
    /// incoming column that isn't a key. `[]` inserts new keys only.
    #[serde(default)]
    update_columns: Option<Vec<String>>,
}

impl PostgresDestConfig {
    fn validate(&self) -> Result<()> {
        if self.mode == WriteMode::Merge {
            if self.merge_key.is_empty() {
                bail!("`mode: merge` requires `merge_key` (the key columns to match on)");
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
            if self.update_columns.is_some() {
                bail!("`update_columns` only applies to `mode: merge` (mode is `{mode}`)");
            }
        }
        Ok(())
    }
}

pub struct PostgresDestPlugin {
    config: Option<PostgresDestConfig>,
    client: Option<Client>,
    /// Column names from the first batch — the `COPY`/`MERGE` column list.
    columns: Vec<String>,
    rows_written: u64,
}

impl Default for PostgresDestPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl PostgresDestPlugin {
    pub fn new() -> Self {
        Self { config: None, client: None, columns: Vec::new(), rows_written: 0 }
    }

    fn cfg(&self) -> Result<&PostgresDestConfig> {
        self.config.as_ref().context("postgres destination: not configured")
    }

    fn client(&self) -> Result<&Client> {
        self.client.as_ref().context("postgres destination: not prepared")
    }

    /// Where `COPY` writes: the staging table for `merge`, else the target.
    fn copy_target(&self) -> Result<&str> {
        let cfg = self.cfg()?;
        Ok(match cfg.mode {
            WriteMode::Merge => STAGING,
            WriteMode::Append | WriteMode::Overwrite => cfg.target_table.as_str(),
        })
    }
}

#[async_trait]
impl DestinationPlugin for PostgresDestPlugin {
    fn plugin_name(&self) -> &str {
        "loadsmith-destination-postgres"
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
        let cfg: PostgresDestConfig = config.parse()?;
        cfg.validate()?;
        self.config = Some(cfg);
        Ok(())
    }

    async fn prepare(&mut self) -> Result<()> {
        let cfg = self.cfg()?;
        // Shared connection layer — full TLS parity with the source plugin.
        let client = crate::conn::connect(&cfg.conn).await?;

        // One transaction for the whole load — nothing is durable in the target
        // until finalize() commits.
        client.batch_execute("BEGIN").await.context("BEGIN failed")?;

        let target = &cfg.target_table;
        match cfg.mode {
            WriteMode::Append => {}
            // TRUNCATE is transactional in Postgres: readers keep seeing the old
            // contents (they wait on the lock) until the load commits, and a
            // failed load rolls the truncation back.
            WriteMode::Overwrite => client
                .batch_execute(&format!("TRUNCATE TABLE {target}"))
                .await
                .with_context(|| format!("TRUNCATE {target} failed"))?,
            WriteMode::Merge => client
                .batch_execute(&format!(
                    "CREATE TEMP TABLE {STAGING} (LIKE {target} INCLUDING DEFAULTS, \
                     {seq} bigint GENERATED ALWAYS AS IDENTITY) ON COMMIT DROP",
                    seq = quote_ident(STAGING_SEQ),
                ))
                .await
                .context("create staging table failed")?,
        }

        self.client = Some(client);
        Ok(())
    }

    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        if self.columns.is_empty() {
            self.columns = batch.schema().fields().iter().map(|f| f.name().clone()).collect();
        }
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.rows_written += batch.num_rows() as u64;

        let cols = self.columns.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
        let sql = format!("COPY {} ({cols}) FROM STDIN WITH (FORMAT text)", self.copy_target()?);
        let payload = batch_to_copy_text(&batch)?;

        let client = self.client()?;
        let sink = client.copy_in(&sql).await.context("COPY start failed")?;
        futures_util::pin_mut!(sink);
        sink.as_mut().send(Bytes::from(payload)).await.context("COPY send failed")?;
        sink.finish().await.context("COPY finish failed")?;
        Ok(())
    }

    async fn finalize(&mut self) -> Result<u64> {
        let cfg = self.cfg()?;
        let client = self.client()?;
        if cfg.mode == WriteMode::Merge && !self.columns.is_empty() {
            let merge = build_merge_sql(
                &cfg.target_table,
                &self.columns,
                &cfg.merge_key,
                cfg.update_columns.as_deref(),
            )?;
            client.batch_execute(&merge).await.context("MERGE failed")?;
        }
        // The commit is the durability point: the truncation, COPY and MERGE
        // land together.
        client.batch_execute("COMMIT").await.context("COMMIT failed")?;
        Ok(self.rows_written)
    }

    async fn cancel(&mut self) {
        if let Some(client) = &self.client {
            let _ = client.batch_execute("ROLLBACK").await;
        }
    }
}

/// Quotes a Postgres identifier, escaping embedded double quotes.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The columns a match updates: `update_columns` when given, else every
/// incoming column that isn't a key. Each must be an incoming, non-key column.
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

/// Builds the `MERGE` from the staging table into the target, keyed on
/// `merge_key`. Within one load the **last row of each key wins**: the staging
/// rows are reduced to the latest per key (load order) first, since `MERGE`
/// cannot apply two source rows to one target row.
fn build_merge_sql(
    target: &str,
    columns: &[String],
    merge_key: &[String],
    update_columns: Option<&[String]>,
) -> Result<String> {
    for k in merge_key {
        if !columns.iter().any(|c| c == k) {
            bail!("merge_key column '{k}' is not present in the data");
        }
    }
    let keys = merge_key.iter().map(|k| quote_ident(k)).collect::<Vec<_>>().join(", ");
    let on = merge_key
        .iter()
        .map(|k| format!("t.{0} = s.{0}", quote_ident(k)))
        .collect::<Vec<_>>()
        .join(" AND ");

    let set = update_set(columns, merge_key, update_columns)?;
    let matched = if set.is_empty() {
        "WHEN MATCHED THEN DO NOTHING".to_string()
    } else {
        let set = set.iter().map(|c| format!("{0} = s.{0}", quote_ident(c))).collect::<Vec<_>>().join(", ");
        format!("WHEN MATCHED THEN UPDATE SET {set}")
    };

    let col_list = columns.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
    let val_list = columns.iter().map(|c| format!("s.{}", quote_ident(c))).collect::<Vec<_>>().join(", ");
    let latest = format!(
        "SELECT DISTINCT ON ({keys}) {col_list} FROM {STAGING} ORDER BY {keys}, {seq} DESC",
        seq = quote_ident(STAGING_SEQ),
    );

    Ok(format!(
        "MERGE INTO {target} t USING ({latest}) s ON {on} \
         {matched} \
         WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({val_list})"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use loadsmith_plugin_sdk::{PluginConfig, UnknownKeys};

    async fn configure(json: serde_json::Value) -> Result<()> {
        PostgresDestPlugin::new().configure(PluginConfig::new(json, UnknownKeys::Error)).await
    }

    fn base(extra: serde_json::Value) -> serde_json::Value {
        let mut v = serde_json::json!({ "dbname": "d", "user": "u", "password": "p", "target_table": "events" });
        v.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
        v
    }

    #[tokio::test]
    async fn merge_requires_a_merge_key() {
        let e = configure(base(serde_json::json!({ "mode": "merge" }))).await.unwrap_err().to_string();
        assert!(e.contains("requires `merge_key`"), "{e}");
        assert!(configure(base(serde_json::json!({ "mode": "merge", "merge_key": ["id"] }))).await.is_ok());
    }

    #[tokio::test]
    async fn merge_options_are_rejected_outside_merge() {
        for mode in ["append", "overwrite"] {
            let e = configure(base(serde_json::json!({ "mode": mode, "merge_key": ["id"] })))
                .await
                .unwrap_err()
                .to_string();
            assert!(e.contains("only applies to `mode: merge`"), "{e}");
        }
        let e = configure(base(serde_json::json!({ "mode": "merge", "merge_key": ["id"], "update_columns": ["id"] })))
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("never updated"), "{e}");
    }

    #[tokio::test]
    async fn retired_modes_name_the_new_value() {
        for (old, new) in [("atomic", "append"), ("staged_merge", "merge")] {
            let e = configure(base(serde_json::json!({ "mode": old }))).await.unwrap_err().to_string();
            assert!(e.contains(&format!("did you mean `{new}`?")), "{e}");
            assert!(e.contains("unknown `mode` value"), "{e}");
        }
    }

    #[test]
    fn default_mode_is_append() {
        let cfg: PostgresDestConfig = serde_json::from_value(base(serde_json::json!({}))).unwrap();
        assert_eq!(cfg.mode, WriteMode::Append);
    }

    fn strings(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn merge_sql_dedupes_staging_updates_non_key_and_inserts() {
        let cols = strings(&["id", "name", "updated_at"]);
        let sql = build_merge_sql("events", &cols, &strings(&["id"]), None).unwrap();
        assert!(sql.contains(
            "USING (SELECT DISTINCT ON (\"id\") \"id\", \"name\", \"updated_at\" FROM _ls_staging \
             ORDER BY \"id\", \"_ls_seq\" DESC) s"
        ), "{sql}");
        assert!(sql.contains("ON t.\"id\" = s.\"id\""));
        assert!(sql.contains("WHEN MATCHED THEN UPDATE SET \"name\" = s.\"name\", \"updated_at\" = s.\"updated_at\""));
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT (\"id\", \"name\", \"updated_at\") VALUES (s.\"id\", s.\"name\", s.\"updated_at\")"));
    }

    #[test]
    fn merge_sql_update_columns() {
        let cols = strings(&["id", "name", "updated_at"]);
        let sql = build_merge_sql("t", &cols, &strings(&["id"]), Some(&strings(&["name"]))).unwrap();
        assert!(sql.contains("WHEN MATCHED THEN UPDATE SET \"name\" = s.\"name\" WHEN NOT MATCHED"), "{sql}");
        // `[]` inserts new keys only.
        let sql = build_merge_sql("t", &cols, &strings(&["id"]), Some(&[])).unwrap();
        assert!(sql.contains("WHEN MATCHED THEN DO NOTHING"));
        assert!(build_merge_sql("t", &cols, &strings(&["id"]), Some(&strings(&["nope"]))).is_err());
    }

    #[test]
    fn merge_sql_all_keys_does_nothing_on_match() {
        let cols = strings(&["a", "b"]);
        let sql = build_merge_sql("t", &cols, &cols, None).unwrap();
        assert!(sql.contains("WHEN MATCHED THEN DO NOTHING"));
    }

    #[test]
    fn merge_sql_rejects_key_not_in_data() {
        assert!(build_merge_sql("t", &strings(&["a"]), &strings(&["missing"]), None).is_err());
    }
}
