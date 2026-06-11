use anyhow::{bail, Context, Result};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::SinkExt;
use loadsmith_plugin_sdk::DestinationPlugin;
use crate::conn::ConnectionConfig;
use serde::Deserialize;
use tokio_postgres::Client;

use crate::copy::batch_to_copy_text;

/// Staging table used by `staged_merge`. Temporary, dropped on commit.
const STAGING: &str = "_ls_staging";

#[derive(Debug, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CommitMode {
    /// One transaction for the whole load; `COPY` straight into the target,
    /// `COMMIT` at the end. All-or-nothing, at-least-once.
    #[default]
    Atomic,
    /// `COPY` into a staging table, then `MERGE` by `merge_key` into the target
    /// in a single transaction. Idempotent by key ⇒ exactly-once effective.
    StagedMerge,
}

#[derive(Debug, Deserialize)]
struct PostgresDestConfig {
    /// All connection-level fields (host/port/tls/keepalives/session/…), shared
    /// with the source plugin via the crate's own `conn` module.
    #[serde(flatten)]
    conn: ConnectionConfig,

    /// Target table (optionally schema-qualified, e.g. `public.events`).
    target_table: String,
    #[serde(default)]
    mode: CommitMode,
    /// Primary/merge key columns — required for `staged_merge`.
    #[serde(default)]
    merge_key: Vec<String>,
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

    fn cfg(&self) -> &PostgresDestConfig {
        self.config.as_ref().expect("configured")
    }

    /// Where `COPY` writes: the staging table for `staged_merge`, else straight
    /// into the target.
    fn copy_target(&self) -> &str {
        match self.cfg().mode {
            CommitMode::StagedMerge => STAGING,
            CommitMode::Atomic => self.cfg().target_table.as_str(),
        }
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

    async fn configure(&mut self, config: serde_json::Value) -> Result<()> {
        let cfg: PostgresDestConfig =
            serde_json::from_value(config).context("invalid postgres destination config")?;
        if cfg.mode == CommitMode::StagedMerge && cfg.merge_key.is_empty() {
            bail!("mode 'staged_merge' requires a non-empty merge_key");
        }
        self.config = Some(cfg);
        Ok(())
    }

    async fn prepare(&mut self) -> Result<()> {
        // Shared connection layer — full TLS parity with the source plugin.
        let client = crate::conn::connect(&self.cfg().conn).await?;

        // One transaction for the whole load — nothing is durable in the target
        // until finalize() commits (or, for staged_merge, until the swap).
        client.batch_execute("BEGIN").await.context("BEGIN failed")?;

        if self.cfg().mode == CommitMode::StagedMerge {
            let target = self.cfg().target_table.clone();
            client
                .batch_execute(&format!(
                    "CREATE TEMP TABLE {STAGING} (LIKE {target} INCLUDING DEFAULTS) ON COMMIT DROP"
                ))
                .await
                .context("create staging table failed")?;
        }

        self.client = Some(client);
        Ok(())
    }

    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        if self.columns.is_empty() {
            self.columns =
                batch.schema().fields().iter().map(|f| f.name().clone()).collect();
        }
        self.rows_written += batch.num_rows() as u64;

        let cols = self.columns.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
        let sql = format!(
            "COPY {} ({cols}) FROM STDIN WITH (FORMAT text)",
            self.copy_target()
        );
        let payload = batch_to_copy_text(&batch)?;

        let client = self.client.as_ref().unwrap();
        let sink = client.copy_in(&sql).await.context("COPY start failed")?;
        futures_util::pin_mut!(sink);
        sink.as_mut().send(Bytes::from(payload)).await.context("COPY send failed")?;
        sink.finish().await.context("COPY finish failed")?;
        Ok(())
    }

    async fn finalize(&mut self) -> Result<u64> {
        let client = self.client.as_ref().unwrap();
        if self.cfg().mode == CommitMode::StagedMerge && !self.columns.is_empty() {
            let merge = build_merge_sql(
                &self.cfg().target_table,
                STAGING,
                &self.columns,
                &self.cfg().merge_key,
            )?;
            client.batch_execute(&merge).await.context("MERGE failed")?;
        }
        // The commit is the durability point: COPY+MERGE land atomically.
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

/// Builds the `MERGE` that swaps staging into the target, keyed on `merge_key`.
/// Updates the non-key columns on match, inserts the full row otherwise. When
/// every column is a key (nothing to update) it does nothing on match.
fn build_merge_sql(
    target: &str,
    staging: &str,
    columns: &[String],
    merge_key: &[String],
) -> Result<String> {
    for k in merge_key {
        if !columns.iter().any(|c| c == k) {
            bail!("merge_key column '{k}' is not present in the data");
        }
    }
    let on = merge_key
        .iter()
        .map(|k| format!("t.{0} = s.{0}", quote_ident(k)))
        .collect::<Vec<_>>()
        .join(" AND ");

    let non_key: Vec<&String> = columns.iter().filter(|c| !merge_key.contains(c)).collect();
    let matched = if non_key.is_empty() {
        "WHEN MATCHED THEN DO NOTHING".to_string()
    } else {
        let set = non_key
            .iter()
            .map(|c| format!("{0} = s.{0}", quote_ident(c)))
            .collect::<Vec<_>>()
            .join(", ");
        format!("WHEN MATCHED THEN UPDATE SET {set}")
    };

    let col_list = columns.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
    let val_list = columns.iter().map(|c| format!("s.{}", quote_ident(c))).collect::<Vec<_>>().join(", ");

    Ok(format!(
        "MERGE INTO {target} t USING {staging} s ON {on} \
         {matched} \
         WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({val_list})"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn config_requires_merge_key_for_staged_merge() {
        let mut p = PostgresDestPlugin::new();
        // staged_merge without a key is rejected at configure().
        let err = p
            .configure(serde_json::json!({
                "dbname": "d", "user": "u", "password": "p",
                "target_table": "events", "mode": "staged_merge"
            }))
            .await;
        assert!(err.is_err());
        // A valid staged_merge config with a key passes.
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
        let cfg: PostgresDestConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.mode, CommitMode::Atomic);
    }

    #[test]
    fn merge_sql_updates_non_key_and_inserts() {
        let cols = vec!["id".to_string(), "name".to_string(), "updated_at".to_string()];
        let keys = vec!["id".to_string()];
        let sql = build_merge_sql("events", "_ls_staging", &cols, &keys).unwrap();
        assert!(sql.contains("ON t.\"id\" = s.\"id\""));
        assert!(sql.contains("WHEN MATCHED THEN UPDATE SET \"name\" = s.\"name\", \"updated_at\" = s.\"updated_at\""));
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT (\"id\", \"name\", \"updated_at\") VALUES (s.\"id\", s.\"name\", s.\"updated_at\")"));
    }

    #[test]
    fn merge_sql_all_keys_does_nothing_on_match() {
        let cols = vec!["a".to_string(), "b".to_string()];
        let keys = vec!["a".to_string(), "b".to_string()];
        let sql = build_merge_sql("t", "_ls_staging", &cols, &keys).unwrap();
        assert!(sql.contains("WHEN MATCHED THEN DO NOTHING"));
    }

    #[test]
    fn merge_sql_rejects_key_not_in_data() {
        let cols = vec!["a".to_string()];
        let keys = vec!["missing".to_string()];
        assert!(build_merge_sql("t", "_ls_staging", &cols, &keys).is_err());
    }
}
