use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use loadsmith_plugin_sdk::SourcePlugin;
use crate::conn::ConnectionConfig;
use serde::Deserialize;
use std::sync::Arc;
use tokio_postgres::Client;

// ── Config structs ────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum TransactionIsolation {
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

/// Incremental load. The core persists the high watermark of `cursor_column`
/// and hands it back on the next run; the source resumes reading strictly after
/// it. `cursor_column` should be an ordered, monotonically-advancing column
/// (e.g. an `updated_at` timestamp) — see the at-least-once caveat in the docs.
#[derive(Debug, Deserialize)]
struct IncrementalConfig {
    cursor_column: String,
    /// Watermark to use on the very first run (no persisted state yet). Omit to
    /// read everything on the first run.
    #[serde(default)]
    initial_value: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PostgresConfig {
    /// All connection-level fields (host/port/tls/keepalives/session/…), shared
    /// with the destination plugin via the crate's own `conn` module.
    #[serde(flatten)]
    conn: ConnectionConfig,

    query: String,
    #[serde(default = "default_batch_size")]
    batch_size: usize,

    /// Isolation level for the cursor transaction — source-specific (the
    /// destination opens its own transaction), so it stays here.
    transaction_isolation: Option<TransactionIsolation>,

    #[serde(default)]
    incremental: Option<IncrementalConfig>,
}

fn default_batch_size() -> usize {
    1000
}

// ── Plugin ────────────────────────────────────────────────────────────────────

pub struct PostgresSourcePlugin {
    client: Option<Client>,
    schema: Option<Arc<Schema>>,
    batch_size: usize,
    exhausted: bool,

    // ── Incremental state ──────────────────────────────────────────────────
    /// The user query, declared into the cursor lazily (after `resume_from`).
    query: String,
    /// `BEGIN [ISOLATION LEVEL ...]` chosen from config.
    begin_sql: String,
    /// Set when an `incremental` block is present.
    cursor_column: Option<String>,
    /// First-run watermark from config (used only when no resume value).
    initial_value: Option<String>,
    /// Watermark handed back by the core from the previous run.
    resume_value: Option<String>,
    /// High watermark observed in this run (cursor value of the last row seen).
    last_cursor_value: Option<String>,
    /// Cursor is declared lazily on the first batch, once the resume value is in.
    cursor_declared: bool,
}

impl Default for PostgresSourcePlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl PostgresSourcePlugin {
    pub fn new() -> Self {
        Self {
            client: None,
            schema: None,
            batch_size: 1000,
            exhausted: false,
            query: String::new(),
            begin_sql: "BEGIN".to_string(),
            cursor_column: None,
            initial_value: None,
            resume_value: None,
            last_cursor_value: None,
            cursor_declared: false,
        }
    }

    /// Builds the query declared into the cursor. For a plain (non-incremental)
    /// source this is the user query verbatim. For an incremental source it
    /// wraps the user query in a subquery, filtering `> watermark` and ordering
    /// by the cursor so the last row seen is the new high watermark.
    ///
    /// The watermark is embedded as a single-quoted literal (escaped) rather
    /// than a bind parameter so Postgres coerces it to the cursor column's own
    /// type — the value is the DB's own text rendering of that column from a
    /// prior run, so it round-trips. Postgres-identifier and literal quoting are
    /// applied; the cursor value originates from our own state, but we escape
    /// defensively all the same.
    fn cursor_query(&self) -> String {
        match &self.cursor_column {
            None => self.query.clone(),
            Some(col) => {
                let ident = format!("\"{}\"", col.replace('"', "\"\""));
                let bound = self.resume_value.as_ref().or(self.initial_value.as_ref());
                let where_clause = match bound {
                    Some(v) => format!(" WHERE _ls.{ident} > '{}'", v.replace('\'', "''")),
                    None => String::new(),
                };
                format!("SELECT * FROM ({}) AS _ls{where_clause} ORDER BY _ls.{ident} ASC", self.query)
            }
        }
    }

    /// Opens the transaction + cursor on first use. Deferred out of `configure`
    /// so the resume watermark (delivered after configure, via `resume_from`) is
    /// folded into the query.
    async fn ensure_cursor(&mut self) -> Result<()> {
        if self.cursor_declared {
            return Ok(());
        }
        let query = self.cursor_query();
        let client = self.client.as_ref().unwrap();
        client.batch_execute(&self.begin_sql).await.context("BEGIN failed")?;
        client
            .execute(&format!("DECLARE ls_cursor NO SCROLL CURSOR FOR ({query})"), &[])
            .await
            .context("DECLARE cursor failed")?;
        self.cursor_declared = true;
        Ok(())
    }
}

#[async_trait]
impl SourcePlugin for PostgresSourcePlugin {
    fn plugin_name(&self) -> &str {
        "loadsmith-source-postgres"
    }
    fn plugin_version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    fn capabilities(&self) -> Vec<String> {
        // Advertised statically: the plugin is *able* to do incremental loads.
        // Activation is per-pipeline, via the `incremental` config block.
        vec!["batch_read".into(), "schema_inference".into(), "incremental_state".into()]
    }

    async fn resume_from(&mut self, cursor_value: Option<serde_json::Value>) {
        // The core stored the watermark as the opaque value we reported last run
        // (a string, since we read via the text protocol). Accept a string or a
        // JSON number/other by rendering it back to text for the SQL literal.
        self.resume_value = cursor_value.map(|v| match v {
            serde_json::Value::String(s) => s,
            other => other.to_string(),
        });
    }

    fn current_watermark(&self) -> Option<serde_json::Value> {
        self.last_cursor_value.clone().map(serde_json::Value::String)
    }

    async fn configure(&mut self, config: serde_json::Value) -> Result<()> {
        let cfg: PostgresConfig =
            serde_json::from_value(config).context("invalid postgres source config")?;

        // ── Connect (shared TLS/connection layer) ──────────────────────────
        let client = crate::conn::connect(&cfg.conn).await?;

        // ── Schema inference via prepare() ────────────────────────────────
        // The incremental wrapper (`SELECT * FROM (<query>) …`) preserves the
        // column set, so the schema is inferred from the original query here.

        let stmt = client.prepare(&cfg.query).await.context("failed to prepare query")?;
        let schema = Arc::new(crate::types::columns_to_schema(stmt.columns()));

        // The transaction + cursor are opened lazily on the first batch, after
        // `resume_from` has supplied the watermark (see `ensure_cursor`).
        self.begin_sql = match &cfg.transaction_isolation {
            None => "BEGIN".to_string(),
            Some(TransactionIsolation::ReadCommitted) => {
                "BEGIN ISOLATION LEVEL READ COMMITTED".to_string()
            }
            Some(TransactionIsolation::RepeatableRead) => {
                "BEGIN ISOLATION LEVEL REPEATABLE READ".to_string()
            }
            Some(TransactionIsolation::Serializable) => {
                "BEGIN ISOLATION LEVEL SERIALIZABLE".to_string()
            }
        };
        if let Some(inc) = &cfg.incremental {
            self.cursor_column = Some(inc.cursor_column.clone());
            self.initial_value = inc.initial_value.clone();
        }
        self.query = cfg.query;
        self.schema = Some(schema);
        self.batch_size = cfg.batch_size;
        self.client = Some(client);

        Ok(())
    }

    async fn schema(&mut self) -> Result<Schema> {
        Ok(self.schema.as_ref().unwrap().as_ref().clone())
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.exhausted {
            return Ok(None);
        }

        // Declare the cursor on first use, now that the resume watermark is in.
        self.ensure_cursor().await?;

        let schema = self.schema.as_ref().unwrap().clone();
        let client = self.client.as_ref().unwrap();

        // Use simple_query (text protocol) so all types come back as strings.
        // This avoids binary-format issues with NUMERIC, TIME, and other types
        // that tokio_postgres's FromSql doesn't support out of the box.
        let messages = client
            .simple_query(&format!("FETCH FORWARD {} FROM ls_cursor", self.batch_size))
            .await
            .context("FETCH failed")?;

        let rows: Vec<tokio_postgres::SimpleQueryRow> = messages
            .into_iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
                _ => None,
            })
            .collect();

        if rows.is_empty() {
            self.exhausted = true;
            let _ = client.batch_execute("CLOSE ls_cursor; COMMIT").await;
            return Ok(None);
        }

        // Track the high watermark: rows are ordered by the cursor ascending, so
        // the last row of this batch carries the largest cursor value seen.
        if let Some(col) = &self.cursor_column {
            if let Some(last) = rows.last() {
                if let Some(i) = last.columns().iter().position(|c| c.name() == col) {
                    if let Some(v) = last.get(i) {
                        self.last_cursor_value = Some(v.to_string());
                    }
                }
            }
        }

        let batch =
            crate::types::rows_to_batch_text(&rows, &schema).context("RecordBatch build failed")?;

        Ok(Some(batch))
    }

    async fn cancel(&mut self) {
        if let Some(client) = &self.client {
            let _ = client.batch_execute("CLOSE ls_cursor; ROLLBACK").await;
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // Connection-level config (host/tls/keepalives/session/duration parsing) is
    // owned and tested by the `conn` module. Here we cover only the
    // source-specific config (query/batch_size/transaction_isolation/incremental)
    // and the incremental cursor-query construction.

    #[test]
    fn config_deserializes_minimal() {
        let json = serde_json::json!({
            "host": "localhost",
            "dbname": "lab",
            "user": "lab",
            "password": "secret",
            "query": "SELECT 1"
        });
        let cfg: PostgresConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.conn.dbname, "lab");
        assert_eq!(cfg.batch_size, 1000);
        assert!(cfg.conn.tls.is_none());
        assert!(cfg.incremental.is_none());
    }

    #[test]
    fn config_flattens_connection_and_keeps_isolation() {
        let json = serde_json::json!({
            "host": ["node-a", "node-b"],
            "dbname": "lab",
            "user": "lab",
            "password": "s",
            "query": "SELECT 1",
            "tls": { "mode": "require" },
            "transaction_isolation": "repeatable-read",
            "session": { "work_mem": "256MB" }
        });
        let cfg: PostgresConfig = serde_json::from_value(json).unwrap();
        // Connection fields land on the flattened `conn`.
        assert!(cfg.conn.tls.is_some());
        assert!(cfg.conn.session.is_some());
        // Isolation is source-specific and stays on PostgresConfig.
        assert!(matches!(
            cfg.transaction_isolation,
            Some(TransactionIsolation::RepeatableRead)
        ));
    }

    #[test]
    fn config_deserializes_incremental() {
        let json = serde_json::json!({
            "host": "localhost",
            "dbname": "lab",
            "user": "lab",
            "password": "s",
            "query": "SELECT id, updated_at FROM events",
            "incremental": { "cursor_column": "updated_at", "initial_value": "2026-01-01" }
        });
        let cfg: PostgresConfig = serde_json::from_value(json).unwrap();
        let inc = cfg.incremental.unwrap();
        assert_eq!(inc.cursor_column, "updated_at");
        assert_eq!(inc.initial_value.as_deref(), Some("2026-01-01"));
    }

    fn plugin_with(query: &str, cursor: Option<&str>) -> PostgresSourcePlugin {
        let mut p = PostgresSourcePlugin::new();
        p.query = query.to_string();
        p.cursor_column = cursor.map(String::from);
        p
    }

    #[test]
    fn cursor_query_plain_is_verbatim() {
        let p = plugin_with("SELECT * FROM t", None);
        assert_eq!(p.cursor_query(), "SELECT * FROM t");
    }

    #[test]
    fn cursor_query_first_run_orders_without_filter() {
        let p = plugin_with("SELECT id, updated_at FROM t", Some("updated_at"));
        assert_eq!(
            p.cursor_query(),
            "SELECT * FROM (SELECT id, updated_at FROM t) AS _ls ORDER BY _ls.\"updated_at\" ASC"
        );
    }

    #[test]
    fn cursor_query_resume_filters_and_orders() {
        let mut p = plugin_with("SELECT id, updated_at FROM t", Some("updated_at"));
        p.resume_value = Some("2026-06-09 08:00:00".into());
        assert_eq!(
            p.cursor_query(),
            "SELECT * FROM (SELECT id, updated_at FROM t) AS _ls \
             WHERE _ls.\"updated_at\" > '2026-06-09 08:00:00' ORDER BY _ls.\"updated_at\" ASC"
        );
    }

    #[test]
    fn cursor_query_resume_takes_priority_over_initial() {
        let mut p = plugin_with("SELECT updated_at FROM t", Some("updated_at"));
        p.initial_value = Some("2000-01-01".into());
        p.resume_value = Some("2026-06-09".into());
        assert!(p.cursor_query().contains("> '2026-06-09'"));
        assert!(!p.cursor_query().contains("2000-01-01"));
    }

    #[test]
    fn cursor_query_escapes_quotes() {
        let mut p = plugin_with("SELECT c FROM t", Some("we\"ird"));
        p.resume_value = Some("o'brien".into());
        let q = p.cursor_query();
        assert!(q.contains("\"we\"\"ird\""));
        assert!(q.contains("> 'o''brien'"));
    }

    #[test]
    fn config_rejects_unknown_isolation_level() {
        let json = serde_json::json!({
            "host": "localhost",
            "dbname": "d",
            "user": "u",
            "password": "p",
            "query": "SELECT 1",
            "transaction_isolation": "chaos"
        });
        assert!(serde_json::from_value::<PostgresConfig>(json).is_err());
    }
}
