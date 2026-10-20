use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use loadsmith_plugin_sdk::SourcePlugin;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Row};
use serde::Deserialize;
use std::sync::Arc;

use crate::conn::ConnectionConfig;

// ── Config structs ────────────────────────────────────────────────────────────

/// Incremental load. The core persists the high watermark of `cursor_column`
/// and hands it back on the next run; the source resumes reading strictly after
/// it. `cursor_column` should be an ordered, monotonically-advancing column.
#[derive(Debug, Deserialize)]
struct IncrementalConfig {
    cursor_column: String,
    /// Watermark to use on the very first run (no persisted state yet).
    #[serde(default)]
    initial_value: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MysqlConfig {
    /// Connection-level fields, shared with the destination via `conn`.
    #[serde(flatten)]
    conn: ConnectionConfig,

    query: String,
    #[serde(default = "default_batch_size")]
    batch_size: usize,

    #[serde(default)]
    incremental: Option<IncrementalConfig>,
}

fn default_batch_size() -> usize {
    1000
}

/// Upper bound on the polite disconnect in `cancel()`.
const CANCEL_DISCONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

// ── Plugin ────────────────────────────────────────────────────────────────────

/// MySQL source.
///
/// `mysql_async`'s `QueryResult` (0.37) exposes no `Stream` adapter, so rows are
/// collected once on first read and then handed out in `batch_size` chunks. This
/// is fine for the bounded smoke/TLS workloads; a future improvement for very
/// large extracts is a `for_each`-backed bounded bridge.
pub struct MysqlSourcePlugin {
    conn: Option<Conn>,
    schema: Option<Arc<Schema>>,
    batch_size: usize,

    query: String,
    cursor_column: Option<String>,
    cursor_idx: Option<usize>,
    initial_value: Option<String>,
    resume_value: Option<String>,
    last_cursor_value: Option<String>,

    rows: Vec<Row>,
    pos: usize,
    loaded: bool,
}

impl Default for MysqlSourcePlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl MysqlSourcePlugin {
    pub fn new() -> Self {
        Self {
            conn: None,
            schema: None,
            batch_size: 1000,
            query: String::new(),
            cursor_column: None,
            cursor_idx: None,
            initial_value: None,
            resume_value: None,
            last_cursor_value: None,
            rows: Vec::new(),
            pos: 0,
            loaded: false,
        }
    }

    /// The query actually run. For a plain source this is the user query
    /// verbatim. For an incremental source it wraps the user query, filtering
    /// `> watermark` and ordering by the cursor so the last row seen is the new
    /// high watermark. The watermark is embedded as an escaped single-quoted
    /// literal so MySQL coerces it to the cursor column's own type.
    fn cursor_query(&self) -> String {
        match &self.cursor_column {
            None => self.query.clone(),
            Some(col) => {
                let ident = format!("`{}`", col.replace('`', "``"));
                let bound = self.resume_value.as_ref().or(self.initial_value.as_ref());
                let where_clause = match bound {
                    Some(v) => format!(" WHERE _ls.{ident} > '{}'", v.replace('\'', "''")),
                    None => String::new(),
                };
                format!("SELECT * FROM ({}) AS _ls{where_clause} ORDER BY _ls.{ident} ASC", self.query)
            }
        }
    }

    /// Runs the query and buffers all rows, deferred until the first batch so the
    /// resume watermark (delivered after `configure`) is folded in.
    async fn ensure_loaded(&mut self) -> Result<()> {
        if self.loaded {
            return Ok(());
        }
        let query = self.cursor_query();
        let conn = self.conn.as_mut().context("not configured")?;
        self.rows = conn.query_iter(query).await.context("query failed")?
            .collect::<Row>().await.context("row fetch failed")?;
        self.loaded = true;
        Ok(())
    }
}

#[async_trait]
impl SourcePlugin for MysqlSourcePlugin {
    fn plugin_name(&self) -> &str {
        "loadsmith-source-mysql"
    }
    fn plugin_version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    fn capabilities(&self) -> Vec<String> {
        vec!["batch_read".into(), "schema_inference".into(), "incremental_state".into()]
    }

    async fn resume_from(&mut self, cursor_value: Option<serde_json::Value>) {
        self.resume_value = cursor_value.map(|v| match v {
            serde_json::Value::String(s) => s,
            other => other.to_string(),
        });
    }

    fn current_watermark(&self) -> Option<serde_json::Value> {
        self.last_cursor_value.clone().map(serde_json::Value::String)
    }

    async fn configure(&mut self, config: serde_json::Value) -> Result<()> {
        let cfg: MysqlConfig =
            serde_json::from_value(config).map_err(|e| anyhow::anyhow!("invalid mysql source config: {e}"))?;

        let mut conn = crate::conn::connect(&cfg.conn).await?;

        // Schema inference via a prepared statement; the incremental wrapper
        // preserves the column set, so the base query's columns are authoritative.
        let stmt = conn.prep(&cfg.query).await.context("failed to prepare query")?;
        let schema = crate::types::columns_to_schema(stmt.columns().as_ref());

        if let Some(inc) = &cfg.incremental {
            self.cursor_idx = schema.fields().iter().position(|f| f.name() == &inc.cursor_column);
            self.cursor_column = Some(inc.cursor_column.clone());
            self.initial_value = inc.initial_value.clone();
        }

        self.query = cfg.query;
        self.schema = Some(Arc::new(schema));
        self.batch_size = cfg.batch_size.max(1);
        self.conn = Some(conn);
        Ok(())
    }

    async fn schema(&mut self) -> Result<Schema> {
        Ok(self.schema.as_ref().unwrap().as_ref().clone())
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        self.ensure_loaded().await?;

        if self.pos >= self.rows.len() {
            return Ok(None);
        }
        let end = (self.pos + self.batch_size).min(self.rows.len());
        let schema = self.schema.as_ref().unwrap().clone();
        let slice = &self.rows[self.pos..end];

        // High watermark: rows are ordered by the cursor ascending, so the last
        // row of this chunk carries the largest cursor value seen so far.
        if let Some(idx) = self.cursor_idx {
            if let Some(last) = slice.last() {
                if let Some(v) = last.as_ref(idx).and_then(crate::types::value_text) {
                    self.last_cursor_value = Some(v);
                }
            }
        }

        let batch = crate::types::rows_to_batch(slice, &schema).context("RecordBatch build failed")?;
        self.pos = end;
        Ok(Some(batch))
    }

    async fn cancel(&mut self) {
        // The SDK may have dropped an in-flight `next_batch` (the buffered
        // query) before calling this, leaving the connection mid-result. A
        // polite COM_QUIT is worth a short wait, never more: on timeout the
        // connection is dropped, and the server ends the session when the
        // socket closes.
        if let Some(conn) = self.conn.take() {
            let _ = tokio::time::timeout(CANCEL_DISCONNECT_TIMEOUT, conn.disconnect()).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plugin_with(query: &str, cursor: Option<&str>) -> MysqlSourcePlugin {
        let mut p = MysqlSourcePlugin::new();
        p.query = query.to_string();
        p.cursor_column = cursor.map(String::from);
        p
    }

    #[test]
    fn config_deserializes_minimal() {
        let json = serde_json::json!({
            "host": "localhost", "dbname": "lab", "user": "lab", "password": "lab",
            "query": "SELECT 1"
        });
        let cfg: MysqlConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.conn.dbname, "lab");
        assert_eq!(cfg.batch_size, 1000);
        assert!(cfg.incremental.is_none());
    }

    #[test]
    fn config_deserializes_incremental_and_tls() {
        let json = serde_json::json!({
            "dbname": "lab", "user": "lab", "password": "lab",
            "query": "SELECT id, updated_at FROM t",
            "tls": { "mode": "require" },
            "incremental": { "cursor_column": "updated_at", "initial_value": "2026-01-01" }
        });
        let cfg: MysqlConfig = serde_json::from_value(json).unwrap();
        assert!(cfg.conn.tls.is_some());
        assert_eq!(cfg.incremental.unwrap().cursor_column, "updated_at");
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
            "SELECT * FROM (SELECT id, updated_at FROM t) AS _ls ORDER BY _ls.`updated_at` ASC"
        );
    }

    #[test]
    fn cursor_query_resume_filters_and_orders() {
        let mut p = plugin_with("SELECT id, updated_at FROM t", Some("updated_at"));
        p.resume_value = Some("2026-06-09 08:00:00".into());
        assert_eq!(
            p.cursor_query(),
            "SELECT * FROM (SELECT id, updated_at FROM t) AS _ls \
             WHERE _ls.`updated_at` > '2026-06-09 08:00:00' ORDER BY _ls.`updated_at` ASC"
        );
    }

    #[test]
    fn cursor_query_resume_priority_and_escaping() {
        let mut p = plugin_with("SELECT c FROM t", Some("we`ird"));
        p.initial_value = Some("2000-01-01".into());
        p.resume_value = Some("o'brien".into());
        let q = p.cursor_query();
        assert!(q.contains("`we``ird`"));
        assert!(q.contains("> 'o''brien'"));
        assert!(!q.contains("2000-01-01"));
    }
}
