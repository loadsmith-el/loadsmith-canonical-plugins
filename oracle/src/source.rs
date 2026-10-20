use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use loadsmith_plugin_sdk::SourcePlugin;
use oracledb::{Connection, DbType};
use serde::Deserialize;
use std::sync::Arc;

use crate::conn::ConnectionConfig;
use crate::types::{self, quote_ident, typed_expr, Cell};

// ── Config structs ────────────────────────────────────────────────────────────

/// Incremental load. The core persists the high watermark of `cursor_column` and
/// hands it back on the next run; the source resumes reading strictly after it.
#[derive(Debug, Deserialize)]
struct IncrementalConfig {
    cursor_column: String,
    /// Watermark to use on the very first run (no persisted state yet).
    #[serde(default)]
    initial_value: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OracleConfig {
    #[serde(flatten)]
    conn: ConnectionConfig,

    query: String,
    #[serde(default = "default_batch_size")]
    batch_size: usize,

    #[serde(default)]
    incremental: Option<IncrementalConfig>,

    /// Unit of the emitted DATE/TIMESTAMP values (default `ms`; `ns` keeps
    /// `TIMESTAMP(9)` whole, within 1677–2262).
    #[serde(default)]
    timestamp_unit: crate::vocab::TimestampUnit,
}

fn default_batch_size() -> usize {
    1000
}

// ── Plugin ────────────────────────────────────────────────────────────────────

/// Oracle source. Rows are fetched once (the driver streams the full result set) and
/// then handed out in `batch_size` chunks.
pub struct OracleSourcePlugin {
    conn: Option<Connection>,
    schema: Option<Arc<Schema>>,
    batch_size: usize,

    /// The user's base query (inner SELECT).
    query: String,
    /// The projected SELECT list (temporal/CLOB columns wrapped in `TO_CHAR`).
    select_list: String,

    cursor_column: Option<String>,
    cursor_idx: Option<usize>,
    cursor_type: Option<&'static DbType>,
    initial_value: Option<String>,
    resume_value: Option<String>,
    last_cursor_value: Option<String>,

    rows: Vec<Vec<Cell>>,
    pos: usize,
    loaded: bool,
}

impl Default for OracleSourcePlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl OracleSourcePlugin {
    pub fn new() -> Self {
        Self {
            conn: None,
            schema: None,
            batch_size: 1000,
            query: String::new(),
            select_list: "*".to_string(),
            cursor_column: None,
            cursor_idx: None,
            cursor_type: None,
            initial_value: None,
            resume_value: None,
            last_cursor_value: None,
            rows: Vec::new(),
            pos: 0,
            loaded: false,
        }
    }

    /// The query actually run: the projected column list over the user query as a
    /// subquery, optionally filtered `> watermark` and ordered by the cursor so
    /// the last row seen is the new high watermark.
    fn build_query(&self) -> String {
        let base = format!("SELECT {} FROM ({}) \"_LS\"", self.select_list, self.query);
        match &self.cursor_column {
            None => base,
            Some(col) => {
                let ident = quote_ident(col);
                let bound = self.resume_value.as_ref().or(self.initial_value.as_ref());
                let where_clause = match bound {
                    Some(v) => {
                        // The watermark is text; wrap it in the cursor column's
                        // explicit TO_* conversion so the comparison never relies
                        // on session NLS (temporal cursors especially).
                        let literal = format!("'{}'", v.replace('\'', "''"));
                        let rhs = match self.cursor_type {
                            Some(t) => typed_expr(&literal, t),
                            None => literal,
                        };
                        format!(" WHERE \"_LS\".{ident} > {rhs}")
                    }
                    None => String::new(),
                };
                format!("{base}{where_clause} ORDER BY \"_LS\".{ident} ASC")
            }
        }
    }

    /// Runs the query and buffers all rows, deferred until the first batch so the
    /// resume watermark (delivered after `configure`) is folded in. Synchronous
    /// driver work, run on a blocking thread.
    fn load_rows(&self) -> Result<Vec<Vec<Cell>>> {
        let query = self.build_query();
        let conn = self.conn.as_ref().context("not configured")?;
        let schema = self.schema.as_ref().context("not configured")?;
        let rs = conn.query(&query, &[]).map_err(|e| anyhow::anyhow!("query failed: {e}"))?;

        let ncols = schema.fields().len();
        let mut out = Vec::new();
        for row in rs {
            let row = row.map_err(|e| anyhow::anyhow!("row fetch failed: {e}"))?;
            let cells = (0..ncols).map(|i| types::read_cell(&row, i)).collect::<Result<Vec<_>>>()?;
            out.push(cells);
        }
        Ok(out)
    }

    fn ensure_loaded(&mut self) -> Result<()> {
        if self.loaded {
            return Ok(());
        }
        let rows = tokio::task::block_in_place(|| self.load_rows())?;
        self.rows = rows;
        self.loaded = true;
        Ok(())
    }
}

#[async_trait]
impl SourcePlugin for OracleSourcePlugin {
    fn plugin_name(&self) -> &str {
        "loadsmith-source-oracle"
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

    async fn configure(&mut self, config: loadsmith_plugin_sdk::PluginConfig) -> Result<()> {
        // Unknown keys (at any depth, flattened blocks included) follow the
        // operator's policy: an error by default, a warning with
        // `--ignore-unknown-keys`.
        let cfg: OracleConfig = config.parse()?;

        let (conn, cols) = tokio::task::block_in_place(|| -> Result<_> {
            let conn = crate::conn::connect(&cfg.conn)?;
            // Describe the column set without fetching data.
            let describe_sql = format!("SELECT * FROM ({}) \"_LS\" WHERE 1 = 0", cfg.query);
            let rs = conn
                .query(&describe_sql, &[])
                .map_err(|e| anyhow::anyhow!("schema describe failed: {e}"))?;
            let cols = types::describe(rs.columns());
            Ok((conn, cols))
        })?;

        let (select_list, schema) = types::projected_select(&cols, cfg.timestamp_unit.arrow());

        if let Some(inc) = &cfg.incremental {
            // Resolve the config cursor name against the catalog's upper-cased
            // names (a simple `event_ts` matches the column `EVENT_TS`).
            let cursor = types::fold_ident(&inc.cursor_column);
            let idx = schema.fields().iter().position(|f| types::fold_ident(f.name()) == cursor);
            if idx.is_none() {
                let names = schema.fields().iter().map(|f| format!("`{}`", f.name())).collect::<Vec<_>>().join(", ");
                anyhow::bail!(
                    "incremental.cursor_column `{}` is not a column of the query; its columns are {names}",
                    inc.cursor_column
                );
            }
            self.cursor_idx = idx;
            self.cursor_type = cols
                .iter()
                .find(|c| types::fold_ident(&c.name) == cursor)
                .map(|c| c.oracle_type);
            self.cursor_column = Some(inc.cursor_column.clone());
            self.initial_value = inc.initial_value.clone();
        }

        self.query = cfg.query;
        self.select_list = select_list;
        self.schema = Some(Arc::new(schema));
        self.batch_size = cfg.batch_size.max(1);
        self.conn = Some(conn);
        Ok(())
    }

    async fn schema(&mut self) -> Result<Schema> {
        Ok(self.schema.as_ref().context("oracle source: not configured")?.as_ref().clone())
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        self.ensure_loaded()?;

        if self.pos >= self.rows.len() {
            return Ok(None);
        }
        let end = (self.pos + self.batch_size).min(self.rows.len());
        let schema = self.schema.as_ref().context("oracle source: not configured")?.clone();
        let slice = &self.rows[self.pos..end];

        // High watermark: rows are ordered by the cursor ascending, so the last
        // row of this chunk carries the largest cursor value seen so far.
        if let Some(idx) = self.cursor_idx {
            if let Some(last) = slice.last() {
                if let Some(v) = last.get(idx).and_then(Cell::text) {
                    self.last_cursor_value = Some(v.to_string());
                }
            }
        }

        let batch = types::rows_to_batch(slice, &schema).context("RecordBatch build failed")?;
        self.pos = end;
        Ok(Some(batch))
    }

    async fn cancel(&mut self) {
        // Dropping the connection closes it; nothing async to await.
        self.conn = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plugin_with(query: &str, cursor: Option<&str>) -> OracleSourcePlugin {
        let mut p = OracleSourcePlugin::new();
        p.query = query.to_string();
        p.select_list = "\"ID\", \"TS\"".to_string();
        p.cursor_column = cursor.map(String::from);
        p
    }

    #[test]
    fn config_deserializes_minimal() {
        let json = serde_json::json!({
            "service_name": "FREEPDB1", "user": "lab", "password": "lab",
            "query": "SELECT 1 FROM dual"
        });
        let cfg: OracleConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.conn.user, "lab");
        assert_eq!(cfg.batch_size, 1000);
        assert!(cfg.incremental.is_none());
    }

    #[test]
    fn unknown_keys_are_found_in_the_flattened_connection_block() {
        use loadsmith_plugin_sdk::{PluginConfig, UnknownKeys};
        let block = serde_json::json!({
            "service_name": "FREEPDB1", "user": "lab", "password": "lab",
            "query": "SELECT 1 FROM dual",
            "servce_name": "X",
            "batch_sise": 10
        });
        let err = PluginConfig::new(block.clone(), UnknownKeys::Error)
            .parse::<OracleConfig>()
            .unwrap_err()
            .to_string();
        assert!(err.contains("`servce_name`") && err.contains("`batch_sise`"), "{err}");
        assert!(PluginConfig::new(block, UnknownKeys::Warn).parse::<OracleConfig>().is_ok());
    }

    #[test]
    fn build_query_plain_projects_over_subquery() {
        let p = plugin_with("SELECT id, ts FROM t", None);
        assert_eq!(p.build_query(), "SELECT \"ID\", \"TS\" FROM (SELECT id, ts FROM t) \"_LS\"");
    }

    #[test]
    fn build_query_first_run_orders_without_filter() {
        let p = plugin_with("SELECT id, ts FROM t", Some("TS"));
        assert_eq!(
            p.build_query(),
            "SELECT \"ID\", \"TS\" FROM (SELECT id, ts FROM t) \"_LS\" ORDER BY \"_LS\".\"TS\" ASC"
        );
    }

    #[test]
    fn build_query_resume_filters_and_orders() {
        let mut p = plugin_with("SELECT id, ts FROM t", Some("TS"));
        p.resume_value = Some("2026-06-09 08:00:00.000000".into());
        let q = p.build_query();
        assert!(q.contains("WHERE \"_LS\".\"TS\" > '2026-06-09 08:00:00.000000'"));
        assert!(q.ends_with("ORDER BY \"_LS\".\"TS\" ASC"));
    }

    #[test]
    fn build_query_resume_priority_and_escaping() {
        let mut p = plugin_with("SELECT c FROM t", Some("WE\"IRD"));
        p.initial_value = Some("2000".into());
        p.resume_value = Some("o'brien".into());
        let q = p.build_query();
        assert!(q.contains("\"WE\"\"IRD\""));
        assert!(q.contains("> 'o''brien'"));
        assert!(!q.contains("2000"));
    }
}
