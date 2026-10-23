use anyhow::{bail, Context, Result};
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use futures_util::TryStreamExt;
use loadsmith_plugin_sdk::{SourcePlugin, TieSafeCursor};
use mysql_async::prelude::{Query, Queryable};
use mysql_async::{Conn, ResultSetStream, Row, TextProtocol};
use serde::Deserialize;
use std::sync::Arc;

use crate::conn::ConnectionConfig;
use crate::telemetry::op;
use crate::vocab::TimestampUnit;

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

    /// Unit of the emitted timestamps (default `ms`; `us` keeps the full
    /// precision of `DATETIME(6)`/`TIMESTAMP(6)`).
    #[serde(default)]
    timestamp_unit: TimestampUnit,
}

fn default_batch_size() -> usize {
    1000
}

/// Upper bound on the polite disconnect in `cancel()`.
const CANCEL_DISCONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// The row stream of the running query. It owns the connection, so it is
/// `'static` and lives in the plugin across `next_batch` calls.
type RowStream = ResultSetStream<'static, 'static, 'static, Row, TextProtocol>;

// ── Plugin ────────────────────────────────────────────────────────────────────

/// MySQL source. The query's rows are streamed from the server
/// (`QueryResult::stream`), `batch_size` at a time: memory holds one batch,
/// not the result set.
pub struct MysqlSourcePlugin {
    /// The connection, until the query starts (then the stream owns it).
    conn: Option<Conn>,
    stream: Option<RowStream>,
    schema: Option<Arc<Schema>>,
    batch_size: usize,
    exhausted: bool,

    query: String,
    cursor_column: Option<String>,
    cursor_idx: Option<usize>,
    initial_value: Option<String>,
    resume_value: Option<String>,
    /// The incremental cursor's values seen so far: the watermark reported is
    /// tie-safe (a value no later row repeats; the last one at the end).
    cursor: TieSafeCursor<String>,
    /// `db.namespace` of the telemetry spans (the database name).
    namespace: String,
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
            stream: None,
            schema: None,
            batch_size: 1000,
            exhausted: false,
            query: String::new(),
            cursor_column: None,
            cursor_idx: None,
            initial_value: None,
            resume_value: None,
            cursor: TieSafeCursor::new(),
            namespace: String::new(),
        }
    }

    /// The query actually run. For a plain source this is the user query
    /// verbatim. For an incremental source it wraps the user query, filtering
    /// `> watermark` and ordering by the cursor so the last row seen is the new
    /// high watermark. The watermark is embedded as an escaped single-quoted
    /// literal so MySQL coerces it to the cursor column's own type.
    fn cursor_query(&self) -> String {
        self.cursor_query_with(false)
    }

    /// The cursor query; `redacted` puts a `?` in place of the watermark
    /// literal — the statement text telemetry records (never a value).
    fn cursor_query_with(&self, redacted: bool) -> String {
        match &self.cursor_column {
            None => self.query.clone(),
            Some(col) => {
                let ident = format!("`{}`", col.replace('`', "``"));
                let bound = self.resume_value.as_ref().or(self.initial_value.as_ref());
                let where_clause = match bound {
                    Some(_) if redacted => format!(" WHERE _ls.{ident} > ?"),
                    Some(v) => format!(" WHERE _ls.{ident} > '{}'", v.replace('\'', "''").replace('\\', "\\\\")),
                    None => String::new(),
                };
                format!("SELECT * FROM ({}) AS _ls{where_clause} ORDER BY _ls.{ident} ASC", self.query)
            }
        }
    }

    /// Starts the query on first use, so the resume watermark (delivered after
    /// `configure`) is folded in.
    async fn ensure_stream(&mut self) -> Result<()> {
        if self.stream.is_some() || self.exhausted {
            return Ok(());
        }
        let query = self.cursor_query();
        let conn = self.conn.take().context("mysql source: not configured")?;
        // One span for the query's execution, up to its first row.
        let text = loadsmith_plugin_sdk::telemetry::enabled().then(|| self.cursor_query_with(true));
        let stream: RowStream = op("SELECT", &self.namespace, None, text.as_deref(), async {
            query.stream::<Row, _>(conn).await.context("query failed")
        })
        .await?;
        self.stream = Some(stream);
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
        self.cursor.watermark().cloned().map(serde_json::Value::String)
    }

    async fn configure(&mut self, config: loadsmith_plugin_sdk::PluginConfig) -> Result<()> {
        // Unknown keys (at any depth, flattened blocks included) follow the
        // operator's policy: an error by default, a warning with
        // `--ignore-unknown-keys`.
        let cfg: MysqlConfig = config.parse()?;

        let mut conn = crate::conn::connect(&cfg.conn).await?;

        // Schema inference via a prepared statement; the incremental wrapper
        // preserves the column set, so the base query's columns are authoritative.
        let ns = cfg.conn.dbname.as_str();
        let stmt = op("PREPARE", ns, None, Some(&cfg.query), async {
            conn.prep(&cfg.query).await.context("failed to prepare query")
        })
        .await?;
        let schema = crate::types::columns_to_schema(stmt.columns().as_ref(), cfg.timestamp_unit.arrow());
        conn.close(stmt).await.context("closing the prepared statement")?;

        if let Some(inc) = &cfg.incremental {
            let idx = cursor_index(&inc.cursor_column, &schema)?;
            self.cursor_idx = Some(idx);
            // The column's own name: the cursor query quotes it.
            self.cursor_column = Some(schema.field(idx).name().clone());
            self.initial_value = inc.initial_value.clone();
        }

        self.namespace = cfg.conn.dbname.clone();
        self.query = cfg.query;
        self.schema = Some(Arc::new(schema));
        self.batch_size = cfg.batch_size.max(1);
        self.conn = Some(conn);
        Ok(())
    }

    async fn schema(&mut self) -> Result<Schema> {
        Ok(self.schema.as_ref().context("mysql source: not configured")?.as_ref().clone())
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        self.ensure_stream().await?;
        let Some(stream) = self.stream.as_mut() else {
            self.cursor.end();
            return Ok(None);
        };

        let mut rows: Vec<Row> = Vec::with_capacity(self.batch_size);
        while rows.len() < self.batch_size {
            match stream.try_next().await.context("row fetch failed")? {
                Some(row) => rows.push(row),
                None => {
                    // End of the result set: drop the stream (and with it the
                    // connection).
                    self.stream = None;
                    self.exhausted = true;
                    break;
                }
            }
        }
        if rows.is_empty() {
            self.cursor.end();
            return Ok(None);
        }

        // Track the watermark: rows are ordered by the cursor ascending, so the
        // chunk's values are read back from its last row.
        if let Some(idx) = self.cursor_idx {
            self.cursor.batch(rows.iter().rev().filter_map(|r| r.as_ref(idx)).filter_map(crate::types::value_text));
        }

        let schema = self.schema.as_ref().context("mysql source: not configured")?.clone();
        let batch = crate::types::rows_to_batch(&rows, &schema).context("RecordBatch build failed")?;
        Ok(Some(batch))
    }

    async fn cancel(&mut self) {
        // A running query's stream owns the connection: dropping it closes the
        // socket, and the server ends the session. Before the query starts, a
        // polite COM_QUIT is worth a short wait, never more.
        self.stream = None;
        if let Some(conn) = self.conn.take() {
            let _ = tokio::time::timeout(CANCEL_DISCONNECT_TIMEOUT, conn.disconnect()).await;
        }
    }
}

/// The index of the incremental cursor among the query's columns. MySQL
/// column names are case-insensitive, so an exact match wins, then a
/// case-insensitive one; a name that is not a column fails (it would
/// otherwise silently never advance the watermark).
fn cursor_index(cursor: &str, schema: &Schema) -> Result<usize> {
    let fields = schema.fields();
    if let Some(i) = fields.iter().position(|f| f.name() == cursor) {
        return Ok(i);
    }
    if let Some(i) = fields.iter().position(|f| f.name().eq_ignore_ascii_case(cursor)) {
        return Ok(i);
    }
    let names = fields.iter().map(|f| format!("`{}`", f.name())).collect::<Vec<_>>().join(", ");
    bail!("incremental.cursor_column `{cursor}` is not a column of the query; its columns are {names}")
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
        assert_eq!(cfg.timestamp_unit, TimestampUnit::Ms);
    }

    #[test]
    fn config_deserializes_incremental_and_tls() {
        let json = serde_json::json!({
            "dbname": "lab", "user": "lab", "password": "lab",
            "query": "SELECT id, updated_at FROM t",
            "tls": { "mode": "require" },
            "timestamp_unit": "us",
            "incremental": { "cursor_column": "updated_at", "initial_value": "2026-01-01" }
        });
        let cfg: MysqlConfig = serde_json::from_value(json).unwrap();
        assert!(cfg.conn.tls.is_some());
        assert_eq!(cfg.timestamp_unit, TimestampUnit::Us);
        assert_eq!(cfg.incremental.unwrap().cursor_column, "updated_at");
    }

    #[test]
    fn unknown_keys_are_found_in_the_flattened_connection_block() {
        use loadsmith_plugin_sdk::{PluginConfig, UnknownKeys};
        let block = serde_json::json!({
            "dbname": "lab", "user": "lab", "password": "lab", "query": "SELECT 1",
            "prot": 3306,
            "tls": { "mode": "require", "root_crt": "x" }
        });
        let err = PluginConfig::new(block.clone(), UnknownKeys::Error)
            .parse::<MysqlConfig>()
            .unwrap_err()
            .to_string();
        assert!(err.contains("`prot`") && err.contains("`tls.root_crt`"), "{err}");
        assert!(PluginConfig::new(block, UnknownKeys::Warn).parse::<MysqlConfig>().is_ok());
    }

    #[test]
    fn cursor_column_is_validated_case_insensitively() {
        use arrow_schema::{DataType, Field};
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, true), Field::new("Updated_At", DataType::Utf8, true)]);
        assert_eq!(cursor_index("Updated_At", &schema).unwrap(), 1);
        assert_eq!(cursor_index("updated_at", &schema).unwrap(), 1);
        let e = cursor_index("updatedat", &schema).unwrap_err().to_string();
        assert!(e.contains("not a column of the query") && e.contains("`Updated_At`"), "{e}");
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
    fn telemetry_statement_text_never_carries_the_watermark() {
        let mut p = plugin_with("SELECT id, updated_at FROM t", Some("updated_at"));
        p.resume_value = Some("2026-06-09 08:00:00".into());
        let text = p.cursor_query_with(true);
        assert!(!text.contains("2026"), "{text}");
        assert!(text.contains("WHERE _ls.`updated_at` > ? ORDER BY"), "{text}");
    }

    #[test]
    fn cursor_query_resume_priority_and_escaping() {
        let mut p = plugin_with("SELECT c FROM t", Some("we`ird"));
        p.initial_value = Some("2000-01-01".into());
        p.resume_value = Some("o'brien\\".into());
        let q = p.cursor_query();
        assert!(q.contains("`we``ird`"));
        assert!(q.contains("> 'o''brien\\\\'"), "{q}");
        assert!(!q.contains("2000-01-01"));
    }
}
