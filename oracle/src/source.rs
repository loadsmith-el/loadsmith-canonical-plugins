use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use loadsmith_plugin_sdk::{FlowGate, Resumption, SourcePlugin, TieSafeCursor};
use oracledb::{Connection, DbType, Row};
use serde::Deserialize;
use std::sync::Arc;

use crate::conn::ConnectionConfig;
use crate::parallel::{Item, ParallelConfig, Partition, ResumeSnapshot, ResumeToken, Split, TableQuery};
use std::collections::BTreeSet;
use crate::telemetry::{op, DbOp};
use crate::types::{self, quote_ident, typed_expr};

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

    /// Scale of a `NUMBER` without a declared precision (a bare `NUMBER`
    /// column, `SUM(x)`, …), emitted as `Decimal128(38, number_scale)`
    /// (default 10). A value with more decimal places fails the run.
    #[serde(default = "default_number_scale")]
    number_scale: u8,

    /// `INTERVAL` columns as `Interval(MonthDayNano)` (`native`, the
    /// default) or as Oracle's `TO_CHAR` text (`text`).
    #[serde(default)]
    interval_as: crate::vocab::NativeOrText,

    /// Partitioned full load: `workers` connections read disjoint ROWID
    /// ranges of the query's table at one SCN (see [`crate::parallel`]).
    #[serde(default)]
    parallel: Option<ParallelConfig>,
}

fn default_number_scale() -> u8 {
    types::DEFAULT_NUMBER_SCALE as u8
}

fn default_batch_size() -> usize {
    1000
}

// ── Plugin ────────────────────────────────────────────────────────────────────

/// A partitioned load: how it reads, and how far it got.
struct Partitioned {
    query: TableQuery,
    workers: usize,
    /// The partitions a new load is cut into (`parallel.partitions`).
    pieces: usize,
    split: Split,
    resume_snapshot: ResumeSnapshot,
    /// The SCN the partitions are read at (once the workers start).
    scn: u64,
    /// Every partition of the load (empty until the workers start).
    partitions: Vec<Partition>,
    /// The partitions whose every batch `next_batch` has returned.
    done: BTreeSet<usize>,
    /// The failed run's token, when this run continues it; `scn` 0 there
    /// means a new SCN.
    resumed: Option<ResumeToken>,
}

/// Oracle source. The query's cursor stays open across batches, owned by a
/// fetch thread that reads `batch_size` rows per driver round trip, ahead of
/// the batch being built as far as the core's credit allows (one chunk without
/// credit): memory holds a few batches whatever the result set's size, and the
/// database fetch overlaps the conversion. The fetch thread only decodes the
/// wire; the values are read natively when the batch is built.
pub struct OracleSourcePlugin {
    conn: Option<Connection>,
    schema: Option<Arc<Schema>>,
    batch_size: usize,

    /// The user's base query (inner SELECT).
    query: String,
    /// The projected SELECT list (zoned timestamps in `SYS_EXTRACT_UTC`,
    /// LONG/INTERVAL columns in `TO_CHAR`).
    select_list: String,

    cursor_column: Option<String>,
    cursor_idx: Option<usize>,
    cursor_type: Option<&'static DbType>,
    initial_value: Option<String>,
    resume_value: Option<String>,
    /// The incremental cursor's values seen so far: the watermark reported is
    /// tie-safe (a value no later row repeats; the last one at the end).
    cursor: TieSafeCursor<String>,

    /// Rows from the fetch thread (which owns the open cursor), from the first
    /// batch until end of data. Dropping it stops the thread.
    fetched: Option<std::sync::mpsc::Receiver<Result<Vec<Row>>>>,
    /// The core's grant: the fetch thread never reads a chunk past it.
    gate: FlowGate,
    /// A partitioned load, the connection config the workers connect with,
    /// and their batches.
    partitioned: Option<Partitioned>,
    conn_cfg: Option<Arc<ConnectionConfig>>,
    partition_batches: Option<std::sync::mpsc::Receiver<Result<Item>>>,
    /// The `SELECT` span: open while the cursor is, so it covers the fetch.
    select_span: tracing::Span,
    returned_rows: u64,
    exhausted: bool,
    /// `db.namespace` of the telemetry spans.
    namespace: String,
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
            cursor: TieSafeCursor::new(),
            fetched: None,
            gate: FlowGate::open(),
            partitioned: None,
            conn_cfg: None,
            partition_batches: None,
            select_span: tracing::Span::none(),
            returned_rows: 0,
            exhausted: false,
            namespace: String::new(),
        }
    }

    /// The query actually run: the projected column list over the user query as a
    /// subquery, optionally filtered `> watermark` and ordered by the cursor so
    /// the last row seen is the new high watermark.
    fn build_query(&self) -> String {
        self.build_query_with(false)
    }

    /// The query; `redacted` puts a `?` in place of the watermark literal —
    /// the statement text telemetry records (never a value).
    fn build_query_with(&self, redacted: bool) -> String {
        let base = format!("SELECT {} FROM ({}) \"_LS\"", self.select_list, self.query);
        match &self.cursor_column {
            None => base,
            Some(col) => {
                let ident = quote_ident(col);
                let bound = self.resume_value.as_ref().or(self.initial_value.as_ref());
                let where_clause = match bound {
                    Some(_) if redacted => format!(" WHERE \"_LS\".{ident} > ?"),
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

    /// Executes the query, deferred until the first batch so the resume
    /// watermark (delivered after `configure`) is folded in. The driver fetches
    /// `batch_size` rows per round trip. Synchronous driver work, run on a
    /// blocking thread.
    fn open_cursor(&mut self) -> Result<()> {
        let query = self.build_query();
        let conn = self.conn.as_ref().context("not configured")?;
        let fetch = u32::try_from(self.batch_size).unwrap_or(u32::MAX);
        // One span for the query: execution now, the fetch of every row until
        // the cursor is exhausted (the span stays open meanwhile).
        let text = loadsmith_plugin_sdk::telemetry::enabled().then(|| self.build_query_with(true));
        let select = DbOp::new("SELECT", &self.namespace, None, text.as_deref());
        self.select_span = select.span().clone();
        let cursor = tokio::task::block_in_place(|| {
            select.run(|| {
                conn.statement(&query)
                    .and_then(|b| b.fetch_array_size(fetch).prefetch_rows(fetch).build())
                    .and_then(|stmt| stmt.query(&[]))
                    .map_err(|e| anyhow::anyhow!("query failed: {e}"))
            })
        })?;
        let batch_size = self.batch_size;
        // The core's credit bounds the chunks read ahead; the channel holds one
        // (the only bound when an older core sends no credit).
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let gate = self.gate.clone();
        let runtime = tokio::runtime::Handle::current();
        std::thread::Builder::new()
            .name("oracle-fetch".into())
            .spawn(move || fetch_loop(cursor, batch_size, gate, runtime, tx))
            .context("spawn the fetch thread")?;
        self.fetched = Some(rx);
        Ok(())
    }

    /// A partitioned load: starts the workers on the first call, then hands
    /// on their batches as they come, noting each partition they finish;
    /// `None` once every worker is done.
    fn next_partitioned(&mut self) -> Result<Option<RecordBatch>> {
        if self.partition_batches.is_none() {
            self.start_partitions()?;
        }
        loop {
            let rx = self.partition_batches.as_ref().context("no partition workers")?;
            match tokio::task::block_in_place(|| rx.recv()) {
                Ok(item) => match item? {
                    Item::Batch(batch) => {
                        self.returned_rows += batch.num_rows() as u64;
                        return Ok(Some(batch));
                    }
                    Item::Done(id) => {
                        if let Some(p) = self.partitioned.as_mut() {
                            p.done.insert(id);
                        }
                    }
                },
                Err(_) => {
                    self.exhausted = true;
                    self.partition_batches = None;
                    tracing::info!(rows = self.returned_rows, "partitioned read complete");
                    return Ok(None);
                }
            }
        }
    }

    /// Takes the snapshot SCN and cuts the table (or takes the failed run's
    /// SCN and partitions, when continuing it) on the configure connection,
    /// then starts the workers on the partitions not yet done.
    fn start_partitions(&mut self) -> Result<()> {
        let conn = self.conn.as_ref().context("not configured")?;
        let plan = self.partitioned.as_mut().context("not partitioned")?;
        let (query, workers, pieces) = (plan.query.clone(), plan.workers, plan.pieces);
        tokio::task::block_in_place(|| -> Result<()> {
            match plan.resumed.take() {
                Some(token) => {
                    plan.scn = if token.scn > 0 { token.scn } else { crate::parallel::snapshot_scn(conn, &query)? };
                    plan.partitions = token.partitions;
                    plan.done = token.done.into_iter().collect();
                }
                // More partitions than workers, so one that finishes early takes another.
                None => {
                    plan.scn = crate::parallel::snapshot_scn(conn, &query)?;
                    plan.partitions = match &plan.split {
                        Split::Rowid => {
                            crate::parallel::ranges_between(&crate::parallel::sample_bounds(conn, &query, pieces)?)
                        }
                        Split::Key(key) => crate::parallel::key_ranges(
                            crate::parallel::key_min_max(conn, &query, key, plan.scn)?,
                            pieces,
                        ),
                    };
                }
            }
            Ok(())
        })?;
        let todo: std::collections::VecDeque<(usize, Partition)> =
            plan.partitions.iter().cloned().enumerate().filter(|(id, _)| !plan.done.contains(id)).collect();
        tracing::info!(
            workers,
            partitions = plan.partitions.len(),
            to_read = todo.len(),
            scn = plan.scn,
            split_by = ?plan.split,
            table = %query.table,
            "partitioned read"
        );
        let key = match &plan.split {
            Split::Key(k) => Some(k.clone()),
            Split::Rowid => None,
        };
        let shared = Arc::new(crate::parallel::Shared {
            conn: self.conn_cfg.clone().context("not configured")?,
            query,
            scn: plan.scn,
            select_list: self.select_list.clone(),
            key,
            partitions: std::sync::Mutex::new(todo),
            batch_size: self.batch_size,
            schema: self.schema.clone().context("not configured")?,
            namespace: self.namespace.clone(),
            started: Default::default(),
        });
        let rx = crate::parallel::spawn_workers(shared, workers, self.gate.clone(), tokio::runtime::Handle::current())?;
        self.partition_batches = Some(rx);
        Ok(())
    }

    /// The next up-to-`batch_size` rows (empty at the end of data).
    fn next_rows(&mut self) -> Result<Vec<Row>> {
        let rx = self.fetched.as_ref().context("no open cursor")?;
        match tokio::task::block_in_place(|| rx.recv()) {
            Ok(rows) => rows,
            Err(_) => anyhow::bail!("the fetch thread stopped without a result"),
        }
    }
}

/// Reads the cursor `batch_size` rows at a time, each chunk only once the core
/// granted it, until the end of data (sent as an empty chunk) or an error;
/// stops early when the receiver is gone. The rows go out as fetched: their
/// values are converted on the batch-building side.
fn fetch_loop(
    mut cursor: oracledb::Cursor,
    batch_size: usize,
    mut gate: FlowGate,
    runtime: tokio::runtime::Handle,
    tx: std::sync::mpsc::SyncSender<Result<Vec<Row>>>,
) {
    for seq in 1.. {
        runtime.block_on(gate.wait_for(seq));
        let mut chunk = Vec::with_capacity(batch_size);
        let mut failed = None;
        for row in cursor.by_ref().take(batch_size) {
            match row.map_err(|e| anyhow::anyhow!("row fetch failed: {e}")) {
                Ok(row) => chunk.push(row),
                Err(e) => {
                    failed = Some(e);
                    break;
                }
            }
        }
        let last = failed.is_some() || chunk.is_empty();
        let sent = match failed {
            Some(e) => tx.send(Err(e)),
            None => tx.send(Ok(chunk)),
        };
        if last || sent.is_err() {
            return;
        }
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

    async fn resume_at(&mut self, token: serde_json::Value) -> Resumption {
        let Some(plan) = self.partitioned.as_mut() else {
            return Resumption::StartOver("this read is not partitioned: it starts over".into());
        };
        let mut token = match ResumeToken::parse(token, &plan.split) {
            Ok(t) => t,
            Err(e) => return Resumption::StartOver(format!("cannot continue the failed run ({e:#}); the partitioned load starts over")),
        };
        let Some(conn) = self.conn.as_ref() else {
            return Resumption::StartOver("not configured: the partitioned load starts over".into());
        };
        let left = token.partitions.len() - token.done.len();
        let how = match tokio::task::block_in_place(|| crate::parallel::scn_readable(conn, &plan.query, token.scn)) {
            Ok(()) => format!("continuing the failed run: the {left} partitions not yet done, at its SCN {}", token.scn),
            Err(e) if plan.resume_snapshot == ResumeSnapshot::New => {
                token.scn = 0;
                format!("continuing the failed run: the {left} partitions not yet done, at a new SCN (its SCN {e})")
            }
            Err(e) => {
                return Resumption::StartOver(format!(
                    "the failed run's SCN {e}: the partitioned load starts over \
                     (`parallel.resume_snapshot: new` with a key `split_by` reads the {left} partitions not yet \
                     done at a new SCN instead)"
                ))
            }
        };
        plan.resumed = Some(token);
        Resumption::Continue(how)
    }

    fn current_resume_token(&self) -> Option<serde_json::Value> {
        let plan = self.partitioned.as_ref()?;
        if plan.partitions.is_empty() {
            return None;
        }
        serde_json::to_value(ResumeToken::new(plan.scn, &plan.split, &plan.partitions, &plan.done)).ok()
    }

    async fn resume_from(&mut self, cursor_value: Option<serde_json::Value>) {
        self.resume_value = cursor_value.map(|v| match v {
            serde_json::Value::String(s) => s,
            other => other.to_string(),
        });
    }

    fn attach_flow(&mut self, gate: FlowGate) {
        self.gate = gate;
    }

    fn current_watermark(&self) -> Option<serde_json::Value> {
        self.cursor.watermark().cloned().map(serde_json::Value::String)
    }

    async fn configure(&mut self, config: loadsmith_plugin_sdk::PluginConfig) -> Result<()> {
        // Unknown keys (at any depth, flattened blocks included) follow the
        // operator's policy: an error by default, a warning with
        // `--ignore-unknown-keys`.
        let cfg: OracleConfig = config.parse()?;
        if let Some(p) = &cfg.parallel {
            let (workers, pieces) = p.sizes()?;
            if cfg.incremental.is_some() {
                anyhow::bail!(
                    "`parallel` reads a full load; it cannot be combined with `incremental` (batches would leave \
                     out of cursor order)"
                );
            }
            self.partitioned = Some(Partitioned {
                query: TableQuery::parse(&cfg.query)?,
                workers,
                pieces,
                split: p.split()?,
                resume_snapshot: p.resume_snapshot,
                scn: 0,
                partitions: Vec::new(),
                done: BTreeSet::new(),
                resumed: None,
            });
        }
        if cfg.number_scale > 38 {
            anyhow::bail!("number_scale must be at most 38 (got {})", cfg.number_scale);
        }

        let (conn, cols) = tokio::task::block_in_place(|| -> Result<_> {
            let conn = crate::conn::connect(&cfg.conn)?;
            // Describe the column set without fetching data.
            let describe_sql = format!("SELECT * FROM ({}) \"_LS\" WHERE 1 = 0", cfg.query);
            let cols = op("SELECT", crate::conn::namespace(&cfg.conn), None, Some(&describe_sql), || {
                let rs = conn
                    .query(&describe_sql, &[])
                    .map_err(|e| anyhow::anyhow!("schema describe failed: {e}"))?;
                Ok(types::describe(rs.columns()))
            })?;
            Ok((conn, cols))
        })?;

        let (select_list, schema) = types::projected_select(
            &cols,
            cfg.timestamp_unit.arrow(),
            cfg.number_scale as i8,
            cfg.interval_as == crate::vocab::NativeOrText::Text,
        );

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

        self.namespace = crate::conn::namespace(&cfg.conn).to_string();
        self.conn_cfg = Some(Arc::new(cfg.conn));
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
        if self.exhausted {
            return Ok(None);
        }
        if self.partitioned.is_some() {
            return self.next_partitioned();
        }
        if self.fetched.is_none() {
            self.open_cursor()?;
        }
        let rows = self.next_rows()?;
        if rows.is_empty() {
            self.exhausted = true;
            self.cursor.end();
            self.fetched = None;
            self.select_span.record("db.response.returned_rows", self.returned_rows);
            self.select_span = tracing::Span::none();
            return Ok(None);
        }
        self.returned_rows += rows.len() as u64;
        let schema = self.schema.as_ref().context("oracle source: not configured")?.clone();
        let slice = &rows[..];

        // Track the watermark: rows are ordered by the cursor ascending, so the
        // chunk's values are read back from its last row. (A value that can't
        // be read fails the batch build below.)
        if let Some(idx) = self.cursor_idx {
            let t = self.cursor_type;
            self.cursor.batch(
                slice.iter().rev().filter_map(|r| types::read_value(r, idx).ok().and_then(|v| v.watermark(t))),
            );
        }

        let batch = types::rows_to_batch(slice, &schema).context("RecordBatch build failed")?;
        Ok(Some(batch))
    }

    async fn cancel(&mut self) {
        // Dropping the receiver stops the fetch thread (which then drops the
        // cursor), and dropping the connection closes it.
        self.fetched = None;
        self.partition_batches = None;
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
    fn telemetry_statement_text_never_carries_the_watermark() {
        let mut p = plugin_with("SELECT id, ts FROM t", Some("TS"));
        p.resume_value = Some("2026-06-09 08:00:00.000000".into());
        let text = p.build_query_with(true);
        assert!(!text.contains("2026"), "{text}");
        assert!(text.contains("WHERE \"_LS\".\"TS\" > ? ORDER BY"), "{text}");
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
