use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use loadsmith_plugin_sdk::{FlowGate, Resumption, SourcePlugin, TieSafeCursor};
use crate::conn::ConnectionConfig;
use crate::parallel::{Item, ParallelConfig, Partition, ResumeSnapshot, ResumeToken, Split, TableQuery};
use crate::read::{Fetch, Reading, RowReader, Rows};
use crate::telemetry::op;
use crate::types::TextRows;
use crate::vocab::{config_enum, TimestampUnit};
use serde::Deserialize;
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_postgres::Client;

// ── Config structs ────────────────────────────────────────────────────────────

config_enum! {
    /// How a type with a native Arrow form that some consumers can't read
    /// (`uuid_as`, `interval_as`) is emitted.
    #[derive(Default)]
    enum NativeOrText for "uuid_as` / `interval_as" {
        /// The native Arrow type (the default).
        #[default]
        Native = "native",
        /// The database's own text.
        Text = "text",
    }
    retired {}
}

config_enum! {
    /// Isolation level of the cursor transaction.
    enum TransactionIsolation for "transaction_isolation" {
        ReadCommitted = "read-committed",
        RepeatableRead = "repeatable-read",
        Serializable = "serializable",
    }
    retired {}
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

    /// How rows are read: `copy` (the default) or `cursor` (see
    /// [`crate::read`]).
    #[serde(default)]
    fetch: Fetch,

    /// Isolation level for the cursor transaction — source-specific (the
    /// destination opens its own transaction), so it stays here.
    transaction_isolation: Option<TransactionIsolation>,

    #[serde(default)]
    incremental: Option<IncrementalConfig>,

    /// Unit of the emitted timestamps (default `ms`; `us` keeps Postgres' full
    /// precision, `ns` is µs × 1000 within 1677–2262).
    #[serde(default)]
    timestamp_unit: TimestampUnit,

    /// Scale of a `numeric` without a declared precision, emitted as
    /// `Decimal128(38, number_scale)` (default 10). A value with more decimal
    /// places, more digits, or `NaN`/`Infinity` fails the run.
    #[serde(default = "default_number_scale")]
    number_scale: u8,

    /// `uuid` as `FixedSizeBinary(16)` with the `arrow.uuid` extension
    /// (`native`, the default: Parquet's UUID type) or as its text (`text`,
    /// for consumers that show a UUID column as bytes).
    #[serde(default)]
    uuid_as: NativeOrText,

    /// `interval` as `Interval(MonthDayNano)` (`native`, the default) or as
    /// Postgres' text in the session's IntervalStyle (`text`).
    #[serde(default)]
    interval_as: NativeOrText,

    /// Partitioned full load: `workers` connections read disjoint page
    /// ranges of the query's table in one snapshot (see [`crate::parallel`]).
    #[serde(default)]
    parallel: Option<ParallelConfig>,
}

/// The output formats the text decoder parses, pinned on every session.
const SESSION_PINS: &str = "SET DateStyle = 'ISO'; SET bytea_output = 'hex'";
/// Added to the pins when intervals are decoded (`interval_as: native`).
const INTERVAL_PIN: &str = "; SET IntervalStyle = 'iso_8601'";

fn default_number_scale() -> u8 {
    crate::types::DEFAULT_NUMBER_SCALE as u8
}

fn default_batch_size() -> usize {
    1000
}

/// Upper bound on the cleanup in `cancel()`.
const CANCEL_ROLLBACK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

// ── Plugin ────────────────────────────────────────────────────────────────────

/// What the fetch task hands to `next_batch`: one batch of rows (empty at
/// the end of the query), or the error that stopped it.
type Fetched = Result<Rows>;

/// A partitioned load: how it reads, and how far it got.
struct Partitioned {
    query: TableQuery,
    workers: usize,
    /// The partitions a new load is cut into (`parallel.partitions`).
    pieces: usize,
    split: Split,
    resume_snapshot: ResumeSnapshot,
    /// Every partition of the load (empty until the workers start).
    partitions: Vec<Partition>,
    /// The partitions whose every batch `next_batch` has returned.
    done: BTreeSet<usize>,
    /// The failed run's token, when this run continues it.
    resumed: Option<ResumeToken>,
}

pub struct PostgresSourcePlugin {
    client: Option<Arc<Client>>,
    schema: Option<Arc<Schema>>,
    batch_size: usize,
    exhausted: bool,
    /// The core's grant: the fetch task never FETCHes past it.
    gate: FlowGate,
    /// The fetch task's rows, one FETCH per message; started on the first batch.
    fetched: Option<mpsc::Receiver<Fetched>>,
    fetcher: Option<tokio::task::JoinHandle<()>>,
    /// A partitioned load, the connection config the workers connect with,
    /// and their batches.
    partitioned: Option<Partitioned>,
    conn_cfg: Option<Arc<ConnectionConfig>>,
    partition_batches: Option<mpsc::Receiver<Result<Item>>>,
    fetch: Fetch,
    /// The incremental cursor's column index.
    cursor_index: Option<usize>,

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
    /// The incremental cursor's values seen so far: the watermark reported is
    /// tie-safe (a value no later row repeats; the last one at the end).
    cursor: TieSafeCursor<String>,
    /// Cursor is declared lazily on the first batch, once the resume value is in.
    cursor_declared: bool,
    /// `db.namespace` of the telemetry spans (the database name).
    namespace: String,
    /// The pins of every session ([`SESSION_PINS`], [`INTERVAL_PIN`]).
    session_pins: String,
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
            gate: FlowGate::open(),
            fetched: None,
            fetcher: None,
            partitioned: None,
            conn_cfg: None,
            partition_batches: None,
            fetch: Fetch::Copy,
            cursor_index: None,
            query: String::new(),
            begin_sql: "BEGIN".to_string(),
            cursor_column: None,
            initial_value: None,
            resume_value: None,
            cursor: TieSafeCursor::new(),
            cursor_declared: false,
            namespace: String::new(),
            session_pins: SESSION_PINS.to_string(),
        }
    }

    /// Builds the query the source reads. For a plain (non-incremental)
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
        self.cursor_query_with(false)
    }

    /// The cursor query; `redacted` puts a `?` in place of the watermark
    /// literal — the statement text telemetry records (never a value).
    fn cursor_query_with(&self, redacted: bool) -> String {
        match &self.cursor_column {
            None => self.query.clone(),
            Some(col) => {
                let ident = format!("\"{}\"", col.replace('"', "\"\""));
                let bound = self.resume_value.as_ref().or(self.initial_value.as_ref());
                let where_clause = match bound {
                    Some(_) if redacted => format!(" WHERE _ls.{ident} > ?"),
                    Some(v) => format!(" WHERE _ls.{ident} > '{}'", v.replace('\'', "''")),
                    None => String::new(),
                };
                format!("SELECT * FROM ({}) AS _ls{where_clause} ORDER BY _ls.{ident} ASC", self.query)
            }
        }
    }

    /// Opens the transaction on first use. Deferred out of `configure` so the
    /// resume watermark (delivered after configure, via `resume_from`) is
    /// folded into the query the fetch task reads.
    async fn ensure_cursor(&mut self) -> Result<()> {
        if self.cursor_declared {
            return Ok(());
        }
        let client = self.client.as_ref().context("postgres source: not configured")?;
        let ns = self.namespace.as_str();
        let begin = self.begin_sql.as_str();
        op("BEGIN", ns, None, Some(begin), async { client.batch_execute(begin).await.context("BEGIN failed") }).await?;
        self.cursor_declared = true;
        Ok(())
    }

    /// How this source reads its queries.
    fn reading(&self) -> Result<Reading> {
        Ok(Reading {
            fetch: self.fetch,
            batch_size: self.batch_size,
            columns: self.schema.as_ref().context("postgres source: not configured")?.fields().len(),
            namespace: self.namespace.clone(),
        })
    }

    /// Starts the fetch task, which reads the next batches while `next_batch`
    /// converts the last one: the database and the conversion overlap
    /// instead of taking turns.
    fn start_fetching(&mut self) -> Result<()> {
        let client = self.client.clone().context("postgres source: not configured")?;
        let read = Read {
            how: self.reading()?,
            query: self.cursor_query(),
            // The statement text telemetry records (never a value).
            text: loadsmith_plugin_sdk::telemetry::enabled().then(|| self.cursor_query_with(true)),
        };
        // One batch waits in the channel at most; the gate bounds the rest.
        let (tx, rx) = mpsc::channel(1);
        let task = fetch_ahead(client, self.gate.clone(), read, tx);
        self.fetcher = Some(tokio::spawn(tracing::Instrument::in_current_span(task)));
        self.fetched = Some(rx);
        Ok(())
    }

    /// A partitioned load: starts the workers on the first call, then hands
    /// on their batches as they come, noting each partition they finish;
    /// `None` once every worker is done (and the snapshot's transaction is
    /// closed).
    async fn next_partitioned(&mut self) -> Result<Option<RecordBatch>> {
        if self.partition_batches.is_none() {
            self.start_partitions().await?;
        }
        loop {
            let rx = self.partition_batches.as_mut().context("no partition workers")?;
            match rx.recv().await {
                Some(item) => match item? {
                    Item::Batch(batch) => return Ok(Some(batch)),
                    Item::Done(id) => {
                        if let Some(p) = self.partitioned.as_mut() {
                            p.done.insert(id);
                        }
                    }
                },
                None => {
                    self.exhausted = true;
                    self.partition_batches = None;
                    if let Some(client) = &self.client {
                        let _ = client.batch_execute("COMMIT").await;
                    }
                    return Ok(None);
                }
            }
        }
    }

    /// Exports the snapshot on the source's connection, cuts the table (or
    /// takes the failed run's partitions, when continuing it), then starts
    /// the workers in the snapshot on the partitions not yet done.
    async fn start_partitions(&mut self) -> Result<()> {
        let client = self.client.as_ref().context("postgres source: not configured")?;
        let plan = self.partitioned.as_mut().context("not partitioned")?;
        let (query, workers, pieces) = (plan.query.clone(), plan.workers, plan.pieces);
        let (snapshot, pages) = crate::parallel::export_snapshot(client, &query, &self.namespace).await?;
        match plan.resumed.take() {
            Some(token) => {
                plan.partitions = token.partitions;
                plan.done = token.done.into_iter().collect();
            }
            // More partitions than workers, so one that finishes early takes another.
            None => {
                plan.partitions = match &plan.split {
                    Split::Ctid => crate::parallel::page_ranges(pages, pieces),
                    Split::Key(key) => {
                        let min_max = crate::parallel::key_min_max(client, &query, key).await?;
                        crate::parallel::key_ranges(min_max, pieces)
                    }
                };
            }
        }
        let todo: std::collections::VecDeque<(usize, Partition)> =
            plan.partitions.iter().cloned().enumerate().filter(|(id, _)| !plan.done.contains(id)).collect();
        tracing::info!(
            workers,
            partitions = plan.partitions.len(),
            to_read = todo.len(),
            pages,
            split_by = ?plan.split,
            table = %query.table_ref,
            "partitioned read"
        );
        let key = match &plan.split {
            Split::Key(k) => Some(k.clone()),
            Split::Ctid => None,
        };
        let shared = Arc::new(crate::parallel::Shared {
            conn: self.conn_cfg.clone().context("not configured")?,
            query,
            key,
            snapshot,
            partitions: std::sync::Mutex::new(todo),
            reading: self.reading()?,
            schema: self.schema.clone().context("not configured")?,
            namespace: self.namespace.clone(),
            session_pins: self.session_pins.clone(),
            credits: Default::default(),
        });
        self.partition_batches = Some(crate::parallel::spawn_workers(shared, workers, self.gate.clone()));
        Ok(())
    }
}

/// What the fetch task reads, and how.
struct Read {
    how: Reading,
    query: String,
    text: Option<String>,
}

/// Reads the query batch by batch, each only once the core granted it, and
/// commits the read transaction when it is exhausted.
async fn fetch_ahead(client: Arc<Client>, mut gate: FlowGate, read: Read, tx: mpsc::Sender<Fetched>) {
    let ns = read.how.namespace.clone();
    let opened = RowReader::open(client.clone(), &read.how, &read.query, "ls_cursor", read.text.as_deref()).await;
    let mut reader = match opened {
        Ok(r) => r,
        Err(e) => {
            let _ = tx.send(Err(e)).await;
            return;
        }
    };
    let mut seq = 0;
    loop {
        seq += 1;
        gate.wait_for(seq).await;
        let rows = reader.next().await;
        let last = rows.as_ref().map_or(true, |r| r.is_empty());
        if matches!(&rows, Ok(r) if r.is_empty()) {
            let _ = op("COMMIT", &ns, None, Some("COMMIT"), async {
                client.batch_execute("COMMIT").await.context("COMMIT failed")
            })
            .await;
        }
        if tx.send(rows).await.is_err() || last {
            return;
        }
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

    async fn resume_at(&mut self, token: serde_json::Value) -> Resumption {
        let Some(plan) = self.partitioned.as_mut() else {
            return Resumption::StartOver("this read is not partitioned: it starts over".into());
        };
        let token = match ResumeToken::parse(token, &plan.split) {
            Ok(t) => t,
            Err(e) => return Resumption::StartOver(format!("cannot continue the failed run ({e:#}); the partitioned load starts over")),
        };
        let left = token.partitions.len() - token.done.len();
        if plan.resume_snapshot != ResumeSnapshot::New {
            return Resumption::StartOver(format!(
                "the failed run's snapshot ended with its transaction: the partitioned load starts over \
                 (`parallel.resume_snapshot: new` with a key `split_by` reads the {left} partitions not yet \
                 done at a new snapshot instead)"
            ));
        }
        plan.resumed = Some(token);
        Resumption::Continue(format!("continuing the failed run: the {left} partitions not yet done, at a new snapshot"))
    }

    fn current_resume_token(&self) -> Option<serde_json::Value> {
        let plan = self.partitioned.as_ref()?;
        if plan.partitions.is_empty() {
            return None;
        }
        serde_json::to_value(ResumeToken::new(&plan.split, &plan.partitions, &plan.done)).ok()
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
        let cfg: PostgresConfig = config.parse()?;
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
                partitions: Vec::new(),
                done: BTreeSet::new(),
                resumed: None,
            });
        }
        if cfg.number_scale > 38 {
            anyhow::bail!("number_scale must be at most 38 (got {})", cfg.number_scale);
        }

        // ── Connect (shared TLS/connection layer) ──────────────────────────
        let client = crate::conn::connect(&cfg.conn).await?;
        // Rows come back over the text protocol; pin the output formats the
        // decoder parses (after any user `session` GUCs, so they can't break it).
        // DateStyle 'ISO' only sets the output style, keeping the user's
        // DMY/MDY input order. TimeZone is deliberately left alone: timestamptz
        // offsets are parsed, and changing it would alter the query's semantics.
        let ns = cfg.conn.dbname.as_str();
        self.session_pins = match cfg.interval_as {
            NativeOrText::Native => format!("{SESSION_PINS}{INTERVAL_PIN}"),
            NativeOrText::Text => SESSION_PINS.to_string(),
        };
        let pin = self.session_pins.as_str();
        op("SET", ns, None, Some(pin), async { client.batch_execute(pin).await.context("pinning the output formats") })
            .await?;

        // ── Schema inference via prepare() ────────────────────────────────
        // The incremental wrapper (`SELECT * FROM (<query>) …`) preserves the
        // column set, so the schema is inferred from the original query here.

        let stmt = op("PREPARE", ns, None, Some(&cfg.query), async {
            client.prepare(&cfg.query).await.context("failed to prepare query")
        })
        .await?;
        let map = crate::types::TypeMap {
            uuid_text: cfg.uuid_as == NativeOrText::Text,
            interval_text: cfg.interval_as == NativeOrText::Text,
            ..crate::types::TypeMap::new(cfg.timestamp_unit.arrow(), cfg.number_scale as i8)
        };
        let schema = Arc::new(crate::types::columns_to_schema(stmt.columns(), &map));

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
            check_cursor_column(&inc.cursor_column, &schema)?;
            self.cursor_index = schema.fields().iter().position(|f| f.name() == &inc.cursor_column);
            self.cursor_column = Some(inc.cursor_column.clone());
            self.initial_value = inc.initial_value.clone();
        }
        self.namespace = cfg.conn.dbname.clone();
        self.conn_cfg = Some(Arc::new(cfg.conn));
        self.query = cfg.query;
        self.schema = Some(schema);
        self.batch_size = cfg.batch_size;
        self.fetch = cfg.fetch;
        self.client = Some(Arc::new(client));

        Ok(())
    }

    async fn schema(&mut self) -> Result<Schema> {
        Ok(self.schema.as_ref().context("postgres source: not configured")?.as_ref().clone())
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.exhausted {
            return Ok(None);
        }
        if self.partitioned.is_some() {
            return self.next_partitioned().await;
        }

        // Declare the cursor on first use, now that the resume watermark is in.
        if self.fetched.is_none() {
            self.ensure_cursor().await?;
            self.start_fetching()?;
        }

        let schema = self.schema.as_ref().context("postgres source: not configured")?.clone();
        let fetched = self.fetched.as_mut().expect("started above");
        let rows = fetched.recv().await.context("the fetch task stopped")??;

        if rows.is_empty() {
            // The fetch task closed the cursor and committed.
            self.exhausted = true;
            self.cursor.end();
            return Ok(None);
        }

        // Track the watermark: rows are ordered by the cursor ascending, so the
        // batch's values are read back from its last row.
        if let Some(i) = self.cursor_index {
            self.cursor.batch((0..rows.len()).rev().filter_map(|r| rows.cell(r, i)).map(str::to_string));
        }

        let batch =
            crate::types::rows_to_batch_text(&rows, &schema).context("RecordBatch build failed")?;

        Ok(Some(batch))
    }

    async fn cancel(&mut self) {
        // The SDK may have dropped an in-flight `next_batch` before calling
        // this; its FETCH still runs server-side, and this ROLLBACK queues
        // behind it. Wait briefly, then drop the client: the server rolls the
        // (read-only) transaction back when the connection closes. A bare
        // ROLLBACK also closes the cursor, and unlike `CLOSE …; ROLLBACK` it
        // still runs when the transaction is already aborted or the cursor was
        // never declared.
        if let Some(fetcher) = self.fetcher.take() {
            fetcher.abort();
        }
        // Workers stop at their next send; their connections close with them.
        self.partition_batches = None;
        if let Some(client) = self.client.take() {
            let rollback = client.batch_execute("ROLLBACK");
            let _ = tokio::time::timeout(CANCEL_ROLLBACK_TIMEOUT, rollback).await;
        }
    }
}

/// The incremental cursor must be one of the query's output columns (the
/// cursor query quotes it, so the name is matched exactly). Without this
/// check a typo would silently reload everything on every run.
fn check_cursor_column(cursor: &str, schema: &Schema) -> Result<()> {
    if schema.fields().iter().any(|f| f.name() == cursor) {
        return Ok(());
    }
    let names = schema.fields().iter().map(|f| format!("`{}`", f.name())).collect::<Vec<_>>().join(", ");
    let hint = schema
        .fields()
        .iter()
        .find(|f| f.name().eq_ignore_ascii_case(cursor))
        .map(|f| format!(" (did you mean `{}`?)", f.name()))
        .unwrap_or_default();
    anyhow::bail!("incremental.cursor_column `{cursor}` is not a column of the query{hint}; its columns are {names}")
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
    fn telemetry_statement_text_never_carries_the_watermark() {
        let mut p = plugin_with("SELECT id, updated_at FROM t", Some("updated_at"));
        p.resume_value = Some("2026-05-01 10:00:00".into());
        assert!(p.cursor_query().contains("'2026-05-01 10:00:00'"));
        let text = p.cursor_query_with(true);
        assert!(!text.contains("2026"), "{text}");
        assert!(text.contains("WHERE _ls.\"updated_at\" > ?"), "{text}");
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
    fn cursor_column_must_be_a_query_column() {
        let schema = Schema::new(vec![arrow_schema::Field::new("updated_at", arrow_schema::DataType::Utf8, true)]);
        assert!(check_cursor_column("updated_at", &schema).is_ok());
        let e = check_cursor_column("Updated_At", &schema).unwrap_err().to_string();
        assert!(e.contains("did you mean `updated_at`?") && e.contains("its columns are `updated_at`"), "{e}");
    }

    #[test]
    fn timestamp_unit_defaults_to_ms() {
        let json = serde_json::json!({ "dbname": "d", "user": "u", "query": "SELECT 1" });
        let cfg: PostgresConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.timestamp_unit, TimestampUnit::Ms);
        let json = serde_json::json!({ "dbname": "d", "user": "u", "query": "SELECT 1", "timestamp_unit": "us" });
        let cfg: PostgresConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.timestamp_unit, TimestampUnit::Us);
    }

    #[test]
    fn unknown_keys_are_found_in_the_flattened_connection_block() {
        use loadsmith_plugin_sdk::{PluginConfig, UnknownKeys};
        let block = serde_json::json!({
            "host": "localhost", "dbname": "d", "user": "u", "password": "p",
            "query": "SELECT 1",
            "hots": "x",
            "tls": { "mode": "require", "verfy": true },
            "incremental": { "cursor_column": "c", "intial_value": "1" }
        });
        let err = PluginConfig::new(block.clone(), UnknownKeys::Error)
            .parse::<PostgresConfig>()
            .unwrap_err()
            .to_string();
        for key in ["`hots`", "`tls.verfy`", "`incremental.intial_value`"] {
            assert!(err.contains(key), "{key} missing from: {err}");
        }
        // The operator's warn policy ignores them and parses the rest.
        assert!(PluginConfig::new(block, UnknownKeys::Warn).parse::<PostgresConfig>().is_ok());
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
