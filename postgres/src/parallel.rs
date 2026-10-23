//! Partitioned full loads (`parallel:`): `workers` connections read disjoint
//! page ranges of one table (`ctid`, a TID range scan since PostgreSQL 14),
//! all in the same snapshot — exported by the source's own transaction
//! (`pg_export_snapshot()`) and imported by every worker (`SET TRANSACTION
//! SNAPSHOT`) — so the load is one consistent read. Each worker fetches **and
//! converts** its rows; the batches leave in whatever order they finish.
//!
//! The ranges cut the table's pages (`pg_relation_size`) into `8 × workers`
//! pieces, half-open and open at the end, so every row is read exactly once
//! (rows on pages added after the count are in the last range). They sit in
//! one queue, so a worker that finishes early takes the next one.
//!
//! The core's credit bounds the batches fetched across all workers: a worker
//! takes a credit before each FETCH and gives it back when the FETCH comes
//! back empty (the end of its range).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use loadsmith_plugin_sdk::FlowGate;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_postgres::{Client, SimpleQueryMessage, SimpleQueryRow};

use crate::conn::ConnectionConfig;
use crate::telemetry::op;

/// `parallel:` of the source config.
#[derive(Debug, Deserialize)]
pub struct ParallelConfig {
    /// Connections reading at once.
    pub workers: usize,
}

/// The most workers a load may open.
pub const MAX_WORKERS: usize = 64;

/// The oldest server with TID range scans (without them every worker would
/// read the whole table).
const MIN_SERVER_VERSION: i64 = 140_000;

/// A query a partitioned read can split: `SELECT <columns> FROM <table>
/// [<alias>] [WHERE <filter>]`, one table, no joins, grouping or ordering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableQuery {
    pub columns: String,
    /// The table reference as written (`public.events`, `"Mixed"`).
    pub table_ref: String,
    pub alias: Option<String>,
    pub filter: Option<String>,
}

impl TableQuery {
    pub fn parse(query: &str) -> Result<Self> {
        let shape = || {
            anyhow::anyhow!(
                "`parallel` splits a query of the form `SELECT <columns> FROM <table> [<alias>] [WHERE <filter>]` \
                 (one table; no joins, subqueries in FROM, GROUP BY, ORDER BY, LIMIT or set operators); \
                 this query is not one"
            )
        };
        let q = query.trim().trim_end_matches(';').trim();
        let words = top_level_words(q);
        let is = |i: usize, w: &str| words.get(i).is_some_and(|(_, word)| word.eq_ignore_ascii_case(w));
        if !is(0, "SELECT") {
            return Err(shape());
        }
        let from = words.iter().position(|(_, w)| w.eq_ignore_ascii_case("FROM")).ok_or_else(shape)?;
        let columns = q[words[0].0 + 6..words[from].0].trim().to_string();
        let mut after = words[from + 1..].iter();
        let (_, table_ref) = after.next().ok_or_else(shape)?;
        let mut alias = None;
        let mut filter = None;
        if let Some((at, w)) = after.next() {
            let mut next = Some((*at, *w));
            if w.eq_ignore_ascii_case("AS") {
                next = after.next().map(|(a, w)| (*a, *w));
                alias = Some(next.ok_or_else(shape)?.1.to_string());
                next = after.next().map(|(a, w)| (*a, *w));
            } else if !w.eq_ignore_ascii_case("WHERE") {
                alias = Some(w.to_string());
                next = after.next().map(|(a, w)| (*a, *w));
            }
            match next {
                None => {}
                Some((at, w)) if w.eq_ignore_ascii_case("WHERE") => filter = Some(q[at + 5..].trim().to_string()),
                Some(_) => return Err(shape()),
            }
        }
        if *table_ref == "," || alias.as_deref() == Some(",") || table_ref.starts_with('(') {
            return Err(shape());
        }
        if let Some(f) = &filter {
            let banned = ["GROUP", "ORDER", "LIMIT", "OFFSET", "FETCH", "HAVING", "UNION", "INTERSECT", "EXCEPT", "WINDOW", "FOR"];
            if top_level_words(f).iter().any(|(_, w)| banned.iter().any(|b| w.eq_ignore_ascii_case(b))) {
                return Err(shape());
            }
        }
        if columns.is_empty() || words[1..from].iter().any(|(_, w)| w.eq_ignore_ascii_case("DISTINCT")) {
            return Err(shape());
        }
        Ok(Self { columns, table_ref: table_ref.to_string(), alias, filter })
    }

    /// The query a worker runs for `range`.
    pub fn range_query(&self, range: &Range) -> String {
        let alias = self.alias.as_deref().map(|a| format!(" {a}")).unwrap_or_default();
        let mut conds = vec![format!("ctid >= '({},0)'::tid", range.first_page)];
        if let Some(end) = range.end_page {
            conds.push(format!("ctid < '({end},0)'::tid"));
        }
        if let Some(f) = &self.filter {
            conds.push(format!("({f})"));
        }
        format!("SELECT {} FROM {}{alias} WHERE {}", self.columns, self.table_ref, conds.join(" AND "))
    }
}

/// The words of `s` outside quotes and parentheses, with their byte offsets.
/// A quoted identifier is one word, quotes included; a comma is a word.
fn top_level_words(s: &str) -> Vec<(usize, &str)> {
    let b = s.as_bytes();
    let mut words = Vec::new();
    let mut depth = 0usize;
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'\'' => {
                i += 1;
                while i < b.len() && !(b[i] == b'\'' && b.get(i + 1) != Some(&b'\'')) {
                    i += if b[i] == b'\'' { 2 } else { 1 };
                }
                i += 1;
            }
            b'(' => {
                if depth == 0 {
                    words.push((i, &s[i..i + 1]));
                }
                depth += 1;
                i += 1;
            }
            b')' => {
                depth = depth.saturating_sub(1);
                i += 1;
            }
            b',' => {
                if depth == 0 {
                    words.push((i, &s[i..i + 1]));
                }
                i += 1;
            }
            c if c.is_ascii_whitespace() => i += 1,
            _ => {
                let start = i;
                while i < b.len() && !b[i].is_ascii_whitespace() && !matches!(b[i], b'(' | b')' | b',' | b'\'') {
                    if b[i] == b'"' {
                        i += 1;
                        while i < b.len() && b[i] != b'"' {
                            i += 1;
                        }
                    }
                    i += 1;
                }
                if depth == 0 {
                    words.push((start, &s[start..i.min(s.len())]));
                }
            }
        }
    }
    words
}

/// Pages `first_page..end_page` of the table; `None` reaches its end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Range {
    pub first_page: u64,
    pub end_page: Option<u64>,
}

/// `pages` cut into `pieces` ranges of about equal pages, the last open.
pub fn page_ranges(pages: u64, pieces: usize) -> Vec<Range> {
    let pieces = (pieces as u64).min(pages).max(1);
    (0..pieces)
        .map(|k| Range {
            first_page: k * pages / pieces,
            end_page: (k + 1 < pieces).then(|| (k + 1) * pages / pieces),
        })
        .collect()
}

/// Opens the snapshot every worker reads in, on the source's own connection:
/// a repeatable-read transaction that stays open until the workers are done.
/// Returns the snapshot id and the table's pages.
pub async fn export_snapshot(client: &Client, q: &TableQuery, namespace: &str) -> Result<(String, u64)> {
    let version: i64 = scalar(client, "SHOW server_version_num").await?.parse().context("server_version_num")?;
    if version < MIN_SERVER_VERSION {
        anyhow::bail!(
            "`parallel` needs PostgreSQL 14 or later (TID range scans); this server is {version}: \
             every worker would read the whole table"
        );
    }
    let begin = "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY";
    op("BEGIN", namespace, None, Some(begin), async { client.batch_execute(begin).await.context("BEGIN failed") }).await?;
    let snapshot = scalar(client, "SELECT pg_export_snapshot()").await?;
    let size_sql = format!(
        "SELECT pg_relation_size({}::regclass) / current_setting('block_size')::bigint",
        quote_literal(&q.table_ref)
    );
    let pages: u64 = scalar(client, &size_sql).await?.parse().context("the table's pages")?;
    Ok((snapshot, pages))
}

/// The one value of a one-row query.
async fn scalar(client: &Client, sql: &str) -> Result<String> {
    let messages = client.simple_query(sql).await.with_context(|| format!("{sql} failed"))?;
    messages
        .into_iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(r) => r.get(0).map(str::to_string),
            _ => None,
        })
        .with_context(|| format!("{sql}: no value"))
}

fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// What every worker shares.
pub struct Shared {
    pub conn: Arc<ConnectionConfig>,
    pub query: TableQuery,
    pub snapshot: String,
    pub ranges: Mutex<VecDeque<Range>>,
    pub batch_size: usize,
    pub schema: Arc<Schema>,
    pub namespace: String,
    /// The DateStyle/bytea pins the source applies to its own session.
    pub session_pins: String,
    /// FETCHes started and not given back, across workers (the credit taken).
    pub started: AtomicU64,
}

/// Starts `workers` tasks; their batches arrive on the returned channel, which
/// closes once every worker is done. A worker's first error ends it (the source
/// then fails the run, which stops the others).
pub fn spawn_workers(shared: Arc<Shared>, workers: usize, gate: FlowGate) -> mpsc::Receiver<Result<RecordBatch>> {
    let (tx, rx) = mpsc::channel(workers);
    for _ in 0..workers {
        let (shared, gate, tx) = (shared.clone(), gate.clone(), tx.clone());
        tokio::spawn(tracing::Instrument::in_current_span(async move {
            if let Err(e) = work(&shared, gate, &tx).await {
                let _ = tx.send(Err(e)).await;
            }
        }));
    }
    rx
}

/// One worker: its own connection in the shared snapshot; ranges from the
/// queue until it is empty.
async fn work(shared: &Shared, mut gate: FlowGate, tx: &mpsc::Sender<Result<RecordBatch>>) -> Result<()> {
    let client = crate::conn::connect(&shared.conn).await?;
    let ns = shared.namespace.as_str();
    let setup = format!(
        "{}; BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET TRANSACTION SNAPSHOT {}",
        shared.session_pins,
        quote_literal(&shared.snapshot)
    );
    op("BEGIN", ns, None, Some(&setup), async { client.batch_execute(&setup).await.context("joining the snapshot") })
        .await?;
    let fetch_sql = format!("FETCH FORWARD {} FROM ls_part", shared.batch_size);
    loop {
        let Some(range) = shared.ranges.lock().map_err(|_| anyhow::anyhow!("range queue poisoned"))?.pop_front() else {
            return Ok(());
        };
        let declare = format!("DECLARE ls_part NO SCROLL CURSOR FOR {}", shared.query.range_query(&range));
        op("DECLARE", ns, None, Some(&declare), async { client.batch_execute(&declare).await.context("DECLARE failed") })
            .await?;
        loop {
            take_credit(shared, &mut gate).await;
            let messages = op("FETCH", ns, None, Some(&fetch_sql), async {
                client.simple_query(&fetch_sql).await.context("FETCH failed")
            })
            .await?;
            let rows: Vec<SimpleQueryRow> = messages
                .into_iter()
                .filter_map(|m| match m {
                    SimpleQueryMessage::Row(r) => Some(r),
                    _ => None,
                })
                .collect();
            if rows.is_empty() {
                // The end of this range: the credit is not used.
                shared.started.fetch_sub(1, Ordering::AcqRel);
                break;
            }
            let batch = crate::types::rows_to_batch_text(&rows, &shared.schema).context("RecordBatch build failed")?;
            if tx.send(Ok(batch)).await.is_err() {
                return Ok(()); // the source is gone
            }
        }
        client.batch_execute("CLOSE ls_part").await.context("CLOSE failed")?;
    }
}

/// Waits until the core's grant covers one more FETCH than all workers have
/// started, and takes it.
async fn take_credit(shared: &Shared, gate: &mut FlowGate) {
    loop {
        let started = shared.started.load(Ordering::Acquire);
        if gate.granted() > started {
            if shared.started.compare_exchange(started, started + 1, Ordering::AcqRel, Ordering::Acquire).is_ok() {
                return;
            }
        } else {
            gate.wait_for(started + 1).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_table_queries_parse() {
        let q = TableQuery::parse("SELECT * FROM events_scale").unwrap();
        assert_eq!((q.columns.as_str(), q.table_ref.as_str(), q.alias, q.filter), ("*", "events_scale", None, None));
        let q = TableQuery::parse("select e.id, upper(e.name) from public.events as e where e.kind = 'a b' and e.id > 5;")
            .unwrap();
        assert_eq!((q.table_ref.as_str(), q.alias.as_deref()), ("public.events", Some("e")));
        assert_eq!(q.filter.as_deref(), Some("e.kind = 'a b' and e.id > 5"));
        let q = TableQuery::parse(r#"SELECT "Id" FROM "My Schema"."My Table" t WHERE x IN (SELECT 1)"#).unwrap();
        assert_eq!((q.table_ref.as_str(), q.alias.as_deref()), (r#""My Schema"."My Table""#, Some("t")));
    }

    #[test]
    fn other_shapes_are_refused() {
        for q in [
            "SELECT * FROM a, b",
            "SELECT * FROM a JOIN b ON a.id = b.id",
            "SELECT * FROM (SELECT * FROM t) s",
            "SELECT * FROM t ORDER BY id",
            "SELECT * FROM t WHERE x = 1 LIMIT 10",
            "SELECT kind, count(*) FROM t GROUP BY kind",
            "SELECT DISTINCT kind FROM t",
            "SELECT * FROM t UNION SELECT * FROM u",
            "WITH x AS (SELECT 1) SELECT * FROM x",
            "SELECT * FROM t WHERE x = 1 FOR UPDATE",
        ] {
            assert!(TableQuery::parse(q).is_err(), "{q}");
        }
    }

    #[test]
    fn the_range_query_scans_its_pages() {
        let q = TableQuery::parse("SELECT * FROM public.events e WHERE e.kind = 'x'").unwrap();
        assert_eq!(
            q.range_query(&Range { first_page: 10, end_page: Some(20) }),
            "SELECT * FROM public.events e WHERE ctid >= '(10,0)'::tid AND ctid < '(20,0)'::tid AND (e.kind = 'x')"
        );
        let q = TableQuery::parse("SELECT * FROM t").unwrap();
        assert_eq!(q.range_query(&Range { first_page: 7, end_page: None }), "SELECT * FROM t WHERE ctid >= '(7,0)'::tid");
    }

    #[test]
    fn pages_are_cut_evenly_and_the_last_range_is_open() {
        let r = page_ranges(100, 4);
        assert_eq!(
            r,
            [
                Range { first_page: 0, end_page: Some(25) },
                Range { first_page: 25, end_page: Some(50) },
                Range { first_page: 50, end_page: Some(75) },
                Range { first_page: 75, end_page: None },
            ]
        );
        assert_eq!(page_ranges(2, 8).len(), 2, "no more ranges than pages");
        assert_eq!(page_ranges(0, 8), [Range { first_page: 0, end_page: None }], "an empty table: one open range");
    }
}
