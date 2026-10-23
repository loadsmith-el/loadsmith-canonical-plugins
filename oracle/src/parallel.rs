//! Partitioned full loads (`parallel:`): `workers` connections read disjoint
//! ROWID ranges of one table, all at the same SCN (`AS OF SCN`), so the load
//! is one consistent snapshot. Each worker fetches **and converts** its rows on
//! its own thread; the batches leave in whatever order the workers finish them.
//!
//! The ranges are cut at ROWIDs sampled from the table (`SAMPLE BLOCK`, a
//! small fraction of its blocks; every ROWID of a small table): half-open
//! ranges `[b(i), b(i+1))`, open at both ends, so together they cover every
//! row exactly once whatever the sample. Only `SELECT` on the table is needed.
//! The ranges sit in one queue, so a worker that finishes early takes the next
//! one.
//!
//! The core's credit bounds the batches fetched across all workers: a worker
//! takes a credit before each fetch and gives it back when the fetch comes
//! back empty (the end of its range), so no credit is spent on a batch that
//! never comes.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use loadsmith_plugin_sdk::FlowGate;
use oracledb::{Connection, Row};
use serde::Deserialize;

use crate::conn::ConnectionConfig;
use crate::telemetry::DbOp;
use crate::types::{self, fold_ident};

/// `parallel:` of the source config.
#[derive(Debug, Deserialize)]
pub struct ParallelConfig {
    /// Connections reading at once.
    pub workers: usize,
}

/// The most workers a load may open.
pub const MAX_WORKERS: usize = 64;

/// A query a partitioned read can split: `SELECT <columns> FROM <table>
/// [<alias>] [WHERE <filter>]`, one table, no joins, grouping or ordering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableQuery {
    pub columns: String,
    /// The owner as Oracle stores it (folded), when the query names one.
    pub owner: Option<String>,
    /// The table as Oracle stores it (folded).
    pub table: String,
    /// The table reference as written (`hr.events`), for the rewritten query.
    pub table_ref: String,
    pub alias: Option<String>,
    pub filter: Option<String>,
}

impl TableQuery {
    pub fn parse(query: &str) -> Result<Self> {
        let shape = || {
            anyhow::anyhow!(
                "`parallel` splits a query of the form `SELECT <columns> FROM <table> [<alias>] [WHERE <filter>]` \
                 (one table; no joins, subqueries in FROM, GROUP BY, ORDER BY or set operators); \
                 this query is not one"
            )
        };
        let q = query.trim().trim_end_matches(';').trim();
        let words = top_level_words(q);
        let kw = |i: usize, w: &str| words.get(i).is_some_and(|(_, word)| word.eq_ignore_ascii_case(w));
        if !kw(0, "SELECT") {
            return Err(shape());
        }
        let from = words.iter().position(|(_, w)| w.eq_ignore_ascii_case("FROM")).ok_or_else(shape)?;
        let columns = q[words[0].0 + 6..words[from].0].trim().to_string();
        // After FROM: the table reference, an optional alias, an optional WHERE.
        let mut after = words[from + 1..].iter();
        let (_, table_ref) = after.next().ok_or_else(shape)?;
        let mut alias = None;
        let mut filter = None;
        if let Some((at, w)) = after.next() {
            if w.eq_ignore_ascii_case("WHERE") {
                filter = Some(q[at + 5..].trim().to_string());
            } else {
                alias = Some(w.to_string());
                match after.next() {
                    None => {}
                    Some((at, w)) if w.eq_ignore_ascii_case("WHERE") => filter = Some(q[at + 5..].trim().to_string()),
                    Some(_) => return Err(shape()),
                }
            }
        }
        if table_ref.contains(['(', ',']) || alias.as_deref().is_some_and(|a| a.contains(['(', ','])) {
            return Err(shape());
        }
        // A filter may not carry clauses that change the row set's shape.
        if let Some(f) = &filter {
            let banned = ["GROUP", "ORDER", "CONNECT", "START", "UNION", "INTERSECT", "MINUS", "EXCEPT", "FETCH", "OFFSET", "HAVING", "MODEL"];
            if top_level_words(f).iter().any(|(_, w)| banned.iter().any(|b| w.eq_ignore_ascii_case(b))) {
                return Err(shape());
            }
        }
        if columns.is_empty() || words[1..from].iter().any(|(_, w)| w.eq_ignore_ascii_case("DISTINCT") || w.eq_ignore_ascii_case("UNIQUE")) {
            return Err(shape());
        }
        let (owner, table) = match split_qualified(table_ref) {
            Some((o, t)) => (Some(catalog_name(o)), catalog_name(t)),
            None => (None, catalog_name(table_ref)),
        };
        Ok(Self { columns, owner, table, table_ref: table_ref.to_string(), alias, filter })
    }

    /// The query a worker runs for `range` at `scn`. The bounds are ROWID
    /// literals: each range's statement text is its own (the driver's
    /// statement cache, re-executing one cached text with new binds on a
    /// connection, breaks the protocol).
    pub fn range_query(&self, scn: u64, range: &Range) -> String {
        let alias = self.alias.as_deref().map(|a| format!(" {a}")).unwrap_or_default();
        let mut conds = Vec::new();
        if let Some(lo) = &range.lo {
            conds.push(format!("ROWID >= CHARTOROWID('{lo}')"));
        }
        if let Some(hi) = &range.hi {
            conds.push(format!("ROWID < CHARTOROWID('{hi}')"));
        }
        if let Some(f) = &self.filter {
            conds.push(format!("({f})"));
        }
        let filter = if conds.is_empty() { String::new() } else { format!(" WHERE {}", conds.join(" AND ")) };
        format!("SELECT {} FROM {} AS OF SCN {scn}{alias}{filter}", self.columns, self.table_ref)
    }
}

/// The words of `s` outside quotes and parentheses, with their byte offsets.
/// A quoted identifier (`"My Table"`) is one word, quotes included; a comma
/// is a word of its own.
fn top_level_words(s: &str) -> Vec<(usize, &str)> {
    let b = s.as_bytes();
    let mut words = Vec::new();
    let mut depth = 0usize;
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'\'' => {
                // A string literal, '' escapes included.
                i += 1;
                while i < b.len() && !(b[i] == b'\'' && b.get(i + 1) != Some(&b'\'')) {
                    i += if b[i] == b'\'' { 2 } else { 1 };
                }
                i += 1;
            }
            b'(' => {
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

/// An identifier as the catalog stores it: a quoted one verbatim (without its
/// quotes), an unquoted one upper-cased.
fn catalog_name(ident: &str) -> String {
    match ident.strip_prefix('"').and_then(|i| i.strip_suffix('"')) {
        Some(quoted) => quoted.replace("\"\"", "\""),
        None => fold_ident(ident),
    }
}

/// `owner.table` → (owner, table), honoring quoted parts.
fn split_qualified(r: &str) -> Option<(&str, &str)> {
    let mut in_quotes = false;
    for (i, c) in r.char_indices() {
        match c {
            '"' => in_quotes = !in_quotes,
            '.' if !in_quotes => return Some((&r[..i], &r[i + 1..])),
            _ => {}
        }
    }
    None
}

/// One ROWID range, `[lo, hi)`; `None` is open on that side.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Range {
    pub lo: Option<String>,
    pub hi: Option<String>,
}

/// The ranges between consecutive `bounds` (sorted ROWIDs), open at both
/// ends, in the order workers should take them: the two open ones first —
/// they also hold the blocks the sample did not reach, so they are the
/// largest, and taken last they would leave one worker reading alone.
pub fn ranges_between(bounds: &[String]) -> Vec<Range> {
    let mut edges: Vec<Option<String>> = vec![None];
    edges.extend(bounds.iter().cloned().map(Some));
    edges.push(None);
    let mut ranges: Vec<Range> = edges.windows(2).map(|w| Range { lo: w[0].clone(), hi: w[1].clone() }).collect();
    if ranges.len() > 2 {
        let last = ranges.pop().expect("more than two");
        ranges.insert(1, last);
    }
    ranges
}

/// `pieces - 1` ROWIDs that cut the table into about equal parts. A block
/// sample (1% of the blocks; every block of a table too small to sample)
/// gives each segment's span of blocks (data object, file, first and last
/// block); the spans are cut evenly by block, rows per block being about even
/// in a heap table, and Oracle builds and sorts the boundary ROWIDs.
pub fn sample_bounds(conn: &Connection, q: &TableQuery, pieces: usize) -> Result<Vec<String>> {
    let spans = |sample: &str| -> Result<Vec<(i64, i64, i64, i64)>> {
        let sql = format!(
            "SELECT o, f, MIN(b), MAX(b) FROM (SELECT DBMS_ROWID.ROWID_OBJECT(ROWID) o, \
             DBMS_ROWID.ROWID_RELATIVE_FNO(ROWID) f, DBMS_ROWID.ROWID_BLOCK_NUMBER(ROWID) b \
             FROM {}{sample}) GROUP BY o, f ORDER BY o, f",
            q.table_ref
        );
        let cursor = conn
            .statement(&sql)
            .and_then(|b| b.build())
            .and_then(|stmt| stmt.query(&[]))
            .map_err(|e| anyhow::anyhow!("sampling the blocks of {}: {e}", q.table_ref))?;
        cursor
            .map(|row| {
                let row = row.map_err(|e| anyhow::anyhow!("sampling the blocks: {e}"))?;
                let get = |i: usize| row.get::<i64>(i).map_err(|e| anyhow::anyhow!("sampling the blocks: {e}"));
                Ok((get(0)?, get(1)?, get(2)?, get(3)?))
            })
            .collect()
    };
    let mut segments = spans(" SAMPLE BLOCK (1)")?;
    if segments.iter().map(|&(_, _, lo, hi)| hi - lo + 1).sum::<i64>() < pieces as i64 * 8 {
        segments = spans("")?;
    }
    let points = block_bounds(&segments, pieces);
    if points.is_empty() {
        return Ok(Vec::new());
    }
    // Oracle builds the ROWIDs, in the order of the points (by segment and
    // block: ROWID order within a segment). DBMS_ROWID.ROWID_CREATE returns
    // their text (a VARCHAR2): compared as ROWIDs only through CHARTOROWID.
    let rows = points
        .iter()
        .enumerate()
        .map(|(n, (o, f, b))| format!("SELECT {n} n, {o} o, {f} f, {b} b FROM DUAL"))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let built = format!("SELECT n, DBMS_ROWID.ROWID_CREATE(1, o, f, b, 0) r FROM ({rows})");
    let bounds: Vec<String> = query_strings(conn, &format!("SELECT r FROM ({built}) ORDER BY n"))?;
    // They become SQL literals: only ROWID characters may pass.
    if let Some(bad) = bounds.iter().find(|r| !r.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'+' || b == b'/')) {
        anyhow::bail!("unexpected ROWID text {bad:?}");
    }
    // The ranges `[b(i), b(i+1))` are disjoint only if each bound is greater
    // than the one before **as the ROWID predicate compares them** (not as
    // text: the ROWID alphabet is not in ASCII order). Checked by the
    // database; otherwise the load reads the table as one range.
    let unordered = query_strings(
        conn,
        &format!(
            "SELECT TO_CHAR(COUNT(*)) FROM (SELECT r, LEAD(r) OVER (ORDER BY n) nx FROM ({built})) \
             WHERE nx IS NOT NULL AND NOT (CHARTOROWID(r) < CHARTOROWID(nx))"
        ),
    )?;
    if unordered.first().map(String::as_str) != Some("0") {
        tracing::warn!(table = %q.table_ref, "parallel: the table's ROWID bounds are not ordered; reading it as one range");
        return Ok(Vec::new());
    }
    Ok(bounds)
}

/// The first column of every row of `sql`, as text.
fn query_strings(conn: &Connection, sql: &str) -> Result<Vec<String>> {
    let cursor = conn
        .statement(sql)
        .and_then(|b| b.build())
        .and_then(|stmt| stmt.query(&[]))
        .map_err(|e| anyhow::anyhow!("building ROWID bounds: {e}"))?;
    cursor
        .map(|row| row.and_then(|r| r.get::<String>(0)).map_err(|e| anyhow::anyhow!("building ROWID bounds: {e}")))
        .collect()
}

/// The `pieces - 1` cut points (data object, file, block) that split the
/// segments' block spans into `pieces` runs of about equal blocks.
fn block_bounds(segments: &[(i64, i64, i64, i64)], pieces: usize) -> Vec<(i64, i64, i64)> {
    let total: i64 = segments.iter().map(|&(_, _, lo, hi)| hi - lo + 1).sum();
    let pieces = (pieces as i64).min(total).max(1);
    let mut points = Vec::new();
    let (mut k, mut passed) = (1i64, 0i64);
    for &(o, f, lo, hi) in segments {
        let len = hi - lo + 1;
        while k < pieces && k * total / pieces < passed + len {
            points.push((o, f, lo + (k * total / pieces - passed)));
            k += 1;
        }
        passed += len;
    }
    points
}

/// The SCN every worker reads at: now, as precisely as an unprivileged user
/// can ask (`TIMESTAMP_TO_SCN` is exact to a few seconds back).
pub fn snapshot_scn(conn: &Connection) -> Result<u64> {
    let row = conn
        .query_row("SELECT TO_CHAR(TIMESTAMP_TO_SCN(SYSTIMESTAMP)) FROM DUAL", &[])
        .map_err(|e| anyhow::anyhow!("reading the snapshot SCN: {e}"))?;
    let scn: String = row.get(0).map_err(|e| anyhow::anyhow!("reading the snapshot SCN: {e}"))?;
    scn.trim().parse().with_context(|| format!("snapshot SCN {scn:?}"))
}

/// What every worker shares.
pub struct Shared {
    pub conn: Arc<ConnectionConfig>,
    pub query: TableQuery,
    pub scn: u64,
    /// The projected column list, as the unpartitioned query applies it.
    pub select_list: String,
    pub ranges: Mutex<VecDeque<Range>>,
    pub batch_size: usize,
    pub schema: Arc<Schema>,
    pub namespace: String,
    /// Fetches started and not given back, across workers (the credit taken).
    pub started: AtomicU64,
}

/// Starts `workers` threads; their batches arrive on the returned channel,
/// which closes once every worker is done. The first error ends a worker
/// (the source then fails the run, which stops the others).
pub fn spawn_workers(
    shared: Arc<Shared>,
    workers: usize,
    gate: FlowGate,
    runtime: tokio::runtime::Handle,
) -> Result<mpsc::Receiver<Result<RecordBatch>>> {
    let (tx, rx) = mpsc::sync_channel(workers);
    for n in 0..workers {
        let (shared, gate, runtime, tx) = (shared.clone(), gate.clone(), runtime.clone(), tx.clone());
        std::thread::Builder::new()
            .name(format!("oracle-part-{n}"))
            .spawn(move || {
                if let Err(e) = work(&shared, gate, &runtime, &tx) {
                    let _ = tx.send(Err(e));
                }
            })
            .context("spawn a partition worker")?;
    }
    Ok(rx)
}

/// One worker: its own connection; ranges from the queue until it is empty.
fn work(
    shared: &Shared,
    mut gate: FlowGate,
    runtime: &tokio::runtime::Handle,
    tx: &mpsc::SyncSender<Result<RecordBatch>>,
) -> Result<()> {
    let conn = crate::conn::connect(&shared.conn)?;
    let fetch = u32::try_from(shared.batch_size).unwrap_or(u32::MAX);
    let (mut rows_read, mut ranges_read) = (0u64, 0u64);
    loop {
        let Some(range) = shared.ranges.lock().map_err(|_| anyhow::anyhow!("range queue poisoned"))?.pop_front() else {
            tracing::debug!(rows = rows_read, ranges = ranges_read, "partition worker done");
            return Ok(());
        };
        ranges_read += 1;
        let inner = shared.query.range_query(shared.scn, &range);
        let sql = format!("SELECT {} FROM ({inner}) \"_LS\"", shared.select_list);
        let select = DbOp::new("SELECT", &shared.namespace, None, None);
        let mut cursor = select.run(|| {
            conn.statement(&sql)
                .and_then(|b| b.fetch_array_size(fetch).prefetch_rows(fetch).build())
                .and_then(|stmt| stmt.query(&[]))
                .map_err(|e| anyhow::anyhow!("partition query failed: {e}"))
        })?;
        loop {
            take_credit(shared, &mut gate, runtime);
            let rows = cursor
                .by_ref()
                .take(shared.batch_size)
                .collect::<std::result::Result<Vec<Row>, _>>()
                .map_err(|e| anyhow::anyhow!("row fetch failed: {e}"));
            let rows = match rows {
                Ok(rows) if rows.is_empty() => {
                    // The end of this range: the credit is not used.
                    shared.started.fetch_sub(1, Ordering::AcqRel);
                    break;
                }
                Ok(rows) => rows,
                Err(e) => return Err(e),
            };
            // A short chunk is the cursor's last: never ask an exhausted
            // cursor again (the driver would send another fetch, and its
            // reply would break the next query on this connection).
            let last = rows.len() < shared.batch_size;
            rows_read += rows.len() as u64;
            let batch = types::rows_to_batch(&rows, &shared.schema).context("RecordBatch build failed")?;
            if tx.send(Ok(batch)).is_err() {
                return Ok(()); // the source is gone
            }
            if last {
                break;
            }
        }
    }
}

/// Waits until the core's grant covers one more fetch than all workers have
/// started, and takes it.
fn take_credit(shared: &Shared, gate: &mut FlowGate, runtime: &tokio::runtime::Handle) {
    loop {
        let started = shared.started.load(Ordering::Acquire);
        if gate.granted() > started {
            if shared.started.compare_exchange(started, started + 1, Ordering::AcqRel, Ordering::Acquire).is_ok() {
                return;
            }
        } else {
            runtime.block_on(gate.wait_for(started + 1));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_table_queries_parse() {
        let q = TableQuery::parse("SELECT * FROM events_scale").unwrap();
        assert_eq!((q.columns.as_str(), q.owner, q.table.as_str(), q.alias, q.filter), ("*", None, "EVENTS_SCALE", None, None));
        let q = TableQuery::parse("select e.id, upper(e.name) as n from hr.events e where e.kind = 'a b' and e.id > 5;").unwrap();
        assert_eq!(q.columns, "e.id, upper(e.name) as n");
        assert_eq!((q.owner.as_deref(), q.table.as_str(), q.alias.as_deref()), (Some("HR"), "EVENTS", Some("e")));
        assert_eq!(q.filter.as_deref(), Some("e.kind = 'a b' and e.id > 5"));
        let q = TableQuery::parse(r#"SELECT "Id" FROM "Mixed"."My Table" WHERE x IN (SELECT 1 FROM dual)"#).unwrap();
        assert_eq!((q.owner.as_deref(), q.table.as_str()), (Some("Mixed"), "My Table"));
        assert_eq!(q.filter.as_deref(), Some("x IN (SELECT 1 FROM dual)"));
    }

    #[test]
    fn other_shapes_are_refused() {
        for q in [
            "SELECT * FROM a, b",
            "SELECT * FROM a JOIN b ON a.id = b.id",
            "SELECT * FROM (SELECT * FROM t)",
            "SELECT * FROM t ORDER BY id",
            "SELECT * FROM t WHERE x = 1 ORDER BY id",
            "SELECT kind, count(*) FROM t GROUP BY kind",
            "SELECT DISTINCT kind FROM t",
            "SELECT * FROM t UNION SELECT * FROM u",
            "WITH x AS (SELECT 1 FROM dual) SELECT * FROM x",
        ] {
            assert!(TableQuery::parse(q).is_err(), "{q}");
        }
    }

    #[test]
    fn the_range_query_reads_at_the_scn() {
        let q = TableQuery::parse("SELECT * FROM hr.events e WHERE e.kind = 'x'").unwrap();
        let mid = Range { lo: Some("AAA".into()), hi: Some("BBB".into()) };
        assert_eq!(
            q.range_query(42, &mid),
            "SELECT * FROM hr.events AS OF SCN 42 e WHERE ROWID >= CHARTOROWID('AAA') AND ROWID < CHARTOROWID('BBB') \
             AND (e.kind = 'x')"
        );
        let q = TableQuery::parse("SELECT * FROM t").unwrap();
        let last = Range { lo: Some("ZZZ".into()), hi: None };
        assert_eq!(q.range_query(7, &last), "SELECT * FROM t AS OF SCN 7 WHERE ROWID >= CHARTOROWID('ZZZ')");
        assert_eq!(q.range_query(7, &Range { lo: None, hi: None }), "SELECT * FROM t AS OF SCN 7");
    }

    #[test]
    fn ranges_cover_everything_once() {
        let bounds: Vec<String> = ["B", "C", "D"].map(String::from).to_vec();
        let ranges = ranges_between(&bounds);
        assert_eq!(ranges.len(), 4);
        // The open ranges first, then the inner ones in order.
        assert_eq!((ranges[0].lo.as_deref(), ranges[1].hi.as_deref()), (None, None));
        let mut sorted = ranges.clone();
        sorted.sort_by_key(|r| r.lo.clone());
        assert!(sorted.windows(2).all(|w| w[0].hi == w[1].lo), "contiguous: {sorted:?}");
        assert_eq!(ranges_between(&[]), [Range { lo: None, hi: None }]);
    }

    #[test]
    fn blocks_are_cut_evenly_across_segments() {
        // 100 blocks in one segment, 4 pieces: cuts at 25, 50, 75 blocks in.
        assert_eq!(block_bounds(&[(7, 1, 1000, 1099)], 4), [(7, 1, 1025), (7, 1, 1050), (7, 1, 1075)]);
        // Two segments of 60 and 40 blocks: the cuts follow the total.
        assert_eq!(block_bounds(&[(7, 1, 0, 59), (8, 2, 500, 539)], 4), [(7, 1, 25), (7, 1, 50), (8, 2, 515)]);
        // Fewer blocks than pieces: one cut per block at most.
        assert_eq!(block_bounds(&[(7, 1, 10, 11)], 8), [(7, 1, 11)]);
        assert!(block_bounds(&[], 4).is_empty());
    }
}
