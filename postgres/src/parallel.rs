//! Partitioned full loads (`parallel:`): `workers` connections read disjoint
//! partitions of one table, all in the same snapshot — exported by the
//! source's own transaction (`pg_export_snapshot()`) and imported by every
//! worker (`SET TRANSACTION SNAPSHOT`) — so the load is one consistent read.
//! Each worker reads **and converts** its rows; the batches leave in whatever
//! order they finish.
//!
//! **Partitions.** By default (`split_by: ctid`) page ranges: the table's
//! pages (`pg_relation_size`) cut into `8 × workers` pieces, half-open and
//! open at the end, so every row is read exactly once (rows on pages added
//! after the count are in the last range; a TID range scan since PostgreSQL
//! 14). With `split_by: <column>` (an integer column, ideally indexed) ranges
//! of its values between its minimum and maximum, open at both ends, plus one
//! partition for its NULLs. They sit in one queue, so a worker that finishes
//! early takes the next one.
//!
//! **Resume.** The source reports the partitions it has fully returned as its
//! resume token; the core persists the token at the delivered prefix. An
//! exported snapshot ends with its transaction, so a resumed run cannot read
//! at the failed run's snapshot: by default it starts over. With
//! `resume_snapshot: new` (key partitions only) it reads the partitions not
//! yet done at a new snapshot — the output then mixes two points in time,
//! which is why page partitions (an UPDATE moves a row to another page, so it
//! could be read twice or not at all) cannot.
//!
//! The core's credit bounds the batches fetched across all workers: a worker
//! takes a credit before each FETCH and gives it back when the FETCH comes
//! back empty (the end of its range).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use loadsmith_plugin_sdk::FlowGate;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_postgres::{Client, SimpleQueryMessage};

use crate::conn::ConnectionConfig;
use crate::read::{Reading, RowReader};
use crate::types::TextRows;
use crate::telemetry::op;
use crate::vocab::config_enum;

/// `parallel:` of the source config.
#[derive(Debug, Deserialize)]
pub struct ParallelConfig {
    /// Connections reading at once.
    pub workers: usize,
    /// `ctid` (the default): page ranges. A column of the table: ranges of
    /// its values (an integer column).
    #[serde(default)]
    pub split_by: Option<String>,
    /// What a resumed run reads at (see the module docs).
    #[serde(default)]
    pub resume_snapshot: ResumeSnapshot,
}

config_enum! {
    /// The snapshot a resumed partitioned load reads its remaining
    /// partitions at.
    #[derive(Default)]
    pub enum ResumeSnapshot for "resume_snapshot" {
        /// The failed run's, or none: a Postgres snapshot ends with its
        /// transaction, so the load starts over.
        #[default]
        Same = "same",
        /// A new one: the partitions not yet done are read at it (key
        /// partitions only).
        New = "new",
    }
    retired {}
}

/// How the table is cut.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Split {
    Ctid,
    /// A column, as written in the config (used verbatim in SQL).
    Key(String),
}

impl Split {
    /// The token's name for it.
    fn name(&self) -> &str {
        match self {
            Split::Ctid => "ctid",
            Split::Key(c) => c,
        }
    }
}

impl ParallelConfig {
    /// The split, checked: a column must be one identifier (quoted or not),
    /// and `resume_snapshot: new` needs one.
    pub fn split(&self) -> Result<Split> {
        let split = match self.split_by.as_deref().map(str::trim) {
            None => Split::Ctid,
            Some(c) if c.eq_ignore_ascii_case("ctid") => Split::Ctid,
            Some(c) if is_identifier(c) => Split::Key(c.to_string()),
            Some(c) => bail!("parallel.split_by `{c}` must be `ctid` or one column name"),
        };
        if self.resume_snapshot == ResumeSnapshot::New && split == Split::Ctid {
            bail!(
                "parallel.resume_snapshot: new needs a key column in `split_by`: with page ranges a row \
                 updated between the two snapshots can move to another page and be read twice or not at all"
            );
        }
        Ok(split)
    }
}

/// One column name: a plain identifier or a double-quoted one.
fn is_identifier(s: &str) -> bool {
    match s.strip_prefix('"').and_then(|q| q.strip_suffix('"')) {
        Some(quoted) => !quoted.is_empty() && !quoted.replace("\"\"", "").contains('"'),
        None => {
            s.chars().next().is_some_and(|c| c.is_alphabetic() || c == '_')
                && s.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '$')
        }
    }
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

    /// The query a worker runs for `partition` (of a `Key` split on `key`).
    pub fn partition_query(&self, partition: &Partition, key: Option<&str>) -> String {
        let alias = self.alias.as_deref().map(|a| format!(" {a}")).unwrap_or_default();
        let key = key.unwrap_or("ctid");
        let mut conds = Vec::new();
        match partition {
            Partition::Pages { first, end } => {
                conds.push(format!("ctid >= '({first},0)'::tid"));
                if let Some(end) = end {
                    conds.push(format!("ctid < '({end},0)'::tid"));
                }
            }
            Partition::Key { lo, hi } => {
                if let Some(lo) = lo {
                    conds.push(format!("{key} >= {lo}"));
                }
                if let Some(hi) = hi {
                    conds.push(format!("{key} < {hi}"));
                }
            }
            Partition::Null => conds.push(format!("{key} IS NULL")),
        }
        if let Some(f) = &self.filter {
            conds.push(format!("({f})"));
        }
        let filter = if conds.is_empty() { String::new() } else { format!(" WHERE {}", conds.join(" AND ")) };
        format!("SELECT {} FROM {}{alias}{filter}", self.columns, self.table_ref)
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

/// One partition of the table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Partition {
    /// Pages `first..end` (`None`: to the end of the table).
    Pages { first: u64, end: Option<u64> },
    /// Key values `lo <= key < hi` (`None`: unbounded on that side; integer
    /// text, checked before it becomes SQL).
    Key { lo: Option<String>, hi: Option<String> },
    /// The rows whose key is NULL.
    Null,
}

/// `pages` cut into `pieces` ranges of about equal pages, the last open.
pub fn page_ranges(pages: u64, pieces: usize) -> Vec<Partition> {
    let pieces = (pieces as u64).min(pages).max(1);
    (0..pieces)
        .map(|k| Partition::Pages { first: k * pages / pieces, end: (k + 1 < pieces).then(|| (k + 1) * pages / pieces) })
        .collect()
}

/// The key range `[min, max]` cut into `pieces` ranges of about equal width,
/// open at both ends (values outside it, written meanwhile, are read too),
/// then the NULLs. An empty table (no minimum) is one unbounded partition.
pub fn key_ranges(min_max: Option<(i128, i128)>, pieces: usize) -> Vec<Partition> {
    let Some((min, max)) = min_max else {
        return vec![Partition::Key { lo: None, hi: None }];
    };
    let span = max - min + 1;
    let mut cuts: Vec<i128> = Vec::new();
    for k in 1..pieces as i128 {
        let cut = min + span * k / pieces as i128;
        if cut > min && cuts.last().is_none_or(|&last| cut > last) {
            cuts.push(cut);
        }
    }
    let mut edges: Vec<Option<String>> = vec![None];
    edges.extend(cuts.iter().map(|c| Some(c.to_string())));
    edges.push(None);
    let mut parts: Vec<Partition> =
        edges.windows(2).map(|w| Partition::Key { lo: w[0].clone(), hi: w[1].clone() }).collect();
    parts.push(Partition::Null);
    parts
}

/// What a partitioned load reports as its resume token: how it cut the
/// table, and the partitions it has fully returned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumeToken {
    /// `ctid` or the key column.
    pub split_by: String,
    pub partitions: Vec<Partition>,
    pub done: Vec<usize>,
}

impl ResumeToken {
    pub fn new(split: &Split, partitions: &[Partition], done: &std::collections::BTreeSet<usize>) -> Self {
        Self { split_by: split.name().to_string(), partitions: partitions.to_vec(), done: done.iter().copied().collect() }
    }

    /// The token for `split`, if `value` is one (the same split, sane ids).
    pub fn parse(value: serde_json::Value, split: &Split) -> Result<Self> {
        let token: Self = serde_json::from_value(value).context("not a partitioned load's resume token")?;
        if token.split_by != split.name() {
            bail!("the token splits by `{}`, the config by `{}`", token.split_by, split.name());
        }
        if let Some(id) = token.done.iter().find(|&&id| id >= token.partitions.len()) {
            bail!("the token names partition {id} of {}", token.partitions.len());
        }
        for p in &token.partitions {
            if let Partition::Key { lo, hi } = p {
                if [lo, hi].into_iter().flatten().any(|b| b.parse::<i128>().is_err()) {
                    bail!("the token's key bounds are not integers");
                }
            }
        }
        Ok(token)
    }
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

/// The minimum and maximum of the `key` column over the query's rows, in the
/// snapshot (the caller's transaction); `None` when there is no value. The
/// key must be an integer column.
pub async fn key_min_max(client: &Client, q: &TableQuery, key: &str) -> Result<Option<(i128, i128)>> {
    let alias = q.alias.as_deref().map(|a| format!(" {a}")).unwrap_or_default();
    let filter = q.filter.as_deref().map(|f| format!(" WHERE {f}")).unwrap_or_default();
    let sql = format!("SELECT min({key})::text, max({key})::text FROM {}{alias}{filter}", q.table_ref);
    let messages = client.simple_query(&sql).await.with_context(|| format!("{sql} failed"))?;
    let row = messages.into_iter().find_map(|m| match m {
        SimpleQueryMessage::Row(r) => Some(r),
        _ => None,
    });
    let Some(row) = row else { return Ok(None) };
    match (row.get(0), row.get(1)) {
        (Some(lo), Some(hi)) => {
            let int = |v: &str| {
                v.parse::<i128>().map_err(|_| {
                    anyhow::anyhow!("parallel.split_by `{key}` must be an integer column (its minimum is {v:?})")
                })
            };
            Ok(Some((int(lo)?, int(hi)?)))
        }
        _ => Ok(None),
    }
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
    /// The key column of a `Key` split.
    pub key: Option<String>,
    pub snapshot: String,
    /// The partitions left to read, with their ids.
    pub partitions: Mutex<VecDeque<(usize, Partition)>>,
    pub reading: Reading,
    pub schema: Arc<Schema>,
    pub namespace: String,
    /// The DateStyle/bytea pins the source applies to its own session.
    pub session_pins: String,
    /// FETCHes started and not given back, across workers (the credit taken).
    pub started: AtomicU64,
}

/// What a worker hands the source: a batch, or the end of a partition (sent
/// after its last batch, so every batch of a partition is ahead of it).
pub enum Item {
    Batch(RecordBatch),
    Done(usize),
}

/// Starts `workers` tasks; their batches arrive on the returned channel, which
/// closes once every worker is done. A worker's first error ends it (the source
/// then fails the run, which stops the others).
pub fn spawn_workers(shared: Arc<Shared>, workers: usize, gate: FlowGate) -> mpsc::Receiver<Result<Item>> {
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

/// One worker: its own connection in the shared snapshot; partitions from
/// the queue until it is empty.
async fn work(shared: &Shared, mut gate: FlowGate, tx: &mpsc::Sender<Result<Item>>) -> Result<()> {
    let client = Arc::new(crate::conn::connect(&shared.conn).await?);
    let ns = shared.namespace.as_str();
    let setup = format!(
        "{}; BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET TRANSACTION SNAPSHOT {}",
        shared.session_pins,
        quote_literal(&shared.snapshot)
    );
    op("BEGIN", ns, None, Some(&setup), async { client.batch_execute(&setup).await.context("joining the snapshot") })
        .await?;
    loop {
        let next = shared.partitions.lock().map_err(|_| anyhow::anyhow!("partition queue poisoned"))?.pop_front();
        let Some((id, partition)) = next else {
            return Ok(());
        };
        let query = shared.query.partition_query(&partition, shared.key.as_deref());
        let mut reader = RowReader::open(client.clone(), &shared.reading, &query, "ls_part", Some(&query)).await?;
        loop {
            take_credit(shared, &mut gate).await;
            let rows = reader.next().await?;
            if rows.is_empty() {
                // The end of this partition: the credit is not used.
                shared.started.fetch_sub(1, Ordering::AcqRel);
                break;
            }
            let batch = crate::types::rows_to_batch_text(&rows, &shared.schema).context("RecordBatch build failed")?;
            if tx.send(Ok(Item::Batch(batch))).await.is_err() {
                return Ok(()); // the source is gone
            }
        }
        if tx.send(Ok(Item::Done(id))).await.is_err() {
            return Ok(());
        }
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
    fn the_partition_query_scans_its_pages_or_keys() {
        let q = TableQuery::parse("SELECT * FROM public.events e WHERE e.kind = 'x'").unwrap();
        assert_eq!(
            q.partition_query(&Partition::Pages { first: 10, end: Some(20) }, None),
            "SELECT * FROM public.events e WHERE ctid >= '(10,0)'::tid AND ctid < '(20,0)'::tid AND (e.kind = 'x')"
        );
        let q = TableQuery::parse("SELECT * FROM t").unwrap();
        assert_eq!(q.partition_query(&Partition::Pages { first: 7, end: None }, None), "SELECT * FROM t WHERE ctid >= '(7,0)'::tid");
        let key = Some("id");
        assert_eq!(
            q.partition_query(&Partition::Key { lo: Some("10".into()), hi: Some("20".into()) }, key),
            "SELECT * FROM t WHERE id >= 10 AND id < 20"
        );
        assert_eq!(q.partition_query(&Partition::Key { lo: None, hi: Some("5".into()) }, key), "SELECT * FROM t WHERE id < 5");
        assert_eq!(q.partition_query(&Partition::Key { lo: None, hi: None }, key), "SELECT * FROM t");
        assert_eq!(q.partition_query(&Partition::Null, key), "SELECT * FROM t WHERE id IS NULL");
    }

    #[test]
    fn pages_are_cut_evenly_and_the_last_range_is_open() {
        let pages = |first, end| Partition::Pages { first, end };
        assert_eq!(page_ranges(100, 4), [pages(0, Some(25)), pages(25, Some(50)), pages(50, Some(75)), pages(75, None)]);
        assert_eq!(page_ranges(2, 8).len(), 2, "no more ranges than pages");
        assert_eq!(page_ranges(0, 8), [pages(0, None)], "an empty table: one open range");
    }

    #[test]
    fn keys_are_cut_evenly_open_at_both_ends_with_the_nulls_last() {
        let key = |lo: Option<i32>, hi: Option<i32>| Partition::Key { lo: lo.map(|v| v.to_string()), hi: hi.map(|v| v.to_string()) };
        assert_eq!(
            key_ranges(Some((1, 100)), 4),
            [key(None, Some(26)), key(Some(26), Some(51)), key(Some(51), Some(76)), key(Some(76), None), Partition::Null]
        );
        assert_eq!(key_ranges(Some((-5, -5)), 4), [key(None, None), Partition::Null], "one value: one range");
        assert_eq!(key_ranges(Some((0, 2)), 8), [key(None, Some(1)), key(Some(1), Some(2)), key(Some(2), None), Partition::Null]);
        assert_eq!(key_ranges(None, 4), [key(None, None)], "no value at all: read everything as one");
    }

    #[test]
    fn split_by_is_checked() {
        let cfg = |split: Option<&str>, snap: &str| -> Result<Split> {
            let mut v = serde_json::json!({ "workers": 4, "resume_snapshot": snap });
            if let Some(s) = split {
                v["split_by"] = s.into();
            }
            serde_json::from_value::<ParallelConfig>(v).unwrap().split()
        };
        assert_eq!(cfg(None, "same").unwrap(), Split::Ctid);
        assert_eq!(cfg(Some("CTID"), "same").unwrap(), Split::Ctid);
        assert_eq!(cfg(Some("event_id"), "new").unwrap(), Split::Key("event_id".into()));
        assert_eq!(cfg(Some("\"Event Id\""), "new").unwrap(), Split::Key("\"Event Id\"".into()));
        assert!(cfg(Some("id; DROP TABLE t"), "same").is_err());
        assert!(cfg(Some("a + b"), "same").is_err());
        let e = cfg(None, "new").unwrap_err().to_string();
        assert!(e.contains("needs a key column"), "{e}");
    }

    #[test]
    fn tokens_roundtrip_and_are_checked() {
        let split = Split::Key("id".into());
        let parts = key_ranges(Some((1, 100)), 2);
        let done = [0usize, 2].into_iter().collect();
        let token = serde_json::to_value(ResumeToken::new(&split, &parts, &done)).unwrap();
        let back = ResumeToken::parse(token.clone(), &split).unwrap();
        assert_eq!((back.partitions, back.done), (parts, vec![0, 2]));
        assert!(ResumeToken::parse(token.clone(), &Split::Ctid).is_err(), "another split");
        let mut bad = token.clone();
        bad["done"] = serde_json::json!([9]);
        assert!(ResumeToken::parse(bad, &split).is_err());
        let mut bad = token;
        bad["partitions"][0]["hi"] = "1; DROP TABLE t".into();
        assert!(ResumeToken::parse(bad, &split).is_err(), "bounds become SQL: integers only");
    }
}
