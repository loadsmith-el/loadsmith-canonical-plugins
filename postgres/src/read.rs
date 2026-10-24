//! How the source reads a query's rows: `COPY (<query>) TO STDOUT` (the
//! default) or a cursor (`DECLARE` + `FETCH`), batch by batch, as text.
//!
//! **Why `COPY`.** Both come back in Postgres' text output format, the one the
//! decoder in [`crate::types`] parses, but a `FETCH` sends every row as a
//! protocol `DataRow` (a 4-byte length before each value) and costs a round
//! trip per batch, while `COPY` streams tab-separated lines: on the bench's
//! 34-column table, 525 MiB per million rows through a cursor against 429 MiB
//! through `COPY` (the binary protocol would move ~495 MiB: its 8-byte
//! timestamps save less than the lengths cost). On a link to a distant
//! database both the bytes and the round trips are the run's time.
//!
//! **Why a cursor too.** Not every server that speaks the Postgres protocol
//! has `COPY … TO STDOUT` (Redshift, some proxies): `fetch: cursor` reads
//! through a cursor as before.

use std::pin::Pin;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use futures_util::StreamExt;
use tokio_postgres::{Client, CopyOutStream, SimpleQueryMessage, SimpleQueryRow};

use crate::telemetry::{op, DbOp};
use crate::types::TextRows;
use crate::vocab::config_enum;

config_enum! {
    /// How rows are read from the server.
    #[derive(Default)]
    pub enum Fetch for "fetch" {
        /// `COPY (<query>) TO STDOUT`: fewer bytes, no round trip per batch.
        #[default]
        Copy = "copy",
        /// `DECLARE … CURSOR` + `FETCH FORWARD <batch_size>`.
        Cursor = "cursor",
    }
    retired {}
}

/// One batch of rows, as text.
pub enum Rows {
    Fetched(Vec<SimpleQueryRow>),
    Copied(CopyRows),
}

impl TextRows for Rows {
    fn len(&self) -> usize {
        match self {
            Rows::Fetched(rows) => rows.len(),
            Rows::Copied(rows) => rows.rows,
        }
    }

    fn cell(&self, row: usize, col: usize) -> Option<&str> {
        match self {
            Rows::Fetched(rows) => rows[row].get(col),
            Rows::Copied(rows) => rows.cell(row, col),
        }
    }
}

/// Rows parsed from `COPY` text: every value unescaped into one buffer, and
/// each cell's span in it (`None` for `\N`, SQL NULL), row by row.
pub struct CopyRows {
    text: String,
    cells: Vec<Option<(u32, u32)>>,
    columns: usize,
    rows: usize,
}

impl CopyRows {
    fn cell(&self, row: usize, col: usize) -> Option<&str> {
        self.cells[row * self.columns + col].map(|(start, end)| &self.text[start as usize..end as usize])
    }
}

/// How queries are read: the same for every query of a load.
#[derive(Debug, Clone)]
pub struct Reading {
    pub fetch: Fetch,
    pub batch_size: usize,
    /// The query's column count, which every `COPY` line must have.
    pub columns: usize,
    /// `db.namespace` of the telemetry spans.
    pub namespace: String,
}

/// A query being read, batch by batch.
pub enum RowReader {
    Cursor { client: Arc<Client>, fetch_sql: String, close_sql: String, namespace: String },
    Copy(CopyReader),
}

impl RowReader {
    /// Starts reading `query`: a `COPY`, or a cursor named `cursor` (the
    /// caller's transaction must be open). `statement_text` is what telemetry
    /// records of it (never a value), when it does.
    pub async fn open(
        client: Arc<Client>,
        how: &Reading,
        query: &str,
        cursor: &str,
        statement_text: Option<&str>,
    ) -> Result<Self> {
        let Reading { fetch, batch_size, columns, namespace } = how;
        let namespace = namespace.as_str();
        match *fetch {
            Fetch::Cursor => {
                let declare = format!("DECLARE {cursor} NO SCROLL CURSOR FOR ({query})");
                let text = statement_text.map(|t| format!("DECLARE {cursor} NO SCROLL CURSOR FOR ({t})"));
                op("DECLARE", namespace, None, text.as_deref(), async {
                    client.batch_execute(&declare).await.context("DECLARE cursor failed")
                })
                .await?;
                Ok(RowReader::Cursor {
                    client,
                    fetch_sql: format!("FETCH FORWARD {batch_size} FROM {cursor}"),
                    close_sql: format!("CLOSE {cursor}"),
                    namespace: namespace.to_string(),
                })
            }
            Fetch::Copy => {
                let sql = format!("COPY ({query}) TO STDOUT");
                let text = statement_text.map(|t| format!("COPY ({t}) TO STDOUT"));
                let stream = op("COPY", namespace, None, text.as_deref(), async {
                    client.copy_out(sql.as_str()).await.context("COPY TO STDOUT failed")
                })
                .await?;
                Ok(RowReader::Copy(CopyReader::new(stream, *columns, *batch_size)))
            }
        }
    }

    /// The next batch; empty once the query is exhausted (a cursor is then
    /// closed; the caller's transaction stays open).
    pub async fn next(&mut self) -> Result<Rows> {
        match self {
            RowReader::Cursor { client, fetch_sql, close_sql, namespace } => {
                let fetch = DbOp::new("FETCH", namespace, None, Some(fetch_sql));
                let span = fetch.span().clone();
                let messages = fetch.run(async { client.simple_query(fetch_sql).await.context("FETCH failed") }).await?;
                let rows: Vec<SimpleQueryRow> = messages
                    .into_iter()
                    .filter_map(|m| match m {
                        SimpleQueryMessage::Row(r) => Some(r),
                        _ => None,
                    })
                    .collect();
                span.record("db.response.returned_rows", rows.len() as u64);
                if rows.is_empty() {
                    client.batch_execute(close_sql).await.context("CLOSE failed")?;
                }
                Ok(Rows::Fetched(rows))
            }
            RowReader::Copy(reader) => Ok(Rows::Copied(reader.next().await?)),
        }
    }
}

/// Reads `COPY … TO STDOUT` in its text format: one line per row, values
/// separated by tabs, `\N` for NULL, backslash escapes inside values.
pub struct CopyReader {
    stream: Pin<Box<CopyOutStream>>,
    /// Bytes received and not yet parsed, from `pos`.
    buf: Vec<u8>,
    pos: usize,
    ended: bool,
    columns: usize,
    batch_size: usize,
}

impl CopyReader {
    fn new(stream: CopyOutStream, columns: usize, batch_size: usize) -> Self {
        Self { stream: Box::pin(stream), buf: Vec::new(), pos: 0, ended: false, columns, batch_size: batch_size.max(1) }
    }

    /// Up to `batch_size` rows; none once the `COPY` is over.
    async fn next(&mut self) -> Result<CopyRows> {
        let mut parser = LineParser::new(self.columns, self.batch_size);
        while parser.rows < self.batch_size {
            if let Some(nl) = memchr(b'\n', &self.buf[self.pos..]) {
                let line = self.pos..self.pos + nl;
                parser.line(&self.buf[line])?;
                self.pos += nl + 1;
                continue;
            }
            if self.ended {
                if self.pos < self.buf.len() {
                    bail!("COPY output ended in the middle of a row");
                }
                break;
            }
            // Keep only the unparsed tail, then read more.
            self.buf.drain(..self.pos);
            self.pos = 0;
            match self.stream.next().await {
                Some(chunk) => self.buf.extend_from_slice(&chunk.context("reading COPY output")?),
                None => self.ended = true,
            }
        }
        parser.finish()
    }
}

fn memchr(needle: u8, haystack: &[u8]) -> Option<usize> {
    haystack.iter().position(|&b| b == needle)
}

/// Builds [`CopyRows`] from `COPY` text lines.
struct LineParser {
    text: Vec<u8>,
    cells: Vec<Option<(u32, u32)>>,
    columns: usize,
    rows: usize,
}

impl LineParser {
    fn new(columns: usize, rows: usize) -> Self {
        Self { text: Vec::new(), cells: Vec::with_capacity(columns * rows), columns, rows: 0 }
    }

    /// One line (without its newline).
    fn line(&mut self, line: &[u8]) -> Result<()> {
        let mut fields = 0;
        for field in line.split(|&b| b == b'\t') {
            fields += 1;
            if field == b"\\N" {
                self.cells.push(None);
                continue;
            }
            let start = self.text.len();
            if field.contains(&b'\\') {
                unescape_into(field, &mut self.text)?;
            } else {
                self.text.extend_from_slice(field);
            }
            let end = self.text.len();
            if end > u32::MAX as usize {
                bail!("a COPY batch over 4 GiB of text: lower batch_size");
            }
            self.cells.push(Some((start as u32, end as u32)));
        }
        if fields != self.columns {
            bail!("a COPY row has {fields} values, the query {} columns", self.columns);
        }
        self.rows += 1;
        Ok(())
    }

    fn finish(self) -> Result<CopyRows> {
        let text = String::from_utf8(self.text).context("COPY output is not UTF-8 (client_encoding must be UTF8)")?;
        Ok(CopyRows { text, cells: self.cells, columns: self.columns, rows: self.rows })
    }
}

/// Appends `field` with its backslash escapes resolved (`\b \f \n \r \t \v`,
/// `\\`, octal `\ooo`, hex `\xhh`; any other `\c` is `c`).
fn unescape_into(field: &[u8], out: &mut Vec<u8>) -> Result<()> {
    let mut i = 0;
    while i < field.len() {
        let b = field[i];
        if b != b'\\' {
            out.push(b);
            i += 1;
            continue;
        }
        let Some(&c) = field.get(i + 1) else { bail!("COPY value ends with a lone backslash") };
        i += 2;
        match c {
            b'b' => out.push(0x08),
            b'f' => out.push(0x0c),
            b'n' => out.push(b'\n'),
            b'r' => out.push(b'\r'),
            b't' => out.push(b'\t'),
            b'v' => out.push(0x0b),
            b'0'..=b'7' => {
                let mut v = u32::from(c - b'0');
                for _ in 0..2 {
                    match field.get(i) {
                        Some(&d @ b'0'..=b'7') => {
                            v = v * 8 + u32::from(d - b'0');
                            i += 1;
                        }
                        _ => break,
                    }
                }
                out.push(v as u8);
            }
            b'x' if field.get(i).is_some_and(u8::is_ascii_hexdigit) => {
                let mut v = 0u32;
                for _ in 0..2 {
                    match field.get(i).and_then(|d| (*d as char).to_digit(16)) {
                        Some(d) => {
                            v = v * 16 + d;
                            i += 1;
                        }
                        None => break,
                    }
                }
                out.push(v as u8);
            }
            other => out.push(other),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(lines: &[&[u8]], columns: usize) -> Result<CopyRows> {
        let mut p = LineParser::new(columns, lines.len());
        for l in lines {
            p.line(l)?;
        }
        p.finish()
    }

    #[test]
    fn lines_split_into_cells_with_nulls() {
        let rows = parse(&[b"1\tabc\t\\N", b"2\t\t"], 3).unwrap();
        assert_eq!(rows.rows, 2);
        assert_eq!((rows.cell(0, 0), rows.cell(0, 1), rows.cell(0, 2)), (Some("1"), Some("abc"), None));
        assert_eq!((rows.cell(1, 1), rows.cell(1, 2)), (Some(""), Some("")), "empty is not NULL");
    }

    #[test]
    fn escapes_are_resolved() {
        let rows = parse(&[br"a\tb\nc\\d\rx", br"\101\x42\\N\q", br"\\x0a1f"], 1).unwrap();
        assert_eq!(rows.cell(0, 0), Some("a\tb\nc\\d\rx"));
        assert_eq!(rows.cell(1, 0), Some("AB\\Nq"), "an escaped backslash before N is text, not NULL");
        assert_eq!(rows.cell(2, 0), Some("\\x0a1f"), "bytea hex keeps its backslash");
    }

    #[test]
    fn utf8_passes_through() {
        let rows = parse(&["ação\tµs".as_bytes()], 2).unwrap();
        assert_eq!((rows.cell(0, 0), rows.cell(0, 1)), (Some("ação"), Some("µs")));
    }

    #[test]
    fn a_row_with_the_wrong_width_fails() {
        let e = parse(&[b"1\t2"], 3).err().unwrap().to_string();
        assert!(e.contains("2 values, the query 3 columns"), "{e}");
        assert!(parse(&[br"abc\"], 1).is_err(), "a lone trailing backslash");
    }
}
