//! One CSV file → Arrow batches, streamed: it holds the inference sample
//! (at most `infer_rows` records, emitted first) and one batch, never the
//! file.

use std::collections::VecDeque;
use std::io::Read;
use std::sync::Arc;

use anyhow::{bail, Result};
use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema, SchemaRef};
use csv::StringRecord;
use encoding_rs_io::{DecodeReaderBytes, DecodeReaderBytesBuilder};
use tabular::table::infer_type;
use tabular::typed::{header_names, infer_cell, text_batch};

use crate::options::CsvOptions;

/// A CSV file being read.
pub struct CsvReader<R: Read> {
    rdr: csv::Reader<DecodeReaderBytes<R, Vec<u8>>>,
    schema: SchemaRef,
    /// The inference sample, not emitted yet.
    sample: VecDeque<StringRecord>,
    batch_size: usize,
    eof: bool,
}

impl<R: Read> CsvReader<R> {
    /// Reads the header (if any) and, without declared columns, the
    /// inference sample; the schema is then known.
    pub fn open(reader: R, opts: &CsvOptions) -> Result<Self> {
        // Transcodes to UTF-8 as it reads; a byte-order mark overrides the
        // configured encoding and is dropped (as encoding_rs' `decode`).
        let decoded = DecodeReaderBytesBuilder::new().encoding(Some(opts.encoding)).bom_override(true).build(reader);
        let rdr = csv::ReaderBuilder::new()
            .delimiter(opts.delimiter)
            .has_headers(false)
            // Record widths are checked here, with the line in the message.
            .flexible(true)
            .from_reader(decoded);
        let mut this = Self { rdr, schema: Arc::new(Schema::empty()), sample: VecDeque::new(), batch_size: opts.batch_size, eof: false };

        let header = if opts.header {
            match this.read()? {
                Some(rec) => Some(rec),
                None if opts.columns.is_some() => None,
                None => bail!("the file is empty: there is no header row"),
            }
        } else {
            None
        };

        if let Some(declared) = &opts.columns {
            if let Some(h) = &header {
                if h.len() != declared.fields().len() {
                    bail!(
                        "the header has {} columns, `columns` declares {}",
                        h.len(),
                        declared.fields().len()
                    );
                }
            }
            this.schema = Arc::clone(declared);
            return Ok(this);
        }

        while this.sample.len() < opts.infer_rows {
            match this.read()? {
                Some(rec) => this.sample.push_back(rec),
                None => break,
            }
        }
        let width = match (&header, this.sample.front()) {
            (Some(h), _) => h.len(),
            (None, Some(first)) => first.len(),
            (None, None) => bail!("the file is empty: there is no record to infer columns from"),
        };
        let names = match &header {
            Some(h) => header_names(h.iter())?,
            None => (0..width).map(|i| format!("col_{i}")).collect(),
        };
        let fields: Vec<Field> = names
            .into_iter()
            .enumerate()
            .map(|(c, name)| {
                let cells: Vec<_> = this.sample.iter().map(|r| infer_cell(r.get(c).unwrap_or(""))).collect();
                // Inferred columns are nullable, so files with and without
                // empty cells share one schema.
                Field::new(name, infer_type(&cells), true)
            })
            .collect();
        this.schema = Arc::new(Schema::new(fields));
        for rec in &this.sample {
            check_width(rec, width)?;
        }
        Ok(this)
    }

    /// The file's schema (declared or inferred).
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// The next batch of at most `batch_size` rows; `None` at the end.
    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let width = self.schema.fields().len();
        let mut rows = Vec::with_capacity(self.batch_size.min(64 * 1024));
        while rows.len() < self.batch_size {
            let rec = match self.sample.pop_front() {
                Some(rec) => rec,
                None => match self.read()? {
                    Some(rec) => rec,
                    None => break,
                },
            };
            check_width(&rec, width)?;
            rows.push(rec);
        }
        if rows.is_empty() {
            return Ok(None);
        }
        let columns: Vec<Vec<Option<&str>>> =
            (0..width).map(|c| rows.iter().map(|r| r.get(c)).collect()).collect();
        let batch = text_batch(&self.schema, &columns).map_err(|e| {
            let field = self.schema.field(e.column);
            anyhow::anyhow!("line {}, column {} (`{}`): {}", line(&rows[e.row]), e.column + 1, field.name(), e.message)
        })?;
        Ok(Some(batch))
    }

    /// The next record; `None` at the end of the file.
    fn read(&mut self) -> Result<Option<StringRecord>> {
        if self.eof {
            return Ok(None);
        }
        let mut rec = StringRecord::new();
        match self.rdr.read_record(&mut rec) {
            Ok(true) => Ok(Some(rec)),
            Ok(false) => {
                self.eof = true;
                Ok(None)
            }
            Err(e) => bail!("malformed CSV: {e}"),
        }
    }
}

fn line(rec: &StringRecord) -> u64 {
    rec.position().map_or(0, |p| p.line())
}

fn check_width(rec: &StringRecord, width: usize) -> Result<()> {
    if rec.len() != width {
        bail!("line {}: {} fields, expected {width}", line(rec), rec.len());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Date32Type, Float64Type, Int32Type, Int64Type};
    use arrow_array::Array;
    use arrow_schema::DataType;
    use serde_json::json;
    use std::cell::Cell as StdCell;
    use std::rc::Rc;

    fn opts(v: serde_json::Value) -> CsvOptions {
        CsvOptions::from_value(v).unwrap()
    }

    fn read_all(bytes: &[u8], o: &CsvOptions) -> Result<(SchemaRef, Vec<RecordBatch>)> {
        let mut r = CsvReader::open(bytes, o)?;
        let mut out = Vec::new();
        while let Some(b) = r.next_batch()? {
            out.push(b);
        }
        Ok((r.schema(), out))
    }

    fn types(s: &Schema) -> Vec<DataType> {
        s.fields().iter().map(|f| f.data_type().clone()).collect()
    }

    #[test]
    fn infers_nullable_types_from_the_sample() {
        let csv = b"id,name,score,ok,day\n1,alice,1.5,true,2024-01-02\n2,bob,,false,2024-01-03\n";
        let (schema, batches) = read_all(csv, &opts(json!({}))).unwrap();
        assert_eq!(
            types(&schema),
            [DataType::Int64, DataType::Utf8, DataType::Float64, DataType::Boolean, DataType::Date32]
        );
        assert!(schema.fields().iter().all(|f| f.is_nullable()));
        assert_eq!(batches.len(), 1);
        let b = &batches[0];
        assert_eq!(b.column(0).as_primitive::<Int64Type>().values(), &[1, 2]);
        assert_eq!(b.column(1).as_string::<i32>().value(1), "bob");
        assert_eq!(b.column(2).as_primitive::<Float64Type>().value(0), 1.5);
        assert!(b.column(2).is_null(1));
        assert_eq!(b.column(4).as_primitive::<Date32Type>().value(0), 19724);
    }

    #[test]
    fn infer_rows_bounds_the_sample_and_later_rows_must_fit() {
        // The sample (2 rows) says int64; line 4 is not one.
        let csv = b"n\n1\n2\nthree\n";
        let o = opts(json!({"infer_rows": 2}));
        let err = read_all(csv, &o).unwrap_err().to_string();
        assert!(err.contains("line 4, column 1 (`n`): `three` is not a valid int64"), "{err}");
        // With a sample that sees it, the column is text.
        let (schema, _) = read_all(csv, &opts(json!({}))).unwrap();
        assert_eq!(types(&schema), [DataType::Utf8]);
    }

    #[test]
    fn without_a_header_columns_are_numbered() {
        let (schema, batches) = read_all(b"1;x\n2;y\n", &opts(json!({"header": false, "delimiter": ";"}))).unwrap();
        assert_eq!(schema.field(0).name(), "col_0");
        assert_eq!(schema.field(1).name(), "col_1");
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn empty_lines_are_skipped() {
        let (_, batches) = read_all(b"a,b\n\n1,2\n\n3,4\n", &opts(json!({}))).unwrap();
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn delimiter_and_quoting() {
        let csv = b"a|b\n\"x|y\"|\"say \"\"hi\"\"\"\n";
        let (_, batches) = read_all(csv, &opts(json!({"delimiter": "|"}))).unwrap();
        let b = &batches[0];
        assert_eq!(b.column(0).as_string::<i32>().value(0), "x|y");
        assert_eq!(b.column(1).as_string::<i32>().value(0), "say \"hi\"");
    }

    #[test]
    fn declared_columns_win_over_inference() {
        let o = opts(json!({"columns": [
            {"name": "id", "type": "int32", "nullable": false},
            {"name": "code", "type": "utf8"},
        ]}));
        // The header's names are not checked, only its width.
        let (schema, batches) = read_all(b"ID,CODE\n1,007\n2,\n", &o).unwrap();
        assert_eq!(schema.field(0), &Field::new("id", DataType::Int32, false));
        assert_eq!(types(&schema), [DataType::Int32, DataType::Utf8]);
        let b = &batches[0];
        assert_eq!(b.column(0).as_primitive::<Int32Type>().values(), &[1, 2]);
        assert_eq!(b.column(1).as_string::<i32>().value(0), "007", "no inference on a declared utf8");
        assert!(b.column(1).is_null(1));

        let err = read_all(b"a,b,c\n1,2,3\n", &o).err().unwrap().to_string();
        assert!(err.contains("the header has 3 columns, `columns` declares 2"), "{err}");
        let err = read_all(b"id,code\n,x\n", &o).unwrap_err().to_string();
        assert!(err.contains("line 2, column 1 (`id`): empty value in a non-nullable column"), "{err}");
        // An empty file is a valid, empty file when the columns are declared.
        assert!(read_all(b"", &o).unwrap().1.is_empty());
    }

    #[test]
    fn encodings_are_transcoded() {
        // "ação" in windows-1252 / latin1.
        let latin1 = b"name\na\xe7\xe3o\n";
        let (_, b) = read_all(latin1, &opts(json!({"encoding": "latin1"}))).unwrap();
        assert_eq!(b[0].column(0).as_string::<i32>().value(0), "ação");
        // UTF-16LE with a BOM: the BOM wins over the (default) utf-8.
        let mut utf16 = vec![0xFF, 0xFE];
        for u in "x\nç\n".encode_utf16() {
            utf16.extend_from_slice(&u.to_le_bytes());
        }
        let (schema, b) = read_all(&utf16, &opts(json!({}))).unwrap();
        assert_eq!(schema.field(0).name(), "x");
        assert_eq!(b[0].column(0).as_string::<i32>().value(0), "ç");
        // A UTF-8 BOM is not part of the first column's name.
        let (schema, _) = read_all(b"\xEF\xBB\xBFid\n1\n", &opts(json!({}))).unwrap();
        assert_eq!(schema.field(0).name(), "id");
    }

    #[test]
    fn ragged_rows_and_bad_headers_are_errors() {
        let err = read_all(b"a,b\n1,2\n3\n", &opts(json!({}))).unwrap_err().to_string();
        assert!(err.contains("line 3: 1 fields, expected 2"), "{err}");
        let err = read_all(b"a,a\n1,2\n", &opts(json!({}))).err().unwrap().to_string();
        assert!(err.contains("both named `a`"), "{err}");
        let (schema, _) = read_all(b"a,\n1,2\n", &opts(json!({}))).unwrap();
        assert_eq!(schema.field(1).name(), "col_1");
        assert!(read_all(b"", &opts(json!({}))).err().unwrap().to_string().contains("no header row"));
        assert!(read_all(b"", &opts(json!({"header": false}))).is_err());
        // A header-only file: zero rows, every column text.
        let (schema, batches) = read_all(b"a,b\n", &opts(json!({}))).unwrap();
        assert_eq!(types(&schema), [DataType::Utf8, DataType::Utf8]);
        assert!(batches.is_empty());
    }

    #[test]
    fn batches_respect_batch_size() {
        let mut csv = b"n\n".to_vec();
        for i in 0..2500 {
            csv.extend_from_slice(format!("{i}\n").as_bytes());
        }
        let (_, batches) = read_all(&csv, &opts(json!({"batch_size": 1000, "infer_rows": 10}))).unwrap();
        let sizes: Vec<_> = batches.iter().map(|b| b.num_rows()).collect();
        assert_eq!(sizes, [1000, 1000, 500]);
        let all: Vec<i64> =
            batches.iter().flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec()).collect();
        assert_eq!(all, (0..2500).collect::<Vec<_>>());
    }

    /// An endless CSV (`n` then 0, 1, 2, …) that counts the bytes read.
    struct Endless {
        next: u64,
        pending: Vec<u8>,
        read: Rc<StdCell<usize>>,
    }

    impl Read for Endless {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.pending.is_empty() {
                self.pending = if self.next == 0 && self.read.get() == 0 {
                    b"n\n".to_vec()
                } else {
                    let line = format!("{}\n", self.next);
                    self.next += 1;
                    line.into_bytes()
                };
            }
            let n = self.pending.len().min(buf.len());
            buf[..n].copy_from_slice(&self.pending[..n]);
            self.pending.drain(..n);
            self.read.set(self.read.get() + n);
            Ok(n)
        }
    }

    #[test]
    fn streams_batches_before_the_end_of_the_file() {
        // The input never ends: a reader that loaded the file would never
        // return. Batches arrive, and the bytes read stay bounded by the
        // sample + one batch (+ the reader's buffers).
        let read = Rc::new(StdCell::new(0));
        let src = Endless { next: 0, pending: Vec::new(), read: Rc::clone(&read) };
        let mut r = CsvReader::open(src, &opts(json!({"infer_rows": 100, "batch_size": 1000}))).unwrap();
        assert_eq!(types(&r.schema()), [DataType::Int64]);
        let mut seen = 0i64;
        for _ in 0..50 {
            let b = r.next_batch().unwrap().unwrap();
            assert_eq!(b.num_rows(), 1000);
            let col = b.column(0).as_primitive::<Int64Type>();
            assert_eq!(col.value(0), seen);
            seen += 1000;
            let consumed: usize = 2 + (0..seen).map(|i| i.to_string().len() + 1).sum::<usize>();
            assert!(read.get() < consumed + 64 * 1024, "read {} bytes for {seen} rows", read.get());
        }
    }
}
