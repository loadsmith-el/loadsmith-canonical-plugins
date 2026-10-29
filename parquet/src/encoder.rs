//! A Parquet writer that encodes the columns of each batch in parallel.
//!
//! It does exactly what `ArrowWriter` does — the same row group boundaries
//! (`max_row_group_row_count` rows, a batch split across them), the same column
//! writers fed the same leaves in the same order, the same footer with the
//! encoded Arrow schema — so the file is byte for byte the one `ArrowWriter`
//! writes. Only the encoding of a batch's columns, each by its own column
//! writer, runs on up to `threads` threads at once.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use parquet::arrow::arrow_writer::{compute_leaves, ArrowColumnWriter, ArrowRowGroupWriterFactory};
use parquet::arrow::{add_encoded_arrow_schema_to_metadata, ArrowSchemaConverter};
use parquet::errors::{ParquetError, Result};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::{SchemaDescriptor, Type};

pub struct ColumnParallelWriter<W: std::io::Write + Send> {
    writer: SerializedFileWriter<W>,
    schema: SchemaRef,
    /// Makes each row group's column writers.
    columns: ArrowRowGroupWriterFactory,
    /// For each top-level field, the index of its first leaf column writer.
    leaf_offsets: Vec<usize>,
    max_row_group_size: usize,
    threads: usize,
    in_progress: Option<RowGroup>,
}

/// The row group being written: one writer per leaf column.
struct RowGroup {
    writers: Vec<Mutex<ArrowColumnWriter>>,
    rows: usize,
}

impl<W: std::io::Write + Send> ColumnParallelWriter<W> {
    pub fn try_new(out: W, schema: SchemaRef, props: WriterProperties, threads: usize) -> Result<Self> {
        let mut props = props;
        let parquet = ArrowSchemaConverter::new().with_coerce_types(props.coerce_types()).convert(&schema)?;
        add_encoded_arrow_schema_to_metadata(&schema, &mut props);
        let max_row_group_size = props.max_row_group_row_count().unwrap_or(usize::MAX);
        let writer = SerializedFileWriter::new(out, parquet.root_schema_ptr(), Arc::new(props))?;
        let columns = ArrowRowGroupWriterFactory::new(&writer, schema.clone());
        let leaf_offsets = leaf_offsets(&parquet);
        Ok(Self { writer, schema, columns, leaf_offsets, max_row_group_size, threads: threads.max(1), in_progress: None })
    }

    /// Encodes `batch`, flushing a row group whenever one reaches
    /// `max_row_group_size` rows (a batch that crosses the limit is split).
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let buffered = self.in_progress.as_ref().map_or(0, |g| g.rows);
        if buffered + batch.num_rows() > self.max_row_group_size {
            let to_write = self.max_row_group_size - buffered;
            self.write(&batch.slice(0, to_write))?;
            return self.write(&batch.slice(to_write, batch.num_rows() - to_write));
        }
        if self.in_progress.is_none() {
            let writers = self.columns.create_column_writers(self.writer.flushed_row_groups().len())?;
            self.in_progress = Some(RowGroup { writers: writers.into_iter().map(Mutex::new).collect(), rows: 0 });
        }
        let group = self.in_progress.as_mut().expect("just set");
        encode(&self.schema, batch, &group.writers, &self.leaf_offsets, self.threads)?;
        group.rows += batch.num_rows();
        if group.rows >= self.max_row_group_size {
            self.flush()?;
        }
        Ok(())
    }

    /// Writes the buffered rows as a row group.
    pub fn flush(&mut self) -> Result<()> {
        let Some(group) = self.in_progress.take() else { return Ok(()) };
        let mut row_group = self.writer.next_row_group()?;
        for writer in group.writers {
            let writer = writer.into_inner().map_err(|_| ParquetError::General("a column writer panicked".into()))?;
            writer.close()?.append_to_row_group(&mut row_group)?;
        }
        row_group.close()?;
        Ok(())
    }

    /// Flushes the last row group and writes the footer.
    pub fn close(mut self) -> Result<()> {
        self.flush()?;
        self.writer.close()?;
        Ok(())
    }

    pub fn bytes_written(&self) -> usize {
        self.writer.bytes_written()
    }

    /// The anticipated encoded size of the row group in progress.
    pub fn in_progress_size(&self) -> usize {
        self.in_progress.as_ref().map_or(0, |g| {
            g.writers.iter().map(|w| w.lock().map(|w| w.get_estimated_total_bytes()).unwrap_or(0)).sum()
        })
    }
}

/// Encodes every column of `batch` into its writers: each field's leaves into
/// the writers starting at its offset, fields taken by up to `threads`
/// threads from a shared counter.
fn encode(
    schema: &SchemaRef,
    batch: &RecordBatch,
    writers: &[Mutex<ArrowColumnWriter>],
    leaf_offsets: &[usize],
    threads: usize,
) -> Result<()> {
    let fields = schema.fields();
    let field = |j: usize| -> Result<()> {
        let leaves = compute_leaves(&fields[j], batch.column(j))?;
        for (k, leaf) in leaves.iter().enumerate() {
            let mut writer = writers[leaf_offsets[j] + k]
                .lock()
                .map_err(|_| ParquetError::General("a column writer panicked".into()))?;
            writer.write(leaf)?;
        }
        Ok(())
    };
    let threads = threads.min(fields.len());
    if threads <= 1 {
        return (0..fields.len()).try_for_each(field);
    }
    let next = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                scope.spawn(|| -> Result<()> {
                    loop {
                        let j = next.fetch_add(1, Ordering::Relaxed);
                        if j >= fields.len() {
                            return Ok(());
                        }
                        field(j)?;
                    }
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap_or_else(|_| Err(ParquetError::General("an encoding thread panicked".into()))))
            .collect::<Result<Vec<()>>>()
            .map(|_| ())
    })
}

/// The index of each top-level field's first leaf column (fields with nested
/// types have several).
fn leaf_offsets(parquet: &SchemaDescriptor) -> Vec<usize> {
    fn leaves(t: &Type) -> usize {
        if t.is_primitive() {
            1
        } else {
            t.get_fields().iter().map(|f| leaves(f)).sum()
        }
    }
    let mut offset = 0;
    parquet
        .root_schema()
        .get_fields()
        .iter()
        .map(|f| {
            let at = offset;
            offset += leaves(f);
            at
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{ListBuilder, StringBuilder};
    use arrow_array::{ArrayRef, Decimal128Array, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;

    fn batch(seed: i64, rows: usize) -> RecordBatch {
        let ids: ArrayRef = Arc::new(Int64Array::from_iter((0..rows as i64).map(|i| (i % 7 != 0).then_some(i * 31 + seed))));
        let names: ArrayRef = Arc::new(StringArray::from_iter((0..rows).map(|i| (i % 5 != 0).then(|| format!("n{}", i as i64 + seed)))));
        let amounts: ArrayRef = Arc::new(
            Decimal128Array::from_iter((0..rows as i128).map(|i| Some(i * 12_345 + seed as i128)))
                .with_precision_and_scale(18, 4)
                .unwrap(),
        );
        let ratio: ArrayRef = Arc::new(Float64Array::from_iter_values((0..rows).map(|i| i as f64 / 3.0)));
        let ts: ArrayRef =
            Arc::new(TimestampMicrosecondArray::from_iter_values((0..rows as i64).map(|i| i * 1_000_003 + seed)).with_timezone("UTC"));
        let mut tags = ListBuilder::new(StringBuilder::new());
        for i in 0..rows {
            if i % 4 == 0 {
                tags.append_null();
            } else {
                for k in 0..(i % 3) {
                    tags.values().append_value(format!("t{k}"));
                }
                tags.append(true);
            }
        }
        let tags: ArrayRef = Arc::new(tags.finish());
        RecordBatch::try_from_iter([("id", ids), ("name", names), ("amount", amounts), ("ratio", ratio), ("ts", ts), ("tags", tags)])
            .unwrap()
    }

    /// The same batches through ArrowWriter and through the parallel writer,
    /// with row groups that batches cross: the same bytes.
    #[test]
    fn writes_the_bytes_arrow_writer_writes() {
        let batches: Vec<RecordBatch> = (0..9).map(|s| batch(s, 1_000 + s as usize * 137)).collect();
        let schema = batches[0].schema();
        let props = || {
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .set_max_row_group_row_count(Some(2_500))
                .build()
        };
        let mut expected = Vec::new();
        let mut w = ArrowWriter::try_new(&mut expected, schema.clone(), Some(props())).unwrap();
        for b in &batches {
            w.write(b).unwrap();
        }
        w.close().unwrap();
        for threads in [1, 3, 8] {
            let mut got = Vec::new();
            let mut w = ColumnParallelWriter::try_new(&mut got, schema.clone(), props(), threads).unwrap();
            for b in &batches {
                w.write(b).unwrap();
            }
            w.close().unwrap();
            assert!(got == expected, "{threads} threads: {} vs {} bytes", got.len(), expected.len());
        }
    }

    #[test]
    fn nested_fields_take_several_leaves() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("l", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), true),
            Field::new("t", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        ]));
        let parquet = ArrowSchemaConverter::new().convert(&schema).unwrap();
        assert_eq!(leaf_offsets(&parquet), [0, 1, 2]);
    }
}
