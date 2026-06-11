use anyhow::{bail, Context, Result};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use loadsmith_plugin_sdk::DestinationPlugin;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde::Deserialize;
use std::path::PathBuf;

/// Smallest `max_file_size` we accept. A Parquet file pays a fixed cost (the
/// `PAR1` magic at both ends plus a thrift-encoded footer carrying the schema
/// and per-column statistics) before a single data byte lands, so anything
/// tiny would be almost entirely overhead. 64 KiB is a schema-agnostic sanity
/// floor — ~2 orders of magnitude below a typical target like 500 KiB — not a
/// hard Parquet limit.
const MIN_FILE_SIZE: u64 = 64 * 1024;

/// Compression codec for the Parquet writer. Limited to the common,
/// dependency-free codecs the `parquet` crate enables by default.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
enum CompressionKind {
    #[default]
    Snappy,
    Gzip,
    Zstd,
    Lz4,
    Uncompressed,
}

impl CompressionKind {
    fn to_parquet(self) -> Compression {
        match self {
            CompressionKind::Snappy => Compression::SNAPPY,
            CompressionKind::Gzip => Compression::GZIP(Default::default()),
            CompressionKind::Zstd => Compression::ZSTD(Default::default()),
            CompressionKind::Lz4 => Compression::LZ4,
            CompressionKind::Uncompressed => Compression::UNCOMPRESSED,
        }
    }

    /// The token embedded in the output filename (`events.snappy.parquet`).
    fn label(self) -> &'static str {
        match self {
            CompressionKind::Snappy => "snappy",
            CompressionKind::Gzip => "gzip",
            CompressionKind::Zstd => "zstd",
            CompressionKind::Lz4 => "lz4",
            CompressionKind::Uncompressed => "uncompressed",
        }
    }
}

#[derive(Debug, Deserialize)]
struct ParquetConfig {
    /// Output directory. Files are created inside it.
    path: PathBuf,
    /// Filename prefix, e.g. `events` → `events.snappy.parquet`.
    prefix: String,
    #[serde(default)]
    compression: CompressionKind,
    /// Docker-style size string ("500KiB", "10MB", "2GiB"). Absent ⇒ single
    /// file. A bare number is interpreted as KiB.
    max_file_size: Option<String>,
}

/// Parse a Docker-style size string into bytes. A bare integer (no unit) is
/// interpreted as KiB — a 500-byte Parquet chunk is nonsensical, so the
/// domain default is KiB rather than Docker's raw bytes. Explicit suffixes
/// ("500b", "10MB", "2GiB") are honored as-is via the `bytesize` crate.
fn parse_size(s: &str) -> Result<u64> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        bail!("max_file_size is empty");
    }
    if let Ok(n) = trimmed.parse::<u64>() {
        return Ok(n * 1024);
    }
    let bs: bytesize::ByteSize = trimmed
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid max_file_size {trimmed:?}: {e}"))?;
    Ok(bs.as_u64())
}

/// Build the filename for a given sequence number. In single-file mode
/// (`threshold` absent) the sequence is omitted entirely.
fn file_name(prefix: &str, compression: CompressionKind, seq: Option<u64>) -> String {
    match seq {
        Some(n) => format!("{prefix}.{n:08}.{}.parquet", compression.label()),
        None => format!("{prefix}.{}.parquet", compression.label()),
    }
}

pub struct ParquetPlugin {
    config: Option<ParquetConfig>,
    /// Byte threshold parsed from `max_file_size`; `None` ⇒ single-file mode.
    threshold: Option<u64>,
    /// Writer for the chunk currently being filled.
    writer: Option<ArrowWriter<std::fs::File>>,
    /// Schema captured from the first batch, reused to open later chunks.
    schema: Option<SchemaRef>,
    /// Sequence number of the chunk currently open (0 ⇒ none opened yet).
    seq: u64,
    /// Path of the chunk currently open, moved to `ready` once finalized.
    current_path: Option<PathBuf>,
    /// Every file created during the run, so `cancel()` can remove all of them.
    created: Vec<PathBuf>,
    /// Files finalized (footer written) since the last `take_ready_objects`,
    /// announced to the core as `ObjectReady` so a sink can deliver them.
    ready: Vec<PathBuf>,
    rows_written: u64,
}

impl ParquetPlugin {
    pub fn new() -> Self {
        Self {
            config: None,
            threshold: None,
            writer: None,
            schema: None,
            seq: 0,
            current_path: None,
            created: Vec::new(),
            ready: Vec::new(),
            rows_written: 0,
        }
    }

    /// Open the next chunk writer against a freshly created file, recording its
    /// path for potential cleanup.
    fn open_writer(&mut self, schema: SchemaRef) -> Result<()> {
        let cfg = self.config.as_ref().unwrap();
        // In single-file mode the sequence is omitted from the name.
        let seq = if self.threshold.is_some() {
            self.seq += 1;
            Some(self.seq)
        } else {
            None
        };
        let path = cfg.path.join(file_name(&cfg.prefix, cfg.compression, seq));
        let file = std::fs::File::create(&path)
            .with_context(|| format!("cannot create output file: {}", path.display()))?;
        let props = WriterProperties::builder()
            .set_compression(cfg.compression.to_parquet())
            .build();
        let writer = ArrowWriter::try_new(file, schema, Some(props))
            .with_context(|| format!("cannot open parquet writer for {}", path.display()))?;
        self.created.push(path.clone());
        self.current_path = Some(path);
        self.writer = Some(writer);
        Ok(())
    }

    /// Finalizes the open writer (writes the footer) and records its path as
    /// ready for delivery. No-op when no writer is open.
    fn close_current(&mut self) -> Result<()> {
        if let Some(writer) = self.writer.take() {
            writer.close().context("parquet close error")?;
            if let Some(path) = self.current_path.take() {
                self.ready.push(path);
            }
        }
        Ok(())
    }

    /// Estimated on-disk size of the current chunk: bytes already flushed plus
    /// the in-progress (buffered) row group.
    fn current_size(&self) -> u64 {
        match self.writer.as_ref() {
            Some(w) => w.bytes_written() as u64 + w.in_progress_size() as u64,
            None => 0,
        }
    }
}

#[async_trait]
impl DestinationPlugin for ParquetPlugin {
    fn plugin_name(&self) -> &str {
        "loadsmith-destination-parquet"
    }

    fn plugin_version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    fn capabilities(&self) -> Vec<String> {
        // `object_output` tells the core this destination stages files a sink
        // can deliver — it announces each via `take_ready_objects`/`ObjectReady`.
        vec!["batch_write".into(), "object_output".into()]
    }

    async fn configure(&mut self, config: serde_json::Value) -> Result<()> {
        let cfg: ParquetConfig =
            serde_json::from_value(config).context("invalid parquet destination config")?;

        // Validate only — no files opened here (that's prepare/write_batch).
        if cfg.prefix.trim().is_empty() {
            bail!("prefix must not be empty");
        }
        if !cfg.path.is_dir() {
            bail!("output directory does not exist: {}", cfg.path.display());
        }
        if let Some(raw) = &cfg.max_file_size {
            let bytes = parse_size(raw)?;
            if bytes < MIN_FILE_SIZE {
                bail!(
                    "max_file_size must be at least 64KiB (got {raw:?} = {bytes} bytes); \
                     this is a sanity floor, not a hard Parquet limit — a smaller cap \
                     would yield files that are mostly footer overhead"
                );
            }
            self.threshold = Some(bytes);
        }

        self.config = Some(cfg);
        Ok(())
    }

    async fn prepare(&mut self) -> Result<()> {
        // The schema is only known once the first batch arrives, so the writer
        // is opened lazily in write_batch(). Nothing to do here.
        Ok(())
    }

    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        if self.schema.is_none() {
            self.schema = Some(batch.schema());
        }
        if self.writer.is_none() {
            let schema = self.schema.clone().unwrap();
            self.open_writer(schema)?;
        }

        let rows = batch.num_rows() as u64;
        self.writer
            .as_mut()
            .unwrap()
            .write(&batch)
            .context("parquet write error")?;
        self.rows_written += rows;

        // Roll over to the next chunk once the current file passes the cap.
        if let Some(threshold) = self.threshold {
            if self.current_size() >= threshold {
                // Close + mark ready so the sink can deliver this chunk while
                // the pump keeps filling the next one. Next batch opens the
                // next chunk (seq is bumped in open_writer).
                self.close_current()?;
            }
        }
        Ok(())
    }

    fn take_ready_objects(&mut self) -> Vec<PathBuf> {
        std::mem::take(&mut self.ready)
    }

    async fn finalize(&mut self) -> Result<u64> {
        // Close the last open chunk and mark it ready. Zero batches ⇒ no schema
        // ever arrived ⇒ no file created (documented consequence of deferring
        // writer init to the first batch).
        self.close_current()?;
        Ok(self.rows_written)
    }

    async fn cancel(&mut self) {
        // Drop the in-progress writer without finalizing its footer, then remove
        // every file produced this run. A partial multi-chunk dataset is more
        // likely to mislead a downstream consumer (looks complete) than help.
        self.writer = None;
        for path in self.created.drain(..) {
            let _ = std::fs::remove_file(&path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_number_is_kib() {
        assert_eq!(parse_size("500").unwrap(), 500 * 1024);
        assert_eq!(parse_size("  64 ").unwrap(), 64 * 1024);
    }

    #[test]
    fn suffixed_sizes() {
        assert_eq!(parse_size("64KiB").unwrap(), 64 * 1024);
        assert_eq!(parse_size("1MiB").unwrap(), 1024 * 1024);
        assert_eq!(parse_size("1MB").unwrap(), 1_000_000);
        assert_eq!(parse_size("500b").unwrap(), 500);
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse_size("not-a-size").is_err());
        assert!(parse_size("").is_err());
    }

    #[test]
    fn single_file_name_omits_sequence() {
        assert_eq!(
            file_name("events", CompressionKind::Snappy, None),
            "events.snappy.parquet"
        );
        assert_eq!(
            file_name("orders-2024", CompressionKind::Uncompressed, None),
            "orders-2024.uncompressed.parquet"
        );
    }

    #[test]
    fn chunked_name_is_zero_padded_8_digits() {
        assert_eq!(
            file_name("events", CompressionKind::Snappy, Some(1)),
            "events.00000001.snappy.parquet"
        );
        assert_eq!(
            file_name("events", CompressionKind::Zstd, Some(42)),
            "events.00000042.zstd.parquet"
        );
    }

    #[test]
    fn padding_is_minimum_width_not_a_cap() {
        // {:08} is a minimum width — large sequences widen rather than truncate.
        assert_eq!(
            file_name("e", CompressionKind::Lz4, Some(100_000_000)),
            "e.100000000.lz4.parquet"
        );
    }

    #[test]
    fn compression_labels_match_codecs() {
        assert_eq!(CompressionKind::Snappy.label(), "snappy");
        assert_eq!(CompressionKind::Gzip.label(), "gzip");
        assert_eq!(CompressionKind::Zstd.label(), "zstd");
        assert_eq!(CompressionKind::Lz4.label(), "lz4");
        assert_eq!(CompressionKind::Uncompressed.label(), "uncompressed");
    }
}
