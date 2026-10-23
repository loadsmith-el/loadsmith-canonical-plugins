use anyhow::{bail, Context, Result};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use loadsmith_plugin_sdk::{telemetry, BatchRange, DestinationPlugin, ReadyObject};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::WriterProperties;
use serde::Deserialize;
use std::path::{Path, PathBuf};
use tracing::Span;

use crate::vocab::config_enum;

/// Smallest `max_file_size` we accept. A Parquet file pays a fixed cost (the
/// `PAR1` magic at both ends plus a thrift-encoded footer carrying the schema
/// and per-column statistics) before a single data byte lands, so anything
/// tiny would be almost entirely overhead. 64 KiB is a schema-agnostic sanity
/// floor — ~2 orders of magnitude below a typical target like 500 KiB — not a
/// hard Parquet limit.
const MIN_FILE_SIZE: u64 = 64 * 1024;

config_enum! {
    /// `compression`: the Parquet codec. Limited to the codecs the `parquet`
    /// crate enables by default.
    #[derive(Default)]
    enum CompressionKind for "compression" {
        #[default]
        Snappy = "snappy",
        Gzip = "gzip",
        Zstd = "zstd",
        Lz4 = "lz4",
        Uncompressed = "uncompressed",
    }
    retired { "none" => "uncompressed", "off" => "uncompressed", "zstandard" => "zstd", "gz" => "gzip" }
}

impl CompressionKind {
    /// The token embedded in the output filename (`events.snappy.parquet`).
    fn label(self) -> &'static str {
        self.as_str()
    }

    /// The codec, at `level` when given (gzip 0–10, zstd 1–22).
    fn to_parquet(self, level: Option<u32>) -> Result<Compression> {
        let bad = |e: parquet::errors::ParquetError| anyhow::anyhow!("invalid `compression_level`: {e}");
        Ok(match (self, level) {
            (CompressionKind::Gzip, Some(l)) => Compression::GZIP(GzipLevel::try_new(l).map_err(bad)?),
            (CompressionKind::Zstd, Some(l)) => {
                let l = i32::try_from(l).map_err(|_| anyhow::anyhow!("invalid `compression_level` {l}"))?;
                Compression::ZSTD(ZstdLevel::try_new(l).map_err(bad)?)
            }
            (other, Some(_)) => bail!("`compression_level` applies to `gzip` and `zstd` only, not `{}`", other.as_str()),
            (CompressionKind::Snappy, None) => Compression::SNAPPY,
            (CompressionKind::Gzip, None) => Compression::GZIP(Default::default()),
            (CompressionKind::Zstd, None) => Compression::ZSTD(Default::default()),
            (CompressionKind::Lz4, None) => Compression::LZ4,
            (CompressionKind::Uncompressed, None) => Compression::UNCOMPRESSED,
        })
    }
}

#[derive(Debug, Deserialize)]
struct ParquetConfig {
    /// Output directory, created when missing. Files are created inside it.
    path: PathBuf,
    /// Filename prefix, e.g. `events` → `events.snappy.parquet`.
    prefix: String,
    #[serde(default)]
    compression: CompressionKind,
    /// Codec level (gzip 0–10, zstd 1–22); the codec's default when absent.
    compression_level: Option<u32>,
    /// Size with a unit ("500KiB", "10MB", "2GiB"). Absent ⇒ single file.
    max_file_size: Option<String>,
    /// Rows per row group (default [`DEFAULT_ROW_GROUP_SIZE`]).
    row_group_size: Option<usize>,
}

/// Rows per row group unless `row_group_size` says otherwise. A row group is
/// buffered in memory until written, so this bounds the writer's memory; 128Ki
/// rows is the range DuckDB and Spark use (the `parquet` crate's own default,
/// 1Mi rows, held a million-row table in memory).
const DEFAULT_ROW_GROUP_SIZE: usize = 128 * 1024;

/// Parses a size string with a unit into bytes ("500KiB", "10MB", "2GiB",
/// "65536B"). A bare number is rejected: it used to mean KiB, and a unit makes
/// the size unambiguous (the same rule as the engine's duration strings).
fn parse_size(s: &str) -> Result<u64> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        bail!("max_file_size is empty");
    }
    if trimmed.parse::<u64>().is_ok() {
        bail!(
            "max_file_size {trimmed:?} has no unit: write e.g. \"{trimmed}KiB\" (what a bare number \
             used to mean), \"{trimmed}MiB\" or \"{trimmed}B\""
        );
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

/// The temp name a file is written under until it is complete.
fn temp_name(final_name: &str) -> String {
    format!(".{final_name}.tmp")
}

/// Whether `name` is a file this plugin writes for `prefix` — a single file
/// or a chunk, with any codec, or the temp name of one.
fn is_dataset_file(prefix: &str, name: &str) -> bool {
    let name = match name.strip_prefix('.').and_then(|n| n.strip_suffix(".tmp")) {
        Some(inner) => inner,
        None => name,
    };
    let Some(rest) = name.strip_prefix(prefix).and_then(|r| r.strip_prefix('.')) else {
        return false;
    };
    let Some(rest) = rest.strip_suffix(".parquet") else {
        return false;
    };
    let label = match rest.split_once('.') {
        Some((seq, label)) if seq.len() >= 8 && seq.bytes().all(|b| b.is_ascii_digit()) => label,
        Some(_) => return false,
        None => rest,
    };
    CompressionKind::VALUES.iter().any(|(l, _)| *l == label)
}

/// Removes the files of the `prefix` dataset in `dir`, except `keep` — one
/// `cleanup` span (`file.directory`, `loadsmith.files.removed`).
fn remove_dataset(dir: &Path, prefix: &str, keep: Option<&Path>) -> Result<()> {
    let span = if telemetry::enabled() {
        tracing::info_span!(
            "cleanup",
            file.directory = %dir.display(),
            loadsmith.files.removed = tracing::field::Empty,
            error = tracing::field::Empty,
        )
    } else {
        Span::none()
    };
    let mut removed = 0u64;
    let result = span.in_scope(|| remove_dataset_files(dir, prefix, keep, &mut removed));
    span.record("loadsmith.files.removed", removed);
    if let Err(e) = &result {
        span.record("error", format!("{e:#}"));
    }
    result
}

fn remove_dataset_files(dir: &Path, prefix: &str, keep: Option<&Path>, removed: &mut u64) -> Result<()> {
    for entry in std::fs::read_dir(dir).with_context(|| format!("cannot list {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        let is_ours = entry.file_name().to_str().is_some_and(|n| is_dataset_file(prefix, n));
        if is_ours && Some(path.as_path()) != keep && entry.file_type()?.is_file() {
            std::fs::remove_file(&path).with_context(|| format!("cannot remove stale {}", path.display()))?;
            tracing::info!("removed {} (previous run)", path.display());
            *removed += 1;
        }
    }
    Ok(())
}

/// Moves the finished temp file into place — a `rename` span.
fn rename(temp: &Path, path: &Path) -> Result<()> {
    let span = if telemetry::enabled() {
        tracing::info_span!("rename", file.path = %temp.display(), loadsmith.file.target = %path.display())
    } else {
        Span::none()
    };
    span.in_scope(|| {
        std::fs::rename(temp, path).with_context(|| format!("cannot rename {} to {}", temp.display(), path.display()))
    })
}

/// What `configure` validated.
struct Settings {
    dir: PathBuf,
    prefix: String,
    compression: CompressionKind,
    props: WriterProperties,
    /// Byte threshold parsed from `max_file_size`; `None` ⇒ single-file mode.
    threshold: Option<u64>,
}

/// The chunk being written: under its temp name until closed.
struct OpenFile {
    writer: ArrowWriter<std::fs::File>,
    temp: PathBuf,
    path: PathBuf,
    /// The file's `write <file>` span, open until the file is closed.
    span: Span,
    rows: u64,
    /// The ordinal of the first batch written to this file.
    first_batch: u64,
}

/// The span of one file written: from its creation to its rename into place
/// (`file.path`, `file.size`, `loadsmith.object.rows`). A root of the
/// plugin's trace, not a child of the batch that opened it.
fn file_span(name: &str, path: &Path) -> Span {
    if !telemetry::enabled() {
        return Span::none();
    }
    tracing::info_span!(
        parent: None,
        "file",
        otel.name = format!("write {name}"),
        file.path = %path.display(),
        file.size = tracing::field::Empty,
        loadsmith.object.rows = tracing::field::Empty,
        error = tracing::field::Empty,
    )
}

pub struct ParquetPlugin {
    settings: Option<Settings>,
    current: Option<OpenFile>,
    /// Sequence number of the chunk currently open (0 ⇒ none opened yet).
    seq: u64,
    /// Every file created during the run (temp or final), so `cancel()` can
    /// remove all of them.
    created: Vec<PathBuf>,
    /// Files finalized (footer written, renamed) since the last `take_ready`,
    /// with the batches each holds, announced to the core as `ObjectReady` so
    /// a sink can deliver them.
    ready: Vec<ReadyObject>,
    rows_written: u64,
    /// Batches received so far: the ordinal of the last one (1-based).
    batches: u64,
}

impl ParquetPlugin {
    pub fn new() -> Self {
        Self {
            settings: None,
            current: None,
            seq: 0,
            created: Vec::new(),
            ready: Vec::new(),
            rows_written: 0,
            batches: 0,
        }
    }

    fn settings(&self) -> Result<&Settings> {
        self.settings.as_ref().context("the destination is not configured")
    }

    /// Opens the next file (under its temp name) for `batch`'s schema.
    fn open_writer(&mut self, batch: &RecordBatch) -> Result<()> {
        // In single-file mode the sequence is omitted from the name.
        let seq = if self.settings()?.threshold.is_some() {
            self.seq += 1;
            Some(self.seq)
        } else {
            None
        };
        let s = self.settings()?;
        let name = file_name(&s.prefix, s.compression, seq);
        let path = s.dir.join(&name);
        let temp = s.dir.join(temp_name(&name));
        let props = s.props.clone();
        let file = std::fs::File::create(&temp)
            .with_context(|| format!("cannot create output file: {}", temp.display()))?;
        self.created.push(temp.clone());
        let writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
            .with_context(|| format!("cannot open parquet writer for {}", temp.display()))?;
        let span = file_span(&name, &path);
        self.current = Some(OpenFile { writer, temp, path, span, rows: 0, first_batch: self.batches });
        Ok(())
    }

    /// Finalizes the open file (writes the footer), renames it to its final
    /// name and records it as ready for delivery. No-op when none is open.
    fn close_current(&mut self) -> Result<()> {
        if let Some(f) = self.current.take() {
            let span = f.span.clone();
            let batches = BatchRange { first: f.first_batch, last: self.batches };
            let result = span.in_scope(|| -> Result<PathBuf> {
                f.writer.close().context("parquet close error")?;
                rename(&f.temp, &f.path)?;
                if !span.is_none() {
                    span.record("loadsmith.object.rows", f.rows);
                    if let Ok(meta) = std::fs::metadata(&f.path) {
                        span.record("file.size", meta.len());
                    }
                }
                Ok(f.path)
            });
            match result {
                Ok(path) => {
                    self.created.push(path.clone());
                    self.ready.push(ReadyObject { path, batches: Some(batches) });
                }
                Err(e) => {
                    span.record("error", format!("{e:#}"));
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    /// Estimated on-disk size of the current chunk: bytes already flushed plus
    /// the in-progress (buffered) row group.
    fn current_size(&self) -> u64 {
        match self.current.as_ref() {
            Some(f) => f.writer.bytes_written() as u64 + f.writer.in_progress_size() as u64,
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
        // can deliver — it announces each via `take_ready`/`ObjectReady`.
        vec!["batch_write".into(), "object_output".into()]
    }

    async fn configure(&mut self, config: loadsmith_plugin_sdk::PluginConfig) -> Result<()> {
        let cfg: ParquetConfig = config.parse()?;

        // Validate only — no files touched here (that's prepare/write_batch).
        if cfg.prefix.trim().is_empty() {
            bail!("prefix must not be empty");
        }
        if cfg.prefix.contains('/') || cfg.prefix.starts_with('.') {
            bail!("prefix {:?} must be a plain file-name prefix (no `/`, no leading `.`)", cfg.prefix);
        }
        if cfg.path.exists() && !cfg.path.is_dir() {
            bail!("path {} exists and is not a directory", cfg.path.display());
        }
        let threshold = match &cfg.max_file_size {
            Some(raw) => {
                let bytes = parse_size(raw)?;
                if bytes < MIN_FILE_SIZE {
                    bail!(
                        "max_file_size must be at least 64KiB (got {raw:?} = {bytes} bytes); \
                         this is a sanity floor, not a hard Parquet limit — a smaller cap \
                         would yield files that are mostly footer overhead"
                    );
                }
                Some(bytes)
            }
            None => None,
        };
        let mut props = WriterProperties::builder().set_compression(cfg.compression.to_parquet(cfg.compression_level)?);
        let rows = cfg.row_group_size.unwrap_or(DEFAULT_ROW_GROUP_SIZE);
        if rows == 0 {
            bail!("row_group_size must be at least 1");
        }
        props = props.set_max_row_group_size(rows);
        self.settings = Some(Settings {
            dir: cfg.path,
            prefix: cfg.prefix,
            compression: cfg.compression,
            props: props.build(),
            threshold,
        });
        Ok(())
    }

    async fn prepare(&mut self) -> Result<()> {
        let s = self.settings()?;
        std::fs::create_dir_all(&s.dir)
            .with_context(|| format!("cannot create output directory: {}", s.dir.display()))?;
        // Chunked mode streams each chunk to the sink as soon as it is
        // closed, so the previous run's set has to go first: otherwise a run
        // with fewer chunks leaves stale ones behind. (Single-file mode
        // replaces its file atomically at the end instead, see finalize.)
        if s.threshold.is_some() {
            remove_dataset(&s.dir, &s.prefix, None)?;
        }
        // The writer is opened lazily in write_batch(): the schema is only
        // known once the first batch arrives.
        Ok(())
    }

    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        self.batches += 1;
        if self.current.is_none() {
            self.open_writer(&batch)?;
        }
        let rows = batch.num_rows() as u64;
        let current = self.current.as_mut().context("no open parquet writer")?;
        current.writer.write(&batch).context("parquet write error")?;
        current.rows += rows;
        self.rows_written += rows;

        // Roll over to the next chunk once the current file passes the cap.
        if let Some(threshold) = self.settings()?.threshold {
            if self.current_size() >= threshold {
                // Close + mark ready so the sink can deliver this chunk while
                // the pump keeps filling the next one. Next batch opens the
                // next chunk (seq is bumped in open_writer).
                self.close_current()?;
            }
        }
        Ok(())
    }

    fn take_ready(&mut self) -> Vec<ReadyObject> {
        std::mem::take(&mut self.ready)
    }

    fn first_object_seq(&mut self, seq: u64) {
        // `open_writer` bumps the sequence before naming a chunk.
        self.seq = seq.saturating_sub(1);
    }

    async fn finalize(&mut self) -> Result<u64> {
        // Close the last open chunk and mark it ready. Zero batches ⇒ no schema
        // ever arrived ⇒ no file created.
        self.close_current()?;
        let s = self.settings()?;
        if s.threshold.is_none() {
            // The new file is in place (renamed over the old one); drop what
            // else a previous run left under this prefix (a chunked set, a
            // file with another codec).
            let keep = self.created.last().cloned();
            remove_dataset(&s.dir, &s.prefix, keep.as_deref())?;
        }
        Ok(self.rows_written)
    }

    async fn cancel(&mut self) {
        // Drop the in-progress writer without finalizing its footer, then remove
        // every file produced this run. A partial multi-chunk dataset is more
        // likely to mislead a downstream consumer (looks complete) than help.
        if let Some(f) = self.current.take() {
            f.span.record("error", "cancelled");
        }
        for path in self.created.drain(..) {
            let _ = std::fs::remove_file(&path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Int64Array};
    use serde_json::json;
    use std::sync::Arc;

    #[test]
    fn sizes_need_a_unit() {
        let e = parse_size("500").unwrap_err().to_string();
        assert!(e.contains("has no unit") && e.contains("\"500KiB\""), "{e}");
        assert_eq!(parse_size("64KiB").unwrap(), 64 * 1024);
        assert_eq!(parse_size(" 1MiB ").unwrap(), 1024 * 1024);
        assert_eq!(parse_size("1MB").unwrap(), 1_000_000);
        assert_eq!(parse_size("500b").unwrap(), 500);
        assert!(parse_size("not-a-size").is_err());
        assert!(parse_size("").is_err());
    }

    #[test]
    fn file_names() {
        assert_eq!(file_name("events", CompressionKind::Snappy, None), "events.snappy.parquet");
        assert_eq!(file_name("events", CompressionKind::Zstd, Some(42)), "events.00000042.zstd.parquet");
        // {:08} is a minimum width — large sequences widen rather than truncate.
        assert_eq!(file_name("e", CompressionKind::Lz4, Some(100_000_000)), "e.100000000.lz4.parquet");
        assert_eq!(temp_name("e.lz4.parquet"), ".e.lz4.parquet.tmp");
    }

    #[test]
    fn dataset_files_are_matched_exactly() {
        for ours in ["ev.snappy.parquet", "ev.00000001.zstd.parquet", "ev.123456789.uncompressed.parquet", ".ev.gzip.parquet.tmp"] {
            assert!(is_dataset_file("ev", ours), "{ours}");
        }
        for other in ["ev2.snappy.parquet", "ev.v2.snappy.parquet", "ev.1.snappy.parquet", "ev.snappy.parquet.bak", "ev.brotli.parquet", "x.ev.snappy.parquet"] {
            assert!(!is_dataset_file("ev", other), "{other}");
        }
        assert!(is_dataset_file("a.b", "a.b.00000003.lz4.parquet"));
    }

    #[test]
    fn compression_values_and_levels() {
        let e = serde_json::from_value::<CompressionKind>(json!("none")).unwrap_err().to_string();
        assert!(e.contains("did you mean `uncompressed`?"), "{e}");
        assert!(CompressionKind::Zstd.to_parquet(Some(3)).is_ok());
        assert!(CompressionKind::Gzip.to_parquet(Some(11)).is_err());
        assert!(CompressionKind::Snappy.to_parquet(Some(1)).unwrap_err().to_string().contains("gzip"));
    }

    fn scratch(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("loadsmith-parquet-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        d
    }

    fn listing(dir: &Path) -> Vec<String> {
        let mut v: Vec<String> =
            std::fs::read_dir(dir).unwrap().map(|e| e.unwrap().file_name().to_string_lossy().into_owned()).collect();
        v.sort();
        v
    }

    async fn run(dir: &Path, extra: serde_json::Value, batches: usize) -> u64 {
        let mut cfg = json!({ "path": dir, "prefix": "ev" });
        if let (Some(o), Some(e)) = (cfg.as_object_mut(), extra.as_object()) {
            o.extend(e.clone());
        }
        let mut p = ParquetPlugin::new();
        p.configure(loadsmith_plugin_sdk::PluginConfig::new(cfg, loadsmith_plugin_sdk::UnknownKeys::Error)).await.unwrap();
        p.prepare().await.unwrap();
        for b in 0..batches {
            let ids: ArrayRef = Arc::new(Int64Array::from_iter_values((0..20_000).map(|i| i * 7919 + b as i64)));
            p.write_batch(RecordBatch::try_from_iter([("id", ids)]).unwrap()).await.unwrap();
        }
        p.finalize().await.unwrap()
    }

    #[tokio::test]
    async fn reruns_leave_no_stale_files() {
        let dir = scratch("rerun").join("nested");
        let chunked = json!({ "max_file_size": "64KiB", "compression": "uncompressed", "row_group_size": 5000 });
        assert_eq!(run(&dir, chunked.clone(), 6).await, 120_000);
        let first = listing(&dir);
        assert!(first.len() > 2, "{first:?}");
        std::fs::write(dir.join("unrelated.parquet"), b"keep").unwrap();
        run(&dir, chunked, 1).await;
        let second = listing(&dir);
        assert!(second.len() < first.len(), "{second:?}");
        assert!(second.iter().all(|n| !n.ends_with(".tmp")), "{second:?}");
        // Single-file mode replaces the chunk set.
        run(&dir, json!({}), 1).await;
        assert_eq!(listing(&dir), ["ev.snappy.parquet", "unrelated.parquet"]);
        let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    }

    /// Every chunk announces its batches; together they cover 1..=n in order.
    #[tokio::test]
    async fn chunks_announce_contiguous_batch_ranges() {
        let dir = scratch("ranges");
        let cfg = json!({ "path": dir, "prefix": "ev", "max_file_size": "64KiB", "compression": "uncompressed",
                          "row_group_size": 5000 });
        let mut p = ParquetPlugin::new();
        p.configure(loadsmith_plugin_sdk::PluginConfig::new(cfg, loadsmith_plugin_sdk::UnknownKeys::Error)).await.unwrap();
        p.prepare().await.unwrap();
        let mut ready = Vec::new();
        for b in 0..9 {
            let ids: ArrayRef = Arc::new(Int64Array::from_iter_values((0..20_000).map(|i| i * 7919 + b as i64)));
            p.write_batch(RecordBatch::try_from_iter([("id", ids)]).unwrap()).await.unwrap();
            ready.extend(p.take_ready());
        }
        p.finalize().await.unwrap();
        ready.extend(p.take_ready());
        assert!(ready.len() > 2, "{ready:?}");
        let mut next = 1;
        for o in &ready {
            let r = o.batches.expect("every chunk has a range");
            assert_eq!(r.first, next, "{ready:?}");
            assert!(r.last >= r.first);
            next = r.last + 1;
        }
        assert_eq!(next, 10, "the ranges cover all 9 batches");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn config_is_strict() {
        let err = |v: serde_json::Value| async move {
            let mut p = ParquetPlugin::new();
            let c = loadsmith_plugin_sdk::PluginConfig::new(v, loadsmith_plugin_sdk::UnknownKeys::Error);
            format!("{:#}", p.configure(c).await.unwrap_err())
        };
        assert!(err(json!({ "path": "/tmp", "prefix": "e", "max_file_sze": "1MiB" })).await.contains("max_file_sze"));
        assert!(err(json!({ "path": "/tmp", "prefix": "e", "max_file_size": "512" })).await.contains("no unit"));
        assert!(err(json!({ "path": "/tmp", "prefix": "a/b" })).await.contains("plain file-name prefix"));
    }
}
