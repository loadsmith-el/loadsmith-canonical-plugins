//! `loadsmith-parser-csv`: turns the CSV files a raw source announces into
//! one Arrow stream.
//!
//! Each file is **streamed** ([`reader::CsvReader`]): the parser holds the
//! inference sample (`infer_rows` records) and one batch, never the whole
//! file. Without declared `columns:`, every file's schema is inferred from
//! its own sample (nullable columns); the SDK fails the run with
//! `SCHEMA_MISMATCH` when a later file's schema differs from the first's.

pub mod options;
pub mod reader;

use std::fs::File;
use std::path::Path;

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use loadsmith_plugin_sdk::ParserPlugin;

pub use options::CsvOptions;
pub use reader::CsvReader;

/// The span of opening one file (inside the SDK's `loadsmith.parse` span,
/// which carries its path and rows): `read <file>` with `file.path`,
/// `file.size`, the column count and whether the schema was inferred (the
/// inference sample is read here).
fn read_span(path: &Path, declared: bool) -> tracing::Span {
    if !loadsmith_plugin_sdk::telemetry::enabled() {
        return tracing::Span::none();
    }
    let name = path.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
    tracing::info_span!(
        "read",
        otel.name = format!("read {name}"),
        file.path = %path.display(),
        file.size = tracing::field::Empty,
        loadsmith.schema.columns = tracing::field::Empty,
        loadsmith.schema.inferred = !declared,
        error = tracing::field::Empty,
    )
}

/// The parser plugin.
#[derive(Default)]
pub struct CsvParser {
    opts: CsvOptions,
    current: Option<CsvReader<File>>,
}

#[async_trait]
impl ParserPlugin for CsvParser {
    fn config_schema() -> Option<serde_json::Value> {
        Some(loadsmith_plugin_sdk::config_schema::schema_for::<crate::options::RawOptions>())
    }
    fn plugin_name(&self) -> &str {
        "loadsmith-parser-csv"
    }
    fn plugin_version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    async fn configure(&mut self, config: loadsmith_plugin_sdk::PluginConfig) -> Result<()> {
        self.opts = CsvOptions::from_config(&config)?;
        Ok(())
    }

    fn declared_schema(&self) -> Option<Schema> {
        self.opts.declared()
    }

    async fn open(&mut self, path: &Path) -> Result<Schema> {
        self.current = None;
        let span = read_span(path, self.opts.declared().is_some());
        let reader = span.in_scope(|| -> Result<CsvReader<File>> {
            let file = File::open(path).context("cannot open the file")?;
            if !span.is_none() {
                if let Ok(meta) = file.metadata() {
                    span.record("file.size", meta.len());
                }
            }
            CsvReader::open(file, &self.opts)
        });
        let reader = reader.inspect_err(|e| {
            span.record("error", format!("{e:#}"));
        })?;
        let schema = reader.schema().as_ref().clone();
        span.record("loadsmith.schema.columns", schema.fields().len() as u64);
        self.current = Some(reader);
        Ok(schema)
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let Some(reader) = self.current.as_mut() else {
            return Ok(None);
        };
        let batch = reader.next_batch()?;
        if batch.is_none() {
            self.current = None;
        }
        Ok(batch)
    }

    async fn cancel(&mut self) {
        self.current = None;
    }
}
