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

/// The parser plugin.
#[derive(Default)]
pub struct CsvParser {
    opts: CsvOptions,
    current: Option<CsvReader<File>>,
}

#[async_trait]
impl ParserPlugin for CsvParser {
    fn plugin_name(&self) -> &str {
        "loadsmith-parser-csv"
    }
    fn plugin_version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    async fn configure(&mut self, config: serde_json::Value) -> Result<()> {
        self.opts = CsvOptions::from_config(config)?;
        Ok(())
    }

    fn declared_schema(&self) -> Option<Schema> {
        self.opts.declared()
    }

    async fn open(&mut self, path: &Path) -> Result<Schema> {
        self.current = None;
        let file = File::open(path).context("cannot open the file")?;
        let reader = CsvReader::open(file, &self.opts)?;
        let schema = reader.schema().as_ref().clone();
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
