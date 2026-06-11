use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use loadsmith_plugin_sdk::DestinationPlugin;
use serde::Deserialize;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
struct JsonlConfig {
    path: Option<PathBuf>,
}

enum Output {
    File(BufWriter<std::fs::File>),
    Stdout,
}

impl Output {
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            Output::File(w) => w.write_all(buf),
            Output::Stdout => std::io::stdout().write_all(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Output::File(w) => w.flush(),
            Output::Stdout => std::io::stdout().flush(),
        }
    }
}

pub struct JsonlPlugin {
    config: Option<JsonlConfig>,
    output: Option<Output>,
    rows_written: u64,
}

impl JsonlPlugin {
    pub fn new() -> Self {
        Self { config: None, output: None, rows_written: 0 }
    }
}

#[async_trait]
impl DestinationPlugin for JsonlPlugin {
    fn plugin_name(&self) -> &str {
        "loadsmith-destination-jsonl"
    }
    fn plugin_version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    async fn configure(&mut self, config: serde_json::Value) -> Result<()> {
        let cfg: JsonlConfig =
            serde_json::from_value(config).context("invalid jsonl destination config")?;
        self.config = Some(cfg);
        Ok(())
    }

    async fn prepare(&mut self) -> Result<()> {
        let path = self.config.as_ref().unwrap().path.as_ref();
        let output = match path {
            Some(p) => {
                let file = std::fs::File::create(p)
                    .with_context(|| format!("cannot create output file: {}", p.display()))?;
                Output::File(BufWriter::new(file))
            }
            None => Output::Stdout,
        };
        self.output = Some(output);
        Ok(())
    }

    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let rows = loadsmith_arrow::record_batch_to_json_rows(&batch);
        let out = self.output.as_mut().unwrap();
        for row in rows {
            let line = serde_json::to_string(&row).context("json serialize error")?;
            out.write_all(line.as_bytes()).context("write error")?;
            out.write_all(b"\n").context("write error")?;
            self.rows_written += 1;
        }
        Ok(())
    }

    async fn finalize(&mut self) -> Result<u64> {
        if let Some(out) = self.output.as_mut() {
            out.flush().context("flush error")?;
        }
        Ok(self.rows_written)
    }

    async fn cancel(&mut self) {
        let _ = self.output.as_mut().map(|o| o.flush());
    }
}
