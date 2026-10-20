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

    async fn configure(&mut self, config: loadsmith_plugin_sdk::PluginConfig) -> Result<()> {
        // Plugin config as raw JSON (unknown-key checks: see `PluginConfig::parse`).
        let config = config.into_value();
        let cfg: JsonlConfig =
            serde_json::from_value(config).map_err(|e| anyhow::anyhow!("invalid jsonl destination config: {e}"))?;
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
        // The run will not complete: never leave a partial file that looks
        // like a finished export. Close it (unflushed) and remove it.
        // Stdout output cannot be taken back.
        if let Some(Output::File(_)) = self.output.take() {
            if let Some(path) = self.config.as_ref().and_then(|c| c.path.as_ref()) {
                if let Err(e) = std::fs::remove_file(path) {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        tracing::warn!("cannot remove partial output {}: {e}", path.display());
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Int64Array};
    use std::sync::Arc;

    fn batch() -> RecordBatch {
        let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        RecordBatch::try_from_iter([("id", ids)]).unwrap()
    }

    fn out_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("loadsmith-jsonl-{name}-{}.jsonl", std::process::id()))
    }

    async fn started(path: &std::path::Path) -> JsonlPlugin {
        let mut p = JsonlPlugin::new();
        p.configure(loadsmith_plugin_sdk::PluginConfig::new(serde_json::json!({ "path": path }), loadsmith_plugin_sdk::UnknownKeys::Error)).await.unwrap();
        p.prepare().await.unwrap();
        p.write_batch(batch()).await.unwrap();
        p
    }

    #[tokio::test]
    async fn cancel_removes_the_partial_file() {
        let path = out_path("cancel");
        let mut p = started(&path).await;
        assert!(path.exists());
        p.cancel().await;
        assert!(!path.exists(), "partial output must be deleted on cancel");
    }

    #[tokio::test]
    async fn finalize_keeps_the_file() {
        let path = out_path("finalize");
        let mut p = started(&path).await;
        assert_eq!(p.finalize().await.unwrap(), 3);
        let text = std::fs::read_to_string(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        assert_eq!(text.lines().count(), 3);
    }
}
