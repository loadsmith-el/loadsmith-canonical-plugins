use anyhow::{bail, Context, Result};
use arrow_array::RecordBatch;
use arrow_schema::TimeUnit;
use async_trait::async_trait;
use loadsmith_plugin_sdk::DestinationPlugin;
use serde::Deserialize;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use crate::json::{self, Temporal};
use crate::vocab::config_enum;

config_enum! {
    /// `temporal_format`: how dates, times, timestamps and durations are
    /// written.
    #[derive(Default)]
    pub enum TemporalFormat for "temporal_format" {
        /// ISO-8601 text with the column's precision.
        #[default]
        Iso8601 = "iso8601",
        /// Epoch integers (see `epoch_unit`).
        Epoch = "epoch",
    }
    retired {
        "iso" => "iso8601", "iso-8601" => "iso8601", "rfc3339" => "iso8601", "text" => "iso8601",
        "unix" => "epoch", "integer" => "epoch", "epoch-ms" => "epoch",
    }
}

config_enum! {
    /// `epoch_unit`: the unit of epoch integers (`temporal_format: epoch`).
    #[derive(Default)]
    pub enum EpochUnit for "epoch_unit" {
        /// Each column's own unit: no precision lost.
        Column = "column",
        S = "s",
        #[default]
        Ms = "ms",
        Us = "us",
        Ns = "ns",
    }
    retired {
        "native" => "column", "source" => "column",
        "second" => "s", "seconds" => "s",
        "millisecond" => "ms", "milliseconds" => "ms", "millis" => "ms",
        "microsecond" => "us", "microseconds" => "us", "micros" => "us",
        "nanosecond" => "ns", "nanoseconds" => "ns", "nanos" => "ns",
    }
}

#[derive(Debug, Deserialize)]
struct JsonlConfig {
    /// Output file. Required: stdout is the plugin's protocol channel.
    path: PathBuf,
    #[serde(default)]
    temporal_format: TemporalFormat,
    /// Only with `temporal_format: epoch` (`ms` when absent).
    #[serde(default)]
    epoch_unit: Option<EpochUnit>,
}

impl JsonlConfig {
    fn temporal(&self) -> Result<Temporal> {
        Ok(match (self.temporal_format, self.epoch_unit) {
            (TemporalFormat::Iso8601, None) => Temporal::Iso,
            (TemporalFormat::Iso8601, Some(_)) => {
                bail!("`epoch_unit` only applies with `temporal_format: epoch`")
            }
            (TemporalFormat::Epoch, unit) => Temporal::Epoch(match unit.unwrap_or_default() {
                EpochUnit::Column => None,
                EpochUnit::S => Some(TimeUnit::Second),
                EpochUnit::Ms => Some(TimeUnit::Millisecond),
                EpochUnit::Us => Some(TimeUnit::Microsecond),
                EpochUnit::Ns => Some(TimeUnit::Nanosecond),
            }),
        })
    }
}

/// What `configure` validated.
struct Settings {
    path: PathBuf,
    temporal: Temporal,
}

pub struct JsonlPlugin {
    settings: Option<Settings>,
    output: Option<BufWriter<std::fs::File>>,
    rows_written: u64,
}

impl JsonlPlugin {
    pub fn new() -> Self {
        Self { settings: None, output: None, rows_written: 0 }
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
        let cfg: JsonlConfig = config.parse()?;
        if cfg.path.as_os_str().is_empty() {
            bail!("`path` is empty");
        }
        let temporal = cfg.temporal()?;
        self.settings = Some(Settings { path: cfg.path, temporal });
        Ok(())
    }

    async fn prepare(&mut self) -> Result<()> {
        let path = &self.settings.as_ref().context("prepare() before configure()")?.path;
        let file = std::fs::File::create(path)
            .with_context(|| format!("cannot create output file: {}", path.display()))?;
        self.output = Some(BufWriter::new(file));
        Ok(())
    }

    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let temporal = self.settings.as_ref().context("write_batch() before configure()")?.temporal;
        let out = self.output.as_mut().context("write_batch() before prepare()")?;
        for row in json::rows(&batch, temporal) {
            serde_json::to_writer(&mut *out, &row).context("json serialize error")?;
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
        if self.output.take().is_some() {
            if let Some(path) = self.settings.as_ref().map(|s| &s.path) {
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
    use arrow_array::{ArrayRef, Int64Array, TimestampMicrosecondArray};
    use serde_json::json;
    use std::sync::Arc;

    fn config(v: serde_json::Value) -> loadsmith_plugin_sdk::PluginConfig {
        loadsmith_plugin_sdk::PluginConfig::new(v, loadsmith_plugin_sdk::UnknownKeys::Error)
    }

    async fn configure_err(v: serde_json::Value) -> String {
        format!("{:#}", JsonlPlugin::new().configure(config(v)).await.unwrap_err())
    }

    fn batch() -> RecordBatch {
        let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        RecordBatch::try_from_iter([("id", ids)]).unwrap()
    }

    fn out_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("loadsmith-jsonl-{name}-{}.jsonl", std::process::id()))
    }

    async fn started(path: &std::path::Path, extra: serde_json::Value) -> JsonlPlugin {
        let mut cfg = json!({ "path": path });
        if let (Some(o), Some(e)) = (cfg.as_object_mut(), extra.as_object()) {
            o.extend(e.clone());
        }
        let mut p = JsonlPlugin::new();
        p.configure(config(cfg)).await.unwrap();
        p.prepare().await.unwrap();
        p
    }

    #[tokio::test]
    async fn cancel_removes_the_partial_file() {
        let path = out_path("cancel");
        let mut p = started(&path, json!({})).await;
        p.write_batch(batch()).await.unwrap();
        assert!(path.exists());
        p.cancel().await;
        assert!(!path.exists(), "partial output must be deleted on cancel");
    }

    #[tokio::test]
    async fn finalize_keeps_the_file() {
        let path = out_path("finalize");
        let mut p = started(&path, json!({})).await;
        p.write_batch(batch()).await.unwrap();
        assert_eq!(p.finalize().await.unwrap(), 3);
        let text = std::fs::read_to_string(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        assert_eq!(text.lines().count(), 3);
    }

    #[tokio::test]
    async fn config_is_strict() {
        assert!(configure_err(json!({})).await.contains("path"));
        assert!(configure_err(json!({ "path": "/x", "pth": 1 })).await.contains("pth"));
        let e = configure_err(json!({ "path": "/x", "temporal_format": "iso" })).await;
        assert!(e.contains("did you mean `iso8601`?"), "{e}");
        let e = configure_err(json!({ "path": "/x", "temporal_format": "epoch", "epoch_unit": "micros" })).await;
        assert!(e.contains("did you mean `us`?"), "{e}");
        let e = configure_err(json!({ "path": "/x", "epoch_unit": "us" })).await;
        assert!(e.contains("only applies with `temporal_format: epoch`"), "{e}");
    }

    #[test]
    fn temporal_settings() {
        let t = |v: serde_json::Value| config(v).parse::<JsonlConfig>().unwrap().temporal().unwrap();
        assert_eq!(t(json!({ "path": "/x" })), Temporal::Iso);
        assert_eq!(t(json!({ "path": "/x", "temporal_format": "epoch" })), Temporal::Epoch(Some(TimeUnit::Millisecond)));
        assert_eq!(t(json!({ "path": "/x", "temporal_format": "epoch", "epoch_unit": "column" })), Temporal::Epoch(None));
    }

    async fn written(name: &str, extra: serde_json::Value) -> String {
        let path = out_path(name);
        let mut p = started(&path, extra).await;
        let ts: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![1_500_001]).with_timezone("UTC"));
        p.write_batch(RecordBatch::try_from_iter([("ts", ts)]).unwrap()).await.unwrap();
        p.finalize().await.unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        text
    }

    #[tokio::test]
    async fn writes_iso_by_default_and_epoch_on_request() {
        assert_eq!(written("iso", json!({})).await, "{\"ts\":\"1970-01-01T00:00:01.500001Z\"}\n");
        assert_eq!(written("epoch", json!({ "temporal_format": "epoch" })).await, "{\"ts\":1500}\n");
        let native = json!({ "temporal_format": "epoch", "epoch_unit": "column" });
        assert_eq!(written("native", native).await, "{\"ts\":1500001}\n");
    }
}
