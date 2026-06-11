use std::path::PathBuf;

use anyhow::{Context, Result};
use async_trait::async_trait;
use loadsmith_plugin_sdk::SinkPlugin;
use serde::Deserialize;

/// Environment variable used in tests to simulate a sink crash: after
/// delivering this many objects, the process aborts **once**. The core's sink
/// supervisor must then respawn and resume from the delivery ledger; the
/// respawned instance sees the marker file and runs to completion. Unset in
/// normal operation.
const CRASH_AFTER_ENV: &str = "LOADSMITH_SINK_CRASH_AFTER";

/// Marker recording that the one-shot crash already happened, so only the first
/// instance aborts and the resume actually completes (rather than crash-looping
/// to the supervisor's restart cap).
fn crash_marker() -> PathBuf {
    std::env::temp_dir().join("loadsmith-sink-crash.marker")
}

#[derive(Debug, Deserialize)]
struct LocalCopyConfig {
    /// Destination directory. Each delivered file is copied here under its
    /// original file name.
    dest: PathBuf,
}

pub struct LocalCopyPlugin {
    config: Option<LocalCopyConfig>,
    /// Abort after this many deliveries when set (test fault injection).
    crash_after: Option<u64>,
    delivered: u64,
}

impl LocalCopyPlugin {
    pub fn new() -> Self {
        let crash_after = std::env::var(CRASH_AFTER_ENV).ok().and_then(|v| v.parse().ok());
        Self { config: None, crash_after, delivered: 0 }
    }
}

#[async_trait]
impl SinkPlugin for LocalCopyPlugin {
    fn plugin_name(&self) -> &str {
        "loadsmith-sink-local-copy"
    }

    fn plugin_version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    async fn configure(&mut self, config: serde_json::Value) -> Result<()> {
        let cfg: LocalCopyConfig =
            serde_json::from_value(config).context("invalid local-copy sink config")?;
        // Create the destination directory up front so delivery is just a copy.
        std::fs::create_dir_all(&cfg.dest)
            .with_context(|| format!("cannot create dest directory: {}", cfg.dest.display()))?;
        self.config = Some(cfg);
        Ok(())
    }

    async fn prepare(&mut self) -> Result<()> {
        Ok(())
    }

    async fn deliver(&mut self, path: PathBuf) -> Result<()> {
        let cfg = self.config.as_ref().expect("configured before deliver");

        let file_name = path
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("staged path has no file name: {}", path.display()))?;
        let target = cfg.dest.join(file_name);

        // Copy overwrites any existing target, so re-delivery after a crash is
        // idempotent — the contract the supervisor relies on for resume.
        std::fs::copy(&path, &target).with_context(|| {
            format!("copy {} → {}", path.display(), target.display())
        })?;

        self.delivered += 1;
        tracing::debug!(from = %path.display(), to = %target.display(), "delivered object");

        // Test-only: simulate a single mid-run crash so recovery is exercised.
        // The marker ensures only the first instance aborts; the respawned one
        // resumes and completes.
        if let Some(n) = self.crash_after {
            let marker = crash_marker();
            if self.delivered >= n && !marker.exists() {
                let _ = std::fs::write(&marker, b"1");
                eprintln!("[local-copy] simulating crash after {} deliveries", self.delivered);
                std::process::exit(137);
            }
        }
        Ok(())
    }

    async fn finalize(&mut self) -> Result<u64> {
        Ok(self.delivered)
    }

    async fn cancel(&mut self) {
        // Nothing to undo — delivered copies are left in place; partial copies
        // are overwritten on the next idempotent re-delivery.
    }
}

impl Default for LocalCopyPlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn copies_into_dest_dir() {
        let tmp = std::env::temp_dir().join(format!("lc-test-{}", std::process::id()));
        let src_dir = tmp.join("src");
        let dst_dir = tmp.join("dst");
        std::fs::create_dir_all(&src_dir).unwrap();
        let src_file = src_dir.join("part.0001.parquet");
        std::fs::write(&src_file, b"hello").unwrap();

        let mut p = LocalCopyPlugin::new();
        p.configure(serde_json::json!({ "dest": dst_dir.to_string_lossy() })).await.unwrap();
        p.deliver(src_file).await.unwrap();

        let copied = dst_dir.join("part.0001.parquet");
        assert_eq!(std::fs::read(&copied).unwrap(), b"hello");
        assert_eq!(p.finalize().await.unwrap(), 1);

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn redelivery_is_idempotent() {
        let tmp = std::env::temp_dir().join(format!("lc-idem-{}", std::process::id()));
        let src_dir = tmp.join("src");
        let dst_dir = tmp.join("dst");
        std::fs::create_dir_all(&src_dir).unwrap();
        let src_file = src_dir.join("part.parquet");
        std::fs::write(&src_file, b"data").unwrap();

        let mut p = LocalCopyPlugin::new();
        p.configure(serde_json::json!({ "dest": dst_dir.to_string_lossy() })).await.unwrap();
        p.deliver(src_file.clone()).await.unwrap();
        p.deliver(src_file).await.unwrap(); // same path again — must not error

        assert_eq!(std::fs::read(dst_dir.join("part.parquet")).unwrap(), b"data");
        let _ = std::fs::remove_dir_all(&tmp);
    }
}
