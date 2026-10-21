//! `loadsmith-sink-local-copy-chaos`: the local-copy sink with fault
//! injection, for the lab only (never released).
//!
//! It delivers exactly like `local-copy` (it wraps the same code) and takes the
//! same config. When `LOADSMITH_SINK_CRASH_AFTER=<n>` is set, the process exits
//! abruptly (code 137) after its `n`-th delivery — **once**: a marker file in
//! the temp dir records the crash, so the instance the core's sink supervisor
//! respawns resumes from the delivery ledger and completes instead of
//! crash-looping to the restart cap.

use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;
use loadsmith_local_copy::LocalCopyPlugin;
use loadsmith_plugin_sdk::{PluginConfig, SinkPlugin};

const CRASH_AFTER_ENV: &str = "LOADSMITH_SINK_CRASH_AFTER";

fn crash_marker() -> PathBuf {
    std::env::temp_dir().join("loadsmith-sink-crash.marker")
}

struct ChaosSink {
    inner: LocalCopyPlugin,
    crash_after: Option<u64>,
}

#[async_trait]
impl SinkPlugin for ChaosSink {
    fn plugin_name(&self) -> &str {
        "loadsmith-sink-local-copy-chaos"
    }
    fn plugin_version(&self) -> &str {
        self.inner.plugin_version()
    }
    async fn configure(&mut self, config: PluginConfig) -> Result<()> {
        self.inner.configure(config).await
    }
    async fn prepare(&mut self) -> Result<()> {
        self.inner.prepare().await
    }
    async fn deliver(&mut self, path: PathBuf) -> Result<()> {
        self.inner.deliver(path).await?;
        if let Some(n) = self.crash_after {
            let marker = crash_marker();
            if self.inner.delivered() >= n && !marker.exists() {
                let _ = std::fs::write(&marker, b"1");
                eprintln!("[local-copy-chaos] simulating a crash after {} deliveries", self.inner.delivered());
                std::process::exit(137);
            }
        }
        Ok(())
    }
    async fn finalize(&mut self) -> Result<u64> {
        self.inner.finalize().await
    }
    async fn cancel(&mut self) {
        self.inner.cancel().await
    }
}

#[tokio::main]
async fn main() {
    let crash_after = std::env::var(CRASH_AFTER_ENV).ok().and_then(|v| v.parse().ok());
    loadsmith_plugin_sdk::run_sink(ChaosSink { inner: LocalCopyPlugin::new(), crash_after }).await
}
