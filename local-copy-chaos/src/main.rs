//! `loadsmith-sink-local-copy-chaos`: the local-copy sink with fault
//! injection, for the lab only (never released).
//!
//! It delivers exactly like `local-copy` (it wraps the same code) and takes the
//! same config. When `LOADSMITH_SINK_CRASH_AFTER=<n>` is set, the process exits
//! abruptly (code 137) after its `n`-th delivery — **once**: a marker file in
//! the temp dir records the crash, so the instance the core's sink supervisor
//! respawns resumes from the delivery ledger and completes instead of
//! crash-looping to the restart cap.
//!
//! When `LOADSMITH_SINK_FAIL_AFTER=<n>` is set, every delivery after the
//! `n`-th successful one **fails**, in this instance and in every respawned
//! one (a counter file in the temp dir carries the count): a target that stays
//! down, so the run fails with objects undelivered.

use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;
use loadsmith_local_copy::LocalCopyPlugin;
use loadsmith_plugin_sdk::{PluginConfig, SinkPlugin};

const CRASH_AFTER_ENV: &str = "LOADSMITH_SINK_CRASH_AFTER";
const FAIL_AFTER_ENV: &str = "LOADSMITH_SINK_FAIL_AFTER";

fn crash_marker() -> PathBuf {
    std::env::temp_dir().join("loadsmith-sink-crash.marker")
}

/// Successful deliveries so far, across respawned instances.
fn delivered_counter() -> PathBuf {
    std::env::temp_dir().join("loadsmith-sink-delivered.count")
}

struct ChaosSink {
    inner: LocalCopyPlugin,
    crash_after: Option<u64>,
    fail_after: Option<u64>,
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
        if let Some(n) = self.fail_after {
            let counter = delivered_counter();
            let done: u64 = std::fs::read_to_string(&counter).ok().and_then(|s| s.trim().parse().ok()).unwrap_or(0);
            if done >= n {
                anyhow::bail!("[local-copy-chaos] simulated target outage: {} deliveries done, failing the rest", done);
            }
            self.inner.deliver(path).await?;
            std::fs::write(&counter, (done + 1).to_string())?;
            return Ok(());
        }
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
    let fail_after = std::env::var(FAIL_AFTER_ENV).ok().and_then(|v| v.parse().ok());
    loadsmith_plugin_sdk::run_sink(ChaosSink { inner: LocalCopyPlugin::new(), crash_after, fail_after }).await
}
