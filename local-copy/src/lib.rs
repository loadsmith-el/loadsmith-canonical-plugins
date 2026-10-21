//! The local-copy sink: delivers each finalized object into a local directory.
//!
//! A library as well as the `loadsmith-sink-local-copy` binary, so the lab's
//! fault-injection build (`local-copy-chaos`, never released) wraps this exact
//! delivery code instead of shipping a crash switch in the release binary.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use async_trait::async_trait;
use loadsmith_plugin_sdk::{telemetry, SinkPlugin};
use tracing::Span;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct LocalCopyConfig {
    /// Destination directory, created when missing. Each delivered file is
    /// copied here under its original file name.
    dest: PathBuf,
}

pub struct LocalCopyPlugin {
    dest: Option<PathBuf>,
    delivered: u64,
}

impl LocalCopyPlugin {
    pub fn new() -> Self {
        Self { dest: None, delivered: 0 }
    }

    /// Objects delivered so far.
    pub fn delivered(&self) -> u64 {
        self.delivered
    }
}

impl Default for LocalCopyPlugin {
    fn default() -> Self {
        Self::new()
    }
}

/// The temp name `target` is copied under before the rename: hidden, in the
/// same directory (so the rename is atomic), unique to this process.
fn temp_path(target: &Path, file_name: &std::ffi::OsStr) -> PathBuf {
    let mut name = std::ffi::OsString::from(".");
    name.push(file_name);
    name.push(format!(".{}.tmp", std::process::id()));
    target.with_file_name(name)
}

/// Copies `src` to `target` atomically: a reader of `target` sees either the
/// previous file or the complete new one, never a partial copy.
fn copy_atomic(src: &Path, target: &Path, file_name: &std::ffi::OsStr) -> Result<()> {
    let temp = temp_path(target, file_name);
    // Telemetry: a `copy <file>` span (`file.path`, `file.size`) over the copy
    // and its sync, then a `rename` span for the atomic move into place.
    let on = telemetry::enabled();
    let copy_span = if on {
        tracing::info_span!(
            "copy",
            otel.name = format!("copy {}", file_name.to_string_lossy()),
            file.path = %target.display(),
            file.size = tracing::field::Empty,
            error = tracing::field::Empty,
        )
    } else {
        Span::none()
    };
    let copied = copy_span
        .in_scope(|| {
            std::fs::copy(src, &temp)
                .with_context(|| format!("copy {} → {}", src.display(), temp.display()))
                .and_then(|bytes| {
                    copy_span.record("file.size", bytes);
                    // Durable before it becomes visible under its real name.
                    std::fs::File::open(&temp)
                        .and_then(|f| f.sync_all())
                        .with_context(|| format!("sync {}", temp.display()))
                })
        })
        .and_then(|_| {
            let span = if on {
                tracing::info_span!(
                    "rename",
                    file.path = %temp.display(),
                    loadsmith.file.target = %target.display(),
                    error = tracing::field::Empty,
                )
            } else {
                Span::none()
            };
            span.in_scope(|| {
                std::fs::rename(&temp, target)
                    .with_context(|| format!("rename {} → {}", temp.display(), target.display()))
            })
            .inspect_err(|e| {
                span.record("error", format!("{e:#}"));
            })
        });
    if let Err(e) = &copied {
        copy_span.record("error", format!("{e:#}"));
        let _ = std::fs::remove_file(&temp);
    }
    copied
}

#[async_trait]
impl SinkPlugin for LocalCopyPlugin {
    fn plugin_name(&self) -> &str {
        "loadsmith-sink-local-copy"
    }

    fn plugin_version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    async fn configure(&mut self, config: loadsmith_plugin_sdk::PluginConfig) -> Result<()> {
        let cfg: LocalCopyConfig = config.parse()?;
        if cfg.dest.as_os_str().is_empty() {
            anyhow::bail!("`dest` is empty");
        }
        // Create the destination directory up front so delivery is just a copy.
        std::fs::create_dir_all(&cfg.dest)
            .with_context(|| format!("cannot create dest directory: {}", cfg.dest.display()))?;
        self.dest = Some(cfg.dest);
        Ok(())
    }

    async fn prepare(&mut self) -> Result<()> {
        Ok(())
    }

    async fn deliver(&mut self, path: PathBuf) -> Result<()> {
        let dest = self.dest.as_ref().context("deliver() before configure()")?;
        let file_name = path
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("staged path has no file name: {}", path.display()))?;
        let target = dest.join(file_name);

        // Replaces any existing target, so re-delivery after a crash is
        // idempotent — the contract the supervisor relies on for resume.
        copy_atomic(&path, &target, file_name)?;

        self.delivered += 1;
        tracing::debug!(from = %path.display(), to = %target.display(), "delivered object");
        Ok(())
    }

    async fn finalize(&mut self) -> Result<u64> {
        Ok(self.delivered)
    }

    async fn cancel(&mut self) {
        // Nothing to undo — delivered copies are left in place, and a copy cut
        // short never reached its final name.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loadsmith_plugin_sdk::{PluginConfig, UnknownKeys};
    use serde_json::json;

    fn scratch(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("lc-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        d
    }

    async fn configured(dest: &Path) -> LocalCopyPlugin {
        let mut p = LocalCopyPlugin::new();
        p.configure(PluginConfig::new(json!({ "dest": dest }), UnknownKeys::Error)).await.unwrap();
        p
    }

    fn listing(dir: &Path) -> Vec<String> {
        let mut v: Vec<String> =
            std::fs::read_dir(dir).unwrap().map(|e| e.unwrap().file_name().to_string_lossy().into_owned()).collect();
        v.sort();
        v
    }

    #[tokio::test]
    async fn copies_into_dest_dir() {
        let tmp = scratch("copy");
        let src_dir = tmp.join("src");
        let dst_dir = tmp.join("dst");
        std::fs::create_dir_all(&src_dir).unwrap();
        let src_file = src_dir.join("part.0001.parquet");
        std::fs::write(&src_file, b"hello").unwrap();

        let mut p = configured(&dst_dir).await;
        p.deliver(src_file).await.unwrap();

        assert_eq!(std::fs::read(dst_dir.join("part.0001.parquet")).unwrap(), b"hello");
        assert_eq!(listing(&dst_dir), ["part.0001.parquet"], "no temp file left behind");
        assert_eq!(p.finalize().await.unwrap(), 1);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn redelivery_replaces_the_target() {
        let tmp = scratch("idem");
        let src_dir = tmp.join("src");
        let dst_dir = tmp.join("dst");
        std::fs::create_dir_all(&src_dir).unwrap();
        let src_file = src_dir.join("part.parquet");
        std::fs::write(&src_file, b"data").unwrap();

        let mut p = configured(&dst_dir).await;
        p.deliver(src_file.clone()).await.unwrap();
        std::fs::write(&src_file, b"newer").unwrap();
        p.deliver(src_file).await.unwrap(); // same path again — must not error

        assert_eq!(std::fs::read(dst_dir.join("part.parquet")).unwrap(), b"newer");
        assert_eq!(listing(&dst_dir), ["part.parquet"]);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn a_failed_copy_leaves_no_temp_file() {
        let tmp = scratch("fail");
        let mut p = configured(&tmp.join("dst")).await;
        assert!(p.deliver(tmp.join("missing.parquet")).await.is_err());
        assert!(listing(&tmp.join("dst")).is_empty());
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn config_is_strict() {
        let err = |v: serde_json::Value| async move {
            format!("{:#}", LocalCopyPlugin::new().configure(PluginConfig::new(v, UnknownKeys::Error)).await.unwrap_err())
        };
        assert!(err(json!({})).await.contains("dest"));
        assert!(err(json!({ "dest": "/tmp/x", "dset": 1 })).await.contains("`dset`"));
        let mut p = LocalCopyPlugin::new();
        assert!(p.deliver(PathBuf::from("/x")).await.unwrap_err().to_string().contains("before configure"));
    }
}
