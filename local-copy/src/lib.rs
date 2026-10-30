//! The local-copy sink: delivers each finalized object into a local directory.
//!
//! **Object names.** Each file lands at `dest` + the `key` template (the same
//! as the s3 sink's, see [`ObjectKey`]): `{run_date}/{run_id}/{file}` by
//! default, so every run adds its own files and never replaces an earlier
//! run's; `key: "{file}"` keeps them flat in `dest`. Within a run the name is
//! stable, so a re-delivery replaces the same file.
//!
//! A library as well as the `loadsmith-sink-local-copy` binary, so the lab's
//! fault-injection build (`local-copy-chaos`, never released) wraps this exact
//! delivery code instead of shipping a crash switch in the release binary.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use async_trait::async_trait;
use loadsmith_plugin_sdk::{telemetry, ObjectKey, RunInfo, SinkPlugin};
use tracing::Span;
use serde::Deserialize;

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct LocalCopyConfig {
    /// Destination directory, created when missing.
    dest: PathBuf,
    /// Where under `dest` each file lands ([`ObjectKey`]): `{file}`
    /// (required), `{run_id}`, `{run_date}`, `{run_time}`, `{pipeline}`.
    #[serde(default = "default_key")]
    key: String,
}

fn default_key() -> String {
    ObjectKey::DEFAULT.to_string()
}

/// A configured sink: the directory, the names, the run they name.
struct Target {
    dest: PathBuf,
    keys: ObjectKey,
    run: Option<RunInfo>,
}

pub struct LocalCopyPlugin {
    target: Option<Target>,
    delivered: u64,
}

impl LocalCopyPlugin {
    pub fn new() -> Self {
        Self { target: None, delivered: 0 }
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
    fn config_schema() -> Option<serde_json::Value> {
        Some(loadsmith_plugin_sdk::config_schema::schema_for::<LocalCopyConfig>())
    }
    fn plugin_name(&self) -> &str {
        "loadsmith-sink-local-copy"
    }

    fn plugin_version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    async fn configure(&mut self, config: loadsmith_plugin_sdk::PluginConfig) -> Result<()> {
        let run = config.run().cloned();
        let cfg: LocalCopyConfig = config.parse()?;
        if cfg.dest.as_os_str().is_empty() {
            anyhow::bail!("`dest` is empty");
        }
        let keys = ObjectKey::parse(None, &cfg.key, run.as_ref())?;
        // Create the destination directory up front so delivery is just a copy.
        std::fs::create_dir_all(&cfg.dest)
            .with_context(|| format!("cannot create dest directory: {}", cfg.dest.display()))?;
        self.target = Some(Target { dest: cfg.dest, keys, run });
        Ok(())
    }

    async fn prepare(&mut self) -> Result<()> {
        Ok(())
    }

    async fn deliver(&mut self, path: PathBuf) -> Result<()> {
        let t = self.target.as_ref().context("deliver() before configure()")?;
        let file_name = path
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("staged path has no file name: {}", path.display()))?;
        let key = t.keys.render(&file_name.to_string_lossy(), t.run.as_ref())?;
        let relative = Path::new(&key);
        if relative.components().any(|c| !matches!(c, std::path::Component::Normal(_))) {
            anyhow::bail!("object name {key:?} must stay inside `dest` (no `..`, no absolute path)");
        }
        let target = t.dest.join(relative);
        if let Some(dir) = target.parent() {
            std::fs::create_dir_all(dir).with_context(|| format!("cannot create {}", dir.display()))?;
        }
        let file_name = target.file_name().unwrap_or(file_name).to_os_string();
        let file_name = file_name.as_os_str();

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

    fn run() -> RunInfo {
        // 2026-10-22T12:00:00Z
        RunInfo { id: "4242-1792670400000".into(), pipeline: "orders".into(), started_at_unix_ms: 1_792_670_400_000 }
    }

    /// Flat names (`key: {file}`), as the delivery tests below expect.
    async fn configured(dest: &Path) -> LocalCopyPlugin {
        with_config(json!({ "dest": dest, "key": "{file}" }), Some(run())).await.unwrap()
    }

    async fn with_config(cfg: serde_json::Value, run: Option<RunInfo>) -> Result<LocalCopyPlugin> {
        let mut p = LocalCopyPlugin::new();
        p.configure(PluginConfig::new(cfg, UnknownKeys::Error).with_run(run)).await?;
        Ok(p)
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

    /// Regression: with flat names, a second incremental run into the same
    /// `dest` replaced the first run's files (both name theirs `part.0001…`).
    #[tokio::test]
    async fn the_default_key_gives_each_run_its_own_prefix() {
        let tmp = scratch("runs");
        let src_dir = tmp.join("src");
        let dst_dir = tmp.join("dst");
        std::fs::create_dir_all(&src_dir).unwrap();
        let src_file = src_dir.join("part.0001.parquet");
        std::fs::write(&src_file, b"run 1").unwrap();
        let mut p = with_config(json!({ "dest": dst_dir }), Some(run())).await.unwrap();
        p.deliver(src_file.clone()).await.unwrap();

        std::fs::write(&src_file, b"run 2").unwrap();
        let second = RunInfo { id: "4343-1792674000000".into(), started_at_unix_ms: 1_792_674_000_000, ..run() };
        let mut p = with_config(json!({ "dest": dst_dir }), Some(second)).await.unwrap();
        p.deliver(src_file).await.unwrap();

        let day = dst_dir.join("2026-10-22");
        assert_eq!(listing(&day), ["4242-1792670400000", "4343-1792674000000"]);
        assert_eq!(std::fs::read(day.join("4242-1792670400000/part.0001.parquet")).unwrap(), b"run 1");
        assert_eq!(std::fs::read(day.join("4343-1792674000000/part.0001.parquet")).unwrap(), b"run 2");
        assert_eq!(listing(&day.join("4242-1792670400000")), ["part.0001.parquet"], "no temp file left behind");
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn keys_are_checked_at_configure() {
        let dest = scratch("keys");
        let err = |cfg, run| async move { format!("{:#}", with_config(cfg, run).await.err().unwrap()) };
        assert!(err(json!({ "dest": dest, "key": "{run_id}" }), Some(run())).await.contains("{file}"));
        assert!(err(json!({ "dest": dest }), None).await.contains("upgrade the engine"));
        assert!(with_config(json!({ "dest": dest, "key": "{file}" }), None).await.is_ok(), "a flat key needs no run");
        let mut p = with_config(json!({ "dest": dest, "key": "../{file}" }), None).await.unwrap();
        let src = dest.join("f");
        std::fs::write(&src, b"x").unwrap();
        assert!(p.deliver(src).await.unwrap_err().to_string().contains("inside `dest`"));
        let _ = std::fs::remove_dir_all(&dest);
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
        assert!(err(json!({ "dest": "/tmp/x", "key": "{nope}" })).await.contains("unknown placeholder"));
    }
}
