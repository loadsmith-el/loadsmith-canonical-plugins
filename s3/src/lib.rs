//! The s3 sink: delivers each finalized object to an S3 bucket — AWS S3 or
//! any S3-compatible store (MinIO, Ceph, R2, …) through `endpoint`.
//!
//! Requests are signed with AWS SigV4 (presigned URLs, `rusty-s3` on the
//! RustCrypto `hmac`/`sha2`) and sent over `reqwest` on rustls with the
//! installed `rustls-rustcrypto` provider: pure Rust, nothing to install on the
//! host (no AWS CLI, no C toolchain).
//!
//! **Object keys.** Each file lands at `prefix` + the `key` template, which
//! names the run (`{run_date}/{run_id}/{file}` by default) from the core's
//! `configure.run`, so every run adds its own objects and never overwrites an
//! earlier run's. Within a run the key is stable: a re-delivery after a sink
//! respawn replaces the same object (S3 `PUT` is atomic: readers see the old
//! object or the whole new one).
//!
//! **Uploads.** A file up to `part_size_mib` goes in one `PUT`; a larger one
//! as a multipart upload (one part in memory at a time), aborted when a part
//! or the completion fails. Every request is retried on network errors,
//! `429` and `5xx` with exponential backoff.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use loadsmith_plugin_sdk::{telemetry, ObjectKey, RunInfo, SinkPlugin};
use reqwest::{Client, StatusCode};
use rusty_s3::actions::S3Action;
use rusty_s3::{Bucket, Credentials, UrlStyle};
use serde::Deserialize;
use tokio::io::AsyncReadExt;
use tracing::Span;

/// S3's minimum part size (every part but the last).
const MIN_PART_SIZE_MIB: u64 = 5;
/// S3's maximum number of parts in one multipart upload.
const MAX_PARTS: u64 = 10_000;
/// How long a presigned request URL stays valid (each request signs anew).
const SIGNATURE_TTL: Duration = Duration::from_secs(3600);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum UrlStyleConfig {
    /// `https://<bucket>.<host>/<key>` — AWS's default.
    VirtualHost,
    /// `https://<host>/<bucket>/<key>` — what most S3-compatible stores need.
    Path,
}

#[derive(Debug, Deserialize)]
struct S3Config {
    bucket: String,
    /// Default `AWS_REGION`, then `AWS_DEFAULT_REGION`, then `us-east-1`.
    #[serde(default)]
    region: Option<String>,
    /// An S3-compatible endpoint (`http://minio:9000`). Default: AWS,
    /// `https://s3.<region>.amazonaws.com`.
    #[serde(default)]
    endpoint: Option<String>,
    /// Default `virtual-host` on AWS, `path` with an `endpoint`.
    #[serde(default)]
    url_style: Option<UrlStyleConfig>,
    /// Prepended to every key (`raw/orders` → `raw/orders/<key>`).
    #[serde(default)]
    prefix: Option<String>,
    /// Object key template ([`ObjectKey`]): `{file}` (required), `{run_id}`,
    /// `{run_date}` (`YYYY-MM-DD`, UTC), `{run_time}` (`YYYYMMDDTHHMMSSZ`),
    /// `{pipeline}`.
    #[serde(default = "default_key")]
    key: String,
    /// Default `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` /
    /// `AWS_SESSION_TOKEN` from the environment.
    #[serde(default)]
    access_key_id: Option<String>,
    #[serde(default)]
    secret_access_key: Option<String>,
    #[serde(default)]
    session_token: Option<String>,
    /// Files up to this size go in one `PUT`; larger ones in parts of this
    /// size (min 5, raised as needed to stay within 10,000 parts).
    #[serde(default = "default_part_size_mib")]
    part_size_mib: u64,
    /// Limit for one HTTP request.
    #[serde(default = "default_request_timeout_seconds")]
    request_timeout_seconds: u64,
    /// Tries per request (network errors, `429`, `5xx`), with 1s, 2s, 4s, …
    /// between them.
    #[serde(default = "default_max_attempts")]
    max_attempts: u32,
}

fn default_key() -> String {
    ObjectKey::DEFAULT.to_string()
}
fn default_part_size_mib() -> u64 {
    16
}
fn default_request_timeout_seconds() -> u64 {
    300
}
fn default_max_attempts() -> u32 {
    4
}

/// Everything a configured sink needs to upload.
struct Target {
    bucket: Bucket,
    credentials: Credentials,
    keys: ObjectKey,
    run: Option<RunInfo>,
    part_size: u64,
    max_attempts: u32,
    http: Client,
}

pub struct S3SinkPlugin {
    target: Option<Target>,
    delivered: u64,
}

impl S3SinkPlugin {
    pub fn new() -> Self {
        Self { target: None, delivered: 0 }
    }
}

impl Default for S3SinkPlugin {
    fn default() -> Self {
        Self::new()
    }
}

fn env_var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

fn make_credentials(key: String, secret: String, token: Option<String>) -> Credentials {
    match token {
        Some(token) => Credentials::new_with_token(key, secret, token),
        None => Credentials::new(key, secret),
    }
}

fn credentials(cfg: &S3Config) -> Result<Credentials> {
    match (&cfg.access_key_id, &cfg.secret_access_key) {
        (Some(key), Some(secret)) => {
            Ok(make_credentials(key.clone(), secret.clone(), cfg.session_token.clone()))
        }
        (Some(_), None) | (None, Some(_)) => {
            bail!("set both `access_key_id` and `secret_access_key`, or neither (to read them from AWS_* variables)")
        }
        (None, None) => match (env_var("AWS_ACCESS_KEY_ID"), env_var("AWS_SECRET_ACCESS_KEY")) {
            (Some(key), Some(secret)) => {
                Ok(make_credentials(key, secret, cfg.session_token.clone().or_else(|| env_var("AWS_SESSION_TOKEN"))))
            }
            _ => bail!(
                "no S3 credentials: set `access_key_id` and `secret_access_key` (e.g. from `secret()`), \
                 or AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in the environment"
            ),
        },
    }
}

impl Target {
    fn from_config(cfg: S3Config, run: Option<RunInfo>) -> Result<Self> {
        if cfg.bucket.trim().is_empty() {
            bail!("`bucket` is empty");
        }
        if cfg.part_size_mib < MIN_PART_SIZE_MIB {
            bail!("`part_size_mib` must be at least {MIN_PART_SIZE_MIB} (S3's minimum part size), got {}", cfg.part_size_mib);
        }
        if cfg.max_attempts == 0 {
            bail!("`max_attempts` must be at least 1");
        }
        let keys = ObjectKey::parse(cfg.prefix.as_deref(), &cfg.key, run.as_ref())?;
        let region = cfg
            .region
            .clone()
            .or_else(|| env_var("AWS_REGION"))
            .or_else(|| env_var("AWS_DEFAULT_REGION"))
            .unwrap_or_else(|| "us-east-1".to_string());
        let (endpoint, default_style) = match &cfg.endpoint {
            Some(e) => (e.clone(), UrlStyleConfig::Path),
            None => (format!("https://s3.{region}.amazonaws.com"), UrlStyleConfig::VirtualHost),
        };
        let style = match cfg.url_style.unwrap_or(default_style) {
            UrlStyleConfig::Path => UrlStyle::Path,
            UrlStyleConfig::VirtualHost => UrlStyle::VirtualHost,
        };
        let url = endpoint.parse().with_context(|| format!("`endpoint` {endpoint:?} is not a URL"))?;
        let bucket = Bucket::new(url, style, cfg.bucket.clone(), region)
            .map_err(|e| anyhow::anyhow!("bucket {:?} at {endpoint}: {e}", cfg.bucket))?;
        let credentials = credentials(&cfg)?;

        loadsmith_tls::install_provider();
        let http = Client::builder()
            .timeout(Duration::from_secs(cfg.request_timeout_seconds))
            .build()
            .context("failed to build the HTTP client")?;
        Ok(Self {
            bucket,
            credentials,
            keys,
            run,
            part_size: cfg.part_size_mib * 1024 * 1024,
            max_attempts: cfg.max_attempts,
            http,
        })
    }

    /// Sends one request, built anew (and signed anew) for every attempt.
    /// Retries network errors, `429` and `5xx`; any other status fails.
    async fn send(&self, what: &str, build: impl Fn() -> reqwest::RequestBuilder) -> Result<reqwest::Response> {
        let mut attempt = 1;
        loop {
            let outcome = build().send().await;
            let retryable = match &outcome {
                Ok(r) if r.status().is_success() => return Ok(outcome.expect("checked Ok")),
                Ok(r) => r.status() == StatusCode::TOO_MANY_REQUESTS || r.status().is_server_error(),
                Err(e) => !e.is_builder(),
            };
            if !retryable || attempt >= self.max_attempts {
                return Err(match outcome {
                    Ok(r) => {
                        let status = r.status();
                        let body = r.text().await.unwrap_or_default();
                        anyhow::anyhow!("{what}: HTTP {status}: {}", s3_error(&body))
                    }
                    Err(e) => anyhow::anyhow!("{what}: {e}"),
                });
            }
            let wait = Duration::from_secs(1 << (attempt - 1).min(5));
            tracing::warn!(attempt, max_attempts = self.max_attempts, "{what} failed, retrying in {wait:?}");
            tokio::time::sleep(wait).await;
            attempt += 1;
        }
    }

    async fn put_object(&self, key: &str, body: Vec<u8>) -> Result<()> {
        let action = self.bucket.put_object(Some(&self.credentials), key);
        self.send(&format!("PUT {key}"), || self.http.put(action.sign(SIGNATURE_TTL)).body(body.clone())).await?;
        Ok(())
    }

    /// The part size for a file of `size` bytes: `part_size`, raised so the
    /// upload stays within S3's 10,000 parts.
    fn part_size_for(&self, size: u64) -> u64 {
        self.part_size.max(size.div_ceil(MAX_PARTS))
    }

    async fn multipart(&self, path: &Path, key: &str, size: u64) -> Result<()> {
        let create = self.bucket.create_multipart_upload(Some(&self.credentials), key);
        let resp = self.send(&format!("create multipart upload {key}"), || self.http.post(create.sign(SIGNATURE_TTL))).await?;
        let body = resp.text().await.context("create multipart upload: reading the response")?;
        // Read the `UploadId` element alone: S3-compatible stores differ on the
        // root element (`InitiateMultipartUploadResult` on AWS) and namespace.
        let upload_id = xml_element(&body, "UploadId")
            .filter(|id| !id.is_empty())
            .with_context(|| format!("create multipart upload {key}: no UploadId in the response: {}", s3_error(&body)))?;

        let uploaded = self.upload_parts(path, key, &upload_id, size).await;
        let completed = match uploaded {
            Ok(etags) => self.complete(key, &upload_id, &etags).await,
            Err(e) => Err(e),
        };
        if completed.is_err() {
            // Best effort: an incomplete upload keeps its parts (and their
            // storage cost) until aborted or expired by a lifecycle rule.
            let abort = self.bucket.abort_multipart_upload(Some(&self.credentials), key, &upload_id);
            if let Err(e) = self.http.delete(abort.sign(SIGNATURE_TTL)).send().await {
                tracing::warn!("abort multipart upload {key}: {e}");
            }
        }
        completed
    }

    async fn upload_parts(&self, path: &Path, key: &str, upload_id: &str, size: u64) -> Result<Vec<String>> {
        let part_size = self.part_size_for(size);
        let mut file = tokio::fs::File::open(path).await.with_context(|| format!("open {}", path.display()))?;
        let mut etags = Vec::new();
        let mut remaining = size;
        let mut number: u16 = 1;
        while remaining > 0 {
            let len = remaining.min(part_size);
            let mut part = vec![0u8; len as usize];
            file.read_exact(&mut part).await.with_context(|| format!("read {}", path.display()))?;
            let action = self.bucket.upload_part(Some(&self.credentials), key, number, upload_id);
            let resp = self
                .send(&format!("upload part {number} of {key}"), || self.http.put(action.sign(SIGNATURE_TTL)).body(part.clone()))
                .await?;
            let etag = resp
                .headers()
                .get("etag")
                .and_then(|v| v.to_str().ok())
                .with_context(|| format!("upload part {number} of {key}: no ETag in the response"))?;
            etags.push(etag.to_string());
            remaining -= len;
            number += 1;
        }
        Ok(etags)
    }

    async fn complete(&self, key: &str, upload_id: &str, etags: &[String]) -> Result<()> {
        let action =
            self.bucket.complete_multipart_upload(Some(&self.credentials), key, upload_id, etags.iter().map(String::as_str));
        let url = action.sign(SIGNATURE_TTL);
        let body = action.body();
        let resp = self.send(&format!("complete multipart upload {key}"), || self.http.post(url.clone()).body(body.clone())).await?;
        // S3 can answer 200 and still report the failure in the body.
        let text = resp.text().await.unwrap_or_default();
        if text.contains("<Error>") {
            bail!("complete multipart upload {key}: {}", s3_error(&text));
        }
        Ok(())
    }

    /// Uploads `path` to `key`; returns the bytes sent.
    async fn upload(&self, path: &Path, key: &str) -> Result<u64> {
        let size = tokio::fs::metadata(path).await.with_context(|| format!("stat {}", path.display()))?.len();
        if size <= self.part_size {
            let body = tokio::fs::read(path).await.with_context(|| format!("read {}", path.display()))?;
            self.put_object(key, body).await?;
        } else {
            self.multipart(path, key, size).await?;
        }
        Ok(size)
    }
}

/// The text of the first `<name>…</name>` element of an S3 XML body.
fn xml_element(body: &str, name: &str) -> Option<String> {
    let open = format!("<{name}>");
    let start = body.find(&open)? + open.len();
    let end = body[start..].find(&format!("</{name}>"))? + start;
    Some(body[start..end].trim().to_string())
}

/// The `Code: Message` of an S3 XML error body, or the body itself (trimmed).
fn s3_error(body: &str) -> String {
    let tag = |name: &str| xml_element(body, name);
    match (tag("Code"), tag("Message")) {
        (Some(code), Some(msg)) => format!("{code}: {msg}"),
        (Some(code), None) => code,
        _ => body.trim().chars().take(300).collect(),
    }
}

#[async_trait]
impl SinkPlugin for S3SinkPlugin {
    fn plugin_name(&self) -> &str {
        "loadsmith-sink-s3"
    }

    fn plugin_version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    async fn configure(&mut self, config: loadsmith_plugin_sdk::PluginConfig) -> Result<()> {
        let cfg: S3Config = config.parse()?;
        self.target = Some(Target::from_config(cfg, config.run().cloned())?);
        Ok(())
    }

    async fn prepare(&mut self) -> Result<()> {
        Ok(())
    }

    async fn deliver(&mut self, path: PathBuf) -> Result<()> {
        let target = self.target.as_ref().context("deliver() before configure()")?;
        let file_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .with_context(|| format!("staged path has no UTF-8 file name: {}", path.display()))?;
        let key = target.keys.render(file_name, target.run.as_ref())?;

        // Telemetry: an `upload <file>` span (`file.path`, `file.size`, the key).
        let span = if telemetry::enabled() {
            tracing::info_span!(
                "upload",
                otel.name = format!("upload {file_name}"),
                file.path = %path.display(),
                file.size = tracing::field::Empty,
                loadsmith.s3.bucket = %target.bucket.name(),
                loadsmith.s3.key = %key,
                error = tracing::field::Empty,
            )
        } else {
            Span::none()
        };
        match target.upload(&path, &key).await {
            Ok(bytes) => {
                span.record("file.size", bytes);
            }
            Err(e) => {
                span.record("error", format!("{e:#}"));
                return Err(e);
            }
        }

        self.delivered += 1;
        tracing::debug!(from = %path.display(), bucket = %target.bucket.name(), key = %key, "delivered object");
        Ok(())
    }

    async fn finalize(&mut self) -> Result<u64> {
        Ok(self.delivered)
    }

    async fn cancel(&mut self) {
        // Nothing to undo: delivered objects stay, and a multipart upload cut
        // short is aborted by the failing delivery itself.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::prelude::*;
    use loadsmith_plugin_sdk::{PluginConfig, UnknownKeys};
    use serde_json::json;

    fn run() -> RunInfo {
        // 2026-10-22T12:00:00Z
        RunInfo { id: "4242-1792670400000".into(), pipeline: "orders".into(), started_at_unix_ms: 1_792_670_400_000 }
    }

    fn scratch(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("s3-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    async fn configured(cfg: serde_json::Value) -> Result<S3SinkPlugin> {
        let mut p = S3SinkPlugin::new();
        p.configure(PluginConfig::new(cfg, UnknownKeys::Error).with_run(Some(run()))).await?;
        Ok(p)
    }

    fn mock_cfg(server: &MockServer, extra: serde_json::Value) -> serde_json::Value {
        let mut v = json!({
            "bucket": "lake", "endpoint": server.base_url(), "region": "us-east-1",
            "access_key_id": "AKIDEXAMPLE", "secret_access_key": "secret", "max_attempts": 2,
        });
        v.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
        v
    }

    #[tokio::test]
    async fn config_is_strict_and_checked() {
        let err = |v: serde_json::Value| async move { format!("{:#}", configured(v).await.err().unwrap()) };
        let base = json!({ "bucket": "b", "access_key_id": "k", "secret_access_key": "s" });
        let with = |extra: serde_json::Value| {
            let mut v = base.clone();
            v.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
            v
        };
        assert!(err(json!({})).await.contains("bucket"));
        assert!(err(with(json!({ "buckt": 1 }))).await.contains("`buckt`"));
        assert!(err(with(json!({ "part_size_mib": 4 }))).await.contains("at least 5"));
        assert!(err(with(json!({ "access_key_id": null }))).await.contains("both"));
        assert!(err(with(json!({ "url_style": "dns" }))).await.contains("url_style"));
        // A key naming the run needs a core that sends it.
        let mut p = S3SinkPlugin::new();
        let e = p.configure(PluginConfig::new(base.clone(), UnknownKeys::Error)).await.unwrap_err();
        assert!(format!("{e:#}").contains("configure.run"), "{e:#}");
        let mut p = S3SinkPlugin::new();
        p.configure(PluginConfig::new(with(json!({ "key": "{file}" })), UnknownKeys::Error)).await.unwrap();
    }

    #[tokio::test]
    async fn small_files_go_in_one_put() {
        let server = MockServer::start_async().await;
        let put = server
            .mock_async(|when, then| {
                when.method(PUT).path("/lake/raw/2026-10-22/4242-1792670400000/part.parquet").body("hello");
                then.status(200).header("etag", "\"5d41\"");
            })
            .await;
        let dir = scratch("put");
        let file = dir.join("part.parquet");
        std::fs::write(&file, b"hello").unwrap();

        let mut p = configured(mock_cfg(&server, json!({ "prefix": "raw" }))).await.unwrap();
        p.deliver(file).await.unwrap();
        put.assert_async().await;
        assert_eq!(p.finalize().await.unwrap(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn server_errors_are_retried_then_reported() {
        let server = MockServer::start_async().await;
        let put = server
            .mock_async(|when, then| {
                when.method(PUT);
                then.status(503).body("<Error><Code>SlowDown</Code><Message>Reduce your request rate.</Message></Error>");
            })
            .await;
        let dir = scratch("retry");
        let file = dir.join("f.parquet");
        std::fs::write(&file, b"x").unwrap();

        let mut p = configured(mock_cfg(&server, json!({}))).await.unwrap();
        let e = format!("{:#}", p.deliver(file).await.unwrap_err());
        assert!(e.contains("503") && e.contains("SlowDown: Reduce your request rate."), "{e}");
        put.assert_hits_async(2).await; // max_attempts
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn client_errors_fail_at_once() {
        let server = MockServer::start_async().await;
        let put = server
            .mock_async(|when, then| {
                when.method(PUT);
                then.status(403).body("<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>");
            })
            .await;
        let dir = scratch("denied");
        let file = dir.join("f.parquet");
        std::fs::write(&file, b"x").unwrap();
        let mut p = configured(mock_cfg(&server, json!({}))).await.unwrap();
        assert!(format!("{:#}", p.deliver(file).await.unwrap_err()).contains("AccessDenied"));
        put.assert_hits_async(1).await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn large_files_go_multipart() {
        let server = MockServer::start_async().await;
        let path = "/lake/2026-10-22/4242-1792670400000/big.parquet";
        let create = server
            .mock_async(|when, then| {
                when.method(POST).path(path).query_param_exists("uploads");
                then.status(200).body(
                    "<InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Bucket>lake</Bucket><Key>k</Key><UploadId>up-1</UploadId></InitiateMultipartUploadResult>",
                );
            })
            .await;
        let part1 = server
            .mock_async(|when, then| {
                when.method(PUT).path(path).query_param("partNumber", "1").query_param("uploadId", "up-1");
                then.status(200).header("etag", "\"e1\"");
            })
            .await;
        let part2 = server
            .mock_async(|when, then| {
                when.method(PUT).path(path).query_param("partNumber", "2").query_param("uploadId", "up-1");
                then.status(200).header("etag", "\"e2\"");
            })
            .await;
        let complete = server
            .mock_async(|when, then| {
                when.method(POST).path(path).query_param("uploadId", "up-1").body_contains("<PartNumber>2</PartNumber>");
                then.status(200).body("<CompleteMultipartUploadResult><ETag>\"x-2\"</ETag></CompleteMultipartUploadResult>");
            })
            .await;
        let dir = scratch("multi");
        let file = dir.join("big.parquet");
        std::fs::write(&file, vec![7u8; 5 * 1024 * 1024 + 10]).unwrap();

        let mut p = configured(mock_cfg(&server, json!({ "part_size_mib": 5 }))).await.unwrap();
        p.deliver(file).await.unwrap();
        create.assert_async().await;
        part1.assert_async().await;
        part2.assert_async().await;
        complete.assert_async().await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_failed_completion_aborts_the_upload() {
        let server = MockServer::start_async().await;
        server
            .mock_async(|when, then| {
                when.method(POST).query_param_exists("uploads");
                then.status(200).body("<InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><UploadId>up-9</UploadId></InitiateMultipartUploadResult>");
            })
            .await;
        server
            .mock_async(|when, then| {
                when.method(PUT).query_param_exists("partNumber");
                then.status(200).header("etag", "\"e\"");
            })
            .await;
        // 200 with an error body: S3's way of failing a completion late.
        server
            .mock_async(|when, then| {
                when.method(POST).query_param("uploadId", "up-9");
                then.status(200).body("<Error><Code>InternalError</Code><Message>try again</Message></Error>");
            })
            .await;
        let abort = server
            .mock_async(|when, then| {
                when.method(DELETE).query_param("uploadId", "up-9");
                then.status(204);
            })
            .await;
        let dir = scratch("abort");
        let file = dir.join("big.parquet");
        std::fs::write(&file, vec![1u8; 6 * 1024 * 1024]).unwrap();

        let mut p = configured(mock_cfg(&server, json!({ "part_size_mib": 5 }))).await.unwrap();
        let e = format!("{:#}", p.deliver(file).await.unwrap_err());
        assert!(e.contains("InternalError"), "{e}");
        abort.assert_async().await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn upload_id_is_read_whatever_the_root_element() {
        let aws = r#"<?xml version="1.0"?><InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>b</Bucket><Key>k</Key><UploadId>abc-1</UploadId></InitiateMultipartUploadResult>"#;
        let compat = r#"<?xml version="1.0" encoding="UTF-8"?><InitiateMultipartUpload><Bucket>b</Bucket><Key>k</Key><UploadId>2</UploadId></InitiateMultipartUpload>"#;
        assert_eq!(xml_element(aws, "UploadId").as_deref(), Some("abc-1"));
        assert_eq!(xml_element(compat, "UploadId").as_deref(), Some("2"));
        assert_eq!(xml_element("<Error/>", "UploadId"), None);
    }

    #[test]
    fn part_size_stays_within_s3_limits() {
        let t = Target::from_config(
            serde_json::from_value(json!({ "bucket": "b", "access_key_id": "k", "secret_access_key": "s", "key": "{file}" })).unwrap(),
            None,
        )
        .unwrap();
        assert_eq!(t.part_size_for(1), 16 * 1024 * 1024);
        let huge = 400 * 1024 * 1024 * 1024u64; // 400 GiB
        assert!(huge.div_ceil(t.part_size_for(huge)) <= MAX_PARTS);
    }
}
