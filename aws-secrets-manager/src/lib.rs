//! `loadsmith-provider-aws-secrets-manager`: resolves
//! `{{ secret('<instance>', '<ref>') }}` values from AWS Secrets Manager.
//!
//! A reference is `secret-id[#key]`:
//! - `secret-id` is the secret's name or ARN (`prod/db`,
//!   `arn:aws:secretsmanager:…:secret:prod/db-AbCdEf`);
//! - without `#key`, the value is the whole `SecretString`;
//! - with `#key`, the `SecretString` is parsed as JSON and `key` (a dotted
//!   path, where a numeric segment indexes an array) selects a scalar — the
//!   shape the AWS console gives a database secret (`prod/db#password`).
//!
//! Each secret is fetched once per run (`GetSecretValue`, stage
//! `version_stage`, default `AWSCURRENT`), however many keys are taken from
//! it. Requests are SigV4-signed with the AWS credential chain
//! ([`loadsmith_aws::credentials`]) and retried on network errors,
//! throttling and `5xx`.
//!
//! **No message produced here ever contains a value** — nor a JSON parser
//! error text, which can quote the secret. Errors name the secret id, the
//! key and the AWS error code.

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use loadsmith_aws::{sigv4, Credentials, Explicit, Provider};
use loadsmith_plugin_sdk::{PluginConfig, ProviderPlugin, ResolveFailure};
use reqwest::{StatusCode, Url};
use serde::Deserialize;
use serde_json::Value;

const NOT_FOUND: &str = "NOT_FOUND";
const INVALID_REF: &str = "INVALID_REF";
const ACCESS_DENIED: &str = "ACCESS_DENIED";
const RESOLVE_FAILED: &str = "RESOLVE_FAILED";

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    /// Default `AWS_REGION`, then `AWS_DEFAULT_REGION`, then `us-east-1`.
    #[serde(default)]
    region: Option<String>,
    /// Another endpoint (LocalStack, a VPC endpoint's DNS name). Default
    /// `AWS_ENDPOINT_URL_SECRETS_MANAGER`, then
    /// `https://secretsmanager.<region>.amazonaws.com`.
    #[serde(default)]
    endpoint: Option<String>,
    /// The staging label to read. Default `AWSCURRENT`.
    #[serde(default)]
    version_stage: Option<String>,
    /// Default: the AWS credential chain.
    #[serde(default)]
    access_key_id: Option<String>,
    #[serde(default)]
    secret_access_key: Option<String>,
    #[serde(default)]
    session_token: Option<String>,
    /// Limit for one request. Default 30.
    #[serde(default)]
    request_timeout_seconds: Option<u64>,
    /// Tries per request (network errors, throttling, `5xx`), with 1s, 2s,
    /// 4s, … between them. Default 4.
    #[serde(default)]
    max_attempts: Option<u32>,
}

struct Client {
    endpoint: Url,
    region: String,
    version_stage: Option<String>,
    credentials: Provider,
    http: reqwest::Client,
    max_attempts: u32,
}

#[derive(Default)]
pub struct SecretsManagerProvider {
    client: Option<Client>,
    /// `SecretString` per secret id, fetched once.
    fetched: HashMap<String, String>,
}

impl SecretsManagerProvider {
    pub fn new() -> Self {
        Self::default()
    }
}

fn fail(code: &str, message: impl Into<String>) -> ResolveFailure {
    ResolveFailure::new(code, message)
}

/// An AWS JSON-protocol error: `{"__type": "…#Code", "message"/"Message": …}`.
fn aws_error(body: &str) -> (String, String) {
    let v: Value = serde_json::from_str(body).unwrap_or(Value::Null);
    let code = v["__type"].as_str().unwrap_or("").rsplit('#').next().unwrap_or("").to_string();
    let message = v["message"].as_str().or_else(|| v["Message"].as_str()).unwrap_or("").to_string();
    (code, message)
}

impl Client {
    fn from_config(cfg: Config, credentials: Provider) -> Result<Self> {
        let region = loadsmith_aws::region(cfg.region.as_deref());
        let endpoint = cfg
            .endpoint
            .or_else(|| std::env::var("AWS_ENDPOINT_URL_SECRETS_MANAGER").ok().filter(|v| !v.is_empty()))
            .unwrap_or_else(|| format!("https://secretsmanager.{region}.amazonaws.com"));
        let endpoint: Url = endpoint.parse().with_context(|| format!("`endpoint` {endpoint:?} is not a URL"))?;
        if cfg.max_attempts == Some(0) {
            bail!("`max_attempts` must be at least 1");
        }
        let http = loadsmith_aws::http_client(Duration::from_secs(cfg.request_timeout_seconds.unwrap_or(30)))?;
        Ok(Self {
            endpoint,
            region,
            version_stage: cfg.version_stage,
            credentials,
            http,
            max_attempts: cfg.max_attempts.unwrap_or(4),
        })
    }

    /// One signed `GetSecretValue` call.
    async fn call(&self, credentials: &Credentials, body: &[u8]) -> reqwest::Result<reqwest::Response> {
        let payload = sigv4::sha256_hex(body);
        let mut headers = vec![
            ("content-type".to_string(), "application/x-amz-json-1.1".to_string()),
            ("x-amz-target".to_string(), "secretsmanager.GetSecretValue".to_string()),
        ];
        sigv4::sign(
            sigv4::Request { method: "POST", url: &self.endpoint, headers: &mut headers, payload_sha256: &payload },
            credentials,
            &self.region,
            "secretsmanager",
            SystemTime::now(),
        );
        let mut req = self.http.post(self.endpoint.clone());
        for (k, v) in &headers {
            if k != "host" {
                req = req.header(k.as_str(), v.as_str());
            }
        }
        req.body(body.to_vec()).send().await
    }

    /// The `SecretString` of `secret_id`.
    async fn get_secret_string(&self, secret_id: &str) -> Result<String, ResolveFailure> {
        let mut request = serde_json::json!({ "SecretId": secret_id });
        if let Some(stage) = &self.version_stage {
            request["VersionStage"] = Value::String(stage.clone());
        }
        let body = serde_json::to_vec(&request).expect("a JSON object serializes");
        let mut attempt = 1;
        loop {
            let credentials = self
                .credentials
                .get()
                .await
                .map_err(|e| fail(RESOLVE_FAILED, format!("no AWS credentials: {e:#}")))?;
            let outcome = self.call(&credentials, &body).await;
            let retry = match outcome {
                Ok(resp) if resp.status().is_success() => {
                    let text = resp
                        .text()
                        .await
                        .map_err(|e| fail(RESOLVE_FAILED, format!("reading secret '{secret_id}': {e}")))?;
                    let v: Value = serde_json::from_str(&text)
                        .map_err(|_| fail(RESOLVE_FAILED, format!("Secrets Manager answered '{secret_id}' with no JSON")))?;
                    return match v["SecretString"].as_str() {
                        Some(s) => Ok(s.to_string()),
                        None if v.get("SecretBinary").is_some() => Err(fail(
                            INVALID_REF,
                            format!("secret '{secret_id}' is binary (SecretBinary); only SecretString secrets are supported"),
                        )),
                        None => Err(fail(NOT_FOUND, format!("secret '{secret_id}' has no SecretString"))),
                    };
                }
                Ok(resp) => {
                    let status = resp.status();
                    let (code, message) = aws_error(&resp.text().await.unwrap_or_default());
                    let shown = if message.is_empty() { code.clone() } else { format!("{code}: {message}") };
                    let failure = match code.as_str() {
                        "ResourceNotFoundException" => fail(NOT_FOUND, format!("secret '{secret_id}' not found ({shown})")),
                        "AccessDeniedException" | "DecryptionFailure" | "UnrecognizedClientException"
                        | "InvalidSignatureException" | "ExpiredTokenException" => {
                            fail(ACCESS_DENIED, format!("secret '{secret_id}': {shown}"))
                        }
                        "InvalidParameterException" | "InvalidRequestException" => {
                            fail(INVALID_REF, format!("secret '{secret_id}': {shown}"))
                        }
                        _ => fail(RESOLVE_FAILED, format!("secret '{secret_id}': HTTP {status}: {shown}")),
                    };
                    let retryable = status.is_server_error()
                        || status == StatusCode::TOO_MANY_REQUESTS
                        || code == "ThrottlingException";
                    if !retryable {
                        return Err(failure);
                    }
                    failure
                }
                Err(e) => fail(RESOLVE_FAILED, format!("secret '{secret_id}': {e}")),
            };
            if attempt >= self.max_attempts {
                return Err(retry);
            }
            let wait = Duration::from_secs(1 << (attempt - 1).min(5));
            tracing::warn!(attempt, max_attempts = self.max_attempts, "GetSecretValue '{secret_id}' failed, retrying in {wait:?}");
            tokio::time::sleep(wait).await;
            attempt += 1;
        }
    }
}

/// Splits `secret-id[#key]`.
fn parse_ref(reference: &str) -> Result<(&str, Option<Vec<&str>>), ResolveFailure> {
    let (id, key) = match reference.split_once('#') {
        Some((id, key)) => (id, Some(key)),
        None => (reference, None),
    };
    if id.trim().is_empty() {
        return Err(fail(INVALID_REF, format!("reference '{reference}' has no secret id (expected secret-id[#key])")));
    }
    let key = match key {
        None => None,
        Some(key) => {
            let segments: Vec<&str> = key.split('.').collect();
            if segments.iter().any(|s| s.is_empty()) {
                return Err(fail(INVALID_REF, format!("reference '{reference}' has an empty key segment")));
            }
            Some(segments)
        }
    };
    Ok((id, key))
}

/// The scalar at the dotted `segments` of the JSON secret `text`.
fn select(text: &str, segments: &[&str], id: &str) -> Result<String, ResolveFailure> {
    let key = segments.join(".");
    // The parser's error text could quote the secret: only its position.
    let doc: Value = serde_json::from_str(text).map_err(|e| {
        fail(INVALID_REF, format!("secret '{id}' is not JSON (line {}, column {}), so it has no key '{key}'", e.line(), e.column()))
    })?;
    let mut node = &doc;
    for segment in segments {
        let next = match node {
            Value::Object(map) => map.get(*segment),
            Value::Array(items) => segment.parse::<usize>().ok().and_then(|i| items.get(i)),
            _ => None,
        };
        node = next.ok_or_else(|| fail(NOT_FOUND, format!("no key '{key}' in secret '{id}'")))?;
    }
    match node {
        Value::String(s) => Ok(s.clone()),
        Value::Number(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        Value::Null => Err(fail(NOT_FOUND, format!("key '{key}' in secret '{id}' is null"))),
        Value::Object(_) | Value::Array(_) => Err(fail(
            INVALID_REF,
            format!("key '{key}' in secret '{id}' selects an object or a list, not a single value"),
        )),
    }
}

#[async_trait]
impl ProviderPlugin for SecretsManagerProvider {
    fn plugin_name(&self) -> &str {
        "loadsmith-provider-aws-secrets-manager"
    }

    fn plugin_version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    async fn configure(&mut self, config: PluginConfig) -> Result<()> {
        // No `config:` block at all is every default.
        let cfg: Config = if config.is_null() { Config::default() } else { config.parse()? };
        loadsmith_tls::install_provider();
        let region = loadsmith_aws::region(cfg.region.as_deref());
        let explicit = Explicit {
            access_key_id: cfg.access_key_id.as_deref(),
            secret_access_key: cfg.secret_access_key.as_deref(),
            session_token: cfg.session_token.as_deref(),
        };
        let credentials = Provider::resolve(explicit, &region).await?;
        self.client = Some(Client::from_config(cfg, credentials)?);
        Ok(())
    }

    async fn resolve(&mut self, reference: &str) -> Result<String> {
        let client = self.client.as_ref().context("resolve() before configure()")?;
        let (id, key) = parse_ref(reference)?;
        if !self.fetched.contains_key(id) {
            let value = client.get_secret_string(id).await?;
            self.fetched.insert(id.to_string(), value);
        }
        let text = &self.fetched[id];
        match key {
            None => Ok(text.clone()),
            Some(segments) => Ok(select(text, &segments, id)?),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::prelude::*;
    use loadsmith_plugin_sdk::UnknownKeys;
    use serde_json::json;

    const SECRET: &str = "s3cr3t-Value!";

    async fn configured(server: &MockServer, extra: Value) -> Result<SecretsManagerProvider> {
        let mut cfg = json!({
            "endpoint": server.base_url(), "region": "sa-east-1",
            "access_key_id": "AKID", "secret_access_key": "s", "max_attempts": 2,
        });
        cfg.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
        let mut p = SecretsManagerProvider::new();
        p.configure(PluginConfig::new(cfg, UnknownKeys::Error)).await?;
        Ok(p)
    }

    async fn failure(p: &mut SecretsManagerProvider, reference: &str) -> ResolveFailure {
        let e = p.resolve(reference).await.expect_err("expected a failure");
        let f = e.downcast::<ResolveFailure>().expect("a ResolveFailure");
        assert!(!f.message.contains(SECRET), "message leaks the value: {}", f.message);
        f
    }

    #[tokio::test]
    async fn whole_secrets_and_json_keys_resolve_with_one_fetch_per_secret() {
        let server = MockServer::start_async().await;
        let get = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/")
                    .header("x-amz-target", "secretsmanager.GetSecretValue")
                    .header("content-type", "application/x-amz-json-1.1")
                    .header_exists("authorization")
                    .json_body(json!({ "SecretId": "prod/db" }));
                then.status(200).json_body(json!({
                    "Name": "prod/db",
                    "SecretString": json!({ "username": "loader", "password": SECRET, "port": 5432, "hosts": ["a", "b"] }).to_string(),
                }));
            })
            .await;
        let mut p = configured(&server, json!({})).await.unwrap();
        assert_eq!(p.resolve("prod/db#password").await.unwrap(), SECRET);
        assert_eq!(p.resolve("prod/db#port").await.unwrap(), "5432");
        assert_eq!(p.resolve("prod/db#hosts.1").await.unwrap(), "b");
        assert!(p.resolve("prod/db").await.unwrap().contains("\"username\":\"loader\""));
        get.assert_hits_async(1).await;
    }

    #[tokio::test]
    async fn the_version_stage_is_sent() {
        let server = MockServer::start_async().await;
        let get = server
            .mock_async(|when, then| {
                when.method(POST).json_body(json!({ "SecretId": "t", "VersionStage": "AWSPREVIOUS" }));
                then.status(200).json_body(json!({ "SecretString": "old" }));
            })
            .await;
        let mut p = configured(&server, json!({ "version_stage": "AWSPREVIOUS" })).await.unwrap();
        assert_eq!(p.resolve("t").await.unwrap(), "old");
        get.assert_async().await;
    }

    #[tokio::test]
    async fn aws_errors_map_to_failure_codes() {
        let server = MockServer::start_async().await;
        let err = |id: &'static str, status: u16, ty: &'static str| {
            let server = &server;
            async move {
                server
                    .mock_async(move |when, then| {
                        when.method(POST).json_body(json!({ "SecretId": id }));
                        then.status(status).json_body(json!({ "__type": ty, "message": "nope" }));
                    })
                    .await
            }
        };
        err("missing", 400, "ResourceNotFoundException").await;
        err("denied", 400, "AccessDeniedException").await;
        let throttled = err("busy", 400, "ThrottlingException").await;
        server
            .mock_async(|when, then| {
                when.method(POST).json_body(json!({ "SecretId": "bin" }));
                then.status(200).json_body(json!({ "SecretBinary": "AAEC" }));
            })
            .await;
        server
            .mock_async(|when, then| {
                when.method(POST).json_body(json!({ "SecretId": "plain" }));
                then.status(200).json_body(json!({ "SecretString": SECRET }));
            })
            .await;
        let mut p = configured(&server, json!({})).await.unwrap();
        assert_eq!(failure(&mut p, "missing").await.code, NOT_FOUND);
        assert_eq!(failure(&mut p, "denied").await.code, ACCESS_DENIED);
        assert_eq!(failure(&mut p, "busy").await.code, RESOLVE_FAILED);
        throttled.assert_hits_async(2).await; // retried up to max_attempts
        assert_eq!(failure(&mut p, "bin").await.code, INVALID_REF);
        // A non-JSON secret has no keys — and the message must not quote it.
        let f = failure(&mut p, "plain#password").await;
        assert_eq!(f.code, INVALID_REF);
        assert!(f.message.contains("is not JSON"), "{}", f.message);
        assert_eq!(failure(&mut p, "#k").await.code, INVALID_REF);
        assert_eq!(failure(&mut p, "plain#a..b").await.code, INVALID_REF);
    }

    #[tokio::test]
    async fn config_is_strict() {
        let server = MockServer::start_async().await;
        let e = configured(&server, json!({ "regoin": "x" })).await.err().unwrap();
        assert!(format!("{e:#}").contains("regoin"), "{e:#}");
        let e = configured(&server, json!({ "access_key_id": null })).await.err().unwrap();
        assert!(format!("{e:#}").contains("both"), "{e:#}");
    }
}
