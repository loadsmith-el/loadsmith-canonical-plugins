//! Microsoft Graph client: OAuth client-credentials, site/drive resolution, file
//! download, and SharePoint List item paging.
//!
//! HTTPS uses `reqwest` over rustls with the process-default crypto provider
//! (rustls-rustcrypto, installed by [`loadsmith_tls::install_provider`]) + webpki
//! roots — pure Rust, no `ring`/`aws-lc-rs` (the multi-arch posture inherited from
//! loadsmith). `graph_base_url`/`authority_url` are injectable so tests point at a
//! mock and sovereign clouds work without code changes.

use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use reqwest::header::{HeaderMap, RETRY_AFTER};
use reqwest::StatusCode;
use serde_json::Value;

use crate::config::{parse_duration, SharepointConfig};

/// Longest wait between two attempts, whatever `Retry-After` asks for.
const MAX_RETRY_WAIT: Duration = Duration::from_secs(120);

/// Graph client over a configurable base URL.
pub struct GraphClient {
    http: reqwest::Client,
    tenant_id: String,
    client_id: String,
    client_secret: String,
    graph_base_url: String,
    authority_url: String,
    scope: String,
    max_retries: u32,
    token: Option<(String, Instant)>,
}

impl GraphClient {
    pub fn new(cfg: &SharepointConfig) -> Result<Self> {
        let timeout = parse_duration(&cfg.request_timeout).context("invalid `request_timeout`")?;
        if timeout.is_zero() {
            bail!("`request_timeout` must be greater than zero");
        }
        let graph_base_url = cfg.graph_base_url.trim_end_matches('/').to_string();
        let scope = match &cfg.scope {
            Some(s) if s.trim().is_empty() => bail!("`scope` is empty"),
            Some(s) => s.clone(),
            None => default_scope(&graph_base_url)?,
        };
        // Ensure the pure-Rust crypto provider is the process default before
        // reqwest builds its rustls TLS config.
        loadsmith_tls::install_provider();
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .context("failed to build HTTP client")?;
        Ok(Self {
            http,
            tenant_id: cfg.tenant_id.clone(),
            client_id: cfg.client_id.clone(),
            client_secret: cfg.client_secret.clone(),
            graph_base_url,
            authority_url: cfg.authority_url.trim_end_matches('/').to_string(),
            scope,
            max_retries: cfg.max_retries,
            token: None,
        })
    }

    /// Sends the request `build` makes, again on `429`/`503` (after the
    /// server's `Retry-After`, else an exponential backoff) and on a timeout or
    /// a failed connection, at most `max_retries` more times.
    async fn send(&self, what: &str, build: impl Fn() -> reqwest::RequestBuilder) -> Result<reqwest::Response> {
        let mut attempt = 0u32;
        loop {
            let wait = match build().send().await {
                Ok(resp) => match retry_wait(resp.status(), resp.headers(), attempt) {
                    Some(wait) if attempt < self.max_retries => {
                        tracing::warn!("{what} returned {}; retrying in {wait:?}", resp.status());
                        wait
                    }
                    _ => return Ok(resp),
                },
                Err(e) if (e.is_timeout() || e.is_connect()) && attempt < self.max_retries => {
                    let wait = backoff(attempt);
                    tracing::warn!("{what} failed ({e}); retrying in {wait:?}");
                    wait
                }
                Err(e) => return Err(anyhow::Error::new(e).context(format!("{what} failed"))),
            };
            tokio::time::sleep(wait).await;
            attempt += 1;
        }
    }

    /// Returns a cached or freshly-minted access token (client-credentials grant).
    async fn token(&mut self) -> Result<String> {
        if let Some((tok, expiry)) = &self.token {
            if Instant::now() < *expiry {
                return Ok(tok.clone());
            }
        }
        let url = format!("{}/{}/oauth2/v2.0/token", self.authority_url, self.tenant_id);
        let form = [
            ("client_id", self.client_id.as_str()),
            ("client_secret", self.client_secret.as_str()),
            ("scope", self.scope.as_str()),
            ("grant_type", "client_credentials"),
        ];
        let resp = self.send("token request", || self.http.post(&url).form(&form)).await?;
        let body = read_json(resp, "token").await?;
        let token = body
            .get("access_token")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("token response missing access_token"))?
            .to_string();
        let expires_in = body.get("expires_in").and_then(Value::as_u64).unwrap_or(3600);
        let expiry = Instant::now() + Duration::from_secs(expires_in.saturating_sub(60).max(60));
        self.token = Some((token.clone(), expiry));
        Ok(token)
    }

    async fn get(&mut self, url: &str) -> Result<reqwest::Response> {
        let token = self.token().await?;
        self.send(&format!("GET {url}"), || self.http.get(url).bearer_auth(&token)).await
    }

    async fn get_json(&mut self, url: &str) -> Result<Value> {
        let resp = self.get(url).await?;
        read_json(resp, url).await
    }

    async fn get_bytes(&mut self, url: &str) -> Result<Vec<u8>> {
        let resp = self.get(url).await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            bail!("GET {url} returned {status}: {body}");
        }
        Ok(resp.bytes().await.context("reading body bytes")?.to_vec())
    }

    /// Resolves the site id from an explicit id or a `host/path` reference.
    pub async fn resolve_site_id(
        &mut self,
        site: Option<&str>,
        site_id: Option<&str>,
    ) -> Result<String> {
        if let Some(id) = site_id {
            return Ok(id.to_string());
        }
        let site = site.ok_or_else(|| anyhow::anyhow!("provide one of 'site' or 'site_id'"))?;
        let (host, path) = split_site_ref(site)?;
        let url = format!("{}/v1.0/sites/{host}:{path}?$select=id", self.graph_base_url);
        let body = self.get_json(&url).await?;
        body.get("id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| anyhow::anyhow!("could not resolve site id for '{site}'"))
    }

    /// Resolves the drive id from an explicit id or a drive display name.
    pub async fn resolve_drive_id(
        &mut self,
        site_id: &str,
        drive: Option<&str>,
        drive_id: Option<&str>,
    ) -> Result<String> {
        if let Some(id) = drive_id {
            return Ok(id.to_string());
        }
        let drive = drive.ok_or_else(|| anyhow::anyhow!("provide one of 'drive' or 'drive_id'"))?;
        let url = format!("{}/v1.0/sites/{site_id}/drives?$select=id,name", self.graph_base_url);
        let body = self.get_json(&url).await?;
        for d in body.get("value").and_then(Value::as_array).unwrap_or(&Vec::new()) {
            if d.get("name").and_then(Value::as_str) == Some(drive) {
                if let Some(id) = d.get("id").and_then(Value::as_str) {
                    return Ok(id.to_string());
                }
            }
        }
        bail!("drive '{drive}' not found for site '{site_id}'")
    }

    /// Downloads a file's content by drive path (`/folder/file.xlsx`).
    pub async fn download_file(&mut self, drive_id: &str, path: &str) -> Result<Vec<u8>> {
        let normalized = if path.starts_with('/') { path.to_string() } else { format!("/{path}") };
        let encoded = encode_path(&normalized);
        let url =
            format!("{}/v1.0/drives/{drive_id}/root:{encoded}:/content", self.graph_base_url);
        self.get_bytes(&url).await
    }

    /// Resolves a list id from a display name (falls back to treating the value as
    /// an id if no display-name match is found).
    pub async fn resolve_list_id(&mut self, site_id: &str, list: &str) -> Result<String> {
        let url = format!(
            "{}/v1.0/sites/{site_id}/lists?$select=id,displayName,name",
            self.graph_base_url
        );
        let body = self.get_json(&url).await?;
        if let Some(arr) = body.get("value").and_then(Value::as_array) {
            for l in arr {
                let matches = l.get("displayName").and_then(Value::as_str) == Some(list)
                    || l.get("name").and_then(Value::as_str) == Some(list);
                if matches {
                    if let Some(id) = l.get("id").and_then(Value::as_str) {
                        return Ok(id.to_string());
                    }
                }
            }
        }
        // No display-name match — assume the caller passed an id directly.
        Ok(list.to_string())
    }

    /// Fetches every list item's `fields` object, following `@odata.nextLink`.
    pub async fn fetch_list_items(&mut self, site_id: &str, list_id: &str) -> Result<Vec<Value>> {
        let mut url = format!(
            "{}/v1.0/sites/{site_id}/lists/{list_id}/items?$expand=fields",
            self.graph_base_url
        );
        let mut fields = Vec::new();
        loop {
            let body = self.get_json(&url).await?;
            if let Some(arr) = body.get("value").and_then(Value::as_array) {
                for item in arr {
                    if let Some(f) = item.get("fields") {
                        fields.push(f.clone());
                    }
                }
            }
            match body.get("@odata.nextLink").and_then(Value::as_str) {
                Some(next) => url = next.to_string(),
                None => break,
            }
        }
        Ok(fields)
    }
}

async fn read_json(resp: reqwest::Response, what: &str) -> Result<Value> {
    let status = resp.status();
    let text = resp.text().await.with_context(|| format!("reading {what} body"))?;
    if !status.is_success() {
        bail!("{what} returned {status}: {text}");
    }
    serde_json::from_str(&text).with_context(|| format!("{what}: invalid JSON: {text}"))
}

/// The default token scope: the origin of `graph_base_url` + `/.default`
/// (`https://graph.microsoft.com/.default` for the public cloud).
fn default_scope(graph_base_url: &str) -> Result<String> {
    let url = reqwest::Url::parse(graph_base_url)
        .with_context(|| format!("invalid `graph_base_url` '{graph_base_url}'"))?;
    let origin = url.origin();
    if !origin.is_tuple() {
        bail!("invalid `graph_base_url` '{graph_base_url}': expected an http(s) URL");
    }
    Ok(format!("{}/.default", origin.ascii_serialization()))
}

/// How long to wait before retrying a response, or `None` when it is not
/// retried: `429` and `503` are, after their `Retry-After` (seconds) when
/// present, else an exponential backoff — capped at [`MAX_RETRY_WAIT`].
fn retry_wait(status: StatusCode, headers: &HeaderMap, attempt: u32) -> Option<Duration> {
    if status != StatusCode::TOO_MANY_REQUESTS && status != StatusCode::SERVICE_UNAVAILABLE {
        return None;
    }
    let asked = headers
        .get(RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(Duration::from_secs);
    Some(asked.unwrap_or_else(|| backoff(attempt)).min(MAX_RETRY_WAIT))
}

/// 1 s, 2 s, 4 s, … capped at [`MAX_RETRY_WAIT`].
fn backoff(attempt: u32) -> Duration {
    Duration::from_secs(1u64 << attempt.min(16)).min(MAX_RETRY_WAIT)
}

/// Splits `host/server-relative/path` into `(host, "/server-relative/path")`.
/// Accepts a leading scheme, which is stripped.
fn split_site_ref(site: &str) -> Result<(String, String)> {
    let s = site
        .trim()
        .trim_start_matches("https://")
        .trim_start_matches("http://");
    match s.split_once('/') {
        Some((host, rest)) if !host.is_empty() => Ok((host.to_string(), format!("/{rest}"))),
        _ => bail!("invalid 'site' reference '{site}' (expected host/path)"),
    }
}

/// Percent-encodes a drive path, preserving `/`. Unreserved chars pass through.
fn encode_path(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    for b in path.bytes() {
        match b {
            b'/' | b'-' | b'.' | b'_' | b'~' => out.push(b as char),
            b if b.is_ascii_alphanumeric() => out.push(b as char),
            b => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_site_ref() {
        let (h, p) = split_site_ref("contoso.sharepoint.com/sites/Team").unwrap();
        assert_eq!(h, "contoso.sharepoint.com");
        assert_eq!(p, "/sites/Team");
        let (h, p) = split_site_ref("https://contoso.sharepoint.com/sites/Team").unwrap();
        assert_eq!(h, "contoso.sharepoint.com");
        assert_eq!(p, "/sites/Team");
    }

    #[test]
    fn scope_follows_the_graph_cloud() {
        assert_eq!(default_scope("https://graph.microsoft.com").unwrap(), "https://graph.microsoft.com/.default");
        assert_eq!(default_scope("https://graph.microsoft.us/v1.0").unwrap(), "https://graph.microsoft.us/.default");
        assert_eq!(default_scope("http://127.0.0.1:8080").unwrap(), "http://127.0.0.1:8080/.default");
        assert!(default_scope("graph.microsoft.com").is_err());
    }

    #[test]
    fn retries_honor_retry_after_and_back_off() {
        let mut h = HeaderMap::new();
        assert_eq!(retry_wait(StatusCode::NOT_FOUND, &h, 0), None);
        assert_eq!(retry_wait(StatusCode::INTERNAL_SERVER_ERROR, &h, 0), None);
        assert_eq!(retry_wait(StatusCode::SERVICE_UNAVAILABLE, &h, 0), Some(Duration::from_secs(1)));
        assert_eq!(retry_wait(StatusCode::TOO_MANY_REQUESTS, &h, 3), Some(Duration::from_secs(8)));
        assert_eq!(retry_wait(StatusCode::TOO_MANY_REQUESTS, &h, 30), Some(MAX_RETRY_WAIT));
        h.insert(RETRY_AFTER, "7".parse().unwrap());
        assert_eq!(retry_wait(StatusCode::TOO_MANY_REQUESTS, &h, 0), Some(Duration::from_secs(7)));
        h.insert(RETRY_AFTER, "3600".parse().unwrap());
        assert_eq!(retry_wait(StatusCode::SERVICE_UNAVAILABLE, &h, 0), Some(MAX_RETRY_WAIT));
    }

    #[test]
    fn encodes_spaces_and_accents_keeping_slashes() {
        assert_eq!(encode_path("/a b/c.xlsx"), "/a%20b/c.xlsx");
        assert_eq!(encode_path("/Á/x"), "/%C3%81/x");
    }
}
