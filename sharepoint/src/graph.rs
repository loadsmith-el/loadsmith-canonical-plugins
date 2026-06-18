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
use serde_json::Value;

/// Graph client over a configurable base URL.
pub struct GraphClient {
    http: reqwest::Client,
    tenant_id: String,
    client_id: String,
    client_secret: String,
    graph_base_url: String,
    authority_url: String,
    token: Option<(String, Instant)>,
}

impl GraphClient {
    pub fn new(
        tenant_id: String,
        client_id: String,
        client_secret: String,
        graph_base_url: String,
        authority_url: String,
    ) -> Result<Self> {
        // Ensure the pure-Rust crypto provider is the process default before
        // reqwest builds its rustls TLS config.
        loadsmith_tls::install_provider();
        let http = reqwest::Client::builder()
            .build()
            .context("failed to build HTTP client")?;
        Ok(Self {
            http,
            tenant_id,
            client_id,
            client_secret,
            graph_base_url: graph_base_url.trim_end_matches('/').to_string(),
            authority_url: authority_url.trim_end_matches('/').to_string(),
            token: None,
        })
    }

    /// Returns a cached or freshly-minted access token (client-credentials grant).
    async fn token(&mut self) -> Result<String> {
        if let Some((tok, expiry)) = &self.token {
            if Instant::now() < *expiry {
                return Ok(tok.clone());
            }
        }
        let url = format!("{}/{}/oauth2/v2.0/token", self.authority_url, self.tenant_id);
        let resp = self
            .http
            .post(&url)
            .form(&[
                ("client_id", self.client_id.as_str()),
                ("client_secret", self.client_secret.as_str()),
                ("scope", "https://graph.microsoft.com/.default"),
                ("grant_type", "client_credentials"),
            ])
            .send()
            .await
            .context("token request failed")?;
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

    async fn get_json(&mut self, url: &str) -> Result<Value> {
        let token = self.token().await?;
        let resp = self
            .http
            .get(url)
            .bearer_auth(token)
            .send()
            .await
            .with_context(|| format!("GET {url} failed"))?;
        read_json(resp, url).await
    }

    async fn get_bytes(&mut self, url: &str) -> Result<Vec<u8>> {
        let token = self.token().await?;
        let resp = self
            .http
            .get(url)
            .bearer_auth(token)
            .send()
            .await
            .with_context(|| format!("GET {url} failed"))?;
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
    fn encodes_spaces_and_accents_keeping_slashes() {
        assert_eq!(encode_path("/a b/c.xlsx"), "/a%20b/c.xlsx");
        assert_eq!(encode_path("/Á/x"), "/%C3%81/x");
    }
}
