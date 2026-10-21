//! HTTP client telemetry for the Graph calls (OpenTelemetry semantic
//! conventions): one `client` span per call (retries included, each one a
//! `retry` event) and the `http.client.request.duration` histogram. Never
//! records a token, a secret or a request body; `url.full` has its query
//! values redacted except the harmless OData projections. Nothing is built
//! while telemetry is off.

use std::time::{Duration, Instant};

use loadsmith_plugin_sdk::telemetry::{self, histogram, kv};
use tracing::field::Empty;
use tracing::Span;

/// Query parameters whose values are kept in `url.full` (column projections,
/// never data or opaque tokens).
const KEPT_PARAMS: &[&str] = &["$select", "$expand", "$top"];

/// One HTTP call being measured.
pub struct HttpCall {
    span: Span,
    started: Instant,
    method: &'static str,
    server: Option<(String, u16)>,
    status: Option<u16>,
    error_type: Option<String>,
}

impl HttpCall {
    pub fn new(method: &'static str, url: &str) -> Self {
        if !telemetry::enabled() {
            return Self { span: Span::none(), started: Instant::now(), method, server: None, status: None, error_type: None };
        }
        let parsed = reqwest::Url::parse(url).ok();
        let server = parsed
            .as_ref()
            .and_then(|u| Some((u.host_str()?.to_string(), u.port_or_known_default()?)));
        let span = tracing::info_span!(
            "http",
            otel.name = method,
            otel.kind = "client",
            http.request.method = method,
            url.full = sanitize_url(url).as_str(),
            server.address = server.as_ref().map(|(h, _)| h.as_str()),
            server.port = server.as_ref().map(|(_, p)| *p),
            http.response.status_code = Empty,
            http.request.resend_count = Empty,
            http.response.body.size = Empty,
            error.type = Empty,
            error = Empty,
        );
        Self { span, started: Instant::now(), method, server, status: None, error_type: None }
    }

    pub fn span(&self) -> &Span {
        &self.span
    }

    /// The final response's status (`error.type` = the code when ≥ 400).
    pub fn response(&mut self, status: u16) {
        if self.span.is_none() {
            return;
        }
        self.status = Some(status);
        self.span.record("http.response.status_code", status);
        if status >= 400 {
            self.error_type = Some(status.to_string());
            self.span.record("error.type", status.to_string().as_str());
        }
    }

    /// The request failed without a response.
    pub fn transport_error(&mut self, e: &reqwest::Error) {
        if self.span.is_none() {
            return;
        }
        let ty = if e.is_timeout() {
            "timeout"
        } else if e.is_connect() {
            "connect"
        } else {
            "_OTHER"
        };
        self.error_type = Some(ty.to_string());
        self.span.record("error.type", ty);
    }

    /// One retry: a `retry` span event with what caused it and the wait used
    /// (the server's `Retry-After` when it sent one, else the backoff).
    pub fn retry(&self, resend: u32, cause: &str, wait: Duration, retry_after: Option<&str>) {
        if self.span.is_none() {
            return;
        }
        self.span.record("http.request.resend_count", resend);
        self.span.in_scope(|| {
            tracing::warn!(
                http.request.resend_count = resend,
                loadsmith.retry.cause = cause,
                loadsmith.retry.wait = wait.as_secs_f64(),
                http.response.header.retry_after = retry_after,
                "retry"
            )
        });
    }

    /// Closes the call: the error status on failure and the duration histogram.
    pub fn finish<T>(self, result: &anyhow::Result<T>) {
        if self.span.is_none() {
            return;
        }
        let mut attrs = vec![kv("http.request.method", self.method)];
        if let Some((host, port)) = &self.server {
            attrs.push(kv("server.address", host.clone()));
            attrs.push(kv("server.port", *port));
        }
        if let Some(status) = self.status {
            attrs.push(kv("http.response.status_code", status));
        }
        if let Err(e) = result {
            let ty = self.error_type.clone().unwrap_or_else(|| "_OTHER".into());
            self.span.record("error.type", ty.as_str());
            self.span.record("error", format!("{e:#}"));
            attrs.push(kv("error.type", ty));
        }
        histogram("http.client.request.duration").with_unit("s").record(self.started.elapsed(), &attrs);
    }
}

/// `url.full` without secrets: no user info, and every query value replaced
/// by `REDACTED` except [`KEPT_PARAMS`]. An unparseable URL keeps only the
/// part before `?`.
pub fn sanitize_url(url: &str) -> String {
    let Ok(mut u) = reqwest::Url::parse(url) else {
        return url.split('?').next().unwrap_or_default().to_string();
    };
    let _ = u.set_username("");
    let _ = u.set_password(None);
    if let Some(query) = u.query().map(str::to_string) {
        let redacted = query
            .split('&')
            .map(|pair| match pair.split_once('=') {
                Some((k, v)) if KEPT_PARAMS.contains(&k.replace("%24", "$").as_str()) => format!("{k}={v}"),
                Some((k, _)) => format!("{k}=REDACTED"),
                None => pair.to_string(),
            })
            .collect::<Vec<_>>()
            .join("&");
        u.set_query(Some(&redacted));
    }
    u.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_full_redacts_query_values_and_user_info() {
        let u = sanitize_url(
            "https://graph.microsoft.com/v1.0/sites/s/lists/l/items?$expand=fields&$skiptoken=abc123&sig=SECRET",
        );
        assert!(!u.contains("abc123") && !u.contains("SECRET"), "{u}");
        assert!(u.contains("$expand=fields"), "{u}");
        assert!(u.contains("$skiptoken=REDACTED&sig=REDACTED"), "{u}");

        let u = sanitize_url("https://user:pw@login.example.com/t/oauth2/v2.0/token");
        assert_eq!(u, "https://login.example.com/t/oauth2/v2.0/token");
        assert_eq!(sanitize_url("not a url?token=x"), "not a url");
    }
}
