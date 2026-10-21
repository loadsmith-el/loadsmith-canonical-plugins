//! Shared mysql connection config + `connect()`.
//!
//! Both the source and destination plugins embed [`ConnectionConfig`] (via
//! `#[serde(flatten)]`) and call [`connect`] / [`build_opts`], so the connection
//! surface — TLS (all modes + mTLS), keepalive, statement timeout, and session
//! variables — lives in exactly one place.
//!
//! Unlike the postgres plugin (which hands `tokio-postgres-rustls-improved` a
//! `rustls::ClientConfig` directly), `mysql_async` builds its own rustls config
//! from [`SslOpts`] using the **process-default `CryptoProvider`**. We install
//! that provider (rustls-rustcrypto, the pure-Rust provider) via
//! [`loadsmith_tls::install_provider`] before connecting, so the same multi-arch
//! posture holds: no ring / aws-lc / native-tls anywhere in the tree.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use crate::vocab::{TlsBlock, TlsMode};
use mysql_async::{ClientIdentity, Conn, Opts, OptsBuilder, SslOpts};
use serde::Deserialize;

// ── Config struct ─────────────────────────────────────────────────────────────

/// The connection-level config shared by both mysql plugins. Embed it with
/// `#[serde(flatten)]` alongside plugin-specific fields (query/target_table/…).
#[derive(Debug, Deserialize)]
pub struct ConnectionConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    pub dbname: String,
    pub user: String,
    pub password: String,

    pub tls: Option<TlsBlock>,

    /// Duration string ("10s", "500ms", "2m"); applied as the session
    /// `max_execution_time` (milliseconds) — MySQL's per-statement read timeout.
    pub statement_timeout: Option<String>,
    /// Session variables applied on connect via `SET SESSION <k> = <v>`.
    pub session: Option<HashMap<String, String>>,
    /// TCP keepalive idle duration ("30s", "2m").
    pub tcp_keepalive: Option<String>,
}

fn default_host() -> String {
    "localhost".to_string()
}
fn default_port() -> u16 {
    3306
}

// ── Duration parsing ──────────────────────────────────────────────────────────

/// Parses a human-readable duration string into a `Duration`.
/// Accepted suffixes: `ms`, `s`, `m`, `h`, `d`.
pub fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    // Check "ms" before "s" to avoid stripping just the trailing 's' from "10ms".
    if let Some(n) = s.strip_suffix("ms") {
        let ms = n.trim().parse::<u64>().map_err(|_| dur_err(s))?;
        return Ok(Duration::from_millis(ms));
    }
    if let Some(n) = s.strip_suffix('s') {
        return Ok(Duration::from_secs(n.trim().parse::<u64>().map_err(|_| dur_err(s))?));
    }
    if let Some(n) = s.strip_suffix('m') {
        return Ok(Duration::from_secs(n.trim().parse::<u64>().map_err(|_| dur_err(s))? * 60));
    }
    if let Some(n) = s.strip_suffix('h') {
        return Ok(Duration::from_secs(n.trim().parse::<u64>().map_err(|_| dur_err(s))? * 3600));
    }
    if let Some(n) = s.strip_suffix('d') {
        return Ok(Duration::from_secs(n.trim().parse::<u64>().map_err(|_| dur_err(s))? * 86400));
    }
    Err(dur_err(s))
}

fn dur_err(s: &str) -> anyhow::Error {
    anyhow::anyhow!(
        "invalid duration '{s}': expected a number followed by ms/s/m/h/d (e.g. '10s', '500ms', '2h')"
    )
}

// ── TLS mapping ───────────────────────────────────────────────────────────────

/// Maps the generic [`TlsBlock`] onto `mysql_async`'s [`SslOpts`]. Returns
/// `None` for [`TlsMode::Disable`] (plain connection).
fn ssl_opts(cfg: &TlsBlock) -> Result<Option<SslOpts>> {
    let opts = match cfg.mode {
        TlsMode::Disable => return Ok(None),
        // mysql_async has no opportunistic mode: it can't fall back to
        // plaintext, so `prefer` would silently mean `require`.
        TlsMode::Prefer => bail!(
            "mysql: tls.mode `prefer` is not supported (the driver has no plaintext fallback); \
             use `require` to always encrypt, or `disable`"
        ),
        // Encrypt-only: no chain or hostname checks (mirrors loadsmith-tls's
        // NoVerifier path).
        TlsMode::Require => SslOpts::default()
            .with_danger_accept_invalid_certs(true)
            .with_danger_skip_domain_validation(true),
        // Verify the chain against root_cert; skip the hostname for verify-ca.
        TlsMode::VerifyCa | TlsMode::VerifyFull => {
            let root = cfg
                .root_cert
                .as_ref()
                .context("tls mode verify-ca/verify-full requires root_cert")?;
            SslOpts::default()
                .with_root_certs(vec![root.clone().into_bytes().into()])
                .with_danger_skip_domain_validation(matches!(cfg.mode, TlsMode::VerifyCa))
        }
    };
    Ok(Some(with_client_identity(opts, cfg)?))
}

/// Adds mTLS client identity when both `client_cert` and `client_key` are set.
fn with_client_identity(opts: SslOpts, cfg: &TlsBlock) -> Result<SslOpts> {
    match (&cfg.client_cert, &cfg.client_key) {
        (Some(cert), Some(key)) => Ok(opts.with_client_identity(Some(ClientIdentity::new(
            cert.clone().into_bytes().into(),
            key.clone().into_bytes().into(),
        )))),
        (None, None) => Ok(opts),
        _ => bail!("tls client_cert and client_key must be provided together"),
    }
}

// ── Opts builder + connect ────────────────────────────────────────────────────

/// Builds `mysql_async::Opts` from the config. Side effect: installs the
/// process-wide rustls-rustcrypto provider (idempotent) so mysql_async's TLS
/// uses the pure-Rust crypto provider.
pub fn build_opts(cfg: &ConnectionConfig) -> Result<Opts> {
    loadsmith_tls::install_provider();

    let mut init: Vec<String> = Vec::new();
    if let Some(s) = &cfg.statement_timeout {
        let ms = parse_duration(s).context("invalid statement_timeout")?.as_millis();
        init.push(format!("SET SESSION max_execution_time = {ms}"));
    }
    if let Some(session) = &cfg.session {
        for (k, v) in session {
            if k.trim().eq_ignore_ascii_case("time_zone") {
                bail!(
                    "mysql: `session.time_zone` can't be set: the session time zone is pinned to \
                     '+00:00', so TIMESTAMP values cross as UTC instants (use CONVERT_TZ in the \
                     query for local times)"
                );
            }
            init.push(format!("SET SESSION {k} = {v}"));
        }
    }
    // TIMESTAMP columns are read and written in the session time zone: pin it
    // to UTC so they cross as instants (DATETIME is unaffected).
    init.push("SET time_zone = '+00:00'".to_string());

    let mut builder = OptsBuilder::default()
        .ip_or_hostname(cfg.host.clone())
        .tcp_port(cfg.port)
        .user(Some(cfg.user.clone()))
        .pass(Some(cfg.password.clone()))
        .db_name(Some(cfg.dbname.clone()))
        .init(init);

    if let Some(s) = &cfg.tcp_keepalive {
        builder = builder.tcp_keepalive(Some(parse_duration(s).context("invalid tcp_keepalive")?));
    }

    if let Some(tls) = &cfg.tls {
        if let Some(ssl) = ssl_opts(tls)? {
            builder = builder.ssl_opts(ssl);
        }
    }

    Ok(Opts::from(builder))
}

/// Connects to MySQL with the configured TLS mode and session setup.
/// One `connect` span covers TCP, TLS, authentication and the init
/// statements.
pub async fn connect(cfg: &ConnectionConfig) -> Result<Conn> {
    let opts = build_opts(cfg)?;
    if !loadsmith_plugin_sdk::telemetry::enabled() {
        return Conn::new(opts).await.context("mysql connect failed");
    }
    let tls_mode = cfg.tls.as_ref().map(|t| t.mode).unwrap_or_default().as_str();
    let span = tracing::info_span!(
        "connect",
        otel.kind = "client",
        db.system.name = crate::telemetry::SYSTEM,
        db.namespace = cfg.dbname.as_str(),
        server.address = cfg.host.as_str(),
        server.port = cfg.port,
        loadsmith.tls.mode = tls_mode,
        error = tracing::field::Empty,
    );
    let result = tracing::Instrument::instrument(Conn::new(opts), span.clone()).await.context("mysql connect failed");
    if let Err(e) = &result {
        span.record("error", format!("{e:#}"));
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_duration_units() {
        assert_eq!(parse_duration("10s").unwrap(), Duration::from_secs(10));
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_duration("2m").unwrap(), Duration::from_secs(120));
        assert!(parse_duration("100").is_err());
        assert!(parse_duration("10y").is_err());
    }

    #[test]
    fn config_minimal_defaults() {
        let json = serde_json::json!({
            "dbname": "lab", "user": "lab", "password": "lab"
        });
        let cfg: ConnectionConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.host, "localhost");
        assert_eq!(cfg.port, 3306);
        assert!(cfg.tls.is_none());
    }

    #[test]
    fn ssl_disable_is_none() {
        let cfg = TlsBlock { mode: TlsMode::Disable, ..Default::default() };
        assert!(ssl_opts(&cfg).unwrap().is_none());
    }

    #[test]
    fn ssl_require_is_some() {
        let cfg = TlsBlock { mode: TlsMode::Require, ..Default::default() };
        assert!(ssl_opts(&cfg).unwrap().is_some());
    }

    #[test]
    fn ssl_prefer_is_rejected() {
        let cfg = TlsBlock { mode: TlsMode::Prefer, ..Default::default() };
        assert!(ssl_opts(&cfg).unwrap_err().to_string().contains("`prefer` is not supported"));
    }

    #[test]
    fn session_time_zone_is_pinned() {
        let json = serde_json::json!({ "dbname": "d", "user": "u", "password": "p", "session": { "time_zone": "'+02:00'" } });
        let cfg: ConnectionConfig = serde_json::from_value(json).unwrap();
        assert!(build_opts(&cfg).unwrap_err().to_string().contains("pinned"));
        let json = serde_json::json!({ "dbname": "d", "user": "u", "password": "p" });
        let opts = build_opts(&serde_json::from_value(json).unwrap()).unwrap();
        assert!(opts.init().iter().any(|s| s == "SET time_zone = '+00:00'"));
    }

    #[test]
    fn ssl_verify_ca_requires_root_cert() {
        let cfg = TlsBlock { mode: TlsMode::VerifyCa, ..Default::default() };
        assert!(ssl_opts(&cfg).is_err());
    }

    #[test]
    fn ssl_client_identity_requires_both() {
        let cfg = TlsBlock {
            mode: TlsMode::Require,
            client_cert: Some("PEM".into()),
            ..Default::default()
        };
        assert!(ssl_opts(&cfg).is_err());
    }
}
