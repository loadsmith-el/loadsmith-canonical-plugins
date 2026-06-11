//! Shared postgres connection config + `connect()`.
//!
//! Both the source and destination plugins embed [`ConnectionConfig`] (via
//! `#[serde(flatten)]`) and call [`connect`], so the full connection surface —
//! multi-host, TLS (all modes + mTLS), channel binding, keepalives, timeouts,
//! and session GUCs — lives in exactly one place. Generic TLS comes from
//! `loadsmith-tls`; this module only maps it onto the postgres driver.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use loadsmith_tls::{TlsConfig, TlsMode};
use serde::Deserialize;
use tokio_postgres::config::SslMode;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, NoTls};
use tokio_postgres_rustls_improved::MakeRustlsConnect;

// ── Config structs ──────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChannelBinding {
    Disable,
    Prefer,
    Require,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TargetSessionAttrs {
    Any,
    ReadWrite,
    ReadOnly,
}

#[derive(Debug, Deserialize)]
pub struct TcpKeepalives {
    pub idle: Option<String>,
    pub interval: Option<String>,
    pub retries: Option<u32>,
}

/// Accepts either a single host string or a list of hosts for multi-node clusters.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum HostSpec {
    One(String),
    Many(Vec<String>),
}

impl Default for HostSpec {
    fn default() -> Self {
        HostSpec::One("localhost".into())
    }
}

/// The connection-level config shared by both postgres plugins. Embed it with
/// `#[serde(flatten)]` alongside plugin-specific fields (query/table/etc.).
#[derive(Debug, Deserialize)]
pub struct ConnectionConfig {
    #[serde(default)]
    pub host: HostSpec,
    #[serde(default = "default_port")]
    pub port: u16,
    pub dbname: String,
    pub user: String,
    pub password: String,

    pub application_name: Option<String>,
    /// Duration string: "10s", "500ms", "2m", "1h".
    pub connect_timeout: Option<String>,
    /// Duration string; converted to milliseconds for Postgres `statement_timeout`.
    pub statement_timeout: Option<String>,
    /// Extra GUC parameters passed via the startup-message `options` field.
    pub options: Option<HashMap<String, String>>,

    pub tls: Option<TlsConfig>,
    pub channel_binding: Option<ChannelBinding>,
    pub target_session_attrs: Option<TargetSessionAttrs>,

    /// GUC parameters applied via `SELECT set_config($1, $2, false)` after connecting.
    pub session: Option<HashMap<String, String>>,
    pub tcp_keepalives: Option<TcpKeepalives>,
}

fn default_port() -> u16 {
    5432
}

// ── Duration parsing ────────────────────────────────────────────────────────

/// Parses a human-readable duration string into a `Duration`.
/// Accepted suffixes: `ms`, `s`, `m`, `h`, `d`.
/// Examples: `"10s"`, `"30m"`, `"500ms"`, `"2h"`, `"1d"`.
pub fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    // Check "ms" before "s" to avoid stripping just the trailing 's' from "10ms".
    if let Some(n) = s.strip_suffix("ms") {
        let ms = n
            .trim()
            .parse::<u64>()
            .map_err(|_| anyhow::anyhow!("invalid duration '{s}': expected integer before 'ms'"))?;
        return Ok(Duration::from_millis(ms));
    }
    if let Some(n) = s.strip_suffix('s') {
        let secs = n
            .trim()
            .parse::<u64>()
            .map_err(|_| anyhow::anyhow!("invalid duration '{s}': expected integer before 's'"))?;
        return Ok(Duration::from_secs(secs));
    }
    if let Some(n) = s.strip_suffix('m') {
        let mins = n
            .trim()
            .parse::<u64>()
            .map_err(|_| anyhow::anyhow!("invalid duration '{s}': expected integer before 'm'"))?;
        return Ok(Duration::from_secs(mins * 60));
    }
    if let Some(n) = s.strip_suffix('h') {
        let hours = n
            .trim()
            .parse::<u64>()
            .map_err(|_| anyhow::anyhow!("invalid duration '{s}': expected integer before 'h'"))?;
        return Ok(Duration::from_secs(hours * 3600));
    }
    if let Some(n) = s.strip_suffix('d') {
        let days = n
            .trim()
            .parse::<u64>()
            .map_err(|_| anyhow::anyhow!("invalid duration '{s}': expected integer before 'd'"))?;
        return Ok(Duration::from_secs(days * 86400));
    }
    Err(anyhow::anyhow!(
        "invalid duration '{s}': expected a number followed by ms/s/m/h/d \
         (e.g. '10s', '30m', '500ms', '2h')"
    ))
}

// ── Connect ─────────────────────────────────────────────────────────────────

/// Connects to Postgres over the configured TLS mode and applies post-connect
/// session GUCs. Returns a live `Client`; the connection task is spawned
/// internally. The caller owns transactions/cursors/queries.
pub async fn connect(cfg: &ConnectionConfig) -> Result<Client> {
    // ── Build tokio_postgres::Config ───────────────────────────────────────
    let mut pg = tokio_postgres::Config::new();

    match &cfg.host {
        HostSpec::One(h) => {
            pg.host(h).port(cfg.port);
        }
        HostSpec::Many(hosts) => {
            for h in hosts {
                pg.host(h).port(cfg.port);
            }
        }
    }

    pg.dbname(&cfg.dbname).user(&cfg.user).password(cfg.password.as_str());

    if let Some(name) = &cfg.application_name {
        pg.application_name(name);
    }

    if let Some(s) = &cfg.connect_timeout {
        pg.connect_timeout(parse_duration(s).context("invalid connect_timeout")?);
    }

    if let Some(opts) = &cfg.options {
        let s: String = opts
            .iter()
            .map(|(k, v)| format!("-c {k}={v}"))
            .collect::<Vec<_>>()
            .join(" ");
        pg.options(&s);
    }

    if let Some(cb) = &cfg.channel_binding {
        pg.channel_binding(match cb {
            ChannelBinding::Disable => tokio_postgres::config::ChannelBinding::Disable,
            ChannelBinding::Prefer => tokio_postgres::config::ChannelBinding::Prefer,
            ChannelBinding::Require => tokio_postgres::config::ChannelBinding::Require,
        });
    }

    if let Some(tsa) = &cfg.target_session_attrs {
        pg.target_session_attrs(match tsa {
            TargetSessionAttrs::Any => tokio_postgres::config::TargetSessionAttrs::Any,
            TargetSessionAttrs::ReadWrite => tokio_postgres::config::TargetSessionAttrs::ReadWrite,
            TargetSessionAttrs::ReadOnly => tokio_postgres::config::TargetSessionAttrs::ReadOnly,
        });
    }

    if let Some(ka) = &cfg.tcp_keepalives {
        pg.keepalives(true);
        if let Some(s) = &ka.idle {
            pg.keepalives_idle(parse_duration(s).context("invalid tcp_keepalives.idle")?);
        }
        if let Some(s) = &ka.interval {
            pg.keepalives_interval(parse_duration(s).context("invalid tcp_keepalives.interval")?);
        }
        if let Some(n) = ka.retries {
            pg.keepalives_retries(n);
        }
    }

    // ── Connect with TLS ───────────────────────────────────────────────────
    // loadsmith-tls validates the config and builds the generic rustls
    // ClientConfig; here we just map the mode to postgres' SslMode and wrap the
    // config in the postgres driver's connector.
    let tls_cfg = cfg.tls.clone().unwrap_or_default();
    pg.ssl_mode(match tls_cfg.mode {
        TlsMode::Disable => SslMode::Disable,
        TlsMode::Prefer => SslMode::Prefer,
        TlsMode::Require | TlsMode::VerifyCa | TlsMode::VerifyFull => SslMode::Require,
    });

    // Each arm spawns its own connection task (types differ per arm) and returns Client.
    macro_rules! connect {
        ($connector:expr) => {{
            let (c, conn) = pg.connect($connector).await.context("postgres connect failed")?;
            tokio::spawn(async move {
                if let Err(e) = conn.await {
                    eprintln!("[postgres] connection task error: {e}");
                }
            });
            c
        }};
    }

    let client: Client = match loadsmith_tls::client_config(&tls_cfg)? {
        None => connect!(NoTls),
        Some(config) => connect!(MakeRustlsConnect::new(config)),
    };

    // ── Post-connect: session GUC parameters ──────────────────────────────
    if let Some(session) = &cfg.session {
        for (key, value) in session {
            client
                .execute(
                    "SELECT set_config($1, $2, false)",
                    &[
                        key as &(dyn ToSql + Sync),
                        value as &(dyn ToSql + Sync),
                    ],
                )
                .await
                .with_context(|| format!("set_config({key}) failed"))?;
        }
    }

    // statement_timeout → milliseconds (Postgres GUC_UNIT_MS)
    if let Some(s) = &cfg.statement_timeout {
        let ms = parse_duration(s).context("invalid statement_timeout")?.as_millis().to_string();
        client
            .execute(
                "SELECT set_config($1, $2, false)",
                &[
                    &"statement_timeout" as &(dyn ToSql + Sync),
                    &ms.as_str() as &(dyn ToSql + Sync),
                ],
            )
            .await
            .context("set statement_timeout failed")?;
    }

    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_duration_seconds() {
        assert_eq!(parse_duration("10s").unwrap(), Duration::from_secs(10));
        assert_eq!(parse_duration("0s").unwrap(), Duration::from_secs(0));
    }

    #[test]
    fn parse_duration_millis() {
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_duration("1000ms").unwrap(), Duration::from_secs(1));
    }

    #[test]
    fn parse_duration_minutes_hours_days() {
        assert_eq!(parse_duration("30m").unwrap(), Duration::from_secs(1800));
        assert_eq!(parse_duration("2h").unwrap(), Duration::from_secs(7200));
        assert_eq!(parse_duration("1d").unwrap(), Duration::from_secs(86400));
    }

    #[test]
    fn parse_duration_rejects_bad_input() {
        assert!(parse_duration("100").is_err());
        assert!(parse_duration("10y").is_err());
        assert!(parse_duration("abc").is_err());
    }

    #[test]
    fn parse_duration_ms_not_confused_with_s() {
        assert_eq!(parse_duration("10ms").unwrap(), Duration::from_millis(10));
    }

    #[test]
    fn connection_config_minimal() {
        let json = serde_json::json!({
            "host": "localhost", "dbname": "lab", "user": "lab", "password": "secret"
        });
        let cfg: ConnectionConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.dbname, "lab");
        assert_eq!(cfg.port, 5432);
        assert!(cfg.tls.is_none());
        assert!(matches!(cfg.host, HostSpec::One(ref h) if h == "localhost"));
    }

    #[test]
    fn connection_config_tls_and_multihost() {
        let json = serde_json::json!({
            "host": ["a", "b"], "dbname": "d", "user": "u", "password": "p",
            "tls": { "mode": "verify-full", "root_cert": "PEM" },
            "channel_binding": "require"
        });
        let cfg: ConnectionConfig = serde_json::from_value(json).unwrap();
        assert!(matches!(cfg.host, HostSpec::Many(ref v) if v.len() == 2));
        assert!(matches!(cfg.tls.as_ref().unwrap().mode, TlsMode::VerifyFull));
        assert!(matches!(cfg.channel_binding, Some(ChannelBinding::Require)));
    }
}
