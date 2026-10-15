//! Shared Oracle connection config + `connect()` for the pure-Rust `oracledb`
//! thin driver.
//!
//! Both the source and destination embed [`ConnectionConfig`] (via
//! `#[serde(flatten)]`) and call [`connect`], so the connection surface — the
//! TNS descriptor, connect timeout, and TLS — lives in one place.
//!
//! **TLS** goes through rustls inside the driver. The server certificate is
//! verified against the webpki roots plus every certificate in
//! `<wallet_dir>/ewallet.pem` — a *PEM wallet*: put your private CA there (a
//! plain PEM CA file renamed to `ewallet.pem` works; no `orapki`). A binary
//! `cwallet.sso` is NOT read. The host name is always verified against the
//! certificate SAN (standard TLS), so `server_dn_match` / `distinguished_name`
//! are accepted for config compatibility but not enforced by the driver yet
//! (oracle/rust-oracledb#25). Crypto is the canon's pure-Rust rustls-rustcrypto
//! provider, installed process-wide by [`connect`] via `loadsmith-tls`.
//!
//! **Text↔value conversions are kept NLS-independent at the SQL level** (the
//! source renders temporal/CLOB columns with `TO_CHAR`, the destination wraps
//! temporal binds in `TO_DATE`/`TO_TIMESTAMP`/`TO_TIMESTAMP_TZ`). Doing the
//! conversion in SQL keeps the exact canonical text format regardless of session
//! NLS — and matches the connector's text-preserving contract with the rest of
//! Loadsmith.

use std::time::Duration;

use anyhow::{bail, Context, Result};
use oracledb::Connection;
use serde::Deserialize;

// ── Canonical text formats (shared by source TO_CHAR and destination TO_*) ─────

/// `DATE` → text. Oracle DATE always carries a time component.
pub const FMT_DATE: &str = "YYYY-MM-DD HH24:MI:SS";
/// `TIMESTAMP` → text, microsecond precision (canonical data is µs).
pub const FMT_TIMESTAMP: &str = "YYYY-MM-DD HH24:MI:SS.FF6";
/// `TIMESTAMP WITH [LOCAL] TIME ZONE` → text, with explicit offset.
pub const FMT_TIMESTAMP_TZ: &str = "YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM";

// ── Config structs ─────────────────────────────────────────────────────────────

/// Connection-level config shared by both oracle plugins. Embed it with
/// `#[serde(flatten)]` alongside plugin-specific fields (query/target_table/…).
#[derive(Debug, Deserialize)]
pub struct ConnectionConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    /// Oracle service name (e.g. `FREEPDB1`). Either this or [`Self::sid`].
    #[serde(default)]
    pub service_name: Option<String>,
    /// Legacy SID, as an alternative to `service_name`.
    #[serde(default)]
    pub sid: Option<String>,
    pub user: String,
    pub password: String,

    #[serde(default)]
    pub tls: Option<TlsConfig>,

    /// Connection establishment timeout ("10s", "500ms"); default 10s.
    #[serde(default)]
    pub connect_timeout: Option<String>,
}

/// TLS (TCPS). When `enabled`, the connector uses `PROTOCOL=TCPS`; the server is
/// verified against the webpki roots plus the PEM wallet at [`Self::wallet_dir`].
#[derive(Debug, Clone, Deserialize)]
pub struct TlsConfig {
    /// Turn on TCPS. When false the block is a no-op (plaintext TCP).
    #[serde(default)]
    pub enabled: bool,
    /// Directory holding `ewallet.pem` — the trusted (private) CA certificate(s)
    /// in PEM. Not needed when the server cert chains to a public (webpki) root.
    #[serde(default)]
    pub wallet_dir: Option<String>,
    /// `SSL_SERVER_DN_MATCH`. Accepted for compatibility; the thin driver always
    /// verifies the host name against the certificate SAN (see module docs).
    #[serde(default = "default_true")]
    pub server_dn_match: bool,
    /// Expected server distinguished name (`SSL_SERVER_CERT_DN`). Accepted for
    /// compatibility; not enforced by the thin driver yet.
    #[serde(default)]
    pub distinguished_name: Option<String>,
}

fn default_host() -> String {
    "localhost".to_string()
}
fn default_port() -> u16 {
    1521
}
fn default_true() -> bool {
    true
}

// ── Duration parsing ──────────────────────────────────────────────────────────

/// Parses a human-readable duration string into a `Duration`.
/// Accepted suffixes: `ms`, `s`, `m`, `h`, `d`.
pub fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    // Check "ms" before "s" to avoid stripping just the trailing 's' from "10ms".
    if let Some(n) = s.strip_suffix("ms") {
        return Ok(Duration::from_millis(n.trim().parse::<u64>().map_err(|_| dur_err(s))?));
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

// ── Connect descriptor ─────────────────────────────────────────────────────────

/// Builds the TNS connect descriptor string passed to `oracledb::Config::set_connect_string`.
/// Plaintext uses `PROTOCOL=TCP`; TLS uses `PROTOCOL=TCPS` plus a `SECURITY`
/// section (wallet directory + DN matching).
pub fn build_descriptor(cfg: &ConnectionConfig) -> Result<String> {
    let connect_data = match (&cfg.service_name, &cfg.sid) {
        (Some(svc), _) => format!("(SERVICE_NAME={svc})"),
        (None, Some(sid)) => format!("(SID={sid})"),
        (None, None) => bail!("oracle config requires either service_name or sid"),
    };

    let tls_on = cfg.tls.as_ref().map(|t| t.enabled).unwrap_or(false);
    let protocol = if tls_on { "TCPS" } else { "TCP" };

    let timeout = match &cfg.connect_timeout {
        Some(s) => parse_duration(s).context("invalid connect_timeout")?.as_secs().max(1),
        None => 10,
    };

    let security = if tls_on {
        let tls = cfg.tls.as_ref().unwrap();
        let mut parts = String::new();
        parts.push_str(if tls.server_dn_match {
            "(SSL_SERVER_DN_MATCH=yes)"
        } else {
            "(SSL_SERVER_DN_MATCH=no)"
        });
        if let Some(dn) = &tls.distinguished_name {
            parts.push_str(&format!("(SSL_SERVER_CERT_DN=\"{dn}\")"));
        }
        if let Some(dir) = &tls.wallet_dir {
            parts.push_str(&format!("(MY_WALLET_DIRECTORY={dir})"));
        }
        format!("(SECURITY={parts})")
    } else {
        String::new()
    };

    Ok(format!(
        "(DESCRIPTION=(CONNECT_TIMEOUT={timeout})\
         (ADDRESS=(PROTOCOL={protocol})(HOST={host})(PORT={port}))\
         (CONNECT_DATA={connect_data}){security})",
        host = cfg.host,
        port = cfg.port,
    ))
}

/// Connects to Oracle through the thin driver using the built descriptor.
///
/// Synchronous; call inside [`tokio::task::block_in_place`].
pub fn connect(cfg: &ConnectionConfig) -> Result<Connection> {
    // The driver builds its rustls config from the process-default provider;
    // make that the canon's pure-Rust rustls-rustcrypto (idempotent).
    loadsmith_tls::install_provider();
    let descriptor = build_descriptor(cfg)?;
    let mut config = oracledb::Config::default()
        .set_credentials(&cfg.user, &cfg.password)
        .set_connect_string(&descriptor)
        .map_err(|e| anyhow::anyhow!("invalid oracle connect descriptor: {e}"))?;
    if let Some(dir) = cfg.tls.as_ref().filter(|t| t.enabled).and_then(|t| t.wallet_dir.as_ref()) {
        config = config.set_wallet_location(dir.as_str());
    }
    let conn = oracledb::connect(config).map_err(|e| anyhow::anyhow!("oracle connect failed: {e}"))?;
    // Numbers cross the connector as text, so pin the decimal/group characters on
    // every session. Otherwise a comma-decimal locale (e.g. NLS_LANG=GERMAN…)
    // makes the source render `3.14` as `3,14` and/or the destination reject the
    // `.`-formatted values we bind (ORA-01722). `.` decimal + `,` group is the
    // canonical form the read/write paths assume. (Temporal columns already use
    // explicit TO_* formats and are unaffected.)
    conn.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'", &[])
        .map_err(|e| anyhow::anyhow!("pinning NLS_NUMERIC_CHARACTERS failed: {e}"))?;
    Ok(conn)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(json: serde_json::Value) -> ConnectionConfig {
        serde_json::from_value(json).unwrap()
    }

    #[test]
    fn parse_duration_units() {
        assert_eq!(parse_duration("10s").unwrap(), Duration::from_secs(10));
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_duration("2m").unwrap(), Duration::from_secs(120));
        assert!(parse_duration("100").is_err());
        assert!(parse_duration("10y").is_err());
    }

    #[test]
    fn descriptor_plaintext_service() {
        let c = cfg(serde_json::json!({
            "service_name": "FREEPDB1", "user": "lab", "password": "lab"
        }));
        let d = build_descriptor(&c).unwrap();
        assert!(d.contains("(PROTOCOL=TCP)"));
        assert!(d.contains("(SERVICE_NAME=FREEPDB1)"));
        assert!(d.contains("(PORT=1521)"));
        assert!(!d.contains("SECURITY"));
        assert!(d.contains("(CONNECT_TIMEOUT=10)"));
    }

    #[test]
    fn descriptor_requires_service_or_sid() {
        let c = cfg(serde_json::json!({ "user": "lab", "password": "lab" }));
        assert!(build_descriptor(&c).is_err());
    }

    #[test]
    fn descriptor_tls_uses_tcps_and_wallet() {
        let c = cfg(serde_json::json!({
            "service_name": "FREEPDB1", "user": "lab", "password": "lab",
            "tls": { "enabled": true, "wallet_dir": "/wallet", "server_dn_match": false }
        }));
        let d = build_descriptor(&c).unwrap();
        assert!(d.contains("(PROTOCOL=TCPS)"));
        assert!(d.contains("(MY_WALLET_DIRECTORY=/wallet)"));
        assert!(d.contains("(SSL_SERVER_DN_MATCH=no)"));
    }
}
