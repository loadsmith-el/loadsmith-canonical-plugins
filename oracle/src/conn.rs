//! Shared Oracle connection config + `connect()` for the pure-Rust `oracledb`
//! thin driver.
//!
//! Both the source and destination embed [`ConnectionConfig`] (via
//! `#[serde(flatten)]`) and call [`connect`], so the connection surface — the
//! TNS descriptor, connect timeout, and TLS — lives in one place.
//!
//! **TLS** takes the canon's shared `tls` block shape (`mode` / `root_cert` /
//! `client_cert` / `client_key`, as in postgres and mysql) and maps it onto
//! what the thin driver can do. The driver always verifies the chain AND the
//! host name, and only reads a trusted CA from a file (`<dir>/ewallet.pem`), so:
//! - `disable` → plaintext `PROTOCOL=TCP`;
//! - `verify-full` → `PROTOCOL=TCPS`; an inline `root_cert` is written to a
//!   private temp dir as `ewallet.pem` for the connect and removed right after
//!   (without `root_cert`, the public webpki roots apply);
//! - `prefer` / `require` / `verify-ca` and mTLS are rejected with a clear error:
//!   the driver can't skip verification, and can't combine a client identity
//!   with a private CA yet (oracle/rust-oracledb#54).
//!
//! Crypto is the canon's pure-Rust rustls-rustcrypto provider, installed
//! process-wide by [`connect`] via `loadsmith-tls`.
//!
//! **Text↔value conversions are kept NLS-independent at the SQL level** (the
//! source reads temporals natively and renders CLOB/INTERVAL columns with
//! `TO_CHAR`; the destination and the incremental watermark wrap temporal text
//! in `TO_DATE`/`TO_TIMESTAMP`/`TO_TIMESTAMP_TZ`). Doing the conversion in SQL
//! keeps the exact canonical text format regardless of session NLS.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use crate::vocab::TlsMode;
use oracledb::Connection;
use serde::Deserialize;

// ── Canonical text formats (read by `TO_*`: destination binds, watermarks) ─────

/// `DATE` ⇄ text. Oracle DATE always carries a time component.
pub const FMT_DATE: &str = "YYYY-MM-DD HH24:MI:SS";
/// Text → `TIMESTAMP` (`TO_TIMESTAMP`): `FF` reads 0 to 9 fraction digits.
pub const FMT_TIMESTAMP_IN: &str = "YYYY-MM-DD HH24:MI:SS.FF";
/// Text → `TIMESTAMP WITH TIME ZONE`.
pub const FMT_TIMESTAMP_TZ_IN: &str = "YYYY-MM-DD HH24:MI:SS.FF TZH:TZM";

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

/// The canon's shared `tls` block (same shape as `loadsmith_tls::TlsConfig`).
/// A stale pre-0.3 block (`enabled`/`wallet_dir`) has only unknown keys, so
/// `PluginConfig::parse` rejects it (by default) instead of it silently
/// reading as `mode: disable` (plaintext).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct TlsConfig {
    #[serde(default)]
    pub mode: TlsMode,
    /// Trusted CA certificate(s), inline PEM. Optional for `verify-full`: without
    /// it, the server must chain to a public (webpki) root.
    #[serde(default)]
    pub root_cert: Option<String>,
    /// mTLS client certificate (inline PEM). Not supported by the driver yet.
    #[serde(default)]
    pub client_cert: Option<String>,
    /// mTLS client key (inline PEM). Not supported by the driver yet.
    #[serde(default)]
    pub client_key: Option<String>,
}

impl TlsConfig {
    /// Rejects what the thin driver can't honour. Returns whether TLS is on.
    fn validate(&self) -> Result<bool> {
        if self.client_cert.is_some() || self.client_key.is_some() {
            bail!(
                "oracle tls: client_cert/client_key (mTLS) are not supported yet by the \
                 oracledb thin driver (oracle/rust-oracledb#54)"
            );
        }
        match self.mode {
            TlsMode::Disable => Ok(false),
            TlsMode::VerifyFull => {
                if let Some(pem) = &self.root_cert {
                    if !pem.contains("-----BEGIN CERTIFICATE-----") {
                        bail!("oracle tls: root_cert contains no PEM certificate");
                    }
                    // The driver treats an ewallet.pem holding a private key as a
                    // client identity, not as trust roots, so never write one.
                    if pem.contains("PRIVATE KEY") {
                        bail!("oracle tls: root_cert must hold only certificates, not a private key");
                    }
                }
                Ok(true)
            }
            TlsMode::Prefer | TlsMode::Require | TlsMode::VerifyCa => bail!(
                "oracle tls: mode `{}` is not supported: the oracledb thin driver always \
                 verifies the certificate chain and host name. Use `verify-full` (with \
                 `root_cert` for a private CA) or `disable`",
                self.mode.as_str()
            ),
        }
    }
}

/// A private, single-use directory holding `ewallet.pem` (the inline
/// `root_cert`), removed on drop. The driver only reads trust roots from a file.
struct TempWallet {
    dir: PathBuf,
}

impl TempWallet {
    fn create(root_cert_pem: &str) -> Result<Self> {
        use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
        use std::io::Write;
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let dir = std::env::temp_dir()
            .join(format!("loadsmith-oracle-wallet-{}-{nanos}", std::process::id()));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&dir)
            .with_context(|| format!("creating temp wallet dir {}", dir.display()))?;
        let wallet = TempWallet { dir };
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(wallet.dir.join("ewallet.pem"))
            .context("writing temp ewallet.pem")?;
        f.write_all(root_cert_pem.as_bytes()).context("writing temp ewallet.pem")?;
        Ok(wallet)
    }

    fn location(&self) -> String {
        self.dir.to_string_lossy().into_owned()
    }
}

impl Drop for TempWallet {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn default_host() -> String {
    "localhost".to_string()
}
fn default_port() -> u16 {
    1521
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
/// Plaintext uses `PROTOCOL=TCP`; TLS (`mode: verify-full`) uses `PROTOCOL=TCPS`.
/// The trust roots go to the driver through `set_wallet_location`, not here.
pub fn build_descriptor(cfg: &ConnectionConfig) -> Result<String> {
    let connect_data = match (&cfg.service_name, &cfg.sid) {
        (Some(svc), _) => format!("(SERVICE_NAME={svc})"),
        (None, Some(sid)) => format!("(SID={sid})"),
        (None, None) => bail!("oracle config requires either service_name or sid"),
    };

    let tls_on = match &cfg.tls {
        Some(tls) => tls.validate()?,
        None => false,
    };
    let protocol = if tls_on { "TCPS" } else { "TCP" };

    let timeout = match &cfg.connect_timeout {
        Some(s) => parse_duration(s).context("invalid connect_timeout")?.as_secs().max(1),
        None => 10,
    };

    Ok(format!(
        "(DESCRIPTION=(CONNECT_TIMEOUT={timeout})\
         (ADDRESS=(PROTOCOL={protocol})(HOST={host})(PORT={port}))\
         (CONNECT_DATA={connect_data}))",
        host = cfg.host,
        port = cfg.port,
    ))
}

/// Connects to Oracle through the thin driver using the built descriptor.
///
/// Synchronous; call inside [`tokio::task::block_in_place`].
pub fn connect(cfg: &ConnectionConfig) -> Result<Connection> {
    // One `connect` span over TCP, TLS, authentication and the session setup.
    if !loadsmith_plugin_sdk::telemetry::enabled() {
        return connect_inner(cfg);
    }
    let tls_mode = cfg.tls.as_ref().map(|t| t.mode).unwrap_or_default().as_str();
    let span = tracing::info_span!(
        "connect",
        otel.kind = "client",
        db.system.name = crate::telemetry::SYSTEM,
        db.namespace = namespace(cfg),
        server.address = cfg.host.as_str(),
        server.port = cfg.port,
        loadsmith.tls.mode = tls_mode,
        error = tracing::field::Empty,
    );
    let result = span.in_scope(|| connect_inner(cfg));
    if let Err(e) = &result {
        span.record("error", format!("{e:#}"));
    }
    result
}

/// `db.namespace` of the telemetry spans: the service name, else the SID.
pub fn namespace(cfg: &ConnectionConfig) -> &str {
    cfg.service_name.as_deref().or(cfg.sid.as_deref()).unwrap_or_default()
}

fn connect_inner(cfg: &ConnectionConfig) -> Result<Connection> {
    // The driver builds its rustls config from the process-default provider;
    // make that the canon's pure-Rust rustls-rustcrypto (idempotent).
    loadsmith_tls::install_provider();
    let descriptor = build_descriptor(cfg)?;
    let mut config = oracledb::Config::default()
        .set_credentials(&cfg.user, &cfg.password)
        .set_connect_string(&descriptor)
        .map_err(|e| anyhow::anyhow!("invalid oracle connect descriptor: {e}"))?;
    // build_descriptor validated the block: a root_cert only matters for
    // verify-full. The driver reads it at connect time; the temp dir is removed
    // when `_wallet` drops at the end of this function.
    let _wallet = match cfg.tls.as_ref().filter(|t| t.mode == TlsMode::VerifyFull) {
        Some(TlsConfig { root_cert: Some(pem), .. }) => {
            let wallet = TempWallet::create(pem)?;
            config = config.set_wallet_location(wallet.location());
            Some(wallet)
        }
        _ => None,
    };
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

    fn tls_cfg(tls: serde_json::Value) -> Result<String> {
        let c: ConnectionConfig = serde_json::from_value(serde_json::json!({
            "service_name": "FREEPDB1", "user": "lab", "password": "lab", "tls": tls
        }))?;
        build_descriptor(&c)
    }

    const PEM: &str = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n";

    #[test]
    fn tls_verify_full_uses_tcps() {
        let d = tls_cfg(serde_json::json!({ "mode": "verify-full", "root_cert": PEM })).unwrap();
        assert!(d.contains("(PROTOCOL=TCPS)"));
        assert!(!d.contains("SECURITY"));
        // root_cert is optional (public webpki roots).
        assert!(tls_cfg(serde_json::json!({ "mode": "verify-full" })).is_ok());
    }

    #[test]
    fn tls_disable_is_plaintext() {
        let d = tls_cfg(serde_json::json!({ "mode": "disable" })).unwrap();
        assert!(d.contains("(PROTOCOL=TCP)"));
    }

    #[test]
    fn tls_rejects_modes_the_driver_cannot_honour() {
        for mode in ["prefer", "require", "verify-ca"] {
            let err = tls_cfg(serde_json::json!({ "mode": mode })).unwrap_err().to_string();
            assert!(err.contains("not supported"), "{mode}: {err}");
        }
    }

    #[test]
    fn tls_rejects_mtls_and_bad_root_cert() {
        assert!(tls_cfg(serde_json::json!({
            "mode": "verify-full", "client_cert": PEM, "client_key": "k"
        }))
        .is_err());
        assert!(tls_cfg(serde_json::json!({ "mode": "verify-full", "root_cert": "nope" })).is_err());
        let with_key = format!("{PEM}-----BEGIN PRIVATE KEY-----\nx\n-----END PRIVATE KEY-----\n");
        assert!(tls_cfg(serde_json::json!({ "mode": "verify-full", "root_cert": with_key })).is_err());
    }

    #[test]
    fn tls_rejects_the_pre_0_3_block() {
        // A stale `enabled`/`wallet_dir` block must fail, not silently read as
        // `mode: disable` (plaintext).
        let block = serde_json::json!({
            "service_name": "FREEPDB1", "user": "lab", "password": "lab",
            "tls": { "enabled": true, "wallet_dir": "/wallet" }
        });
        let err = loadsmith_plugin_sdk::PluginConfig::new(block, loadsmith_plugin_sdk::UnknownKeys::Error)
            .parse::<ConnectionConfig>()
            .unwrap_err()
            .to_string();
        assert!(err.contains("tls.enabled") && err.contains("tls.wallet_dir"), "{err}");
    }

    #[test]
    fn temp_wallet_is_private_and_removed() {
        use std::os::unix::fs::PermissionsExt;
        let w = TempWallet::create(PEM).unwrap();
        let dir = w.dir.clone();
        let file = dir.join("ewallet.pem");
        assert_eq!(std::fs::read_to_string(&file).unwrap(), PEM);
        assert_eq!(std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777, 0o700);
        assert_eq!(std::fs::metadata(&file).unwrap().permissions().mode() & 0o777, 0o600);
        drop(w);
        assert!(!dir.exists());
    }
}
