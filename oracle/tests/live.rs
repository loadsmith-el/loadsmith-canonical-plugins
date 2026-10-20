//! Live integration tests against a real Oracle (pure-Rust thin driver — no
//! Instant Client needed).
//!
//! Ignored by default (they need a reachable Oracle). Run against the
//! `lab-oracle` fixture:
//!
//! ```bash
//! # start the fixture (canonical images repo) and export its endpoint:
//! export ORACLE_TEST_DSN="//localhost:1521/FREEPDB1"
//! export ORACLE_TEST_USER=lab ORACLE_TEST_PASSWORD=lab
//! cargo test -p loadsmith-oracle --test live -- --ignored --nocapture
//! ```
//!
//! The headline assertion is `reads_full_wide_table`: the full 34-column,
//! 100k-row `SELECT *` that the community `oracle-rs` driver truncated (multi-
//! packet responses). The driver must read every row.

use std::env;

use loadsmith_oracle::OracleSourcePlugin;
use loadsmith_plugin_sdk::SourcePlugin;

fn dsn() -> Option<(String, String, String)> {
    Some((
        env::var("ORACLE_TEST_DSN").ok()?,
        env::var("ORACLE_TEST_USER").unwrap_or_else(|_| "lab".into()),
        env::var("ORACLE_TEST_PASSWORD").unwrap_or_else(|_| "lab".into()),
    ))
}

/// Builds a source config from `ORACLE_TEST_*` + a query.
fn source_config(query: &str) -> serde_json::Value {
    let (dsn, user, password) = dsn().expect("ORACLE_TEST_DSN must be set for live tests");
    // DSN form "//host:port/service" → host/port/service_name fields.
    let rest = dsn.trim_start_matches("//");
    let (hostport, service) = rest.split_once('/').expect("DSN must be //host:port/service");
    let (host, port) = hostport.split_once(':').expect("DSN host:port");
    serde_json::json!({
        "host": host,
        "port": port.parse::<u16>().unwrap(),
        "service_name": service,
        "user": user,
        "password": password,
        "query": query,
        "batch_size": 2000,
    })
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a live Oracle"]
async fn reads_full_wide_table() {
    let mut src = OracleSourcePlugin::new();
    src.configure(loadsmith_plugin_sdk::PluginConfig::new(source_config("SELECT * FROM spacecraft_telemetry_events"), loadsmith_plugin_sdk::UnknownKeys::Error))
        .await
        .expect("configure");

    let mut total = 0usize;
    while let Some(batch) = src.next_batch().await.expect("next_batch") {
        total += batch.num_rows();
    }
    // The exact case oracle-rs failed: a wide (34-col) full-table read.
    assert_eq!(total, 100_000, "expected the full seeded table, got {total}");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a live Oracle"]
async fn schema_has_all_columns() {
    let mut src = OracleSourcePlugin::new();
    src.configure(loadsmith_plugin_sdk::PluginConfig::new(source_config("SELECT * FROM spacecraft_telemetry_events"), loadsmith_plugin_sdk::UnknownKeys::Error))
        .await
        .expect("configure");
    let schema = src.schema().await.expect("schema");
    assert_eq!(schema.fields().len(), 34, "expected the 34-column telemetry schema");
}
