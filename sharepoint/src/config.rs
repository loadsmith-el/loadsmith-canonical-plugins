//! Pipeline config for the SharePoint source — the serde mirror of the YAML the
//! user writes. Shapes follow the Python `sp-bulk` so existing pipelines port
//! over with minimal change.

use serde::Deserialize;

fn default_true() -> bool {
    true
}
fn default_filler() -> String {
    "_".to_string()
}
fn default_batch_size() -> usize {
    1000
}
fn default_delimiter() -> char {
    ','
}
fn default_graph_base() -> String {
    "https://graph.microsoft.com".to_string()
}
fn default_request_timeout() -> String {
    "60s".to_string()
}
fn default_max_retries() -> u32 {
    5
}
fn default_authority() -> String {
    "https://login.microsoftonline.com".to_string()
}

/// Parses a duration with a unit (`500ms`, `30s`, `2m`, `1h`, `1d`); a bare number
/// is rejected, like every duration in the canonical plugins.
pub fn parse_duration(s: &str) -> anyhow::Result<std::time::Duration> {
    use std::time::Duration;
    let t = s.trim();
    let err = || anyhow::anyhow!("invalid duration '{s}': expected a number followed by ms/s/m/h/d (e.g. '30s', '500ms')");
    let (n, mult_ms) = if let Some(n) = t.strip_suffix("ms") {
        (n, 1)
    } else if let Some(n) = t.strip_suffix('s') {
        (n, 1_000)
    } else if let Some(n) = t.strip_suffix('m') {
        (n, 60_000)
    } else if let Some(n) = t.strip_suffix('h') {
        (n, 3_600_000)
    } else if let Some(n) = t.strip_suffix('d') {
        (n, 86_400_000)
    } else {
        return Err(err());
    };
    let n: u64 = n.trim().parse().map_err(|_| err())?;
    Ok(Duration::from_millis(n.checked_mul(mult_ms).ok_or_else(err)?))
}

/// Top-level source config.
#[derive(Debug, Deserialize)]
pub struct SharepointConfig {
    pub tenant_id: String,
    pub client_id: String,
    pub client_secret: String,

    /// Site by hostname+path (e.g. `contoso.sharepoint.com/sites/Team`) — resolved
    /// to an id via Graph. Provide this OR `site_id`.
    pub site: Option<String>,
    pub site_id: Option<String>,
    /// Drive by display name — resolved via Graph. Provide this OR `drive_id`.
    pub drive: Option<String>,
    pub drive_id: Option<String>,

    /// Graph + login base URLs. Default to the public Microsoft endpoints; override
    /// for sovereign clouds (e.g. `graph.microsoft.us`) or to point tests at a mock.
    #[serde(default = "default_graph_base")]
    pub graph_base_url: String,
    #[serde(default = "default_authority")]
    pub authority_url: String,
    /// OAuth scope of the token. Default: `<graph_base_url origin>/.default`
    /// (`https://graph.microsoft.us/.default` for US Government L4), so a
    /// sovereign cloud needs only `graph_base_url` + `authority_url`.
    pub scope: Option<String>,

    /// Per-request HTTP timeout (a duration: `60s`, `2m`); a bare number is
    /// rejected.
    #[serde(default = "default_request_timeout")]
    pub request_timeout: String,
    /// Retries of a request answered `429 Too Many Requests` or `503 Service
    /// Unavailable` (or that timed out / failed to connect) before the run
    /// fails. `0` disables retrying.
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    pub input: InputConfig,

    #[serde(default)]
    pub transform: Vec<TransformConfig>,

    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

/// How data is obtained from the source: a file download (then parsed) or a
/// SharePoint List read.
#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum InputConfig {
    /// Download a file from the drive at `file` (a path like `/folder/sheet.xlsx`)
    /// and parse it.
    File { file: String, parser: ParserConfig },
    /// Read items from a SharePoint List, by display name or id.
    List { list: String },
}

/// Bytes → table parser (file mode only).
#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ParserConfig {
    Excel {
        sheet: Option<String>,
        #[serde(default = "default_true")]
        header: bool,
        #[serde(default)]
        skip_rows: usize,
    },
    Csv {
        #[serde(default = "default_delimiter")]
        delimiter: char,
        encoding: Option<String>,
        #[serde(default = "default_true")]
        header: bool,
    },
}

/// One transform step. The `kind` tag is the compiled "registry": each variant
/// maps to a `Transform` impl in [`crate::transform`].
#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum TransformConfig {
    /// Keep rows matching a structured predicate (see [`crate::transform::filter`]).
    /// The `where` value is parsed into a `Predicate` at build time so we can emit
    /// precise, path-bearing errors.
    Filter {
        #[serde(rename = "where")]
        predicate: serde_json::Value,
    },
    /// Normalize column names (NFKD→ascii, lower, non-alnum→filler, dedup).
    Normalize {
        #[serde(default = "default_true")]
        columns: bool,
        #[serde(default = "default_filler")]
        filler: String,
    },
    /// Forward-fill blanks in a column (un-merge merged spreadsheet cells).
    FixMergedColumns { column: String },
    /// Drop fully-empty rows.
    DropEmptyRows {
        #[serde(default)]
        zero_is_empty: bool,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserializes_file_mode_minimal() {
        let json = serde_json::json!({
            "tenant_id": "t", "client_id": "c", "client_secret": "s",
            "site_id": "sid", "drive_id": "did",
            "input": { "kind": "file", "file": "/a.xlsx", "parser": { "kind": "excel" } }
        });
        let cfg: SharepointConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.batch_size, 1000);
        assert_eq!(cfg.graph_base_url, "https://graph.microsoft.com");
        assert!(matches!(cfg.input, InputConfig::File { .. }));
    }

    #[test]
    fn deserializes_list_mode() {
        let json = serde_json::json!({
            "tenant_id": "t", "client_id": "c", "client_secret": "s",
            "site_id": "sid", "drive_id": "did",
            "input": { "kind": "list", "list": "My List" }
        });
        let cfg: SharepointConfig = serde_json::from_value(json).unwrap();
        match cfg.input {
            InputConfig::List { list } => assert_eq!(list, "My List"),
            _ => panic!("expected list"),
        }
    }

    #[test]
    fn excel_parser_defaults() {
        let json = serde_json::json!({ "kind": "excel" });
        let p: ParserConfig = serde_json::from_value(json).unwrap();
        match p {
            ParserConfig::Excel { header, skip_rows, sheet } => {
                assert!(header);
                assert_eq!(skip_rows, 0);
                assert!(sheet.is_none());
            }
            _ => panic!("expected excel"),
        }
    }

    #[test]
    fn unknown_transform_kind_is_rejected() {
        let json = serde_json::json!({ "kind": "explode" });
        assert!(serde_json::from_value::<TransformConfig>(json).is_err());
        let json = serde_json::json!({ "kind": "drop-empty-rows" });
        assert!(serde_json::from_value::<TransformConfig>(json).is_ok());
    }

    #[test]
    fn durations_need_a_unit() {
        use std::time::Duration;
        assert_eq!(parse_duration("60s").unwrap(), Duration::from_secs(60));
        assert_eq!(parse_duration("250ms").unwrap(), Duration::from_millis(250));
        assert_eq!(parse_duration("2m").unwrap(), Duration::from_secs(120));
        assert!(parse_duration("60").is_err());
        assert!(parse_duration("1y").is_err());
    }
}
