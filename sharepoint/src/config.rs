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
fn default_authority() -> String {
    "https://login.microsoftonline.com".to_string()
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

    pub input: InputConfig,

    #[serde(default)]
    pub transform: Vec<TransformConfig>,

    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

/// How data is obtained from the source: a file download (then parsed) or a
/// SharePoint List read.
#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InputConfig {
    /// Download a file from the drive at `file` (a path like `/folder/sheet.xlsx`)
    /// and parse it.
    File { file: String, parser: ParserConfig },
    /// Read items from a SharePoint List, by display name or id.
    List { list: String },
}

/// Bytes → table parser (file mode only).
#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
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
#[serde(tag = "kind", rename_all = "snake_case")]
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
    }
}
