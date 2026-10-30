//! The parser's `config`.

use anyhow::{bail, Context, Result};
use arrow_schema::{Schema, SchemaRef};
use encoding_rs::Encoding;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub(crate) struct RawOptions {
    #[serde(default = "default_delimiter")]
    delimiter: String,
    encoding: Option<String>,
    #[serde(default = "default_true")]
    header: bool,
    #[serde(default = "default_infer_rows")]
    infer_rows: usize,
    columns: Option<Value>,
    #[serde(default = "default_batch_size")]
    batch_size: usize,
}

fn default_delimiter() -> String {
    ",".into()
}
fn default_true() -> bool {
    true
}
fn default_infer_rows() -> usize {
    1000
}
fn default_batch_size() -> usize {
    1000
}

/// Validated options.
#[derive(Debug, Clone)]
pub struct CsvOptions {
    /// The field delimiter, one ASCII byte.
    pub delimiter: u8,
    /// The files' text encoding (UTF-8 by default; a byte-order mark wins).
    pub encoding: &'static Encoding,
    /// Whether the first record names the columns.
    pub header: bool,
    /// How many data records type inference reads (without `columns`).
    pub infer_rows: usize,
    /// The declared schema (`columns:`), if any.
    pub columns: Option<SchemaRef>,
    /// Rows per Arrow batch.
    pub batch_size: usize,
}

impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            delimiter: b',',
            encoding: encoding_rs::UTF_8,
            header: true,
            infer_rows: default_infer_rows(),
            columns: None,
            batch_size: default_batch_size(),
        }
    }
}

impl CsvOptions {
    /// Reads and validates the plugin's `config` (`null` = every default).
    pub fn from_config(config: &loadsmith_plugin_sdk::PluginConfig) -> Result<Self> {
        let raw: RawOptions = if config.is_null() {
            loadsmith_plugin_sdk::PluginConfig::new(Value::Object(Default::default()), config.unknown_keys()).parse()?
        } else {
            config.parse()?
        };
        let delimiter = match raw.delimiter.as_bytes() {
            [b] if b.is_ascii() && !matches!(b, b'"' | b'\n' | b'\r') => *b,
            _ => bail!("delimiter must be one ASCII character other than a quote or newline, got {:?}", raw.delimiter),
        };
        let encoding = tabular::csv::encoding(raw.encoding.as_deref())?;
        if raw.infer_rows == 0 {
            bail!("infer_rows must be at least 1");
        }
        if raw.batch_size == 0 {
            bail!("batch_size must be at least 1");
        }
        let columns = match &raw.columns {
            None => None,
            Some(v) => Some(Arc::new(tabular::typed::declared_schema(v).context("invalid `columns`")?)),
        };
        Ok(Self { delimiter, encoding, header: raw.header, infer_rows: raw.infer_rows, columns, batch_size: raw.batch_size })
    }

    /// [`from_config`](Self::from_config) for a bare value, rejecting unknown
    /// keys.
    pub fn from_value(config: Value) -> Result<Self> {
        Self::from_config(&loadsmith_plugin_sdk::PluginConfig::new(config, loadsmith_plugin_sdk::UnknownKeys::Error))
    }

    /// The declared schema, owned.
    pub fn declared(&self) -> Option<Schema> {
        self.columns.as_deref().cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn defaults() {
        let o = CsvOptions::from_value(Value::Null).unwrap();
        assert_eq!((o.delimiter, o.header, o.infer_rows, o.batch_size), (b',', true, 1000, 1000));
        assert_eq!(o.encoding, encoding_rs::UTF_8);
        assert!(o.columns.is_none());
    }

    #[test]
    fn options_are_validated() {
        let err = |v: Value| format!("{:#}", CsvOptions::from_value(v).unwrap_err());
        assert!(err(json!({"delimiter": ";;"})).contains("one ASCII character"));
        assert!(err(json!({"delimiter": "€"})).contains("one ASCII character"));
        assert!(err(json!({"delimiter": "\""})).contains("quote"));
        assert!(err(json!({"encoding": "klingon"})).contains("unknown encoding 'klingon'"));
        assert!(err(json!({"infer_rows": 0})).contains("infer_rows"));
        assert!(err(json!({"batch_size": 0})).contains("batch_size"));
        assert!(err(json!({"columns": [{"name": "a", "type": "nope"}]})).contains("unknown type"));
        assert!(err(json!({"quote": "'"})).contains("unknown config key"));
    }

    #[test]
    fn tab_semicolon_and_encoding_labels() {
        let o = CsvOptions::from_value(json!({"delimiter": "\t", "encoding": "latin1"})).unwrap();
        assert_eq!(o.delimiter, b'\t');
        assert_eq!(o.encoding, encoding_rs::WINDOWS_1252);
        assert_eq!(CsvOptions::from_value(json!({"delimiter": ";"})).unwrap().delimiter, b';');
    }
}
