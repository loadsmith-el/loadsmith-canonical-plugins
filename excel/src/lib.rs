//! `loadsmith-parser-excel`: turns one sheet of each workbook a raw source
//! announces into one Arrow stream.
//!
//! A workbook (xlsx, xlsm, xlsb, xls, ods — by extension) is a compressed
//! archive that calamine reads whole, so one file's sheet is held in memory
//! while its batches are cut. Without declared `columns:`, the column types
//! are inferred from the whole sheet with the `sharepoint` rules (nullable
//! columns; all-int → int64, numeric → float64, all-bool → boolean,
//! all-datetime → timestamp(ms), else utf8). Excel stores every number as a
//! double, so numeric cells usually infer as float64; declare `columns:` to
//! get integers. The SDK fails the run with `SCHEMA_MISMATCH` when a later
//! file's schema differs from the first's.

use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{Field, Schema, SchemaRef};
use async_trait::async_trait;
use loadsmith_plugin_sdk::ParserPlugin;
use serde::Deserialize;
use serde_json::Value;
use tabular::excel::{read_workbook, Sheet};
use tabular::table::{build_array, Table};
use tabular::typed::{declared_schema, header_names, text_batch};

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawSheet {
    Index(usize),
    Name(String),
}

#[derive(Debug, Deserialize)]
struct RawOptions {
    sheet: Option<RawSheet>,
    #[serde(default = "default_true")]
    header: bool,
    #[serde(default)]
    skip_rows: usize,
    columns: Option<Value>,
    #[serde(default = "default_batch_size")]
    batch_size: usize,
}

fn default_true() -> bool {
    true
}
fn default_batch_size() -> usize {
    1000
}

/// Validated options.
#[derive(Debug, Clone)]
pub struct ExcelOptions {
    pub sheet: Sheet,
    pub header: bool,
    pub skip_rows: usize,
    pub columns: Option<SchemaRef>,
    pub batch_size: usize,
}

impl Default for ExcelOptions {
    fn default() -> Self {
        Self { sheet: Sheet::First, header: true, skip_rows: 0, columns: None, batch_size: default_batch_size() }
    }
}

impl ExcelOptions {
    /// Reads and validates the plugin's `config` (`null` = every default).
    pub fn from_config(config: &loadsmith_plugin_sdk::PluginConfig) -> Result<Self> {
        let raw: RawOptions = if config.is_null() {
            loadsmith_plugin_sdk::PluginConfig::new(Value::Object(Default::default()), config.unknown_keys()).parse()?
        } else {
            config.parse()?
        };
        if raw.batch_size == 0 {
            bail!("batch_size must be at least 1");
        }
        let sheet = match raw.sheet {
            None => Sheet::First,
            Some(RawSheet::Index(i)) => Sheet::Index(i),
            Some(RawSheet::Name(n)) if n.is_empty() => bail!("sheet name is empty"),
            Some(RawSheet::Name(n)) => Sheet::Name(n),
        };
        let columns = match &raw.columns {
            None => None,
            Some(v) => Some(Arc::new(declared_schema(v).context("invalid `columns`")?)),
        };
        Ok(Self { sheet, header: raw.header, skip_rows: raw.skip_rows, columns, batch_size: raw.batch_size })
    }

    /// [`from_config`](Self::from_config) for a bare value, rejecting unknown
    /// keys.
    pub fn from_value(config: Value) -> Result<Self> {
        Self::from_config(&loadsmith_plugin_sdk::PluginConfig::new(config, loadsmith_plugin_sdk::UnknownKeys::Error))
    }
}

/// One sheet, read; batches are cut from it.
pub struct SheetReader {
    table: Table,
    schema: SchemaRef,
    declared: bool,
    next_row: usize,
    batch_size: usize,
    /// Rows before the first data row (skipped + header), for messages.
    first_line: usize,
}

impl SheetReader {
    /// Reads the selected sheet of the workbook at `path`.
    pub fn open(path: &Path, opts: &ExcelOptions) -> Result<Self> {
        let mut table = read_workbook(path, &opts.sheet, opts.header, opts.skip_rows)?;
        let first_line = opts.skip_rows + usize::from(opts.header) + 1;
        let (schema, declared) = match &opts.columns {
            Some(declared) => {
                if !table.columns.is_empty() && table.columns.len() != declared.fields().len() {
                    bail!(
                        "the sheet has {} columns, `columns` declares {}",
                        table.columns.len(),
                        declared.fields().len()
                    );
                }
                (Arc::clone(declared), true)
            }
            None => {
                if table.columns.is_empty() {
                    bail!("the sheet is empty: there are no columns to infer");
                }
                if opts.header {
                    table.columns = header_names(table.columns.iter().map(String::as_str))?;
                }
                let inferred = table.arrow_schema();
                (inferred, false)
            }
        };
        Ok(Self { table, schema, declared, next_row: 0, batch_size: opts.batch_size, first_line })
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// The next batch; `None` after the sheet's last row.
    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let start = self.next_row;
        let end = (start + self.batch_size).min(self.table.rows.len());
        if start >= end {
            return Ok(None);
        }
        self.next_row = end;
        let rows = &self.table.rows[start..end];
        let batch = if self.declared {
            // Declared types read each cell's text, strictly.
            let text: Vec<Vec<Option<String>>> = (0..self.schema.fields().len())
                .map(|c| rows.iter().map(|r| r.get(c).and_then(|cell| cell.as_text())).collect())
                .collect();
            let columns: Vec<Vec<Option<&str>>> =
                text.iter().map(|col| col.iter().map(Option::as_deref).collect()).collect();
            text_batch(&self.schema, &columns).map_err(|e| {
                anyhow::anyhow!(
                    "row {}, column {} (`{}`): {}",
                    self.first_line + start + e.row,
                    e.column + 1,
                    self.schema.field(e.column).name(),
                    e.message
                )
            })?
        } else {
            // The inferred types fit every cell of the sheet.
            let arrays: Vec<ArrayRef> = self
                .schema
                .fields()
                .iter()
                .enumerate()
                .map(|(c, f): (usize, &Arc<Field>)| build_array(rows, c, f.data_type()))
                .collect::<Result<_>>()?;
            RecordBatch::try_new(Arc::clone(&self.schema), arrays)?
        };
        Ok(Some(batch))
    }
}

/// The parser plugin.
#[derive(Default)]
pub struct ExcelParser {
    opts: ExcelOptions,
    current: Option<SheetReader>,
}

#[async_trait]
impl ParserPlugin for ExcelParser {
    fn plugin_name(&self) -> &str {
        "loadsmith-parser-excel"
    }
    fn plugin_version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    async fn configure(&mut self, config: loadsmith_plugin_sdk::PluginConfig) -> Result<()> {
        self.opts = ExcelOptions::from_config(&config)?;
        Ok(())
    }

    fn declared_schema(&self) -> Option<Schema> {
        self.opts.columns.as_deref().cloned()
    }

    async fn open(&mut self, path: &Path) -> Result<Schema> {
        self.current = None;
        let reader = SheetReader::open(path, &self.opts)?;
        let schema = reader.schema().as_ref().clone();
        self.current = Some(reader);
        Ok(schema)
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let Some(reader) = self.current.as_mut() else {
            return Ok(None);
        };
        let batch = reader.next_batch()?;
        if batch.is_none() {
            self.current = None;
        }
        Ok(batch)
    }

    async fn cancel(&mut self) {
        self.current = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Float64Type, Int32Type};
    use arrow_array::Array;
    use arrow_schema::DataType;
    use rust_xlsxwriter::Workbook;
    use serde_json::json;
    use std::path::PathBuf;

    struct Tmp(PathBuf);

    impl Drop for Tmp {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    /// Sheet `first`: a banner row, then `id,name` and 3 rows; sheet `two`:
    /// `x` and 2500 rows.
    fn workbook(tag: &str) -> Tmp {
        let mut wb = Workbook::new();
        let ws = wb.add_worksheet().set_name("first").unwrap();
        ws.write(0, 0, "report 2026").unwrap();
        ws.write(1, 0, "id").unwrap();
        ws.write(1, 1, "name").unwrap();
        for (i, name) in ["ana", "bia", ""].iter().enumerate() {
            let r = i as u32 + 2;
            ws.write(r, 0, i as f64 + 1.0).unwrap();
            if !name.is_empty() {
                ws.write(r, 1, *name).unwrap();
            }
        }
        let ws = wb.add_worksheet().set_name("two").unwrap();
        ws.write(0, 0, "x").unwrap();
        for i in 0..2500u32 {
            ws.write(i + 1, 0, i).unwrap();
        }
        let path = std::env::temp_dir().join(format!("loadsmith-parser-excel-{tag}-{}.xlsx", std::process::id()));
        wb.save(&path).unwrap();
        Tmp(path)
    }

    fn opts(v: Value) -> ExcelOptions {
        ExcelOptions::from_value(v).unwrap()
    }

    fn read_all(path: &Path, o: &ExcelOptions) -> Result<(SchemaRef, Vec<RecordBatch>)> {
        let mut r = SheetReader::open(path, o)?;
        let mut out = Vec::new();
        while let Some(b) = r.next_batch()? {
            out.push(b);
        }
        Ok((r.schema(), out))
    }

    #[test]
    fn first_sheet_with_skip_rows_and_inference() {
        let wb = workbook("first");
        let (schema, batches) = read_all(&wb.0, &opts(json!({"skip_rows": 1}))).unwrap();
        assert_eq!(schema.field(0), &Field::new("id", DataType::Float64, true));
        assert_eq!(schema.field(1), &Field::new("name", DataType::Utf8, true));
        let b = &batches[0];
        assert_eq!(b.column(0).as_primitive::<Float64Type>().values(), &[1.0, 2.0, 3.0]);
        assert!(b.column(1).is_null(2));
    }

    #[test]
    fn sheet_by_name_or_index_and_batching() {
        let wb = workbook("select");
        for sheet in [json!("two"), json!(1)] {
            let (schema, batches) = read_all(&wb.0, &opts(json!({"sheet": sheet}))).unwrap();
            assert_eq!(schema.field(0).name(), "x");
            let sizes: Vec<_> = batches.iter().map(|b| b.num_rows()).collect();
            assert_eq!(sizes, [1000, 1000, 500]);
        }
        let err = |v: Value| SheetReader::open(&wb.0, &opts(v)).err().map(|e| format!("{e:#}")).unwrap();
        assert!(err(json!({"sheet": "nope"})).contains("sheet 'nope' not found"));
        assert!(err(json!({"sheet": 5})).contains("sheet index 5 out of range (the workbook has 2 sheets)"));
    }

    #[test]
    fn declared_columns_convert_strictly() {
        let wb = workbook("declared");
        let o = opts(json!({"skip_rows": 1, "columns": [
            {"name": "id", "type": "int32", "nullable": false},
            {"name": "name", "type": "utf8"},
        ]}));
        let (schema, batches) = read_all(&wb.0, &o).unwrap();
        assert_eq!(schema.field(0), &Field::new("id", DataType::Int32, false));
        assert_eq!(batches[0].column(0).as_primitive::<Int32Type>().values(), &[1, 2, 3]);
        // Without skip_rows, the banner row is the header and `id` is a data row.
        let o = opts(json!({"header": false, "columns": [
            {"name": "id", "type": "int32"},
            {"name": "name", "type": "utf8"},
        ]}));
        let err = read_all(&wb.0, &o).unwrap_err().to_string();
        assert!(err.contains("row 1, column 1 (`id`): `report 2026` is not a valid int32"), "{err}");
    }

    #[test]
    fn config_is_validated() {
        let err = |v: Value| format!("{:#}", ExcelOptions::from_value(v).unwrap_err());
        assert!(err(json!({"sheet": ""})).contains("empty"));
        assert!(err(json!({"batch_size": 0})).contains("batch_size"));
        assert!(err(json!({"delimiter": ","})).contains("unknown config key"));
        assert!(err(json!({"columns": []})).contains("empty"));
    }

    #[test]
    fn a_file_that_is_not_a_workbook_fails_to_open() {
        let p = std::env::temp_dir().join(format!("loadsmith-parser-excel-bad-{}.xlsx", std::process::id()));
        std::fs::write(&p, b"id,name\n1,a\n").unwrap();
        let t = Tmp(p);
        assert!(SheetReader::open(&t.0, &ExcelOptions::default()).is_err());
    }
}
