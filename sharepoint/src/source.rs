//! The SharePoint source plugin. `configure` does the whole extract — authenticate
//! to Graph, fetch (a file then parse it, or a list's items), run the in-plugin
//! transform chain — and materializes the result into Arrow batches. `schema` and
//! `next_batch` then just serve what was prepared.

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use loadsmith_plugin_sdk::SourcePlugin;

use crate::config::{InputConfig, SharepointConfig, TransformConfig};
use crate::graph::GraphClient;

/// Parses every `filter` predicate up front (before any network call), and
/// applies the unknown-keys policy to the keys they do not use: the `where`
/// trees are free-form values that the SDK's typed parse cannot see into.
fn check_filters(cfg: &SharepointConfig, policy: loadsmith_plugin_sdk::UnknownKeys) -> Result<()> {
    let mut unknown = Vec::new();
    let mut parsed = Ok(());
    for (i, step) in cfg.transform.iter().enumerate() {
        if let TransformConfig::Filter { predicate } = step {
            let path = format!("transform[{i}].where");
            if let Err(e) = crate::transform::filter::Predicate::parse(predicate, &path, &mut unknown) {
                parsed = Err(e);
                break;
            }
        }
    }
    if unknown.is_empty() {
        return parsed;
    }
    if policy == loadsmith_plugin_sdk::UnknownKeys::Warn {
        for key in &unknown {
            tracing::warn!("ignoring unknown config key `{key}`");
        }
        return parsed;
    }
    // A misspelled key (`valu`) usually also explains the parse error (no
    // `value`): name it first.
    let keys = unknown
        .into_iter()
        .map(|path| loadsmith_plugin_sdk::config::UnknownKey { path, suggestion: None })
        .collect();
    Err(loadsmith_plugin_sdk::ConfigRejection::UnknownKeys(keys).into())
}

pub struct SharepointSourcePlugin {
    batch_size: usize,
    schema: Option<SchemaRef>,
    batches: Vec<RecordBatch>,
    cursor: usize,
}

impl Default for SharepointSourcePlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl SharepointSourcePlugin {
    pub fn new() -> Self {
        SharepointSourcePlugin { batch_size: 1000, schema: None, batches: Vec::new(), cursor: 0 }
    }
}

#[async_trait]
impl SourcePlugin for SharepointSourcePlugin {
    fn config_schema() -> Option<serde_json::Value> {
        Some(loadsmith_plugin_sdk::config_schema::schema_for::<crate::config::SharepointConfig>())
    }
    fn plugin_name(&self) -> &str {
        "loadsmith-source-sharepoint"
    }
    fn plugin_version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    async fn configure(&mut self, config: loadsmith_plugin_sdk::PluginConfig) -> Result<()> {
        let unknown_keys = config.unknown_keys();
        let cfg: SharepointConfig = config.parse().map_err(crate::vocab::hint_unknown_variant)?;
        if cfg.batch_size == 0 {
            anyhow::bail!("`batch_size` must be at least 1");
        }
        self.batch_size = cfg.batch_size;
        check_filters(&cfg, unknown_keys)?;

        let mut graph = GraphClient::new(&cfg)?;

        let site_id = graph
            .resolve_site_id(cfg.site.as_deref(), cfg.site_id.as_deref())
            .await
            .context("resolving site")?;

        // ── Extract: file (download + parse) or list (items → table) ──────────
        let table = match &cfg.input {
            InputConfig::File { file, parser } => {
                let drive_id = graph
                    .resolve_drive_id(&site_id, cfg.drive.as_deref(), cfg.drive_id.as_deref())
                    .await
                    .context("resolving drive")?;
                let bytes = graph.download_file(&drive_id, file).await.context("downloading file")?;
                crate::parse::parse(&bytes, parser).context("parsing file")?
            }
            InputConfig::List { list } => {
                let list_id = graph.resolve_list_id(&site_id, list).await.context("resolving list")?;
                let items =
                    graph.fetch_list_items(&site_id, &list_id).await.context("fetching list items")?;
                crate::list::items_to_table(&items).context("building table from list")?
            }
        };

        // ── In-plugin transforms, then materialize to Arrow batches ───────────
        let table = crate::transform::apply_chain(&cfg.transform, table)?;
        let (schema, batches) = table.to_record_batches(self.batch_size)?;
        self.schema = Some(schema);
        self.batches = batches;
        self.cursor = 0;
        Ok(())
    }

    async fn schema(&mut self) -> Result<Schema> {
        let schema = self
            .schema
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("schema() called before configure()"))?;
        Ok(schema.as_ref().clone())
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.cursor >= self.batches.len() {
            return Ok(None);
        }
        let batch = self.batches[self.cursor].clone();
        self.cursor += 1;
        Ok(Some(batch))
    }

    async fn cancel(&mut self) {}
}
