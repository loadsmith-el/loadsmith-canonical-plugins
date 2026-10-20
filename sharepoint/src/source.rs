//! The SharePoint source plugin. `configure` does the whole extract — authenticate
//! to Graph, fetch (a file then parse it, or a list's items), run the in-plugin
//! transform chain — and materializes the result into Arrow batches. `schema` and
//! `next_batch` then just serve what was prepared.

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use loadsmith_plugin_sdk::SourcePlugin;

use crate::config::{InputConfig, SharepointConfig};
use crate::graph::GraphClient;

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
    fn plugin_name(&self) -> &str {
        "loadsmith-source-sharepoint"
    }
    fn plugin_version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    async fn configure(&mut self, config: loadsmith_plugin_sdk::PluginConfig) -> Result<()> {
        // Plugin config as raw JSON (unknown-key checks: see `PluginConfig::parse`).
        let config = config.into_value();
        let cfg: SharepointConfig =
            serde_json::from_value(config).map_err(|e| anyhow::anyhow!("invalid sharepoint source config: {e}"))?;
        self.batch_size = cfg.batch_size;

        let mut graph = GraphClient::new(
            cfg.tenant_id,
            cfg.client_id,
            cfg.client_secret,
            cfg.graph_base_url,
            cfg.authority_url,
        )?;

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
