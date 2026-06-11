use anyhow::{Context, Result};
use async_trait::async_trait;
use loadsmith_plugin_sdk::ConfigProviderPlugin;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct FileConfig {
    uri: String,
}

struct FileProvider {
    path: Option<std::path::PathBuf>,
}

impl FileProvider {
    fn new() -> Self {
        Self { path: None }
    }
}

#[async_trait]
impl ConfigProviderPlugin for FileProvider {
    fn plugin_name(&self) -> &str {
        "loadsmith-config-provider-file"
    }
    fn plugin_version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    async fn configure(&mut self, config: serde_json::Value) -> Result<()> {
        let cfg: FileConfig =
            serde_json::from_value(config).context("invalid file provider config")?;

        let uri = cfg.uri;
        let path_str = uri
            .strip_prefix("file://")
            .ok_or_else(|| anyhow::anyhow!("file provider requires file:// URI, got: {uri}"))?;

        self.path = Some(std::path::PathBuf::from(path_str));
        Ok(())
    }

    async fn fetch(&mut self) -> Result<Vec<u8>> {
        let path = self.path.as_ref().unwrap();
        std::fs::read(path)
            .with_context(|| format!("cannot read file: {}", path.display()))
    }
}

#[tokio::main]
async fn main() {
    loadsmith_plugin_sdk::run_config_provider(FileProvider::new()).await
}
