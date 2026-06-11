mod plugin;

#[tokio::main]
async fn main() {
    loadsmith_plugin_sdk::run_destination(plugin::ParquetPlugin::new()).await
}
