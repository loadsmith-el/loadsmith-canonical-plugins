mod plugin;

#[tokio::main]
async fn main() {
    loadsmith_plugin_sdk::run_sink(plugin::LocalCopyPlugin::new()).await
}
