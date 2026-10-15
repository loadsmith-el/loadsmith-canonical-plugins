#[tokio::main]
async fn main() {
    loadsmith_plugin_sdk::run_source(loadsmith_oracle::OracleSourcePlugin::new()).await
}
