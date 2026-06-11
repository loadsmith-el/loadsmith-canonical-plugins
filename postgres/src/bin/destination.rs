#[tokio::main]
async fn main() {
    loadsmith_plugin_sdk::run_destination(loadsmith_postgres::PostgresDestPlugin::new()).await
}
