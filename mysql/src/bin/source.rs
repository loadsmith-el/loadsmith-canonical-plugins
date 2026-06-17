#[tokio::main]
async fn main() {
    loadsmith_plugin_sdk::run_source(loadsmith_mysql::MysqlSourcePlugin::new()).await
}
