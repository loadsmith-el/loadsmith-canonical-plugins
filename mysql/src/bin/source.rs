// mimalloc: release builds are static musl, whose default allocator is slow
// under the many small allocations of Arrow batch building.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() {
    loadsmith_plugin_sdk::run_source(loadsmith_mysql::MysqlSourcePlugin::new()).await
}
