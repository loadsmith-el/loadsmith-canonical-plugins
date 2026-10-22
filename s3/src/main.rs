// mimalloc: release builds are static musl, whose default allocator is slow
// under many small allocations.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() {
    loadsmith_plugin_sdk::run_sink(loadsmith_s3::S3SinkPlugin::new()).await
}
