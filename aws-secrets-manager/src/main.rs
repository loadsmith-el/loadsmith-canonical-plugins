// mimalloc: release builds are static musl, whose default allocator is slow
// under many small allocations.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() {
    loadsmith_plugin_sdk::run_provider(loadsmith_aws_secrets_manager::SecretsManagerProvider::new()).await
}
