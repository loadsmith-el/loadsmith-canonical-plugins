use anyhow::Result;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use loadsmith_plugin_sdk::DestinationPlugin;

/// A destination that discards every batch, counting rows only.
///
/// It writes nothing to disk or network — useful for large throughput/volume
/// tests where only the source + pump behaviour matters, not the output. The
/// reported `rows_written` is the count of rows it received, so row-count
/// assertions still work.
pub struct NullPlugin {
    rows_written: u64,
}

impl NullPlugin {
    pub fn new() -> Self {
        Self { rows_written: 0 }
    }
}

#[async_trait]
impl DestinationPlugin for NullPlugin {
    fn plugin_name(&self) -> &str {
        "loadsmith-destination-null"
    }
    fn plugin_version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    async fn configure(&mut self, _config: serde_json::Value) -> Result<()> {
        // No configuration: the null sink takes nothing and ignores any config.
        Ok(())
    }

    async fn prepare(&mut self) -> Result<()> {
        // Nothing to open.
        Ok(())
    }

    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        // Count and drop. `batch` is dropped at the end of the call.
        self.rows_written += batch.num_rows() as u64;
        Ok(())
    }

    async fn finalize(&mut self) -> Result<u64> {
        Ok(self.rows_written)
    }

    async fn cancel(&mut self) {
        // Nothing to clean up.
    }
}
