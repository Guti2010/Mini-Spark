mod worker;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    worker::run().await
}
