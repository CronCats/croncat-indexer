#![feature(type_alias_impl_trait)]

use color_eyre::Result;

pub mod env;
pub mod indexer;
pub mod ingestion;
pub mod streams;
pub mod util;

#[tokio::main]
async fn main() -> Result<()> {
    env::setup()?;

    let blocks_handle = tokio::spawn(async move { indexer::system::blocks().await });
    let transactions_handle = tokio::spawn(async move { indexer::system::transactions().await });
    try_flat_join!(blocks_handle, transactions_handle)?;

    Ok(())
}
