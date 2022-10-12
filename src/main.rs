#![feature(type_alias_impl_trait)]

use block::{poll_stream_blocks, ws_block_stream};
use color_eyre::{Report, Result};
use croncat_indexer::flatten_join;
use ingestion::{Dispatcher, ProviderSystem, Sequencer};
use sea_orm::Database;
use tokio::{
    sync::{broadcast, mpsc},
    try_join,
};
use tracing::info;

mod block;
mod env;
mod indexer;
mod ingestion;
mod util;

#[tokio::main]
async fn main() -> Result<()> {
    env::setup()?;

    let (provider_system_tx, provider_system_rx) = mpsc::unbounded_channel();
    let mut provider_system = ProviderSystem::new(provider_system_tx);

    // Websocket provider.
    provider_system.add_provider_stream(
        "polkachu-ws",
        ws_block_stream("wss://juno-testnet-rpc.polkachu.com/websocket"),
    );
    // Polling provider.
    provider_system.add_provider_stream(
        "polkachu-http",
        poll_stream_blocks("https://juno-testnet-rpc.polkachu.com", 3),
    );

    let provider_system_handle = tokio::spawn(async move { provider_system.produce().await });

    // Create a sequencer to dedup and sort the blocks with a cache size of 32.
    let (sequencer_tx, sequencer_rx) = mpsc::unbounded_channel();
    let mut sequencer = Sequencer::new(provider_system_rx, sequencer_tx, 32)?;
    let sequencer_handle = tokio::spawn(async move { sequencer.consume().await });

    // Dispatch the blocks to the indexer.
    let (dispatcher_tx, mut dispatcher_rx) = broadcast::channel(10);
    let mut dispatcher = Dispatcher::new(sequencer_rx, dispatcher_tx.clone());
    let dispatcher_handle = tokio::spawn(async move { dispatcher.fanout().await });

    // Create an indexer to process the blocks.
    let indexer_handle = tokio::spawn(async move {
        let db = Database::connect("postgresql://postgres:postgres@localhost:5432/croncat_indexer")
            .await?;

        while let Ok(block) = dispatcher_rx.recv().await {
            info!(
                "Indexing block {} @ {}",
                block.header().height,
                block.header().time
            );
            indexer::index_block(&db, block).await?
        }

        Ok::<(), Report>(())
    });

    let _ = try_join!(
        flatten_join(provider_system_handle),
        flatten_join(sequencer_handle),
        flatten_join(dispatcher_handle),
        flatten_join(indexer_handle),
    )?;

    Ok(())
}
