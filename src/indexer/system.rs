use color_eyre::{Report, Result};
use croncat_pipeline::{try_flat_join, Dispatcher, ProviderSystem, Sequencer};
use sea_orm::Database;
use tendermint_rpc::HttpClient;
use tokio::sync::{broadcast, mpsc};
use tokio_retry::strategy::{jitter, FibonacciBackoff};
use tokio_retry::Retry;
use tracing::{info, trace};

use super::config::Config;
use crate::indexer;
use crate::streams::block::{poll_stream_blocks, ws_block_stream};

pub async fn run(config: Config) -> Result<()> {
    let (provider_system_tx, provider_system_rx) = mpsc::unbounded_channel();
    let mut provider_system = ProviderSystem::new(provider_system_tx);

    let mut last_polling_url = None;

    for source in config.sources {
        let name = source.to_string();

        match source.source_type {
            indexer::config::SourceType::Websocket => {
                provider_system.add_provider_stream(name, ws_block_stream(source.url.to_string()));
            }
            indexer::config::SourceType::Polling => {
                last_polling_url = Some(source.url.clone());
                provider_system
                    .add_provider_stream(name, poll_stream_blocks(source.url.to_string(), 2));
            }
        }
    }

    // Run the provider system.
    let provider_system_handle = tokio::spawn(async move { provider_system.produce().await });

    // Create a sequencer to dedup and sort the blocks with a cache size of 32.
    let (sequencer_tx, sequencer_rx) = mpsc::unbounded_channel();
    let mut sequencer = Sequencer::new(provider_system_rx, sequencer_tx, 128)?;
    let sequencer_handle = tokio::spawn(async move { sequencer.consume().await });

    // Dispatch the blocks to the indexer.
    let (dispatcher_tx, mut dispatcher_rx) = broadcast::channel(512);
    let mut dispatcher = Dispatcher::new(sequencer_rx, dispatcher_tx.clone());
    let dispatcher_handle = tokio::spawn(async move { dispatcher.fanout().await });

    // Create an indexer to process the blocks.
    let indexer_handle = tokio::spawn(async move {
        let rpc_client = HttpClient::new(last_polling_url.unwrap().to_string().as_str())?;
        let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgresql://postgres:postgres@localhost:5432/croncat_indexer".to_string()
        });
        let db = Database::connect(database_url).await?;

        while let Ok(block) = dispatcher_rx.recv().await {
            info!(
                "Indexing block {} from {}",
                block.header().height,
                block.header().time
            );
            let retry_strategy = FibonacciBackoff::from_millis(100).map(jitter).take(10);

            Retry::spawn(retry_strategy, || async {
                let result = indexer::index_block(&db, &rpc_client, block.clone()).await;
                if result.is_err() {
                    trace!(
                        "Indexing {} from {} failed, retrying...",
                        block.header().height,
                        block.header().time
                    );
                }
                result
            })
            .await?;
        }

        Ok::<(), Report>(())
    });

    let _ = try_flat_join!(
        provider_system_handle,
        sequencer_handle,
        dispatcher_handle,
        indexer_handle,
    )?;

    Ok(())
}
