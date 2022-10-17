use color_eyre::{eyre::eyre, Report, Result};
use croncat_pipeline::{try_flat_join, Dispatcher, ProviderSystem, Sequencer};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use sea_orm::Database;
use tendermint_rpc::HttpClient;
use tokio::sync::{broadcast, mpsc};
use tokio_retry::strategy::{jitter, FibonacciBackoff, FixedInterval};
use tokio_retry::Retry;
use tracing::{error, info, trace, warn};

use super::config::filter::Filter;
use super::config::{Config, Source};
use crate::indexer;
use crate::streams::block::{poll_stream_blocks, ws_block_stream};

pub async fn run(name: &str, chain_id: &str, sources: &[Source], filters: &[Filter]) -> Result<()> {
    // Setup system channels.
    let (provider_system_tx, provider_system_rx) = mpsc::unbounded_channel();
    let mut provider_system = ProviderSystem::new(provider_system_tx);

    // Use this to query RPC for transactions.
    let mut last_polling_url = None;

    // Load sources from the configuration.
    for source in sources.iter().cloned() {
        let name = source.to_string();

        match source.source_type {
            indexer::config::SourceType::Websocket => {
                provider_system.add_provider_stream(name, ws_block_stream(source.url.to_string()));
            }
            indexer::config::SourceType::Polling => {
                last_polling_url = Some(source.url.clone());
                provider_system
                    .add_provider_stream(name, poll_stream_blocks(source.url.to_string(), 3));
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

    let name = name.to_owned();
    let chain_id = chain_id.to_owned();
    let filters = filters.to_owned();
    // Create an indexer to process the blocks.
    let indexer_handle = tokio::spawn(async move {
        let rpc_client = HttpClient::new(last_polling_url.unwrap().to_string().as_str())?;
        let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgresql://postgres:postgres@localhost:5432/croncat_indexer".to_string()
        });
        let db = Database::connect(database_url).await?;

        while let Ok(block) = dispatcher_rx.recv().await {
            let expected_chain_id = &chain_id;
            let chain_id = block.header().chain_id.to_string();
            if chain_id != *expected_chain_id {
                warn!(
                    "Chain ID mismatch, expected {} but found {}",
                    expected_chain_id, chain_id
                );
                warn!("No further processing will be done for this block");
                continue;
            }

            info!(
                "[{}] Indexing block {} ({}) from {}",
                name,
                block.header().height,
                block.header().chain_id,
                block.header().time
            );
            let retry_strategy = FibonacciBackoff::from_millis(100).map(jitter).take(10);
            Retry::spawn(retry_strategy, || async {
                let result = indexer::index_block(&db, &rpc_client, &filters, block.clone()).await;
                if result.is_err() {
                    trace!(
                        "[{}] Indexing {} ({}) from {} failed, retrying...",
                        name,
                        block.header().height,
                        block.header().chain_id,
                        block.header().time
                    );
                }
                result
            })
            .await
            .map_err(|err| {
                eyre!(
                    "[{}] Failed to index block {} ({}) from {}: {}",
                    name,
                    block.header().height,
                    block.header().chain_id,
                    block.header().time,
                    err
                )
            })?;
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

pub async fn run_all() -> Result<()> {
    // Load the configurations from the pwd.
    let configs = Config::get_configs_from_pwd()?;

    // If we have no configs then we should just exit.
    if configs.is_empty() {
        error!("No configs found in {}", std::env::current_dir()?.display());
        std::process::exit(1);
    }

    // Otherwise we should run all the indexers based on each config.
    let mut indexer_handles = FuturesUnordered::new();

    for (path, config) in Config::get_configs_from_pwd()? {
        info!("Starting indexer for {}: {}", config.name, path.display());
        trace!("Configuration details: {:#?}", config);

        let retry_strategy = FixedInterval::from_millis(5000);
        let indexer_handle = tokio::spawn(async move {
            Retry::spawn(retry_strategy, || async {
                indexer::system::run(
                    &config.name,
                    &config.chain_id,
                    &config.sources,
                    &config.filters,
                )
                .await
                .map_err(|err| {
                    error!("Indexer {} ({}) crashed!", config.name, path.display());
                    error!("Error: {}", err);
                    error!("Retrying in 5 seconds...");

                    err
                })
            })
            .await?;

            Ok::<(), Report>(())
        });
        indexer_handles.push(indexer_handle);
    }

    // Wait for all the indexers to finish.
    while let Some(indexer_handle) = indexer_handles.next().await {
        indexer_handle??;
    }

    Ok(())
}
