use std::time::Duration;

use color_eyre::{eyre::eyre, Report, Result};
use croncat_pipeline::{try_flat_join, Dispatcher, ProviderSystem, Sequencer};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use sea_orm::{ConnectOptions, Database, DatabaseConnection};
use tendermint_rpc::HttpClient;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio_retry::strategy::{jitter, FibonacciBackoff, FixedInterval};
use tokio_retry::Retry;
use tracing::{error, info, log, trace, warn};

use super::config::filter::Filter;
use super::config::{Config, Source, SourceType};
use crate::indexer;
use crate::streams::block::{poll_stream_blocks, ws_block_stream};

///
/// Run a configured indexer.
///
pub async fn run(
    name: &str,
    chain_id: &str,
    sources: &[Source],
    filters: &Vec<Filter>,
) -> Result<()> {
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

    // Create an indexer to process the blocks.
    let name = name.to_owned();
    let chain_id = chain_id.to_owned();
    let filters = filters.to_owned();
    let indexer_handle = tokio::spawn(async move {
        let rpc_client = HttpClient::new(last_polling_url.unwrap().to_string().as_str())?;
        let db = get_database_connection().await?;

        // While there are still blocks to process.
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

pub async fn run_historical(
    name: &str,
    chain_id: &str,
    sources: &Vec<Source>,
    filters: &Vec<Filter>,
) -> Result<()> {
    let name = name.to_owned();
    let chain_id = chain_id.to_owned();
    let filters = filters.to_owned();
    let sources = sources.to_owned();
    let historical_indexer_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
        let db = get_database_connection().await?;
        let last_polling_url = sources
            .iter()
            .find(|s| s.source_type == SourceType::Polling)
            .unwrap()
            .url
            .clone();
        let rpc_client = HttpClient::new(last_polling_url.to_string().as_str())?;

        loop {
            indexer::index_historical_blocks(&name, &chain_id, &rpc_client, &db, &filters)
                .await
                .map_err(|err| {
                    error!("[{}] Failed to index historical blocks: {}", name, err);
                    err
                })?;
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        }
    });

    try_flat_join!(historical_indexer_handle)?;

    Ok(())
}

///
/// Run every configured indexer.
///
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

        let indexer_retry_strategy = retry_strategy.clone();
        let indexer_name = config.name.clone();
        let indexer_chain_id = config.chain_id.clone();
        let indexer_sources = config.sources.clone();
        let indexer_filters = config.filters.clone();
        let indexer_path = path.clone();
        let indexer_handle = tokio::spawn(async move {
            Retry::spawn(indexer_retry_strategy, || async {
                indexer::system::run(
                    &indexer_name,
                    &indexer_chain_id,
                    &indexer_sources,
                    &indexer_filters,
                )
                .await
                .map_err(|err| {
                    error!(
                        "Indexer {} ({}) crashed!",
                        indexer_name,
                        indexer_path.display()
                    );
                    error!("Error: {}", err);
                    error!("Retrying in 5 seconds...");

                    err
                })
            })
            .await?;

            Ok::<(), Report>(())
        });
        indexer_handles.push(indexer_handle);

        // If we have a historical source then we should run that indexer.
        let historical_retry_strategy = retry_strategy.clone();
        let historical_name = config.name.clone();
        let historical_chain_id = config.chain_id.clone();
        let historical_sources = config.sources.clone();
        let historical_filters = config.filters.clone();
        let historical_indexer_handle = tokio::spawn(async move {
            Retry::spawn(historical_retry_strategy, || async {
                indexer::system::run_historical(
                    &historical_name,
                    &historical_chain_id,
                    &historical_sources,
                    &historical_filters,
                )
                .await
                .map_err(|err| {
                    error!(
                        "Historical indexer {} ({}) crashed!",
                        config.name,
                        path.display()
                    );
                    error!("Error: {}", err);
                    error!("Retrying in 5 seconds...");

                    err
                })
            })
            .await?;

            Ok::<(), Report>(())
        });

        indexer_handles.push(historical_indexer_handle);
    }

    // Wait for all the indexers to finish.
    while let Some(indexer_handle) = indexer_handles.next().await {
        indexer_handle??;
    }

    Ok(())
}

///
/// Get a database connection based on the DATABASE_URL environment variable.
///
pub async fn get_database_connection() -> Result<DatabaseConnection> {
    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/croncat_indexer".to_string()
    });

    let mut opt = ConnectOptions::new(database_url);
    opt.max_connections(25)
        .min_connections(5)
        .connect_timeout(Duration::from_secs(8))
        .idle_timeout(Duration::from_secs(8))
        .max_lifetime(Duration::from_secs(8))
        .sqlx_logging(true)
        .sqlx_logging_level(log::LevelFilter::Info);

    Database::connect(opt).await.map_err(|err| err.into())
}
