use std::time::Duration;

use color_eyre::Report;
use color_eyre::{eyre::eyre, Result};
use sea_orm::entity::prelude::*;
use sea_orm::Set;
use snafu::Snafu;
use tendermint::abci;
use tendermint_rpc::endpoint::tx;
use tendermint_rpc::HttpClient;
use tokio::time::timeout;
use tokio_retry::strategy::{jitter, FibonacciBackoff};
use tokio_retry::Retry;
use tracing::{info, trace};

use self::config::filter::Filter;
use self::historical::get_block_gaps;
use crate::streams::block::Block;
// Sane model aliases
use self::model::block::Model as DatabaseBlock;
use model::block::ActiveModel as BlockModel;
use model::transaction::ActiveModel as TransactionModel;

pub mod config;
pub mod historical;
#[allow(clippy::all)]
pub mod model; // Tell clippy to ignore the generated model code.
pub mod rpc;
pub mod system;

///
/// Block errors.
///
#[derive(Debug, Snafu)]
pub enum BlockError {
    #[snafu(display("Failed to connect to rpc endpoint: {}", source))]
    Connect { source: Report },
    #[snafu(display("Failed to subscribe to block events: {}", source))]
    Subscribe { source: Report },
    #[snafu(display("Block stream recv timed out after {timeout:?}"))]
    Timeout { timeout: Duration },
    #[snafu(display("Received event without block"))]
    EventWithoutBlock,
    #[snafu(display("Block stream recv error {source}"))]
    TendermintError { source: tendermint_rpc::Error },
    #[snafu(display("Unexpected error {source}"))]
    UnexpectedError { source: Report },
}

///
/// Create a block database entry from a block.
///
impl From<Block> for BlockModel {
    fn from(block: Block) -> Self {
        let height: i64 = block.header().height.into();
        let chain_id = block.header().chain_id.to_string();
        let time = DateTime::parse_from_str(
            block.header().time.to_rfc3339().as_str(),
            "%Y-%m-%dT%H:%M:%S%.f%Z",
        )
        .unwrap();
        let hash = block.header().hash().to_string();
        let num_txs = block.data().as_ref().iter().count() as i64;

        Self {
            id: Set(Uuid::new_v4()),
            height: Set(height),
            chain_id: Set(chain_id),
            time: Set(time),
            hash: Set(hash),
            num_txs: Set(num_txs),
        }
    }
}

///
/// Create a transaction database entry from a transaction.
///
impl TransactionModel {
    ///
    /// Convert a transaction into a database entry.
    ///
    fn from_response(block_id: Uuid, transaction: tx::Response) -> Result<Self> {
        let hash = transaction.hash.to_string();
        let code = transaction.tx_result.code.value() as i32;
        let height = transaction.height.value() as i64;
        let gas_wanted = transaction.tx_result.gas_wanted.to_string();
        let gas_used = transaction.tx_result.gas_used.to_string();
        let events = Self::decode_events(transaction.tx_result.events)?;
        let log = transaction.tx_result.log.to_string();
        let info = transaction.tx_result.info.to_string();

        Ok(Self {
            id: Set(Uuid::new_v4()),
            hash: Set(hash),
            block_id: Set(block_id),
            code: Set(code),
            height: Set(height),
            gas_wanted: Set(gas_wanted),
            gas_used: Set(gas_used),
            events: Set(events),
            log: Set(log),
            info: Set(info),
        })
    }

    ///
    /// Decode events from a transaction.
    ///
    fn decode_events(events: Vec<abci::Event>) -> Result<serde_json::Value> {
        let mut decoded_events = Vec::new();
        for event in events {
            let decoded_attributes: Vec<serde_json::Value> = event
                .attributes
                .iter()
                .map(|attribute| {
                    let mut map = serde_json::Map::new();
                    map.insert(
                        "key".to_string(),
                        serde_json::Value::String(attribute.key.to_string()),
                    );
                    map.insert(
                        "value".to_string(),
                        serde_json::Value::String(attribute.value.to_string()),
                    );
                    serde_json::Value::Object(map)
                })
                .collect();
            let mut map = serde_json::Map::new();
            map.insert(
                "type".to_string(),
                serde_json::Value::String(event.type_str),
            );
            map.insert(
                "attributes".to_string(),
                serde_json::Value::Array(decoded_attributes),
            );
            decoded_events.push(serde_json::Value::Object(map));
        }
        Ok(serde_json::Value::Array(decoded_events))
    }
}

///
/// Index a block into the database.
///
pub async fn index_block(
    db: &DatabaseConnection,
    rpc_client: &HttpClient,
    filters: &[Filter],
    block: Block,
) -> Result<()> {
    let block_insert_result = BlockModel::from(block).insert(db).await;

    match block_insert_result {
        Ok(block) => {
            // If we have transactions to index, do so.
            if block.num_txs > 0 {
                let retry_strategy = FibonacciBackoff::from_millis(50).map(jitter).take(15);

                // Retry the transaction query up to 10 times.
                Retry::spawn(retry_strategy, || async {
                    index_transactions_for_block(db, rpc_client, filters, &block).await
                })
                .await?;
            }
        }
        Err(err) => {
            match err {
                // If the block already exists, we can safely ignore the error.
                DbErr::Query(message) => {
                    if message
                        .to_string()
                        .contains("duplicate key value violates unique constraint")
                    {
                        trace!("Block already exists in database, skipping");
                    } else {
                        return Err(eyre!("Failed to insert block: {}", message));
                    }
                }
                // Otherwise we should bubble up the error.
                _ => return Err(err.into()),
            }
        }
    }

    Ok(())
}

///
/// Get transactions from a block.
///
pub async fn index_transactions_for_block(
    db: &DatabaseConnection,
    rpc_client: &HttpClient,
    filters: &[Filter],
    block: &DatabaseBlock,
) -> Result<()> {
    trace!("Fetching transactions for block {}", block.height);

    let poll_timeout_duration = Duration::from_secs(60);
    let mut found_txs = 0;
    let mut current_page = 0;

    let mut txs = vec![];

    // Handle pagination of transactions.
    while found_txs < block.num_txs {
        current_page += 1;

        // Get transactions for block from RPC.
        let page_txs = timeout(
            poll_timeout_duration,
            rpc::get_transactions_for_block(rpc_client, block.height, current_page),
        )
        .await?
        .map_err(|e| {
            eyre!(
                "Failed to get transactions for height {}: {}",
                block.height,
                e
            )
        })?;

        // Error if we didn't find any transactions, when we should have.
        if page_txs.is_empty() {
            return Err(eyre!(
                "No transactions found from RPC for block with transactions {}",
                block.height
            ));
        }

        found_txs += page_txs.len() as i64;

        txs.extend(page_txs);
    }

    // Filter transactions based on the provided filters.
    let txs = txs
        .into_iter()
        .filter(|tx| {
            let mut matches = 0;
            for filter in filters {
                if filter.matches(tx) {
                    matches += 1
                }
            }
            matches == filters.len()
        })
        .collect::<Vec<_>>();

    // Insert transactions into the database.
    for tx in txs.iter() {
        let transaction = TransactionModel::from_response(block.id, tx.clone())?;
        transaction
            .insert(db)
            .await
            .map_err(|e| eyre!("Failed to insert transaction: {}", e))?;
    }

    trace!(
        "Successfully inserted {} transactions for height {}",
        found_txs,
        block.height
    );

    Ok(())
}

///
/// Index historical blocks into the database.
///
pub async fn index_historical_blocks(
    name: &str,
    chain_id: &str,
    rpc_client: &HttpClient,
    db: &DatabaseConnection,
    filters: &[Filter],
) -> Result<()> {
    let gaps = get_block_gaps(db, chain_id.to_string(), 7).await?;

    if gaps.is_empty() {
        info!("No gaps found, skipping historical block indexing");
        return Ok(());
    }

    info!(
        "Found {} gaps in block history for {} ({})",
        name,
        chain_id,
        gaps.len()
    );

    for gap in gaps {
        for range in gap {
            let (start, end) = *range;
            info!("Indexing gap blocks from {} to {}", start, end);
            let block = rpc::get_block(rpc_client, start).await?;
            index_block(db, rpc_client, filters, block.into()).await?;
        }
    }

    Ok(())
}
