use std::time::Duration;

use color_eyre::{eyre::eyre, Result};
use sea_orm::entity::prelude::*;
use sea_orm::Set;
use tendermint::abci;
use tendermint_rpc::endpoint::tx;
use tendermint_rpc::query::Query;
use tendermint_rpc::{Client, HttpClient, Order};
use tokio::time::timeout;
use tokio_retry::strategy::{jitter, FibonacciBackoff};
use tokio_retry::Retry;
use tracing::trace;

use self::config::filter::Filter;

// Sane model aliases
use self::model::block::Model as DatabaseBlock;
use crate::streams::block::Block;
use model::block::ActiveModel as BlockModel;
use model::transaction::ActiveModel as TransactionModel;

// Tell clippy to ignore the generated model code.
pub mod config;
#[allow(clippy::all)]
pub mod model;
pub mod system;

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
    filters: &Vec<Filter>,
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
                    if message.contains("duplicate key value violates unique constraint") {
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
    filters: &Vec<Filter>,
    block: &DatabaseBlock,
) -> Result<()> {
    trace!("Fetching transactions for block {}", block.height);

    let poll_timeout_duration = Duration::from_secs(60);
    let mut found_txs = 0;
    let mut current_page = 0;

    // Handle pagination of transactions.
    while found_txs < block.num_txs {
        current_page += 1;

        // Get transactions for block from RPC.
        let txs = timeout(
            poll_timeout_duration,
            rpc_client.tx_search(
                Query::eq("tx.height", block.height),
                true,
                current_page,
                100,
                Order::Ascending,
            ),
        )
        .await?
        .map_err(|e| {
            eyre!(
                "Failed to get transactions for height {}: {}",
                block.height,
                e
            )
        })?
        .txs;

        // Filter transactions based on the provided filters.
        let txs = txs
            .into_iter()
            .filter(|tx| {
                let mut matches = true;
                for filter in filters {
                    if *filter != tx.tx_result.events {
                        matches = false;
                        break;
                    }
                }
                matches
            })
            .collect::<Vec<_>>();

        // Error if we didn't find any transactions, when we should have.
        if txs.is_empty() {
            return Err(eyre!(
                "No transactions found from RPC for block with transactions {}",
                block.height
            ));
        }

        // Insert transactions into the database.
        for tx in txs.iter() {
            let transaction = TransactionModel::from_response(block.id, tx.clone())?;
            transaction
                .insert(db)
                .await
                .map_err(|e| eyre!("Failed to insert transaction: {}", e))?;
            found_txs += 1;
        }
    }

    trace!(
        "Successfully inserted {} transactions for height {}",
        found_txs,
        block.height
    );

    Ok(())
}
