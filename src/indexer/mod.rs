use std::time::Duration;

use color_eyre::{eyre::eyre, Result};
use sea_orm::entity::prelude::*;
use sea_orm::Set;
use tendermint_rpc::endpoint::tx;
use tendermint_rpc::query::Query;
use tendermint_rpc::{Client, HttpClient, Order};
use tokio::time::timeout;
use tokio_retry::strategy::{jitter, FibonacciBackoff};
use tokio_retry::Retry;
use tracing::trace;

// Sane aliases
use self::model::block::Model as DatabaseBlock;
use crate::streams::block::Block;
use model::block::ActiveModel as BlockModel;
use model::transaction::ActiveModel as TransactionModel;

// Tell clippy to ignore the generated model code.
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
        let log = serde_json::to_value(transaction.tx_result.log)?;

        Ok(Self {
            id: Set(Uuid::new_v4()),
            hash: Set(hash),
            block_id: Set(block_id),
            code: Set(code),
            height: Set(height),
            gas_wanted: Set(gas_wanted),
            gas_used: Set(gas_used),
            log: Set(log),
        })
    }
}

///
/// Index a block into the database.
///
pub async fn index_block(
    db: &DatabaseConnection,
    block: Block,
    rpc_client: &HttpClient,
) -> Result<()> {
    let block = BlockModel::from(block)
        .insert(db)
        .await
        .map_err(|e| eyre!("Failed to insert block: {}", e))?;

    if block.num_txs > 0 {
        let retry_strategy = FibonacciBackoff::from_millis(50).map(jitter).take(10);

        Retry::spawn(retry_strategy, || async {
            index_transactions_for_block(db, &block, rpc_client).await
        })
        .await?;
    }

    Ok(())
}

///
/// Get transactions from a block.
///
pub async fn index_transactions_for_block(
    db: &DatabaseConnection,
    block: &DatabaseBlock,
    rpc_client: &HttpClient,
) -> Result<()> {
    trace!("Fetching transactions for block {}", block.height);

    let poll_timeout_duration = Duration::from_secs(60);
    let mut found_txs = 0;
    let mut current_page = 0;

    while found_txs < block.num_txs {
        current_page += 1;

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

        if txs.is_empty() {
            return Err(eyre!(
                "No transactions found for block with transactions {}",
                block.height
            ));
        }

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
