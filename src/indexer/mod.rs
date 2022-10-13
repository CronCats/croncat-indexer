use color_eyre::{eyre::eyre, Result};
use sea_orm::entity::prelude::*;
use sea_orm::Set;

// Sane aliases
use crate::streams::block::Block;
use crate::streams::transaction::Transaction;
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
impl From<Transaction> for TransactionModel {
    fn from(transaction: Transaction) -> Self {
        Self {
            id: Set(Uuid::new_v4()),
            hash: Set(transaction.hash.to_string()),
            height: Set(transaction.height),
            gas_wanted: Set(transaction.gas_wanted),
            gas_used: Set(transaction.gas_used),
            log: Set(transaction.log.map(|l| serde_json::from_str(&l).unwrap())),
        }
    }
}

///
/// Index a block into the database.
///
pub async fn index_block(db: &DatabaseConnection, block: Block) -> Result<()> {
    BlockModel::from(block)
        .insert(db)
        .await
        .map_err(|e| eyre!("Failed to insert block: {}", e))?;

    Ok(())
}

///
/// Index a transaction into the database.
///
pub async fn index_transaction(db: &DatabaseConnection, transaction: Transaction) -> Result<()> {
    TransactionModel::from(transaction)
        .insert(db)
        .await
        .map_err(|e| eyre!("Failed to insert transaction: {}", e))?;

    Ok(())
}
