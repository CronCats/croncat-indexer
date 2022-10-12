use color_eyre::{eyre::eyre, Result};

use sea_orm::entity::prelude::*;
use sea_orm::Set;

pub use model::{prelude::*, *};

#[allow(clippy::all)]
mod model;

pub async fn index_block(db: &DatabaseConnection, block: crate::block::Block) -> Result<()> {
    let height: i64 = block.header().height.into();
    let chain_id = block.header().chain_id.to_string();
    let time = block.header().time.to_rfc3339();
    let time = DateTime::parse_from_str(time.as_str(), "%Y-%m-%dT%H:%M:%S%.f%Z")?;
    let hash = block.header().hash().to_string();
    let num_txs = block.data().as_ref().iter().count() as i64;

    let block = block::ActiveModel {
        height: Set(height),
        chain_id: Set(chain_id),
        time: Set(time),
        hash: Set(hash),
        num_txs: Set(num_txs), // TODO: Probably don't need this field.
    };

    block
        .insert(db)
        .await
        .map_err(|e| eyre!("Failed to insert block: {}", e))?;

    Ok(())
}
