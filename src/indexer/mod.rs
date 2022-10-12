use color_eyre::{eyre::eyre, Result};

use sea_orm::entity::prelude::*;
use sea_orm::Set;

#[allow(clippy::all)]
pub use model::{prelude::*, *};

mod model;

pub async fn index_block(db: &DatabaseConnection, block: crate::block::Block) -> Result<()> {
    // TODO: Need to have the block number be a u64 in the model.
    let height: u64 = block.header().height.into();
    let chain_id = block.header().chain_id.to_string();
    let time = block.header().time.to_rfc3339();
    let time = DateTime::parse_from_str(time.as_str(), "%Y-%m-%dT%H:%M:%S%.f%Z")?;

    let block = block::ActiveModel {
        height: Set(i32::try_from(height)?),
        chain_id: Set(chain_id),
        time: Set(time),
        num_txs: Set(0), // TODO: Probably don't need this field.
    };

    block
        .insert(db)
        .await
        .map_err(|e| eyre!("Failed to insert block: {}", e))?;

    Ok(())
}
