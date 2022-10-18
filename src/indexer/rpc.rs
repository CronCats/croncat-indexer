use color_eyre::Result;
use tendermint::Block;
use tendermint_rpc::{
    endpoint::{block, tx, tx_search},
    query::Query,
    Client, HttpClient, Order,
};

use super::BlockError;

///
/// Wrap an RPC call with error handling.
///
#[macro_export]
macro_rules! rpc_call {
    ($client:expr, $method:ident) => {
        $client
            .$method()
            .await
            .map_err(|source| BlockError::TendermintError { source })
    };
    ($client:expr, $method:ident, $($args:expr),*) => {
        $client
            .$method($($args),*)
            .await
            .map_err(|source| BlockError::TendermintError { source })
    };
}

///
/// Get the latest block a given rpc client.
///
pub async fn get_latest_block(rpc_client: &HttpClient) -> Result<Block> {
    let block::Response { block, .. } = rpc_call!(rpc_client, latest_block)?;

    Ok(block)
}

///
/// Get a block at a given height from a given rpc client.
///
pub async fn get_block(rpc_client: &HttpClient, height: i64) -> Result<Block> {
    let block::Response { block, .. } = rpc_call!(rpc_client, block, height as u32)?;

    Ok(block)
}

///
/// Get transactions for a given block from a given rpc client.
///
pub async fn get_transactions_for_block(
    rpc_client: &HttpClient,
    height: i64,
    current_page: u32,
) -> Result<Vec<tx::Response>> {
    let tx_search::Response { txs, .. } = rpc_call!(
        rpc_client,
        tx_search,
        Query::eq("tx.height", height),
        true,
        current_page,
        100,
        Order::Ascending
    )?;

    Ok(txs)
}
