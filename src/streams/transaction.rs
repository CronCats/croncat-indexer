use std::{pin::Pin, time::Duration};

use async_stream::try_stream;
use color_eyre::{Report, Result};
use futures::{StreamExt, TryStream};
use sha2::{Digest, Sha256};
use snafu::Snafu;
use tendermint_rpc::{event::EventData, query::EventType, SubscriptionClient, WebSocketClient};
use tracing::info;

///
/// Block stream errors.
///
#[derive(Debug, Snafu)]
pub enum TransactionStreamError {
    #[snafu(display("Failed to connect to rpc endpoint: {}", source))]
    Connect { source: Report },
    #[snafu(display("Failed to subscribe to transaction events: {}", source))]
    Subscribe { source: Report },
    #[snafu(display("Transaction polling recv timed out after {timeout:?}"))]
    PollingTimeout { timeout: Duration },
    #[snafu(display("Transaction stream recv error {source}"))]
    TendermintError { source: tendermint_rpc::Error },
    #[snafu(display("Unexpected error {source}"))]
    UnexpectedError { source: Report },
}

///
/// Transaction wrapper
///
#[derive(Debug, Clone)]
pub struct Transaction {
    pub seq_index: String,
    pub index: i64,
    pub height: i64,
    pub hash: String,
    pub gas_wanted: Option<String>,
    pub gas_used: Option<String>,
    pub log: Option<String>,
}

impl From<tendermint_rpc::event::TxInfo> for Transaction {
    fn from(transaction: tendermint_rpc::event::TxInfo) -> Self {
        // Custom sequence index to be used for sorting
        let seq_index =
            transaction.height.to_string() + ":" + &transaction.index.unwrap_or(0).to_string();

        // Fields to retrieve from TxInfo
        let height = transaction.height;
        let index = transaction.index.unwrap_or(0);
        let gas_wanted = transaction.result.gas_wanted;
        let gas_used = transaction.result.gas_used;
        let log = transaction.result.log;

        // Calculate the hash of the transaction ourselves.
        let mut hasher = Sha256::new();
        hasher.update(transaction.tx);
        let hash = format!("{:X}", hasher.finalize());

        Self {
            seq_index,
            index,
            height,
            hash,
            gas_wanted,
            gas_used,
            log,
        }
    }
}

impl Ord for Transaction {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.seq_index.cmp(&other.seq_index)
    }
}

impl PartialOrd for Transaction {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Transaction {
    fn eq(&self, other: &Self) -> bool {
        self.seq_index == other.seq_index
    }
}

impl Eq for Transaction {}

type TransactionStream =
    Pin<Box<dyn TryStream<Item = Result<Transaction>, Ok = Transaction, Error = Report> + Send>>;

///
/// Stream blocks from the given rpc endpoint.
///
pub fn ws_transaction_stream(ws_rpc_host: &'static str) -> TransactionStream {
    Box::pin(try_stream! {
        let (client, driver) = WebSocketClient::new(ws_rpc_host).await.map_err(|source| TransactionStreamError::Connect { source: source.into() })?;
        let driver_handle = tokio::spawn(async move {
            driver.run().await
        });

        let mut subscription = client.subscribe(EventType::Tx.into()).await.map_err(|source| TransactionStreamError::Subscribe { source: source.into() })?;

        while let Some(event) = subscription.next().await
        {
            let event = event.map_err(|err| TransactionStreamError::TendermintError { source: err })?;
            let data = event.data;

            match data {
                EventData::Tx { tx_result, .. } => {
                    info!("Received tx_result at height {} and index {}", tx_result.height, tx_result.index.unwrap_or(0));
                    yield tx_result.into();
                },
                _ => continue,
            }
        }

        client.close().map_err(|source| TransactionStreamError::TendermintError { source })?;
        driver_handle.await??;
    })
}

// ///
// /// Stream polled blocks from the given rpc endpoint.
// ///

// pub fn poll_stream_transactions(
//     http_rpc_host: &'static str,
//     poll_duration_secs: u64,
// ) -> TransactionStream {
//     Box::pin(try_stream! {
//         let client = HttpClient::new(http_rpc_host).map_err(|source| TransactionStreamError::Connect { source: source.into() })?;
//         let poll_timeout_duration = Duration::from_secs(30);
//         loop {
//             let block = timeout(poll_timeout_duration, client.latest_block()).await.map_err(|_| TransactionStreamError::PollingTimeout { timeout: poll_timeout_duration })?;
//             let block = block.map_err(|source| TransactionStreamError::TendermintError { source })?.block;

//             let block_results = timeout(poll_timeout_duration, client.block_results(block.header().height)).await.map_err(|_| TransactionStreamError::PollingTimeout { timeout: poll_timeout_duration })?;
//             let txs_results = block_results.map_err(|source| TransactionStreamError::TendermintError { source })?.txs_results;

//             if txs_results.is_none() {
//                 info!("No transactions at height {}", block.header().height);
//             } else {
//                 for (index, tx_result) in txs_results.unwrap().iter().enumerate() {
//                     info!("Received tx_result at height {} and index {}", block.header().height, index);
//                     yield tx_result.into();
//                 }
//             }

//             // yield block..into();
//             tokio::time::sleep(Duration::from_secs(poll_duration_secs)).await;
//             info!("Polled transactions at height {}", block.header().height);
//         }
//     })
// }
