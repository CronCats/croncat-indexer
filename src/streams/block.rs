use std::{pin::Pin, time::Duration};

use async_stream::try_stream;
use color_eyre::{Report, Result};
use delegate::delegate;
use futures::{StreamExt, TryStream};
use tendermint_rpc::{
    event::EventData, query::EventType, HttpClient, SubscriptionClient, WebSocketClient,
};
use tokio::time::timeout;
use tracing::trace;

use crate::indexer::{rpc, BlockError};

///
/// Block wrapper
///
#[derive(Debug, Clone)]
pub struct Block {
    pub inner: tendermint::Block,
}

#[allow(dead_code)]
impl Block {
    delegate! {
        to self.inner {
            pub fn header(&self) -> &tendermint::block::Header;
            pub fn data(&self) -> &tendermint::abci::transaction::Data;
        }
    }
}

impl From<tendermint::Block> for Block {
    fn from(block: tendermint::Block) -> Self {
        Self { inner: block }
    }
}

impl Ord for Block {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.header().height.cmp(&other.header().height)
    }
}

impl PartialOrd for Block {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Block {
    fn eq(&self, other: &Self) -> bool {
        self.header().height == other.header().height
    }
}

impl Eq for Block {}

type BlockStream = Pin<Box<dyn TryStream<Item = Result<Block>, Ok = Block, Error = Report> + Send>>;

///
/// Stream blocks from the given rpc endpoint.
///
pub fn ws_block_stream(ws_rpc_host: String) -> BlockStream {
    Box::pin(try_stream! {
        let (client, driver) = WebSocketClient::new(ws_rpc_host.as_str()).await.map_err(|source| BlockError::Connect { source: source.into() })?;
        let driver_handle = tokio::spawn(async move {
            driver.run().await
        });

        let mut subscription = client.subscribe(EventType::NewBlock.into()).await.map_err(|source| BlockError::Subscribe { source: source.into() })?;

        let recv_timeout_duration = Duration::from_secs(60);
        while let Some(event) =
            timeout(recv_timeout_duration, subscription.next())
            .await
            .map_err(|_| BlockError::Timeout { timeout: recv_timeout_duration })?
        {
            let event = event.map_err(|err| BlockError::TendermintError { source: err })?;
            let data = event.data;

            match data {
                EventData::NewBlock { block, .. } => {
                    let block = block.ok_or_else(|| BlockError::EventWithoutBlock)?;
                    trace!("Received block {} ({})", block.header().height, block.header().chain_id);
                    yield block.into();
                },
                _ => continue,
            }
        }

        client.close().map_err(|source| BlockError::TendermintError { source })?;
        driver_handle.await??;
    })
}

///
/// Stream polled blocks from the given rpc endpoint.
///
pub fn poll_stream_blocks(http_rpc_host: String, poll_duration_secs: u64) -> BlockStream {
    Box::pin(try_stream! {
        let client = HttpClient::new(http_rpc_host.as_str()).map_err(|source| BlockError::Connect { source: source.into() })?;

        let poll_timeout_duration = Duration::from_secs(30);
        loop {
            let block = timeout(poll_timeout_duration, rpc::get_latest_block(&client))
                .await
                .map_err(|_| BlockError::Timeout { timeout: poll_timeout_duration })??;
            trace!("Polled block {} ({})", block.header().height, block.header().chain_id);
            yield block.into();
            tokio::time::sleep(Duration::from_secs(poll_duration_secs)).await;
        }
    })
}
