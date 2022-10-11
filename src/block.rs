use std::{pin::Pin, time::Duration};

use async_stream::try_stream;
use color_eyre::{eyre::eyre, Report, Result};
use delegate::delegate;
use futures::{pin_mut, StreamExt, TryStream, TryStreamExt};
use snafu::Snafu;
use tendermint_rpc::{
    event::EventData, query::EventType, Client, HttpClient, SubscriptionClient, WebSocketClient,
};
use tokio::{sync::broadcast, time::timeout};

use tracing::{info, trace};

///
/// Block stream errors.
///
#[derive(Debug, Snafu)]
pub enum BlockStreamError {
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
pub fn ws_block_stream(ws_rpc_host: &'static str) -> BlockStream {
    Box::pin(try_stream! {
        let (client, driver) = WebSocketClient::new(ws_rpc_host).await.map_err(|source| BlockStreamError::Connect { source: source.into() })?;
        let driver_handle = tokio::spawn(async move {
            driver.run().await
        });

        let mut subscription = client.subscribe(EventType::NewBlock.into()).await.map_err(|source| BlockStreamError::Subscribe { source: source.into() })?;

        let recv_timeout_duration = Duration::from_secs(30);
        while let Some(event) =
            timeout(recv_timeout_duration, subscription.next())
            .await
            .map_err(|_| BlockStreamError::Timeout { timeout: recv_timeout_duration })?
        {
            let event = event.map_err(|err| BlockStreamError::TendermintError { source: err })?;
            let data = event.data;

            match data {
                EventData::NewBlock { block, .. } => {
                    let block = block.ok_or_else(|| BlockStreamError::EventWithoutBlock)?;
                    trace!("Received block {}", block.header().height);
                    yield block.into();
                },
                _ => continue,
            }
        }

        client.close().map_err(|source| BlockStreamError::TendermintError { source })?;
        driver_handle.await??;
    })
}

///
/// Stream polled blocks from the given rpc endpoint.
///

pub fn poll_stream_blocks(http_rpc_host: &'static str, poll_duration_secs: u64) -> BlockStream {
    Box::pin(try_stream! {
        let client = HttpClient::new(http_rpc_host).map_err(|source| BlockStreamError::Connect { source: source.into() })?;
        let poll_timeout_duration = Duration::from_secs(30);
        loop {
            let block = timeout(poll_timeout_duration, client.latest_block()).await.map_err(|_| BlockStreamError::Timeout { timeout: poll_timeout_duration })?;
            let block = block.map_err(|source| BlockStreamError::TendermintError { source })?.block;
            yield block.clone().into();
            tokio::time::sleep(Duration::from_secs(poll_duration_secs)).await;
            trace!("Polled block {}", block.header().height);
        }
    })
}

///
/// Consume a stream of blocks from a given rpc endpoint and print them.
///
#[allow(dead_code)]
pub async fn stream_blocks(
    block_stream: impl TryStream<Item = Result<Block>>,
    incoming_blocks_tx: &broadcast::Sender<Block>,
) -> Result<()> {
    pin_mut!(block_stream);

    while let Some(block) = block_stream
        .try_next()
        .await
        .map_err(|err| eyre!("Block stream failed: {}", err))?
    {
        info!(
            "Received block: {:?} {:?}",
            block.header().height,
            block.header().time
        );

        incoming_blocks_tx
            .send(block)
            .map_err(|err| eyre!("Failed to send block: {}", err))?;
    }

    Ok(())
}
