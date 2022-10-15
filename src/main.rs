#![feature(type_alias_impl_trait)]

use color_eyre::Result;
use futures::{stream::FuturesUnordered, StreamExt};
use indexer::config::Config;
use tracing::{error, info, trace};

pub mod env;
pub mod indexer;
pub mod streams;

#[tokio::main]
async fn main() -> Result<()> {
    env::setup()?;

    let mut indexer_handles = FuturesUnordered::new();

    let configs = Config::get_configs_from_pwd()?;

    if configs.is_empty() {
        error!("No configs found in {}", std::env::current_dir()?.display());
        std::process::exit(1);
    }

    for (path, config) in Config::get_configs_from_pwd()? {
        info!("Starting indexer for {}", path.display());
        trace!("Configuration details: {:#?}", config);
        let indexer_handle = indexer::system::run(config);
        indexer_handles.push(indexer_handle);
    }

    while let Some(indexer_handle) = indexer_handles.next().await {
        indexer_handle?;
    }

    Ok(())
}
