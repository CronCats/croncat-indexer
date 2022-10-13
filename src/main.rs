#![feature(type_alias_impl_trait)]

use color_eyre::Result;

pub mod env;
pub mod indexer;
pub mod streams;

#[tokio::main]
async fn main() -> Result<()> {
    env::setup()?;
    indexer::system::run().await
}
