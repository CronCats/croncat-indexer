use std::{
    fmt::{self, Formatter},
    fs::File,
    path::PathBuf,
};

use color_eyre::{eyre::eyre, Report, Result};
use convert_case::{Case, Casing};
use enum_display::EnumDisplay;
use filter::Filter;
use serde::{Deserialize, Serialize};
use url::Url;

pub mod filter;

/// Source type for indexing.
#[derive(Debug, Copy, Clone, EnumDisplay, PartialEq, Eq, Serialize, Deserialize)]
#[enum_display(case = "Kebab")]
pub enum SourceType {
    /// A websocket source.
    #[serde(alias = "ws", rename = "websocket")]
    Websocket,
    /// A polling source.
    #[serde(alias = "http", rename = "polling")]
    Polling,
}

/// A data source for indexing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Source {
    /// The name of the source.
    pub name: String,
    /// The type of the source.
    #[serde(alias = "type", rename = "type")]
    pub source_type: SourceType,
    /// The URL of the source.
    pub url: Url,
}

impl Source {
    /// Create a new source.
    #[allow(dead_code)]
    pub fn new(
        name: impl Into<String>,
        source_type: SourceType,
        url: impl Into<String>,
    ) -> Result<Self> {
        Ok(Self {
            name: name.into().to_case(Case::Kebab),
            source_type,
            url: Url::parse(url.into().as_str())?,
        })
    }
}

impl fmt::Display for Source {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let port = match self.url.port() {
            Some(port) => port,
            None => match self.url.scheme() {
                "http" => 80,
                "https" => 443,
                "ws" => 80,
                "wss" => 443,
                _ => 80,
            },
        };

        write!(
            f,
            "{}-{}-{}:{}",
            self.source_type,
            self.name,
            self.url.host().unwrap(),
            self.url.port().unwrap_or(port)
        )
    }
}

impl TryFrom<PathBuf> for Config {
    type Error = Report;

    fn try_from(path: PathBuf) -> Result<Self> {
        let file = File::open(path)?;
        let config: Self = serde_yaml::from_reader(file)?;
        Ok(config)
    }
}

/// Configuration for the indexer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    /// The sources to index.
    pub name: String,
    /// The chain id of the chain to index.
    #[serde(alias = "chain-id")]
    pub chain_id: String,
    /// The sources to index from.
    pub sources: Vec<Source>,
    /// The filters to apply to the sources.
    pub filters: Vec<Filter>,
}

impl Config {
    pub fn get_configs_from_pwd() -> Result<Vec<(PathBuf, Self)>> {
        glob::glob("./*.config.yaml")
            .unwrap()
            .map(|path| {
                let path = path?;
                let config = Self::try_from(path.clone())
                    .map_err(|err| eyre!("Invalid configuration {}: {}", path.display(), err))?;
                Ok((path, config))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use indoc::indoc;

    use super::*;
    use crate::indexer::config::filter::AttributeFilter;

    #[test]
    fn source_new() {
        let source =
            Source::new("Test Source", SourceType::Polling, "http://localhost:26657").unwrap();
        assert_eq!(source.name, "test-source");
        assert_eq!(source.source_type, SourceType::Polling);
        assert_eq!(source.url, Url::parse("http://localhost:26657").unwrap());
    }

    #[test]
    fn source_display() {
        let source = Source::new(
            "Block Stream",
            SourceType::Websocket,
            "wss://juno-testnet-rpc.polkachu.com/websocket",
        )
        .unwrap();

        assert_eq!(
            source.to_string(),
            "websocket-block-stream-juno-testnet-rpc.polkachu.com:443"
        );
    }

    #[test]
    fn source_serialize() {
        let source = Source::new(
            "Block Stream",
            SourceType::Websocket,
            "wss://juno-testnet-rpc.polkachu.com/websocket",
        )
        .unwrap();

        let expected = indoc! { r#"
            name: block-stream
            type: websocket
            url: wss://juno-testnet-rpc.polkachu.com/websocket
        "# };

        assert_eq!(serde_yaml::to_string(&source).unwrap(), expected);
    }

    #[test]
    fn source_deserialize() {
        let source = Source::new(
            "Block Stream",
            SourceType::Websocket,
            "wss://juno-testnet-rpc.polkachu.com/websocket",
        )
        .unwrap();

        let expected = indoc! { r#"
            name: block-stream
            type: websocket
            url: wss://juno-testnet-rpc.polkachu.com/websocket
        "# };

        assert_eq!(serde_yaml::from_str::<Source>(expected).unwrap(), source);
    }

    #[test]
    fn config_serialize() {
        let config = Config {
            name: "test".to_string(),
            chain_id: "uni-5".to_string(),
            sources: vec![Source::new(
                "Block Stream",
                SourceType::Websocket,
                "wss://juno-testnet-rpc.polkachu.com/websocket",
            )
            .unwrap()],
            filters: vec![Filter {
                type_str: "message".try_into().unwrap(),
                attributes: vec![AttributeFilter {
                    key: "action".try_into().unwrap(),
                    value: Some("MsgExecuteContract".try_into().unwrap()),
                }],
            }],
        };

        let yaml = serde_yaml::to_string(&config).unwrap();

        assert_eq!(
            yaml.trim(),
            indoc! {r#"
                name: test
                chain_id: uni-5
                sources:
                - name: block-stream
                  type: websocket
                  url: wss://juno-testnet-rpc.polkachu.com/websocket
                filters:
                - type: message
                  attributes:
                  - key: action
                    value: MsgExecuteContract            
            "#}
            .trim()
        )
    }

    #[test]
    fn config_deserialize() {
        let yaml = indoc! {r#"
            name: test
            chain_id: uni-5
            sources:
            - name: block-stream
              type: websocket
              url: wss://juno-testnet-rpc.polkachu.com/websocket
            filters:
            - type: message
              attributes:
              - key: action
                value: MsgExecuteContract            
        "#};

        let config: Config = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(
            config,
            Config {
                name: "test".to_string(),
                chain_id: "uni-5".to_string(),
                sources: vec![Source::new(
                    "Block Stream",
                    SourceType::Websocket,
                    "wss://juno-testnet-rpc.polkachu.com/websocket",
                )
                .unwrap()],
                filters: vec![Filter {
                    type_str: "message".try_into().unwrap(),
                    attributes: vec![AttributeFilter {
                        key: "action".try_into().unwrap(),
                        value: Some("MsgExecuteContract".try_into().unwrap()),
                    }],
                }],
            }
        )
    }
}
