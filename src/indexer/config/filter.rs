use std::ops::Deref;

use color_eyre::Report;
use regex::Regex;
use serde::{Deserialize, Serialize};
use tendermint::abci;

#[derive(Debug, Clone)]
/// Filter a field by a regex.
pub struct FilterPattern(Regex);

impl Deref for FilterPattern {
    type Target = Regex;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<&str> for FilterPattern {
    type Error = Report;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(Self(Regex::new(value)?))
    }
}

impl Serialize for FilterPattern {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.0.as_str())
    }
}

impl PartialEq for FilterPattern {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for FilterPattern {}

impl<'de> Deserialize<'de> for FilterPattern {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let regex = Regex::new(&s).map_err(serde::de::Error::custom)?;
        Ok(Self(regex))
    }
}

/// Attributes to filter by.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttributeFilter {
    pub key: FilterPattern,
    pub value: Option<FilterPattern>,
}

/// A filter is a set of rules that determine which data is indexed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Filter {
    #[serde(alias = "type", rename = "type")]
    pub type_str: FilterPattern,
    pub attributes: Vec<AttributeFilter>,
}

impl PartialEq<Vec<abci::Event>> for Filter {
    fn eq(&self, other: &Vec<abci::Event>) -> bool {
        let mut matches = 0;
        for event in other {
            if self.type_str.is_match(event.type_str.as_str()) {
                matches += 1;
                for attribute in &event.attributes {
                    for filter in &self.attributes {
                        if filter.key.is_match(attribute.key.to_string().as_str()) {
                            if let Some(value) = &filter.value {
                                if value.is_match(attribute.value.to_string().as_str()) {
                                    matches += 1;
                                }
                            } else {
                                matches += 1;
                            }
                        }
                    }
                }
            }
        }
        matches == self.attributes.len() + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_pattern_try_from() {
        let filter_pattern = FilterPattern::try_from(".*").unwrap();
        assert_eq!(filter_pattern.as_str(), ".*");

        assert!(FilterPattern::try_from("*.").is_err());
    }

    #[test]
    fn filter_pattern_serialize() {
        let filter_pattern = FilterPattern::try_from(".*").unwrap();
        let yaml = serde_yaml::to_string(&filter_pattern).unwrap();
        assert_eq!(yaml.trim(), r".*");
    }

    #[test]
    fn filter_pattern_deserialize() {
        let yaml = r".*";
        let filter_pattern: FilterPattern = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(filter_pattern.as_str(), r".*");
    }

    #[test]
    fn filter_pattern_eq() {
        let filter_pattern1 = FilterPattern::try_from(".*").unwrap();
        let filter_pattern2 = FilterPattern::try_from(".*").unwrap();
        assert_eq!(filter_pattern1, filter_pattern2);
    }

    #[test]
    fn attribute_filter_serialize() {
        let attribute_filter = AttributeFilter {
            key: FilterPattern::try_from(".*").unwrap(),
            value: Some(FilterPattern::try_from(".*").unwrap()),
        };
        let yaml = serde_yaml::to_string(&attribute_filter).unwrap();
        assert_eq!(
            yaml,
            indoc::indoc! {r#"
                key: .*
                value: .*
            "#}
        );
    }

    #[test]
    fn attribute_filter_deserialize() {
        let yaml = indoc::indoc! {r#"
            key: .*
            value: .*
        "#};

        let attribute_filter: AttributeFilter = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(attribute_filter.key.as_str(), ".*");
        assert_eq!(attribute_filter.value.unwrap().as_str(), ".*");
    }

    #[test]
    fn attribute_filter_eq() {
        let attribute_filter1 = AttributeFilter {
            key: FilterPattern::try_from(".*").unwrap(),
            value: Some(FilterPattern::try_from(".*").unwrap()),
        };
        let attribute_filter2 = AttributeFilter {
            key: FilterPattern::try_from(".*").unwrap(),
            value: Some(FilterPattern::try_from(".*").unwrap()),
        };
        assert_eq!(attribute_filter1, attribute_filter2);
    }

    #[test]
    fn filter_serialize() {
        let filter = Filter {
            type_str: FilterPattern::try_from(".*").unwrap(),
            attributes: vec![AttributeFilter {
                key: FilterPattern::try_from(".*").unwrap(),
                value: Some(FilterPattern::try_from(".*").unwrap()),
            }],
        };
        let yaml = serde_yaml::to_string(&filter).unwrap();
        assert_eq!(
            yaml,
            indoc::indoc! {r#"
                type: .*
                attributes:
                - key: .*
                  value: .*
            "#}
        );
    }

    #[test]
    fn filter_deserialize() {
        let yaml = indoc::indoc! {r#"
            type: .*
            attributes:
            - key: .*
              value: .*
        "#};

        let filter: Filter = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(filter.type_str.as_str(), ".*");
        assert_eq!(filter.attributes[0].key.as_str(), ".*");
        assert_eq!(filter.attributes[0].value.as_ref().unwrap().as_str(), ".*");
    }
}
