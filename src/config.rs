use lazy_static::lazy_static;
use primitives::BigNum;
use serde::{Deserialize, Deserializer};
use std::collections::HashSet;
use std::fmt;
use std::time::Duration;
use url::Url;

lazy_static! {
    static ref DEVELOPMENT: Config = {
        toml::from_str(include_str!("../config/dev.toml"))
            .expect("Failed to parse dev.toml config file")
    };
    static ref PRODUCTION: Config = {
        toml::from_str(include_str!("../config/prod.toml"))
            .expect("Failed to parse prod.toml config file")
    };
}

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub validators: HashSet<Url>,
    #[serde(deserialize_with = "seconds_to_std_duration")]
    /// (Now - Recency) determines if a DateTime is recent or not
    pub recency: Duration,
    #[serde(deserialize_with = "seconds_to_std_duration")]
    pub fetch_campaigns_every: Duration,
    #[serde(deserialize_with = "seconds_to_std_duration")]
    pub update_campaigns_every: Duration,
    pub limits: Limits,
    pub timeouts: Timeouts,
}

impl Config {
    pub fn new(config_path: Option<&str>, environment: &str) -> Result<Config, Error> {
        if let Some(path) = config_path {
            let content = std::fs::read_to_string(path).map_err(Error::Io)?;
            return toml::from_str(&content).map_err(Error::Toml);
        }

        match environment {
            "development" => Ok(DEVELOPMENT.clone()),
            "production" => Ok(PRODUCTION.clone()),
            env => Err(Error::Environment {
                actual: env.to_string(),
            }),
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct Limits {
    #[serde(default)]
    pub limited_identity_earnings_limit: Option<BigNum>,
    pub maximum_publisher_earning_campaigns: u16,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Timeouts {
    #[serde(deserialize_with = "seconds_to_std_duration")]
    /// Timeout Duration for the Cache updating all campaigns statuses
    /// by querying the validators and etc.
    pub cache_update_campaign_statuses: Duration,
    #[serde(deserialize_with = "seconds_to_std_duration")]
    /// Timeout Duration for the Cache fetching new campaigns from the Market
    pub cache_fetch_campaigns_from_market: Duration,
    #[serde(deserialize_with = "seconds_to_std_duration")]
    /// Timeout for querying a single Validator endpoint
    pub validator_request: Duration,
}

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Toml(toml::de::Error),
    Environment { actual: String },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Error::*;

        match self {
            Io(err) => write!(f, "File reading: {}", err),
            Toml(err) => write!(f, "Toml parsing: {}", err),
            Environment { actual } => write!(
                f,
                "Environment can only be `development` or `production`, actual: {}",
                actual
            ),
        }
    }
}

impl std::error::Error for Error {}

fn seconds_to_std_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;
    use std::convert::TryFrom;
    use toml::Value;

    let toml_value: Value = Value::deserialize(deserializer)?;

    let seconds = match toml_value {
        Value::Integer(secs) => u64::try_from(secs).map_err(Error::custom),
        _ => Err(Error::custom("Only integers allowed for this value")),
    }?;

    Ok(Duration::from_secs(seconds))
}
