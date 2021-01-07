use lazy_static::lazy_static;
use primitives::{util::ApiUrl, BigNum};
use serde::{Deserialize, Deserializer};
use std::{collections::HashSet, fmt, str::FromStr, time::Duration};

lazy_static! {
    pub static ref DEVELOPMENT: Config = {
        toml::from_str(include_str!("../config/dev.toml"))
            .expect("Failed to parse dev.toml config file")
    };
    pub static ref PRODUCTION: Config = {
        toml::from_str(include_str!("../config/prod.toml"))
            .expect("Failed to parse prod.toml config file")
    };
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Environment {
    Development,
    Production,
}

impl fmt::Display for Environment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Environment::Development => write!(f, "development"),
            Environment::Production => write!(f, "production"),
        }
    }
}

impl FromStr for Environment {
    type Err = Error;

    fn from_str(environment: &str) -> Result<Self, Self::Err> {
        match environment {
            "development" => Ok(Environment::Development),
            "production" => Ok(Environment::Production),
            env => Err(Error::Environment {
                actual: env.to_string(),
            }),
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub validators: HashSet<ApiUrl>,
    #[serde(deserialize_with = "seconds_to_std_duration")]
    /// (Now - Recency) determines if a DateTime is recent or not
    pub recency: Duration,
    #[serde(deserialize_with = "seconds_to_std_duration")]
    pub fetch_campaigns_every: Duration,
    #[serde(deserialize_with = "seconds_to_std_duration")]
    pub update_campaigns_every: Duration,
    pub limits: Limits,
    pub market: Market,
    pub timeouts: Timeouts,
}

impl Config {
    pub fn new(config_path: Option<&str>, environment: Environment) -> Result<Config, Error> {
        match (config_path, environment) {
            (Some(path), _) => {
                let content = std::fs::read_to_string(path).map_err(Error::Io)?;

                toml::from_str(&content).map_err(Error::Toml)
            }
            (None, Environment::Development) => Ok(DEVELOPMENT.clone()),
            (None, Environment::Production) => Ok(PRODUCTION.clone()),
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct Market {
    /// Applied to:
    /// - The [`MarketApi`](crate::MarketApi) and it's `reqwest::Client` (see [`reqwest::ClientBuilder::tcp_keepalive`](reqwest::ClientBuilder::tcp_keepalive))
    /// - [`market::Proxy`](crate::market::Proxy) on the [`hyper::Client`](hyper::Client) (see [`hyper::client::Builder.html::http2_keep_alive_interval`](hyper::client::Builder.html::http2_keep_alive_interval))
    #[serde(deserialize_with = "seconds_to_std_duration")]
    pub keep_alive_interval: Duration,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Limits {
    #[serde(default)]
    pub limited_identity_earnings_limit: Option<BigNum>,
    pub max_channels_earning_from: u16,
    pub global_min_impression_price: BigNum,
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

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("File reading: {0}")]
    Io(#[from] std::io::Error),
    #[error("Toml parsing: {0}")]
    Toml(#[from] toml::de::Error),
    #[error(
        "Environment can only be {} or {}, actual: {actual}",
        Environment::Development,
        Environment::Production
    )]
    Environment { actual: String },
}

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
