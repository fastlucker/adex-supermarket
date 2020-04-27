use lazy_static::lazy_static;
use primitives::BigNum;
use serde::{Deserialize, Serialize};
use time::Duration;

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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Config {
    /// (Now - Recency) determins if a Datetime is recent or not
    pub recency: Duration,
    pub limited_identity_earnings_limit: BigNum,
    pub cache_fetch_campaigns_every: Duration,
    pub cache_update_campaigns_every: Duration,
    pub timeouts: Timeouts,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Timeouts {
    cache_update_campaign_statuses: Duration,
    cache_fetch_campaigns_from_market: Duration,
    validator_request: Duration,
}
