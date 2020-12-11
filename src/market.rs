use primitives::{
    market::{Campaign, StatusType},
    supermarket::units_for_slot::response::AdUnit,
    AdSlot,
    util::ApiUrl,
};
use reqwest::{Client, Error, StatusCode};
use serde::{Deserialize, Serialize};
use slog::{info, Logger};
use std::fmt;
use url::Url;

pub type MarketUrl = ApiUrl;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct MarketApi {
    pub market_url: MarketUrl,
    client: Client,
    logger: Logger,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", from = "ad_slot::ShimResponse", into = "ad_slot::ShimResponse")]
pub struct AdSlotResponse {
    pub slot: AdSlot,
    pub accepted_referrers: Vec<Url>,
    pub categories: Vec<String>,
    pub alexa_rank: Option<f64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdUnitResponse {
    pub unit: AdUnit,
}

/// Should we query All or only certain statuses
#[derive(Debug)]
pub enum Statuses<'a> {
    All,
    Only(&'a [StatusType]),
}

impl fmt::Display for Statuses<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Statuses::*;

        match self {
            All => write!(f, "all"),
            Only(statuses) => {
                let statuses = statuses.iter().map(ToString::to_string).collect::<Vec<_>>();

                write!(f, "status={}", statuses.join(","))
            }
        }
    }
}

impl MarketApi {
    /// The limit of Campaigns per page when fetching
    /// Limit the value to MAX(500)
    const MARKET_CAMPAIGNS_LIMIT: u64 = 500;
    /// The limit of AdUnits per page when fetching
    /// It should always be > 1
    const MARKET_AD_UNITS_LIMIT: u64 = 1_000;

    pub fn new(market_url: MarketUrl, logger: Logger) -> Result<Self> {
        // @TODO: maybe add timeout?
        let client = Client::builder().build()?;

        Ok(Self {
            market_url,
            client,
            logger,
        })
    }

    /// ipfs: ipfs hash
    /// Handles the 404 case, returning a None, instead of Error
    pub async fn fetch_slot(&self, ipfs: &str) -> Result<Option<AdSlotResponse>> {
        let url = self
            .market_url
            .join(&format!("slots/{}", ipfs))
            .expect("Wrong Market Url for /slots/{IPFS} endpoint");

        let response = self.client.get(url).send().await?;
        if StatusCode::NOT_FOUND == response.status() {
            Ok(None)
        } else {
            let ad_slot_response = response.json::<AdSlotResponse>().await?;
            Ok(Some(ad_slot_response))
        }
    }

    pub async fn fetch_unit(&self, ipfs: &str) -> Result<Option<AdUnitResponse>> {
        let url = self
            .market_url
            .join(&format!("units/{}", ipfs))
            .expect("Wrong Market Url for /units/{IPFS} endpoint");

        match self.client.get(url).send().await?.error_for_status() {
            Ok(response) => {
                let ad_unit_response = response.json::<AdUnitResponse>().await?;

                Ok(Some(ad_unit_response))
            }
            // if we have a `404 Not Found` error, return None
            Err(err) if err.status() == Some(StatusCode::NOT_FOUND) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub async fn fetch_units(&self, ad_slot: &AdSlot) -> Result<Vec<AdUnit>> {
        let mut units = Vec::new();
        let mut skip: u64 = 0;
        let limit = Self::MARKET_AD_UNITS_LIMIT;

        loop {
            // if one page fail, simply return the error for now
            let mut page_results = self.fetch_units_page(&ad_slot.ad_type, skip).await?;
            // get the count before appending the page results to all
            let count = page_results.len() as u64;

            // append all received units
            units.append(&mut page_results);
            // add the number of results we need to skip in the next iteration
            skip += count;

            // if the Market returns < market fetch limit
            // we've got all AdSlots from all pages!
            if count < limit {
                // so break out of the loop
                break;
            }
        }

        Ok(units)
    }

    /// `skip` - how many records it should skip (pagination)
    async fn fetch_units_page(&self, ad_type: &str, skip: u64) -> Result<Vec<AdUnit>> {
        let url = self
            .market_url
            .join(&format!(
                "units?limit={}&skip={}&type={}",
                Self::MARKET_AD_UNITS_LIMIT,
                skip,
                ad_type,
            ))
            .expect("Wrong Market Url for /units endpoint");

        let response = self.client.get(url).send().await?;

        let ad_units: Vec<AdUnit> = response.json().await?;

        Ok(ad_units)
    }

    pub async fn fetch_campaigns(&self, statuses: &Statuses<'_>) -> Result<Vec<Campaign>> {
        let mut campaigns = Vec::new();
        let mut skip: u64 = 0;

        loop {
            // if one page fail, simply return the error for now
            let mut page_results = self.fetch_campaigns_page(&statuses, skip).await?;
            // get the count before appending the page results to all
            let count = page_results.len() as u64;

            // append all received campaigns
            campaigns.append(&mut page_results);
            // add the number of results we need to skip in the next iteration
            skip += count;

            // if the Market returns < market fetch limit
            // we've got all Campaigns from all pages!
            if count < Self::MARKET_CAMPAIGNS_LIMIT {
                // so break out of the loop
                break;
            }
        }

        Ok(campaigns)
    }

    /// `skip` - how many records it should skip (pagination)
    async fn fetch_campaigns_page(
        &self,
        statuses: &Statuses<'_>,
        skip: u64,
    ) -> Result<Vec<Campaign>> {
        let url = self
            .market_url
            .join(&format!(
                "campaigns?{}&limit={}&skip={}",
                statuses,
                Self::MARKET_CAMPAIGNS_LIMIT,
                skip
            ))
            .expect("Wrong Market Url for /campaigns endpoint");

        let response = self.client.get(url.clone()).send().await?;

        let campaigns: Vec<Campaign> = response.json().await?;

        info!(
            &self.logger,
            "{} campaigns fetched from {}",
            campaigns.len(),
            url
        );

        Ok(campaigns)
    }
}

mod ad_slot {
    use std::collections::HashMap;

    use chrono::{DateTime, Utc};
    use reqwest::Url;
    use serde::{Serialize, Deserialize};

    use primitives::{BigNum, AdSlot, ValidatorId, targeting::Rule};

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
    #[serde(rename_all = "camelCase")]
    pub struct ShimResponse {
        pub slot: Shim,
        pub accepted_referrers: Vec<Url>,
        pub categories: Vec<String>,
        pub alexa_rank: Option<f64>,
    }

    impl From<super::AdSlotResponse> for ShimResponse {
        fn from(response: super::AdSlotResponse) -> Self {
            Self {
                slot: Shim::from(response.slot),
                accepted_referrers: response.accepted_referrers,
                categories: response.categories,
                alexa_rank: response.alexa_rank,
            }
        }
    }

    impl From<ShimResponse> for super::AdSlotResponse {
        fn from(shim_response: ShimResponse) -> Self {
            Self {
                slot: shim_response.slot.into(),
                accepted_referrers: shim_response.accepted_referrers,
                categories: shim_response.categories,
                alexa_rank: shim_response.alexa_rank,
            }
        }
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
    #[serde(rename_all = "camelCase")]
    /// This AdSlot Shim has only one difference with the Validator `primitives::AdSlot`
    /// The `created` and `modified` timestamps here are in strings (see https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Date/Date#Timestamp_string),
    /// instead of being millisecond timestamps
    pub struct Shim {
        pub ipfs: String,
        #[serde(rename = "type")]
        pub ad_type: String,
        #[serde(default)]
        pub min_per_impression: Option<HashMap<String, BigNum>>,
        #[serde(default)]
        pub rules: Vec<Rule>,
        #[serde(default)]
        pub fallback_unit: Option<String>,
        pub owner: ValidatorId,
        /// DateTime uses `RFC 3339` by default
        /// This is not the usual `milliseconds timestamp`
        /// as the original
        pub created: DateTime<Utc>,
        #[serde(default)]
        pub title: Option<String>,
        #[serde(default)]
        pub description: Option<String>,
        #[serde(default)]
        pub website: Option<String>,
        #[serde(default)]
        pub archived: bool,
        /// DateTime uses `RFC 3339` by default
        /// This is not the usual `milliseconds timestamp`
        /// as the original
        pub modified: Option<DateTime<Utc>>,
    }

    impl From<AdSlot> for Shim {
        fn from(ad_slot: AdSlot) -> Self {
            Self {
                ipfs: ad_slot.ipfs,
                ad_type: ad_slot.ad_type,
                min_per_impression: ad_slot.min_per_impression,
                rules: ad_slot.rules,
                fallback_unit: ad_slot.fallback_unit,
                owner: ad_slot.owner,
                created: ad_slot.created,
                title: ad_slot.title,
                description: ad_slot.description,
                website: ad_slot.website,
                archived: ad_slot.archived,
                modified: ad_slot.modified
            }
        }
    }

    impl Into<AdSlot> for Shim {
        fn into(self) -> AdSlot {
            AdSlot {
                ipfs: self.ipfs,
                ad_type: self.ad_type,
                min_per_impression: self.min_per_impression,
                rules: self.rules,
                fallback_unit: self.fallback_unit,
                owner: self.owner,
                created: self.created,
                title: self.title,
                description: self.description,
                website: self.website,
                archived: self.archived,
                modified: self.modified,
                
            }
        }
    }
}
