use primitives::{
    market::{Campaign, StatusType},
    supermarket::units_for_slot::response::AdUnit,
    AdSlot,
};
use reqwest::{Client, Error, StatusCode};
use serde::{Deserialize, Serialize};
use slog::{info, Logger};
use std::fmt;
use url::Url;

pub type MarketUrl = String;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct MarketApi {
    pub market_url: MarketUrl,
    client: Client,
    logger: Logger,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
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

impl MarketApi {
    /// The limit of Campaigns per page when fetching
    /// Limit the value to MAX(500)
    const MARKET_CAMPAIGNS_LIMIT: u64 = 500;
    /// The limit of AdUnits per page when fetching
    /// It should always be > 1
    const MARKET_AD_UNITS_LIMIT: u64 = 1_000;

    pub fn new(market_url: MarketUrl, logger: Logger) -> Result<Self> {
        // @TODO: maybe add timeout?
        let client = Client::new();

        Ok(Self {
            market_url,
            client,
            logger,
        })
    }

    /// ipfs: ipfs hash
    /// Handles the 404 case, returning a None, instead of Error
    pub async fn fetch_slot(&self, ipfs: &str) -> Result<Option<AdSlotResponse>> {
        let url = format!("{}/slots/{}", self.market_url, ipfs);

        let response = self.client.get(&url).send().await?;
        if StatusCode::NOT_FOUND == response.status() {
            Ok(None)
        } else {
            let ad_slot_response = response.json::<AdSlotResponse>().await?;
            Ok(Some(ad_slot_response))
        }
    }

    pub async fn fetch_unit(&self, ipfs: &str) -> Result<Option<AdUnitResponse>> {
        let url = format!("{}/units/{}", self.market_url, ipfs);

        match self.client.get(&url).send().await?.error_for_status() {
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
        let url = format!(
            "{}/units?limit={}&skip={}&type={}",
            self.market_url,
            Self::MARKET_AD_UNITS_LIMIT,
            skip,
            ad_type,
        );
        let response = self.client.get(&url).send().await?;

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
        let url = format!(
            "{}/campaigns?{}&limit={}&skip={}",
            self.market_url,
            statuses,
            Self::MARKET_CAMPAIGNS_LIMIT,
            skip
        );
        let response = self.client.get(&url).send().await?;

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
