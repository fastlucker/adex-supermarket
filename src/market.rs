use primitives::{
    market::{Campaign, StatusType},
    AdSlot, AdUnit,
};
use reqwest::{Client, Error, StatusCode};
use serde::Deserialize;
use slog::{info, Logger};
use std::fmt;

pub(crate) type MarketUrl = String;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub(crate) struct MarketApi {
    pub market_url: MarketUrl,
    client: Client,
    logger: Logger,
}

impl MarketApi {
    /// The limit of Campaigns per page when fetching
    /// Limit the value to MAX(500)
    const MARKET_CAMPAIGNS_LIMIT: u64 = 500;
    /// The limit of AdUnits per page when fetching
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
    pub async fn fetch_slot(&self, ipfs: &str) -> Result<Option<AdSlot>> {
        #[derive(Deserialize)]
        struct AdSlotResponse {
            slot: AdSlot,
        }
        let url = format!("{}/slots/{}", self.market_url, ipfs);
        let response = self.client.get(&url).send().await?;

        // Handle
        if let StatusCode::NOT_FOUND = response.status() {
            Ok(None)
        } else {
            let ad_slot_response = response.json::<AdSlotResponse>().await?;
            Ok(Some(ad_slot_response.slot))
        }
    }

    pub async fn fetch_units(&self, ad_slot: &AdSlot) -> Result<Vec<AdUnit>> {
        let mut campaigns = Vec::new();
        let mut skip: u64 = 0;
        let limit = Self::MARKET_AD_UNITS_LIMIT;

        loop {
            // if one page fail, simply return the error for now
            let mut page_results = self.fetch_units_page(&ad_slot.ad_type, skip).await?;
            // get the count before appending the page results to all
            let count = page_results.len() as u64;

            // append all received campaigns
            campaigns.append(&mut page_results);
            // add the number of results we need to skip in the next iteration
            skip += count;

            // if the Market returns < market fetch limit
            // we've got all AdSlots from all pages!
            if count < limit {
                // so break out of the loop
                break;
            }
        }

        Ok(campaigns)
    }

    // @TODO: Once we have the impled `type` param, pass it instead of filtering the response
    // @see: https://github.com/AdExNetwork/adex-market/issues/110
    /// `skip` - how many records it should skip (pagination)
    async fn fetch_units_page(&self, ad_type: &str, skip: u64) -> Result<Vec<AdUnit>> {
        let url = format!(
            "{}/units?limit={}&skip={}",
            self.market_url,
            Self::MARKET_AD_UNITS_LIMIT,
            skip
        );
        let response = self.client.get(&url).send().await?;

        let all_ad_units: Vec<AdUnit> = response.json().await?;

        let ad_units = all_ad_units
            .into_iter()
            .filter(|ad_unit| ad_unit.ad_type == ad_type)
            .collect();

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
pub(crate) enum Statuses<'a> {
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
