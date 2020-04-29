use primitives::market::{Campaign, StatusType};
use reqwest::{Client, Error};
use slog::{info, Logger};
use std::fmt;

pub(crate) type MarketUrl = String;

#[derive(Debug, Clone)]
pub(crate) struct MarketApi {
    market_url: MarketUrl,
    client: Client,
    logger: Logger,
}

impl MarketApi {
    /// The limit of campaigns per page when fetching
    /// MAX(500)
    const MARKET_LIMIT: u64 = 500;

    pub fn new(market_url: MarketUrl, logger: Logger) -> Result<Self, Error> {
        let client = Client::new();

        Ok(Self {
            market_url,
            client,
            logger,
        })
    }

    pub async fn fetch_campaigns(&self, statuses: &Statuses<'_>) -> Result<Vec<Campaign>, Error> {
        let mut campaigns = Vec::new();
        let mut skip: u64 = 0;

        loop {
            // if one page fail, simply return the error for now
            let mut page_results = self.fetch_page(Self::MARKET_LIMIT, &statuses, skip).await?;
            // get the count before appending the page results to all
            let count = page_results.len() as u64;

            // append all received campaigns
            campaigns.append(&mut page_results);
            // add the number of results we need to skip in the next iteration
            skip += count;

            // if the Market returns < market fetch limit
            // we've got all Campaigns from all pages!
            if count < Self::MARKET_LIMIT {
                // so break out of the loop
                break;
            }
        }

        Ok(campaigns)
    }

    /// `skip` - how many records it should skip (pagination)
    async fn fetch_page(
        &self,
        limit: u64,
        statuses: &Statuses<'_>,
        skip: u64,
    ) -> Result<Vec<Campaign>, Error> {
        let url = format!(
            "{}/campaigns?{}&limit={}&skip={}",
            self.market_url, statuses, limit, skip
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
