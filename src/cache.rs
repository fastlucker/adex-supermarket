use crate::{
    market::{MarketApi, MarketUrl, Statuses},
    status::{is_finalized, IsFinalized},
    SentryApi,
};
use primitives::{
    market::{Campaign, StatusType, StatusType::*},
    BalancesMap, ChannelId,
};
use reqwest::Error;
use slog::{info, Logger};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

type Cached<T> = Arc<RwLock<T>>;

#[derive(Debug, Clone)]
pub struct Cache {
    pub active: Cached<HashMap<ChannelId, Campaign>>,
    pub finalized: Cached<HashSet<ChannelId>>,
    pub balance_from_finalized: Cached<BalancesMap>,
    market: Arc<MarketApi>,
    logger: Logger,
    sentry: SentryApi,
}

impl Cache {
    const FINALIZED_STATUSES: [StatusType; 3] = [Exhausted, Withdraw, Expired];
    const NON_FINALIZED: [StatusType; 9] = [
        Active,
        Ready,
        Pending,
        Initializing,
        Waiting,
        Offline,
        Disconnected,
        Unhealthy,
        Invalid,
    ];
    // const UNSOUND: [StatusType; 4] = [Offline, Disconnected, Unhealthy, Invalid];

    /// Fetches all the campaigns from the Market and returns the Cache instance
    pub async fn initialize(market_url: MarketUrl, logger: Logger) -> Result<Self, Error> {
        let market = MarketApi::new(market_url, logger.clone())?;
        let sentry = SentryApi::new()?;

        let all_campaigns = market.fetch_campaigns(&Statuses::All).await?;

        let (active, finalized, balances) = all_campaigns.into_iter().fold(
            (
                HashMap::default(),
                HashSet::default(),
                BalancesMap::default(),
            ),
            |(mut active, mut finalized, mut balances), campaign: Campaign| {
                if Self::FINALIZED_STATUSES.contains(&campaign.status.status_type) {
                    // we don't care if the campaign was already in the set
                    finalized.insert(campaign.channel.id);
                }

                balances = campaign.status.balances.iter().fold(
                    balances,
                    |mut acc, (publisher, balance)| {
                        acc.entry(publisher.clone())
                            .and_modify(|current_balance| *current_balance += balance)
                            .or_insert_with(|| balance.clone());

                        acc
                    },
                );

                active.insert(campaign.channel.id, campaign);

                (active, finalized, balances)
            },
        );

        Ok(Self {
            market: Arc::new(market),
            active: Arc::new(RwLock::new(active)),
            finalized: Arc::new(RwLock::new(finalized)),
            balance_from_finalized: Arc::new(RwLock::new(balances)),
            logger,
            sentry,
        })
    }

    /// Will update the campaigns in the Cache, fetching new campaigns from the Market
    pub async fn update_campaigns(&self) -> Result<(), Error> {
        let statuses = Statuses::Only(&Self::NON_FINALIZED);
        let fetched_campaigns = self.market.fetch_campaigns(&statuses).await?;

        let current_campaigns = self.active.clone();

        let filtered: Vec<(ChannelId, Campaign)> = {
            // we need to release the read lock before writing!
            // Hence the scope of the `filtered` variable
            let read_campaigns = current_campaigns.read().await;

            fetched_campaigns
                .into_iter()
                .filter_map(|campaign| {
                    // if the key doesn't exist, leave it
                    if !read_campaigns.contains_key(&campaign.channel.id) {
                        Some((campaign.channel.id, campaign))
                    } else {
                        None
                    }
                })
                .collect()
        };

        if !filtered.is_empty() {
            let new_campaigns = filtered.len();
            let mut campaigns = current_campaigns.write().await;
            campaigns.extend(filtered.into_iter());
            info!(
                &self.logger,
                "Added {} new campaigns ({:?}) to the Cache", new_campaigns, statuses
            );
        } else {
            info!(
                &self.logger,
                "No new campaigns ({:?}) added to Cache", statuses
            );
        }

        Ok(())
    }

    pub async fn finalize_campaign(&self, campaign: &Campaign) -> Result<bool, Error> {
        let is_finalized = match is_finalized(&self.sentry, &campaign).await? {
            IsFinalized::Yes { balances, .. } => {
                // Put in finalized
                self.finalized.write().await.insert(campaign.channel.id);

                // Sum the balances in balances_from_finalized
                let mut finalized_balances = self.balance_from_finalized.write().await;

                for (publisher, value) in balances {
                    finalized_balances
                        .entry(publisher)
                        .and_modify(|current_balance| *current_balance += &value)
                        .or_insert(value);
                }

                true
            }
            _ => false,
        };

        Ok(is_finalized)
    }
}
