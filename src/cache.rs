use crate::market::{MarketApi, MarketUrl, Statuses};
use primitives::{
    market::{Campaign, StatusType, StatusType::*},
    BalancesMap, ChannelId,
};
use reqwest::Error;
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
}

impl Cache {
    const FINALIZED_STATUSES: [StatusType; 3] = [Exhausted, Withdraw, Expired];
    const NON_FINALIZED_CAMPAIGN_STATUSES: [StatusType; 9] = [Active, Ready, Pending, Initializing, Waiting, Offline, Disconnected, Unhealthy, Invalid];
    // const UNSOUND: [StatusType; 4] = [Offline, Disconnected, Unhealthy, Invalid];

    /// Fetches all the campaigns from the Market and returns the Cache instance
    pub async fn initialize(market_url: MarketUrl) -> Result<Self, Error> {
        let market = MarketApi::new(market_url)?;

        let all_campaigns = market.fetch_campaigns(&Statuses::All).await?;

        let (active, finalized, balances) = all_campaigns.into_iter().fold(
            (HashMap::default(), HashSet::default(), BalancesMap::default()),
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
                            .or_default();

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
        })
    }

    /// Will update the campaigns in the Cache
    pub async fn update_campaigns(&self) -> Result<(), Error> {
        let statuses = Statuses::Only(&Self::NON_FINALIZED_CAMPAIGN_STATUSES);
        let fetched_campaigns = self.market.fetch_campaigns(&statuses).await?;

        let current_campaigns = self.active.clone();

        let read_campaigns = current_campaigns.read().await;
        
        let filtered = fetched_campaigns.into_iter().filter_map(|campaign| {
            // if the key doesn't exist, leave it
            if !read_campaigns.contains_key(&campaign.channel.id) {
                Some((campaign.channel.id, campaign))
            } else {
                None
            }
        });

        {
            current_campaigns.write().await.extend(filtered);
        }

        Ok(())
    }
}
