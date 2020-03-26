use std::sync::Arc;
use std::collections::{HashMap, HashSet};
use primitives::{market::{Campaign, StatusType}, ChannelId, BalancesMap};
use hyper::Uri;
use tokio::sync::RwLock;

type Cached<T> = Arc<RwLock<T>>;

#[derive(Debug, Clone)]
pub struct Cache {
    pub active: Cached<HashMap<ChannelId, Campaign>>,
    pub finalized: Cached<HashSet<ChannelId>>,
    pub balance_from_finalized: Cached<BalancesMap>,
}

impl Cache {
    pub async fn initialize(market_uri: &Uri) -> Result<Self, reqwest::Error> {
        use StatusType::*;
        let fetch_all_url = format!("{}campaigns?all", market_uri);

        let all_campaigns = Self::fetch_campaigns(&fetch_all_url).await?;

        // @TODO: Check with @Ivo if these are all campaigns
        // Closed = Exhausted ? from market
        let finalized_statuses = [Exhausted, Withdraw, Expired];
        let init = (HashMap::default(), HashSet::default(), BalancesMap::default());
        let (active, finalized, balances) = all_campaigns.into_iter().fold(init, |(mut active, mut finalized, mut balances), campaign: Campaign| {
            if finalized_statuses.contains(&campaign.status.status_type) {
                // we don't care if the campaign was already in the set
                finalized.insert(campaign.channel.id);
            }


            balances = campaign.status.balances.iter().fold(balances, |mut acc, (publisher, balance)| {
                acc.entry(publisher.clone()).and_modify(|current_balance| {
                    *current_balance += balance
                }).or_default();

                acc
            });

            active.insert(campaign.channel.id, campaign);
        
            
            (active, finalized, balances)
        });

        Ok(Self {
            active: Arc::new(RwLock::new(active)),
            finalized: Arc::new(RwLock::new(finalized)),
            balance_from_finalized: Arc::new(RwLock::new(balances)),
        })
    }

    /// Will update the campaigns in the Cache
    pub async fn update_campaigns(&self) -> Result<(), reqwest::Error> {
        // TODO: Implement
        Ok(())
    }

    async fn fetch_campaigns<U: reqwest::IntoUrl>(url: U) -> Result<Vec<Campaign>, reqwest::Error> {
        // @TODO: maybe add timeout?
        let client = reqwest::Client::builder().build()?;
        
        
        let campaigns = client
                .post(url)
                .send()
                .await?
                .json()
                .await?;
    
        Ok(campaigns)
    }
}

