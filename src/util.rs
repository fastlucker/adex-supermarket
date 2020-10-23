#[cfg(test)]
pub mod test {

    use slog::{o, Discard, Drain, Logger};
    use slog_async::Async;

    pub fn logger() -> Logger {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();

        Logger::root(drain, o!())
    }

    pub fn discard_logger() -> Logger {
        let drain = Discard.fuse();
        let drain = Async::new(drain).build().fuse();

        Logger::root(drain, o!())
    }

    pub mod cache {
        use crate::{
            cache::{ActiveAction, ActiveCache, CacheLike, Cached, FinalizedCache},
            status::get_status,
            Config, SentryApi,
        };
        use async_trait::async_trait;
        use chrono::{TimeZone, Utc};
        use primitives::{
            supermarket::{Campaign, Status},
            targeting::{Function, Rule, Value},
            util::tests::prep_db::{DUMMY_CHANNEL, IDS},
            validator::ValidatorId,
            AdUnit, BalancesMap, BigNum, Channel, ChannelId, IPFS,
        };
        use reqwest::{Error, Url};
        use slog::{error, info, Logger};
        use std::str::FromStr;
        use std::{
            collections::{HashMap, HashSet},
            convert::TryFrom,
        };

        #[derive(Debug, Clone)]
        pub struct MockCache {
            pub active: Cached<ActiveCache>,
            pub finalized: Cached<FinalizedCache>,
            validators: HashSet<Url>,
            logger: Logger,
            sentry: SentryApi,
        }

        #[async_trait]
        impl<'a> CacheLike<'a> for MockCache {
            async fn initialize(logger: Logger, config: Config) -> Result<Self, Error> {
                info!(
                    &logger,
                    "Initialize a mock Cache"; "validators" => format_args!("{:?}", &config.validators)
                );
                let sentry = SentryApi::new(config.timeouts.validator_request)?;

                let validators = config.validators.clone();

                let mock_cache = Self {
                    active: Default::default(),
                    finalized: Default::default(),
                    validators,
                    logger,
                    sentry,
                };

                // collect and initialize the active campaigns
                mock_cache.fetch_new_campaigns().await;

                Ok(mock_cache)
            }

            async fn fetch_new_campaigns(&self) {
                let campaigns = collect_mock_campaigns();

                let (active, finalized) = campaigns.into_iter().fold(
                    (HashMap::new(), HashSet::new()),
                    |(mut active, mut finalized), (id, campaign)| {
                        // we don't need to check if the ChannelId is already in either active or finalized,
                        // since the Campaigns (ChannelId) cannot repeat in the HashMap
                        match &campaign.status {
                            Status::Finalized(_) => {
                                finalized.insert(id);
                            }
                            _ => {
                                active.insert(id, campaign);
                            }
                        }
                        (active, finalized)
                    },
                );

                self.update(ActiveAction::New(active), finalized).await
            }

            async fn update(&self, new_active: ActiveAction, new_finalized: FinalizedCache) {
                // Updates Active cache
                // - Extend the Active Campaigns with the new ones
                // - Remove the Finalized `ChannelId`s from the Active Campaigns
                {
                    match &new_active {
                        ActiveAction::New(new_active) => {
                            info!(&self.logger, "Adding New / Updating {} Active Campaigns", new_active.len(); "ChannelIds" => format_args!("{:?}", new_active.keys()));
                        }

                        ActiveAction::Update(update_active) => {
                            info!(&self.logger, "Updating {} Active Campaigns", update_active.len(); "ChannelIds" => format_args!("{:?}", update_active.keys()));
                        }
                    }

                    let mut active = self.active.write().await;

                    match new_active {
                        // This will replace existing Campaigns and it will add the newly found ones
                        ActiveAction::New(new_active) => active.extend(new_active),
                        ActiveAction::Update(update_active) => {
                            for (channel_id, (new_status, new_balances)) in update_active {
                                active
                                    .entry(channel_id)
                                    .and_modify(|campaign: &mut Campaign| {
                                        campaign.status = new_status;
                                        campaign.balances = new_balances;
                                    });
                            }
                        }
                    }

                    info!(&self.logger, "Try to Finalize Campaigns in the Active Cache"; "finalized" => format_args!("{:?}", &new_finalized));

                    for id in new_finalized.iter() {
                        // remove from active campaigns and log
                        if let Some(campaign) = active.remove(id) {
                            info!(&self.logger, "Removed Campaign ({:?}) from Active Cache", id; "campaign" => ?campaign);
                        }
                        // the None variant is when a campaign is Finalized before it was inserted inside the Active Cache
                    }
                } // Active cache - release of RwLockWriteGuard

                // Updates Finalized cache
                // - Extend the Finalized `ChannelId`s with the new ones
                {
                    info!(&self.logger, "Extend with {} Finalized Campaigns", new_finalized.len(); "finalized" => format_args!("{:?}", &new_finalized));
                    let mut finalized = self.finalized.write().await;

                    finalized.extend(new_finalized);
                } // Finalized cache - release of RwLockWriteGuard
            }

            async fn fetch_campaign_updates(&self) {
                // we need this scope to drop the Read Lock on `self.active`
                // before performing the finalize & update actions
                let (update, finalize) = {
                    let active = self.active.read().await;

                    let mut update = HashMap::new();
                    let mut finalize = HashSet::new();
                    for (id, campaign) in active.iter() {
                        match get_status(&self.sentry, &campaign.channel).await {
                            Ok((Status::Finalized(_), _balances)) => {
                                finalize.insert(*id);
                            }
                            Ok((new_status, new_balances)) => {
                                update.insert(*id, (new_status, new_balances));
                            }
                            Err(err) => error!(
                                &self.logger,
                                "Error getting Campaign ({:?}) status", id; "error" => ?err
                            ),
                        };
                    }

                    (update, finalize)
                };

                self.update(ActiveAction::Update(update), finalize).await;
            }

            async fn get_active_campaigns(&self) -> HashMap<ChannelId, Campaign> {
                // Needed to access the campaigns from RwLock
                {
                    let read_campaigns = self.active.read().await;
                    let mut active = HashMap::new();
                    for (id, campaign) in read_campaigns.iter() {
                        active.insert(*id, campaign.clone());
                    }
                    active
                }
            }
        }

        fn collect_mock_campaigns() -> HashMap<ChannelId, Campaign> {
            let mut campaigns = HashMap::new();
            let mut channel = DUMMY_CHANNEL.clone();

            channel.spec.ad_units = get_mock_units();
            channel.targeting_rules = get_mock_rules();
            channel.spec.min_per_impression = 100000000000000.into();
            channel.spec.max_per_impression = 1000000000000000.into();
            let mut campaign = Campaign {
                channel,
                status: Status::Active,
                balances: Default::default(),
            };
            campaign.balances.insert(
                IDS["publisher"],
                BigNum::from_str("100000000000000").expect("should convert"),
            );

            campaigns.insert(campaign.channel.id, campaign);
            campaigns
        }

        fn get_mock_rules() -> Vec<Rule> {
            let get_rule = Rule::Function(Function::Get("adSlot.categories".to_string()));
            let categories_array = Rule::Value(Value::Array(vec![
                Value::String("IAB3".to_string()),
                Value::String("IAB13-7".to_string()),
                Value::String("IAB5".to_string()),
            ]));
            let intersects_rule = Rule::Function(Function::Intersects(
                Box::new(get_rule),
                Box::new(categories_array),
            ));
            let only_show_if_rule = Rule::Function(Function::OnlyShowIf(Box::new(intersects_rule)));
            vec![only_show_if_rule]
        }

        fn get_mock_units() -> Vec<AdUnit> {
            vec![
                AdUnit {
                    ipfs: IPFS::try_from("Qmasg8FrbuSQpjFu3kRnZF9beg8rEBFrqgi1uXDRwCbX5f")
                        .expect("should convert"),
                    media_url: "ipfs://QmcUVX7fvoLMM93uN2bD3wGTH8MXSxeL8hojYfL2Lhp7mR".to_string(),
                    media_mime: "image/jpeg".to_string(),
                    target_url: "https://www.adex.network/?stremio-test-banner-1".to_string(),
                    archived: false,
                    description: Some("test description".to_string()),
                    ad_type: "legacy_300x100".to_string(),
                    created: Utc.timestamp(1_564_383_600, 0),
                    min_targeting_score: Some(1.00),
                    modified: None,
                    owner: ValidatorId::try_from("0xB7d3F81E857692d13e9D63b232A90F4A1793189E")
                        .expect("should create ValidatorId"),
                    title: Some("test title".to_string()),
                },
                AdUnit {
                    ipfs: IPFS::try_from("QmVhRDGXoM3Fg3HZD5xwMuxtb9ZErwC8wHt8CjsfxaiUbZ")
                        .expect("should convert"),
                    media_url: "ipfs://QmQB7uz7Gxfy7wqAnrnBcZFaVJLos8J9gn8mRcHQU6dAi1".to_string(),
                    media_mime: "image/jpeg".to_string(),
                    target_url: "https://www.adex.network/?adex-campaign=true&pub=stremio"
                        .to_string(),
                    archived: false,
                    description: Some("test description".to_string()),
                    ad_type: "legacy_300x100".to_string(),
                    created: Utc.timestamp(1_564_383_600, 0),
                    min_targeting_score: Some(1.00),
                    modified: None,
                    owner: ValidatorId::try_from("0xB7d3F81E857692d13e9D63b232A90F4A1793189E")
                        .expect("should create ValidatorId"),
                    title: Some("test title".to_string()),
                },
                AdUnit {
                    ipfs: IPFS::try_from("QmYwcpMjmqJfo9ot1jGe9rfXsszFV1WbEA59QS7dEVHfJi")
                        .expect("should convert"),
                    media_url: "ipfs://QmQB7uz7Gxfy7wqAnrnBcZFaVJLos8J9gn8mRcHQU6dAi1".to_string(),
                    media_mime: "image/jpeg".to_string(),
                    target_url: "https://www.adex.network/?adex-campaign=true".to_string(),
                    archived: false,
                    description: Some("test description".to_string()),
                    ad_type: "legacy_300x100".to_string(),
                    created: Utc.timestamp(1_564_383_600, 0),
                    min_targeting_score: Some(1.00),
                    modified: None,
                    owner: ValidatorId::try_from("0xB7d3F81E857692d13e9D63b232A90F4A1793189E")
                        .expect("should create ValidatorId"),
                    title: Some("test title".to_string()),
                },
                AdUnit {
                    ipfs: IPFS::try_from("QmTAF3FsFDS7Ru8WChoD9ofiHTH8gAQfR4mYSnwxqTDpJH")
                        .expect("should convert"),
                    media_url: "ipfs://QmQAcfBJpDDuH99A4p3pFtUmQwamS8UYStP5HxHC7bgYXY".to_string(),
                    media_mime: "image/jpeg".to_string(),
                    target_url: "https://adex.network".to_string(),
                    archived: false,
                    description: Some("test description".to_string()),
                    ad_type: "legacy_300x100".to_string(),
                    created: Utc.timestamp(1_564_383_600, 0),
                    min_targeting_score: Some(1.00),
                    modified: None,
                    owner: ValidatorId::try_from("0xB7d3F81E857692d13e9D63b232A90F4A1793189E")
                        .expect("should create ValidatorId"),
                    title: Some("test title".to_string()),
                },
            ]
        }
    }
}
