use crate::{
    status::{get_status, Status},
    Config, SentryApi,
};
use futures::future::{join_all, FutureExt};
use primitives::{BalancesMap, Channel, ChannelId};
use reqwest::Error;
use slog::{error, info, Logger};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use url::Url;

type Cached<T> = Arc<RwLock<T>>;

pub type ActiveCache = HashMap<ChannelId, Campaign>;
pub type FinalizedCache = HashSet<ChannelId>;

pub enum ActiveAction {
    /// Update exiting Campaigns in the Cache
    Update(HashMap<ChannelId, (Status, BalancesMap)>),
    /// Add new and/or replace (if Campaign exist already) Campaigns to the Cache
    New(ActiveCache),
}

#[derive(Debug, Clone)]
pub struct Cache {
    pub active: Cached<ActiveCache>,
    pub finalized: Cached<FinalizedCache>,
    validators: HashSet<Url>,
    logger: Logger,
    sentry: SentryApi,
}

#[derive(Debug, Clone)]
pub struct Campaign {
    channel: Channel,
    status: Status,
    balances: BalancesMap,
}

impl Campaign {
    pub fn new(channel: Channel, status: Status, balances: BalancesMap) -> Self {
        Self {
            channel,
            status,
            balances,
        }
    }
}

impl Cache {
    /// Fetches all the campaigns from the Validators on initialization.
    pub(crate) async fn initialize(logger: Logger, config: Config) -> Result<Self, Error> {
        info!(
            &logger,
            "Initialize Cache"; "validators" => format_args!("{:?}", &config.validators)
        );
        let sentry = SentryApi::new(config.timeouts.validator_request)?;

        let validators = config.validators.clone();

        let cache = Self {
            active: Default::default(),
            finalized: Default::default(),
            validators,
            logger,
            sentry,
        };

        // collect and initialize the active campaigns
        cache.fetch_new_campaigns().await;

        Ok(cache)
    }

    /// # Update the Campaigns in the Cache with:
    /// Collects all the campaigns from all the Validators and computes their Statuses
    /// - New Campaigns
    /// - New Finalized Campaigns
    pub async fn fetch_new_campaigns(&self) {
        let campaigns = collect_all_campaigns(&self.logger, &self.sentry, &self.validators).await;

        let (active, finalized) = campaigns.into_iter().fold(
            (HashMap::new(), HashSet::new()),
            |(mut active, mut finalized), (id, campaign)| {
                // Handle new Campaigns only if the Active & Finalized cache have this ChannelId
                if !active.contains_key(&id) && !finalized.contains(&id) {
                    match campaign.status {
                        Status::Finalized(_) => {
                            finalized.insert(id);
                        }
                        _ => {
                            active.insert(id, campaign);
                        }
                    }
                }
                (active, finalized)
            },
        );

        self.update(ActiveAction::New(active), finalized).await
    }

    /// Updates the full Cache with the new values:
    ///
    /// 1. Updates Active cache:
    /// - If we have new Campaigns it extends the Active Campaigns with the new ones
    /// - If we have update for Status & BalancesMap it finds the Campaigns and updates them
    /// - Remove the Finalized `ChannelId`s from the Active Campaigns
    ///
    /// 2. Updates Finalized cache
    /// - Extend the Finalized `ChannelId`s with the new ones
    pub async fn update(&self, new_active: ActiveAction, new_finalized: FinalizedCache) {
        // Updates Active cache
        // - Extend the Active Campaigns with the new ones
        // - Remove the Finalized `ChannelId`s from the Active Campaigns
        {
            match &new_active {
                ActiveAction::New(new_active) => {
                    info!(&self.logger, "Adding New {} Active Campaigns", new_active.len(); "ChannelIds" => format_args!("{:?}", new_active.keys()));
                }

                ActiveAction::Update(update_active) => {
                    info!(&self.logger, "Updating {} Active Campaigns", update_active.len(); "ChannelIds" => format_args!("{:?}", update_active.keys()));
                }
            }

            let mut active = self.active.write().await;

            match new_active {
                ActiveAction::New(new_active) => active.extend(new_active),
                ActiveAction::Update(update_active) => {
                    for (id, (new_status, new_balances)) in update_active {
                        active.get_mut(&id).and_then(|campaign| {
                            campaign.status = new_status;
                            campaign.balances = new_balances;

                            Some(campaign)
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

    /// Reads the active campaigns and schedules a list of non-finalized campaigns
    /// for update from the Validators
    ///
    /// Checks the Campaign status:
    /// If Finalized:
    /// - Add to the Finalized cache
    /// Other statuses:
    /// - Update the Status & Balances from the latest Leader NewState
    pub async fn fetch_campaign_updates(&self) {
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
}

async fn collect_all_campaigns(
    logger: &Logger,
    sentry: &SentryApi,
    validators: &HashSet<Url>,
) -> HashMap<ChannelId, Campaign> {
    let mut campaigns = HashMap::new();

    for channel in get_all_channels(logger, sentry, validators).await {
        // @TODO: We need to figure out a way to distinguish between Channels, check if they are the same and to remove incorrect ones
        // For now just check if the channel is already inside the fetched channels and log if it is
        if campaigns.contains_key(&channel.id) {
            // @TODO: Issue #23 Check ChannelId and the received Channel hash
            info!(
                logger,
                "Skipping Campaign ({:?}) because it's already fetched from another Validator",
                &channel.id
            )
        } else {
            match get_status(&sentry, &channel).await {
                Ok((status, balances)) => {
                    let channel_id = channel.id;
                    let campaign = Campaign::new(channel, status, balances);

                    campaigns.insert(channel_id, campaign);
                }
                Err(err) => error!(
                    logger,
                    "Failed to fetch Campaign ({:?}) status from Validator", channel.id; "error" => ?err
                ),
            }
        }
    }

    campaigns
}

/// Retrieves all channels from all Validator URLs
async fn get_all_channels<'a>(
    logger: &Logger,
    sentry: &SentryApi,
    validators: &HashSet<Url>,
) -> Vec<Channel> {
    let futures = validators.iter().map(|validator| {
        sentry
            .get_validator_channels(validator)
            .map(move |result| (validator, result))
    });

    join_all(futures)
    .await
    .into_iter()
    .filter_map(|(validator, result)| match result {
            Ok(channels) => {
                info!(logger, "Fetched {} active Channels from Validator ({})", channels.len(), validator);

                Some(channels)
            },
            Err(err) => {
                error!(logger, "Failed to fetch Channels from Validator ({})", validator; "error" => ?err);

                None
            }
        })
    .flatten()
    .collect()
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::status::test::{get_approve_state_msg, get_heartbeat_msg, get_new_state_msg};
    use crate::{config::DEVELOPMENT, util::test::discard_logger};
    use chrono::{Duration, Utc};
    use primitives::{
        sentry::{
            ChannelListResponse, LastApproved, LastApprovedResponse, ValidatorMessage,
            ValidatorMessageResponse,
        },
        util::tests::prep_db::{DUMMY_CHANNEL, DUMMY_VALIDATOR_FOLLOWER, DUMMY_VALIDATOR_LEADER},
    };
    use wiremock::{
        matchers::{method, path},
        Mock, MockServer, ResponseTemplate,
    };

    // @TODO: Issue #23 Remove after writing test and using the function
    #[allow(dead_code)]
    fn setup_cache(
        active: HashMap<ChannelId, Campaign>,
        finalized: HashSet<ChannelId>,
        validators: HashSet<Url>,
    ) -> Result<Cache, Box<dyn std::error::Error>> {
        let sentry = SentryApi::new(std::time::Duration::from_secs(60))?;
        let logger = discard_logger();

        let cache = Cache {
            active: Arc::new(RwLock::new(active)),
            finalized: Arc::new(RwLock::new(finalized)),
            logger,
            sentry,
            validators,
        };

        Ok(cache)
    }

    #[tokio::test]
    async fn cache_initializes() {
        let mock_server = MockServer::start().await;

        let leader_url = format!("{}/leader", mock_server.uri());
        let follower_url = format!("{}/follower", mock_server.uri());

        let mut channel = DUMMY_CHANNEL.clone();
        let mut leader = DUMMY_VALIDATOR_LEADER.clone();
        let leader_id = leader.id.clone();
        leader.url = leader_url.clone();

        let mut follower = DUMMY_VALIDATOR_FOLLOWER.clone();
        let follower_id = follower.id.clone();
        follower.url = follower_url.clone();

        channel.spec.validators = (leader, follower).into();

        let mut config = DEVELOPMENT.clone();
        config.validators = vec![leader_url.parse().unwrap(), follower_url.parse().unwrap()]
            .into_iter()
            .collect();

        let leader_channels = ChannelListResponse {
            channels: vec![channel.clone()],
            total_pages: 1,
        };
        let leader_last_approved = LastApprovedResponse {
            last_approved: Some(LastApproved {
                new_state: Some(get_new_state_msg()),
                approve_state: None,
            }),
            heartbeats: Some(vec![
                get_heartbeat_msg(Duration::zero(), follower_id),
                get_heartbeat_msg(Duration::zero(), leader_id),
            ]),
        };
        let leader_latest_new_state = ValidatorMessageResponse {
            validator_messages: vec![ValidatorMessage {
                from: follower_id,
                received: Utc::now(),
                msg: get_new_state_msg().msg,
            }],
        };

        let follower_channels = ChannelListResponse {
            channels: vec![channel.clone()],
            total_pages: 1,
        };

        let follower_last_approved = LastApprovedResponse {
            last_approved: Some(LastApproved {
                new_state: None,
                approve_state: Some(get_approve_state_msg(true)),
            }),
            heartbeats: Some(vec![
                get_heartbeat_msg(Duration::zero(), follower_id),
                get_heartbeat_msg(Duration::zero(), leader_id),
            ]),
        };

        Mock::given(method("GET"))
            .and(path("/leader/channel/list"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&leader_channels))
            .expect(1_u64)
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path("/leader/last-approved"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&leader_last_approved))
            .expect(1_u64)
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path(format!(
                "/leader/validator-messages/{}/NewState",
                &leader_id
            )))
            .respond_with(ResponseTemplate::new(200).set_body_json(&leader_latest_new_state))
            .expect(1_u64)
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path("/follower/channel/list"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&follower_channels))
            .expect(1_u64)
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path("/follower/last-approved"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&follower_last_approved))
            .expect(1_u64)
            .mount(&mock_server)
            .await;

        let cache = Cache::initialize(discard_logger(), config)
            .await
            .expect("Should initialize");

        let active_cache = cache.active.read().await;
        let active_campaign = active_cache
            .get(&channel.id)
            .expect("This Campaign should be active");

        assert_eq!(Status::Waiting, active_campaign.status);
        // TODO: Issue #23 Add a BalancesMap and expect it!
        assert_eq!(BalancesMap::default(), active_campaign.balances);
    }
}
