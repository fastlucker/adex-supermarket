use crate::{
    cache::{ActiveAction, ActiveCache, Client, FinalizedCache},
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
    AdUnit, BigNum, ChannelId, IPFS,
};
use reqwest::{Error, Url};
use slog::{error, info, Logger};
use std::str::FromStr;
use std::{
    collections::{HashMap, HashSet},
    convert::TryFrom,
};

#[derive(Debug, Clone)]
pub struct MockClient {
    validators: HashSet<Url>,
    logger: Logger,
    sentry: SentryApi,
}

impl MockClient {
    pub async fn init(logger: Logger, config: Config) -> Result<Self, Error> {
        info!(
            &logger,
            "Initialize a mock Cache"; "validators" => format_args!("{:?}", &config.validators)
        );
        let sentry = SentryApi::new(config.timeouts.validator_request)?;

        let validators = config.validators.clone();

        Ok(Self {
            validators,
            logger,
            sentry,
        })
    }
}

#[async_trait]
impl Client for MockClient {
    fn logger(&self) -> Logger {
        self.logger.clone()
    }

    async fn collect_campaigns(&self) -> HashMap<ChannelId, Campaign> {
        collect_mock_campaigns()
    }

    async fn fetch_campaign_updates(
        &self,
        active: &ActiveCache,
    ) -> (ActiveAction, FinalizedCache) {
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

        (ActiveAction::Update(update), finalize)
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
