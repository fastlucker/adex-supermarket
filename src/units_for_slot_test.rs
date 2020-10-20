use super::*;
use crate::{
    cache::{CacheLike, MockCache},
    config::Config,
    util::test::discard_logger,
    MarketApi,
};
use chrono::{TimeZone, Utc};
use http::request::Request;
use hyper::Body;
use primitives::{
    supermarket::units_for_slot::response::{
        AdUnit, Campaign as ResponseCampaign, Channel as ResponseChannel, Spec as ResponseSpec,
        UnitsWithPrice,
    },
    targeting::{input, Function, Rule, Value},
    util::tests::prep_db::{DUMMY_CHANNEL, DUMMY_VALIDATOR_FOLLOWER, DUMMY_VALIDATOR_LEADER},
    validator::ValidatorId,
    AdSlot, BigNum, IPFS,
};
use std::collections::HashMap;
use std::iter::Iterator;
use std::str::FromStr;
use std::sync::Arc;
use url::Url;
use wiremock::{
    matchers::{method, path},
    Mock, MockServer, ResponseTemplate,
};

mod units_for_slot_tests {
    use super::*;
    fn get_mock_campaign(
        targeting_rules: Vec<Rule>,
        units_with_price: Vec<UnitsWithPrice>,
    ) -> ResponseCampaign {
        let mut channel = DUMMY_CHANNEL.clone();
        let mut leader = DUMMY_VALIDATOR_LEADER.clone();
        leader.url = "https://itchy.adex.network".to_string();

        let mut follower = DUMMY_VALIDATOR_FOLLOWER.clone();
        follower.url = "https://scratchy.adex.network".to_string();
        channel.spec.validators = (leader, follower).into();

        let response_channel = ResponseChannel {
            id: channel.id,
            creator: channel.creator,
            deposit_asset: channel.deposit_asset,
            deposit_amount: channel.deposit_amount,
            spec: ResponseSpec {
                withdraw_period_start: channel.spec.withdraw_period_start,
                active_from: channel.spec.active_from,
                created: channel.spec.created,
                validators: channel.spec.validators,
            },
        };
        ResponseCampaign {
            channel: response_channel,
            targeting_rules,
            units_with_price,
        }
    }

    // fn setup_channel(leader_url: &Url, follower_url: &Url) -> Channel {
    //     let mut channel = DUMMY_CHANNEL.clone();
    //     let mut leader = DUMMY_VALIDATOR_LEADER.clone();
    //     leader.url = leader_url.to_string();

    //     let mut follower = DUMMY_VALIDATOR_FOLLOWER.clone();
    //     follower.url = follower_url.to_string();

    //     channel.spec.validators = (leader, follower).into();

    //     channel
    // }

    fn get_units_with_price() -> Vec<UnitsWithPrice> {
        let units = get_mock_units();
        units
            .into_iter()
            .map(|u| UnitsWithPrice {
                unit: u,
                price: BigNum::from_str("1000000000000000000").expect("should convert"),
            })
            .collect()
    }

    fn get_mock_units() -> Vec<AdUnit> {
        vec![
            AdUnit {
                id: IPFS::try_from("Qmasg8FrbuSQpjFu3kRnZF9beg8rEBFrqgi1uXDRwCbX5f")
                    .expect("should convert"),
                media_url: "ipfs://QmcUVX7fvoLMM93uN2bD3wGTH8MXSxeL8hojYfL2Lhp7mR".to_string(),
                media_mime: "image/jpeg".to_string(),
                target_url: "https://www.adex.network/?stremio-test-banner-1".to_string(),
            },
            AdUnit {
                id: IPFS::try_from("QmVhRDGXoM3Fg3HZD5xwMuxtb9ZErwC8wHt8CjsfxaiUbZ")
                    .expect("should convert"),
                media_url: "ipfs://QmQB7uz7Gxfy7wqAnrnBcZFaVJLos8J9gn8mRcHQU6dAi1".to_string(),
                media_mime: "image/jpeg".to_string(),
                target_url: "https://www.adex.network/?adex-campaign=true&pub=stremio".to_string(),
            },
            AdUnit {
                id: IPFS::try_from("QmYwcpMjmqJfo9ot1jGe9rfXsszFV1WbEA59QS7dEVHfJi")
                    .expect("should convert"),
                media_url: "ipfs://QmQB7uz7Gxfy7wqAnrnBcZFaVJLos8J9gn8mRcHQU6dAi1".to_string(),
                media_mime: "image/jpeg".to_string(),
                target_url: "https://www.adex.network/?adex-campaign=true".to_string(),
            },
            AdUnit {
                id: IPFS::try_from("QmTAF3FsFDS7Ru8WChoD9ofiHTH8gAQfR4mYSnwxqTDpJH")
                    .expect("should convert"),
                media_url: "ipfs://QmQAcfBJpDDuH99A4p3pFtUmQwamS8UYStP5HxHC7bgYXY".to_string(),
                media_mime: "image/jpeg".to_string(),
                target_url: "https://adex.network".to_string(),
            },
        ]
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

    fn get_mock_slot() -> AdSlotResponse {
        let mut min_per_impression: HashMap<String, BigNum> = HashMap::new();
        min_per_impression.insert(
            "0x89d24A6b4CcB1B6fAA2625fE562bDD9a23260359".to_string(),
            BigNum::from_str("700000000000000").expect("should convert"),
        );
        let rules = get_mock_rules();

        let ad_slot = AdSlot {
            ipfs: "QmVwXu9oEgYSsL6G1WZtUQy6dEReqs3Nz9iaW4Cq5QLV8C".to_string(),
            ad_type: "legacy_300x100".to_string(),
            archived: false,
            created: Utc.timestamp(1_564_383_600, 0),
            description: Some("test slot".to_string()),
            // fallback_unit: Some("QmTAF3FsFDS7Ru8WChoD9ofiHTH8gAQfR4mYSnwxqTDpJH".to_string()),
            fallback_unit: None,
            min_per_impression: Some(min_per_impression),
            modified: Some(Utc.timestamp(1_564_383_600, 0)),
            owner: ValidatorId::try_from("0xB7d3F81E857692d13e9D63b232A90F4A1793189E")
                .expect("should create ValidatorId"),
            title: Some("Test slot".to_string()),
            website: Some("https://adex.network".to_string()),
            rules,
        };


        AdSlotResponse {
            slot: ad_slot,
            accepted_referrers: Default::default(),
            categories: Default::default(),
            alexa_rank: Some(1.0),
        }
    }

    fn get_expected_response(rules: Vec<Rule>) -> UnitsForSlotResponse {
        let targeting_input_base = input::Source {
            ad_view: None,
            global: input::Global {
                ad_slot_id: "QmVwXu9oEgYSsL6G1WZtUQy6dEReqs3Nz9iaW4Cq5QLV8C".to_string(),
                ad_slot_type: "legacy_728x90".to_string(),
                publisher_id: ValidatorId::try_from("0x13e72959d8055DaFA6525050A8cc7c479D4c09A3")
                    .expect("should create ValidatorId"),
                country: Some("BG".to_string()),
                event_type: "IMPRESSION".to_string(),
                seconds_since_epoch: u64::try_from(Utc::now().timestamp()).expect("Should convert"),
                user_agent_os: Some("Mac OS".to_string()),
                user_agent_browser_family: Some("Chrome".to_string()),
                ad_unit: None,
                balances: None,
                channel: None,
            },
            ad_slot: Some(input::AdSlot {
                categories: vec![
                    "IAB3".to_string(),
                    "IAB13-7".to_string(),
                    "IAB5".to_string(),
                ],
                hostname: "adex.network".to_string(),
                alexa_rank: None,
            }),
        };
        let accepted_referrers: Vec<Url> = Vec::new();
        let fallback_unit: Option<AdUnit> = None;
        let units_with_price = get_units_with_price();
        let campaign = get_mock_campaign(rules, units_with_price);
        let campaigns = vec![campaign];

        UnitsForSlotResponse {
            targeting_input_base: targeting_input_base.into(),
            accepted_referrers,
            campaigns,
            fallback_unit,
        }
    }

    #[tokio::test]
    async fn test_units_for_slot_route() {
        let logger = discard_logger();

        let server = MockServer::start().await;

        let market = Arc::new(
            MarketApi::new(server.uri().trim_end_matches('/').to_string(), logger.clone())
                .expect("should create market instance"),
        );

        let config = Config::new(None, "development").expect("should get config");
        let mock_cache = MockCache::initialize(logger.clone(), config.clone())
            .await
            .expect("should initialize cache");

        let mock_units = get_mock_units();
        let mock_slot = get_mock_slot();

        Mock::given(method("GET"))
            .and(path("/units"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&mock_units))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path(format!("/slots/{}", mock_slot.slot.ipfs)))
            .respond_with(ResponseTemplate::new(200).set_body_json(&mock_slot))
            .mount(&server)
            .await;
        let rules = get_mock_rules();
        let expected_response = get_expected_response(rules);

        let request = Request::get(format!("/units-for-slot/{}", mock_slot.slot.ipfs))
        .body(Body::empty())
        .unwrap();

        let actual_response = get_units_for_slot(&logger, market, &config, &mock_cache, request)
        .await
        .expect("call shouldn't fail with provided data");

        assert_eq!(http::StatusCode::OK, actual_response.status());

        let units_for_slot: UnitsForSlotResponse = serde_json::from_slice(&hyper::body::to_bytes(actual_response).await.unwrap()).expect("Should deserialize");

        assert_eq!(expected_response.campaigns.len(), units_for_slot.campaigns.len());
    }
}
