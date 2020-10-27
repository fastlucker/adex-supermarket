use super::*;
use crate::{
    cache::mock_client::MockClient, config::Config, util::test::discard_logger, MarketApi,
};
use chrono::{TimeZone, Utc};
use http::request::Request;
use hyper::Body;
use primitives::{
    supermarket::units_for_slot::response::{
        AdUnit, Campaign as ResponseCampaign, Channel as ResponseChannel, UnitsWithPrice,
    },
    targeting::{input, Function, Rule, Value},
    util::tests::prep_db::{DUMMY_CHANNEL, IDS},
    AdSlot, BigNum, IPFS,
};
use std::collections::HashMap;
use std::iter::Iterator;
use std::sync::Arc;
use wiremock::{
    matchers::{method, path},
    Mock, MockServer, ResponseTemplate,
};

mod units_for_slot_tests {
    use primitives::{Channel, ChannelId};

    use super::*;

    fn get_mock_campaign(channel: Channel, units: &[AdUnit]) -> ResponseCampaign {
        let units_with_price = get_units_with_price(&channel, &units);
        let targeting_rules = channel.spec.targeting_rules.clone();
        ResponseCampaign {
            channel: ResponseChannel::from(channel),
            targeting_rules,
            units_with_price,
        }
    }

    fn get_units_with_price(channel: &Channel, units: &[AdUnit]) -> Vec<UnitsWithPrice> {
        units
            .iter()
            .cloned()
            .map(|u| UnitsWithPrice {
                unit: u,
                price: channel.spec.min_per_impression.clone(),
            })
            .collect()
    }

    fn get_supermarket_ad_units() -> Vec<AdUnit> {
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

    fn get_mock_rules(categories: &[&str]) -> Vec<Rule> {
        let get_rule = Function::new_get("adSlot.categories");
        let categories_array =
            Value::Array(categories.iter().map(|s| Value::new_string(s)).collect());
        let intersects_rule = Function::new_intersects(get_rule, categories_array);
        vec![Function::new_only_show_if(intersects_rule).into()]
    }

    fn get_supermarket_ad_slot(rules: &[Rule], categories: &[&str]) -> AdSlotResponse {
        let mut min_per_impression: HashMap<String, BigNum> = HashMap::new();
        min_per_impression.insert(
            "0x89d24A6b4CcB1B6fAA2625fE562bDD9a23260359".to_string(),
            700_000_000_000_000.into(),
        );

        let ad_slot = AdSlot {
            ipfs: "QmVwXu9oEgYSsL6G1WZtUQy6dEReqs3Nz9iaW4Cq5QLV8C".to_string(),
            ad_type: "legacy_300x100".to_string(),
            archived: false,
            created: Utc.timestamp(1_564_383_600, 0),
            description: Some("test slot".to_string()),
            fallback_unit: None,
            min_per_impression: Some(min_per_impression),
            modified: Some(Utc.timestamp(1_564_383_600, 0)),
            owner: IDS["publisher"],
            title: Some("Test slot".to_string()),
            website: Some("https://adex.network".to_string()),
            rules: rules.to_vec(),
        };

        AdSlotResponse {
            slot: ad_slot,
            accepted_referrers: Default::default(),
            categories: categories.iter().map(|s| String::from(*s)).collect(),
            alexa_rank: Some(1.0),
        }
    }

    fn get_expected_response(channel: Channel, units: &[AdUnit]) -> UnitsForSlotResponse {
        let targeting_input_base = input::Source {
            ad_view: None,
            global: input::Global {
                ad_slot_id: "QmVwXu9oEgYSsL6G1WZtUQy6dEReqs3Nz9iaW4Cq5QLV8C".to_string(),
                ad_slot_type: "legacy_728x90".to_string(),
                publisher_id: IDS["publisher"],
                country: None,
                event_type: "IMPRESSION".to_string(),
                seconds_since_epoch: u64::try_from(Utc::now().timestamp()).expect("Should convert"),
                user_agent_os: Some("UNKNOWN".to_string()),
                user_agent_browser_family: Some("UNKNOWN".to_string()),
                ad_unit: None,
                balances: None,
                channel: None,
            },
            ad_slot: Some(input::AdSlot {
                categories: vec!["IAB3".into(), "IAB13-7".into(), "IAB5".into()],
                hostname: "adex.network".to_string(),
                alexa_rank: Some(1.0),
            }),
        };

        UnitsForSlotResponse {
            targeting_input_base: targeting_input_base.into(),
            accepted_referrers: vec![],
            campaigns: vec![get_mock_campaign(channel, units)],
            fallback_unit: None,
        }
    }

    fn mock_channel_units() -> Vec<primitives::AdUnit> {
        vec![
            primitives::AdUnit {
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
                owner: IDS["publisher"],
                title: Some("test title".to_string()),
            },
            primitives::AdUnit {
                ipfs: IPFS::try_from("QmVhRDGXoM3Fg3HZD5xwMuxtb9ZErwC8wHt8CjsfxaiUbZ")
                    .expect("should convert"),
                media_url: "ipfs://QmQB7uz7Gxfy7wqAnrnBcZFaVJLos8J9gn8mRcHQU6dAi1".to_string(),
                media_mime: "image/jpeg".to_string(),
                target_url: "https://www.adex.network/?adex-campaign=true&pub=stremio".to_string(),
                archived: false,
                description: Some("test description".to_string()),
                ad_type: "legacy_300x100".to_string(),
                created: Utc.timestamp(1_564_383_600, 0),
                min_targeting_score: Some(1.00),
                modified: None,
                owner: IDS["publisher"],
                title: Some("test title".to_string()),
            },
            primitives::AdUnit {
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
                owner: IDS["publisher"],
                title: Some("test title".to_string()),
            },
            primitives::AdUnit {
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
                owner: IDS["publisher"],
                title: Some("test title".to_string()),
            },
        ]
    }

    fn mock_channel(rules: &[Rule]) -> Channel {
        let mut channel = DUMMY_CHANNEL.clone();

        channel.spec.ad_units = mock_channel_units();
        // NOTE: always set the spec.targeting_rules first
        channel.spec.targeting_rules = rules.to_vec();
        channel.spec.min_per_impression = 100_000_000_000_000.into();
        channel.spec.max_per_impression = 1_000_000_000_000_000.into();

        channel
    }

    fn mock_cache_campaign(channel: Channel) -> HashMap<ChannelId, Campaign> {
        let mut campaigns = HashMap::new();

        let mut campaign = Campaign {
            channel,
            status: Status::Active,
            balances: Default::default(),
        };
        campaign
            .balances
            .insert(IDS["publisher"], 100_000_000_000_000.into());

        campaigns.insert(campaign.channel.id, campaign);
        campaigns
    }

    #[tokio::test]
    async fn test_units_for_slot_route() {
        let logger = discard_logger();

        let server = MockServer::start().await;

        let market = Arc::new(
            MarketApi::new(server.uri() + "/market", logger.clone())
                .expect("should create market instance"),
        );

        let categories: [&str; 3] = ["IAB3", "IAB13-7", "IAB5"];
        let rules = get_mock_rules(&categories);
        let channel = mock_channel(&rules);

        let config = Config::new(None, "development").expect("should get config");
        let mock_client =
            MockClient::init(vec![mock_cache_campaign(channel.clone())], vec![], None).await;
        let mock_cache = Cache::initialize(mock_client).await;

        let ad_units = get_supermarket_ad_units();
        let mock_slot = get_supermarket_ad_slot(&rules, &categories);

        Mock::given(method("GET"))
            .and(path("/market/units"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&ad_units))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path(format!("/market/slots/{}", mock_slot.slot.ipfs)))
            .respond_with(ResponseTemplate::new(200).set_body_json(&mock_slot))
            .mount(&server)
            .await;
        let expected_response = get_expected_response(channel.clone(), &ad_units);

        let request = Request::get(format!(
            "/units-for-slot/{}?depositAsset=0x89d24A6b4CcB1B6fAA2625fE562bDD9a23260359",
            mock_slot.slot.ipfs
        ))
        .body(Body::empty())
        .unwrap();

        let actual_response = get_units_for_slot(&logger, market, &config, &mock_cache, request)
            .await
            .expect("call shouldn't fail with provided data");

        assert_eq!(http::StatusCode::OK, actual_response.status());

        let units_for_slot: UnitsForSlotResponse =
            serde_json::from_slice(&hyper::body::to_bytes(actual_response).await.unwrap())
                .expect("Should deserialize");

        // FIXME: Enable assert when issue #340 is solved: https://github.com/AdExNetwork/adex-validator-stack-rust/issues/340
        // assert_eq!(expected_response.targeting_input_base, units_for_slot.targeting_input_base);

        assert_eq!(
            expected_response.campaigns.len(),
            units_for_slot.campaigns.len()
        );
        assert_eq!(expected_response.campaigns, units_for_slot.campaigns);
        assert_eq!(
            expected_response.fallback_unit,
            units_for_slot.fallback_unit
        );
    }
}
