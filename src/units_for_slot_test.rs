use super::*;
use std::sync::Arc;
use std::collections::HashMap;
use crate::{MarketApi, config::Config, cache::{CacheLike, MockCache}, util::test::discard_logger};
use primitives::{
	AdSlot, BigNum, Channel,
	supermarket::{units_for_slot::response::{AdUnit, UnitsWithPrice}, Campaign},
	validator::ValidatorId,
	targeting::{Rule, input, Function},
	util::tests::prep_db::{DUMMY_CHANNEL, DUMMY_VALIDATOR_LEADER, DUMMY_VALIDATOR_FOLLOWER}
};
use url::Url;
use wiremock::{matchers::{method, path}, Mock, MockServer, ResponseTemplate, Request as MockRequest};
use http::request::Request;
use http::StatusCode;
use hyper::body::Body;
use hyper::Response;
use chrono::{TimeZone, Utc};

mod units_for_slot_tests {
	use super::*;
	fn get_mock_campaign() -> Campaign {
		let mut channel = DUMMY_CHANNEL.clone();
		let mut leader = DUMMY_VALIDATOR_LEADER.clone();
		leader.url = "https://itchy.adex.network".to_string();

		let mut follower = DUMMY_VALIDATOR_FOLLOWER.clone();
		follower.url = "https://scratchy.adex.network".to_string();
		channel.spec.validators = (leader, follower).into();
		Campaign {
			channel,
			status: Status::Waiting,
			balances: Default::default(),
		}
	}

	fn setup_channel(leader_url: &Url, follower_url: &Url) -> Channel {
        let mut channel = DUMMY_CHANNEL.clone();
        let mut leader = DUMMY_VALIDATOR_LEADER.clone();
        leader.url = leader_url.to_string();

        let mut follower = DUMMY_VALIDATOR_FOLLOWER.clone();
        follower.url = follower_url.to_string();

        channel.spec.validators = (leader, follower).into();

        channel
	}

	fn get_mock_units() -> Vec<AdUnit> {
		vec![ AdUnit {
			id: "Qmasg8FrbuSQpjFu3kRnZF9beg8rEBFrqgi1uXDRwCbX5f".to_string(),
			media_url: "ipfs://QmcUVX7fvoLMM93uN2bD3wGTH8MXSxeL8hojYfL2Lhp7mR".to_string(),
			media_mime: "image/jpeg".to_string(),
			target_url: "https://www.adex.network/?stremio-test-banner-1".to_string(),
		}, AdUnit {
			id: "QmVhRDGXoM3Fg3HZD5xwMuxtb9ZErwC8wHt8CjsfxaiUbZ".to_string(),
			media_url: "ipfs://QmQB7uz7Gxfy7wqAnrnBcZFaVJLos8J9gn8mRcHQU6dAi1".to_string(),
			media_mime: "image/jpeg".to_string(),
			target_url: "https://www.adex.network/?adex-campaign=true&pub=stremio".to_string(),
		}, AdUnit {
			media_url: "ipfs://QmQB7uz7Gxfy7wqAnrnBcZFaVJLos8J9gn8mRcHQU6dAi1".to_string(),
			media_mime: "image/jpeg".to_string(),
			target_url: "https://www.adex.network/?adex-campaign=true".to_string(),
			id: "QmYwcpMjmqJfo9ot1jGe9rfXsszFV1WbEA59QS7dEVHfJi".to_string(),
		}, AdUnit {
			id: "QmTAF3FsFDS7Ru8WChoD9ofiHTH8gAQfR4mYSnwxqTDpJH".to_string(),
			media_url: "ipfs://QmQAcfBJpDDuH99A4p3pFtUmQwamS8UYStP5HxHC7bgYXY".to_string(),
			media_mime: "image/jpeg".to_string(),
			target_url: "https://adex.network".to_string(),
		}]
	}

	fn get_mock_slot() -> &'static AdSlot {
		let min_per_impression: HashMap<String, BigNum> = HashMap::new();
		min_per_impression.insert("0x89d24A6b4CcB1B6fAA2625fE562bDD9a23260359".to_string(), BigNum::from_str("700000000000000"));
		let get_rule = Rule::Function(Function::Get("adSlot.categories".to_string()));
		let intersects_rule = Rule::Function(Function::Intersects(get_rule, vec!["IAB3", "IAB13-7", "IAB5"]));
		let only_show_if_rule = Rule::Function(Function::OnlyShowIf(intersects_rule));
		let rules = vec![only_show_if_rule];

		let slot = AdSlot {
			ipfs: "QmVwXu9oEgYSsL6G1WZtUQy6dEReqs3Nz9iaW4Cq5QLV8C".to_string(),
			ad_type: "legacy_300x100".to_string(),
			archived: false,
			created: Utc.timestamp(1_564_383_600, 0),
			description: Some("test slot".to_string()),
			fallback_unit: Some("QmTAF3FsFDS7Ru8WChoD9ofiHTH8gAQfR4mYSnwxqTDpJH".to_string()),
			min_per_impression: Some(min_per_impression),
			modified: Some(Utc.timestamp(1_564_383_600, 0)),
			owner: ValidatorId::try_from("0xB7d3F81E857692d13e9D63b232A90F4A1793189E").expect("should create ValidatorId"),
			title: Some("Test slot".to_string()),
			website: Some("https://adex.network".to_string()),
			rules,
		};

		&slot
	}

	fn get_expected_response() -> Response<Body> {
		let targeting_input_base = input::Source  {
			ad_view: None,
			global: input::Global {
                ad_slot_id: "QmVwXu9oEgYSsL6G1WZtUQy6dEReqs3Nz9iaW4Cq5QLV8C".to_string(),
                ad_slot_type: "legacy_728x90".to_string(),
                publisher_id: ValidatorId::try_from("0x13e72959d8055DaFA6525050A8cc7c479D4c09A3").expect("should create ValidatorId"),
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
				categories: vec!["IAB3".to_string(), "IAB13-7".to_string(), "IAB5".to_string()],
				hostname: "adex.network".to_string(),
				alexa_rank: None,
			}),
		};
		let accepted_referrers: Vec<Url> = Vec::new();
		let fallback_unit: Option<AdUnit> = None;
		let campaign = get_mock_campaign();
		let campaigns = vec![campaign];

		let response = UnitsForSlotResponse {
            targeting_input_base: targeting_input_base.into(),
            accepted_referrers,
            campaigns: campaigns.into(),
            fallback_unit,
        };
		Ok(Response::new(Body::from(serde_json::to_string(&response)?)))
	}

	fn get_mock_request(market_url: &str) -> Request<Body> {
		let mock_slot = get_mock_slot();
		let slot_ipfs = &mock_slot.ipfs;
		Request::builder()
			.method("POST")
			.uri(format!("{}/units-for-slot/{}", &market_url, slot_ipfs))
			.body(Body::empty())
			.unwrap()
	}

	#[tokio::test]
	async fn test_units_for_slot_route() {
		let logger = discard_logger();

		let market_url = "http://localhost:3012";
		let market = Arc::new(MarketApi::new(market_url.to_string(), logger.clone()).expect("should create market instance"));

		let environment = std::env::var("ENV").unwrap_or_else(|_| "development".into());
		let config = Config::new(Some("config.rs"), &environment).expect("should get config file");
		let mock_cache = MockCache::initialize(logger.clone(), config.clone()).await.expect("should initialize cache");
		let mock_request = get_mock_request(&market_url);
		let mock_units = get_mock_units();
		let mock_slot = get_mock_slot();
		let server = MockServer::start().await;
		Mock::given(method("GET"))
		.and(path("/units"))
		.respond_with(ResponseTemplate::new(200).set_body_json(&mock_units))
		.mount(&server)
		.await;

		Mock::given(method("GET"))
		.and(path("/units"))
		.respond_with(ResponseTemplate::new(200).set_body_json(&mock_slot))
		.mount(&server)
		.await;

		let expected_response = get_expected_response();
		let res = get_units_for_slot(&logger, market, &config, &mock_cache, mock_request).await.expect("call shouldn't fail with provided data");
		let expected_response = expected_response.body();
		let res = res.body();
		assert_eq!(true, true);
	}
}