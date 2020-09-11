use super::*;
use std::sync::Arc;
use std::collections::HashMap;
use crate::{MarketApi, config::Config, cache::{CacheLike, MockCache}, util::test::discard_logger};
use primitives::{AdSlot, BigNum, Channel, supermarket::units_for_slot::response::AdUnit, validator::ValidatorId, util::tests::prep_db::{DUMMY_CHANNEL, DUMMY_VALIDATOR_LEADER, DUMMY_VALIDATOR_FOLLOWER}};
use url::Url;
use wiremock::{matchers::{method, path}, Mock, MockServer, ResponseTemplate, Request as MockRequest};
use http::request::Request;
use hyper::body::Body;
use chrono::{TimeZone, Utc};

mod units_for_slot_tests {
	use super::*;
	fn setup_channel(leader_url: &Url, follower_url: &Url) -> Channel {
        let mut channel = DUMMY_CHANNEL.clone();
        let mut leader = DUMMY_VALIDATOR_LEADER.clone();
        leader.url = leader_url.to_string();

        let mut follower = DUMMY_VALIDATOR_FOLLOWER.clone();
        follower.url = follower_url.to_string();

        channel.spec.validators = (leader, follower).into();

        channel
	}

	fn get_mock_rules() -> Vec<Rule> {
		vec![] // TODO: add rules and modify the dummy channel to match them
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
		}]
	}

	fn get_mock_slot() -> &AdSlot {
		let min_per_impression: HashMap<String, BigNum> = HashMap::new();
		min_per_impression.insert("0x89d24A6b4CcB1B6fAA2625fE562bDD9a23260359".to_string(), BigNum::from_str("700000000000000"));
		let rules = get_mock_rules();
		let slot = AdSlot {
			ipfs: "QmXuYnyPAmF7DphCV8ja1Yvf5CCFZvZWUe65nzagqVthfV".to_string(),
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
		Response::default()
	}

	fn get_mock_request(market_url: &str) -> Request<Body> {
		let mock_slot = get_mock_slot();
		let slot_ipfs = &mock_slot.ipfs;
		Request::builder()
			.method("POST")
			.uri(format!("{}/units-for-slot/{}", &market_url, slot_ipfs))
			.body(Body::empty())
			.unwrap();
	}

	#[tokio::test]
	async fn test_units_for_slot_route() {
		let logger = discard_logger();

		let market_url = "http://localhost:3012";
		let market = Arc::new(MarketApi::new(market_url.to_string(), logger.clone()).expect("should create market instance"));
		// let market = Arc::new(MarketApi::new(market_url, logger.clone())?);


		let environment = std::env::var("ENV").unwrap_or_else(|_| "development".into());
		let config = Config::new(Some("config.rs"), &environment).expect("should get config file");
		let mock_cache = MockCache::initialize(logger.clone(), config.clone()).await.expect("should initialize cache");
		let leader_url = Url::parse("https://itchy.adex.network").expect("should parse this URL");
		let follower_url = Url::parse("https://scratchy.adex.network").expect("should parse this URL");
		let channel = setup_channel(&leader_url, &follower_url);
		let campaign = Campaign {
            channel,
            status: Status::Waiting,
            balances: Default::default(),
		};
		let mock_request = get_mock_request(&market_url);
		let mock_units = get_mock_units();
		let server = MockServer::start().await;
		let expected_response = get_expected_response();
		Mock::given(method("GET"))
			.and(path("/units"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&mock_units))
            .mount(&server)
            .await;
		let res = get_units_for_slot(&logger, market, &config, &mock_cache, mock_request).await.expect("call shouldn't fail with provided data");
		assert_eq!(res.body, expected_response.body);
	}
}