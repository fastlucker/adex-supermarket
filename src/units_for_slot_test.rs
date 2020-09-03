use super::*;
use slog::{Logger};
use std::sync::Arc;
use crate::{MarketApi, config::Config, cache::{Cache, MockCache}};
use lazy_static::lazy_static;


use wiremock::{matchers::method, Mock, MockServer, ResponseTemplate};

mod units_for_slot_tests {
	use super::*;
	// TODO
	// Pre-test:
	// 1. Create Cache trait
	// 2. Mock Cache struct
	// 3. Create mock data (campaigns/slots/units)
	// Test:
	// 1. Make a call to self/units-for-slot and pass the mock slot 
	// 2. Check output if it is correct
	fn setup_channel(leader_url: &Url, follower_url: &Url) -> Channel {
        let mut channel = DUMMY_CHANNEL.clone();
        let mut leader = DUMMY_VALIDATOR_LEADER.clone();
        leader.url = leader_url.to_string();

        let mut follower = DUMMY_VALIDATOR_FOLLOWER.clone();
        follower.url = follower_url.to_string();

        channel.spec.validators = (leader, follower).into();

        channel
	}

	#[tokio::test]
	async fn test_units_for_slot_route() {
		let logger = Logger.new();
		let mock_cache = MockCache::initialize(logger, config)
		let logger = slog::Logger::root(
			slog::Discard,
			o!("key1" => "value1", "key2" => "value2"),
		);

		let market_url = String::from("http://localhost:3012");
		let market = Arc::new(MarketApi::new(market_url, logger.clone()).await);

		let environment = std::env::var("ENV").unwrap_or_else(|_| "development".into());
		let config = Config::new("config.rs".to_string(), &environment).expect("should get config file");
		let cache = Cache::initialize(logger.clone(), config.clone()).await.expect("should initialize cache");
		let campaign = Campaign {
            channel,
            status: Status::Waiting,
            balances: Default::default(),
        };
		get_units_for_slot(&logger, market, config, cache, req: Request<Body>)
	}
}