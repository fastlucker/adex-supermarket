use super::*;
use slog::{Logger};
use std::sync::Arc;
use crate::{MarketApi, config::Config, cache::Cache};
use lazy_static::lazy_static;


use wiremock::{matchers::method, Mock, MockServer, ResponseTemplate};

mod units_for_slot_tests {
	use super::*;
	// TODO
    #[tokio::test]
	async fn get_response() {
		let logger = slog::Logger::root(
			slog::Discard,
			o!("key1" => "value1", "key2" => "value2"),
		);

		let market_url = String::from("http://localhost:3012");
		let market = Arc::new(MarketApi::new(market_url, logger.clone()).await);

		let environment = std::env::var("ENV").unwrap_or_else(|_| "development".into());
		let config = Config::new("config.rs".to_string(), &environment).expect("should get config file");
		let cache = Cache::initialize(logger.clone(), config.clone()).await.expect("should initialize cache");

		// let req =
		get_units_for_slot(&logger, market, config, cache, req);

		assert_eq!(true, true);
	}
}