#![deny(clippy::all)]
#![deny(rust_2018_idioms)]
use cache::Cache;
use hyper::{client::HttpConnector, Body, Client, Method, Request, Response, Server};
use std::fmt;
use std::net::SocketAddr;

use http::Uri;
use slog::{error, info, Logger};

mod cache;
pub mod config;
mod market;
mod sentry_api;
// @TODO: mod status; This is suppressing the warnings
pub mod status;

pub use config::{Config, Timeouts};
pub use sentry_api::SentryApi;

#[derive(Debug)]
pub enum Error {
    Hyper(hyper::Error),
    Http(http::Error),
    Reqwest(reqwest::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Hyper(e) => e.fmt(f),
            Error::Http(e) => e.fmt(f),
            Error::Reqwest(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for Error {}

impl From<hyper::Error> for Error {
    fn from(e: hyper::Error) -> Error {
        Error::Hyper(e)
    }
}

impl From<http::Error> for Error {
    fn from(e: http::Error) -> Error {
        Error::Http(e)
    }
}

impl From<reqwest::Error> for Error {
    fn from(e: reqwest::Error) -> Error {
        Error::Reqwest(e)
    }
}

impl From<http::uri::InvalidUri> for Error {
    fn from(e: http::uri::InvalidUri) -> Error {
        Error::Http(e.into())
    }
}

pub async fn serve(
    addr: SocketAddr,
    logger: Logger,
    market_url: String,
    config: Config,
) -> Result<(), Error> {
    use hyper::service::{make_service_fn, service_fn};

    let client = Client::new();

    let cache = spawn_fetch_campaigns(&market_url, logger.clone(), config).await?;

    // And a MakeService to handle each connection...
    let make_service = make_service_fn(|_| {
        let client = client.clone();
        let cache = cache.clone();
        let logger = logger.clone();
        let market_url = market_url.clone();
        async move {
            Ok::<_, Error>(service_fn(move |req| {
                let client = client.clone();
                let cache = cache.clone();
                let market_url = market_url.clone();
                let logger = logger.clone();
                async move { handle(req, cache, client, logger, market_url).await }
            }))
        }
    });

    // Then bind and serve...
    let server = Server::bind(&addr).serve(make_service);

    // And run forever...
    if let Err(e) = server.await {
        error!(&logger, "server error: {}", e);
    }

    Ok(())
}

async fn handle(
    mut req: Request<Body>,
    _cache: Cache,
    client: Client<HttpConnector>,
    logger: Logger,
    market_uri: String,
) -> Result<Response<Body>, Error> {
    match (req.uri().path(), req.method()) {
        ("/units-for-slot", &Method::GET) => {
            info!(&logger, "/units-for-slot requested");
            // @TODO: Implement route and caching
            Ok(Response::new(Body::from("/units-for-slot")))
        }
        _ => {
            use http::uri::PathAndQuery;

            let path_and_query = req
                .uri()
                .path_and_query()
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| PathAndQuery::from_static(""));

            let uri = format!("{}{}", market_uri, path_and_query).parse::<Uri>()?;

            *req.uri_mut() = uri;

            let proxy_response = client.request(req).await?;

            info!(&logger, "Proxied request to market");

            Ok(proxy_response)
        }
    }
}

async fn spawn_fetch_campaigns(
    market_uri: &str,
    logger: Logger,
    config: Config,
) -> Result<Cache, reqwest::Error> {
    let cache = Cache::initialize(market_uri.into(), logger.clone(), config.clone()).await?;
    info!(
        &logger,
        "Campaigns have been fetched from the Market & Cache is now initialized..."
    );

    let cache_spawn = cache.clone();
    // Every few minutes, we will get the non-finalized from the market,
    // in order to keep discovering new campaigns.
    tokio::spawn(async move {
        use futures::stream::{select, StreamExt};
        use tokio::time::{interval, timeout, Instant};
        info!(&logger, "Task for updating campaign has been spawned");

        // Every X seconds, we will update our active campaigns from the
        // validators (update their latest balance tree).
        let new_interval = interval(config.fetch_campaigns_every).map(TimeFor::New);
        let update_interval = interval(config.update_campaigns_every).map(TimeFor::Update);

        enum TimeFor {
            New(Instant),
            Update(Instant),
        }

        let mut select_time = select(new_interval, update_interval);

        while let Some(time_for) = select_time.next().await {
            // @TODO: Timeout the action
            match time_for {
                TimeFor::New(_) => {
                    let timeout_duration = config.timeouts.cache_fetch_campaigns_from_market;

                    match timeout(timeout_duration, cache_spawn.fetch_new_campaigns()).await {
                        Err(_elapsed) => error!(
                            &logger,
                            "Fetching new Campaigns timed out";
                            "allowed secs" => timeout_duration.as_secs()
                        ),
                        Ok(Err(e)) => error!(&logger, "{}", e),
                        _ => info!(&logger, "New Campaigns fetched from Market!"),
                    }
                }
                TimeFor::Update(_) => {
                    let timeout_duration = config.timeouts.cache_update_campaign_statuses;

                    match timeout(timeout_duration, cache_spawn.update_campaigns()).await {
                        Ok(_) => info!(&logger, "Campaigns statuses updated from Validators!"),
                        Err(_elapsed) => error!(
                                &logger,
                                "Updating Campaigns statuses timed out";
                                "allowed secs" => timeout_duration.as_secs()
                        ),
                    }
                }
            }
        }
    });

    Ok(cache)
}
