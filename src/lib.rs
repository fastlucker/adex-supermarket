#![deny(clippy::all)]
#![deny(rust_2018_idioms)]
pub use cache::Cache;
use hyper::{Body, Client, Method, Request, Response, Server};
use hyper_tls::HttpsConnector;
use std::net::SocketAddr;
use std::sync::Arc;
use thiserror::Error;

use http::{header::HOST, StatusCode, Uri};
use slog::{error, info, Logger};

pub mod cache;
pub mod config;
pub mod market;
pub mod sentry_api;
pub mod status;
mod units_for_slot;
pub mod util;

use market::{MarketApi, MarketUrl};
use units_for_slot::get_units_for_slot;

pub use config::{Config, Timeouts};
pub use sentry_api::SentryApi;

pub(crate) static ROUTE_UNITS_FOR_SLOT: &str = "/units-for-slot/";

// Uses Https Client
type HyperClient = Client<HttpsConnector<hyper::client::connect::HttpConnector>>;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Hyper(#[from] hyper::Error),
    #[error(transparent)]
    Http(#[from] http::Error),
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    #[error(transparent)]
    Url(#[from] url::ParseError),
    #[error(transparent)]
    Serde(#[from] serde_json::error::Error),
    #[error(transparent)]
    SentryApi(#[from] sentry_api::Error),
}

impl From<http::uri::InvalidUri> for Error {
    fn from(e: http::uri::InvalidUri) -> Error {
        Error::Http(e.into())
    }
}

// TODO: Fix `clone()`s for `Config`
pub async fn serve(
    addr: SocketAddr,
    logger: Logger,
    market_url: MarketUrl,
    config: Config,
) -> Result<(), Error> {
    use hyper::service::{make_service_fn, service_fn};

    let https = HttpsConnector::new();
    let client: HyperClient = Client::builder().build(https);

    let market = Arc::new(MarketApi::new(market_url, logger.clone())?);

    let cache = spawn_fetch_campaigns(logger.clone(), config.clone()).await?;

    // And a MakeService to handle each connection...
    let make_service = make_service_fn(|_| {
        let client = client.clone();
        let cache = cache.clone();
        let logger = logger.clone();
        let market = market.clone();
        let config = config.clone();
        async move {
            Ok::<_, Error>(service_fn(move |req| {
                let client = client.clone();
                let cache = cache.clone();
                let market = market.clone();
                let logger = logger.clone();
                let config = config.clone();
                async move {
                    match handle(req, config, cache, client, logger.clone(), market).await {
                        Err(error) => {
                            error!(&logger, "Error ocurred"; "error" => ?error);
                            Err(error)
                        }
                        Ok(resp) => Ok(resp),
                    }
                }
            }))
        }
    });

    // Then bind and serve...
    let server = Server::bind(&addr).serve(make_service);

    let graceful = server.with_graceful_shutdown(shutdown_signal());

    // And run forever...
    if let Err(e) = graceful.await {
        error!(&logger, "server error: {}", e);
    }

    Ok(())
}

async fn handle<C: cache::Client>(
    mut req: Request<Body>,
    config: Config,
    cache: Cache<C>,
    client: HyperClient,
    logger: Logger,
    market: Arc<MarketApi>,
) -> Result<Response<Body>, Error> {
    let is_units_for_slot = req.uri().path().starts_with(ROUTE_UNITS_FOR_SLOT);

    match (is_units_for_slot, req.method()) {
        (true, &Method::GET) => {
            get_units_for_slot(&logger, market.clone(), &config, &cache, req).await
        }
        (_, method) => {
            let method = method.clone();

            let path_and_query = req
                .uri()
                .path_and_query()
                .map(|p_q| {
                    let string = p_q.to_string();
                    // the MarketUrl (i.e. ApiUrl) always suffixes the path
                    string
                        .strip_prefix('/')
                        .map(ToString::to_string)
                        .unwrap_or(string)
                })
                .unwrap_or_default();

            let uri = format!("{}{}", market.market_url, path_and_query);

            *req.uri_mut() = uri.parse::<Uri>()?;

            // for Cloudflare we need to add a HOST header
            let market_host_header = {
                let url = market.market_url.to_url();
                let host = url
                    .host_str()
                    .expect("MarketUrl always has a host")
                    .to_string();

                match url.port() {
                    Some(port) => format!("{}:{}", host, port),
                    None => host,
                }
            };

            let host = market_host_header
                .parse()
                .expect("The MarketUrl should be valid HOST header");
            req.headers_mut().insert(HOST, host);

            let proxy_response = match client.request(req).await {
                Ok(response) => {
                    info!(&logger, "Proxied request to market"; "uri" => uri, "method" => %method);

                    response
                }
                Err(err) => {
                    error!(&logger, "Proxying request to market failed"; "uri" => uri, "method" => %method, "error" => ?&err);

                    service_unavailable()
                }
            };

            Ok(proxy_response)
        }
    }
}

async fn shutdown_signal() {
    // Wait for the CTRL+C signal
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C signal handler");
}

async fn spawn_fetch_campaigns(
    logger: Logger,
    config: Config,
) -> Result<Cache<cache::ApiClient>, Error> {
    let api_client = cache::ApiClient::init(logger.clone(), config.clone()).await?;
    let cache = Cache::initialize(api_client).await;

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
                        _ => info!(&logger, "New Campaigns fetched from Validators!"),
                    }
                }
                TimeFor::Update(_) => {
                    let timeout_duration = config.timeouts.cache_update_campaign_statuses;

                    match timeout(timeout_duration, cache_spawn.fetch_campaign_updates()).await {
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

pub(crate) fn not_found() -> Response<Body> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::empty())
        .expect("Not Found response should be valid")
}

pub(crate) fn service_unavailable() -> Response<Body> {
    Response::builder()
        .status(StatusCode::SERVICE_UNAVAILABLE)
        .body(Body::empty())
        .expect("Bad Request response should be valid")
}
