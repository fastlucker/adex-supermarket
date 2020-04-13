#![deny(clippy::all)]
#![deny(rust_2018_idioms)]
use cache::Cache;
use hyper::{client::HttpConnector, Body, Client, Method, Request, Response, Server};
use std::fmt;
use std::net::SocketAddr;

use http::Uri;
use slog::{error, info, Logger};

mod cache;
mod market;
mod sentry_api;
// @TODO: mod status; This is suppressing the warnings
pub mod status;

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

pub async fn serve(addr: SocketAddr, logger: Logger, market_url: String) -> Result<(), Error> {
    use hyper::service::{make_service_fn, service_fn};

    let client = Client::new();

    let cache = spawn_fetch_campaigns(&market_url, logger.clone()).await?;

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

async fn spawn_fetch_campaigns(market_uri: &str, logger: Logger) -> Result<Cache, reqwest::Error> {
    let cache = Cache::initialize(market_uri.into(), logger.clone()).await?;
    info!(
        &logger,
        "Campaigns are fetched from market & Cache is initialized..."
    );

    let cache_spawn = cache.clone();
    // Every few minutes, we will get the non-finalized from the market,
    // in order to keep discovering new campaigns.
    tokio::spawn(async move {
        use tokio::time::{delay_for, Duration};
        info!(&logger, "Task for updating campaign has been spawned");

        loop {
            // @TODO: Move to config
            delay_for(Duration::from_secs(1 * 60)).await;
            if let Err(e) = cache_spawn.update_campaigns().await {
                error!(&logger, "{}", e);
            }
            info!(&logger, "Campaigns updated!");
        }
        // Every few seconds, we will update our active campaigns from the
        // validators (update their latest balance tree).
    });

    Ok(cache)
}
