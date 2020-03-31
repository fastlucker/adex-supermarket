#![deny(clippy::all)]
#![deny(rust_2018_idioms)]
use cache::Cache;
use hyper::{client::HttpConnector, Body, Client, Method, Request, Response, Server};
use std::fmt;
use std::net::SocketAddr;

use http::{uri::Authority, Uri};

mod cache;
mod market;

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

pub async fn serve(addr: SocketAddr) -> Result<(), Error> {
    use hyper::service::{make_service_fn, service_fn};

    let client = Client::new();
    // @TODO: take this from config or env. variable
    let market_url = "http://localhost:8005";

    let cache = spawn_fetch_campaigns(market_url).await?;

    // And a MakeService to handle each connection...
    let make_service = make_service_fn(|_| {
        let client = client.clone();
        let cache = cache.clone();
        async move {
            Ok::<_, Error>(service_fn(move |req| {
                let client = client.clone();
                let cache = cache.clone();
                async move { handle(req, cache, client).await }
            }))
        }
    });

    // Then bind and serve...
    let server = Server::bind(&addr).serve(make_service);

    // And run forever...
    if let Err(e) = server.await {
        eprintln!("server error: {}", e);
    }

    Ok(())
}

async fn handle(
    mut req: Request<Body>,
    _cache: Cache,
    client: Client<HttpConnector>,
) -> Result<Response<Body>, Error> {
    match (req.uri().path(), req.method()) {
        ("/units-for-slot", &Method::GET) => {
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

            let uri = Uri::builder()
                // @TODO: Move to config or env. variable
                .scheme("http")
                .authority(Authority::from_static("localhost:8005"))
                .path_and_query(path_and_query)
                .build()?;

            *req.uri_mut() = uri;

            Ok(client.request(req).await?)
        }
    }
}

async fn spawn_fetch_campaigns(market_uri: &str) -> Result<Cache, reqwest::Error> {
    let cache = Cache::initialize(market_uri.into()).await?;

    let cache_spawn = cache.clone();
    // Every few minutes, we will get the non-finalized from the market,
    // in order to keep discovering new campaigns.
    tokio::spawn(async move {
        use tokio::time::{delay_for, Duration};
        loop {
            // @TODO: Move to config
            delay_for(Duration::from_secs(1 * 60)).await;
            // @TODO: Logging
            match cache_spawn.update_campaigns().await {
                Err(e) => eprintln!("{}", e),
                _ => println!("Success! It should log as well"),
            };
        }
        // Every few seconds, we will update our active campaigns from the
        // validators (update their latest balance tree).
    });

    Ok(cache)
}
