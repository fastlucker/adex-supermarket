#![deny(clippy::all)]
#![deny(rust_2018_idioms)]
use hyper::{client::HttpConnector, Body, Client, Method, Request, Response, Server};
use std::fmt;
use std::net::SocketAddr;

#[derive(Debug)]
pub enum Error {
    Hyper(hyper::Error),
    Http(http::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Hyper(e) => e.fmt(f),
            Error::Http(e) => e.fmt(f),
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

pub async fn serve(addr: SocketAddr) {
    use hyper::service::{make_service_fn, service_fn};

    let client = Client::new();

    // And a MakeService to handle each connection...
    let make_service = make_service_fn(|_| {
        let client = client.clone();
        async move {
            Ok::<_, Error>(service_fn(move |req| {
                let client = client.clone();
                async move { handle(req, client.clone()).await }
            }))
        }
    });

    // Then bind and serve...
    let server = Server::bind(&addr).serve(make_service);

    // And run forever...
    if let Err(e) = server.await {
        eprintln!("server error: {}", e);
    }
}

async fn handle(
    mut req: Request<Body>,
    client: Client<HttpConnector>,
) -> Result<Response<Body>, Error> {
    match (req.uri().path(), req.method()) {
        ("/units-for-slot", &Method::GET) => {
            // @TODO: Implement route and caching
            Ok(Response::new(Body::from("/units-for-slot")))
        }
        _ => {
            use http::uri::{Authority, PathAndQuery, Uri};

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
