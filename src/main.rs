#![deny(clippy::all)]
#![deny(rust_2018_idioms)]
use std::net::SocketAddr;

use supermarket::serve;

use slog::Drain;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Construct our SocketAddr to listen on...
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));

    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    let logger = slog::Logger::root(drain, slog::o!());

    Ok(serve(addr, logger).await?)
}
