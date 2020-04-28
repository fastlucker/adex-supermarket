#![deny(clippy::all)]
#![deny(rust_2018_idioms)]
use std::net::SocketAddr;

use clap::{App, Arg};
use supermarket::{serve, Config};

use slog::Drain;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = App::new("Supermarket")
        .version("0.1.0")
        .arg(
            Arg::with_name("marketUrl")
                .short("m")
                .help("URL for the market")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("config")
                .short("c")
                .help("Config file path")
                .takes_value(true),
        )
        .get_matches();

    let market_url = cli
        .value_of("marketUrl")
        .expect("No market URL provided!")
        .to_string();

    let environment = std::env::var("ENV").unwrap_or_else(|_| "development".into());
    let config_path = cli.value_of("config");

    let config = Config::new(config_path, &environment)?;
    // Construct our SocketAddr to listen on...
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));

    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    let logger = slog::Logger::root(drain, slog::o!());

    Ok(serve(addr, logger, market_url, config).await?)
}
