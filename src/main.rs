#![deny(clippy::all)]
#![deny(rust_2018_idioms)]
use clap::{crate_version, App, Arg};
use supermarket::{config::Environment, serve, Config};

use slog::{info, Drain};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::str::FromStr;

const DEFAULT_PORT: u16 = 3000;
const DEFAULT_IP_ADDR: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = App::new("Supermarket")
        .version(crate_version!())
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
        .expect("No Market URL provided!")
        .parse()
        .expect("Market Url couldn't be parsed as URL");

    let environment = std::env::var("ENV")
        .ok()
        .map(|s| Environment::from_str(&s))
        .transpose()?
        .unwrap_or(Environment::Development);

    let port = std::env::var("PORT")
        .ok()
        .map(|s| u16::from_str(&s))
        .transpose()?
        .unwrap_or(DEFAULT_PORT);

    let ip_addr = std::env::var("IP_ADDR")
        .map(|s| {
            s.parse::<IpAddr>()
                .expect("Invalid Ip address was provided")
        })
        .unwrap_or_else(|_| DEFAULT_IP_ADDR);

    // Construct our SocketAddr to listen on...
    let socket_addr: SocketAddr = (ip_addr, port).into();

    let config_path = cli.value_of("config");

    let config = Config::new(config_path, environment)?;

    let logger = logger();

    info!(
        &logger,
        "ENV: `{}`; IP_ADDR: `{}`; PORT: `{}`; {:#?}", environment, ip_addr, port, config
    );

    info!(&logger, "Web server listening on: {}", &socket_addr);

    Ok(serve(socket_addr, logger, market_url, config).await?)
}

pub fn logger() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    slog::Logger::root(drain, slog::o!())
}
