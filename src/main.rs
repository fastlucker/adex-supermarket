#![deny(clippy::all)]
#![deny(rust_2018_idioms)]
use std::net::SocketAddr;

use clap::{App, Arg};
use supermarket::serve;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = App::new("Supermarket")
        .version("0.1.0")
        .arg(
            Arg::with_name("marketUrl")
                .help("URL for the market")
                .required(true)
                .takes_value(true),
        )
        .get_matches();

    let market_url = cli.value_of("marketUrl").expect("No market URL provided!").to_string();
    // Construct our SocketAddr to listen on...
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));

    Ok(serve(addr, market_url).await?)
}
