# AdEx Supermarket

## Running the Supermarket

The Supermarket runs on port `3000`. For a full list of all available CLI options on the Supermarket run `--help`:

```bash
cargo run -- --help
```

### CLI options

`supermarket [OPTIONS] -m <marketUrl>`

`--marketUrl` / `-m`: *required* - The url of the [`adex-market`](https://github.com/AdExNetwork/adex-market)

`--config` / `-c`: *optional* - If set it will use custom config file path, otherwise it will use [`prod.toml`](./config/prod.toml) (`production`) or [`dev.toml`](./config/dev.toml) (`development`), see the [`ENV` environment variable](#environment-variables) for more details.

### Environment variables

* `ENV` - *default*: `development` - `production` or `development`
* `PORT` - *default*: `3000` - the port on which the API will be accessible

### Docker

You can use the included [`Dockerfile`](./Dockerfile) to run the supermarket in a container.
First build the image:

```bash
docker build -t adex-supermarket .
```

You can use the same [environment variables](#environment-variables) as we as set the CLI option of the Supermarket using the following environment variables:

* `MARKET_URL`: *required* - sets the `--marketUrl` / `-m`
* `CONFIG`: *optional* - if set, it will pass the `--config` / `-c` option with the specified configuration file path


After building the image you can start a container:

```
docker run --detach -e ENV=production -e MARKET_URL=https://localhost:3000 adex-supermarket
```

## Development & Testing

For development purposes, all you have to do is run `Clippy`, `Rustfmt` & make sure that the tests pass:

```bash
cargo clippy
cargo fmt
cargo test
```

### Comparing market/supermarket output for /units-for-slot route

1. In `adex-market` run `npm run units-for-slot-test-output`

2. Run `cargo test get_sample_units_for_slot_output -- --show-output --ignored`
