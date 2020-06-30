use crate::{
    cache::Campaign, market::AdSlotResponse, not_found, status::Status, Cache, Config, Error,
    MarketApi, ROUTE_UNITS_FOR_SLOT,
};
use chrono::Utc;
use hyper::{header::USER_AGENT, Body, Request, Response};
use primitives::{
    channel::Pricing,
    targeting::{get_pricing_bounds, AdSlot, Error as EvalError, Global, Input, Output, Rule},
    util::tests::prep_db::DUMMY_CHANNEL,
    ValidatorId,
};
use slog::{info, Logger};
use std::convert::TryFrom;
use std::sync::Arc;
use url::{form_urlencoded, Url};
use woothee::parser::Parser;

pub async fn get_units_for_slot(
    logger: &Logger,
    market: Arc<MarketApi>,
    config: &Config,
    cache: &Cache,
    req: Request<Body>,
) -> Result<Response<Body>, Error> {
    let ipfs = req.uri().path().trim_start_matches(ROUTE_UNITS_FOR_SLOT);

    if ipfs.is_empty() {
        Ok(not_found())
    } else {
        let ad_slot_response = match market.fetch_slot(&ipfs).await? {
            Some(response) => {
                info!(&logger, "Fetched AdSlot"; "AdSlot" => ipfs);

                response
            }
            None => {
                info!(
                    &logger,
                    "AdSlot ({}) not found in Market",
                    ipfs;
                    "AdSlot" => ipfs
                );

                return Ok(not_found());
            }
        };

        let units = market.fetch_units(&ad_slot_response.slot).await?;

        let units_ipfses: Vec<&str> = units.iter().map(|au| au.ipfs.as_str()).collect();

        info!(&logger, "Fetched AdUnits for AdSlot"; "AdSlot" => ipfs, "AdUnits" => ?&units_ipfses);

        let query = req.uri().query().unwrap_or_default();
        let parsed_query = form_urlencoded::parse(query.as_bytes());

        let deposit_assets: Vec<String> = parsed_query
            .filter_map(|(key, value)| {
                if key == "depositAsset" {
                    Some(value.to_string())
                } else {
                    None
                }
            })
            .collect();

        // For each adUnits apply input
        let ua_parser = Parser::new();
        let user_agent = req
            .headers()
            .get(USER_AGENT)
            .and_then(|h| h.to_str().map(ToString::to_string).ok())
            .unwrap_or_default();
        let parsed = ua_parser.parse(&user_agent);
        let user_agent_os = parsed.as_ref().map(|p| p.os.to_string());

        let user_agent_browser_family = parsed.as_ref().map(|p| p.browser_type.to_string());

        let country = req
            .headers()
            .get("cf-ipcountry")
            .and_then(|h| h.to_str().map(ToString::to_string).ok());

        let hostname = Url::parse(&ad_slot_response.slot.website.clone().unwrap_or_default())
            .ok()
            .and_then(|url| url.host().map(|h| h.to_string()))
            .unwrap_or_default();

        let publisher_id = ad_slot_response.slot.owner;

        let campaigns_limited_by_earner =
            get_campaigns(cache, config, &deposit_assets, publisher_id).await;

        apply_targeting(
            campaigns_limited_by_earner,
            ad_slot_response,
            country,
            user_agent_os,
            user_agent_browser_family,
            hostname,
        )
        .await;

        // @TODO: https://github.com/AdExNetwork/adex-supermarket/issues/9

        Ok(Response::new(Body::from("")))
    }
}

async fn get_campaigns(
    cache: &Cache,
    config: &Config,
    deposit_assets: &[String],
    publisher_id: ValidatorId,
) -> Vec<Campaign> {
    let active_campaigns = cache.active.read().await;

    let (mut campaigns_by_earner, rest_of_campaigns): (Vec<&Campaign>, Vec<&Campaign>) =
        active_campaigns
            .iter()
            .filter_map(|(_, campaign)| {
                // The Supermarket has the Active status combining Active & Ready from Market
                if campaign.status == Status::Active
                    && campaign.channel.creator != publisher_id
                    && deposit_assets.contains(&campaign.channel.deposit_asset)
                {
                    Some(campaign)
                } else {
                    None
                }
            })
            .partition(|&campaign| campaign.balances.contains_key(&publisher_id));

    if campaigns_by_earner.len() >= config.limits.max_channels_earning_from.into() {
        campaigns_by_earner.into_iter().cloned().collect()
    } else {
        campaigns_by_earner.extend(rest_of_campaigns.iter());

        campaigns_by_earner.into_iter().cloned().collect()
    }
}

async fn apply_targeting(
    campaigns: Vec<Campaign>,
    ad_slot_response: AdSlotResponse,
    country: Option<String>,
    user_agent_os: Option<String>,
    user_agent_browser_family: Option<String>,
    hostname: String,
) {
    let publisher_id = ad_slot_response.slot.owner;
    campaigns
        .into_iter()
        .filter_map::<response::Campaign, _>(|campaign| {
            let ad_units = campaign
                .channel
                .spec
                .ad_units
                .iter()
                .filter(|ad_unit| ad_unit.ad_type == ad_slot_response.slot.ad_type)
                .cloned()
                .collect::<Vec<_>>();

            if ad_units.len() == 0 {
                None
            } else {
                let targeting_rules = if campaign.channel.targeting_rules.len() > 0 {
                    campaign.channel.targeting_rules.clone()
                } else {
                    campaign.channel.spec.targeting_rules.clone()
                };

                let matching_units: Vec<(response::AdUnit, Pricing)> = ad_units
                    .into_iter()
                    .filter_map(|ad_unit| {
                        let input = Input {
                            ad_view: None,
                            global: Global {
                                ad_slot_id: ad_slot_response.slot.ipfs.clone(),
                                ad_slot_type: ad_slot_response.slot.ad_type.clone(),
                                publisher_id,
                                country: country.clone(),
                                event_type: "IMPRESSION".to_string(),
                                // TODO: handle the error instead of `panic!`ing
                                seconds_since_epoch: u64::try_from(Utc::now().timestamp())
                                    .expect("Should convert"),
                                user_agent_os: user_agent_os.clone(),
                                user_agent_browser_family: user_agent_browser_family.clone(),
                                ad_unit: Default::default(),
                                balances: Default::default(),
                                channel: DUMMY_CHANNEL.clone(),
                                status: Default::default(),
                            },
                            ad_slot: Some(AdSlot {
                                categories: ad_slot_response.categories.clone(),
                                hostname: hostname.clone(),
                                alexa_rank: ad_slot_response.alexa_rank,
                            }),
                        };

                        let pricing_bounds = get_pricing_bounds(&campaign.channel, "IMPRESSION");
                        let mut output = Output {
                            show: true,
                            boost: 1.0,
                            // only "IMPRESSION" event can be used for this `Output`
                            price: vec![("IMPRESSION".to_string(), pricing_bounds.min.clone())]
                                .into_iter()
                                .collect(),
                        };

                        eval_multiple(&targeting_rules, &input, &mut output);

                        if output.show == false {
                            return None;
                        }

                        let price = Pricing {
                            min: pricing_bounds.min,
                            max: match output.price.get("IMPRESSION") {
                                Some(output_price) => output_price.min(&pricing_bounds.max).clone(),
                                None => pricing_bounds.max,
                            },
                        };

                        // @TODO: if (price.lt(GLOBAL_MIN_IMPRESSION_PRICE)) return null

                        // @TODO:
                        // // Execute the adSlot rules after we've taken the price since they're not
                        // // allowed to change the price
                        // if (!evaluateMultiple(input, output, adSlotRules, onTypeErr).show)
                        // return null

                        let ad_unit = response::AdUnit::from(ad_unit);

                        Some((ad_unit, price))
                    })
                    .collect();

                // TODO: The rest of the filtration and mapping here!
                None
            }
        });
}

// @TODO: Logging & move to Targeting when ready
fn eval_multiple(rules: &[Rule], input: &Input, output: &mut Output) {
    for rule in rules {
        match rule.eval(input, output) {
            Ok(_) => {}
            Err(EvalError::UnknownVariable) => {}
            Err(EvalError::TypeError) => todo!("OnTypeErr logging"),
        }

        if output.show == false {
            return;
        }
    }
}

mod response {
    use chrono::{
        serde::{ts_milliseconds, ts_milliseconds_option},
        DateTime, Utc,
    };
    use primitives::{BigNum, ChannelId, SpecValidators, ValidatorId};
    use serde::Serialize;

    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub(super) struct Campaign {
        pub id: ChannelId,
        pub creator: ValidatorId,
        pub deposit_asset: String,
        pub deposit_amount: BigNum,
        pub spec: Spec,
    }

    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub(super) struct Spec {
        #[serde(with = "ts_milliseconds")]
        pub withdraw_period_start: DateTime<Utc>,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "ts_milliseconds_option"
        )]
        pub active_from: Option<DateTime<Utc>>,
        #[serde(with = "ts_milliseconds")]
        pub created: DateTime<Utc>,
        pub validators: SpecValidators,
    }

    pub(super) struct AdUnit {
        /// Same as `ipfs`
        pub id: String,
        pub media_url: String,
        pub media_mime: String,
        pub target_url: String,
    }

    impl From<primitives::AdUnit> for AdUnit {
        fn from(ad_unit: primitives::AdUnit) -> Self {
            Self {
                id: ad_unit.ipfs,
                media_url: ad_unit.media_url,
                media_mime: ad_unit.media_mime,
                target_url: ad_unit.target_url,
            }
        }
    }
}
