use crate::{
    cache::Campaign, market::AdSlotResponse, not_found, service_unavailable, status::Status, Cache,
    Config, Error, MarketApi, ROUTE_UNITS_FOR_SLOT,
};
use chrono::Utc;
use hyper::{header::USER_AGENT, Body, Request, Response};
use primitives::{
    targeting::{get_pricing_bounds, AdSlot, Error as EvalError, Global, Input, Output, Rule},
    util::tests::prep_db::DUMMY_CHANNEL,
    ValidatorId,
};
use response::UnitsWithPrice;
use serde::Serialize;
use slog::{error, info, Logger};
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
        let fetch_slot = match market.fetch_slot(&ipfs).await {
            Ok(response) => response,
            Err(err) => {
                error!(&logger, "Error fetching AdSlot"; "AdSlot" => ipfs, "error" => %err);

                return Ok(service_unavailable());
            }
        };

        let ad_slot_response = match fetch_slot {
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
        // @TODO: Handle error with units retrieval
        let units = market.fetch_units(&ad_slot_response.slot).await?;
        let accepted_referrers = ad_slot_response.accepted_referrers.clone();
        let units_ipfses: Vec<&str> = units.iter().map(|au| au.ipfs.as_str()).collect();
        // let fallback_unit_ipfs = &ad_slot_response.slot.fallback_unit?;
        // let fallback_unit = market.fetch_unit(fallback_unit_ipfs).await?;
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

        info!(&logger, "Fetched Cache campaigns"; "length" => campaigns_limited_by_earner.len(), "publisher_id" => %publisher_id);

        // We return those in the result (which means AdView would have those) but we don't actually use them
        // we do that in order to have the same variables as the validator, so that the `price` is the same
        let targeting_input_ad_slot = Some(AdSlot {
            categories: ad_slot_response.categories.clone(),
            hostname: hostname.clone(),
            alexa_rank: ad_slot_response.alexa_rank,
        });

        let campaigns = apply_targeting(
            config,
            campaigns_limited_by_earner,
            ad_slot_response,
            country,
            user_agent_os,
            user_agent_browser_family,
            hostname,
        )
        .await;

        // @TODO: https://github.com/AdExNetwork/adex-supermarket/issues/9
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct UnitsForSlotResponse {
            //? targeting_input_base: Vec<Rule>,
            accepted_referrers: Vec<Url>,
            // fallback_unit: AdUnit,
            campaigns: Vec<response::Campaign>,
        }

        let response = UnitsForSlotResponse {
            accepted_referrers,
            campaigns,
        };

        Ok(Response::new(Body::from(serde_json::to_string(&response)?)))
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
    config: &Config,
    campaigns: Vec<Campaign>,
    ad_slot_response: AdSlotResponse,
    country: Option<String>,
    user_agent_os: Option<String>,
    user_agent_browser_family: Option<String>,
    hostname: String,
) -> Vec<response::Campaign> {
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

            if ad_units.is_empty() {
                None
            } else {
                let targeting_rules = if !campaign.channel.targeting_rules.is_empty() {
                    campaign.channel.targeting_rules.clone()
                } else {
                    campaign.channel.spec.targeting_rules.clone()
                };

                let matching_units: Vec<UnitsWithPrice> = ad_units
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
                            ad_slot: None,
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

                        if !output.show {
                            return None;
                        }

                        let max_price = match output.price.get("IMPRESSION") {
                            Some(output_price) => output_price.min(&pricing_bounds.max).clone(),
                            None => pricing_bounds.max,
                        };
                        let price = pricing_bounds.min.max(max_price);

                        if price < config.limits.global_min_impression_price {
                            return None;
                        }

                        // Execute the adSlot rules after we've taken the price since they're not
                        // allowed to change the price
                        eval_multiple(&ad_slot_response.slot.rules, &input, &mut output);
                        if !output.show {
                            return None;
                        }

                        let ad_unit = response::AdUnit::from(ad_unit);

                        Some(UnitsWithPrice {
                            unit: ad_unit,
                            price,
                        })
                    })
                    .collect();

                if matching_units.is_empty() {
                    None
                } else {
                    Some(response::Campaign {
                        channel: campaign.channel.into(),
                        targeting_rules,
                        units_with_price: matching_units,
                    })
                }
            }
        })
        .collect()
}

// @TODO: Logging & move to Targeting when ready
fn eval_multiple(rules: &[Rule], input: &Input, output: &mut Output) {
    for rule in rules {
        match rule.eval(input, output) {
            Ok(_) => {}
            Err(EvalError::UnknownVariable) => {}
            Err(EvalError::TypeError) => todo!("OnTypeErr logging"),
        }

        if !output.show {
            return;
        }
    }
}

mod response {
    use chrono::{
        serde::{ts_milliseconds, ts_milliseconds_option},
        DateTime, Utc,
    };
    use primitives::{targeting::Rule, BigNum, ChannelId, SpecValidators, ValidatorId};
    use serde::Serialize;

    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub(super) struct UnitsWithPrice {
        pub unit: AdUnit,
        pub price: BigNum,
    }

    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub(super) struct Campaign {
        #[serde(flatten)]
        pub channel: Channel,
        pub targeting_rules: Vec<Rule>,
        pub units_with_price: Vec<UnitsWithPrice>,
    }

    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub(super) struct Channel {
        pub id: ChannelId,
        pub creator: ValidatorId,
        pub deposit_asset: String,
        pub deposit_amount: BigNum,
        pub spec: Spec,
    }

    impl From<primitives::Channel> for Channel {
        fn from(channel: primitives::Channel) -> Self {
            Self {
                id: channel.id,
                creator: channel.creator,
                deposit_asset: channel.deposit_asset,
                deposit_amount: channel.deposit_amount,
                spec: channel.spec.into(),
            }
        }
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

    impl From<primitives::ChannelSpec> for Spec {
        fn from(channel_spec: primitives::ChannelSpec) -> Self {
            Self {
                withdraw_period_start: channel_spec.withdraw_period_start,
                active_from: channel_spec.active_from,
                created: channel_spec.created,
                validators: channel_spec.validators,
            }
        }
    }

    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
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
