use crate::{not_found, Error, MarketApi, ROUTE_UNITS_FOR_SLOT};
use chrono::Utc;
use hyper::{header::USER_AGENT, Body, Request, Response};
use primitives::{
    targeting::{AdSlot, Global, Input},
    util::tests::prep_db::DUMMY_CHANNEL,
};
use slog::{info, Logger};
use std::sync::Arc;
use url::Url;
use woothee::parser::Parser;

pub async fn get_units_for_slot(
    logger: &Logger,
    market: Arc<MarketApi>,
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

        // @TODO: https://github.com/AdExNetwork/adex-supermarket/issues/9

        // For each adUnits apply input
        let parsed = Parser::new().parse(req.headers().get(USER_AGENT).unwrap().to_str().unwrap());
        let user_agent_os = Some(
            parsed
                .as_ref()
                .map(|p| p.os.to_string())
                .unwrap_or_default(),
        );
        let user_agent_browser_family = Some(
            parsed
                .as_ref()
                .map(|p| p.browser_type.to_string())
                .unwrap_or_default(),
        );
        let country = Some(String::from(
            req.headers().get("cf-ipcountry").unwrap().to_str().unwrap(),
        ));

        let hostname = Url::parse(&ad_slot_response.slot.website.unwrap())?
            .host()
            .unwrap()
            .to_string();
        // let input = Input {
        //     ad_view: None,
        //     global: Global {
        //         ad_slot_id: ad_slot_response.slot.id,
        //         ad_slot_type: ad_slot_response.slot.ad_type,
        //         publisher_id: ad_slot_response.slot.owner,
        //         country: req.headers().get("cf-ipcountry"),
        //         event_type: "IMPRESSION".to_string(),
        //         seconds_since_epoch: Utc::now().timestamp(),
        //         user_agent_os,
        //         user_agent_browser_family,
        //         ad_unit: Some(ad_unit),
        //         channel: DUMMY_CHANNEL.clone(),
        //         status: Some(Status::Initializing),
        //         balances: Some(input_balances),
        //     },
        //     ad_slot: None,
        // };
        let targeting_input_base = Input {
            ad_view: None,
            global: Global {
                ad_slot_id: ad_slot_response.slot.ipfs,
                ad_slot_type: ad_slot_response.slot.ad_type,
                publisher_id: ad_slot_response.slot.owner,
                country,
                event_type: "IMPRESSION".to_string(),
                seconds_since_epoch: Utc::now().timestamp() as u64,
                user_agent_os,
                user_agent_browser_family,
                ad_unit: Default::default(),
                balances: Default::default(),
                channel: DUMMY_CHANNEL.clone(),
                status: Default::default(),
            },
            ad_slot: Some(AdSlot {
                categories: ad_slot_response.categories,
                hostname,
                alexa_rank: ad_slot_response.alexa_rank,
            }),
        };

        Ok(Response::new(Body::from("")))
    }
}
