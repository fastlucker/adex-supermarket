use crate::{not_found, Error, MarketApi, ROUTE_UNITS_FOR_SLOT};
use chrono::Utc;
use hyper::{Body, Request, Response, header::USER_AGENT};
use primitives::targeting::{Global, Input};
use slog::{error, info, Logger};
use std::sync::Arc;
use woothee::parse::Parser;

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
        let parsed = Parser::new().parse(req.headers().get(USER_AGENT).unwrap_or_default());
        let user_agent_os = parsed.as_ref().map(|p| p.os.to_string()).unwrap_or_default();
        let user_agent_browser_family = parsed.as_ref().map(|p| p.browser_type.to_string()).unwrap_or_default();

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
        // const ua = UAParser(req.headers['user-agent'])
        // const targetingInputBase = {
        //     adSlotId: id,
        //     adSlotType: adSlot.type,
        //     publisherId,
        //     country: req.headers['cf-ipcountry'],
        //     eventType: 'IMPRESSION',
        //     secondsSinceEpoch: Math.floor(Date.now() / 1000),
        //     userAgentOS: ua.os.name,
        //     userAgentBrowserFamily: ua.browser.name,
        //     'adSlot.categories': categories,
        //     'adSlot.hostname': adSlot.website
        //         ? url.parse(adSlot.website).hostname
        //         : undefined,
        //     'adSlot.alexaRank': typeof alexaRank === 'number' ? alexaRank : undefined,
        // }

        Ok(Response::new(Body::from("")))
    }
}
