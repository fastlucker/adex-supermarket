use primitives::{market::Campaign, BigNum};

use chrono::Utc;
use sentry_api::SentryApi;

#[derive(Debug)]
pub enum Status {
    // Active and Ready
    Active,
    Pending,
    Initializing,
    Waiting,
    Finalized(Finalized),
    Unsound {
        disconnected: bool,
        offline: bool,
        rejected_state: bool,
        unhealthy: bool,
    },
}

#[derive(Debug)]
pub enum Finalized {
    Expired,
    Exhausted,
    Withdraw,
}

pub async fn get_status(
    sentry: &SentryApi,
    campaign: &Campaign,
) -> Result<Status, Box<dyn std::error::Error>> {
    use primitives::validator::MessageTypes;

    // Is campaign expired?
    if Utc::now() > campaign.channel.valid_until {
        return Ok(Status::Finalized(Finalized::Expired));
    }

    let leader = campaign.channel.spec.validators.leader();
    let follower = campaign.channel.spec.validators.follower();

    let leader_la = sentry.get_last_approved(&leader).await?;
    let _follower_la = sentry.get_last_approved(&follower).await?;

    let last_approved_balances = leader_la
        .last_approved
        .and_then(|last_approved| last_approved.new_state)
        .and_then(|new_state| match new_state.msg {
            MessageTypes::NewState(new_state) => Some(new_state.balances),
            _ => None,
        })
        .unwrap_or_default();

    // TODO: Get from last approved
    let total_balances: BigNum = last_approved_balances.values().sum();

    // Is campaign exhausted?
    if total_balances >= campaign.channel.deposit_amount {
        return Ok(Status::Finalized(Finalized::Exhausted));
    }

    // impl: Is in withdraw period?
    if Utc::now() > campaign.channel.spec.withdraw_period_start {
        return Ok(Status::Finalized(Finalized::Withdraw));
    }

    // impl: isInitializing
    if is_initializing() {
        return Ok(Status::Initializing);
    }

    // impl: isOffline
    let offline = is_offline();

    // impl: isDisconnected
    let disconnected = is_disconnected();

    // impl: isInvalid
    let rejected_state = is_rejected_state();

    // impl: isUnhealthy
    let unhealthy = is_unhealthy();

    if disconnected || offline || rejected_state || unhealthy {
        return Ok(Status::Unsound {
            disconnected,
            offline,
            rejected_state,
            unhealthy,
        });
    }

    // isAtive & isReady (including both Ready & Waiting)
    if is_active() && is_ready() {
        Ok(Status::Active)
    } else {
        Ok(Status::Waiting)
    }
}

fn is_initializing() -> bool {
    todo!()
}

fn is_offline() -> bool {
    todo!()
}

fn is_disconnected() -> bool {
    todo!()
}

fn is_rejected_state() -> bool {
    todo!()
}

fn is_unhealthy() -> bool {
    todo!()
}

fn is_active() -> bool {
    todo!()
}

fn is_ready() -> bool {
    todo!()
}

pub mod sentry_api {

    use primitives::{sentry::LastApprovedResponse, ValidatorDesc};
    use reqwest::{Client, Error};

    pub struct SentryApi {
        client: Client,
    }

    impl SentryApi {
        pub fn new() -> Result<Self, Error> {
            let client = Client::builder().build()?;

            Ok(Self { client })
        }

        pub async fn get_last_approved(
            &self,
            validator: &ValidatorDesc,
        ) -> Result<LastApprovedResponse, Error> {
            let url = format!("{}/last-approved?withHeartbeat=true", validator.url);
            let response = self.client.get(&url).send().await?;

            response.json().await
        }
    }
}
