use chrono::Duration;
use primitives::sentry::LastApproved;
use primitives::{
    market::Campaign,
    sentry::{LastApprovedResponse, HeartbeatValidatorMessage},
    validator::{Heartbeat, MessageTypes},
    BigNum,
};

use chrono::{DateTime, Utc};
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

struct Messages {
    last_approved: Option<LastApproved>,
    leader_heartbeats: Vec<Heartbeat>,
    follower_heartbeats: Vec<Heartbeat>,
}

pub async fn get_status(
    sentry: &SentryApi,
    campaign: &Campaign,
) -> Result<Status, Box<dyn std::error::Error>> {
    // Is campaign expired?
    if Utc::now() > campaign.channel.valid_until {
        return Ok(Status::Finalized(Finalized::Expired));
    }

    // impl: Is in withdraw period?
    if Utc::now() > campaign.channel.spec.withdraw_period_start {
        return Ok(Status::Finalized(Finalized::Withdraw));
    }

    let leader = campaign.channel.spec.validators.leader();
    let follower = campaign.channel.spec.validators.follower();

    let leader_la = sentry.get_last_approved(&leader).await?;

    let total_balances: BigNum = leader_la
        .last_approved
        .as_ref()
        .and_then(|last_approved| last_approved.new_state.as_ref())
        .and_then(|new_state| match &new_state.msg {
            MessageTypes::NewState(new_state) => {
                let total = new_state.balances.values().sum();
                Some(total)
            }
            _ => None,
        })
        .unwrap_or_default();

    // Is campaign exhausted?
    if total_balances >= campaign.channel.deposit_amount {
        return Ok(Status::Finalized(Finalized::Exhausted));
    }

    // don't fetch follower, if campaign is exhausted
    let follower_la = sentry.get_last_approved(&follower).await?;

    // setup the messages for the checks
    let messages = Messages {
        last_approved: leader_la.last_approved,
        leader_heartbeats: get_heartbeats(leader_la.heartbeats),
        follower_heartbeats: get_heartbeats(follower_la.heartbeats),
    };

    // impl: isInitializing
    if is_initializing() {
        return Ok(Status::Initializing);
    }

    // impl: isOffline
    let offline = is_offline(&messages);

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

fn get_heartbeats(heartbeats: Option<Vec<HeartbeatValidatorMessage>>) -> Vec<Heartbeat> {
    match heartbeats {
        Some(heartbeats) => heartbeats
            .into_iter()
            .filter_map(|heartbeat| match heartbeat.msg {
                MessageTypes::Heartbeat(heartbeat) => Some(heartbeat),
                _ => None,
            })
            .collect(),
        None => Default::default(),
    }
}

fn is_initializing() -> bool {
    todo!()
}

fn is_offline(messages: &Messages) -> bool {
    let recency = Duration::minutes(4);
    messages.leader_heartbeats.iter().filter(|h| is_date_recent(&recency, &h.timestamp)).count() == 0 ||
    messages.follower_heartbeats.iter().filter(|h| is_date_recent(&recency, &h.timestamp)).count() == 0
}

fn is_date_recent(recency: &Duration, date: &DateTime<Utc>) -> bool {
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
