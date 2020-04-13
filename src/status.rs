use crate::sentry_api::SentryApi;
use chrono::{DateTime, Duration, Utc};
use primitives::{
    market::Campaign,
    sentry::{HeartbeatValidatorMessage, LastApproved, LastApprovedResponse},
    validator::{Heartbeat, MessageTypes},
    BalancesMap, BigNum,
};
use reqwest::Error;

#[derive(Debug)]
pub enum Status {
    // Active and Ready
    Active,
    Pending,
    Initializing,
    Waiting,
    Finalized(Finalized, BalancesMap),
    Unsound {
        disconnected: bool,
        offline: bool,
        rejected_state: bool,
        unhealthy: bool,
    },
}

#[derive(Debug, PartialEq, Eq)]
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

#[derive(Debug, PartialEq, Eq)]
pub enum IsFinalized {
    Yes {
        reason: Finalized,
        balances: BalancesMap,
    },
    No {
        leader: LastApprovedResponse,
    },
}

/// # Finalized if:
/// - Is campaign expired?
/// - Is in withdraw period?
/// - Is campaign exhausted?
pub async fn is_finalized(sentry: &SentryApi, campaign: &Campaign) -> Result<IsFinalized, Error> {
    // Is campaign expired?
    if Utc::now() > campaign.channel.valid_until {
        let balances = fetch_balances(&sentry, &campaign).await?;

        return Ok(IsFinalized::Yes {
            reason: Finalized::Expired,
            balances,
        });
    }

    // Is in withdraw period?
    if Utc::now() > campaign.channel.spec.withdraw_period_start {
        let balances = fetch_balances(&sentry, &campaign).await?;

        return Ok(IsFinalized::Yes {
            reason: Finalized::Withdraw,
            balances,
        });
    }

    let leader = campaign.channel.spec.validators.leader();
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
        // get balances from the Leader response
        let balances = leader_la
            .last_approved
            .and_then(|last_approved| last_approved.new_state)
            .and_then(|new_state| match new_state.msg {
                MessageTypes::NewState(new_state) => Some(new_state.balances),
                _ => None,
            })
            .unwrap_or_default();

        return Ok(IsFinalized::Yes {
            reason: Finalized::Exhausted,
            balances,
        });
    }

    Ok(IsFinalized::No { leader: leader_la })
}

pub async fn get_status(sentry: &SentryApi, campaign: &Campaign) -> Result<Status, Error> {
    // continue only if Campaign is not Finalized
    let leader_la = match is_finalized(sentry, campaign).await? {
        IsFinalized::Yes { reason, balances } => return Ok(Status::Finalized(reason, balances)),
        IsFinalized::No { leader } => leader,
    };

    let follower = campaign.channel.spec.validators.follower();
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
    let offline = is_offline(&messages.leader_heartbeats, &messages.follower_heartbeats);

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

    // isActive & isReady (we don't need distinguish between Active & Ready here)
    if is_active() && is_ready() {
        Ok(Status::Active)
    } else {
        Ok(Status::Waiting)
    }
}

/// Calls SentryApi for the Leader's LastApproved NewState and returns the NewState Balance
async fn fetch_balances(sentry: &SentryApi, campaign: &Campaign) -> Result<BalancesMap, Error> {
    let leader_la = sentry
        .get_last_approved(&campaign.channel.spec.validators.leader())
        .await?;

    let balances = leader_la
        .last_approved
        .and_then(|last_approved| last_approved.new_state)
        .and_then(|new_state| match new_state.msg {
            MessageTypes::NewState(new_state) => Some(new_state.balances),
            _ => None,
        })
        .unwrap_or_default();

    Ok(balances)
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

// at least one validator doesn't have a recent Heartbeat message
fn is_offline(leader: &[Heartbeat], follower: &[Heartbeat]) -> bool {
    // @TODO: Move to configuration
    let recency = Duration::minutes(4);

    !leader
        .iter()
        .any(|h| is_date_recent(&recency, &h.timestamp))
        || !follower
            .iter()
            .any(|h| is_date_recent(&recency, &h.timestamp))
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn is_offline_no_heartbeats() {
        assert!(
            is_offline(&[], &[]),
            "On empty heartbeast it should be offline!"
        )
    }

    // @TODO: test is_offline()
}
