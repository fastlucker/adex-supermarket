use crate::sentry_api::SentryApi;
use chrono::{DateTime, Duration, Utc};
use primitives::{
    sentry::{HeartbeatValidatorMessage, LastApprovedResponse, NewStateValidatorMessage},
    validator::{ApproveState, MessageTypes},
    BalancesMap, BigNum, Channel, ValidatorId,
};
use reqwest::Error;

// Re-export the Status & Finalized enums
pub use primitives::supermarket::{Status, Finalized};

#[cfg(test)]
#[path = "status_test.rs"]
pub mod test;

struct Messages {
    leader: LastApprovedResponse,
    follower: LastApprovedResponse,
    recency: Duration,
}

impl Messages {
    fn get_leader_new_state(&self) -> Option<&NewStateValidatorMessage> {
        self.leader
            .last_approved
            .as_ref()
            .and_then(|last_approved| last_approved.new_state.as_ref())
    }

    fn get_leader_new_state_balances(&self) -> BalancesMap {
        self.leader
            .last_approved
            .as_ref()
            .and_then(|last_approved| last_approved.new_state.as_ref())
            .and_then(|new_state| match &new_state.msg {
                MessageTypes::NewState(new_state) => Some(new_state.balances.clone()),
                _ => None,
            })
            .unwrap_or_default()
    }

    fn get_follower_approve_state_msg(&self) -> Option<&ApproveState> {
        self.follower
            .last_approved
            .as_ref()
            .and_then(|last_approved| last_approved.approve_state.as_ref())
            .and_then(|approve_state| match &approve_state.msg {
                MessageTypes::ApproveState(approve_state) => Some(approve_state),
                _ => None,
            })
    }

    fn has_leader_hb(&self) -> bool {
        self.leader
            .heartbeats
            .as_ref()
            .map(|heartbeats| !heartbeats.is_empty())
            .unwrap_or(false)
    }

    fn has_follower_hb(&self) -> bool {
        self.follower
            .heartbeats
            .as_ref()
            .map(|heartbeats| !heartbeats.is_empty())
            .unwrap_or(false)
    }

    fn has_follower_approve_state(&self) -> bool {
        self.follower
            .last_approved
            .as_ref()
            .map(|last_approved| last_approved.approve_state.is_some())
            .unwrap_or(false)
    }

    fn has_recent_follower_hb(&self) -> bool {
        self.follower
            .heartbeats
            .as_ref()
            .map(|heartbeats| self.has_recent_heartbeat_from(&heartbeats, None))
            .unwrap_or(false)
    }

    fn has_recent_leader_hb_from(&self, validator: &ValidatorId) -> bool {
        self.leader
            .heartbeats
            .as_ref()
            .map(|heartbeats| self.has_recent_heartbeat_from(heartbeats, Some(validator)))
            .unwrap_or(false)
    }

    fn has_recent_follower_hb_from(&self, validator: &ValidatorId) -> bool {
        self.follower
            .heartbeats
            .as_ref()
            .map(|heartbeats| self.has_recent_heartbeat_from(heartbeats, Some(validator)))
            .unwrap_or(false)
    }

    fn has_recent_leader_hb(&self) -> bool {
        self.leader
            .heartbeats
            .as_ref()
            .map(|heartbeats| self.has_recent_heartbeat_from(&heartbeats, None))
            .unwrap_or(false)
    }

    fn has_leader_new_state(&self) -> bool {
        self.get_leader_new_state().is_some()
    }

    /// `from`: If `None` it will just check for a recent Heartbeat
    fn has_recent_heartbeat_from(
        &self,
        heartbeats: &[HeartbeatValidatorMessage],
        from: Option<&ValidatorId>,
    ) -> bool {
        heartbeats
            .iter()
            .any(|heartbeat_msg| match (from, &heartbeat_msg.msg) {
                (Some(from), MessageTypes::Heartbeat(heartbeat))
                    if &heartbeat_msg.from == from
                        && is_date_recent(self.recency, &heartbeat.timestamp) =>
                {
                    true
                }
                (None, MessageTypes::Heartbeat(heartbeat))
                    if is_date_recent(self.recency, &heartbeat.timestamp) =>
                {
                    true
                }
                _ => false,
            })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum IsFinalized {
    Yes {
        reason: Finalized,
        balances: BalancesMap,
    },
    No {
        leader: Box<LastApprovedResponse>,
    },
}

/// # Finalized if:
/// - Is Channel expired?
/// - Is in withdraw period?
/// - Is Channel exhausted?
pub async fn is_finalized(sentry: &SentryApi, channel: &Channel) -> Result<IsFinalized, Error> {
    // Is Channel expired?
    if Utc::now() > channel.valid_until {
        let balances = fetch_balances(&sentry, &channel).await?;

        return Ok(IsFinalized::Yes {
            reason: Finalized::Expired,
            balances,
        });
    }

    // Is in withdraw period?
    if Utc::now() > channel.spec.withdraw_period_start {
        let balances = fetch_balances(&sentry, &channel).await?;

        return Ok(IsFinalized::Yes {
            reason: Finalized::Withdraw,
            balances,
        });
    }

    let leader = channel.spec.validators.leader();
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

    // Is channel exhausted?
    if total_balances >= channel.deposit_amount {
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

    Ok(IsFinalized::No {
        leader: Box::new(leader_la),
    })
}

pub async fn get_status(
    sentry: &SentryApi,
    channel: &Channel,
) -> Result<(Status, BalancesMap), Error> {
    // continue only if Campaign is not Finalized
    let leader_la = match is_finalized(sentry, channel).await? {
        IsFinalized::Yes { reason, balances } => return Ok((Status::Finalized(reason), balances)),
        IsFinalized::No { leader } => leader,
    };

    let follower = channel.spec.validators.follower();
    let follower_la = sentry.get_last_approved(&follower).await?;

    // setup the messages for the checks
    let messages = Messages {
        leader: *leader_la,
        follower: follower_la,
        recency: Duration::minutes(4),
    };

    // impl: isInitializing
    if is_initializing(&messages) {
        return Ok((
            Status::Initializing,
            messages.get_leader_new_state_balances(),
        ));
    }

    // impl: isOffline
    let offline = is_offline(&messages);

    // impl: isDisconnected
    let disconnected = is_disconnected(&channel, &messages);

    // impl: isInvalid
    let rejected_state = is_rejected_state(&channel, &messages, &sentry).await?;

    // impl: isUnhealthy
    let unhealthy = is_unhealthy(&messages);

    if disconnected || offline || rejected_state || unhealthy {
        let status = Status::Unsound {
            disconnected,
            offline,
            rejected_state,
            unhealthy,
        };
        return Ok((status, messages.get_leader_new_state_balances()));
    }

    // isActive & isReady (we don't need distinguish between Active & Ready here)
    if is_active(&messages) && is_ready(&messages) {
        Ok((Status::Active, messages.get_leader_new_state_balances()))
    } else {
        Ok((Status::Waiting, messages.get_leader_new_state_balances()))
    }
}

/// Calls SentryApi for the Leader's LastApproved NewState and returns the NewState Balance
async fn fetch_balances(sentry: &SentryApi, channel: &Channel) -> Result<BalancesMap, Error> {
    let leader_la = sentry
        .get_last_approved(&channel.spec.validators.leader())
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

/// there are no messages at all for at least one validator
fn is_initializing(messages: &Messages) -> bool {
    (!messages.has_leader_hb() && !messages.has_leader_new_state())
        || (!messages.has_follower_hb() && !messages.has_follower_approve_state())
}

/// at least one validator doesn't have a recent Heartbeat message
fn is_offline(messages: &Messages) -> bool {
    !messages.has_recent_leader_hb() || !messages.has_recent_follower_hb()
}

fn is_date_recent(recency: Duration, date: &DateTime<Utc>) -> bool {
    date >= &(Utc::now() - recency)
}

/// validators have recent Heartbeat messages, but they don't seem to be propagating messages between one another (the majority of Heartbeats are not found on both validators)
fn is_disconnected(channel: &Channel, messages: &Messages) -> bool {
    let leader = &channel.spec.validators.leader().id;
    let follower = &channel.spec.validators.follower().id;

    !(messages.has_recent_leader_hb_from(follower) && messages.has_recent_follower_hb_from(leader))
}

async fn is_rejected_state(
    channel: &Channel,
    messages: &Messages,
    sentry: &SentryApi,
) -> Result<bool, Error> {
    let leader_new_state = match (
        messages.has_follower_approve_state(),
        messages.get_leader_new_state(),
    ) {
        (false, Some(_)) => return Ok(true),
        (_, None) => return Ok(false),
        (_, Some(new_state)) => new_state,
    };

    let latest_new_state = sentry
        .get_latest_new_state(channel.spec.validators.leader())
        .await?;

    let latest_new_state = match latest_new_state {
        Some(new_state) => new_state,
        None => return Ok(false), // nothing to compare, shouldn't happen
    };

    let date_diff = leader_new_state.received - latest_new_state.received;
    let is_last_approved_old = date_diff < Duration::zero();
    let is_latest_new_state_a_minute_old =
        (Utc::now() - latest_new_state.received) > Duration::minutes(1);

    Ok(is_last_approved_old && is_latest_new_state_a_minute_old)
}

fn is_unhealthy(messages: &Messages) -> bool {
    let follower_approve_state = messages.get_follower_approve_state_msg();
    match (messages.has_leader_new_state(), follower_approve_state) {
        (true, Some(approve_state)) => !approve_state.is_healthy,
        _ => false,
    }
}

fn is_active(messages: &Messages) -> bool {
    let follower_approve_state = messages.get_follower_approve_state_msg();
    let is_healthy = match follower_approve_state {
        Some(approve_state) => approve_state.is_healthy,
        None => false,
    };

    messages.has_recent_leader_hb()
        && messages.has_recent_follower_hb()
        && messages.has_leader_new_state()
        && messages.has_follower_approve_state()
        && is_healthy
}

fn is_ready(messages: &Messages) -> bool {
    messages.has_recent_follower_hb()
        && messages.has_recent_leader_hb()
        && !messages.has_leader_new_state()
}
