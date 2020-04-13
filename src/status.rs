use crate::sentry_api::SentryApi;
use chrono::{DateTime, Duration, Utc};
use primitives::{
    sentry::{HeartbeatValidatorMessage, LastApproved, LastApprovedResponse},
    validator::{Heartbeat, MessageTypes},
    BalancesMap, BigNum, Channel,
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
    leader_last_approved: Option<LastApproved>,
    follower_last_approved: Option<LastApproved>,
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

    Ok(IsFinalized::No { leader: Box::new(leader_la) })
}

pub async fn get_status(sentry: &SentryApi, channel: &Channel) -> Result<Status, Error> {
    // continue only if Campaign is not Finalized
    let leader_la = match is_finalized(sentry, channel).await? {
        IsFinalized::Yes { reason, balances } => return Ok(Status::Finalized(reason, balances)),
        IsFinalized::No { leader } => leader,
    };

    let follower = channel.spec.validators.follower();
    let follower_la = sentry.get_last_approved(&follower).await?;

    // setup the messages for the checks
    let messages = Messages {
        leader_last_approved: leader_la.last_approved,
        follower_last_approved: follower_la.last_approved,
        leader_heartbeats: get_heartbeats(leader_la.heartbeats),
        follower_heartbeats: get_heartbeats(follower_la.heartbeats),
    };

    // impl: isInitializing
    if is_initializing(&messages.leader_heartbeats, &messages.follower_heartbeats, &messages.leader_last_approved, &messages.follower_last_approved) {
        return Ok(Status::Initializing);
    }

    // impl: isOffline
    let offline = is_offline(&messages.leader_heartbeats, &messages.follower_heartbeats);

    // impl: isDisconnected
    let disconnected = is_disconnected(&messages.follower_heartbeats, &leader);

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

fn is_hb_by_validator(validator: &ValidatorDesc, heartbeat: &HeartbeatValidatorMessage) -> bool {
    heartbeat.from == validator.id
}

fn get_new_state(last_approved: &Option<LastApproved>) -> Vec<NewState> {
    match last_approved {
        Some(last_approved) => last_approved.new_state
            .into_iter()
            .filter_map(|new_state| match new_state.msg {
                MessageTypes::NewState(new_state) => Some(new_state),
                _ => None,
            })
            .collect(),
        None => Default::default(),
    }
}

fn get_approve_state(last_approved: &Option<LastApproved>) -> Vec<ApproveState> {
    match last_approved {
        Some(last_approved) => last_approved.approve_state
            .into_iter()
            .filter_map(|approve_state| match approve_state.msg {
                MessageTypes::ApproveState(approve_state) => Some(approve_state),
                _ => None,
            })
            .collect(),
        None => Default::default(),
    }
}

fn get_hb_by_validator(validator: &ValidatorDesc, heartbeats: &[Heartbeat]) -> Vec<Heartbeat> {
    heartbeats
        .into_iter()
        .filter_map(|h| {
            if is_hb_by_validator(&validator, &h) {
                match h.msg {
                    MessageTypes::Heartbeat(h) => Some(h),
                    _ => None,
                }
            } else {
                None
            }
        })
        .collect()
}

// there are no messages at all for at least one validator
fn is_initializing(leader: &[Heartbeat], follower: &[Heartbeat], leader_la: &Option<LastApproved>, follower_la: &Option<LastApproved>) -> bool {
    (leader.is_empty() && get_new_state(leader_la).is_empty()) || (follower.is_empty() && get_approve_state(follower_la).is_empty())
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
    date >= &(Utc::now() - *recency)
}

// validators have recent Heartbeat messages, but they don't seem to be propagating messages between one another (the majority of Heartbeats are not found on both validators)
fn is_disconnected(follower_hb: &[Heartbeat], leader: &ValidatorDesc) -> bool {
    let recency = Duration::minutes(4);

    let recent_follower_hb = follower_hb
        .into_iter()
        .filter_map(|h| {
            if is_date_recent(&recency, &h.timestamp) {
                Some(h)
            } else {
                None
            }
        })
        .count();

    let recent_follower_hb_from_leader = get_hb_by_validator(&leader, follower_hb)
        .into_iter()
        .filter_map(|h| {
            if is_date_recent(&recency, &h.timestamp) {
                Some(h)
            } else {
                None
            }
        })
        .count();

        recent_follower_hb > 0 && recent_follower_hb_from_leader > 0
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
    use httptest::{mappers::*, responders::*, Expectation, Server, ServerPool};
    use primitives::util::tests::prep_db::{
        DUMMY_CHANNEL, DUMMY_VALIDATOR_FOLLOWER, DUMMY_VALIDATOR_LEADER,
    };

    static SERVER_POOL: ServerPool = ServerPool::new(4);

    fn get_test_channel(server: &Server) -> Channel {
        let mut channel = DUMMY_CHANNEL.clone();
        let mut leader = DUMMY_VALIDATOR_LEADER.clone();
        leader.url = server.url_str("/leader");

        let mut follower = DUMMY_VALIDATOR_FOLLOWER.clone();
        follower.url = server.url_str("/follower");

        channel.spec.validators = (leader, follower).into();

        channel
    }

    #[tokio::test]
    async fn test_is_finalized_when_expired() {
        let server = SERVER_POOL.get_server();
        let mut channel = get_test_channel(&server);
        channel.valid_until = Utc::now() - Duration::seconds(5);

        let response = LastApprovedResponse {
            last_approved: None,
            heartbeats: None,
        };

        server.expect(Expectation::matching(any()).respond_with(json_encoded(response)));

        let sentry = SentryApi::new().expect("Should work");

        let actual = is_finalized(&sentry, &channel)
            .await
            .expect("Should query dummy server");
        let expected = IsFinalized::Yes {
            reason: Finalized::Expired,
            balances: Default::default(),
        };

        assert_eq!(expected, actual);
    }

    #[tokio::test]
    async fn test_is_finalized_in_withdraw_period() {
        let server = SERVER_POOL.get_server();
        let mut channel = get_test_channel(&server);
        channel.spec.withdraw_period_start = Utc::now() - Duration::seconds(5);

        let response = LastApprovedResponse {
            last_approved: None,
            heartbeats: None,
        };

        server.expect(Expectation::matching(any()).respond_with(json_encoded(response)));

        let sentry = SentryApi::new().expect("Should work");

        let actual = is_finalized(&sentry, &channel)
            .await
            .expect("Should query dummy server");
        let expected = IsFinalized::Yes {
            reason: Finalized::Withdraw,
            balances: Default::default(),
        };

        assert_eq!(expected, actual);
    }

    #[test]
    fn is_offline_no_heartbeats() {
        assert!(
            is_offline(&[], &[]),
            "On empty heartbeast it should be offline!"
        )
    }

    // is_date_recent()
    #[test]
    fn now_date_is_recent() {
        let now = Utc::now();
        let recency = Duration::minutes(4);
        assert!(
            is_date_recent(&recency, &now),
            "The present moment is a recent date!"
        )
    }

    #[test]
    fn slightly_past_is_recent() {
        let recency = Duration::minutes(4);
        let on_the_edge = Utc::now().checked_sub_signed(Duration::minutes(3)).unwrap();
        assert!(
            is_date_recent(&recency, &on_the_edge),
            "When date is just as old as the recency limit, it still counts as recent"
        )
    }

    #[test]
    fn old_date_is_not_recent() {
        let recency = Duration::minutes(4);
        let past = Utc::now() - Duration::minutes(10);
        assert_eq!(
            is_date_recent(&recency, &past),
            false,
            "Date older than the recency limit is not recent"
        )
    }

    // is_initializing()
    #[test]
    fn two_empty_message_arrays() {
        // let leader_hbs: [Heartbeat; 0] = [];
        // let follower_hbs: [Heartbeat; 0] = [];
        // let new_state_msgs: [NewState; 0] = [];
        // let approve_state_msgs: [ApproveState; 0] = [];

        // assert_eq!(
        //     is_initializing(&leader_hbs, &follower_hbs, &new_state_msgs, &approve_state_msgs),
        //     true,
        //     "Both leader heartbeats + newstate and follower heatbeats + approvestate pairs are empty arrays"
        // )
    }

    #[test]
    fn first_message_arr_is_empty() {
        todo!()
    }

    #[test]
    fn second_message_arr_is_empty() {
        todo!()
    }

    #[test]
    fn both_arrays_have_messages() {
        todo!()
    }

    // is_disconnected()
    #[test]
    fn no_recent_hbs_on_both_sides() {
        todo!()
    }

    #[test]
    fn no_recent_follower_hbs() {
        todo!()
    }

    #[test]
    fn no_recent_leader_hb_on_follower_validator() {
        todo!()
    }

    #[test]
    fn recent_hbs_on_both_arrays() {
        todo!()
    }

    // @TODO: test is_offline()
}
