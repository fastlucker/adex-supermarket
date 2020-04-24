use crate::sentry_api::SentryApi;
use chrono::{DateTime, Duration, Utc};
use primitives::{
    market::Status as MarketStatus,
    sentry::{HeartbeatValidatorMessage, LastApproved, LastApprovedResponse},
    validator::{Heartbeat, MessageTypes},
    BalancesMap, BigNum, Channel, ValidatorId,
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

impl From<&MarketStatus> for Status {
    fn from(market_status: &MarketStatus) -> Self {
        use primitives::market::StatusType::*;
        match market_status.status_type {
            Active | Ready => Self::Active,
            Pending => Self::Pending,
            Initializing => Self::Initializing,
            Waiting => Self::Waiting,
            Offline => Self::Unsound {
                disconnected: false,
                offline: true,
                rejected_state: false,
                unhealthy: false,
            },
            Disconnected => Self::Unsound {
                disconnected: true,
                offline: false,
                rejected_state: false,
                unhealthy: false,
            },
            Unhealthy => Self::Unsound {
                disconnected: false,
                offline: false,
                rejected_state: false,
                unhealthy: true,
            },
            Invalid => Self::Unsound {
                disconnected: false,
                offline: false,
                rejected_state: true,
                unhealthy: false,
            },
            Expired => Self::Finalized(Finalized::Expired, market_status.balances.clone()),
            Exhausted => Self::Finalized(Finalized::Exhausted, market_status.balances.clone()),
            Withdraw => Self::Finalized(Finalized::Withdraw, market_status.balances.clone()),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Finalized {
    Expired,
    Exhausted,
    Withdraw,
}

struct Messages {
    leader: LastApprovedResponse,
    follower: LastApprovedResponse,
    recency: Duration,
}

impl Messages {
    fn has_leader_hb(&self) -> bool {
        self.leader
            .heartbeats
            .as_ref()
            .map(|heartbeats| heartbeats.len() > 0)
            .unwrap_or(false)
    }

    fn has_follower_hb(&self) -> bool {
        self.follower
            .heartbeats
            .as_ref()
            .map(|heartbeats| heartbeats.len() > 0)
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
        self.leader
            .last_approved
            .as_ref()
            .map(|last_approved| last_approved.new_state.is_some())
            .unwrap_or(false)
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
        leader: *leader_la,
        follower: follower_la,
        recency: Duration::minutes(4),
    };

    // impl: isInitializing
    if is_initializing(&messages) {
        return Ok(Status::Initializing);
    }

    // impl: isOffline
    let offline = is_offline(&messages);

    // impl: isDisconnected
    let disconnected = is_disconnected(&channel, &messages);

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
    use primitives::{
        util::tests::prep_db::{
            DUMMY_CHANNEL, DUMMY_VALIDATOR_FOLLOWER, DUMMY_VALIDATOR_LEADER,
        },
        sentry::{ApproveStateValidatorMessage, NewStateValidatorMessage, LastApproved},
        validator::{ApproveState, NewState, Heartbeat}
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

    fn get_approve_state_msg() -> ApproveStateValidatorMessage {
        ApproveStateValidatorMessage {
            from: DUMMY_VALIDATOR_LEADER.id,
            received: Utc::now(),
            msg: MessageTypes::ApproveState(ApproveState {
                state_root: String::from("0x0"),
                signature: String::from("0x0"),
                is_healthy: true,
            }),
        }
    }

    fn get_heartbeat_msg(recency: Duration, validator_id: ValidatorId) -> HeartbeatValidatorMessage {
        HeartbeatValidatorMessage {
            from: validator_id,
            received: Utc::now() - recency,
            msg: MessageTypes::Heartbeat(Heartbeat {
                signature: String::from("0x0"),
                state_root: String::from("0x0"),
                timestamp: Utc::now() - recency,
            }),
        }
    }

    fn get_new_state_msg() -> NewStateValidatorMessage {
        NewStateValidatorMessage {
            from: DUMMY_VALIDATOR_LEADER.id,
            received: Utc::now(),
            msg: MessageTypes::NewState(NewState {
                signature: String::from("0x0"),
                state_root: String::from("0x0"),
                balances: Default::default(),
            }),
        }
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
        let messages = Messages {
            leader: LastApprovedResponse {
                last_approved: None,
                heartbeats: Some(vec![]),
            },
            follower: LastApprovedResponse {
                last_approved: None,
                heartbeats: Some(vec![]),
            },
            recency: Duration::minutes(4),
        };

        assert!(
            is_offline(&messages),
            "On empty heartbeat it should be offline!"
        );

        let messages = Messages {
            leader: LastApprovedResponse {
                last_approved: None,
                heartbeats: None,
            },
            follower: LastApprovedResponse {
                last_approved: None,
                heartbeats: Some(vec![]),
            },
            recency: Duration::minutes(4),
        };

        assert!(
            is_offline(&messages),
            "On empty heartbeat it should be offline!"
        );
    }

    // is_date_recent()
    #[test]
    fn now_date_is_recent() {
        let now = Utc::now();
        let recency = Duration::minutes(4);
        assert!(
            is_date_recent(recency, &now),
            "The present moment is a recent date!"
        )
    }

    #[test]
    fn slightly_past_is_recent() {
        let recency = Duration::minutes(4);
        let on_the_edge = Utc::now().checked_sub_signed(Duration::minutes(3)).unwrap();
        assert!(
            is_date_recent(recency, &on_the_edge),
            "When date is just as old as the recency limit, it still counts as recent"
        )
    }

    #[test]
    fn old_date_is_not_recent() {
        let recency = Duration::minutes(4);
        let past = Utc::now() - Duration::minutes(10);
        assert_eq!(
            is_date_recent(recency, &past),
            false,
            "Date older than the recency limit is not recent"
        )
    }

    // is_initializing()
    #[test]
    fn two_empty_message_arrays() {
        let messages = Messages {
            leader: LastApprovedResponse {
                last_approved: None,
                heartbeats: Some(vec![]),
            },
            follower: LastApprovedResponse {
                last_approved: None,
                heartbeats: Some(vec![]),
            },
            recency: Duration::minutes(4),
        };

        assert_eq!(
            is_initializing(&messages),
            true,
            "Both leader heartbeats + newstate and follower heatbeats + approvestate pairs are empty arrays"
        )
    }

    #[test]
    fn leader_has_no_messages() {
        let channel = DUMMY_CHANNEL.clone();
        let approve_state = get_approve_state_msg();
        let heartbeat = get_heartbeat_msg(Duration::minutes(0), channel.spec.validators.leader().id);
        let messages = Messages {
            leader: LastApprovedResponse {
                last_approved: None,
                heartbeats: Some(vec![]),
            },
            follower: LastApprovedResponse {
                last_approved: Some(LastApproved {
                    new_state: None,
                    approve_state: Some(approve_state),
                }),
                heartbeats: Some(vec![heartbeat]),
            },
            recency: Duration::minutes(4),
        };

        assert_eq!(
            is_initializing(&messages),
            true,
            "Leader has no new messages but the follower has heartbeats and approvestate"
        )
    }

    #[test]
    fn follower_has_no_messages() {
        let channel = DUMMY_CHANNEL.clone();
        let heartbeat = get_heartbeat_msg(Duration::minutes(0), channel.spec.validators.leader().id);
        let new_state = get_new_state_msg();

        let messages = Messages {
            leader: LastApprovedResponse {
                last_approved: Some(LastApproved {
                    new_state: Some(new_state),
                    approve_state: None,
                }),
                heartbeats: Some(vec![heartbeat]),
            },
            follower: LastApprovedResponse {
                last_approved: None,
                heartbeats: Some(vec![]),
            },
            recency: Duration::minutes(4),
        };

        assert_eq!(
            is_initializing(&messages),
            true,
            "Follower has no new messages but leader has heartbeats and newstate"
        )
    }

    #[test]
    fn both_arrays_have_messages() {
        let channel = DUMMY_CHANNEL.clone();
        let leader_heartbeats = vec![get_heartbeat_msg(Duration::minutes(0), channel.spec.validators.leader().id)];
        let follower_heartbeats = vec![get_heartbeat_msg(Duration::minutes(0), channel.spec.validators.leader().id)];
        let new_state = get_new_state_msg();
        let approve_state = get_approve_state_msg();

        let messages = Messages {
            leader: LastApprovedResponse {
                last_approved: Some(LastApproved {
                    new_state: Some(new_state),
                    approve_state: None,
                }),
                heartbeats: Some(leader_heartbeats),
            },
            follower: LastApprovedResponse {
                last_approved: Some(LastApproved {
                    new_state: None,
                    approve_state: Some(approve_state),
                }),
                heartbeats: Some(follower_heartbeats),
            },
            recency: Duration::minutes(4),
        };

        assert_eq!(
            is_initializing(&messages),
            false,
            "Both arrays have messages"
        )
    }

    // is_disconnected()
    #[test]
    fn no_recent_hbs_on_both_sides() {
        let channel = DUMMY_CHANNEL.clone();
        let leader_heartbeats = vec![get_heartbeat_msg(Duration::minutes(10), channel.spec.validators.leader().id)];
        let follower_heartbeats = vec![get_heartbeat_msg(Duration::minutes(10), channel.spec.validators.leader().id)];

        let messages = Messages {
            leader: LastApprovedResponse {
                last_approved: Some(LastApproved {
                    new_state: None,
                    approve_state: None,
                }),
                heartbeats: Some(leader_heartbeats),
            },
            follower: LastApprovedResponse {
                last_approved: Some(LastApproved {
                    new_state: None,
                    approve_state: None,
                }),
                heartbeats: Some(follower_heartbeats),
            },
            recency: Duration::minutes(4),
        };

        assert_eq!(
            is_disconnected(&channel, &messages),
            true,
            "Both leader and follower heartbeats have no recent messages"
        )
    }

    #[test]
    fn no_recent_follower_hbs() {
        let channel = DUMMY_CHANNEL.clone();
        let leader_heartbeats = vec![get_heartbeat_msg(Duration::minutes(1), channel.spec.validators.leader().id), get_heartbeat_msg(Duration::minutes(1), channel.spec.validators.follower().id)];
        let follower_heartbeats = vec![get_heartbeat_msg(Duration::minutes(10), channel.spec.validators.leader().id), get_heartbeat_msg(Duration::minutes(10), channel.spec.validators.follower().id)];
        let new_state = get_new_state_msg();
        let approve_state = get_approve_state_msg();

        let messages = Messages {
            leader: LastApprovedResponse {
                last_approved: Some(LastApproved {
                    new_state: Some(new_state),
                    approve_state: None,
                }),
                heartbeats: Some(leader_heartbeats),
            },
            follower: LastApprovedResponse {
                last_approved: Some(LastApproved {
                    new_state: None,
                    approve_state: Some(approve_state),
                }),
                heartbeats: Some(follower_heartbeats),
            },
            recency: Duration::minutes(4),
        };

        assert_eq!(
            is_disconnected(&channel, &messages),
            true,
            "No recent heartbeats on both validators"
        )
    }

    #[test]
    fn no_hb_in_leader_where_from_points_to_follower() {
        let channel = DUMMY_CHANNEL.clone();
        let leader_heartbeats = vec![get_heartbeat_msg(Duration::minutes(0), channel.spec.validators.leader().id)];
        let follower_heartbeats = vec![get_heartbeat_msg(Duration::minutes(0), channel.spec.validators.leader().id), get_heartbeat_msg(Duration::minutes(0), channel.spec.validators.follower().id)];
        let new_state = get_new_state_msg();
        let approve_state = get_approve_state_msg();

        let messages = Messages {
            leader: LastApprovedResponse {
                last_approved: Some(LastApproved {
                    new_state: Some(new_state),
                    approve_state: None,
                }),
                heartbeats: Some(leader_heartbeats),
            },
            follower: LastApprovedResponse {
                last_approved: Some(LastApproved {
                    new_state: None,
                    approve_state: Some(approve_state),
                }),
                heartbeats: Some(follower_heartbeats),
            },
            recency: Duration::minutes(4),
        };

        assert_eq!(
            is_disconnected(&channel, &messages),
            true,
            "Leader validator heartbeats have no recent messages that came from the follower"
        )
    }

    #[test]
    fn no_hb_in_follower_where_from_points_to_leader() {
        let channel = DUMMY_CHANNEL.clone();
        let leader_heartbeats = vec![get_heartbeat_msg(Duration::minutes(0), channel.spec.validators.leader().id), get_heartbeat_msg(Duration::minutes(0), channel.spec.validators.leader().id)];
        let follower_heartbeats = vec![get_heartbeat_msg(Duration::minutes(0), channel.spec.validators.follower().id)];

        let new_state = get_new_state_msg();
        let approve_state = get_approve_state_msg();

        let messages = Messages {
            leader: LastApprovedResponse {
                last_approved: Some(LastApproved {
                    new_state: Some(new_state),
                    approve_state: None,
                }),
                heartbeats: Some(leader_heartbeats),
            },
            follower: LastApprovedResponse {
                last_approved: Some(LastApproved {
                    new_state: None,
                    approve_state: Some(approve_state),
                }),
                heartbeats: Some(follower_heartbeats),
            },
            recency: Duration::minutes(4),
        };

        assert_eq!(
            is_disconnected(&channel, &messages),
            true,
            "Follower validator heartbeats have no recent messages that came from the leader"
        )
    }

    #[test]
    fn recent_hbs_coming_from_both_validators() {
        let channel = DUMMY_CHANNEL.clone();
        let leader_heartbeats = vec![get_heartbeat_msg(Duration::minutes(0), channel.spec.validators.leader().id), get_heartbeat_msg(Duration::minutes(0), channel.spec.validators.follower().id)];
        let follower_heartbeats = vec![get_heartbeat_msg(Duration::minutes(0), channel.spec.validators.leader().id), get_heartbeat_msg(Duration::minutes(0), channel.spec.validators.follower().id)];
        let new_state = get_new_state_msg();
        let approve_state = get_approve_state_msg();

        let messages = Messages {
            leader: LastApprovedResponse {
                last_approved: Some(LastApproved {
                    new_state: Some(new_state),
                    approve_state: None,
                }),
                heartbeats: Some(leader_heartbeats),
            },
            follower: LastApprovedResponse {
                last_approved: Some(LastApproved {
                    new_state: None,
                    approve_state: Some(approve_state),
                }),
                heartbeats: Some(follower_heartbeats),
            },
            recency: Duration::minutes(4),
        };

        assert_eq!(
            is_disconnected(&channel, &messages),
            false,
            "Leader hb has recent messages that came from the follower, and the follower has recent messages that came from the leader"
        )
    }

    // @TODO: test is_offline()
}
