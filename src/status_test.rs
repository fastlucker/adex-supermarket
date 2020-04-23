use super::*;
use crate::sentry_api::SentryApi;
use chrono::{Duration, Utc};
use primitives::{
    sentry::{LastApproved, LastApprovedResponse, NewStateValidatorMessage},
    validator::{Heartbeat, MessageTypes, NewState},
    BalancesMap, Channel,
};

use httptest::{mappers::*, responders::*, Expectation, Server, ServerPool};
use primitives::util::tests::prep_db::{
    DUMMY_CHANNEL, DUMMY_VALIDATOR_FOLLOWER, DUMMY_VALIDATOR_LEADER,
};

static SERVER_POOL: ServerPool = ServerPool::new(4);

fn get_request_channel(server: &Server) -> Channel {
    let mut channel = DUMMY_CHANNEL.clone();
    let mut leader = DUMMY_VALIDATOR_LEADER.clone();
    leader.url = server.url_str("/leader");

    let mut follower = DUMMY_VALIDATOR_FOLLOWER.clone();
    follower.url = server.url_str("/follower");

    channel.spec.validators = (leader, follower).into();

    channel
}

fn get_heartbeat_msg(recency: Duration, from: ValidatorId) -> HeartbeatValidatorMessage {
    HeartbeatValidatorMessage {
        from,
        received: Utc::now() - recency,
        msg: MessageTypes::Heartbeat(Heartbeat {
            signature: String::new(),
            state_root: String::new(),
            timestamp: Utc::now() - recency,
        }),
    }
}

mod is_finalized {
    use super::*;

    #[tokio::test]
    async fn it_is_finalized_when_expired() {
        let server = SERVER_POOL.get_server();
        let mut channel = get_request_channel(&server);
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
    async fn it_is_finalized_when_in_withdraw_period() {
        let server = SERVER_POOL.get_server();
        let mut channel = get_request_channel(&server);
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

    #[tokio::test]
    async fn it_is_finalized_when_channel_is_exhausted() {
        let server = SERVER_POOL.get_server();
        let channel = get_request_channel(&server);

        let leader = channel.spec.validators.leader().id;
        let follower = channel.spec.validators.follower().id;

        let deposit = channel.deposit_amount.clone();
        let balances: BalancesMap = vec![
            (leader, BigNum::from(400)),
            (follower, deposit - BigNum::from(400)),
        ]
        .into_iter()
        .collect();
        let expected_balances = balances.clone();

        let response = LastApprovedResponse {
            last_approved: Some(LastApproved {
                new_state: Some(NewStateValidatorMessage {
                    from: channel.spec.validators.leader().id,
                    received: Utc::now(),
                    msg: MessageTypes::NewState(NewState {
                        state_root: String::new(),
                        signature: String::new(),
                        balances,
                    }),
                }),
                approve_state: None,
            }),
            heartbeats: None,
        };

        server.expect(Expectation::matching(any()).respond_with(json_encoded(response)));

        let sentry = SentryApi::new().expect("Should work");

        let actual = is_finalized(&sentry, &channel)
            .await
            .expect("Should query dummy server");

        let expected = IsFinalized::Yes {
            reason: Finalized::Exhausted,
            balances: expected_balances,
        };

        assert_eq!(expected, actual);
    }

    #[tokio::test]
    async fn it_is_not_finalized() {
        let server = SERVER_POOL.get_server();
        let channel = get_request_channel(&server);

        let leader = channel.spec.validators.leader().id;
        let follower = channel.spec.validators.follower().id;

        let balances: BalancesMap = vec![(leader, 400.into()), (follower, 1.into())]
            .into_iter()
            .collect();

        let leader_response = LastApprovedResponse {
            last_approved: Some(LastApproved {
                new_state: Some(NewStateValidatorMessage {
                    from: channel.spec.validators.leader().id,
                    received: Utc::now(),
                    msg: MessageTypes::NewState(NewState {
                        state_root: String::new(),
                        signature: String::new(),
                        balances,
                    }),
                }),
                approve_state: None,
            }),
            heartbeats: None,
        };

        server.expect(Expectation::matching(any()).respond_with(json_encoded(&leader_response)));

        let sentry = SentryApi::new().expect("Should work");

        let actual = is_finalized(&sentry, &channel)
            .await
            .expect("Should query dummy server");

        let expected = IsFinalized::No {
            leader: Box::new(leader_response),
        };

        assert_eq!(expected, actual);
    }
}

mod is_offline {
    use super::*;

    use lazy_static::lazy_static;

    lazy_static! {
        static ref RECENCY: Duration = Duration::minutes(4);
    }

    fn get_messages(
        leader: Option<Vec<HeartbeatValidatorMessage>>,
        follower: Option<Vec<HeartbeatValidatorMessage>>,
    ) -> Messages {
        Messages {
            leader: LastApprovedResponse {
                last_approved: None,
                heartbeats: leader,
            },
            follower: LastApprovedResponse {
                last_approved: None,
                heartbeats: follower,
            },
            // we don't check the Recency, so we don't care about it's value
            recency: *RECENCY,
        }
    }

    #[test]
    fn it_is_offline_when_no_heartbeats() {
        let messages = get_messages(Some(vec![]), Some(vec![]));

        assert!(
            is_offline(&messages),
            "Offline: If empty heartbeats for Leader & Follower!"
        );

        let messages = get_messages(None, None);

        assert!(
            is_offline(&messages),
            "Offline: If `None` heartbeats for Leader & Follower!"
        );
    }

    #[test]
    fn it_is_offline_when_it_has_one_recent_heartbeat() {
        let leader = DUMMY_VALIDATOR_LEADER.id;
        let follower = DUMMY_VALIDATOR_FOLLOWER.id;

        let leader_hb = get_heartbeat_msg(Duration::zero(), leader);
        let messages = get_messages(Some(vec![leader_hb]), None);

        assert!(
            is_offline(&messages),
            "Offline: If `None` / empty heartbeats in the Follower!"
        );

        let follower_hb = get_heartbeat_msg(Duration::zero(), follower);

        let messages = get_messages(None, Some(vec![follower_hb]));

        assert!(
            is_offline(&messages),
            "Offline: If `None` / empty heartbeats in the Leader!"
        );
    }

    #[test]
    fn it_is_offline_when_it_has_old_validators_heartbeats() {
        let leader = DUMMY_VALIDATOR_LEADER.id;
        let follower = DUMMY_VALIDATOR_FOLLOWER.id;
        let old_recency = *RECENCY + Duration::minutes(1);
        let old_leader_hb = get_heartbeat_msg(old_recency, leader);
        let old_follower_hb = get_heartbeat_msg(old_recency, follower);

        let messages = get_messages(Some(vec![old_leader_hb]), Some(vec![old_follower_hb]));

        assert!(
            is_offline(&messages),
            "Offline: If it does not have recent heartbeats in both Leader & Follower!"
        );
    }

    #[test]
    fn it_is_not_offline_when_it_has_recent_heartbeats() {
        let leader = DUMMY_VALIDATOR_LEADER.id;
        let follower = DUMMY_VALIDATOR_FOLLOWER.id;
        let recent_leader_hb = get_heartbeat_msg(Duration::zero(), leader);
        let recent_follower_hb = get_heartbeat_msg(Duration::zero(), follower);

        let messages = get_messages(Some(vec![recent_leader_hb]), Some(vec![recent_follower_hb]));

        assert_eq!(
            is_offline(&messages),
            false,
            "Not offline: If it has recent heartbeats in both Leader & Follower!"
        );
    }
}
