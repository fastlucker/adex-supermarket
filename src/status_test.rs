use super::*;
use crate::sentry_api::SentryApi;
use chrono::{Duration, Utc};
use primitives::{
    sentry::{
        ApproveStateValidatorMessage, LastApproved, LastApprovedResponse, NewStateValidatorMessage,
        ValidatorMessage, ValidatorMessageResponse,
    },
    util::tests::prep_db::{DUMMY_CHANNEL, DUMMY_VALIDATOR_FOLLOWER, DUMMY_VALIDATOR_LEADER, IDS},
    validator::{ApproveState, Heartbeat, MessageTypes, NewState},
    BalancesMap, Channel,
};

use httptest::{mappers::*, responders::*, Expectation, Server, ServerPool};
use lazy_static::lazy_static;

static SERVER_POOL: ServerPool = ServerPool::new(4);

lazy_static! {
    static ref RECENCY: Duration = Duration::minutes(4);
    static ref RECENT: Duration = Duration::minutes(3);
}

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

fn get_approve_state_msg(is_healthy: bool) -> ApproveStateValidatorMessage {
    ApproveStateValidatorMessage {
        from: DUMMY_VALIDATOR_LEADER.id,
        received: Utc::now(),
        msg: MessageTypes::ApproveState(ApproveState {
            state_root: String::from("0x0"),
            signature: String::from("0x0"),
            is_healthy,
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

fn get_new_state_validator_msg() -> ValidatorMessage {
    ValidatorMessage {
        from: DUMMY_VALIDATOR_LEADER.id,
        received: Utc::now(),
        msg: MessageTypes::NewState(NewState {
            signature: String::from("0x0"),
            state_root: String::from("0x0"),
            balances: Default::default(),
        }),
    }
}

mod is_finalized {
    use super::*;

    lazy_static! {
        static ref SENTRY_API_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);
    }

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

        let sentry = SentryApi::new(*SENTRY_API_TIMEOUT).expect("Should work");

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

        let sentry = SentryApi::new(*SENTRY_API_TIMEOUT).expect("Should work");

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

        let sentry = SentryApi::new(*SENTRY_API_TIMEOUT).expect("Should work");

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

        let sentry = SentryApi::new(*SENTRY_API_TIMEOUT).expect("Should work");

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

mod is_date_recent {
    use super::*;

    #[test]
    fn now_date_is_recent() {
        let now = Utc::now();
        let recency = *RECENCY;
        assert!(
            is_date_recent(recency, &now),
            "The present moment is a recent date!"
        )
    }

    #[test]
    fn slightly_past_is_recent() {
        let recency = *RECENCY;
        let on_the_edge = Utc::now() - Duration::minutes(3);
        assert!(
            is_date_recent(recency, &on_the_edge),
            "When date is just as old as the recency limit, it still counts as recent"
        )
    }

    #[test]
    fn old_date_is_not_recent() {
        let recency = *RECENCY;
        let past = Utc::now() - Duration::minutes(10);
        assert_eq!(
            is_date_recent(recency, &past),
            false,
            "Date older than the recency limit is not recent"
        )
    }
}

mod is_initializing {
    use super::*;

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
            recency: *RECENCY,
        };

        assert_eq!(
            is_initializing(&messages),
            true,
            "Both leader heartbeats + newstate and follower heatbeats + approvestate pairs are empty arrays"
        )
    }

    #[test]
    fn leader_has_no_messages() {
        let approve_state = get_approve_state_msg(true);
        let heartbeat = get_heartbeat_msg(Duration::zero(), IDS["leader"]);
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
            recency: *RECENCY,
        };

        assert_eq!(
            is_initializing(&messages),
            true,
            "Leader has no new messages but the follower has heartbeats and approvestate"
        )
    }

    #[test]
    fn follower_has_no_messages() {
        let heartbeat = get_heartbeat_msg(Duration::zero(), IDS["leader"]);
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
            recency: *RECENCY,
        };

        assert_eq!(
            is_initializing(&messages),
            true,
            "Follower has no new messages but leader has heartbeats and newstate"
        )
    }

    #[test]
    fn both_arrays_have_messages() {
        let leader_heartbeats = vec![get_heartbeat_msg(Duration::zero(), IDS["leader"])];
        let follower_heartbeats = vec![get_heartbeat_msg(Duration::zero(), IDS["leader"])];
        let new_state = get_new_state_msg();
        let approve_state = get_approve_state_msg(true);

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
            recency: *RECENCY,
        };

        assert_eq!(
            is_initializing(&messages),
            false,
            "Both arrays have messages"
        )
    }
}

mod is_disconnected {
    use super::*;

    lazy_static! {
        static ref TEN_MINUTES: Duration = Duration::minutes(10);
    }

    #[test]
    fn no_recent_hbs_on_both_sides() {
        let channel = DUMMY_CHANNEL.clone();
        let leader_heartbeats = vec![get_heartbeat_msg(
            *TEN_MINUTES,
            channel.spec.validators.leader().id,
        )];
        let follower_heartbeats = vec![get_heartbeat_msg(
            *TEN_MINUTES,
            channel.spec.validators.leader().id,
        )];

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
            recency: *RECENCY,
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
        let leader_heartbeats = vec![
            get_heartbeat_msg(*TEN_MINUTES, channel.spec.validators.leader().id),
            get_heartbeat_msg(*TEN_MINUTES, channel.spec.validators.follower().id),
        ];
        let follower_heartbeats = vec![
            get_heartbeat_msg(*TEN_MINUTES, channel.spec.validators.leader().id),
            get_heartbeat_msg(*TEN_MINUTES, channel.spec.validators.follower().id),
        ];

        let new_state = get_new_state_msg();
        let approve_state = get_approve_state_msg(true);

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
            recency: *RECENCY,
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
        let leader_heartbeats = vec![get_heartbeat_msg(
            Duration::zero(),
            channel.spec.validators.leader().id,
        )];
        let follower_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];

        let new_state = get_new_state_msg();
        let approve_state = get_approve_state_msg(true);

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
            recency: *RECENCY,
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
        let leader_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
        ];
        let follower_heartbeats = vec![get_heartbeat_msg(
            Duration::zero(),
            channel.spec.validators.follower().id,
        )];

        let new_state = get_new_state_msg();
        let approve_state = get_approve_state_msg(true);

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
            recency: *RECENCY,
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
        let leader_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];
        let follower_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];

        let new_state = get_new_state_msg();
        let approve_state = get_approve_state_msg(true);

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
            recency: *RECENCY,
        };

        assert_eq!(
            is_disconnected(&channel, &messages),
            false,
            "Leader hb has recent messages that came from the follower, and the follower has recent messages that came from the leader"
        )
    }
}

mod is_rejected_state {
    use super::*;

    lazy_static! {
        static ref SENTRY_API_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);
    }

    #[tokio::test]
    async fn new_state_but_no_approve_state() {
        let server = SERVER_POOL.get_server();
        let channel = get_request_channel(&server);

        let leader_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];
        let follower_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];

        let new_state = get_new_state_msg();

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
                    approve_state: None,
                }),
                heartbeats: Some(follower_heartbeats),
            },
            recency: Duration::minutes(4),
        };

        server.expect(
            Expectation::matching(any())
                .times(0)
                .respond_with(status_code(500)),
        );
        let sentry = SentryApi::new(*SENTRY_API_TIMEOUT).expect("Should work");
        let result = is_rejected_state(&channel, &messages, &sentry)
            .await
            .expect("Should call for latest new state");
        assert_eq!(
            result, true,
            "Recent new_state messages but the follower does not issue or propagate approve_state"
        )
    }
    #[tokio::test]
    async fn last_approved_new_state_is_outdated() {
        let server = SERVER_POOL.get_server();
        let channel = get_request_channel(&server);

        let leader_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];
        let follower_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];

        let mut new_state = get_new_state_msg();
        new_state.received = Utc::now() - Duration::minutes(5);
        let mut latest_new_state = get_new_state_validator_msg();
        latest_new_state.received = Utc::now() - Duration::minutes(2);
        let approve_state = get_approve_state_msg(true);

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
        let mock_response = ValidatorMessageResponse {
            validator_messages: vec![latest_new_state],
        };

        server.expect(Expectation::matching(any()).respond_with(json_encoded(&mock_response)));
        let sentry = SentryApi::new(*SENTRY_API_TIMEOUT).expect("Should work");

        let result = is_rejected_state(&channel, &messages, &sentry)
            .await
            .expect("Should call for latest new state");

        assert_eq!(
            result,
            true,
            "Last approved new_state is older than the latest new_state AND the latest new_state is older than one minute"
        )
    }

    #[tokio::test]
    async fn recent_new_state_and_approve_state() {
        let server = SERVER_POOL.get_server();
        let channel = get_request_channel(&server);

        let leader_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];
        let follower_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];

        let mut new_state = get_new_state_msg();
        new_state.received = Utc::now() - Duration::minutes(5);
        let mut latest_new_state = get_new_state_validator_msg();
        latest_new_state.received = Utc::now() - Duration::zero();
        let approve_state = get_approve_state_msg(true);

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

        let mock_response = ValidatorMessageResponse {
            validator_messages: vec![latest_new_state],
        };

        server.expect(Expectation::matching(any()).respond_with(json_encoded(&mock_response)));
        let sentry = SentryApi::new(*SENTRY_API_TIMEOUT).expect("Should work");

        let result = is_rejected_state(&channel, &messages, &sentry)
            .await
            .expect("Should call for latest new state");

        assert_eq!(
            result, false,
            "Recent new_state messages and the follower propagates approve_state"
        )
    }

    #[tokio::test]
    async fn latest_new_state_is_very_new() {
        let server = SERVER_POOL.get_server();
        let channel = get_request_channel(&server);

        let leader_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];
        let follower_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];

        let mut new_state = get_new_state_msg();
        new_state.received = Utc::now() - Duration::minutes(5);
        let mut latest_new_state = get_new_state_validator_msg();
        latest_new_state.received = Utc::now() - Duration::zero();
        let approve_state = get_approve_state_msg(true);

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

        let mock_response = ValidatorMessageResponse {
            validator_messages: vec![latest_new_state],
        };

        server.expect(Expectation::matching(any()).respond_with(json_encoded(&mock_response)));
        let sentry = SentryApi::new(*SENTRY_API_TIMEOUT).expect("Should work");

        let result = is_rejected_state(&channel, &messages, &sentry)
            .await
            .expect("Should call for latest new state");
        assert_eq!(
            result, false,
            "Last approved newState is older than latest newstate but NOT older than a minute"
        )
    }

    #[tokio::test]
    async fn approved_and_latest_new_state_are_the_same() {
        let server = SERVER_POOL.get_server();
        let channel = get_request_channel(&server);

        let leader_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];
        let follower_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];

        let five_min_ago = Utc::now() - Duration::minutes(5);
        let mut new_state = get_new_state_msg();
        new_state.received = five_min_ago;
        let mut latest_new_state = get_new_state_validator_msg();
        latest_new_state.received = five_min_ago;
        let approve_state = get_approve_state_msg(true);

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
        let sentry = SentryApi::new(*SENTRY_API_TIMEOUT).expect("Should work");
        let mock_response = ValidatorMessageResponse {
            validator_messages: vec![latest_new_state],
        };

        server.expect(Expectation::matching(any()).respond_with(json_encoded(&mock_response)));
        let result = is_rejected_state(&channel, &messages, &sentry)
            .await
            .expect("Should call for latest new state");
        assert_eq!(
            result,
            false,
            "Last approved new state and latest new state are the same message and it is older than a minute"
        )
    }

    #[tokio::test]
    async fn approved_and_latest_new_state_are_the_same_and_new() {
        let server = SERVER_POOL.get_server();
        let channel = get_request_channel(&server);

        let leader_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];
        let follower_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];

        let thirty_sec_ago = Utc::now() - Duration::seconds(30);
        let mut new_state = get_new_state_msg();
        new_state.received = thirty_sec_ago;
        let mut latest_new_state = get_new_state_validator_msg();
        latest_new_state.received = thirty_sec_ago;
        let approve_state = get_approve_state_msg(true);

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
        let mock_response = ValidatorMessageResponse {
            validator_messages: vec![latest_new_state],
        };

        server.expect(Expectation::matching(any()).respond_with(json_encoded(&mock_response)));
        let sentry = SentryApi::new(*SENTRY_API_TIMEOUT).expect("Should work");

        let result = is_rejected_state(&channel, &messages, &sentry)
            .await
            .expect("Should call for latest new state");
        assert_eq!(
            result,
            false,
            "Last approved new state and latest new state are the same message and it is NOT older than a minute"
        )
    }
}

mod is_unhealthy {
    use super::*;

    #[test]
    fn approve_state_is_unhealthy() {
        let channel = DUMMY_CHANNEL.clone();
        let leader_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];
        let follower_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];

        let new_state = get_new_state_msg();
        let approve_state = get_approve_state_msg(false);

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
            is_unhealthy(&messages),
            true,
            "Recent new state and approve state but approve state reports unhealthy"
        )
    }

    #[test]
    fn approve_state_is_healthy() {
        let channel = DUMMY_CHANNEL.clone();
        let leader_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];
        let follower_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];

        let new_state = get_new_state_msg();
        let approve_state = get_approve_state_msg(true);

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
            is_unhealthy(&messages),
            false,
            "Recent new state and approve state and approve state reports healthy"
        )
    }

    #[test]
    fn no_recent_new_state_messages() {
        let channel = DUMMY_CHANNEL.clone();
        let leader_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];
        let follower_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];

        let approve_state = get_approve_state_msg(false);

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
                    approve_state: Some(approve_state),
                }),
                heartbeats: Some(follower_heartbeats),
            },
            recency: Duration::minutes(4),
        };

        assert_eq!(
            is_unhealthy(&messages),
            false,
            "Approve state is unhealthy but there are no recent new state messages"
        )
    }
}

mod is_active {
    use super::*;

    #[test]
    fn recent_heartbeats_and_healthy_approve_state() {
        let channel = DUMMY_CHANNEL.clone();
        let leader_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];
        let follower_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];

        let new_state = get_new_state_msg();
        let approve_state = get_approve_state_msg(true);

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
            recency: *RECENCY,
        };

        assert_eq!(
            is_active(&messages),
            true,
            "Recent heartbeat messages on both validators and approve state reports healthy"
        )
    }

    #[test]
    fn recent_heartbeats_but_unhealthy_approve_state() {
        let channel = DUMMY_CHANNEL.clone();
        let leader_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];
        let follower_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];

        let new_state = get_new_state_msg();
        let approve_state = get_approve_state_msg(false);

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
            recency: *RECENCY,
        };

        assert_eq!(
            is_active(&messages),
            false,
            "Recent heartbeat messages on both validators but approve state reports unhealthy"
        )
    }

    #[test]
    fn no_recent_heartbeats() {
        let channel = DUMMY_CHANNEL.clone();
        let leader_heartbeats = vec![
            get_heartbeat_msg(Duration::minutes(10), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::minutes(10), channel.spec.validators.follower().id),
        ];
        let follower_heartbeats = vec![
            get_heartbeat_msg(Duration::minutes(10), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::minutes(10), channel.spec.validators.follower().id),
        ];

        let new_state = get_new_state_msg();
        let approve_state = get_approve_state_msg(true);

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
            recency: *RECENCY,
        };

        assert_eq!(
            is_active(&messages),
            false,
            "No recent heartbeats on both validators"
        )
    }

    #[test]
    fn no_recent_heartbeats_on_leader() {
        let channel = DUMMY_CHANNEL.clone();
        let leader_heartbeats = vec![
            get_heartbeat_msg(Duration::minutes(10), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::minutes(10), channel.spec.validators.follower().id),
        ];
        let follower_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];

        let new_state = get_new_state_msg();
        let approve_state = get_approve_state_msg(true);

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
            recency: *RECENCY,
        };

        assert_eq!(
            is_active(&messages),
            false,
            "No recent heartbeats on leader validator"
        )
    }

    #[test]
    fn no_recent_heartbeats_on_follower() {
        let channel = DUMMY_CHANNEL.clone();
        let leader_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];
        let follower_heartbeats = vec![
            get_heartbeat_msg(Duration::minutes(10), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::minutes(10), channel.spec.validators.follower().id),
        ];

        let new_state = get_new_state_msg();
        let approve_state = get_approve_state_msg(true);

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
            recency: *RECENCY,
        };

        assert_eq!(
            is_active(&messages),
            false,
            "No recent heartbeats on follower validator"
        )
    }

    #[test]
    fn no_approve_state_message() {
        let channel = DUMMY_CHANNEL.clone();
        let leader_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];
        let follower_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];

        let new_state = get_new_state_msg();

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
                    approve_state: None,
                }),
                heartbeats: Some(follower_heartbeats),
            },
            recency: *RECENCY,
        };

        assert_eq!(
            is_active(&messages),
            false,
            "No approve state message on follower validators"
        )
    }
}

mod is_ready {
    use super::*;

    #[test]
    fn recent_messages_but_no_new_state() {
        let channel = DUMMY_CHANNEL.clone();
        let leader_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];
        let follower_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];

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
            recency: *RECENCY,
        };

        assert_eq!(
            is_ready(&messages),
            true,
            "Recent hearbeat messages and no new state means its ready"
        )
    }

    #[test]
    fn no_recent_messages_and_no_new_state() {
        let channel = DUMMY_CHANNEL.clone();
        let leader_heartbeats = vec![
            get_heartbeat_msg(Duration::minutes(10), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::minutes(10), channel.spec.validators.follower().id),
        ];
        let follower_heartbeats = vec![
            get_heartbeat_msg(Duration::minutes(10), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::minutes(10), channel.spec.validators.follower().id),
        ];

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
            recency: *RECENCY,
        };

        assert_eq!(
            is_ready(&messages),
            false,
            "No recent hearbeat messages on both validators and no new state means its not ready"
        )
    }

    #[test]
    fn no_recent_messages_on_leader_and_no_new_state() {
        let channel = DUMMY_CHANNEL.clone();
        let leader_heartbeats = vec![
            get_heartbeat_msg(Duration::minutes(10), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::minutes(10), channel.spec.validators.follower().id),
        ];
        let follower_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];

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
            recency: *RECENCY,
        };

        assert_eq!(
            is_ready(&messages),
            false,
            "No recent hearbeat messages on leader valdiator and no new state means its not ready"
        )
    }

    #[test]
    fn no_recent_messages_on_follower_and_no_new_state() {
        let channel = DUMMY_CHANNEL.clone();
        let leader_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];
        let follower_heartbeats = vec![
            get_heartbeat_msg(Duration::minutes(10), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::minutes(10), channel.spec.validators.follower().id),
        ];

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
            recency: *RECENCY,
        };

        assert_eq!(
            is_ready(&messages),
            false,
            "No recent hearbeat messages on follower and no new state means its not ready"
        )
    }

    #[test]
    fn recent_messages_and_new_state() {
        let channel = DUMMY_CHANNEL.clone();
        let leader_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];
        let follower_heartbeats = vec![
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.leader().id),
            get_heartbeat_msg(Duration::zero(), channel.spec.validators.follower().id),
        ];

        let new_state = get_new_state_msg();

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
                    approve_state: None,
                }),
                heartbeats: Some(follower_heartbeats),
            },
            recency: *RECENCY,
        };

        assert_eq!(
            is_ready(&messages),
            false,
            "Recent hearbeat messages and a new state message means it is past the ready stage"
        )
    }
}
