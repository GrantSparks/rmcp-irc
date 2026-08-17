//! End-to-end gateway tests against a deterministic in-process Ergo fixture.

use std::{
    collections::{BTreeSet, HashSet},
    net::{Ipv4Addr, SocketAddr},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use crate::{
    agent::{
        AgentId,
        actor::{CompletionMode, ConnectMilestone},
        journal::{EventClass, EventDirection, EventFilter},
    },
    config::{Config, DccReceiveRoot},
    dcc::{
        confine::ReceiveChoice,
        negotiation::{CtcpMessage, DccOffer, encode_address, parse_address},
        session::{DccKind, DccSession, DccSessionId, DccState},
        transfer::DestinationConflict,
    },
    gateway::{ConnectRequest, Gateway},
    irc::{
        correlation::CommandOutcome,
        registration::{NickConflictPolicy, Nickname},
        wire::{OutboundMessage, WireMessage},
    },
    mcp::{
        attention::{AttentionCheckOutput, AttentionCheckState, AttentionOpenInput},
        authorization::OwnerId,
        dcc_accept::DESTINATION_INPUT,
        request_profile::RequestProfile,
        service::{DccAcceptResolution, IrcMcpService},
        tools::DccAcceptInput,
        watch::WatchFilter,
    },
    tests::fake_ergo::FakeErgo,
};
use bytes::Bytes;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinSet,
};

fn connect_request(nickname: &str) -> ConnectRequest {
    ConnectRequest {
        nickname: Nickname::new(nickname).expect("nickname"),
        nickname_fallbacks: Vec::new(),
        nick_conflict_policy: NickConflictPolicy::Suffix,
        username: None,
        real_name: None,
        channels: Default::default(),
        activity: Default::default(),
    }
}

#[tokio::test]
async fn live_gateway_registers_queries_history_and_keeps_cursors_independent() {
    let fake = FakeErgo::spawn().await;
    let mut lines = fake.client_lines.subscribe();
    let gateway = Gateway::new(fake.config());
    let connected = gateway
        .connect(connect_request("Athena"))
        .await
        .expect("connect");
    assert_eq!(connected.motd.text, "Coordinate clearly.");

    let whois = gateway
        .execute(
            &connected.agent_id,
            OutboundMessage::new("WHOIS", vec!["Hermes".into()]),
            CompletionMode::Auto,
            Duration::from_secs(1),
        )
        .await
        .expect("WHOIS");
    assert_eq!(whois.outcome, CommandOutcome::Completed);
    assert_eq!(whois.replies.len(), 2);
    assert!(whois.first_event_cursor.is_some());

    let explicitly_collected_names = gateway
        .execute(
            &connected.agent_id,
            OutboundMessage::new("NAMES", vec!["#agents".into()]),
            CompletionMode::Collect,
            Duration::from_secs(1),
        )
        .await
        .expect("explicitly collected NAMES");
    assert_eq!(
        explicitly_collected_names.outcome,
        CommandOutcome::Completed
    );
    assert!(explicitly_collected_names.acknowledged);
    assert_eq!(explicitly_collected_names.replies.len(), 4);
    assert!(
        explicitly_collected_names
            .replies
            .last()
            .is_some_and(|reply| reply.command == "BATCH")
    );

    for mode in [CompletionMode::Auto, CompletionMode::Collect] {
        let time = gateway
            .execute(
                &connected.agent_id,
                OutboundMessage::new("TIME", Vec::new()),
                mode,
                Duration::from_secs(1),
            )
            .await
            .expect("direct labeled TIME reply");
        assert_eq!(time.outcome, CommandOutcome::Completed);
        assert!(time.acknowledged);
        assert_eq!(time.replies.len(), 1);
        assert_eq!(time.replies[0].command, "391");
    }

    let version = gateway
        .execute(
            &connected.agent_id,
            OutboundMessage::new("VERSION", Vec::new()),
            CompletionMode::Auto,
            Duration::from_secs(1),
        )
        .await
        .expect("batched VERSION");
    assert_eq!(version.outcome, CommandOutcome::Completed);
    assert!(version.acknowledged);
    assert_eq!(
        version
            .replies
            .iter()
            .map(|reply| reply.command.as_str())
            .collect::<Vec<_>>(),
        ["BATCH", "351", "005", "BATCH"]
    );
    let version_events = gateway
        .read_events(
            &connected.agent_id,
            None,
            1_000,
            Duration::ZERO,
            EventFilter {
                command_id: Some(version.command_id.as_str().to_owned()),
                ..EventFilter::default()
            },
        )
        .await
        .expect("VERSION events");
    assert_eq!(
        version_events
            .events
            .iter()
            .filter(|event| event.direction == EventDirection::Inbound)
            .count(),
        4
    );

    for message in [
        OutboundMessage::new("TOPIC", vec!["#agents".into()]).with_trailing("stress topic"),
        OutboundMessage::new("MODE", vec!["#agents".into(), "+i".into()]),
        OutboundMessage::new("MONITOR", vec!["S".into(), "peer".into()]),
        OutboundMessage::new("MONITOR", vec!["+".into(), "peer".into()]),
        OutboundMessage::new("MONITOR", vec!["-".into(), "peer".into()]),
    ] {
        let command = message.command.clone();
        let result = gateway
            .execute(
                &connected.agent_id,
                message,
                CompletionMode::Auto,
                Duration::from_secs(1),
            )
            .await
            .unwrap_or_else(|error| panic!("{command} mutation: {error}"));
        assert_eq!(result.outcome, CommandOutcome::Completed, "{command}");
        assert!(result.acknowledged, "{command}");
        assert_eq!(result.replies.len(), 1, "{command}");
    }

    let before = gateway
        .snapshot(&connected.agent_id)
        .await
        .expect("snapshot")
        .journal
        .latest;
    let sent = gateway
        .execute(
            &connected.agent_id,
            OutboundMessage::new("PRIVMSG", vec!["#agents".into()]).with_trailing("hello"),
            CompletionMode::Auto,
            Duration::from_secs(1),
        )
        .await
        .expect("send");
    assert_eq!(sent.outcome, CommandOutcome::Completed);

    let first = gateway
        .read_events(
            &connected.agent_id,
            Some(before.clone()),
            100,
            Duration::ZERO,
            EventFilter::default(),
        )
        .await
        .expect("first cursor");
    let second = gateway
        .read_events(
            &connected.agent_id,
            Some(before),
            100,
            Duration::ZERO,
            EventFilter::default(),
        )
        .await
        .expect("second cursor");
    assert_eq!(first.next_cursor, second.next_cursor);
    assert_eq!(first.events.len(), second.events.len());

    let history = gateway
        .execute(
            &connected.agent_id,
            OutboundMessage::new(
                "CHATHISTORY",
                vec!["LATEST".into(), "#agents".into(), "*".into(), "10".into()],
            ),
            CompletionMode::Auto,
            Duration::from_secs(1),
        )
        .await
        .expect("history");
    assert_eq!(history.outcome, CommandOutcome::Completed);
    let page = gateway
        .read_events(
            &connected.agent_id,
            None,
            1_000,
            Duration::ZERO,
            EventFilter {
                origin: Some(crate::agent::journal::EventOrigin::History),
                ..EventFilter::default()
            },
        )
        .await
        .expect("history events");
    assert!(
        page.events
            .iter()
            .any(|event| event.class == EventClass::MessageChannel)
    );
    gateway
        .execute(
            &connected.agent_id,
            OutboundMessage::new(
                "CHATHISTORY",
                vec!["LATEST".into(), "#agents".into(), "*".into(), "10".into()],
            ),
            CompletionMode::Auto,
            Duration::from_secs(1),
        )
        .await
        .expect("repeat explicit history");
    let repeated = gateway
        .read_events(
            &connected.agent_id,
            None,
            1_000,
            Duration::ZERO,
            EventFilter {
                origin: Some(crate::agent::journal::EventOrigin::History),
                ..EventFilter::default()
            },
        )
        .await
        .expect("repeated history events");
    assert_eq!(
        repeated
            .events
            .iter()
            .filter(
                |event| event.wire.as_ref().and_then(|wire| wire.tag_value("msgid"))
                    == Some("history-1")
            )
            .count(),
        2,
        "explicit history calls must return complete typed events even for previously seen msgids"
    );

    let history_request = |target: &str| {
        OutboundMessage::new(
            "CHATHISTORY",
            vec!["LATEST".into(), target.into(), "*".into(), "10".into()],
        )
    };
    let (alpha, beta) = tokio::join!(
        gateway.execute(
            &connected.agent_id,
            history_request("#history-alpha"),
            CompletionMode::Auto,
            Duration::from_secs(1),
        ),
        gateway.execute(
            &connected.agent_id,
            history_request("#history-beta"),
            CompletionMode::Auto,
            Duration::from_secs(1),
        )
    );
    for (result, target) in [
        (alpha.expect("parallel alpha history"), "#history-alpha"),
        (beta.expect("parallel beta history"), "#history-beta"),
    ] {
        let page = gateway
            .read_events(
                &connected.agent_id,
                None,
                100,
                Duration::ZERO,
                EventFilter {
                    command_id: Some(result.command_id.as_str().to_owned()),
                    class: Some(EventClass::MessageChannel),
                    origin: Some(crate::agent::journal::EventOrigin::History),
                    ..EventFilter::default()
                },
            )
            .await
            .expect("command-correlated history events");
        assert_eq!(page.events.len(), 1);
        assert_eq!(page.events[0].target.as_deref(), Some(target));
        assert_eq!(
            page.events[0].correlation.command_id.as_deref(),
            Some(result.command_id.as_str())
        );
    }

    gateway
        .execute(
            &connected.agent_id,
            OutboundMessage::new("TESTCTCP", Vec::new()),
            CompletionMode::FireAndForget,
            Duration::from_secs(1),
        )
        .await
        .expect("CTCP fixture trigger");
    let clientinfo = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let line = lines.recv().await.expect("captured CTCP line");
            if line.starts_with("NOTICE peer :\u{1}CLIENTINFO") {
                return line;
            }
        }
    })
    .await
    .expect("CLIENTINFO reply");
    assert!(clientinfo.contains("DCC PING TIME VERSION"));

    gateway
        .disconnect(&connected.agent_id, Some("done".into()))
        .await
        .expect("disconnect");
}

#[tokio::test]
async fn sixteen_agents_arbitrate_one_nickname_and_query_without_crossover() {
    let fake = FakeErgo::spawn().await;
    let gateway = Arc::new(Gateway::new(fake.config()));
    let mut connects = JoinSet::new();
    for _ in 0..16 {
        let gateway = gateway.clone();
        connects.spawn(async move { gateway.connect(connect_request("Hermes")).await });
    }
    let mut connected = Vec::new();
    while let Some(result) = connects.join_next().await {
        connected.push(result.expect("join").expect("connect"));
    }
    let nicknames: HashSet<_> = connected
        .iter()
        .map(|agent| agent.nickname.to_string())
        .collect();
    assert_eq!(nicknames.len(), 16);
    assert_eq!(gateway.agent_count().await, 16);

    let mut queries = JoinSet::new();
    for agent in &connected {
        let gateway = gateway.clone();
        let agent_id = agent.agent_id.clone();
        let expected = agent.nickname.to_string();
        queries.spawn(async move {
            let result = gateway
                .execute(
                    &agent_id,
                    OutboundMessage::new("WHOIS", vec![expected.clone()]),
                    CompletionMode::Auto,
                    Duration::from_secs(1),
                )
                .await?;
            Ok::<_, crate::error::GatewayError>((expected, result))
        });
    }
    while let Some(result) = queries.join_next().await {
        let (expected, result) = result.expect("join").expect("query");
        assert_eq!(result.outcome, CommandOutcome::Completed);
        assert!(result.replies.iter().all(|reply| {
            reply
                .params
                .get(1)
                .is_none_or(|subject| subject == &expected)
        }));
    }

    for agent in connected {
        gateway
            .disconnect(&agent.agent_id, None)
            .await
            .expect("disconnect");
    }
}

#[tokio::test]
async fn ordinary_dcc_chat_uses_a_real_direct_socket_and_emits_both_directions() {
    let fake = FakeErgo::spawn().await;
    let mut lines = fake.client_lines.subscribe();
    let gateway = Gateway::new(fake.config());
    let connected = gateway
        .connect(connect_request("Hestia"))
        .await
        .expect("connect");
    let before = gateway
        .snapshot(&connected.agent_id)
        .await
        .expect("snapshot")
        .journal
        .latest;
    let session = gateway
        .dcc_chat_open(&connected.agent_id, "Peer".into(), false)
        .await
        .expect("open DCC");

    let offer_line = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let line = lines.recv().await.expect("captured line");
            if line.contains("DCC CHAT") {
                return line;
            }
        }
    })
    .await
    .expect("DCC offer timeout");
    let message = WireMessage::parse(Bytes::copy_from_slice(offer_line.as_bytes())).expect("wire");
    let ctcp = CtcpMessage::parse(message.trailing.as_deref().expect("trailing")).expect("CTCP");
    let DccOffer::Chat { address, port, .. } =
        DccOffer::parse(ctcp.body.as_deref().expect("DCC body")).expect("offer")
    else {
        panic!("expected chat offer");
    };
    let endpoint = SocketAddr::new(parse_address(&address).expect("address"), port);
    let mut peer = TcpStream::connect(endpoint).await.expect("direct connect");

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let sessions = gateway
                .dcc_list(&connected.agent_id, None, None, None)
                .await
                .expect("list");
            if sessions.iter().any(|item| {
                item.id == session.id && item.state == crate::dcc::session::DccState::Active
            }) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("DCC activation");

    gateway
        .dcc_chat_send(&connected.agent_id, session.id.clone(), "outbound".into())
        .await
        .expect("chat send");
    let mut outbound = [0; 9];
    peer.read_exact(&mut outbound).await.expect("peer read");
    assert_eq!(&outbound, b"outbound\n");
    peer.write_all(b"inbound\n").await.expect("peer write");

    let page = gateway
        .read_events(
            &connected.agent_id,
            Some(before),
            100,
            Duration::from_secs(2),
            EventFilter {
                class: Some(EventClass::DccChatMessage),
                ..EventFilter::default()
            },
        )
        .await
        .expect("chat events");
    assert!(!page.events.is_empty());
    gateway
        .dcc_cancel(&connected.agent_id, session.id)
        .await
        .expect("cancel");
    gateway
        .disconnect(&connected.agent_id, None)
        .await
        .expect("disconnect");
}

#[tokio::test]
async fn reconnect_keeps_the_stream_refreshes_motd_and_requests_history_after_the_last_msgid() {
    let fake = FakeErgo::spawn().await;
    let mut lines = fake.client_lines.subscribe();
    let mut config = fake.config();
    // A fixed, wide enough backoff so the scheduled attempt this test asserts on
    // is observable through a status snapshot instead of only inferable from the
    // delay the caller was handed.
    config.reconnect.initial_delay_ms = 200;
    config.reconnect.max_delay_ms = 200;
    let gateway = Gateway::new(config);
    let connected = gateway
        .connect(connect_request("Brigid"))
        .await
        .expect("connect");
    gateway
        .execute(
            &connected.agent_id,
            OutboundMessage::new("JOIN", vec!["#forge".into()]),
            CompletionMode::Auto,
            Duration::from_secs(1),
        )
        .await
        .expect("join");
    gateway
        .execute(
            &connected.agent_id,
            OutboundMessage::new("PRIVMSG", vec!["#forge".into()]).with_trailing("checkpoint"),
            CompletionMode::Auto,
            Duration::from_secs(1),
        )
        .await
        .expect("message");
    let initial = gateway
        .snapshot(&connected.agent_id)
        .await
        .expect("initial snapshot");
    gateway
        .execute(
            &connected.agent_id,
            OutboundMessage::new("DROPME", Vec::new()),
            CompletionMode::FireAndForget,
            Duration::from_secs(1),
        )
        .await
        .expect("drop trigger");

    // Sample the degraded window on the way past it: a reconnecting actor must
    // publish when it will try again, not only how long it decided to wait.
    let mut scheduled = None;
    let reconnected = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let snapshot = gateway
                .snapshot(&connected.agent_id)
                .await
                .expect("reconnect snapshot");
            if snapshot.state.connection_state == crate::agent::state::ConnectionState::Reconnecting
                && let Some(next_attempt_at) = snapshot.state.reconnect.next_attempt_at
            {
                scheduled = Some((
                    snapshot.state.reconnect.attempt,
                    snapshot.state.reconnect.delay_ms,
                    next_attempt_at,
                    snapshot.state.snapshot_at,
                ));
            }
            if snapshot.state.connection_state == crate::agent::state::ConnectionState::Ready
                && snapshot.state.motd.source == Some(crate::agent::state::MotdSource::Reconnect)
            {
                return snapshot;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("reconnect deadline");
    assert_eq!(reconnected.journal.stream_id, initial.journal.stream_id);

    let (attempt, delay_ms, next_attempt_at, snapshot_at) =
        scheduled.expect("a reconnecting snapshot must publish its next attempt");
    assert!(attempt >= 1);
    assert_eq!(delay_ms, Some(200));
    assert!(
        next_attempt_at > snapshot_at,
        "the scheduled attempt must lie ahead of the snapshot that published it, \
         so a caller can tell how much of the wait is left"
    );
    assert!(
        reconnected.state.reconnect.next_attempt_at.is_none(),
        "a ready connection has no attempt pending"
    );
    assert_eq!(reconnected.state.reconnect.delay_ms, None);
    assert_eq!(reconnected.state.reconnect.attempt, 0);

    let recovery = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let line = lines.recv().await.expect("captured reconnect line");
            if line.starts_with("CHATHISTORY AFTER #forge") {
                return line;
            }
        }
    })
    .await
    .expect("history recovery command");
    assert!(recovery.contains("msgid=echo-Brigid"));

    let recovered = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let page = gateway
                .read_events(
                    &connected.agent_id,
                    None,
                    1_000,
                    Duration::ZERO,
                    EventFilter::default(),
                )
                .await
                .expect("recovery events");
            if page.events.iter().any(|event| {
                event.wire.as_ref().and_then(|wire| wire.tag_value("msgid")) == Some("reconnect-1")
            }) {
                return page.events;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("recovered event deadline");
    assert_eq!(
        recovered
            .iter()
            .filter(
                |event| event.wire.as_ref().and_then(|wire| wire.tag_value("msgid"))
                    == Some("echo-Brigid")
            )
            .count(),
        1,
        "reconnect recovery must suppress an overlapping live message"
    );
    assert!(recovered.iter().any(|event| {
        event.wire.as_ref().and_then(|wire| wire.tag_value("msgid")) == Some("reconnect-1")
            && event.origin == crate::agent::journal::EventOrigin::History
    }));

    let refreshed = gateway
        .execute(
            &connected.agent_id,
            OutboundMessage::new("MOTD", Vec::new()),
            CompletionMode::Auto,
            Duration::from_secs(1),
        )
        .await
        .expect("MOTD query");
    assert_eq!(refreshed.outcome, CommandOutcome::Completed);
    let snapshot = gateway
        .snapshot(&connected.agent_id)
        .await
        .expect("refreshed snapshot");
    assert_eq!(
        snapshot.state.motd.source,
        Some(crate::agent::state::MotdSource::Query)
    );
    assert_eq!(snapshot.state.motd.text, "Fresh instructions.");

    gateway
        .disconnect(&connected.agent_id, None)
        .await
        .expect("disconnect");
}

/// A connect that authenticates reports the same stage set as one that does
/// not, and reports each stage once.
///
/// The channel this drains is sized for the sequence — `ConnectMilestone::TOTAL`
/// — precisely so a caller that is slow to read cannot lose a stage. That
/// guarantee only holds if the actor emits each stage once: a SASL connect
/// settles capability negotiation twice, once to authenticate and once to end,
/// and the duplicate would overrun the channel and drop whichever stage came
/// last. The last stage is the one that says the guest is finally present in its
/// channels, so nothing is read here until the connect has finished, which is
/// the worst case the sizing claims to cover.
#[tokio::test]
async fn a_sasl_connect_reports_each_stage_exactly_once_and_ends_at_the_last_one() {
    let fake = FakeErgo::spawn().await;
    let mut config = fake.config();
    config.irc.sasl = Some(crate::tests::fake_ergo::sasl_config());
    let gateway = Gateway::new(config);
    let (milestones, mut reached) =
        tokio::sync::mpsc::channel(crate::agent::actor::ConnectMilestone::TOTAL as usize);

    let connected = gateway
        .connect_as(
            OwnerId::local(),
            connect_request("Ariadne"),
            Some(milestones),
        )
        .await
        .expect("an authenticated connect");

    let mut reported = Vec::new();
    while let Ok(milestone) = reached.try_recv() {
        reported.push(milestone);
    }
    let steps: Vec<u32> = reported.iter().map(|stage| stage.step()).collect();
    assert!(
        steps.windows(2).all(|pair| pair[0] < pair[1]),
        "stages are ordinals reported in order, never repeated: {reported:?}"
    );
    for stage in [
        ConnectMilestone::Connecting,
        ConnectMilestone::TransportReady { encrypted: false },
        ConnectMilestone::CapabilitiesNegotiated,
        ConnectMilestone::Authenticated,
        ConnectMilestone::Registered,
        ConnectMilestone::MotdComplete,
        ConnectMilestone::AutojoinSynchronized,
    ] {
        assert_eq!(
            reported.iter().filter(|seen| **seen == stage).count(),
            1,
            "{stage:?} was reported {} times: {reported:?}",
            reported.iter().filter(|seen| **seen == stage).count()
        );
    }
    assert_eq!(
        steps.last().copied(),
        Some(ConnectMilestone::TOTAL),
        "the stage saying the guest is present in its channels must survive: {reported:?}"
    );

    gateway
        .disconnect(&connected.agent_id, None)
        .await
        .expect("disconnect");
}

/// A bounded wait is bounded by what the caller asked for, not by how long the
/// relay happens to be offline.
///
/// Reads park inside the actor and are expired by its housekeeping tick. That
/// tick only ran in the connected loop, so a read issued during reconnect
/// backoff waited out the whole backoff — a caller that asked for half a second
/// and got the reconnect delay instead, with no way to tell the two apart.
#[tokio::test]
async fn a_bounded_read_returns_on_its_own_deadline_while_the_relay_is_reconnecting() {
    let fake = FakeErgo::spawn().await;
    let mut config = fake.config();
    // Long enough that a read waiting out the backoff is unmistakable.
    config.reconnect.initial_delay_ms = 30_000;
    config.reconnect.max_delay_ms = 30_000;
    config.reconnect.jitter = 0.0;
    let gateway = Gateway::new(config);
    let connected = gateway
        .connect(connect_request("Nyx"))
        .await
        .expect("connect");
    gateway
        .execute(
            &connected.agent_id,
            OutboundMessage::new("DROPME", Vec::new()),
            CompletionMode::FireAndForget,
            Duration::from_secs(1),
        )
        .await
        .expect("drop trigger");

    // Park the read from inside the backoff, and from the position the loss
    // itself left behind, so nothing already journaled can answer it early.
    let parked = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = gateway
                .snapshot(&connected.agent_id)
                .await
                .expect("snapshot");
            if snapshot.state.connection_state == crate::agent::state::ConnectionState::Reconnecting
            {
                return snapshot.journal.latest;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the relay must enter backoff");

    let started = std::time::Instant::now();
    let page = tokio::time::timeout(
        Duration::from_secs(5),
        gateway.read_events(
            &connected.agent_id,
            Some(parked),
            10,
            Duration::from_millis(300),
            EventFilter::default(),
        ),
    )
    .await
    .expect("a bounded read must not wait out the backoff")
    .expect("read events");

    assert!(page.events.is_empty(), "nothing arrived while offline");
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "the read returned after {:?}, which is the backoff rather than its own deadline",
        started.elapsed()
    );
}

/// Journal pressure has to reach the caller as relay state, not only as a
/// server-side trace.
///
/// The journal unit tests can prove the accounting and the rate limit, but not
/// that anything emits the warning: the record is journaled from the actor's
/// housekeeping tick, because pushing it from inside eviction would re-enter the
/// journal while it is still over budget. This covers that wiring end to end.
#[tokio::test]
async fn a_journal_under_pressure_reports_eviction_in_status_and_as_an_event() {
    let fake = FakeErgo::spawn().await;
    let mut config = fake.config();
    // Registration alone overruns a four-record window, so eviction is certain
    // without having to manufacture traffic for it.
    config.limits.event_count = 4;
    let gateway = Gateway::new(config);
    let connected = gateway
        .connect(connect_request("Mnemosyne"))
        .await
        .expect("connect");

    let (stats, event) = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let snapshot = gateway
                .snapshot(&connected.agent_id)
                .await
                .expect("snapshot");
            if snapshot.journal.evicted_events > 0
                && let Some(event) = snapshot
                    .recent_events
                    .iter()
                    .find(|event| event.class == EventClass::JournalPressure)
            {
                return (snapshot.journal.clone(), event.clone());
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("journal pressure deadline");

    assert!(stats.evicted_bytes > 0);
    assert!(stats.last_eviction_at.is_some());
    assert_eq!(
        stats.oversized_rejections, 0,
        "nothing here exceeds the byte budget, so only eviction should be counted"
    );
    assert_eq!(stats.retained_events, 4);
    let Some(crate::agent::journal::EventPayload::Pressure(reported)) = event.semantic.as_ref()
    else {
        panic!("a pressure record must carry its typed accounting");
    };
    assert!(reported.evicted_since_previous_report > 0);
    assert_eq!(reported.max_events, 4);
    assert_eq!(reported.retained_events, 4);

    gateway
        .disconnect(&connected.agent_id, None)
        .await
        .expect("disconnect");
}

/// An agent's own echoed message must never register as a mention.
///
/// This exercises the wiring that the `journal` unit tests cannot reach: the
/// actor has to have a registered nickname in reduced state at ingest time and
/// fold it with the server's advertised `CASEMAPPING`. If any of that is
/// missing, an agent addressing someone else would flag itself on every line
/// it sends. The fixture echoes a PRIVMSG back sourced from the sender, so the
/// message names `Athena` and arrives from `Athena`.
#[tokio::test]
async fn an_agents_own_echoed_message_is_never_flagged_as_a_mention() {
    let fake = FakeErgo::spawn().await;
    let gateway = Gateway::new(fake.config());
    let connected = gateway
        .connect(connect_request("Athena"))
        .await
        .expect("connect");

    let sent = gateway
        .execute(
            &connected.agent_id,
            OutboundMessage::new("PRIVMSG", vec!["#agents".into()])
                .with_trailing("Athena here, handing off to Hermes"),
            CompletionMode::Auto,
            Duration::from_secs(1),
        )
        .await
        .expect("PRIVMSG");
    assert_eq!(sent.outcome, CommandOutcome::Completed);

    let echoed = gateway
        .read_events(
            &connected.agent_id,
            None,
            50,
            Duration::from_millis(200),
            EventFilter {
                class: Some(EventClass::MessageChannel),
                direction: Some(EventDirection::Inbound),
                ..EventFilter::default()
            },
        )
        .await
        .expect("read echoed message");
    assert!(
        !echoed.events.is_empty(),
        "the fixture should echo the message back"
    );
    assert!(
        echoed.events.iter().all(|event| !event.mentions_me),
        "an agent's own nickname in its own message is not a mention"
    );

    // The mention filter must therefore select nothing here, and doing so must
    // not drag the cursor past the echoed traffic it declined.
    let mentions = gateway
        .read_events(
            &connected.agent_id,
            None,
            50,
            Duration::ZERO,
            EventFilter {
                mentions_me: Some(true),
                ..EventFilter::default()
            },
        )
        .await
        .expect("read mentions");
    assert!(mentions.events.is_empty());
    assert!(
        mentions.next_cursor.sequence < echoed.next_cursor.sequence,
        "an empty filtered read must not consume the events it filtered out"
    );

    gateway
        .disconnect(&connected.agent_id, None)
        .await
        .expect("disconnect");
}

/// The cursor resource must deliver everything after a consumed sequence, so
/// that `subscriptions/listen` plus `resources/read` is a complete loop with no
/// tool call and no polling in it.
#[tokio::test]
async fn event_cursor_pages_carry_the_journal_forward_without_a_tool_call() {
    let fake = FakeErgo::spawn().await;
    let gateway = Gateway::new(fake.config());
    let connected = gateway
        .connect(connect_request("Athena"))
        .await
        .expect("connect");

    let uri = format!(
        "irc://agents/{}/events/after/0",
        connected.agent_id.as_str()
    );
    let parsed = crate::mcp::resources::AgentResourceUri::from_str(&uri).expect("parse cursor URI");
    assert_eq!(
        parsed.kind,
        crate::mcp::resources::ResourceKind::EventsAfter(0)
    );

    // Registration alone puts events in the journal, so reading from zero must
    // return a positioned page rather than an unpositioned preview.
    let snapshot = gateway
        .snapshot(&connected.agent_id)
        .await
        .expect("snapshot");
    let first = gateway
        .read_events(
            &connected.agent_id,
            Some(crate::agent::journal::EventCursor {
                stream_id: snapshot.journal.stream_id.clone(),
                sequence: 0,
            }),
            10,
            Duration::ZERO,
            EventFilter::default(),
        )
        .await
        .expect("first cursor page");
    assert!(!first.events.is_empty());
    assert_eq!(
        first.status,
        crate::agent::journal::CursorStatus::Current,
        "reading from zero is a normal read, not a gap"
    );

    // Reading again from the cursor the page handed back must not repeat work
    // and must stay positioned on the same stream.
    let second = gateway
        .read_events(
            &connected.agent_id,
            Some(first.next_cursor.clone()),
            10,
            Duration::ZERO,
            EventFilter::default(),
        )
        .await
        .expect("second cursor page");
    assert_eq!(second.stream_id, first.stream_id);
    assert!(
        second
            .events
            .iter()
            .all(|event| event.cursor.sequence > first.next_cursor.sequence),
        "a page must never re-deliver what the previous cursor consumed"
    );

    gateway
        .disconnect(&connected.agent_id, None)
        .await
        .expect("disconnect");
}

/// A watch is a selection, not a queue: reading its resource must change
/// nothing, and its events must come from a position the caller supplies.
///
/// This is the property the old consuming read did not have. A host re-reads
/// resources at moments the model did not choose, so a read that advanced a
/// hidden cursor silently discarded events the model never saw.
#[tokio::test]
async fn a_watch_descriptor_read_is_pure_and_its_events_are_caller_positioned() {
    let fake = FakeErgo::spawn().await;
    let gateway = Gateway::new(fake.config());
    let connected = gateway
        .connect(connect_request("Ariadne"))
        .await
        .expect("connect");

    let created = gateway
        .create_watch(
            &connected.agent_id,
            WatchFilter {
                targets: BTreeSet::from(["#agents".to_string()]),
                ..WatchFilter::default()
            },
        )
        .await
        .expect("create watch");
    let watch_id = created.watch.watch_id.clone();
    let start = created.latest_cursor.clone();

    // The fixture echoes a PRIVMSG back as an inbound line, so this produces
    // traffic on the watched channel and on one the watch must ignore.
    for channel in ["#agents", "#elsewhere"] {
        gateway
            .execute(
                &connected.agent_id,
                OutboundMessage::new("PRIVMSG", vec![channel.into()])
                    .with_trailing("thread through the maze"),
                CompletionMode::Auto,
                Duration::from_secs(1),
            )
            .await
            .expect("send");
    }

    // Any number of descriptor reads describe the same watch and consume
    // nothing, which is what makes a host-initiated re-read harmless.
    let first = gateway.read_watch(&watch_id).await.expect("read watch");
    let second = gateway.read_watch(&watch_id).await.expect("re-read watch");
    assert_eq!(first.watch.watch_id, watch_id);
    assert_eq!(first.consume.events_read_tool, "irc.events.read");
    assert_eq!(
        second.consume.from_latest, first.consume.from_latest,
        "a descriptor read must not move anything"
    );
    assert_eq!(second.journal.stream_id, first.journal.stream_id);

    let window = gateway
        .read_watch_window(&watch_id, start.clone())
        .await
        .expect("positioned window");
    assert_eq!(window.status, crate::agent::journal::CursorStatus::Current);
    assert_eq!(window.requested_cursor, start);
    assert!(
        !window.events.is_empty(),
        "a watch on an active channel must deliver something"
    );
    assert!(
        window
            .events
            .iter()
            .all(|event| event.target.as_deref() == Some("#agents")),
        "a watch must not deliver targets it did not select: {:?}",
        window.events
    );
    assert!(
        window
            .events
            .iter()
            .any(|event| event.text.as_deref() == Some("thread through the maze")),
        "the watch dropped the message it was created for"
    );
    assert!(
        window
            .events
            .iter()
            .all(|event| event.cursor.sequence <= window.next_cursor.sequence),
        "the position must never run ahead of the events it returned"
    );
    assert_eq!(
        window.next_uri,
        crate::mcp::watch::watch_events_uri(&watch_id, &window.next_cursor)
    );

    // The retry a lost response forces: the same position, the same window.
    let replayed = gateway
        .read_watch_window(&watch_id, start)
        .await
        .expect("replayed window");
    assert_eq!(
        replayed.next_cursor, window.next_cursor,
        "re-reading one position must return one window"
    );
    assert_eq!(
        replayed
            .events
            .iter()
            .map(|event| event.cursor.clone())
            .collect::<Vec<_>>(),
        window
            .events
            .iter()
            .map(|event| event.cursor.clone())
            .collect::<Vec<_>>()
    );

    gateway.close_watch(&watch_id).expect("close watch");
    assert!(
        gateway.read_watch(&watch_id).await.is_err(),
        "a closed watch must not remain readable"
    );
    assert!(
        gateway
            .read_watch_window(&watch_id, window.next_cursor)
            .await
            .is_err(),
        "a closed watch must not keep serving windows"
    );

    gateway
        .disconnect(&connected.agent_id, None)
        .await
        .expect("disconnect");
}

/// A model-attention read has a selection-specific scan checkpoint. General
/// watch cursors intentionally remain on their last match, but a drained
/// attention selection can move through unrelated traffic without losing
/// anything it could ever select.
#[tokio::test]
async fn a_quiet_attention_check_catches_up_to_the_journal_head() {
    let fake = FakeErgo::spawn().await;
    let gateway = Gateway::new(fake.config());
    let connected = gateway
        .connect(connect_request("Hestia"))
        .await
        .expect("connect");
    let input = AttentionOpenInput {
        agent_id: connected.agent_id.clone(),
        full_traffic_targets: BTreeSet::new(),
    };
    let created = gateway
        .create_watch(&connected.agent_id, input.filter())
        .await
        .expect("open attention");

    // These are our own echoed messages and protocol replies, so the inbound
    // attention policy must ignore them while the underlying journal advances.
    for text in ["one", "two", "three"] {
        gateway
            .execute(
                &connected.agent_id,
                OutboundMessage::new("PRIVMSG", vec!["#background".into()]).with_trailing(text),
                CompletionMode::Auto,
                Duration::from_secs(1),
            )
            .await
            .expect("send background traffic");
    }

    let page = gateway
        .read_attention_events(
            &connected.agent_id,
            &created.watch.watch_id,
            created.latest_cursor.clone(),
            100,
            Duration::ZERO,
        )
        .await
        .expect("attention check");
    assert!(page.events.is_empty());
    assert!(page.latest.sequence > created.latest_cursor.sequence);
    assert_eq!(
        page.next_cursor, created.latest_cursor,
        "the underlying general journal page keeps its compatibility cursor"
    );

    let output = AttentionCheckOutput::from_page(page);
    assert_eq!(output.state, AttentionCheckState::Quiet);
    assert!(output.resume_cursor.sequence > created.latest_cursor.sequence);

    gateway
        .disconnect(&connected.agent_id, None)
        .await
        .expect("disconnect");
}

/// Two consumers of one watch each own their position, so neither can consume
/// the other's backlog — the failure an acknowledged server-side cursor keeps.
#[tokio::test]
async fn two_readers_of_one_watch_do_not_disturb_each_others_positions() {
    let fake = FakeErgo::spawn().await;
    let gateway = Gateway::new(fake.config());
    let connected = gateway
        .connect(connect_request("Theseus"))
        .await
        .expect("connect");

    let created = gateway
        .create_watch(
            &connected.agent_id,
            WatchFilter {
                targets: BTreeSet::from(["#agents".to_string()]),
                ..WatchFilter::default()
            },
        )
        .await
        .expect("create watch");
    let watch_id = created.watch.watch_id.clone();
    let shared_start = created.latest_cursor.clone();

    for text in ["first", "second", "third"] {
        gateway
            .execute(
                &connected.agent_id,
                OutboundMessage::new("PRIVMSG", vec!["#agents".into()]).with_trailing(text),
                CompletionMode::Auto,
                Duration::from_secs(1),
            )
            .await
            .expect("send");
    }

    // Reader A drains the whole backlog and keeps reading from where it got to.
    let drained = gateway
        .read_watch_events(
            &connected.agent_id,
            &watch_id,
            Some(shared_start.clone()),
            100,
            Duration::ZERO,
        )
        .await
        .expect("reader A");
    assert!(drained.events.len() >= 3);
    let exhausted = gateway
        .read_watch_events(
            &connected.agent_id,
            &watch_id,
            Some(drained.next_cursor.clone()),
            100,
            Duration::ZERO,
        )
        .await
        .expect("reader A again");
    assert!(
        exhausted.events.is_empty(),
        "reader A should have consumed its own backlog"
    );

    // Reader B, still holding the original position, sees exactly the same
    // backlog rather than the empty window reader A now sees.
    let independent = gateway
        .read_watch_events(
            &connected.agent_id,
            &watch_id,
            Some(shared_start.clone()),
            100,
            Duration::ZERO,
        )
        .await
        .expect("reader B");
    assert_eq!(
        independent
            .events
            .iter()
            .map(|event| event.cursor.clone())
            .collect::<Vec<_>>(),
        drained
            .events
            .iter()
            .map(|event| event.cursor.clone())
            .collect::<Vec<_>>(),
        "one reader consumed another reader's window"
    );

    // The other consumption path is positioned the same way, so a third reader
    // holding the original position is equally unaffected.
    let window = gateway
        .read_watch_window(&watch_id, shared_start)
        .await
        .expect("reader C");
    assert!(!window.events.is_empty());
    assert!(
        window
            .events
            .iter()
            .all(|event| drained.events.iter().any(|raw| raw.cursor == event.cursor)),
        "the compact window must be a projection of the same selected events"
    );

    gateway
        .disconnect(&connected.agent_id, None)
        .await
        .expect("disconnect");
}

/// `has_more` must be a signal a caller can loop on: every pass makes progress
/// and the loop ends, even when the stream retains records the watch declined.
#[tokio::test]
async fn draining_a_watch_backlog_makes_progress_while_more_remains() {
    let fake = FakeErgo::spawn().await;
    let gateway = Gateway::new(fake.config());
    let connected = gateway
        .connect(connect_request("Daedalus"))
        .await
        .expect("connect");

    let created = gateway
        .create_watch(
            &connected.agent_id,
            WatchFilter {
                targets: BTreeSet::from(["#agents".to_string()]),
                ..WatchFilter::default()
            },
        )
        .await
        .expect("create watch");
    let watch_id = created.watch.watch_id.clone();

    for text in ["one", "two", "three", "four"] {
        for channel in ["#agents", "#elsewhere"] {
            gateway
                .execute(
                    &connected.agent_id,
                    OutboundMessage::new("PRIVMSG", vec![channel.into()]).with_trailing(text),
                    CompletionMode::Auto,
                    Duration::from_secs(1),
                )
                .await
                .expect("send");
        }
    }

    let mut cursor = created.latest_cursor.clone();
    let mut collected = Vec::new();
    let mut passes = 0_usize;
    loop {
        let page = gateway
            .read_watch_events(
                &connected.agent_id,
                &watch_id,
                Some(cursor.clone()),
                1,
                Duration::ZERO,
            )
            .await
            .expect("drain one");
        assert!(
            page.next_cursor.sequence >= cursor.sequence,
            "a drain pass must never move backwards"
        );
        if page.has_more {
            assert_eq!(
                page.events.len(),
                1,
                "has_more must only be set when the limit truncated the window"
            );
            assert!(
                page.next_cursor.sequence > cursor.sequence,
                "has_more with no progress would loop forever"
            );
        }
        collected.extend(page.events.iter().map(|event| event.cursor.clone()));
        cursor = page.next_cursor;
        passes += 1;
        assert!(passes < 64, "draining did not terminate");
        if !page.has_more {
            break;
        }
    }
    assert!(collected.len() >= 4, "the drain lost part of the backlog");
    // Draining is complete: the same cursor now returns nothing, even though
    // the stream retains the #elsewhere traffic this watch never wanted.
    let exhausted = gateway
        .read_watch_events(
            &connected.agent_id,
            &watch_id,
            Some(cursor.clone()),
            10,
            Duration::ZERO,
        )
        .await
        .expect("exhausted read");
    assert!(exhausted.events.is_empty());
    assert!(!exhausted.has_more);
    assert!(
        exhausted.next_cursor.sequence < exhausted.latest.sequence,
        "records the watch declined must remain in the stream"
    );

    gateway
        .disconnect(&connected.agent_id, None)
        .await
        .expect("disconnect");
}

/// A watch's selection is applied inside the journal read, so a page's worth of
/// traffic it does not want can neither hide a match nor block the position.
#[tokio::test]
async fn a_watch_selection_reaches_matches_past_a_page_of_uninteresting_events() {
    let fake = FakeErgo::spawn().await;
    let gateway = Gateway::new(fake.config());
    let connected = gateway
        .connect(connect_request("Icarus"))
        .await
        .expect("connect");

    let created = gateway
        .create_watch(
            &connected.agent_id,
            WatchFilter {
                targets: BTreeSet::from(["#agents".to_string()]),
                ..WatchFilter::default()
            },
        )
        .await
        .expect("create watch");

    for _ in 0..8 {
        gateway
            .execute(
                &connected.agent_id,
                OutboundMessage::new("PRIVMSG", vec!["#elsewhere".into()])
                    .with_trailing("not for this watch"),
                CompletionMode::Auto,
                Duration::from_secs(1),
            )
            .await
            .expect("send");
    }
    gateway
        .execute(
            &connected.agent_id,
            OutboundMessage::new("PRIVMSG", vec!["#agents".into()]).with_trailing("for this watch"),
            CompletionMode::Auto,
            Duration::from_secs(1),
        )
        .await
        .expect("send");

    // One event of headroom: the match sits well past the first page of raw
    // records, and must still arrive on the first read.
    let page = gateway
        .read_watch_events(
            &connected.agent_id,
            &created.watch.watch_id,
            Some(created.latest_cursor),
            1,
            Duration::ZERO,
        )
        .await
        .expect("read past the uninteresting traffic");
    assert_eq!(page.events.len(), 1);
    assert_eq!(page.events[0].target.as_deref(), Some("#agents"));
    assert_eq!(page.next_cursor, page.events[0].cursor);
    assert!(
        !page.has_more,
        "only records this watch declined are left, so there is nothing more for it"
    );

    gateway
        .disconnect(&connected.agent_id, None)
        .await
        .expect("disconnect");
}

/// Retention loss and a foreign stream must be reported, not papered over, on
/// both consumption paths — a caller cannot recover a position it is not told
/// about.
#[tokio::test]
async fn a_watch_reports_gaps_and_stream_resets_through_both_consumption_paths() {
    let fake = FakeErgo::spawn().await;
    let mut config = fake.config();
    // Small enough that ordinary traffic evicts the position taken below.
    config.limits.event_count = 8;
    let gateway = Gateway::new(config);
    let connected = gateway
        .connect(connect_request("Minos"))
        .await
        .expect("connect");

    let created = gateway
        .create_watch(&connected.agent_id, WatchFilter::default())
        .await
        .expect("create watch");
    let watch_id = created.watch.watch_id.clone();
    let stale = crate::agent::journal::EventCursor {
        stream_id: created.latest_cursor.stream_id.clone(),
        sequence: 0,
    };

    for _ in 0..12 {
        gateway
            .execute(
                &connected.agent_id,
                OutboundMessage::new("PRIVMSG", vec!["#agents".into()]).with_trailing("churn"),
                CompletionMode::Auto,
                Duration::from_secs(1),
            )
            .await
            .expect("send");
    }

    let gapped = gateway
        .read_watch_window(&watch_id, stale.clone())
        .await
        .expect("gapped window");
    assert_eq!(
        gapped.status,
        crate::agent::journal::CursorStatus::EventGap,
        "a position the journal has evicted past must be reported as a gap"
    );
    let gapped_tool = gateway
        .read_watch_events(
            &connected.agent_id,
            &watch_id,
            Some(stale),
            100,
            Duration::ZERO,
        )
        .await
        .expect("gapped read");
    assert_eq!(
        gapped_tool.status,
        crate::agent::journal::CursorStatus::EventGap
    );
    // Recovery is a normal read from what the report handed back.
    let recovered = gateway
        .read_watch_window(&watch_id, gapped.next_cursor)
        .await
        .expect("recovered window");
    assert_eq!(
        recovered.status,
        crate::agent::journal::CursorStatus::Current
    );

    let foreign = crate::agent::journal::EventCursor {
        stream_id: "00000000-0000-4000-8000-000000000000".into(),
        sequence: 3,
    };
    assert_eq!(
        gateway
            .read_watch_window(&watch_id, foreign.clone())
            .await
            .expect("reset window")
            .status,
        crate::agent::journal::CursorStatus::StreamReset
    );
    assert_eq!(
        gateway
            .read_watch_events(
                &connected.agent_id,
                &watch_id,
                Some(foreign),
                100,
                Duration::ZERO
            )
            .await
            .expect("reset read")
            .status,
        crate::agent::journal::CursorStatus::StreamReset
    );

    gateway
        .disconnect(&connected.agent_id, None)
        .await
        .expect("disconnect");
}

/// A long poll through a watch has to behave exactly as an unfiltered one: the
/// parked read keeps the watch's selection and re-runs it when the journal
/// grows, which is what makes this the fallback for a host without
/// subscriptions.
#[tokio::test]
async fn a_watch_long_poll_wakes_on_traffic_the_watch_selected() {
    let fake = FakeErgo::spawn().await;
    let gateway = Gateway::new(fake.config());
    let connected = gateway
        .connect(connect_request("Pasiphae"))
        .await
        .expect("connect");
    let created = gateway
        .create_watch(
            &connected.agent_id,
            WatchFilter {
                targets: BTreeSet::from(["#agents".to_string()]),
                ..WatchFilter::default()
            },
        )
        .await
        .expect("create watch");

    let poll = gateway.read_watch_events(
        &connected.agent_id,
        &created.watch.watch_id,
        Some(created.latest_cursor.clone()),
        10,
        Duration::from_secs(5),
    );
    let send = gateway.execute(
        &connected.agent_id,
        OutboundMessage::new("PRIVMSG", vec!["#agents".into()]).with_trailing("wake up"),
        CompletionMode::Auto,
        Duration::from_secs(1),
    );
    let (page, sent) = tokio::join!(poll, send);
    sent.expect("send");
    let page = page.expect("long poll");
    assert!(
        page.events
            .iter()
            .all(|event| event.target.as_deref() == Some("#agents")),
        "a woken long poll must still apply the watch selection: {:?}",
        page.events
    );
    assert!(page.next_cursor.sequence > created.latest_cursor.sequence);

    gateway
        .disconnect(&connected.agent_id, None)
        .await
        .expect("disconnect");
}

/// Watch handles are bounded per caller and expire when nobody uses them, so
/// the "unknown or expired watch handle" the error text promises is real.
#[tokio::test]
async fn watch_handles_are_bounded_per_caller_and_expire_when_unused() {
    let fake = FakeErgo::spawn().await;
    let mut config = fake.config();
    config.limits.max_watches_per_owner = 1;
    let gateway = Gateway::new(config);
    let mine = OwnerId::from_bearer("mine");
    let theirs = OwnerId::from_bearer("theirs");
    let connected = gateway
        .connect_as(mine.clone(), connect_request("Ariadne"), None)
        .await
        .expect("connect");
    let other = gateway
        .connect_as(theirs, connect_request("Phaedra"), None)
        .await
        .expect("connect");

    gateway
        .create_watch(&connected.agent_id, WatchFilter::default())
        .await
        .expect("within the allowance");
    let refused = gateway
        .create_watch(&connected.agent_id, WatchFilter::default())
        .await
        .expect_err("past the allowance");
    assert_eq!(refused.kind(), crate::error::ErrorKind::ResourceLimit);
    // The bound is per caller, not per process.
    gateway
        .create_watch(&other.agent_id, WatchFilter::default())
        .await
        .expect("another caller has its own allowance");

    for agent_id in [&connected.agent_id, &other.agent_id] {
        gateway
            .disconnect(agent_id, None)
            .await
            .expect("disconnect");
    }

    // Expiry, with a window short enough to observe.
    let fake = FakeErgo::spawn().await;
    let mut config = fake.config();
    config.limits.watch_ttl_ms = 1;
    let gateway = Gateway::new(config);
    let connected = gateway
        .connect(connect_request("Ariadne"))
        .await
        .expect("connect");
    let created = gateway
        .create_watch(&connected.agent_id, WatchFilter::default())
        .await
        .expect("create watch");
    tokio::time::sleep(Duration::from_millis(20)).await;
    let expired = gateway
        .read_watch(&created.watch.watch_id)
        .await
        .expect_err("an unused watch must lapse");
    assert!(
        expired.to_string().contains("unknown or expired"),
        "an expired handle must say so: {expired}"
    );
    assert!(gateway.watches().list().is_empty());

    gateway
        .disconnect(&connected.agent_id, None)
        .await
        .expect("disconnect");
}

#[tokio::test]
async fn disconnecting_an_agent_releases_its_watches() {
    let fake = FakeErgo::spawn().await;
    let gateway = Gateway::new(fake.config());
    let connected = gateway
        .connect(connect_request("Icarus"))
        .await
        .expect("connect");
    let created = gateway
        .create_watch(&connected.agent_id, WatchFilter::default())
        .await
        .expect("create watch");

    gateway
        .disconnect(&connected.agent_id, None)
        .await
        .expect("disconnect");

    assert!(
        gateway
            .watches()
            .describe(&created.watch.watch_id)
            .is_none(),
        "a watch outlived the stream it was watching"
    );
}

#[tokio::test]
async fn one_callers_handles_are_invisible_and_unusable_to_another() {
    let fake = FakeErgo::spawn().await;
    let gateway = Gateway::new(fake.config());
    let mine = OwnerId::from_bearer("mine");
    let theirs = OwnerId::from_bearer("theirs");

    let connected = gateway
        .connect_as(mine.clone(), connect_request("Minos"), None)
        .await
        .expect("connect");
    let created = gateway
        .create_watch(&connected.agent_id, WatchFilter::default())
        .await
        .expect("create watch");

    assert_eq!(
        gateway.agent_ids_for(&mine).await,
        vec![connected.agent_id.clone()]
    );
    assert!(
        gateway.agent_ids_for(&theirs).await.is_empty(),
        "another caller must not see handles it did not create"
    );
    assert!(
        gateway
            .authorize_agent(&mine, &connected.agent_id)
            .await
            .is_ok()
    );
    assert!(
        gateway
            .authorize_agent(&theirs, &connected.agent_id)
            .await
            .is_err(),
        "another caller must not be able to operate this agent"
    );
    // A watch is reachable only through the agent it watches.
    assert!(
        gateway
            .authorize_watch(&mine, &created.watch.watch_id)
            .await
            .is_ok()
    );
    assert!(
        gateway
            .authorize_watch(&theirs, &created.watch.watch_id)
            .await
            .is_err()
    );

    // Refusal must not reveal that the handle exists at all.
    let unknown = gateway
        .authorize_agent(&theirs, &connected.agent_id)
        .await
        .expect_err("refused");
    assert!(
        unknown.message.contains("unknown or expired"),
        "an unowned handle must read exactly like a missing one: {unknown:?}"
    );

    gateway
        .disconnect(&connected.agent_id, None)
        .await
        .expect("disconnect");
}

/// A configuration whose DCC receive roots are named directories inside `base`.
///
/// Loopback peers are permitted because the peer in these tests is a real socket
/// in this process, which is exactly the case the production refusal exists to
/// keep out.
pub(super) fn config_with_receive_roots(
    fake: &FakeErgo,
    base: &std::path::Path,
    names: &[&str],
) -> Config {
    let mut config = fake.config();
    config.dcc.allow_private_addresses = true;
    config.dcc.receive_roots = names
        .iter()
        .map(|name| DccReceiveRoot {
            name: (*name).to_owned(),
            path: base.join(name),
        })
        .collect();
    config.validate().expect("receive roots are valid");
    config
}

/// Have the fixture push one inbound DCC SEND offer and wait for the actor to
/// retain it.
pub(super) async fn offered_transfer(
    gateway: &Gateway,
    agent_id: &AgentId,
    endpoint: SocketAddr,
    size: u64,
    filename: &str,
) -> DccSession {
    gateway
        .execute(
            agent_id,
            OutboundMessage::new(
                "TESTDCCSEND",
                vec![
                    encode_address(endpoint.ip()),
                    endpoint.port().to_string(),
                    size.to_string(),
                    filename.to_owned(),
                ],
            ),
            CompletionMode::FireAndForget,
            Duration::from_secs(1),
        )
        .await
        .expect("DCC SEND fixture trigger");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let offered = gateway
                .dcc_list(agent_id, Some(DccState::Offered), Some(DccKind::Send), None)
                .await
                .expect("list DCC sessions");
            if let Some(session) = offered
                .into_iter()
                .find(|session| session.filename.as_deref() == Some(filename))
            {
                return session;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the offer must be retained")
}

/// Wait for one DCC session to settle, then return it.
async fn settled_transfer(
    gateway: &Gateway,
    agent_id: &AgentId,
    session_id: &DccSessionId,
) -> DccSession {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let session = gateway
                .dcc_session(agent_id, session_id)
                .await
                .expect("session");
            if session.state.is_terminal() {
                return session;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the transfer must settle")
}

/// One request's declarations, as a well-formed 2026-07-28 caller sends them.
fn declaring_elicitation(elicitation: bool) -> RequestProfile {
    let mut capabilities = rmcp::model::ClientCapabilities::default();
    if elicitation {
        capabilities.elicitation = Some(rmcp::model::ElicitationCapability::new());
    }
    let mut meta = rmcp::model::RequestMetaObject::new();
    meta.set_protocol_version(rmcp::model::ProtocolVersion::V_2026_07_28);
    meta.set_client_capabilities(capabilities);
    RequestProfile::from_meta(&meta)
}

fn accept_input(
    agent_id: &AgentId,
    session_id: &DccSessionId,
    root: Option<&str>,
    destination_path: Option<&str>,
) -> DccAcceptInput {
    DccAcceptInput {
        agent_id: agent_id.clone(),
        dcc_session_id: session_id.clone(),
        root: root.map(str::to_owned),
        destination_path: destination_path.map(std::path::PathBuf::from),
        conflict: DestinationConflict::Fail,
    }
}

/// One accepted form answer, shaped the way a client echoes an elicitation back.
fn answered(root: Option<&str>, destination_path: Option<&str>) -> rmcp::model::InputResponses {
    let mut content = serde_json::Map::new();
    if let Some(root) = root {
        content.insert("root".into(), serde_json::Value::String(root.into()));
    }
    if let Some(path) = destination_path {
        content.insert(
            "destination_path".into(),
            serde_json::Value::String(path.into()),
        );
    }
    rmcp::model::InputResponses::from([(
        DESTINATION_INPUT.to_owned(),
        serde_json::json!({ "action": "accept", "content": content }),
    )])
}

#[tokio::test]
async fn an_accepted_transfer_lands_under_the_receive_root_the_caller_named() {
    let scratch = tempfile::tempdir().expect("scratch");
    let fake = FakeErgo::spawn().await;
    let gateway = Gateway::new(config_with_receive_roots(
        &fake,
        scratch.path(),
        &["inbox", "archive"],
    ));
    let connected = gateway
        .connect(connect_request("Iris"))
        .await
        .expect("connect");

    let data = vec![0x37_u8; 40_000];
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("peer listener");
    let endpoint = listener.local_addr().expect("peer address");
    let payload = data.clone();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("peer accept");
        stream.write_all(&payload).await.expect("peer write");
        let mut acknowledgement = [0_u8; 4];
        loop {
            stream
                .read_exact(&mut acknowledgement)
                .await
                .expect("acknowledgement");
            if u64::from(u32::from_be_bytes(acknowledgement)) >= payload.len() as u64 {
                return;
            }
        }
    });

    let offer = offered_transfer(
        &gateway,
        &connected.agent_id,
        endpoint,
        data.len() as u64,
        "report.txt",
    )
    .await;
    let accepted = gateway
        .dcc_accept(
            &connected.agent_id,
            offer.id.clone(),
            Some(ReceiveChoice {
                root: "archive".into(),
                path: std::path::PathBuf::from("august/report.txt"),
            }),
            DestinationConflict::Fail,
        )
        .await
        .expect("accept");
    // The chosen root and root-relative destination are reported from the moment
    // of acceptance, not only once the file has landed.
    assert_eq!(accepted.receive_root.as_deref(), Some("archive"));
    assert_eq!(
        accepted.receive_path.as_deref(),
        Some(std::path::Path::new("august/report.txt"))
    );

    let settled = settled_transfer(&gateway, &connected.agent_id, &offer.id).await;
    peer.await.expect("peer");
    assert_eq!(settled.state, DccState::Completed, "{settled:?}");
    assert_eq!(settled.receive_root.as_deref(), Some("archive"));
    assert_eq!(
        settled.receive_path.as_deref(),
        Some(std::path::Path::new("august/report.txt"))
    );

    let landed = scratch.path().join("archive/august/report.txt");
    assert_eq!(
        tokio::fs::read(&landed).await.expect("received file"),
        data,
        "the file must land beneath the root the caller named"
    );
    assert!(
        !scratch.path().join("inbox/august").exists(),
        "naming one root must not touch another"
    );
    assert_eq!(
        settled
            .local_path
            .as_deref()
            .and_then(std::path::Path::file_name),
        landed.file_name()
    );

    gateway
        .disconnect(&connected.agent_id, None)
        .await
        .expect("disconnect");
}

#[tokio::test]
async fn one_configured_receive_root_is_selected_without_asking_anybody() {
    let scratch = tempfile::tempdir().expect("scratch");
    let fake = FakeErgo::spawn().await;
    let gateway = Arc::new(Gateway::new(config_with_receive_roots(
        &fake,
        scratch.path(),
        &["inbox"],
    )));
    let service = IrcMcpService::new(gateway.clone());
    let connected = gateway
        .connect(connect_request("Selene"))
        .await
        .expect("connect");
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("peer listener");
    let offer = offered_transfer(
        &gateway,
        &connected.agent_id,
        listener.local_addr().expect("peer address"),
        16,
        "report.txt",
    )
    .await;

    // Neither a root nor a destination: one configured root answers the first
    // question and the offered filename answers the second, so even a client
    // that could not answer an elicitation needs no round trip.
    let resolution = service
        .plan_dcc_accept(
            &OwnerId::local(),
            &declaring_elicitation(false),
            &accept_input(&connected.agent_id, &offer.id, None, None),
            None,
            None,
        )
        .await
        .expect("plan");
    let DccAcceptResolution::Ready(plan) = resolution else {
        panic!("a single root needs no input: {resolution:?}");
    };
    assert_eq!(
        plan.destination,
        Some(ReceiveChoice {
            root: "inbox".into(),
            path: std::path::PathBuf::from("report.txt"),
        })
    );

    gateway
        .disconnect(&connected.agent_id, None)
        .await
        .expect("disconnect");
}

#[tokio::test]
async fn several_receive_roots_ask_where_the_file_goes_and_then_accept_the_answer() {
    let scratch = tempfile::tempdir().expect("scratch");
    let fake = FakeErgo::spawn().await;
    let gateway = Arc::new(Gateway::new(config_with_receive_roots(
        &fake,
        scratch.path(),
        &["inbox", "archive"],
    )));
    let service = IrcMcpService::new(gateway.clone());
    let connected = gateway
        .connect(connect_request("Clio"))
        .await
        .expect("connect");
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("peer listener");
    let offer = offered_transfer(
        &gateway,
        &connected.agent_id,
        listener.local_addr().expect("peer address"),
        16,
        "report.txt",
    )
    .await;
    let input = accept_input(&connected.agent_id, &offer.id, None, None);

    let resolution = service
        .plan_dcc_accept(
            &OwnerId::local(),
            &declaring_elicitation(true),
            &input,
            None,
            None,
        )
        .await
        .expect("plan");
    let DccAcceptResolution::NeedsInput(request) = resolution else {
        panic!("an unresolvable root must be asked about: {resolution:?}");
    };

    // The exact wire shape matters: this is what a client has to render.
    let wire = serde_json::to_value(&request).expect("serialize");
    assert_eq!(wire["resultType"], "input_required");
    let elicitation = &wire["inputRequests"][DESTINATION_INPUT];
    assert_eq!(elicitation["method"], "elicitation/create");
    assert_eq!(elicitation["params"]["mode"], "form");
    let schema = &elicitation["params"]["requestedSchema"];
    assert_eq!(
        schema["properties"]["root"]["enum"],
        serde_json::json!(["inbox", "archive"]),
        "the form must offer exactly the configured roots, in configured order"
    );
    assert_eq!(schema["required"], serde_json::json!(["root"]));
    assert_eq!(
        schema["properties"]["destination_path"]["default"],
        "report.txt"
    );
    let sealed = request.request_state.clone().expect("sealed request state");
    assert!(
        !sealed.contains("archive"),
        "request state must be opaque to the client: {sealed}"
    );

    // The retry re-sends the original call plus the answer and the state.
    let resolution = service
        .plan_dcc_accept(
            &OwnerId::local(),
            &declaring_elicitation(true),
            &input,
            Some(&sealed),
            Some(&answered(Some("archive"), Some("august/report.txt"))),
        )
        .await
        .expect("plan the retry");
    let DccAcceptResolution::Ready(plan) = resolution else {
        panic!("an answered choice must resolve: {resolution:?}");
    };
    assert_eq!(
        plan.destination,
        Some(ReceiveChoice {
            root: "archive".into(),
            path: std::path::PathBuf::from("august/report.txt"),
        })
    );

    // An answer that names only a root takes the offered filename.
    let resolution = service
        .plan_dcc_accept(
            &OwnerId::local(),
            &declaring_elicitation(true),
            &input,
            Some(&sealed),
            Some(&answered(Some("inbox"), None)),
        )
        .await
        .expect("plan the sparse retry");
    let DccAcceptResolution::Ready(plan) = resolution else {
        panic!("a root alone must resolve: {resolution:?}");
    };
    assert_eq!(
        plan.destination,
        Some(ReceiveChoice {
            root: "inbox".into(),
            path: std::path::PathBuf::from("report.txt"),
        })
    );

    // A retry that echoes the state but answers nothing asks again rather than
    // failing, which is what the specification requires of a partial response.
    let resolution = service
        .plan_dcc_accept(
            &OwnerId::local(),
            &declaring_elicitation(true),
            &input,
            Some(&sealed),
            None,
        )
        .await
        .expect("plan the empty retry");
    assert!(matches!(resolution, DccAcceptResolution::NeedsInput(_)));

    // A session that can no longer be accepted is never asked about: the
    // lifecycle refusal is the only answer worth giving.
    gateway
        .dcc_reject(&connected.agent_id, offer.id.clone())
        .await
        .expect("reject");
    let resolution = service
        .plan_dcc_accept(
            &OwnerId::local(),
            &declaring_elicitation(true),
            &input,
            None,
            None,
        )
        .await
        .expect("plan a retired offer");
    assert!(matches!(resolution, DccAcceptResolution::Ready(_)));
    let refused = gateway
        .dcc_accept(
            &connected.agent_id,
            offer.id.clone(),
            None,
            DestinationConflict::Fail,
        )
        .await
        .expect_err("a rejected offer cannot be accepted");
    assert!(
        refused.to_string().contains("not an incoming offer"),
        "{refused}"
    );

    gateway
        .disconnect(&connected.agent_id, None)
        .await
        .expect("disconnect");
}

#[tokio::test]
async fn a_caller_that_cannot_answer_a_form_is_told_which_roots_exist() {
    let scratch = tempfile::tempdir().expect("scratch");
    let fake = FakeErgo::spawn().await;
    let gateway = Arc::new(Gateway::new(config_with_receive_roots(
        &fake,
        scratch.path(),
        &["inbox", "archive"],
    )));
    let service = IrcMcpService::new(gateway.clone());
    let connected = gateway
        .connect(connect_request("Thalia"))
        .await
        .expect("connect");
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("peer listener");
    let offer = offered_transfer(
        &gateway,
        &connected.agent_id,
        listener.local_addr().expect("peer address"),
        16,
        "report.txt",
    )
    .await;

    let resolution = service
        .plan_dcc_accept(
            &OwnerId::local(),
            &declaring_elicitation(false),
            &accept_input(&connected.agent_id, &offer.id, None, None),
            None,
            None,
        )
        .await
        .expect("plan");
    let DccAcceptResolution::Settled(result) = resolution else {
        panic!("a client with no way to answer must be refused: {resolution:?}");
    };
    assert_eq!(result.is_error, Some(true));
    let structured = result.structured_content.expect("structured error")["error"].clone();
    assert_eq!(
        structured["receive_roots"],
        serde_json::json!(["inbox", "archive"]),
        "the refusal has to carry the whole choice so the retry can be explicit"
    );
    assert_eq!(structured["default_destination_path"], "report.txt");

    // An absolute destination is refused the same way whoever is asking, because
    // the root name is the only thing that carries filesystem authority.
    let absolute = if cfg!(windows) {
        r"C:\elsewhere\report.txt"
    } else {
        "/elsewhere/report.txt"
    };
    let resolution = service
        .plan_dcc_accept(
            &OwnerId::local(),
            &declaring_elicitation(true),
            &accept_input(
                &connected.agent_id,
                &offer.id,
                Some("inbox"),
                Some(absolute),
            ),
            None,
            None,
        )
        .await
        .expect("plan");
    let DccAcceptResolution::Settled(result) = resolution else {
        panic!("an absolute destination must be refused: {resolution:?}");
    };
    assert_eq!(result.is_error, Some(true));
    assert!(
        result.structured_content.expect("structured error")["error"]["message"]
            .as_str()
            .expect("message")
            .contains("must be relative")
    );

    // Nothing above consumed the offer.
    assert_eq!(
        gateway
            .dcc_session(&connected.agent_id, &offer.id)
            .await
            .expect("session")
            .state,
        DccState::Offered
    );

    gateway
        .disconnect(&connected.agent_id, None)
        .await
        .expect("disconnect");
}

#[tokio::test]
async fn a_request_state_cannot_be_redeemed_by_another_caller_or_against_another_offer() {
    let scratch = tempfile::tempdir().expect("scratch");
    let fake = FakeErgo::spawn().await;
    let gateway = Arc::new(Gateway::new(config_with_receive_roots(
        &fake,
        scratch.path(),
        &["inbox", "archive"],
    )));
    let service = IrcMcpService::new(gateway.clone());
    let connected = gateway
        .connect(connect_request("Nemesis"))
        .await
        .expect("connect");
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("peer listener");
    let endpoint = listener.local_addr().expect("peer address");
    let first = offered_transfer(&gateway, &connected.agent_id, endpoint, 16, "first.txt").await;
    let mine = OwnerId::from_bearer("alice");

    let DccAcceptResolution::NeedsInput(request) = service
        .plan_dcc_accept(
            &mine,
            &declaring_elicitation(true),
            &accept_input(&connected.agent_id, &first.id, None, None),
            None,
            None,
        )
        .await
        .expect("plan")
    else {
        panic!("expected a destination question");
    };
    let sealed = request.request_state.expect("sealed request state");
    let answer = answered(Some("archive"), None);

    // A second offer from the same peer, so only the sealed binding separates
    // the two exchanges.
    let second = offered_transfer(&gateway, &connected.agent_id, endpoint, 16, "second.txt").await;

    for (label, owner, state, target) in [
        (
            "another caller",
            OwnerId::from_bearer("mallory"),
            sealed.clone(),
            &first,
        ),
        (
            "the shared local owner",
            OwnerId::local(),
            sealed.clone(),
            &first,
        ),
        (
            "a tampered state",
            mine.clone(),
            format!("{sealed}x"),
            &first,
        ),
        ("a truncated state", mine.clone(), "rs1.".to_owned(), &first),
        (
            "a state minted for another offer",
            mine.clone(),
            sealed.clone(),
            &second,
        ),
    ] {
        let resolution = service
            .plan_dcc_accept(
                &owner,
                &declaring_elicitation(true),
                &accept_input(&connected.agent_id, &target.id, None, None),
                Some(&state),
                Some(&answer),
            )
            .await
            .expect("a refused state is an in-band result, never a protocol error");
        let DccAcceptResolution::Settled(result) = resolution else {
            panic!("{label} must be refused deterministically: {resolution:?}");
        };
        assert_eq!(result.is_error, Some(true), "{label}");
        // Every offer survives every refusal; only its own expiry retires it.
        for offer in [&first, &second] {
            assert_eq!(
                gateway
                    .dcc_session(&connected.agent_id, &offer.id)
                    .await
                    .expect("session")
                    .state,
                DccState::Offered,
                "{label}"
            );
        }
    }

    gateway
        .disconnect(&connected.agent_id, None)
        .await
        .expect("disconnect");
}

#[tokio::test]
async fn declining_the_destination_question_leaves_the_offer_pending() {
    let scratch = tempfile::tempdir().expect("scratch");
    let fake = FakeErgo::spawn().await;
    let gateway = Arc::new(Gateway::new(config_with_receive_roots(
        &fake,
        scratch.path(),
        &["inbox", "archive"],
    )));
    let service = IrcMcpService::new(gateway.clone());
    let connected = gateway
        .connect(connect_request("Eirene"))
        .await
        .expect("connect");
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("peer listener");
    let offer = offered_transfer(
        &gateway,
        &connected.agent_id,
        listener.local_addr().expect("peer address"),
        16,
        "report.txt",
    )
    .await;
    let input = accept_input(&connected.agent_id, &offer.id, None, None);
    let DccAcceptResolution::NeedsInput(request) = service
        .plan_dcc_accept(
            &OwnerId::local(),
            &declaring_elicitation(true),
            &input,
            None,
            None,
        )
        .await
        .expect("plan")
    else {
        panic!("expected a destination question");
    };
    let sealed = request.request_state.expect("sealed request state");

    for action in ["decline", "cancel"] {
        let resolution = service
            .plan_dcc_accept(
                &OwnerId::local(),
                &declaring_elicitation(true),
                &input,
                Some(&sealed),
                Some(&rmcp::model::InputResponses::from([(
                    DESTINATION_INPUT.to_owned(),
                    serde_json::json!({ "action": action }),
                )])),
            )
            .await
            .expect("plan");
        let DccAcceptResolution::Settled(result) = resolution else {
            panic!("a refusal is terminal, not another round: {resolution:?}");
        };
        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect("structured error")["error"].clone();
        assert_eq!(structured["kind"], "declined");
        assert!(
            structured["message"]
                .as_str()
                .expect("message")
                .contains("still pending")
        );
        assert_eq!(
            gateway
                .dcc_session(&connected.agent_id, &offer.id)
                .await
                .expect("session")
                .state,
            DccState::Offered,
            "declining writes nothing and consumes nothing"
        );
    }

    gateway
        .disconnect(&connected.agent_id, None)
        .await
        .expect("disconnect");
}
