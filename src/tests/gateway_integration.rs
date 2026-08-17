//! End-to-end gateway tests against a deterministic in-process Ergo fixture.

use std::{
    collections::{BTreeSet, HashSet},
    net::SocketAddr,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use crate::{
    agent::{
        actor::CompletionMode,
        journal::{EventClass, EventDirection, EventFilter},
    },
    dcc::negotiation::{CtcpMessage, DccOffer, parse_address},
    gateway::{ConnectRequest, Gateway},
    irc::{
        correlation::CommandOutcome,
        registration::{NickConflictPolicy, Nickname},
        wire::{OutboundMessage, WireMessage},
    },
    mcp::{authorization::OwnerId, watch::WatchFilter},
    tests::fake_ergo::FakeErgo,
};
use bytes::Bytes;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
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
