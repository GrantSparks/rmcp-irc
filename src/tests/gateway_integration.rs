//! End-to-end gateway tests against a deterministic in-process Ergo fixture.

use std::{
    collections::{BTreeSet, HashSet},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use crate::{
    agent::{
        actor::CompletionMode,
        journal::{EventClass, EventDirection, EventFilter},
    },
    config::{Config, IrcTransport},
    dcc::negotiation::{CtcpMessage, DccOffer, parse_address},
    gateway::{ConnectRequest, Gateway},
    irc::{
        correlation::CommandOutcome,
        registration::{NickConflictPolicy, Nickname},
        wire::{OutboundMessage, WireMessage},
    },
    mcp::watch::WatchFilter,
};
use bytes::Bytes;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{Mutex, broadcast},
    task::JoinSet,
};

struct FakeErgo {
    address: SocketAddr,
    client_lines: broadcast::Sender<String>,
    task: tokio::task::JoinHandle<()>,
}

impl FakeErgo {
    async fn spawn() -> Self {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind fake Ergo");
        let address = listener.local_addr().expect("fake address");
        let nicknames = Arc::new(Mutex::new(HashSet::new()));
        let (client_lines, _) = broadcast::channel(4_096);
        let publisher = client_lines.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let nicknames = nicknames.clone();
                let publisher = publisher.clone();
                tokio::spawn(async move {
                    serve_client(stream, nicknames, publisher).await;
                });
            }
        });
        Self {
            address,
            client_lines,
            task,
        }
    }

    fn config(&self) -> Config {
        let mut config = Config::default();
        config.irc.host = self.address.ip().to_string();
        config.irc.port = self.address.port();
        config.irc.transport = IrcTransport::Plain;
        config.onboarding.connect_timeout_ms = 5_000;
        config.onboarding.nickname_attempts = 32;
        config.reconnect.initial_delay_ms = 10;
        config.reconnect.max_delay_ms = 50;
        config.reconnect.jitter = 0.0;
        config.limits.max_agents = 32;
        config.dcc.bind_address = IpAddr::V4(Ipv4Addr::LOCALHOST);
        config.dcc.advertised_address = Some(IpAddr::V4(Ipv4Addr::LOCALHOST));
        config.dcc.port_start = 49_152;
        config.dcc.port_end = 65_000;
        config
    }
}

impl Drop for FakeErgo {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve_client(
    stream: TcpStream,
    nicknames: Arc<Mutex<HashSet<String>>>,
    client_lines: broadcast::Sender<String>,
) {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    let mut nickname = None::<String>;
    let mut user_seen = false;
    let mut cap_end_seen = false;
    let mut welcomed = false;
    loop {
        line.clear();
        let Ok(read) = reader.read_line(&mut line).await else {
            break;
        };
        if read == 0 {
            break;
        }
        let raw = line.trim_end_matches(['\r', '\n']).to_owned();
        let _ = client_lines.send(raw.clone());
        let Ok(message) = WireMessage::parse(Bytes::copy_from_slice(raw.as_bytes())) else {
            continue;
        };
        let command = message.command.to_ascii_uppercase();
        match command.as_str() {
            "CAP" if message.params.first().is_some_and(|value| value == "LS") => {
                write_line(
                    &mut writer,
                    ":fake CAP * LS :batch cap-notify draft/chathistory echo-message labeled-response message-tags server-time standard-replies",
                )
                .await;
            }
            "CAP" if message.params.first().is_some_and(|value| value == "REQ") => {
                let requested = message.trailing.as_deref().unwrap_or_default();
                write_line(
                    &mut writer,
                    &format!(
                        ":fake CAP {} ACK :{requested}",
                        nickname.as_deref().unwrap_or("*")
                    ),
                )
                .await;
            }
            "CAP" if message.params.first().is_some_and(|value| value == "END") => {
                cap_end_seen = true;
                maybe_welcome(
                    &mut writer,
                    nickname.as_deref(),
                    user_seen,
                    cap_end_seen,
                    &mut welcomed,
                )
                .await;
            }
            "NICK" => {
                let candidate = message.params.first().cloned().unwrap_or_default();
                let mut occupied = nicknames.lock().await;
                let collision = occupied.contains(&candidate);
                if !collision {
                    if let Some(previous) = nickname.replace(candidate.clone()) {
                        occupied.remove(&previous);
                    }
                    occupied.insert(candidate.clone());
                }
                drop(occupied);
                if collision {
                    write_line(
                        &mut writer,
                        &format!(":fake 433 * {candidate} :Nickname is already in use"),
                    )
                    .await;
                } else {
                    maybe_welcome(
                        &mut writer,
                        nickname.as_deref(),
                        user_seen,
                        cap_end_seen,
                        &mut welcomed,
                    )
                    .await;
                }
            }
            "USER" => {
                user_seen = true;
                maybe_welcome(
                    &mut writer,
                    nickname.as_deref(),
                    user_seen,
                    cap_end_seen,
                    &mut welcomed,
                )
                .await;
            }
            "HELP" => {
                let target = nickname.as_deref().unwrap_or("*");
                let subject = message.params.first().map_or("INDEX", String::as_str);
                write_line(
                    &mut writer,
                    &format!(":fake 704 {target} {subject} :WHOIS JOIN HISTORY"),
                )
                .await;
                write_line(
                    &mut writer,
                    &format!(":fake 706 {target} {subject} :End of HELP"),
                )
                .await;
            }
            "WHOIS" => {
                let target = nickname.as_deref().unwrap_or("*");
                let subject = message.params.first().map_or("someone", String::as_str);
                let tag = label_prefix(&message);
                write_line(
                    &mut writer,
                    &format!("{tag}:fake 311 {target} {subject} user host * :Test User"),
                )
                .await;
                write_line(
                    &mut writer,
                    &format!("{tag}:fake 318 {target} {subject} :End of WHOIS"),
                )
                .await;
            }
            "NAMES" => {
                let target = nickname.as_deref().unwrap_or("*");
                let channel = message.params.first().map_or("#test", String::as_str);
                let label = message.tag_value("label").unwrap_or("names");
                write_line(
                    &mut writer,
                    &format!("@label={label} :fake BATCH +{label} labeled-response"),
                )
                .await;
                write_line(
                    &mut writer,
                    &format!("@batch={label} :fake 353 {target} = {channel} :{target}"),
                )
                .await;
                write_line(
                    &mut writer,
                    &format!("@batch={label} :fake 366 {target} {channel} :End of NAMES list"),
                )
                .await;
                write_line(&mut writer, &format!(":fake BATCH -{label}")).await;
            }
            "JOIN" => {
                let channel = message.params.first().map_or("#test", String::as_str);
                let tag = label_prefix(&message);
                write_line(
                    &mut writer,
                    &format!(
                        "{tag}:{}!guest@localhost JOIN {channel}",
                        nickname.as_deref().unwrap_or("guest")
                    ),
                )
                .await;
            }
            "PRIVMSG" => {
                let target = message.params.first().map_or("#test", String::as_str);
                let text = message.trailing.as_deref().unwrap_or_default();
                let label = message
                    .tag_value("label")
                    .map(|label| format!("label={label};"))
                    .unwrap_or_default();
                let tags = format!(
                    "@{label}time=2026-08-17T00:00:01Z;msgid=echo-{} ",
                    nickname.as_deref().unwrap_or("guest")
                );
                write_line(
                    &mut writer,
                    &format!(
                        "{tags}:{}!guest@localhost PRIVMSG {target} :{text}",
                        nickname.as_deref().unwrap_or("guest")
                    ),
                )
                .await;
            }
            "MOTD" => {
                let target = nickname.as_deref().unwrap_or("*");
                let tag = label_prefix(&message);
                write_line(
                    &mut writer,
                    &format!("{tag}:fake 375 {target} :- refreshed -"),
                )
                .await;
                write_line(
                    &mut writer,
                    &format!("{tag}:fake 372 {target} :Fresh instructions."),
                )
                .await;
                write_line(
                    &mut writer,
                    &format!("{tag}:fake 376 {target} :End of MOTD"),
                )
                .await;
            }
            "TIME" => {
                let target = nickname.as_deref().unwrap_or("*");
                let tag = label_prefix(&message);
                write_line(
                    &mut writer,
                    &format!("{tag}:fake 391 {target} fake :Mon, 17 Aug 2026 05:08:34 UTC"),
                )
                .await;
            }
            "VERSION" => {
                let target = nickname.as_deref().unwrap_or("*");
                let label = message.tag_value("label").unwrap_or("version");
                write_line(
                    &mut writer,
                    &format!("@label={label} :fake BATCH +{label} labeled-response"),
                )
                .await;
                write_line(
                    &mut writer,
                    &format!("@batch={label} :fake 351 {target} ergo-test fake"),
                )
                .await;
                write_line(
                    &mut writer,
                    &format!("@batch={label} :fake 005 {target} NICKLEN=30 :supported"),
                )
                .await;
                write_line(&mut writer, &format!(":fake BATCH -{label}")).await;
            }
            "TOPIC" if message.trailing.is_some() => {
                let channel = message.params.first().map_or("#test", String::as_str);
                let text = message.trailing.as_deref().unwrap_or_default();
                let tag = label_prefix(&message);
                write_line(
                    &mut writer,
                    &format!(
                        "{tag}:{}!guest@localhost TOPIC {channel} :{text}",
                        nickname.as_deref().unwrap_or("guest")
                    ),
                )
                .await;
            }
            "MODE"
                if message
                    .params
                    .get(1)
                    .is_some_and(|modes| modes.starts_with(['+', '-'])) =>
            {
                let target = message.params.first().map_or("#test", String::as_str);
                let modes = &message.params[1];
                let tag = label_prefix(&message);
                write_line(
                    &mut writer,
                    &format!(
                        "{tag}:{}!guest@localhost MODE {target} {modes}",
                        nickname.as_deref().unwrap_or("guest")
                    ),
                )
                .await;
            }
            "MONITOR" => {
                let target = nickname.as_deref().unwrap_or("*");
                let tag = label_prefix(&message);
                let operation = message.params.first().map_or("", String::as_str);
                if operation == "+" {
                    let monitored = message.params.get(1).map_or("peer", String::as_str);
                    write_line(
                        &mut writer,
                        &format!("{tag}:fake 730 {target} :{monitored}!u@h"),
                    )
                    .await;
                } else {
                    write_line(&mut writer, &format!("{tag}:fake ACK")).await;
                }
            }
            "CHATHISTORY" => {
                let target = message.params.get(1).map_or("#test", String::as_str);
                let label = message.tag_value("label").unwrap_or("history");
                let recovery = message.params.first().is_some_and(|mode| mode == "AFTER");
                let opening = if recovery {
                    format!("@label={label} :fake BATCH +{label} chathistory {target}")
                } else {
                    format!("@label={label} :fake BATCH +{label} :chathistory")
                };
                write_line(&mut writer, &opening).await;
                if recovery {
                    let inner = format!("{label}-inner");
                    write_line(
                        &mut writer,
                        &format!("@batch={label} :fake BATCH +{inner} multiline"),
                    )
                    .await;
                    write_line(
                        &mut writer,
                        &format!("@batch={inner};time=2026-08-17T00:00:00Z;msgid=echo-{} :old!u@h PRIVMSG {target} :checkpoint", nickname.as_deref().unwrap_or("guest")),
                    )
                    .await;
                    write_line(
                        &mut writer,
                        &format!("@batch={inner};time=2026-08-17T00:00:01Z;msgid=reconnect-1 :old!u@h PRIVMSG {target} :missed while disconnected"),
                    )
                    .await;
                    write_line(&mut writer, &format!("@batch={label} :fake BATCH -{inner}")).await;
                } else {
                    let message_id = if target == "#agents" {
                        "history-1".to_owned()
                    } else {
                        format!("history-{}", target.trim_start_matches('#'))
                    };
                    write_line(
                        &mut writer,
                        &format!("@batch={label};time=2026-08-17T00:00:00Z;msgid={message_id} :old!u@h PRIVMSG {target} :historic"),
                    )
                    .await;
                }
                write_line(&mut writer, &format!("@batch={label} :fake BATCH -{label}")).await;
            }
            "DROPME" => break,
            "TESTCTCP" => {
                let target = nickname.as_deref().unwrap_or("guest");
                write_line(
                    &mut writer,
                    &format!(":peer!u@h PRIVMSG {target} :\u{1}CLIENTINFO\u{1}"),
                )
                .await;
            }
            "QUIT" => break,
            _ => {}
        }
    }
    if let Some(nickname) = nickname {
        nicknames.lock().await.remove(&nickname);
    }
}

async fn maybe_welcome(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    nickname: Option<&str>,
    user_seen: bool,
    cap_end_seen: bool,
    welcomed: &mut bool,
) {
    let Some(nickname) = nickname else {
        return;
    };
    if !user_seen || !cap_end_seen || *welcomed {
        return;
    }
    *welcomed = true;
    write_line(writer, &format!(":fake 001 {nickname} :Welcome")).await;
    write_line(
        writer,
        &format!(":fake 005 {nickname} CASEMAPPING=ascii NICKLEN=30 LINELEN=2048 :supported"),
    )
    .await;
    write_line(writer, &format!(":fake 375 {nickname} :- fake MOTD -")).await;
    write_line(
        writer,
        &format!(":fake 372 {nickname} :Coordinate clearly."),
    )
    .await;
    write_line(writer, &format!(":fake 376 {nickname} :End of MOTD")).await;
}

async fn write_line(writer: &mut tokio::net::tcp::OwnedWriteHalf, line: &str) {
    writer.write_all(line.as_bytes()).await.expect("fake write");
    writer.write_all(b"\r\n").await.expect("fake CRLF");
    writer.flush().await.expect("fake flush");
}

fn label_prefix(message: &WireMessage) -> String {
    message
        .tag_value("label")
        .map_or_else(String::new, |label| format!("@label={label} "))
}

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
    let gateway = Gateway::new(fake.config());
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

    let reconnected = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let snapshot = gateway
                .snapshot(&connected.agent_id)
                .await
                .expect("reconnect snapshot");
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

#[tokio::test]
async fn a_watch_delivers_only_what_it_selected_and_advances_past_it() {
    let fake = FakeErgo::spawn().await;
    let gateway = Gateway::new(fake.config());
    let connected = gateway
        .connect(connect_request("Ariadne"))
        .await
        .expect("connect");

    let watch = gateway
        .create_watch(
            &connected.agent_id,
            WatchFilter {
                targets: BTreeSet::from(["#agents".to_string()]),
                ..WatchFilter::default()
            },
            None,
        )
        .await
        .expect("create watch");

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

    let first = gateway
        .read_watch(&watch.watch_id)
        .await
        .expect("read watch");
    assert_eq!(first.status, crate::agent::journal::CursorStatus::Current);
    assert!(
        !first.events.is_empty(),
        "a watch on an active channel must deliver something"
    );
    assert!(
        first
            .events
            .iter()
            .all(|event| event.target.as_deref() == Some("#agents")),
        "a watch must not deliver targets it did not select: {:?}",
        first.events
    );
    assert!(
        first
            .events
            .iter()
            .any(|event| event.text.as_deref() == Some("thread through the maze")),
        "the watch dropped the message it was created for"
    );

    // The position lives with the watch, so a second read with no argument
    // continues rather than repeating.
    let second = gateway
        .read_watch(&watch.watch_id)
        .await
        .expect("second read");
    assert!(
        second
            .events
            .iter()
            .all(|event| event.cursor.sequence > first.next_cursor.sequence),
        "a watch re-delivered events its previous read had consumed"
    );

    gateway.close_watch(&watch.watch_id).expect("close watch");
    assert!(
        gateway.read_watch(&watch.watch_id).await.is_err(),
        "a closed watch must not remain readable"
    );

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
    let watch = gateway
        .create_watch(&connected.agent_id, WatchFilter::default(), None)
        .await
        .expect("create watch");

    gateway
        .disconnect(&connected.agent_id, None)
        .await
        .expect("disconnect");

    assert!(
        gateway.watches().describe(&watch.watch_id).is_none(),
        "a watch outlived the stream it was watching"
    );
}
