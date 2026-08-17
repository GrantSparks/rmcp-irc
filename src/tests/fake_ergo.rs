//! A deterministic in-process IRC server the whole test suite can connect to.
//!
//! Registration is a real multi-round-trip exchange — capability negotiation,
//! nickname arbitration, welcome, MOTD — so no test that cares what happens
//! during a connect can substitute a stub for it. This fixture speaks enough of
//! the protocol to drive that exchange to completion, over a real loopback
//! socket, without a server process or a network.
//!
//! It lives in its own module because two very different kinds of test need the
//! same server: the gateway tests, which drive the actor directly, and the
//! transport tests, which need a connected agent before an MCP request can name
//! a handle at all.

use std::{
    collections::HashSet,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::Bytes;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{Mutex, broadcast},
};

use crate::{
    config::{Config, IrcTransport},
    irc::wire::WireMessage,
};

/// Channel this server protects with a key, so a join without one collects the
/// real `ERR_BADCHANNELKEY` an MCP caller has to answer.
pub(super) const KEYED_CHANNEL: &str = "#locked";

/// The key [`KEYED_CHANNEL`] accepts. Anything else is refused identically to
/// no key at all, which is what makes "wrong key" and "missing key"
/// distinguishable only by what the call itself carried.
pub(super) const CHANNEL_KEY: &str = "sesame";

/// SASL identity this server accepts.
pub(super) const SASL_USERNAME: &str = "ariadne";

/// Environment variable the accepted SASL password is read from, by the server
/// here and by the configuration under test alike.
///
/// A configured SASL password is only ever a reference to an environment
/// variable, and exporting one from inside a test is `unsafe` in this edition,
/// which this crate forbids. So the fixture agrees with the configuration on a
/// variable every process already has rather than on one neither can set.
pub(super) const SASL_PASSWORD_ENV: &str = "PATH";

/// The password [`SASL_PASSWORD_ENV`] currently names.
pub(super) fn sasl_password() -> String {
    std::env::var(SASL_PASSWORD_ENV).unwrap_or_default()
}

/// The SASL configuration this server authenticates.
pub(super) fn sasl_config() -> crate::config::SaslConfig {
    crate::config::SaslConfig {
        username: SASL_USERNAME.to_owned(),
        password: crate::config::CredentialRef {
            env: SASL_PASSWORD_ENV.to_owned(),
        },
    }
}

pub(super) struct FakeErgo {
    pub(super) address: SocketAddr,
    pub(super) client_lines: broadcast::Sender<String>,
    task: tokio::task::JoinHandle<()>,
}

impl FakeErgo {
    pub(super) async fn spawn() -> Self {
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

    pub(super) fn config(&self) -> Config {
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
    // Base64 collected from `AUTHENTICATE` lines, once a mechanism was chosen.
    let mut sasl_payload = None::<String>;
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
                    ":fake CAP * LS :batch cap-notify draft/chathistory draft/message-redaction draft/read-marker echo-message labeled-response message-tags sasl=PLAIN server-time standard-replies",
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
            // SASL PLAIN, in the shape a client actually performs it: the
            // mechanism, an empty challenge, then the payload in as many
            // chunks as it takes. Nothing about the connect sequence is
            // observable without a server that completes this exchange.
            "AUTHENTICATE" => {
                let argument = message
                    .params
                    .first()
                    .map(String::as_str)
                    .or(message.trailing.as_deref())
                    .unwrap_or_default();
                let target = nickname.as_deref().unwrap_or("*");
                if argument.eq_ignore_ascii_case("PLAIN") {
                    sasl_payload = Some(String::new());
                    write_line(&mut writer, "AUTHENTICATE +").await;
                    continue;
                }
                let Some(payload) = sasl_payload.as_mut() else {
                    continue;
                };
                // A final chunk is short, and an exactly-full payload is
                // terminated by a lone `+`.
                let complete = argument.len() < 400 || argument == "+";
                if argument != "+" {
                    payload.push_str(argument);
                }
                if !complete {
                    continue;
                }
                let accepted = STANDARD.decode(payload.as_bytes()).is_ok_and(|decoded| {
                    decoded == format!("\0{SASL_USERNAME}\0{}", sasl_password()).as_bytes()
                });
                sasl_payload = None;
                if accepted {
                    write_line(
                        &mut writer,
                        &format!(":fake 903 {target} :SASL authentication successful"),
                    )
                    .await;
                } else {
                    write_line(
                        &mut writer,
                        &format!(":fake 904 {target} :SASL authentication failed"),
                    )
                    .await;
                }
            }
            "NICK" => {
                let candidate = message.params.first().cloned().unwrap_or_default();
                let mut occupied = nicknames.lock().await;
                let collision = occupied.contains(&candidate);
                let previous = if collision {
                    None
                } else {
                    let previous = nickname.replace(candidate.clone());
                    if let Some(previous) = previous.as_ref() {
                        occupied.remove(previous);
                    }
                    occupied.insert(candidate.clone());
                    previous
                };
                drop(occupied);
                if collision {
                    write_line(
                        &mut writer,
                        &format!(":fake 433 * {candidate} :Nickname is already in use"),
                    )
                    .await;
                } else if welcomed {
                    // A rename after registration is echoed, which is the only
                    // acknowledgement a client gets for one.
                    let tag = label_prefix(&message);
                    write_line(
                        &mut writer,
                        &format!(
                            "{tag}:{}!guest@localhost NICK :{candidate}",
                            previous.as_deref().unwrap_or("guest")
                        ),
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
                // Labelled, so a correlated `irc.help` completes rather than
                // waiting out its deadline on replies it cannot attribute.
                let tag = label_prefix(&message);
                write_line(
                    &mut writer,
                    &format!("{tag}:fake 704 {target} {subject} :WHOIS JOIN HISTORY"),
                )
                .await;
                write_line(
                    &mut writer,
                    &format!("{tag}:fake 706 {target} {subject} :End of HELP"),
                )
                .await;
            }
            "PART" => {
                let channel = message.params.first().map_or("#test", String::as_str);
                let reason = message.trailing.as_deref().unwrap_or("");
                let tag = label_prefix(&message);
                write_line(
                    &mut writer,
                    &format!(
                        "{tag}:{}!guest@localhost PART {channel} :{reason}",
                        nickname.as_deref().unwrap_or("guest")
                    ),
                )
                .await;
            }
            "INVITE" => {
                let target = nickname.as_deref().unwrap_or("*");
                let invited = message.params.first().map_or("peer", String::as_str);
                let channel = message.params.get(1).map_or("#test", String::as_str);
                let tag = label_prefix(&message);
                write_line(
                    &mut writer,
                    &format!("{tag}:fake 341 {target} {invited} {channel}"),
                )
                .await;
            }
            "AWAY" => {
                let target = nickname.as_deref().unwrap_or("*");
                let numeric = if message.trailing.is_some() { 306 } else { 305 };
                let tag = label_prefix(&message);
                write_line(
                    &mut writer,
                    &format!("{tag}:fake {numeric} {target} :You are no longer away"),
                )
                .await;
            }
            "LIST" => {
                let target = nickname.as_deref().unwrap_or("*");
                let tag = label_prefix(&message);
                write_line(
                    &mut writer,
                    &format!("{tag}:fake 322 {target} #agents 1 :Coordinate here"),
                )
                .await;
                write_line(
                    &mut writer,
                    &format!("{tag}:fake 323 {target} :End of LIST"),
                )
                .await;
            }
            // A tag-only message carries no text, so the echo is the only thing
            // a typing state or a reaction can be acknowledged by.
            "TAGMSG" => {
                let target = message.params.first().map_or("#test", String::as_str);
                let tags = message
                    .tag_value("label")
                    .map_or_else(String::new, |label| format!("@label={label} "));
                write_line(
                    &mut writer,
                    &format!(
                        "{tags}:{}!guest@localhost TAGMSG {target}",
                        nickname.as_deref().unwrap_or("guest")
                    ),
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
                let target = nickname.as_deref().unwrap_or("guest");
                // One keyed channel, refused with the numeric a real server
                // sends, so the key round trip has something authentic to fail
                // against before it succeeds.
                if channel.eq_ignore_ascii_case(KEYED_CHANNEL)
                    && message.params.get(1).map(String::as_str) != Some(CHANNEL_KEY)
                {
                    write_line(
                        &mut writer,
                        &format!("{tag}:fake 475 {target} {channel} :Cannot join channel (+k)"),
                    )
                    .await;
                } else {
                    write_line(
                        &mut writer,
                        &format!("{tag}:{target}!guest@localhost JOIN {channel}"),
                    )
                    .await;
                }
            }
            "KICK" => {
                let channel = message.params.first().map_or("#test", String::as_str);
                let removed = message.params.get(1).map_or("someone", String::as_str);
                let reason = message.trailing.as_deref().unwrap_or("");
                let tag = label_prefix(&message);
                write_line(
                    &mut writer,
                    &format!(
                        "{tag}:{}!guest@localhost KICK {channel} {removed} :{reason}",
                        nickname.as_deref().unwrap_or("guest")
                    ),
                )
                .await;
            }
            // A server that owns the marker: a query echoes what it holds and a
            // set echoes the timestamp it accepted, which is the only value a
            // caller may report as the marker.
            "MARKREAD" => {
                let target = message.params.first().map_or("#test", String::as_str);
                let timestamp = message
                    .params
                    .get(1)
                    .map_or("timestamp=2026-08-17T00:00:00.000Z", String::as_str);
                let tag = label_prefix(&message);
                write_line(
                    &mut writer,
                    &format!("{tag}:fake MARKREAD {target} {timestamp}"),
                )
                .await;
            }
            "REDACT" => {
                let target = message.params.first().map_or("#test", String::as_str);
                let message_id = message.params.get(1).map_or("msgid", String::as_str);
                let reason = message.trailing.as_deref().unwrap_or("");
                let tag = label_prefix(&message);
                write_line(
                    &mut writer,
                    &format!(
                        "{tag}:{}!guest@localhost REDACT {target} {message_id} :{reason}",
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
            "TOPIC" if message.trailing.is_none() => {
                let target = nickname.as_deref().unwrap_or("*");
                let channel = message.params.first().map_or("#test", String::as_str);
                let tag = label_prefix(&message);
                write_line(
                    &mut writer,
                    &format!("{tag}:fake 332 {target} {channel} :Coordinate here"),
                )
                .await;
                write_line(
                    &mut writer,
                    &format!("{tag}:fake 333 {target} {channel} guest 1700000001"),
                )
                .await;
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
            "MODE" if message.params.len() < 2 => {
                let target = nickname.as_deref().unwrap_or("*");
                let channel = message.params.first().map_or("#test", String::as_str);
                let tag = label_prefix(&message);
                write_line(
                    &mut writer,
                    &format!("{tag}:fake 324 {target} {channel} +nt"),
                )
                .await;
                write_line(
                    &mut writer,
                    &format!("{tag}:fake 329 {target} {channel} 1700000000"),
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
            // Push an inbound CTCP DCC SEND offer from a peer, which is the
            // only way a real gateway learns of one and therefore the only
            // honest way to exercise acceptance end to end. Parameters are the
            // encoded address, port, size, and filename the offer advertises.
            "TESTDCCSEND" => {
                let target = nickname.as_deref().unwrap_or("guest");
                let address = message.params.first().map_or("0", String::as_str);
                let port = message.params.get(1).map_or("0", String::as_str);
                let size = message.params.get(2).map_or("0", String::as_str);
                let filename = message.params.get(3).map_or("offered.bin", String::as_str);
                write_line(
                    &mut writer,
                    &format!(
                        ":peer!u@h PRIVMSG {target} :\u{1}DCC SEND {filename} {address} {port} \
                         {size}\u{1}"
                    ),
                )
                .await;
            }
            // Deliver one arbitrary line to this client, so a test can make a
            // peer say something without opening a second connection and
            // without the fixture having to know why.
            "TESTLINE" => {
                if let Some(line) = message.trailing.as_deref() {
                    write_line(&mut writer, line).await;
                }
            }
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
        &format!(
            ":fake 005 {nickname} CASEMAPPING=ascii NICKLEN=30 LINELEN=2048 MONITOR=100 :supported"
        ),
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

/// Write one line, tolerating a client that has already gone.
///
/// A guest that abandons its registration — the caller asking about a refused
/// nickname rather than accepting a substitute — closes the socket in the middle
/// of the exchange. That is ordinary client behavior, not a fixture failure, so
/// the half-written reply is dropped rather than turned into a panic in a
/// background task.
async fn write_line(writer: &mut tokio::net::tcp::OwnedWriteHalf, line: &str) {
    if writer.write_all(line.as_bytes()).await.is_err() || writer.write_all(b"\r\n").await.is_err()
    {
        return;
    }
    let _ = writer.flush().await;
}

fn label_prefix(message: &WireMessage) -> String {
    message
        .tag_value("label")
        .map_or_else(String::new, |label| format!("@label={label} "))
}
