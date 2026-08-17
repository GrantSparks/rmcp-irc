//! Best-effort state reduced from authoritative IRC traffic.

use std::collections::{BTreeMap, BTreeSet};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::time::Timestamp;

use crate::irc::{
    isupport::{CaseMapping, IsupportRegistry},
    semantic::{MembershipChange, PresenceChange, SemanticEvent, SemanticProjection, Source},
    target::ChannelName,
    wire::WireMessage,
};

use super::{AgentId, journal::EventCursor};

/// Current lifecycle of an upstream IRC connection.
#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionState {
    /// Actor exists but no socket is connected.
    #[default]
    Disconnected,
    /// Establishing TCP or TLS.
    Connecting,
    /// Negotiating capabilities and registering the guest.
    Registering,
    /// Registration and initial MOTD collection completed.
    Ready,
    /// Reconnect backoff or resynchronization is active.
    Reconnecting,
    /// Actor stopped after a terminal failure.
    TerminalError,
}

/// Availability of the latest MOTD.
#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MotdStatus {
    /// No complete MOTD result has been observed.
    #[default]
    NotReceived,
    /// The server sent a complete MOTD.
    Received,
    /// The server returned ERR_NOMOTD.
    NotAvailable,
}

/// Operation that produced the current MOTD.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MotdSource {
    /// Initial registration.
    Initial,
    /// Registration after a network reconnect.
    Reconnect,
    /// Explicit MOTD query.
    Query,
}

/// Latest complete server MOTD representation.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
pub struct MotdState {
    /// Whether a complete MOTD, 422, or neither has been observed.
    pub status: MotdStatus,
    /// Ordered text lines without local interpretation.
    pub lines: Vec<String>,
    /// Joined text presented to MCP clients.
    pub text: String,
    /// Raw 375/372/376 sequence or 422.
    pub wire_replies: Vec<WireMessage>,
    /// Initial registration, reconnect, or explicit query.
    pub source: Option<MotdSource>,
    /// RFC 3339 completion time.
    pub received_at: Option<Timestamp>,
}

/// Best-effort identity and presence information.
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct IdentityState {
    /// Current case-preserved nickname.
    pub nickname: Option<String>,
    /// Authenticated IRC account, if announced.
    pub account: Option<String>,
    /// Current username.
    pub username: Option<String>,
    /// Current hostname.
    pub hostname: Option<String>,
    /// Current real name.
    pub real_name: Option<String>,
    /// User modes known from server state.
    pub modes: BTreeSet<String>,
    /// Away message, or null when present/unknown.
    pub away_message: Option<String>,
}

/// Best-effort information about one channel member.
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct MemberState {
    /// IRC account when known.
    pub account: Option<String>,
    /// Username when known.
    pub username: Option<String>,
    /// Hostname when known.
    pub hostname: Option<String>,
    /// Away state when known.
    pub away: Option<bool>,
    /// Server-advertised membership prefixes.
    pub prefixes: BTreeSet<String>,
}

/// Advisory snapshot for one joined channel.
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct ChannelState {
    /// Case-preserved channel name.
    pub name: String,
    /// Latest topic text.
    pub topic: Option<String>,
    /// Nickname that set the topic, when known.
    pub topic_setter: Option<String>,
    /// Topic timestamp, when known.
    pub topic_set_at: Option<Timestamp>,
    /// Known channel modes and their values.
    pub modes: BTreeMap<String, Option<String>>,
    /// Case-preserved nicknames and known presence data.
    pub members: BTreeMap<String, MemberState>,
}

/// Current reconnect scheduling state.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
pub struct ReconnectState {
    /// Number of consecutive reconnect attempts.
    pub attempt: u32,
    /// Time of the next planned attempt.
    pub next_attempt_at: Option<Timestamp>,
    /// Current deterministic delay before jitter.
    pub delay_ms: Option<u64>,
}

/// Advisory state snapshot owned by one agent actor.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct AgentState {
    /// Routing handle.
    pub agent_id: AgentId,
    /// Connection lifecycle.
    pub connection_state: ConnectionState,
    /// Whether RPL_WELCOME has completed for the active connection.
    pub registered: bool,
    /// RFC 3339 time at which the active connection became ready.
    pub connected_since: Option<Timestamp>,
    /// Own identity and presence.
    pub identity: IdentityState,
    /// Joined channels keyed by canonical comparison key.
    pub channels: BTreeMap<String, ChannelState>,
    /// Monitored nick presence keyed by canonical comparison key.
    pub monitored_presence: BTreeMap<String, bool>,
    /// Latest MOTD.
    pub motd: MotdState,
    /// Current reconnect scheduling information.
    pub reconnect: ReconnectState,
    /// Last event included by the reducer.
    pub through_cursor: Option<EventCursor>,
    /// RFC 3339 time this snapshot was last changed.
    pub snapshot_at: Timestamp,
    /// Last terminal or protocol error visible to callers.
    pub last_error: Option<String>,
}

impl AgentState {
    /// Create the initial disconnected state for a provisional actor.
    pub fn new(agent_id: AgentId, now: Timestamp) -> Self {
        Self {
            agent_id,
            connection_state: ConnectionState::Disconnected,
            registered: false,
            connected_since: None,
            identity: IdentityState::default(),
            channels: BTreeMap::new(),
            monitored_presence: BTreeMap::new(),
            motd: MotdState::default(),
            reconnect: ReconnectState::default(),
            through_cursor: None,
            snapshot_at: now,
            last_error: None,
        }
    }

    /// Update lifecycle state and snapshot time together.
    pub fn set_connection_state(&mut self, state: ConnectionState, now: Timestamp) {
        self.connection_state = state;
        self.snapshot_at = now;
    }

    /// Apply one typed IRC projection to the advisory snapshot.
    pub fn reduce(
        &mut self,
        projection: &SemanticProjection,
        cursor: EventCursor,
        case_mapping: CaseMapping,
        now: Timestamp,
    ) {
        match &projection.event {
            SemanticEvent::Membership {
                source,
                channel,
                subject,
                change,
                ..
            } => self.reduce_membership(
                source,
                channel.as_ref().map(ChannelName::as_str),
                subject.as_deref(),
                *change,
                case_mapping,
            ),
            SemanticEvent::Presence { source, change } => {
                self.reduce_presence(&source.name, change, case_mapping);
            }
            SemanticEvent::ChannelState {
                channel,
                topic,
                modes,
                ..
            } => {
                // A MODE that applied to a user carries no channel and does
                // not touch channel state.
                let Some(channel) = channel else {
                    return;
                };
                let key = case_mapping.fold(channel.as_str());
                let state = self.channels.entry(key).or_insert_with(|| ChannelState {
                    name: channel.to_string(),
                    ..ChannelState::default()
                });
                if let Some(topic) = topic {
                    state.topic = Some(topic.clone());
                }
                if let Some(modes) = modes {
                    for mode in modes {
                        state.modes.insert(mode.clone(), None);
                    }
                }
            }
            _ => {}
        }
        self.through_cursor = Some(cursor);
        self.snapshot_at = now;
    }

    fn reduce_membership(
        &mut self,
        source: &Source,
        channel: Option<&str>,
        subject: Option<&str>,
        change: MembershipChange,
        case_mapping: CaseMapping,
    ) {
        let own_nickname = self.identity.nickname.as_deref();
        let affected = subject.unwrap_or(&source.name);
        let is_self = own_nickname.is_some_and(|own| case_mapping.same(own, affected));
        if change == MembershipChange::Quit {
            let member_key = case_mapping.fold(affected);
            for channel in self.channels.values_mut() {
                channel.members.remove(&member_key);
            }
            return;
        }
        let Some(channel_name) = channel else {
            return;
        };
        let channel_key = case_mapping.fold(channel_name);
        match change {
            MembershipChange::Joined => {
                let state = self
                    .channels
                    .entry(channel_key)
                    .or_insert_with(|| ChannelState {
                        name: channel_name.to_owned(),
                        ..ChannelState::default()
                    });
                if is_self {
                    // A self JOIN starts a fresh authoritative membership
                    // snapshot; subsequent NAMES chunks rebuild the set.
                    state.members.clear();
                    // The echoed JOIN carries the hostmask the server will put
                    // in front of this identity's relayed messages, which is
                    // the first authoritative sighting of the rewritten
                    // username and the resolved host.
                    if subject.is_none() {
                        if let Some(username) = &source.user {
                            self.identity.username = Some(username.clone());
                        }
                        if let Some(hostname) = &source.host {
                            self.identity.hostname = Some(hostname.clone());
                        }
                    }
                }
                state
                    .members
                    .entry(case_mapping.fold(affected))
                    .and_modify(|member| {
                        member.account.clone_from(&source.account);
                        member.username.clone_from(&source.user);
                        member.hostname.clone_from(&source.host);
                    })
                    .or_insert_with(|| MemberState {
                        account: source.account.clone(),
                        username: source.user.clone(),
                        hostname: source.host.clone(),
                        ..MemberState::default()
                    });
            }
            MembershipChange::Parted | MembershipChange::Kicked => {
                if is_self {
                    self.channels.remove(&channel_key);
                } else if let Some(state) = self.channels.get_mut(&channel_key) {
                    state.members.remove(&case_mapping.fold(affected));
                }
            }
            MembershipChange::Invited | MembershipChange::Quit => {}
        }
    }

    fn reduce_presence(
        &mut self,
        source: &str,
        change: &PresenceChange,
        case_mapping: CaseMapping,
    ) {
        let is_self = self
            .identity
            .nickname
            .as_deref()
            .is_some_and(|own| case_mapping.same(own, source));
        if is_self {
            match change {
                PresenceChange::Nickname(nickname) => {
                    self.identity.nickname = Some(nickname.clone());
                }
                PresenceChange::Account(account) => self.identity.account.clone_from(account),
                PresenceChange::Away(message) => self.identity.away_message.clone_from(message),
                PresenceChange::Host { user, host } => {
                    self.identity.username = Some(user.clone());
                    self.identity.hostname = Some(host.clone());
                }
                PresenceChange::RealName(real_name) => {
                    self.identity.real_name = Some(real_name.clone());
                }
            }
        }

        let old_key = case_mapping.fold(source);
        for channel in self.channels.values_mut() {
            let Some(mut member) = channel.members.remove(&old_key) else {
                continue;
            };
            let key = match change {
                PresenceChange::Nickname(nickname) => case_mapping.fold(nickname),
                PresenceChange::Account(account) => {
                    member.account.clone_from(account);
                    old_key.clone()
                }
                PresenceChange::Away(message) => {
                    member.away = Some(message.is_some());
                    old_key.clone()
                }
                PresenceChange::Host { user, host } => {
                    member.username = Some(user.clone());
                    member.hostname = Some(host.clone());
                    old_key.clone()
                }
                PresenceChange::RealName(_) => old_key.clone(),
            };
            channel.members.insert(key, member);
        }
    }

    /// Apply authoritative numeric snapshots that do not map cleanly to a
    /// single semantic transition.
    pub fn reduce_wire(&mut self, message: &WireMessage, isupport: &IsupportRegistry) {
        let mapping = isupport.case_mapping();
        if message.command.eq_ignore_ascii_case("MODE")
            && let Some(target) = message.params.first()
            && isupport.is_channel(target)
        {
            apply_modes(
                self.channel_mut(target, mapping),
                &message.params[1..],
                isupport,
            );
        }
        if message.command.eq_ignore_ascii_case("TOPIC")
            && let Some(channel) = message.params.first()
        {
            let state = self.channel_mut(channel, mapping);
            state.topic_setter = message.prefix.as_ref().map(|prefix| prefix.name.clone());
            state.topic_set_at = message.tag_value("time").and_then(|time| time.parse().ok());
        }
        match message.numeric() {
            Some(331) => {
                if let Some(channel) = message.params.get(1) {
                    self.channel_mut(channel, mapping).topic = None;
                }
            }
            Some(332) => {
                if let Some(channel) = message.params.get(1) {
                    self.channel_mut(channel, mapping).topic = message.trailing.clone();
                }
            }
            Some(333) => {
                if let Some(channel) = message.params.get(1) {
                    let state = self.channel_mut(channel, mapping);
                    state.topic_setter = message.params.get(2).cloned();
                    // RPL_TOPICWHOTIME carries Unix epoch seconds, not RFC 3339.
                    state.topic_set_at = message
                        .params
                        .get(3)
                        .and_then(|seconds| seconds.parse::<i64>().ok())
                        .and_then(Timestamp::from_unix_seconds);
                }
            }
            Some(353) => self.reduce_names(message, isupport),
            Some(324) => {
                if let Some(channel) = message.params.get(1) {
                    let state = self.channel_mut(channel, mapping);
                    state.modes.clear();
                    apply_modes(state, &message.params[2..], isupport);
                }
            }
            Some(221) => {
                self.identity.modes = message
                    .params
                    .get(1)
                    .into_iter()
                    .flat_map(|modes| modes.chars())
                    .filter(|mode| !matches!(mode, '+' | '-'))
                    .map(|mode| mode.to_string())
                    .collect();
            }
            Some(352) => self.reduce_who(message, mapping),
            Some(730) => self.reduce_monitor(message, true, mapping),
            Some(731) => self.reduce_monitor(message, false, mapping),
            _ => {}
        }
    }

    fn channel_mut(&mut self, channel: &str, mapping: CaseMapping) -> &mut ChannelState {
        self.channels
            .entry(mapping.fold(channel))
            .or_insert_with(|| ChannelState {
                name: channel.to_owned(),
                ..ChannelState::default()
            })
    }

    fn reduce_names(&mut self, message: &WireMessage, isupport: &IsupportRegistry) {
        let Some(channel) = message.params.get(2) else {
            return;
        };
        let mapping = isupport.case_mapping();
        let prefixes = isupport.membership_prefixes();
        let channel = self.channel_mut(channel, mapping);
        for raw in message
            .trailing
            .as_deref()
            .unwrap_or_default()
            .split_whitespace()
        {
            let mut name = raw;
            let mut membership = BTreeSet::new();
            while let Some(first) = name.chars().next() {
                let Some(prefix) = prefixes.iter().find(|prefix| prefix.symbol == first) else {
                    break;
                };
                membership.insert(prefix.symbol.to_string());
                name = &name[first.len_utf8()..];
            }
            let (nick_user, host) = name
                .split_once('@')
                .map_or((name, None), |(left, host)| (left, Some(host.to_owned())));
            let (nickname, user) = nick_user
                .split_once('!')
                .map_or((nick_user, None), |(nick, user)| {
                    (nick, Some(user.to_owned()))
                });
            if nickname.is_empty() {
                continue;
            }
            let member = channel.members.entry(mapping.fold(nickname)).or_default();
            member.prefixes = membership;
            if user.is_some() {
                member.username = user;
            }
            if host.is_some() {
                member.hostname = host;
            }
        }
    }

    fn reduce_who(&mut self, message: &WireMessage, mapping: CaseMapping) {
        let (Some(channel), Some(user), Some(host), Some(nickname)) = (
            message.params.get(1),
            message.params.get(2),
            message.params.get(3),
            message.params.get(5),
        ) else {
            return;
        };
        let away = message
            .params
            .get(6)
            .and_then(|flags| flags.chars().next())
            .map(|flag| flag == 'G');
        let member = self
            .channel_mut(channel, mapping)
            .members
            .entry(mapping.fold(nickname))
            .or_default();
        member.username = Some(user.clone());
        member.hostname = Some(host.clone());
        member.away = away;
    }

    fn reduce_monitor(&mut self, message: &WireMessage, online: bool, mapping: CaseMapping) {
        let values = message
            .trailing
            .as_deref()
            .or_else(|| message.params.get(1).map(String::as_str))
            .unwrap_or_default();
        for entry in values.split(',').filter(|entry| !entry.is_empty()) {
            let nickname = entry.split(['!', '@']).next().unwrap_or(entry);
            self.monitored_presence
                .insert(mapping.fold(nickname), online);
        }
    }
}

fn apply_modes(state: &mut ChannelState, params: &[String], isupport: &IsupportRegistry) {
    let Some(mode_string) = params.first() else {
        return;
    };
    let groups = isupport.channel_modes();
    let membership: BTreeSet<char> = isupport
        .membership_prefixes()
        .into_iter()
        .map(|prefix| prefix.mode)
        .collect();
    let mut adding = true;
    let mut values = params.iter().skip(1);
    for mode in mode_string.chars() {
        match mode {
            '+' => adding = true,
            '-' => adding = false,
            mode => {
                let takes_value = groups[0].contains(mode)
                    || groups[1].contains(mode)
                    || membership.contains(&mode)
                    || adding && groups[2].contains(mode);
                let value = takes_value.then(|| values.next().cloned()).flatten();
                if adding {
                    state.modes.insert(mode.to_string(), value);
                } else {
                    state.modes.remove(&mode.to_string());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn wire(line: &str) -> WireMessage {
        WireMessage::parse(Bytes::copy_from_slice(line.as_bytes())).expect("wire message")
    }

    #[test]
    fn numeric_snapshots_reduce_names_topics_modes_who_and_monitor() {
        let mut isupport = IsupportRegistry::new();
        isupport.apply_tokens([
            "CASEMAPPING=ascii",
            "PREFIX=(ov)@+",
            "CHANMODES=b,k,l,imnpst",
        ]);
        let mut state = AgentState::new(AgentId::new(), Timestamp::now());

        state.reduce_wire(&wire(":s 332 me #Room :A topic"), &isupport);
        state.reduce_wire(&wire(":s 333 me #Room setter 1234"), &isupport);
        state.reduce_wire(&wire(":s 353 me = #Room :@Alice +bob!u@host"), &isupport);
        state.reduce_wire(&wire(":s 324 me #Room +ntk secret"), &isupport);
        state.reduce_wire(
            &wire(":s 352 me #Room carol host server Carol G :0 Real Name"),
            &isupport,
        );
        state.reduce_wire(&wire(":s 730 me :Alice!u@h,bob!u@h"), &isupport);

        let channel = &state.channels["#room"];
        assert_eq!(channel.topic.as_deref(), Some("A topic"));
        assert_eq!(channel.topic_setter.as_deref(), Some("setter"));
        assert_eq!(channel.modes["k"].as_deref(), Some("secret"));
        assert!(channel.modes.contains_key("n"));
        assert!(channel.members["alice"].prefixes.contains("@"));
        assert_eq!(channel.members["bob"].username.as_deref(), Some("u"));
        assert_eq!(channel.members["carol"].away, Some(true));
        assert!(state.monitored_presence["alice"]);
        assert!(state.monitored_presence["bob"]);
    }

    #[test]
    fn a_self_join_records_the_hostmask_the_server_will_relay() {
        let isupport = IsupportRegistry::new();
        let mut state = AgentState::new(AgentId::new(), Timestamp::now());
        state.identity.nickname = Some("Mnemosyne".into());
        fn join(state: &mut AgentState, isupport: &IsupportRegistry, line: &str) {
            let message = wire(line);
            let projection = crate::irc::semantic::project(&message, isupport);
            state.reduce(
                &projection,
                EventCursor {
                    stream_id: "stream".into(),
                    sequence: 1,
                },
                isupport.case_mapping(),
                Timestamp::now(),
            );
        }

        join(
            &mut state,
            &isupport,
            ":Mnemosyne!~mcp-agent-f372cdb8-@127.0.0.1 JOIN #control",
        );

        // The requested username is not what the server relays; only the echoed
        // JOIN reveals the rewritten form and the resolved host.
        assert_eq!(
            state.identity.username.as_deref(),
            Some("~mcp-agent-f372cdb8-")
        );
        assert_eq!(state.identity.hostname.as_deref(), Some("127.0.0.1"));

        join(
            &mut state,
            &isupport,
            ":Fimafeng!~Fimafeng@172.18.0.4 JOIN #control",
        );

        assert_eq!(
            state.identity.username.as_deref(),
            Some("~mcp-agent-f372cdb8-")
        );
        assert_eq!(state.identity.hostname.as_deref(), Some("127.0.0.1"));
    }
}
