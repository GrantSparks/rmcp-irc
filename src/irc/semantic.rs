//! Typed semantic projections derived from lossless wire events.
//!
//! Projections supplement rather than replace the originating [`WireMessage`].

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::time::Timestamp;

use super::{isupport::IsupportRegistry, target::ChannelName, wire::WireMessage};

/// Semantic event classes required by the MCP contract.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticClass {
    /// Channel message.
    MessageChannel,
    /// Private message.
    MessagePrivate,
    /// CTCP ACTION.
    MessageAction,
    /// IRC NOTICE.
    MessageNotice,
    /// IRC TAGMSG or capability-specific tag event.
    MessageTagged,
    /// JOIN, PART, KICK, INVITE, or QUIT.
    Membership,
    /// NICK, ACCOUNT, AWAY, CHGHOST, or SETNAME.
    Presence,
    /// MODE, TOPIC, or channel rename.
    ChannelState,
    /// CAP or ISUPPORT change.
    ProtocolCompatibility,
    /// MOTD completion.
    ServerMotd,
    /// Correlated reply, numeric, or standard reply.
    ProtocolReply,
    /// Unknown command, numeric, batch, or tag semantics.
    ProtocolUnknown,
    /// Connection lifecycle.
    ConnectionLifecycle,
    /// CTCP query or reply.
    Ctcp,
    /// DCC negotiation, chat, transfer, or lifecycle.
    Dcc,
}

/// Who sent a message, as far as the wire reveals.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
pub struct Source {
    /// Nickname or server name exactly as sent.
    pub name: String,
    /// User component, when present.
    pub user: Option<String>,
    /// Host component, when present.
    pub host: Option<String>,
    /// Account name from `account-tag`, when the capability is active.
    pub account: Option<String>,
}

/// How a membership set changed.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MembershipChange {
    /// The source joined.
    Joined,
    /// The source left.
    Parted,
    /// The subject was removed by the source.
    Kicked,
    /// The subject was invited by the source.
    Invited,
    /// The source disconnected.
    Quit,
}

/// How an identity changed.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum PresenceChange {
    /// The source now uses this nickname.
    Nickname(String),
    /// The source logged into or out of an account.
    Account(Option<String>),
    /// The source set or cleared an away message.
    Away(Option<String>),
    /// The source changed user and host.
    Host {
        /// New user component.
        user: String,
        /// New host component.
        host: String,
    },
    /// The source changed its real name.
    RealName(String),
}

/// Whether a reaction was attached to or removed from a message.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReactionChange {
    /// A reaction was attached.
    Added,
    /// A reaction was removed.
    Removed,
}

/// IRCv3 typing indicator state.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TypingStatus {
    /// The sender is actively changing their input field.
    Active,
    /// The sender paused without clearing their input field.
    Paused,
    /// The sender cleared their input field without sending.
    Done,
}

/// Which part of the MOTD sequence one reply carries.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MotdStage {
    /// 375: the MOTD is starting.
    Start,
    /// 372: one MOTD line.
    Line,
    /// 376: the MOTD is complete.
    End,
    /// 422: this server has no MOTD.
    Missing,
}

impl MotdStage {
    /// Classify a numeric as part of the MOTD sequence.
    const fn from_numeric(numeric: u16) -> Option<Self> {
        match numeric {
            375 => Some(Self::Start),
            372 => Some(Self::Line),
            376 => Some(Self::End),
            422 => Some(Self::Missing),
            _ => None,
        }
    }

    /// Whether this stage ends the sequence, with or without text.
    #[cfg(test)]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::End | Self::Missing)
    }
}

/// Typed payload for one semantic class.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "class")]
pub enum SemanticEvent {
    /// A message addressed to a channel.
    MessageChannel {
        /// Sender.
        source: Source,
        /// Channel it was sent to.
        channel: ChannelName,
        /// Message text.
        text: String,
    },
    /// A message addressed to this connection.
    MessagePrivate {
        /// Sender.
        source: Source,
        /// Target nickname as addressed.
        target: String,
        /// Message text.
        text: String,
    },
    /// A CTCP ACTION, in a channel or privately.
    MessageAction {
        /// Sender.
        source: Source,
        /// Channel or nickname it was sent to.
        target: String,
        /// Action text without the CTCP framing.
        text: String,
    },
    /// A NOTICE, in a channel or privately.
    MessageNotice {
        /// Sender.
        source: Source,
        /// Channel or nickname it was sent to.
        target: String,
        /// Notice text.
        text: String,
    },
    /// A tag-only message.
    MessageTagged {
        /// Sender.
        source: Source,
        /// Channel or nickname it was sent to.
        target: String,
    },
    /// A lightweight reaction attached to a server-identified message.
    MessageReaction {
        /// Sender.
        source: Source,
        /// Channel or nickname containing the message.
        target: String,
        /// Referenced server message ID.
        message_id: String,
        /// Reaction value.
        reaction: String,
        /// Whether the reaction was added or removed.
        change: ReactionChange,
    },
    /// A privacy-sensitive typing indicator.
    Typing {
        /// Sender.
        source: Source,
        /// Channel or nickname observing the indicator.
        target: String,
        /// Published typing status.
        status: TypingStatus,
    },
    /// A message was redacted from one conversation.
    MessageRedaction {
        /// Actor who requested or performed the redaction.
        source: Source,
        /// Channel or nickname containing the message.
        target: String,
        /// Redacted server message ID.
        message_id: String,
        /// Optional reason supplied by the actor.
        reason: Option<String>,
    },
    /// Synchronized local read marker for one conversation.
    ReadMarker {
        /// Channel or nickname buffer.
        target: String,
        /// Last-read timestamp, or `None` when the server returned `*`.
        read_at: Option<Timestamp>,
    },
    /// A CTCP query or reply other than ACTION.
    Ctcp {
        /// Sender.
        source: Source,
        /// Channel or nickname it was sent to.
        target: String,
        /// Uppercase CTCP command.
        command: String,
        /// Arguments following the command, if any.
        arguments: Option<String>,
        /// Whether this was a reply rather than a query.
        is_reply: bool,
    },
    /// A membership transition.
    Membership {
        /// Who caused it.
        source: Source,
        /// Channel it applies to, absent for QUIT.
        channel: Option<ChannelName>,
        /// Who it happened to, when that differs from the source.
        subject: Option<String>,
        /// What changed.
        change: MembershipChange,
        /// Reason text, when the server supplied one.
        reason: Option<String>,
    },
    /// An identity transition.
    Presence {
        /// Whose identity changed.
        source: Source,
        /// What changed.
        change: PresenceChange,
    },
    /// A channel mode or topic change.
    ChannelState {
        /// Who changed it.
        source: Source,
        /// Channel it applies to, absent when a MODE applied to a user.
        channel: Option<ChannelName>,
        /// New topic, for a TOPIC change.
        topic: Option<String>,
        /// Mode string and its arguments, for a MODE change.
        modes: Option<Vec<String>>,
    },
    /// A numeric reply or standard reply.
    ProtocolReply {
        /// Numeric value, when the command was a numeric.
        numeric: Option<u16>,
        /// Case-preserved command spelling.
        command: String,
        /// Ordered parameters after the client's own name.
        parameters: Vec<String>,
        /// Trailing text, when present.
        text: Option<String>,
    },
    /// One reply in the server's MOTD sequence.
    ServerMotd {
        /// Which part of the sequence this reply is.
        stage: MotdStage,
        /// Line text, absent when the server sent none.
        text: Option<String>,
    },
    /// A capability or ISUPPORT change.
    ProtocolCompatibility {
        /// Case-preserved command spelling.
        command: String,
        /// Tokens the server named.
        tokens: Vec<String>,
    },
    /// Connection lifecycle transition.
    ConnectionLifecycle {
        /// Case-preserved command spelling.
        command: String,
        /// Server-supplied text, when present.
        text: Option<String>,
    },
    /// Anything the gateway does not model semantically.
    ProtocolUnknown {
        /// Case-preserved command spelling.
        command: String,
    },
}

impl SemanticEvent {
    /// Stable class used by event filters.
    pub const fn class(&self) -> SemanticClass {
        match self {
            Self::MessageChannel { .. } => SemanticClass::MessageChannel,
            Self::MessagePrivate { .. } => SemanticClass::MessagePrivate,
            Self::MessageAction { .. } => SemanticClass::MessageAction,
            Self::MessageNotice { .. } => SemanticClass::MessageNotice,
            Self::MessageTagged { .. } => SemanticClass::MessageTagged,
            Self::MessageReaction { .. } | Self::MessageRedaction { .. } => {
                SemanticClass::MessageTagged
            }
            Self::Typing { .. } => SemanticClass::Presence,
            Self::ReadMarker { .. } => SemanticClass::ProtocolReply,
            Self::Ctcp { .. } => SemanticClass::Ctcp,
            Self::Membership { .. } => SemanticClass::Membership,
            Self::Presence { .. } => SemanticClass::Presence,
            Self::ChannelState { .. } => SemanticClass::ChannelState,
            Self::ServerMotd { .. } => SemanticClass::ServerMotd,
            Self::ProtocolReply { .. } => SemanticClass::ProtocolReply,
            Self::ProtocolCompatibility { .. } => SemanticClass::ProtocolCompatibility,
            Self::ConnectionLifecycle { .. } => SemanticClass::ConnectionLifecycle,
            Self::ProtocolUnknown { .. } => SemanticClass::ProtocolUnknown,
        }
    }

    /// Who the wire attributed this to, when it attributed it to anybody.
    ///
    /// Server-generated records — numerics, the MOTD, capability negotiation,
    /// and this gateway's own lifecycle notes — have no speaker, which is
    /// exactly the distinction a caller needs to tell somebody talking from the
    /// protocol working.
    pub const fn source(&self) -> Option<&Source> {
        match self {
            Self::MessageChannel { source, .. }
            | Self::MessagePrivate { source, .. }
            | Self::MessageAction { source, .. }
            | Self::MessageNotice { source, .. }
            | Self::MessageTagged { source, .. }
            | Self::MessageReaction { source, .. }
            | Self::MessageRedaction { source, .. }
            | Self::Typing { source, .. }
            | Self::Ctcp { source, .. }
            | Self::Membership { source, .. }
            | Self::Presence { source, .. }
            | Self::ChannelState { source, .. } => Some(source),
            Self::ReadMarker { .. }
            | Self::ProtocolReply { .. }
            | Self::ServerMotd { .. }
            | Self::ProtocolCompatibility { .. }
            | Self::ConnectionLifecycle { .. }
            | Self::ProtocolUnknown { .. } => None,
        }
    }
}

/// Semantic projection paired with the class its consumers filter on.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
pub struct SemanticProjection {
    /// Stable class used by event filters.
    pub class: SemanticClass,
    /// Class-specific structured data.
    pub event: SemanticEvent,
}

impl From<SemanticEvent> for SemanticProjection {
    fn from(event: SemanticEvent) -> Self {
        Self {
            class: event.class(),
            event,
        }
    }
}

/// Project one wire message into its semantic class.
///
/// The ISUPPORT registry decides what counts as a channel, because that is a
/// per-server property rather than a fixed `#` convention.
pub fn project(message: &WireMessage, isupport: &IsupportRegistry) -> SemanticProjection {
    let source = source_of(message);
    let command = message.command.to_ascii_uppercase();
    let target = message.params.first().cloned();
    let text = message.trailing.clone();
    let prefixes = isupport.channel_types();
    // A channel parameter that the server's own prefixes do not accept is not
    // a channel, and is reported as absent rather than guessed at.
    let as_channel = |value: Option<String>| {
        value.and_then(|value| ChannelName::with_prefixes(value, &prefixes).ok())
    };

    let event = match command.as_str() {
        "PRIVMSG" => project_privmsg(message, isupport, source, target, text),
        "NOTICE" => SemanticEvent::MessageNotice {
            source,
            target: target.unwrap_or_default(),
            text: text.unwrap_or_default(),
        },
        "TAGMSG" => project_tagmsg(message, source, target),
        "REDACT" => SemanticEvent::MessageRedaction {
            source,
            target: target.unwrap_or_default(),
            message_id: message.params.get(1).cloned().unwrap_or_default(),
            reason: message.params.get(2).cloned().or(text),
        },
        "MARKREAD" => SemanticEvent::ReadMarker {
            target: target.unwrap_or_default(),
            read_at: message
                .params
                .get(1)
                .or(message.trailing.as_ref())
                .filter(|value| value.as_str() != "*")
                .and_then(|value| value.strip_prefix("timestamp="))
                .and_then(|value| value.parse().ok()),
        },
        "JOIN" => SemanticEvent::Membership {
            source,
            channel: as_channel(target),
            subject: None,
            change: MembershipChange::Joined,
            reason: None,
        },
        "PART" => SemanticEvent::Membership {
            source,
            channel: as_channel(target),
            subject: None,
            change: MembershipChange::Parted,
            reason: text,
        },
        "KICK" => SemanticEvent::Membership {
            source,
            channel: as_channel(target),
            subject: message.params.get(1).cloned(),
            change: MembershipChange::Kicked,
            reason: text,
        },
        "INVITE" => SemanticEvent::Membership {
            source,
            channel: as_channel(message.params.get(1).cloned().or(text)),
            subject: target,
            change: MembershipChange::Invited,
            reason: None,
        },
        "QUIT" => SemanticEvent::Membership {
            source,
            channel: None,
            subject: None,
            change: MembershipChange::Quit,
            reason: text,
        },
        "NICK" => SemanticEvent::Presence {
            source,
            change: PresenceChange::Nickname(target.or(text).unwrap_or_default()),
        },
        "ACCOUNT" => {
            let account = target.filter(|value| value != "*");
            SemanticEvent::Presence {
                source,
                change: PresenceChange::Account(account),
            }
        }
        "AWAY" => SemanticEvent::Presence {
            source,
            change: PresenceChange::Away(text),
        },
        "CHGHOST" => SemanticEvent::Presence {
            source,
            change: PresenceChange::Host {
                user: target.unwrap_or_default(),
                host: message.params.get(1).cloned().unwrap_or_default(),
            },
        },
        "SETNAME" => SemanticEvent::Presence {
            source,
            change: PresenceChange::RealName(text.unwrap_or_default()),
        },
        "TOPIC" => SemanticEvent::ChannelState {
            source,
            channel: as_channel(target),
            // A spaceless topic arrives as params[1] with no colon, not as the
            // trailing `text`.
            topic: message.final_field(1),
            modes: None,
        },
        "MODE" => SemanticEvent::ChannelState {
            source,
            channel: as_channel(target.clone()),
            topic: None,
            modes: Some(message.params.iter().skip(1).cloned().collect()),
        },
        "CAP" => SemanticEvent::ProtocolCompatibility {
            command,
            tokens: text
                .unwrap_or_default()
                .split(' ')
                .filter(|token| !token.is_empty())
                .map(str::to_owned)
                .collect(),
        },
        "ERROR" => SemanticEvent::ConnectionLifecycle { command, text },
        _ => project_numeric(message, command, text),
    };
    event.into()
}

fn project_tagmsg(message: &WireMessage, source: Source, target: Option<String>) -> SemanticEvent {
    let target = target.unwrap_or_default();
    let reply_to = message
        .tag_value("+reply")
        .or_else(|| message.tag_value("reply"));
    for (key, change) in [
        ("+draft/react", ReactionChange::Added),
        ("+draft/unreact", ReactionChange::Removed),
    ] {
        if let (Some(message_id), Some(reaction)) = (reply_to, message.tag_value(key)) {
            return SemanticEvent::MessageReaction {
                source,
                target,
                message_id: message_id.to_owned(),
                reaction: reaction.to_owned(),
                change,
            };
        }
    }
    let typing = message
        .tag_value("+typing")
        .or_else(|| message.tag_value("typing"));
    let status = match typing {
        Some("active") => Some(TypingStatus::Active),
        Some("paused") => Some(TypingStatus::Paused),
        Some("done") => Some(TypingStatus::Done),
        _ => None,
    };
    if let Some(status) = status {
        return SemanticEvent::Typing {
            source,
            target,
            status,
        };
    }
    SemanticEvent::MessageTagged { source, target }
}

fn project_privmsg(
    message: &WireMessage,
    isupport: &IsupportRegistry,
    source: Source,
    target: Option<String>,
    text: Option<String>,
) -> SemanticEvent {
    let target = target.unwrap_or_default();
    let text = text.unwrap_or_default();
    if let Some(ctcp) = Ctcp::parse(&text) {
        return if ctcp.command == "ACTION" {
            SemanticEvent::MessageAction {
                source,
                target,
                text: ctcp.arguments.unwrap_or_default(),
            }
        } else {
            SemanticEvent::Ctcp {
                source,
                target,
                command: ctcp.command,
                arguments: ctcp.arguments,
                is_reply: false,
            }
        };
    }
    let _ = message;
    if let Ok(channel) = ChannelName::with_prefixes(target.clone(), &isupport.channel_types()) {
        SemanticEvent::MessageChannel {
            source,
            channel,
            text,
        }
    } else {
        SemanticEvent::MessagePrivate {
            source,
            target,
            text,
        }
    }
}

fn project_numeric(message: &WireMessage, command: String, text: Option<String>) -> SemanticEvent {
    match message.numeric() {
        Some(numeric) => {
            // The first parameter of a numeric is the client's own name and
            // carries no information the consumer needs.
            let parameters = message.params.iter().skip(1).cloned().collect();
            if let Some(stage) = MotdStage::from_numeric(numeric) {
                return SemanticEvent::ServerMotd { stage, text };
            }
            if matches!(numeric, 1 | 5 | 421) {
                return SemanticEvent::ProtocolCompatibility {
                    command,
                    tokens: message.params.iter().skip(1).cloned().collect(),
                };
            }
            SemanticEvent::ProtocolReply {
                numeric: Some(numeric),
                command,
                parameters,
                text,
            }
        }
        None => SemanticEvent::ProtocolUnknown { command },
    }
}

fn source_of(message: &WireMessage) -> Source {
    let account = message.tag_value("account").map(str::to_owned);
    message.prefix.as_ref().map_or_else(
        || Source {
            name: String::new(),
            user: None,
            host: None,
            account: account.clone(),
        },
        |prefix| Source {
            name: prefix.name.clone(),
            user: prefix.user.clone(),
            host: prefix.host.clone(),
            account: account.clone(),
        },
    )
}

/// A CTCP payload carried inside a PRIVMSG or NOTICE.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ctcp {
    /// Uppercase CTCP command.
    pub command: String,
    /// Everything after the command, when present.
    pub arguments: Option<String>,
}

impl Ctcp {
    /// Parse CTCP framing without modifying the original text.
    pub fn parse(text: &str) -> Option<Self> {
        let body = text.strip_prefix('\u{1}')?;
        let body = body.strip_suffix('\u{1}').unwrap_or(body);
        if body.is_empty() {
            return None;
        }
        let (command, arguments) = body.split_once(' ').map_or_else(
            || (body, None),
            |(command, arguments)| (command, Some(arguments.to_owned())),
        );
        Some(Self {
            command: command.to_ascii_uppercase(),
            arguments,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn parse(line: &str) -> WireMessage {
        WireMessage::parse(Bytes::copy_from_slice(line.as_bytes())).expect("parse")
    }

    fn isupport() -> IsupportRegistry {
        let mut registry = IsupportRegistry::new();
        registry.apply_tokens(["CHANTYPES=#&"]);
        registry
    }

    #[test]
    fn channel_and_private_messages_are_distinguished_by_isupport() {
        let channel = project(&parse(":a!u@h PRIVMSG #room :hello"), &isupport());
        assert_eq!(channel.class, SemanticClass::MessageChannel);
        let SemanticEvent::MessageChannel { channel, text, .. } = channel.event else {
            panic!("expected a channel message");
        };
        assert_eq!(channel.as_str(), "#room");
        assert_eq!(text, "hello");

        let private = project(&parse(":a!u@h PRIVMSG Kuebiko :hi"), &isupport());
        assert_eq!(private.class, SemanticClass::MessagePrivate);
    }

    #[test]
    fn ctcp_action_and_other_ctcp_are_separate_classes() {
        let action = project(
            &parse(":a!u@h PRIVMSG #room :\u{1}ACTION waves\u{1}"),
            &isupport(),
        );
        assert_eq!(
            action.event,
            SemanticEvent::MessageAction {
                source: Source {
                    name: "a".into(),
                    user: Some("u".into()),
                    host: Some("h".into()),
                    account: None,
                },
                target: "#room".into(),
                text: "waves".into(),
            }
        );

        let version = project(
            &parse(":a!u@h PRIVMSG Kuebiko :\u{1}VERSION\u{1}"),
            &isupport(),
        );
        assert_eq!(version.class, SemanticClass::Ctcp);
    }

    #[test]
    fn the_account_tag_reaches_the_projection() {
        let projection = project(
            &parse("@account=grant :grant!u@h PRIVMSG #room :hello"),
            &isupport(),
        );
        let SemanticEvent::MessageChannel { source, .. } = projection.event else {
            panic!("expected a channel message");
        };
        assert_eq!(source.account.as_deref(), Some("grant"));
    }

    #[test]
    fn membership_transitions_carry_their_subject_and_reason() {
        let kick = project(&parse(":op!u@h KICK #room victim :spam"), &isupport());
        assert_eq!(
            kick.event,
            SemanticEvent::Membership {
                source: Source {
                    name: "op".into(),
                    user: Some("u".into()),
                    host: Some("h".into()),
                    account: None,
                },
                channel: Some(ChannelName::new("#room").expect("channel")),
                subject: Some("victim".into()),
                change: MembershipChange::Kicked,
                reason: Some("spam".into()),
            }
        );

        let quit = project(&parse(":a!u@h QUIT :bye"), &isupport());
        let SemanticEvent::Membership {
            channel, change, ..
        } = quit.event
        else {
            panic!("expected membership");
        };
        assert_eq!(change, MembershipChange::Quit);
        assert!(channel.is_none());
    }

    #[test]
    fn presence_changes_are_typed_by_what_changed() {
        let nick = project(&parse(":a!u@h NICK :b"), &isupport());
        let SemanticEvent::Presence { change, .. } = nick.event else {
            panic!("expected presence");
        };
        assert_eq!(change, PresenceChange::Nickname("b".into()));

        let logout = project(&parse(":a!u@h ACCOUNT *"), &isupport());
        let SemanticEvent::Presence { change, .. } = logout.event else {
            panic!("expected presence");
        };
        assert_eq!(change, PresenceChange::Account(None));
    }

    #[test]
    fn numerics_project_as_replies_without_the_client_name() {
        let projection = project(
            &parse(":server 318 Kuebiko alice :End of /WHOIS"),
            &isupport(),
        );
        assert_eq!(
            projection.event,
            SemanticEvent::ProtocolReply {
                numeric: Some(318),
                command: "318".into(),
                parameters: vec!["alice".into()],
                text: Some("End of /WHOIS".into()),
            }
        );
    }

    #[test]
    fn the_motd_sequence_projects_as_its_own_class() {
        let stages: Vec<SemanticEvent> = [
            ":server 375 Kuebiko :- server Message of the Day -",
            ":server 372 Kuebiko :- be excellent to each other",
            ":server 376 Kuebiko :End of /MOTD command.",
        ]
        .iter()
        .map(|line| project(&parse(line), &isupport()).event)
        .collect();

        assert_eq!(
            stages[0],
            SemanticEvent::ServerMotd {
                stage: MotdStage::Start,
                text: Some("- server Message of the Day -".into()),
            }
        );
        assert_eq!(
            stages[1],
            SemanticEvent::ServerMotd {
                stage: MotdStage::Line,
                text: Some("- be excellent to each other".into()),
            }
        );
        let SemanticEvent::ServerMotd { stage, .. } = stages[2] else {
            panic!("expected the MOTD terminator");
        };
        assert!(stage.is_terminal());
        assert_eq!(
            project(&parse(":server 375 Kuebiko :x"), &isupport()).class,
            SemanticClass::ServerMotd
        );
    }

    #[test]
    fn a_server_without_a_motd_is_a_terminal_motd_event_not_an_error() {
        let projection = project(
            &parse(":server 422 Kuebiko :MOTD File is missing"),
            &isupport(),
        );
        assert_eq!(projection.class, SemanticClass::ServerMotd);
        let SemanticEvent::ServerMotd { stage, .. } = projection.event else {
            panic!("expected a MOTD event");
        };
        assert_eq!(stage, MotdStage::Missing);
        assert!(stage.is_terminal());
    }

    #[test]
    fn reactions_and_typing_are_typed_from_client_only_tags() {
        let reaction = project(
            &parse("@+reply=abc;+draft/react=wave :a!u@h TAGMSG #room"),
            &isupport(),
        );
        assert_eq!(reaction.class, SemanticClass::MessageTagged);
        assert_eq!(
            reaction.event,
            SemanticEvent::MessageReaction {
                source: Source {
                    name: "a".into(),
                    user: Some("u".into()),
                    host: Some("h".into()),
                    account: None,
                },
                target: "#room".into(),
                message_id: "abc".into(),
                reaction: "wave".into(),
                change: ReactionChange::Added,
            }
        );

        let typing = project(&parse("@+typing=paused :a!u@h TAGMSG Kuebiko"), &isupport());
        assert_eq!(typing.class, SemanticClass::Presence);
        assert!(matches!(
            typing.event,
            SemanticEvent::Typing {
                status: TypingStatus::Paused,
                ..
            }
        ));
    }

    #[test]
    fn redactions_preserve_the_target_message_and_optional_reason() {
        let projection = project(
            &parse(":a!u@h REDACT #room abc :sent accidentally"),
            &isupport(),
        );
        assert!(matches!(
            projection.event,
            SemanticEvent::MessageRedaction {
                target,
                message_id,
                reason: Some(reason),
                ..
            } if target == "#room" && message_id == "abc" && reason == "sent accidentally"
        ));
    }

    #[test]
    fn read_markers_project_valid_timestamps_and_the_unknown_marker() {
        let known = project(
            &parse(":server MARKREAD #room timestamp=2026-08-17T07:00:00.123Z"),
            &isupport(),
        );
        assert!(matches!(
            known.event,
            SemanticEvent::ReadMarker {
                read_at: Some(_),
                ..
            }
        ));

        let unknown = project(&parse(":server MARKREAD #room *"), &isupport());
        assert_eq!(
            unknown.event,
            SemanticEvent::ReadMarker {
                target: "#room".into(),
                read_at: None,
            }
        );
    }

    #[test]
    fn unknown_commands_remain_unknown_rather_than_guessed() {
        let projection = project(&parse(":server FUTURE #room :data"), &isupport());
        assert_eq!(
            projection.event,
            SemanticEvent::ProtocolUnknown {
                command: "FUTURE".into()
            }
        );
    }

    #[test]
    fn a_projection_serializes_with_its_class_tag() {
        let projection = project(&parse(":a!u@h PRIVMSG #room :hi"), &isupport());
        let json = serde_json::to_value(&projection).expect("serialize");
        assert_eq!(json["class"], "message_channel");
        assert_eq!(json["event"]["class"], "message_channel");
        assert_eq!(json["event"]["text"], "hi");
    }
}
