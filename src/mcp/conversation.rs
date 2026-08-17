//! Compact, model-facing projections of journal events.
//!
//! The journal is lossless on purpose: every retained record can carry its
//! complete parsed wire message, its semantic projection, correlation
//! identifiers, and provenance. That is the right shape for diagnosis and the
//! wrong shape for ordinary model context, where the same conversation costs
//! several times as many tokens as the words actually exchanged.
//!
//! [`CompactEvent`] is the conversational half of that split. It keeps what a
//! reader needs to follow and answer a conversation — who spoke, where, when,
//! what they said, whether it was addressed to us — and its cursor, so a
//! compact read is still a durable position in the same stream. Anything
//! needed to debug the protocol stays in the wire resource.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    agent::journal::{EventClass, EventCursor, EventDirection, EventPayload, IrcEvent},
    irc::semantic::{SemanticEvent, Source},
    time::Timestamp,
};

/// One journal event reduced to its conversational content.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct CompactEvent {
    /// Durable position of this event in the agent's stream.
    pub cursor: EventCursor,
    /// Server time when the server supplied one, otherwise receipt time.
    pub at: Timestamp,
    /// Stable event class.
    pub class: EventClass,
    /// Whether the gateway received or sent this.
    pub direction: EventDirection,
    /// Case-preserved channel or nickname this belongs to.
    pub target: Option<String>,
    /// Case-preserved nickname that produced it, when one is known.
    pub source: Option<String>,
    /// Conversational text, when the event carries any.
    pub text: Option<String>,
    /// Whether the event is addressed to the owning agent.
    pub mentions_me: bool,
    /// One-line description for events whose meaning is not their text, such
    /// as joins, nick changes, and topic changes.
    pub summary: Option<String>,
}

impl CompactEvent {
    /// Reduce one journal event, or return `None` when it carries nothing a
    /// conversational reader would want.
    ///
    /// Returning `None` is what keeps a transcript readable: numerics, capability
    /// negotiation, and malformed-line records are all genuinely useful, but they
    /// belong to the wire resource, not to the conversation.
    pub fn project(event: &IrcEvent) -> Option<Self> {
        let (source, text, summary) = match event.semantic.as_ref()? {
            EventPayload::Irc(projection) => conversational_parts(&projection.event)?,
            EventPayload::DccChatMessage(message) => {
                (None, Some(message.text.clone()), Some("direct chat".into()))
            }
            // Everything else is protocol or gateway bookkeeping.
            _ => return None,
        };
        Some(Self {
            cursor: event.cursor.clone(),
            at: event.server_time.unwrap_or(event.received_at),
            class: event.class,
            direction: event.direction,
            target: event.target.clone(),
            source,
            text,
            mentions_me: event.mentions_me,
            summary,
        })
    }
}

/// Split one semantic event into its speaker, its text, and a summary of what
/// it did, or `None` when it is not conversational at all.
fn conversational_parts(
    event: &SemanticEvent,
) -> Option<(Option<String>, Option<String>, Option<String>)> {
    let named = |source: &Source| Some(source.name.clone());
    Some(match event {
        SemanticEvent::MessageChannel { source, text, .. }
        | SemanticEvent::MessagePrivate { source, text, .. } => {
            (named(source), Some(text.clone()), None)
        }
        SemanticEvent::MessageAction { source, text, .. } => (
            named(source),
            Some(text.clone()),
            Some(format!("{} {text}", source.name)),
        ),
        SemanticEvent::MessageNotice { source, text, .. } => {
            (named(source), Some(text.clone()), Some("notice".into()))
        }
        SemanticEvent::Membership {
            source,
            subject,
            change,
            reason,
            ..
        } => {
            let who = subject.clone().unwrap_or_else(|| source.name.clone());
            let reason = reason
                .as_ref()
                .map(|reason| format!(" ({reason})"))
                .unwrap_or_default();
            (
                named(source),
                None,
                Some(format!("{who} {}{reason}", membership_verb(change))),
            )
        }
        SemanticEvent::Presence { source, change } => (
            named(source),
            None,
            Some(format!("{} {}", source.name, presence_verb(change))),
        ),
        SemanticEvent::ChannelState {
            source,
            topic,
            modes,
            ..
        } => {
            let summary = match (topic, modes) {
                (Some(topic), _) => format!("{} set the topic to: {topic}", source.name),
                (None, Some(modes)) => format!("{} set mode {}", source.name, modes.join(" ")),
                (None, None) => return None,
            };
            (named(source), topic.clone(), Some(summary))
        }
        // Tag-only messages, CTCP, numerics, MOTD, capability and lifecycle
        // records carry no conversational content of their own.
        _ => return None,
    })
}

fn membership_verb(change: &crate::irc::semantic::MembershipChange) -> &'static str {
    use crate::irc::semantic::MembershipChange;
    match change {
        MembershipChange::Joined => "joined",
        MembershipChange::Parted => "left",
        MembershipChange::Kicked => "was kicked",
        MembershipChange::Quit => "quit",
        MembershipChange::Invited => "was invited",
    }
}

fn presence_verb(change: &crate::irc::semantic::PresenceChange) -> String {
    use crate::irc::semantic::PresenceChange;
    match change {
        PresenceChange::Nickname(value) => format!("is now known as {value}"),
        PresenceChange::Account(Some(account)) => format!("logged in as {account}"),
        PresenceChange::Account(None) => "logged out".into(),
        PresenceChange::Away(Some(message)) => format!("is away: {message}"),
        PresenceChange::Away(None) => "is back".into(),
        PresenceChange::Host { user, host } => format!("is now {user}@{host}"),
        PresenceChange::RealName(value) => format!("is now \"{value}\""),
    }
}
