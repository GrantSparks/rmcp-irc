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
    /// Registered IRC account reported by `account-tag`, when present. On the
    /// configured collaboration network an account identifies a human; a
    /// missing value remains unknown rather than proving the sender is an
    /// agent.
    pub source_account: Option<String>,
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
        let (source, source_account, text, summary) = match event.semantic.as_ref()? {
            EventPayload::Irc(projection) => conversational_parts(&projection.event)?,
            EventPayload::DccChatMessage(message) => (
                None,
                None,
                Some(message.text.clone()),
                Some("direct chat".into()),
            ),
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
            source_account,
            text,
            mentions_me: event.mentions_me,
            summary,
        })
    }
}

/// Split one semantic event into its speaker, its text, and a summary of what
/// it did, or `None` when it is not conversational at all.
type ConversationalParts = (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

fn conversational_parts(event: &SemanticEvent) -> Option<ConversationalParts> {
    // A line the gateway wrote carries no prefix, so its parsed source name is
    // empty. That is genuinely "unknown speaker", not a speaker called "", and
    // reporting it as a name would put an empty string where a reader expects a
    // nickname and a stray leading space in front of every summary.
    let named = |source: &Source| (!source.name.is_empty()).then(|| source.name.clone());
    let account = |source: &Source| source.account.clone();
    Some(match event {
        SemanticEvent::MessageChannel { source, text, .. }
        | SemanticEvent::MessagePrivate { source, text, .. } => {
            (named(source), account(source), Some(text.clone()), None)
        }
        SemanticEvent::MessageAction { source, text, .. } => (
            named(source),
            account(source),
            Some(text.clone()),
            Some(with_actor(named(source).as_deref(), text)),
        ),
        SemanticEvent::MessageNotice { source, text, .. } => (
            named(source),
            account(source),
            Some(text.clone()),
            Some("notice".into()),
        ),
        SemanticEvent::Membership {
            source,
            subject,
            change,
            reason,
            ..
        } => {
            let who = subject.clone().or_else(|| named(source));
            let reason = reason
                .as_ref()
                .map(|reason| format!(" ({reason})"))
                .unwrap_or_default();
            (
                named(source),
                account(source),
                None,
                Some(with_actor(
                    who.as_deref(),
                    &format!("{}{reason}", membership_verb(change)),
                )),
            )
        }
        SemanticEvent::Presence { source, change } => (
            named(source),
            account(source),
            None,
            Some(with_actor(named(source).as_deref(), &presence_verb(change))),
        ),
        SemanticEvent::ChannelState {
            source,
            topic,
            modes,
            ..
        } => {
            let actor = named(source);
            let summary = match (topic, modes) {
                (Some(topic), _) => match actor.as_deref() {
                    Some(actor) => format!("{actor} set the topic to: {topic}"),
                    None => format!("topic set to: {topic}"),
                },
                (None, Some(modes)) => match actor.as_deref() {
                    Some(actor) => format!("{actor} set mode {}", modes.join(" ")),
                    None => format!("mode set {}", modes.join(" ")),
                },
                (None, None) => return None,
            };
            (named(source), account(source), topic.clone(), Some(summary))
        }
        // Tag-only messages, CTCP, numerics, MOTD, capability and lifecycle
        // records carry no conversational content of their own.
        _ => return None,
    })
}

/// Account attached to a conversational IRC message, when `account-tag`
/// supplied one.
///
/// This is deliberately only positive evidence. A missing tag may mean the
/// sender is an unregistered agent, but it can also mean the capability was
/// not active or the event came from older history.
pub fn source_account(event: &IrcEvent) -> Option<&str> {
    let EventPayload::Irc(projection) = event.semantic.as_ref()? else {
        return None;
    };
    let source = match &projection.event {
        SemanticEvent::MessageChannel { source, .. }
        | SemanticEvent::MessagePrivate { source, .. }
        | SemanticEvent::MessageAction { source, .. }
        | SemanticEvent::MessageNotice { source, .. } => source,
        _ => return None,
    };
    source.account.as_deref()
}

/// Prefix a summary with whoever caused it, or leave it as the bare predicate
/// when the wire named nobody.
///
/// Only the agent's own outgoing lines reach this without an actor: they have
/// no prefix to parse one from, and the caller already knows it was the one
/// talking.
fn with_actor(actor: Option<&str>, predicate: &str) -> String {
    actor.map_or_else(
        || predicate.to_owned(),
        |actor| format!("{actor} {predicate}"),
    )
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        agent::{
            AgentId,
            journal::{EventCorrelation, EventOrigin, EventVerbosity},
        },
        irc::{semantic::SemanticProjection, target::ChannelName},
    };

    #[test]
    fn a_line_the_agent_wrote_itself_reports_no_speaker_rather_than_an_empty_one() {
        // An outgoing line has no prefix to parse a nickname out of, which used
        // to reach the reader as a speaker named "" and summaries that opened
        // with a bare space.
        let projected = project(SemanticEvent::ChannelState {
            source: source(""),
            channel: Some(ChannelName::new("#project".to_owned()).expect("channel")),
            topic: Some("what we are doing".into()),
            modes: None,
        });

        assert_eq!(projected.source, None);
        assert_eq!(
            projected.summary.as_deref(),
            Some("topic set to: what we are doing")
        );
    }

    #[test]
    fn a_line_somebody_sent_still_names_them() {
        let projected = project(SemanticEvent::ChannelState {
            source: source("grant"),
            channel: Some(ChannelName::new("#project".to_owned()).expect("channel")),
            topic: Some("what we are doing".into()),
            modes: None,
        });

        assert_eq!(projected.source.as_deref(), Some("grant"));
        assert_eq!(
            projected.summary.as_deref(),
            Some("grant set the topic to: what we are doing")
        );
    }

    fn source(name: &str) -> Source {
        Source {
            name: name.to_owned(),
            user: None,
            host: None,
            account: None,
        }
    }

    fn project(event: SemanticEvent) -> CompactEvent {
        let irc = IrcEvent {
            cursor: EventCursor {
                stream_id: "stream".into(),
                sequence: 1,
            },
            agent_id: AgentId::new(),
            direction: EventDirection::Outbound,
            class: EventClass::ChannelState,
            origin: EventOrigin::Live,
            verbosity: EventVerbosity::Semantic,
            target: Some("#project".into()),
            server_time: None,
            received_at: Timestamp::now(),
            correlation: EventCorrelation::default(),
            semantic: Some(EventPayload::Irc(SemanticProjection::from(event))),
            wire: None,
            mentions_me: false,
            authored_by_me: true,
        };
        CompactEvent::project(&irc).expect("a channel-state event is conversational")
    }
}
