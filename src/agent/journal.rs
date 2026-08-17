//! Bounded, cursor-addressed event storage for one agent actor.

use std::collections::VecDeque;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    agent::{AgentId, state::ConnectionState, state::MotdState},
    dcc::{
        session::{DccSession, DccSessionId},
        transfer::TransferProgress,
    },
    irc::{
        codec::MalformedReason,
        semantic::{SemanticClass, SemanticProjection},
        wire::WireMessage,
    },
    time::Timestamp,
};

/// Stable event class understood by filters and clients.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
pub enum EventClass {
    /// Channel message.
    #[serde(rename = "message.channel")]
    MessageChannel,
    /// Private message.
    #[serde(rename = "message.private")]
    MessagePrivate,
    /// CTCP ACTION message.
    #[serde(rename = "message.action")]
    MessageAction,
    /// IRC NOTICE message.
    #[serde(rename = "message.notice")]
    MessageNotice,
    /// Tag-only message.
    #[serde(rename = "message.tagged")]
    MessageTagged,
    /// CTCP query or reply.
    #[serde(rename = "ctcp")]
    Ctcp,
    /// Membership transition.
    #[serde(rename = "membership")]
    Membership,
    /// Presence transition.
    #[serde(rename = "presence")]
    Presence,
    /// Channel state transition.
    #[serde(rename = "channel.state")]
    ChannelState,
    /// Numeric or standard reply.
    #[serde(rename = "protocol.reply")]
    ProtocolReply,
    /// Capability or ISUPPORT transition.
    #[serde(rename = "protocol.compatibility")]
    ProtocolCompatibility,
    /// Protocol input without a typed projection.
    #[serde(rename = "protocol.unknown")]
    ProtocolUnknown,
    /// Connection lifecycle transition.
    #[serde(rename = "connection.lifecycle")]
    ConnectionLifecycle,
    /// Complete server MOTD.
    #[serde(rename = "server.motd")]
    ServerMotd,
    /// Incoming direct-chat offer.
    #[serde(rename = "dcc.chat.offered")]
    DccChatOffered,
    /// DCC CTCP negotiation that is further classified by a paired event.
    #[serde(rename = "dcc.control")]
    DccControl,
    /// Incoming file-transfer offer.
    #[serde(rename = "dcc.transfer.offered")]
    DccTransferOffered,
    /// Direct socket became active.
    #[serde(rename = "dcc.connected")]
    DccConnected,
    /// Direct chat line.
    #[serde(rename = "dcc.chat.message")]
    DccChatMessage,
    /// Direct chat peer closed cleanly.
    #[serde(rename = "dcc.chat.closed")]
    DccChatClosed,
    /// File-transfer progress.
    #[serde(rename = "dcc.transfer.progress")]
    DccTransferProgress,
    /// File transfer completed.
    #[serde(rename = "dcc.transfer.completed")]
    DccTransferCompleted,
    /// Direct session was rejected.
    #[serde(rename = "dcc.rejected")]
    DccRejected,
    /// Direct session was cancelled.
    #[serde(rename = "dcc.cancelled")]
    DccCancelled,
    /// Direct session failed.
    #[serde(rename = "dcc.failed")]
    DccFailed,
}

impl From<SemanticClass> for EventClass {
    fn from(value: SemanticClass) -> Self {
        match value {
            SemanticClass::MessageChannel => Self::MessageChannel,
            SemanticClass::MessagePrivate => Self::MessagePrivate,
            SemanticClass::MessageAction => Self::MessageAction,
            SemanticClass::MessageNotice => Self::MessageNotice,
            SemanticClass::MessageTagged => Self::MessageTagged,
            SemanticClass::Membership => Self::Membership,
            SemanticClass::Presence => Self::Presence,
            SemanticClass::ChannelState => Self::ChannelState,
            SemanticClass::ProtocolReply => Self::ProtocolReply,
            SemanticClass::ProtocolCompatibility => Self::ProtocolCompatibility,
            SemanticClass::ServerMotd => Self::ServerMotd,
            SemanticClass::ProtocolUnknown => Self::ProtocolUnknown,
            SemanticClass::ConnectionLifecycle => Self::ConnectionLifecycle,
            SemanticClass::Ctcp => Self::Ctcp,
            SemanticClass::Dcc => Self::DccControl,
        }
    }
}

/// Stable position in one process-local event stream.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize)]
pub struct EventCursor {
    /// Random identity of the actor journal.
    pub stream_id: String,
    /// Monotonic event sequence within the stream.
    pub sequence: u64,
}

/// Direction of an event relative to the gateway.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventDirection {
    /// Received from Ergo or a DCC peer.
    Inbound,
    /// Sent toward Ergo or a DCC peer.
    Outbound,
    /// Produced by the gateway itself.
    Internal,
}

/// Provenance of an event.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventOrigin {
    /// Observed on a live connection.
    Live,
    /// Recovered from IRC history.
    History,
    /// Synthesized because no authoritative echo was available.
    Synthetic,
    /// Produced by gateway lifecycle handling.
    Gateway,
}

/// Detail level represented by an event.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventVerbosity {
    /// A typed semantic projection is available.
    Semantic,
    /// Only the lossless protocol representation is understood.
    Wire,
}

/// Structured payload carried by one journal event.
///
/// The enum is untagged so variants serialize as their payload object without
/// an additional discriminator.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(untagged)]
pub enum EventPayload {
    /// Semantic projection of an IRC line.
    Irc(SemanticProjection),
    /// Server message of the day, collected or requeried.
    Motd(MotdState),
    /// Connection lifecycle transition.
    Connection(ConnectionEvent),
    /// A DCC session snapshot.
    DccSession(DccSession),
    /// Progress of a DCC transfer.
    DccProgress(TransferProgress),
    /// One line exchanged over an established DCC CHAT session.
    DccChatMessage(DccChatMessage),
    /// A DCC operation that failed.
    DccFailure(DccFailure),
    /// A line the framing layer refused.
    MalformedLine(MalformedLine),
}

/// One line exchanged over a DCC CHAT session.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct DccChatMessage {
    /// Session the line belongs to.
    pub session_id: DccSessionId,
    /// Whether this gateway received or sent it.
    pub direction: EventDirection,
    /// Line text without its terminator.
    pub text: String,
}

/// A DCC operation that could not be completed.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct DccFailure {
    /// Peer involved, when one is known.
    pub peer: Option<String>,
    /// Human-readable failure text.
    pub error: String,
}

/// A line the framing layer refused, retained for diagnosis.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct MalformedLine {
    /// Why the line was refused.
    pub reason: MalformedReason,
    /// Bytes observed before the line was discarded.
    pub observed_bytes_base64: String,
}

impl From<SemanticProjection> for EventPayload {
    fn from(projection: SemanticProjection) -> Self {
        Self::Irc(projection)
    }
}

/// Connection lifecycle payload, matching its published object form.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
pub struct ConnectionEvent {
    /// New connection state.
    pub state: ConnectionState,
}

/// Part a correlated line plays in one command's exchange.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CorrelationRole {
    /// The line this gateway wrote.
    Request,
    /// A line the server sent in answer to it.
    Response,
}

/// Correlation metadata retained after a command collector completes.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
pub struct EventCorrelation {
    /// Gateway command identifier.
    pub command_id: Option<String>,
    /// IRCv3 label.
    pub label: Option<String>,
    /// Role of the line in the correlated response.
    pub role: Option<CorrelationRole>,
}

/// Event data before the journal assigns a cursor.
#[derive(Clone, Debug)]
pub struct NewEvent {
    /// Owning guest agent.
    pub agent_id: AgentId,
    /// Inbound, outbound, or gateway-internal.
    pub direction: EventDirection,
    /// Stable semantic class, such as `message.channel`.
    pub class: EventClass,
    /// Live, history, synthetic, or gateway-generated.
    pub origin: EventOrigin,
    /// Semantic or wire detail level.
    pub verbosity: EventVerbosity,
    /// Optional case-preserved channel or nickname used by filters.
    pub target: Option<String>,
    /// Server-provided time, when `server-time` is negotiated.
    pub server_time: Option<Timestamp>,
    /// Receipt time generated by the actor.
    pub received_at: Timestamp,
    /// Command correlation information.
    pub correlation: EventCorrelation,
    /// Optional typed payload.
    pub semantic: Option<EventPayload>,
    /// Complete parsed wire data when the event came from IRC.
    pub wire: Option<WireMessage>,
}

/// Journaled event returned to MCP callers.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct IrcEvent {
    /// Position assigned by the journal.
    pub cursor: EventCursor,
    /// Owning guest agent.
    pub agent_id: AgentId,
    /// Inbound, outbound, or gateway-internal.
    pub direction: EventDirection,
    /// Stable semantic/protocol class.
    pub class: EventClass,
    /// Event provenance.
    pub origin: EventOrigin,
    /// Semantic or wire detail level.
    pub verbosity: EventVerbosity,
    /// Optional case-preserved channel or nickname.
    pub target: Option<String>,
    /// Server-provided time, when `server-time` is negotiated.
    pub server_time: Option<Timestamp>,
    /// Receipt time.
    pub received_at: Timestamp,
    /// Command correlation information.
    pub correlation: EventCorrelation,
    /// Optional typed payload.
    pub semantic: Option<EventPayload>,
    /// Lossless wire representation when applicable.
    pub wire: Option<WireMessage>,
}

/// Optional filters applied during a cursor read.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema)]
pub struct EventFilter {
    /// Gateway command identifier that owns the event.
    pub command_id: Option<String>,
    /// Exact event class.
    pub class: Option<EventClass>,
    /// Exact case-preserved target.
    pub target: Option<String>,
    /// Event direction.
    pub direction: Option<EventDirection>,
    /// Event provenance.
    pub origin: Option<EventOrigin>,
    /// Detail level.
    pub verbosity: Option<EventVerbosity>,
}

impl EventFilter {
    fn matches(&self, event: &IrcEvent) -> bool {
        self.command_id
            .as_ref()
            .is_none_or(|command_id| event.correlation.command_id.as_ref() == Some(command_id))
            && self
                .class
                .as_ref()
                .is_none_or(|class| event.class == *class)
            && self
                .target
                .as_ref()
                .is_none_or(|target| event.target.as_ref() == Some(target))
            && self.direction.is_none_or(|value| event.direction == value)
            && self.origin.is_none_or(|value| event.origin == value)
            && self.verbosity.is_none_or(|value| event.verbosity == value)
    }
}

/// Relationship between a requested cursor and retained events.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CursorStatus {
    /// The cursor belongs to this stream and no retained events were skipped.
    Current,
    /// The cursor belongs to another stream or is ahead of this one.
    StreamReset,
    /// Events after the cursor were evicted.
    EventGap,
}

/// One cursor-based journal read.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct EventPage {
    /// Current actor stream.
    pub stream_id: String,
    /// Cursor supplied by the caller.
    pub requested_cursor: Option<EventCursor>,
    /// Relationship of that cursor to the retained window.
    pub status: CursorStatus,
    /// Oldest retained event, if any.
    pub oldest_available: Option<EventCursor>,
    /// Latest assigned event position, including an empty stream at sequence zero.
    pub latest: EventCursor,
    /// Ordered matching retained events.
    pub events: Vec<IrcEvent>,
    /// Cursor callers should supply on their next read.
    pub next_cursor: EventCursor,
}

/// Bounded journal measurements exposed by the event resource.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct JournalStats {
    /// Current stream identifier.
    pub stream_id: String,
    /// Oldest retained cursor.
    pub oldest_available: Option<EventCursor>,
    /// Latest assigned cursor.
    pub latest: EventCursor,
    /// Number of retained records.
    pub retained_events: usize,
    /// Approximate serialized bytes retained.
    pub retained_bytes: usize,
}

/// Failure to retain an event within configured bounds.
#[derive(Debug, thiserror::Error)]
pub enum JournalError {
    /// One event is larger than the complete byte budget.
    #[error("event requires {actual} bytes but the journal permits {limit}")]
    EventTooLarge {
        /// Serialized event size.
        actual: usize,
        /// Configured journal byte budget.
        limit: usize,
    },
    /// Event size estimation unexpectedly failed.
    #[error("could not serialize event for journal sizing: {0}")]
    Serialization(#[from] serde_json::Error),
}

/// Bounded journal with wake-up notifications but no per-reader state.
#[derive(Debug)]
pub struct EventJournal {
    stream_id: String,
    next_sequence: u64,
    max_events: usize,
    max_bytes: usize,
    retained_bytes: usize,
    events: VecDeque<(IrcEvent, usize)>,
}

impl EventJournal {
    /// Create an empty journal with a fresh stream identity.
    pub fn new(max_events: usize, max_bytes: usize) -> Self {
        assert!(max_events > 0, "event count bound must be non-zero");
        assert!(max_bytes > 0, "event byte bound must be non-zero");
        let stream_id = Uuid::new_v4().to_string();
        Self {
            stream_id,
            next_sequence: 1,
            max_events,
            max_bytes,
            retained_bytes: 0,
            events: VecDeque::new(),
        }
    }

    /// Append an event and evict oldest records until both bounds hold.
    pub fn push(&mut self, event: NewEvent) -> Result<EventCursor, JournalError> {
        let cursor = EventCursor {
            stream_id: self.stream_id.clone(),
            sequence: self.next_sequence,
        };
        let event = IrcEvent {
            cursor: cursor.clone(),
            agent_id: event.agent_id,
            direction: event.direction,
            class: event.class,
            origin: event.origin,
            verbosity: event.verbosity,
            target: event.target,
            server_time: event.server_time,
            received_at: event.received_at,
            correlation: event.correlation,
            semantic: event.semantic,
            wire: event.wire,
        };
        let raw_bytes = event.wire.as_ref().map_or(0, |wire| wire.raw_bytes.len());
        let bytes = serde_json::to_vec(&event)?.len().saturating_add(raw_bytes);
        if bytes > self.max_bytes {
            return Err(JournalError::EventTooLarge {
                actual: bytes,
                limit: self.max_bytes,
            });
        }

        self.next_sequence = self.next_sequence.saturating_add(1);
        self.retained_bytes = self.retained_bytes.saturating_add(bytes);
        self.events.push_back((event, bytes));
        while self.events.len() > self.max_events || self.retained_bytes > self.max_bytes {
            if let Some((_, removed_bytes)) = self.events.pop_front() {
                self.retained_bytes = self.retained_bytes.saturating_sub(removed_bytes);
            }
        }
        Ok(cursor)
    }

    /// Read matching events after a caller-owned cursor.
    pub fn read(
        &self,
        cursor: Option<&EventCursor>,
        limit: usize,
        filter: &EventFilter,
    ) -> EventPage {
        let latest = self.latest_cursor();
        let oldest_available = self.events.front().map(|(event, _)| event.cursor.clone());
        let oldest_sequence = oldest_available
            .as_ref()
            .map_or(self.next_sequence, |cursor| cursor.sequence);

        let (status, after_sequence) = match cursor {
            None => (CursorStatus::Current, oldest_sequence.saturating_sub(1)),
            Some(requested) if requested.stream_id != self.stream_id => {
                (CursorStatus::StreamReset, oldest_sequence.saturating_sub(1))
            }
            Some(requested) if requested.sequence > latest.sequence => {
                (CursorStatus::StreamReset, oldest_sequence.saturating_sub(1))
            }
            Some(requested) if requested.sequence.saturating_add(1) < oldest_sequence => {
                (CursorStatus::EventGap, oldest_sequence.saturating_sub(1))
            }
            Some(requested) => (CursorStatus::Current, requested.sequence),
        };

        let mut inspected_sequence = after_sequence;
        let events = self
            .events
            .iter()
            .filter(|(event, _)| event.cursor.sequence > after_sequence)
            .scan((), |(), (event, _)| {
                inspected_sequence = event.cursor.sequence;
                Some(event)
            })
            .filter(|event| filter.matches(event))
            .take(limit)
            .cloned()
            .collect();
        let next_cursor = EventCursor {
            stream_id: self.stream_id.clone(),
            sequence: inspected_sequence.min(latest.sequence),
        };

        EventPage {
            stream_id: self.stream_id.clone(),
            requested_cursor: cursor.cloned(),
            status,
            oldest_available,
            latest,
            events,
            next_cursor,
        }
    }

    /// Return bounded journal measurements.
    pub fn stats(&self) -> JournalStats {
        JournalStats {
            stream_id: self.stream_id.clone(),
            oldest_available: self.events.front().map(|(event, _)| event.cursor.clone()),
            latest: self.latest_cursor(),
            retained_events: self.events.len(),
            retained_bytes: self.retained_bytes,
        }
    }

    fn latest_cursor(&self) -> EventCursor {
        EventCursor {
            stream_id: self.stream_id.clone(),
            sequence: self.next_sequence.saturating_sub(1),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(agent_id: &AgentId, class: EventClass) -> NewEvent {
        NewEvent {
            agent_id: agent_id.clone(),
            direction: EventDirection::Internal,
            class,
            origin: EventOrigin::Gateway,
            verbosity: EventVerbosity::Semantic,
            target: None,
            server_time: None,
            received_at: "2026-08-16T00:00:00.000Z".parse().expect("fixture instant"),
            correlation: EventCorrelation::default(),
            semantic: None,
            wire: None,
        }
    }

    #[test]
    fn reports_eviction_only_when_an_unconsumed_event_was_lost() {
        let agent_id = AgentId::new();
        let mut journal = EventJournal::new(2, 16 * 1024);
        let first = journal
            .push(event(&agent_id, EventClass::MessagePrivate))
            .expect("first");
        journal
            .push(event(&agent_id, EventClass::MessageChannel))
            .expect("second");
        journal
            .push(event(&agent_id, EventClass::MessageNotice))
            .expect("third");

        let page = journal.read(Some(&first), 10, &EventFilter::default());
        assert_eq!(page.status, CursorStatus::Current);
        assert_eq!(page.events.len(), 2);

        let before_first = EventCursor {
            stream_id: first.stream_id,
            sequence: 0,
        };
        let page = journal.read(Some(&before_first), 10, &EventFilter::default());
        assert_eq!(page.status, CursorStatus::EventGap);
        assert_eq!(page.events[0].class, EventClass::MessageChannel);
    }

    #[test]
    fn reports_an_old_stream_as_reset() {
        let journal = EventJournal::new(2, 16 * 1024);
        let cursor = EventCursor {
            stream_id: "old".into(),
            sequence: 99,
        };
        assert_eq!(
            journal
                .read(Some(&cursor), 10, &EventFilter::default())
                .status,
            CursorStatus::StreamReset
        );
    }

    #[test]
    fn filtered_reads_advance_past_inspected_non_matches() {
        let agent_id = AgentId::new();
        let mut journal = EventJournal::new(4, 16 * 1024);
        journal
            .push(event(&agent_id, EventClass::ProtocolReply))
            .expect("skip");
        let wanted = journal
            .push(event(&agent_id, EventClass::MessagePrivate))
            .expect("wanted");
        journal
            .push(event(&agent_id, EventClass::ProtocolReply))
            .expect("skip");

        let page = journal.read(
            None,
            1,
            &EventFilter {
                class: Some(EventClass::MessagePrivate),
                ..EventFilter::default()
            },
        );
        assert_eq!(page.events.len(), 1);
        assert_eq!(page.next_cursor, wanted);

        let page = journal.read(
            Some(&wanted),
            1,
            &EventFilter {
                class: Some(EventClass::MessagePrivate),
                ..EventFilter::default()
            },
        );
        assert!(page.events.is_empty());
        assert_eq!(page.next_cursor.sequence, 3);
    }

    #[test]
    fn a_typed_payload_has_no_enum_discriminator() {
        let connection = EventPayload::Connection(ConnectionEvent {
            state: ConnectionState::Ready,
        });
        assert_eq!(
            serde_json::to_value(&connection).expect("serialize"),
            serde_json::json!({"state": "ready"})
        );

        let failure = EventPayload::DccFailure(DccFailure {
            peer: Some("alice".into()),
            error: "connect refused".into(),
        });
        assert_eq!(
            serde_json::to_value(&failure).expect("serialize"),
            serde_json::json!({"peer": "alice", "error": "connect refused"})
        );
    }

    #[test]
    fn instants_publish_as_rfc3339_text() {
        let event = NewEvent {
            agent_id: AgentId::new(),
            direction: EventDirection::Inbound,
            class: EventClass::MessageChannel,
            origin: EventOrigin::Live,
            verbosity: EventVerbosity::Semantic,
            target: None,
            server_time: Some("2026-08-17T10:00:00.000Z".parse().expect("instant")),
            received_at: "2026-08-17T10:00:00.021Z".parse().expect("instant"),
            correlation: EventCorrelation::default(),
            semantic: None,
            wire: None,
        };
        let mut journal = EventJournal::new(4, 4096);
        journal.push(event).expect("push");
        let stored = journal
            .read(None, 1, &EventFilter::default())
            .events
            .remove(0);
        let json = serde_json::to_value(&stored).expect("serialize");
        assert_eq!(json["received_at"], "2026-08-17T10:00:00.021Z");
        assert_eq!(json["server_time"], "2026-08-17T10:00:00.000Z");
    }
}
