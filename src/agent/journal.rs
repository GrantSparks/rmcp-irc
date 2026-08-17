//! Bounded, cursor-addressed event storage for one agent actor.

use std::collections::{BTreeSet, VecDeque};

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
        isupport::CaseMapping,
        semantic::{SemanticClass, SemanticEvent, SemanticProjection, Source},
        wire::WireMessage,
    },
    mcp::conversation::CompactEvent,
    time::Timestamp,
};

/// Stable event class understood by filters and clients.
#[derive(
    Clone, Copy, Debug, Deserialize, JsonSchema, Ord, PartialEq, Eq, PartialOrd, Serialize,
)]
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
    /// Whether this event is addressed to the owning agent. See
    /// [`IrcEvent::mentions_me`].
    pub mentions_me: bool,
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
    /// Whether this event is addressed to the owning agent: a private message
    /// or notice sent to it, or a channel message naming its current nickname.
    /// Always false for the agent's own echoed messages.
    #[serde(default)]
    pub mentions_me: bool,
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
    /// Keep only events addressed to this agent when true, or only events that
    /// are not when false.
    pub mentions_me: Option<bool>,
}

/// A newest-first window request over the journal.
///
/// Kept separate from [`EventFilter`], which is a tool input schema: a
/// conversational resource addresses a channel or peer by name and needs the
/// server's case mapping applied, which is gateway knowledge rather than
/// something a caller should have to spell exactly.
#[derive(Clone, Debug, Default)]
pub struct RecentQuery {
    /// Selection shared with cursor reads.
    pub filter: EventFilter,
    /// Case-folded target and the mapping used to fold it.
    pub folded_target: Option<(String, CaseMapping)>,
}

impl RecentQuery {
    /// Restrict the window to one channel or peer, compared case-insensitively.
    pub fn for_target(target: &str, case_mapping: CaseMapping) -> Self {
        Self {
            filter: EventFilter::default(),
            folded_target: Some((case_mapping.fold(target), case_mapping)),
        }
    }

    /// Restrict the window to events addressed to the owning agent.
    pub fn mentions() -> Self {
        Self {
            filter: EventFilter {
                mentions_me: Some(true),
                ..EventFilter::default()
            },
            folded_target: None,
        }
    }

    fn matches(&self, event: &IrcEvent) -> bool {
        self.folded_target
            .as_ref()
            .is_none_or(|(wanted, case_mapping)| {
                event
                    .target
                    .as_ref()
                    .is_some_and(|target| case_mapping.fold(target) == *wanted)
            })
            && self.filter.matches(event)
    }
}

/// The complete selection applied during one cursor read.
///
/// Kept separate from [`EventFilter`], which is a tool input schema. A watch
/// selects several targets and classes at once, compares targets with the
/// server's advertised `CASEMAPPING`, and — when the caller is reading a
/// compact window — wants records that have no conversational form left out of
/// the selection entirely rather than dropped after the read. Dropping them
/// afterwards is what used to advance a position over records the caller never
/// received. [`RecentQuery`] carries the same pre-folded target for
/// newest-first windows.
#[derive(Clone, Debug, Default)]
pub struct CursorQuery {
    /// Single-valued selection shared with the tool input schema.
    pub filter: EventFilter,
    /// Case-folded targets to keep. Empty means every target.
    pub folded_targets: BTreeSet<String>,
    /// Mapping used to fold both those targets and each event's own target.
    pub case_mapping: CaseMapping,
    /// Event classes to keep. Empty means every class.
    pub classes: BTreeSet<EventClass>,
    /// Keep only records [`CompactEvent`] can project, so a read that returns
    /// compact events advances only over events it actually returned.
    pub conversational_only: bool,
}

impl CursorQuery {
    /// A read narrowed only by the single-valued tool filter.
    pub fn filtered(filter: EventFilter) -> Self {
        Self {
            filter,
            ..Self::default()
        }
    }

    /// Whether one retained event belongs to this read.
    pub fn selects(&self, event: &IrcEvent) -> bool {
        if !self.classes.is_empty() && !self.classes.contains(&event.class) {
            return false;
        }
        if !self.folded_targets.is_empty() {
            let Some(target) = event.target.as_ref() else {
                return false;
            };
            if !self
                .folded_targets
                .contains(&self.case_mapping.fold(target))
            {
                return false;
            }
        }
        if !self.filter.matches(event) {
            return false;
        }
        // Last, because it is the only test that has to build a projection.
        !self.conversational_only || CompactEvent::project(event).is_some()
    }
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
            && self
                .mentions_me
                .is_none_or(|value| event.mentions_me == value)
    }
}

/// Decide whether an inbound message is addressed to the agent itself.
///
/// A private message or notice sent directly to the agent always counts. In a
/// channel, the nickname must appear as a whole token, so `Theseus` is a
/// mention but `Theseusian` is not. Comparison folds case using the server's
/// advertised `CASEMAPPING`, and the agent's own echoed messages never count.
pub fn addresses_nickname(
    event: &SemanticEvent,
    nickname: &str,
    case_mapping: CaseMapping,
) -> bool {
    if nickname.is_empty() {
        return false;
    }
    let folded_nick = case_mapping.fold(nickname);
    let is_self = |source: &Source| case_mapping.fold(&source.name) == folded_nick;

    let (source, target, text) = match event {
        SemanticEvent::MessageChannel {
            source,
            channel,
            text,
        } => (source, channel.as_str().to_string(), text),
        SemanticEvent::MessagePrivate {
            source,
            target,
            text,
        }
        | SemanticEvent::MessageAction {
            source,
            target,
            text,
        }
        | SemanticEvent::MessageNotice {
            source,
            target,
            text,
        } => (source, target.clone(), text),
        _ => return false,
    };

    if is_self(source) {
        return false;
    }
    // Addressed straight at us: the target is our nickname, not a channel.
    if case_mapping.fold(&target) == folded_nick {
        return true;
    }
    names_nickname(text, &folded_nick, case_mapping)
}

/// Whether `text` contains `folded_nick` as a whole token.
fn names_nickname(text: &str, folded_nick: &str, case_mapping: CaseMapping) -> bool {
    // RFC 2812 nickname characters, plus the specials servers commonly allow.
    let is_nick_char =
        |character: char| character.is_alphanumeric() || "[]\\`_^{|}-".contains(character);
    // Scan the folded text throughout, so match offsets always index the same
    // string they came from.
    let folded_text = case_mapping.fold(text);
    folded_text
        .match_indices(folded_nick)
        .any(|(index, matched)| {
            let before = folded_text[..index].chars().next_back();
            let after = folded_text[index + matched.len()..].chars().next();
            before.is_none_or(|character| !is_nick_char(character))
                && after.is_none_or(|character| !is_nick_char(character))
        })
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
    /// Cursor callers should supply on their next read. This advances only
    /// over events present in `events`, so a filtered read never consumes what
    /// its filter excluded and the cursor stays safe to reuse across filters.
    pub next_cursor: EventCursor,
    /// Whether at least one further event matching *this read's own selection*
    /// is retained past `next_cursor`.
    ///
    /// A read is truncated only by `limit`, never by an uninteresting record in
    /// the way, so this is true exactly when reading again from `next_cursor`
    /// right now would return more. It is never a statement about records the
    /// selection excluded, which means a caller can drain a backlog with
    /// `while has_more` and always make progress.
    pub has_more: bool,
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
            mentions_me: event.mentions_me,
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
        query: &CursorQuery,
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

        let mut selected = self
            .events
            .iter()
            .map(|(event, _)| event)
            .filter(|event| event.cursor.sequence > after_sequence)
            .filter(|event| query.selects(event));
        let events: Vec<IrcEvent> = selected.by_ref().take(limit).cloned().collect();
        // Reported rather than derived from `latest`: "records exist past the
        // cursor" would be true whenever the selection declined the rest of the
        // window, and a caller draining on that signal would re-read the same
        // empty window forever. Asking the same selection for one more event
        // instead makes `has_more` mean "another read returns something".
        let has_more = selected.next().is_some();
        // Advance only over events actually returned. A filter narrows the view;
        // it must never consume what it excluded, because the same caller may
        // read again through a different filter and would otherwise never see
        // those events. Rescanning is bounded by the journal's own capacity.
        let next_sequence = events
            .last()
            .map_or(after_sequence, |event| event.cursor.sequence);
        let next_cursor = EventCursor {
            stream_id: self.stream_id.clone(),
            sequence: next_sequence.min(latest.sequence),
        };

        EventPage {
            stream_id: self.stream_id.clone(),
            requested_cursor: cursor.cloned(),
            status,
            oldest_available,
            latest,
            events,
            next_cursor,
            has_more,
        }
    }

    /// The most recently appended event, if any is still retained.
    pub fn latest_event(&self) -> Option<&IrcEvent> {
        self.events.back().map(|(event, _)| event)
    }

    /// Return the newest matching retained events, oldest-first.
    ///
    /// [`Self::read`] answers "what comes after this cursor", which starts at
    /// the oldest retained record when the caller has no cursor. A preview
    /// window wants the opposite end: once more than `limit` records are
    /// retained, the interesting ones are the most recent. Taking from the back
    /// and reversing keeps the returned slice in the same ascending order as
    /// every other event list, so callers never have to special-case it.
    pub fn read_latest(&self, limit: usize, query: &RecentQuery) -> Vec<IrcEvent> {
        let mut newest: Vec<IrcEvent> = self
            .events
            .iter()
            .rev()
            .map(|(event, _)| event)
            .filter(|event| query.matches(event))
            .take(limit)
            .cloned()
            .collect();
        newest.reverse();
        newest
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
            mentions_me: false,
        }
    }

    fn channel_message(sender: &str, text: &str) -> SemanticEvent {
        SemanticEvent::MessageChannel {
            source: Source {
                name: sender.into(),
                user: None,
                host: None,
                account: None,
            },
            channel: "#control".parse().expect("fixture channel"),
            text: text.into(),
        }
    }

    #[test]
    fn a_channel_message_naming_the_nickname_is_a_mention() {
        let mapping = CaseMapping::Rfc1459;
        for text in [
            "Theseus: please pick this up",
            "cc theseus",
            "ping THESEUS!",
            "(theseus) look here",
        ] {
            assert!(
                addresses_nickname(&channel_message("grant", text), "Theseus", mapping),
                "expected a mention in {text:?}"
            );
        }
    }

    #[test]
    fn a_nickname_inside_a_longer_word_is_not_a_mention() {
        let mapping = CaseMapping::Rfc1459;
        for text in [
            "theseusian ships",
            "atheseus",
            "Theseus_two is someone else",
        ] {
            assert!(
                !addresses_nickname(&channel_message("grant", text), "Theseus", mapping),
                "expected no mention in {text:?}"
            );
        }
    }

    #[test]
    fn an_agents_own_echoed_message_never_mentions_itself() {
        assert!(!addresses_nickname(
            &channel_message("Theseus", "Theseus reporting in"),
            "Theseus",
            CaseMapping::Rfc1459,
        ));
    }

    #[test]
    fn a_private_message_is_a_mention_without_naming_the_nickname() {
        let direct = SemanticEvent::MessagePrivate {
            source: Source {
                name: "grant".into(),
                user: None,
                host: None,
                account: None,
            },
            target: "Theseus".into(),
            text: "no nickname in this body at all".into(),
        };
        assert!(addresses_nickname(&direct, "Theseus", CaseMapping::Rfc1459));
    }

    #[test]
    fn mentions_fold_case_using_the_servers_mapping() {
        // Under RFC 1459 the bracket characters fold onto the brace forms, so a
        // nickname spelled either way is the same person.
        let event = channel_message("grant", "nudging thes{us");
        assert!(addresses_nickname(&event, "Thes[us", CaseMapping::Rfc1459));
        assert!(!addresses_nickname(&event, "Thes[us", CaseMapping::Ascii));
    }

    #[test]
    fn the_mention_filter_selects_only_addressed_events() {
        let agent_id = AgentId::new();
        let mut journal = EventJournal::new(8, 16 * 1024);
        journal
            .push(event(&agent_id, EventClass::MessageChannel))
            .expect("ordinary channel traffic");
        let addressed = journal
            .push(NewEvent {
                mentions_me: true,
                ..event(&agent_id, EventClass::MessageChannel)
            })
            .expect("addressed");

        let page = journal.read(
            None,
            10,
            &CursorQuery::filtered(EventFilter {
                mentions_me: Some(true),
                ..EventFilter::default()
            }),
        );
        assert_eq!(page.events.len(), 1);
        assert_eq!(page.events[0].cursor, addressed);
        assert!(page.events[0].mentions_me);
    }

    #[test]
    fn the_recent_window_reads_the_newest_events_rather_than_the_oldest() {
        let agent = AgentId::new();
        let mut journal = EventJournal::new(64, 1_000_000);
        for _ in 0..20 {
            journal
                .push(event(&agent, EventClass::MessageChannel))
                .expect("push");
        }
        let latest = journal.stats().latest.sequence;

        let recent = journal.read_latest(5, &RecentQuery::default());
        assert_eq!(recent.len(), 5);
        // Ascending order is preserved, and the window ends at the newest
        // record rather than starting at the oldest retained one.
        assert_eq!(recent.last().expect("newest").cursor.sequence, latest);
        assert_eq!(
            recent.first().expect("oldest shown").cursor.sequence,
            latest - 4
        );

        let cursorless_page = journal.read(None, 5, &CursorQuery::default());
        assert_eq!(
            cursorless_page
                .events
                .first()
                .expect("first")
                .cursor
                .sequence,
            1,
            "a cursor-less read still starts at the beginning, which is why the \
             preview needs its own newest-first read"
        );
    }

    #[test]
    fn a_recent_window_can_select_one_target_case_insensitively() {
        let agent = AgentId::new();
        let mut journal = EventJournal::new(64, 1_000_000);
        for target in ["#Control", "#other", "#control"] {
            let mut record = event(&agent, EventClass::MessageChannel);
            record.target = Some(target.to_owned());
            journal.push(record).expect("push");
        }

        let window = journal.read_latest(
            10,
            &RecentQuery::for_target("#CONTROL", CaseMapping::default()),
        );
        assert_eq!(window.len(), 2);
        assert!(
            window
                .iter()
                .all(|event| event.target.as_deref() != Some("#other"))
        );
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

        let page = journal.read(Some(&first), 10, &CursorQuery::default());
        assert_eq!(page.status, CursorStatus::Current);
        assert_eq!(page.events.len(), 2);

        let before_first = EventCursor {
            stream_id: first.stream_id,
            sequence: 0,
        };
        let page = journal.read(Some(&before_first), 10, &CursorQuery::default());
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
                .read(Some(&cursor), 10, &CursorQuery::default())
                .status,
            CursorStatus::StreamReset
        );
    }

    #[test]
    fn a_filtered_read_never_consumes_non_matching_events() {
        let agent_id = AgentId::new();
        let mut journal = EventJournal::new(4, 16 * 1024);
        journal
            .push(event(&agent_id, EventClass::ProtocolReply))
            .expect("leading reply");
        let wanted = journal
            .push(event(&agent_id, EventClass::MessagePrivate))
            .expect("wanted");
        let trailing = journal
            .push(event(&agent_id, EventClass::ProtocolReply))
            .expect("trailing reply");

        let private = CursorQuery::filtered(EventFilter {
            class: Some(EventClass::MessagePrivate),
            ..EventFilter::default()
        });

        let page = journal.read(None, 1, &private);
        assert_eq!(page.events.len(), 1);
        assert_eq!(page.next_cursor, wanted);
        assert!(
            !page.has_more,
            "the trailing non-match is not something this read would return"
        );

        // Nothing further matches, so the cursor must stay put rather than
        // racing ahead over the trailing non-match.
        let page = journal.read(Some(&wanted), 1, &private);
        assert!(page.events.is_empty());
        assert_eq!(page.next_cursor, wanted);

        // The regression that matters: the same cursor read through a
        // different filter must still see the event the first filter skipped.
        let page = journal.read(Some(&wanted), 10, &CursorQuery::default());
        assert_eq!(page.events.len(), 1);
        assert_eq!(page.events[0].cursor, trailing);
        assert_eq!(page.next_cursor, trailing);
    }

    #[test]
    fn an_unfiltered_read_advances_only_to_the_last_returned_event() {
        let agent_id = AgentId::new();
        let mut journal = EventJournal::new(8, 16 * 1024);
        journal
            .push(event(&agent_id, EventClass::ProtocolReply))
            .expect("first");
        let second = journal
            .push(event(&agent_id, EventClass::ProtocolReply))
            .expect("second");
        journal
            .push(event(&agent_id, EventClass::ProtocolReply))
            .expect("third");

        let page = journal.read(None, 2, &CursorQuery::default());
        assert_eq!(page.events.len(), 2);
        assert_eq!(page.next_cursor, second);
        assert_eq!(page.latest.sequence, 3);
        assert!(page.has_more, "the third record is still waiting");
    }

    #[test]
    fn has_more_speaks_only_about_events_the_same_read_would_return() {
        let agent_id = AgentId::new();
        let mut journal = EventJournal::new(16, 64 * 1024);
        let wanted = journal
            .push(event(&agent_id, EventClass::MessagePrivate))
            .expect("wanted");
        for _ in 0..5 {
            journal
                .push(event(&agent_id, EventClass::ProtocolReply))
                .expect("uninteresting");
        }

        let private = CursorQuery::filtered(EventFilter {
            class: Some(EventClass::MessagePrivate),
            ..EventFilter::default()
        });
        let page = journal.read(None, 10, &private);
        assert_eq!(page.next_cursor, wanted);
        assert!(
            page.next_cursor.sequence < page.latest.sequence,
            "later records exist, which is exactly what has_more must not report"
        );
        assert!(
            !page.has_more,
            "reading again would return nothing, so a draining caller must stop"
        );
    }

    #[test]
    fn a_selection_reaches_matches_beyond_a_page_of_uninteresting_records() {
        let agent_id = AgentId::new();
        let mut journal = EventJournal::new(64, 256 * 1024);
        for _ in 0..40 {
            let mut record = event(&agent_id, EventClass::MessageChannel);
            record.target = Some("#elsewhere".into());
            journal.push(record).expect("uninteresting");
        }
        let mut wanted = event(&agent_id, EventClass::MessageChannel);
        wanted.target = Some("#Control".into());
        let wanted = journal.push(wanted).expect("wanted");

        // A single-event limit must not stop the scan at the first page of raw
        // records: the selection is applied while reading, so uninteresting
        // records can never keep a match out of reach or block the cursor.
        let query = CursorQuery {
            folded_targets: BTreeSet::from([CaseMapping::default().fold("#control")]),
            ..CursorQuery::default()
        };
        let page = journal.read(None, 1, &query);
        assert_eq!(page.events.len(), 1);
        assert_eq!(page.events[0].cursor, wanted);
        assert_eq!(page.next_cursor, wanted);
        assert!(!page.has_more);
    }

    #[test]
    fn a_conversational_read_never_advances_over_a_record_it_cannot_project() {
        let agent_id = AgentId::new();
        let mut journal = EventJournal::new(16, 64 * 1024);
        // A numeric reply carries no conversational form, so a compact read
        // must decline it as part of its selection rather than skip it after
        // the fact and drag the cursor along.
        journal
            .push(event(&agent_id, EventClass::ProtocolReply))
            .expect("numeric");

        let compact = CursorQuery {
            conversational_only: true,
            ..CursorQuery::default()
        };
        let page = journal.read(None, 10, &compact);
        assert!(page.events.is_empty());
        assert_eq!(page.next_cursor.sequence, 0);
        assert!(!page.has_more);

        let lossless = journal.read(None, 10, &CursorQuery::default());
        assert_eq!(lossless.events.len(), 1);
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
            mentions_me: false,
        };
        let mut journal = EventJournal::new(4, 4096);
        journal.push(event).expect("push");
        let stored = journal
            .read(None, 1, &CursorQuery::default())
            .events
            .remove(0);
        let json = serde_json::to_value(&stored).expect("serialize");
        assert_eq!(json["received_at"], "2026-08-17T10:00:00.021Z");
        assert_eq!(json["server_time"], "2026-08-17T10:00:00.000Z");
    }
}
