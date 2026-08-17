//! MCP tool input and output schemas.

use std::path::PathBuf;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    agent::{
        AgentId,
        journal::{
            EventClass, EventCursor, EventDirection, EventFilter, EventOrigin, EventPage,
            EventVerbosity,
        },
        state::{AgentState, MotdState},
    },
    dcc::{
        session::{DccKind, DccSession, DccSessionId, DccState},
        transfer::DestinationConflict,
    },
    irc::{
        correlation::CommandResult,
        registration::{NickConflictPolicy, Nickname},
        target::{ChannelName, Target},
        wire::Tag,
    },
    mcp::resources::ResourceUris,
};

/// Tool names exposed by the service. `irc.execute` covers other IRC commands.
#[cfg(test)]
pub const TOOL_NAMES: &[&str] = &[
    "irc.connect",
    "irc.disconnect",
    "irc.status",
    "irc.join",
    "irc.part",
    "irc.send",
    "irc.history",
    "irc.query",
    "irc.execute",
    "irc.events.read",
    "irc.dcc.chat.open",
    "irc.dcc.chat.send",
    "irc.dcc.send",
    "irc.dcc.accept",
    "irc.dcc.reject",
    "irc.dcc.cancel",
    "irc.dcc.list",
];

/// Input for initial guest registration.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConnectInput {
    /// Mythological-character nickname chosen by the MCP client.
    pub nickname: Nickname,
    /// Ordered caller-supplied mythological fallbacks.
    #[serde(default)]
    pub nickname_fallbacks: Vec<Nickname>,
    /// Suffix the requested name or fail after supplied candidates.
    #[serde(default)]
    pub nick_conflict_policy: NickConflictPolicy,
    /// Optional IRC username override.
    pub username: Option<String>,
    /// Optional IRC real-name override.
    pub real_name: Option<String>,
    /// Initial channels in addition to configured defaults.
    #[serde(default)]
    pub channels: Vec<ChannelName>,
}

/// Successful initial guest registration.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct ConnectOutput {
    /// New opaque in-memory routing handle.
    pub agent_id: AgentId,
    /// Final nickname accepted by Ergo.
    pub nickname: Nickname,
    /// Whether a fallback or suffix was used.
    pub nickname_adjusted: bool,
    /// Always true for a successful result.
    pub registered: bool,
    /// Complete initial MOTD in lossless and presentation forms.
    pub motd: MotdState,
    /// Stable links for subsequent reads.
    pub resources: ResourceUris,
}

/// Input for an operation on one agent.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentInput {
    /// Opaque handle returned by `irc.connect`.
    pub agent_id: AgentId,
}

/// Clean actor shutdown request.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DisconnectInput {
    /// Opaque handle returned by `irc.connect`.
    pub agent_id: AgentId,
    /// Optional IRC QUIT reason.
    pub reason: Option<String>,
}

/// Completed actor shutdown.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct DisconnectOutput {
    /// Actor that was removed.
    pub agent_id: AgentId,
    /// Whether the actor has stopped.
    pub disconnected: bool,
    /// Whether QUIT reached the socket writer.
    pub quit_sent: bool,
    /// Direct sessions closed during shutdown.
    pub dcc_sessions_closed: usize,
}

/// Complete status tool output.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct StatusOutput {
    /// Latest advisory actor state.
    pub state: AgentState,
    /// Number of exact capabilities seen in CAP.
    pub advertised_capabilities: usize,
    /// Number of active negotiated capabilities.
    pub negotiated_capabilities: usize,
    /// Current journal bounds.
    pub events: crate::agent::journal::JournalStats,
    /// Stable links for follow-up reads.
    pub resources: ResourceUris,
}

/// Join one channel.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JoinInput {
    /// Owning guest identity.
    pub agent_id: AgentId,
    /// Case-preserved channel name.
    pub channel: ChannelName,
    /// Optional channel key.
    pub key: Option<String>,
    /// Bounded completion deadline.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
}

/// Join result with a stable channel resource link.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct JoinOutput {
    /// Common correlated command result.
    pub result: CommandResult,
    /// Case-preserved requested channel.
    pub channel: ChannelName,
    /// Expanded channel resource URI.
    pub resource: String,
}

/// Part one channel.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PartInput {
    /// Owning guest identity.
    pub agent_id: AgentId,
    /// Case-preserved channel name.
    pub channel: ChannelName,
    /// Optional part reason.
    pub reason: Option<String>,
    /// Bounded completion deadline.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
}

/// Kind accepted by `irc.send`.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SendKind {
    /// IRC PRIVMSG.
    Privmsg,
    /// IRC NOTICE.
    Notice,
    /// CTCP ACTION inside PRIVMSG.
    Action,
    /// IRCv3 TAGMSG.
    Tagmsg,
}

/// Caller decision when text exceeds one IRC line.
#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MultilinePolicy {
    /// Fail unless multiline is negotiated.
    Require,
    /// Prefer multiline, otherwise split safely.
    #[default]
    Prefer,
    /// Split on UTF-8 boundaries when multiline is unavailable.
    Split,
    /// Reject any overlong operation.
    RejectIfTooLong,
}

/// Input for ergonomic message sending.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SendInput {
    /// Explicit guest identity.
    pub agent_id: AgentId,
    /// Nickname or channel.
    pub target: Target,
    /// PRIVMSG, NOTICE, ACTION, or TAGMSG.
    pub kind: SendKind,
    /// Text when permitted by kind.
    pub text: Option<String>,
    /// Caller-controlled client-only tags.
    #[serde(default)]
    pub tags: Vec<Tag>,
    /// Server message ID being replied to.
    pub reply_to: Option<String>,
    /// Long-message behavior.
    #[serde(default)]
    pub multiline: MultilinePolicy,
    /// Bounded completion deadline.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
}

/// Result of sending one logical message, which may require several lines.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct SendOutput {
    /// One result for each written line or one multiline operation.
    pub results: Vec<CommandResult>,
    /// Number of physical IRC messages written.
    pub line_count: usize,
}

/// An exact server-history anchor.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum HistoryAnchor {
    /// RFC 3339 server timestamp.
    Timestamp(String),
    /// Server-assigned message identifier.
    MessageId(String),
}

/// Region selected from server history.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum HistorySelector {
    /// Latest records.
    Latest,
    /// Records before one anchor.
    Before {
        /// Exclusive endpoint.
        anchor: HistoryAnchor,
    },
    /// Records after one anchor.
    After {
        /// Exclusive start point.
        anchor: HistoryAnchor,
    },
    /// Records around one anchor.
    Around {
        /// Center point.
        anchor: HistoryAnchor,
    },
    /// Records between two anchors.
    Between {
        /// Inclusive lower endpoint chosen by the server.
        start: HistoryAnchor,
        /// Inclusive upper endpoint chosen by the server.
        end: HistoryAnchor,
    },
}

/// Server-backed chat history request.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HistoryInput {
    /// Owning guest identity.
    pub agent_id: AgentId,
    /// Nickname or channel whose history is requested.
    pub target: Target,
    /// History region.
    pub selector: HistorySelector,
    /// Maximum returned events.
    #[serde(default = "default_event_limit")]
    pub limit: usize,
    /// Retain history state playback in addition to chat messages.
    #[serde(default)]
    pub include_non_message_events: bool,
    /// Bounded completion deadline.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
}

/// Quality of history support used for this request.
#[derive(Clone, Copy, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryAvailability {
    /// IRCv3 CHATHISTORY was used.
    Native,
    /// Ergo legacy HISTORY was used.
    Degraded,
    /// Neither mechanism is available.
    Unavailable,
}

/// Completed history operation.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct HistoryOutput {
    /// Compatibility level used.
    pub availability: HistoryAvailability,
    /// Correlated command result, absent when unavailable.
    pub result: Option<CommandResult>,
    /// History events projected from the reply batch.
    pub events: Vec<crate::agent::journal::IrcEvent>,
}

/// Typed common server query.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Query {
    /// WHOIS one nickname.
    Whois {
        /// Nickname to inspect.
        nickname: String,
    },
    /// WHOWAS one nickname.
    Whowas {
        /// Historical nickname.
        nickname: String,
    },
    /// WHO or WHOX mask.
    Who {
        /// WHO mask.
        mask: String,
        /// Optional WHOX field selector.
        fields: Option<String>,
    },
    /// NAMES for an optional channel list.
    Names {
        /// Channels; an empty list asks for the server default.
        channels: Vec<String>,
    },
    /// Server channel list with optional mask.
    List {
        /// Optional channel mask.
        mask: Option<String>,
    },
    /// Current channel topic.
    Topic {
        /// Channel whose topic is requested.
        channel: ChannelName,
    },
    /// User or channel modes.
    Mode {
        /// Nickname or channel.
        target: String,
        /// Optional mode-list selector such as `b`.
        mode: Option<String>,
    },
    /// ISON presence query.
    Ison {
        /// Nicknames to check.
        nicknames: Vec<String>,
    },
    /// USERHOST query.
    Userhost {
        /// Nicknames to inspect.
        nicknames: Vec<String>,
    },
    /// MONITOR status/list query.
    Monitor {
        /// Read-only MONITOR operation.
        operation: MonitorQuery,
    },
    /// Current server MOTD.
    Motd,
    /// Server software version.
    Version,
    /// Server time.
    Time,
    /// Server administrative information.
    Admin,
    /// Server information.
    Info,
    /// Network user counts.
    Lusers,
    /// Server statistics selector.
    Stats {
        /// Optional server statistics selector.
        selector: Option<String>,
    },
    /// Server links.
    Links {
        /// Optional server mask.
        mask: Option<String>,
    },
    /// HELP index or subject.
    Help {
        /// Optional command/help subject.
        subject: Option<String>,
    },
}

/// Read-only MONITOR query forms.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum MonitorQuery {
    /// Return the actor's complete server-side monitor list.
    List,
    /// Ask for online/offline status of explicit nicknames.
    Status {
        /// Nicknames whose current status is requested.
        nicknames: Vec<String>,
    },
}

/// Typed common-query request.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QueryInput {
    /// Owning guest identity.
    pub agent_id: AgentId,
    /// Typed query and its required arguments.
    pub query: Query,
    /// Bounded completion deadline.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
}

/// Completion behavior requested from `irc.execute`.
#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ResponseMode {
    /// Select a strategy from static and runtime protocol knowledge.
    #[default]
    Auto,
    /// Collect labeled replies for an otherwise unknown command.
    Collect,
    /// Return once the socket writer accepts the line.
    FireAndForget,
}

/// Structured compatibility escape hatch for every syntactically valid command.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExecuteInput {
    /// Explicit guest identity.
    pub agent_id: AgentId,
    /// IRC command, never a raw CRLF-delimited line.
    pub command: String,
    /// Middle parameters.
    #[serde(default)]
    pub params: Vec<String>,
    /// Optional trailing parameter.
    pub trailing: Option<String>,
    /// Caller tags; label and batch are reserved by the bridge.
    #[serde(default)]
    pub tags: Vec<Tag>,
    /// Completion collector mode.
    #[serde(default)]
    pub response_mode: ResponseMode,
    /// Bounded collector deadline.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
}

/// Cursor-based event read and optional long poll.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EventsReadInput {
    /// Explicit guest identity.
    pub agent_id: AgentId,
    /// Last cursor consumed by this caller.
    pub cursor: Option<EventCursor>,
    /// Maximum ordered events returned.
    #[serde(default = "default_event_limit")]
    pub limit: usize,
    /// Zero for non-blocking, positive for bounded long polling.
    #[serde(default)]
    pub wait_ms: u64,
    /// Optional gateway command identifier filter.
    pub command_id: Option<String>,
    /// Optional strongly typed event class filter.
    pub class: Option<EventClass>,
    /// Optional target nickname or channel filter.
    pub target: Option<Target>,
    /// Optional direction filter.
    pub direction: Option<EventDirection>,
    /// Optional provenance filter.
    pub origin: Option<EventOrigin>,
    /// Optional detail-level filter.
    pub verbosity: Option<EventVerbosity>,
}

impl EventsReadInput {
    /// Build the actor journal filter without duplicating matching logic.
    pub fn filter(&self) -> EventFilter {
        EventFilter {
            command_id: self.command_id.clone(),
            class: self.class,
            target: self.target.as_ref().map(Target::to_string),
            direction: self.direction,
            origin: self.origin,
            verbosity: self.verbosity,
        }
    }
}

/// Open an outbound DCC CHAT offer.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DccChatOpenInput {
    /// Owning guest identity.
    pub agent_id: AgentId,
    /// Peer nickname.
    pub target: Target,
    /// Prefer reverse negotiation.
    #[serde(default)]
    pub reverse: bool,
}

/// Send one line over an active DCC CHAT socket.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DccChatSendInput {
    /// Owning guest identity.
    pub agent_id: AgentId,
    /// Direct-session handle.
    pub dcc_session_id: DccSessionId,
    /// One logical line without CR, LF, or NUL.
    pub text: String,
}

/// Offer one local file through DCC SEND.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DccSendInput {
    /// Owning guest identity.
    pub agent_id: AgentId,
    /// Peer nickname.
    pub target: Target,
    /// Local path on the gateway host.
    pub source_path: PathBuf,
    /// Filename advertised to the peer; defaults to the source basename.
    pub filename: Option<String>,
    /// Prefer reverse negotiation.
    #[serde(default)]
    pub reverse: bool,
}

/// Accept one incoming direct offer.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DccAcceptInput {
    /// Owning guest identity.
    pub agent_id: AgentId,
    /// Offered session.
    pub dcc_session_id: DccSessionId,
    /// Required for SEND and omitted for CHAT.
    pub destination_path: Option<PathBuf>,
    /// Existing-file behavior for SEND.
    #[serde(default)]
    pub conflict: DestinationConflict,
}

/// Identify one existing DCC session.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DccSessionInput {
    /// Owning guest identity.
    pub agent_id: AgentId,
    /// Direct-session handle.
    pub dcc_session_id: DccSessionId,
}

/// Filter the actor's bounded DCC session table.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DccListInput {
    /// Owning guest identity.
    pub agent_id: AgentId,
    /// Optional lifecycle state.
    pub state: Option<DccState>,
    /// Optional data-plane kind.
    pub kind: Option<DccKind>,
    /// Optional exact case-preserved peer name.
    pub peer: Option<String>,
}

/// One DCC session mutation result.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct DccSessionOutput {
    /// Current session snapshot.
    pub session: DccSession,
}

/// Result of writing one DCC CHAT line.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct DccChatSendOutput {
    /// Direct-session handle.
    pub dcc_session_id: DccSessionId,
    /// Whether the bounded writer accepted the line.
    pub queued: bool,
}

/// DCC session-list result.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct DccListOutput {
    /// Matching snapshots in deterministic handle order.
    pub sessions: Vec<DccSession>,
}

/// Structured MCP tool error.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct ToolErrorOutput {
    /// Stable machine-readable category.
    pub kind: String,
    /// Safe human-readable explanation.
    pub message: String,
    /// Whether retrying after external state changes is normally safe.
    pub retriable: bool,
}

/// Event page returned by `irc.events.read`.
pub type EventsReadOutput = EventPage;

/// Default command deadline in milliseconds.
pub const fn default_timeout_ms() -> u64 {
    10_000
}

/// Default event page size.
pub const fn default_event_limit() -> usize {
    100
}
