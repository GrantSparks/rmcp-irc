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
    mcp::{resources::ResourceUris, watch::WatchId},
    time::Timestamp,
};

/// Amount of redundant protocol detail included directly in a tool result.
#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolResultDetail {
    /// Return the authoritative presentation/event form once, or retain
    /// lossless command replies with their duplicate semantic projection null.
    #[default]
    Compact,
    /// Retain every legacy raw and semantic projection directly in the tool
    /// result, even when equivalent data is repeated.
    Full,
}

/// Tool names exposed by the service. `irc.execute` covers other IRC commands.
#[cfg(test)]
pub const TOOL_NAMES: &[&str] = &[
    "irc.connect",
    "irc.attention.open",
    "irc.attention.check",
    "irc.disconnect",
    "irc.status",
    "irc.join",
    "irc.part",
    "irc.send",
    "irc.history",
    "irc.query",
    "irc.whois",
    "irc.names",
    "irc.list",
    "irc.mode.get",
    "irc.help",
    "irc.topic.get",
    "irc.topic.set",
    "irc.nick.set",
    "irc.away.set",
    "irc.kick",
    "irc.invite",
    "irc.monitor.update",
    "irc.mode.set",
    "irc.reaction.update",
    "irc.message.redact",
    "irc.read.get",
    "irc.read.set",
    "irc.typing.set",
    "irc.execute",
    "irc.events.read",
    "irc.watch.create",
    "irc.watch.close",
    "irc.dcc.chat.open",
    "irc.dcc.chat.send",
    "irc.dcc.send",
    "irc.dcc.accept",
    "irc.dcc.reject",
    "irc.dcc.cancel",
    "irc.dcc.list",
];

/// Input for initial guest registration.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectInput {
    /// Mythological-character nickname chosen by the MCP client.
    pub nickname: Nickname,
    /// Ordered caller-supplied mythological fallbacks.
    #[serde(default)]
    pub nickname_fallbacks: Vec<Nickname>,
    /// Nickname collision behavior: `suffix` (default), `fail`, or `elicit`.
    /// `elicit` abandons the attempt on the first rejection and returns an
    /// `input_required` question asking which nickname to register instead; it
    /// requires a request that declared form elicitation.
    #[serde(default)]
    pub nick_conflict_policy: NickConflictPolicy,
    /// Optional IRC username override.
    pub username: Option<String>,
    /// Optional IRC real-name override.
    pub real_name: Option<String>,
    /// Initial IRC channel names, such as `#control`, in addition to configured
    /// defaults.
    #[serde(default)]
    pub channels: Vec<ChannelName>,
    /// Result detail: `compact` (default) keeps the MOTD text but omits its
    /// duplicate line array and raw numerics; `full` returns the legacy
    /// lossless MOTD inline. The linked MOTD resource is always complete.
    #[serde(default)]
    pub result_detail: ToolResultDetail,
    /// How this agent's later tool results should carry the bounded activity
    /// hint: `{"enabled": true, "inline_mentions": 0}` by default. The
    /// preference is fixed here, for this agent, because a hint that any call
    /// could reshape would make results depend on the order they were made in.
    #[serde(default)]
    pub activity: crate::mcp::activity::ActivityPreference,
}

impl ConnectInput {
    /// The arguments a request state is bound to.
    ///
    /// The whole call. A retry re-sends its original arguments unchanged, so a
    /// caller that altered one of these is starting a different registration
    /// and must not redeem the state minted for the first.
    pub fn salient(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
    }
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
    /// Initial MOTD in the requested detail; joined presentation text is
    /// always retained and the linked resource is always lossless.
    pub motd: MotdState,
    /// Stable links for subsequent reads.
    pub resources: ResourceUris,
    /// Detail actually included in this result.
    pub result_detail: ToolResultDetail,
    /// Required next step for remaining responsive after the current model
    /// turn, including the honest scheduler token-cost boundary.
    pub attention: &'static str,
}

/// Input for an operation on one agent.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentInput {
    /// Opaque handle returned by `irc.connect`.
    pub agent_id: AgentId,
    /// Result detail: `compact` (default) keeps one MOTD text representation
    /// in state but omits duplicate lines and raw numerics; `full` returns the
    /// legacy state inline. The linked MOTD resource is always complete.
    #[serde(default)]
    pub result_detail: ToolResultDetail,
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

/// What the calling request declared about itself.
///
/// There is no handshake to inspect in this protocol, so a client that cannot
/// tell why the server withheld a feature has nowhere to look. This echoes the
/// declarations the current request actually arrived with, which is the
/// difference between "the server ignored my capability" and "my capability
/// never reached the server". These are self-reported values for diagnostics
/// only; they are never an authorization identity.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct CallerProfile {
    /// MCP protocol version declared in this request's `_meta`.
    pub protocol_version: Option<String>,
    /// Whether this request carried every `_meta` field the protocol requires.
    pub request_metadata_complete: bool,
    /// Extension identifiers declared in this request's client capabilities.
    pub extensions: Vec<String>,
    /// Whether the client declared it can answer a form-mode elicitation.
    pub form_elicitation: bool,
    /// Whether this request supplied a progress token. Progress notifications
    /// may only name a token an active request opted in with, so `false` means
    /// no progress can be sent for it.
    pub progress_requested: bool,
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
    /// Current journal bounds and its eviction accounting.
    pub events: crate::agent::journal::JournalStats,
    /// Stable links for follow-up reads.
    pub resources: ResourceUris,
    /// Detail actually included in this result.
    pub result_detail: ToolResultDetail,
    /// Capability picture of the request that asked. Absent when the status was
    /// produced outside a request, since it describes the caller rather than
    /// the guest.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub caller: Option<CallerProfile>,
}

/// Join one channel.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JoinInput {
    /// Opaque handle returned by `irc.connect`.
    pub agent_id: AgentId,
    /// Case-preserved channel name.
    pub channel: ChannelName,
    /// Optional channel key. Omitting it for a keyed channel is answered with
    /// an `input_required` question when the request declared form
    /// elicitation, and with the structured rejection otherwise.
    pub key: Option<String>,
    /// Milliseconds to wait for completion. Must be between 1 and the
    /// configured maximum, 30000 by default; anything else is rejected.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Result detail: `full` (default) retains lossless replies and their
    /// semantic projection; `compact` sets the duplicate projection to null.
    #[serde(default = "default_full_result_detail")]
    pub result_detail: ToolResultDetail,
}

impl JoinInput {
    /// The arguments a request state is bound to.
    ///
    /// The whole call, including the absent `key` that made the question
    /// necessary: a retry that supplied one directly is a different join and
    /// has no exchange to resume.
    pub fn salient(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
    }
}

/// Join result with a stable channel resource URI.
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
    /// Opaque handle returned by `irc.connect`.
    pub agent_id: AgentId,
    /// Case-preserved channel name.
    pub channel: ChannelName,
    /// Optional part reason.
    pub reason: Option<String>,
    /// Milliseconds to wait for completion. Must be between 1 and the
    /// configured maximum, 30000 by default; anything else is rejected.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Result detail: `full` (default) retains lossless replies and their
    /// semantic projection; `compact` sets the duplicate projection to null.
    #[serde(default = "default_full_result_detail")]
    pub result_detail: ToolResultDetail,
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
    /// IRCv3 TAGMSG, carrying tags only. Requires the `message-tags`
    /// capability.
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
    /// Opaque handle returned by `irc.connect`.
    pub agent_id: AgentId,
    /// Nickname or channel.
    pub target: Target,
    /// Message kind: `privmsg`, `notice`, `action`, or `tagmsg`.
    pub kind: SendKind,
    /// Message text. Required for `privmsg`, `notice`, and `action`; must be
    /// absent or empty for `tagmsg`, which carries only tags.
    pub text: Option<String>,
    /// Caller-controlled client-only tags.
    #[serde(default)]
    pub tags: Vec<Tag>,
    /// Server message ID being replied to.
    pub reply_to: Option<String>,
    /// Long-message behavior: `require`, `prefer` (default), `split`, or
    /// `reject_if_too_long`.
    #[serde(default)]
    pub multiline: MultilinePolicy,
    /// Milliseconds to wait for completion. Must be between 1 and the
    /// configured maximum, 30000 by default; anything else is rejected.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Result detail for every line: `full` (default) retains lossless replies
    /// and their semantic projection; `compact` sets the duplicate projection
    /// to null.
    #[serde(default = "default_full_result_detail")]
    pub result_detail: ToolResultDetail,
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
    /// Opaque handle returned by `irc.connect`.
    pub agent_id: AgentId,
    /// Nickname or channel whose history is requested.
    pub target: Target,
    /// Tagged history region such as `{"kind":"latest"}` or
    /// `{"kind":"before","anchor":{"kind":"timestamp","value":"..."}}`;
    /// kinds are `latest`, `before`, `after`, `around`, and `between`.
    pub selector: HistorySelector,
    /// Maximum returned events.
    #[serde(default = "default_event_limit")]
    pub limit: usize,
    /// Retain history state playback in addition to chat messages.
    #[serde(default)]
    pub include_non_message_events: bool,
    /// Milliseconds to wait for completion. Must be between 1 and the
    /// configured maximum, 30000 by default; anything else is rejected.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Result detail: `compact` (default) returns history records in `events`
    /// without repeating successful command replies and projections;
    /// `full` retains the legacy duplicates. Failed commands always retain
    /// their diagnostic replies.
    #[serde(default)]
    pub result_detail: ToolResultDetail,
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
    /// Detail actually included in this result. This is `full` after a command
    /// failure because diagnostic replies are never discarded.
    pub result_detail: ToolResultDetail,
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
    /// Opaque handle returned by `irc.connect`.
    pub agent_id: AgentId,
    /// Tagged query object whose `kind` is one of `whois`, `whowas`, `who`,
    /// `names`, `list`, `topic`, `mode`, `ison`, `userhost`, `monitor`, `motd`,
    /// `version`, `time`, `admin`, `info`, `lusers`, `stats`, `links`, or `help`;
    /// for example `{"kind":"names","channels":["#control"]}` or
    /// `{"kind":"topic","channel":"#control"}`.
    pub query: Query,
    /// Milliseconds to wait for completion. Must be between 1 and the
    /// configured maximum, 30000 by default; anything else is rejected.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Result detail: `full` (default) retains lossless replies and their
    /// semantic projection; `compact` sets the duplicate projection to null.
    #[serde(default = "default_full_result_detail")]
    pub result_detail: ToolResultDetail,
}

/// WHOIS one nickname through a command-specific MCP schema.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WhoisInput {
    /// Opaque handle returned by `irc.connect`.
    pub agent_id: AgentId,
    /// Nickname whose current server profile is requested.
    pub nickname: Nickname,
    /// Milliseconds to wait for the complete WHOIS reply sequence, between 1
    /// and the configured maximum of 30000 by default.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Amount of duplicate wire/semantic detail retained in `result`.
    #[serde(default = "default_full_result_detail")]
    pub result_detail: ToolResultDetail,
}

/// Useful fields projected from a WHOIS reply sequence.
#[derive(Clone, Debug, Default, JsonSchema, Serialize)]
pub struct WhoisProfile {
    /// Case-preserved nickname returned by the server.
    pub nickname: Option<String>,
    /// IRC username.
    pub username: Option<String>,
    /// Visible hostname.
    pub hostname: Option<String>,
    /// Human-facing real name.
    pub real_name: Option<String>,
    /// Server currently carrying the user.
    pub server: Option<String>,
    /// Identified account name.
    pub account: Option<String>,
    /// Away reason when the user is away.
    pub away_message: Option<String>,
    /// Channels as presented by the server, including status prefixes.
    pub channels: Vec<String>,
    /// Idle seconds when disclosed.
    pub idle_seconds: Option<u64>,
    /// Sign-on Unix timestamp when disclosed.
    pub signon_timestamp: Option<u64>,
    /// Whether the server reported a secure connection.
    pub secure: bool,
    /// Whether the server reported IRC-operator status.
    pub operator: bool,
}

/// Typed WHOIS result plus the lossless correlated command envelope.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct WhoisOutput {
    /// Requested nickname.
    pub requested_nickname: Nickname,
    /// Command-specific projection of the standard WHOIS numerics.
    pub profile: WhoisProfile,
    /// Lossless correlated command result.
    pub result: CommandResult,
}

/// Read channel membership names through a command-specific MCP schema.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NamesInput {
    /// Opaque handle returned by `irc.connect`.
    pub agent_id: AgentId,
    /// Channels to query; an empty list requests the server default.
    #[serde(default)]
    pub channels: Vec<ChannelName>,
    /// Milliseconds to wait for the complete NAMES reply sequence, between 1
    /// and the configured maximum of 30000 by default.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Amount of duplicate wire/semantic detail retained in `result`.
    #[serde(default = "default_full_result_detail")]
    pub result_detail: ToolResultDetail,
}

/// Names returned for one channel.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct NamesChannel {
    /// Case-preserved channel name.
    pub channel: String,
    /// Server visibility marker from RPL_NAMREPLY (`=`, `*`, or `@`).
    pub visibility: String,
    /// Nicknames as presented by the server, including membership prefixes.
    pub names: Vec<String>,
}

/// Typed NAMES result plus the lossless correlated command envelope.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct NamesOutput {
    /// Membership grouped by channel.
    pub channels: Vec<NamesChannel>,
    /// Lossless correlated command result.
    pub result: CommandResult,
}

/// List visible channels through a command-specific MCP schema.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ListInput {
    /// Opaque handle returned by `irc.connect`.
    pub agent_id: AgentId,
    /// Optional server-side channel mask.
    pub mask: Option<String>,
    /// Milliseconds to wait for the complete LIST reply sequence, between 1
    /// and the configured maximum of 30000 by default.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Amount of duplicate wire/semantic detail retained in `result`.
    #[serde(default = "default_full_result_detail")]
    pub result_detail: ToolResultDetail,
}

/// One RPL_LIST channel entry.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct ChannelListEntry {
    /// Case-preserved channel name.
    pub channel: String,
    /// Visible member count when the server supplied a number.
    pub visible_users: Option<u64>,
    /// Current topic or server-provided list description.
    pub topic: Option<String>,
}

/// Typed LIST result plus the lossless correlated command envelope.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct ListOutput {
    /// Visible channel entries.
    pub channels: Vec<ChannelListEntry>,
    /// Lossless correlated command result.
    pub result: CommandResult,
}

/// Read user or channel modes through a command-specific MCP schema.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModeGetInput {
    /// Opaque handle returned by `irc.connect`.
    pub agent_id: AgentId,
    /// Nickname or channel whose modes are requested.
    pub target: Target,
    /// Optional list-mode selector such as `b`.
    pub mode: Option<String>,
    /// Milliseconds to wait for the MODE reply sequence, between 1 and the
    /// configured maximum of 30000 by default.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Amount of duplicate wire/semantic detail retained in `result`.
    #[serde(default = "default_full_result_detail")]
    pub result_detail: ToolResultDetail,
}

/// One server mode reply with the client nickname removed.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct ModeReply {
    /// Numeric or standard reply command.
    pub command: String,
    /// Ordered reply parameters.
    pub parameters: Vec<String>,
    /// Optional trailing description.
    pub text: Option<String>,
}

/// Typed MODE query result plus the lossless correlated command envelope.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct ModeGetOutput {
    /// Requested nickname or channel.
    pub target: Target,
    /// Mode-specific replies.
    pub modes: Vec<ModeReply>,
    /// Lossless correlated command result.
    pub result: CommandResult,
}

/// Read the HELP index or one subject through a command-specific MCP schema.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HelpInput {
    /// Opaque handle returned by `irc.connect`.
    pub agent_id: AgentId,
    /// Optional command or help subject.
    pub subject: Option<String>,
    /// Milliseconds to wait for the complete HELP reply sequence, between 1
    /// and the configured maximum of 30000 by default.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Amount of duplicate wire/semantic detail retained in `result`.
    #[serde(default = "default_full_result_detail")]
    pub result_detail: ToolResultDetail,
}

/// One typed HELP line.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct HelpLine {
    /// Reply command or numeric.
    pub command: String,
    /// Help subject reported on this line, when present.
    pub subject: Option<String>,
    /// Human-facing help text.
    pub text: Option<String>,
}

/// Typed HELP result plus the lossless correlated command envelope.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct HelpOutput {
    /// Requested subject.
    pub subject: Option<String>,
    /// Ordered help lines.
    pub lines: Vec<HelpLine>,
    /// Lossless correlated command result.
    pub result: CommandResult,
}

/// Read one channel topic through a command-specific MCP schema.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TopicGetInput {
    /// Opaque handle returned by `irc.connect`.
    pub agent_id: AgentId,
    /// Channel whose current topic is requested.
    pub channel: ChannelName,
    /// Milliseconds to wait for the topic reply sequence, between 1 and the
    /// configured maximum of 30000 by default.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Amount of duplicate wire/semantic detail retained in `result`.
    #[serde(default = "default_full_result_detail")]
    pub result_detail: ToolResultDetail,
}

/// Change one channel topic through a command-specific MCP schema.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TopicSetInput {
    /// Opaque handle returned by `irc.connect`.
    pub agent_id: AgentId,
    /// Channel whose topic is changed.
    pub channel: ChannelName,
    /// New topic; an empty string clears it.
    pub topic: String,
    /// Milliseconds to wait for a server reply or echo, between 1 and the
    /// configured maximum of 30000 by default.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Amount of duplicate wire/semantic detail retained in `result`.
    #[serde(default = "default_full_result_detail")]
    pub result_detail: ToolResultDetail,
}

/// Typed topic query or mutation result.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct TopicOutput {
    /// Case-preserved channel name.
    pub channel: ChannelName,
    /// Topic confirmed by the reply, or requested by a successful mutation.
    pub topic: Option<String>,
    /// Nickname or server identity that set the topic when disclosed.
    pub set_by: Option<String>,
    /// Unix timestamp supplied by RPL_TOPICWHOTIME.
    pub set_at: Option<u64>,
    /// Stable channel-resource URI.
    pub resource: String,
    /// Lossless correlated command result.
    pub result: CommandResult,
}

/// Change this guest's nickname through a command-specific MCP schema.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NickSetInput {
    /// Opaque handle returned by `irc.connect`.
    pub agent_id: AgentId,
    /// New nickname.
    pub nickname: Nickname,
    /// Milliseconds to wait for the server echo or rejection, between 1 and
    /// the configured maximum of 30000 by default.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Amount of duplicate wire/semantic detail retained in `result`.
    #[serde(default = "default_full_result_detail")]
    pub result_detail: ToolResultDetail,
}

/// Typed nickname-change result.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct NickSetOutput {
    /// Requested nickname.
    pub nickname: Nickname,
    /// Lossless correlated command result.
    pub result: CommandResult,
}

/// Set or clear this guest's away state.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AwaySetInput {
    /// Opaque handle returned by `irc.connect`.
    pub agent_id: AgentId,
    /// Away message; omit or provide an empty string to clear away state.
    pub message: Option<String>,
    /// Milliseconds to wait for RPL_NOWAWAY or RPL_UNAWAY, between 1 and the
    /// configured maximum of 30000 by default.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Amount of duplicate wire/semantic detail retained in `result`.
    #[serde(default = "default_full_result_detail")]
    pub result_detail: ToolResultDetail,
}

/// Typed away-state result.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct AwaySetOutput {
    /// Whether away state was requested rather than cleared.
    pub away: bool,
    /// Normalized requested away message.
    pub message: Option<String>,
    /// Lossless correlated command result.
    pub result: CommandResult,
}

/// Remove one member from a channel.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct KickInput {
    /// Opaque handle returned by `irc.connect`.
    pub agent_id: AgentId,
    /// Channel from which the member is removed.
    pub channel: ChannelName,
    /// Nickname to remove.
    pub nickname: Nickname,
    /// Optional server-visible reason.
    pub reason: Option<String>,
    /// Milliseconds to wait for the server echo or rejection, between 1 and
    /// the configured maximum of 30000 by default.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Amount of duplicate wire/semantic detail retained in `result`.
    #[serde(default = "default_full_result_detail")]
    pub result_detail: ToolResultDetail,
}

impl KickInput {
    /// The arguments a request state is bound to.
    pub fn salient(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
    }

    /// The action a caller is asked to confirm, in the words of its effect.
    pub fn action(&self) -> String {
        let reason = self
            .reason
            .as_deref()
            .filter(|reason| !reason.is_empty())
            .map_or_else(String::new, |reason| format!(" (reason: {reason})"));
        format!(
            "kick {} from {} as agent {}{reason}",
            self.nickname, self.channel, self.agent_id
        )
    }
}

/// Typed KICK result.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct KickOutput {
    /// Affected channel.
    pub channel: ChannelName,
    /// Requested member nickname.
    pub nickname: Nickname,
    /// Stable channel-resource URI.
    pub resource: String,
    /// Lossless correlated command result.
    pub result: CommandResult,
}

/// Invite one nickname to a channel.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InviteInput {
    /// Opaque handle returned by `irc.connect`.
    pub agent_id: AgentId,
    /// Nickname to invite.
    pub nickname: Nickname,
    /// Destination channel.
    pub channel: ChannelName,
    /// Milliseconds to wait for server confirmation or rejection, between 1
    /// and the configured maximum of 30000 by default.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Amount of duplicate wire/semantic detail retained in `result`.
    #[serde(default = "default_full_result_detail")]
    pub result_detail: ToolResultDetail,
}

/// Typed INVITE result.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct InviteOutput {
    /// Invited nickname.
    pub nickname: Nickname,
    /// Destination channel.
    pub channel: ChannelName,
    /// Stable channel-resource URI.
    pub resource: String,
    /// Lossless correlated command result.
    pub result: CommandResult,
}

/// Server-side MONITOR list mutation.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MonitorUpdateKind {
    /// Add nicknames to the monitor list.
    Add,
    /// Remove nicknames from the monitor list.
    Remove,
    /// Clear the complete monitor list; `nicknames` must be empty.
    Clear,
}

/// Update the server-side MONITOR list through a capability-checked schema.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MonitorUpdateInput {
    /// Opaque handle returned by `irc.connect`.
    pub agent_id: AgentId,
    /// Add, remove, or clear operation.
    pub operation: MonitorUpdateKind,
    /// Nicknames for add/remove; empty only for clear.
    #[serde(default)]
    pub nicknames: Vec<Nickname>,
    /// Milliseconds to wait for any server response, between 1 and the
    /// configured maximum of 30000 by default.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Amount of duplicate wire/semantic detail retained in `result`.
    #[serde(default = "default_full_result_detail")]
    pub result_detail: ToolResultDetail,
}

/// Typed MONITOR mutation result.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct MonitorUpdateOutput {
    /// Applied operation.
    pub operation: MonitorUpdateKind,
    /// Affected nicknames.
    pub nicknames: Vec<Nickname>,
    /// Lossless correlated command result.
    pub result: CommandResult,
}

/// Change user or channel modes through a command-specific MCP schema.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModeSetInput {
    /// Opaque handle returned by `irc.connect`.
    pub agent_id: AgentId,
    /// Nickname or channel whose modes are changed.
    pub target: Target,
    /// Mode change string such as `+o` or `-b`.
    pub modes: String,
    /// Ordered mode arguments.
    #[serde(default)]
    pub arguments: Vec<String>,
    /// Milliseconds to wait for server confirmation or rejection, between 1
    /// and the configured maximum of 30000 by default.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Amount of duplicate wire/semantic detail retained in `result`.
    #[serde(default = "default_full_result_detail")]
    pub result_detail: ToolResultDetail,
}

/// Typed MODE mutation result.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct ModeSetOutput {
    /// Affected nickname or channel.
    pub target: Target,
    /// Requested mode change string.
    pub modes: String,
    /// Requested mode arguments.
    pub arguments: Vec<String>,
    /// Stable channel-resource URI when the target is a channel.
    pub resource: Option<String>,
    /// Lossless correlated command result.
    pub result: CommandResult,
}

/// Whether a reaction is being added or removed.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReactionUpdateKind {
    /// Add a reaction to the referenced message.
    Add,
    /// Remove a previously added reaction.
    Remove,
}

/// Add or remove an IRCv3 reaction through a client-only tag.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReactionUpdateInput {
    /// Opaque handle returned by `irc.connect`.
    pub agent_id: AgentId,
    /// Channel or nickname containing the referenced message.
    pub target: Target,
    /// Server-assigned `msgid` of the message being reacted to.
    pub message_id: String,
    /// Reaction value, such as an emoji or short text token.
    pub reaction: String,
    /// Add or remove operation.
    pub operation: ReactionUpdateKind,
    /// Milliseconds to wait for server echo or rejection, between 1 and the
    /// configured maximum of 30000 by default.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Amount of duplicate wire/semantic detail retained in `result`.
    #[serde(default = "default_full_result_detail")]
    pub result_detail: ToolResultDetail,
}

/// Typed reaction mutation result.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct ReactionUpdateOutput {
    /// Conversation containing the referenced message.
    pub target: Target,
    /// Referenced server message ID.
    pub message_id: String,
    /// Reaction value that was added or removed.
    pub reaction: String,
    /// Applied operation.
    pub operation: ReactionUpdateKind,
    /// Lossless correlated command result.
    pub result: CommandResult,
}

/// Redact one message through negotiated IRCv3 message redaction.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MessageRedactInput {
    /// Opaque handle returned by `irc.connect`.
    pub agent_id: AgentId,
    /// Channel or nickname containing the message.
    pub target: Target,
    /// Server-assigned `msgid` of the message to redact.
    pub message_id: String,
    /// Optional user-supplied reason; no default reason is invented.
    pub reason: Option<String>,
    /// Milliseconds to wait for server confirmation or rejection, between 1
    /// and the configured maximum of 30000 by default.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Amount of duplicate wire/semantic detail retained in `result`.
    #[serde(default = "default_full_result_detail")]
    pub result_detail: ToolResultDetail,
}

impl MessageRedactInput {
    /// The arguments a request state is bound to.
    pub fn salient(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
    }

    /// The action a caller is asked to confirm, in the words of its effect.
    pub fn action(&self) -> String {
        let reason = self
            .reason
            .as_deref()
            .filter(|reason| !reason.is_empty())
            .map_or_else(String::new, |reason| format!(" (reason: {reason})"));
        format!(
            "redact message {} in {} as agent {}{reason}",
            self.message_id, self.target, self.agent_id
        )
    }
}

/// Typed message-redaction result.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct MessageRedactOutput {
    /// Conversation containing the redacted message.
    pub target: Target,
    /// Referenced server message ID.
    pub message_id: String,
    /// User-supplied reason, when present.
    pub reason: Option<String>,
    /// Lossless correlated command result.
    pub result: CommandResult,
}

/// Read the synchronized marker for one IRC conversation.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReadMarkerGetInput {
    /// Opaque handle returned by `irc.connect`.
    pub agent_id: AgentId,
    /// Channel or nickname buffer whose marker is requested.
    pub target: Target,
    /// Milliseconds to wait for the server reply, between 1 and the configured
    /// maximum of 30000 by default.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Amount of duplicate wire/semantic detail retained in `result`.
    #[serde(default = "default_full_result_detail")]
    pub result_detail: ToolResultDetail,
}

/// Advance the synchronized marker for one IRC conversation.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReadMarkerSetInput {
    /// Opaque handle returned by `irc.connect`.
    pub agent_id: AgentId,
    /// Channel or nickname buffer whose marker is advanced.
    pub target: Target,
    /// Timestamp of a previously received message carrying a `time` tag.
    pub read_at: Timestamp,
    /// Milliseconds to wait for the server reply, between 1 and the configured
    /// maximum of 30000 by default.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Amount of duplicate wire/semantic detail retained in `result`.
    #[serde(default = "default_full_result_detail")]
    pub result_detail: ToolResultDetail,
}

/// Typed synchronized read-marker result.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct ReadMarkerOutput {
    /// Conversation whose marker was returned.
    pub target: Target,
    /// Server-confirmed marker, or `None` when the server returned `*` or no
    /// successful marker reply was collected.
    pub read_at: Option<Timestamp>,
    /// Lossless correlated command result.
    pub result: CommandResult,
}

/// IRCv3 typing indicator state.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TypingState {
    /// The user is actively changing the input field.
    Active,
    /// The user paused without clearing the input field.
    Paused,
    /// The user cleared the input field without sending a message.
    Done,
}

impl TypingState {
    /// IRCv3 client-tag spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Done => "done",
        }
    }
}

/// Send a privacy-sensitive, per-target throttled IRCv3 typing indicator.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TypingSetInput {
    /// Opaque handle returned by `irc.connect`.
    pub agent_id: AgentId,
    /// Channel or nickname that can observe the indicator.
    pub target: Target,
    /// Typing state to publish.
    pub state: TypingState,
    /// Milliseconds to wait for server echo or rejection, between 1 and the
    /// configured maximum of 30000 by default.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Amount of duplicate wire/semantic detail retained in `result`.
    #[serde(default = "default_full_result_detail")]
    pub result_detail: ToolResultDetail,
}

/// Typed typing-indicator result.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct TypingSetOutput {
    /// Conversation that can observe the indicator.
    pub target: Target,
    /// Published state.
    pub state: TypingState,
    /// Lossless correlated command result.
    pub result: CommandResult,
}

/// Completion behavior requested from `irc.execute`.
#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ResponseMode {
    /// Select a strategy from static and runtime protocol knowledge.
    #[default]
    Auto,
    /// Collect labeled replies for an otherwise unknown command. Requires the
    /// `labeled-response` capability; the call fails outright without it.
    Collect,
    /// Return once the socket writer accepts the line.
    FireAndForget,
}

/// Structured compatibility escape hatch for every syntactically valid command.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExecuteInput {
    /// Opaque handle returned by `irc.connect`.
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
    /// Completion mode: `auto` (default), `collect`, or `fire_and_forget`.
    #[serde(default)]
    pub response_mode: ResponseMode,
    /// Milliseconds to wait for the collector. Must be between 1 and the
    /// configured maximum, 30000 by default; anything else is rejected.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Result detail: `full` (default) retains lossless replies and their
    /// semantic projection; `compact` sets the duplicate projection to null.
    #[serde(default = "default_full_result_detail")]
    pub result_detail: ToolResultDetail,
}

/// Cursor-based event read and optional long poll.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EventsReadInput {
    /// Opaque handle returned by `irc.connect`.
    pub agent_id: AgentId,
    /// Last cursor consumed by this caller: pass back the `next_cursor` from
    /// the previous read. Omit it to start at the oldest retained event. The
    /// cursor advances only over events actually returned, so narrowing or
    /// changing the filter between reads never skips anything.
    pub cursor: Option<EventCursor>,
    /// Maximum ordered events returned.
    #[serde(default = "default_event_limit")]
    pub limit: usize,
    /// Zero for non-blocking, positive to long poll for that many
    /// milliseconds, up to the configured maximum of 30000 by default.
    #[serde(default)]
    pub wait_ms: u64,
    /// Optional watch handle returned by `irc.watch.create`, whose registered
    /// selection — several targets, several classes, addressed-to-me, direction
    /// — is applied to this read. The watch supplies only the selection; the
    /// position stays `cursor`, so two readers of one watch never disturb each
    /// other. Because a watch already describes a complete selection, every
    /// single-value filter below must be omitted when this is set.
    pub watch_id: Option<WatchId>,
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
    /// Set true to return only events addressed to this agent — a private
    /// message or notice sent to it, or a channel message naming its current
    /// nickname. The agent's own echoed messages never qualify. Set false to
    /// return only everything else; omit for both.
    pub mentions_me: Option<bool>,
    /// Set true to record this read's `next_cursor` as the position later
    /// activity hints count from. This is the only thing in the whole server
    /// that moves that anchor — no tool result, resource read, or notification
    /// ever does — so a caller that reads without it keeps counting from
    /// wherever it last said it had caught up. It changes no delivery state:
    /// this read returns the same events either way, and the cursor you persist
    /// is still your own.
    #[serde(default)]
    pub set_activity_anchor: bool,
}

impl EventsReadInput {
    /// The first single-value filter set alongside `watch_id`, if any.
    ///
    /// The combination is refused rather than intersected. A watch carries
    /// multi-target and multi-class selection these fields cannot express, so
    /// combining them would silently produce a third selection that neither the
    /// watch nor the caller describes — and a caller reading a watch through
    /// someone else's extra filter would persist a cursor for a window it
    /// cannot reproduce. Narrow the watch itself, or read without it.
    pub fn conflicting_filter(&self) -> Option<&'static str> {
        [
            ("command_id", self.command_id.is_some()),
            ("class", self.class.is_some()),
            ("target", self.target.is_some()),
            ("direction", self.direction.is_some()),
            ("origin", self.origin.is_some()),
            ("verbosity", self.verbosity.is_some()),
            ("mentions_me", self.mentions_me.is_some()),
        ]
        .into_iter()
        .find_map(|(name, present)| present.then_some(name))
    }

    /// Build the actor journal filter without duplicating matching logic.
    pub fn filter(&self) -> EventFilter {
        EventFilter {
            command_id: self.command_id.clone(),
            class: self.class,
            target: self.target.as_ref().map(Target::to_string),
            direction: self.direction,
            origin: self.origin,
            verbosity: self.verbosity,
            mentions_me: self.mentions_me,
        }
    }
}

/// Open an outbound DCC CHAT offer.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DccChatOpenInput {
    /// Opaque handle returned by `irc.connect`.
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
    /// Opaque handle returned by `irc.connect`.
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
    /// Opaque handle returned by `irc.connect`.
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
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DccAcceptInput {
    /// Opaque handle returned by `irc.connect`.
    pub agent_id: AgentId,
    /// Offered session.
    pub dcc_session_id: DccSessionId,
    /// Name of a configured `dcc.receive_roots` entry for SEND, omitted for
    /// CHAT. Omitting it when exactly one root is configured selects that root;
    /// omitting it when several are configured asks the caller to choose.
    pub root: Option<String>,
    /// Destination relative to the chosen root for SEND, omitted for CHAT.
    /// Defaults to the offered filename. Absolute paths are refused: the root
    /// name is what carries filesystem authority.
    pub destination_path: Option<PathBuf>,
    /// Existing-file behavior for SEND.
    #[serde(default)]
    pub conflict: DestinationConflict,
}

impl DccAcceptInput {
    /// The arguments a request state is bound to.
    ///
    /// Everything that decides what the acceptance will do, and nothing that
    /// legitimately varies between rounds — a retry re-sends the original
    /// arguments unchanged, so a caller that altered one of these is starting a
    /// different operation and must not redeem the state from the first.
    pub fn salient(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
    }
}

/// Identify one existing DCC session.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DccSessionInput {
    /// Opaque handle returned by `irc.connect`.
    pub agent_id: AgentId,
    /// Direct-session handle.
    pub dcc_session_id: DccSessionId,
}

/// Filter the actor's bounded DCC session table.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DccListInput {
    /// Opaque handle returned by `irc.connect`.
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

const fn default_full_result_detail() -> ToolResultDetail {
    ToolResultDetail::Full
}
