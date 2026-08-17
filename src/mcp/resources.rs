//! Typed MCP resource URIs and payloads.

use std::{fmt, str::FromStr};

use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use rmcp::model::{Annotations, Resource, Role};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use std::collections::BTreeMap;

use crate::{
    agent::{
        AgentId,
        actor::{ActiveLineBudget, AgentSnapshot},
        journal::{EventCursor, IrcEvent, JournalStats},
        state::{AgentState, ChannelState, MemberState, MotdState},
    },
    dcc::session::{DccSession, DccSessionId},
    irc::{capabilities::CompatibilityCatalog, isupport::CaseMapping},
    mcp::conversation::CompactEvent,
    time::Timestamp,
};

/// Links returned by `irc.connect` after successful registration.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct ResourceUris {
    /// Connection and protocol status.
    pub status: String,
    /// Latest server MOTD.
    pub motd: String,
    /// Capabilities, ISUPPORT, HELP, CTCP, and DCC compatibility.
    pub protocol: String,
    /// Best-effort reduced IRC state.
    pub state: String,
    /// Cursor bounds and a recent event window.
    pub events: String,
    /// Compact feed of everything addressed to this agent.
    pub inbox: String,
    /// Lossless protocol records for diagnosis.
    pub wire: String,
    /// In-memory DCC sessions.
    pub dcc: String,
}

impl ResourceUris {
    /// Build all resource links for an agent.
    pub fn for_agent(agent_id: &AgentId) -> Self {
        let base = format!("irc://agents/{}", agent_id.as_str());
        Self {
            status: format!("{base}/status"),
            motd: format!("{base}/motd"),
            protocol: format!("{base}/protocol"),
            state: format!("{base}/state"),
            events: format!("{base}/events"),
            inbox: format!("{base}/inbox"),
            wire: format!("{base}/wire"),
            dcc: format!("{base}/dcc"),
        }
    }

    /// Build the resource URI for one direct session.
    pub fn dcc_session(agent_id: &AgentId, session_id: &DccSessionId) -> String {
        format!(
            "irc://agents/{}/dcc/{}",
            agent_id.as_str(),
            encode_channel_segment(&session_id.to_string())
        )
    }

    /// Build the transcript resource URI for one channel or peer.
    pub fn transcript(agent_id: &AgentId, target: &str) -> String {
        format!(
            "irc://agents/{}/transcripts/{}",
            agent_id.as_str(),
            encode_channel_segment(target)
        )
    }

    /// Build the member-list resource URI for one channel.
    pub fn channel_members(agent_id: &AgentId, channel: &str) -> String {
        format!("{}/members", Self::channel(agent_id, channel))
    }

    /// Build the topic resource URI for one channel.
    pub fn channel_topic(agent_id: &AgentId, channel: &str) -> String {
        format!("{}/topic", Self::channel(agent_id, channel))
    }

    /// Build the cursor-page expansion for one consumed sequence number.
    pub fn events_after(agent_id: &AgentId, sequence: u64) -> String {
        format!("irc://agents/{}/events/after/{sequence}", agent_id.as_str())
    }

    /// Build the channel resource-template expansion for a case-preserved name.
    pub fn channel(agent_id: &AgentId, channel: &str) -> String {
        format!(
            "irc://agents/{}/channels/{}",
            agent_id.as_str(),
            encode_channel_segment(channel)
        )
    }

    /// Ordered resources advertised for an agent.
    pub fn named(&self) -> [(&'static str, &str); 8] {
        [
            ("status", &self.status),
            ("motd", &self.motd),
            ("protocol", &self.protocol),
            ("state", &self.state),
            ("events", &self.events),
            ("inbox", &self.inbox),
            ("wire", &self.wire),
            ("dcc", &self.dcc),
        ]
    }
}

/// A resource and everything a client needs to decide what to do with it.
///
/// Single source of truth for the presentation of a URI. `resources/list`,
/// `resources/templates/list`, and the `resource_link` blocks returned from
/// tool results all build from this, so a link a tool hands back and the same
/// resource in the catalog can never describe themselves differently.
#[derive(Clone, Debug)]
pub struct ResourceDescriptor {
    /// Stable URI.
    pub uri: String,
    /// Programmatic name, unique within the catalog.
    pub name: String,
    /// Human-readable display title.
    pub title: String,
    /// What the resource contains.
    pub description: String,
    /// Always JSON for this service.
    pub mime_type: &'static str,
    /// Who the content is for. Conversational resources are addressed to the
    /// model; lossless protocol records are for the operator reading them.
    pub audience: &'static [Role],
    /// Relative importance, from 0.0 to 1.0.
    pub priority: f32,
    /// When the underlying state last changed, when that is known.
    pub last_modified: Option<Timestamp>,
}

/// Audience shorthand: content a model should read to hold up its side of a
/// conversation.
const FOR_MODEL: &[Role] = &[Role::Assistant, Role::User];
/// Audience shorthand: content that exists to be inspected by a person.
const FOR_OPERATOR: &[Role] = &[Role::User];

impl ResourceDescriptor {
    /// Convert to the wire form used for both catalog entries and links.
    pub fn into_resource(self) -> Resource {
        let mut annotations = Annotations::default()
            .with_audience(self.audience.to_vec())
            .with_priority(self.priority);
        if let Some(last_modified) = self.last_modified {
            annotations = annotations.with_timestamp(last_modified.into());
        }
        Resource::new(self.uri, self.name)
            .with_title(self.title)
            .with_description(self.description)
            .with_mime_type(self.mime_type)
            .with_annotations(annotations)
    }
}

/// Describe one parsed resource URI.
///
/// `last_modified` is supplied by the caller because only the gateway knows
/// when the underlying state last changed; passing `None` simply omits the
/// hint rather than inventing one.
pub fn describe(uri: &AgentResourceUri, last_modified: Option<Timestamp>) -> ResourceDescriptor {
    let agent = uri.agent_id.as_str();
    let (name, title, description, audience, priority) = match &uri.kind {
        ResourceKind::Status => (
            "status".to_string(),
            "Connection status".to_string(),
            "Connection lifecycle, the scheduled reconnect attempt, negotiated capability counts, \
             and event bounds with their eviction accounting."
                .to_string(),
            FOR_MODEL,
            0.5,
        ),
        ResourceKind::Motd => (
            "motd".to_string(),
            "Server message of the day".to_string(),
            "The server's own instructions, which callers are expected to read before \
             participating."
                .to_string(),
            FOR_MODEL,
            0.8,
        ),
        ResourceKind::Protocol => (
            "protocol".to_string(),
            "Protocol compatibility".to_string(),
            "Capabilities, ISUPPORT tokens, HELP responses, and CTCP/DCC support.".to_string(),
            FOR_OPERATOR,
            0.3,
        ),
        ResourceKind::State => (
            "state".to_string(),
            "IRC state".to_string(),
            "Best-effort reduced state: identity, joined channels, and reconnect scheduling."
                .to_string(),
            FOR_MODEL,
            0.6,
        ),
        ResourceKind::Events => (
            "events".to_string(),
            "Event journal".to_string(),
            "Cursor bounds and the newest retained events, losslessly. Subscribe here and read \
             the cursor expansion to consume the stream."
                .to_string(),
            FOR_OPERATOR,
            0.4,
        ),
        ResourceKind::EventsAfter(sequence) => (
            format!("events-after-{sequence}"),
            format!("Events after {sequence}"),
            "Every retained event after this position, with the cursor to read from next."
                .to_string(),
            FOR_OPERATOR,
            0.4,
        ),
        ResourceKind::Inbox => (
            "inbox".to_string(),
            "Inbox".to_string(),
            "Everything addressed to this agent — private messages, and channel messages naming \
             it — as compact conversational records."
                .to_string(),
            FOR_MODEL,
            1.0,
        ),
        ResourceKind::Wire => (
            "wire".to_string(),
            "Wire diagnostics".to_string(),
            "Lossless parsed protocol records and refused lines, for diagnosis rather than for \
             context."
                .to_string(),
            FOR_OPERATOR,
            0.2,
        ),
        ResourceKind::Transcript(target) => (
            format!("transcript-{target}"),
            format!("Transcript of {target}"),
            format!("Compact conversation with {target}: who said what, and when."),
            FOR_MODEL,
            0.9,
        ),
        ResourceKind::Dcc => (
            "dcc".to_string(),
            "Direct sessions".to_string(),
            "Retained DCC chat and file-transfer sessions.".to_string(),
            FOR_MODEL,
            0.5,
        ),
        ResourceKind::DccSession(session_id) => (
            format!("dcc-{session_id}"),
            format!("Direct session {session_id}"),
            "One direct session: its state, endpoint, byte progress, and local path.".to_string(),
            FOR_MODEL,
            0.6,
        ),
        ResourceKind::Channel(channel) => (
            format!("channel-{channel}"),
            format!("Channel {channel}"),
            format!("Topic, modes, and members of {channel}."),
            FOR_MODEL,
            0.7,
        ),
        ResourceKind::ChannelMembers(channel) => (
            format!("channel-{channel}-members"),
            format!("Members of {channel}"),
            format!("Who is currently in {channel}, with their known presence data."),
            FOR_MODEL,
            0.7,
        ),
        ResourceKind::ChannelTopic(channel) => (
            format!("channel-{channel}-topic"),
            format!("Topic of {channel}"),
            format!(
                "The current topic of {channel}. Channel topics carry that channel's standing \
                 instructions."
            ),
            FOR_MODEL,
            0.9,
        ),
    };
    ResourceDescriptor {
        uri: uri.to_string(),
        name: format!("{agent}-{name}"),
        title,
        description,
        mime_type: "application/json",
        audience,
        priority,
        last_modified,
    }
}

/// Every resource that currently exists for one agent, in catalog order.
///
/// Channel resources are included per joined channel rather than left to the
/// templates alone, so a client that only lists resources still discovers the
/// conversations this agent is actually in.
pub fn descriptors_for_agent(
    agent_id: &AgentId,
    state: &AgentState,
    last_modified: Option<Timestamp>,
) -> Vec<ResourceDescriptor> {
    let uris = ResourceUris::for_agent(agent_id);
    let mut descriptors: Vec<ResourceDescriptor> = uris
        .named()
        .into_iter()
        .filter_map(|(_, uri)| AgentResourceUri::from_str(uri).ok())
        .map(|uri| describe(&uri, last_modified))
        .collect();
    for channel in state.channels.values() {
        for kind in [
            ResourceKind::Channel(channel.name.clone()),
            ResourceKind::ChannelMembers(channel.name.clone()),
            ResourceKind::ChannelTopic(channel.name.clone()),
            ResourceKind::Transcript(channel.name.clone()),
        ] {
            descriptors.push(describe(
                &AgentResourceUri {
                    agent_id: agent_id.clone(),
                    kind,
                },
                last_modified,
            ));
        }
    }
    descriptors
}

/// Percent-encode one case-preserved channel name for use as a single URI
/// path segment. Shared so template expansion and argument completion can
/// never disagree about the encoding.
pub fn encode_channel_segment(channel: &str) -> String {
    utf8_percent_encode(channel, NON_ALPHANUMERIC).to_string()
}

/// Per-agent resource kind.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResourceKind {
    /// Connection/status summary.
    Status,
    /// Latest complete MOTD.
    Motd,
    /// Protocol compatibility catalog.
    Protocol,
    /// Advisory actor state.
    State,
    /// Journal bounds and recent records.
    Events,
    /// One cursor-addressed journal page beginning after a sequence.
    ///
    /// This is what makes `subscriptions/listen` plus `resources/read` a
    /// complete delivery loop: the notification names the stable events
    /// resource, and the reader then fetches everything it has not consumed
    /// through this expansion, without a tool call.
    EventsAfter(u64),
    /// Compact feed of events addressed to this agent.
    Inbox,
    /// Lossless protocol records, for diagnosis rather than for context.
    Wire,
    /// Compact conversation transcript for one channel or peer.
    Transcript(String),
    /// Retained direct sessions.
    Dcc,
    /// One direct session, addressable so a long-running transfer can be
    /// followed and its terminal result linked to.
    DccSession(DccSessionId),
    /// One expanded channel snapshot.
    Channel(String),
    /// The member list of one channel on its own.
    ChannelMembers(String),
    /// The topic of one channel on its own.
    ChannelTopic(String),
}

/// Parsed resource URI with a validated actor handle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentResourceUri {
    /// Owning actor.
    pub agent_id: AgentId,
    /// Requested snapshot.
    pub kind: ResourceKind,
}

impl FromStr for AgentResourceUri {
    type Err = ResourceUriError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let path = value
            .strip_prefix("irc://agents/")
            .ok_or(ResourceUriError::Scheme)?;
        // Match on the whole remainder so a trailing segment is rejected rather
        // than silently ignored.
        let segments: Vec<&str> = path.split('/').collect();
        let (agent, rest) = segments.split_first().ok_or(ResourceUriError::Path)?;
        let agent_id = AgentId::from_str(agent).map_err(ResourceUriError::Agent)?;
        let kind = match rest {
            ["status"] => ResourceKind::Status,
            ["motd"] => ResourceKind::Motd,
            ["protocol"] => ResourceKind::Protocol,
            ["state"] => ResourceKind::State,
            ["events"] => ResourceKind::Events,
            ["events", "after", sequence] => ResourceKind::EventsAfter(
                sequence
                    .parse()
                    .map_err(|_| ResourceUriError::CursorSequence)?,
            ),
            ["inbox"] => ResourceKind::Inbox,
            ["wire"] => ResourceKind::Wire,
            ["dcc"] => ResourceKind::Dcc,
            ["dcc", session] if !session.is_empty() => ResourceKind::DccSession(
                DccSessionId::from_str(&decode_segment(session)?)
                    .map_err(ResourceUriError::DccSession)?,
            ),
            ["transcripts", target] if !target.is_empty() => {
                ResourceKind::Transcript(decode_segment(target)?)
            }
            ["channels", channel] if !channel.is_empty() => {
                ResourceKind::Channel(decode_segment(channel)?)
            }
            ["channels", channel, "members"] if !channel.is_empty() => {
                ResourceKind::ChannelMembers(decode_segment(channel)?)
            }
            ["channels", channel, "topic"] if !channel.is_empty() => {
                ResourceKind::ChannelTopic(decode_segment(channel)?)
            }
            _ => return Err(ResourceUriError::Path),
        };
        Ok(Self { agent_id, kind })
    }
}

/// Decode one percent-encoded path segment back to its case-preserved name.
fn decode_segment(segment: &str) -> Result<String, ResourceUriError> {
    Ok(percent_decode_str(segment)
        .decode_utf8()
        .map_err(|_| ResourceUriError::Encoding)?
        .into_owned())
}

/// Invalid stable resource URI.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum ResourceUriError {
    /// URI is outside this service's scheme/authority.
    #[error("resource URI must begin with irc://agents/")]
    Scheme,
    /// Actor handle did not validate.
    #[error("invalid agent handle: {0}")]
    Agent(&'static str),
    /// Path has no matching stable resource or template expansion.
    #[error("unknown IRC resource path")]
    Path,
    /// Channel segment is not valid percent-encoded UTF-8.
    #[error("channel resource segment is not valid UTF-8")]
    Encoding,
    /// Cursor expansion did not carry a decimal sequence number.
    #[error("event cursor segment must be a decimal sequence number")]
    CursorSequence,
    /// A DCC path did not carry a valid session handle.
    #[error("invalid DCC session handle: {0}")]
    DccSession(&'static str),
}

/// Status-resource payload.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct StatusResource {
    /// Advisory actor state.
    pub state: AgentState,
    /// Number of exact CAP advertisements.
    pub advertised_capabilities: usize,
    /// Number of negotiated CAP tokens.
    pub negotiated_capabilities: usize,
    /// Current event bounds, including how much eviction has already discarded.
    pub events: JournalStats,
    /// Stable links.
    pub resources: ResourceUris,
}

/// Protocol-resource payload.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct ProtocolResource {
    /// Complete compatibility catalog.
    pub catalog: CompatibilityCatalog,
    /// Active negotiated/local byte limits.
    pub line_budget: ActiveLineBudget,
}

/// Event-resource payload.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct EventsResource {
    /// Current retained bounds and byte use.
    pub journal: JournalStats,
    /// Bounded recent window.
    pub recent: Vec<IrcEvent>,
    /// Cursor-consumption guidance.
    pub instructions: &'static str,
}

/// DCC-resource payload.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct DccResource {
    /// Retained direct sessions.
    pub sessions: Vec<DccSession>,
}

/// Compact conversational payload shared by the inbox and transcripts.
///
/// This is the model-facing half of the diagnostic/conversational split: the
/// same records the event resource carries losslessly, reduced to who said
/// what, where, and when.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct ConversationResource {
    /// Channel or peer this covers, absent for the inbox, which spans all of
    /// them.
    pub target: Option<String>,
    /// Matching events, oldest first.
    pub events: Vec<CompactEvent>,
    /// Position the newest included event holds in the agent's stream, so a
    /// reader can move to a cursor page or a watch without losing its place.
    pub through_cursor: Option<EventCursor>,
    /// Bounds of the retained window these were drawn from.
    pub journal: JournalStats,
}

/// Lossless protocol records, split out of the conversational resources.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct WireResource {
    /// Retained records that carry a parsed wire message or were refused by
    /// the framing layer, newest window first.
    pub events: Vec<IrcEvent>,
    /// Bounds of the retained window.
    pub journal: JournalStats,
    /// Active negotiated/local byte limits.
    pub line_budget: ActiveLineBudget,
}

/// The member list of one channel, addressable on its own.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct ChannelMembersResource {
    /// Case-preserved channel name.
    pub channel: String,
    /// Case-preserved nicknames and known presence data.
    pub members: BTreeMap<String, MemberState>,
}

/// The topic of one channel, addressable on its own.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct ChannelTopicResource {
    /// Case-preserved channel name.
    pub channel: String,
    /// Latest topic text, absent when the channel has none.
    pub topic: Option<String>,
    /// Nickname that set it, when known.
    pub setter: Option<String>,
    /// When it was set, when known.
    pub set_at: Option<Timestamp>,
}

/// Strongly typed resource payload selected by a parsed URI.
#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case", tag = "resource", content = "data")]
pub enum ResourcePayload {
    /// Connection and protocol summary.
    Status(Box<StatusResource>),
    /// Latest complete MOTD.
    Motd(MotdState),
    /// Compatibility catalog and active limits.
    Protocol(ProtocolResource),
    /// Complete advisory actor state.
    State(Box<AgentState>),
    /// Journal summary and recent events.
    Events(EventsResource),
    /// Compact feed of everything addressed to this agent.
    Inbox(ConversationResource),
    /// Compact transcript for one channel or peer.
    Transcript(ConversationResource),
    /// Lossless protocol records.
    Wire(Box<WireResource>),
    /// Retained direct sessions.
    Dcc(DccResource),
    /// One direct session.
    DccSession(Box<DccSession>),
    /// One channel snapshot.
    Channel(ChannelState),
    /// One channel's members.
    ChannelMembers(ChannelMembersResource),
    /// One channel's topic.
    ChannelTopic(ChannelTopicResource),
}

impl AgentSnapshot {
    /// Return the payload for a parsed resource URI.
    pub fn resource(&self, uri: &AgentResourceUri) -> Result<ResourcePayload, ResourceLookupError> {
        let resources = ResourceUris::for_agent(&uri.agent_id);
        Ok(match &uri.kind {
            ResourceKind::Status => ResourcePayload::Status(Box::new(StatusResource {
                state: self.state.clone(),
                advertised_capabilities: self.protocol.capabilities.len(),
                negotiated_capabilities: self
                    .protocol
                    .capabilities
                    .values()
                    .filter(|entry| {
                        entry.status == crate::irc::capabilities::CapabilityStatus::Negotiated
                    })
                    .count(),
                events: self.journal.clone(),
                resources,
            })),
            ResourceKind::Motd => ResourcePayload::Motd(self.state.motd.clone()),
            ResourceKind::Protocol => ResourcePayload::Protocol(ProtocolResource {
                catalog: self.protocol.clone(),
                line_budget: self.line_budget,
            }),
            ResourceKind::State => ResourcePayload::State(Box::new(self.state.clone())),
            ResourceKind::Events => ResourcePayload::Events(EventsResource {
                journal: self.journal.clone(),
                recent: self.recent_events.clone(),
                instructions: "Subscribe to this URI, then on each notification read irc://agents/{agent_id}/events/after/{sequence} with the last sequence you consumed. The `recent` window here is an unpositioned preview and may slide; only the cursor expansion is safe to consume.",
            }),
            // Served by the gateway, which owns the journal; a snapshot cannot
            // answer a cursor read or a filtered window on its own.
            ResourceKind::EventsAfter(_)
            | ResourceKind::Inbox
            | ResourceKind::Wire
            | ResourceKind::Transcript(_) => {
                return Err(ResourceLookupError::JournalWindowIsAsync);
            }
            ResourceKind::Dcc => ResourcePayload::Dcc(DccResource {
                sessions: self.dcc_sessions.clone(),
            }),
            ResourceKind::DccSession(session_id) => ResourcePayload::DccSession(Box::new(
                self.dcc_sessions
                    .iter()
                    .find(|session| session.id == *session_id)
                    .cloned()
                    .ok_or_else(|| ResourceLookupError::DccSession(session_id.to_string()))?,
            )),
            ResourceKind::Channel(requested) => {
                ResourcePayload::Channel(self.joined_channel(requested)?)
            }
            ResourceKind::ChannelMembers(requested) => {
                let channel = self.joined_channel(requested)?;
                ResourcePayload::ChannelMembers(ChannelMembersResource {
                    channel: channel.name,
                    members: channel.members,
                })
            }
            ResourceKind::ChannelTopic(requested) => {
                let channel = self.joined_channel(requested)?;
                ResourcePayload::ChannelTopic(ChannelTopicResource {
                    channel: channel.name,
                    topic: channel.topic,
                    setter: channel.topic_setter,
                    set_at: channel.topic_set_at,
                })
            }
        })
    }

    /// Resolve one requested channel name against the joined set, folding case
    /// with the server's advertised mapping.
    fn joined_channel(&self, requested: &str) -> Result<ChannelState, ResourceLookupError> {
        let case_mapping = self
            .protocol
            .isupport
            .get("CASEMAPPING")
            .and_then(|token| token.value.as_deref())
            .map(CaseMapping::parse)
            .unwrap_or_default();
        self.state
            .channels
            .get(&case_mapping.fold(requested))
            .cloned()
            .ok_or_else(|| ResourceLookupError::Channel(requested.to_owned()))
    }
}

/// A valid resource path whose current snapshot is unavailable.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum ResourceLookupError {
    /// The actor is not currently joined to the expanded channel name.
    #[error("channel is not present in the actor snapshot: {0}")]
    Channel(String),
    /// The actor is not retaining the requested direct session.
    #[error("direct session is not present in the actor snapshot: {0}")]
    DccSession(String),
    /// A journal window must be read through the gateway rather than a snapshot.
    #[error("journal-backed resources are served by the gateway, not by a snapshot")]
    JournalWindowIsAsync,
}

impl fmt::Display for AgentResourceUri {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let uri = match &self.kind {
            ResourceKind::Status => ResourceUris::for_agent(&self.agent_id).status,
            ResourceKind::Motd => ResourceUris::for_agent(&self.agent_id).motd,
            ResourceKind::Protocol => ResourceUris::for_agent(&self.agent_id).protocol,
            ResourceKind::State => ResourceUris::for_agent(&self.agent_id).state,
            ResourceKind::Events => ResourceUris::for_agent(&self.agent_id).events,
            ResourceKind::Inbox => ResourceUris::for_agent(&self.agent_id).inbox,
            ResourceKind::Wire => ResourceUris::for_agent(&self.agent_id).wire,
            ResourceKind::Dcc => ResourceUris::for_agent(&self.agent_id).dcc,
            ResourceKind::DccSession(session_id) => {
                ResourceUris::dcc_session(&self.agent_id, session_id)
            }
            ResourceKind::EventsAfter(sequence) => {
                ResourceUris::events_after(&self.agent_id, *sequence)
            }
            ResourceKind::Transcript(target) => ResourceUris::transcript(&self.agent_id, target),
            ResourceKind::Channel(channel) => ResourceUris::channel(&self.agent_id, channel),
            ResourceKind::ChannelMembers(channel) => {
                ResourceUris::channel_members(&self.agent_id, channel)
            }
            ResourceKind::ChannelTopic(channel) => {
                ResourceUris::channel_topic(&self.agent_id, channel)
            }
        };
        formatter.write_str(&uri)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_names_are_safe_and_round_trip_as_uri_segments() {
        let agent = AgentId::new();
        let uri = ResourceUris::channel(&agent, "#rust agents");
        assert!(uri.ends_with("/%23rust%20agents"));
        let parsed = AgentResourceUri::from_str(&uri).expect("parse URI");
        assert_eq!(parsed.kind, ResourceKind::Channel("#rust agents".into()));
    }

    #[test]
    fn every_conversational_resource_round_trips_through_its_uri() {
        let agent = AgentId::new();
        let cases = [
            (ResourceUris::for_agent(&agent).inbox, ResourceKind::Inbox),
            (ResourceUris::for_agent(&agent).wire, ResourceKind::Wire),
            (
                ResourceUris::transcript(&agent, "#rust agents"),
                ResourceKind::Transcript("#rust agents".into()),
            ),
            (
                ResourceUris::channel_members(&agent, "#control"),
                ResourceKind::ChannelMembers("#control".into()),
            ),
            (
                ResourceUris::channel_topic(&agent, "#control"),
                ResourceKind::ChannelTopic("#control".into()),
            ),
        ];
        for (uri, expected) in cases {
            let parsed = AgentResourceUri::from_str(&uri).expect("parse URI");
            assert_eq!(parsed.kind, expected);
            assert_eq!(parsed.to_string(), uri, "URI did not render back to itself");
        }
    }

    #[test]
    fn conversational_resources_are_addressed_to_the_model_and_diagnostics_are_not() {
        let agent = AgentId::new();
        let describe_kind = |kind| {
            describe(
                &AgentResourceUri {
                    agent_id: agent.clone(),
                    kind,
                },
                None,
            )
        };

        let inbox = describe_kind(ResourceKind::Inbox);
        let wire = describe_kind(ResourceKind::Wire);
        assert!(inbox.audience.contains(&Role::Assistant));
        assert!(
            !wire.audience.contains(&Role::Assistant),
            "lossless wire records are for an operator, not for model context"
        );
        assert!(
            inbox.priority > wire.priority,
            "what is addressed to this agent must outrank protocol diagnostics"
        );
        // Names stay unique per agent so a client can key on them.
        assert_ne!(inbox.name, wire.name);
        assert!(inbox.name.starts_with(agent.as_str()));
    }

    #[test]
    fn rejects_trailing_resource_segments() {
        let agent = AgentId::new();
        for path in ["status/extra", "events/after/4/extra", "events/after"] {
            let uri = format!("irc://agents/{agent}/{path}");
            assert_eq!(
                AgentResourceUri::from_str(&uri),
                Err(ResourceUriError::Path),
                "{path} should not parse"
            );
        }
    }

    #[test]
    fn event_cursor_pages_round_trip_as_uris() {
        let agent = AgentId::new();
        let uri = ResourceUris::events_after(&agent, 42);
        assert!(uri.ends_with("/events/after/42"));
        let parsed = AgentResourceUri::from_str(&uri).expect("parse cursor URI");
        assert_eq!(parsed.kind, ResourceKind::EventsAfter(42));
        assert_eq!(parsed.to_string(), uri);
    }

    #[test]
    fn a_cursor_page_needs_a_decimal_sequence() {
        let agent = AgentId::new();
        let uri = format!("irc://agents/{agent}/events/after/latest");
        assert_eq!(
            AgentResourceUri::from_str(&uri),
            Err(ResourceUriError::CursorSequence)
        );
    }

    #[test]
    fn the_stable_events_resource_still_parses_alongside_its_cursor_pages() {
        let agent = AgentId::new();
        let uri = ResourceUris::for_agent(&agent).events;
        let parsed = AgentResourceUri::from_str(&uri).expect("parse events URI");
        assert_eq!(parsed.kind, ResourceKind::Events);
    }
}
