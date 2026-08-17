//! Typed MCP resource URIs and payloads.

use std::{fmt, str::FromStr};

use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    agent::{
        AgentId,
        actor::{ActiveLineBudget, AgentSnapshot},
        journal::{IrcEvent, JournalStats},
        state::{AgentState, ChannelState, MotdState},
    },
    dcc::session::DccSession,
    irc::{capabilities::CompatibilityCatalog, isupport::CaseMapping},
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
            dcc: format!("{base}/dcc"),
        }
    }

    /// Build the channel resource-template expansion for a case-preserved name.
    pub fn channel(agent_id: &AgentId, channel: &str) -> String {
        let channel = utf8_percent_encode(channel, NON_ALPHANUMERIC);
        format!("irc://agents/{}/channels/{channel}", agent_id.as_str())
    }

    /// Ordered resources advertised for an agent.
    pub fn named(&self) -> [(&'static str, &str); 6] {
        [
            ("status", &self.status),
            ("motd", &self.motd),
            ("protocol", &self.protocol),
            ("state", &self.state),
            ("events", &self.events),
            ("dcc", &self.dcc),
        ]
    }
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
    /// Retained direct sessions.
    Dcc,
    /// One expanded channel snapshot.
    Channel(String),
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
        let mut segments = path.split('/');
        let agent = segments.next().ok_or(ResourceUriError::Path)?;
        let agent_id = AgentId::from_str(agent).map_err(ResourceUriError::Agent)?;
        let kind = match (segments.next(), segments.next(), segments.next()) {
            (Some("status"), None, None) => ResourceKind::Status,
            (Some("motd"), None, None) => ResourceKind::Motd,
            (Some("protocol"), None, None) => ResourceKind::Protocol,
            (Some("state"), None, None) => ResourceKind::State,
            (Some("events"), None, None) => ResourceKind::Events,
            (Some("dcc"), None, None) => ResourceKind::Dcc,
            (Some("channels"), Some(channel), None) if !channel.is_empty() => {
                let channel = percent_decode_str(channel)
                    .decode_utf8()
                    .map_err(|_| ResourceUriError::Encoding)?
                    .into_owned();
                ResourceKind::Channel(channel)
            }
            _ => return Err(ResourceUriError::Path),
        };
        Ok(Self { agent_id, kind })
    }
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
    /// Current event bounds.
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
    /// Retained direct sessions.
    Dcc(DccResource),
    /// One channel snapshot.
    Channel(ChannelState),
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
                instructions: "Call irc.events.read with the last next_cursor you consumed; resource notifications are only wake-up hints.",
            }),
            ResourceKind::Dcc => ResourcePayload::Dcc(DccResource {
                sessions: self.dcc_sessions.clone(),
            }),
            ResourceKind::Channel(requested) => {
                let case_mapping = self
                    .protocol
                    .isupport
                    .get("CASEMAPPING")
                    .and_then(|token| token.value.as_deref())
                    .map(CaseMapping::parse)
                    .unwrap_or_default();
                let channel = self
                    .state
                    .channels
                    .get(&case_mapping.fold(requested))
                    .cloned()
                    .ok_or_else(|| ResourceLookupError::Channel(requested.clone()))?;
                ResourcePayload::Channel(channel)
            }
        })
    }
}

/// A valid resource path whose current snapshot is unavailable.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum ResourceLookupError {
    /// The actor is not currently joined to the expanded channel name.
    #[error("channel is not present in the actor snapshot: {0}")]
    Channel(String),
}

impl fmt::Display for AgentResourceUri {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let uri = match &self.kind {
            ResourceKind::Status => ResourceUris::for_agent(&self.agent_id).status,
            ResourceKind::Motd => ResourceUris::for_agent(&self.agent_id).motd,
            ResourceKind::Protocol => ResourceUris::for_agent(&self.agent_id).protocol,
            ResourceKind::State => ResourceUris::for_agent(&self.agent_id).state,
            ResourceKind::Events => ResourceUris::for_agent(&self.agent_id).events,
            ResourceKind::Dcc => ResourceUris::for_agent(&self.agent_id).dcc,
            ResourceKind::Channel(channel) => ResourceUris::channel(&self.agent_id, channel),
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
    fn rejects_trailing_resource_segments() {
        let agent = AgentId::new();
        let uri = format!("irc://agents/{agent}/status/extra");
        assert_eq!(
            AgentResourceUri::from_str(&uri),
            Err(ResourceUriError::Path)
        );
    }
}
