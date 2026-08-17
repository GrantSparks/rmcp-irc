//! DCC session identifiers and lifecycle snapshots.

use std::{fmt, net::SocketAddr, path::PathBuf, str::FromStr};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::time::Timestamp;
use uuid::Uuid;

/// In-memory handle for one direct peer session.
#[derive(Clone, Debug, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
#[schemars(transparent)]
pub struct DccSessionId(String);

impl DccSessionId {
    /// Create a fresh process-local handle.
    pub fn new() -> Self {
        Self(format!("dcc-{}", Uuid::new_v4()))
    }
}

impl Default for DccSessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for DccSessionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for DccSessionId {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let Some(uuid) = value.strip_prefix("dcc-") else {
            return Err("DCC handle must start with dcc-");
        };
        Uuid::parse_str(uuid).map_err(|_| "DCC handle contains an invalid UUID")?;
        Ok(Self(value.to_owned()))
    }
}

impl<'de> Deserialize<'de> for DccSessionId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(serde::de::Error::custom)
    }
}

/// DCC data-plane kind.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DccKind {
    /// Line-oriented peer chat.
    Chat,
    /// Streamed file transfer.
    Send,
}

/// Direction of offer and data.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DccDirection {
    /// Gateway initiated the offer.
    Outbound,
    /// Peer initiated the offer.
    Inbound,
}

/// DCC lifecycle retained in the owning agent.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DccState {
    /// Offer exists but has not been accepted.
    Offered,
    /// Direct connection is being established.
    Connecting,
    /// DCC CHAT is established.
    Active,
    /// DCC SEND is moving bytes.
    Transferring,
    /// Session ended successfully.
    Completed,
    /// Incoming offer was rejected.
    Rejected,
    /// A participant cancelled the session.
    Cancelled,
    /// Session ended with an error.
    Failed,
}

impl DccState {
    /// Whether the session can no longer perform work.
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Rejected | Self::Cancelled | Self::Failed
        )
    }

    /// Whether moving to `next` is valid and monotonic.
    pub fn can_transition_to(self, next: Self) -> bool {
        if self == next {
            return self.is_terminal();
        }
        match self {
            Self::Offered => matches!(
                next,
                Self::Connecting | Self::Rejected | Self::Cancelled | Self::Failed
            ),
            Self::Connecting => matches!(
                next,
                Self::Active | Self::Transferring | Self::Cancelled | Self::Failed
            ),
            Self::Active | Self::Transferring => {
                matches!(next, Self::Completed | Self::Cancelled | Self::Failed)
            }
            Self::Completed | Self::Rejected | Self::Cancelled | Self::Failed => false,
        }
    }
}

impl fmt::Display for DccState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Offered => "offered",
            Self::Connecting => "connecting",
            Self::Active => "active",
            Self::Transferring => "transferring",
            Self::Completed => "completed",
            Self::Rejected => "rejected",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        })
    }
}

/// Snapshot returned through DCC tools and resources.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct DccSession {
    /// In-memory routing handle.
    pub id: DccSessionId,
    /// CHAT or SEND.
    pub kind: DccKind,
    /// Incoming or outgoing.
    pub direction: DccDirection,
    /// Peer nickname from CTCP negotiation.
    pub peer: String,
    /// Current lifecycle state.
    pub state: DccState,
    /// Whether reverse DCC negotiation is in use.
    pub reverse: bool,
    /// Opaque reverse/resume token when applicable.
    pub token: Option<String>,
    /// Direct network endpoint when known.
    pub endpoint: Option<SocketAddr>,
    /// Advertised filename.
    pub filename: Option<String>,
    /// Local source or destination path on the gateway host.
    pub local_path: Option<PathBuf>,
    /// Configured `dcc.receive_roots` name an incoming file is confined to.
    ///
    /// Present only for an accepted incoming SEND. Together with
    /// `receive_path` it says where the file landed in the terms the caller
    /// chose it — a root name and a path relative to it — rather than in a
    /// host path the caller has no authority over and may not be able to reach.
    pub receive_root: Option<String>,
    /// Destination relative to `receive_root`, updated if a conflict rename
    /// moved the committed file aside.
    pub receive_path: Option<PathBuf>,
    /// Bytes transferred so far.
    pub transferred_bytes: u64,
    /// Expected bytes when supplied by the peer.
    pub total_bytes: Option<u64>,
    /// RFC 3339 creation time.
    pub created_at: Timestamp,
    /// RFC 3339 last material update time.
    pub updated_at: Timestamp,
    /// Terminal error safe to expose to MCP clients.
    pub error: Option<String>,
}

impl DccSession {
    /// Create a new offered session.
    pub fn offered(
        kind: DccKind,
        direction: DccDirection,
        peer: impl Into<String>,
        now: Timestamp,
    ) -> Self {
        Self {
            id: DccSessionId::new(),
            kind,
            direction,
            peer: peer.into(),
            state: DccState::Offered,
            reverse: false,
            token: None,
            endpoint: None,
            filename: None,
            local_path: None,
            receive_root: None,
            receive_path: None,
            transferred_bytes: 0,
            total_bytes: None,
            created_at: now,
            updated_at: now,
            error: None,
        }
    }

    /// Apply a validated lifecycle transition.
    pub fn transition(&mut self, next: DccState, now: Timestamp) -> Result<(), DccTransitionError> {
        if !self.state.can_transition_to(next) {
            return Err(DccTransitionError {
                current: self.state,
                requested: next,
            });
        }
        self.state = next;
        self.updated_at = now;
        Ok(())
    }
}

/// Invalid lifecycle transition.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
#[error("invalid DCC transition from {current:?} to {requested:?}")]
pub struct DccTransitionError {
    /// Current session state.
    pub current: DccState,
    /// Requested next state.
    pub requested: DccState,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(second: u32) -> Timestamp {
        format!("2026-08-17T00:00:{second:02}.000Z")
            .parse()
            .expect("test instant")
    }

    #[test]
    fn lifecycle_rejects_backwards_or_cross_kind_transitions() {
        let mut session = DccSession::offered(DccKind::Send, DccDirection::Inbound, "peer", at(1));
        session
            .transition(DccState::Connecting, at(2))
            .expect("transition");
        session
            .transition(DccState::Transferring, at(2))
            .expect("transition");
        assert!(session.transition(DccState::Active, at(2)).is_err());
        session
            .transition(DccState::Completed, at(2))
            .expect("complete");
        session
            .transition(DccState::Completed, at(4))
            .expect("terminal idempotency");
    }

    #[test]
    fn state_display_matches_the_public_wire_spelling() {
        assert_eq!(DccState::Offered.to_string(), "offered");
        assert_eq!(DccState::Failed.to_string(), "failed");
    }
}
