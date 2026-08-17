//! Error types shared across gateway layers.

use crate::agent::AgentId;

/// Result type used by gateway internals.
pub type Result<T> = std::result::Result<T, GatewayError>;

/// Stable tool-facing error classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    /// Process configuration is invalid.
    Configuration,
    /// An agent or DCC handle is absent or expired.
    NotFound,
    /// Structured IRC input is invalid or unavailable.
    Validation,
    /// A bounded in-memory resource is exhausted.
    ResourceLimit,
    /// The actor is not currently connected.
    NotConnected,
    /// IRC registration failed before an agent was published.
    Registration,
    /// A bounded operation exceeded its deadline.
    TimedOut,
    /// A written operation may have taken effect but lost confirmation.
    Indeterminate,
    /// DCC negotiation, state, socket, or file processing failed.
    Dcc,
    /// Network or filesystem I/O failed.
    Io,
    /// A supervised actor terminated unexpectedly.
    ActorStopped,
}

impl ErrorKind {
    /// Stable snake-case spelling used in structured tool errors.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Configuration => "configuration_error",
            Self::NotFound => "not_found",
            Self::Validation => "validation_error",
            Self::ResourceLimit => "resource_limit",
            Self::NotConnected => "not_connected",
            Self::Registration => "registration_failed",
            Self::TimedOut => "timed_out",
            Self::Indeterminate => "indeterminate",
            Self::Dcc => "dcc_error",
            Self::Io => "io_error",
            Self::ActorStopped => "actor_stopped",
        }
    }
}

/// Errors that prevent or fail a gateway operation.
#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    /// Configuration could not be read or validated.
    #[error("invalid configuration: {0}")]
    Configuration(String),

    /// A referenced in-memory agent does not exist.
    #[error("unknown or expired agent handle: {0}")]
    AgentNotFound(AgentId),

    /// An IRC message could not be represented safely.
    #[error("invalid IRC operation: {0}")]
    InvalidMessage(String),

    /// A bounded in-memory resource is full.
    #[error("gateway resource limit reached: {0}")]
    ResourceLimit(String),

    /// The actor cannot currently write to Ergo.
    #[error("agent is not connected: {0}")]
    NotConnected(AgentId),

    /// Initial registration failed and no handle was published.
    #[error("IRC guest registration failed: {message}")]
    Registration {
        /// Safe server/local explanation.
        message: String,
        /// Ordered nickname candidates attempted.
        attempted_nicknames: Vec<String>,
    },

    /// A bounded operation exceeded its deadline.
    #[error("operation timed out: {0}")]
    TimedOut(String),

    /// A write may have produced an unconfirmed side effect.
    #[error("operation outcome is indeterminate: {0}")]
    Indeterminate(String),

    /// DCC-specific failure.
    #[error("DCC operation failed: {0}")]
    Dcc(String),

    /// TLS setup or validation failed.
    #[error("TLS connection failed: {0}")]
    Tls(String),

    /// A supervised actor terminated before replying.
    #[error("agent actor stopped unexpectedly: {0}")]
    ActorStopped(AgentId),

    /// An I/O operation failed.
    #[error("{operation}: {source}")]
    Io {
        /// Safe operation context.
        operation: &'static str,
        /// Underlying operating-system error.
        #[source]
        source: std::io::Error,
    },
}

impl GatewayError {
    /// Stable tool-facing category.
    pub const fn kind(&self) -> ErrorKind {
        match self {
            Self::Configuration(_) => ErrorKind::Configuration,
            Self::AgentNotFound(_) => ErrorKind::NotFound,
            Self::InvalidMessage(_) => ErrorKind::Validation,
            Self::ResourceLimit(_) => ErrorKind::ResourceLimit,
            Self::NotConnected(_) => ErrorKind::NotConnected,
            Self::Registration { .. } => ErrorKind::Registration,
            Self::TimedOut(_) => ErrorKind::TimedOut,
            Self::Indeterminate(_) => ErrorKind::Indeterminate,
            Self::Dcc(_) => ErrorKind::Dcc,
            Self::Tls(_) | Self::Io { .. } => ErrorKind::Io,
            Self::ActorStopped(_) => ErrorKind::ActorStopped,
        }
    }

    /// Whether retrying after external state changes is normally safe.
    pub const fn retriable(&self) -> bool {
        matches!(
            self,
            Self::ResourceLimit(_)
                | Self::NotConnected(_)
                | Self::TimedOut(_)
                | Self::Tls(_)
                | Self::Io { .. }
                | Self::ActorStopped(_)
        )
    }

    /// Create an I/O error with operation context.
    pub const fn io(operation: &'static str, source: std::io::Error) -> Self {
        Self::Io { operation, source }
    }
}
