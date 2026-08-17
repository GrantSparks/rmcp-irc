//! Caller identity for handles published over a shared transport.
//!
//! Agent and watch handles are unguessable, but unguessable is not the same as
//! authorized: over Streamable HTTP every caller reaches the same process, so
//! without an owner recorded against each handle any caller who learned one —
//! from a listing, a log, or a shared client — could read and operate it. That
//! is fine for a single local operator on stdio and wrong for a shared
//! service.
//!
//! Ownership here is deliberately coarse. An [`OwnerId`] identifies whoever
//! created a handle, the gateway refuses to resolve a handle for anyone else,
//! and the resource catalog only lists what the caller owns. Who a caller *is*
//! comes from the transport:
//!
//! * stdio has exactly one caller, which owns everything;
//! * HTTP identifies callers by bearer token when tokens are configured, and
//!   otherwise by MCP session, so two sessions on an unauthenticated loopback
//!   endpoint still cannot reach each other's agents.

use std::{
    collections::BTreeSet,
    hash::{DefaultHasher, Hash, Hasher},
};

use rmcp::{ErrorData as McpError, RoleServer, service::RequestContext};
use serde::{Deserialize, Serialize};

/// Identity that owns a set of handles.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct OwnerId(String);

impl OwnerId {
    /// The single caller on a private transport.
    pub fn local() -> Self {
        Self("local".into())
    }

    /// Identity derived from a bearer credential.
    ///
    /// The token is hashed rather than stored, so a handle listing, a log
    /// line, or a debug print can never carry the credential itself.
    pub fn from_bearer(token: &str) -> Self {
        let mut hasher = DefaultHasher::new();
        token.hash(&mut hasher);
        Self(format!("bearer-{:016x}", hasher.finish()))
    }

    /// Identity derived from one MCP session.
    pub fn from_session(session: &str) -> Self {
        Self(format!("session-{session}"))
    }
}

impl std::fmt::Display for OwnerId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// How callers on this transport are identified.
#[derive(Clone, Debug, Default)]
pub enum CallerPolicy {
    /// One trusted local caller owns everything. Used for stdio.
    #[default]
    Local,
    /// Callers are identified per request, and may be required to present a
    /// credential from a configured set.
    Http {
        /// Accepted bearer tokens, already reduced to owner identities. Empty
        /// means the endpoint does not require a credential and callers are
        /// separated by session instead.
        accepted: BTreeSet<OwnerId>,
    },
}

impl CallerPolicy {
    /// Build the HTTP policy for a set of configured bearer tokens.
    pub fn http(tokens: &[String]) -> Self {
        Self::Http {
            accepted: tokens
                .iter()
                .map(|token| OwnerId::from_bearer(token))
                .collect(),
        }
    }

    /// Identify the caller behind one request.
    ///
    /// Fails closed: a request without either an accepted configured
    /// credential or an initialized session is rejected rather than falling
    /// back to a weaker identity.
    pub fn identify(&self, context: &RequestContext<RoleServer>) -> Result<OwnerId, McpError> {
        let Self::Http { accepted } = self else {
            return Ok(OwnerId::local());
        };
        let parts = context.extensions.get::<axum::http::request::Parts>();
        let presented = parts
            .and_then(|parts| parts.headers.get(axum::http::header::AUTHORIZATION))
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .map(str::trim)
            .filter(|token| !token.is_empty())
            .map(OwnerId::from_bearer);

        if accepted.is_empty() {
            // No credential is required, so callers are separated strictly by
            // their negotiated MCP session. Never fall back to the stdio-local
            // owner here: two malformed or pre-initialization HTTP requests
            // without a session header would otherwise collapse into the same
            // identity. An unconfigured Authorization header is not accepted
            // as a self-issued durable identity either.
            return session_owner(parts);
        }
        match presented {
            Some(owner) if accepted.contains(&owner) => Ok(owner),
            Some(_) => Err(McpError::invalid_request(
                "bearer credential is not authorized for this service",
                None,
            )),
            None => Err(McpError::invalid_request(
                "this endpoint requires an Authorization: Bearer credential",
                None,
            )),
        }
    }
}

/// The MCP session a request belongs to, when the transport names one.
fn session_id(parts: &axum::http::request::Parts) -> Option<String> {
    parts
        .headers
        .get("mcp-session-id")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|session| !session.is_empty())
        .map(str::to_owned)
}

/// Resolve the fail-closed owner used by an HTTP endpoint without bearer
/// credentials.
fn session_owner(parts: Option<&axum::http::request::Parts>) -> Result<OwnerId, McpError> {
    parts
        .and_then(session_id)
        .map(|session| OwnerId::from_session(&session))
        .ok_or_else(|| {
            McpError::invalid_request("this endpoint requires an initialized MCP session", None)
        })
}

/// The error returned when a caller names a handle it does not own.
///
/// Deliberately identical to the error for a handle that does not exist:
/// distinguishing them would turn the tool surface into an oracle for which
/// handles other callers hold.
pub fn not_authorized(handle: &str) -> McpError {
    McpError::invalid_params(format!("unknown or expired handle: {handle}"), None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owner_identities_never_carry_the_credential_itself() {
        let owner = OwnerId::from_bearer("hunter2");
        assert!(!owner.to_string().contains("hunter2"));
        assert_eq!(owner, OwnerId::from_bearer("hunter2"));
        assert_ne!(owner, OwnerId::from_bearer("hunter3"));
    }

    #[test]
    fn stdio_has_exactly_one_owner() {
        assert_eq!(OwnerId::local(), OwnerId::local());
        assert_ne!(OwnerId::local(), OwnerId::from_session("abc"));
    }

    #[test]
    fn a_configured_endpoint_accepts_only_its_own_tokens() {
        let policy = CallerPolicy::http(&["good".to_string()]);
        let CallerPolicy::Http { accepted } = &policy else {
            panic!("expected an HTTP policy");
        };
        assert!(accepted.contains(&OwnerId::from_bearer("good")));
        assert!(!accepted.contains(&OwnerId::from_bearer("bad")));
    }

    #[test]
    fn an_unauthorized_handle_is_indistinguishable_from_a_missing_one() {
        // Anything more specific would let a caller probe for handles it does
        // not own.
        assert!(not_authorized("agent-1").message.contains("unknown"));
    }

    #[test]
    fn session_only_http_fails_closed_without_a_session_header() {
        let request = axum::http::Request::new(());
        let (parts, _) = request.into_parts();
        let error = session_owner(Some(&parts)).expect_err("missing session must fail");
        assert!(error.message.contains("initialized MCP session"));
    }

    #[test]
    fn session_only_http_uses_the_negotiated_session_as_owner() {
        let request = axum::http::Request::builder()
            .header("mcp-session-id", "session-a")
            .body(())
            .expect("request");
        let (parts, _) = request.into_parts();
        assert_eq!(
            session_owner(Some(&parts)).expect("session owner"),
            OwnerId::from_session("session-a")
        );
    }
}
