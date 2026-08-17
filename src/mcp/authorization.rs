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
//! * HTTP with configured bearer credentials identifies each caller by the
//!   token it presents, which is a durable authenticated principal;
//! * HTTP without configured credentials is a *trusted* endpoint — the process
//!   refuses to bind one off loopback without an explicit network opt-in — and
//!   therefore exposes exactly one shared local owner.
//!
//! There is deliberately no third, weaker identity. MCP 2026-07-28 has no
//! session lifecycle: `Mcp-Session-Id` is gone from the protocol, so deriving
//! an owner from it separated nobody and only failed closed. The per-request
//! `clientInfo` and `clientCapabilities` a caller declares in `_meta` are
//! self-reported and MUST NOT be treated as authorization identity either;
//! they describe what a client can *handle*, not who it *is*. Separating
//! callers on a shared endpoint requires a credential.

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
        /// means the endpoint requires no credential, and every caller shares
        /// the single local owner.
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
    /// Every request is judged on its own: nothing about a previous request on
    /// the same connection contributes, because in this protocol a connection
    /// is not a session.
    pub fn identify(&self, context: &RequestContext<RoleServer>) -> Result<OwnerId, McpError> {
        let Self::Http { accepted } = self else {
            return Ok(OwnerId::local());
        };
        http_owner(
            accepted,
            context.extensions.get::<axum::http::request::Parts>(),
        )
    }
}

/// Resolve the owner of one HTTP request under a [`CallerPolicy::Http`] policy.
///
/// Split out from [`CallerPolicy::identify`] so the decision can be tested
/// without fabricating a whole `RequestContext`.
fn http_owner(
    accepted: &BTreeSet<OwnerId>,
    parts: Option<&axum::http::request::Parts>,
) -> Result<OwnerId, McpError> {
    if accepted.is_empty() {
        // An endpoint with no configured credentials is not a shared endpoint:
        // startup refuses to bind one off loopback without an explicit
        // trusted-network opt-in. There is nothing left to separate callers
        // by — the protocol has no sessions, and self-declared client identity
        // is not authorization — so this is deliberately one shared owner
        // rather than a weaker per-request identity or a closed door.
        return Ok(OwnerId::local());
    }
    let presented = parts
        .and_then(|parts| parts.headers.get(axum::http::header::AUTHORIZATION))
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(OwnerId::from_bearer);
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

    /// Build the request parts an HTTP caller would arrive with.
    fn parts(headers: &[(&str, &str)]) -> axum::http::request::Parts {
        let mut request = axum::http::Request::builder();
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        let (parts, ()) = request.body(()).expect("request").into_parts();
        parts
    }

    #[test]
    fn stdio_has_exactly_one_owner() {
        assert_eq!(OwnerId::local(), OwnerId::local());
        assert_ne!(OwnerId::local(), OwnerId::from_bearer("abc"));
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
    fn trusted_unauthenticated_http_exposes_one_shared_local_owner() {
        // Startup already restricts this shape to loopback unless the operator
        // opted a trusted network in, so the endpoint has exactly one caller
        // and every request must land on the same owner as stdio does.
        let accepted = BTreeSet::new();
        let first = http_owner(&accepted, Some(&parts(&[]))).expect("first request");
        let second = http_owner(&accepted, None).expect("second request");
        assert_eq!(first, OwnerId::local());
        assert_eq!(first, second);
    }

    #[test]
    fn an_mcp_session_id_header_never_contributes_to_identity() {
        // The header does not exist in this protocol revision. Honoring it
        // would let a caller mint its own owner by inventing a value.
        let accepted = BTreeSet::new();
        let claimed = http_owner(&accepted, Some(&parts(&[("mcp-session-id", "session-a")])))
            .expect("session header is ignored, not rejected");
        assert_eq!(claimed, OwnerId::local());

        let configured = CallerPolicy::http(&["good".to_string()]);
        let CallerPolicy::Http { accepted } = &configured else {
            panic!("expected an HTTP policy");
        };
        let error = http_owner(accepted, Some(&parts(&[("mcp-session-id", "session-a")])))
            .expect_err("a session header is not a credential");
        assert!(error.message.contains("Authorization: Bearer"));
    }

    #[test]
    fn a_credentialed_endpoint_owns_handles_per_accepted_token() {
        let policy = CallerPolicy::http(&["good".to_string()]);
        let CallerPolicy::Http { accepted } = &policy else {
            panic!("expected an HTTP policy");
        };
        assert_eq!(
            http_owner(accepted, Some(&parts(&[("authorization", "Bearer good")])))
                .expect("accepted credential"),
            OwnerId::from_bearer("good")
        );
        let wrong = http_owner(accepted, Some(&parts(&[("authorization", "Bearer bad")])))
            .expect_err("an unconfigured credential is not a self-issued identity");
        assert!(wrong.message.contains("not authorized"));
        let missing =
            http_owner(accepted, Some(&parts(&[]))).expect_err("a credential is required");
        assert!(missing.message.contains("Authorization: Bearer"));
    }
}
