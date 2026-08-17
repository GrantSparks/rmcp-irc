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

use std::hash::{DefaultHasher, Hash, Hasher};

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
    /// line, or a debug print can never carry the credential itself. This is a
    /// *naming* function and nothing more: the digest is short and not
    /// cryptographic, so it may never decide whether a credential is accepted
    /// — see [`AcceptedTokens::authorize`], which compares the credential
    /// itself and only then derives the name for it.
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

/// The bearer credentials one endpoint accepts, and the identity each names.
///
/// The credentials themselves are here because authenticating is a comparison
/// against them. Comparing derived [`OwnerId`]s instead would authorize on a
/// 64-bit non-cryptographic digest, and two tokens that happened to share one
/// would then be one caller: the wrong credential would authenticate and
/// inherit the right one's handles. So the digest is only ever the *name* of a
/// caller that has already been recognized by its bytes.
#[derive(Clone, Default)]
pub struct AcceptedTokens(Vec<(String, OwnerId)>);

impl std::fmt::Debug for AcceptedTokens {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // This is the one place in the process holding live credentials, so a
        // derived `Debug` would put them in every log line that prints the
        // caller policy. Only the count is safe to say.
        formatter
            .debug_struct("AcceptedTokens")
            .field("configured", &self.0.len())
            .finish_non_exhaustive()
    }
}

impl AcceptedTokens {
    /// Accept exactly these configured bearer tokens.
    pub fn new(tokens: &[String]) -> Self {
        Self(
            tokens
                .iter()
                .map(|token| (token.clone(), OwnerId::from_bearer(token)))
                .collect(),
        )
    }

    /// Whether this endpoint requires a credential at all.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The identity behind one presented credential, if this endpoint accepts
    /// it.
    ///
    /// Every configured token is compared, and each comparison runs to its end,
    /// so neither which token matched nor how far a wrong one matched is
    /// visible in the time this takes.
    pub fn authorize(&self, presented: &str) -> Option<OwnerId> {
        let mut matched = None;
        for (token, owner) in &self.0 {
            if constant_time_eq(presented.as_bytes(), token.as_bytes()) {
                matched = Some(owner.clone());
            }
        }
        matched
    }

    /// Accept tokens under identities chosen rather than derived.
    ///
    /// The one failure a byte comparison exists to prevent is a digest
    /// collision: two different credentials that reduce to one identity. A real
    /// collision is not something a test can produce, so this stages the
    /// situation directly — an accepted token recorded under the identity some
    /// *other* token derives — and lets a test assert that the other token is
    /// still refused.
    #[cfg(test)]
    fn with_identities(entries: &[(&str, OwnerId)]) -> Self {
        Self(
            entries
                .iter()
                .map(|(token, owner)| ((*token).to_owned(), owner.clone()))
                .collect(),
        )
    }
}

/// Whether two byte strings are equal, in time that depends only on their
/// lengths.
///
/// Folding the whole width rather than returning at the first difference is
/// what keeps a near-miss credential from being distinguishable from a wrong
/// one by how long it took to reject. Unequal lengths are folded in for the
/// same reason instead of short-circuiting.
fn constant_time_eq(presented: &[u8], expected: &[u8]) -> bool {
    let width = presented.len().max(expected.len());
    let mut difference = u8::from(presented.len() != expected.len());
    for index in 0..width {
        let presented = presented.get(index).copied().unwrap_or_default();
        let expected = expected.get(index).copied().unwrap_or_default();
        difference |= presented ^ expected;
    }
    difference == 0
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
        /// Accepted bearer credentials. Empty means the endpoint requires no
        /// credential, and every caller shares the single local owner.
        accepted: AcceptedTokens,
    },
}

impl CallerPolicy {
    /// Build the HTTP policy for a set of configured bearer tokens.
    pub fn http(tokens: &[String]) -> Self {
        Self::Http {
            accepted: AcceptedTokens::new(tokens),
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
    accepted: &AcceptedTokens,
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
        .filter(|token| !token.is_empty());
    match presented {
        Some(token) => accepted.authorize(token).ok_or_else(|| {
            McpError::invalid_request("bearer credential is not authorized for this service", None)
        }),
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
        assert_eq!(
            accepted.authorize("good"),
            Some(OwnerId::from_bearer("good"))
        );
        for wrong in ["bad", "goo", "goods", "Good", " good", "good ", ""] {
            assert!(accepted.authorize(wrong).is_none(), "{wrong:?}");
        }
    }

    #[test]
    fn a_credential_that_shares_an_identity_with_an_accepted_one_is_still_refused() {
        // The failure a byte comparison exists to prevent: the identity is a
        // 64-bit non-cryptographic digest, so two tokens can in principle
        // reduce to one owner. Producing a real collision is not something a
        // test can do, so this stages one — the accepted credential is recorded
        // under exactly the identity the *wrong* credential derives — and
        // asserts that acceptance never consults it.
        let accepted =
            AcceptedTokens::with_identities(&[("s3cret", OwnerId::from_bearer("guessed"))]);
        assert_eq!(
            accepted.authorize("s3cret"),
            Some(OwnerId::from_bearer("guessed")),
            "the configured credential is still accepted, under the identity recorded for it"
        );
        assert!(
            accepted.authorize("guessed").is_none(),
            "a credential is accepted for its bytes, never for the identity derived from them"
        );
    }

    #[test]
    fn comparing_a_credential_folds_its_whole_width() {
        // Returning at the first differing byte would make a near-miss
        // measurably slower to reject than a wrong first character.
        assert!(constant_time_eq(b"", b""));
        assert!(constant_time_eq(b"s3cret", b"s3cret"));
        for (presented, expected) in [
            (&b"s3cret"[..], &b"s3cres"[..]),
            (b"s3cret", b"s3cre"),
            (b"s3cre", b"s3cret"),
            // Padding shorter input with zero bytes must not make a token and
            // its zero-extension compare equal.
            (b"s3cret\0", b"s3cret"),
            (b"", b"s3cret"),
        ] {
            assert!(!constant_time_eq(presented, expected), "{presented:?}");
        }
    }

    #[test]
    fn a_configured_credential_set_never_prints_its_credentials() {
        let policy = CallerPolicy::http(&["s3cret".to_string()]);
        let rendered = format!("{policy:?}");
        assert!(!rendered.contains("s3cret"), "{rendered}");
        assert!(rendered.contains("configured"), "{rendered}");
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
        let accepted = AcceptedTokens::default();
        let first = http_owner(&accepted, Some(&parts(&[]))).expect("first request");
        let second = http_owner(&accepted, None).expect("second request");
        assert_eq!(first, OwnerId::local());
        assert_eq!(first, second);
    }

    #[test]
    fn an_mcp_session_id_header_never_contributes_to_identity() {
        // The header does not exist in this protocol revision. Honoring it
        // would let a caller mint its own owner by inventing a value.
        let accepted = AcceptedTokens::default();
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
