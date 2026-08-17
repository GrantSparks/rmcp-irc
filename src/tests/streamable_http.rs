//! Protocol-level tests that drive the real Streamable HTTP router.
//!
//! Everything else in this crate exercises the handler by calling it directly,
//! which cannot see the half of the contract that lives in the transport:
//! whether the required `_meta` is enforced, whether a removed header still
//! influences anything, whether an unauthenticated endpoint resolves an owner at
//! all. Those only appear when a request arrives as bytes. These tests build the
//! same router [`crate::http_service`] gives the binary and send single requests
//! through it in-process, so a regression in the request envelope is a failing
//! test rather than a client that cannot connect.
//!
//! Every request here is a complete 2026-07-28 request: no session header, the
//! `MCP-Protocol-Version` header matching `_meta`, `Mcp-Method` (and `Mcp-Name`
//! where the method carries one), and both required `_meta` fields.

use std::sync::Arc;

use rmcp::model::ProtocolVersion;
use tower::ServiceExt;

use crate::{gateway::Gateway, http_service, mcp::authorization::CallerPolicy};

/// The one protocol revision this server serves.
const VERSION: &str = "2026-07-28";

/// Host the router's DNS-rebinding guard accepts by default.
const HOST: &str = "127.0.0.1:8080";

/// One JSON-RPC request plus the HTTP framing a 2026-07-28 POST requires.
struct Envelope {
    method: String,
    name: Option<String>,
    params: serde_json::Value,
    meta: Option<serde_json::Value>,
    headers: Vec<(String, String)>,
    protocol_version_header: Option<String>,
}

impl Envelope {
    /// A request that satisfies every requirement, ready to be spoiled one
    /// field at a time.
    fn new(method: &str) -> Self {
        Self {
            method: method.to_owned(),
            name: None,
            params: serde_json::json!({}),
            meta: Some(client_meta(serde_json::json!({}))),
            headers: Vec::new(),
            protocol_version_header: Some(VERSION.to_owned()),
        }
    }

    /// A `tools/call` for one tool, which also carries `Mcp-Name`.
    fn tool_call(name: &str, arguments: serde_json::Value) -> Self {
        let mut envelope = Self::new("tools/call");
        envelope.name = Some(name.to_owned());
        envelope.params = serde_json::json!({ "name": name, "arguments": arguments });
        envelope
    }

    /// Replace the request `_meta`, or omit it entirely with `None`.
    fn with_meta(mut self, meta: Option<serde_json::Value>) -> Self {
        self.meta = meta;
        self
    }

    /// Add one HTTP header.
    fn with_header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_owned(), value.to_owned()));
        self
    }

    /// Build the HTTP request the router will see.
    fn into_request(self) -> axum::http::Request<axum::body::Body> {
        let mut params = self.params;
        if let (Some(object), Some(meta)) = (params.as_object_mut(), self.meta) {
            object.insert("_meta".into(), meta);
        }
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": self.method,
            "params": params,
        });
        let mut request = axum::http::Request::builder()
            .method(axum::http::Method::POST)
            .uri("/mcp")
            .header(axum::http::header::HOST, HOST)
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .header(
                axum::http::header::ACCEPT,
                "application/json, text/event-stream",
            )
            .header("Mcp-Method", &self.method);
        if let Some(version) = &self.protocol_version_header {
            request = request.header("MCP-Protocol-Version", version);
        }
        if let Some(name) = &self.name {
            request = request.header("Mcp-Name", name);
        }
        for (name, value) in &self.headers {
            request = request.header(name.as_str(), value.as_str());
        }
        request
            .body(axum::body::Body::from(body.to_string()))
            .expect("valid request")
    }
}

/// The per-request `_meta` every 2026-07-28 client sends.
fn client_meta(capabilities: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "io.modelcontextprotocol/protocolVersion": VERSION,
        "io.modelcontextprotocol/clientInfo": { "name": "test-client", "version": "0.0.0" },
        "io.modelcontextprotocol/clientCapabilities": capabilities,
    })
}

/// A router sharing one gateway, so successive requests see the same state the
/// deployed process would.
fn router(callers: CallerPolicy) -> axum::Router {
    http_service(
        Arc::new(Gateway::new(Default::default())),
        vec!["localhost".into(), "127.0.0.1".into()],
        Vec::new(),
        callers,
        tokio_util::sync::CancellationToken::new(),
    )
}

/// Send one request and return its HTTP status with the JSON-RPC message it
/// carried.
///
/// A reply arrives either as JSON or as a one-event SSE stream depending on how
/// the transport classified it, and a test cares about neither, so both are
/// unwrapped to the same value here.
async fn send(
    router: &axum::Router,
    envelope: Envelope,
) -> (axum::http::StatusCode, serde_json::Value) {
    let response = router
        .clone()
        .oneshot(envelope.into_request())
        .await
        .expect("router is infallible");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .expect("read response body");
    let text = String::from_utf8(body.to_vec()).expect("utf-8 response");
    let message = if text.starts_with('{') {
        text
    } else {
        text.lines()
            .find_map(|line| line.strip_prefix("data:"))
            .map(str::trim)
            .expect("an SSE reply carries one data event")
            .to_owned()
    };
    (
        status,
        serde_json::from_str(&message).expect("JSON-RPC message"),
    )
}

#[tokio::test]
async fn a_complete_request_reaches_the_tool_surface() {
    let (status, body) = send(&router(CallerPolicy::Local), Envelope::new("tools/list")).await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    let tools = body["result"]["tools"]
        .as_array()
        .unwrap_or_else(|| panic!("tools/list result: {body}"));
    let status = tools
        .iter()
        .find(|tool| tool["name"] == "irc.status")
        .unwrap_or_else(|| panic!("irc.status is listed: {body}"));
    assert!(
        status["outputSchema"]["properties"]["caller"].is_object(),
        "the declared capability picture is part of the published status schema: {status}"
    );
}

#[tokio::test]
async fn two_unauthenticated_requests_resolve_the_same_shared_owner() {
    // The endpoint is trusted rather than credentialed, so identity resolution
    // must succeed and land on one owner. Before this, both requests failed
    // closed demanding a session that the protocol no longer has.
    let router = router(CallerPolicy::http(&[]));
    let mut answers = Vec::new();
    for _ in 0..2 {
        let (status, body) = send(&router, Envelope::new("resources/list")).await;
        assert_eq!(status, axum::http::StatusCode::OK, "{body}");
        assert!(
            body["error"].is_null(),
            "an owner-scoped listing must not need a credential: {body}"
        );
        answers.push(body["result"]["resources"].clone());
    }
    assert_eq!(
        answers[0], answers[1],
        "a shared owner sees one catalog, not a per-request one"
    );
}

#[tokio::test]
async fn a_request_without_the_required_metadata_is_rejected() {
    let router = router(CallerPolicy::Local);

    let (status, body) = send(&router, Envelope::new("resources/list").with_meta(None)).await;
    assert_eq!(
        status,
        axum::http::StatusCode::BAD_REQUEST,
        "a request that declares nothing about itself cannot be served: {body}"
    );
    assert_eq!(body["error"]["code"], -32602, "{body}");

    // Declaring the version but not the capabilities is equally unserveable:
    // the server would have to guess what the caller can follow.
    let (status, body) = send(
        &router,
        Envelope::new("resources/list").with_meta(Some(serde_json::json!({
            "io.modelcontextprotocol/protocolVersion": VERSION,
        }))),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("clientCapabilities")),
        "{body}"
    );
}

#[tokio::test]
async fn a_request_declaring_another_protocol_version_is_refused() {
    let legacy = ProtocolVersion::V_2025_11_25.as_str();
    let mut envelope = Envelope::new("resources/list").with_meta(Some(serde_json::json!({
        "io.modelcontextprotocol/protocolVersion": legacy,
        "io.modelcontextprotocol/clientCapabilities": {},
    })));
    envelope.protocol_version_header = Some(legacy.to_owned());
    let (status, body) = send(&router(CallerPolicy::Local), envelope).await;
    assert_eq!(status, axum::http::StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(
        body["error"]["code"], -32022,
        "an unsupported version must say so rather than be served under other assumptions: {body}"
    );
}

#[tokio::test]
async fn a_session_header_changes_nothing() {
    // `Mcp-Session-Id` was removed from the protocol. A modern server must
    // ignore it, so the same request must behave identically with and without.
    let router = router(CallerPolicy::http(&[]));
    let (plain_status, plain) = send(&router, Envelope::new("resources/list")).await;
    let (claimed_status, claimed) = send(
        &router,
        Envelope::new("resources/list").with_header("Mcp-Session-Id", "invented-by-the-caller"),
    )
    .await;
    assert_eq!(plain_status, claimed_status);
    assert_eq!(plain["result"], claimed["result"]);
    assert!(claimed["error"].is_null(), "{claimed}");
}

#[tokio::test]
async fn a_credentialed_endpoint_answers_only_accepted_tokens() {
    let router = router(CallerPolicy::http(&["shared-secret".to_string()]));

    let (status, accepted) = send(
        &router,
        Envelope::new("resources/list").with_header("authorization", "Bearer shared-secret"),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{accepted}");
    assert!(accepted["result"]["resources"].is_array(), "{accepted}");

    let (_, wrong) = send(
        &router,
        Envelope::new("resources/list").with_header("authorization", "Bearer guessed"),
    )
    .await;
    assert!(
        wrong["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("not authorized")),
        "{wrong}"
    );

    let (_, missing) = send(&router, Envelope::new("resources/list")).await;
    assert!(
        missing["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("Authorization: Bearer")),
        "{missing}"
    );
}

#[tokio::test]
async fn declared_capabilities_do_not_change_whether_a_call_is_accepted() {
    // Capabilities say how the server may answer, never whether it will. A
    // request that declares extensions and elicitation must therefore travel
    // exactly as far as one that declares neither — here, to the ownership gate
    // that refuses a handle nobody holds.
    let router = router(CallerPolicy::Local);
    let unknown = crate::agent::AgentId::new();
    let call = || {
        Envelope::tool_call(
            "irc.status",
            serde_json::json!({ "agent_id": unknown.as_str() }),
        )
    };

    let (plain_status, plain) = send(&router, call()).await;
    let (declared_status, declared) = send(
        &router,
        call().with_meta(Some(client_meta(serde_json::json!({
            "extensions": { "io.modelcontextprotocol/tasks": {} },
            "elicitation": {},
        })))),
    )
    .await;
    assert_eq!(plain_status, declared_status);
    assert_eq!(plain["error"], declared["error"]);
    assert_eq!(declared["error"]["code"], -32602, "{declared}");
    assert!(
        declared["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("unknown or expired handle")),
        "{declared}"
    );
}

#[tokio::test]
async fn a_tool_call_whose_name_header_disagrees_with_its_body_is_refused() {
    let mut envelope = Envelope::tool_call("irc.status", serde_json::json!({}));
    envelope.name = Some("irc.help".into());
    let (status, body) = send(&router(CallerPolicy::Local), envelope).await;
    assert_eq!(status, axum::http::StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["code"], -32020, "{body}");
}

#[tokio::test]
async fn responses_are_never_cacheable_by_an_intermediary() {
    let response = router(CallerPolicy::Local)
        .oneshot(Envelope::new("tools/list").into_request())
        .await
        .expect("router is infallible");
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("private, no-store")
    );
}
