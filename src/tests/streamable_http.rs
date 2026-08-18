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
//! Unless a test is about rejecting an incomplete request, every request here
//! is a complete 2026-07-28 request: no session header, the
//! `MCP-Protocol-Version` header matching `_meta`, `Mcp-Method` (and `Mcp-Name`
//! where the method carries one), and both required `_meta` fields.

use std::{str::FromStr, sync::Arc, time::Duration};

use rmcp::model::ProtocolVersion;
use tower::ServiceExt;

use crate::{
    agent::actor::ConnectMilestone,
    gateway::Gateway,
    http_service,
    mcp::{
        authorization::CallerPolicy,
        service::{HISTORY_PROGRESS_TOTAL, TASK_INITIAL_STATUS},
        tasks::TASK_TTL,
    },
    tests::fake_ergo::{CHANNEL_KEY, FakeErgo, KEYED_CHANNEL},
};

/// The one protocol revision this server serves.
const VERSION: &str = "2026-07-28";

/// Host the router's DNS-rebinding guard accepts by default.
const HOST: &str = "127.0.0.1:8080";

/// One JSON-RPC request plus the HTTP framing a 2026-07-28 POST requires.
pub(super) struct Envelope {
    method: String,
    pub(super) name: Option<String>,
    pub(super) params: serde_json::Value,
    meta: Option<serde_json::Value>,
    headers: Vec<(String, String)>,
    protocol_version_header: Option<String>,
}

impl Envelope {
    /// A request that satisfies every requirement, ready to be spoiled one
    /// field at a time.
    pub(super) fn new(method: &str) -> Self {
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
    pub(super) fn tool_call(name: &str, arguments: serde_json::Value) -> Self {
        let mut envelope = Self::new("tools/call");
        envelope.name = Some(name.to_owned());
        envelope.params = serde_json::json!({ "name": name, "arguments": arguments });
        envelope
    }

    /// Replace the request `_meta`, or omit it entirely with `None`.
    pub(super) fn with_meta(mut self, meta: Option<serde_json::Value>) -> Self {
        self.meta = meta;
        self
    }

    /// Answer one input request and echo its state back, the way a client
    /// retries an MRTR round: same method, same arguments, new request id.
    fn answering(mut self, key: &str, answer: serde_json::Value, request_state: &str) -> Self {
        if let Some(object) = self.params.as_object_mut() {
            object.insert("inputResponses".into(), serde_json::json!({ key: answer }));
            object.insert("requestState".into(), request_state.into());
        }
        self
    }

    /// Echo a state back without answering anything.
    fn echoing(mut self, request_state: &str) -> Self {
        if let Some(object) = self.params.as_object_mut() {
            object.insert("requestState".into(), request_state.into());
        }
        self
    }

    /// Add one HTTP header.
    pub(super) fn with_header(mut self, name: &str, value: &str) -> Self {
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
pub(super) fn client_meta(capabilities: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "io.modelcontextprotocol/protocolVersion": VERSION,
        "io.modelcontextprotocol/clientInfo": { "name": "test-client", "version": "0.0.0" },
        "io.modelcontextprotocol/clientCapabilities": capabilities,
    })
}

/// Client `_meta` declaring the tasks extension, which is the whole trigger:
/// tasks are server-directed, and the client's only say is whether it declared
/// that it can follow one.
pub(super) fn tasks_meta() -> serde_json::Value {
    client_meta(serde_json::json!({
        "extensions": { rmcp::model::TASKS_EXTENSION_ID: {} },
    }))
}

/// Client `_meta` declaring form-mode elicitation, without which the server
/// must never send an input request at all.
fn elicitation_meta() -> serde_json::Value {
    client_meta(serde_json::json!({ "elicitation": {} }))
}

/// A client that can both follow a task and answer a form.
fn tasks_and_elicitation_meta() -> serde_json::Value {
    client_meta(serde_json::json!({
        "extensions": { rmcp::model::TASKS_EXTENSION_ID: {} },
        "elicitation": {},
    }))
}

/// The question one `input_required` response carries, with its opaque state.
///
/// Asserting the envelope here keeps every flow's test honest about the same
/// three things: the result type, the input request under the expected key, and
/// a `requestState` the client is meant to echo without reading.
fn question(body: &serde_json::Value, key: &str) -> (serde_json::Value, String) {
    assert_eq!(body["result"]["resultType"], "input_required", "{body}");
    assert!(
        body["result"]["taskId"].is_null(),
        "an unanswered question is not a task: {body}"
    );
    let request = body["result"]["inputRequests"][key].clone();
    assert_eq!(request["method"], "elicitation/create", "{body}");
    assert_eq!(request["params"]["mode"], "form", "{body}");
    let state = body["result"]["requestState"]
        .as_str()
        .unwrap_or_else(|| panic!("a sealed request state: {body}"))
        .to_owned();
    (request, state)
}

/// The structured payload of one in-band refusal.
///
/// A refusal is always a result with `isError`, never a JSON-RPC error: the call
/// was well formed and reached the tool, so the answer belongs where the model
/// can read it.
fn refusal(body: &serde_json::Value) -> serde_json::Value {
    assert!(body["error"].is_null(), "a refusal is in band: {body}");
    assert_eq!(body["result"]["resultType"], "complete", "{body}");
    assert_eq!(body["result"]["isError"], true, "{body}");
    assert_eq!(
        body["result"]["structuredContent"]["ok"], false,
        "the envelope discriminator always agrees with isError: {body}"
    );
    body["result"]["structuredContent"]["error"].clone()
}

/// One accepted form answer, shaped the way a client echoes an elicitation back.
fn accepted(content: serde_json::Value) -> serde_json::Value {
    serde_json::json!({ "action": "accept", "content": content })
}

/// Every line the fixture server has received and not yet been read here.
fn written(lines: &mut tokio::sync::broadcast::Receiver<String>) -> Vec<String> {
    let mut seen = Vec::new();
    while let Ok(line) = lines.try_recv() {
        seen.push(line);
    }
    seen
}

/// A router sharing one gateway, so successive requests see the same state the
/// deployed process would.
fn router(callers: CallerPolicy) -> axum::Router {
    router_for(Arc::new(Gateway::new(Default::default())), callers)
}

/// A router over one specific gateway.
///
/// Every request builds its own handler, so the gateway is the only thing two
/// requests share — which is exactly what the cross-request tests are about.
pub(super) fn router_for(gateway: Arc<Gateway>, callers: CallerPolicy) -> axum::Router {
    http_service(
        gateway,
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
pub(super) async fn send(
    router: &axum::Router,
    envelope: Envelope,
) -> (axum::http::StatusCode, serde_json::Value) {
    let (status, messages) = send_stream(router, envelope).await;
    let reply = messages
        .into_iter()
        .find(|message| message.get("method").is_none())
        .expect("a reply to the request itself");
    (status, reply)
}

/// Send one request and return every JSON-RPC message its response carried.
///
/// A request that opted into progress gets notifications *and* its result on
/// one stream, in order, so a test that cares about what the server said while
/// working needs all of them rather than only the last.
async fn send_stream(
    router: &axum::Router,
    envelope: Envelope,
) -> (axum::http::StatusCode, Vec<serde_json::Value>) {
    let response = router
        .clone()
        .oneshot(envelope.into_request())
        .await
        .expect("router is infallible");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 16 << 20)
        .await
        .expect("read response body");
    let text = String::from_utf8(body.to_vec()).expect("utf-8 response");
    let messages: Vec<serde_json::Value> = if text.starts_with('{') {
        vec![serde_json::from_str(&text).expect("JSON-RPC message")]
    } else {
        text.lines()
            .filter_map(|line| line.strip_prefix("data:"))
            .map(|data| serde_json::from_str(data.trim()).expect("JSON-RPC message"))
            .collect()
    };
    assert!(!messages.is_empty(), "a response carries at least a reply");
    (status, messages)
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
    // Every tool declares the same two-branch envelope, and the tool's own
    // output lives under the success branch's `result`.
    let success = &status["outputSchema"]["oneOf"][0];
    assert_eq!(success["properties"]["ok"]["const"], true, "{status}");
    assert!(
        success["properties"]["result"]["properties"]["caller"].is_object(),
        "the declared capability picture is part of the published status schema: {status}"
    );
    assert_eq!(
        status["outputSchema"]["oneOf"][1]["properties"]["ok"]["const"], false,
        "{status}"
    );
    let attention_open = tools
        .iter()
        .find(|tool| tool["name"] == "irc.attention.open")
        .unwrap_or_else(|| panic!("irc.attention.open is listed: {body}"));
    assert!(
        attention_open["inputSchema"]["properties"]["full_traffic_targets"].is_object(),
        "the compound target policy is part of the strict 2026 tool schema: {attention_open}"
    );
    let attention_defs = &attention_open["outputSchema"]["$defs"];
    assert!(
        attention_defs["AttentionSchedule"]["properties"]["intervalSeconds"].is_object(),
        "the documented camel-case cadence is part of the published output schema: {attention_open}"
    );
    let attention_check = tools
        .iter()
        .find(|tool| tool["name"] == "irc.attention.check")
        .unwrap_or_else(|| panic!("irc.attention.check is listed: {body}"));
    assert!(
        attention_check["outputSchema"]["oneOf"][0]["properties"]["result"]["properties"]
            ["resume_cursor"]
            .is_object(),
        "the attention checkpoint is part of the strict 2026 tool schema: {attention_check}"
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
async fn scheduled_attention_checks_are_ordinary_strict_stateless_requests() {
    let router = router(CallerPolicy::Local);
    let arguments = serde_json::json!({
        "agent_id": crate::agent::AgentId::new().as_str(),
        "watch_id": crate::mcp::watch::WatchId::new(),
        "cursor": { "stream_id": "scheduled-check", "sequence": 0 },
        "wait_ms": 0,
    });

    let (missing_status, missing) = send(
        &router,
        Envelope::tool_call("irc.attention.check", arguments.clone()).with_meta(None),
    )
    .await;
    assert_eq!(
        missing_status,
        axum::http::StatusCode::BAD_REQUEST,
        "{missing}"
    );
    assert_eq!(missing["error"]["code"], -32602, "{missing}");

    let (complete_status, complete) = send(
        &router,
        Envelope::tool_call("irc.attention.check", arguments),
    )
    .await;
    assert_eq!(
        complete_status,
        axum::http::StatusCode::BAD_REQUEST,
        "{complete}"
    );
    assert!(
        complete["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("unknown or expired handle")),
        "complete metadata must reach the ordinary per-request ownership gate: {complete}"
    );
}

#[tokio::test]
async fn a_request_declaring_another_protocol_version_is_refused() {
    // Old enough that this server has none of what it promises: no elicitation
    // to answer an input round with, and no structured tool output.
    let legacy = ProtocolVersion::V_2025_03_26.as_str();
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
async fn a_modern_client_receives_connect_without_native_resource_links() {
    let server = FakeErgo::spawn().await;
    let router = router_for(Arc::new(Gateway::new(server.config())), CallerPolicy::Local);
    let connect = Envelope::tool_call("irc.connect", serde_json::json!({ "nickname": "Ariadne" }));

    let (status, body) = send(&router, connect).await;

    assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    assert_eq!(body["result"]["structuredContent"]["ok"], true, "{body}");
    assert!(
        body["result"]["structuredContent"]["result"]["resources"]["status"].is_string(),
        "structured resource URIs remain available: {body}"
    );
    assert!(
        body["result"]["content"]
            .as_array()
            .is_some_and(|content| content.iter().all(|block| block["type"] != "resource_link")),
        "portable tool results must not include a content variant current hosts reject: {body}"
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

/// A router over a connected guest, plus the handle and the file a DCC send
/// needs.
///
/// Tasks are only created for operations that outlive their request, and the
/// only such operations here are DCC transfers, so a task test needs a real
/// registered agent — reached the way a client reaches one, through the
/// transport — before it can start anything to observe.
struct ConnectedFixture {
    server: FakeErgo,
    _directory: tempfile::TempDir,
    gateway: Arc<Gateway>,
    router: axum::Router,
    agent_id: String,
    source_path: String,
}

impl ConnectedFixture {
    /// Connect one guest owned by `credential`, on a gateway whose tasks expire
    /// after `task_ttl`.
    async fn start(callers: CallerPolicy, credential: Option<&str>, task_ttl: Duration) -> Self {
        Self::start_with(callers, credential, task_ttl, |_| {}).await
    }

    /// The same, over a gateway whose configuration `configure` adjusted first.
    async fn start_with(
        callers: CallerPolicy,
        credential: Option<&str>,
        task_ttl: Duration,
        configure: impl FnOnce(&mut crate::config::Config),
    ) -> Self {
        let server = FakeErgo::spawn().await;
        let mut config = server.config();
        configure(&mut config);
        let gateway = Arc::new(Gateway::new(config).with_task_ttl(task_ttl));
        let router = router_for(gateway.clone(), callers);
        let (status, body) = send(
            &router,
            authorized(
                Envelope::tool_call("irc.connect", serde_json::json!({ "nickname": "Ariadne" })),
                credential,
            ),
        )
        .await;
        assert_eq!(status, axum::http::StatusCode::OK, "{body}");
        let agent_id = body["result"]["structuredContent"]["result"]["agent_id"]
            .as_str()
            .unwrap_or_else(|| panic!("a connected agent: {body}"))
            .to_owned();

        let directory = tempfile::tempdir().expect("temporary directory");
        let source_path = directory.path().join("offered.txt");
        std::fs::write(&source_path, b"payload").expect("write offered file");
        Self {
            server,
            _directory: directory,
            gateway,
            router,
            agent_id,
            source_path: source_path.to_string_lossy().into_owned(),
        }
    }

    /// The `irc.dcc.send` call this fixture's agent can make.
    ///
    /// The offer goes to a peer that never connects, so the session stays
    /// non-terminal and the task stays observable for as long as the test needs.
    fn transfer(&self) -> Envelope {
        Envelope::tool_call(
            "irc.dcc.send",
            serde_json::json!({
                "agent_id": self.agent_id,
                "target": "Theseus",
                "source_path": self.source_path,
            }),
        )
    }
}

/// Present `credential` on a request, when the endpoint asks for one.
pub(super) fn authorized(envelope: Envelope, credential: Option<&str>) -> Envelope {
    match credential {
        Some(token) => envelope.with_header("authorization", &format!("Bearer {token}")),
        None => envelope,
    }
}

/// A `tasks/*` request for one task id.
pub(super) fn task_request(method: &str, task_id: &str) -> Envelope {
    let mut envelope = Envelope::new(method);
    envelope.name = Some(task_id.to_owned());
    envelope.params = serde_json::json!({ "taskId": task_id });
    envelope.with_meta(Some(tasks_meta()))
}

#[tokio::test]
async fn a_task_created_by_one_request_is_observed_and_cancelled_by_later_ones() {
    // The bug this covers: the HTTP transport builds a fresh handler per POST,
    // so a task store owned by the handler is invisible to every request after
    // the one that created it — the client holds an id that resolves to nothing
    // and work it cannot stop.
    let fixture =
        ConnectedFixture::start(CallerPolicy::http(&["mine".into()]), Some("mine"), TASK_TTL).await;

    let (status, created) = send(
        &fixture.router,
        authorized(
            fixture.transfer().with_meta(Some(tasks_meta())),
            Some("mine"),
        ),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{created}");
    assert_eq!(created["result"]["resultType"], "task", "{created}");
    let task_id = created["result"]["taskId"]
        .as_str()
        .unwrap_or_else(|| panic!("a created task: {created}"))
        .to_owned();

    // A second request, with its own handler, must resolve the same id.
    let (status, fetched) = send(
        &fixture.router,
        authorized(task_request("tasks/get", &task_id), Some("mine")),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{fetched}");
    assert_eq!(fetched["result"]["taskId"], task_id.as_str(), "{fetched}");
    assert_eq!(fetched["result"]["status"], "working", "{fetched}");

    // And a third must be able to stop it.
    let (status, cancelled) = send(
        &fixture.router,
        authorized(task_request("tasks/cancel", &task_id), Some("mine")),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{cancelled}");
    assert!(cancelled["error"].is_null(), "{cancelled}");

    let settled = loop {
        let (_, polled) = send(
            &fixture.router,
            authorized(task_request("tasks/get", &task_id), Some("mine")),
        )
        .await;
        if polled["result"]["status"] != "working" {
            break polled;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(
        settled["result"]["status"], "cancelled",
        "cancellation from a separate request must reach the transfer: {settled}"
    );
}

#[tokio::test]
async fn another_callers_task_is_indistinguishable_from_one_that_never_existed() {
    let fixture = ConnectedFixture::start(
        CallerPolicy::http(&["mine".into(), "theirs".into()]),
        Some("mine"),
        TASK_TTL,
    )
    .await;
    let (_, created) = send(
        &fixture.router,
        authorized(
            fixture.transfer().with_meta(Some(tasks_meta())),
            Some("mine"),
        ),
    )
    .await;
    let task_id = created["result"]["taskId"]
        .as_str()
        .unwrap_or_else(|| panic!("a created task: {created}"))
        .to_owned();

    let invented_id = "3f1a5c8e-0000-4000-8000-000000000000";
    let (invented_status, invented) = send(
        &fixture.router,
        authorized(task_request("tasks/get", invented_id), Some("theirs")),
    )
    .await;
    let (stolen_status, stolen) = send(
        &fixture.router,
        authorized(task_request("tasks/get", &task_id), Some("theirs")),
    )
    .await;
    assert_eq!(stolen["error"]["code"], -32602, "{stolen}");
    assert_eq!(
        stolen["error"]["message"],
        format!("unknown task: {task_id}"),
        "a task id is a bearer token, so 'not yours' must read exactly as 'no such task': {stolen}"
    );
    // Down to the transport status: any difference at all is an oracle for
    // which ids exist and belong to somebody else.
    assert_eq!(stolen_status, invented_status);
    assert_eq!(stolen["error"]["code"], invented["error"]["code"]);
    assert_eq!(
        stolen["error"]["message"]
            .as_str()
            .map(|message| message.replace(task_id.as_str(), invented_id)),
        invented["error"]["message"].as_str().map(str::to_owned),
        "{stolen} vs {invented}"
    );

    // Nor may another caller stop it.
    let (_, refused) = send(
        &fixture.router,
        authorized(task_request("tasks/cancel", &task_id), Some("theirs")),
    )
    .await;
    assert_eq!(refused["error"]["code"], -32602, "{refused}");
    let (_, mine) = send(
        &fixture.router,
        authorized(task_request("tasks/get", &task_id), Some("mine")),
    )
    .await;
    assert_eq!(
        mine["result"]["status"], "working",
        "a refused caller must not have disturbed the owner's task: {mine}"
    );
}

#[tokio::test]
async fn an_expired_task_is_unknown_even_to_the_caller_that_created_it() {
    let ttl = Duration::from_millis(200);
    let fixture =
        ConnectedFixture::start(CallerPolicy::http(&["mine".into()]), Some("mine"), ttl).await;
    let (_, created) = send(
        &fixture.router,
        authorized(
            fixture.transfer().with_meta(Some(tasks_meta())),
            Some("mine"),
        ),
    )
    .await;
    assert_eq!(created["result"]["ttlMs"], 200, "{created}");
    let task_id = created["result"]["taskId"]
        .as_str()
        .unwrap_or_else(|| panic!("a created task: {created}"))
        .to_owned();

    tokio::time::sleep(ttl * 3).await;
    let (_, expired) = send(
        &fixture.router,
        authorized(task_request("tasks/get", &task_id), Some("mine")),
    )
    .await;
    assert_eq!(
        expired["error"]["message"],
        format!("unknown task: {task_id}"),
        "past its retention window a task is gone, not merely stale: {expired}"
    );
}

#[tokio::test]
async fn a_restarted_process_does_not_recognize_the_task_ids_it_issued() {
    // Tasks follow in-memory transfers, so nothing about them survives a
    // restart. What matters is that the answer afterwards is the deterministic
    // unknown-task error rather than a hang or a fabricated state.
    let fixture =
        ConnectedFixture::start(CallerPolicy::http(&["mine".into()]), Some("mine"), TASK_TTL).await;
    let (_, created) = send(
        &fixture.router,
        authorized(
            fixture.transfer().with_meta(Some(tasks_meta())),
            Some("mine"),
        ),
    )
    .await;
    let task_id = created["result"]["taskId"]
        .as_str()
        .unwrap_or_else(|| panic!("a created task: {created}"))
        .to_owned();

    let restarted = router_for(
        Arc::new(Gateway::new(Default::default())),
        CallerPolicy::http(&["mine".into()]),
    );
    let (status, forgotten) = send(
        &restarted,
        authorized(task_request("tasks/get", &task_id), Some("mine")),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::BAD_REQUEST, "{forgotten}");
    assert_eq!(forgotten["error"]["code"], -32602, "{forgotten}");
    assert_eq!(
        forgotten["error"]["message"],
        format!("unknown task: {task_id}"),
        "{forgotten}"
    );
}

#[tokio::test]
async fn a_client_that_declared_no_tasks_extension_gets_its_result_directly() {
    let fixture = ConnectedFixture::start(CallerPolicy::Local, None, TASK_TTL).await;

    // Same call, same server, no declaration: the answer is the ordinary
    // synchronous result, because a task handle would be one this client has no
    // method to resolve.
    let (status, direct) = send(&fixture.router, fixture.transfer()).await;
    assert_eq!(status, axum::http::StatusCode::OK, "{direct}");
    assert_eq!(direct["result"]["resultType"], "complete", "{direct}");
    assert!(direct["result"]["taskId"].is_null(), "{direct}");

    // And reaching for the task methods without declaring the extension is
    // refused by the capability gate rather than served.
    let mut without = task_request("tasks/get", "3f1a5c8e-0000-4000-8000-000000000000");
    without = without.with_meta(Some(client_meta(serde_json::json!({}))));
    let (_, refused) = send(&fixture.router, without).await;
    assert_eq!(
        refused["error"]["code"], -32021,
        "using an extension without declaring it is a missing-capability error: {refused}"
    );
}

#[tokio::test]
async fn a_long_connect_reports_its_stages_to_a_caller_that_supplied_a_progress_token() {
    // Registration is several round trips inside one `await`. Without progress
    // the caller sees an opaque pause and cannot tell a slow server from a hung
    // one.
    let server = FakeErgo::spawn().await;
    let router = router_for(Arc::new(Gateway::new(server.config())), CallerPolicy::Local);

    let mut meta = client_meta(serde_json::json!({}));
    meta["progressToken"] = serde_json::json!("connect-1");
    let (status, messages) = send_stream(
        &router,
        Envelope::tool_call("irc.connect", serde_json::json!({ "nickname": "Ariadne" }))
            .with_meta(Some(meta)),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK);

    let mut reported = Vec::new();
    for message in &messages {
        if message["method"] != "notifications/progress" {
            continue;
        }
        assert_eq!(message["params"]["progressToken"], "connect-1", "{message}");
        assert_eq!(
            message["params"]["total"],
            f64::from(ConnectMilestone::TOTAL),
            "{message}"
        );
        reported.push((
            message["params"]["progress"].as_f64().expect("a step"),
            message["params"]["message"]
                .as_str()
                .expect("a description")
                .to_owned(),
        ));
    }

    assert!(
        reported.windows(2).all(|pair| pair[0].0 < pair[1].0),
        "progress for one token must strictly increase: {reported:?}"
    );
    let descriptions: Vec<&str> = reported.iter().map(|(_, text)| text.as_str()).collect();
    for stage in [
        ConnectMilestone::Connecting,
        ConnectMilestone::TransportReady { encrypted: false },
        ConnectMilestone::CapabilitiesNegotiated,
        ConnectMilestone::Registered,
        ConnectMilestone::MotdComplete,
        ConnectMilestone::AutojoinSynchronized,
    ] {
        assert!(
            descriptions.contains(&stage.describe()),
            "{stage:?} was never reported: {descriptions:?}"
        );
    }
    assert!(
        descriptions
            .iter()
            .position(|text| *text == ConnectMilestone::Registered.describe())
            < descriptions
                .iter()
                .position(|text| *text == ConnectMilestone::AutojoinSynchronized.describe()),
        "being registered and being present in channels are different facts, reported in order: \
         {descriptions:?}"
    );

    let last = messages.last().expect("a reply");
    assert!(
        last["result"].is_object(),
        "progress stops at the result, which is last on the stream: {last}"
    );
}

#[tokio::test]
async fn a_connect_without_a_progress_token_narrates_nothing() {
    // Progress may only reference a token an active request supplied, so the
    // absence of one is the caller asking for silence.
    let server = FakeErgo::spawn().await;
    let router = router_for(Arc::new(Gateway::new(server.config())), CallerPolicy::Local);
    let (_, messages) = send_stream(
        &router,
        Envelope::tool_call("irc.connect", serde_json::json!({ "nickname": "Ariadne" })),
    )
    .await;
    assert!(
        messages
            .iter()
            .all(|message| message["method"] != "notifications/progress"),
        "{messages:?}"
    );
}

/// A guest holding one nickname, and a router to compete with it on.
///
/// Nickname arbitration only happens against a server that already knows the
/// name, so every test below starts by registering it for real.
async fn contested(callers: CallerPolicy, credential: Option<&str>) -> (FakeErgo, axum::Router) {
    let server = FakeErgo::spawn().await;
    let router = router_for(Arc::new(Gateway::new(server.config())), callers);
    let (status, body) = send(
        &router,
        authorized(
            Envelope::tool_call("irc.connect", serde_json::json!({ "nickname": "Athena" })),
            credential,
        ),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    assert_eq!(
        body["result"]["structuredContent"]["result"]["nickname"], "Athena",
        "{body}"
    );
    (server, router)
}

/// One `irc.connect` that asks rather than substituting a name.
fn asking_connect() -> Envelope {
    Envelope::tool_call(
        "irc.connect",
        serde_json::json!({ "nickname": "Athena", "nick_conflict_policy": "elicit" }),
    )
    .with_meta(Some(elicitation_meta()))
}

#[tokio::test]
async fn a_refused_nickname_is_asked_about_and_the_answer_registers_the_guest() {
    let (_server, router) = contested(CallerPolicy::Local, None).await;

    let (status, asked) = send(&router, asking_connect()).await;
    assert_eq!(status, axum::http::StatusCode::OK, "{asked}");
    let (request, state) = question(&asked, "connect_nickname");
    let schema = &request["params"]["requestedSchema"];
    assert_eq!(
        schema["properties"]["nickname"]["type"], "string",
        "{asked}"
    );
    assert_eq!(
        schema["properties"]["nickname"]["default"], "Athena_2",
        "the name a headless policy would have taken is offered, not imposed: {asked}"
    );
    let message = request["params"]["message"].as_str().expect("a message");
    assert!(
        message.contains("Athena") && message.contains("already in use"),
        "the caller has to see which name was refused and why: {message}"
    );
    assert!(
        !state.contains("Athena"),
        "request state must stay opaque to the client: {state}"
    );

    // The retry re-sends the original call plus the answer and the state.
    let (status, connected) = send(
        &router,
        asking_connect().answering(
            "connect_nickname",
            accepted(serde_json::json!({ "nickname": "Hestia" })),
            &state,
        ),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{connected}");
    assert_eq!(connected["result"]["resultType"], "complete", "{connected}");
    assert_eq!(connected["result"]["isError"], false, "{connected}");
    let output = &connected["result"]["structuredContent"]["result"];
    assert_eq!(output["nickname"], "Hestia", "{connected}");
    assert_eq!(output["registered"], true, "{connected}");
    assert!(output["agent_id"].is_string(), "{connected}");

    // A retry that echoes the state but answers nothing asks again rather than
    // failing, which is what the specification requires of a partial response.
    let (_, again) = send(&router, asking_connect().echoing(&state)).await;
    question(&again, "connect_nickname");
}

#[tokio::test]
async fn a_declined_nickname_connects_nothing_and_a_headless_policy_never_asks() {
    let (_server, router) = contested(CallerPolicy::Local, None).await;
    let (_, asked) = send(&router, asking_connect()).await;
    let (_, state) = question(&asked, "connect_nickname");

    for action in ["decline", "cancel"] {
        let (status, refused) = send(
            &router,
            asking_connect().answering(
                "connect_nickname",
                serde_json::json!({ "action": action }),
                &state,
            ),
        )
        .await;
        assert_eq!(status, axum::http::StatusCode::OK, "{refused}");
        let structured = refusal(&refused);
        assert_eq!(structured["kind"], "declined", "{refused}");
        assert!(
            structured["message"]
                .as_str()
                .is_some_and(|message| message.contains("no guest was connected")),
            "{refused}"
        );
    }

    // Asking is a policy the caller opts into. Without it the same collision is
    // still resolved silently, exactly as it always was, and no question is
    // ever put to a headless client.
    let (status, suffixed) = send(
        &router,
        Envelope::tool_call("irc.connect", serde_json::json!({ "nickname": "Athena" })),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{suffixed}");
    assert_eq!(suffixed["result"]["resultType"], "complete", "{suffixed}");
    assert_eq!(
        suffixed["result"]["structuredContent"]["result"]["nickname"], "Athena_2",
        "{suffixed}"
    );
    assert_eq!(
        suffixed["result"]["structuredContent"]["result"]["nickname_adjusted"], true,
        "{suffixed}"
    );
}

#[tokio::test]
async fn asking_for_a_nickname_needs_a_request_that_can_answer() {
    let (_server, router) = contested(CallerPolicy::Local, None).await;

    // No elicitation declared: the policy cannot be honored, and quietly
    // suffixing would register an identity the caller did not choose.
    let (status, refused) = send(
        &router,
        Envelope::tool_call(
            "irc.connect",
            serde_json::json!({ "nickname": "Athena", "nick_conflict_policy": "elicit" }),
        ),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{refused}");
    let structured = refusal(&refused);
    assert_eq!(structured["kind"], "validation_error", "{refused}");
    let message = structured["message"].as_str().expect("a message");
    assert!(
        message.contains("suffix") && message.contains("fail"),
        "the refusal has to name the policies that do work: {message}"
    );
}

#[tokio::test]
async fn an_abandoned_registration_leaves_no_agent_and_no_reserved_capacity() {
    // The attempt is abandoned before any handle is published. A caller that
    // asks and never answers must therefore cost nothing: if the provisional
    // actor kept its capacity permit, a client that walked away from a question
    // would quietly retire one of the deployment's agent slots.
    let server = FakeErgo::spawn().await;
    let mut config = server.config();
    config.limits.max_agents = 2;
    let router = router_for(Arc::new(Gateway::new(config)), CallerPolicy::Local);

    let (_, held) = send(
        &router,
        Envelope::tool_call("irc.connect", serde_json::json!({ "nickname": "Athena" })),
    )
    .await;
    let agent_id = held["result"]["structuredContent"]["result"]["agent_id"]
        .as_str()
        .unwrap_or_else(|| panic!("a connected agent: {held}"))
        .to_owned();

    for _ in 0..4 {
        let (_, asked) = send(&router, asking_connect()).await;
        question(&asked, "connect_nickname");
    }

    let (_, listed) = send(&router, Envelope::new("resources/list")).await;
    let resources = listed["result"]["resources"]
        .as_array()
        .unwrap_or_else(|| panic!("a resource listing: {listed}"));
    assert!(
        resources.iter().all(|resource| resource["uri"]
            .as_str()
            .is_some_and(|uri| uri.contains(&agent_id))),
        "an abandoned registration publishes no resources: {listed}"
    );

    // And the second slot is still there to be used.
    let (_, connected) = send(
        &router,
        Envelope::tool_call("irc.connect", serde_json::json!({ "nickname": "Hestia" })),
    )
    .await;
    assert_eq!(
        connected["result"]["structuredContent"]["result"]["nickname"], "Hestia",
        "four abandoned attempts must not have consumed the gateway: {connected}"
    );
}

#[tokio::test]
async fn only_the_caller_that_was_asked_can_answer_the_nickname_question() {
    let (_server, router) = contested(
        CallerPolicy::http(&["mine".into(), "theirs".into()]),
        Some("mine"),
    )
    .await;
    let (_, asked) = send(&router, authorized(asking_connect(), Some("mine"))).await;
    let (_, state) = question(&asked, "connect_nickname");
    let answer = |state: &str, credential| {
        authorized(
            asking_connect().answering(
                "connect_nickname",
                accepted(serde_json::json!({ "nickname": "Hestia" })),
                state,
            ),
            credential,
        )
    };

    for (label, credential, forged) in [
        ("another caller", Some("theirs"), state.clone()),
        ("a tampered state", Some("mine"), format!("{state}x")),
        ("a truncated state", Some("mine"), "rs1.".to_owned()),
    ] {
        let (status, refused) = send(&router, answer(&forged, credential)).await;
        assert_eq!(status, axum::http::StatusCode::OK, "{label}: {refused}");
        let structured = refusal(&refused);
        assert_eq!(structured["kind"], "validation_error", "{label}: {refused}");
        assert!(
            structured["message"]
                .as_str()
                .is_some_and(|message| message.contains("request state")),
            "{label}: {refused}"
        );
    }

    // The caller it was minted for still redeems it.
    let (_, connected) = send(&router, answer(&state, Some("mine"))).await;
    assert_eq!(
        connected["result"]["structuredContent"]["result"]["nickname"], "Hestia",
        "{connected}"
    );
}

#[tokio::test]
async fn a_keyed_channel_asks_for_its_key_and_joins_when_it_is_given() {
    let fixture = ConnectedFixture::start(CallerPolicy::Local, None, TASK_TTL).await;
    let join = |key: Option<&str>| {
        let mut arguments = serde_json::json!({
            "agent_id": fixture.agent_id,
            "channel": KEYED_CHANNEL,
            "timeout_ms": 2_000,
        });
        if let Some(key) = key {
            arguments["key"] = key.into();
        }
        Envelope::tool_call("irc.join", arguments).with_meta(Some(elicitation_meta()))
    };

    let (status, asked) = send(&fixture.router, join(None)).await;
    assert_eq!(status, axum::http::StatusCode::OK, "{asked}");
    let (request, state) = question(&asked, "channel_key");
    let schema = &request["params"]["requestedSchema"];
    assert_eq!(schema["properties"]["key"]["type"], "string", "{asked}");
    assert_eq!(schema["required"], serde_json::json!(["key"]), "{asked}");
    let message = request["params"]["message"].as_str().expect("a message");
    assert!(
        message.contains(KEYED_CHANNEL) && message.contains("+k"),
        "the question names the channel and repeats the server's reason: {message}"
    );

    // The retry re-sends the same call — still without a `key` argument — plus
    // the answer and the state, and the JOIN is issued again with the key.
    let (status, joined) = send(
        &fixture.router,
        join(None).answering(
            "channel_key",
            accepted(serde_json::json!({ "key": CHANNEL_KEY })),
            &state,
        ),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{joined}");
    assert_eq!(joined["result"]["resultType"], "complete", "{joined}");
    assert_eq!(joined["result"]["isError"], false, "{joined}");
    assert_eq!(
        joined["result"]["structuredContent"]["result"]["result"]["outcome"], "completed",
        "{joined}"
    );

    // An answered-with-nothing retry asks again rather than failing.
    let (_, again) = send(&fixture.router, join(None).echoing(&state)).await;
    question(&again, "channel_key");

    // A declined key leaves the channel unjoined and says so.
    let (_, declined) = send(
        &fixture.router,
        join(None).answering(
            "channel_key",
            serde_json::json!({ "action": "decline" }),
            &state,
        ),
    )
    .await;
    let structured = refusal(&declined);
    assert_eq!(structured["kind"], "declined", "{declined}");
    assert!(
        structured["message"]
            .as_str()
            .is_some_and(|message| message.contains("not joined")),
        "{declined}"
    );

    // A state this server did not mint is refused in band, and no JOIN is
    // issued for it: the key would otherwise be accepted from anyone who could
    // guess a token.
    let (_, forged) = send(
        &fixture.router,
        join(None).answering(
            "channel_key",
            accepted(serde_json::json!({ "key": CHANNEL_KEY })),
            &format!("{state}x"),
        ),
    )
    .await;
    let structured = refusal(&forged);
    assert_eq!(structured["kind"], "validation_error", "{forged}");
    assert!(
        structured["message"]
            .as_str()
            .is_some_and(|message| message.contains("request state")),
        "{forged}"
    );
}

#[tokio::test]
async fn a_join_that_cannot_be_asked_about_returns_the_rejection_it_always_did() {
    let fixture = ConnectedFixture::start(CallerPolicy::Local, None, TASK_TTL).await;
    let join = |key: Option<&str>, meta: Option<serde_json::Value>| {
        let mut arguments = serde_json::json!({
            "agent_id": fixture.agent_id,
            "channel": KEYED_CHANNEL,
            "timeout_ms": 2_000,
        });
        if let Some(key) = key {
            arguments["key"] = key.into();
        }
        let envelope = Envelope::tool_call("irc.join", arguments);
        match meta {
            Some(meta) => envelope.with_meta(Some(meta)),
            None => envelope,
        }
    };

    for (label, key, meta) in [
        // Nothing to ask with: a headless caller sees exactly the structured
        // rejection this tool has always returned.
        ("no elicitation", None, None),
        // A key that was supplied and still refused is a wrong key. Asking
        // again would only invite a guess.
        ("a wrong key", Some("open-sesame"), Some(elicitation_meta())),
    ] {
        let (status, rejected) = send(&fixture.router, join(key, meta)).await;
        assert_eq!(status, axum::http::StatusCode::OK, "{label}: {rejected}");
        assert_eq!(
            rejected["result"]["resultType"], "complete",
            "{label}: {rejected}"
        );
        assert_eq!(rejected["result"]["isError"], true, "{label}: {rejected}");
        let result = &rejected["result"]["structuredContent"]["error"]["command_result"];
        assert_eq!(result["outcome"], "rejected", "{label}: {rejected}");
        assert!(
            result["replies"]
                .as_array()
                .is_some_and(|replies| replies.iter().any(|reply| reply["command"] == "475")),
            "the raw numeric stays in the result whoever is asking: {label}: {rejected}"
        );
    }

    // And an ordinary channel is joined without any of this.
    let (_, joined) = send(
        &fixture.router,
        Envelope::tool_call(
            "irc.join",
            serde_json::json!({
                "agent_id": fixture.agent_id,
                "channel": "#agents",
                "timeout_ms": 2_000,
            }),
        )
        .with_meta(Some(elicitation_meta())),
    )
    .await;
    assert_eq!(joined["result"]["isError"], false, "{joined}");
}

/// A gateway that requires a person to approve its two destructive mutations.
async fn guarded() -> ConnectedFixture {
    ConnectedFixture::start_with(CallerPolicy::Local, None, TASK_TTL, |config| {
        config.mcp.confirm_destructive = true;
    })
    .await
}

/// Whether the guest wrote one IRC command upstream.
///
/// The command is the first word that is neither a tag block nor a source
/// prefix, so a negotiated `@label=` on the line does not hide it and a reason
/// that happens to contain the word does not invent it.
fn wrote(lines: &mut tokio::sync::broadcast::Receiver<String>, command: &str) -> bool {
    written(lines).iter().any(|line| {
        line.split(' ')
            .find(|word| !word.starts_with('@') && !word.starts_with(':'))
            .is_some_and(|first| first.eq_ignore_ascii_case(command))
    })
}

#[tokio::test]
async fn a_kick_applies_nothing_until_it_is_confirmed() {
    let fixture = guarded().await;
    let mut lines = fixture.server.client_lines.subscribe();
    let kick = || {
        Envelope::tool_call(
            "irc.kick",
            serde_json::json!({
                "agent_id": fixture.agent_id,
                "channel": "#agents",
                "nickname": "Prometheus",
                "reason": "repeated flooding",
                "timeout_ms": 2_000,
            }),
        )
        .with_meta(Some(elicitation_meta()))
    };

    let (status, asked) = send(&fixture.router, kick()).await;
    assert_eq!(status, axum::http::StatusCode::OK, "{asked}");
    let (request, state) = question(&asked, "destructive_confirmation");
    assert_eq!(
        request["params"]["requestedSchema"]["properties"]["confirm"]["type"], "boolean",
        "{asked}"
    );
    let message = request["params"]["message"].as_str().expect("a message");
    for detail in ["Prometheus", "#agents", "repeated flooding"] {
        assert!(
            message.contains(detail),
            "a person can only approve what the question describes: {message}"
        );
    }
    assert!(
        !wrote(&mut lines, "KICK"),
        "asking must not be the same as doing"
    );

    // Saying no is terminal and changes nothing.
    let (_, declined) = send(
        &fixture.router,
        kick().answering(
            "destructive_confirmation",
            accepted(serde_json::json!({ "confirm": false })),
            &state,
        ),
    )
    .await;
    let structured = refusal(&declined);
    assert_eq!(structured["kind"], "declined", "{declined}");
    assert!(
        structured["message"]
            .as_str()
            .is_some_and(|message| message.contains("nothing was applied")),
        "{declined}"
    );
    assert!(!wrote(&mut lines, "KICK"), "a refusal applies nothing");

    // A state that was not minted here is refused, and still changes nothing.
    let (_, forged) = send(
        &fixture.router,
        kick().answering(
            "destructive_confirmation",
            accepted(serde_json::json!({ "confirm": true })),
            &format!("{state}x"),
        ),
    )
    .await;
    assert_eq!(
        refusal(&forged)["kind"],
        "confirmation_required",
        "{forged}"
    );
    assert!(
        !wrote(&mut lines, "KICK"),
        "a forged confirmation is not a confirmation"
    );

    // Only an answered yes reaches the server.
    let (status, applied) = send(
        &fixture.router,
        kick().answering(
            "destructive_confirmation",
            accepted(serde_json::json!({ "confirm": true })),
            &state,
        ),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{applied}");
    assert_eq!(applied["result"]["isError"], false, "{applied}");
    assert_eq!(
        applied["result"]["structuredContent"]["result"]["result"]["outcome"], "completed",
        "{applied}"
    );
    assert!(wrote(&mut lines, "KICK"), "a confirmed kick is a kick");
}

#[tokio::test]
async fn a_redaction_applies_nothing_until_it_is_confirmed() {
    let fixture = guarded().await;
    let mut lines = fixture.server.client_lines.subscribe();
    let redact = || {
        Envelope::tool_call(
            "irc.message.redact",
            serde_json::json!({
                "agent_id": fixture.agent_id,
                "target": "#agents",
                "message_id": "abc123",
                "timeout_ms": 2_000,
            }),
        )
        .with_meta(Some(elicitation_meta()))
    };

    let (_, asked) = send(&fixture.router, redact()).await;
    let (request, state) = question(&asked, "destructive_confirmation");
    let message = request["params"]["message"].as_str().expect("a message");
    assert!(
        message.contains("abc123") && message.contains("#agents"),
        "{message}"
    );
    assert!(!wrote(&mut lines, "REDACT"), "asking writes nothing");

    // An accepted form with the box unfilled has answered nothing, so the
    // question is asked again rather than treated as approval.
    let (_, again) = send(
        &fixture.router,
        redact().answering(
            "destructive_confirmation",
            accepted(serde_json::json!({})),
            &state,
        ),
    )
    .await;
    question(&again, "destructive_confirmation");
    assert!(!wrote(&mut lines, "REDACT"), "an unfilled box is not a yes");

    let (_, applied) = send(
        &fixture.router,
        redact().answering(
            "destructive_confirmation",
            accepted(serde_json::json!({ "confirm": true })),
            &state,
        ),
    )
    .await;
    assert_eq!(applied["result"]["isError"], false, "{applied}");
    assert_eq!(
        applied["result"]["structuredContent"]["result"]["result"]["outcome"], "completed",
        "{applied}"
    );
    assert!(wrote(&mut lines, "REDACT"));
}

#[tokio::test]
async fn a_confirmation_gate_refuses_a_request_it_cannot_ask() {
    // The setting exists because somebody decided a model may not do this
    // alone. Proceeding when there is nobody to ask would delete that policy
    // while appearing to honor it.
    let fixture = guarded().await;
    let mut lines = fixture.server.client_lines.subscribe();
    let (status, refused) = send(
        &fixture.router,
        Envelope::tool_call(
            "irc.kick",
            serde_json::json!({
                "agent_id": fixture.agent_id,
                "channel": "#agents",
                "nickname": "Prometheus",
                "timeout_ms": 2_000,
            }),
        ),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{refused}");
    let structured = refusal(&refused);
    assert_eq!(structured["kind"], "confirmation_required", "{refused}");
    assert!(
        structured["message"]
            .as_str()
            .is_some_and(|message| message.contains("nothing was applied")),
        "{refused}"
    );
    assert!(!wrote(&mut lines, "KICK"));
}

#[tokio::test]
async fn destructive_tools_are_unchanged_while_the_gate_is_off() {
    // The default. A deployment that never asked for confirmation must not
    // acquire a question because its client happens to declare elicitation.
    let fixture = ConnectedFixture::start(CallerPolicy::Local, None, TASK_TTL).await;
    let mut lines = fixture.server.client_lines.subscribe();
    let (status, applied) = send(
        &fixture.router,
        Envelope::tool_call(
            "irc.kick",
            serde_json::json!({
                "agent_id": fixture.agent_id,
                "channel": "#agents",
                "nickname": "Prometheus",
                "timeout_ms": 2_000,
            }),
        )
        .with_meta(Some(elicitation_meta())),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{applied}");
    assert_eq!(applied["result"]["resultType"], "complete", "{applied}");
    assert_eq!(applied["result"]["isError"], false, "{applied}");
    assert!(wrote(&mut lines, "KICK"));
}

/// A gateway with two receive roots, a connected guest, and one offer waiting.
///
/// Two roots is what makes the destination a question at all, and the offer has
/// to be real: acceptance is refused for anything but a pending inbound offer,
/// so a fabricated handle would never reach the decision under test.
struct OfferedFixture {
    _server: FakeErgo,
    _scratch: tempfile::TempDir,
    gateway: Arc<Gateway>,
    router: axum::Router,
    agent_id: String,
    dcc_session_id: String,
}

async fn offered(router_callers: CallerPolicy) -> OfferedFixture {
    let scratch = tempfile::tempdir().expect("scratch");
    let server = FakeErgo::spawn().await;
    let gateway = Arc::new(Gateway::new(
        crate::tests::gateway_integration::config_with_receive_roots(
            &server,
            scratch.path(),
            &["inbox", "archive"],
        ),
    ));
    let router = router_for(gateway.clone(), router_callers);
    let (_, connected) = send(
        &router,
        Envelope::tool_call("irc.connect", serde_json::json!({ "nickname": "Ariadne" })),
    )
    .await;
    let agent_id = connected["result"]["structuredContent"]["result"]["agent_id"]
        .as_str()
        .unwrap_or_else(|| panic!("a connected agent: {connected}"))
        .to_owned();
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("peer listener");
    let offer = crate::tests::gateway_integration::offered_transfer(
        &gateway,
        &crate::agent::AgentId::from_str(&agent_id).expect("agent handle"),
        listener.local_addr().expect("peer address"),
        16,
        "report.txt",
    )
    .await;
    OfferedFixture {
        _server: server,
        _scratch: scratch,
        gateway,
        router,
        agent_id,
        dcc_session_id: offer.id.to_string(),
    }
}

#[tokio::test]
async fn an_accept_that_still_needs_input_creates_no_task_until_it_is_answered() {
    // The tasks extension resolves input exchanges before a task handle is
    // returned: a task can later ask through tasks/get and tasks/update, but
    // this destination decides what transfer is being created and belongs to
    // the original call. Getting this wrong is not cosmetic — the handle
    // arrives after the originating stream closed, so the caller would hold a
    // task that cannot succeed.
    let fixture = offered(CallerPolicy::Local).await;
    let accept = |meta: serde_json::Value| {
        Envelope::tool_call(
            "irc.dcc.accept",
            serde_json::json!({
                "agent_id": fixture.agent_id,
                "dcc_session_id": fixture.dcc_session_id,
            }),
        )
        .with_meta(Some(meta))
    };
    let router = &fixture.router;

    let (status, asked) = send(router, accept(tasks_and_elicitation_meta())).await;
    assert_eq!(status, axum::http::StatusCode::OK, "{asked}");
    // `question` asserts the absence of a task id: this is the whole point.
    let (request, state) = question(&asked, "dcc_destination");
    assert_eq!(
        request["params"]["requestedSchema"]["properties"]["root"]["enum"],
        serde_json::json!(["inbox", "archive"]),
        "{asked}"
    );

    // Answered, the same call is the long-running work a task exists for.
    let (status, created) = send(
        router,
        accept(tasks_and_elicitation_meta()).answering(
            "dcc_destination",
            accepted(serde_json::json!({ "root": "archive" })),
            &state,
        ),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{created}");
    assert_eq!(created["result"]["resultType"], "task", "{created}");
    assert!(created["result"]["taskId"].is_string(), "{created}");
}

#[tokio::test]
async fn a_task_client_that_cannot_answer_gets_the_roots_error_and_no_task() {
    let fixture = offered(CallerPolicy::Local).await;
    let (status, refused) = send(
        &fixture.router,
        Envelope::tool_call(
            "irc.dcc.accept",
            serde_json::json!({
                "agent_id": fixture.agent_id,
                "dcc_session_id": fixture.dcc_session_id,
            }),
        )
        .with_meta(Some(tasks_meta())),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{refused}");
    assert!(
        refused["result"]["taskId"].is_null(),
        "a call that cannot proceed must not become a task: {refused}"
    );
    let structured = refusal(&refused);
    assert_eq!(
        structured["receive_roots"],
        serde_json::json!(["inbox", "archive"]),
        "the refusal carries the whole choice so the retry can be explicit: {refused}"
    );
    assert_eq!(
        structured["default_destination_path"], "report.txt",
        "{refused}"
    );
}

#[tokio::test]
async fn an_expired_destination_choice_refuses_the_acceptance_and_leaves_the_offer_pending() {
    // The expiry branch is otherwise only reachable by waiting out two minutes,
    // so the state is minted already lapsed and driven through the real tool.
    let fixture = offered(CallerPolicy::Local).await;
    let arguments = serde_json::json!({
        "agent_id": fixture.agent_id,
        "dcc_session_id": fixture.dcc_session_id,
    });
    let input: crate::mcp::tools::DccAcceptInput =
        serde_json::from_value(arguments.clone()).expect("the arguments the tool will see");
    let offer = fixture
        .gateway
        .dcc_session(
            &crate::agent::AgentId::from_str(&fixture.agent_id).expect("agent handle"),
            &crate::dcc::session::DccSessionId::from_str(&fixture.dcc_session_id)
                .expect("session handle"),
        )
        .await
        .expect("the waiting offer");
    let expired = fixture
        .gateway
        .request_states()
        .seal_with_ttl(
            &crate::mcp::authorization::OwnerId::local(),
            &crate::mcp::mrtr::OriginatingOperation::for_tool("irc.dcc.accept", &input.salient()),
            &crate::mcp::dcc_accept::PendingAccept::for_offer(
                &offer,
                vec!["inbox".into(), "archive".into()],
            ),
            Duration::from_millis(1),
        )
        .expect("seal a lapsed destination choice");
    tokio::time::sleep(Duration::from_millis(20)).await;

    let (status, refused) = send(
        &fixture.router,
        Envelope::tool_call("irc.dcc.accept", arguments)
            .with_meta(Some(elicitation_meta()))
            .answering(
                "dcc_destination",
                accepted(serde_json::json!({ "root": "archive" })),
                &expired,
            ),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{refused}");
    let structured = refusal(&refused);
    let message = structured["message"].as_str().expect("a message");
    assert!(
        message.contains("expired") && message.contains("start the operation again"),
        "a caller whose window lapsed can recover by starting over, and has to be told so: \
         {message}"
    );

    // Nothing was consumed: the offer is still there to accept.
    let (_, listed) = send(
        &fixture.router,
        Envelope::tool_call(
            "irc.dcc.list",
            serde_json::json!({ "agent_id": fixture.agent_id }),
        ),
    )
    .await;
    let sessions = listed["result"]["structuredContent"]["result"]["sessions"]
        .as_array()
        .unwrap_or_else(|| panic!("a session listing: {listed}"));
    let offer = sessions
        .iter()
        .find(|session| session["id"] == fixture.dcc_session_id.as_str())
        .unwrap_or_else(|| panic!("the offer survives every refusal: {listed}"));
    assert_eq!(offer["state"], "offered", "{listed}");
}

#[tokio::test]
async fn an_expired_confirmation_refuses_the_kick_and_leaves_the_channel_alone() {
    let fixture = guarded().await;
    let mut lines = fixture.server.client_lines.subscribe();
    let arguments = serde_json::json!({
        "agent_id": fixture.agent_id,
        "channel": "#agents",
        "nickname": "Prometheus",
        "reason": "repeated flooding",
        "timeout_ms": 2_000,
    });
    let input: crate::mcp::tools::KickInput =
        serde_json::from_value(arguments.clone()).expect("the arguments the tool will see");
    let expired = fixture
        .gateway
        .request_states()
        .seal_with_ttl(
            &crate::mcp::authorization::OwnerId::local(),
            &crate::mcp::mrtr::OriginatingOperation::for_tool("irc.kick", &input.salient()),
            &crate::mcp::confirm_action::PendingConfirmation::for_action(input.action()),
            Duration::from_millis(1),
        )
        .expect("seal a lapsed confirmation");
    tokio::time::sleep(Duration::from_millis(20)).await;

    let (status, refused) = send(
        &fixture.router,
        Envelope::tool_call("irc.kick", arguments)
            .with_meta(Some(elicitation_meta()))
            .answering(
                "destructive_confirmation",
                accepted(serde_json::json!({ "confirm": true })),
                &expired,
            ),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{refused}");
    let structured = refusal(&refused);
    assert_eq!(structured["kind"], "confirmation_required", "{refused}");
    let message = structured["message"].as_str().expect("a message");
    assert!(
        message.contains("expired") && message.contains("start the operation again"),
        "{message}"
    );
    assert!(
        !wrote(&mut lines, "KICK"),
        "an approval nobody can still redeem is not an approval"
    );
}

#[tokio::test]
async fn one_confirmation_authorizes_exactly_one_kick() {
    // The state is an HMAC over the caller, the arguments and a deadline, so
    // without a record of what has been spent the identical call replays for
    // two minutes — one person's decision authorizing an unbounded number of
    // irreversible mutations.
    let fixture = guarded().await;
    let mut lines = fixture.server.client_lines.subscribe();
    let kick = || {
        Envelope::tool_call(
            "irc.kick",
            serde_json::json!({
                "agent_id": fixture.agent_id,
                "channel": "#agents",
                "nickname": "Prometheus",
                "timeout_ms": 2_000,
            }),
        )
        .with_meta(Some(elicitation_meta()))
    };

    let (_, asked) = send(&fixture.router, kick()).await;
    let (_, state) = question(&asked, "destructive_confirmation");
    let confirmed = || {
        kick().answering(
            "destructive_confirmation",
            accepted(serde_json::json!({ "confirm": true })),
            &state,
        )
    };

    let (_, applied) = send(&fixture.router, confirmed()).await;
    assert_eq!(applied["result"]["isError"], false, "{applied}");
    assert!(wrote(&mut lines, "KICK"), "a confirmed kick is a kick");

    // The identical call, with the identical state and the identical answer.
    let (status, replayed) = send(&fixture.router, confirmed()).await;
    assert_eq!(status, axum::http::StatusCode::OK, "{replayed}");
    let structured = refusal(&replayed);
    assert_eq!(structured["kind"], "confirmation_required", "{replayed}");
    let message = structured["message"].as_str().expect("a message");
    assert!(
        message.contains("already used") && message.contains("confirm again"),
        "the refusal has to name the recovery: {message}"
    );
    assert!(
        !wrote(&mut lines, "KICK"),
        "one approval must not remove somebody twice"
    );
}

#[tokio::test]
async fn a_nickname_retry_that_can_no_longer_be_asked_is_refused_rather_than_asked_again() {
    // Capabilities are per request in this revision, so a retry is free to
    // declare none. Sending it another form would be a protocol violation
    // rather than a degraded experience.
    let (_server, router) = contested(CallerPolicy::Local, None).await;
    let (_, asked) = send(&router, asking_connect()).await;
    let (_, state) = question(&asked, "connect_nickname");

    let (status, refused) = send(
        &router,
        Envelope::tool_call(
            "irc.connect",
            serde_json::json!({ "nickname": "Athena", "nick_conflict_policy": "elicit" }),
        )
        .echoing(&state),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{refused}");
    let structured = refusal(&refused);
    assert_eq!(structured["kind"], "validation_error", "{refused}");
    assert!(
        structured["message"]
            .as_str()
            .is_some_and(|message| message.contains("suffix") && message.contains("fail")),
        "the retry gets the same answer a first call without elicitation would: {refused}"
    );
}

#[tokio::test]
async fn a_channel_key_retry_that_can_no_longer_be_asked_is_refused_rather_than_asked_again() {
    let fixture = ConnectedFixture::start(CallerPolicy::Local, None, TASK_TTL).await;
    let join = |meta: Option<serde_json::Value>| {
        let envelope = Envelope::tool_call(
            "irc.join",
            serde_json::json!({
                "agent_id": fixture.agent_id,
                "channel": KEYED_CHANNEL,
                "timeout_ms": 2_000,
            }),
        );
        match meta {
            Some(meta) => envelope.with_meta(Some(meta)),
            None => envelope,
        }
    };

    let (_, asked) = send(&fixture.router, join(Some(elicitation_meta()))).await;
    let (_, state) = question(&asked, "channel_key");

    let (status, refused) = send(&fixture.router, join(None).echoing(&state)).await;
    assert_eq!(status, axum::http::StatusCode::OK, "{refused}");
    let structured = refusal(&refused);
    assert_eq!(structured["kind"], "validation_error", "{refused}");
    assert!(
        structured["message"]
            .as_str()
            .is_some_and(|message| message.contains("key") && message.contains("not joined")),
        "the refusal names the one thing that still works: {refused}"
    );
}

#[tokio::test]
async fn a_confirmation_retry_that_can_no_longer_be_asked_is_refused_rather_than_asked_again() {
    let fixture = guarded().await;
    let mut lines = fixture.server.client_lines.subscribe();
    let kick = |meta: Option<serde_json::Value>| {
        let envelope = Envelope::tool_call(
            "irc.kick",
            serde_json::json!({
                "agent_id": fixture.agent_id,
                "channel": "#agents",
                "nickname": "Prometheus",
                "timeout_ms": 2_000,
            }),
        );
        match meta {
            Some(meta) => envelope.with_meta(Some(meta)),
            None => envelope,
        }
    };

    let (_, asked) = send(&fixture.router, kick(Some(elicitation_meta()))).await;
    let (_, state) = question(&asked, "destructive_confirmation");

    let (status, refused) = send(&fixture.router, kick(None).echoing(&state)).await;
    assert_eq!(status, axum::http::StatusCode::OK, "{refused}");
    let structured = refusal(&refused);
    assert_eq!(structured["kind"], "confirmation_required", "{refused}");
    assert!(
        structured["message"]
            .as_str()
            .is_some_and(|message| message.contains("nothing was applied")),
        "{refused}"
    );
    assert!(
        !wrote(&mut lines, "KICK"),
        "an unanswered gate applies nothing"
    );
}

#[tokio::test]
async fn a_followed_transfer_whose_agent_disconnects_completes_carrying_the_error() {
    // The tasks extension reserves `failed` for faults in the protocol exchange
    // itself. An agent that went away mid-transfer is an outcome of the work, so
    // the task completes and the tool result says what happened — otherwise a
    // client reading `failed` cannot tell a lost transfer from a broken server.
    let fixture = ConnectedFixture::start(CallerPolicy::Local, None, TASK_TTL).await;
    let (_, created) = send(
        &fixture.router,
        fixture.transfer().with_meta(Some(tasks_meta())),
    )
    .await;
    let task_id = created["result"]["taskId"]
        .as_str()
        .unwrap_or_else(|| panic!("a created task: {created}"))
        .to_owned();

    // Wait until the task is following the session rather than still starting
    // it, so the disconnect interrupts the follow loop it is about.
    loop {
        let (_, polled) = send(&fixture.router, task_request("tasks/get", &task_id)).await;
        if polled["result"]["statusMessage"] != TASK_INITIAL_STATUS {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let (_, disconnected) = send(
        &fixture.router,
        Envelope::tool_call(
            "irc.disconnect",
            serde_json::json!({ "agent_id": fixture.agent_id }),
        ),
    )
    .await;
    assert_eq!(disconnected["result"]["isError"], false, "{disconnected}");

    let settled = loop {
        let (_, polled) = send(&fixture.router, task_request("tasks/get", &task_id)).await;
        if polled["result"]["status"] != "working" {
            break polled;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(
        settled["result"]["status"], "completed",
        "a tool-level outcome settles the task completed, never failed: {settled}"
    );
    assert!(
        settled["result"]["error"].is_null(),
        "a completed task carries a result, not a JSON-RPC error: {settled}"
    );
    let result = &settled["result"]["result"];
    assert_eq!(result["isError"], true, "{settled}");
    let structured = &result["structuredContent"];
    assert_eq!(structured["ok"], false, "{settled}");
    assert_eq!(structured["error"]["kind"], "not_found", "{settled}");
    assert!(
        structured["error"]["session"]["id"].is_string(),
        "the last snapshot the follower held is the only remaining evidence of the transfer: \
         {settled}"
    );
}

#[tokio::test]
async fn a_history_read_reports_its_stages_to_a_caller_that_supplied_a_progress_token() {
    let fixture = ConnectedFixture::start(CallerPolicy::Local, None, TASK_TTL).await;
    let mut meta = client_meta(serde_json::json!({}));
    meta["progressToken"] = serde_json::json!("history-1");
    let (status, messages) = send_stream(&fixture.router, history(&fixture, Some(meta))).await;
    assert_eq!(status, axum::http::StatusCode::OK);

    let mut reported = Vec::new();
    for message in &messages {
        if message["method"] != "notifications/progress" {
            continue;
        }
        assert_eq!(message["params"]["progressToken"], "history-1", "{message}");
        assert_eq!(
            message["params"]["total"],
            f64::from(HISTORY_PROGRESS_TOTAL),
            "the declared total is the one the notifications carry: {message}"
        );
        reported.push(message["params"]["progress"].as_f64().expect("a step"));
    }
    assert!(
        reported.windows(2).all(|pair| pair[0] < pair[1]),
        "progress for one token must strictly increase: {reported:?}"
    );
    assert_eq!(
        reported.last().copied(),
        Some(f64::from(HISTORY_PROGRESS_TOTAL)),
        "every phase reports, so the last step is the total: {reported:?}"
    );

    let last = messages.last().expect("a reply");
    assert!(
        last["result"].is_object(),
        "progress stops at the result, which is last on the stream: {last}"
    );
}

#[tokio::test]
async fn a_history_read_without_a_progress_token_narrates_nothing() {
    let fixture = ConnectedFixture::start(CallerPolicy::Local, None, TASK_TTL).await;
    let (_, messages) = send_stream(&fixture.router, history(&fixture, None)).await;
    assert!(
        messages
            .iter()
            .all(|message| message["method"] != "notifications/progress"),
        "{messages:?}"
    );
}

/// One `irc.history` call, with or without the `_meta` a caller shapes it with.
fn history(fixture: &ConnectedFixture, meta: Option<serde_json::Value>) -> Envelope {
    let envelope = Envelope::tool_call(
        "irc.history",
        serde_json::json!({
            "agent_id": fixture.agent_id,
            "target": "#agents",
            "selector": { "kind": "latest" },
            "timeout_ms": 2_000,
        }),
    );
    match meta {
        Some(meta) => envelope.with_meta(Some(meta)),
        None => envelope,
    }
}

#[tokio::test]
async fn server_discover_reports_this_servers_capabilities_and_the_revision_it_serves() {
    // Discovery is a request like any other here: there is no handshake to
    // carry it, so a client that cannot ask this cannot learn what the server
    // does at all.
    let (status, body) = send(
        &router(CallerPolicy::Local),
        Envelope::new("server/discover"),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    assert!(body["error"].is_null(), "{body}");
    let result = &body["result"];
    assert_eq!(
        result["supportedVersions"],
        serde_json::json!([VERSION]),
        "discovery names the one revision this server implements: {body}"
    );
    for capability in ["tools", "resources", "prompts", "completions"] {
        assert!(
            result["capabilities"][capability].is_object(),
            "{capability} is missing from discovery: {body}"
        );
    }
    assert!(
        result["capabilities"]["extensions"][rmcp::model::TASKS_EXTENSION_ID].is_object(),
        "a client learns here that this server directs work into tasks: {body}"
    );
    assert!(
        result["instructions"]
            .as_str()
            .is_some_and(|text| text.contains("irc.connect")),
        "the onboarding instructions travel with discovery: {body}"
    );
    assert_eq!(
        result["_meta"]["io.modelcontextprotocol/serverInfo"]["name"],
        env!("CARGO_PKG_NAME"),
        "{body}"
    );
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
