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

use std::{sync::Arc, time::Duration};

use rmcp::model::ProtocolVersion;
use tower::ServiceExt;

use crate::{
    agent::actor::ConnectMilestone,
    gateway::Gateway,
    http_service,
    mcp::{authorization::CallerPolicy, tasks::TASK_TTL},
    tests::fake_ergo::FakeErgo,
};

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

/// Client `_meta` declaring the tasks extension, which is the whole trigger:
/// tasks are server-directed, and the client's only say is whether it declared
/// that it can follow one.
fn tasks_meta() -> serde_json::Value {
    client_meta(serde_json::json!({
        "extensions": { rmcp::model::TASKS_EXTENSION_ID: {} },
    }))
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
fn router_for(gateway: Arc<Gateway>, callers: CallerPolicy) -> axum::Router {
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
async fn send(
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
    let body = axum::body::to_bytes(response.into_body(), 1 << 20)
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

/// A router over a connected guest, plus the handle and the file a DCC send
/// needs.
///
/// Tasks are only created for operations that outlive their request, and the
/// only such operations here are DCC transfers, so a task test needs a real
/// registered agent — reached the way a client reaches one, through the
/// transport — before it can start anything to observe.
struct ConnectedFixture {
    _server: FakeErgo,
    _directory: tempfile::TempDir,
    router: axum::Router,
    agent_id: String,
    source_path: String,
}

impl ConnectedFixture {
    /// Connect one guest owned by `credential`, on a gateway whose tasks expire
    /// after `task_ttl`.
    async fn start(callers: CallerPolicy, credential: Option<&str>, task_ttl: Duration) -> Self {
        let server = FakeErgo::spawn().await;
        let gateway = Arc::new(Gateway::new(server.config()).with_task_ttl(task_ttl));
        let router = router_for(gateway, callers);
        let (status, body) = send(
            &router,
            authorized(
                Envelope::tool_call("irc.connect", serde_json::json!({ "nickname": "Ariadne" })),
                credential,
            ),
        )
        .await;
        assert_eq!(status, axum::http::StatusCode::OK, "{body}");
        let agent_id = body["result"]["structuredContent"]["agent_id"]
            .as_str()
            .unwrap_or_else(|| panic!("a connected agent: {body}"))
            .to_owned();

        let directory = tempfile::tempdir().expect("temporary directory");
        let source_path = directory.path().join("offered.txt");
        std::fs::write(&source_path, b"payload").expect("write offered file");
        Self {
            _server: server,
            _directory: directory,
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
fn authorized(envelope: Envelope, credential: Option<&str>) -> Envelope {
    match credential {
        Some(token) => envelope.with_header("authorization", &format!("Bearer {token}")),
        None => envelope,
    }
}

/// A `tasks/*` request for one task id.
fn task_request(method: &str, task_id: &str) -> Envelope {
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
