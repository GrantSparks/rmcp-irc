//! Every tool's declared `outputSchema` against the results it really returns.
//!
//! MCP 2026-07-28 makes this mandatory rather than advisory: a tool that
//! declares an `outputSchema` **MUST** return conforming `structuredContent`.
//! The Rust SDK validates none of it — the crate's own documentation says to
//! bring the `jsonschema` crate if you want that — so an advertised schema that
//! quietly stopped describing the result is a defect no other test here could
//! see. It is exactly how the failure paths drifted: every tool declared its
//! success shape, and gateway errors, in-band refusals, and rejected IRC
//! commands each answered with something else entirely.
//!
//! So the check is end to end and exhaustive. Schemas are read from `tools/list`
//! exactly as a client reads them; results come from real calls through the real
//! Streamable HTTP router wherever the fixture server can produce them, and from
//! the same construction funnel otherwise. Every published tool is validated on
//! both branches of the envelope, and the pre-envelope shapes are asserted to be
//! *rejected*, so the schema is proved to discriminate rather than merely to
//! accept.

use std::{collections::BTreeMap, str::FromStr, sync::Arc};

use jsonschema::Validator;
use serde_json::{Value, json};

use crate::{
    agent::AgentId,
    dcc::session::{DccDirection, DccKind, DccSession, DccSessionId, DccState},
    error::GatewayError,
    gateway::Gateway,
    irc::correlation::{CommandId, CommandOutcome, CommandResult},
    mcp::{
        authorization::CallerPolicy,
        envelope::{self, ToolFailure},
        tools::{DccChatSendOutput, DccSessionOutput, TOOL_NAMES},
    },
    tests::{
        fake_ergo::FakeErgo,
        gateway_integration::config_with_receive_roots,
        streamable_http::{Envelope, router_for, send, task_request, tasks_meta},
    },
    time::Timestamp,
};

/// Deadline every correlated call in here uses, so a fixture that answers
/// nothing fails the test quickly instead of holding it for ten seconds.
const TIMEOUT_MS: u64 = 2_000;

/// The output schema of every published tool, read the way a client reads it.
async fn declared_schemas(router: &axum::Router) -> BTreeMap<String, Validator> {
    let (status, body) = send(router, Envelope::new("tools/list")).await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    let tools = body["result"]["tools"]
        .as_array()
        .unwrap_or_else(|| panic!("tools/list result: {body}"))
        .clone();
    assert_eq!(tools.len(), TOOL_NAMES.len(), "{body}");
    tools
        .into_iter()
        .map(|tool| {
            let name = tool["name"].as_str().expect("a tool name").to_owned();
            let schema = &tool["outputSchema"];
            assert!(schema.is_object(), "{name} publishes no output schema");
            let validator = jsonschema::validator_for(schema)
                .unwrap_or_else(|error| panic!("{name} declares a valid schema: {error}"));
            (name, validator)
        })
        .collect()
}

/// Assert one instance conforms, naming every violation when it does not.
#[track_caller]
fn conforms(validator: &Validator, tool: &str, label: &str, instance: &Value) {
    let problems: Vec<String> = validator
        .iter_errors(instance)
        .map(|error| format!("{error} at {}", error.instance_path()))
        .collect();
    assert!(
        problems.is_empty(),
        "{tool}: the {label} does not conform to the schema {tool} declares: {problems:?}\n\
         instance: {instance}"
    );
}

/// One correlated exchange the server refused, replies and all.
fn rejected_exchange() -> CommandResult {
    CommandResult {
        command_id: CommandId::new(),
        agent_id: AgentId::new(),
        command: "JOIN".into(),
        outcome: CommandOutcome::Rejected,
        written: true,
        acknowledged: true,
        retriable: false,
        label: None,
        replies: vec![
            crate::irc::wire::WireMessage::parse(bytes::Bytes::from_static(
                b":fake 475 Me #locked :Cannot join channel (+k)",
            ))
            .expect("wire reply"),
        ],
        semantic_result: None,
        warnings: Vec::new(),
        first_event_cursor: None,
    }
}

/// One correlated exchange the server took, with the id it assigned.
fn delivered_exchange() -> CommandResult {
    CommandResult {
        command: "PRIVMSG".into(),
        outcome: CommandOutcome::Completed,
        retriable: true,
        replies: vec![
            crate::irc::wire::WireMessage::parse(bytes::Bytes::from_static(
                b"@msgid=line-1 :Me!u@h PRIVMSG #agents :the first half",
            ))
            .expect("wire reply"),
        ],
        ..rejected_exchange()
    }
}

/// Every failure this gateway can produce, each built by the constructor that
/// production uses rather than transcribed here.
fn every_failure() -> Vec<(&'static str, Value)> {
    [
        (
            "gateway error",
            ToolFailure::from_error(&GatewayError::NotConnected(AgentId::new())),
        ),
        (
            "in-band refusal",
            ToolFailure::refusal("declined", "the caller declined; nothing was applied", true),
        ),
        (
            "correlated command failure",
            ToolFailure::from_command(
                CommandOutcome::Rejected,
                "JOIN #locked: Rejected.",
                rejected_exchange(),
            ),
        ),
        (
            "DCC receive-root refusal",
            ToolFailure::unchosen_destination(
                "name one of the configured roots",
                vec!["inbox".into(), "archive".into()],
                Some(std::path::PathBuf::from("report.txt")),
            ),
        ),
        (
            "partial multi-line send",
            ToolFailure::from_command(
                CommandOutcome::Rejected,
                "IRC send failed after processing 2 line(s): Rejected.",
                rejected_exchange(),
            )
            .with_delivered_lines(vec![delivered_exchange()]),
        ),
        (
            "a followed transfer whose agent went away",
            ToolFailure::from_error(&GatewayError::AgentNotFound(AgentId::new())).with_session(
                DccSession::offered(
                    DccKind::Send,
                    DccDirection::Outbound,
                    "peer",
                    Timestamp::now(),
                ),
            ),
        ),
    ]
    .into_iter()
    .map(|(label, failure)| {
        (
            label,
            envelope::failure(&failure).expect("a failure serializes"),
        )
    })
    .collect()
}

/// The failure branch is shared, so every failure has to satisfy every tool.
///
/// This is the assertion the old code could not have made at all: the bare
/// error object `irc.dcc.list` returned on failure was something its declared
/// `DccListOutput` schema rejected outright.
#[tokio::test]
async fn every_declared_schema_accepts_every_failure_this_gateway_can_produce() {
    let router = router_for(
        Arc::new(Gateway::new(Default::default())),
        CallerPolicy::Local,
    );
    let schemas = declared_schemas(&router).await;
    for (tool, validator) in &schemas {
        for (label, instance) in every_failure() {
            assert_eq!(
                instance["ok"],
                json!(false),
                "{tool}: {label} must be discriminated as a failure"
            );
            conforms(validator, tool, label, &instance);
        }
    }
}

/// The schema has to reject as well as accept, or it proves nothing.
#[tokio::test]
async fn a_declared_schema_refuses_the_shapes_the_envelope_replaced() {
    let router = router_for(
        Arc::new(Gateway::new(Default::default())),
        CallerPolicy::Local,
    );
    let schemas = declared_schemas(&router).await;
    let validator = &schemas["irc.dcc.list"];
    for (label, instance) in [
        // The bare error object every tool used to return on failure.
        (
            "a bare tool error",
            json!({"kind": "not_connected", "message": "no", "retriable": true}),
        ),
        // The tool's own output, no longer at the top level.
        ("an unwrapped success", json!({"sessions": []})),
        // A discriminator that disagrees with the branch it introduces.
        (
            "a mislabelled success",
            json!({"ok": false, "result": {"sessions": []}}),
        ),
        // Both branches at once, which closed objects forbid.
        (
            "both branches",
            json!({
                "ok": true,
                "result": {"sessions": []},
                "error": {"kind": "x", "message": "y", "retriable": false},
            }),
        ),
    ] {
        assert!(
            !validator.is_valid(&instance),
            "irc.dcc.list must reject {label}: {instance}"
        );
    }
}

/// Arguments the published input schema rejects are refused before the tool
/// runs, by generated code that knows nothing about this envelope. That result
/// still has to satisfy the schema the tool declared, or the guarantee has a
/// hole in exactly the place a client is most likely to hit it.
#[tokio::test]
async fn a_call_the_input_schema_refuses_still_answers_inside_the_envelope() {
    let router = router_for(
        Arc::new(Gateway::new(Default::default())),
        CallerPolicy::Local,
    );
    let schemas = declared_schemas(&router).await;
    let refused = call(
        &router,
        "irc.connect",
        json!({"nickname_fallbacks": ["Athena"]}),
    )
    .await;
    assert_eq!(refused["isError"], json!(true), "{refused}");
    let structured = refused["structuredContent"].clone();
    assert_eq!(structured["ok"], json!(false), "{refused}");
    assert_eq!(structured["error"]["kind"], "validation_error", "{refused}");
    assert!(
        structured["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("nickname")),
        "the refusal keeps the explanation the dispatcher wrote: {refused}"
    );
    conforms(
        &schemas["irc.connect"],
        "irc.connect",
        "a refused-argument result",
        &structured,
    );
}

/// Drive every tool for real and validate what comes back.
///
/// Going through the router rather than calling the handler is the point: the
/// activity hint is attached there, so a hint that did not fit the declared
/// schema would be invisible to a test that stopped at the tool.
#[tokio::test]
async fn every_tool_result_conforms_to_the_schema_that_tool_declared() {
    let server = FakeErgo::spawn().await;
    let scratch = tempfile::tempdir().expect("scratch");
    // One receive root, so accepting an offer needs no destination question.
    let gateway = Arc::new(Gateway::new(config_with_receive_roots(
        &server,
        scratch.path(),
        &["inbox"],
    )));
    let router = router_for(gateway.clone(), CallerPolicy::Local);
    let schemas = declared_schemas(&router).await;

    // Every tool that produced a validated success branch.
    let mut succeeded: BTreeMap<String, Value> = BTreeMap::new();
    let mut check = |name: &str, result: &Value| {
        let structured = result["structuredContent"].clone();
        assert_eq!(
            structured["ok"] == json!(false),
            result["isError"] == json!(true),
            "{name}: the discriminator and isError must agree: {result}"
        );
        conforms(&schemas[name], name, "live result", &structured);
        // Every call below is one this fixture can complete, and the coverage
        // assertion at the end needs a validated success from each. Saying so
        // here rather than there is what puts the server's own explanation next
        // to the call that failed instead of in a list of missing names.
        assert_eq!(
            structured["ok"],
            json!(true),
            "{name} was expected to succeed here: {}",
            structured["error"]
        );
        succeeded.insert(name.to_owned(), structured);
    };

    let connected = call(&router, "irc.connect", json!({"nickname": "Ariadne"})).await;
    check("irc.connect", &connected);
    let agent = connected["structuredContent"]["result"]["agent_id"]
        .as_str()
        .unwrap_or_else(|| panic!("a connected agent: {connected}"))
        .to_owned();

    let watch = call(
        &router,
        "irc.watch.create",
        json!({"agent_id": agent, "targets": ["#agents"]}),
    )
    .await;
    check("irc.watch.create", &watch);
    let watch_id = watch["structuredContent"]["result"]["watch"]["watch_id"]
        .as_str()
        .expect("a watch handle")
        .to_owned();

    let attention = call(
        &router,
        "irc.attention.open",
        json!({"agent_id": agent, "full_traffic_targets": ["#agents"]}),
    )
    .await;
    check("irc.attention.open", &attention);
    let attention_watch = attention["structuredContent"]["result"]["watch"]["watch_id"]
        .as_str()
        .expect("an attention watch handle")
        .to_owned();
    let attention_cursor = attention["structuredContent"]["result"]["initial_cursor"].clone();
    let attention_check = call(
        &router,
        "irc.attention.check",
        json!({
            "agent_id": agent,
            "watch_id": attention_watch,
            "cursor": attention_cursor,
            "wait_ms": 0,
            "set_activity_anchor": true,
        }),
    )
    .await;
    check("irc.attention.check", &attention_check);

    // An offered incoming transfer, so acceptance has something real to act on.
    // A peer that listens and says nothing keeps the session non-terminal for
    // as long as this test needs it.
    let peer = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("peer listener");
    let offered = crate::tests::gateway_integration::offered_transfer(
        &gateway,
        &AgentId::from_str(&agent).expect("a published handle"),
        peer.local_addr().expect("peer address"),
        7,
        "offered.txt",
    )
    .await
    .id
    .to_string();

    let source = scratch.path().join("outgoing.txt");
    std::fs::write(&source, b"payload").expect("write source");
    let sent = call(
        &router,
        "irc.dcc.send",
        json!({
            "agent_id": agent,
            "target": "Theseus",
            "source_path": source.to_string_lossy(),
        }),
    )
    .await;
    check("irc.dcc.send", &sent);
    let outgoing = sent["structuredContent"]["result"]["session"]["id"]
        .as_str()
        .expect("an outgoing session")
        .to_owned();

    // The ordinary surface, in an order that leaves every handle usable until
    // the two that release one.
    let agent_only = json!({"agent_id": agent});
    for (name, arguments) in [
        ("irc.status", agent_only.clone()),
        (
            "irc.execute",
            json!({"agent_id": agent, "command": "VERSION", "timeout_ms": TIMEOUT_MS}),
        ),
        (
            "irc.join",
            json!({"agent_id": agent, "channel": "#agents", "timeout_ms": TIMEOUT_MS}),
        ),
        (
            "irc.part",
            json!({"agent_id": agent, "channel": "#agents", "timeout_ms": TIMEOUT_MS}),
        ),
        (
            "irc.send",
            json!({
                "agent_id": agent,
                "target": "#agents",
                "kind": "privmsg",
                "text": "hello",
                "timeout_ms": TIMEOUT_MS,
            }),
        ),
        (
            "irc.history",
            json!({
                "agent_id": agent,
                "target": "#agents",
                "selector": {"kind": "latest"},
                "timeout_ms": TIMEOUT_MS,
            }),
        ),
        (
            "irc.query",
            json!({
                "agent_id": agent,
                "query": {"kind": "whois", "nickname": "peer"},
                "timeout_ms": TIMEOUT_MS,
            }),
        ),
        (
            "irc.whois",
            json!({"agent_id": agent, "nickname": "peer", "timeout_ms": TIMEOUT_MS}),
        ),
        (
            "irc.names",
            json!({"agent_id": agent, "channels": ["#agents"], "timeout_ms": TIMEOUT_MS}),
        ),
        (
            "irc.list",
            json!({"agent_id": agent, "timeout_ms": TIMEOUT_MS}),
        ),
        (
            "irc.mode.get",
            json!({"agent_id": agent, "target": "#agents", "timeout_ms": TIMEOUT_MS}),
        ),
        (
            "irc.help",
            json!({"agent_id": agent, "timeout_ms": TIMEOUT_MS}),
        ),
        (
            "irc.topic.get",
            json!({"agent_id": agent, "channel": "#agents", "timeout_ms": TIMEOUT_MS}),
        ),
        (
            "irc.topic.set",
            json!({"agent_id": agent, "channel": "#agents", "topic": "coordinate", "timeout_ms": TIMEOUT_MS}),
        ),
        (
            "irc.nick.set",
            json!({"agent_id": agent, "nickname": "Ariadne2", "timeout_ms": TIMEOUT_MS}),
        ),
        (
            "irc.away.set",
            json!({"agent_id": agent, "message": "back soon", "timeout_ms": TIMEOUT_MS}),
        ),
        (
            "irc.kick",
            json!({"agent_id": agent, "channel": "#agents", "nickname": "peer", "timeout_ms": TIMEOUT_MS}),
        ),
        (
            "irc.invite",
            json!({"agent_id": agent, "channel": "#agents", "nickname": "peer", "timeout_ms": TIMEOUT_MS}),
        ),
        (
            "irc.monitor.update",
            json!({
                "agent_id": agent,
                "operation": "add",
                "nicknames": ["peer"],
                "timeout_ms": TIMEOUT_MS,
            }),
        ),
        (
            "irc.mode.set",
            json!({
                "agent_id": agent,
                "target": "#agents",
                "modes": "+m",
                "timeout_ms": TIMEOUT_MS,
            }),
        ),
        (
            "irc.reaction.update",
            json!({
                "agent_id": agent,
                "target": "#agents",
                "message_id": "echo-Ariadne",
                "reaction": "+1",
                "operation": "add",
                "timeout_ms": TIMEOUT_MS,
            }),
        ),
        (
            "irc.message.redact",
            json!({"agent_id": agent, "target": "#agents", "message_id": "echo-Ariadne", "timeout_ms": TIMEOUT_MS}),
        ),
        (
            "irc.read.get",
            json!({"agent_id": agent, "target": "#agents", "timeout_ms": TIMEOUT_MS}),
        ),
        (
            "irc.read.set",
            json!({
                "agent_id": agent,
                "target": "#agents",
                "read_at": "2026-08-17T00:00:00Z",
                "timeout_ms": TIMEOUT_MS,
            }),
        ),
        (
            "irc.typing.set",
            json!({"agent_id": agent, "target": "#agents", "state": "active", "timeout_ms": TIMEOUT_MS}),
        ),
        ("irc.events.read", agent_only.clone()),
        (
            "irc.dcc.chat.open",
            json!({"agent_id": agent, "target": "Theseus"}),
        ),
        (
            "irc.dcc.accept",
            json!({"agent_id": agent, "dcc_session_id": offered}),
        ),
        ("irc.dcc.list", agent_only.clone()),
        (
            "irc.dcc.cancel",
            json!({"agent_id": agent, "dcc_session_id": outgoing}),
        ),
        ("irc.watch.close", json!({"watch_id": watch_id})),
        ("irc.disconnect", agent_only.clone()),
    ] {
        let result = call(&router, name, arguments).await;
        check(name, &result);
    }

    // Two tools this fixture cannot drive to a successful branch: rejecting an
    // offer needs one that acceptance above has already consumed, and writing a
    // chat line needs a peer that answered. Their outputs go through the same
    // constructor the tools use, so what is validated is still what the funnel
    // produces.
    for (name, output) in [
        (
            "irc.dcc.reject",
            envelope::success(&DccSessionOutput {
                session: DccSession {
                    state: DccState::Rejected,
                    ..DccSession::offered(
                        DccKind::Chat,
                        DccDirection::Inbound,
                        "peer",
                        Timestamp::now(),
                    )
                },
            }),
        ),
        (
            "irc.dcc.chat.send",
            envelope::success(&DccChatSendOutput {
                dcc_session_id: DccSessionId::new(),
                queued: true,
            }),
        ),
    ] {
        let structured = output.expect("a constructed success serializes");
        conforms(&schemas[name], name, "constructed success", &structured);
        succeeded.insert(name.to_owned(), structured);
    }

    let mut expected: Vec<&str> = TOOL_NAMES.to_vec();
    expected.sort_unstable();
    let covered: Vec<&str> = succeeded.keys().map(String::as_str).collect();
    assert_eq!(
        covered,
        expected,
        "every tool needs a validated success envelope; missing: {:?}",
        expected
            .iter()
            .filter(|name| !succeeded.contains_key(**name))
            .collect::<Vec<_>>()
    );
}

/// A task's terminal payload is a complete tool result, so it has to satisfy
/// the tool's declared schema exactly as one returned directly does.
///
/// Nothing else can see it. The payload never travels through `tools/call`, and
/// the client reads it out of `tasks/get` minutes later — which is where a
/// result shaped unlike anything the schema allows would surface, long after
/// the call that produced it.
#[tokio::test]
async fn a_completed_tasks_terminal_result_conforms_to_the_schema_its_tool_declared() {
    let server = FakeErgo::spawn().await;
    let scratch = tempfile::tempdir().expect("scratch");
    // Short enough that the follow loop reaches its own deadline while the peer
    // is still not there, and settles with the session as it stands. That is
    // the terminal payload, and it goes through the same construction a
    // finished transfer's does.
    let gateway = Arc::new(
        Gateway::new(server.config()).with_task_ttl(std::time::Duration::from_millis(600)),
    );
    let router = router_for(gateway, CallerPolicy::Local);
    let schemas = declared_schemas(&router).await;

    let connected = call(&router, "irc.connect", json!({"nickname": "Ariadne"})).await;
    let agent = connected["structuredContent"]["result"]["agent_id"]
        .as_str()
        .unwrap_or_else(|| panic!("a connected agent: {connected}"))
        .to_owned();
    let source = scratch.path().join("outgoing.txt");
    std::fs::write(&source, b"payload").expect("write source");

    let (status, created) = send(
        &router,
        Envelope::tool_call(
            "irc.dcc.send",
            json!({
                "agent_id": agent,
                "target": "Theseus",
                "source_path": source.to_string_lossy(),
            }),
        )
        .with_meta(Some(tasks_meta())),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{created}");
    assert_eq!(created["result"]["resultType"], "task", "{created}");
    let task_id = created["result"]["taskId"]
        .as_str()
        .unwrap_or_else(|| panic!("a created task: {created}"))
        .to_owned();

    let settled = loop {
        let (_, polled) = send(&router, task_request("tasks/get", &task_id)).await;
        if polled["result"]["status"] != "working" {
            break polled;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    };
    assert_eq!(settled["result"]["status"], "completed", "{settled}");
    let structured = settled["result"]["result"]["structuredContent"].clone();
    assert_eq!(structured["ok"], json!(true), "{settled}");
    assert!(
        structured["result"]["session"]["id"].is_string(),
        "the declared schema nests the session under `session`: {settled}"
    );
    conforms(
        &schemas["irc.dcc.send"],
        "irc.dcc.send",
        "a task's terminal result",
        &structured,
    );
}

/// Call one tool and return its JSON-RPC result.
async fn call(router: &axum::Router, name: &str, arguments: Value) -> Value {
    let (status, body) = send(router, Envelope::tool_call(name, arguments)).await;
    assert_eq!(status, axum::http::StatusCode::OK, "{name}: {body}");
    assert!(body["error"].is_null(), "{name}: {body}");
    body["result"].clone()
}
