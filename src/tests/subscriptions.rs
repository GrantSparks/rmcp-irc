//! End-to-end `subscriptions/listen` tests over the real Streamable HTTP router.
//!
//! Everything about resource notifications that matters happens between two
//! components that never meet in a unit test: the actor, which decides what
//! changed, and one open subscription, which decides whether the caller hears
//! about it. The registry can be shown to match an event and the service can be
//! shown to authorize a filter, and a host can still sit on a live subscription
//! hearing nothing — which is the one failure a subscription must not have.
//!
//! These tests therefore keep a real listen stream open against a real gateway
//! and read what arrives on it: a wake-up for traffic a watch selected, silence
//! for traffic it did not, a wake-up when the connection degrades, a refusal for
//! another caller's handle, and the last word from a watch that expired.

use std::{sync::Arc, time::Duration};

use futures_util::StreamExt;
use tower::ServiceExt;

use crate::{
    config::Config,
    gateway::Gateway,
    mcp::authorization::CallerPolicy,
    tests::{
        fake_ergo::FakeErgo,
        streamable_http::{Envelope, authorized, client_meta, router_for, send},
    },
};

/// The one protocol revision this server serves.
const VERSION: &str = "2026-07-28";

/// Host the router's DNS-rebinding guard accepts by default.
const HOST: &str = "127.0.0.1:8080";

/// How long a notification that should arrive is waited for.
///
/// Generous, because a test that is merely slow must not read as a lost
/// notification.
const ARRIVES: Duration = Duration::from_secs(5);

/// How long a notification that should never arrive is waited for.
const STAYS_SILENT: Duration = Duration::from_millis(400);

/// One connected guest, and the router its caller reaches it through.
struct Subscribed {
    _server: FakeErgo,
    router: axum::Router,
    agent_id: String,
}

impl Subscribed {
    /// Connect one guest owned by `credential`, over a gateway `configure`
    /// adjusted first.
    async fn start(
        callers: CallerPolicy,
        credential: Option<&str>,
        configure: impl FnOnce(&mut Config),
    ) -> Self {
        let server = FakeErgo::spawn().await;
        let mut config = server.config();
        configure(&mut config);
        let router = router_for(Arc::new(Gateway::new(config)), callers);
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
        Self {
            _server: server,
            router,
            agent_id,
        }
    }

    /// The default fixture: one local caller, one connected guest.
    async fn local() -> Self {
        Self::start(CallerPolicy::Local, None, |_| {}).await
    }

    /// Register a watch and return its descriptor URI.
    async fn watch(&self, targets: serde_json::Value) -> String {
        let (status, body) = send(
            &self.router,
            Envelope::tool_call(
                "irc.watch.create",
                serde_json::json!({ "agent_id": self.agent_id, "targets": targets }),
            ),
        )
        .await;
        assert_eq!(status, axum::http::StatusCode::OK, "{body}");
        body["result"]["structuredContent"]["result"]["watch"]["uri"]
            .as_str()
            .unwrap_or_else(|| panic!("a registered watch: {body}"))
            .to_owned()
    }

    /// Open compound model attention and retain the fields needed to check it.
    async fn attention(&self) -> (String, String, serde_json::Value) {
        let (status, body) = send(
            &self.router,
            Envelope::tool_call(
                "irc.attention.open",
                serde_json::json!({ "agent_id": self.agent_id }),
            ),
        )
        .await;
        assert_eq!(status, axum::http::StatusCode::OK, "{body}");
        let result = &body["result"]["structuredContent"]["result"];
        (
            result["watch"]["watch_id"]
                .as_str()
                .unwrap_or_else(|| panic!("an attention watch id: {body}"))
                .to_owned(),
            result["watch"]["uri"]
                .as_str()
                .unwrap_or_else(|| panic!("an attention watch URI: {body}"))
                .to_owned(),
            result["initial_cursor"].clone(),
        )
    }

    /// Check attention and return the server-observed delivery block.
    async fn attention_delivery(
        &self,
        watch_id: &str,
        cursor: &serde_json::Value,
    ) -> serde_json::Value {
        let (status, body) = send(
            &self.router,
            Envelope::tool_call(
                "irc.attention.check",
                serde_json::json!({
                    "agent_id": self.agent_id,
                    "watch_id": watch_id,
                    "cursor": cursor,
                    "wait_ms": 0,
                }),
            ),
        )
        .await;
        assert_eq!(status, axum::http::StatusCode::OK, "{body}");
        body["result"]["structuredContent"]["result"]["delivery"].clone()
    }

    /// Say something to one target, which journals both the outbound record and
    /// the server's echo of it.
    async fn say(&self, target: &str, text: &str) {
        let (status, body) = send(
            &self.router,
            Envelope::tool_call(
                "irc.send",
                serde_json::json!({
                    "agent_id": self.agent_id,
                    "target": target,
                    "kind": "privmsg",
                    "text": text,
                }),
            ),
        )
        .await;
        assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    }

    /// Open one subscription over these URIs and read what it delivers.
    async fn listen(&self, uris: &[&str], credential: Option<&str>) -> Listening {
        let filter = serde_json::json!({
            "resourceSubscriptions": uris.iter().map(|uri| (*uri).to_owned()).collect::<Vec<_>>(),
        });
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "subscriptions/listen",
            "params": {
                "_meta": client_meta(serde_json::json!({})),
                "notifications": filter,
            },
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
            .header("MCP-Protocol-Version", VERSION)
            .header("Mcp-Method", "subscriptions/listen");
        if let Some(token) = credential {
            request = request.header("authorization", format!("Bearer {token}"));
        }
        let response = self
            .router
            .clone()
            .oneshot(
                request
                    .body(axum::body::Body::from(body.to_string()))
                    .expect("valid request"),
            )
            .await
            .expect("router is infallible");
        Listening {
            status: response.status(),
            body: response.into_body().into_data_stream(),
            text: String::new(),
        }
    }

    /// Read one resource, returning the whole JSON-RPC reply.
    ///
    /// `resources/read` carries its URI in `Mcp-Name` as well as in the body,
    /// and a request missing it never reaches the handler at all.
    async fn read(&self, uri: &str) -> serde_json::Value {
        let mut request = Envelope::new("resources/read");
        request.name = Some(uri.to_owned());
        request.params = serde_json::json!({ "uri": uri });
        let (_, body) = send(&self.router, request).await;
        body
    }
}

#[tokio::test]
async fn attention_check_reports_only_server_observed_live_delivery() {
    let fixture = Subscribed::local().await;
    let (watch_id, watch_uri, cursor) = fixture.attention().await;

    let before = fixture.attention_delivery(&watch_id, &cursor).await;
    assert_eq!(before["mode"], "polling", "{before}");
    assert_eq!(before["stream_open"], false, "{before}");
    assert_eq!(before["covers_resume_resource"], false, "{before}");

    let mut listening = fixture.listen(&[watch_uri.as_str()], None).await;
    listening.acknowledgment().await;
    let mut active = serde_json::Value::Null;
    for _ in 0..20 {
        active = fixture.attention_delivery(&watch_id, &cursor).await;
        if active["mode"] == "notification" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(active["mode"], "notification", "{active}");
    assert_eq!(active["stream_open"], true, "{active}");
    assert_eq!(active["covers_resume_resource"], true, "{active}");
    assert!(
        active["instruction"]
            .as_str()
            .is_some_and(|instruction| instruction.contains("cancel the recurring check")),
        "{active}"
    );
}

/// One `subscriptions/listen` response, read as it arrives rather than after it
/// ends — a subscription that ended would have nothing left to observe.
struct Listening {
    status: axum::http::StatusCode,
    body: axum::body::BodyDataStream,
    text: String,
}

impl Listening {
    /// The next JSON-RPC message, or `None` when nothing arrives in `within`.
    async fn next(&mut self, within: Duration) -> Option<serde_json::Value> {
        loop {
            if let Some(message) = self.buffered() {
                return Some(message);
            }
            let chunk = tokio::time::timeout(within, self.body.next())
                .await
                .ok()??
                .expect("a readable stream chunk");
            self.text
                .push_str(std::str::from_utf8(&chunk).expect("utf-8 stream"));
        }
    }

    /// One complete SSE `data:` line already received, if there is one.
    fn buffered(&mut self) -> Option<serde_json::Value> {
        while let Some(end) = self.text.find('\n') {
            let line: String = self.text.drain(..=end).collect();
            if let Some(data) = line.trim_end().strip_prefix("data:") {
                return Some(serde_json::from_str(data.trim()).expect("a JSON-RPC message"));
            }
        }
        None
    }

    /// The acknowledgment every subscription opens with, and the filter the
    /// server accepted in it.
    async fn acknowledgment(&mut self) -> serde_json::Value {
        let message = self
            .next(ARRIVES)
            .await
            .expect("a subscription opens with its acknowledgment");
        assert_eq!(
            message["method"], "notifications/subscriptions/acknowledged",
            "{message}"
        );
        message["params"]["notifications"].clone()
    }

    /// The URI of the next resource update, failing if something else arrives.
    async fn updated(&mut self) -> String {
        let message = self.next(ARRIVES).await.expect("a resource update");
        assert_eq!(
            message["method"], "notifications/resources/updated",
            "{message}"
        );
        message["params"]["uri"]
            .as_str()
            .expect("an updated URI")
            .to_owned()
    }

    /// Assert nothing more arrives for as long as anything plausibly could.
    async fn assert_silent(&mut self, why: &str) {
        if let Some(message) = self.next(STAYS_SILENT).await {
            panic!("{why}: {message}");
        }
    }
}

#[tokio::test]
async fn a_watch_subscription_wakes_on_traffic_it_selected_and_stays_silent_otherwise() {
    // The whole point of a watch is that a notification means "there is
    // something here for you". A subscription that woke on every line would
    // make the model read a busy relay for nothing; one that never woke would
    // be worse.
    let fixture = Subscribed::local().await;
    let watch = fixture.watch(serde_json::json!(["#dev"])).await;
    let mut listening = fixture.listen(&[watch.as_str()], None).await;
    assert_eq!(listening.status, axum::http::StatusCode::OK);
    assert_eq!(
        listening.acknowledgment().await["resourceSubscriptions"],
        serde_json::json!([watch]),
    );

    fixture.say("#other", "not for this watch").await;
    listening
        .assert_silent("traffic the watch did not select must not wake it")
        .await;

    fixture.say("#dev", "for this watch").await;
    assert_eq!(listening.updated().await, watch);
}

#[tokio::test]
async fn a_status_subscription_wakes_when_the_connection_degrades() {
    // Connection health reaches a subscriber through the status resource and
    // nowhere else: a relay that quietly went into reconnect backoff would
    // otherwise look exactly like a quiet channel.
    let fixture = Subscribed::start(CallerPolicy::Local, None, |config| {
        config.reconnect.initial_delay_ms = 5_000;
        config.reconnect.max_delay_ms = 5_000;
    })
    .await;
    let status = format!("irc://agents/{}/status", fixture.agent_id);
    let mut listening = fixture.listen(&[status.as_str()], None).await;
    assert_eq!(
        listening.acknowledgment().await["resourceSubscriptions"],
        serde_json::json!([status]),
    );

    let (code, body) = send(
        &fixture.router,
        Envelope::tool_call(
            "irc.execute",
            serde_json::json!({
                "agent_id": fixture.agent_id,
                "command": "DROPME",
                "response_mode": "fire_and_forget",
            }),
        ),
    )
    .await;
    assert_eq!(code, axum::http::StatusCode::OK, "{body}");

    assert_eq!(listening.updated().await, status);
}

#[tokio::test]
async fn another_owners_subscription_to_a_watch_is_refused() {
    // A watch handle names another caller's stream. Subscribing to one has to
    // be refused for the same reason reading it is, and at the earliest point
    // that can see who is asking — before any notification could be delivered.
    let callers = CallerPolicy::http(&["owner".to_owned(), "stranger".to_owned()]);
    let fixture = Subscribed::start(callers, Some("owner"), |_| {}).await;
    let (status, body) = send(
        &fixture.router,
        authorized(
            Envelope::tool_call(
                "irc.watch.create",
                serde_json::json!({ "agent_id": fixture.agent_id }),
            ),
            Some("owner"),
        ),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    let watch = body["result"]["structuredContent"]["result"]["watch"]["uri"]
        .as_str()
        .unwrap_or_else(|| panic!("a registered watch: {body}"))
        .to_owned();

    let mut stranger = fixture.listen(&[watch.as_str()], Some("stranger")).await;
    // The SDK acknowledges a requested filter before the handler can see who
    // asked, so the acknowledgment is not the decision; it echoes back the URI
    // this caller itself supplied and tells it nothing.
    stranger.acknowledgment().await;
    let refusal = stranger
        .next(ARRIVES)
        .await
        .expect("a refused subscription still answers");
    assert!(
        refusal["error"].is_object(),
        "another caller's handle must not open a subscription: {refusal}"
    );
    assert!(
        !refusal["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .is_empty(),
        "{refusal}"
    );

    // The owner's own subscription over the same URI is unaffected, and the
    // traffic that wakes it reaches nothing on the stranger's closed stream.
    let mut owned = fixture.listen(&[watch.as_str()], Some("owner")).await;
    assert_eq!(
        owned.acknowledgment().await["resourceSubscriptions"],
        serde_json::json!([watch]),
    );
    let (status, body) = send(
        &fixture.router,
        authorized(
            Envelope::tool_call(
                "irc.send",
                serde_json::json!({
                    "agent_id": fixture.agent_id,
                    "target": "#dev",
                    "kind": "privmsg",
                    "text": "for the owner alone",
                }),
            ),
            Some("owner"),
        ),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    assert_eq!(owned.updated().await, watch);
    stranger
        .assert_silent("a refused subscription never receives an update")
        .await;
}

#[tokio::test]
async fn only_the_watch_descriptor_is_acknowledged_as_subscribable() {
    // A positioned window is a different URI for every position and is never
    // published as changed. Acknowledging one would promise a wake-up that can
    // never come, so the acknowledgment says plainly which subscriptions are
    // live.
    let fixture = Subscribed::local().await;
    let watch = fixture.watch(serde_json::json!([])).await;
    let positioned = format!("{watch}/events/after/stream/1");

    let mut listening = fixture
        .listen(&[positioned.as_str(), watch.as_str()], None)
        .await;

    assert_eq!(
        listening.acknowledgment().await["resourceSubscriptions"],
        serde_json::json!([watch]),
        "the positioned window is not a subscribable form"
    );
    // And the descriptor beside it still works, so the refusal is per URI
    // rather than a rejection of the whole stream.
    fixture.say("#dev", "anything at all").await;
    assert_eq!(listening.updated().await, watch);
}

#[tokio::test]
async fn a_watch_that_keeps_matching_never_lapses_under_its_subscriber() {
    // The handle's time to live is shorter here than the traffic this test
    // spans. A watch that only counted reads as use would lapse in the middle
    // and drop every match after it, with the subscription still open and the
    // caller hearing nothing — the silent non-delivery the whole design exists
    // to prevent.
    let time_to_live = Duration::from_millis(1_000);
    let fixture = Subscribed::start(CallerPolicy::Local, None, |config| {
        config.limits.watch_ttl_ms = time_to_live.as_millis() as u64;
    })
    .await;
    let watch = fixture.watch(serde_json::json!(["#dev"])).await;
    let mut listening = fixture.listen(&[watch.as_str()], None).await;
    listening.acknowledgment().await;

    // Five rounds a quarter of a lifetime apart: every gap is comfortably
    // inside the window, and the run as a whole is comfortably past it.
    let started = std::time::Instant::now();
    for round in 0..5 {
        tokio::time::sleep(time_to_live / 4).await;
        fixture.say("#dev", &format!("round {round}")).await;
        assert_eq!(
            listening.updated().await,
            watch,
            "round {round} arrived after {:?} and must still be delivered",
            started.elapsed()
        );
    }
    assert!(
        started.elapsed() > time_to_live,
        "this test only means something if it outlasts the handle's time to live"
    );
    // An update for this URI also announces the handle being reclaimed, so the
    // deliveries above only prove what they claim if the handle is still there
    // to deliver through.
    let descriptor = fixture.read(&watch).await;
    assert_eq!(
        descriptor["result"]["contents"][0]["uri"], watch,
        "the watch must have stayed alive on delivery alone: {descriptor}"
    );
}

#[tokio::test]
async fn a_watch_that_goes_quiet_past_its_time_to_live_says_so_once() {
    // Expiry is the end of a subscribable resource. Reclaiming the handle in
    // silence would leave the host holding a live subscription to something
    // that has stopped existing, and no later notification would ever tell it.
    let time_to_live = Duration::from_millis(600);
    let fixture = Subscribed::start(CallerPolicy::Local, None, |config| {
        config.limits.watch_ttl_ms = time_to_live.as_millis() as u64;
    })
    .await;
    let watch = fixture.watch(serde_json::json!(["#dev"])).await;
    let mut listening = fixture.listen(&[watch.as_str()], None).await;
    listening.acknowledgment().await;

    tokio::time::sleep(time_to_live * 2).await;
    // Traffic this watch does not select, which is what makes the point: the
    // update that follows is about the handle, not about the message.
    fixture.say("#other", "nothing this watch wants").await;

    assert_eq!(listening.updated().await, watch);
    listening
        .assert_silent("a handle is retired, and announced, exactly once")
        .await;

    let read = fixture.read(&watch).await;
    assert!(
        read["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("expired"),
        "the caller that re-reads must be told the handle is gone: {read}"
    );
}
