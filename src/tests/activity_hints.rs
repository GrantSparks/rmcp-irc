//! The activity hint's three promises, tested where a caller would see them.
//!
//! A hint rides results the model already asked for, which makes it cheap and
//! makes it dangerous in exactly one way: if producing one moved anything, every
//! tool call in this server would silently consume somebody's backlog. So most
//! of what is asserted here is what does *not* happen — repeated calls returning
//! identical hints, watch and journal positions standing still, one owner's
//! traffic staying invisible to another — with the published bounds and the two
//! explicit suppressions alongside.
//!
//! Everything runs through the real Streamable HTTP router, because the
//! attachment point and the ownership gate both live above the tool.

use std::{str::FromStr, sync::Arc, time::Duration};

use serde_json::{Value, json};

use crate::{
    agent::AgentId,
    gateway::Gateway,
    mcp::{
        activity::{INLINE_MENTIONS_CAP, TARGET_CAP},
        authorization::CallerPolicy,
    },
    tests::{
        fake_ergo::FakeErgo,
        streamable_http::{Envelope, authorized, router_for, send},
    },
};

/// Deadline every correlated call here uses, so a fixture that answers nothing
/// fails the test quickly rather than holding it for the default ten seconds.
const TIMEOUT_MS: u64 = 2_000;

/// One connected guest, its router, and the fixture server behind both.
struct Fixture {
    _server: Arc<FakeErgo>,
    gateway: Arc<Gateway>,
    router: axum::Router,
    agent: String,
    credential: Option<&'static str>,
}

impl Fixture {
    async fn start() -> Self {
        Self::start_with(CallerPolicy::Local, None, "Ariadne", json!({}), |_| {}).await
    }

    /// One guest with an explicit caller policy, connect preference, and
    /// configuration.
    async fn start_with(
        callers: CallerPolicy,
        credential: Option<&'static str>,
        nickname: &str,
        activity: Value,
        configure: impl FnOnce(&mut crate::config::Config),
    ) -> Self {
        let server = Arc::new(FakeErgo::spawn().await);
        let mut config = server.config();
        configure(&mut config);
        let gateway = Arc::new(Gateway::new(config));
        let router = router_for(gateway.clone(), callers);
        let mut fixture = Self {
            _server: server,
            gateway,
            router,
            agent: String::new(),
            credential,
        };
        fixture.agent = fixture.connect(nickname, activity).await;
        fixture
    }

    /// A second caller's guest on this same endpoint and gateway.
    async fn also(&self, credential: Option<&'static str>, nickname: &str) -> Self {
        let mut other = Self {
            _server: self._server.clone(),
            gateway: self.gateway.clone(),
            router: self.router.clone(),
            agent: String::new(),
            credential,
        };
        other.agent = other.connect(nickname, json!({})).await;
        other
    }

    async fn connect(&self, nickname: &str, activity: Value) -> String {
        let connected = self
            .call(
                "irc.connect",
                json!({"nickname": nickname, "activity": activity}),
            )
            .await;
        connected["structuredContent"]["result"]["agent_id"]
            .as_str()
            .unwrap_or_else(|| panic!("a connected agent: {connected}"))
            .to_owned()
    }

    async fn call(&self, name: &str, arguments: Value) -> Value {
        let (status, body) = send(
            &self.router,
            authorized(Envelope::tool_call(name, arguments), self.credential),
        )
        .await;
        assert_eq!(status, axum::http::StatusCode::OK, "{name}: {body}");
        assert!(body["error"].is_null(), "{name}: {body}");
        body["result"].clone()
    }

    /// Register a watch over `targets`, which is what gives the counts a
    /// selection to be counted against.
    async fn watch(&self, targets: Value) -> String {
        let created = self
            .call(
                "irc.watch.create",
                json!({"agent_id": self.agent, "targets": targets}),
            )
            .await;
        created["structuredContent"]["result"]["watch"]["watch_id"]
            .as_str()
            .unwrap_or_else(|| panic!("a watch handle: {created}"))
            .to_owned()
    }

    async fn join(&self, channel: &str) {
        let joined = self
            .call(
                "irc.join",
                json!({"agent_id": self.agent, "channel": channel, "timeout_ms": TIMEOUT_MS}),
            )
            .await;
        assert_eq!(joined["isError"], json!(false), "{joined}");
    }

    /// The hint one ordinary read-only call carries.
    async fn hint(&self) -> Value {
        let result = self
            .call("irc.status", json!({"agent_id": self.agent}))
            .await;
        assert_eq!(result["isError"], json!(false), "{result}");
        result["structuredContent"]["activity"].clone()
    }

    /// Have a peer say something to this guest, and wait for its journal to
    /// hold that record before returning.
    ///
    /// Waiting for the text rather than for the stream to grow matters: the
    /// gateway is exchanging its own protocol traffic throughout, so a bare
    /// sequence bump proves only that *something* arrived.
    async fn peer_says(&self, target: &str, text: &str) {
        let pushed = self
            .call(
                "irc.execute",
                json!({
                    "agent_id": self.agent,
                    "command": "TESTLINE",
                    "trailing": format!(":peer!u@h PRIVMSG {target} :{text}"),
                    "response_mode": "fire_and_forget",
                    "timeout_ms": TIMEOUT_MS,
                }),
            )
            .await;
        assert_eq!(pushed["isError"], json!(false), "{pushed}");
        tokio::time::timeout(Duration::from_secs(5), async {
            while !self.journaled(text).await {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the gateway journals what {target} received: {text}"));
    }

    /// Whether this guest's journal already holds a *received* record carrying
    /// `text`. Inbound only, because the line that asked the fixture to send it
    /// quotes the same text on the way out.
    async fn journaled(&self, text: &str) -> bool {
        self.gateway
            .read_events(
                &AgentId::from_str(&self.agent).expect("a published handle"),
                None,
                1_000,
                Duration::ZERO,
                crate::agent::journal::EventFilter {
                    direction: Some(crate::agent::journal::EventDirection::Inbound),
                    ..crate::agent::journal::EventFilter::default()
                },
            )
            .await
            .expect("a readable journal")
            .events
            .iter()
            .any(|event| {
                serde_json::to_string(event)
                    .unwrap_or_default()
                    .contains(text)
            })
    }
}

/// The counts, the anchor, and the latest position are a pure function of the
/// journal, so calling twice over an unchanged stream returns the same hint.
#[tokio::test]
async fn two_identical_calls_return_identical_hints() {
    let fixture = Fixture::start().await;
    fixture.watch(json!(["#agents"])).await;
    fixture.join("#agents").await;
    fixture.peer_says("#agents", "first").await;
    fixture.peer_says("#agents", "second").await;

    let first = fixture.hint().await;
    let second = fixture.hint().await;
    assert_eq!(first, second, "a read-only call must not disturb its hint");
    assert_eq!(first["unread"], json!({"#agents": 2}), "{first}");
    assert_eq!(first["total"], json!(2), "{first}");
    assert_eq!(first["watches"], json!(1), "{first}");
    assert!(!first["truncated"].as_bool().expect("a marker"), "{first}");

    // A tool that writes upstream is no different: a caller is never behind on
    // its own traffic, so two identical sends leave the hint where it was.
    let message = json!({
        "agent_id": fixture.agent,
        "target": "#agents",
        "kind": "privmsg",
        "text": "acknowledged",
        "timeout_ms": TIMEOUT_MS,
    });
    let sent = fixture.call("irc.send", message.clone()).await;
    let sent_again = fixture.call("irc.send", message).await;
    for (label, hint) in [
        ("the first send", &sent["structuredContent"]["activity"]),
        ("the second", &sent_again["structuredContent"]["activity"]),
    ] {
        assert_eq!(
            hint["unread"], first["unread"],
            "{label} must not make the caller its own messages behind: {hint}"
        );
        assert_eq!(hint["total"], first["total"], "{label}: {hint}");
        assert_eq!(
            hint["anchor"], first["anchor"],
            "{label} must not move the anchor: {hint}"
        );
    }
    // `latest` is the one part that legitimately differs, because it reports
    // where the stream is rather than what is owed: the caller's own message
    // did move it. That is the whole distinction between a hint and a cursor.
    assert_ne!(
        sent_again["structuredContent"]["activity"]["latest"], first["latest"],
        "the reported stream position still has to be current: {sent_again}"
    );
}

/// Reading events hands the caller a cursor and moves nothing on the server.
/// The anchor moves only for the read that asked for it.
#[tokio::test]
async fn the_anchor_moves_only_for_a_read_that_asked_for_it() {
    let fixture = Fixture::start().await;
    let watch = fixture.watch(json!(["#agents"])).await;
    fixture.join("#agents").await;
    fixture.peer_says("#agents", "one").await;

    let started_at = fixture.hint().await["anchor"].clone();
    assert_eq!(fixture.hint().await["total"], json!(1));

    // An ordinary read leaves the anchor alone, so the same traffic is still
    // reported as unread afterwards.
    let read = fixture
        .call(
            "irc.events.read",
            json!({"agent_id": fixture.agent, "watch_id": watch, "limit": 50}),
        )
        .await;
    let page = read["structuredContent"]["result"].clone();
    assert!(page["next_cursor"].is_object(), "{read}");
    let hint = fixture.hint().await;
    assert_eq!(hint["anchor"], started_at, "{hint}");
    assert_eq!(hint["total"], json!(1), "a read is not an acknowledgement");

    // The same read, asking explicitly, is.
    let acknowledged = fixture
        .call(
            "irc.events.read",
            json!({
                "agent_id": fixture.agent,
                "watch_id": watch,
                "limit": 50,
                "set_activity_anchor": true,
            }),
        )
        .await;
    assert_eq!(
        acknowledged["structuredContent"]["result"]["events"], page["events"],
        "asking to move the anchor must not change what the read returns"
    );
    let hint = fixture.hint().await;
    assert_eq!(hint["anchor"], page["next_cursor"], "{hint}");
    assert_eq!(hint["total"], json!(0), "{hint}");
    assert_eq!(hint["unread"], json!({}), "{hint}");

    // The watch still holds no position of its own: the same window is there
    // for whoever presents the same cursor next.
    let again = fixture
        .call(
            "irc.events.read",
            json!({"agent_id": fixture.agent, "watch_id": watch, "limit": 50}),
        )
        .await;
    assert_eq!(
        again["structuredContent"]["result"]["events"], page["events"],
        "computing a hint must never consume a watch window"
    );
}

/// The compact scheduler read can acknowledge the same courtesy hint without
/// consuming its caller-owned attention cursor. This keeps the two merged
/// delivery features from advertising a page forever after it was handled.
#[tokio::test]
async fn an_attention_check_can_align_the_hint_anchor_without_consuming_delivery() {
    let fixture = Fixture::start().await;
    let opened = fixture
        .call("irc.attention.open", json!({"agent_id": fixture.agent}))
        .await;
    let attention = &opened["structuredContent"]["result"];
    let watch_id = attention["watch"]["watch_id"].clone();
    let initial_cursor = attention["initial_cursor"].clone();

    fixture.peer_says("Ariadne", "attention please").await;
    assert_eq!(fixture.hint().await["total"], json!(1));

    let checked = fixture
        .call(
            "irc.attention.check",
            json!({
                "agent_id": fixture.agent,
                "watch_id": watch_id,
                "cursor": initial_cursor,
                "wait_ms": 0,
                "set_activity_anchor": true,
            }),
        )
        .await;
    let page = &checked["structuredContent"]["result"];
    assert_eq!(page["state"], "events", "{checked}");
    assert_eq!(
        page["events"].as_array().map(Vec::len),
        Some(1),
        "{checked}"
    );
    assert!(
        checked["structuredContent"]["activity"].is_null(),
        "the recurring check is already an activity read and carries no redundant hint: {checked}"
    );
    let later_hint = fixture.hint().await;
    assert_eq!(
        later_hint["anchor"], page["resume_cursor"],
        "the next ordinary result sees the explicit acknowledgement"
    );
    assert_eq!(later_hint["total"], 0);

    // The old cursor still returns the same page: only the hint anchor moved.
    let retried = fixture
        .call(
            "irc.attention.check",
            json!({
                "agent_id": fixture.agent,
                "watch_id": watch_id,
                "cursor": initial_cursor,
                "wait_ms": 0,
            }),
        )
        .await;
    assert_eq!(
        retried["structuredContent"]["result"]["events"], page["events"],
        "acknowledging a hint never consumes the attention window"
    );
}

/// Counts stop at the published cap with a marker, and the total keeps what the
/// map dropped.
#[tokio::test]
async fn the_counts_map_stops_at_its_cap_and_says_so() {
    let fixture = Fixture::start().await;
    // One watch over every target, so each channel below is counted.
    fixture.watch(json!([])).await;
    let mut expected_total = 0_u64;
    for index in 0..TARGET_CAP + 2 {
        let channel = format!("#room{index:02}");
        fixture.join(&channel).await;
        // A busier channel for every higher index, so which entries survive the
        // cap is a fact rather than an accident of ordering.
        for line in 0..=index {
            fixture.peer_says(&channel, &format!("line {line}")).await;
            expected_total += 1;
        }
    }

    let hint = fixture.hint().await;
    let unread = hint["unread"].as_object().expect("counts");
    assert_eq!(unread.len(), TARGET_CAP, "{hint}");
    assert_eq!(hint["truncated"], json!(true), "{hint}");
    assert_eq!(
        hint["total"],
        json!(expected_total),
        "the total keeps what the map dropped: {hint}"
    );
    assert!(
        unread.contains_key(&format!("#room{:02}", TARGET_CAP + 1)),
        "the busiest target survives the cap: {hint}"
    );
    assert!(
        !unread.contains_key("#room00"),
        "the quietest target is the one dropped: {hint}"
    );
}

/// Targets are keyed by the server's own case folding, so a watch registered
/// for one spelling counts traffic the server reports in another.
#[tokio::test]
async fn counts_are_keyed_by_the_servers_case_folding() {
    let fixture = Fixture::start().await;
    fixture.watch(json!(["#Agents"])).await;
    fixture.join("#agents").await;
    fixture.peer_says("#agents", "folded").await;
    assert_eq!(fixture.hint().await["unread"], json!({"#agents": 1}));
}

/// Inlining is off unless the connect asked for it, bounded when it did, and
/// refused outright beyond the published maximum.
#[tokio::test]
async fn inlined_mentions_are_opt_in_and_hard_bounded() {
    let quiet = Fixture::start().await;
    quiet.watch(json!([])).await;
    quiet.peer_says("Ariadne", "look at this").await;
    let hint = quiet.hint().await;
    assert_eq!(hint["mentions"], json!([]), "the default inlines nothing");
    assert_eq!(hint["total"], json!(1), "but still counts it: {hint}");

    let loud = Fixture::start_with(
        CallerPolicy::Local,
        None,
        "Hestia",
        json!({"inline_mentions": INLINE_MENTIONS_CAP}),
        |_| {},
    )
    .await;
    loud.watch(json!([])).await;
    for index in 0..INLINE_MENTIONS_CAP + 2 {
        loud.peer_says("Hestia", &format!("mention {index}")).await;
    }
    let hint = loud.hint().await;
    let mentions = hint["mentions"].as_array().expect("inlined mentions");
    assert_eq!(mentions.len(), INLINE_MENTIONS_CAP, "{hint}");
    assert_eq!(
        mentions[INLINE_MENTIONS_CAP - 1]["text"],
        json!(format!("mention {}", INLINE_MENTIONS_CAP + 1)),
        "the newest mention is the one that must survive: {hint}"
    );
    assert!(
        mentions
            .iter()
            .all(|mention| mention["mentions_me"] == json!(true)),
        "only records addressed to the agent are inlineable: {hint}"
    );

    // A preference past the cap is named rather than quietly clamped.
    let refused = loud
        .call(
            "irc.connect",
            json!({
                "nickname": "Selene",
                "activity": {"inline_mentions": INLINE_MENTIONS_CAP + 1},
            }),
        )
        .await;
    assert_eq!(refused["isError"], json!(true), "{refused}");
    let message = refused["structuredContent"]["error"]["message"]
        .as_str()
        .unwrap_or_default();
    assert!(
        message.contains("activity.inline_mentions"),
        "the refusal must name the field: {refused}"
    );
}

/// Hints reach every family of tool that names an agent, and reach neither the
/// call that mints the anchor nor a result that failed.
#[tokio::test]
async fn hints_ride_every_tool_family_that_names_an_agent() {
    let fixture = Fixture::start().await;
    fixture.watch(json!([])).await;
    fixture.peer_says("Ariadne", "are you there").await;

    for (name, arguments) in [
        // A read-only tool.
        ("irc.status", json!({"agent_id": fixture.agent})),
        // Another read-only tool, whose own result is a cursor page.
        (
            "irc.events.read",
            json!({"agent_id": fixture.agent, "limit": 5}),
        ),
        // A command tool that writes upstream and correlates its reply.
        (
            "irc.whois",
            json!({"agent_id": fixture.agent, "nickname": "peer", "timeout_ms": TIMEOUT_MS}),
        ),
        // A DCC tool.
        ("irc.dcc.list", json!({"agent_id": fixture.agent})),
    ] {
        let result = fixture.call(name, arguments).await;
        assert_eq!(result["isError"], json!(false), "{name}: {result}");
        assert_eq!(
            result["structuredContent"]["activity"]["total"],
            json!(1),
            "{name} carries no hint: {result}"
        );
    }

    // `irc.connect` carries none: its own call is where the anchor is born.
    let connected = fixture
        .call("irc.connect", json!({"nickname": "Nyx"}))
        .await;
    assert!(
        connected["structuredContent"]["activity"].is_null(),
        "{connected}"
    );

    // Neither does a failure: the failure branch has no room for one, and news
    // of a mention does not belong inside a report that something went wrong.
    let refused = fixture
        .call(
            "irc.dcc.cancel",
            json!({
                "agent_id": fixture.agent,
                "dcc_session_id": crate::dcc::session::DccSessionId::new().to_string(),
            }),
        )
        .await;
    assert_eq!(refused["isError"], json!(true), "{refused}");
    assert!(
        refused["structuredContent"]["activity"].is_null(),
        "{refused}"
    );
}

/// Two owners on one endpoint. Neither one's results may count the other's
/// traffic, or reveal that the other exists at all.
#[tokio::test]
async fn one_owners_hint_never_counts_another_owners_traffic() {
    let mine = Fixture::start_with(
        CallerPolicy::http(&["mine".into(), "yours".into()]),
        Some("mine"),
        "Ariadne",
        json!({}),
        |_| {},
    )
    .await;
    mine.watch(json!([])).await;

    let theirs = mine.also(Some("yours"), "Hestia").await;
    theirs.watch(json!([])).await;
    theirs.peer_says("Hestia", "for you alone").await;

    assert_eq!(
        mine.hint().await["total"],
        json!(0),
        "another owner's traffic is not this caller's unread mail"
    );
    assert_eq!(theirs.hint().await["total"], json!(1));

    // And the handle itself is still unusable across owners, so there is no
    // second route to the same counts.
    let (_, stolen) = send(
        &mine.router,
        authorized(
            Envelope::tool_call("irc.status", json!({"agent_id": theirs.agent})),
            Some("mine"),
        ),
    )
    .await;
    assert_eq!(stolen["error"]["code"], json!(-32602), "{stolen}");
}

/// Both suppressions, and only those two.
#[tokio::test]
async fn hints_are_suppressed_only_by_configuration_or_the_connect_preference() {
    let disabled = Fixture::start_with(CallerPolicy::Local, None, "Ariadne", json!({}), |config| {
        config.mcp.activity_hints = false;
    })
    .await;
    disabled.watch(json!([])).await;
    disabled.peer_says("Ariadne", "unheard").await;
    assert!(
        disabled.hint().await.is_null(),
        "the deployment turned hints off"
    );

    let opted_out = Fixture::start_with(
        CallerPolicy::Local,
        None,
        "Hestia",
        json!({"enabled": false}),
        |_| {},
    )
    .await;
    opted_out.watch(json!([])).await;
    opted_out.peer_says("Hestia", "unheard").await;
    assert!(
        opted_out.hint().await.is_null(),
        "this agent asked for no hints"
    );

    // A subscribable watch is explicitly *not* a suppression. A host that can be
    // woken has not thereby promised to start a model turn, so the resource
    // being listed and subscribable changes nothing about the hint.
    let subscribed = Fixture::start().await;
    let watch = subscribed.watch(json!([])).await;
    subscribed.peer_says("Ariadne", "still counted").await;
    let (status, resources) = send(&subscribed.router, Envelope::new("resources/list")).await;
    assert_eq!(status, axum::http::StatusCode::OK, "{resources}");
    assert!(
        resources["result"]["resources"]
            .as_array()
            .expect("a listing")
            .iter()
            .any(|resource| resource["uri"] == json!(format!("irc://watches/{watch}"))),
        "the watch is subscribable: {resources}"
    );
    assert_eq!(subscribed.hint().await["total"], json!(1));
}

/// An agent holding no watches has declared no interest, and is told so rather
/// than handed a count of everything that happened on the server.
#[tokio::test]
async fn an_agent_holding_no_watches_is_counted_against_nothing() {
    let fixture = Fixture::start().await;
    fixture.peer_says("Ariadne", "nobody asked for this").await;
    let hint = fixture.hint().await;
    assert_eq!(hint["watches"], json!(0), "{hint}");
    assert_eq!(hint["unread"], json!({}), "{hint}");
    assert_eq!(hint["total"], json!(0), "{hint}");
    assert!(hint["latest"].is_object(), "{hint}");
    assert!(hint["anchor"].is_object(), "{hint}");
}
