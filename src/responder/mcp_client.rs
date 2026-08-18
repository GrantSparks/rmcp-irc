//! Responder-owned MCP/IRC connection using rmcp's stable 2026-07-28 client.

use std::collections::BTreeSet;

use anyhow::{Context, bail, ensure};
use rmcp::{
    ClientLifecycleMode, ClientServiceExt,
    model::{
        CallToolRequestParams, ClientCapabilities, ClientInfo, Implementation, ProtocolVersion,
        ServerNotification, SubscriptionFilter,
    },
    service::{RoleClient, RunningService, Subscription},
    transport::{
        StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
    },
};
use serde_json::{Map, Value, json};

use super::state::{Cursor, StoredSubscriptionFilter};

type Service = RunningService<RoleClient, ClientInfo>;

/// Connected MCP peer. Dropping it stops all subscription streams.
pub struct McpSession {
    service: Service,
}

/// Handles and subscription recipe produced by `irc.attention.open`.
pub struct AttentionHandles {
    /// Compound watch identifier.
    pub watch_id: String,
    /// Starting checkpoint representing now.
    pub initial_cursor: Cursor,
    /// Exact server-returned consolidated filter.
    pub filter: StoredSubscriptionFilter,
    /// The only update URI that wakes a model turn.
    pub model_resume_resource: String,
}

impl McpSession {
    /// Establish an HTTP client that only negotiates MCP 2026-07-28.
    pub async fn connect(endpoint: &str, bearer_token: Option<&str>) -> anyhow::Result<Self> {
        let mut config = StreamableHttpClientTransportConfig::with_uri(endpoint.to_owned());
        if let Some(token) = bearer_token {
            config = config.auth_header(token.to_owned());
        }
        let transport = StreamableHttpClientTransport::from_config(config);
        let client = ClientInfo::new(
            ClientCapabilities::default(),
            Implementation::new("irc-codex-responder", env!("CARGO_PKG_VERSION")),
        )
        .with_protocol_version(ProtocolVersion::V_2026_07_28);
        let service = client
            .serve_with_lifecycle(
                transport,
                ClientLifecycleMode::Discover {
                    preferred_versions: vec![ProtocolVersion::V_2026_07_28],
                },
            )
            .await
            .with_context(|| format!("connect to MCP endpoint {endpoint}"))?;
        let negotiated = service
            .peer_info()
            .context("MCP discovery returned no server information")?;
        ensure!(
            negotiated.protocol_version == ProtocolVersion::V_2026_07_28,
            "MCP server negotiated {}, but responder requires 2026-07-28",
            negotiated.protocol_version
        );
        Ok(Self { service })
    }

    /// Connect one guest using exactly the three ordered operator candidates.
    pub async fn connect_irc(
        &self,
        candidates: &[String],
        channels: &[String],
    ) -> anyhow::Result<(String, String, String)> {
        ensure!(
            candidates.len() == 3,
            "exactly three nickname candidates are required"
        );
        let result = self
            .call(
                "irc.connect",
                json!({
                    "nickname": candidates[0],
                    "nickname_fallbacks": [&candidates[1], &candidates[2]],
                    "nick_conflict_policy": "fail",
                    "username": "codex-responder",
                    "real_name": "irc-codex-responder",
                    "channels": channels,
                    "result_detail": "compact",
                    "activity": {"enabled": false, "inline_mentions": 0}
                }),
            )
            .await
            .context("connect IRC guest")?;
        let agent_id = string_at(&result, "/agent_id")?;
        let nickname = string_at(&result, "/nickname")?;
        ensure!(
            result.get("registered").and_then(Value::as_bool) == Some(true),
            "irc.connect did not report a registered guest"
        );
        let motd = result
            .pointer("/motd/text")
            .and_then(Value::as_str)
            .context("irc.connect returned no MOTD text")?
            .to_owned();
        Ok((agent_id, nickname, motd))
    }

    /// Prove an existing agent handle remains live.
    pub async fn status(&self, agent_id: &str) -> anyhow::Result<Value> {
        self.call(
            "irc.status",
            json!({"agent_id": agent_id, "result_detail": "compact"}),
        )
        .await
    }

    /// Create the single compound attention watch.
    pub async fn open_attention(
        &self,
        agent_id: &str,
        full_traffic_targets: &[String],
    ) -> anyhow::Result<AttentionHandles> {
        let result = self
            .call(
                "irc.attention.open",
                json!({
                    "agent_id": agent_id,
                    "full_traffic_targets": full_traffic_targets
                }),
            )
            .await
            .context("open IRC attention")?;
        let watch_id = string_at(&result, "/watch/watch_id")?;
        let initial_cursor = cursor_at(&result, "/initial_cursor")?;
        let model_resume_resource = string_at(&result, "/subscription/modelResumeResource")?;
        let filter_value = result
            .pointer("/subscription/filterAddition")
            .cloned()
            .context("attention result omitted subscription.filterAddition")?;
        let filter: StoredSubscriptionFilter = serde_json::from_value(filter_value)
            .context("attention result carried an incompatible subscription filter")?;
        ensure!(
            filter
                .resource_subscriptions
                .as_ref()
                .is_some_and(|uris| uris.contains(&model_resume_resource)),
            "attention subscription does not cover modelResumeResource"
        );
        Ok(AttentionHandles {
            watch_id,
            initial_cursor,
            filter,
            model_resume_resource,
        })
    }

    /// Open `subscriptions/listen` and require acknowledgement of the wake URI.
    pub async fn listen(
        &self,
        filter: &StoredSubscriptionFilter,
        model_resume_resource: &str,
    ) -> anyhow::Result<Subscription> {
        let requested = subscription_filter(filter);
        let subscription = self
            .service
            .listen(requested)
            .await
            .context("open MCP subscriptions/listen")?;
        ensure!(
            subscription
                .acknowledged()
                .resource_subscriptions
                .as_ref()
                .is_some_and(|uris| uris.iter().any(|uri| uri == model_resume_resource)),
            "MCP subscription acknowledgement did not cover modelResumeResource {model_resume_resource}"
        );
        Ok(subscription)
    }

    /// Read one compact attention page without moving the caller-owned cursor.
    pub async fn check_attention(
        &self,
        agent_id: &str,
        watch_id: &str,
        cursor: &Cursor,
    ) -> anyhow::Result<Value> {
        self.call(
            "irc.attention.check",
            json!({
                "agent_id": agent_id,
                "watch_id": watch_id,
                "cursor": cursor,
                "limit": 100,
                "wait_ms": 0,
                "set_activity_anchor": true
            }),
        )
        .await
    }

    /// Read a channel's topic through the typed tool.
    pub async fn topic(&self, agent_id: &str, channel: &str) -> anyhow::Result<Value> {
        self.call(
            "irc.topic.get",
            json!({
                "agent_id": agent_id,
                "channel": channel,
                "result_detail": "compact"
            }),
        )
        .await
    }

    /// Read recent server-backed history for one target.
    pub async fn history(&self, agent_id: &str, target: &str) -> anyhow::Result<Value> {
        self.call(
            "irc.history",
            json!({
                "agent_id": agent_id,
                "target": target,
                "selector": {"kind": "latest"},
                "limit": 50,
                "include_non_message_events": true,
                "result_detail": "compact"
            }),
        )
        .await
    }

    /// Dispatch one previously validated `PRIVMSG`.
    pub async fn send(&self, agent_id: &str, target: &str, text: &str) -> anyhow::Result<()> {
        self.call(
            "irc.send",
            json!({
                "agent_id": agent_id,
                "target": target,
                "kind": "privmsg",
                "text": text,
                "multiline": "reject_if_too_long",
                "result_detail": "compact"
            }),
        )
        .await
        .map(|_| ())
    }

    /// Close an attention watch. Unknown/expired handles are harmless at cleanup.
    pub async fn close_watch(&self, watch_id: &str) -> anyhow::Result<()> {
        self.call("irc.watch.close", json!({"watch_id": watch_id}))
            .await
            .map(|_| ())
    }

    /// Disconnect the IRC guest cleanly.
    pub async fn disconnect(&self, agent_id: &str, reason: &str) -> anyhow::Result<()> {
        self.call(
            "irc.disconnect",
            json!({"agent_id": agent_id, "reason": reason}),
        )
        .await
        .map(|_| ())
    }

    /// End the underlying rmcp service.
    pub async fn stop(self) {
        let _ = self.service.cancel().await;
    }

    /// Invoke any typed gateway tool on behalf of the responder thread.
    pub async fn call_gateway_tool(
        &self,
        name: &str,
        agent_id: &str,
        arguments: Value,
    ) -> anyhow::Result<Value> {
        self.call(name, bind_responder_agent(name, agent_id, arguments)?)
            .await
    }

    async fn call(&self, name: &str, arguments: Value) -> anyhow::Result<Value> {
        let arguments: Map<String, Value> = arguments
            .as_object()
            .cloned()
            .context("internal MCP tool arguments were not an object")?;
        let response = self
            .service
            .call_tool(CallToolRequestParams::new(name.to_owned()).with_arguments(arguments))
            .await
            .with_context(|| format!("call MCP tool {name}"))?;
        if response.is_error == Some(true) {
            bail!(
                "MCP tool {name} failed: {}",
                tool_failure(&response.structured_content)
            );
        }
        let structured = response
            .structured_content
            .context("MCP tool returned no structuredContent")?;
        ensure!(
            structured.get("ok").and_then(Value::as_bool) == Some(true),
            "MCP tool {name} returned failure: {}",
            tool_failure(&Some(structured.clone()))
        );
        structured
            .get("result")
            .cloned()
            .with_context(|| format!("MCP tool {name} omitted structured result"))
    }
}

fn bind_responder_agent(name: &str, agent_id: &str, arguments: Value) -> anyhow::Result<Value> {
    ensure!(
        name.starts_with("irc."),
        "gateway tool must be in the irc namespace"
    );
    ensure!(
        !matches!(
            name,
            "irc.connect"
                | "irc.disconnect"
                | "irc.attention.open"
                | "irc.attention.check"
                | "irc.watch.create"
                | "irc.watch.close"
        ),
        "gateway tool {name} is owned by the responder lifecycle"
    );
    let mut arguments = arguments
        .as_object()
        .cloned()
        .context("gateway tool arguments must be an object")?;
    // Every remaining tool is agent scoped. Always replace a caller-supplied
    // handle with the responder-owned identity.
    arguments.insert("agent_id".into(), json!(agent_id));
    Ok(Value::Object(arguments))
}

/// Whether one subscription notification is the model wake resource.
pub fn is_model_wake(notification: &ServerNotification, model_resume_resource: &str) -> bool {
    matches!(
        notification,
        ServerNotification::ResourceUpdatedNotification(update)
            if update.params.uri == model_resume_resource
    )
}

/// Parse an attention check cursor from a known result location.
pub fn cursor_at(value: &Value, pointer: &str) -> anyhow::Result<Cursor> {
    serde_json::from_value(
        value
            .pointer(pointer)
            .cloned()
            .with_context(|| format!("result omitted cursor at {pointer}"))?,
    )
    .with_context(|| format!("result carried an invalid cursor at {pointer}"))
}

/// Private-message senders eligible for a reply in this page.
pub fn private_senders(events: &Value, accepted_nickname: &str) -> BTreeSet<String> {
    events
        .as_array()
        .into_iter()
        .flatten()
        .filter(|event| event.get("direction").and_then(Value::as_str) == Some("inbound"))
        .filter(|event| {
            event
                .get("target")
                .and_then(Value::as_str)
                .is_some_and(|target| target.eq_ignore_ascii_case(accepted_nickname))
        })
        .filter_map(|event| {
            event
                .get("source")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .collect()
}

fn subscription_filter(stored: &StoredSubscriptionFilter) -> SubscriptionFilter {
    let mut filter = SubscriptionFilter::new();
    filter.resources_list_changed = stored.resources_list_changed;
    filter.resource_subscriptions = stored.resource_subscriptions.clone();
    filter
}

fn string_at(value: &Value, pointer: &str) -> anyhow::Result<String> {
    value
        .pointer(pointer)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .with_context(|| format!("result omitted string at {pointer}"))
}

fn tool_failure(structured: &Option<Value>) -> String {
    structured
        .as_ref()
        .and_then(|value| value.pointer("/error/message"))
        .and_then(Value::as_str)
        .unwrap_or("unknown structured failure")
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_replies_are_limited_to_senders_in_that_batch() {
        let events = json!([
            {"direction":"inbound", "target":"Responder", "source":"grant"},
            {"direction":"inbound", "target":"#control", "source":"agent"},
            {"direction":"outbound", "target":"Responder", "source":"spoof"}
        ]);
        assert_eq!(
            private_senders(&events, "responder"),
            BTreeSet::from(["grant".to_owned()])
        );
    }

    #[test]
    fn cursor_shape_is_strict() {
        let value = json!({"resume_cursor":{"stream_id":"s", "sequence":7}});
        assert_eq!(
            cursor_at(&value, "/resume_cursor").expect("cursor"),
            Cursor {
                stream_id: "s".into(),
                sequence: 7
            }
        );
        assert!(cursor_at(&json!({"resume_cursor":{"sequence":7}}), "/resume_cursor").is_err());
    }

    #[test]
    fn gateway_calls_are_bound_to_the_responder_identity() {
        let bound = bind_responder_agent(
            "irc.dcc.send",
            "owned-agent",
            json!({"agent_id":"spoofed-agent", "target":"grant"}),
        )
        .expect("bind call");
        assert_eq!(bound["agent_id"], "owned-agent");
        assert_eq!(bound["target"], "grant");

        for denied in [
            "irc.connect",
            "irc.disconnect",
            "irc.attention.open",
            "irc.attention.check",
            "irc.watch.create",
            "irc.watch.close",
        ] {
            let error = bind_responder_agent(denied, "owned-agent", json!({}))
                .expect_err("responder lifecycle tool must be denied");
            assert!(error.to_string().contains("responder lifecycle"));
        }
    }

    #[test]
    fn gateway_bridge_rejects_invalid_calls() {
        assert!(bind_responder_agent("filesystem.read", "agent", json!({})).is_err());
        assert!(bind_responder_agent("irc.status", "agent", json!([])).is_err());
    }
}
