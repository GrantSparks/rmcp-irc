//! Token-minimized model attention over IRC event watches.
//!
//! MCP resource notifications can wake a host but cannot originate a model
//! turn. This module publishes the portable fallback: one compound watch and a
//! compact non-blocking check suitable for a same-conversation scheduler. The
//! watch remains an explicit process-local handle and the checkpoint remains
//! caller-owned, so the protocol transport itself stays stateless. A check may
//! also explicitly align the separate activity-hint anchor with the checkpoint
//! it returns; neither position is ever moved merely by producing a hint.

use std::collections::BTreeSet;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    agent::{
        AgentId,
        journal::{
            CursorStatus, EventClass, EventCursor, EventDirection, EventPage, EventPayload,
            IrcEvent,
        },
    },
    irc::semantic::SemanticEvent,
    mcp::{
        conversation::CompactEvent,
        resources::ResourceUris,
        tools::default_event_limit,
        watch::{AttentionSelection, WatchDescriptor, WatchFilter, WatchId},
    },
    time::Timestamp,
};

/// Longest interval a model scheduler should use while an IRC agent remains
/// connected and responsible for responding.
pub const ATTENTION_MAX_INTERVAL_SECONDS: u64 = 60;

/// Model-facing lifecycle rule shared by onboarding and tool results.
pub const ATTENTION_ONBOARDING: &str = "Before ending a turn while this IRC agent remains active, \
open attention with irc.attention.open and follow its returned delivery instructions. Keep one \
subscriptions/listen stream open and resume this conversation when modelResumeResource changes, or \
run the returned prompt immediately and then every 60 seconds in this same conversation. Do not use \
an immediate continuation loop; in Codex, a durable goal alone is not a timer. If the client supports \
neither notification mode nor a cadence-aware scheduler, disclose that responsiveness is best-effort. \
Stop delivery, close the watch, and disconnect when the task is done or abandoned.";

/// Input accepted by `irc.attention.open`.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AttentionOpenInput {
    /// Opaque handle returned by `irc.connect`.
    pub agent_id: AgentId,
    /// Task channels or peers whose complete inbound conversation needs
    /// attention. Outside these targets, direct/addressed messages and
    /// account-identified human messages still qualify; sparse lifecycle,
    /// policy, retention, and DCC events always qualify.
    #[serde(default)]
    pub full_traffic_targets: BTreeSet<String>,
}

impl AttentionOpenInput {
    /// Build the single immutable watch selection used by both notifications
    /// and scheduled checks.
    pub fn filter(&self) -> WatchFilter {
        WatchFilter {
            attention: Some(AttentionSelection {
                full_traffic_targets: self.full_traffic_targets.clone(),
            }),
            ..WatchFilter::default()
        }
    }
}

/// Client-neutral recipe for recurring attention checks.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct AttentionSchedule {
    /// Recommended delay between recurring checks after the immediate first
    /// check. This makes the cadence explicit for clients whose generic goal
    /// or continuation mechanism otherwise runs again immediately.
    pub interval_seconds: u64,
    /// Never schedule the next check later than this many seconds.
    pub max_interval_seconds: u64,
    /// The first check should run as soon as setup completes.
    pub run_immediately: bool,
    /// The recurring prompt depends on the cursor retained by this
    /// conversation, so fresh isolated runs are not equivalent.
    pub same_conversation: bool,
    /// Tool the recurring prompt calls.
    pub check_tool: &'static str,
    /// Scheduled model checks must return immediately rather than spending the
    /// already-active model turn in a long poll.
    pub check_wait_ms: u64,
    /// Scheduled checks acknowledge the separate bounded-activity hint at the
    /// checkpoint they return, without changing delivery cursor semantics.
    pub check_sets_activity_anchor: bool,
    /// Honest cost boundary: starting the recurring prompt invokes a model
    /// even when the result is `quiet`.
    pub quiet_checks_consume_model_tokens: bool,
    /// Ordinary text a same-conversation recurring task can run.
    pub prompt: String,
    /// Continuous-delivery modes available to a compatible client.
    pub delivery_modes: Vec<&'static str>,
    /// Lifecycle conditions that remove the recurring task.
    pub cancel_when: Vec<&'static str>,
}

impl AttentionSchedule {
    /// Build a schedule recipe with the concrete handles and starting cursor
    /// needed by the active conversation.
    pub fn new(watch: &WatchDescriptor, initial_cursor: &EventCursor) -> Self {
        Self {
            interval_seconds: ATTENTION_MAX_INTERVAL_SECONDS,
            max_interval_seconds: ATTENTION_MAX_INTERVAL_SECONDS,
            run_immediately: true,
            same_conversation: true,
            check_tool: "irc.attention.check",
            check_wait_ms: 0,
            check_sets_activity_anchor: true,
            quiet_checks_consume_model_tokens: true,
            prompt: format!(
                "Maintain IRC attention for agent `{}` with watch `{}`. Start with cursor \
                 `{}:{}` and thereafter use the last resume_cursor retained in this same \
                 conversation. Call irc.attention.check with wait_ms 0 and \
                 set_activity_anchor true. If state is quiet, end immediately without a summary. \
                 Otherwise report any event_gap or stream_reset, \
                 drain while has_more is true, prioritize messages carrying source_account, answer \
                 relevant messages in their originating target, and retain resume_cursor only after \
                 handling the returned events. If a handle is unknown or expired and the underlying \
                 task is still active, reconnect, reread the MOTD, reopen attention, and replace the \
                 handles. After the immediate first check, run each later check 60 seconds after the \
                 previous check; do not use an immediate continuation loop. In Codex, a durable goal \
                 alone is not a cadence-aware scheduler and can fire repeatedly without waiting, so \
                 use notification mode or an actual scheduled task that honors this interval. Cancel \
                 this recurring task, close the watch, and disconnect when the work is done or \
                 abandoned.",
                watch.agent_id, watch.watch_id, initial_cursor.stream_id, initial_cursor.sequence,
            ),
            delivery_modes: vec![
                "Notification mode: keep the returned subscriptions/listen filter active and resume this same conversation when modelResumeResource changes",
                "Recurring-check mode: run this prompt immediately, then every 60 seconds in this same conversation; do not use an immediate continuation loop",
                "Codex: a durable goal alone is not a timer; use notification mode or a cadence-aware scheduled task that honors intervalSeconds",
                "If neither mode is available, disclose that responsiveness is best-effort",
            ],
            cancel_when: vec!["task_done", "task_abandoned", "agent_disconnected"],
        }
    }
}

/// Result of `irc.attention.open`.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct AttentionOpenOutput {
    /// Registered compound watch; its URI is suitable for a zero-idle-token
    /// host subscription bridge.
    pub watch: WatchDescriptor,
    /// Caller-owned starting checkpoint representing "from now".
    pub initial_cursor: EventCursor,
    /// Filter addition for the client's one consolidated long-lived MCP
    /// subscription. Matching IRC activity is published on that stream as a
    /// resource update; it never arrives as an unsolicited request outside the
    /// stream.
    pub subscription: AttentionSubscription,
    /// Portable recurring-model fallback.
    pub schedule: AttentionSchedule,
    /// Lifecycle and cost boundary in prose for clients that do not surface
    /// structured output prominently.
    pub instructions: &'static str,
}

/// Addition to the client's consolidated `subscriptions/listen` filter.
#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AttentionSubscription {
    /// MCP method whose single response stream carries every requested
    /// notification category.
    pub method: &'static str,
    /// The addition belongs under this field of the listen request; sibling
    /// categories already needed by the client remain present there.
    pub filter_location: &'static str,
    /// `subscriptions/listen` is itself an ordinary 2026-07-28 request and
    /// therefore carries the complete required `_meta` object even though its
    /// response remains open.
    pub complete_request_metadata_required: bool,
    /// Merge this delta with list-change categories and resource URIs the
    /// client already needs; do not open one stream per watch.
    pub merge_into_existing_filter: bool,
    /// Filter fields this attention handle adds.
    pub filter_addition: AttentionSubscriptionFilter,
    /// Of the many notifications multiplexed onto the consolidated stream,
    /// this filtered URI is the one a direct host uses to decide that a model
    /// turn is warranted. Other URIs may only refresh host cache or UI.
    pub model_resume_resource: String,
    /// A listen filter is fixed when its stream is opened. If a stream already
    /// exists, reopen it with the merged filter and resume normal reconnect
    /// handling.
    pub reopen_stream_if_already_listening: bool,
    /// Notification emitted inside the opened stream when the watch matches.
    pub emits: &'static str,
}

/// Fields added to the client-opened subscription filter.
#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AttentionSubscriptionFilter {
    /// Discover appearance or disappearance of agent/watch resources.
    pub resources_list_changed: bool,
    /// Watch resources whose matching updates belong on the stream.
    pub resource_subscriptions: Vec<String>,
}

impl AttentionSubscription {
    /// Build the explicit listener request for one attention watch.
    pub fn new(watch: &WatchDescriptor, resources: &ResourceUris) -> Self {
        Self {
            method: "subscriptions/listen",
            filter_location: "params.notifications",
            complete_request_metadata_required: true,
            merge_into_existing_filter: true,
            filter_addition: AttentionSubscriptionFilter {
                resources_list_changed: true,
                // The attention URI is the filtered conversation signal. The
                // remaining stable resources cover lifecycle, changed MOTD or
                // protocol policy, reduced membership state, and DCC work
                // without subscribing to the noisy lossless event/wire feeds.
                resource_subscriptions: vec![
                    watch.uri.clone(),
                    resources.status.clone(),
                    resources.motd.clone(),
                    resources.protocol.clone(),
                    resources.state.clone(),
                    resources.dcc.clone(),
                ],
            },
            model_resume_resource: watch.uri.clone(),
            reopen_stream_if_already_listening: true,
            emits: "notifications/resources/updated",
        }
    }
}

/// Input accepted by `irc.attention.check`.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AttentionCheckInput {
    /// Opaque handle returned by `irc.connect`.
    pub agent_id: AgentId,
    /// Compound watch handle returned by `irc.attention.open`.
    pub watch_id: WatchId,
    /// Last `resume_cursor` adopted after a successful check, initially the
    /// `initial_cursor` returned by `irc.attention.open`.
    pub cursor: EventCursor,
    /// Maximum compact attention events returned. Defaults to 100 and cannot
    /// exceed the configured event page maximum, 1000 by default.
    #[serde(default = "default_event_limit")]
    pub limit: usize,
    /// Reserved for direct host integrations. Scheduled model checks must use
    /// zero; positive values long poll up to the configured maximum of 30000
    /// milliseconds.
    #[serde(default)]
    pub wait_ms: u64,
    /// Also record the returned `resume_cursor` as the anchor used by bounded
    /// activity hints. The scheduler recipe sets this true so a handled
    /// attention page is not repeatedly advertised as unread on later tool
    /// results. This changes only the courtesy hint: event/watch cursors remain
    /// wholly caller-owned and a retry from the old cursor is still safe.
    #[serde(default)]
    pub set_activity_anchor: bool,
}

/// Outcome of one compact attention check.
#[derive(Clone, Copy, Debug, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AttentionCheckState {
    /// No selected events were retained after the requested position.
    Quiet,
    /// One or more selected events were returned without known loss.
    Events,
    /// The actor stream changed, normally because the gateway restarted.
    StreamReset,
    /// Records after the requested cursor were evicted before this check.
    EventGap,
}

/// Compact event shape used only by model attention.
///
/// It is a superset of the ordinary conversational projection: sparse
/// lifecycle, policy, retention, and DCC signals receive a short summary so a
/// scheduler fallback observes the same critical classes that wake the host's
/// consolidated subscription stream.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct AttentionEvent {
    /// Durable journal position.
    pub cursor: EventCursor,
    /// Server time when available, otherwise gateway receipt time.
    pub at: Timestamp,
    /// Stable event class.
    pub class: EventClass,
    /// Inbound, outbound, or gateway-internal.
    pub direction: EventDirection,
    /// Conversation target, when present.
    pub target: Option<String>,
    /// Case-preserved source nickname, when present.
    pub source: Option<String>,
    /// Registered account from `account-tag`, when present. Missing remains
    /// unknown rather than proving the sender is an agent.
    pub source_account: Option<String>,
    /// Conversational text, when present.
    pub text: Option<String>,
    /// Whether the event addressed the owning agent.
    pub mentions_me: bool,
    /// Short action-oriented description for non-text events.
    pub summary: Option<String>,
}

impl AttentionEvent {
    /// Project every class selected by [`AttentionSelection`] into a compact
    /// model-facing form.
    pub fn project(event: &IrcEvent) -> Option<Self> {
        if let Some(conversation) = CompactEvent::project(event) {
            return Some(Self {
                cursor: conversation.cursor,
                at: conversation.at,
                class: conversation.class,
                direction: conversation.direction,
                target: conversation.target,
                source: conversation.source,
                source_account: conversation.source_account,
                text: conversation.text,
                mentions_me: conversation.mentions_me,
                summary: conversation.summary,
            });
        }

        let summary = match event.semantic.as_ref() {
            Some(EventPayload::Connection(connection)) => format!(
                "connection state changed to {}",
                serialized_name(&connection.state)
            ),
            Some(EventPayload::Motd(_)) => "server MOTD changed; reread the MOTD resource".into(),
            Some(EventPayload::Pressure(pressure)) => format!(
                "journal pressure evicted {} record(s) since the previous report",
                pressure.evicted_since_previous_report
            ),
            Some(EventPayload::DccSession(session)) => format!(
                "DCC {} with {} is {}",
                serialized_name(&session.kind),
                session.peer,
                session.state
            ),
            Some(EventPayload::DccFailure(failure)) => failure.peer.as_ref().map_or_else(
                || format!("DCC failed: {}", failure.error),
                |peer| format!("DCC with {peer} failed: {}", failure.error),
            ),
            Some(EventPayload::Irc(projection)) => match &projection.event {
                SemanticEvent::ProtocolCompatibility { .. } => {
                    "IRC protocol capabilities changed; reread the protocol resource".into()
                }
                SemanticEvent::ServerMotd { .. } => {
                    "server MOTD changed; reread the MOTD resource".into()
                }
                SemanticEvent::ConnectionLifecycle { .. } => {
                    "IRC connection lifecycle changed; reread status".into()
                }
                _ => fallback_summary(event.class)?.into(),
            },
            _ => fallback_summary(event.class)?.into(),
        };
        Some(Self {
            cursor: event.cursor.clone(),
            at: event.server_time.unwrap_or(event.received_at),
            class: event.class,
            direction: event.direction,
            target: event.target.clone(),
            source: None,
            source_account: None,
            text: None,
            mentions_me: event.mentions_me,
            summary: Some(summary),
        })
    }
}

fn fallback_summary(class: EventClass) -> Option<&'static str> {
    Some(match class {
        EventClass::MessageChannel
        | EventClass::MessagePrivate
        | EventClass::MessageAction
        | EventClass::MessageNotice => {
            "message has no compact text; inspect its lossless journal record"
        }
        EventClass::DccChatMessage => {
            "DCC chat message has no compact text; inspect its lossless journal record"
        }
        EventClass::ConnectionLifecycle => "IRC connection lifecycle changed; reread status",
        EventClass::ServerMotd => "server MOTD changed; reread the MOTD resource",
        EventClass::ProtocolCompatibility => {
            "IRC protocol capabilities changed; reread the protocol resource"
        }
        EventClass::ChannelState => "channel topic or modes changed; reread channel state",
        EventClass::JournalPressure => {
            "journal retention is under pressure; drain attention promptly"
        }
        EventClass::DccChatOffered => "DCC chat was offered; reread the DCC resource",
        EventClass::DccTransferOffered => "DCC transfer was offered; reread the DCC resource",
        EventClass::DccConnected => "DCC session connected; reread the DCC resource",
        EventClass::DccChatClosed => "DCC chat closed; reread the DCC resource",
        EventClass::DccTransferCompleted => "DCC transfer completed; reread the DCC resource",
        EventClass::DccRejected => "DCC session was rejected; reread the DCC resource",
        EventClass::DccCancelled => "DCC session was cancelled; reread the DCC resource",
        EventClass::DccFailed => "DCC session failed; reread the DCC resource",
        _ => return None,
    })
}

fn serialized_name(value: &impl Serialize) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "unknown".into())
}

/// Token-minimized result of `irc.attention.check`.
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct AttentionCheckOutput {
    /// Quiet, events, or an explicit loss condition.
    pub state: AttentionCheckState,
    /// Compact selected events. Omitted entirely on the normal quiet path.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<AttentionEvent>,
    /// Position to adopt after all returned events have been handled. Unlike a
    /// general filtered cursor, a drained attention check advances through
    /// inspected non-matches because this watch's selection is immutable.
    pub resume_cursor: EventCursor,
    /// Whether another selected page is already retained. When true, call the
    /// check again immediately using `resume_cursor`.
    pub has_more: bool,
    /// Oldest retained position, included only when reporting loss/reset to aid
    /// explicit recovery.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldest_available: Option<EventCursor>,
}

impl AttentionCheckOutput {
    /// Convert a general journal page into the attention-specific checkpoint
    /// contract.
    pub fn from_page(page: EventPage) -> Self {
        let state = match page.status {
            CursorStatus::StreamReset => AttentionCheckState::StreamReset,
            CursorStatus::EventGap => AttentionCheckState::EventGap,
            CursorStatus::Current if page.events.is_empty() => AttentionCheckState::Quiet,
            CursorStatus::Current => AttentionCheckState::Events,
        };
        let resume_cursor = if page.has_more {
            page.next_cursor.clone()
        } else {
            // The immutable attention selection inspected every retained
            // record through this high-water mark. Skipping its non-matches is
            // safe and prevents an otherwise-quiet narrow cursor from aging
            // out under unrelated traffic.
            page.latest.clone()
        };
        let events: Vec<AttentionEvent> = page
            .events
            .iter()
            .filter_map(AttentionEvent::project)
            .collect();
        debug_assert_eq!(events.len(), page.events.len());
        Self {
            state,
            events,
            resume_cursor,
            has_more: page.has_more,
            oldest_available: (page.status != CursorStatus::Current)
                .then_some(page.oldest_available)
                .flatten(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        agent::{
            journal::{
                ConnectionEvent, EventClass, EventCorrelation, EventDirection, EventOrigin,
                EventPayload, EventVerbosity, IrcEvent,
            },
            state::ConnectionState,
        },
        irc::{
            semantic::{SemanticEvent, SemanticProjection, Source},
            target::ChannelName,
        },
        time::Timestamp,
    };

    fn cursor(sequence: u64) -> EventCursor {
        EventCursor {
            stream_id: "stream".into(),
            sequence,
        }
    }

    fn message(sequence: u64) -> IrcEvent {
        let semantic = SemanticEvent::MessageChannel {
            source: Source {
                name: "grant".into(),
                user: Some("grant".into()),
                host: Some("example.test".into()),
                account: Some("grant".into()),
            },
            channel: ChannelName::new("#work").expect("channel"),
            text: "status?".into(),
        };
        IrcEvent {
            cursor: cursor(sequence),
            agent_id: AgentId::new(),
            direction: EventDirection::Inbound,
            class: EventClass::MessageChannel,
            origin: EventOrigin::Live,
            verbosity: EventVerbosity::Semantic,
            target: Some("#work".into()),
            server_time: None,
            received_at: Timestamp::now(),
            correlation: EventCorrelation::default(),
            semantic: Some(EventPayload::Irc(SemanticProjection::from(semantic))),
            wire: None,
            mentions_me: false,
            authored_by_me: false,
        }
    }

    fn page(events: Vec<IrcEvent>, has_more: bool) -> EventPage {
        EventPage {
            stream_id: "stream".into(),
            requested_cursor: Some(cursor(10)),
            status: CursorStatus::Current,
            oldest_available: Some(cursor(1)),
            latest: cursor(99),
            next_cursor: events
                .last()
                .map_or_else(|| cursor(10), |event| event.cursor.clone()),
            events,
            has_more,
        }
    }

    #[test]
    fn a_quiet_check_advances_through_every_inspected_non_match() {
        let output = AttentionCheckOutput::from_page(page(Vec::new(), false));
        assert_eq!(output.state, AttentionCheckState::Quiet);
        assert_eq!(output.resume_cursor, cursor(99));
        assert!(output.events.is_empty());
        let json = serde_json::to_value(output).expect("serialize attention check");
        assert!(
            json.get("events").is_none(),
            "quiet results stay token-small"
        );
    }

    #[test]
    fn a_truncated_check_resumes_after_the_last_delivered_match() {
        let output = AttentionCheckOutput::from_page(page(vec![message(42)], true));
        assert_eq!(output.state, AttentionCheckState::Events);
        assert_eq!(output.resume_cursor, cursor(42));
        assert!(output.has_more);
        assert_eq!(output.events[0].source_account.as_deref(), Some("grant"));
    }

    #[test]
    fn the_schedule_describes_both_continuous_delivery_modes() {
        let agent_id = AgentId::new();
        let watch = WatchDescriptor {
            watch_id: WatchId::new(),
            agent_id,
            filter: WatchFilter::default(),
            uri: "irc://watches/example".into(),
            expires_at: Timestamp::now(),
        };
        let schedule = AttentionSchedule::new(&watch, &cursor(10));
        assert_eq!(schedule.interval_seconds, 60);
        assert_eq!(schedule.max_interval_seconds, 60);
        assert!(schedule.same_conversation);
        assert!(schedule.check_sets_activity_anchor);
        assert!(schedule.prompt.contains("set_activity_anchor true"));
        assert!(schedule.prompt.contains("60 seconds after"));
        assert!(schedule.prompt.contains("durable goal alone"));
        assert!(
            schedule
                .delivery_modes
                .iter()
                .any(|mode| mode.contains("Notification mode"))
        );
        assert!(
            schedule
                .delivery_modes
                .iter()
                .any(|mode| mode.contains("Recurring-check mode"))
        );
        assert!(
            schedule
                .delivery_modes
                .iter()
                .any(|mode| mode.contains("Codex"))
        );
    }

    #[test]
    fn onboarding_rejects_an_immediate_codex_continuation_loop() {
        assert!(ATTENTION_ONBOARDING.contains("every 60 seconds"));
        assert!(ATTENTION_ONBOARDING.contains("durable goal alone is not a timer"));
        assert!(ATTENTION_ONBOARDING.contains("cadence-aware scheduler"));
    }

    #[test]
    fn a_selected_lifecycle_event_has_a_compact_attention_projection() {
        let mut connection = message(43);
        connection.direction = EventDirection::Internal;
        connection.class = EventClass::ConnectionLifecycle;
        connection.target = None;
        connection.semantic = Some(EventPayload::Connection(ConnectionEvent {
            state: ConnectionState::Reconnecting,
        }));

        let selection = AttentionSelection::default();
        assert!(selection.matches(&connection, Default::default()));
        let output = AttentionCheckOutput::from_page(page(vec![connection], false));
        assert_eq!(output.state, AttentionCheckState::Events);
        assert_eq!(output.events.len(), 1);
        assert!(
            output.events[0]
                .summary
                .as_deref()
                .is_some_and(|summary| summary.contains("reconnecting"))
        );
    }

    #[test]
    fn every_sparse_selected_class_has_a_projection_even_without_a_payload() {
        let selection = AttentionSelection::default();
        for class in crate::mcp::watch::SPARSE_ATTENTION_CLASSES {
            let mut event = message(44);
            event.direction = EventDirection::Internal;
            event.class = *class;
            event.target = None;
            event.semantic = None;
            assert!(selection.matches(&event, Default::default()));
            assert!(
                AttentionEvent::project(&event).is_some(),
                "selected class {class:?} must have a compact fallback"
            );
        }
    }

    #[test]
    fn the_subscription_is_a_delta_for_one_consolidated_listen_stream() {
        let agent_id = AgentId::new();
        let resources = ResourceUris::for_agent(&agent_id);
        let watch = WatchDescriptor {
            watch_id: WatchId::new(),
            agent_id,
            filter: WatchFilter::default(),
            uri: "irc://watches/example".into(),
            expires_at: Timestamp::now(),
        };
        let subscription = AttentionSubscription::new(&watch, &resources);
        assert!(subscription.merge_into_existing_filter);
        assert!(subscription.reopen_stream_if_already_listening);
        assert!(subscription.filter_addition.resources_list_changed);
        assert_eq!(subscription.model_resume_resource, watch.uri);
        assert!(
            subscription
                .filter_addition
                .resource_subscriptions
                .contains(&watch.uri)
        );
        let json = serde_json::to_value(&subscription).expect("serialize subscription delta");
        assert_eq!(json["method"], "subscriptions/listen");
        assert_eq!(json["filterLocation"], "params.notifications");
        assert_eq!(json["completeRequestMetadataRequired"], true);
        assert_eq!(json["modelResumeResource"], "irc://watches/example");
        assert_eq!(json["filterAddition"]["resourcesListChanged"], true);

        let accepted_wire_shape: rmcp::model::SubscriptionFilter =
            serde_json::from_value(json["filterAddition"].clone())
                .expect("filter addition uses the protocol's exact wire shape");
        assert_eq!(accepted_wire_shape.resources_list_changed, Some(true));
        assert_eq!(
            accepted_wire_shape.resource_subscriptions,
            Some(subscription.filter_addition.resource_subscriptions)
        );
    }
}
